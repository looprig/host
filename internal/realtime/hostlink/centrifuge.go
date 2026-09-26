package hostlink

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/prometheus/client_golang/prometheus"
)

// disconnectAuthentication and disconnectUnsupportedVersion are the two
// terminal HostLink disconnect codes. Both sit in 4500-4999, documented by
// centrifuge@v0.38.0 disconnect.go:26 as an application terminal range in
// which a client should perform no automatic reconnect — that line states a
// convention for client implementations, not a behaviour this server enforces.
// The codes are distinct because they distinguish two different failures to the
// Factory client, and the tests assert each exact value rather than only the
// range.
const (
	disconnectAuthentication     uint32 = 4500
	disconnectUnsupportedVersion uint32 = 4501
)

// Transient bytes queued behind a stalled consumer are bounded by burst +
// rate × stall. At 8 KiB + 32 KiB/s × 30 s = 991,232 bytes, transient data
// alone remains below the 1 MiB Centrifuge client queue limit. More than 30 s
// of total stall is needed for transient bytes alone to reach that limit; a
// client stalled that long is disconnected by enduring traffic and repairs by
// replaying the durable journal. Keep the queue limit and admission constants
// together when changing either side of this inequality.
const (
	ephemeralRateBytesPerSecond = 32 * 1024
	ephemeralBurstBytes         = 8 * 1024
	ephemeralMaxFrameBytes      = 4 * 1024
	clientQueueMaxBytes         = 1024 * 1024
)

// Transient admission limits are shared with public Composition validation.
const (
	DefaultTransientRateBytesPerSecond = ephemeralRateBytesPerSecond
	DefaultTransientBurstBytes         = ephemeralBurstBytes
	MaxTransientFrameBytes             = ephemeralMaxFrameBytes
	ClientQueueMaxBytes                = clientQueueMaxBytes
)

type centrifugeServer struct {
	node             *centrifuge.Node
	handler          http.Handler
	mu               sync.Mutex // serializes ephemeral admission with its broadcast
	budget           *ephemeralBudget
	subscribed       map[string]map[string]uint64 // channel -> physical client ID -> subscribe attempt
	nextSubscription uint64
}

// NewCentrifugeServer constructs and starts the embedded JSON/WebSocket
// transport. The node uses Centrifuge's in-process broker only; application
// durability remains in SessionStore rather than transport history.
func NewCentrifugeServer(config Config) (Server, error) {
	if config.Authenticator == nil {
		return nil, errors.New("hostlink: authenticator is required")
	}
	if err := config.TenantID.Validate(); err != nil {
		return nil, errors.New("hostlink: tenant ID is invalid")
	}
	if config.PingInterval < 0 || config.PongTimeout < 0 {
		return nil, errors.New("hostlink: heartbeat durations must not be negative")
	}
	if (config.PingInterval == 0) != (config.PongTimeout == 0) {
		return nil, errors.New("hostlink: ping interval and pong timeout must be configured together")
	}
	// One second is the resolution of the wire, not a preference: Centrifuge
	// advertises the interval as whole seconds — client.go:2466 sends
	// res.Ping = uint32(c.pingInterval.Seconds()) — so a sub-second interval
	// truncates to ping: 0, which a client cannot tell apart from no ping.
	if config.PingInterval > 0 && config.PingInterval < time.Second {
		return nil, errors.New("hostlink: ping interval must be at least one second")
	}
	// Deliberately stricter than the dependency: Centrifuge only logs a warning
	// for this configuration (warnAboutIncorrectPingPongConfig, config.go:229-231)
	// and then runs with it. HostLink rejects it at construction instead.
	if config.PingInterval > 0 && config.PongTimeout > 0 && config.PongTimeout >= config.PingInterval {
		return nil, errors.New("hostlink: pong timeout must be shorter than ping interval")
	}

	for index, capability := range config.Capabilities {
		if !slices.Contains(advertisedCapabilities, capability) || slices.Contains(config.Capabilities[:index], capability) {
			return nil, errors.New("hostlink: " + strconv.Quote(capability) + " is not a capability this package can advertise, or is named twice")
		}
	}
	if config.TransientRateBytesPerSecond < 0 || config.TransientBurstBytes < 0 {
		return nil, errors.New("hostlink: transient rate and burst must be positive")
	}
	metrics := prometheus.NewRegistry()
	nodeConfig := centrifuge.Config{
		Name:               "looprig-hostlink",
		Version:            "v1",
		Metrics:            centrifuge.MetricsConfig{RegistererGatherer: metrics},
		ClientQueueMaxSize: clientQueueMaxBytes,
		// Centrifuge would otherwise default this to 255 and refuse a channel
		// two maximum-length Core identifiers mint. MaxChannelBytes is derived
		// from the encoding, so this ceiling moves with it rather than being a
		// number somebody chose.
		ChannelMaxLength: MaxChannelBytes,
	}
	if config.Multiplexer != nil {
		// ONE budget, not two. Centrifuge defaults ClientChannelLimit to 128
		// (centrifuge@v0.38.0 node.go:135-137), which would be a second and
		// silently different answer to "how many sessions may one link hold" —
		// and the transport's refusal would arrive at subscribe, after the
		// Multiplexer had already accepted the bind. Taking the limit from the
		// Multiplexer leaves no_capacity decided in exactly one place.
		nodeConfig.ClientChannelLimit = config.Multiplexer.perLink
	}
	node, err := centrifuge.New(nodeConfig)
	if err != nil {
		return nil, err
	}
	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		if err := config.Authenticator.VerifyTenant(ctx, config.TenantID, event.Token); err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectAuthentication,
				Reason: "service authentication failed",
			}
		}
		// THE CONNECT FRAMING IS CORE'S, since core v0.9.0. Host decoded the
		// connect Data as a bare VersionNegotiationRequest and replied bare;
		// Factory v0.1.1 wrapped both directions as {"version_negotiation":...}
		// (B8), and neither side could tell. Core froze the BARE shape as the
		// contract and owns both encoders, so this Host names no shape of its
		// own: a wrapped request is refused by Core's strict decoder as an
		// unknown member, and the reply is whatever Core's encoder emits.
		request, err := sessionwire.DecodeHostLinkConnectRequest(event.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectUnsupportedVersion,
				Reason: "unsupported wire version",
			}
		}
		response, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectUnsupportedVersion,
				Reason: "unsupported wire version",
			}
		}
		// THE CAPABILITY ADVERTISEMENT. A Factory refuses to place on, or
		// attach to, a Host that does not advertise hostlink.attach — a v0.1.0
		// Host answers that method from the channel arm with runtime_unavailable,
		// indistinguishable from a real refusal (core spec F4). The set is the
		// dispatch table's, and a test derives the table from source and holds
		// the two equal in both directions. A server without a Multiplexer has
		// no dispatch table and therefore advertises no methods.
		if config.Multiplexer != nil {
			response = response.WithHostLinkMethods(append(append([]string(nil), advertisedMethods...), config.Capabilities...)...)
		}
		data, err := sessionwire.EncodeHostLinkConnectReply(response)
		if err != nil {
			return centrifuge.ConnectReply{}, err
		}
		reply := centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: string(config.TenantID)},
			Data:        data,
		}
		if config.PingInterval != 0 || config.PongTimeout != 0 {
			reply.PingPongConfig = &centrifuge.PingPongConfig{
				PingInterval: config.PingInterval,
				PongTimeout:  config.PongTimeout,
			}
		}
		return reply, nil
	})
	rate, burst := config.TransientRateBytesPerSecond, config.TransientBurstBytes
	if rate == 0 {
		rate = ephemeralRateBytesPerSecond
	}
	if burst == 0 {
		burst = ephemeralBurstBytes
	}
	server := &centrifugeServer{
		node:       node,
		budget:     newEphemeralBudget(rate, burst, time.Now),
		subscribed: make(map[string]map[string]uint64),
	}
	if config.Multiplexer != nil {
		node.OnConnect(func(client *centrifuge.Client) { config.Multiplexer.install(client, server) })
	}
	if err := node.Run(); err != nil {
		return nil, err
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	})
	server.handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !selectsJSONProtocol(request) {
			http.Error(writer, "HostLink requires the JSON protocol", http.StatusBadRequest)
			return
		}
		websocketHandler.ServeHTTP(writer, request)
	})
	return server, nil
}

func (s *centrifugeServer) Handler() http.Handler { return s.handler }

// Publish puts one payload on one channel through the node's in-process broker.
//
// The PublishResult the node returns is deliberately discarded. It carries the
// broker's stream offset and epoch, which exist for history recovery, and this
// node runs with no history at all — application durability is SessionStore's,
// never transport history. Returning an offset a caller could resume from would
// advertise a recovery this transport cannot perform.
func (s *centrifugeServer) Publish(channel string, payload []byte) error {
	_, err := s.node.Publish(channel, payload)
	return err
}

// TryPublishEphemeral publishes only when every subscribed physical client has
// room in its own rate budget. A single client can subscribe to several session
// channels, so a channel-scoped bucket would not bound that client's queue.
// The caller counts false as a payload-free transient drop.
func (s *centrifugeServer) TryPublishEphemeral(channel string, payload []byte) bool {
	if len(payload) == 0 || len(payload) > ephemeralMaxFrameBytes {
		return false
	}
	queuedBytes, err := queuedTransientBytes(channel, payload)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.admitSubscribed(channel, s.node.Hub().Connections(), queuedBytes) {
		return false
	}
	_, err = s.node.Publish(channel, payload)
	return err == nil
}

// queuedTransientBytes uses the same JSON publication envelope Centrifuge's
// pinned in-process broker enqueues for a client-protocol subscriber. Counting
// the encoded queue item includes the channel and framing, not just the data.
func queuedTransientBytes(channel string, payload []byte) (int, error) {
	frame, err := protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: &protocol.Push{
		Channel: channel, Pub: &protocol.Publication{Data: payload},
	}})
	return len(frame), err
}

// noteSubscribed runs before Multiplexer tells Centrifuge to register the
// subscription. Holding the same gate as TryPublish means a newly subscribed
// client is charged before any broadcast can include it.
func (s *centrifugeServer) noteSubscribed(clientID, channel string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribed == nil {
		s.subscribed = make(map[string]map[string]uint64)
	}
	if s.subscribed[channel] == nil {
		s.subscribed[channel] = make(map[string]uint64)
	}
	s.nextSubscription++
	s.subscribed[channel][clientID] = s.nextSubscription
	return s.nextSubscription
}

// subscriptionFinished removes a rejected attempt once Centrifuge has
// completed its reply. A later attempt may already exist for the same channel;
// its generation must survive an older completion callback.
func (s *centrifugeServer) subscriptionFinished(clientID, channel string, attempt uint64, accepted bool) {
	if accepted {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribed[channel][clientID] != attempt {
		return
	}
	delete(s.subscribed[channel], clientID)
	if len(s.subscribed[channel]) == 0 {
		delete(s.subscribed, channel)
	}
}

func (s *centrifugeServer) noteUnsubscribed(clientID, channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribed[channel], clientID)
	if len(s.subscribed[channel]) == 0 {
		delete(s.subscribed, channel)
	}
}

func (s *centrifugeServer) forgetClient(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for channel, clients := range s.subscribed {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(s.subscribed, channel)
		}
	}
	s.budget.forget(clientID)
}

// admitSubscribed is called while s.mu is held. The active snapshot omits
// clients already removed from the Hub; a pending accepted subscription is
// present in s.subscribed even before Client.IsSubscribed becomes true.
func (s *centrifugeServer) admitSubscribed(channel string, active map[string]*centrifuge.Client, size int) bool {
	ids := make([]string, 0, len(s.subscribed[channel]))
	for id := range s.subscribed[channel] {
		if _, found := active[id]; found {
			ids = append(ids, id)
		} else {
			delete(s.subscribed[channel], id)
		}
	}
	if len(s.subscribed[channel]) == 0 {
		delete(s.subscribed, channel)
	}
	return s.budget.admit(ids, size)
}

func (s *centrifugeServer) Close(ctx context.Context) error { return s.node.Shutdown(ctx) }

// selectsJSONProtocol reports whether the request explicitly selects the JSON
// client protocol, and is the sole gate in front of the Centrifuge WebSocket
// handler.
//
// Provenance of the two query keys. "format" and "cf_protocol" are not names
// HostLink chose; they mirror centrifuge@v0.38.0 handler_websocket.go:139,
// which selects the protobuf protocol when
// query.Get("format") == "protobuf" || query.Get("cf_protocol") == "protobuf".
// Mirroring a dependency's private selection logic is only safe while the
// dependency cannot add a third key underneath the mirror, which is why the
// version guard in server_test.go asserts the exact version v0.38.0 rather
// than a minimum. That guard and this function are one unit: moving the pin
// obliges re-reading handler_websocket.go's key list.
//
// Policy. The result is an AND over every signal that was supplied and an OR
// over their presence — every rejection leaves through an early return, and
// selected records only that something explicit was seen at all — so an absent
// selector is a rejection, not a default. Where the two differ, HostLink is
// deliberately stricter than Centrifuge: Centrifuge reads only the first value
// of each key via query.Get, while this rejects a supplied key with any value
// other than "json", and it rejects a repeated physical
// Sec-WebSocket-Protocol field outright because gorilla's Subprotocols reads
// only the first such field (websocket@v1.5.3 server.go:311-321) and would
// silently ignore the rest.
//
// The heartbeat mechanism is HostLink's and not the client's, which is why
// cf_ws_frame_ping_pong is rejected on PRESENCE rather than on value. See
// rejectedTransportQueryKeys.
func selectsJSONProtocol(request *http.Request) bool {
	selected := false
	headerValues := request.Header.Values("Sec-WebSocket-Protocol")
	if len(headerValues) > 1 {
		return false
	}
	if len(headerValues) == 1 {
		for _, token := range strings.Split(headerValues[0], ",") {
			if strings.TrimSpace(token) != "centrifuge-json" {
				return false
			}
			selected = true
		}
	}

	query := request.URL.Query()
	for _, key := range rejectedTransportQueryKeys {
		if _, supplied := query[key]; supplied {
			return false
		}
	}
	for _, key := range []string{"format", "cf_protocol"} {
		values, supplied := query[key]
		if !supplied {
			continue
		}
		for _, value := range values {
			if value != "json" {
				return false
			}
			selected = true
		}
	}
	return selected
}

// rejectedTransportQueryKeys are query keys that reconfigure the TRANSPORT
// rather than select a protocol, and that HostLink refuses outright.
//
// There is one, and O5.1 handed it over undecided. centrifuge@v0.38.0
// handler_websocket.go:142 reads query.Get("cf_ws_frame_ping_pong") == "true"
// and, when it is set, gives the transport a PingPongConfig of {-1,-1}. A
// zero-configured HostLink sends no PingPongConfig of its own, so on that path
// the transport's wins and the connect reply carries no ping or pong field at
// all: a CLIENT would have chosen the Host's liveness mechanism. It is not a
// liveness hole — the WebSocket frame defaults are also 25s/10s — but a
// heartbeat an operator configured and a heartbeat a client asked for are
// different facts, and the second must not be able to impersonate the first.
//
// The asymmetry is the reason this is a rejection and not a defaulting. When
// PingInterval and PongTimeout ARE configured the reply's PingPongConfig wins
// over the transport, so only the zero-config Host is influenceable; answering
// it by always emitting a PingPongConfig would mean restating Centrifuge's own
// 25s/10s defaults in Host and owning them forever. Refusing the key leaves
// exactly one authority for the heartbeat and adds no constant to keep in
// sync.
//
// The gate is on PRESENCE, not on the value "true", and that is deliberate:
// value-sensitivity would mirror the dependency's parsing a second time, which
// is the fragile coupling the provenance note above already warns about. A
// Factory has no reason to send the key at either value, so a 400 for
// cf_ws_frame_ping_pong=false costs nothing and cannot drift.
var rejectedTransportQueryKeys = []string{"cf_ws_frame_ping_pong"}
