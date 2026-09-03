package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/prometheus/client_golang/prometheus"
)

// disconnectAuthentication and disconnectUnsupportedVersion are the two
// terminal HostLink disconnect codes. Both sit in 4500-4999, documented by
// centrifuge@v0.38.0 disconnect.go:26 as an application terminal range in
// which a client performs no automatic reconnect. The codes are distinct because they
// distinguish two different failures to the Factory client, and the tests
// assert each exact value rather than only the range.
const (
	disconnectAuthentication     uint32 = 4500
	disconnectUnsupportedVersion uint32 = 4501
)

type centrifugeServer struct {
	node    *centrifuge.Node
	handler http.Handler
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

	registry := prometheus.NewRegistry()
	node, err := centrifuge.New(centrifuge.Config{
		Name:    "looprig-hostlink",
		Version: "v1",
		Metrics: centrifuge.MetricsConfig{RegistererGatherer: registry},
	})
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
		var request sessionwire.VersionNegotiationRequest
		if err := json.Unmarshal(event.Data, &request); err != nil {
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
		data, err := json.Marshal(response)
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
	if err := node.Run(); err != nil {
		return nil, err
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	})
	server := &centrifugeServer{node: node}
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
// over their presence — selected does double duty as "nothing invalid seen so
// far" and "something explicit was seen at all" — so an absent selector is a
// rejection, not a default. Where the two differ, HostLink is deliberately
// stricter than Centrifuge: Centrifuge reads only the first value of each key
// via query.Get, while this rejects if any value of any supplied key is not
// "json", and it rejects a repeated physical Sec-WebSocket-Protocol field
// outright because gorilla's Subprotocols reads only the first such field
// (websocket@v1.5.3 server.go:311-321) and would silently ignore the rest.
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
