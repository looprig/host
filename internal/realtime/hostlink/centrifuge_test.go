package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/realtime/hostlink"
)

const (
	testTenant     = sessionwire.TenantID("tenant-service")
	testCredential = "factory-service-secret"
)

type recordingAuthenticator struct {
	mu          sync.Mutex
	wantToken   string
	err         error
	credentials []string
}

type authenticatorFunc func(context.Context, sessionwire.TenantID, string) error

func (function authenticatorFunc) VerifyTenant(ctx context.Context, tenant sessionwire.TenantID, credential string) error {
	return function(ctx, tenant, credential)
}

func (a *recordingAuthenticator) VerifyTenant(_ context.Context, tenant sessionwire.TenantID, credential string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.credentials = append(a.credentials, string(tenant)+":"+credential)
	if credential != a.wantToken {
		return errors.New("invalid service credential")
	}
	return a.err
}

func (a *recordingAuthenticator) calls() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.credentials...)
}

// clientReply is the subset of a Centrifuge client-protocol reply these tests
// read. It is not connect-specific: the optional-facilities test decodes
// publish, history, presence and subscribe replies through it too.
type clientReply struct {
	ID      uint32 `json:"id"`
	Connect *struct {
		Data json.RawMessage `json:"data"`
		Ping uint32          `json:"ping"`
		Pong bool            `json:"pong"`
	} `json:"connect,omitempty"`
	Error *struct {
		Code    uint32 `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func TestConnectAuthenticatesServiceAndNegotiatesCoreVersion(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "https://unrelated.factory.example")
	defer connection.Close()
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{7, sessionwire.CurrentWireVersion},
	})
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect reply = %#v, want successful connect", reply)
	}
	var negotiated sessionwire.VersionNegotiationResponse
	if err := json.Unmarshal(reply.Connect.Data, &negotiated); err != nil {
		t.Fatalf("decode negotiation response: %v", err)
	}
	if negotiated.Version != sessionwire.CurrentWireVersion {
		t.Errorf("negotiated version = %d, want %d", negotiated.Version, sessionwire.CurrentWireVersion)
	}
	if got, want := auth.calls(), []string{string(testTenant) + ":" + testCredential}; !slices.Equal(got, want) {
		t.Errorf("authentication calls = %v, want %v", got, want)
	}
}

func TestAuthenticationReceivesTheConnectionContext(t *testing.T) {
	type contextKey struct{}
	const contextValue = "request-scoped-value"
	received := make(chan context.Context, 1)
	auth := authenticatorFunc(func(ctx context.Context, _ sessionwire.TenantID, _ string) error {
		received <- ctx
		return nil
	})
	server, err := hostlink.NewCentrifugeServer(hostlink.Config{TenantID: testTenant, Authenticator: auth})
	if err != nil {
		t.Fatalf("NewCentrifugeServer: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := context.WithValue(request.Context(), contextKey{}, contextValue)
		server.Handler().ServeHTTP(writer, request.WithContext(ctx))
	}))
	defer httpServer.Close()
	connection := dial(t, httpServer.URL, "")
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		connection.Close()
		t.Fatalf("connect reply = %#v", reply)
	}

	var authenticationContext context.Context
	select {
	case ctx := <-received:
		authenticationContext = ctx
		if got := ctx.Value(contextKey{}); got != contextValue {
			t.Errorf("authentication context value = %v, want %q", got, contextValue)
		}
	case <-time.After(time.Second):
		connection.Close()
		t.Fatal("authentication was not called")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	select {
	case <-authenticationContext.Done():
		if !errors.Is(authenticationContext.Err(), context.Canceled) {
			t.Errorf("authentication context error = %v, want context.Canceled", authenticationContext.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("authentication context was not canceled with its connection")
	}
}

func TestServerRejectsIncompleteOrUnrepresentableHeartbeatConfiguration(t *testing.T) {
	t.Parallel()

	auth := &recordingAuthenticator{wantToken: testCredential}
	for _, test := range []struct {
		name string
		ping time.Duration
		pong time.Duration
	}{
		{name: "ping without pong", ping: time.Second},
		{name: "pong without ping", pong: time.Second},
		{name: "subsecond ping", ping: 500 * time.Millisecond, pong: 250 * time.Millisecond},
		{name: "pong not shorter", ping: time.Second, pong: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := hostlink.NewCentrifugeServer(hostlink.Config{
				TenantID:      testTenant,
				Authenticator: auth,
				PingInterval:  test.ping,
				PongTimeout:   test.pong,
			})
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = server.Close(ctx)
				t.Fatal("NewCentrifugeServer accepted an unsafe heartbeat configuration")
			}
		})
	}
}

func TestConnectRejectsInvalidServiceCredential(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	disconnect := rejectedConnect(t, connection, "wrong-secret", sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	assertTerminalDisconnect(t, disconnect, 4500, "authentication")
}

func TestConnectRejectsUnsupportedCoreVersion(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	disconnect := rejectedConnect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{2, 3},
	})
	assertTerminalDisconnect(t, disconnect, 4501, "unsupported wire version")
	if len(auth.calls()) != 1 {
		t.Fatalf("authentication calls = %d, want 1 at connect time", len(auth.calls()))
	}
}

func TestServiceLinkIgnoresBrowserOriginAndDisablesCompression(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	dialer := websocket.Dialer{EnableCompression: true, Subprotocols: []string{"centrifuge-json"}}
	header := http.Header{"Origin": {"https://hostile.browser.example"}}
	connection, response, err := dialer.Dial(wsURL(httpServer.URL), header)
	if err != nil {
		t.Fatalf("dial with unrelated Origin: %v", err)
	}
	if response.Header.Get("Sec-WebSocket-Extensions") != "" {
		connection.Close()
		t.Fatalf("compression was negotiated: %q", response.Header.Get("Sec-WebSocket-Extensions"))
	}
	if extension := connection.Subprotocol(); extension != "centrifuge-json" {
		connection.Close()
		t.Fatalf("subprotocol = %q, want centrifuge-json", extension)
	}
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	if reply.Connect == nil {
		connection.Close()
		t.Fatalf("unrelated Origin was rejected: %#v", reply)
	}
	connection.Close()
}

func TestHostLinkAcceptsExplicitJSONSelectors(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	for _, test := range []struct {
		name            string
		suffix          string
		protocols       []string
		wantSubprotocol string
	}{
		{name: "header", protocols: []string{"centrifuge-json"}, wantSubprotocol: "centrifuge-json"},
		{name: "format query", suffix: "?format=json"},
		{name: "protocol query", suffix: "?cf_protocol=json"},
		{name: "repeated JSON format query", suffix: "?format=json&format=json"},
		{name: "repeated JSON protocol query", suffix: "?cf_protocol=json&cf_protocol=json"},
		{name: "both JSON queries", suffix: "?format=json&cf_protocol=json"},
		// The selector reads only "format" and "cf_protocol", so an unrecognized
		// query key must not turn a valid selection into a rejection. This row is
		// named for that property: cf_protocol_version appears nowhere in
		// centrifuge@v0.38.0's production code, so no value of it can be read.
		{name: "unrecognized query key alongside JSON", suffix: "?format=json&cf_protocol_version=v1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dialer := websocket.Dialer{Subprotocols: test.protocols}
			connection, response, err := dialer.Dial(wsURL(httpServer.URL)+test.suffix, nil)
			if err != nil {
				t.Fatalf("explicit JSON dial: %v (response %#v)", err, response)
			}
			defer connection.Close()
			if got := connection.Subprotocol(); got != test.wantSubprotocol {
				t.Fatalf("negotiated subprotocol = %q, want %q", got, test.wantSubprotocol)
			}
		})
	}
}

func TestHostLinkRejectsInvalidProtocolSelectors(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	type rejection struct {
		name      string
		suffix    string
		protocols []string
		header    http.Header
	}
	tests := []rejection{
		{name: "absent"},
		{name: "unknown header", protocols: []string{"something-else"}},
		{name: "protobuf header", protocols: []string{"centrifuge-protobuf"}},
		{name: "JSON plus unknown header tokens", protocols: []string{"centrifuge-json", "something-else"}},
		{name: "unknown plus JSON header tokens", protocols: []string{"something-else", "centrifuge-json"}},
		{name: "JSON plus protobuf header tokens", protocols: []string{"centrifuge-json", "centrifuge-protobuf"}},
		{name: "protobuf plus JSON header tokens", protocols: []string{"centrifuge-protobuf", "centrifuge-json"}},
		{name: "empty header token", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json,"}}},
		{name: "multiple physical header fields", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json", "centrifuge-json"}}},
		{name: "unknown in second physical header field", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json", "something-else"}}},
		{name: "unknown in first physical header field", header: http.Header{"Sec-WebSocket-Protocol": {"something-else", "centrifuge-json"}}},
		{name: "protobuf in second physical header field", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json", "centrifuge-protobuf"}}},
		{name: "protobuf in first physical header field", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-protobuf", "centrifuge-json"}}},
		// A query selector alone sets selected, so these are the only rows that isolate
		// the multi-physical-field guard: drop it and len(headerValues) == 1 is false,
		// the header is never inspected, and the query admits the upgrade on its own.
		// What then gets negotiated depends on field order — gorilla reads only the
		// first Sec-WebSocket-Protocol field — so the protobuf-first rows would speak
		// centrifuge-protobuf and the other two centrifuge-json. The duplicate-JSON row
		// is the one that shows a repeated physical field is refused even when every
		// token is valid, which no protobuf-carrying row can show.
		{name: "protobuf-first multiple physical header fields with valid format query", suffix: "?format=json", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-protobuf", "centrifuge-json"}}},
		{name: "JSON-first multiple physical header fields with valid format query", suffix: "?format=json", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json", "centrifuge-protobuf"}}},
		{name: "protobuf-first multiple physical header fields with valid protocol query", suffix: "?cf_protocol=json", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-protobuf", "centrifuge-json"}}},
		{name: "duplicate JSON physical header fields with valid protocol query", suffix: "?cf_protocol=json", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json", "centrifuge-json"}}},
		{name: "header casing variant", protocols: []string{"Centrifuge-JSON"}},
		{name: "valid format with invalid protocol", suffix: "?format=json&cf_protocol=protobuf"},
		{name: "invalid format with valid protocol", suffix: "?format=protobuf&cf_protocol=json"},
	}

	// The production selector loops over {"format", "cf_protocol"} and applies the
	// same rule to each, so the two key paths are generated here from one variant
	// list rather than written out twice. That makes their identity structural:
	// the halves cannot drift, and a variant added below is added to both keys.
	// Every row also carries a valid centrifuge-json header, so the rejection can
	// only come from the query value.
	for _, key := range []string{"format", "cf_protocol"} {
		for _, variant := range []struct {
			name   string
			values []string
		}{
			{name: "protobuf", values: []string{"protobuf"}},
			{name: "unknown", values: []string{"something-else"}},
			{name: "empty", values: []string{""}},
			{name: "casing variant", values: []string{"JSON"}},
			{name: "repeated JSON then protobuf", values: []string{"json", "protobuf"}},
			{name: "repeated protobuf then JSON", values: []string{"protobuf", "json"}},
			{name: "repeated JSON then empty", values: []string{"json", ""}},
		} {
			query := url.Values{}
			for _, value := range variant.values {
				query.Add(key, value)
			}
			tests = append(tests, rejection{
				name:      key + " " + variant.name,
				suffix:    "?" + query.Encode(),
				protocols: []string{"centrifuge-json"},
			})
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dialer := websocket.Dialer{Subprotocols: test.protocols}
			connection, response, err := dialer.Dial(wsURL(httpServer.URL)+test.suffix, test.header)
			assertUpgradeRejected(t, connection, response, err)
		})
	}
}

func assertUpgradeRejected(t *testing.T, connection *websocket.Conn, response *http.Response, err error) {
	t.Helper()
	if connection != nil {
		connection.Close()
	}
	if err == nil {
		t.Fatal("non-JSON HostLink unexpectedly upgraded")
	}
	if response == nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("non-JSON response = %#v, want HTTP 400", response)
	}
	// Status alone does not identify the rejecting layer: gorilla's own handshake
	// validation also answers 400. On ErrBadHandshake the dialer preserves up to
	// 1KB of the body (websocket@v1.5.3 client.go:398-403), so read it and pin the
	// rejection to HostLink's own selector message.
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read rejection body: %v", err)
	}
	response.Body.Close()
	if !strings.Contains(string(body), "HostLink requires the JSON protocol") {
		t.Fatalf("rejection body = %q, want HostLink's protocol-selector message", body)
	}
}

func TestHeartbeatPingKeepsAResponsiveLinkOpen(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{PingInterval: time.Second, PongTimeout: 500 * time.Millisecond})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	if reply.Connect == nil || reply.Connect.Ping != 1 || !reply.Connect.Pong {
		t.Fatalf("connect heartbeat = %#v, want ping=1 and pong=true", reply.Connect)
	}
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, ping, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read server ping: %v", err)
	}
	if string(ping) != "{}" {
		t.Fatalf("server ping = %q, want {}", ping)
	}
	if err := connection.WriteMessage(websocket.TextMessage, []byte("{}")); err != nil {
		t.Fatalf("write pong: %v", err)
	}
	connection.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	if _, _, err := connection.ReadMessage(); err != nil {
		t.Fatalf("link did not remain open for the next heartbeat: %v", err)
	}
}

// This is a characterization test of the deliberately minimal Centrifuge node
// configuration: without application handlers these optional client-protocol
// facilities fail closed. It was added after the adapter existed, so its value
// is regression coverage rather than RED evidence for the implementation.
func TestOptionalRealtimeFacilitiesAreUnavailable(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	commands := []string{
		`{"id":2,"publish":{"channel":"hostlink:test","data":{"value":1}}}`,
		`{"id":3,"history":{"channel":"hostlink:test"}}`,
		`{"id":4,"presence":{"channel":"hostlink:test"}}`,
		`{"id":5,"subscribe":{"channel":"hostlink:test","recover":true,"offset":1,"epoch":"old"}}`,
	}
	for _, command := range commands {
		if err := connection.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
			t.Fatalf("write optional command %s: %v", command, err)
		}
		connection.SetReadDeadline(time.Now().Add(time.Second))
		_, payload, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read optional command response for %s: %v", command, err)
		}
		var reply clientReply
		if err := json.Unmarshal(payload, &reply); err != nil {
			t.Fatalf("decode optional command response %q: %v", payload, err)
		}
		if reply.Error == nil {
			t.Fatalf("optional command succeeded: command=%s response=%s", command, payload)
		}
	}
}

func TestReconnectAuthenticatesAndNegotiatesAgain(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	for attempt := 0; attempt < 2; attempt++ {
		connection := dial(t, httpServer.URL, "")
		reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
		if reply.Connect == nil {
			connection.Close()
			t.Fatalf("attempt %d connect reply = %#v", attempt+1, reply)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("attempt %d close: %v", attempt+1, err)
		}
	}
	if got := len(auth.calls()); got != 2 {
		t.Fatalf("authentication calls after reconnect = %d, want 2", got)
	}
}

func TestGracefulCloseStopsConnectionsAndIsIdempotent(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer httpServer.Close()

	connection := dial(t, httpServer.URL, "")
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		connection.Close()
		t.Fatalf("connect reply = %#v", reply)
	}
	readDone := make(chan error, 1)
	go func() {
		_, _, err := connection.ReadMessage()
		readDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("connection remained open after graceful close")
		}
	case <-time.After(time.Second):
		t.Fatal("connection remained open after graceful close")
	}
	if err := server.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// startServer builds a server from heartbeat, whose TenantID and Authenticator
// are overwritten by testTenant and auth; only the heartbeat fields are read
// from it.
func startServer(t *testing.T, auth hostlink.Authenticator, heartbeat hostlink.Config) (hostlink.Server, *httptest.Server) {
	t.Helper()
	heartbeat.TenantID = testTenant
	heartbeat.Authenticator = auth
	server, err := hostlink.NewCentrifugeServer(heartbeat)
	if err != nil {
		t.Fatalf("NewCentrifugeServer: %v", err)
	}
	return server, httptest.NewServer(server.Handler())
}

func closeServers(t *testing.T, server hostlink.Server, httpServer *httptest.Server) {
	t.Helper()
	httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func dial(t *testing.T, serverURL, origin string) *websocket.Conn {
	t.Helper()
	header := http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}}
	if origin != "" {
		header.Set("Origin", origin)
	}
	connection, response, err := websocket.DefaultDialer.Dial(wsURL(serverURL), header)
	if err != nil {
		t.Fatalf("dial websocket (response %#v): %v", response, err)
	}
	return connection
}

func connect(t *testing.T, connection *websocket.Conn, token string, request sessionwire.VersionNegotiationRequest) clientReply {
	t.Helper()
	writeConnect(t, connection, token, request)
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read connect reply: %v", err)
	}
	var reply clientReply
	if err := json.Unmarshal(payload, &reply); err != nil {
		t.Fatalf("decode connect reply %q: %v", payload, err)
	}
	return reply
}

func rejectedConnect(t *testing.T, connection *websocket.Conn, token string, request sessionwire.VersionNegotiationRequest) *websocket.CloseError {
	t.Helper()
	writeConnect(t, connection, token, request)
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := connection.ReadMessage()
	if err == nil {
		t.Fatal("rejected connect returned a message instead of closing")
	}
	var closeError *websocket.CloseError
	if !errors.As(err, &closeError) {
		t.Fatalf("rejected connect error = %T %v, want websocket close", err, err)
	}
	return closeError
}

func writeConnect(t *testing.T, connection *websocket.Conn, token string, request sessionwire.VersionNegotiationRequest) {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal negotiation request: %v", err)
	}
	command := struct {
		ID      uint32 `json:"id"`
		Connect struct {
			Token string          `json:"token"`
			Data  json.RawMessage `json:"data"`
			Name  string          `json:"name"`
		} `json:"connect"`
	}{ID: 1}
	command.Connect.Token = token
	command.Connect.Data = data
	command.Connect.Name = "looprig-factory"
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal connect command: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write connect command: %v", err)
	}
}

// assertTerminalDisconnect pins both halves of a HostLink rejection: the exact
// disconnect code, which is what tells the two failures apart, and separately
// its membership of Centrifuge's 4500-4999 application terminal range, which is
// the dependency contract that makes the code terminal at all. The codes are
// unexported and this test is external, so they are written out here.
func assertTerminalDisconnect(t *testing.T, disconnect *websocket.CloseError, code int, reason string) {
	t.Helper()
	if disconnect.Code != code {
		t.Fatalf("disconnect code = %d, want %d", disconnect.Code, code)
	}
	if disconnect.Code < 4500 || disconnect.Code > 4999 {
		t.Fatalf("disconnect code = %d, want Centrifuge's terminal application range", disconnect.Code)
	}
	if !strings.Contains(disconnect.Text, reason) {
		t.Fatalf("disconnect reason = %q, want to contain %q", disconnect.Text, reason)
	}
}

func wsURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + "/connection/websocket"
}
