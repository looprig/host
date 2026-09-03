package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

type connectReply struct {
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
	Push *struct {
		Disconnect *struct {
			Code      uint32 `json:"code"`
			Reason    string `json:"reason"`
			Reconnect bool   `json:"reconnect"`
		} `json:"disconnect,omitempty"`
	} `json:"push,omitempty"`
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
	if got, want := auth.calls(), []string{string(testTenant) + ":" + testCredential}; !reflectStringsEqual(got, want) {
		t.Errorf("authentication calls = %v, want %v", got, want)
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
	assertTerminalDisconnect(t, disconnect, "authentication")
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
	assertTerminalDisconnect(t, disconnect, "unsupported wire version")
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

func TestHostLinkRejectsEveryProtobufSelection(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	for _, test := range []struct {
		name   string
		suffix string
		header http.Header
	}{
		{name: "format query", suffix: "?format=protobuf"},
		{name: "protocol query", suffix: "?cf_protocol=protobuf"},
		{name: "subprotocol header", header: http.Header{"Sec-WebSocket-Protocol": {"centrifuge-protobuf"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, response, err := websocket.DefaultDialer.Dial(wsURL(httpServer.URL)+test.suffix, test.header)
			if err == nil {
				t.Fatal("protobuf HostLink unexpectedly upgraded")
			}
			if response == nil || response.StatusCode != http.StatusBadRequest {
				t.Fatalf("protobuf response = %#v, want HTTP 400", response)
			}
		})
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
		var reply connectReply
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

func startServer(t *testing.T, auth hostlink.Authenticator, overrides hostlink.Config) (hostlink.Server, *httptest.Server) {
	t.Helper()
	overrides.TenantID = testTenant
	overrides.Authenticator = auth
	server, err := hostlink.NewCentrifugeServer(overrides)
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

func connect(t *testing.T, connection *websocket.Conn, token string, request sessionwire.VersionNegotiationRequest) connectReply {
	t.Helper()
	writeConnect(t, connection, token, request)
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read connect reply: %v", err)
	}
	var reply connectReply
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

func assertTerminalDisconnect(t *testing.T, disconnect *websocket.CloseError, reason string) {
	t.Helper()
	if disconnect.Code < 4500 || disconnect.Code > 4999 {
		t.Fatalf("disconnect code = %d, want terminal application range", disconnect.Code)
	}
	if !strings.Contains(disconnect.Text, reason) {
		t.Fatalf("disconnect reason = %q, want to contain %q", disconnect.Text, reason)
	}
}

func wsURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + "/connection/websocket"
}

func reflectStringsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
