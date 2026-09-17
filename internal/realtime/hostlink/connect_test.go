package hostlink_test

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
)

// TestTheConnectReplyBytesArePinned holds the connect reply's Data to LITERAL
// BYTES over a real link.
//
// The literal is the contract Factory decodes, so it is pinned as bytes and
// not as a decoded struct: a struct comparison would pass a reply whose
// members were reordered, wrapped, or re-encoded, and the two modules have
// already disagreed once over exactly this frame (B8: Factory wrapped it as
// {"version_negotiation":...}; Core v0.9.0 froze the BARE shape as the
// contract). Before this Host took Core's connect framing the reply was
// `{"version":1}`, recorded in CODEX_RESULT_HOST_V020.md; with the
// capability advertisement it is the bytes below, in the order the Host
// advertises them.
func TestTheConnectReplyBytesArePinned(t *testing.T) {
	f := newFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	const want = `{"hostlink_methods":["hostlink.bind","hostlink.unbind","hostlink.attach","hostlink.drain","hostlink.drain_status"],"version":1}`
	if got := string(reply.Connect.Data); got != want {
		t.Fatalf("connect reply data = %s, want %s", got, want)
	}
}

func TestConnectWithoutMultiplexerAdvertisesNoMethods(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect reply = %#v, want successful connect", reply)
	}
	const want = `{"version":1}`
	if got := string(reply.Connect.Data); got != want {
		t.Fatalf("connect reply data = %s, want zero-capability reply %s", got, want)
	}
	negotiated, err := sessionwire.DecodeHostLinkConnectReply(reply.Connect.Data)
	if err != nil {
		t.Fatalf("Core's own decoder refuses the zero-capability reply %s: %v", reply.Connect.Data, err)
	}
	if methods := negotiated.HostLinkMethods(); len(methods) != 0 {
		t.Fatalf("connect reply advertises %v without a Multiplexer", methods)
	}
}

// TestTheAdvertisedMethodsAreExactlyTheOnesDispatchRoutes holds the connect
// reply's hostlink_methods to the dispatch switch, in BOTH directions, and then
// proves each advertised name is really a reserved handler over the wire.
//
// THE DISPATCH SET IS DERIVED FROM SOURCE, not from a second list: the `case`
// expressions of Multiplexer.dispatch in bindings.go are read by AST and each
// identifier is resolved through the package's exported constants — an
// identifier this test cannot resolve is a failure, so a new case cannot slip
// past as "unknown". A method advertised and not routed (a phantom), or routed
// and not advertised, fails the set comparison.
//
// THE BEHAVIOURAL ARM is what makes the AST arm about the running Host rather
// than about a file: an RPC with a malformed body on a RESERVED method is
// refused at the transport (bad request, 107, from that method's own decoder),
// while the same RPC on a name the switch does not know falls to the channel
// arm and is answered with a HostLinkError (runtime_unavailable, not bound).
// Every advertised name must take the first path; the phantom control takes
// the second.
func TestTheAdvertisedMethodsAreExactlyTheOnesDispatchRoutes(t *testing.T) {
	f := newFixture(t, withAttacher(&recordingAttacher{answer: acceptedObservation(testSession)}))
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	negotiated, err := sessionwire.DecodeHostLinkConnectReply(reply.Connect.Data)
	if err != nil {
		t.Fatalf("Core's own decoder refuses this Host's connect reply %s: %v", reply.Connect.Data, err)
	}
	advertised := negotiated.HostLinkMethods()
	if len(advertised) == 0 {
		t.Fatal("the connect reply advertises no methods")
	}
	for _, method := range []string{sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodBind} {
		if !negotiated.Supports(method) {
			t.Fatalf("Supports(%q) = false; a Factory would refuse to place on this Host", method)
		}
	}

	routed := dispatchCases(t)
	sort.Strings(advertised)
	sort.Strings(routed)
	if !slices.Equal(advertised, routed) {
		t.Fatalf("advertised %v but dispatch routes %v; the two must be the same set", advertised, routed)
	}

	// BEHAVIOURAL: each advertised method is a reserved handler.
	id := uint32(10)
	for _, method := range advertised {
		id++
		answer := rpc(t, connection, id, method, map[string]any{"version": 1})
		if answer.Error == nil || answer.Error.Code != 107 {
			t.Fatalf("%s with a malformed body answered %#v, want transport code 107 from its own decoder; is it really routed?", method, answer)
		}
	}
	// THE CONTROL: a name the switch does not know is answered from the channel
	// arm with a HostLinkError, so the 107s above are a property of the routed
	// methods and not of every RPC.
	phantom := rpc(t, connection, id+1, "hostlink.phantom", map[string]any{"version": 1})
	refusal, published := refusalOfReply(phantom)
	if !published || refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("an unknown method answered %#v, want runtime_unavailable from the channel arm", phantom)
	}
}

// dispatchCases reads the `case` identifiers of Multiplexer.dispatch from
// bindings.go and resolves each to its wire value through the package's
// exported constants.
func dispatchCases(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "bindings.go", nil, 0)
	if err != nil {
		t.Fatalf("parse bindings.go: %v", err)
	}
	values := map[string]string{
		"MethodBind":        hostlink.MethodBind,
		"MethodUnbind":      hostlink.MethodUnbind,
		"MethodAttach":      hostlink.MethodAttach,
		"MethodDrain":       hostlink.MethodDrain,
		"MethodDrainStatus": hostlink.MethodDrainStatus,
	}
	var routed []string
	var found bool
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "dispatch" || function.Recv == nil {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expression := range clause.List {
				identifier, ok := expression.(*ast.Ident)
				if !ok {
					t.Fatalf("dispatch has a case that is not a bare identifier: %T; this guard cannot resolve it", expression)
				}
				value, known := values[identifier.Name]
				if !known {
					t.Fatalf("dispatch routes %s, which this guard cannot resolve to a wire value; add it to the table AND decide whether it is advertised", identifier.Name)
				}
				routed = append(routed, value)
			}
			return true
		})
	}
	if !found {
		t.Fatal("bindings.go declares no dispatch method; this guard read nothing")
	}
	if len(routed) == 0 {
		t.Fatal("dispatch has no case clauses; this guard read nothing")
	}
	return routed
}

func refusalOfReply(reply rpcReply) (sessionwire.HostLinkError, bool) {
	if reply.Error != nil || reply.RPC == nil || len(reply.RPC.Data) == 0 {
		return sessionwire.HostLinkError{}, false
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply.RPC.Data, &refusal); err != nil {
		return sessionwire.HostLinkError{}, false
	}
	return refusal, true
}

// TestAWrappedConnectRequestIsRefused is B8 from Host's side: a connect whose
// Data is {"version_negotiation":{...}} — the shape Factory v0.1.1 sent — is
// refused with the unsupported-version disconnect, because Core's strict
// decoder refuses the unknown member. Core v0.9.0 froze the bare shape as the
// contract; Factory is the side that changes.
func TestAWrappedConnectRequestIsRefused(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	wrapped := json.RawMessage(`{"version_negotiation":{"supported_versions":[1]}}`)
	disconnect := rejectedConnectRaw(t, connection, testCredential, wrapped)
	assertTerminalDisconnect(t, disconnect, 4501, "unsupported wire version")

	// THE CONTROL: the same offer, bare, connects.
	second := dial(t, httpServer.URL, "")
	defer second.Close()
	if reply := connect(t, second, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("the bare offer was refused too: %#v", reply)
	}
}

// rejectedConnectRaw writes a connect whose Data is exactly raw and reads the
// terminal disconnect.
func rejectedConnectRaw(t *testing.T, connection *websocket.Conn, token string, raw json.RawMessage) *websocket.CloseError {
	t.Helper()
	command := map[string]any{"id": 1, "connect": map[string]any{"token": token, "data": raw, "name": "looprig-factory"}}
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal connect command: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write connect command: %v", err)
	}
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = connection.ReadMessage()
	if err == nil {
		t.Fatal("wrapped connect returned a message instead of closing")
	}
	var closeError *websocket.CloseError
	if !errors.As(err, &closeError) {
		t.Fatalf("wrapped connect error = %T %v, want websocket close", err, err)
	}
	return closeError
}
