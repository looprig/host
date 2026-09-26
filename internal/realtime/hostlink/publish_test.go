package hostlink_test

import (
	"encoding/json"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
)

// push is the asynchronous frame a publication arrives in. It is a separate
// shape from the command reply because a push carries no id: a reader that
// decoded it as a reply would match every id it was waiting for.
type push struct {
	Push *struct {
		Channel string `json:"channel"`
		Pub     *struct {
			Data json.RawMessage `json:"data"`
		} `json:"pub"`
	} `json:"push,omitempty"`
}

// TestAPublicationReachesTheLinkThatBoundTheSession is the production reader
// service.Publications had none of.
//
// The seam takes a channel and a payload and nothing else, and until this task
// nothing in the module could satisfy it: Server exposed a handler and a close
// and no way to put bytes on a channel, so the live event relay had a derivation
// and no transport. What this asserts is the whole of that seam's contract —
// the exact bytes handed in arrive on the named channel at a link that bound
// the session — rather than that a call returned nil.
func TestAPublicationReachesTheLinkThatBoundTheSession(t *testing.T) {
	f := newFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, bindRequest(testSession)))
	channel := hostlink.ChannelFor(residencyKey(testSession))
	if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
		t.Fatalf("subscribe to a bound channel was refused: %#v", *reply.Error)
	}

	payload := []byte(`{"event_id":"event-one","body":{"kind":"message"}}`)
	if err := server.Publish(channel, payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, frame, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read publication: %v", err)
	}
	var received push
	if err := json.Unmarshal(frame, &received); err != nil {
		t.Fatalf("decode publication %q: %v", frame, err)
	}
	if received.Push == nil || received.Push.Pub == nil {
		t.Fatalf("frame %q is not a publication push", frame)
	}
	if received.Push.Channel != channel {
		t.Errorf("publication arrived on channel %q, want %q", received.Push.Channel, channel)
	}
	if string(received.Push.Pub.Data) != string(payload) {
		t.Errorf("publication body = %s, want %s", received.Push.Pub.Data, payload)
	}
}

// TestAPublicationToAnUnsubscribedChannelIsNotAnError records the half of the
// contract a caller must design around: Publish reports that the transport
// accepted the bytes, not that anybody received them.
//
// It matters because the event relay's own failure handling branches on this
// return. A transport that refused an unsubscribed channel would make an
// ordinary state — a session whose Factory link has dropped and not yet
// reconnected — indistinguishable from a broken relay, and the relay would
// invalidate a route that was merely idle.
func TestAPublicationToAnUnsubscribedChannelIsNotAnError(t *testing.T) {
	f := newFixture(t)
	server, httpServer := startServer(t, &recordingAuthenticator{wantToken: testCredential}, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	if err := server.Publish(hostlink.ChannelFor(residencyKey(testSession)), []byte(`{"event_id":"event-one"}`)); err != nil {
		t.Fatalf("Publish to a channel nobody subscribes = %v, want nil", err)
	}
}

// The pinned Centrifuge writer does not expose per-connection queue headroom.
// Until it does, transient publication must be refused before enqueue.
func TestTransientPublishIsDisabledWithoutQueueHeadroom(t *testing.T) {
	f := newFixture(t)
	server, httpServer := startServer(t, &recordingAuthenticator{wantToken: testCredential}, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)
	admitter, ok := server.(interface{ TryPublishEphemeral(string, []byte) bool })
	if !ok {
		t.Fatal("HostLink has no transient admission seam")
	}
	if admitter.TryPublishEphemeral(hostlink.ChannelFor(residencyKey(testSession)), []byte(`{"type":"ephemeral_publication"}`)) {
		t.Fatal("Centrifuge admitted a transient frame without trustworthy queue headroom")
	}
}
