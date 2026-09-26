package hostlink_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

// A bound, subscribed Centrifuge client must receive the admitted preview on
// the same ordered channel as enduring publications.
func TestTransientPublicationReachesSubscribedClientInOrder(t *testing.T) {
	f := newFixture(t)
	server, httpServer := startServer(t, &recordingAuthenticator{wantToken: testCredential}, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)
	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, bindRequest(testSession)))
	channel := hostlink.ChannelFor(residencyKey(testSession))
	if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
		t.Fatalf("subscribe: %#v", *reply.Error)
	}
	admitter, ok := server.(interface{ TryPublishEphemeral(string, []byte) bool })
	if !ok {
		t.Fatal("HostLink has no transient admission seam")
	}
	frames := [][]byte{[]byte(`{"event_id":"before"}`), []byte(`{"type":"ephemeral_publication"}`), []byte(`{"event_id":"after"}`)}
	if err := server.Publish(channel, frames[0]); err != nil {
		t.Fatal(err)
	}
	if !admitter.TryPublishEphemeral(channel, frames[1]) {
		t.Fatal("first transient frame was refused despite an empty bucket")
	}
	if err := server.Publish(channel, frames[2]); err != nil {
		t.Fatal(err)
	}
	var received []push
	for len(received) < len(frames) {
		connection.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, raw, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read frame %d: %v", len(received), err)
		}
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
			var got push
			if err := json.Unmarshal(line, &got); err != nil || got.Push == nil || got.Push.Pub == nil {
				t.Fatalf("frame %d = %s, decode error %v", len(received), line, err)
			}
			received = append(received, got)
		}
	}
	if len(received) != len(frames) {
		t.Fatalf("received %d frames, want %d", len(received), len(frames))
	}
	for i, want := range frames {
		got := received[i]
		if got.Push.Channel != channel || string(got.Push.Pub.Data) != string(want) {
			t.Fatalf("frame %d = %s on %q, want %s on %q", i, got.Push.Pub.Data, got.Push.Channel, want, channel)
		}
	}
}

func TestConcreteTransientAdmissionChargesEncodedQueueBytes(t *testing.T) {
	f := newFixture(t)
	server, httpServer := startServer(t, &recordingAuthenticator{wantToken: testCredential}, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)
	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, bindRequest(testSession)))
	channel := hostlink.ChannelFor(residencyKey(testSession))
	if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
		t.Fatalf("subscribe: %#v", *reply.Error)
	}
	admitter := server.(interface{ TryPublishEphemeral(string, []byte) bool })
	payload := []byte(`"` + strings.Repeat("x", 4094) + `"`)
	if !admitter.TryPublishEphemeral(channel, payload) {
		t.Fatal("first 4 KiB payload was refused")
	}
	if admitter.TryPublishEphemeral(channel, payload) {
		t.Fatal("second 4 KiB payload was admitted; encoded queue bytes exceed the 8 KiB burst")
	}
}

// A stalled replica may be disconnected when its own Centrifuge queue fills;
// another replica must continue to receive every enduring frame in order.
func TestSlowSubscriberDoesNotLoseOrReorderEnduringForHealthySubscriber(t *testing.T) {
	f := newFixture(t)
	server, httpServer := startServer(t, &recordingAuthenticator{wantToken: testCredential}, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)
	channel := hostlink.ChannelFor(residencyKey(testSession))
	openSubscriber := func() *websocket.Conn {
		connection := dial(t, httpServer.URL, "")
		if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
			t.Fatalf("connect reply = %#v", reply)
		}
		acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, bindRequest(testSession)))
		if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
			t.Fatalf("subscribe: %#v", *reply.Error)
		}
		return connection
	}
	slow := openSubscriber()
	defer slow.Close()
	healthy := openSubscriber()
	defer healthy.Close()
	const count = 320 // More than a 1 MiB queue at 4 KiB per publication.
	for i := 0; i < count; i++ {
		prefix := fmt.Sprintf(`{"seq":%d,"padding":"`, i)
		payload := []byte(prefix + strings.Repeat("x", 4096-len(prefix)-2) + `"}`)
		if err := server.Publish(channel, payload); err != nil {
			t.Fatalf("Publish enduring %d: %v", i, err)
		}
		healthy.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, raw, err := healthy.ReadMessage()
		if err != nil {
			t.Fatalf("healthy read %d: %v", i, err)
		}
		var received struct {
			Push struct {
				Pub struct {
					Data struct {
						Seq int `json:"seq"`
					} `json:"data"`
				} `json:"pub"`
			} `json:"push"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(raw), &received); err != nil || received.Push.Pub.Data.Seq != i {
			t.Fatalf("healthy frame %d = %s, decode error %v", i, raw, err)
		}
	}
	// The stalled link either catches up in the same order or is disconnected
	// with an ordered prefix. Its queue never silently skips an enduring frame.
	seen := 0
	for seen < count {
		slow.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, raw, err := slow.ReadMessage()
		if err != nil {
			var timeout net.Error
			if seen == 0 || errors.As(err, &timeout) && timeout.Timeout() {
				t.Fatalf("slow subscriber received %d enduring frames before error: %v", seen, err)
			}
			var closeError *websocket.CloseError
			if !errors.As(err, &closeError) && !errors.Is(err, io.EOF) {
				t.Fatalf("slow subscriber error after %d frames: %v", seen, err)
			}
			break
		}
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
			var frame struct {
				Push struct {
					Pub struct {
						Data struct {
							Seq int `json:"seq"`
						} `json:"data"`
					} `json:"pub"`
				} `json:"push"`
			}
			if err := json.Unmarshal(line, &frame); err != nil || frame.Push.Pub.Data.Seq != seen {
				t.Fatalf("slow frame %d = %s, decode error %v", seen, line, err)
			}
			seen++
		}
	}
}
