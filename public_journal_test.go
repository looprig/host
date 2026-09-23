package host_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host"
)

// linkPublications reads every enduring publication pushed on one channel
// until done reports true for the ones read so far.
func linkPublications(t *testing.T, connection *websocket.Conn, channel string, done func([]sessionwire.EnduringPublication) bool) []sessionwire.EnduringPublication {
	t.Helper()
	linkMu.Lock()
	defer linkMu.Unlock()
	var got []sessionwire.EnduringPublication
	deadline := time.Now().Add(20 * time.Second)
	for !done(got) {
		if err := connection.SetReadDeadline(deadline); err != nil {
			t.Fatalf("deadline: %v", err)
		}
		_, frame, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read publications (have %d): %v", len(got), err)
		}
		for _, line := range strings.Split(string(frame), "\n") {
			var envelope struct {
				Push *struct {
					Channel string `json:"channel"`
					Pub     *struct {
						Data json.RawMessage `json:"data"`
					} `json:"pub"`
				} `json:"push,omitempty"`
			}
			if json.Unmarshal([]byte(strings.TrimSpace(line)), &envelope) != nil || envelope.Push == nil || envelope.Push.Pub == nil || envelope.Push.Channel != channel {
				continue
			}
			var publication sessionwire.EnduringPublication
			if err := json.Unmarshal(envelope.Push.Pub.Data, &publication); err != nil {
				t.Fatalf("decode publication %s: %v", envelope.Push.Pub.Data, err)
			}
			got = append(got, publication)
		}
	}
	return got
}

// readWholeJournal pages a journal reader from the head.
func readWholeJournal(t *testing.T, reader interface {
	ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
}, session sessionwire.SessionID) []sessionwire.JournalEvent {
	t.Helper()
	var events []sessionwire.JournalEvent
	var cursor sessionwire.Cursor
	for {
		page, err := reader.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{TenantID: composeTenant, SessionID: session, Cursor: cursor, Limit: 3})
		if err != nil {
			t.Fatalf("ReadPublicJournal: %v", err)
		}
		events = append(events, page.Events...)
		if page.NextCursor == "" {
			return events
		}
		cursor = page.NextCursor
	}
}

type bodyHeader struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Cause     *struct {
		CommandID *string `json:"command_id"`
	} `json:"cause"`
}

func headerOf(t *testing.T, body json.RawMessage) bodyHeader {
	t.Helper()
	var header bodyHeader
	if err := json.Unmarshal(body, &header); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	return header
}

// TestNoRuntimeIdentityReachesAClientLiveOrDurably is finding W1 end to end,
// over a real harness runtime, the real HostLink, and the runtime journal
// Factory's JournalResolver reads.
//
// Harness stamps every public body with the RUNTIME session id and every
// command-caused body with the RUNTIME command id. Neither may reach a client:
// the live tail Host publishes and the durable read a composition hands
// Factory through host.PublicJournal must both carry the PUBLIC session id and
// the PUBLIC command id the client admitted instead — and must agree byte for
// byte on every event, since a consumer joins the two.
func TestNoRuntimeIdentityReachesAClientLiveOrDurably(t *testing.T) {
	world := newRealRuntimeWorld(t)
	service, _ := world.hostWith(t, 4, func(blueprint *host.Composition) {
		blueprint.Options.ReconcileInterval = 50 * time.Millisecond
	})
	t.Cleanup(func() { stopBounded(service) })

	server := httptest.NewServer(service.Handler())
	defer server.Close()
	link := dialHostLink(t, server.URL, composeTenant)
	defer link.Close()
	attach := rpcOver(t, link, 2, sessionwire.HostLinkMethodAttach, sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: composeTenant, SessionID: composeSession,
		HostID: "host-a", HostGeneration: 4, AgentID: composeAgent, RuntimeCompatibilityID: string(composeCompat),
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory", IdempotencyKey: "attach-w1",
	})
	if attach.Error != nil {
		t.Fatalf("attach: %#v", *attach.Error)
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(attach.RPC.Data, &observation); err != nil {
		t.Fatalf("decode the observation: %v", err)
	}
	assertAcceptedRPC(t, rpcOver(t, link, 3, sessionwire.HostLinkMethodBind, sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: composeTenant, SessionID: composeSession,
		HostID: "host-a", HostGeneration: 4, LeaseEpoch: observation.LeaseEpoch,
		RuntimeCompatibilityID: string(composeCompat), IdempotencyKey: "bind-w1",
	}), "bind")
	channel := sessionwire.HostLinkChannel(composeTenant, composeSession)
	if reply := sendOver(t, link, map[string]any{"id": 4, "subscribe": map[string]any{"channel": channel}}, 4); reply.Error != nil {
		t.Fatalf("subscribe: %#v", *reply.Error)
	}

	command := world.admitInput(t, "hello from the client")
	live := linkPublications(t, link, channel, func(got []sessionwire.EnduringPublication) bool {
		for _, publication := range got {
			if strings.Contains(string(publication.Body), `"type":"TurnDone"`) {
				return true
			}
		}
		return false
	})
	world.awaitApplied(t, command, "hello from the client")
	entry, err := world.factory.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{TenantID: composeTenant, SessionID: composeSession, CommandID: command})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	runtimeCommand := string(entry.Record.Descriptor.RuntimeCommandID)
	private := []string{world.runtimeID.String(), runtimeCommand}

	runtimeStore, err := sessionstore.Open(t.Context(), world.fixture.journalBackend, sessionstore.WithLegacySingleTenant(composeTenant))
	if err != nil {
		t.Fatalf("open the runtime journal as a resolver does: %v", err)
	}
	// THE CONTROL: the journal itself holds both runtime identities, so the
	// assertions below are about the projection and not about a quiet runtime.
	raw := readWholeJournal(t, runtimeStore, sessionwire.SessionID(world.binding.RuntimeSessionID))
	var rawText bytes.Buffer
	for _, event := range raw {
		rawText.Write(event.Body)
	}
	for _, id := range private {
		if !strings.Contains(rawText.String(), id) {
			t.Fatalf("the raw runtime journal never names %s; the fixture does not exercise W1", id)
		}
	}

	journals := host.NewPublicJournals(0)
	public, err := journals.Reader(runtimeStore, composeTenant, composeSession, world.binding)
	if err != nil {
		t.Fatalf("PublicJournals.Reader: %v", err)
	}
	durable := readWholeJournal(t, public, sessionwire.SessionID(world.binding.RuntimeSessionID))
	if len(durable) != len(raw) {
		t.Fatalf("the public journal holds %d events, the runtime journal %d", len(durable), len(raw))
	}
	byEvent := map[sessionwire.EventID]json.RawMessage{}
	sawCause := false
	for i, event := range durable {
		if event.EventID != raw[i].EventID || event.JournalSeq != raw[i].JournalSeq {
			t.Fatalf("durable event %d is %s@%d, the runtime journal's %s@%d", i, event.EventID, event.JournalSeq, raw[i].EventID, raw[i].JournalSeq)
		}
		for _, id := range private {
			if strings.Contains(string(event.Body), id) {
				t.Errorf("the durable body at %d names the private id %s: %s", event.JournalSeq, id, event.Body)
			}
		}
		header := headerOf(t, event.Body)
		if header.SessionID != string(composeSession) {
			t.Errorf("the durable %s at %d names session %q, want the public %q", header.Type, event.JournalSeq, header.SessionID, composeSession)
		}
		if header.Cause != nil && header.Cause.CommandID != nil && *header.Cause.CommandID == string(command) {
			sawCause = true
		}
		byEvent[event.EventID] = event.Body
	}
	if !sawCause {
		t.Error("no durable body names the admitted command as its cause")
	}

	if len(live) == 0 {
		t.Fatal("no live publication was read")
	}
	for _, publication := range live {
		for _, id := range private {
			if strings.Contains(string(publication.Body), id) {
				t.Errorf("the live body at %d names the private id %s: %s", publication.JournalSeq, id, publication.Body)
			}
		}
		stored, ok := byEvent[publication.EventID]
		if !ok {
			t.Errorf("live event %s@%d is not in the journal", publication.EventID, publication.JournalSeq)
			continue
		}
		if !bytes.Equal(stored, publication.Body) {
			t.Errorf("event %s@%d reads differently live and durably:\n live    %s\n durable %s", publication.EventID, publication.JournalSeq, publication.Body, stored)
		}
	}

	// A resolver's read under any other name is refused, not projected.
	if _, err := public.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{TenantID: composeTenant, SessionID: composeSession}); !errors.Is(err, host.ErrPublicJournalScope) {
		t.Fatalf("a read naming the public session = %v, want ErrPublicJournalScope", err)
	}
}
