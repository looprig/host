package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

type liveSubscriber struct {
	stream <-chan department.LivePublication
}

func (s liveSubscriber) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, errors.New("committed fallback used for a live subscriber")
}
func (s liveSubscriber) SubscribeLivePublic(context.Context) (<-chan department.LivePublication, error) {
	return s.stream, nil
}

type livePublications struct {
	recordingPublications
	admit bool
}

func (p *livePublications) TryPublishEphemeral(channel string, payload []byte) bool {
	if !p.admit {
		return false
	}
	return p.Publish(channel, payload) == nil
}

type publicCommands struct{ runtime uuid.UUID }

func (m publicCommands) PublicCommand(_ context.Context, runtime uuid.UUID, _ uint64) (sessionwire.CommandID, bool, error) {
	if runtime == m.runtime {
		return "public-command", true, nil
	}
	return "", false, nil
}

func TestLiveTailPublishesMixedFramesInOrderWithPublicIdentities(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	privateSession := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	privateCommand := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	ids := publicbody.Identities{RuntimeSessionID: privateSession, SessionID: key.SessionID, Commands: publicCommands{runtime: privateCommand}}
	rewrite := publicbody.Projection(ids)
	stream := make(chan department.LivePublication, 3)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{
		TenantID: key.TenantID, SessionID: key.SessionID,
		Body: json.RawMessage(`{"v":1,"type":"TokenDelta","session_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","loop_id":"loop-1","turn_id":"turn-1","cause":{"command_id":"11111111-2222-3333-4444-555555555555"},"chunk":{"chunk_type":"text","text":"one"}}`),
	}}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, rewrite, rewrite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "first transient HostLink frame", func() bool { return len(publications.recorded()) == 1 })
	stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{
		TenantID: key.TenantID, SessionID: key.SessionID, EventID: "event-2", JournalSeq: 2, CoveredThrough: 2,
		Body: json.RawMessage(`{"session_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","type":"StepDone"}`),
	}}
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{
		TenantID: key.TenantID, SessionID: key.SessionID,
		Body: json.RawMessage(`{"v":1,"type":"TokenDelta","session_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","loop_id":"loop-1","turn_id":"turn-1","chunk":{"chunk_type":"text","text":"two"}}`),
	}}
	waitFor(t, "three HostLink frames", func() bool { return len(publications.recorded()) == 3 })
	frames := publications.recorded()
	for i, frame := range frames {
		if frame.channel != hostlink.ChannelFor(key) || bytes.Contains(frame.payload, []byte(privateSession.String())) || bytes.Contains(frame.payload, []byte(privateCommand.String())) {
			t.Fatalf("frame %d has wrong channel or private ID: %s", i, frame.payload)
		}
	}
	if !bytes.Contains(frames[0].payload, []byte(`"text":"one"`)) || !bytes.Contains(frames[1].payload, []byte(`"event_id":"event-2"`)) || !bytes.Contains(frames[2].payload, []byte(`"text":"two"`)) {
		t.Fatalf("frames are out of order: %s / %s / %s", frames[0].payload, frames[1].payload, frames[2].payload)
	}
}

func TestFullEphemeralTransportAndMappingFailureDoNotDelayEnduring(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 2)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: json.RawMessage(`{"v":1,"type":"TokenDelta","loop_id":"loop-1","turn_id":"turn-1","chunk":{"chunk_type":"text","text":"secret"}}`)}}
	stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "event-2", JournalSeq: 2, CoveredThrough: 2, Body: json.RawMessage(`{"type":"StepDone"}`)}}
	publications := &livePublications{admit: false}
	tails := newTails(t, publications, &recordingRoutes{})
	badMapping := func(context.Context, json.RawMessage, uint64) (json.RawMessage, error) {
		return nil, &publicbody.MappingError{Cause: errors.New("store down")}
	}
	tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, badMapping)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "enduring frame", func() bool { return len(publications.recorded()) == 1 })
	if !bytes.Contains(publications.recorded()[0].payload, []byte(`"event_id":"event-2"`)) {
		t.Fatalf("enduring frame did not pass: %s", publications.recorded()[0].payload)
	}
	if tail.EphemeralDrops() == 0 {
		t.Fatal("ephemeral drop was not counted")
	}
}

func TestAdjacentTextAggregatesAndFlushesByInterval(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 2)
	for _, text := range []string{"hello ", "world"} {
		body, _ := json.Marshal(map[string]any{"v": 1, "type": "TokenDelta", "session_id": string(key.SessionID), "loop_id": "loop-1", "turn_id": "turn-1", "chunk": map[string]any{"chunk_type": "text", "text": text}})
		stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: body}}
	}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) { return body, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "aggregated text frame", func() bool { return len(publications.recorded()) == 1 })
	time.Sleep(50 * time.Millisecond)
	frames := publications.recorded()
	if len(frames) != 1 || !bytes.Contains(frames[0].payload, []byte(`"text":"hello world"`)) {
		t.Fatalf("aggregated frames = %+v", frames)
	}
}

func TestSlowTransientProjectionCannotHoldEnduringFrame(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 2)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: json.RawMessage(`{"v":1,"type":"TokenDelta","loop_id":"loop-1","turn_id":"turn-1","chunk":{"chunk_type":"text","text":"x"}}`)}}
	stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{"type":"StepDone"}`)}}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{})
	project := func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) {
		close(entered)
		<-release
		return body, nil
	}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, project)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("transient projector was not entered")
	}
	deadline := time.After(time.Second)
	for len(publications.recorded()) == 0 {
		select {
		case <-deadline:
			t.Fatal("enduring publication waited for a slow transient projection")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestHarnessTextDeltaReachesTheHostLinkChannelBeforeCommittedEvent(t *testing.T) {
	live := newLiveSession(t, "tenant-alpha")
	publications := &livePublications{admit: true}
	ids := publicbody.Identities{RuntimeSessionID: live.rigID, SessionID: live.key.SessionID}
	rewrite := publicbody.Projection(ids)
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), live.key, live.runtime, rewrite, rewrite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	header, err := live.factory.Stamp(event.Header{Coordinates: identity.Coordinates{SessionID: live.rigID, LoopID: newUUID(t), TurnID: newUUID(t)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []content.Chunk{
		&content.ThinkingChunk{Thinking: "private-thought"},
		&content.ToolUseChunk{InputJSON: `{"secret":"tool-input"}`},
		&content.ImageChunk{},
		&content.TextChunk{Text: "hello live"},
	} {
		if err := live.hub.PublishEventChecked(t.Context(), event.TokenDelta{Header: header, Chunk: chunk}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "text delta on HostLink", func() bool { return len(publications.recorded()) == 1 })
	live.appendPublicEvent(t)
	waitFor(t, "enduring frame after text", func() bool { return len(publications.recorded()) == 2 })
	frames := publications.recorded()
	for _, frame := range frames {
		if frame.channel != hostlink.ChannelFor(live.key) || bytes.Contains(frame.payload, []byte("private-thought")) || bytes.Contains(frame.payload, []byte("tool-input")) {
			t.Fatalf("unexpected HostLink frame: %s", frame.payload)
		}
	}
	if !bytes.Contains(frames[0].payload, []byte(`"text":"hello live"`)) || !bytes.Contains(frames[1].payload, []byte(`"event_id"`)) {
		t.Fatalf("text and enduring order = %s / %s", frames[0].payload, frames[1].payload)
	}
}

func TestLiveTailRejectsOtherEphemeralBodyTypes(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 1)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{
		TenantID: key.TenantID, SessionID: key.SessionID,
		Body: json.RawMessage(`{"v":1,"type":"ToolCallStarted","session_id":"session-a","loop_id":"loop-1","turn_id":"turn-1","chunk":{"chunk_type":"text","text":"wrong-kind"}}`),
	}}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) { return body, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "invalid ephemeral drop", func() bool { return tail.EphemeralDrops() == 1 || len(publications.recorded()) > 0 })
	if len(publications.recorded()) != 0 {
		t.Fatalf("non-TokenDelta body reached HostLink: %s", publications.recorded()[0].payload)
	}
}

func TestLiveTailRejectsBodyNamingAnotherSession(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 1)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{
		TenantID: key.TenantID, SessionID: key.SessionID,
		Body: json.RawMessage(`{"v":1,"type":"TokenDelta","session_id":"foreign-private","loop_id":"loop-1","turn_id":"turn-1","chunk":{"chunk_type":"text","text":"wrong-session"}}`),
	}}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) { return body, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "foreign body drop", func() bool { return tail.EphemeralDrops() == 1 || len(publications.recorded()) > 0 })
	if len(publications.recorded()) != 0 {
		t.Fatalf("foreign session body reached HostLink: %s", publications.recorded()[0].payload)
	}
}
