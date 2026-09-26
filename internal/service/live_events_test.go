package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/livetext"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
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

type refusingPublications struct {
	recordingPublications
	mu       sync.Mutex
	attempts int
	refuse   int
}

func (p *refusingPublications) TryPublishEphemeral(channel string, payload []byte) bool {
	p.mu.Lock()
	p.attempts++
	refuse := p.attempts <= p.refuse
	p.mu.Unlock()
	if refuse {
		return false
	}
	return p.Publish(channel, payload) == nil
}
func (p *refusingPublications) tried() int { p.mu.Lock(); defer p.mu.Unlock(); return p.attempts }

type manualFlushTimer struct{ ticks chan time.Time }

func (m *manualFlushTimer) C() <-chan time.Time      { return m.ticks }
func (m *manualFlushTimer) Stop() bool               { return true }
func (m *manualFlushTimer) Reset(time.Duration) bool { return true }
func (m *manualFlushTimer) fire()                    { m.ticks <- time.Now() }

func liveBody(key registry.Key, loop, turn, text string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{"v": 1, "type": "TokenDelta", "session_id": string(key.SessionID), "loop_id": loop, "turn_id": turn, "chunk": map[string]any{"chunk_type": "text", "text": text}})
	return body
}
func liveFrame(key registry.Key, loop, turn, text string) department.LivePublication {
	return department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: liveBody(key, loop, turn, text)}}
}

func TestRefusedFlushRetriesWithContiguousText(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 3)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 3)}
	publications := &refusingPublications{refuse: 1}
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, FlushInterval: time.Second, NewFlushTimer: func(time.Duration) service.FlushTimer { return timer }})
	if err != nil {
		t.Fatal(err)
	}
	project := func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) { return body, nil }
	tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, project)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- liveFrame(key, "loop-1", "turn-1", "hello ")
	timer.fire()
	waitFor(t, "first refusal", func() bool { return publications.tried() >= 1 })
	stream <- liveFrame(key, "loop-1", "turn-1", "world")
	var joined string
	waitFor(t, "retry admission", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		joined = ""
		for _, frame := range publications.recorded() {
			var message struct {
				Body struct {
					Chunk struct {
						Text string `json:"text"`
					} `json:"chunk"`
				} `json:"body"`
			}
			_ = json.Unmarshal(frame.payload, &message)
			joined += message.Body.Chunk.Text
		}
		return joined == "hello world"
	})
	if joined != "hello world" {
		t.Fatalf("retry lost text: %q", joined)
	}
}

func TestEscapeHeavyTextNeverExceedsTransportCap(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 3)
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, b json.RawMessage, _ uint64) (json.RawMessage, error) { return b, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- liveFrame(key, "loop-1", "turn-1", strings.Repeat("\"", 2000))
	waitFor(t, "oversized encoded delta suppressed", func() bool { return tail.EphemeralDrops() >= 1 })
	stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{"type":"StepDone"}`)}}
	waitFor(t, "durable boundary", func() bool { return tail.Published() == 1 })
	stream <- liveFrame(key, "loop-1", "turn-2", strings.Repeat("\"", 1500))
	waitFor(t, "bounded escaped preview", func() bool { return tail.EphemeralPublished() == 1 })
	for _, frame := range publications.recorded() {
		if len(frame.payload) > 4096 {
			t.Fatalf("oversized frame: %d", len(frame.payload))
		}
	}
}

func TestEscapedMergeIsBoundedByMarshalledEnvelope(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 2)
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, b json.RawMessage, _ uint64) (json.RawMessage, error) { return b, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	first := liveFrame(key, "loop-1", "turn-1", strings.Repeat("\"", 1200))
	if encoded, err := json.Marshal(first.Ephemeral); err != nil || len(encoded) > 4096 {
		t.Fatalf("first escaped frame alone is too large: %d, %v", len(encoded), err)
	}
	second := liveFrame(key, "loop-1", "turn-1", strings.Repeat("\"", 800))
	merged, _ := livetext.Merge(first.Ephemeral.Body, second.Ephemeral.Body)
	candidate := *first.Ephemeral
	candidate.Body = merged
	encoded, _ := json.Marshal(candidate)
	if len(encoded) <= 4096 {
		t.Fatalf("test merged envelope is only %d bytes", len(encoded))
	}
	stream <- first
	stream <- second
	waitFor(t, "encoded merge overflow", func() bool { return tail.EphemeralDrops() >= 2 })
	if tail.EphemeralPublished() != 0 || len(publications.recorded()) != 0 {
		t.Fatal("escaped merge over 4 KiB reached HostLink")
	}
}

func TestCapOverflowSuppressesTurnUntilNextStep(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 6)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 2)}
	publications := &livePublications{admit: true}
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, NewFlushTimer: func(time.Duration) service.FlushTimer { return timer }})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, b json.RawMessage, _ uint64) (json.RawMessage, error) { return b, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- liveFrame(key, "loop-1", "turn-1", strings.Repeat("a", 1500))
	stream <- liveFrame(key, "loop-1", "turn-1", strings.Repeat("b", 600))
	stream <- liveFrame(key, "loop-1", "turn-1", "suppressed")
	waitFor(t, "gap suppression", func() bool { return tail.EphemeralDrops() >= 3 })
	timer.fire()
	if tail.EphemeralPublished() != 0 {
		t.Fatal("gapped turn emitted text")
	}
	stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "step", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{"type":"StepDone","loop_id":"loop-1"}`)}}
	waitFor(t, "step boundary", func() bool { return tail.Published() == 1 })
	stream <- liveFrame(key, "loop-1", "turn-1", "resumed")
	waitFor(t, "next step preview", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return tail.EphemeralPublished() == 1
	})
	frames := publications.recorded()
	if len(frames) != 2 || !bytes.Contains(frames[1].payload, []byte(`"text":"resumed"`)) {
		t.Fatalf("next step frames: %+v", frames)
	}
}

func TestLiveTerminalCauseReachesTail(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	cause := errors.New("committed field missing")
	stream := make(chan department.LivePublication, 1)
	stream <- department.LivePublication{Terminal: cause}
	publications := &livePublications{admit: true}
	var logs bytes.Buffer
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, b json.RawMessage, _ uint64) (json.RawMessage, error) { return b, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	select {
	case <-tail.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal cause did not end tail")
	}
	end, got := tail.End()
	if end != service.TailEndLost || !errors.Is(got, cause) {
		t.Fatalf("tail end = %s, %v", end, got)
	}
	if !bytes.Contains(logs.Bytes(), []byte(cause.Error())) {
		t.Fatalf("terminal cause was not logged: %s", logs.String())
	}
}

type dualSubscriber struct {
	committed <-chan sessionwire.EnduringPublication
	liveCalls int
}

func (s *dualSubscriber) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return s.committed, nil
}
func (s *dualSubscriber) SubscribeLivePublic(context.Context) (<-chan department.LivePublication, error) {
	s.liveCalls++
	return nil, errors.New("mixed subscription must stay disabled")
}

func TestNilTransientProjectorUsesByteIdenticalCommittedRelay(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	body := json.RawMessage(`{"type":"x"}`)
	event := sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "event-1", JournalSeq: 1, CoveredThrough: 1, Body: body}
	committed := make(chan sessionwire.EnduringPublication, 1)
	committed <- event
	subscriber := &dualSubscriber{committed: committed}
	publications := &livePublications{admit: true}
	tail, err := newTails(t, publications, &recordingRoutes{}).PublishProjected(t.Context(), key, subscriber, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "committed frame", func() bool {
		select {
		case <-tail.Done():
			return true
		default:
		}
		return len(publications.recorded()) == 1 || subscriber.liveCalls != 0
	})
	if len(publications.recorded()) != 1 {
		end, cause := tail.End()
		t.Fatalf("committed relay ended %s: %v", end, cause)
	}
	if subscriber.liveCalls != 0 || tail.EphemeralPublished() != 0 || !bytes.Contains(publications.recorded()[0].payload, body) {
		t.Fatalf("default relay changed: live calls %d, transient %d, frame %s", subscriber.liveCalls, tail.EphemeralPublished(), publications.recorded()[0].payload)
	}
	baselineStream := make(chan sessionwire.EnduringPublication, 1)
	baselineStream <- event
	baseline := &recordingPublications{}
	baselineTail, err := newTails(t, baseline, &recordingRoutes{}).Publish(t.Context(), key, scriptedSubscriber{stream: baselineStream})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(baselineTail.Stop)
	waitFor(t, "enduring-only baseline", func() bool { return len(baseline.recorded()) == 1 })
	if !bytes.Equal(publications.recorded()[0].payload, baseline.recorded()[0].payload) {
		t.Fatalf("default frame differs from enduring-only relay:\n%s\n%s", publications.recorded()[0].payload, baseline.recorded()[0].payload)
	}
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
	if tail.Published() != 1 || tail.EphemeralPublished() != 2 {
		t.Fatalf("publication counts = enduring %d, transient %d", tail.Published(), tail.EphemeralPublished())
	}
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
	timer := &manualFlushTimer{ticks: make(chan time.Time, 2)}
	projected := make(chan struct{}, 2)
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, NewFlushTimer: func(time.Duration) service.FlushTimer { return timer }})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, func(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) {
		projected <- struct{}{}
		return body, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	<-projected
	<-projected
	waitFor(t, "aggregated text frame", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return len(publications.recorded()) == 1
	})
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
