package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// optionsSubscriber implements every live capability and records which one the
// tail chose, so a test can tell the preferred seam from the fallback.
type optionsSubscriber struct {
	stream <-chan department.LivePublication
	mu     sync.Mutex
	asked  []department.LiveOptions
	legacy int
}

func (s *optionsSubscriber) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	s.mu.Lock()
	s.legacy++
	s.mu.Unlock()
	return nil, nil
}

func (s *optionsSubscriber) SubscribeLivePublic(context.Context) (<-chan department.LivePublication, error) {
	s.mu.Lock()
	s.legacy++
	s.mu.Unlock()
	return s.stream, nil
}

func (s *optionsSubscriber) SubscribeLivePublicWithReasoning(context.Context) (<-chan department.LivePublication, error) {
	s.mu.Lock()
	s.legacy++
	s.mu.Unlock()
	return s.stream, nil
}

func (s *optionsSubscriber) SubscribeLivePublicWith(_ context.Context, options department.LiveOptions) (<-chan department.LivePublication, error) {
	s.mu.Lock()
	s.asked = append(s.asked, options)
	s.mu.Unlock()
	return s.stream, nil
}

func toolBody(key registry.Key, kind, loop, turn string, members map[string]any) json.RawMessage {
	body := map[string]any{
		"v": 1, "type": kind, "session_id": string(key.SessionID), "loop_id": loop, "turn_id": turn, "step_id": "step",
		"tool_execution_id": "exec-1", "tool_use_id": "toolu_1", "tool_name": "Bash",
	}
	for name, value := range members {
		body[name] = value
	}
	encoded, _ := json.Marshal(body)
	return encoded
}

func toolFrame(key registry.Key, kind, loop, turn string, members map[string]any) department.LivePublication {
	return department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: toolBody(key, kind, loop, turn, members)}}
}

func identityProjector(_ context.Context, body json.RawMessage, _ uint64) (json.RawMessage, error) {
	return body, nil
}

func toolTails(t *testing.T, publications service.Publications, include bool, timer service.FlushTimer, logger *slog.Logger) *service.Tails {
	t.Helper()
	options := service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, IncludeToolSteps: include, Logger: logger}
	if timer != nil {
		options.NewFlushTimer = func(time.Duration) service.FlushTimer { return timer }
	}
	tails, err := service.NewTails(options)
	if err != nil {
		t.Fatal(err)
	}
	return tails
}

func TestToolStepFlushesPendingTextFirstAndIsNeverMerged(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 4)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 4)}
	wire := &livePublications{admit: true}
	tail, err := toolTails(t, wire, true, timer, nil).PublishProjected(t.Context(), key, &optionsSubscriber{stream: stream}, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- liveFrame(key, "loop", "turn", "before ")
	stream <- toolFrame(key, "ToolCallStarted", "loop", "turn", map[string]any{"summary": "ls"})
	stream <- liveFrame(key, "loop", "turn", "after")
	waitFor(t, "text and tool frames", func() bool { return len(wire.recorded()) == 2 })
	waitFor(t, "trailing text on the flush timer", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return len(wire.recorded()) == 3
	})
	frames := wire.recorded()
	for i, want := range []string{`"text":"before "`, `"type":"ToolCallStarted"`, `"text":"after"`} {
		if !bytes.Contains(frames[i].payload, []byte(want)) {
			t.Fatalf("frame %d = %s, want %s", i, frames[i].payload, want)
		}
	}
	if bytes.Contains(frames[1].payload, []byte(`"text"`)) {
		t.Fatalf("tool frame merged with text: %s", frames[1].payload)
	}
	if tail.EphemeralPublished() != 3 || tail.EphemeralDrops() != 0 {
		t.Fatalf("published %d, drops %d", tail.EphemeralPublished(), tail.EphemeralDrops())
	}
}

func TestRefusedToolStepIsDroppedWithoutRetryAndLeavesTextUngapped(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 4)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 4)}
	wire := &refusingPublications{refuse: 1}
	tail, err := toolTails(t, wire, true, timer, nil).PublishProjected(t.Context(), key, &optionsSubscriber{stream: stream}, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	// The transport refuses the tool frame itself.
	stream <- toolFrame(key, "ToolCallStarted", "loop", "turn", map[string]any{"summary": "ls"})
	waitFor(t, "refused tool frame", func() bool { return tail.EphemeralDrops() == 1 })
	stream <- liveFrame(key, "loop", "turn", "still streams")
	waitFor(t, "text after the refused tool frame", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return tail.EphemeralPublished() == 1
	})
	if wire.tried() != 2 {
		t.Fatalf("transport tried %d times, want one tool offer and one text; a refused tool frame is never retried", wire.tried())
	}
	frames := wire.recorded()
	if len(frames) != 1 || !bytes.Contains(frames[0].payload, []byte(`"text":"still streams"`)) {
		t.Fatalf("frames = %+v", frames)
	}
}

func TestToolStepDroppedWhenPendingTextCannotFlushKeepsTheText(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 4)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 4)}
	wire := &refusingPublications{refuse: 100}
	tail, err := toolTails(t, wire, true, timer, nil).PublishProjected(t.Context(), key, &optionsSubscriber{stream: stream}, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- liveFrame(key, "loop", "turn", "held ")
	stream <- toolFrame(key, "ToolCallCompleted", "loop", "turn", map[string]any{"result_preview": "ok"})
	waitFor(t, "tool dropped behind unflushable text", func() bool { return tail.EphemeralDrops() == 1 })
	if wire.tried() != 1 {
		t.Fatalf("transport tried %d times, want only the text flush; the tool frame must not overtake it", wire.tried())
	}
	wire.allowNext()
	// Text arriving later for the same key still merges and publishes: the
	// dropped tool frame did not gap it.
	stream <- liveFrame(key, "loop", "turn", "joined")
	waitFor(t, "held text published", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return tail.EphemeralPublished() == 1
	})
	frames := wire.recorded()
	if len(frames) != 1 || !bytes.Contains(frames[0].payload, []byte(`"text":"held joined"`)) || bytes.Contains(frames[0].payload, []byte("ToolCall")) {
		t.Fatalf("frames = %+v", frames)
	}
	if tail.EphemeralDrops() != 1 {
		t.Fatalf("drops = %d, want only the tool frame", tail.EphemeralDrops())
	}
}

func TestToolStepsNeedTheOption(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 3)
	timer := &manualFlushTimer{ticks: make(chan time.Time, 4)}
	wire := &livePublications{admit: true}
	subscriber := &optionsSubscriber{stream: stream}
	tail, err := toolTails(t, wire, false, timer, nil).PublishProjected(t.Context(), key, subscriber, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	stream <- toolFrame(key, "ToolCallStarted", "loop", "turn", map[string]any{"summary": "ls"})
	stream <- toolFrame(key, "ToolCallCompleted", "loop", "turn", map[string]any{"result_preview": "ok"})
	stream <- liveFrame(key, "loop", "turn", "text")
	waitFor(t, "text frame", func() bool {
		select {
		case timer.ticks <- time.Now():
		default:
		}
		return tail.EphemeralPublished() == 1
	})
	for _, frame := range wire.recorded() {
		if bytes.Contains(frame.payload, []byte("ToolCall")) {
			t.Fatalf("tool frame published with the option off: %s", frame.payload)
		}
	}
	if tail.EphemeralDrops() != 2 {
		t.Fatalf("drops = %d, want both tool frames", tail.EphemeralDrops())
	}
	if len(subscriber.asked) != 1 || subscriber.asked[0] != (department.LiveOptions{}) || subscriber.legacy != 0 {
		t.Fatalf("subscription = options %+v, legacy calls %d; want the preferred seam with zero options", subscriber.asked, subscriber.legacy)
	}
}

func TestLiveOptionsSubscriberIsPreferred(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	subscriber := &optionsSubscriber{stream: make(chan department.LivePublication)}
	tails, err := service.NewTails(service.TailOptions{Publications: &livePublications{admit: true}, Routes: &recordingRoutes{}, IncludeReasoning: true, IncludeToolSteps: true})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), key, subscriber, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	tail.Stop()
	want := department.LiveOptions{IncludeReasoning: true, IncludeToolSteps: true}
	if len(subscriber.asked) != 1 || subscriber.asked[0] != want || subscriber.legacy != 0 {
		t.Fatalf("subscription = options %+v, legacy calls %d; want one %+v", subscriber.asked, subscriber.legacy, want)
	}
}

func TestRequestedToolStepsWithoutRuntimeSupportLogsOnceAndStreamsText(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	var logs bytes.Buffer
	wire := &livePublications{admit: true}
	tails := toolTails(t, wire, true, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	for range 2 {
		stream := make(chan department.LivePublication, 1)
		stream <- liveFrame(key, "loop", "turn", "text")
		tail, err := tails.PublishProjected(t.Context(), key, liveSubscriber{stream}, nil, identityProjector)
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, "text over the fallback seam", func() bool { return tail.EphemeralPublished() == 1 })
		tail.Stop()
	}
	if got := strings.Count(logs.String(), `"msg":"host: tool step previews unavailable"`); got != 1 {
		t.Fatalf("missing-runtime log = %s, want one record", logs.String())
	}
}

func TestOversizedToolStepIsFittedUnderTheFrameCap(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 2)
	wire := &livePublications{admit: true}
	tail, err := toolTails(t, wire, true, nil, nil).PublishProjected(t.Context(), key, &optionsSubscriber{stream: stream}, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	// Every byte of both members escapes to six: the worst case harness's
	// 1 KiB summary and 2 KiB preview can produce.
	heavy := strings.Repeat("\x01", 2048)
	stream <- toolFrame(key, "ToolCallCompleted", "loop", "turn", map[string]any{"summary": heavy[:1024], "result_preview": heavy, "is_error": true})
	waitFor(t, "fitted tool frame", func() bool { return len(wire.recorded()) == 1 })
	frame := wire.recorded()[0].payload
	if len(frame) > 4096 {
		t.Fatalf("tool frame is %d bytes, over the 4 KiB cap", len(frame))
	}
	var envelope struct {
		Body struct {
			ResultPreview string `json:"result_preview"`
			ToolUseID     string `json:"tool_use_id"`
			IsError       bool   `json:"is_error"`
		} `json:"body"`
	}
	if err := json.Unmarshal(frame, &envelope); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(envelope.Body.ResultPreview, "…") || envelope.Body.ToolUseID != "toolu_1" || !envelope.Body.IsError {
		t.Fatalf("fitted body = %+v", envelope.Body)
	}
}

func TestToolStepNamingAnotherSessionIsDropped(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	stream := make(chan department.LivePublication, 1)
	foreign := toolBody(registry.Key{TenantID: key.TenantID, SessionID: "foreign"}, "ToolCallStarted", "loop", "turn", nil)
	stream <- department.LivePublication{Ephemeral: &sessionwire.EphemeralPublication{TenantID: key.TenantID, SessionID: key.SessionID, Body: foreign}}
	wire := &livePublications{admit: true}
	tail, err := toolTails(t, wire, true, nil, nil).PublishProjected(t.Context(), key, &optionsSubscriber{stream: stream}, nil, identityProjector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	waitFor(t, "foreign tool drop", func() bool { return tail.EphemeralDrops() == 1 || len(wire.recorded()) > 0 })
	if len(wire.recorded()) != 0 {
		t.Fatalf("foreign tool step reached HostLink: %s", wire.recorded()[0].payload)
	}
}

// TestHarnessToolStepsReachTheHostLinkChannelWithoutRawArguments runs the real
// hub and the real runtime adapter: the model's raw tool arguments stream as a
// ToolUseChunk delta and must never cross, while the tool's audit summary does.
func TestHarnessToolStepsReachTheHostLinkChannelWithoutRawArguments(t *testing.T) {
	const secret = "sk-live-tool-secret-marker"
	live := newLiveSession(t, "tenant-alpha")
	publications := &livePublications{admit: true}
	rewrite := publicbody.Projection(publicbody.Identities{RuntimeSessionID: live.rigID, SessionID: live.key.SessionID})
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: &recordingRoutes{}, IncludeReasoning: true, IncludeToolSteps: true})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), live.key, live.runtime, rewrite, rewrite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	header, err := live.factory.Stamp(event.Header{Coordinates: identity.Coordinates{SessionID: live.rigID, LoopID: newUUID(t), TurnID: newUUID(t), StepID: newUUID(t)}})
	if err != nil {
		t.Fatal(err)
	}
	execution := newUUID(t)
	for _, ev := range []event.Event{
		event.TokenDelta{Header: header, Chunk: &content.ToolUseChunk{ID: "toolu_live", Name: "Bash", InputJSON: `{"command":"curl -H 'Authorization: ` + secret + `'"}`}},
		event.ToolCallStarted{Header: header, ToolExecutionID: execution, ToolUseID: "toolu_live", ToolName: "Bash", Summary: "curl (redacted)"},
		event.ToolCallCompleted{Header: header, ToolExecutionID: execution, ToolUseID: "toolu_live", ToolName: "Bash", ElapsedMillis: 7, ResultPreview: "200 OK"},
	} {
		if err := live.hub.PublishEventChecked(t.Context(), ev); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "both tool frames", func() bool { return len(publications.recorded()) == 2 })
	live.appendPublicEvent(t)
	waitFor(t, "committed frame after the tool steps", func() bool { return len(publications.recorded()) == 3 })
	frames := publications.recorded()
	for i, want := range []string{`"type":"ToolCallStarted"`, `"type":"ToolCallCompleted"`, `"event_id"`} {
		if !bytes.Contains(frames[i].payload, []byte(want)) {
			t.Fatalf("frame %d = %s, want %s", i, frames[i].payload, want)
		}
	}
	for i, frame := range frames {
		if bytes.Contains(frame.payload, []byte(secret)) || bytes.Contains(frame.payload, []byte("Authorization")) {
			t.Fatalf("frame %d leaked raw tool arguments: %s", i, frame.payload)
		}
	}
	if !bytes.Contains(frames[0].payload, []byte(`"summary":"curl (redacted)"`)) || !bytes.Contains(frames[0].payload, []byte(`"tool_use_id":"toolu_live"`)) || !bytes.Contains(frames[1].payload, []byte(`"elapsed_ms":7`)) {
		t.Fatalf("tool frames = %s / %s", frames[0].payload, frames[1].payload)
	}
}

// TestToolStepsOffIsByteIdenticalToTheLegacySeam replays one mixed stream — a
// tool step a runtime emitted anyway, text, and a committed boundary — over
// the legacy LivePublicationSubscriber and over LiveOptionsSubscriber with the
// option off, and requires the same HostLink bytes and counters.
func TestToolStepsOffIsByteIdenticalToTheLegacySeam(t *testing.T) {
	key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	run := func(t *testing.T, subscriber func(<-chan department.LivePublication) department.PublicationSubscriber) ([][]byte, uint64, uint64) {
		stream := make(chan department.LivePublication, 4)
		timer := &manualFlushTimer{ticks: make(chan time.Time, 4)}
		wire := &livePublications{admit: true}
		tail, err := toolTails(t, wire, false, timer, nil).PublishProjected(t.Context(), key, subscriber(stream), nil, identityProjector)
		if err != nil {
			t.Fatal(err)
		}
		defer tail.Stop()
		stream <- toolFrame(key, "ToolCallStarted", "loop", "turn", map[string]any{"summary": "ls"})
		stream <- liveFrame(key, "loop", "turn", "hello")
		waitFor(t, "text frame", func() bool {
			select {
			case timer.ticks <- time.Now():
			default:
			}
			return tail.EphemeralPublished() == 1
		})
		stream <- department.LivePublication{Enduring: &sessionwire.EnduringPublication{TenantID: key.TenantID, SessionID: key.SessionID, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{"type":"StepDone","loop_id":"loop"}`)}}
		waitFor(t, "committed frame", func() bool { return tail.Published() == 1 })
		var payloads [][]byte
		for _, frame := range wire.recorded() {
			payloads = append(payloads, frame.payload)
		}
		return payloads, tail.EphemeralPublished(), tail.EphemeralDrops()
	}
	legacy, legacyPublished, legacyDrops := run(t, func(stream <-chan department.LivePublication) department.PublicationSubscriber {
		return liveSubscriber{stream}
	})
	options, optionsPublished, optionsDrops := run(t, func(stream <-chan department.LivePublication) department.PublicationSubscriber {
		return &optionsSubscriber{stream: stream}
	})
	if len(legacy) != 2 || legacyPublished != 1 || legacyDrops != 1 {
		t.Fatalf("legacy baseline = %d frames, published %d, drops %d", len(legacy), legacyPublished, legacyDrops)
	}
	if len(options) != len(legacy) || optionsPublished != legacyPublished || optionsDrops != legacyDrops {
		t.Fatalf("options seam = %d frames, published %d, drops %d; legacy %d, %d, %d", len(options), optionsPublished, optionsDrops, len(legacy), legacyPublished, legacyDrops)
	}
	for i := range legacy {
		if !bytes.Equal(legacy[i], options[i]) {
			t.Fatalf("frame %d differs:\n%s\n%s", i, legacy[i], options[i])
		}
	}
}
