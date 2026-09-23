package compose

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

var supervisedKey = registry.Key{TenantID: "tenant-a", SessionID: "session-public"}

const (
	supervisedRuntime = "11111111-1111-4111-8111-111111111111"
	supervisedCommand = "ffffffff-ffff-4fff-8fff-ffffffffffff"
)

// fast is the retry and restart bound every test here runs with.
var fast = backoff{floor: time.Millisecond, ceiling: 4 * time.Millisecond}

// flakyInbox fails its first `failures` reads, then serves one admission.
type flakyInbox struct {
	failures atomic.Int32
	reads    atomic.Int32
}

func (f *flakyInbox) ListOrdered(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, after uint64, _ int) ([]commands.Command, error) {
	f.reads.Add(1)
	if f.failures.Add(-1) >= 0 {
		return nil, errors.New("store briefly unreachable")
	}
	if after >= 1 {
		return nil, nil
	}
	return []commands.Command{{CommandID: "command-public", AcceptedOrder: 1, RuntimeCommandID: uuid.MustParse(supervisedCommand)}}, nil
}

// wirePublications records every payload put on the wire; failNext refuses
// the next one.
type wirePublications struct {
	mu       sync.Mutex
	payloads [][]byte
	failNext bool
}

func (w *wirePublications) Publish(_ string, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failNext {
		w.failNext = false
		return errors.New("transport refused")
	}
	w.payloads = append(w.payloads, append([]byte(nil), payload...))
	return nil
}

// bodies returns the body of every publication put on the wire.
func (w *wirePublications) bodies() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, payload := range w.payloads {
		var publication struct {
			Body json.RawMessage `json:"body"`
		}
		_ = json.Unmarshal(payload, &publication)
		out = append(out, string(publication.Body))
	}
	return out
}

type countingRoutes struct{ invalidated atomic.Int32 }

func (r *countingRoutes) InvalidateSession(registry.Key) []hostlink.Binding {
	r.invalidated.Add(1)
	return nil
}

// resubscribable hands each SubscribeCommitted a fresh stream, fed by send.
type resubscribable struct {
	mu      sync.Mutex
	streams []chan sessionwire.EnduringPublication
	opened  chan struct{}
}

func newResubscribable() *resubscribable { return &resubscribable{opened: make(chan struct{}, 16)} }

func (r *resubscribable) SubscribeCommitted(ctx context.Context, _ sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	stream := make(chan sessionwire.EnduringPublication, 4)
	r.mu.Lock()
	r.streams = append(r.streams, stream)
	r.mu.Unlock()
	r.opened <- struct{}{}
	return stream, nil
}

func (r *resubscribable) send(seq uint64, body string) {
	r.mu.Lock()
	stream := r.streams[len(r.streams)-1]
	r.mu.Unlock()
	stream <- sessionwire.EnduringPublication{
		TenantID: supervisedKey.TenantID, SessionID: supervisedKey.SessionID,
		EventID: sessionwire.EventID("event-" + string(rune('a'+seq))), JournalSeq: seq, CoveredThrough: seq,
		Body: json.RawMessage(body),
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func causedBody() string {
	return `{"cause":{"command_id":"` + supervisedCommand + `"},"session_id":"` + supervisedRuntime + `","type":"TurnStarted"}`
}

// TestATransientMappingReadDoesNotEndTheLiveTail is review F2: a store error
// reading the command mapping is retried, the tail keeps running, and the
// body is published projected once the read succeeds.
func TestATransientMappingReadDoesNotEndTheLiveTail(t *testing.T) {
	handler := newCapturingHandler()
	inbox := &flakyInbox{}
	inbox.failures.Store(3)
	ids := publicbody.Identities{
		RuntimeSessionID: uuid.MustParse(supervisedRuntime), SessionID: supervisedKey.SessionID,
		Commands: publicbody.NewIndex(inboxCommands{inbox: inbox, key: supervisedKey}),
	}
	wire := &wirePublications{}
	routes := &countingRoutes{}
	tails, err := service.NewTails(service.TailOptions{Publications: wire, Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	runtime := newResubscribable()
	tail, err := tails.PublishProjected(t.Context(), supervisedKey, runtime, retryingProjection(publicbody.Projection(ids), slog.New(handler), supervisedKey, fast))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	awaitSignal(t, runtime.opened, "the subscription")
	runtime.send(1, causedBody())

	want := `{"cause":{"command_id":"command-public"},"session_id":"session-public","type":"TurnStarted"}`
	awaitCondition(t, "the projected body on the wire", func() bool {
		bodies := wire.bodies()
		return len(bodies) == 1 && bodies[0] == want
	})
	if end, _ := tail.End(); end != service.TailEndRunning {
		t.Fatalf("the tail ended %q after transient read errors", end)
	}
	if routes.invalidated.Load() != 0 {
		t.Fatal("a transient read error invalidated the session's routes")
	}
	warnings := 0
	for _, record := range handler.snapshot() {
		if record.level == slog.LevelWarn && strings.Contains(record.message, "retrying the publication") {
			warnings++
		}
	}
	if warnings != 3 {
		t.Fatalf("logged %d retry warnings, want 3", warnings)
	}
}

// TestAProjectionNotAboutTheMappingIsNotRetried: a non-canonical body cannot
// project on any retry.
func TestAProjectionNotAboutTheMappingIsNotRetried(t *testing.T) {
	calls := 0
	project := retryingProjection(func(context.Context, json.RawMessage, uint64) (json.RawMessage, error) {
		calls++
		return nil, publicbody.ErrNonCanonical
	}, discardLogger, supervisedKey, fast)
	if _, err := project(t.Context(), json.RawMessage(`{}`), 1); !errors.Is(err, publicbody.ErrNonCanonical) || calls != 1 {
		t.Fatalf("= %v after %d calls, want ErrNonCanonical after 1", err, calls)
	}
}

// TestAStopDuringAMappingRetryIsAStop: the retry ends with the context.
func TestAStopDuringAMappingRetryIsAStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	project := retryingProjection(func(context.Context, json.RawMessage, uint64) (json.RawMessage, error) {
		cancel()
		return nil, &publicbody.MappingError{Cause: errors.New("down")}
	}, discardLogger, supervisedKey, fast)
	if _, err := project(ctx, json.RawMessage(`{}`), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("= %v, want the context's error", err)
	}
}

// TestARefusedTailIsLoggedAndRestarted is review F2's last half: a genuine
// refusal invalidates the routes (Factory resets its viewers durably), is
// logged at ERROR, and a new tail is subscribed so the reset viewers have a
// stream to rejoin.
func TestARefusedTailIsLoggedAndRestarted(t *testing.T) {
	handler := newCapturingHandler()
	svc := &Service{options: Options{Logger: slog.New(handler)}, tailRestartBounds: fast}
	wire := &wirePublications{failNext: true}
	routes := &countingRoutes{}
	tails, err := service.NewTails(service.TailOptions{Publications: wire, Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	runtime := newResubscribable()
	supervised, err := svc.superviseTail(t.Context(), supervisedKey, func() (*service.Tail, error) {
		return tails.PublishProjected(t.Context(), supervisedKey, runtime, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(supervised.Stop)
	awaitSignal(t, runtime.opened, "the first subscription")
	runtime.send(1, `{"type":"refused"}`)
	awaitSignal(t, runtime.opened, "the restarted subscription")
	runtime.send(2, `{"type":"delivered"}`)
	awaitCondition(t, "the restarted tail's publication", func() bool {
		bodies := wire.bodies()
		return len(bodies) == 1 && bodies[0] == `{"type":"delivered"}`
	})
	if routes.invalidated.Load() != 1 {
		t.Fatalf("routes invalidated %d times, want once for the refusal", routes.invalidated.Load())
	}
	logged := false
	for _, record := range handler.snapshot() {
		if record.level == slog.LevelError && strings.Contains(record.message, "refused a publication") && strings.Contains(record.attributes["error"], "transport refused") {
			logged = true
		}
	}
	if !logged {
		t.Fatal("the refusal was not logged at ERROR with its cause")
	}

	// Stop ends the restarted tail and restarts nothing more.
	supervised.Stop()
	supervised.mu.Lock()
	current := supervised.current
	supervised.mu.Unlock()
	awaitSignal(t, current.Done(), "the restarted tail to stop")
	if end, _ := current.End(); end != service.TailEndStopped {
		t.Fatalf("the stopped tail ended %q", end)
	}
	select {
	case <-runtime.opened:
		t.Fatal("a stopped supervisor subscribed again")
	case <-time.After(50 * time.Millisecond):
	}
}
