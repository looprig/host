package compose

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// scriptedRuntime is a runtime whose release and abandon a test drives step by
// step. Each call takes the next scripted behaviour; an unscripted call
// succeeds at once.
type scriptedRuntime struct {
	mu       sync.Mutex
	releases int
	abandons int
	done     chan struct{}
	doneOnce sync.Once

	release []func(context.Context) error
	abandon []func(context.Context) error
}

func newScriptedRuntime() *scriptedRuntime { return &scriptedRuntime{done: make(chan struct{})} }

func (r *scriptedRuntime) Done() <-chan struct{} { return r.done }
func (r *scriptedRuntime) tearDown()             { r.doneOnce.Do(func() { close(r.done) }) }

func (r *scriptedRuntime) ReleaseResidency(ctx context.Context) error {
	r.mu.Lock()
	r.releases++
	var step func(context.Context) error
	if len(r.release) != 0 {
		step, r.release = r.release[0], r.release[1:]
	}
	r.mu.Unlock()
	if step == nil {
		r.tearDown()
		return nil
	}
	return step(ctx)
}

func (r *scriptedRuntime) PersistenceFaulted() <-chan struct{} { return nil }
func (r *scriptedRuntime) PersistenceFault() error             { return nil }
func (r *scriptedRuntime) AbandonResidency(ctx context.Context) error {
	r.mu.Lock()
	r.abandons++
	var step func(context.Context) error
	if len(r.abandon) != 0 {
		step, r.abandon = r.abandon[0], r.abandon[1:]
	}
	r.mu.Unlock()
	if step == nil {
		r.tearDown()
		return nil
	}
	return step(ctx)
}

func (r *scriptedRuntime) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.releases, r.abandons
}

// blocking returns a step that waits for release (ignoring its context when
// deaf) and then runs finish.
func blocking(release <-chan struct{}, deaf bool, finish func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if deaf {
			<-release
		} else {
			select {
			case <-release:
			case <-ctx.Done():
				return fmt.Errorf("not idle: %w", ctx.Err())
			}
		}
		return finish(ctx)
	}
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// Codex P1: a success check and a flight election that were not one critical
// section let a caller that had checked "not released" start a second release
// after another caller's release had succeeded. Now a caller arriving during a
// flight joins it, and one arriving after its success is answered without a
// call.
func TestASuccessfulReleaseIsNeverIssuedTwice(t *testing.T) {
	runtime := newScriptedRuntime()
	gate := make(chan struct{})
	runtime.release = []func(context.Context) error{blocking(gate, true, func(context.Context) error { runtime.tearDown(); return nil })}
	var td teardown

	results := make(chan error, 2)
	go func() { results <- td.release(context.Background(), runtime) }()
	eventually(t, "the first release to be issued", func() bool { r, _ := runtime.counts(); return r == 1 })
	go func() { results <- td.release(context.Background(), runtime) }() // arrives mid-flight
	time.Sleep(10 * time.Millisecond)
	close(gate)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("release = %v, want nil", err)
		}
	}
	if err := td.release(context.Background(), runtime); err != nil { // arrives after success
		t.Fatalf("a release after success = %v", err)
	}
	if releases, _ := runtime.counts(); releases != 1 {
		t.Fatalf("the runtime was asked to release %d times, want once", releases)
	}
}

// Codex P2: waiters read a reusable result another flight could overwrite. A
// waiter now reads only the flight it joined: held between observing its
// flight's completion and reading the result while a retry succeeds, it still
// gets its own flight's failure.
func TestAWaiterReadsOnlyItsOwnFlightsResult(t *testing.T) {
	runtime := newScriptedRuntime()
	refused := errors.New("not idle")
	runtime.release = []func(context.Context) error{func(context.Context) error { return refused }}
	var td teardown
	hold := make(chan struct{})
	holding := make(chan struct{})
	var once sync.Once
	td.waited = func(flight *teardownFlight) {
		once.Do(func() {
			close(holding)
			<-hold
		})
	}

	first := make(chan error, 1)
	go func() { first <- td.release(context.Background(), runtime) }()
	<-holding // the first waiter saw its flight fail, and is held before reading
	td.waited = nil
	if err := td.release(context.Background(), runtime); err != nil {
		t.Fatalf("the retry = %v, want success", err)
	}
	close(hold)
	if err := <-first; !errors.Is(err, refused) {
		t.Fatalf("the first waiter got %v, want its own flight's refusal", err)
	}
	if !td.released() {
		t.Fatal("the retry's success was not recorded")
	}
}

// Codex P2: a teardown that completes after its caller's deadline — harness
// finishes a begun teardown and reports the context error beside it — was
// counted a failure, and its session context could not be cancelled once the
// manager record was pruned. Now the outcome is read by joining the teardown,
// and the registered callback (the context's cancel) runs on the late success.
func TestALateSuccessIsASuccessAndStillCancels(t *testing.T) {
	runtime := newScriptedRuntime()
	begun := make(chan struct{})
	finish := make(chan struct{})
	runtime.release = []func(context.Context) error{
		func(ctx context.Context) error {
			runtime.tearDown() // the teardown begins: liveness ends
			close(begun)
			<-finish // and runs to completion whatever the context says
			<-ctx.Done()
			return fmt.Errorf("session: context done: %w", ctx.Err())
		},
		// the join: harness answers it with the teardown's own (clean) result
		func(context.Context) error { return nil },
	}
	var td teardown
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() { waited <- td.release(ctx, runtime) }()
	<-begun
	cancel()
	if err := <-waited; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("the waiter whose context ended got %v, want an in-flight error", err)
	}
	cancelled := make(chan struct{})
	td.whenReleased(func() { close(cancelled) }) // as endResidency registers after pruning
	close(finish)
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the late success did not run the registered cancellation")
	}
	if !td.released() {
		t.Fatal("a teardown that completed cleanly after its caller's deadline was not counted released")
	}
}

// Codex P2: release and abandon elections were not atomic with each other.
// Abandoning dominates: a graceful flight is cancelled and awaited, and the
// abandon runs only if it failed; a release requested while abandoning — or
// after a failed abandon — issues nothing and joins the abandon.
func TestReleaseAndAbandonAreElectedAsOne(t *testing.T) {
	t.Run("an abandon cancels a graceful release waiting for idle, then abandons", func(t *testing.T) {
		runtime := newScriptedRuntime()
		never := make(chan struct{})
		runtime.release = []func(context.Context) error{blocking(never, false, nil)}
		var td teardown
		released := make(chan error, 1)
		go func() { released <- td.release(context.Background(), runtime) }()
		eventually(t, "the release to be issued", func() bool { r, _ := runtime.counts(); return r == 1 })
		if err := td.abandon(context.Background(), runtime, runtime); err != nil {
			t.Fatalf("abandon = %v", err)
		}
		if err := <-released; err == nil {
			t.Fatal("the cancelled graceful release reported success")
		}
		if releases, abandons := runtime.counts(); releases != 1 || abandons != 1 || !td.released() {
			t.Fatalf("releases=%d abandons=%d released=%v, want 1, 1, true", releases, abandons, td.released())
		}
	})

	t.Run("an abandon requested during a release that succeeds does not abandon", func(t *testing.T) {
		runtime := newScriptedRuntime()
		gate := make(chan struct{})
		runtime.release = []func(context.Context) error{blocking(gate, true, func(context.Context) error { runtime.tearDown(); return nil })}
		var td teardown
		go func() { _ = td.release(context.Background(), runtime) }()
		eventually(t, "the release to be issued", func() bool { r, _ := runtime.counts(); return r == 1 })
		abandoned := make(chan error, 1)
		go func() { abandoned <- td.abandon(context.Background(), runtime, runtime) }()
		time.Sleep(10 * time.Millisecond)
		close(gate)
		if err := <-abandoned; err != nil {
			t.Fatalf("abandon = %v", err)
		}
		if _, abandons := runtime.counts(); abandons != 0 {
			t.Fatalf("abandoned %d times after a successful release", abandons)
		}
	})

	t.Run("a release during an abandon joins it and issues nothing", func(t *testing.T) {
		runtime := newScriptedRuntime()
		gate := make(chan struct{})
		failure := errors.New("journal lease release failed")
		runtime.abandon = []func(context.Context) error{blocking(gate, true, func(context.Context) error { runtime.tearDown(); return failure })}
		var td teardown
		abandoned := make(chan error, 1)
		go func() { abandoned <- td.abandon(context.Background(), runtime, runtime) }()
		eventually(t, "the abandon to be issued", func() bool { _, a := runtime.counts(); return a == 1 })
		released := make(chan error, 1)
		go func() { released <- td.release(context.Background(), runtime) }()
		time.Sleep(10 * time.Millisecond)
		close(gate)
		if err := <-abandoned; !errors.Is(err, failure) {
			t.Fatalf("abandon = %v", err)
		}
		if err := <-released; !errors.Is(err, failure) {
			t.Fatalf("the joined release = %v, want the abandon's failure", err)
		}
		if err := td.release(context.Background(), runtime); !errors.Is(err, failure) {
			t.Fatalf("a release after the failed abandon = %v, want its failure", err)
		}
		if releases, _ := runtime.counts(); releases != 0 {
			t.Fatalf("issued %d releases into an abandon", releases)
		}
	})

	t.Run("a waiter bounded by its context leaves the abandon running", func(t *testing.T) {
		runtime := newScriptedRuntime()
		gate := make(chan struct{})
		runtime.abandon = []func(context.Context) error{blocking(gate, true, func(context.Context) error { runtime.tearDown(); return nil })}
		var td teardown
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := td.abandon(ctx, runtime, runtime); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("abandon = %v, want an in-flight error", err)
		}
		ran := make(chan struct{})
		td.whenReleased(func() { close(ran) })
		close(gate)
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatal("the late abandon's success did not run the callback")
		}
	})
}
