package compose

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/looprig/host/department"
)

// teardown is ONE residency's runtime teardown: the graceful release and the
// crash-equivalent abandon, as one state machine under one mutex.
//
// IT REPLACES TWO FLIGHTS THAT RACED EACH OTHER. A graceful-release flight and
// an abandon flight, each with its own lock, let a success check and a flight
// election interleave (a successful release issued twice), let a waiter read a
// reusable result another flight had overwritten, let an abandon be elected
// between a release's check and its election, and could not cancel a session
// context once its manager record had been pruned. Each fix to one
// interleaving opened another; so every decision here is one critical section.
//
// THE PHASES, and the only transitions:
//
//	resident   --release-->  releasing(graceful flight)
//	resident   --abandon-->  abandoning(abandon flight)
//	releasing  --success-->  released
//	releasing  --failure-->  resident          (graceful is retryable)
//	releasing  --failure, runtime torn down--> failed (terminal)
//	abandoning --success-->  released
//	abandoning --failure-->  failed            (terminal)
//
// ABANDONING DOMINATES. An abandon requested while a graceful flight runs
// cancels that flight's context (which ends harness's idle wait; a teardown
// already begun ignores it), waits for its outcome, and abandons only if it
// failed. A release requested while abandoning — or after a failed teardown —
// never issues a release; it joins that flight's outcome.
//
// EVERY FLIGHT IS AN IMMUTABLE OBJECT created at its election. A waiter keeps
// the flight it joined and reads only that flight's result, so a later flight
// can never overwrite what it sees. A waiter bounded by its own context stops
// WAITING when it ends; the flight runs on, on a context detached from every
// caller, and its completion still transitions the phase.
//
// SUCCESS IS THE RUNTIME'S OUTCOME, NOT THE CALLER'S DEADLINE. harness v0.45.0
// finishes a teardown that has begun whatever its caller's context says, and
// then reports the context's error beside a successful cleanup. So when a call
// fails with a context error after the runtime's liveness has ended (the
// teardown began), the outcome is read by joining that teardown with a fresh
// context: harness answers a join with the teardown's own cleanup result. That
// second call joins; it does not start a second teardown.
//
// THE TERMINAL SUCCESS RUNS THE REGISTERED onReleased CALLBACKS, outside the
// lock. The composition registers one that cancels the session context when it
// prunes the manager record, so a runtime that finishes tearing down after its
// drain reported it Parked still has its context cancelled.
type teardown struct {
	mu         sync.Mutex
	phase      teardownPhase
	flight     *teardownFlight
	onReleased []func()

	// waited, when set, runs after a waiter observed its flight complete and
	// before it reads the result. It exists so a test can hold a waiter at
	// exactly that point while another flight runs.
	waited func(*teardownFlight)
}

type teardownPhase uint8

const (
	phaseResident teardownPhase = iota
	phaseReleasing
	phaseAbandoning
	phaseReleased
	phaseFailed
)

type teardownKind uint8

const (
	kindGraceful teardownKind = iota
	kindAbandon
)

func (k teardownKind) String() string {
	if k == kindAbandon {
		return "abandon"
	}
	return "release"
}

// teardownFlight is one runtime call, immutable once done closes.
type teardownFlight struct {
	kind   teardownKind
	done   chan struct{}
	cancel context.CancelFunc

	// err is written exactly once, before done closes, and never after.
	err error
}

// teardownFlightBound is the generous bound on one runtime teardown call. It
// is a safety net, not a policy: callers bound their own waits.
const teardownFlightBound = 5 * time.Minute

// teardownJoinBound bounds the join that reads a begun teardown's outcome.
const teardownJoinBound = time.Minute

// teardownRuntime is what the state machine calls on the runtime.
type teardownRuntime interface {
	ReleaseResidency(context.Context) error
	Done() <-chan struct{}
}

// released reports whether the runtime has torn down successfully. It never
// waits on a flight.
func (t *teardown) released() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phase == phaseReleased
}

// whenReleased runs callback once the runtime has released — at once if it
// already has. A runtime that never releases never runs it.
func (t *teardown) whenReleased(callback func()) {
	t.mu.Lock()
	if t.phase != phaseReleased {
		t.onReleased = append(t.onReleased, callback)
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	callback()
}

// release gives the runtime up gracefully, or joins the teardown already in
// flight, waiting for it bounded by ctx.
func (t *teardown) release(ctx context.Context, runtime teardownRuntime) error {
	t.mu.Lock()
	switch t.phase {
	case phaseReleased:
		t.mu.Unlock()
		return nil
	case phaseReleasing, phaseAbandoning, phaseFailed:
		flight := t.flight
		t.mu.Unlock()
		return t.wait(ctx, flight)
	}
	flight := t.electLocked(kindGraceful, ctx, runtime, runtime.ReleaseResidency)
	t.mu.Unlock()
	return t.wait(ctx, flight)
}

// abandon gives the runtime up crash-equivalently, waiting bounded by ctx. A
// graceful flight in progress is cancelled and awaited first; the abandon
// runs only if it failed.
func (t *teardown) abandon(ctx context.Context, runtime teardownRuntime, faults department.PersistenceFaults) error {
	for {
		t.mu.Lock()
		switch t.phase {
		case phaseReleased:
			t.mu.Unlock()
			return nil
		case phaseAbandoning, phaseFailed:
			flight := t.flight
			t.mu.Unlock()
			return t.wait(ctx, flight)
		case phaseReleasing:
			flight := t.flight
			flight.cancel()
			t.mu.Unlock()
			if err := t.waitFor(ctx, flight); err != nil {
				return err // still in flight past ctx
			}
			continue // re-decide under the lock with the flight's outcome in place
		}
		flight := t.electLocked(kindAbandon, context.Background(), runtime, faults.AbandonResidency)
		t.mu.Unlock()
		return t.wait(ctx, flight)
	}
}

// electLocked starts one flight. The caller holds t.mu. A graceful flight's
// context is also cancelled when the electing caller's context ends, which is
// what ends the runtime's idle wait; an abandon's is detached from callers.
func (t *teardown) electLocked(kind teardownKind, caller context.Context, runtime teardownRuntime, call func(context.Context) error) *teardownFlight {
	ctx, cancel := context.WithTimeout(context.Background(), teardownFlightBound)
	flight := &teardownFlight{kind: kind, done: make(chan struct{}), cancel: cancel}
	if kind == kindGraceful {
		stop := context.AfterFunc(caller, cancel)
		flight.cancel = func() { stop(); cancel() }
	}
	t.flight = flight
	if kind == kindGraceful {
		t.phase = phaseReleasing
	} else {
		t.phase = phaseAbandoning
	}
	// #nosec G118 -- the flight belongs to the residency, not to its caller.
	go t.run(ctx, flight, runtime, call)
	return flight
}

// run performs one flight's runtime call and transitions the phase.
func (t *teardown) run(ctx context.Context, flight *teardownFlight, runtime teardownRuntime, call func(context.Context) error) {
	err := call(ctx)
	if err != nil && isContextDone(err) && channelClosed(runtime.Done()) {
		// The teardown began and the call reported its context beside it: read
		// the teardown's own outcome by joining it.
		joinCtx, cancel := context.WithTimeout(context.Background(), teardownJoinBound)
		err = call(joinCtx)
		cancel()
	}
	flight.cancel()

	t.mu.Lock()
	flight.err = err
	var callbacks []func()
	switch {
	case err == nil:
		t.phase = phaseReleased
		callbacks, t.onReleased = t.onReleased, nil
	case flight.kind == kindGraceful && !channelClosed(runtime.Done()):
		t.phase = phaseResident // nothing was torn down; a later release may retry
	default:
		t.phase = phaseFailed
	}
	close(flight.done)
	t.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

// wait waits for flight, bounded by ctx, and returns its outcome.
func (t *teardown) wait(ctx context.Context, flight *teardownFlight) error {
	if err := t.waitFor(ctx, flight); err != nil {
		return err
	}
	return flight.err
}

// waitFor waits for flight to complete, bounded by ctx; it reports only
// whether the wait ended first.
func (t *teardown) waitFor(ctx context.Context, flight *teardownFlight) error {
	select {
	case <-flight.done:
		if t.waited != nil {
			t.waited(flight)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("compose: the runtime's %s is still in flight: %w", flight.kind, ctx.Err())
	}
}

// isContextDone reports whether err carries a context's cancellation or expiry.
func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// channelClosed reports, without blocking, whether channel is closed.
func channelClosed(channel <-chan struct{}) bool {
	if channel == nil {
		return false
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
