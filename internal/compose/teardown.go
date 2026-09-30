package compose

import (
	"context"
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
// ABANDONING DOMINATES, AND IS REMEMBERED. An abandon requested while a
// graceful flight runs records abandonPending, cancels that flight's context
// (which ends harness's idle wait; a teardown already begun ignores it), and
// waits. The flight's own completion — not any waiter — then starts the
// abandon if the release failed without tearing the runtime down, in the same
// critical section that records the failure, so no release can be elected in
// between and an abandon whose waiter gave up still happens. A release
// requested while abandoning, while an abandon is pending, or after a failed
// teardown never issues a release; it joins.
//
// EVERY FLIGHT IS AN IMMUTABLE OBJECT created at its election. A waiter keeps
// the flight it joined and reads only that flight's result, so a later flight
// can never overwrite what it sees. A waiter bounded by its own context stops
// WAITING when it ends; the flight runs on, on a context detached from every
// caller, and its completion still transitions the phase.
//
// SUCCESS IS THE RUNTIME'S OUTCOME. Each flight calls the runtime exactly
// once, on a context detached from its callers, and takes its result as the
// outcome: department.Releaser obliges a runtime to report its teardown's own
// result rather than its caller's deadline (harnessruntime does so for
// harness by joining a begun teardown itself). An error after the runtime's
// liveness ended is a teardown that happened and failed: terminal, never
// retried here.
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

	// pendingAbandon, when set while releasing, is the abandon a caller asked
	// for; the graceful flight's completion starts it if the release failed.
	pendingAbandon func(context.Context) error

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

	// handedTo is the pending abandon this graceful flight's failure started,
	// or nil; like err it is set before done closes.
	handedTo *teardownFlight
}

// teardownFlightBound is the generous bound on one runtime teardown call. It
// is a safety net, not a policy: callers bound their own waits.
const teardownFlightBound = 5 * time.Minute

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
// flight, waiting for it bounded by ctx. A graceful flight that failed and
// handed over to a pending abandon is followed into that abandon, so a caller
// asking for the runtime to be released learns how its release actually ended.
func (t *teardown) release(ctx context.Context, runtime teardownRuntime) error {
	t.mu.Lock()
	var flight *teardownFlight
	switch t.phase {
	case phaseReleased:
		t.mu.Unlock()
		return nil
	case phaseAbandoning, phaseFailed:
		flight = t.flight
		t.mu.Unlock()
		return t.wait(ctx, flight)
	case phaseReleasing:
		flight = t.flight
	default:
		flight = t.electLocked(kindGraceful, ctx, runtime, runtime.ReleaseResidency)
	}
	t.mu.Unlock()
	if err := t.waitFor(ctx, flight); err != nil {
		return err
	}
	// HANDED OVER when this flight's failure started the pending abandon:
	// follow that abandon. Any other later flight is not this caller's.
	if flight.err != nil && flight.handedTo != nil {
		return t.wait(ctx, flight.handedTo)
	}
	return flight.err
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
			t.pendingAbandon = faults.AbandonResidency // the flight's completion honours it
			flight.cancel()
			t.mu.Unlock()
			if err := t.waitFor(ctx, flight); err != nil {
				return err // still in flight past ctx; the pending abandon still happens
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
	flight.cancel()

	t.mu.Lock()
	flight.err = err
	var callbacks []func()
	pending := t.pendingAbandon
	t.pendingAbandon = nil
	switch {
	case err == nil:
		t.phase = phaseReleased
		callbacks, t.onReleased = t.onReleased, nil
	case flight.kind == kindGraceful && !channelClosed(runtime.Done()):
		if pending != nil {
			// THE PENDING ABANDON STARTS HERE, in the same critical section
			// that records the failed release, so no release can win the
			// interval and no waiter has to be present for it to happen.
			flight.handedTo = t.electLocked(kindAbandon, context.Background(), runtime, pending)
		} else {
			t.phase = phaseResident // nothing was torn down; a later release may retry
		}
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
