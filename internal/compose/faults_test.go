package compose

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
)

// awaitCondition polls a condition the composition reaches on its own goroutine.
func awaitCondition(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestAFaultedRuntimeIsReleasedForASuccessor is D3's Host half. A runtime that
// latches a persistence fault can never persist again and refuses every command,
// so a Host that kept it resident wedged the session's whole command stream. The
// composition must give it up crash-equivalently — abandoned, never released
// (that would anchor to a log it cannot trust) — tombstone the route, hand the
// grant back and credit the capacity, so a successor can attach and restore.
func TestAFaultedRuntimeIsReleasedForASuccessor(t *testing.T) {
	// ONE slot, so a leaked admission charge refuses the second attach below.
	f := newFixture(t, func(_ *Options, host *hostconfig.Options) { host.Capacity = 1 })
	f.start()
	f.attach(tenantA, sessionA)
	first := f.runtime
	if _, held := f.svc.ConsumerFor(keyA); !held {
		t.Fatal("the attached session has no consumer; the fixture never made it resident")
	}

	first.Fault(errors.New("injected journal append failure"))

	awaitCondition(t, "the faulted runtime to be abandoned", func() bool { return first.Abandoned() == 1 })
	awaitCondition(t, "the faulted session to leave this Host", func() bool {
		_, held := f.svc.ConsumerFor(keyA)
		return !held && f.svc.residentFor(keyA) == nil
	})
	awaitCondition(t, "the residency grant to be released", func() bool { return f.trace.count("lease.release") == 1 })
	if got := first.Released(); got != 0 {
		t.Errorf("ReleaseResidency called %d times on a faulted runtime; it must be abandoned, not released", got)
	}
	if f.trace.count("locations.tombstone") != 1 {
		t.Errorf("tombstones = %d, want 1: the route must stop advertising a runtime that cannot persist", f.trace.count("locations.tombstone"))
	}
	var releasing bool
	for _, row := range f.store.published() {
		if row.Residency == sessionwire.SessionResidencyReleasing && !row.Accepting {
			releasing = true
		}
	}
	if !releasing {
		t.Error("no releasing, non-accepting observation was published before the tombstone")
	}
	if f.trace.count("workspace.release") != 1 {
		t.Errorf("workspace releases = %d, want 1", f.trace.count("workspace.release"))
	}

	// A successor: the same Host re-attaching the session must succeed, which it
	// cannot if the registry entry or the grant leaked; and with one slot, another
	// session must fit, which it cannot if the admission charge leaked.
	f.rig.Session = newControllableSession(testRigSessionID)
	if held := f.attach(tenantA, sessionB); !held.Attached {
		t.Fatal("the released slot was not credited back: another session could not attach")
	}
}

// TestAHealthyRuntimeIsNotAbandoned is the control: no fault, no release.
func TestAHealthyRuntimeIsNotAbandoned(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	time.Sleep(50 * time.Millisecond)
	if got := f.runtime.Abandoned(); got != 0 {
		t.Fatalf("a healthy runtime was abandoned %d times", got)
	}
	if _, held := f.svc.ConsumerFor(keyA); !held {
		t.Fatal("a healthy session lost its consumer")
	}
}

// TestAStrandedAttemptReleasesTheRuntimeOnceAndOnlyForItsOwnResidency: the
// applier's report and the fault broadcast converge on ONE crash-equivalent
// release, and a report naming a residency this Host no longer holds under that
// generation touches nothing.
func TestAStrandedAttemptReleasesTheRuntimeOnceAndOnlyForItsOwnResidency(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	held := f.svc.residentFor(keyA)
	if held == nil {
		t.Fatal("the attached session is not tracked")
	}
	cause := errors.New("injected: the prefix append failed")

	f.svc.strandedAttempt(keyA, held.generation+1, "command-stale", cause)
	time.Sleep(50 * time.Millisecond)
	if got := f.runtime.Abandoned(); got != 0 {
		t.Fatalf("a report for another generation abandoned the runtime %d times", got)
	}

	f.svc.strandedAttempt(keyA, held.generation, "command-a", cause)
	f.runtime.Fault(cause)
	f.svc.strandedAttempt(keyA, held.generation, "command-a", cause)
	awaitCondition(t, "the stranded runtime to be abandoned", func() bool { return f.runtime.Abandoned() >= 1 })
	awaitCondition(t, "the session to leave this Host", func() bool { return f.svc.residentFor(keyA) == nil })
	time.Sleep(50 * time.Millisecond)
	if got := f.runtime.Abandoned(); got != 1 {
		t.Errorf("the runtime was abandoned %d times, want exactly once", got)
	}
	if got := f.trace.count("lease.release"); got != 1 {
		t.Errorf("the grant was released %d times, want exactly once", got)
	}
}

// blockingFaultingSession is a publishing runtime whose dispatch blocks until
// released and which offers the durable-health capability, so a test can fault it
// while a consumer pass is inside ApplyCommand.
type blockingFaultingSession struct {
	*publishingSession
	entered  chan struct{}
	release  chan struct{}
	faulted  chan struct{}
	mu       sync.Mutex
	inFlight bool
	// abandonedInFlight records an abandon that arrived while a dispatch was
	// still inside ApplyCommand — the ordering D3 gate F2 forbids.
	abandonedInFlight bool
	abandoned         int
	// onAbandon runs inside AbandonResidency, before it returns.
	onAbandon func()
}

func (s *blockingFaultingSession) ApplyCommand(ctx context.Context, command department.RuntimeCommand) error {
	s.mu.Lock()
	s.inFlight = true
	s.mu.Unlock()
	close(s.entered)
	<-s.release
	s.mu.Lock()
	s.inFlight = false
	s.mu.Unlock()
	return s.publishingSession.ApplyCommand(ctx, command)
}

func (s *blockingFaultingSession) PersistenceFaulted() <-chan struct{} { return s.faulted }
func (s *blockingFaultingSession) PersistenceFault() error             { return errors.New("injected fault") }
func (s *blockingFaultingSession) AbandonResidency(context.Context) error {
	if s.onAbandon != nil {
		s.onAbandon()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abandoned++
	if s.inFlight {
		s.abandonedInFlight = true
	}
	return nil
}

func (s *blockingFaultingSession) snapshot() (abandoned int, inFlight bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abandoned, s.abandonedInFlight
}

// TestAGiveUpWaitsForTheConsumerPassInFlight is D3 gate F2: the runtime is
// abandoned only after the consumer's pass in flight has left ApplyCommand.
// Consumer.Stop does not wait for a pass; Halt does, and it is what orders a
// dispatch before the abandon.
func TestAGiveUpWaitsForTheConsumerPassInFlight(t *testing.T) {
	store := newDurableCommands(keyA)
	inner := newPublishingSession()
	inner.effects = store
	runtime := &blockingFaultingSession{publishingSession: inner, entered: make(chan struct{}), release: make(chan struct{}), faulted: make(chan struct{})}
	f := newFixture(t, func(o *Options, _ *hostconfig.Options) {
		o.Inbox = store
		o.Cursors = store
		dispositions := newFakeDispositions(o.Clock.(*fakeClock), store)
		dispositions.payloads = store
		o.Records = dispositions
		o.Writers = &fakeDispositionWriters{store: dispositions}
		inner.disposition = func(command sessionwire.CommandID) { dispositions.setEvidence(command, "applied") }
	})
	f.rig.Session = runtime
	f.start()
	f.attach(tenantA, sessionA)
	// The session's work must already be stopped when the runtime is abandoned:
	// its consumer gone, so nothing can dispatch into a runtime mid-abandon.
	var consumerAtAbandon atomic.Bool
	runtime.onAbandon = func() {
		_, held := f.svc.ConsumerFor(keyA)
		consumerAtAbandon.Store(held)
	}

	store.accept(t, "command-in-flight", `{"blocks":[{"text":"hello"}]}`)
	f.svc.Wake(keyA)
	select {
	case <-runtime.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the consumer never dispatched the command")
	}

	close(runtime.faulted)
	time.Sleep(100 * time.Millisecond)
	if abandoned, _ := runtime.snapshot(); abandoned != 0 {
		t.Fatal("the runtime was abandoned while the consumer's pass was still inside ApplyCommand")
	}
	close(runtime.release)
	awaitCondition(t, "the runtime to be abandoned once the pass left", func() bool {
		abandoned, _ := runtime.snapshot()
		return abandoned == 1
	})
	if _, inFlight := runtime.snapshot(); inFlight {
		t.Fatal("the abandon arrived while a dispatch was in flight")
	}
	if consumerAtAbandon.Load() {
		t.Fatal("the runtime was abandoned while the session's consumer was still running")
	}
}

// TestAStaleGiveUpDoesNotUnchargeASuccessor is D3 gate F3: the capacity credit
// and the warm forget are keyed by session only, so a give-up for a residency
// that has since been REPLACED under the same key must leave them alone.
func TestAStaleGiveUpDoesNotUnchargeASuccessor(t *testing.T) {
	f := newFixture(t, func(_ *Options, host *hostconfig.Options) { host.Capacity = 1 })
	f.start()
	f.attach(tenantA, sessionA)
	stale := f.svc.residentFor(keyA)
	faults, ok := persistenceFaultsFor(stale.runtime)
	if !ok {
		t.Fatal("the fixture runtime offers no PersistenceFaults")
	}
	// A successor residency now holds the key (and its admission charge).
	successor := &resident{key: stale.key, agent: stale.agent, generation: stale.generation + 1, runtime: stale.runtime}
	f.svc.mu.Lock()
	f.svc.sessions[keyA] = successor
	f.svc.mu.Unlock()

	f.svc.releaseUnusable(t.Context(), stale, faults, "test", errors.New("late"))

	f.rig.Session = newControllableSession(testRigSessionID)
	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	var refused *residency.AttachError
	if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorNoCapacity {
		t.Fatalf("attach into a Host whose one slot the successor holds = %v, want no_capacity: a stale give-up credited the slot back", err)
	}
	if f.svc.residentFor(keyA) != successor {
		t.Error("a stale give-up removed the successor's residency")
	}
}
