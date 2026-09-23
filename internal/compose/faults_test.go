package compose

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/hostconfig"
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
