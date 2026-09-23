package compose

import (
	"testing"
	"time"
)

// TestALostResidencyCanBeAttachedAgain is finding F5 (P3.1 gate, 2026-09-23)
// reproduced without a database.
//
// A PostgreSQL crash through PgBouncer fails one pgstore lease renewal, and
// pgstore closes the residency grant's Lost channel. The heartbeat surrenders
// and ResidencyLost stops the command consumer — correctly: the grant is gone.
// What must follow is that Factory can place the session again. It cannot if
// this Host goes on answering an attach for the session with the DEAD
// residency: Factory is told the session is resident here, nothing consumes
// its inbox, and every later command stays pending for good, silently.
func TestALostResidencyCanBeAttachedAgain(t *testing.T) {
	f := newFixture(t)
	f.start()
	first := f.attach(tenantA, sessionA)

	f.store.loseLease(keyA)
	awaitForgotten(t, f)
	if f.runtime.Abandoned() != 1 || f.runtime.Released() != 0 {
		t.Fatalf("the lost residency's runtime was abandoned %d and released %d times, want 1 and 0: a runtime left running keeps renewing the journal lease a successor's restore needs",
			f.runtime.Abandoned(), f.runtime.Released())
	}
	if _, resident := f.svc.manager.Observe(keyA); resident {
		t.Fatal("the registry still holds the lost residency, so an attach would be answered with it")
	}

	// The successor runtime this Host launches on the re-attach.
	f.rig.Session = newControllableSession(testRigSessionID)
	second := f.attach(tenantA, sessionA)
	if got := f.trace.count("lease.acquire"); got != 2 {
		t.Fatalf("lease.acquire recorded %d times after a lost residency was attached again, want 2: the dead residency was reported as resident", got)
	}
	if !second.Attached || second.Generation == first.Generation {
		t.Fatalf("second attach = (attached %v, generation %d), first generation %d: want a fresh residency", second.Attached, second.Generation, first.Generation)
	}
}

// awaitForgotten waits, bounded, for the lost-residency handoff to finish.
func awaitForgotten(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.svc.mu.Lock()
		_, present := f.svc.sessions[keyA]
		f.svc.mu.Unlock()
		if !present {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the composition never forgot the session whose grant was lost")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestALostResidencyWhoseRuntimeCannotBeAbandonedIsReleased is the fallback:
// a runtime that forwards no crash-equivalent release is released
// nonterminally rather than left running under a grant this Host lost, and the
// session still becomes attachable again.
func TestALostResidencyWhoseRuntimeCannotBeAbandonedIsReleased(t *testing.T) {
	f := newFixture(t)
	runtime := newPublishingSession()
	f.rig.Session = runtime
	f.start()
	f.attach(tenantA, sessionA)

	f.store.loseLease(keyA)
	awaitForgotten(t, f)
	if runtime.Released() != 1 {
		t.Fatalf("a runtime with no abandon capability was released %d times after its residency was lost, want 1", runtime.Released())
	}
	if _, resident := f.svc.manager.Observe(keyA); resident {
		t.Fatal("the registry still holds the lost residency, so an attach would be answered with it")
	}
}
