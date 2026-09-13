package compose

import (
	"errors"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/testkit"
)

// ---------------------------------------------------------------------------
// The attach a composed Host could not perform
// ---------------------------------------------------------------------------
//
// EVERY TEST HERE IS IN COMPOSITION AND NONE OF THEM IS AT AN ADAPTER, and the
// distinction is a correction rather than a preference. The claim "a composed
// Host can consume" was previously discharged by a differential over three
// adapter methods, and a gate accepted a sentence that was wider than its probe.
// The subject below is *Service: the object a binary builds, holding the
// residency manager, the ownership seam, the consumer and the live tail.

// TestAComposedHostAttachesADispositionSessionAndStartsItsConsumer is the
// property the attach-time journal fence made unreachable.
//
// THE FENCE WAS LEGACY RESIDUE. Step 3 committed an opening in-stream ownership
// record BEFORE hydration, through a call whose bindSessionScope pins
// ProtocolModeLegacy — so a session whose catalog binding is disposition, which
// is the only binding AcquireResidency will grant residency over, conflicted on
// binding.protocol_mode and the attach died at StepFence. BeginOwnership, at
// step 8, never ran, and the durable command consumer was therefore never
// started on any session this Host could actually hold.
//
// THE PROBE IS THE THREE THINGS THE SEQUENCE OWES, not one. An attach that
// merely returns nil has proved nothing about ownership: the residency must be
// reported attached, the composition must be tracking the session the ownership
// step installs, and the consumer that step starts must be registered and
// reachable through the seam HostLink delivers commands over. A build that
// returned success and started nothing satisfies the first alone.
func TestAComposedHostAttachesADispositionSessionAndStartsItsConsumer(t *testing.T) {
	f := newFixture(t)
	f.start()

	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	if mode := f.store.mode(key); mode != modeDisposition {
		t.Fatalf("the session under test is bound to %q, want %q; this test is about the mode a Host can hold", mode, modeDisposition)
	}

	held, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID:  tenantA,
		SessionID: sessionA,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if err != nil {
		var refusal *residency.AttachError
		if errors.As(err, &refusal) {
			t.Fatalf("the attach failed at step %q: %v", refusal.Step, err)
		}
		t.Fatalf("the attach failed: %v", err)
	}
	if !held.Attached {
		t.Fatalf("Attach reported Attached false; this call established the residency")
	}

	// (a) OWNERSHIP RAN. trackResident is called only from the composition's
	// BeginOwnership, after the inner heartbeat ownership succeeded, so a
	// tracked session is step 8 having completed rather than step 7.
	f.svc.mu.Lock()
	_, tracked := f.svc.sessions[key]
	f.svc.mu.Unlock()
	if !tracked {
		t.Errorf("the composition tracks no resident session, so BeginOwnership did not complete")
	}

	// (b) THE CONSUMER IS RUNNING AND REACHABLE. ConsumerFor is the seam the
	// HostLink command path resolves a delivery through; a consumer started and
	// not registered is one no command can ever reach.
	if _, running := f.svc.ConsumerFor(key); !running {
		t.Errorf("no durable command consumer is registered for the attached session")
	}

	// THERE IS NO "NO JOURNAL WRITER WAS OPENED" ASSERTION HERE, deliberately.
	// compose.Options declares no opener at all, so the claim is held by the
	// compiler, and a trace counter over a call nothing can make is a negative
	// assertion that cannot fail. The structural form of the rule lives where a
	// reinstatement would be written:
	// internal/residency.TestNoProductionFileOpensAJournalWriter.

	// (c) THE RUNTIME'S GRANT IS WHAT THE RESIDENCY REPORTS.
	if !held.JournalEpochHeld {
		t.Errorf("the residency reports no journal grant; the fixture's runtime holds one")
	}
	if want := residency.JournalEpoch(testkit.FirstJournalEpoch); held.JournalEpoch != want {
		t.Errorf("the residency reports journal epoch %d, want the runtime's %d", held.JournalEpoch, want)
	}
}

// TestAComposedHostRefusesALegacySessionBeforeItTakesAnything is the OTHER
// protocol mode, and it exists because a suite that only ever drives one mode
// cannot see a mode defect at all.
//
// IT IS NOT A SECOND SPELLING OF THE TEST ABOVE. Removing the fence does not
// make a Host mode-blind: a legacy-bound session has no residency grant to take,
// because AcquireResidency refuses it at the catalog binding, so the attach
// fails at StepLease with nothing acquired. That is the branch the mode still
// governs after the fence is gone, and it is Host's own branch rather than the
// store's — step 2's refusal path.
func TestAComposedHostRefusesALegacySessionBeforeItTakesAnything(t *testing.T) {
	f := newFixture(t)
	f.start()

	legacy := sessionwire.SessionID("session-legacy")
	key := registry.Key{TenantID: tenantA, SessionID: legacy}
	f.store.bind(key, modeLegacy)

	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID:  tenantA,
		SessionID: legacy,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if err == nil {
		t.Fatalf("the attach of a legacy-bound session succeeded; the store grants residency only in disposition mode")
	}
	var refusal *residency.AttachError
	if !errors.As(err, &refusal) {
		t.Fatalf("the refusal is %T, want a *residency.AttachError naming the step", err)
	}
	if refusal.Step != residency.StepLease {
		t.Errorf("the attach failed at step %q, want %q; a legacy session is refused the residency grant itself", refusal.Step, residency.StepLease)
	}
	if !strings.Contains(err.Error(), "binding.protocol_mode") {
		t.Errorf("the refusal does not name the catalog binding: %v", err)
	}

	// NOTHING WAS TAKEN AND NOTHING WAS STARTED.
	f.svc.mu.Lock()
	_, tracked := f.svc.sessions[key]
	f.svc.mu.Unlock()
	if tracked {
		t.Errorf("the composition tracks a session whose lease was refused")
	}
	if _, running := f.svc.ConsumerFor(key); running {
		t.Errorf("a consumer was started for a session whose lease was refused")
	}

	// THE CONTROL. The same fixture, the same Host, a disposition session: the
	// refusal above is a decision about the binding rather than a Host that
	// cannot attach anything.
	if held := f.attach(tenantA, sessionA); !held.Attached {
		t.Fatalf("the control attach of a disposition session did not establish a residency")
	}
	if _, running := f.svc.ConsumerFor(registry.Key{TenantID: tenantA, SessionID: sessionA}); !running {
		t.Fatalf("the control session started no consumer, so the probe above cannot distinguish a refusal from a broken fixture")
	}
}
