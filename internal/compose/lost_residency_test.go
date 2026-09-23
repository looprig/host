package compose

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
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
	handler := newCapturingHandler()
	f := newFixture(t, withLogger(handler))
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

	// B4: one WARN names the loss; the expected refusals of a lost grant (the
	// tombstone, the lease) raise no second one on a clean abandon.
	var gone, incomplete int
	for _, record := range handler.snapshot() {
		switch {
		case record.level < slog.LevelWarn:
		case strings.Contains(record.message, "residency grant for the session is gone"):
			gone++
		case strings.Contains(record.message, "lost session's"):
			incomplete++
		}
	}
	if gone != 1 || incomplete != 0 {
		t.Fatalf("lost-grant WARNs = %d and incomplete-release WARNs = %d, want 1 and 0", gone, incomplete)
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

// TestALostResidencyCreditsItsAdmissionBack is gate MF2: with ONE slot, a
// lost residency that kept its admission charge would refuse every other
// session no_capacity.
func TestALostResidencyCreditsItsAdmissionBack(t *testing.T) {
	f := newFixture(t, func(_ *Options, host *hostconfig.Options) { host.Capacity = 1 })
	f.start()
	f.attach(tenantA, sessionA)

	f.store.loseLease(keyA)
	awaitForgotten(t, f)

	f.rig.Session = newControllableSession(testRigSessionID)
	if held := f.attach(tenantA, sessionB); !held.Attached {
		t.Fatal("the lost residency's slot was not credited back: another session could not attach")
	}
}

// TestAStaleLostReleaseLeavesASuccessorAlone is gate MF1 (D3 gate F3's rule on
// this path): the admission credit and the warm forget are keyed by session,
// so a lost-residency release that finishes after a SUCCESSOR holds the key
// must leave the successor's charge and warm watch alone.
func TestAStaleLostReleaseLeavesASuccessorAlone(t *testing.T) {
	f := newFixture(t, func(_ *Options, host *hostconfig.Options) { host.Capacity = 1 })
	f.start()
	f.attach(tenantA, sessionA)
	stale := f.svc.residentFor(keyA)
	// A successor residency now holds the key, its admission charge and — the
	// watch Watch installed for keyA stands in for it — its warm watch.
	successor := &resident{key: stale.key, agent: stale.agent, generation: stale.generation + 1, runtime: stale.runtime, lease: stale.lease}
	f.svc.mu.Lock()
	f.svc.sessions[keyA] = successor
	f.svc.mu.Unlock()

	f.svc.releaseLost(t.Context(), stale, residency.LossReasonLeaseLost)

	if err := f.svc.warm.Watch(warmSession{resident: successor}); !errors.Is(err, residency.ErrWarmSessionWatched) {
		t.Fatalf("re-watching the successor's key = %v, want ErrWarmSessionWatched: a stale lost release forgot the successor's warm watch", err)
	}
	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	var refused *residency.AttachError
	if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorNoCapacity {
		t.Fatalf("attach into a Host whose one slot the successor holds = %v, want no_capacity: a stale lost release credited the slot back", err)
	}
	if f.svc.residentFor(keyA) != successor {
		t.Error("a stale lost release removed the successor's residency")
	}
}

// busyReleaseSession is a runtime with no abandon capability whose nonterminal
// release waits, as harness's does, for a whole-session idle that never comes.
type busyReleaseSession struct {
	*publishingSession
}

func (busyReleaseSession) ReleaseResidency(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestALostResidencysReleaseIsBounded is gate S1: a runtime that never goes
// idle must not hold the registry entry — F5's whole subject — for as long as
// its turn runs.
func TestALostResidencysReleaseIsBounded(t *testing.T) {
	previous := lostReleaseBound
	lostReleaseBound = 50 * time.Millisecond
	t.Cleanup(func() { lostReleaseBound = previous })

	handler := newCapturingHandler()
	f := newFixture(t, withLogger(handler))
	f.rig.Session = busyReleaseSession{publishingSession: newPublishingSession()}
	f.start()
	f.attach(tenantA, sessionA)

	f.store.loseLease(keyA)
	awaitForgotten(t, f)
	if _, resident := f.svc.manager.Observe(keyA); resident {
		t.Fatal("the registry still holds the lost residency after the bounded release gave up")
	}
	var reported bool
	for _, record := range handler.snapshot() {
		if record.level >= slog.LevelWarn && strings.Contains(record.message, "runtime did not release") {
			reported = true
		}
	}
	if !reported {
		t.Fatal("a runtime that did not release was not reported")
	}
}
