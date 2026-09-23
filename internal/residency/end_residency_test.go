package residency

import (
	"context"
	"fmt"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// endAsAReleaseDoes removes the registry entry as FinishRelease does, credits
// the charge, and hands the ended residency to the Manager.
func endAsAReleaseDoes(t *testing.T, f *fixture, held Residency, runtimeReleased bool) {
	t.Helper()
	if !f.registry.inner.RemoveByGeneration(held.Key, held.Generation) {
		t.Fatalf("the residency of %s was not in the registry", held.Key.SessionID)
	}
	f.publisher.ReleaseOwned(held.Key, held.Generation)
	if !f.manager.EndResidency(held.Key, held.Generation, runtimeReleased) {
		t.Fatalf("EndResidency(%s, %d) pruned nothing", held.Key.SessionID, held.Generation)
	}
}

// TestEveryEndedResidencyIsPrunedAndItsContextCancelled is booked finding B1:
// the Manager's session records were never pruned, so every session that left
// a long-lived pooled Host kept its record — an uncancelled session context
// and an ownership handle whose heartbeat reaches the runtime — and the Host
// grew by one per distinct session it ever held.
func TestEveryEndedResidencyIsPrunedAndItsContextCancelled(t *testing.T) {
	f := newFixture(t)
	baseline := f.manager.Records()
	var contexts []context.Context
	for i := range 50 {
		session := sessionwire.SessionID(fmt.Sprintf("session-pass-%02d", i))
		held, err := f.manager.Attach(context.Background(), f.requestFor(session, ModeCreate))
		if err != nil {
			t.Fatalf("Attach(%s): %v", session, err)
		}
		ctx, ok := f.manager.SessionContext(held.Key)
		if !ok {
			t.Fatalf("no session context for %s", session)
		}
		contexts = append(contexts, ctx)
		endAsAReleaseDoes(t, f, held, true)
	}
	if got := f.manager.Records(); got != baseline {
		t.Fatalf("the Manager holds %d records after 50 sessions passed through, want the baseline %d", got, baseline)
	}
	for i, ctx := range contexts {
		if ctx.Err() == nil {
			t.Fatalf("session %d's context was not cancelled when its residency ended", i)
		}
	}
}

// TestAParkedRuntimesContextIsNotCancelledWhenItsRecordIsPruned: a runtime
// whose release was refused (a drain of a session parked at a gate) is left
// PARKED, and cancelling its context would make it abandon the gate after the
// publisher stopped. Its record is pruned; its context is not cancelled.
func TestAParkedRuntimesContextIsNotCancelledWhenItsRecordIsPruned(t *testing.T) {
	f := newFixture(t)
	held, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	ctx, _ := f.manager.SessionContext(held.Key)
	endAsAReleaseDoes(t, f, held, false)
	if f.manager.Records() != 0 {
		t.Fatal("the parked residency's record was kept")
	}
	if ctx.Err() != nil {
		t.Fatal("a parked runtime's session context was cancelled")
	}
}

// TestAStaleEndLeavesTheSuccessorsRecordAlone: EndResidency is fenced by
// generation, so an old residency's late end cannot prune — or cancel — the
// residency that replaced it; and after an end the session attaches afresh.
func TestAStaleEndLeavesTheSuccessorsRecordAlone(t *testing.T) {
	f := newFixture(t)
	first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	endAsAReleaseDoes(t, f, first, true)
	second, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil || !second.Attached || second.Generation == first.Generation {
		t.Fatalf("re-attach after the end = (%+v, %v), want a fresh residency", second, err)
	}
	if f.manager.EndResidency(first.Key, first.Generation, true) {
		t.Fatal("a stale end pruned the successor's record")
	}
	ctx, ok := f.manager.SessionContext(second.Key)
	if !ok || ctx.Err() != nil {
		t.Fatal("a stale end removed or cancelled the successor's session")
	}
	again, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil || again.Attached || again.Generation != second.Generation {
		t.Fatalf("an attach of the resident successor = (%+v, %v), want the idempotent answer", again, err)
	}
}
