package residency

import (
	"context"
	"errors"
	"testing"
)

// TestCancelAttachesReachesOnlyAnUncommittedAttach pins the two halves of the
// drain's last resort. An attach the drain cancels before its commit point is
// told so by Commit and must not become resident; an attach that has committed
// is in the drain's resident set, and cancelling its session context would
// tear a live runtime down beneath the drain's own release.
func TestCancelAttachesReachesOnlyAnUncommittedAttach(t *testing.T) {
	t.Run("cancelled before commit", func(t *testing.T) {
		f := newFixture(t)
		f.ownership.honourCommit = true
		f.ownership.withRequest = func(OwnershipRequest) { f.manager.CancelAttaches() }
		_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
		if !errors.Is(err, errCommitRefused) {
			t.Fatalf("Attach = %v, want the ownership's refusal of an attach cancelled before it committed", err)
		}
		// AND IT ROLLED BACK: nothing it took is still held.
		if held := f.leases.heldCount(); held != 0 {
			t.Fatalf("%d session leases still held after the cancelled attach", held)
		}
		if _, resident := f.registry.Get(f.key()); resident {
			t.Fatal("the cancelled attach left a registry entry")
		}
		if records := f.manager.Records(); records != 0 {
			t.Fatalf("manager.Records() = %d after the cancelled attach, want 0", records)
		}
		if _, releases := f.admissions.counts(); releases == 0 {
			t.Fatal("the cancelled attach's admission charge was never released")
		}
	})
	t.Run("committed before cancel", func(t *testing.T) {
		f := newFixture(t)
		committed := false
		f.ownership.withRequest = func(request OwnershipRequest) {
			committed = request.Commit()
			f.manager.CancelAttaches()
		}
		held, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
		if err != nil || !held.Attached {
			t.Fatalf("Attach = (%+v, %v), want attached", held, err)
		}
		if !committed {
			t.Fatal("Commit answered false for an attach nothing had cancelled")
		}
		ctx, live := f.manager.SessionContext(f.key())
		if !live || ctx.Err() != nil {
			t.Fatalf("the committed session's context is (%v, live=%v): CancelAttaches cancelled a residency it had no claim on", ctx.Err(), live)
		}
	})
	t.Run("returned attaches are forgotten", func(t *testing.T) {
		f := newFixture(t)
		if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		f.manager.CancelAttaches()
		if ctx, _ := f.manager.SessionContext(f.key()); ctx == nil || ctx.Err() != nil {
			t.Fatal("CancelAttaches cancelled a residency whose attach had already returned")
		}
		f.manager.mu.Lock()
		pending := len(f.manager.pending)
		f.manager.mu.Unlock()
		if pending != 0 {
			t.Fatalf("%d attaches still pending after every attach returned", pending)
		}
	})
}
