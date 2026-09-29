package residency

import (
	"context"
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
		committed := true
		f.ownership.withRequest = func(request OwnershipRequest) {
			f.manager.CancelAttaches()
			committed = request.Commit()
		}
		_, _ = f.manager.Attach(context.Background(), f.request(ModeCreate))
		if committed {
			t.Fatal("Commit answered true for an attach CancelAttaches had already cancelled")
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
