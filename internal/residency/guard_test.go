package residency

import (
	"errors"
	"testing"
)

// TestTheOwnershipGuardIsTheAttachsOwnFence is the probe for the hand-off
// commands.Fence's own documentation asks for: the composition root must hand
// the command consumer THE SAME fence the attach built, not a second one over
// the same lease.
func TestTheOwnershipGuardIsTheAttachsOwnFence(t *testing.T) {
	t.Parallel()

	lease := &fakeLease{trace: &trace{}, epoch: 7, lost: make(chan struct{})}
	fence := newEpochFence(lease)
	guard := OwnershipRequest{Fence: fence}.Guard()

	if err := guard.Held(); err != nil {
		t.Fatalf("Held() on a live grant = %v, want nil", err)
	}
	select {
	case <-guard.Lost():
		t.Fatal("Lost() is closed on a live grant")
	default:
	}

	// A supersession recorded through the EXPORTED write must be visible to
	// the UNEXPORTED reader the heartbeat uses. Two fences over one lease agree
	// on Lost() and disagree here, which is the divergence this test exists for.
	if err := guard.Write(func() error { return ErrEpochSuperseded }); !errors.Is(err, ErrEpochSuperseded) {
		t.Fatalf("Write returning a supersession = %v, want ErrEpochSuperseded", err)
	}
	if err := fence.held(); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("the attach's own fence reports held() = %v after the guard recorded a supersession, want ErrLeaseNotHeld", err)
	}
	if reason, ended := fence.endedBy(); !ended || reason != LossReasonEpochSuperseded {
		t.Fatalf("the attach's own fence recorded (%q, %v), want (%q, true)", reason, ended, LossReasonEpochSuperseded)
	}
	if err := guard.Held(); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("Held() after the supersession = %v, want ErrLeaseNotHeld", err)
	}
}

// TestAGuardOverNoFenceRefusesRatherThanPanics fixes the fail-closed reading of
// the zero value. BeginOwnership refuses a nil fence, so this shape is not
// reachable through the attach path; a method on an exported struct is reachable
// from anywhere, and the answer a guard with nothing to guard gives must be
// "ownership is gone" rather than a nil dereference on the first write.
func TestAGuardOverNoFenceRefusesRatherThanPanics(t *testing.T) {
	t.Parallel()

	guard := OwnershipRequest{}.Guard()

	if err := guard.Held(); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("Held() with no fence = %v, want ErrLeaseNotHeld", err)
	}
	ran := false
	if err := guard.Write(func() error { ran = true; return nil }); !errors.Is(err, ErrLeaseNotHeld) {
		t.Fatalf("Write with no fence = %v, want ErrLeaseNotHeld", err)
	}
	if ran {
		t.Fatal("Write with no fence ran the write")
	}
	select {
	case <-guard.Lost():
		t.Fatal("Lost() with no fence is closed; a closed channel would wake every select that guards a loss")
	default:
	}
}
