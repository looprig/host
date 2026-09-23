package residency

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestAResidencyWhoseTeardownIsClaimedIsNotReportedAsResident is the F5
// hardening: once Heartbeat.surrender has claimed the teardown (the grant is
// gone), an attach must be refused not_admitting — visibly and retryably —
// rather than answered with the dead residency as an idempotent success.
func TestAResidencyWhoseTeardownIsClaimedIsNotReportedAsResident(t *testing.T) {
	f := newFixture(t)
	first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// The control: before the claim, a repeated attach is the idempotent hit.
	if again, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil || again.Attached || again.Generation != first.Generation {
		t.Fatalf("repeated attach before any teardown = (%+v, %v), want the idempotent resident answer", again, err)
	}

	if _, won := f.registry.inner.BeginTeardown(f.key(), first.Generation); !won {
		t.Fatal("the teardown claim was not won")
	}
	_, err = f.manager.Attach(context.Background(), f.request(ModeCreate))
	var refused *AttachError
	if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorNotAdmitting || refused.Step != StepValidate {
		t.Fatalf("attach of a residency whose teardown is claimed = %v, want not_admitting at validate", err)
	}
}
