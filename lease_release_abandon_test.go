package host_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/storage"

	"github.com/looprig/host"
)

// failingReleaseLeaser hands out the backend's own leases, but once armed a
// lease's Release fails WITHOUT releasing it, as a lease store that is down.
type failingReleaseLeaser struct {
	storage.Leaser
	armed *atomic.Bool
}

func (l failingReleaseLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	lease, err := l.Leaser.Acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	return failingReleaseLease{Lease: lease, armed: l.armed}, nil
}

type failingReleaseLease struct {
	storage.Lease
	armed *atomic.Bool
}

var errLeaseStoreDown = errors.New("lease store down")

func (l failingReleaseLease) Release(ctx context.Context) error {
	if l.armed.Load() {
		return errLeaseStoreDown
	}
	return l.Lease.Release(ctx)
}

// assertLeaseKept holds a Stop report to harness v0.45.0's consumer
// obligation: a runtime that could not release its journal lease leaves the
// residency held, so the session is Parked (never Abandoned), the failure at
// step carries the *session.LeaseReleaseError and its cause, and the drain is
// not `drained`.
func assertLeaseKept(t *testing.T, report host.DrainReport, step string) {
	t.Helper()
	want := []host.DrainSession{{TenantID: composeTenant, SessionID: composeSession}}
	if len(report.Abandoned) != 0 || !slices.Equal(report.Parked, want) {
		t.Fatalf("Abandoned = %+v, Parked = %+v, want the session parked", report.Abandoned, report.Parked)
	}
	found := false
	for _, failure := range report.Failures {
		var kept *session.LeaseReleaseError
		if failure.Step == step && errors.As(failure.Err, &kept) && errors.Is(failure.Err, errLeaseStoreDown) {
			found = true
		}
	}
	if !found {
		t.Fatalf("failures = %+v, want a %s failure carrying *session.LeaseReleaseError", report.Failures, step)
	}
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("state = %q, want draining: the residency is still held", report.State)
	}
}

// AN ABANDON WHOSE JOURNAL-LEASE RELEASE FAILED IS NOT AN ABANDONMENT. harness
// v0.44.0 logged and swallowed the failure, so Host reported the session
// Abandoned while its lease was held; v0.45.0 returns it.
func TestAnAbandonWhoseJournalLeaseReleaseFailedIsParked(t *testing.T) {
	armed := &atomic.Bool{}
	world := newGateE2EWorld(t, gateE2EOptions{
		replaySafeAsk: true,
		leaser:        func(inner storage.Leaser) storage.Leaser { return failingReleaseLeaser{Leaser: inner, armed: armed} },
	})
	first, launcher, _ := world.host(t, 4)
	world.submit(t, launcher.controller(), "PLEASE-ASK")
	world.gates(t, 1)
	armed.Store(true)

	report, err := stopReport(t, first)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertLeaseKept(t, report, "abandon_residency")
}

// AND ON THE NORMAL PATH: an idle session's graceful release that could not
// give its journal lease back is not a release either, and is not abandoned on
// top of the torn-down runtime.
func TestAGracefulReleaseWhoseJournalLeaseReleaseFailedIsParked(t *testing.T) {
	armed := &atomic.Bool{}
	world := newGateE2EWorld(t, gateE2EOptions{
		leaser: func(inner storage.Leaser) storage.Leaser { return failingReleaseLeaser{Leaser: inner, armed: armed} },
	})
	first, _, _ := world.host(t, 4)
	armed.Store(true)

	report, err := stopReport(t, first)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertLeaseKept(t, report, "release_residency")
	for _, failure := range report.Failures {
		if failure.Step == "abandon_residency" {
			t.Errorf("a runtime whose release kept its lease was abandoned too: %v", failure)
		}
	}
}
