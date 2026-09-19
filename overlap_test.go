package host_test

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
)

// TestASupersededGenerationStillReleasesItsSessions is the v0.3.0 booking's
// reproduction, kept as the regression test.
//
// Host A (host-a, generation 4) holds a session. Before A stops, a newer
// incarnation of the SAME HostID (generation 5) starts and publishes its own
// target rows, raising their generation mark above A's. A's drain then cannot
// withdraw a row that is no longer its own. It used to abort there — Stop
// returned `host target generation` with an empty report — BEFORE releasing
// the session's residency lease, so the lease was never handed back and every
// later Host, of any generation, was refused with epoch_mismatch for as long
// as the lease lived.
//
// A row a newer generation owns is not this Host's to withdraw, so the drain
// proceeds, releases the session, and the newer incarnation (and anyone after
// it) can attach.
func TestASupersededGenerationStillReleasesItsSessions(t *testing.T) {
	world := newRealRuntimeWorld(t)

	older, _ := world.host(t, 4, nil)
	if _, err := attachAsFactoryDoes(t, older); err != nil {
		t.Fatalf("Host A (generation 4) attach: %v", err)
	}

	newer, newerLauncher := world.host(t, 5, nil)
	t.Cleanup(func() { stopBounded(newer) })

	report, err := stopReport(t, older)
	if err != nil {
		t.Fatalf("Host A's Stop after a newer generation of its HostID started = %v, want the drain to complete", err)
	}
	if report.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("Host A's drain state = %q with failures %v, want %q", report.State, report.Failures, sessionwire.HostLinkDrainStateDrained)
	}

	if _, err := attachAsFactoryDoes(t, newer); err != nil {
		t.Fatalf("the newer generation's attach after the older one stopped = %v, want the released lease to be acquirable", err)
	}
	if creates, restores := newerLauncher.counts(); creates+len(restores) != 1 {
		t.Fatalf("the newer generation launched creates=%d restores=%v, want exactly one launch", creates, restores)
	}
}

// stopReport drains a Host, bounded, and returns what Stop returned.
func stopReport(t *testing.T, service *host.Service) (host.DrainReport, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return service.Stop(ctx)
}
