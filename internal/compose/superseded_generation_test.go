package compose

import (
	"errors"
	"testing"

	"github.com/looprig/host/internal/service"
)

// errDirectoryDown is an ordinary, retryable directory failure.
var errDirectoryDown = errors.New("injected: the target directory is unreachable")

// TestADrainPastASupersededTargetRowStillReleasesTheSession is the unit half of
// the v0.3.0 same-HostID overlap fix: a withdrawal refused because a NEWER
// generation of this HostID owns the row is not this Host's failure, so the
// drain proceeds and hands the residency lease back.
func TestADrainPastASupersededTargetRowStillReleasesTheSession(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.directory.mu.Lock()
	f.directory.withdrawErr = errors.Join(service.ErrTargetGenerationSuperseded, errDirectoryDown)
	f.directory.mu.Unlock()

	if _, err := f.svc.Stop(t.Context()); err != nil {
		t.Fatalf("Stop past a superseded target row = %v, want the drain to proceed", err)
	}
	if released := f.trace.count("lease.release"); released != 1 {
		t.Fatalf("the session lease was released %d times, want 1", released)
	}
}

// TestADrainPastAnOrdinaryDirectoryFailureStillRefuses is the control: only the
// superseded-generation refusal is skipped. Any other failure to withdraw is a
// refused publication, and the drain does not begin — nothing downstream knows
// it started, so releasing sessions would release work a Factory still routes.
func TestADrainPastAnOrdinaryDirectoryFailureStillRefuses(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.directory.mu.Lock()
	f.directory.withdrawErr = errDirectoryDown
	f.directory.mu.Unlock()

	if _, err := f.svc.Stop(t.Context()); !errors.Is(err, errDirectoryDown) {
		t.Fatalf("Stop past an ordinary directory failure = %v, want the refusal", err)
	}
	if released := f.trace.count("lease.release"); released != 0 {
		t.Fatalf("the session lease was released %d times after a refused drain publication, want 0", released)
	}
}

// TestAHeartbeatPublicationStillReportsASupersededRow: the skip belongs to the
// drain's nonaccepting publication alone. The ordinary publication — Start's
// first write, and every heartbeat — still reports the refusal, because a Host
// whose rows a newer incarnation owns is not advertising anything.
func TestAHeartbeatPublicationStillReportsASupersededRow(t *testing.T) {
	f := newFixture(t)
	f.directory.mu.Lock()
	f.directory.publishErr = errors.Join(service.ErrTargetGenerationSuperseded, errDirectoryDown)
	f.directory.mu.Unlock()
	if err := f.svc.Start(t.Context()); !errors.Is(err, service.ErrTargetGenerationSuperseded) {
		t.Fatalf("Start with a superseded target row = %v, want the refusal reported", err)
	}
}
