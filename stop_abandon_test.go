package host_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/testkit"
)

// refusingAbandonSession is a runtime whose graceful release is refused, as
// harness refuses one for a session awaiting a gate, and whose crash-equivalent
// abandon is scripted: it may fail, or ignore its context until unblocked.
type refusingAbandonSession struct {
	*testkit.FullSession
	faulted    chan struct{}
	abandonErr error
	unblock    chan struct{} // nil: the abandon returns at once
	abandons   atomic.Int32
}

func (s *refusingAbandonSession) ReleaseResidency(context.Context) error {
	return errors.New("not whole-session idle: a gate is open")
}
func (s *refusingAbandonSession) PersistenceFaulted() <-chan struct{} { return s.faulted }
func (s *refusingAbandonSession) PersistenceFault() error             { return nil }
func (s *refusingAbandonSession) AbandonResidency(context.Context) error {
	s.abandons.Add(1)
	if s.unblock != nil {
		<-s.unblock // IGNORES ITS CONTEXT, as a teardown on a private context does
	}
	if s.abandonErr != nil {
		return s.abandonErr
	}
	s.Stop() // torn down: the runtime's liveness ends
	return nil
}

// composedStop attaches the fixture's session on a composed Host with short
// drain bounds, stops it, and returns the report and how long Stop took.
func composedStop(t *testing.T, runtime *refusingAbandonSession) (host.DrainReport, time.Duration) {
	t.Helper()
	f := newComposeFixture(t)
	runtime.FullSession = f.session
	f.rig.Session = runtime
	f.seedDispositionSession(t, f.otherParty(t), composeSession)
	blueprint := f.blueprint(t)
	blueprint.Drain = host.DrainOptions{Grace: time.Second, IdleBoundary: 200 * time.Millisecond, PublishBound: 500 * time.Millisecond}
	service, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	began := time.Now()
	stopped := make(chan host.DrainReport, 1)
	go func() {
		report, err := service.Stop(ctx)
		if err != nil {
			t.Errorf("Stop: %v", err)
		}
		stopped <- report
	}()
	select {
	case report := <-stopped:
		return report, time.Since(began)
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return: an abandon that overran its bound held the drain")
		return host.DrainReport{}, 0
	}
}

func hasFailure(report host.DrainReport, step string) bool {
	return slices.ContainsFunc(report.Failures, func(f host.DrainFailure) bool { return f.Step == step })
}

// AN ABANDON THAT IGNORES ITS BOUND DOES NOT HOLD STOP. The drain gives up on
// it after two idle boundaries, reports the session Parked, and the composed
// cleanup — FinishRelease included — must not wait for the abandon either.
func TestComposedStopReturnsWhenAnAbandonIgnoresItsBound(t *testing.T) {
	runtime := &refusingAbandonSession{faulted: make(chan struct{}), unblock: make(chan struct{})}
	t.Cleanup(func() { close(runtime.unblock) })
	report, took := composedStop(t, runtime)
	want := []host.DrainSession{{TenantID: composeTenant, SessionID: composeSession}}
	if !slices.Equal(report.Parked, want) || len(report.Abandoned) != 0 {
		t.Fatalf("Parked = %+v, Abandoned = %+v, want the session parked", report.Parked, report.Abandoned)
	}
	if !hasFailure(report, "abandon_residency") {
		t.Fatalf("failures = %+v, want the overrun abandon recorded", report.Failures)
	}
	if runtime.abandons.Load() != 1 {
		t.Errorf("abandoned %d times, want once", runtime.abandons.Load())
	}
	t.Logf("Stop returned in %v", took)
}

// AN ABANDON THAT FAILS IS NOT AN ABANDONMENT: nothing proves the journal
// lease was released, so the session is Parked with the failure recorded.
func TestComposedStopParksASessionWhoseAbandonFailed(t *testing.T) {
	failure := errors.New("journal lease release failed")
	runtime := &refusingAbandonSession{faulted: make(chan struct{}), abandonErr: failure}
	report, _ := composedStop(t, runtime)
	if len(report.Abandoned) != 0 || len(report.Parked) != 1 {
		t.Fatalf("Abandoned = %+v, Parked = %+v, want the session parked", report.Abandoned, report.Parked)
	}
	found := false
	for _, recorded := range report.Failures {
		if recorded.Step == "abandon_residency" && errors.Is(recorded.Err, failure) {
			found = true
		}
	}
	if !found {
		t.Fatalf("failures = %+v, want the abandon failure with its cause", report.Failures)
	}
}

// AND THE PLAIN CASE: an abandon that succeeds is reported Abandoned.
func TestComposedStopReportsASuccessfulAbandon(t *testing.T) {
	runtime := &refusingAbandonSession{faulted: make(chan struct{})}
	report, _ := composedStop(t, runtime)
	want := []host.DrainSession{{TenantID: composeTenant, SessionID: composeSession}}
	if !slices.Equal(report.Abandoned, want) || len(report.Parked) != 0 {
		t.Fatalf("Abandoned = %+v, Parked = %+v, want the session abandoned", report.Abandoned, report.Parked)
	}
}
