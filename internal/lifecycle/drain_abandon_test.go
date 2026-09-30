package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/registry"
)

const stepAbandonResidency step = "abandon_residency"

// abandoningSession is a fakeSession whose runtime can be given up
// crash-equivalently when its nonterminal release is refused.
type abandoningSession struct {
	*fakeSession

	err error
	// deaf makes AbandonResidency ignore cancellation until unblock closes.
	deaf    bool
	unblock chan struct{}

	mu        sync.Mutex
	abandoned int
	ctxErr    error
}

func (s *abandoningSession) AbandonResidency(ctx context.Context) error {
	s.mu.Lock()
	s.abandoned++
	s.ctxErr = ctx.Err()
	s.mu.Unlock()
	if err := s.run(stepAbandonResidency); err != nil {
		return err
	}
	if s.deaf {
		<-s.unblock
		return nil
	}
	return s.err
}

func (s *abandoningSession) seen() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abandoned, s.ctxErr
}

var _ lifecycle.Abandoner = (*abandoningSession)(nil)

// drainToEnd fires every timer until the drain finishes, for a drain that arms
// bounds the test does not want to pick apart.
func drainToEnd(t *testing.T, f *fixture) lifecycle.Report {
	t.Helper()
	finished := make(chan lifecycle.Report, 1)
	go func() { finished <- f.drainer.Wait() }()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case report := <-finished:
			return report
		case <-deadline:
			t.Fatal("the drain did not finish")
		case <-time.After(time.Millisecond):
			f.clock.fireAll()
		}
	}
}

// A RUNTIME THAT REFUSES ITS NONTERMINAL RELEASE IS ABANDONED, NOT PARKED, when
// it can be: after the refusal and BEFORE FinishRelease hands the residency
// back, under a context the (spent) grace does not reach. The report names it.
func TestARefusedReleaseIsAbandonedBeforeTheResidencyIsHandedBack(t *testing.T) {
	t.Parallel()
	gated := &abandoningSession{fakeSession: newSession("session-gated", nil)}
	gated.gated = true
	gated.hang = true
	f := newFixture(t, gated.fakeSession)
	f.residents.sessions = []lifecycle.Session{gated}

	mustStart(t, f.drainer)
	report := drainToEnd(t, f)

	abandoned, ctxErr := gated.seen()
	if abandoned != 1 {
		t.Fatalf("abandoned %d times, want once", abandoned)
	}
	if ctxErr != nil {
		t.Errorf("the abandon ran under a done context (%v); the grace was spent on the refused release", ctxErr)
	}
	steps := gated.stepsTaken()
	release, abandon, finish := slices.Index(steps, stepReleaseResidency), slices.Index(steps, stepAbandonResidency), slices.Index(steps, stepFinishRelease)
	if release < 0 || abandon < release || finish < abandon {
		t.Fatalf("steps = %v, want release_residency, then abandon_residency, then finish_release", steps)
	}
	if !slices.Equal(report.Abandoned, []registry.Key{gated.Key()}) || len(report.Parked) != 0 {
		t.Fatalf("Abandoned = %v, Parked = %v, want the session abandoned and nothing parked", report.Abandoned, report.Parked)
	}
	for _, failure := range report.Failures {
		if failure.Step == lifecycle.StepAbandonResidency {
			t.Fatalf("a successful abandon was reported as a failure: %v", failure)
		}
	}
}

func TestARefusedReleaseThatCannotBeAbandonedIsReportedParked(t *testing.T) {
	t.Parallel()
	refusal := errors.New("not idle")

	t.Run("no Abandoner", func(t *testing.T) {
		t.Parallel()
		plain := newSession("session-plain", nil)
		plain.errs[stepReleaseResidency] = refusal
		f := newFixture(t, plain)
		mustStart(t, f.drainer)
		report := awaitDrained(t, f.drainer)
		if !slices.Equal(report.Parked, []registry.Key{plain.Key()}) || len(report.Abandoned) != 0 {
			t.Fatalf("Parked = %v, Abandoned = %v, want the session parked", report.Parked, report.Abandoned)
		}
	})

	t.Run("the runtime offers no crash-equivalent release", func(t *testing.T) {
		t.Parallel()
		session := &abandoningSession{fakeSession: newSession("session-unable", nil), err: lifecycle.ErrNotAbandonable}
		session.errs[stepReleaseResidency] = refusal
		f := newFixture(t, session.fakeSession)
		f.residents.sessions = []lifecycle.Session{session}
		mustStart(t, f.drainer)
		report := awaitDrained(t, f.drainer)
		if !slices.Equal(report.Parked, []registry.Key{session.Key()}) {
			t.Fatalf("Parked = %v, want the session", report.Parked)
		}
		for _, failure := range report.Failures {
			if failure.Step == lifecycle.StepAbandonResidency {
				t.Errorf("an unavailable abandon was reported as a failure: %v", failure)
			}
		}
	})

	t.Run("the abandon fails", func(t *testing.T) {
		t.Parallel()
		failure := errors.New("lease store down")
		session := &abandoningSession{fakeSession: newSession("session-failing", nil), err: failure}
		session.errs[stepReleaseResidency] = refusal
		f := newFixture(t, session.fakeSession)
		f.residents.sessions = []lifecycle.Session{session}
		mustStart(t, f.drainer)
		report := awaitDrained(t, f.drainer)
		if !slices.Equal(report.Parked, []registry.Key{session.Key()}) {
			t.Fatalf("Parked = %v, want the session", report.Parked)
		}
		found := false
		for _, recorded := range report.Failures {
			if recorded.Step == lifecycle.StepAbandonResidency && errors.Is(recorded, failure) {
				found = true
			}
		}
		if !found {
			t.Fatalf("failures = %v, want the abandon failure recorded", report.Failures)
		}
		if !slices.Contains(session.stepsTaken(), stepFinishRelease) {
			t.Fatal("a failed abandon stopped the drain before FinishRelease")
		}
	})

	t.Run("the abandon ignores its bound", func(t *testing.T) {
		t.Parallel()
		session := &abandoningSession{fakeSession: newSession("session-deaf", nil), deaf: true, unblock: make(chan struct{})}
		t.Cleanup(func() { close(session.unblock) })
		session.errs[stepReleaseResidency] = refusal
		f := newFixture(t, session.fakeSession)
		f.residents.sessions = []lifecycle.Session{session}
		mustStart(t, f.drainer)
		report := drainToEnd(t, f)
		if !slices.Equal(report.Parked, []registry.Key{session.Key()}) {
			t.Fatalf("Parked = %v, want the session", report.Parked)
		}
		found := false
		for _, recorded := range report.Failures {
			if recorded.Step == lifecycle.StepAbandonResidency && errors.Is(recorded, lifecycle.ErrWaitAbandoned) {
				found = true
			}
		}
		if !found {
			t.Fatalf("failures = %v, want the bounded abandon recorded", report.Failures)
		}
	})
}

func TestAReleasedRuntimeIsNeverAbandoned(t *testing.T) {
	t.Parallel()
	session := &abandoningSession{fakeSession: newSession("session-idle", nil)}
	f := newFixture(t, session.fakeSession)
	f.residents.sessions = []lifecycle.Session{session}
	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if abandoned, _ := session.seen(); abandoned != 0 {
		t.Fatalf("a runtime that released was abandoned %d times", abandoned)
	}
	if len(report.Abandoned) != 0 || len(report.Parked) != 0 {
		t.Fatalf("Abandoned = %v, Parked = %v, want neither", report.Abandoned, report.Parked)
	}
}

// A RELEASE OR ABANDON THAT REPORTS THE RESIDENCY STILL HELD (harness v0.45.0's
// LeaseReleaseError, in Host's vocabulary) is not a hand-off: the session is
// Parked, it is not abandoned on top of a torn-down runtime, and the drain is
// NOT `drained`.
func TestAResidencyStillHeldIsParkedAndWithholdsDrained(t *testing.T) {
	t.Parallel()
	held := fmt.Errorf("journal lease release failed: %w", lifecycle.ErrResidencyStillHeld)

	t.Run("on the graceful release", func(t *testing.T) {
		t.Parallel()
		session := &abandoningSession{fakeSession: newSession("session-release-held", nil)}
		session.errs[stepReleaseResidency] = held
		f := newFixture(t, session.fakeSession)
		f.residents.sessions = []lifecycle.Session{session}
		mustStart(t, f.drainer)
		report := f.drainer.Wait()
		if abandoned, _ := session.seen(); abandoned != 0 {
			t.Errorf("a runtime whose release reported its lease held was abandoned %d times", abandoned)
		}
		if !slices.Equal(report.Parked, []registry.Key{session.Key()}) || len(report.Abandoned) != 0 {
			t.Fatalf("Parked = %v, Abandoned = %v, want the session parked", report.Parked, report.Abandoned)
		}
		if report.State != sessionwire.HostLinkDrainStateDraining {
			t.Fatalf("state = %q, want draining: the residency is still held", report.State)
		}
	})

	t.Run("on the abandon", func(t *testing.T) {
		t.Parallel()
		session := &abandoningSession{fakeSession: newSession("session-abandon-held", nil), err: held}
		session.errs[stepReleaseResidency] = errors.New("not idle")
		f := newFixture(t, session.fakeSession)
		f.residents.sessions = []lifecycle.Session{session}
		mustStart(t, f.drainer)
		report := f.drainer.Wait()
		if !slices.Equal(report.Parked, []registry.Key{session.Key()}) {
			t.Fatalf("Parked = %v, want the session", report.Parked)
		}
		if report.State != sessionwire.HostLinkDrainStateDraining {
			t.Fatalf("state = %q, want draining: the residency is still held", report.State)
		}
	})
}
