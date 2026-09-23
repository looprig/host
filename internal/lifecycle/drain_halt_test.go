package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/looprig/host/internal/lifecycle"
)

// haltingSession is a fakeSession that also offers the optional consumption
// halt, recording it in the shared journal so its order against the other
// steps is asserted rather than assumed.
type haltingSession struct {
	*fakeSession

	// blockHalt makes HaltConsumption wait for its context, as a consumer with
	// a pass in flight does.
	blockHalt bool
	haltErr   error
}

// HaltConsumption records the halt and, when blockHalt is set, waits for
// cancellation.
func (s *haltingSession) HaltConsumption(ctx context.Context) error {
	if err := s.run(stepHaltConsumption); err != nil {
		return err
	}
	if s.blockHalt {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.haltErr
}

// stepHaltConsumption is the journal name of the optional halt step.
const stepHaltConsumption step = "halt_consumption"

// epochSession reports the residency epoch it is held under.
type epochSession struct {
	*fakeSession
	epoch uint64
}

// ResidencyEpoch reports the epoch this session was built with.
func (s *epochSession) ResidencyEpoch() uint64 { return s.epoch }

// TestADrainHaltsConsumptionBeforeItMarksReleasingWaitsOrCheckpoints is the
// I2.3 finding-1 fix at the drain's own layer: a draining session's command
// consumer is halted as the FIRST thing the drain does to it — the warm
// release's step 0 — so a command admitted while the drain waits for idle or
// checkpoints stays pending for the successor.
func TestADrainHaltsConsumptionBeforeItMarksReleasingWaitsOrCheckpoints(t *testing.T) {
	t.Parallel()
	alpha := &haltingSession{fakeSession: newSession("session-alpha", nil)}
	f := newFixture(t)
	alpha.journal = f.journal
	f.residents.add(alpha)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 0 {
		t.Fatalf("failures = %v, want none", report.Failures)
	}
	halt := f.journal.indexOf("session-alpha:" + string(stepHaltConsumption))
	if halt < 0 {
		t.Fatalf("the drain never halted the session's consumption: %v", f.journal.recorded())
	}
	for _, later := range []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease} {
		at := f.journal.indexOf("session-alpha:" + string(later))
		if at < 0 || at < halt {
			t.Fatalf("%s ran at %d and the halt at %d: %v", later, at, halt, f.journal.recorded())
		}
	}
}

// TestAHaltThatDoesNotFinishIsBoundedAndRecorded: a pass in flight that never
// finishes costs the idle boundary, is recorded under its own step, and does
// not stop the release.
func TestAHaltThatDoesNotFinishIsBoundedAndRecorded(t *testing.T) {
	t.Parallel()
	alpha := &haltingSession{fakeSession: newSession("session-alpha", nil), blockHalt: true}
	f := newFixture(t)
	alpha.journal = f.journal
	f.residents.add(alpha)

	mustStart(t, f.drainer)
	waitFor(t, "the halt to wait on the idle boundary", func() bool { return f.clock.pendingFor(testIdleGrace) == 1 })
	f.clock.fireFor(testIdleGrace)
	report := awaitDrained(t, f.drainer)

	if !slices.Contains(alpha.stepsTaken(), stepFinishRelease) {
		t.Fatalf("a halt that did not finish stopped the release: %v", alpha.stepsTaken())
	}
	var halt *lifecycle.Failure
	for i := range report.Failures {
		if report.Failures[i].Step == lifecycle.StepHaltConsumption {
			halt = &report.Failures[i]
		}
	}
	if halt == nil {
		t.Fatalf("failures = %v, want a %s failure", report.Failures, lifecycle.StepHaltConsumption)
	}
	if !errors.Is(halt.Err, lifecycle.ErrIdleBoundary) {
		t.Fatalf("the halt failure %v does not say the boundary ended it", halt.Err)
	}
}

// TestEveryDrainFailureCarriesTheSessionsLeaseEpoch is the I2.3 spec-case-4
// gap: a forced release names the session, the step and the Host generation,
// and now the residency epoch the session was held under.
func TestEveryDrainFailureCarriesTheSessionsLeaseEpoch(t *testing.T) {
	t.Parallel()
	alpha := &epochSession{fakeSession: newSession("session-alpha", nil), epoch: 7}
	refused := errors.New("the runtime refused to release residency")
	alpha.errs[stepReleaseResidency] = refused
	alpha.errs[stepCheckpoint] = errors.New("checkpoint refused")
	plain := newSession("session-beta", nil)
	plain.errs[stepCheckpoint] = errors.New("checkpoint refused")
	f := newFixture(t, plain)
	alpha.journal = f.journal
	f.residents.add(alpha)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 3 {
		t.Fatalf("failures = %v, want three", report.Failures)
	}
	for _, failure := range report.Failures {
		want := uint64(0)
		if failure.Key == alpha.key {
			want = 7
		}
		if failure.ResidencyEpoch != want {
			t.Errorf("failure %v carries lease epoch %d, want %d", failure, failure.ResidencyEpoch, want)
		}
	}
}
