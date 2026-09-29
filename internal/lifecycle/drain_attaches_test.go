package lifecycle_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/registry"
)

// fakeAttaches is the Attaches capability: attaches in flight, keyed by
// session, each finishing when its channel closes.
type fakeAttaches struct {
	mu             sync.Mutex
	inflight       map[registry.Key]chan struct{}
	cancels        int
	finishOnCancel bool
	sessionWaits   []registry.Key
}

func newAttaches(keys ...registry.Key) *fakeAttaches {
	a := &fakeAttaches{inflight: map[registry.Key]chan struct{}{}}
	for _, key := range keys {
		a.inflight[key] = make(chan struct{})
	}
	return a
}

func (a *fakeAttaches) await(ctx context.Context, selected func(registry.Key) bool) error {
	a.mu.Lock()
	var waiting []chan struct{}
	for key, done := range a.inflight {
		if selected(key) {
			waiting = append(waiting, done)
		}
	}
	a.mu.Unlock()
	for _, done := range waiting {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (a *fakeAttaches) AwaitAttaches(ctx context.Context) error {
	return a.await(ctx, func(registry.Key) bool { return true })
}

func (a *fakeAttaches) AwaitSessionAttaches(ctx context.Context, key registry.Key) error {
	a.mu.Lock()
	a.sessionWaits = append(a.sessionWaits, key)
	a.mu.Unlock()
	return a.await(ctx, func(attaching registry.Key) bool { return attaching == key })
}

func (a *fakeAttaches) CancelAttaches() {
	a.mu.Lock()
	a.cancels++
	finish := a.finishOnCancel
	a.mu.Unlock()
	if finish {
		a.finishAll()
	}
}

func (a *fakeAttaches) finish(key registry.Key) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if done, ok := a.inflight[key]; ok {
		close(done)
		delete(a.inflight, key)
	}
}

func (a *fakeAttaches) finishAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, done := range a.inflight {
		close(done)
		delete(a.inflight, key)
	}
}

func (a *fakeAttaches) cancelCount() int { a.mu.Lock(); defer a.mu.Unlock(); return a.cancels }

func (a *fakeAttaches) sessionWaitCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.sessionWaits)
}

// attachingResidents is a Residents that also offers Attaches.
type attachingResidents struct {
	*residents
	*fakeAttaches
}

// endedSession is a fakeSession whose residency ended on its own.
type endedSession struct {
	*fakeSession
}

func (endedSession) Ended() bool { return true }

func newAttachDrainer(t *testing.T, idle time.Duration, attaches *fakeAttaches, sessions ...lifecycle.Session) (*lifecycle.Drainer, *fakeClock) {
	t.Helper()
	clock := &fakeClock{}
	admissions := &fakeLedger{}
	index := &residents{}
	for _, session := range sessions {
		index.add(session)
	}
	drainer, err := lifecycle.NewDrainer(lifecycle.Options{
		Generation:   testGeneration,
		Clock:        clock,
		Admissions:   admissions,
		Advertiser:   &advertiser{ledger: admissions},
		Residents:    attachingResidents{residents: index, fakeAttaches: attaches},
		Grace:        testGrace,
		IdleBoundary: idle,
		PublishBound: testPublishBound,
	})
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	return drainer, clock
}

func waitReport(drainer *lifecycle.Drainer) <-chan lifecycle.Report {
	reported := make(chan lifecycle.Report, 1)
	go func() { reported <- drainer.Wait() }()
	return reported
}

// TestTheDrainWaitsForAnAttachInFlightAndDoesNotCancelIt: an attach admitted
// before the drain began holds `drained` until it finishes, and one that
// finishes within its patience is never cancelled — its ordinary rollback is
// the path that gives everything back.
func TestTheDrainWaitsForAnAttachInFlightAndDoesNotCancelIt(t *testing.T) {
	key := registry.Key{TenantID: testTenant, SessionID: "attaching"}
	attaches := newAttaches(key)
	drainer, clock := newAttachDrainer(t, testIdleGrace, attaches)
	mustStart(t, drainer)
	reported := waitReport(drainer)

	waitFor(t, "the drain to arm its cancellation", func() bool { return clock.pendingFor(testGrace-testIdleGrace) == 1 })
	select {
	case report := <-reported:
		t.Fatalf("the drain finished (%+v) with an attach in flight", report)
	case <-time.After(50 * time.Millisecond):
	}
	attaches.finish(key)
	report := <-reported
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("report = %+v, want drained with no failures", report)
	}
	if cancels := attaches.cancelCount(); cancels != 0 {
		t.Fatalf("CancelAttaches ran %d times for an attach that finished on its own", cancels)
	}
}

// TestAnAttachInFlightIsCancelledOneIdleBoundaryBeforeTheGrace pins the
// cancellation's timing: not at once, and not at the grace — one idle boundary
// before it, so the rollback it forces also fits inside the platform grace.
func TestAnAttachInFlightIsCancelledOneIdleBoundaryBeforeTheGrace(t *testing.T) {
	attaches := newAttaches(registry.Key{TenantID: testTenant, SessionID: "attaching"})
	attaches.finishOnCancel = true
	drainer, clock := newAttachDrainer(t, testIdleGrace, attaches)
	mustStart(t, drainer)
	reported := waitReport(drainer)

	cancelAt := testGrace - testIdleGrace
	waitFor(t, "the drain to arm its cancellation", func() bool { return clock.pendingFor(cancelAt) == 1 })
	if cancels := attaches.cancelCount(); cancels != 0 {
		t.Fatalf("CancelAttaches ran %d times before its bound", cancels)
	}
	clock.fireFor(cancelAt)
	report := <-reported
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("report = %+v, want drained with no failures", report)
	}
	if cancels := attaches.cancelCount(); cancels != 1 {
		t.Fatalf("CancelAttaches ran %d times, want once", cancels)
	}
}

// TestTheCancellationIsFlooredAtHalfTheGrace: an idle boundary that is most of
// the grace would leave an attach almost no time of its own before it is
// cancelled.
func TestTheCancellationIsFlooredAtHalfTheGrace(t *testing.T) {
	attaches := newAttaches(registry.Key{TenantID: testTenant, SessionID: "attaching"})
	attaches.finishOnCancel = true
	drainer, clock := newAttachDrainer(t, testGrace, attaches)
	mustStart(t, drainer)
	reported := waitReport(drainer)

	waitFor(t, "the drain to arm its floored cancellation", func() bool { return clock.pendingFor(testGrace/2) == 1 })
	clock.fireFor(testGrace / 2)
	if report := <-reported; report.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("report = %+v, want drained", report)
	}
}

// TestAnAttachThatOutlivesTheGraceWithholdsDrained: nothing the drain can do
// ends it, so it is abandoned at the grace — never after it — and the drain may
// not claim release finished.
func TestAnAttachThatOutlivesTheGraceWithholdsDrained(t *testing.T) {
	attaches := newAttaches(registry.Key{TenantID: testTenant, SessionID: "attaching"})
	drainer, clock := newAttachDrainer(t, testIdleGrace, attaches)
	mustStart(t, drainer)
	reported := waitReport(drainer)

	cancelAt := testGrace - testIdleGrace
	waitFor(t, "the drain to arm its cancellation", func() bool { return clock.pendingFor(cancelAt) == 1 })
	clock.fireFor(cancelAt)
	waitFor(t, "the cancellation", func() bool { return attaches.cancelCount() == 1 })
	waitFor(t, "the drain to arm its grace", func() bool { return clock.pendingFor(testGrace) == 1 })
	clock.fireFor(testGrace)
	report := <-reported
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("state = %q with an attach abandoned, want draining", report.State)
	}
	if len(report.Failures) != 1 || report.Failures[0].Step != lifecycle.StepSettleAttaches || !errors.Is(report.Failures[0].Err, lifecycle.ErrWaitAbandoned) {
		t.Fatalf("failures = %v, want one abandoned settle_attaches", report.Failures)
	}
	attaches.finishAll()
}

// TestASessionIsReleasedOnlyAfterItsOwnAttachFinishes is review advisory A1 at
// this layer: a session committed into the snapshot while its attach was still
// finishing is not touched until that attach returns.
func TestASessionIsReleasedOnlyAfterItsOwnAttachFinishes(t *testing.T) {
	shared := &journal{}
	session := newSession("committing", shared)
	attaches := newAttaches(session.Key())
	drainer, _ := newAttachDrainer(t, testIdleGrace, attaches, session)
	mustStart(t, drainer)
	reported := waitReport(drainer)

	waitFor(t, "the drain to wait for the session's attach", func() bool { return attaches.sessionWaitCount() == 1 })
	if steps := session.stepsTaken(); len(steps) != 0 {
		t.Fatalf("the drain ran %v on a session whose attach had not finished", steps)
	}
	attaches.finish(session.Key())
	report := <-reported
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("report = %+v, want drained with no failures", report)
	}
	if steps := session.stepsTaken(); len(steps) == 0 || steps[len(steps)-1] != stepFinishRelease {
		t.Fatalf("steps = %v, want the full release after the attach finished", steps)
	}
}

// TestASessionWhoseAttachRolledBackIsNotReleasedAgain: the attach the drain
// waited for failed and gave everything back itself, so a second release would
// only book failures against a generation that no longer exists.
func TestASessionWhoseAttachRolledBackIsNotReleasedAgain(t *testing.T) {
	shared := &journal{}
	session := newSession("rolled-back", shared)
	attaches := newAttaches(session.Key())
	drainer, _ := newAttachDrainer(t, testIdleGrace, attaches, endedSession{session})
	mustStart(t, drainer)
	reported := waitReport(drainer)

	waitFor(t, "the drain to wait for the session's attach", func() bool { return attaches.sessionWaitCount() == 1 })
	attaches.finish(session.Key())
	report := <-reported
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("report = %+v, want drained with no failures", report)
	}
	if steps := session.stepsTaken(); len(steps) != 0 {
		t.Fatalf("the drain ran %v on a residency that had already ended", steps)
	}
}
