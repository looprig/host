package compose

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// keyB is the session the drain/attach race tests attach late.
var keyB = registry.Key{TenantID: tenantA, SessionID: sessionB}

// parkPoint is where a late attach is held: every one is past admission and
// holds a residency grant.
type parkPoint string

const (
	// parkHydrate holds the durable read, before the pre-launch drain check.
	parkHydrate parkPoint = "hydrate"
	// parkLaunch holds the runtime launch itself — the tests lane's repro
	// shape: the runtime exists and holds its journal lease.
	parkLaunch parkPoint = "launch"
	// parkOwnership holds the ownership start after its early drain check and
	// before the commit point, so only the commit point's own check can refuse.
	parkOwnership parkPoint = "ownership"
)

// parkAt arms one park point for the next attach of sessionB.
func parkAt(f *fixture, point parkPoint, honourCancel bool) (entered <-chan struct{}, release func()) {
	parked := make(chan struct{})
	released := make(chan struct{})
	var once, open sync.Once
	hold := func(ctx context.Context) error {
		once.Do(func() { close(parked) })
		if honourCancel {
			select {
			case <-released:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		<-released
		return nil
	}
	switch point {
	case parkHydrate:
		f.store.mu.Lock()
		f.store.loadHold = func(ctx context.Context, asked sessionwire.SessionID) error {
			if asked != sessionB {
				return nil
			}
			return hold(ctx)
		}
		f.store.mu.Unlock()
	case parkLaunch:
		f.rig.LaunchHook = hold
	case parkOwnership:
		writers := f.svc.options.Writers.(*fakeDispositionWriters)
		writers.mu.Lock()
		writers.hook = func() { _ = hold(context.Background()) }
		writers.mu.Unlock()
	}
	return parked, func() { open.Do(func() { close(released) }) }
}

// attachLate starts an attach of sessionB on its own runtime and returns its
// answer channel.
func attachLate(t *testing.T, f *fixture) (*controllableSession, <-chan error) {
	t.Helper()
	late := newControllableSession(testRigSessionID)
	f.rig.Session = late
	answered := make(chan error, 1)
	go func() {
		_, err := f.svc.Attach(context.Background(), residency.Request{
			TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
			Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-late"},
		})
		answered <- err
	}()
	return late, answered
}

func await[T any](t *testing.T, what string, from <-chan T) T {
	t.Helper()
	select {
	case got := <-from:
		return got
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// assertLateAttachGaveEverythingBack is the claim a successor needs: nothing on
// this Host still holds the late session — not its residency grant, not its
// runtime, not its registry entry, not its admission charge.
func assertLateAttachGaveEverythingBack(t *testing.T, f *fixture, late *controllableSession, attachErr error, wantCode sessionwire.HostLinkErrorCode) {
	t.Helper()
	var refused *residency.AttachError
	if !errors.As(attachErr, &refused) || refused.Code != wantCode {
		t.Fatalf("the late attach answered %v, want a %q refusal: a draining Host must not report a session attached that its drain will never release", attachErr, wantCode)
	}
	if lease := f.store.leaseOf(keyB); lease == nil || !lease.released.Load() {
		t.Fatalf("the late session's residency grant (%v) was never released", lease)
	}
	if launched(f) && late.Released() == 0 {
		t.Fatal("the late session's runtime was launched and never released its residency, so its journal lease is still held and a successor's hydrate is refused")
	}
	if _, held := f.svc.registry.Get(keyB); held {
		t.Fatal("the late session is still in the registry")
	}
	f.svc.mu.Lock()
	_, resident := f.svc.sessions[keyB]
	f.svc.mu.Unlock()
	if resident {
		t.Fatal("the late session is still tracked as resident")
	}
}

// launched reports whether the rig launched a runtime for sessionB.
func launched(f *fixture) bool {
	for _, create := range f.rig.Creates() {
		if create.SessionID == sessionB {
			return true
		}
	}
	return false
}

// cancelAt is when the drain gives up waiting and cancels: one idle boundary
// before the grace (see lifecycle.Drainer.settleAttaches).
func cancelAt(f *fixture) time.Duration { return f.svc.options.Grace - f.svc.options.IdleBoundary }

// TestAnAttachInFlightWhenTheDrainReadsItsSessionsIsGivenBackBeforeStopReturns
// is the tests lane's C1 wedge, reduced to this package, at every point an
// attach can be parked past admission.
//
// An attach passes admission and takes its lease; THEN the Host stops, and its
// drain reads which sessions to release and parks on another session's
// checkpoint; THEN the attach finishes. Before the fix the attach reported
// success on a draining Host, the drain never released it, and Stop returned
// `drained` while the late session's runtime still held its journal lease — so
// every successor's hydrate was refused for the life of the process.
//
// Each park point is refused by a different check — the pre-launch refusal, the
// ownership start's early refusal, and the commit point's authoritative one —
// and every one must give back what it took.
func TestAnAttachInFlightWhenTheDrainReadsItsSessionsIsGivenBackBeforeStopReturns(t *testing.T) {
	for _, point := range []parkPoint{parkHydrate, parkLaunch, parkOwnership} {
		t.Run(string(point), func(t *testing.T) {
			checkpoints := newHoldingCheckpointer()
			t.Cleanup(checkpoints.open)
			f := newFixture(t, func(o *Options, _ *hostconfig.Options) { o.Checkpointer = checkpoints })
			f.start()
			f.attach(tenantA, sessionA)
			chargedBefore := f.svc.capacity.ConsumedWeight()
			writers := f.svc.options.Writers.(*fakeDispositionWriters)
			writerCalls := func() int { writers.mu.Lock(); defer writers.mu.Unlock(); return writers.calls }
			ownershipBefore := writerCalls()

			parked, release := parkAt(f, point, false)
			t.Cleanup(release)
			late, attached := attachLate(t, f)
			await(t, "the late attach to park past admission", parked)

			stopped := make(chan lifecycle.Report, 1)
			go func() {
				report, err := f.svc.Stop(context.Background())
				if err != nil {
					t.Errorf("Stop: %v", err)
				}
				stopped <- report
			}()
			await(t, "the drain to reach the resident session's checkpoint", checkpoints.entered)

			release()
			attachErr := await(t, "the late attach to answer", attached)
			checkpoints.open()
			report := await(t, "Stop to return", stopped)

			if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
				t.Fatalf("Stop reported %+v, want drained with no failures", report)
			}
			assertLateAttachGaveEverythingBack(t, f, late, attachErr, sessionwire.HostLinkErrorNotAdmitting)
			// THE EARLY REFUSALS, each pinned where it applies: an attach still
			// hydrating when the drain began launches no runtime, and one that
			// launched starts no ownership work (no consumer that could begin an
			// attempt its rollback would strand).
			switch point {
			case parkHydrate:
				if launched(f) {
					t.Fatal("an attach the drain had already doomed launched a runtime anyway")
				}
			case parkLaunch:
				if !launched(f) {
					t.Fatal("the late attach never launched its runtime, so this row did not reach the park point it names")
				}
				if writerCalls() != ownershipBefore {
					t.Fatal("an attach the drain had already doomed started its ownership work (a disposition writer was bound)")
				}
			case parkOwnership:
				if !launched(f) {
					t.Fatal("the late attach never launched its runtime, so this row did not reach the park point it names")
				}
			}
			if consumed := f.svc.capacity.ConsumedWeight(); consumed != chargedBefore {
				t.Fatalf("ConsumedWeight = %d after Stop, want %d: the late attach's admission charge was not credited back", consumed, chargedBefore)
			}
		})
	}
}

// TestTheDrainDoesNotReportDrainedWhileAnAttachIsInFlight is the other
// ordering: nothing is resident, so the drain has nothing to release, and an
// attach admitted before it began is still hydrating. `drained` is the signal a
// Factory deletes a workload on; reporting it while an attach may yet take a
// runtime's journal lease tells the Factory a lie it cannot detect.
//
// THE PARK HONOURS CANCELLATION, which is what pins "wait first": a drain that
// cancelled the attach at once would end the park with context.Canceled rather
// than let it reach the drain check and answer not_admitting.
func TestTheDrainDoesNotReportDrainedWhileAnAttachIsInFlight(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked, release := parkAt(f, parkHydrate, true)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the late attach to park past admission", parked)

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()
	select {
	case report := <-waited:
		t.Fatalf("the drain finished (%+v) while an attach it admitted was still in flight", report)
	case <-time.After(100 * time.Millisecond):
	}
	if status, begun := f.svc.drainer.ObserveDrain(hostlink.DrainScope{}); !begun || status.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("ObserveDrain = (%+v, %v) with an attach in flight, want draining", status, begun)
	}

	release()
	attachErr := await(t, "the late attach to answer", attached)
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("the drain reported %+v, want drained with no failures", report)
	}
	assertLateAttachGaveEverythingBack(t, f, late, attachErr, sessionwire.HostLinkErrorNotAdmitting)
}

// TestAnAttachStillInFlightBeforeTheGraceIsCancelledAndRolledBack bounds the
// wait above. An attach that has not answered one idle boundary before the
// platform grace is cancelled — its session context, which every collaborator
// it is waiting on was handed — and rolls back, giving back the grant it took,
// inside the grace. Both a blocked durable read and a blocked runtime launch
// are cancelled this way.
func TestAnAttachStillInFlightBeforeTheGraceIsCancelledAndRolledBack(t *testing.T) {
	for _, point := range []parkPoint{parkHydrate, parkLaunch} {
		t.Run(string(point), func(t *testing.T) {
			f := newFixture(t)
			f.start()

			parked, release := parkAt(f, point, true)
			t.Cleanup(release)
			_, attached := attachLate(t, f)
			await(t, "the late attach to park past admission", parked)

			if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
				t.Fatalf("StartDrain: %v", err)
			}
			waited := make(chan lifecycle.Report, 1)
			go func() { waited <- f.svc.drainer.Wait() }()
			f.clock.fireWhenWaiting(t, cancelAt(f))

			attachErr := await(t, "the cancelled attach to answer", attached)
			if !errors.Is(attachErr, context.Canceled) {
				t.Fatalf("the late attach answered %v, want it cancelled", attachErr)
			}
			report := await(t, "the drain to finish", waited)
			if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
				t.Fatalf("the drain reported %+v, want drained with no failures", report)
			}
			if lease := f.store.leaseOf(keyB); lease == nil || !lease.released.Load() {
				t.Fatalf("the cancelled attach's residency grant (%v) was never released", lease)
			}
			if _, held := f.svc.registry.Get(keyB); held {
				t.Fatal("the cancelled attach left a registry entry")
			}
		})
	}
}

// TestAnAttachThatIgnoresCancellationWithholdsDrained is the bound's other
// half. An attach whose collaborator ignores its cancelled context cannot hold
// the drain past the platform grace — and because it may still hold a grant and
// a runtime, the drain must NOT report drained: that is the signal a Factory
// deletes the workload on. The attach, when it does return, still rolls back
// rather than becoming resident.
func TestAnAttachThatIgnoresCancellationWithholdsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked, release := parkAt(f, parkHydrate, false)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the late attach to park past admission", parked)

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()
	f.clock.fireWhenWaiting(t, cancelAt(f))
	// The grace is armed by the drain's own goroutine, in no fixed order with
	// the cancellation, so wait for its handle by count rather than by event.
	f.clock.awaitWaiters(t, f.svc.options.Grace, 1)
	if fired := f.clock.fire(f.svc.options.Grace); fired == 0 {
		t.Fatal("nothing had armed the platform grace")
	}

	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the drain reported %q with an attach still in flight, want draining", report.State)
	}
	var settle *lifecycle.Failure
	for i := range report.Failures {
		if report.Failures[i].Step == lifecycle.StepSettleAttaches {
			settle = &report.Failures[i]
		}
	}
	if settle == nil || !errors.Is(settle.Err, lifecycle.ErrWaitAbandoned) || !strings.Contains(settle.Err.Error(), string(sessionB)) {
		t.Fatalf("the drain's failures %v do not name the abandoned attach of %s", report.Failures, sessionB)
	}

	release()
	assertLateAttachGaveEverythingBack(t, f, late, await(t, "the late attach to answer", attached), sessionwire.HostLinkErrorNotAdmitting)
}

// TestACommittedAttachFinishesBeforeTheDrainReleasesIt is review advisory A1.
//
// An attach that COMMITTED before the drain read its snapshot is in the
// snapshot, but it has not returned: its last step publishes the `resident`
// row. Released beneath it, that row landed AFTER the drain's tombstone, the
// attach returned Attached on a Host that had just released the session, and
// its manager record was never pruned. The drain must let that attach finish
// before it releases the session — and the cancellation it makes before the
// grace must not touch it, because it is already the drain's to release.
func TestACommittedAttachFinishesBeforeTheDrainReleasesIt(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked := make(chan context.Context, 1)
	proceed := make(chan struct{})
	var open sync.Once
	var first atomic.Bool
	var order []string
	var orderMu sync.Mutex
	note := func(step string) { orderMu.Lock(); order = append(order, step); orderMu.Unlock() }
	f.trace.watch(func(step string) {
		if step == "locations.tombstone" || step == "checkpoint" {
			note(step)
		}
	})
	f.store.mu.Lock()
	f.store.publishHold = func(ctx context.Context, row sessionwire.HostLinkRegistryObservation) {
		if row.SessionID != sessionB || row.Residency != sessionwire.SessionResidencyResident {
			return
		}
		// ONLY THE ATTACH'S OWN PUBLICATION PARKS; a heartbeat beat (which the
		// drain's nonaccepting publication drives) passes straight through.
		if first.CompareAndSwap(false, true) {
			parked <- ctx
			<-proceed
			note("publish resident")
		}
	}
	f.store.mu.Unlock()
	t.Cleanup(func() { open.Do(func() { close(proceed) }) })

	late := newControllableSession(testRigSessionID)
	f.rig.Session = late
	type answer struct {
		held residency.Residency
		err  error
	}
	attached := make(chan answer, 1)
	go func() {
		held, err := f.svc.Attach(context.Background(), residency.Request{
			TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
			Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-late"},
		})
		attached <- answer{held, err}
	}()
	attachCtx := await(t, "the committed attach to park in its resident publication", parked)
	if resident := f.svc.ResidentSessions(); len(resident) != 1 || resident[0].Key() != keyB {
		t.Fatalf("the parked attach is not in the resident set (%d sessions), so it has not committed and this test measures nothing", len(resident))
	}

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	// The drain must not release the session while its attach is publishing.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		orderMu.Lock()
		touched := len(order) != 0
		orderMu.Unlock()
		if touched {
			t.Fatalf("the drain released the session (%v) while its committed attach was still publishing", order)
		}
		time.Sleep(time.Millisecond)
	}

	// The pre-grace cancellation reaches only uncommitted attaches.
	f.clock.fireWhenWaiting(t, cancelAt(f))
	if err := attachCtx.Err(); err != nil {
		t.Fatalf("the committed attach's session context was cancelled (%v): the drain tore down a residency it was about to release", err)
	}

	open.Do(func() { close(proceed) })
	got := await(t, "the committed attach to answer", attached)
	if got.err != nil || !got.held.Attached {
		t.Fatalf("the committed attach answered (%+v, %v), want attached: it committed before the drain began", got.held, got.err)
	}
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("the drain reported %+v, want drained with no failures", report)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) < 2 || order[0] != "publish resident" {
		t.Fatalf("order = %v, want the resident publication before any release step", order)
	}
	if lease := f.store.leaseOf(keyB); lease == nil || !lease.released.Load() {
		t.Fatal("the drain did not release the committed session's grant")
	}
	if late.Released() == 0 {
		t.Fatal("the drain did not release the committed session's runtime")
	}
	if records := f.svc.manager.Records(); records != 0 {
		t.Fatalf("manager.Records() = %d after the drain, want 0: the residency record outlived its release", records)
	}
}

// refuseResidentRowOf makes the store refuse sessionB's step-8 `resident`
// publication — AFTER the attach has committed into the resident set — and
// optionally parks it first. It returns the park's entry and release.
func refuseResidentRowOf(f *fixture, park bool) (entered <-chan struct{}, release func()) {
	parked := make(chan struct{})
	proceed := make(chan struct{})
	var first atomic.Bool
	var open sync.Once
	f.store.mu.Lock()
	f.store.publishRefuse = func(row sessionwire.HostLinkRegistryObservation) error {
		if row.SessionID != sessionB || row.Residency != sessionwire.SessionResidencyResident || !first.CompareAndSwap(false, true) {
			return nil
		}
		if park {
			close(parked)
			<-proceed
		}
		return errors.New("injected: the resident row could not be written")
	}
	f.store.mu.Unlock()
	return parked, func() { open.Do(func() { close(proceed) }) }
}

// assertCommittedAttachRolledBackOnce is F1's claim: a committed attach that
// fails at step 8 leaves the composition holding nothing, and gives back its
// runtime and grant exactly once.
func assertCommittedAttachRolledBackOnce(t *testing.T, f *fixture, late *controllableSession) {
	t.Helper()
	f.svc.mu.Lock()
	_, resident := f.svc.sessions[keyB]
	f.svc.mu.Unlock()
	if resident {
		t.Fatal("the rolled-back attach is still tracked as resident: a zombie with a running consumer")
	}
	if _, running := f.svc.ConsumerFor(keyB); running {
		t.Fatal("the rolled-back attach's command consumer is still registered")
	}
	if f.svc.warm.Watching(keyB) {
		t.Fatal("the rolled-back attach is still warm-watched")
	}
	if released := late.Released(); released != 1 {
		t.Fatalf("the runtime was released %d times, want exactly once", released)
	}
	if lease := f.store.leaseOf(keyB); lease == nil || lease.releases.Load() != 1 {
		t.Fatalf("the grant (%v) was not released exactly once", lease)
	}
}

// TestACommittedAttachThatFailsAtItsLastStepLeavesNoResident is review finding
// F1 without a drain. The attach commits into the composition's resident set at
// step 7 and then fails to publish its `resident` row at step 8. The manager's
// rollback stopped only the heartbeat, so the composition kept the session
// resident with its consumer running — and a re-attach of the same key then
// met the stale warm watch.
func TestACommittedAttachThatFailsAtItsLastStepLeavesNoResident(t *testing.T) {
	f := newFixture(t)
	f.start()
	refuseResidentRowOf(f, false)

	late, attached := attachLate(t, f)
	if err := await(t, "the attach to answer", attached); err == nil {
		t.Fatal("the attach whose resident row was refused reported success")
	}
	assertCommittedAttachRolledBackOnce(t, f, late)

	f.rig.Session = newControllableSession(testRigSessionID)
	if _, err := f.svc.Attach(context.Background(), residency.Request{
		TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-again"},
	}); err != nil {
		t.Fatalf("a re-attach of the rolled-back session was refused: %v", err)
	}
}

// TestADrainWaitingOnACommittedAttachThatRollsBackReportsDrained is F1 with a
// drain waiting on the attach. It must see the residency ended and release
// nothing a second time: before the fix it checkpointed the rolled-back
// session, released its runtime and grant twice, and stayed `draining` on the
// failed release steps of a heartbeat that was already gone.
func TestADrainWaitingOnACommittedAttachThatRollsBackReportsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()
	parked, release := refuseResidentRowOf(f, true)
	t.Cleanup(release)

	late, attached := attachLate(t, f)
	await(t, "the committed attach to reach its resident publication", parked)
	if resident := f.svc.ResidentSessions(); len(resident) != 1 {
		t.Fatalf("the parked attach is not in the resident set (%d sessions), so it has not committed", len(resident))
	}
	started := make(chan error, 1)
	go func() {
		_, err := f.svc.StartDrain(hostlink.DrainScope{})
		started <- err
	}()
	if err := await(t, "the drain to begin", started); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	release()
	if err := await(t, "the attach to answer", attached); err == nil {
		t.Fatal("the attach whose resident row was refused reported success")
	}
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("the drain reported %+v, want drained with no failures", report)
	}
	if f.trace.indexOf("checkpoint") != -1 {
		t.Fatal("the drain checkpointed a session whose attach had already rolled back")
	}
	assertCommittedAttachRolledBackOnce(t, f, late)
}

// refuseLeaseReleaseOf makes sessionB's grant refuse its release, so an
// attach's rollback of it cannot complete.
func refuseLeaseReleaseOf(f *fixture) {
	f.store.mu.Lock()
	f.store.leaseReleaseErr = map[sessionwire.SessionID]error{sessionB: errors.New("injected: the grant could not be released")}
	f.store.mu.Unlock()
}

// failureNaming reports whether a drain report carries a failure that
// withholds `drained` and names sessionB.
func withheldFor(report lifecycle.Report, session sessionwire.SessionID) bool {
	if report.State != sessionwire.HostLinkDrainStateDraining {
		return false
	}
	for _, failure := range report.Failures {
		if failure.Key.SessionID == session || strings.Contains(failure.Err.Error(), string(session)) {
			return true
		}
	}
	return false
}

// TestAnIncompleteRollbackOfACommittedAttachWithholdsDrained is review finding
// P1-a for an attach the drain was waiting on. Its step 8 fails and its
// rollback cannot release the grant. The drain skipped the ended residency and
// reported `drained` — with a grant this Host still holds.
func TestAnIncompleteRollbackOfACommittedAttachWithholdsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()
	refuseLeaseReleaseOf(f)
	parked, release := refuseResidentRowOf(f, true)
	t.Cleanup(release)

	_, attached := attachLate(t, f)
	await(t, "the committed attach to reach its resident publication", parked)
	started := make(chan error, 1)
	go func() {
		_, err := f.svc.StartDrain(hostlink.DrainScope{})
		started <- err
	}()
	if err := await(t, "the drain to begin", started); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	release()
	var refused *residency.AttachError
	if err := await(t, "the attach to answer", attached); !errors.As(err, &refused) || len(refused.Unreleased) == 0 {
		t.Fatalf("the attach answered %v, want a refusal reporting what its rollback could not release", err)
	}
	if report := await(t, "the drain to finish", waited); !withheldFor(report, sessionB) {
		t.Fatalf("the drain reported %+v, want draining with a failure naming %s: its rollback left a grant held", report, sessionB)
	}
}

// TestAnIncompleteRollbackOfAnUncommittedAttachWithholdsDrained is P1-a for an
// attach that never entered the resident set: the drain's Host-wide settle
// must carry its rollback's failure rather than count it settled.
func TestAnIncompleteRollbackOfAnUncommittedAttachWithholdsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()
	refuseLeaseReleaseOf(f)
	parked, release := parkAt(f, parkHydrate, false)
	t.Cleanup(release)

	_, attached := attachLate(t, f)
	await(t, "the late attach to park past admission", parked)
	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	release()
	var refused *residency.AttachError
	if err := await(t, "the attach to answer", attached); !errors.As(err, &refused) || len(refused.Unreleased) == 0 {
		t.Fatalf("the attach answered %v, want a refusal reporting what its rollback could not release", err)
	}
	if report := await(t, "the drain to finish", waited); !withheldFor(report, sessionB) {
		t.Fatalf("the drain reported %+v, want draining with a failure naming %s", report, sessionB)
	}
}

// TestAResidencyRemovedByAnotherPathIsNotReportedEnded is review finding P1-b.
// A warm release that finished — even one whose tombstone failed — removes the
// resident, and the drain's snapshot of it must NOT read that removal as
// "ended cleanly": only the drain's own FinishRelease on the snapshot surfaces
// the failure the release retained. Ended answers true only for the attach
// rollback that recorded its outcome.
func TestAResidencyRemovedByAnotherPathIsNotReportedEnded(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	snapshot := f.svc.ResidentSessions()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot holds %d sessions, want 1", len(snapshot))
	}
	held := f.svc.residentFor(keyA)
	f.svc.forgetResident(held) // what a finished warm release does
	if snapshot[0].(lifecycle.Ender).Ended() {
		t.Fatal("Ended() = true for a residency another path removed; the drain would skip it and lose that path's failures")
	}
}

// TestACommittedAttachParkedPastTheGraceIsLeftHeldAndReleasedOnce is review
// finding P2. The drain's wait for a committed attach expires at the grace. It
// must NOT release that residency: the attach may still fail and unwind its own
// runtime and grant, and the two releases would bypass each other's guards.
// The drain stays `draining`, and the grant is released exactly once — by the
// attach's own rollback when its publication finally fails.
func TestACommittedAttachParkedPastTheGraceIsLeftHeldAndReleasedOnce(t *testing.T) {
	f := newFixture(t)
	f.start()
	parked, release := refuseResidentRowOf(f, true)
	t.Cleanup(release)

	late, attached := attachLate(t, f)
	await(t, "the committed attach to reach its resident publication", parked)
	started := make(chan error, 1)
	go func() {
		_, err := f.svc.StartDrain(hostlink.DrainScope{})
		started <- err
	}()
	if err := await(t, "the drain to begin", started); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	f.clock.fireWhenWaiting(t, cancelAt(f))
	f.clock.awaitWaiters(t, f.svc.options.Grace, 1)
	f.clock.fire(f.svc.options.Grace)
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the drain reported %q with a committed attach still in flight, want draining", report.State)
	}
	if f.trace.indexOf("checkpoint") != -1 || late.Released() != 0 {
		t.Fatal("the drain released a residency whose attach was still in flight")
	}

	release()
	if err := await(t, "the attach to answer", attached); err == nil {
		t.Fatal("the attach whose resident row was refused reported success")
	}
	if released := late.Released(); released != 1 {
		t.Fatalf("the runtime was released %d times, want exactly once", released)
	}
	if lease := f.store.leaseOf(keyB); lease == nil || lease.releases.Load() != 1 {
		t.Fatalf("the grant (%v) was not released exactly once", lease)
	}
}

// ---------------------------------------------------------------------------
// One release authority per residency, and the Host's release ledger
// ---------------------------------------------------------------------------

// TestAnIncompleteRollbackTheDrainNeverSawWithholdsDrained is re-review P1(1).
// A committed attach fails at step 8; its rollback removes the residency and
// then cannot release the grant. Whether that happens wholly before the drain
// began, or while the drain was taking its snapshot, the snapshot never names
// the session — and the drain reported `drained` with the grant still held.
// The release ledger records it independently of any snapshot.
func TestAnIncompleteRollbackTheDrainNeverSawWithholdsDrained(t *testing.T) {
	for _, when := range []string{"before the drain", "during the drain's snapshot"} {
		t.Run(when, func(t *testing.T) {
			f := newFixture(t)
			f.start()
			refuseLeaseReleaseOf(f)
			refuseResidentRowOf(f, false)
			if when == "during the drain's snapshot" {
				// The drain begins from inside the rollback's grant release —
				// after the residency was removed, before the release fails.
				f.store.mu.Lock()
				f.store.onLeaseRelease = map[sessionwire.SessionID]func(){sessionB: func() {
					if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
						t.Errorf("StartDrain: %v", err)
					}
					if resident := f.svc.ResidentSessions(); len(resident) != 0 {
						t.Errorf("the drain's snapshot holds %d sessions, want none: this row is about a snapshot that missed the session", len(resident))
					}
				}}
				f.store.mu.Unlock()
			}

			_, attached := attachLate(t, f)
			var refused *residency.AttachError
			if err := await(t, "the attach to answer", attached); !errors.As(err, &refused) || len(refused.Unreleased) == 0 {
				t.Fatalf("the attach answered %v, want a refusal whose rollback could not release everything", err)
			}
			report, err := f.svc.Stop(context.Background())
			if err != nil {
				t.Fatalf("Stop: %v", err)
			}
			if !withheldFor(report, sessionB) {
				t.Fatalf("Stop reported %+v, want draining with a failure naming %s: its grant is still held", report, sessionB)
			}
		})
	}
}

// TestADrainRacingAWarmReleaseSharesItsGrantRelease is re-review P1(2). A warm
// release is inside the grant's release when the drain reaches the same step.
// The drain used to see the step latched and answer nil at once — `drained`
// before the grant was released — and never saw the warm release's error. It
// now waits for that one release and reports its outcome.
func TestADrainRacingAWarmReleaseSharesItsGrantRelease(t *testing.T) {
	f := newFixture(t)
	f.start()
	inRelease := make(chan struct{})
	proceed := make(chan struct{})
	f.store.mu.Lock()
	f.store.leaseReleaseErr = map[sessionwire.SessionID]error{sessionA: errors.New("injected: the grant could not be released")}
	f.store.onLeaseRelease = map[sessionwire.SessionID]func(){sessionA: func() {
		close(inRelease)
		<-proceed
	}}
	f.store.mu.Unlock()
	f.attach(tenantA, sessionA)
	held := f.svc.residentFor(keyA)

	warmDone := make(chan error, 1)
	go func() { warmDone <- (warmSession{resident: held}).ReleaseLease(context.Background()) }()
	await(t, "the warm release to enter the grant's release", inRelease)

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()
	close(proceed)

	if err := await(t, "the warm release to answer", warmDone); err == nil {
		t.Fatal("the warm release's grant release reported success")
	}
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the drain reported %q, want draining: the grant it shared with the warm release was never released", report.State)
	}
	if lease := f.store.leaseOf(keyA); lease.releases.Load() != 1 {
		t.Fatalf("the grant was released %d times, want once", lease.releases.Load())
	}
}

// TestNoOtherPathReleasesAnUnsettledAttachTwice is re-review P2(3). An attach
// has committed and is still publishing its `resident` row. A warm release may
// not begin on it (its attach has not returned), and any other path that does
// release it — here a give-up running concurrently — shares the rollback's one
// release of the runtime and the grant, so each is released exactly once.
func TestNoOtherPathReleasesAnUnsettledAttachTwice(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.work.setState(keyA, residency.WorkStateIdle)
	f.work.setState(keyB, residency.WorkStateIdle)
	if !f.svc.ConfirmIdle(keyA) {
		t.Fatal("an idle, settled residency is not confirmed idle, so the refusal below would mean nothing")
	}

	parked, release := refuseResidentRowOf(f, true)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the committed attach to reach its resident publication", parked)
	if f.svc.ConfirmIdle(keyB) {
		t.Fatal("a warm release may begin on a residency whose attach has not returned")
	}

	held := f.svc.residentFor(keyB)
	if err := held.ReleaseResidency(context.Background()); err != nil {
		t.Fatalf("concurrent runtime release: %v", err)
	}
	if err := held.releaseLease(context.Background()); err != nil {
		t.Fatalf("concurrent grant release: %v", err)
	}

	release()
	if err := await(t, "the attach to answer", attached); err == nil {
		t.Fatal("the attach whose resident row was refused reported success")
	}
	if released := late.Released(); released != 1 {
		t.Fatalf("the runtime was released %d times, want exactly once", released)
	}
	if lease := f.store.leaseOf(keyB); lease.releases.Load() != 1 {
		t.Fatalf("the grant was released %d times, want exactly once", lease.releases.Load())
	}
}

// ---------------------------------------------------------------------------
// A benign race must never poison the release ledger
// ---------------------------------------------------------------------------

// TestARollbackAfterACompleteGiveUpLeavesTheHostDrainable is round-3 finding 1.
// A committed attach is still publishing its `resident` row when its runtime
// faults, and the give-up releases the session COMPLETELY — runtime abandoned,
// tombstone written, local entry removed, grant and workspace released, charge
// credited. The attach's publication then fails and it rolls back. Every
// compensation must receive that give-up's verdict through the residency's
// release authority: compensations run directly reported "nothing was
// removed" and "not charged", which went on the permanent release ledger, and
// the Host could never drain again.
func TestARollbackAfterACompleteGiveUpLeavesTheHostDrainable(t *testing.T) {
	f := newFixture(t)
	f.start()
	parked, release := refuseResidentRowOf(f, true)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the committed attach to reach its resident publication", parked)

	late.Fault(errors.New("injected journal append failure"))
	awaitCondition(t, "the give-up to finish", func() bool {
		lease := f.store.leaseOf(keyB)
		return f.svc.residentFor(keyB) == nil && lease != nil && lease.releases.Load() == 1
	})
	if _, held := f.svc.registry.Get(keyB); held {
		t.Fatal("the give-up left the registry entry, so it did not complete and this row measures a different race")
	}
	if late.Abandoned() != 1 {
		t.Fatalf("the runtime was abandoned %d times by the give-up, want once", late.Abandoned())
	}

	release()
	var refused *residency.AttachError
	if err := await(t, "the attach to answer", attached); !errors.As(err, &refused) {
		t.Fatalf("the attach answered %v, want a refusal", err)
	} else if len(refused.UnreleasedCauses) != 0 {
		t.Fatalf("the rollback reported %v unreleased after a complete give-up had released everything", refused.UnreleasedCauses)
	}
	if err := f.svc.Unreleased(); err != nil {
		t.Fatalf("the release ledger holds %v after a benign race", err)
	}
	// THE TOMBSTONE TOO: the rollback's must be the give-up's, not a second
	// write around the heartbeat's first-verdict guard.
	if tombstones := f.trace.count("locations.tombstone"); tombstones != 1 {
		t.Fatalf("%d tombstones were written, want the give-up's one", tombstones)
	}
	if lease := f.store.leaseOf(keyB); lease.releases.Load() != 1 {
		t.Fatalf("the grant was released %d times, want once", lease.releases.Load())
	}
	if late.Released() != 0 || late.Abandoned() != 1 {
		t.Fatalf("the runtime was released %d / abandoned %d times, want 0 / 1", late.Released(), late.Abandoned())
	}
	if consumed := f.svc.capacity.ConsumedWeight(); consumed != 0 {
		t.Fatalf("ConsumedWeight = %d, want 0: the charge was credited more or less than once", consumed)
	}
	report, err := f.svc.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("a later drain reported %+v, want drained with no failures", report)
	}
}

// TestAGrantLostMidWarmReleaseLeavesTheHostDrainable is round-3 finding 2. A
// warm release has released the runtime and not yet finished; the grant is
// lost; the lost-residency give-up finishes locally with the tombstone refused
// by the ended fence — the expected shape of a lost grant. The warm release
// then resumes, receives that same verdict and reports itself released. That
// refusal is not a leak and must not reach the release ledger.
func TestAGrantLostMidWarmReleaseLeavesTheHostDrainable(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	held := f.svc.residentFor(keyA)
	warm := warmSession{resident: held}
	ctx := context.Background()
	if err := warm.BeginRelease(ctx); err != nil {
		t.Fatalf("warm BeginRelease: %v", err)
	}
	if err := warm.ReleaseResidency(ctx); err != nil {
		t.Fatalf("warm ReleaseResidency: %v", err)
	}

	f.store.loseLease(keyA)
	awaitCondition(t, "the lost-residency give-up to finish", func() bool { return f.svc.residentFor(keyA) == nil })

	finishErr := warm.FinishRelease(ctx)
	if finishErr == nil || withoutLostGrant(finishErr) != nil {
		t.Fatalf("the resumed warm FinishRelease answered %v, want only the expected lost-grant refusal, or this row measures nothing", finishErr)
	}
	f.svc.WarmRelease(residency.WarmOutcome{
		Key: keyA, Generation: held.generation, Kind: residency.WarmOutcomeReleased,
		Failures: []residency.WarmFailure{{Step: residency.WarmStepFinishRelease, Err: finishErr}},
	})
	if err := f.svc.Unreleased(); err != nil {
		t.Fatalf("the release ledger holds %v after a lost grant mid-warm-release", err)
	}
	report, err := f.svc.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("a later drain reported %+v, want drained with no failures", report)
	}
}

// TestAGenuineWarmReleaseFailureStillWithholdsDrained is the classification's
// other side: a warm release whose grant release genuinely fails is on the
// ledger, and a later drain may not report drained.
func TestAGenuineWarmReleaseFailureStillWithholdsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.svc.WarmRelease(residency.WarmOutcome{
		Key: keyA, Generation: f.svc.residentFor(keyA).generation, Kind: residency.WarmOutcomeReleased,
		Failures: []residency.WarmFailure{{Step: residency.WarmStepReleaseLease, Err: errors.New("injected: the provider refused the release")}},
	})
	report, err := f.svc.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if report.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("a drain after a genuinely failed warm release reported %q, want draining", report.State)
	}
}

// TestARollbackBeforeCommitUnderALostGrantLeavesTheHostDrainable is round-4's
// finding. The attach has published its `attaching` row (step 6) and is
// starting ownership when the grant is lost and a drain begins; the commit is
// then refused and the attach rolls back BEFORE any release authority exists.
// Its tombstone is refused by the ended fence — the expected shape of a lost
// grant — and every other compensation succeeds. That refusal is not a leak:
// the release ledger stays empty and the drain reports drained. It is also the
// row that pins the lost-grant classification on the attach-rollback path.
func TestARollbackBeforeCommitUnderALostGrantLeavesTheHostDrainable(t *testing.T) {
	f := newFixture(t)
	f.start()
	parked, release := parkAt(f, parkOwnership, false)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the attach to park in its ownership start", parked)

	f.store.loseLease(keyB)
	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()

	release()
	var refused *residency.AttachError
	if err := await(t, "the attach to answer", attached); !errors.As(err, &refused) {
		t.Fatalf("the attach answered %v, want a refusal", err)
	}
	if len(refused.UnreleasedCauses) == 0 {
		t.Fatal("the rollback reported nothing unreleased, so the lost-grant refusal this row is about never happened")
	}
	for _, cause := range refused.UnreleasedCauses {
		if withoutLostGrant(cause) != nil {
			t.Fatalf("the rollback reported %v, which is not the expected lost-grant refusal: this row needs every other compensation to succeed", cause)
		}
	}
	if err := f.svc.Unreleased(); err != nil {
		t.Fatalf("the release ledger holds %v after a lost grant before commit", err)
	}
	if late.Released() != 1 {
		t.Fatalf("the runtime was released %d times, want once", late.Released())
	}
	report := await(t, "the drain to finish", waited)
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("the drain reported %+v, want drained with no failures", report)
	}
}
