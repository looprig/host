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
	// The grace was armed when the drain began, before the cancellation.
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
