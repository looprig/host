package compose

import (
	"context"
	"errors"
	"strings"
	"sync"
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

// parkLoads parks every LoadSessionState of one session — past admission and
// past the lease — until the returned release is called. When honourCancel is
// set the park also ends, with the context's error, if the attach is cancelled.
func parkLoads(f *fixture, session sessionwire.SessionID, honourCancel bool) (entered <-chan struct{}, release func()) {
	parked := make(chan struct{})
	released := make(chan struct{})
	var once, open sync.Once
	f.store.mu.Lock()
	f.store.loadHold = func(ctx context.Context, asked sessionwire.SessionID) error {
		if asked != session {
			return nil
		}
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
	f.store.mu.Unlock()
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
func assertLateAttachGaveEverythingBack(t *testing.T, f *fixture, late *controllableSession, attachErr error) {
	t.Helper()
	var refused *residency.AttachError
	if !errors.As(attachErr, &refused) || refused.Code != sessionwire.HostLinkErrorNotAdmitting {
		t.Fatalf("the late attach answered %v, want a not_admitting refusal: a draining Host must not report a session attached that its drain will never release", attachErr)
	}
	if lease := f.store.leaseOf(keyB); lease == nil || !lease.released.Load() {
		t.Fatalf("the late session's residency grant (%v) was never released", lease)
	}
	if late.Released() == 0 {
		t.Fatal("the late session's runtime never released its residency, so its journal lease is still held and a successor's hydrate is refused")
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

// TestAnAttachInFlightWhenTheDrainReadsItsSessionsIsGivenBackBeforeStopReturns
// is the tests lane's C1 wedge, reduced to this package.
//
// An attach passes admission and takes its lease; THEN the Host stops, and its
// drain reads which sessions to release and parks on another session's
// checkpoint; THEN the attach finishes. Before the fix the attach reported
// success on a draining Host, the drain never released it, and Stop returned
// `drained` while the late session's runtime still held its journal lease — so
// every successor's hydrate was refused for the life of the process.
func TestAnAttachInFlightWhenTheDrainReadsItsSessionsIsGivenBackBeforeStopReturns(t *testing.T) {
	checkpoints := newHoldingCheckpointer()
	t.Cleanup(checkpoints.open)
	f := newFixture(t, func(o *Options, _ *hostconfig.Options) { o.Checkpointer = checkpoints })
	f.start()
	f.attach(tenantA, sessionA)
	chargedBefore := f.svc.capacity.ConsumedWeight()

	parked, release := parkLoads(f, sessionB, false)
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
	assertLateAttachGaveEverythingBack(t, f, late, attachErr)
	if consumed := f.svc.capacity.ConsumedWeight(); consumed != chargedBefore {
		t.Fatalf("ConsumedWeight = %d after Stop, want %d: the late attach's admission charge was not credited back", consumed, chargedBefore)
	}
}

// TestTheDrainDoesNotReportDrainedWhileAnAttachIsInFlight is the other
// ordering: nothing is resident, so the drain has nothing to release, and an
// attach admitted before it began is still hydrating. `drained` is the signal a
// Factory deletes a workload on; reporting it while an attach may yet take a
// runtime's journal lease tells the Factory a lie it cannot detect.
func TestTheDrainDoesNotReportDrainedWhileAnAttachIsInFlight(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked, release := parkLoads(f, sessionB, false)
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
	assertLateAttachGaveEverythingBack(t, f, late, attachErr)
}

// TestAnAttachStillInFlightAtTheGraceIsCancelledAndRolledBack bounds the wait
// above. An attach whose hydration has not answered by the platform grace is
// cancelled — its session context, which every collaborator it is waiting on
// was handed — and it rolls back, giving back the grant it took. The drain
// then reports drained: nothing is left holding anything.
func TestAnAttachStillInFlightAtTheGraceIsCancelledAndRolledBack(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked, release := parkLoads(f, sessionB, true)
	t.Cleanup(release)
	_, attached := attachLate(t, f)
	await(t, "the late attach to park past admission", parked)

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()
	f.clock.fireWhenWaiting(t, f.svc.options.Grace)

	attachErr := await(t, "the cancelled attach to answer", attached)
	if !errors.Is(attachErr, context.Canceled) {
		t.Fatalf("the late attach answered %v, want its hydration cancelled", attachErr)
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
}

// TestAnAttachThatIgnoresCancellationWithholdsDrained is the bound's other
// half. An attach whose collaborator ignores its cancelled context cannot hold
// the drain past the grace plus one idle boundary — and because it may still
// hold a grant and a runtime, the drain must NOT report drained: that is the
// signal a Factory deletes the workload on. The attach, when it does return,
// still rolls back rather than becoming resident.
func TestAnAttachThatIgnoresCancellationWithholdsDrained(t *testing.T) {
	f := newFixture(t)
	f.start()

	parked, release := parkLoads(f, sessionB, false)
	t.Cleanup(release)
	late, attached := attachLate(t, f)
	await(t, "the late attach to park past admission", parked)

	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.svc.drainer.Wait() }()
	f.clock.fireWhenWaiting(t, f.svc.options.Grace)
	f.clock.fireWhenWaiting(t, f.svc.options.IdleBoundary)

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
	assertLateAttachGaveEverythingBack(t, f, late, await(t, "the late attach to answer", attached))
}
