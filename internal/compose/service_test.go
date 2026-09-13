package compose

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

const (
	tenantA  = sessionwire.TenantID("tenant-a")
	tenantB  = sessionwire.TenantID("tenant-b")
	sessionA = sessionwire.SessionID("session-a")
	sessionB = sessionwire.SessionID("session-b")
)

// keyA is the session every single-session test uses.
var keyA = registry.Key{TenantID: tenantA, SessionID: sessionA}

// ---------------------------------------------------------------------------
// Start: the first publication is synchronous
// ---------------------------------------------------------------------------

// TestStartPublishesEveryTargetBeforeItReturns is the composition's half of
// "deployable".
//
// A Host that started and advertised later, or not at all, is a container a
// platform reports as healthy and a Factory can never place onto — the failure
// that is hardest to see from either side. The assertion is on the rows the
// durable directory received, one per Department LaunchTarget, before Start
// returned.
//
// THE "BEFORE IT RETURNS" HALF IS CARRIED BY THE NEXT TEST AND NOT BY THIS ONE,
// which is written down because this test's own assertion is a COUNT read after
// Start returned, and a count cannot see an ordering.
// TestStartFailsWhenTheFirstPublicationIsRefused is the ordering probe: a Start
// that published on a goroutine would return nil against a refusing directory,
// and that test fails on it deterministically. Measured as MF3.
func TestStartPublishesEveryTargetBeforeItReturns(t *testing.T) {
	f := newFixture(t)

	if published := f.directory.publications(); len(published) != 0 {
		t.Fatalf("a composed but unstarted Host published %d rows, want 0", len(published))
	}
	f.start()

	published := f.directory.publications()
	if len(published) != f.host.Department().Len() {
		t.Fatalf("Start published %d rows for %d launch targets", len(published), f.host.Department().Len())
	}
	row := published[0]
	if !row.Report.Accepting || row.Report.AvailableCapacity != f.host.Capacity() {
		t.Errorf("the first publication = (accepting %v, capacity %d), want (true, %d)",
			row.Report.Accepting, row.Report.AvailableCapacity, f.host.Capacity())
	}
	if row.Report.HostGeneration != testGen {
		t.Errorf("published generation = %d, want the run's %d", row.Report.HostGeneration, testGen)
	}
}

// TestNewBuildsAndDoesNotStart asserts the constructor's own claim.
//
// compose.New says "IT BUILDS AND DOES NOT START … either a Service with
// nothing running or an error and nothing at all", and until this existed the
// only probe of it was that an unstarted Host had published no rows — one
// observable of "nothing running", not the claim. A goroutine count is the
// claim. The comparison is one-sided on purpose: goroutines left over from
// EARLIER tests in this package go on exiting while this runs, so a count that
// falls is ordinary and only a count that RISES is New having started
// something.
//
// Start is the control. Without it a probe that could see no goroutine at all
// would pass the first assertion for the wrong reason.
func TestNewBuildsAndDoesNotStart(t *testing.T) {
	f := newFixture(t)

	before := runtime.NumGoroutine()
	built, err := New(f.svc.options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("New left %d more goroutines running (%d -> %d); it must build and not start", after-before, before, after)
	}

	beforeStart := runtime.NumGoroutine()
	if err := built.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The control: Start opens the advertisement heartbeat and the work
	// sampler, so this probe can see a started Host.
	if after := runtime.NumGoroutine(); after <= beforeStart {
		t.Errorf("Start left the goroutine count at %d (was %d), so this probe cannot see a running Host and the assertion above is vacuous", after, beforeStart)
	}
	if _, err := built.Stop(t.Context()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// TestStartFailsWhenTheFirstPublicationIsRefused is the other half. A refusal
// here is a configuration fault — an expiry the directory will not store, a
// clock Core rejects, a store that is not there — and it must fail the way a
// configuration fails.
func TestStartFailsWhenTheFirstPublicationIsRefused(t *testing.T) {
	f := newFixture(t)
	refusal := errors.New("the directory refused")
	f.directory.publishErr = refusal

	if err := f.svc.Start(t.Context()); !errors.Is(err, refusal) {
		t.Fatalf("Start with a refusing directory = %v, want the refusal", err)
	}
	if f.svc.Live() || f.svc.Ready() {
		t.Error("a Host whose first publication was refused reports itself live or ready")
	}
}

// TestACompositionRefusesAnExpiryTheDirectoryWouldRefuse moves that failure one
// step earlier still.
//
// host.Options puts NO CEILING on RegistryExpiry, so a Host with an expiry
// beyond what the target directory stores is constructible, passes every option
// rule it has, and can never publish a single row — it starts and goes silent.
// The bound is the store's OWN and is read from it rather than restated here,
// which is what keeps the two from drifting apart.
func TestACompositionRefusesAnExpiryTheDirectoryWouldRefuse(t *testing.T) {
	f := newFixture(t)

	beyond := f.svc.options.Targets.MaxAdvertisementTTL() + time.Minute
	wider := f.svc.options
	widerHost, err := hostWithExpiry(f, beyond)
	if err != nil {
		t.Fatalf("host.New with a %v expiry: %v", beyond, err)
	}
	wider.Host = widerHost

	var refusal *InvalidOptionsError
	if _, err := New(wider); !errors.As(err, &refusal) {
		t.Fatalf("New with a %v expiry = %v, want *InvalidOptionsError", beyond, err)
	}
	if refusal.Field != "Host.RegistryExpiry" {
		t.Errorf("the refusal names %q, want Host.RegistryExpiry", refusal.Field)
	}

	// The control: the same composition at an expiry the directory accepts is
	// built, so the refusal is about the bound and not about the fixture.
	withinHost, err := hostWithExpiry(f, f.svc.options.Targets.MaxAdvertisementTTL())
	if err != nil {
		t.Fatalf("host.New at the bound: %v", err)
	}
	within := f.svc.options
	within.Host = withinHost
	if _, err := New(within); err != nil {
		t.Fatalf("New at exactly the directory's bound = %v, want a composed Host", err)
	}
}

// ---------------------------------------------------------------------------
// The warm TTL reaches the releaser
// ---------------------------------------------------------------------------

// TestTheWiredHostHandsItsWarmTTLToTheReleaser discharges the item O6.2 booked
// against this task as O6.2-warmttl-unread.
//
// Until this composition existed, NOTHING IN THE MODULE READ host.WarmTTL. The
// releaser takes a TTL rather than a Host, so O6.2's own warm test could only
// take a number out of a Host and hand it to the releaser itself — which proves
// the releaser honours a number, not that the Host's number reaches it. What is
// asserted here is the whole path: a Host is constructed with a TTL, the
// composition wires it, a resident session is observed idle, and the countdown
// the composition's own clock is asked to arm is that TTL.
func TestTheWiredHostHandsItsWarmTTLToTheReleaser(t *testing.T) {
	const configured = 137 * time.Second

	f := newFixture(t, func(_ *Options, hostOptions *host.Options) {
		hostOptions.WarmTTL = configured
	})
	f.start()
	f.attach(tenantA, sessionA)

	// The releaser creates the countdown at Watch and stops it at once, so this
	// is the TTL reaching the CLOCK.
	if got := f.clock.constructions(); !slices.Equal(got, []time.Duration{configured}) {
		t.Fatalf("warm countdowns were created for %v, want exactly [%v]", got, configured)
	}
	// And nothing is armed until something observes the session idle, which is
	// the half that decides whether a session is released at all.
	if got := f.clock.rearmings(); len(got) != 0 {
		t.Fatalf("a session nobody has observed idle armed %v", got)
	}

	f.work.setState(keyA, residency.WorkStateIdle)
	f.clock.fireWhenWaiting(t, f.svc.options.WorkPoll)

	armed := f.awaitRearming(t)
	if armed != configured {
		t.Errorf("the warm countdown was armed for %v, want the Host's configured %v", armed, configured)
	}
	// The non-vacuity half: a TTL equal to the fixture's default would prove
	// nothing, because the assertion would pass against a composition that
	// ignored the Host and used the default.
	if configured == defaultFixtureWarmTTL {
		t.Fatal("the configured TTL equals the fixture's default, so this test cannot tell them apart")
	}
}

// TestAGateWaitNeverArmsTheWarmCountdown keeps the warm path's own rule wired
// through the composition's sampler.
//
// 04-host.md: "A waiting resident gate is not whole-session idle and therefore
// is not warm-evicted by this plan." residency enforces the rule; what this
// asserts is that the COMPOSITION feeds it the three-valued state rather than a
// boolean. A sampler that reported anything-not-working as idle would arm a
// countdown against a session with a human waiting on it, and every test in
// residency would stay green because residency would have been told "idle".
//
// The idle arm is the control. Without it, a sampler that observed NOTHING
// would satisfy the gate arm too.
func TestAGateWaitNeverArmsTheWarmCountdown(t *testing.T) {
	for _, test := range []struct {
		name  string
		state residency.WorkState
		armed bool
	}{
		{name: "gate wait", state: residency.WorkStateGateWaiting},
		{name: "working", state: residency.WorkStateWorking},
		{name: "idle", state: residency.WorkStateIdle, armed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			f.start()
			f.attach(tenantA, sessionA)

			f.work.setState(keyA, test.state)
			f.clock.fireWhenWaiting(t, f.svc.options.WorkPoll)

			if test.armed {
				if armed := f.awaitRearming(t); armed != defaultFixtureWarmTTL {
					t.Errorf("an idle session armed %v, want %v", armed, defaultFixtureWarmTTL)
				}
				return
			}
			f.clock.fireWhenWaiting(t, f.svc.options.WorkPoll)
			if got := f.clock.rearmings(); len(got) != 0 {
				t.Errorf("a %s session armed a warm countdown for %v", test.name, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Readiness and liveness
// ---------------------------------------------------------------------------

// TestReadinessTurnsFalseAtDrainWhileLivenessStaysTrue is 04-host.md's step 2,
// and the pair is the whole of it.
//
// THE SEMANTICS RELIED ON ARE KUBERNETES': a failing READINESS probe removes the
// Pod from Service endpoints and leaves the container running, and a failing
// LIVENESS probe RESTARTS the container. So a Host that answered a drain by
// failing liveness would have the platform kill the process it had just asked to
// shut down gracefully — in the middle of the checkpoints the drain exists to
// take — while a Host that answered by staying ready would go on receiving
// placements it has stopped admitting. Both halves are asserted at the same
// instant, because either alone is satisfied by a probe pair that answers
// identically.
func TestReadinessTurnsFalseAtDrainWhileLivenessStaysTrue(t *testing.T) {
	f := newFixture(t)

	if f.svc.Ready() || f.svc.Live() {
		t.Fatal("an unstarted Host reports itself ready or live")
	}
	f.start()
	if !f.svc.Ready() || !f.svc.Live() {
		t.Fatalf("a started Host reports (ready %v, live %v), want both true", f.svc.Ready(), f.svc.Live())
	}

	f.store.holdReleasing()
	f.attach(tenantA, sessionA)
	if _, err := f.svc.drainer.StartDrain(drainScope()); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}

	if f.svc.Ready() {
		t.Error("a draining Host still reports itself ready, so a platform goes on placing onto it")
	}
	if !f.svc.Live() {
		t.Error("a draining Host reports itself not live, so a platform restarts it mid-checkpoint")
	}

	f.store.releaseHold()
	if _, err := f.svc.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if f.svc.Ready() || f.svc.Live() {
		t.Errorf("a stopped Host reports (ready %v, live %v), want both false", f.svc.Ready(), f.svc.Live())
	}
}

// ---------------------------------------------------------------------------
// The drain's acknowledgement ordering
// ---------------------------------------------------------------------------

// TestTheDrainAcknowledgesAfterNonacceptingIsDurableAndBeforeAnyCheckpoint is
// O6.3's step 3 proved at the composition, where the production Advertiser
// exists for the first time.
//
// The ordering matters in both directions and this asserts both. If the
// acknowledgement came FIRST, a Host would have told a placement controller it
// had stopped accepting while a Factory reading the durable directory still saw
// a ranked, accepting target. If it came after the checkpoints, the RPC whose
// contract is "initiation plus generation" would be blocking for the length of
// every resident session's release.
//
// IT IS DETERMINISTIC AND NOT A RACE THE TEST HOPES TO WIN. The drain's
// per-session work runs on its own goroutine, so the store holds the first
// durable write that work makes — the `releasing` observation BeginRelease
// publishes — until this test lets it go. The checkpoint is downstream of that
// write, so "no checkpoint has happened yet" is a fact rather than a sampling.
func TestTheDrainAcknowledgesAfterNonacceptingIsDurableAndBeforeAnyCheckpoint(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.store.holdReleasing()
	f.attach(tenantA, sessionA)

	// THE ROWS THE ATTACH ITSELF WROTE ARE EXCLUDED, and the exclusion is not a
	// tidy-up: an attach publishes an `attaching` observation that already
	// carries accepting=false, so an assertion over every row ever written is
	// satisfied before the drain does anything at all. Measured — without this
	// watermark, deleting the drain publication entirely left the suite green.
	beforeDrain := len(f.store.published())
	if _, err := f.svc.drainer.StartDrain(drainScope()); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}

	// At the acknowledgement: the target row is durably withdrawn, the session
	// row is durably nonaccepting, and nothing has been checkpointed.
	if len(f.directory.withdrawals()) == 0 {
		t.Error("the drain acknowledged before any target advertisement was withdrawn")
	}
	duringDrain := f.store.published()[beforeDrain:]
	if !slices.ContainsFunc(duringDrain, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.SessionID == sessionA && !row.Accepting && row.Residency == sessionwire.SessionResidencyResident
	}) {
		t.Errorf("the drain acknowledged before it published a nonaccepting row for the RESIDENT session; rows written during the drain: %#v", duringDrain)
	}
	if position := f.trace.indexOf("checkpoint"); position != -1 {
		t.Errorf("a checkpoint ran at trace position %d, before the drain acknowledged: %v", position, f.trace.trace())
	}

	f.store.releaseHold()
	report := f.svc.drainer.Wait()
	if len(report.Failures) != 0 {
		t.Fatalf("the drain reported failures: %v", report.Failures)
	}
	if f.trace.indexOf("checkpoint") == -1 {
		t.Fatalf("no checkpoint ran at all, so the ordering above was vacuous: %v", f.trace.trace())
	}
}

// TestStopReleasesTheSessionAndClosesEveryTransportLast is the composed stop
// order.
//
// THE TRANSPORT CLOSES LAST and that is internal/lifecycle's Link contract
// rather than tidiness: Factory reaches the bounded drain-status observation
// through the link, so a link closed when the drain began would leave it
// inferring completion from a disconnect — the inference the status observation
// exists to replace.
//
// LAST IS ASSERTED AND NOT ONLY CLOSED. An earlier version of this test ended
// at "no transport survived the drain", which is a statement about the END
// STATE and says nothing about the word in its own name; a Stop that closed
// every transport as its FIRST statement, before StartDrain, passed it. The
// probe is therefore a POSITION: every durable step of the drain is observed as
// it runs, and each one must find the transports still open.
func TestStopReleasesTheSessionAndClosesEveryTransportLast(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)

	var (
		mu          sync.Mutex
		observed    []string
		afterClosed []string
	)
	f.trace.watch(func(step string) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, step)
		if len(f.svc.links.all()) == 0 {
			afterClosed = append(afterClosed, step)
		}
	})

	report, err := f.svc.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(report.Failures) != 0 {
		t.Fatalf("drain failures: %v", report.Failures)
	}

	trace := f.trace.trace()
	for _, expected := range []string{"checkpoint", "lease.release", "workspace.release"} {
		if f.trace.indexOf(expected) == -1 {
			t.Errorf("the drain never ran %q: %v", expected, trace)
		}
	}
	if released := f.runtime.Released(); released != 1 {
		t.Errorf("the runtime was released %d times, want 1", released)
	}

	mu.Lock()
	ran, early := append([]string(nil), observed...), append([]string(nil), afterClosed...)
	mu.Unlock()
	// The non-vacuity closer: an observer that saw nothing would report no step
	// running after the close, which is the same green as the correct order.
	if len(ran) == 0 {
		t.Fatal("no drain step was observed while it ran, so the ordering below is vacuous")
	}
	if len(early) != 0 {
		t.Errorf("the tenant transports were already closed when the drain ran %v; they must close LAST: %v", early, ran)
	}
	// And they do close, after the drain, which is the other half of "last".
	if held := f.svc.links.all(); len(held) != 0 {
		t.Errorf("%d tenant transports survived the drain", len(held))
	}
	// THE RESIDENCY GRANT IS HANDED BACK, and it is now the only grant an
	// attach takes: the journal grant this used to check alongside it was the
	// attach-time fence's, and a composed Host opens no journal writer.
	if released := f.trace.count("lease.release"); released != 1 {
		t.Errorf("the residency grant was released %d times, want 1", released)
	}
}

// TestADrainedHostIsNotReadyAndNotLive closes the probe pair's other end.
func TestADrainedHostIsNotReadyAndNotLive(t *testing.T) {
	f := newFixture(t)
	f.start()
	if _, err := f.svc.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if f.svc.Ready() || f.svc.Live() {
		t.Errorf("a drained Host reports (ready %v, live %v), want both false", f.svc.Ready(), f.svc.Live())
	}
}

// ---------------------------------------------------------------------------
// The compatibility wait
// ---------------------------------------------------------------------------

// TestTheCompatibilityWaitReturnsAtIdle is the first of its five endings.
func TestTheCompatibilityWaitReturnsAtIdle(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)

	outcome, err := f.svc.AwaitSessionIdle(t.Context(), keyA)
	if err != nil || outcome != CompatibilityIdle {
		t.Fatalf("AwaitSessionIdle = (%q, %v), want (%q, nil)", outcome, err, CompatibilityIdle)
	}
}

// TestTheCompatibilityWaitRefusesASessionThisHostDoesNotHold is the refusal
// that is not one of the five endings, and it is separate because it is not a
// WAIT at all: there is nothing to wait for.
func TestTheCompatibilityWaitRefusesASessionThisHostDoesNotHold(t *testing.T) {
	f := newFixture(t)
	f.start()

	outcome, err := f.svc.AwaitSessionIdle(t.Context(), keyA)
	if !errors.Is(err, ErrNotResident) {
		t.Fatalf("AwaitSessionIdle for a cold session = (%q, %v), want ErrNotResident", outcome, err)
	}
	if outcome != "" {
		t.Errorf("a refused wait reported outcome %q, want none", outcome)
	}
}

// TestACancelledCompatibilityWaitReportsStopped is the fourth ending reached
// through the caller's own context.
func TestACancelledCompatibilityWaitReportsStopped(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()

	ctx, cancel := context.WithCancel(t.Context())
	outcomes := make(chan CompatibilityOutcome, 1)
	go func() {
		outcome, _ := f.svc.AwaitSessionIdle(ctx, keyA)
		outcomes <- outcome
	}()
	cancel()

	if outcome := <-outcomes; outcome != CompatibilityStopped {
		t.Errorf("a cancelled wait = %q, want %q", outcome, CompatibilityStopped)
	}
}

// ---------------------------------------------------------------------------
// The HostLink command path
// ---------------------------------------------------------------------------

// TestTheHostLinkCommandPathDoesNotWaitForIdle keeps step 3's two halves apart.
//
// The compatibility wait is for a caller that has not been ported off a
// synchronous API; the HostLink delivery acknowledges PICKUP AND STATUS. This
// asserts the second: a delivery for a session whose runtime never goes idle
// returns, and the consumer was woken. If the delivery had been wired through
// the compatibility wait, it would not return at all — which is why the runtime
// is held non-idle for the whole test rather than merely being slow.
func TestTheHostLinkCommandPathDoesNotWaitForIdle(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()

	consumer, held := f.svc.ConsumerFor(keyA)
	if !held {
		t.Fatal("no consumer is registered for a resident session, so the delivery path has nothing to reach")
	}

	done := make(chan struct{})
	go func() {
		consumer.Hint("command-one")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a HostLink command delivery did not return while the session was not idle")
	}
}

// drainScope is the whole-Host scope, spelled once.
func drainScope() hostlink.DrainScope { return hostlink.DrainScope{} }

// TestTheCompatibilityWaitReturnsAtAGateBoundary is the second ending, and it
// is the one that needs the optional work-state source.
func TestTheCompatibilityWaitReturnsAtAGateBoundary(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()
	f.work.setState(keyA, residency.WorkStateGateWaiting)

	// THE FALLBACK IS WHAT MAKES THIS A VALUE ASSERTION RATHER THAN A
	// TIMEOUT-SHAPED ONE, and the two bounds are fired in sequence rather than
	// together on purpose. Firing both at once would leave the wait's select
	// with two ready cases and Go picks among those uniformly at random, so the
	// test would be flaky in the direction that hides the defect. The poll is
	// fired first; the timeout is released only once the wait has gone round its
	// loop, which is the discriminator this needs — a composition that had lost
	// the gate case reports timed_out instead of hanging. An earlier version
	// took ANY fresh WorkPoll arming as that signal, and the arming it saw was
	// the work sampler's rather than the wait's, because both arm WorkPoll on
	// the one fake clock; see awaitOutcome for the count that tells them apart.
	outcome, err := f.awaitOutcome(t, t.Context(), func() {
		f.clock.fire(f.svc.options.WorkPoll)
	}, f.svc.options.CompatibilityTimeout)
	if err != nil || outcome != CompatibilityGateBoundary {
		t.Fatalf("AwaitSessionIdle at a gate = (%q, %v), want (%q, nil)", outcome, err, CompatibilityGateBoundary)
	}
}

// TestAGateBoundaryIsUnreachableWithNoWorkStateSource is the absence the seam's
// documentation claims, asserted rather than promised.
//
// department.Runtime exposes no reader for a resident gate wait, so a Host
// composed without the optional source cannot distinguish one; the honest
// outcome is then the configured timeout, and NOT a gate boundary invented from
// no information.
func TestAGateBoundaryIsUnreachableWithNoWorkStateSource(t *testing.T) {
	f := newFixture(t, func(composed *Options, _ *host.Options) {
		composed.WorkStates = nil
	})
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()

	outcome, err := f.awaitOutcome(t, t.Context(), func() {
		f.clock.fire(f.svc.options.WorkPoll)
	}, f.svc.options.CompatibilityTimeout)
	if err != nil || outcome != CompatibilityTimedOut {
		t.Fatalf("AwaitSessionIdle with no work-state source = (%q, %v), want (%q, nil)", outcome, err, CompatibilityTimedOut)
	}
}

// TestTheCompatibilityWaitReturnsAtTheConfiguredTimeout is the fifth ending,
// and it is also the CONTROL for the gate boundary.
//
// The session here is WORKING, not idle and not at a gate, and the poll is fired
// before the timeout — so the wait is made to ask the work-state source and to
// get an answer that is not a gate. Without that, a gate check that answered
// "yes" for every state it could read would survive: the only test that ever
// fired a poll would be the one where the answer happened to be right. Measured,
// as mutant M14.
func TestTheCompatibilityWaitReturnsAtTheConfiguredTimeout(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()
	f.work.setState(keyA, residency.WorkStateWorking)

	outcome, err := f.awaitOutcome(t, t.Context(), func() {
		f.clock.fire(f.svc.options.WorkPoll)
	}, f.svc.options.CompatibilityTimeout)
	if err != nil || outcome != CompatibilityTimedOut {
		t.Fatalf("AwaitSessionIdle at the bound = (%q, %v), want (%q, nil)", outcome, err, CompatibilityTimedOut)
	}
}

// TestTheCompatibilityWaitReturnsWhenTheRuntimeStopsAnswering is the third
// ending.
func TestTheCompatibilityWaitReturnsWhenTheRuntimeStopsAnswering(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()

	outcome, err := f.awaitOutcome(t, t.Context(), f.runtime.Stop)
	if err != nil || outcome != CompatibilityFailed {
		t.Fatalf("AwaitSessionIdle over a stopped runtime = (%q, %v), want (%q, nil)", outcome, err, CompatibilityFailed)
	}
}

// TestTheCompatibilityWaitReturnsWhenTheResidencyGrantIsLost is the third
// ending reached the other way.
//
// The two are ONE outcome and that is deliberate: from the caller's side, a
// runtime that has stopped answering and a residency another Host now owns are
// both "this Host can no longer tell you what that session did".
func TestTheCompatibilityWaitReturnsWhenTheResidencyGrantIsLost(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.runtime.HoldIdle()

	outcome, err := f.awaitOutcome(t, t.Context(), func() { f.store.loseLease(keyA) })
	if err != nil || outcome != CompatibilityFailed {
		t.Fatalf("AwaitSessionIdle under a lost grant = (%q, %v), want (%q, nil)", outcome, err, CompatibilityFailed)
	}
}

// ---------------------------------------------------------------------------
// The three production adapters, audited against their fakes
// ---------------------------------------------------------------------------

// TestTheProductionWarmTimerIsTimeTimersOwnThreeMethods is the WarmClock half of
// this task's fake-versus-real audit, and it runs in BOTH directions.
//
// residency.WarmTimer was declared with time.Timer's own three methods
// specifically so that the production adapter would be a wrapper rather than a
// reimplementation. What the fake could hide is a wrapper that reinterpreted a
// result — most plausibly a Stop that returned true unconditionally, which would
// make "I disarmed a live countdown" and "it had already fired" indistinguishable
// to the releaser. This drives the real timer through both.
func TestTheProductionWarmTimerIsTimeTimersOwnThreeMethods(t *testing.T) {
	t.Parallel()

	clock := SystemClock{}
	timer := clock.NewWarmTimer(time.Hour)
	if !timer.Stop() {
		t.Error("Stop on a live countdown reported false")
	}
	if timer.Stop() {
		t.Error("Stop on an already-stopped countdown reported true, so a releaser cannot tell a disarm from an expiry")
	}

	fired := clock.NewWarmTimer(time.Nanosecond)
	select {
	case <-fired.C():
	case <-time.After(5 * time.Second):
		t.Fatal("a one-nanosecond countdown never fired")
	}
	if !fired.Reset(time.Hour) {
		// time.Timer.Reset reports whether the timer was active. An expired one
		// was not, so false is correct here and the assertion is inverted on
		// purpose: what would be wrong is a wrapper that claimed otherwise.
		t.Log("Reset on an expired countdown reported false, which is time.Timer's own answer")
	}
	if !fired.Stop() {
		t.Log("Stop after Reset reported false")
	}
}

// TestTheTwoReleaseSeamsGiveFinishReleaseTwoMeanings is the lifecycle.Session
// half of the audit, and it is the divergence the audit found.
//
// lifecycle.Session documents FinishRelease as "writes the epoch-fenced
// tombstone AND RELEASES THE LEASE". residency.WarmSession documents its own
// FinishRelease as the tombstone and the registry entry, and gives the lease its
// own ReleaseLease and the local state its own DropState. ONE TYPE CANNOT
// SATISFY BOTH UNDER ONE NAME, so the composition supplies two views of one
// session. A reviewer who assumed the two agreed would produce a Host that leaks
// a lease on every drain, which is why this is asserted and not described.
func TestTheTwoReleaseSeamsGiveFinishReleaseTwoMeanings(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)

	f.svc.mu.Lock()
	held := f.svc.sessions[keyA]
	f.svc.mu.Unlock()
	if held == nil {
		t.Fatal("the composition holds no session after an attach")
	}

	// The warm view's FinishRelease leaves the grants held: this seam gives the
	// lease its own step.
	if err := (warmSession{resident: held}).FinishRelease(t.Context()); err != nil {
		t.Fatalf("warm FinishRelease: %v", err)
	}
	if f.trace.indexOf("lease.release") != -1 {
		t.Errorf("the warm seam's FinishRelease released the lease: %v", f.trace.trace())
	}
	if err := (warmSession{resident: held}).ReleaseLease(t.Context()); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if f.trace.indexOf("lease.release") == -1 {
		t.Fatalf("ReleaseLease released nothing: %v", f.trace.trace())
	}
}

// TestTheDrainSeamsFinishReleaseReleasesTheLease is the other direction, on a
// fresh session so the once-guards are not already spent.
func TestTheDrainSeamsFinishReleaseReleasesTheLease(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)

	f.svc.mu.Lock()
	held := f.svc.sessions[keyA]
	f.svc.mu.Unlock()

	if err := (releaseSession{resident: held}).FinishRelease(t.Context()); err != nil {
		t.Fatalf("drain FinishRelease: %v", err)
	}
	for _, expected := range []string{"lease.release", "workspace.release"} {
		if f.trace.indexOf(expected) == -1 {
			t.Errorf("the drain seam's FinishRelease never ran %q: %v", expected, f.trace.trace())
		}
	}
}

// TestEveryReleaseStepIsIdempotent is what makes the two views safe to hold at
// once.
//
// Both a drain and a warm release can reach one session — a warm countdown that
// fires while a drain is running is an ordinary race — and a session released
// twice must not release a lease twice, write a tombstone twice, checkpoint
// twice or release the runtime twice.
//
// EVERY MEANS ALL SIX, AND THE TWO VIEWS ARE INTERLEAVED. An earlier version of
// this test drove two of the warm seam's six steps and asserted three counts,
// while Checkpoint, BeginRelease, FinishRelease and the runtime release had no
// guard at all: issuing the epoch-fenced tombstone write twice left the whole
// suite green. Both halves of that are fixed here — the steps are guarded in
// session.go, and this drives each of them three times and then crosses the
// seams, because a warm release and a drain reaching one resident is the whole
// reason there are two views of it.
func TestEveryReleaseStepIsIdempotent(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)

	f.svc.mu.Lock()
	held := f.svc.sessions[keyA]
	f.svc.mu.Unlock()
	if held == nil {
		t.Fatal("the composition holds no session after an attach")
	}

	warm := warmSession{resident: held}
	drain := releaseSession{resident: held}
	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "BeginRelease", run: warm.BeginRelease},
		{name: "Checkpoint", run: warm.Checkpoint},
		{name: "ReleaseResidency", run: warm.ReleaseResidency},
		{name: "FinishRelease", run: warm.FinishRelease},
		{name: "ReleaseLease", run: warm.ReleaseLease},
		{name: "DropState", run: warm.DropState},
		// The interleaving. This one view runs the tombstone, the heartbeat
		// stop, both grants and the local state as ONE step, which is what makes
		// it the crossing rather than a seventh warm step.
		{name: "drain FinishRelease", run: drain.FinishRelease},
	}
	for attempt := 1; attempt <= 3; attempt++ {
		for _, step := range steps {
			if err := step.run(t.Context()); err != nil {
				t.Fatalf("%s attempt %d: %v", step.name, attempt, err)
			}
		}
	}

	for _, counted := range []struct {
		step string
		got  int
		what string
	}{
		{step: "BeginRelease", got: releasingRows(f.store.published()), what: "marked the residency releasing"},
		{step: "Checkpoint", got: f.trace.count("checkpoint"), what: "checkpointed"},
		{step: "ReleaseResidency", got: f.runtime.Released(), what: "released the runtime"},
		{step: "FinishRelease", got: f.trace.count("locations.tombstone"), what: "wrote the epoch-fenced tombstone"},
		{step: "ReleaseLease", got: f.trace.count("lease.release"), what: "released the residency grant"},
		{step: "DropState", got: f.trace.count("workspace.release"), what: "released the workspace"},
	} {
		if counted.got != 1 {
			t.Errorf("%s %s %d times across three rounds and both views, want 1: %v",
				counted.step, counted.what, counted.got, f.trace.trace())
		}
	}
}

// releasingRows counts the durable rows marking a residency releasing.
func releasingRows(rows []sessionwire.HostLinkRegistryObservation) int {
	total := 0
	for _, row := range rows {
		if row.Residency == sessionwire.SessionResidencyReleasing {
			total++
		}
	}
	return total
}

// ---------------------------------------------------------------------------
// The HostLink surface
// ---------------------------------------------------------------------------

// TestTheHandlerGivesEachTenantItsOwnEndpoint is the H8 guard at the HTTP
// surface, where a real Factory arrives.
//
// The bijection test below the composition proves the registry keeps one
// Multiplexer per tenant; this proves the ROUTE reaches it. A handler that
// ignored the path segment would serve every tenant from one transport, and the
// transport authenticates against the tenant it was CONFIGURED with — so a
// single shared endpoint would be authenticating every connection as whichever
// tenant happened to be first.
func TestTheHandlerGivesEachTenantItsOwnEndpoint(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := httptest.NewServer(f.svc.Handler())
	defer server.Close()

	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		response, err := http.Get(server.URL + HostLinkPathPrefix + tenant)
		if err != nil {
			t.Fatalf("GET %s: %v", tenant, err)
		}
		response.Body.Close()
		// The upgrade is refused because this is not a WebSocket request; what
		// matters is that it reached THAT TENANT'S transport rather than a
		// shared one, and that is what the link registry below records.
		if response.StatusCode == http.StatusNotFound {
			t.Fatalf("GET %s was not routed at all (404)", tenant)
		}
	}

	first, held := f.svc.links.lookup("tenant-a")
	if !held {
		t.Fatal("no link was built for tenant-a")
	}
	second, heldSecond := f.svc.links.lookup("tenant-b")
	if !heldSecond {
		t.Fatal("no link was built for tenant-b")
	}
	if first.mux == second.mux {
		t.Error("two tenants reached one Multiplexer through the HTTP surface, which reverses H8")
	}
	if first.server == second.server {
		t.Error("two tenants reached one transport, so both would authenticate as one tenant")
	}
}

// TestEachTenantsTransportAuthenticatesAsTheTenantOnItsPath is the H8 guard at
// the AUTHENTICATING layer, and it is the half the bind guard below does not
// cover.
//
// buildTenantLink names the tenant TWICE: once on the Multiplexer, which
// decides which sessions may be bound over a table, and once on
// hostlink.Config, which is the tenant the transport AUTHENTICATES every
// physical connection as. A guard over one object of a two-object invariant is
// not a guard over the invariant. With only the Multiplexer watched, latching
// the transport's TenantID to a constant left the whole suite green — and that
// is a cross-tenant authorization bypass, not an untidiness: Multiplexer.Bind
// compares the request's tenant against the table's and never consults the
// connection's credential, so a caller holding only tenant-a's credential
// would connect at tenant-b's path, authenticate as tenant-a, land on
// tenant-b's table and bind tenant-b's sessions.
//
// The assertion is therefore on the ARGUMENT the authenticator was called
// with, per endpoint, and on the refusal a foreign credential gets.
func TestEachTenantsTransportAuthenticatesAsTheTenantOnItsPath(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := httptest.NewServer(f.svc.Handler())
	defer server.Close()

	for _, tenant := range []sessionwire.TenantID{tenantA, tenantB} {
		if !hostLinkConnect(t, server.URL, tenant, credentialFor(tenant)) {
			t.Fatalf("%s's own credential was refused at %s's endpoint", tenant, tenant)
		}
	}
	want := []string{
		string(tenantA) + ":" + credentialFor(tenantA),
		string(tenantB) + ":" + credentialFor(tenantB),
	}
	if got := f.auth.verifications(); !slices.Equal(got, want) {
		t.Fatalf("the transports authenticated %v, want %v: a transport asked about a tenant other than the one on its path", got, want)
	}

	// THE BYPASS ARM. tenant-a's credential at tenant-b's endpoint. A transport
	// latched to tenant-a would ask about tenant-a, the credential would verify,
	// and the connection would be admitted to tenant-b's Multiplexer.
	if hostLinkConnect(t, server.URL, tenantB, credentialFor(tenantA)) {
		t.Fatal("a tenant-a credential connected at tenant-b's endpoint, which is a cross-tenant bypass")
	}
	refused := f.auth.verifications()
	last := refused[len(refused)-1]
	if want := string(tenantB) + ":" + credentialFor(tenantA); last != want {
		t.Errorf("the refusing transport asked about %q, want %q", last, want)
	}
}

// TestAPathWithNoTenantIsRefusedRatherThanRouted keeps the endpoint from having
// a default tenant.
func TestAPathWithNoTenantIsRefusedRatherThanRouted(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := httptest.NewServer(f.svc.Handler())
	defer server.Close()

	for _, path := range []string{"/hostlink/", "/hostlink/a/b"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, response.StatusCode)
		}
	}
	if held := f.svc.links.all(); len(held) != 0 {
		t.Errorf("%d links were built for requests that named no tenant", len(held))
	}
}

// TestEachTenantsMultiplexerIsConfiguredForThatTenant closes the hole the
// pointer-identity guard cannot see.
//
// A composition that built a DISTINCT routing table per tenant while configuring
// every one of them with the same TenantID would satisfy the bijection — the
// tables really are distinct — and would still be the H8 reversal, because
// hostlink refuses a bind whose tenant is not the one its table was built for.
// Every connection would then authenticate and route as whichever tenant the
// composition latched onto. The property is therefore asserted through BINDS:
// each tenant's own session binds over its own table, and the other tenant's
// session is refused there with RefusalForeignTenant.
func TestEachTenantsMultiplexerIsConfiguredForThatTenant(t *testing.T) {
	f := newFixture(t)
	f.start()

	const otherTenant = sessionwire.TenantID("tenant-b")
	const otherSession = sessionwire.SessionID("session-b")
	first := f.attach(tenantA, sessionA)
	second := f.attach(otherTenant, otherSession)

	for _, test := range []struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		epoch   uint64
	}{
		{tenant: tenantA, session: sessionA, epoch: uint64(first.LeaseEpoch)},
		{tenant: otherTenant, session: otherSession, epoch: uint64(second.LeaseEpoch)},
	} {
		link, held := f.svc.links.lookup(test.tenant)
		if !held {
			t.Fatalf("no link for %q", test.tenant)
		}
		if _, err := link.mux.Bind("link-1", f.bindRequest(test.tenant, test.session, test.epoch)); err != nil {
			t.Fatalf("%q binding its own session over its own table: %v", test.tenant, err)
		}
	}

	// The cross-tenant arm. This is what a shared or latched table would allow.
	link, _ := f.svc.links.lookup(tenantA)
	_, err := link.mux.Bind("link-2", f.bindRequest(otherTenant, otherSession, uint64(second.LeaseEpoch)))
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("binding another tenant's session over this tenant's table = %v, want a *BindError", err)
	}
	if refusal.Refusal != hostlink.RefusalForeignTenant {
		t.Errorf("the refusal is %q, want %q: the table did not know which tenant it was built for",
			refusal.Refusal, hostlink.RefusalForeignTenant)
	}
}

// TestABusyTenantIsNeverEvictedByAStrangerConnecting is the safety half of the
// tenant bound.
//
// Reclaiming under pressure is only sound because it takes EMPTY links: a
// Multiplexer with no bindings routes nothing and holds no session state, so
// closing its transport costs a Factory a reconnect. A reclaim that took a link
// holding bindings would let any caller that could open a socket drop a working
// tenant's routes, and the refusal it replaces — this Host has no room — is the
// correct answer. Measured as mutant M15: without this row the rule was
// unguarded.
func TestABusyTenantIsNeverEvictedByAStrangerConnecting(t *testing.T) {
	f := newFixture(t, func(composed *Options, _ *host.Options) {
		composed.MaxTenantLinks = 1
	})
	f.start()
	held := f.attach(tenantA, sessionA)

	link, found := f.svc.links.lookup(tenantA)
	if !found {
		t.Fatal("the attach built no link for its tenant")
	}
	if _, err := link.mux.Bind("link-1", f.bindRequest(tenantA, sessionA, uint64(held.LeaseEpoch))); err != nil {
		t.Fatalf("binding the resident session: %v", err)
	}
	if link.mux.Len() == 0 {
		t.Fatal("the bind left the routing table empty, so this test cannot tell a busy link from an idle one")
	}

	if _, err := f.svc.links.resolve("tenant-b"); !errors.Is(err, errTooManyTenants) {
		t.Fatalf("a second tenant at a full Host holding a BUSY link = %v, want the capacity refusal", err)
	}
	if _, stillHeld := f.svc.links.lookup(tenantA); !stillHeld {
		t.Error("a stranger connecting evicted a tenant that held live routes")
	}
}

// ---------------------------------------------------------------------------
// R-1: the whole-Host drain across tenants
// ---------------------------------------------------------------------------

// TestATenantLinkCannotDrainAnotherTenantsSessions is R-1, and it is the
// composition-level statement of the defect because no single package could see
// it: hostlink knows one tenant per Multiplexer and lifecycle knows one drain,
// and the hole is exactly the join — every tenant's Multiplexer is handed the
// SAME Host-level Drainer, so a whole-Host drain begun over ANY of them covers
// EVERY tenant's resident sessions.
//
// IT IS AVAILABILITY AND NOT CONFIDENTIALITY. tenant-a reads nothing of
// tenant-b's, addresses none of its sessions and learns nothing about them; what
// it can do is have them checkpointed and released early, which is an outage
// tenant-b did not ask for and cannot see coming.
//
// THE NEGATIVE ASSERTION IS MADE NON-VACUOUS TWICE. tenant-b's session is
// asserted RESIDENT before the drain request, so "tenant-b was not drained" is a
// statement about a session there was something to drain; and the same session
// is drained at the end of the test through the process-lifecycle path, so the
// silence in between is a property of the refusal rather than of a fixture with
// nothing in it.
func TestATenantLinkCannotDrainAnotherTenantsSessions(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	f.attach(tenantB, sessionB)

	// The premise. Both tenants hold a LIVE, RESIDENT, DRAINABLE session, so
	// the assertions below are about a drain that could have happened.
	resident := map[registry.Key]bool{}
	for _, held := range f.svc.ResidentSessions() {
		resident[held.Key()] = true
	}
	for _, key := range []registry.Key{{TenantID: tenantA, SessionID: sessionA}, {TenantID: tenantB, SessionID: sessionB}} {
		if !resident[key] {
			t.Fatalf("%v is not resident, so this test has nothing to protect", key)
		}
	}

	link, held := f.svc.links.lookup(tenantA)
	if !held {
		t.Fatal("no link was built for tenant-a")
	}

	// THE ATTACK. A whole-Host drain names NO tenant and no session, and it
	// arrives on a link that authenticated as tenant-a and nothing else.
	whole := sessionwire.HostLinkDrainRequest{
		Version:        sessionwire.CurrentWireVersion,
		HostID:         testHostID,
		HostGeneration: testGen,
		IdempotencyKey: "tenant-a-drains-the-host",
	}
	beforeDrain := len(f.store.published())
	_, err := link.mux.StartDrain(whole)
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("a whole-Host drain over tenant-a's authenticated link = %v, want a refusal: "+
			"one tenant's link can begin a drain covering every other tenant's sessions", err)
	}
	if refusal.Refusal != hostlink.RefusalWrongDrainScope {
		t.Errorf("the refusal is %q, want %q", refusal.Refusal, hostlink.RefusalWrongDrainScope)
	}

	// tenant-b is untouched, on every observable this composition has.
	duringDrain := f.store.published()[beforeDrain:]
	if position := slices.IndexFunc(duringDrain, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.TenantID == tenantB && row.SessionID == sessionB && !row.Accepting
	}); position != -1 {
		t.Errorf("tenant-a's drain published a nonaccepting row for tenant-b's session: %#v", duringDrain[position])
	}
	if position := f.trace.indexOf("checkpoint"); position != -1 {
		t.Errorf("tenant-a's drain checkpointed a session at trace position %d: %v", position, f.trace.trace())
	}
	stillResident := map[registry.Key]bool{}
	for _, held := range f.svc.ResidentSessions() {
		stillResident[held.Key()] = true
	}
	if !stillResident[registry.Key{TenantID: tenantB, SessionID: sessionB}] {
		t.Error("tenant-a's drain released tenant-b's session")
	}
	if !f.svc.Ready() {
		t.Error("tenant-a's drain stopped the whole Host admitting, which is the outage this refusal exists to prevent")
	}

	// THE POSITIVE CONTROL, and it is two claims at once: tenant-b's session
	// really was drainable — so the silence above is the refusal's and not the
	// fixture's — and the PROCESS-LIFECYCLE path still drains the whole Host,
	// which is the one path R-1 leaves open.
	if _, err := f.svc.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	afterStop := f.store.published()[beforeDrain:]
	if !slices.ContainsFunc(afterStop, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.TenantID == tenantB && row.SessionID == sessionB && !row.Accepting
	}) {
		t.Fatalf("the process-lifecycle drain published no nonaccepting row for tenant-b's session, "+
			"so the absence asserted above proves nothing: %#v", afterStop)
	}
	if f.trace.indexOf("checkpoint") == -1 {
		t.Fatalf("the process-lifecycle drain checkpointed nothing, so the absence asserted above proves nothing: %v", f.trace.trace())
	}
}

// TestADedicatedHostRefusesADrainFromATenantThatDoesNotHoldItsSession is R-1's
// second half, and it is the axis the first round and its gate BOTH missed.
//
// A dedicated Host's fixed session is ONE SessionID, and `buildTenantLink` hands
// that same value to EVERY tenant's Multiplexer. The resolver then compares the
// request's session against it and the request's tenant against the LINK's — and
// never compares the fixed session's OWNER against the requester. So a link
// authenticated as tenant-b, holding nothing at all, could name
// `{tenant-b, the-fixed-session}`, pass every rung, and begin the Host-wide
// drain that releases tenant-a's session. It is the same defect class as the
// empty-tenant hole — a drain whose scope the Host cannot attribute to the
// requester — surviving on the same falsified premise.
//
// WHAT MAKES THE OWNER KNOWABLE HERE IS CAPACITY, not a new seam.
// `Options.validatePlacement` requires `Capacity == 1` for dedicated placement,
// so a dedicated Host holds AT MOST ONE resident session. "the requester holds
// the fixed session" and "the requester owns everything this Host-wide drain
// would touch" are therefore the same statement, and `Residencies.Get` — the
// read-only seam the bind path already uses — decides it.
//
// The negative assertion is non-vacuous twice, as before: tenant-a's session is
// asserted resident first, and tenant-a's OWN link drains it at the end.
func TestADedicatedHostRefusesADrainFromATenantThatDoesNotHoldItsSession(t *testing.T) {
	f := newFixture(t, func(_ *Options, host *host.Options) {
		host.Placement = sessionwire.HostPlacementDedicated
		host.FixedSessionID = sessionA
		host.Capacity = 1
	})
	f.start()
	f.attach(tenantA, sessionA)

	resident := map[registry.Key]bool{}
	for _, held := range f.svc.ResidentSessions() {
		resident[held.Key()] = true
	}
	if !resident[registry.Key{TenantID: tenantA, SessionID: sessionA}] {
		t.Fatal("tenant-a's session is not resident, so this test has nothing to protect")
	}

	// THE ATTACK. tenant-b authenticates, holds NOTHING, and names this Host's
	// fixed session under its OWN tenant — so the tenant rung passes, the
	// pooled rung passes, and the fixed-session rung passes.
	stranger, err := f.svc.links.resolve(tenantB)
	if err != nil {
		t.Fatalf("resolving a link for tenant-b: %v", err)
	}
	attack := sessionwire.HostLinkDrainRequest{
		Version:        sessionwire.CurrentWireVersion,
		HostID:         testHostID,
		HostGeneration: testGen,
		IdempotencyKey: "tenant-b-drains-tenant-a",
		TenantID:       tenantB,
		SessionID:      sessionA,
	}
	beforeDrain := len(f.store.published())
	_, err = stranger.mux.StartDrain(attack)
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("a drain naming this Host's fixed session, from a tenant holding nothing = %v, want a refusal: "+
			"tenant-b can drain tenant-a's session on a dedicated Host", err)
	}
	if refusal.Refusal != hostlink.RefusalWrongDrainScope {
		t.Errorf("the refusal is %q, want %q", refusal.Refusal, hostlink.RefusalWrongDrainScope)
	}

	duringAttack := f.store.published()[beforeDrain:]
	if position := slices.IndexFunc(duringAttack, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.TenantID == tenantA && row.SessionID == sessionA && !row.Accepting
	}); position != -1 {
		t.Errorf("tenant-b's drain published a nonaccepting row for tenant-a's session: %#v", duringAttack[position])
	}
	if position := f.trace.indexOf("checkpoint"); position != -1 {
		t.Errorf("tenant-b's drain checkpointed a session at trace position %d: %v", position, f.trace.trace())
	}
	if !f.svc.Ready() {
		t.Error("tenant-b's drain stopped the whole Host admitting")
	}

	// THE POSITIVE CONTROL, and it is the stronger one available: the session's
	// OWN tenant drains it over its OWN link, through the same resolver. That
	// proves the session was drainable over HostLink all along — so the silence
	// above is the refusal's — and that the fix refuses the stranger WITHOUT
	// refusing the legitimate caller, which a blanket refusal would not.
	owner, err := f.svc.links.resolve(tenantA)
	if err != nil {
		t.Fatalf("resolving the owning tenant's link: %v", err)
	}
	legitimate := attack
	legitimate.TenantID = tenantA
	legitimate.IdempotencyKey = "tenant-a-drains-its-own-session"
	if _, err := owner.mux.StartDrain(legitimate); err != nil {
		t.Fatalf("the session's own tenant was refused its own drain: %v", err)
	}
	f.svc.drainer.Wait()
	afterOwner := f.store.published()[beforeDrain:]
	if !slices.ContainsFunc(afterOwner, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.TenantID == tenantA && row.SessionID == sessionA && !row.Accepting
	}) {
		t.Fatalf("the owner's own drain published no nonaccepting row for its session, "+
			"so the absence asserted above proves nothing: %#v", afterOwner)
	}
	if f.trace.indexOf("checkpoint") == -1 {
		t.Fatalf("the owner's own drain checkpointed nothing, so the absence asserted above proves nothing: %v", f.trace.trace())
	}
}

// TestRungEightsAttributionDependsOnTheDedicatedCapacityBound is the TRIP-WIRE
// for R-1's second half, and it exists because rung 8 is a property that holds
// today for a reason enforced in a DIFFERENT FILE.
//
// THE DEPENDENCY, SPELLED OUT SO IT CANNOT BE RELAXED BY ACCIDENT.
// `drainScope`'s last rung refuses a fixed-session drain unless the requesting
// tenant currently HOLDS that session, and it treats that as sufficient
// attribution for a drain that covers the WHOLE HOST. It is sufficient only
// because a dedicated Host holds AT MOST ONE resident session — which is not
// hostlink's rule at all. It is `host.Options.validatePlacement` pinning
// `Capacity` to 1 for dedicated placement. Relax that rule and rung 8 silently
// stops being an attribution: a Host could hold tenant-a's session and
// tenant-b's at once, tenant-b would hold the fixed session, pass rung 8, and
// begin the Host-wide drain that releases tenant-a's — the exact defect R-1
// closed, reopened with no assertion anywhere failing.
//
// So this test fails, by assertion and with a message that names the hole, if
// the bound moves. It is deliberately in `internal/compose`, which is the one
// package that can see both `host.Options` and the resolver that depends on it.
func TestRungEightsAttributionDependsOnTheDedicatedCapacityBound(t *testing.T) {
	dedicated := func(capacity uint64) host.Options {
		options := newFixture(t).hostOptions
		options.Placement = sessionwire.HostPlacementDedicated
		options.FixedSessionID = sessionA
		options.Capacity = capacity
		return options
	}

	// PART 1: THE RULE. A dedicated Host above capacity one must be refused at
	// construction. This is the arm that fires when someone relaxes the bound.
	if _, err := host.New(dedicated(2)); err == nil {
		t.Fatal("A DEDICATED HOST WAS ACCEPTED AT CAPACITY 2, WHICH REOPENS THE R-1 DRAIN HOLE. " +
			"hostlink's drainScope refuses a fixed-session drain unless the requesting tenant HOLDS " +
			"that session, and treats that as attribution for a HOST-WIDE drain. That is sound only " +
			"while a dedicated Host holds at most one resident session. At capacity 2 a link " +
			"authenticated as tenant-b could hold the fixed session, pass the rung, and drain " +
			"tenant-a's session. Either restore the bound or replace the rung with an attribution " +
			"that does not depend on it.")
	} else {
		var invalid *host.InvalidOptionsError
		if !errors.As(err, &invalid) || invalid.Code != host.OptionErrorCodeDedicatedCap {
			t.Fatalf("a dedicated Host at capacity 2 was refused for the wrong reason (%v); "+
				"rung 8 depends on the CAPACITY bound specifically, so a refusal from some other "+
				"rule is not the guarantee it relies on", err)
		}
	}

	// PART 2: THE POSITIVE CONTROL for that refusal. Capacity one IS accepted,
	// so Part 1 is a statement about the bound and not about dedicated
	// placement being unbuildable.
	if _, err := host.New(dedicated(1)); err != nil {
		t.Fatalf("a dedicated Host at capacity 1 was refused, so Part 1 proves nothing: %v", err)
	}

	// PART 3: THE PROPERTY ITSELF, MEASURED RATHER THAN READ OFF THE RULE. What
	// rung 8 needs is not the spelling `Capacity != 1`; it is that a second
	// tenant cannot become resident beside the holder. A relaxation that kept
	// the option check and broke the enforcement would satisfy Part 1 and still
	// open the hole, so the behaviour is asserted on a running Host.
	f := newFixture(t, func(_ *Options, options *host.Options) {
		options.Placement = sessionwire.HostPlacementDedicated
		options.FixedSessionID = sessionA
		options.Capacity = 1
	})
	f.start()
	f.attach(tenantA, sessionA)

	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID:  tenantB,
		SessionID: sessionA,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantB, ActorID: "actor-b"},
	})
	if err == nil {
		t.Fatal("A SECOND TENANT BECAME RESIDENT ON A DEDICATED HOST, WHICH REOPENS THE R-1 DRAIN HOLE. " +
			"drainScope's last rung reads 'this tenant holds the fixed session' as 'this tenant owns " +
			"everything a Host-wide drain would touch'. With two tenants resident those are no longer " +
			"the same statement.")
	}
	held := 0
	for range f.svc.ResidentSessions() {
		held++
	}
	if held != 1 {
		t.Fatalf("a dedicated Host holds %d resident sessions, want exactly one: rung 8's attribution "+
			"covers one session and this Host has more than it can account for", held)
	}
}
