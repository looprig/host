package residency

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// ---------------------------------------------------------------------------
// The manual clock
// ---------------------------------------------------------------------------

// manualClock is a host.Clock whose timers fire only when a test says so.
//
// NO SLEEPING AND NO POLLING ANYWHERE, which is the rule a fake clock exists to
// make possible rather than a style. Every wait below is a blocking receive on
// something the subject itself produces: the loop asking for its next timer.
// The only time.After in this file is a DIAGNOSTIC on the failure arm — it
// turns "the test hung for ten minutes and the harness killed it" into a named
// failure, and no passing run ever reaches it.
//
// The timers are hand-built as &time.Timer{C: ch}, which is the only way to
// hand back a timer a test controls. SUCH A TIMER PANICS ON Stop, so the loop
// must never call it; see Heartbeat.run for what that costs.
type manualClock struct {
	mu        sync.Mutex
	now       time.Time
	requested []time.Duration

	created chan chan time.Time
	current chan time.Time
}

func newManualClock(at time.Time) *manualClock {
	return &manualClock{now: at, created: make(chan chan time.Time, 64)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(d time.Duration) *time.Timer {
	c.mu.Lock()
	c.requested = append(c.requested, d)
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.created <- ch
	return &time.Timer{C: ch}
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *manualClock) requests() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.requested...)
}

// waitForTimer blocks until the loop asks for its next timer, which is the
// signal that everything it was doing before is finished.
func (c *manualClock) waitForTimer(t *testing.T) {
	t.Helper()
	if c.current != nil {
		// Already holding the timer the loop is parked on. Waiting again would
		// block until a timer that will not be created until this one fires.
		return
	}
	select {
	case ch := <-c.created:
		c.current = ch
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat never asked for a timer")
	}
}

// waitForTimerOrExit is waitForTimer for the cases in which the loop may leave
// instead of scheduling again. It reports whether a timer was created.
func (c *manualClock) waitForTimerOrExit(t *testing.T, done <-chan struct{}) bool {
	t.Helper()
	if c.current != nil {
		return true
	}
	select {
	case ch := <-c.created:
		c.current = ch
		return true
	case <-done:
		return false
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat neither asked for a timer nor left")
		return false
	}
}

func (c *manualClock) fire(t *testing.T) {
	t.Helper()
	if c.current == nil {
		t.Fatal("fire was called with no timer waiting; call waitForTimer first")
	}
	c.current <- c.Now()
	c.current = nil
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeTeardown records the residencies handed to it.
type fakeTeardown struct {
	mu   sync.Mutex
	lost []LostResidency
}

func (f *fakeTeardown) ResidencyLost(_ context.Context, residency LostResidency) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lost = append(f.lost, residency)
}

func (f *fakeTeardown) handedOver() []LostResidency {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]LostResidency(nil), f.lost...)
}

// The Host's ONE drain flag satisfies DrainState as written, which is the
// compile-time half of not building a second one.
var _ DrainState = (*service.CapacityPublisher)(nil)

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const (
	// testEpoch is the lease epoch every heartbeat fixture runs under. It is
	// DELIBERATELY NEITHER 0 NOR 1, and neither is the host generation or the
	// registry generation it must not be confused with: this task has already
	// lost one probe to a fence stamped with a literal that equalled the
	// fixture's first epoch. 37, 41 and 1 are three distinct numbers, so a
	// tombstone or an observation carrying the wrong one is visible.
	testEpoch uint64 = 37

	testHeartbeatInterval = 5 * time.Second
)

type heartbeatFixture struct {
	t         *testing.T
	trace     *trace
	clock     *manualClock
	host      *host.Host
	registry  *spyRegistry
	locations *fakeLocations
	publisher *service.CapacityPublisher
	teardown  *fakeTeardown
	lease     *fakeLease
	runtime   *fakeRuntime
	entry     registry.Entry
	beat      *Heartbeat
	cancel    context.CancelFunc
}

func newHeartbeatFixture(t *testing.T, configure ...func(*heartbeatFixture)) *heartbeatFixture {
	t.Helper()
	steps := &trace{}
	clock := newManualClock(testClockAt)
	f := &heartbeatFixture{
		t:         t,
		trace:     steps,
		clock:     clock,
		registry:  &spyRegistry{inner: registry.New(clock), trace: steps},
		locations: &fakeLocations{trace: steps},
		teardown:  &fakeTeardown{},
		lease:     &fakeLease{trace: steps, epoch: testEpoch, lost: make(chan struct{})},
	}
	f.runtime = &fakeRuntime{trace: steps, sessionID: testSession, agentID: testAgent, done: make(chan struct{})}

	target := &fakeTarget{
		trace:         steps,
		compatibility: testCompat,
		capabilities: department.Capabilities{
			SupportsPooled:    true,
			SupportsDedicated: true,
			AdmissionWeight:   testWeight,
			CaptureSafety:     department.CaptureSafetyStreaming,
		},
	}
	dept, err := department.New([]department.Registration{{AgentID: testAgent, Target: target}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	built, err := host.New(host.Options{
		HostID:            testHost,
		TenantID:          testTenant,
		InternalEndpoint:  testEndpoint,
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      stubSessionStore{},
		Workspaces:        stubHostWorkspaces{},
		Clock:             clock,
		Auth:              stubAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          testCapacity,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: testHeartbeatInterval,
		RegistryExpiry:    testExpiry,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: 23 * time.Second,
		ReconcileBatch:    129,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	f.host = built

	publisher, err := service.NewCapacityPublisher(service.CapacityOptions{Host: built, HostGeneration: testGeneration})
	if err != nil {
		t.Fatalf("service.NewCapacityPublisher: %v", err)
	}
	f.publisher = publisher

	for _, apply := range configure {
		apply(f)
	}

	entry, installed := f.registry.inner.Insert(registry.Key{TenantID: testTenant, SessionID: testSession}, registry.Admission{
		AgentID:         testAgent,
		Target:          target,
		CompatibilityID: testCompat,
		Runtime:         f.runtime,
		LeaseEpoch:      testEpoch,
	})
	if !installed {
		t.Fatal("the seeded residency was not installed")
	}
	f.entry = entry

	ownership, err := NewHeartbeatOwnership(HeartbeatOptions{
		Host:           built,
		HostGeneration: testGeneration,
		Registry:       f.registry,
		Locations:      f.locations,
		Drain:          publisher,
		Teardown:       f.teardown,
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	t.Cleanup(cancel)
	handle, err := ownership.BeginOwnership(ctx, OwnershipRequest{
		Key:             entry.Key,
		AgentID:         testAgent,
		CompatibilityID: testCompat,
		LeaseEpoch:      testEpoch,
		Generation:      entry.Generation,
		Runtime:         f.runtime,
		Lease:           f.lease,
	})
	if err != nil {
		t.Fatalf("BeginOwnership: %v", err)
	}
	beat, isHeartbeat := handle.(*Heartbeat)
	if !isHeartbeat {
		t.Fatalf("BeginOwnership returned %T, want *Heartbeat", handle)
	}
	f.beat = beat
	t.Cleanup(func() { _ = beat.Stop(context.Background()) })
	return f
}

func (f *heartbeatFixture) key() registry.Key {
	return registry.Key{TenantID: testTenant, SessionID: testSession}
}

// pulse fires one heartbeat and returns once the loop has scheduled the next
// one, which is the only signal that the beat is complete. Nothing here sleeps.
func (f *heartbeatFixture) pulse(t *testing.T) {
	t.Helper()
	f.clock.waitForTimer(t)
	f.clock.fire(t)
	f.clock.waitForTimer(t)
}

// pulseExpectingExit fires one heartbeat and reports whether the loop left
// rather than scheduling again.
func (f *heartbeatFixture) pulseExpectingExit(t *testing.T) bool {
	t.Helper()
	f.clock.waitForTimer(t)
	f.clock.fire(t)
	return !f.clock.waitForTimerOrExit(t, f.beat.done)
}

// awaitExit blocks until the loop has left.
func (f *heartbeatFixture) awaitExit(t *testing.T) {
	t.Helper()
	select {
	case <-f.beat.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat never left")
	}
}

// ---------------------------------------------------------------------------
// Cadence and content
// ---------------------------------------------------------------------------

// TestEveryHeartbeatCarriesTheCurrentEpochGenerationAndState is step 2, and the
// word doing the work is CURRENT.
//
// The clock advances between beats and the registry state changes under them,
// so an observation derived once and re-sent would be visibly stale. Each beat
// is compared against a whole independently spelled record rather than a field
// or two: this task has already shipped a projection whose every field was
// right except one nobody compared.
func TestEveryHeartbeatCarriesTheCurrentEpochGenerationAndState(t *testing.T) {
	f := newHeartbeatFixture(t)

	want := func(at time.Time, residency sessionwire.SessionResidency, accepting bool) sessionwire.HostLinkRegistryObservation {
		return sessionwire.HostLinkRegistryObservation{
			Version:                sessionwire.CurrentWireVersion,
			TenantID:               testTenant,
			SessionID:              testSession,
			HostID:                 testHost,
			HostGeneration:         testGeneration,
			AgentID:                testAgent,
			RuntimeCompatibilityID: string(testCompat),
			Placement:              sessionwire.HostPlacementPooled,
			InternalEndpoint:       testEndpoint,
			Residency:              residency,
			Accepting:              accepting,
			LeaseEpoch:             testEpoch,
			ObservedAt:             at,
			ExpiresAt:              at.Add(testExpiry),
		}
	}

	first := f.clock.Now()
	f.pulse(t)
	f.clock.advance(testHeartbeatInterval)
	second := f.clock.Now()
	f.pulse(t)

	published := f.locations.publishedAll()
	if len(published) != 2 {
		t.Fatalf("%d observations were published, want 2", len(published))
	}
	if published[0] != want(first, sessionwire.SessionResidencyResident, true) {
		t.Errorf("the first beat published\n  %+v\nwant\n  %+v", published[0], want(first, sessionwire.SessionResidencyResident, true))
	}
	if published[1] != want(second, sessionwire.SessionResidencyResident, true) {
		t.Errorf("the second beat published\n  %+v\nwant\n  %+v", published[1], want(second, sessionwire.SessionResidencyResident, true))
	}
	if published[0].ObservedAt == published[1].ObservedAt {
		t.Error("both beats carry the same observation time, so the record is derived once and re-sent rather than re-derived")
	}
	for i, observation := range published {
		if err := observation.Validate(); err != nil {
			t.Errorf("observation %d is one Core refuses: %v", i, err)
		}
	}
	if beats := f.beat.Beats(); beats != 2 {
		t.Errorf("the heartbeat counted %d beats, want 2", beats)
	}
}

func TestTheHeartbeatCadenceIsTheHostsConfiguredInterval(t *testing.T) {
	f := newHeartbeatFixture(t)
	for range 3 {
		f.pulse(t)
	}
	requests := f.clock.requests()
	if len(requests) < 4 {
		t.Fatalf("%d timers were asked for across three beats, want at least 4", len(requests))
	}
	for i, requested := range requests {
		if requested != testHeartbeatInterval {
			t.Errorf("timer %d was asked for %s, want the Host's RegistryHeartbeat of %s", i, requested, testHeartbeatInterval)
		}
	}
	// The interval and the expiry are DIFFERENT values in this fixture, so a
	// heartbeat scheduled on the expiry — which is the mistake that loses the
	// registry entry to a single missed beat — is visible rather than equal.
	if testHeartbeatInterval == testExpiry {
		t.Fatal("the fixture's heartbeat interval equals its expiry, so scheduling on the wrong one is undetectable")
	}
	for i, observation := range f.locations.publishedAll() {
		if observation.ExpiresAt.Sub(observation.ObservedAt) != testExpiry {
			t.Errorf("observation %d expires %s after it was observed, want the Host's RegistryExpiry of %s", i, observation.ExpiresAt.Sub(observation.ObservedAt), testExpiry)
		}
	}
}

// TestAHeartbeatFollowsTheRegistrysResidencyState covers the mapping in both
// directions, because a heartbeat that published "resident" unconditionally
// would pass a test that only ever saw a resident entry.
func TestAHeartbeatFollowsTheRegistrysResidencyState(t *testing.T) {
	for _, row := range []struct {
		name      string
		move      func(*heartbeatFixture)
		residency sessionwire.SessionResidency
		accepting bool
	}{
		{name: "resident", move: func(*heartbeatFixture) {}, residency: sessionwire.SessionResidencyResident, accepting: true},
		{
			name:      "releasing",
			move:      func(f *heartbeatFixture) { f.registry.inner.MarkReleasing(f.key(), f.entry.Generation) },
			residency: sessionwire.SessionResidencyReleasing,
		},
		{
			// A draining state has no wire member of its own: Core accepts only
			// attaching, resident and releasing on the observation, and a
			// session being torn down is releasing as far as a router cares.
			name:      "draining maps to releasing",
			move:      func(f *heartbeatFixture) { f.registry.inner.BeginTeardown(f.key(), f.entry.Generation) },
			residency: sessionwire.SessionResidencyReleasing,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newHeartbeatFixture(t)
			row.move(f)
			f.pulse(t)

			published := f.locations.publishedAll()
			if len(published) != 1 {
				t.Fatalf("%d observations, want 1", len(published))
			}
			if published[0].Residency != row.residency {
				t.Errorf("the beat published residency %q, want %q", published[0].Residency, row.residency)
			}
			if published[0].Accepting != row.accepting {
				t.Errorf("the beat published Accepting %t, want %t", published[0].Accepting, row.accepting)
			}
			if err := published[0].Validate(); err != nil {
				t.Errorf("the observation is one Core refuses: %v", err)
			}
		})
	}
}

// TestDrainStopsTheAdvertisedAcceptingWithoutEndingResidency drives the Host's
// REAL drain flag, which is the point.
//
// A stub returning false would let a heartbeat that never consulted the flag
// pass, and internal/service's documentation names two sources of "draining" as
// the defect: a Host that stops accepting in one place while still advertising
// Accepting from the other. The control beat before the drain is what makes the
// change attributable to the drain rather than to anything else.
func TestDrainStopsTheAdvertisedAcceptingWithoutEndingResidency(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	f.publisher.BeginDrain()
	f.pulse(t)

	published := f.locations.publishedAll()
	if len(published) != 2 {
		t.Fatalf("%d observations, want 2", len(published))
	}
	if !published[0].Accepting {
		t.Fatal("the control beat before the drain published Accepting false, so this test cannot attribute the change to the drain")
	}
	if published[1].Accepting {
		t.Error("a draining Host still advertises Accepting; §18.1 stops admission first and lets the session reach idle")
	}
	if published[1].Residency != sessionwire.SessionResidencyResident {
		t.Errorf("draining moved the residency to %q; a draining Host still HOLDS its sessions until they reach a safe boundary", published[1].Residency)
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.State != registry.StateResident {
		t.Error("draining changed the local residency state; drain is a Host-wide admission decision, not a per-session one")
	}
}

// ---------------------------------------------------------------------------
// Loss of ownership
// ---------------------------------------------------------------------------

func TestLeaseLossStopsAdmissionAndTriggersOneTeardownOwner(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	publishedBefore := len(f.locations.publishedAll())

	close(f.lease.lost)
	f.awaitExit(t)

	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("the residency was removed; a lost lease claims teardown, it does not perform it")
	}
	if entry.Accepting {
		t.Error("the residency still accepts work after its lease was lost; admission must stop immediately")
	}
	if !entry.TeardownOwned {
		t.Error("no teardown owner was claimed")
	}
	taken := f.teardown.handedOver()
	if len(taken) != 1 {
		t.Fatalf("the teardown observer was handed %d residencies, want exactly 1", len(taken))
	}
	want := LostResidency{
		Key:             f.key(),
		AgentID:         testAgent,
		CompatibilityID: testCompat,
		LeaseEpoch:      testEpoch,
		Generation:      f.entry.Generation,
		Runtime:         f.runtime,
		Reason:          LossReasonLeaseLost,
	}
	if taken[0] != want {
		t.Errorf("the observer was handed\n  %+v\nwant\n  %+v", taken[0], want)
	}
	// The runtime is handed over UNRELEASED. Releasing it here would be a
	// second release path with no checkpoint, no lease release and no
	// tombstone.
	if released, shutdowns := f.runtime.counts(); released != 0 || shutdowns != 0 {
		t.Errorf("the runtime was released %d times and shut down %d times by the heartbeat; the release protocol is O6.1's", released, shutdowns)
	}
	if published := len(f.locations.publishedAll()); published != publishedBefore {
		t.Errorf("%d observations were published after the lease was lost, want none: a writer that has lost its grant must not touch a fenced record", published-publishedBefore)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("a tombstone was written under a lost lease: %v; the epoch may already be superseded, and §18.2 lets registry expiry remove the stale route", tombstones)
	}
}

// TestOnlyOneTeardownOwnerIsTriggered is the other direction: a heartbeat that
// finds teardown already claimed must not hand the residency over a second
// time, however many times ownership is lost.
func TestOnlyOneTeardownOwnerIsTriggered(t *testing.T) {
	f := newHeartbeatFixture(t)
	// Somebody else claims teardown first — a drain supervisor, a reconciler.
	if _, won := f.registry.inner.BeginTeardown(f.key(), f.entry.Generation); !won {
		t.Fatal("the rival could not claim teardown, so this test never reached its subject")
	}

	close(f.lease.lost)
	f.awaitExit(t)

	if taken := f.teardown.handedOver(); len(taken) != 0 {
		t.Errorf("the observer was handed %d residencies, want none: teardown was already owned", len(taken))
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.Accepting {
		t.Error("admission is still open although teardown is owned")
	}
}

// TestALaterEpochObservationEndsOwnership is the same fact arriving by the
// other of the two paths §10.1 describes.
func TestALaterEpochObservationEndsOwnership(t *testing.T) {
	// WRAPPED, not the bare sentinel: a store returns its own error carrying
	// this one, and a manager comparing with == rather than errors.Is would
	// treat a superseded epoch as ambiguity and retry forever under a lease it
	// no longer holds.
	f := newHeartbeatFixture(t, func(f *heartbeatFixture) {
		f.locations.publishErr = fmt.Errorf("sessionstore: rejecting the write: %w", ErrEpochSuperseded)
	})

	if !f.pulseExpectingExit(t) {
		t.Fatal("the heartbeat scheduled another beat after a fenced write was refused for a later epoch")
	}
	f.awaitExit(t)

	taken := f.teardown.handedOver()
	if len(taken) != 1 {
		t.Fatalf("the observer was handed %d residencies, want exactly 1", len(taken))
	}
	if taken[0].Reason != LossReasonEpochSuperseded {
		t.Errorf("the loss was reported as %q, want %q; the two paths to the same fact must stay distinguishable to an operator", taken[0].Reason, LossReasonEpochSuperseded)
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.Accepting || !entry.TeardownOwned {
		t.Error("a superseded epoch did not stop admission and claim teardown the way a lost lease does")
	}
	if failures := f.beat.ConsecutiveFailures(); failures != 0 {
		t.Errorf("a superseded epoch was counted as %d store failures; it is not ambiguity and must not be retried", failures)
	}
}

// TestStoreAmbiguityIsNotLossOfOwnership is the control for the two tests
// above. A store that failed for an unclassified reason has said nothing about
// who owns the session, and treating it as loss would tear down a residency
// this Host still holds the lease for.
func TestStoreAmbiguityIsNotLossOfOwnership(t *testing.T) {
	f := newHeartbeatFixture(t, func(f *heartbeatFixture) { f.locations.publishErr = errors.New("the store is unreachable") })

	for range 3 {
		f.pulse(t)
	}
	if failures := f.beat.ConsecutiveFailures(); failures != 3 {
		t.Errorf("the heartbeat recorded %d consecutive failures, want 3", failures)
	}
	if taken := f.teardown.handedOver(); len(taken) != 0 {
		t.Errorf("an unreachable store tore down %d residencies; the LEASE decides ownership and it is still held", len(taken))
	}
	if entry, held := f.registry.Get(f.key()); !held || !entry.Accepting || entry.TeardownOwned {
		t.Error("an unreachable store stopped admission or claimed teardown")
	}

	// And it RECOVERS: the run of failures is a run, not a latch.
	f.locations.mu.Lock()
	f.locations.publishErr = nil
	f.locations.mu.Unlock()
	f.pulse(t)
	if failures := f.beat.ConsecutiveFailures(); failures != 0 {
		t.Errorf("after a successful beat the failure run is %d, want 0", failures)
	}
	if beats := f.beat.Beats(); beats != 1 {
		t.Errorf("the heartbeat counted %d beats, want the 1 that succeeded", beats)
	}
}

// TestAReplacedResidencyStopsItsHeartbeat holds the generation rule the local
// registry already enforces for mutations, at the one place that only reads.
func TestAReplacedResidencyStopsItsHeartbeat(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	if !f.registry.inner.RemoveByGeneration(f.key(), f.entry.Generation) {
		t.Fatal("the seeded residency could not be removed")
	}
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{})}
	replacement, installed := f.registry.inner.Insert(f.key(), registry.Admission{
		AgentID: testAgent, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
	})
	if !installed || replacement.Generation == f.entry.Generation {
		t.Fatal("the replacement was not installed under a new generation")
	}
	publishedBefore := len(f.locations.publishedAll())

	if !f.pulseExpectingExit(t) {
		t.Fatal("the heartbeat kept beating for a residency that has been replaced under it")
	}
	f.awaitExit(t)

	if published := len(f.locations.publishedAll()); published != publishedBefore {
		t.Errorf("%d observations were published for a generation that is gone", published-publishedBefore)
	}
	if taken := f.teardown.handedOver(); len(taken) != 0 {
		t.Errorf("a replaced residency was handed to the teardown observer %d times; it is not this heartbeat's to tear down", len(taken))
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.Generation != replacement.Generation || !entry.Accepting {
		t.Error("the replacement residency was disturbed by the old heartbeat leaving")
	}
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

// TestReleaseMarksReleasingPublishesItAndTombstones is §9.3's ordering for the
// durable-location half, and the ORDER is the contract.
//
// Marking the entry releasing before publishing is what stops admission before
// anything advertises a route that will not serve; tombstoning after publishing
// releasing is what leaves a reader with a reason rather than a silence.
func TestReleaseMarksReleasingPublishesItAndTombstones(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	before := f.trace.recorded()

	if err := f.beat.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	got := f.trace.recorded()[len(before):]
	want := []string{"registry.releasing", "location.publish:releasing", "location.tombstone", "registry.remove"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("release did\n  %v\nwant\n  %v", got, want)
	}
	published := f.locations.publishedAll()
	releasing := published[len(published)-1]
	if releasing.Residency != sessionwire.SessionResidencyReleasing {
		t.Errorf("the release published residency %q, want %q", releasing.Residency, sessionwire.SessionResidencyReleasing)
	}
	if releasing.Accepting {
		t.Error("the release published Accepting true")
	}
	if releasing.LeaseEpoch != testEpoch {
		t.Errorf("the release published under epoch %d, want the held %d", releasing.LeaseEpoch, testEpoch)
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds the residency after release")
	}
	// The loop is gone, and it is gone BEFORE the writes: a beat racing the
	// release would publish `resident` after `releasing`.
	select {
	case <-f.beat.done:
	default:
		t.Error("the heartbeat loop is still running after release")
	}
	if f.trace.indexOf("location.publish:resident") > f.trace.indexOf("location.publish:releasing") {
		t.Error("a resident observation was published after the releasing one")
	}
}

// TestReleaseWritesTheExpiredEpochTombstoneAgainstItsOwnHighWater is step 3.
//
// The epoch is 37 while the host generation is 41 and the registry generation
// is 1, so a tombstone stamped with the wrong one of the three is visible. A
// fixture whose numbers coincided is how this task lost a probe once already.
func TestReleaseWritesTheExpiredEpochTombstoneAgainstItsOwnHighWater(t *testing.T) {
	f := newHeartbeatFixture(t)
	if err := f.beat.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	tombstones := f.locations.tombstones()
	if len(tombstones) != 1 {
		t.Fatalf("%d tombstones were written, want 1", len(tombstones))
	}
	if tombstones[0] != testEpoch {
		t.Errorf("the tombstone was stamped with %d, want the lease epoch %d (the host generation is %d and the registry generation is %d)",
			tombstones[0], testEpoch, testGeneration, f.entry.Generation)
	}
}

// TestNothingCanRemoveALocationWithoutItsEpoch is step 3's other half — "never
// erase the high-water" — held STRUCTURALLY, because it is a property of the
// surface rather than of one call.
//
// §10.1: deleting a logical record never deletes its fencing high-water mark,
// so cleanup writes an epoch-fenced tombstone rather than erasing the record. A
// Locations that offered an unfenced delete would let a correct caller do the
// wrong thing, and no behavioural test over the correct caller could see it.
func TestNothingCanRemoveALocationWithoutItsEpoch(t *testing.T) {
	locations := reflect.TypeOf((*Locations)(nil)).Elem()
	if locations.NumMethod() == 0 {
		t.Fatal("Locations has no methods, so this guard examines nothing")
	}
	for i := range locations.NumMethod() {
		method := locations.Method(i)
		if !removesALocation(method.Name) {
			continue
		}
		if !takesAnEpoch(method.Type) {
			t.Errorf("Locations.%s removes a location and takes no lease epoch; §10.1 removes a route with a fenced tombstone and never by erasing the fencing high-water mark", method.Name)
		}
	}

	// The detector, on a synthetic surface that breaks the rule, and a control
	// one position over. Locations already satisfies the rule, so without these
	// the assertion above would pass for a check that returned early.
	violating := reflect.TypeOf((*erasableLocations)(nil)).Elem()
	found := 0
	for i := range violating.NumMethod() {
		method := violating.Method(i)
		if removesALocation(method.Name) && !takesAnEpoch(method.Type) {
			found++
		}
	}
	if found != 1 {
		t.Errorf("the detector found %d unfenced removals on a surface carrying exactly one, want 1", found)
	}
	control := reflect.TypeOf((*fencedLocations)(nil)).Elem()
	for i := range control.NumMethod() {
		method := control.Method(i)
		if removesALocation(method.Name) && !takesAnEpoch(method.Type) {
			t.Errorf("the detector reported %s on a surface whose removal IS fenced", method.Name)
		}
	}
}

// removesALocation reports whether a method name describes removing a record.
func removesALocation(name string) bool {
	for _, verb := range []string{"Delete", "Remove", "Clear", "Erase", "Tombstone", "Expire"} {
		if strings.HasPrefix(name, verb) {
			return true
		}
	}
	return false
}

// takesAnEpoch reports whether a method signature carries a lease epoch.
func takesAnEpoch(signature reflect.Type) bool {
	for i := range signature.NumIn() {
		if signature.In(i).Kind() == reflect.Uint64 {
			return true
		}
	}
	return false
}

// erasableLocations is the SYNTHETIC FIXTURE that violates the rule, because
// Locations satisfies it and a guard whose only subject already passes has an
// untested detector by construction.
type erasableLocations interface {
	PublishResidency(context.Context, sessionwire.HostLinkRegistryObservation) error
	DeleteResidency(context.Context, sessionwire.TenantID, sessionwire.SessionID) error
}

// fencedLocations is the control one position over: the same removal, fenced.
type fencedLocations interface {
	PublishResidency(context.Context, sessionwire.HostLinkRegistryObservation) error
	DeleteResidency(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64) error
}

// TestAReleaseThatCannotWriteNamesWhatItOwed is the honesty AttachError.Unreleased
// established, applied to the other durable path: a release that reports
// success while leaving a live route with no owner is the worst outcome
// available, and it must not be indistinguishable from a clean one.
func TestAReleaseThatCannotWriteNamesWhatItOwed(t *testing.T) {
	stuck := errors.New("the store is unreachable")
	f := newHeartbeatFixture(t, func(f *heartbeatFixture) {
		f.locations.publishErr = stuck
		f.locations.tombstoneErr = stuck
	})

	err := f.beat.Release(context.Background())
	if err == nil {
		t.Fatal("a release that wrote neither the releasing observation nor the tombstone reported success")
	}
	var refused *ReleaseError
	if !errors.As(err, &refused) {
		t.Fatalf("error is %T, want *ReleaseError", err)
	}
	for _, want := range []string{"releasing observation", "residency tombstone"} {
		found := false
		for _, unwritten := range refused.Unwritten {
			if strings.HasPrefix(unwritten, want+": ") {
				found = true
			}
		}
		if !found {
			t.Errorf("the failure does not name %q among what it could not write: %v", want, refused.Unwritten)
		}
	}
	// It CONTINUES past a failed write, for the reason the attach rollback
	// does: abandoning the rest turns one stale record into two.
	if f.trace.indexOf("location.tombstone") < 0 {
		t.Error("a refused releasing observation abandoned the tombstone")
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("a release that could not write also failed to drop the local entry, which cannot fail")
	}
	// A REPEAT RETURNS THE FIRST VERDICT and writes nothing further. Without
	// this row a Release that re-attempted after a failure would look
	// identical, and it would re-attempt against a residency whose local entry
	// is already gone — finding nothing to release and reporting success, which
	// converts a stale durable route into a clean bill of health.
	writes := len(f.trace.recorded())
	again := f.beat.Release(context.Background())
	if !errors.Is(again, err) {
		t.Errorf("a repeated release reported %v, want the first verdict %v", again, err)
	}
	if len(f.trace.recorded()) != writes {
		t.Errorf("a repeated release made further calls: %v", f.trace.recorded()[writes:])
	}

	// The CONTROL: a store that answers leaves nothing named, and a repeat of
	// THAT is silent too.
	g := newHeartbeatFixture(t)
	if err := g.beat.Release(context.Background()); err != nil {
		t.Errorf("a clean release reported %v", err)
	}
	if err := g.beat.Release(context.Background()); err != nil {
		t.Errorf("a repeated clean release reported %v", err)
	}
}

// ---------------------------------------------------------------------------
// Stop
// ---------------------------------------------------------------------------

func TestStopEndsTheLoopWritesNothingAndIsIdempotent(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	before := f.trace.recorded()

	for range 3 {
		if err := f.beat.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
	requireSteps(t, f.trace.recorded(), before)
	if _, held := f.registry.Get(f.key()); !held {
		t.Error("Stop removed the residency; the caller that stops a heartbeat either rolls back an attach or releases, and both own their own cleanup")
	}
	if taken := f.teardown.handedOver(); len(taken) != 0 {
		t.Errorf("Stop handed %d residencies to the teardown observer", len(taken))
	}
}

func TestCancellingTheSessionContextEndsTheLoopWithoutTearingDown(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.pulse(t)
	before := f.trace.recorded()

	f.cancel()
	f.awaitExit(t)

	requireSteps(t, f.trace.recorded(), before)
	if taken := f.teardown.handedOver(); len(taken) != 0 {
		t.Errorf("cancelling the session context tore down %d residencies; a process ending is not a lease being lost", len(taken))
	}
	if entry, held := f.registry.Get(f.key()); !held || !entry.Accepting {
		t.Error("cancelling the session context stopped admission")
	}
}

// ---------------------------------------------------------------------------
// The seam the Manager consumes
// ---------------------------------------------------------------------------

// TestTheManagerStartsARealHeartbeat wires the real ownership into the real
// Manager, which is the only thing that proves the two halves fit.
func TestTheManagerStartsARealHeartbeat(t *testing.T) {
	f := newFixture(t)
	ownership, err := NewHeartbeatOwnership(HeartbeatOptions{
		Host:           f.host,
		HostGeneration: testGeneration,
		Registry:       f.registry,
		Locations:      f.locations,
		Drain:          f.publisher,
		Teardown:       &fakeTeardown{},
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}
	manager, err := NewManager(Options{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Admissions: f.admissions, Leases: f.leases, Journal: f.journal,
		Durable: f.durable, Workspaces: f.workspaces, Locations: f.locations,
		Ownership: ownership,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(manager.Close)

	residency, err := manager.Attach(context.Background(), f.request(ModeRestore))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	manager.mu.Lock()
	record := manager.sessions[residency.Key]
	manager.mu.Unlock()
	if record == nil {
		t.Fatal("no session record")
	}
	beat, isHeartbeat := record.ownership.(*Heartbeat)
	if !isHeartbeat {
		t.Fatalf("the residency's ownership is %T, want *Heartbeat", record.ownership)
	}
	if beat.epoch != residency.LeaseEpoch {
		t.Errorf("the heartbeat runs under epoch %d and the residency reports %d", beat.epoch, residency.LeaseEpoch)
	}
	if beat.lease == nil {
		t.Fatal("the heartbeat was handed no lease, so it can never observe Lost()")
	}
	if beat.lease.Epoch() != residency.LeaseEpoch {
		t.Errorf("the heartbeat holds a lease of epoch %d, want the residency's %d", beat.lease.Epoch(), residency.LeaseEpoch)
	}
	if err := beat.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestBeginOwnershipRefusesARequestWithoutItsLease(t *testing.T) {
	f := newHeartbeatFixture(t)
	ownership, err := NewHeartbeatOwnership(HeartbeatOptions{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Locations: f.locations, Drain: f.publisher, Teardown: f.teardown,
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}
	handle, err := ownership.BeginOwnership(context.Background(), OwnershipRequest{
		Key: f.key(), AgentID: testAgent, CompatibilityID: testCompat,
		LeaseEpoch: testEpoch, Generation: f.entry.Generation, Runtime: f.runtime,
	})
	if err == nil {
		_ = handle.Stop(context.Background())
		t.Fatal("ownership began without a lease, so nothing could ever observe Lost()")
	}
	if handle != nil {
		t.Error("a refused BeginOwnership returned a handle")
	}
	var invalid *InvalidManagerOptionsError
	if !errors.As(err, &invalid) || invalid.Field != "Lease" {
		t.Errorf("error is %v, want an *InvalidManagerOptionsError naming Lease", err)
	}
}

func TestNewHeartbeatOwnershipRefusesAnIncompleteConfiguration(t *testing.T) {
	f := newHeartbeatFixture(t)
	complete := HeartbeatOptions{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Locations: f.locations, Drain: f.publisher, Teardown: f.teardown,
	}
	if _, err := NewHeartbeatOwnership(complete); err != nil {
		t.Fatalf("the complete configuration was refused: %v", err)
	}
	for _, row := range []struct {
		field  string
		remove func(*HeartbeatOptions)
	}{
		{"Host", func(o *HeartbeatOptions) { o.Host = nil }},
		{"HostGeneration", func(o *HeartbeatOptions) { o.HostGeneration = 0 }},
		{"Registry", func(o *HeartbeatOptions) { o.Registry = nil }},
		{"Locations", func(o *HeartbeatOptions) { o.Locations = nil }},
		{"Drain", func(o *HeartbeatOptions) { o.Drain = nil }},
		{"Teardown", func(o *HeartbeatOptions) { o.Teardown = nil }},
	} {
		t.Run(row.field, func(t *testing.T) {
			options := complete
			row.remove(&options)
			ownership, err := NewHeartbeatOwnership(options)
			if err == nil {
				t.Fatalf("a configuration without %s was accepted", row.field)
			}
			if ownership != nil {
				t.Error("a refused configuration returned an ownership")
			}
			var invalid *InvalidManagerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("error is %T, want *InvalidManagerOptionsError", err)
			}
			if invalid.Field != row.field {
				t.Errorf("the refusal names field %q, want %q", invalid.Field, row.field)
			}
		})
	}
}

// TestALeaseLostMidBeatIsNotPublished closes the window between reading the
// registry and writing the record.
//
// A LOOP-LEVEL re-check only stops a beat that was DUE at the moment the lease
// closed; it says nothing about a lease lost while a beat is already under way,
// and that window contains the read of the registry. Neither window can be hit
// by racing two goroutines and hoping — this program has shipped exactly one
// coin-flip test — so the loss is injected at the one point inside a beat a
// test can name: the registry read.
func TestALeaseLostMidBeatIsNotPublished(t *testing.T) {
	f := newHeartbeatFixture(t)
	f.registry.afterGet = func() { close(f.lease.lost) }

	if !f.pulseExpectingExit(t) {
		t.Fatal("the heartbeat scheduled another beat after losing its lease mid-beat")
	}
	f.awaitExit(t)

	if published := f.locations.publishedAll(); len(published) != 0 {
		t.Errorf("%d observations were published by a beat whose lease was lost before the write; a fenced record must never receive a write from a holder that has lost its grant", len(published))
	}
	handed := f.teardown.handedOver()
	if len(handed) != 1 || handed[0].Reason != LossReasonLeaseLost {
		t.Errorf("the mid-beat loss handed over %d residencies %v, want exactly one lease_lost", len(handed), handed)
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.Accepting {
		t.Error("admission is still open after a mid-beat lease loss")
	}
}

// TestStopAndReleaseAreSafeTogether covers the one genuinely concurrent pair
// this type has: an attach rolling back calls Stop through OwnershipHandle
// while a release calls Release, and both may arrive at once because they come
// from different owners — O3.1's unwinder and O6.1's release protocol.
func TestStopAndReleaseAreSafeTogether(t *testing.T) {
	f := newHeartbeatFixture(t)

	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(4)
	errs := make([]error, 4)
	for i := range 4 {
		go func() {
			defer done.Done()
			<-start
			if i%2 == 0 {
				errs[i] = f.beat.Stop(context.Background())
				return
			}
			errs[i] = f.beat.Release(context.Background())
		}()
	}
	close(start)
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d reported %v", i, err)
		}
	}
	// Exactly one release did the durable work; the other found the residency
	// already gone and wrote nothing, which is what the generation check on
	// MarkReleasing buys.
	if tombstones := f.locations.tombstones(); len(tombstones) != 1 {
		t.Errorf("%d tombstones were written by two concurrent releases, want 1", len(tombstones))
	}
	if published := f.locations.publishedAll(); len(published) != 1 {
		t.Errorf("%d observations were published, want the single releasing one", len(published))
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the residency survived the release")
	}
}
