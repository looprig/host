package residency

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
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

// The Host's ONE ledger satisfies Admissions — the drain flag included — which
// is the compile-time half of not building a second one. The heartbeat and the
// Manager name the SAME interface, so a composition cannot hand them different
// objects without noticing.
var _ Admissions = (*service.CapacityPublisher)(nil)

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const (
	// testEpoch is the RESIDENCY epoch every heartbeat fixture runs under. It is
	// DELIBERATELY NEITHER 0 NOR 1, and neither is the host generation or the
	// registry generation it must not be confused with: this task has already
	// lost one probe to a fence stamped with a literal that equalled the
	// fixture's first epoch. 37, 41 and 1 are three distinct numbers, so a
	// tombstone or an observation carrying the wrong one is visible.
	//
	// ITS TYPE IS THE FOURTH THING IT MUST NOT BE CONFUSED WITH. The runtime's
	// JOURNAL epoch is a different grant from a different issuer; testkit mints
	// those from 1. Since O3.4 the distinction is carried by the type as well as
	// by the number, so a heartbeat that published a journal epoch as the
	// registry route's lease_epoch does not compile.
	testEpoch ResidencyEpoch = 37

	testHeartbeatInterval = 5 * time.Second
)

type heartbeatFixture struct {
	t         *testing.T
	trace     *trace
	clock     *manualClock
	host      *hostconfig.Host
	registry  *spyRegistry
	locations *fakeLocations
	publisher *service.CapacityPublisher
	teardown  *fakeTeardown
	// fence is the grant's fence, held so a test can end it from the OUTSIDE —
	// which is what the Manager does when one of its own fenced writes is
	// refused while this heartbeat is running.
	fence *epochFence
	// tombstoneErr is applied after the fixture builds, so a test can fail the
	// tombstone while the releasing observation still succeeds.
	tombstoneErr error
	lease        *fakeLease
	runtime      *fakeRuntime
	entry        registry.Entry
	beat         *Heartbeat
	cancel       context.CancelFunc
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
	f.runtime = &fakeRuntime{trace: steps, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}

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
	built, err := hostconfig.New(hostconfig.Options{
		HostID:            testHost,
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
		t.Fatalf("hostconfig.New: %v", err)
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
		LeaseEpoch:      uint64(testEpoch),
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
		Admissions:     publisher,
		Teardown:       f.teardown,
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}

	f.fence = newEpochFence(f.lease)
	if f.tombstoneErr != nil {
		f.locations.tombstoneErr = f.tombstoneErr
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
		Fence:           f.fence,
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

// awaitExit blocks until the loop has left AND any teardown handoff has
// returned.
//
// IT WAITS ON settled, NOT done, and the difference is a race this fixture
// would otherwise have. done closes before the observer runs — deliberately, so
// a blocking observer cannot wedge the handle — so a test asserting what was
// handed over must wait for the handoff rather than for the loop.
func (f *heartbeatFixture) awaitExit(t *testing.T) {
	t.Helper()
	select {
	case <-f.beat.settled:
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
			LeaseEpoch:             uint64(testEpoch),
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
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
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

	if err := f.beat.BeginRelease(context.Background()); err != nil {
		t.Fatalf("BeginRelease: %v", err)
	}
	// THE SEAM O6.1 CHECKPOINTS IN. Everything up to here must have happened
	// before a checkpoint runs; nothing after it may have.
	begun := f.trace.recorded()[len(before):]
	if wantBegun := []string{"registry.releasing", "location.publish:releasing"}; !reflect.DeepEqual(begun, wantBegun) {
		t.Fatalf("BeginRelease did\n  %v\nwant\n  %v", begun, wantBegun)
	}
	if entry, held := f.registry.Get(f.key()); !held || entry.Accepting || entry.State != registry.StateReleasing {
		t.Error("admission is still open after BeginRelease; §9.3 stops it before the checkpoint, not after")
	}
	// The loop is STILL RUNNING, and its beats now carry the releasing state. A
	// heartbeat that stopped here would let the route expire mid-checkpoint and
	// read as cold while this Host still holds the lease and the runtime.
	select {
	case <-f.beat.done:
		t.Fatal("the heartbeat stopped at BeginRelease; a checkpoint can outlast the registry expiry")
	default:
	}
	f.pulse(t)
	beating := f.locations.publishedAll()
	mid := beating[len(beating)-1]
	if mid.Residency != sessionwire.SessionResidencyReleasing || mid.Accepting {
		t.Errorf("a beat during the checkpoint window published residency %q accepting %t, want releasing and false", mid.Residency, mid.Accepting)
	}

	beforeFinish := len(f.trace.recorded())
	if err := f.beat.FinishRelease(context.Background()); err != nil {
		t.Fatalf("FinishRelease: %v", err)
	}
	finished := f.trace.recorded()[beforeFinish:]
	if wantFinished := []string{"location.tombstone", "registry.remove"}; !reflect.DeepEqual(finished, wantFinished) {
		t.Fatalf("FinishRelease did\n  %v\nwant\n  %v", finished, wantFinished)
	}
	published := f.locations.publishedAll()
	releasing := published[len(published)-1]
	if releasing.Residency != sessionwire.SessionResidencyReleasing {
		t.Errorf("the release published residency %q, want %q", releasing.Residency, sessionwire.SessionResidencyReleasing)
	}
	if releasing.Accepting {
		t.Error("the release published Accepting true")
	}
	if releasing.LeaseEpoch != uint64(testEpoch) {
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
	if err := f.beat.BeginRelease(context.Background()); err != nil {
		t.Fatalf("BeginRelease: %v", err)
	}
	if err := f.beat.FinishRelease(context.Background()); err != nil {
		t.Fatalf("FinishRelease: %v", err)
	}
	tombstones := f.locations.tombstones()
	if len(tombstones) != 1 {
		t.Fatalf("%d tombstones were written, want 1", len(tombstones))
	}
	if tombstones[0] != uint64(testEpoch) {
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

	begun := f.beat.BeginRelease(context.Background())
	err := f.beat.FinishRelease(context.Background())
	if begun == nil || err == nil {
		t.Fatalf("a release that wrote neither the releasing observation (%v) nor the tombstone (%v) reported success", begun, err)
	}
	for _, row := range []struct {
		half   error
		wanted string
	}{
		{begun, "releasing observation"},
		{err, "residency tombstone"},
	} {
		var refused *ReleaseError
		if !errors.As(row.half, &refused) {
			t.Fatalf("error is %T, want *ReleaseError", row.half)
		}
		found := false
		for _, unwritten := range refused.Unwritten {
			if strings.HasPrefix(unwritten, row.wanted+": ") {
				found = true
			}
		}
		if !found {
			t.Errorf("the failure does not name %q among what it could not write: %v", row.wanted, refused.Unwritten)
		}
	}
	// A refused FIRST half does not abandon the second, for the reason the
	// attach rollback continues: abandoning the rest turns one stale record
	// into two.
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
	again := f.beat.FinishRelease(context.Background())
	if !errors.Is(again, err) {
		t.Errorf("a repeated release reported %v, want the first verdict %v", again, err)
	}
	if len(f.trace.recorded()) != writes {
		t.Errorf("a repeated release made further calls: %v", f.trace.recorded()[writes:])
	}

	// The CONTROL: a store that answers leaves nothing named, and a repeat of
	// THAT is silent too.
	g := newHeartbeatFixture(t)
	if err := g.beat.BeginRelease(context.Background()); err != nil {
		t.Errorf("a clean BeginRelease reported %v", err)
	}
	if err := g.beat.FinishRelease(context.Background()); err != nil {
		t.Errorf("a clean FinishRelease reported %v", err)
	}
	if err := g.beat.FinishRelease(context.Background()); err != nil {
		t.Errorf("a repeated clean FinishRelease reported %v", err)
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
		Admissions:     f.publisher,
		Teardown:       &fakeTeardown{},
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}
	manager, err := NewManager(Options{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Admissions: f.admissions, Leases: f.leases,
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
	if beat.fence == nil {
		t.Fatal("the heartbeat was handed no fence, so it can never observe that its grant is gone")
	}
	if beat.fence.lease == nil || beat.fence.lease.Epoch() != residency.LeaseEpoch {
		t.Errorf("the heartbeat's fence holds %v, want a lease of the residency's epoch %d", beat.fence.lease, residency.LeaseEpoch)
	}
	if err := beat.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestBeginOwnershipRefusesARequestWithoutItsFence(t *testing.T) {
	f := newHeartbeatFixture(t)
	ownership, err := NewHeartbeatOwnership(HeartbeatOptions{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Locations: f.locations, Admissions: f.publisher, Teardown: f.teardown,
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
		t.Fatal("ownership began without a fence, so nothing could ever observe that the grant was gone")
	}
	if handle != nil {
		t.Error("a refused BeginOwnership returned a handle")
	}
	var invalid *InvalidManagerOptionsError
	if !errors.As(err, &invalid) || invalid.Field != "Fence" {
		t.Errorf("error is %v, want an *InvalidManagerOptionsError naming Fence", err)
	}
}

func TestNewHeartbeatOwnershipRefusesAnIncompleteConfiguration(t *testing.T) {
	f := newHeartbeatFixture(t)
	complete := HeartbeatOptions{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Locations: f.locations, Admissions: f.publisher, Teardown: f.teardown,
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
		{"Admissions", func(o *HeartbeatOptions) { o.Admissions = nil }},
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
			if err := f.beat.BeginRelease(context.Background()); err != nil {
				errs[i] = err
				return
			}
			errs[i] = f.beat.FinishRelease(context.Background())
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

// TestALostLeaseWritesNothingThroughTheHandleEither is F1: the rule that a
// holder which has lost its grant must not touch a fenced record was enforced
// inside the loop's own path and NOWHERE ELSE.
//
// It is reachable through the intended O6.1 route rather than by contrivance:
// LostResidency carries no handle, but Manager.sessions[key].ownership does, so
// a teardown owner releasing residency after a loss goes straight through these
// methods. Before this, they consulted the generation and never the lease, and
// wrote a `releasing` observation and a tombstone stamped with an epoch this
// Host no longer held — returning nil. A real store fences both; the whole
// argument for writing nothing after a loss is that the holder must not ATTEMPT
// the write.
func TestALostLeaseWritesNothingThroughTheHandleEither(t *testing.T) {
	for _, row := range []struct {
		name   string
		reason LossReason
		build  func(*heartbeatFixture)
		end    func(*testing.T, *heartbeatFixture)
	}{
		{
			name:   "the lease reports itself lost",
			reason: LossReasonLeaseLost,
			build:  func(*heartbeatFixture) {},
			end:    func(_ *testing.T, f *heartbeatFixture) { close(f.lease.lost) },
		},
		{
			// THE PATH THAT HAD NO ROW, and the stronger of the two. Lost()
			// fires on renewal failure or expiry and does not prove a successor
			// exists; a refused fenced write IS that proof. A handle consulting
			// only the channel went on writing under an epoch it had been told
			// was superseded — strictly worse than the case the channel covers.
			name:   "a fenced write is refused for a later epoch",
			reason: LossReasonEpochSuperseded,
			build: func(f *heartbeatFixture) {
				f.locations.publishErr = fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
			},
			end: func(t *testing.T, f *heartbeatFixture) {
				if !f.pulseExpectingExit(t) {
					t.Fatal("the heartbeat kept beating after a fenced write was refused")
				}
				// Cleared, so an attempted write would actually LAND. Leaving
				// it set would make "nothing was published" true for the wrong
				// reason.
				f.locations.mu.Lock()
				f.locations.publishErr = nil
				f.locations.mu.Unlock()
			},
		},
		{
			// THE THIRD PATH. §10.1 has two fencing mechanisms and a store may
			// surface both through the same call, so a location write refused
			// by a successor's committed sequence is inside the contract. It
			// matters because the beat used to re-derive the classification
			// with a NARROWER test than the fence's — only ErrEpochSuperseded —
			// so this arrived as a store failure, ConsecutiveFailures counted a
			// run that was not one, and the operator was eventually told
			// lease_lost for a conflict that was not that. Asking the fence
			// what it recorded is what makes the three agree.
			name:   "a fenced write is refused by a later owner's sequence",
			reason: LossReasonFenceConflict,
			build: func(f *heartbeatFixture) {
				f.locations.publishErr = fmt.Errorf("sessionstore: %w", ErrFenceConflict)
			},
			end: func(t *testing.T, f *heartbeatFixture) {
				if !f.pulseExpectingExit(t) {
					t.Fatal("the heartbeat kept beating after a fenced write was refused by a later owner's sequence")
				}
				if failures := f.beat.ConsecutiveFailures(); failures != 0 {
					t.Errorf("a refused fence was counted as %d store failures; it is not ambiguity", failures)
				}
				f.locations.mu.Lock()
				f.locations.publishErr = nil
				f.locations.mu.Unlock()
			},
		},
	} {
		t.Run(row.name, func(t *testing.T) { runsNoWriteAfterLoss(t, row.reason, row.build, row.end) })
	}
}

func runsNoWriteAfterLoss(t *testing.T, reason LossReason, build func(*heartbeatFixture), end func(*testing.T, *heartbeatFixture)) {
	t.Helper()
	f := newHeartbeatFixture(t, build)
	end(t, f)
	f.awaitExit(t)
	if handed := f.teardown.handedOver(); len(handed) != 1 || handed[0].Reason != reason {
		t.Fatalf("the loss handed over %v, want exactly one %q", handed, reason)
	}
	// The loop surrendered: the entry is owned for teardown and still present,
	// which is exactly the state O6.1 picks the handle up in.
	if entry, held := f.registry.Get(f.key()); !held || !entry.TeardownOwned {
		t.Fatal("the loss did not leave the residency claimed for teardown, so this test never reached its subject")
	}
	before := f.trace.recorded()

	begun := f.beat.BeginRelease(context.Background())
	finished := f.beat.FinishRelease(context.Background())

	for _, row := range []struct {
		half error
		name string
	}{{begun, "BeginRelease"}, {finished, "FinishRelease"}} {
		if row.half == nil {
			t.Errorf("%s reported success after the lease was lost", row.name)
			continue
		}
		if !errors.Is(row.half, ErrLeaseNotHeld) {
			t.Errorf("%s reported %v, want an error unwrapping to ErrLeaseNotHeld", row.name, row.half)
		}
		var refused *ReleaseError
		if !errors.As(row.half, &refused) {
			t.Errorf("%s reported %T, want *ReleaseError", row.name, row.half)
		}
	}
	for _, forbidden := range []string{"location.publish:releasing", "location.tombstone"} {
		for _, step := range f.trace.recorded()[len(before):] {
			if step == forbidden {
				t.Errorf("%q was written under a lost lease", forbidden)
			}
		}
	}
	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published after the lease was lost", published)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("a tombstone was written under a lost lease: %v", tombstones)
	}
	// The LOCAL entry still goes: leaving it would make this Host believe it
	// holds a session it has released.
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds a residency that has been released")
	}

	// The CONTROL, one position over: the same two calls with the lease still
	// held write both records. Without this the assertions above would pass for
	// a release that never wrote anything at all.
	g := newHeartbeatFixture(t)
	if err := g.beat.BeginRelease(context.Background()); err != nil {
		t.Fatalf("the control's BeginRelease reported %v", err)
	}
	if err := g.beat.FinishRelease(context.Background()); err != nil {
		t.Fatalf("the control's FinishRelease reported %v", err)
	}
	if len(g.locations.publishedAll()) != 1 || len(g.locations.tombstones()) != 1 {
		t.Errorf("the control published %d observations and %d tombstones, want 1 and 1",
			len(g.locations.publishedAll()), len(g.locations.tombstones()))
	}
}

// TestAReplacedResidencyIsNotReleasedByTheOldHandle is F3, and it is a row away
// from TestAReplacedResidencyStopsItsHeartbeat's fixture: the same setup, the
// other entry point.
//
// A release that ignored the generation would tombstone under this heartbeat's
// epoch and remove a route the REPLACEMENT installed — taking a live session
// off the map to clean up one that is already gone.
func TestAReplacedResidencyIsNotReleasedByTheOldHandle(t *testing.T) {
	f := newHeartbeatFixture(t)
	if !f.registry.inner.RemoveByGeneration(f.key(), f.entry.Generation) {
		t.Fatal("the seeded residency could not be removed")
	}
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	replacement, installed := f.registry.inner.Insert(f.key(), registry.Admission{
		AgentID: testAgent, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
	})
	if !installed || replacement.Generation == f.entry.Generation {
		t.Fatal("the replacement was not installed under a new generation")
	}

	// REPORTED, NOT COLLAPSED TO NIL. §9.3 makes step 1 a precondition for
	// step 3, so a caller reading nil from BeginRelease and going on to
	// checkpoint would be checkpointing a residency whose admission this handle
	// never stopped, because there was nothing here to stop. Success and no-op
	// must not be indistinguishable — the argument this file already makes for
	// ReleaseError, applied to the one return that did not follow it.
	for _, row := range []struct {
		name string
		call func() error
	}{
		{"BeginRelease", func() error { return f.beat.BeginRelease(context.Background()) }},
		{"FinishRelease", func() error { return f.beat.FinishRelease(context.Background()) }},
	} {
		err := row.call()
		if !errors.Is(err, ErrNothingToRelease) {
			t.Errorf("%s reported %v for a residency that is not its own, want an error unwrapping to ErrNothingToRelease", row.name, err)
		}
		var refused *ReleaseError
		if !errors.As(err, &refused) {
			t.Errorf("%s reported %T, want *ReleaseError", row.name, err)
		}
	}

	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published for a generation that is gone", published)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("the old handle tombstoned the replacement's route: %v", tombstones)
	}
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("the old handle removed the replacement residency")
	}
	if entry.Generation != replacement.Generation || entry.State != registry.StateResident || !entry.Accepting {
		t.Errorf("the replacement was disturbed: %+v", entry)
	}
}

// blockingTeardown never returns from ResidencyLost.
type blockingTeardown struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingTeardown) ResidencyLost(context.Context, LostResidency) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
}

// TestABlockingTeardownObserverDoesNotWedgeTheHandle is F5.
//
// The observer is arbitrary caller code called on the heartbeat's own
// goroutine. Stop waits for the loop to leave and FinishRelease calls Stop, so
// an observer that blocked used to wedge every caller of this handle —
// including O3.1's attach rollback, which is the one path that must never hang.
// The handle is freed before the observer runs, so a wedged observer now costs
// one goroutine and nothing else.
func TestABlockingTeardownObserverDoesNotWedgeTheHandle(t *testing.T) {
	blocking := &blockingTeardown{entered: make(chan struct{}), release: make(chan struct{})}
	// The fixture's own heartbeat is left alone; this drives a SECOND handle
	// over the same collaborators, because the property under test is about
	// the handle a wedged observer belongs to.
	f := newHeartbeatFixture(t)
	if err := f.beat.Stop(context.Background()); err != nil {
		t.Fatalf("stopping the fixture's own heartbeat: %v", err)
	}
	ownership, err := NewHeartbeatOwnership(HeartbeatOptions{
		Host: f.host, HostGeneration: testGeneration, Registry: f.registry,
		Locations: f.locations, Admissions: f.publisher, Teardown: blocking,
	})
	if err != nil {
		t.Fatalf("NewHeartbeatOwnership: %v", err)
	}
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("no seeded residency")
	}
	lease := &fakeLease{trace: f.trace, epoch: testEpoch, lost: make(chan struct{})}
	handle, err := ownership.BeginOwnership(context.Background(), OwnershipRequest{
		Key: f.key(), AgentID: testAgent, CompatibilityID: testCompat,
		LeaseEpoch: testEpoch, Generation: entry.Generation, Runtime: f.runtime, Fence: newEpochFence(lease),
	})
	if err != nil {
		t.Fatalf("BeginOwnership: %v", err)
	}
	defer close(blocking.release)

	close(lease.lost)
	<-blocking.entered

	// THE TWO SIGNALS MEAN DIFFERENT THINGS, and with the observer provably
	// inside its call the difference is decidable rather than a race: done is
	// closed because the loop has left, and settled is NOT because the handoff
	// has not returned. A settled that closed with done would let awaitExit
	// outrun the handoff, and every assertion about what was handed over would
	// pass or fail on scheduling.
	beating, isHeartbeat := handle.(*Heartbeat)
	if !isHeartbeat {
		t.Fatalf("BeginOwnership returned %T, want *Heartbeat", handle)
	}
	select {
	case <-beating.done:
	default:
		t.Error("done is still open while the loop has left; the handle is being held by the observer")
	}
	select {
	case <-beating.settled:
		t.Error("settled closed while the teardown observer is still inside its call, so a test waiting on it can outrun the handoff it is asserting about")
	default:
	}

	// The observer is wedged. Stop must still return, because the loop has
	// already left; without that ordering this receive never completes and the
	// harness kills the run instead of reporting anything.
	stopped := make(chan error, 1)
	go func() { stopped <- handle.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Stop reported %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stop is wedged behind a blocking teardown observer, and so is every other caller of this handle")
	}
}

// TestARivalTeardownClaimIsNotReleasedThroughThisHandle reads the ownership
// this package built BeginTeardown to record.
//
// registry.BeginTeardown is single-owner and its ownership is RECORDED rather
// than inferred from a re-enterable state, and this package's own comments
// argue that is precisely so a second owner need not be guessed at. It then did
// not read it: with a rival holding the claim, both halves returned nil,
// published, tombstoned at the held epoch, and removed the local entry out from
// under the claimed owner.
func TestARivalTeardownClaimIsNotReleasedThroughThisHandle(t *testing.T) {
	f := newHeartbeatFixture(t)
	// A drain supervisor or a reconciler claims teardown while this heartbeat
	// still holds the lease.
	if _, won := f.registry.inner.BeginTeardown(f.key(), f.entry.Generation); !won {
		t.Fatal("the rival could not claim teardown, so this test never reached its subject")
	}
	before := f.trace.recorded()

	begun := f.beat.BeginRelease(context.Background())
	finished := f.beat.FinishRelease(context.Background())

	for _, row := range []struct {
		half error
		name string
	}{{begun, "BeginRelease"}, {finished, "FinishRelease"}} {
		if !errors.Is(row.half, ErrTeardownOwnedElsewhere) {
			t.Errorf("%s reported %v, want an error unwrapping to ErrTeardownOwnedElsewhere", row.name, row.half)
		}
		var refused *ReleaseError
		if !errors.As(row.half, &refused) {
			t.Errorf("%s reported %T, want *ReleaseError", row.name, row.half)
		}
	}
	if steps := f.trace.recorded()[len(before):]; len(steps) != 0 {
		t.Errorf("a handle whose residency is claimed by another owner did %v", steps)
	}
	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published under a rival's teardown claim", published)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("the rival's route was tombstoned: %v", tombstones)
	}
	if _, held := f.registry.Get(f.key()); !held {
		t.Error("the local entry was removed out from under the claimed teardown owner")
	}

	// THE CONTROL, one position over, AND IT PINS THE ORDERING RATHER THAN A
	// CONJUNCT. A handle whose OWN surrender claimed the teardown is the state
	// O6.1 releases from, and it must not be told a rival holds the claim —
	// there is no rival. What makes that true is that the lease is checked
	// FIRST, not a "whose claim is this" test: such a conjunct existed, was
	// unreachable-true, and left the package green when its accessor was
	// replaced with `return false`. Asserting the DIAGNOSIS is what makes the
	// ordering killable; asserting only that it was refused would not be.
	g := newHeartbeatFixture(t)
	close(g.lease.lost)
	g.awaitExit(t)
	if entry, held := g.registry.Get(g.key()); !held || !entry.TeardownOwned {
		t.Fatal("the control's residency is not claimed for teardown")
	}
	err := g.beat.FinishRelease(context.Background())
	if errors.Is(err, ErrTeardownOwnedElsewhere) {
		t.Error("a handle was told a rival owns the teardown of a claim it made itself; the lease must be checked before the claim")
	}
	if !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("the control reported %v, want ErrLeaseNotHeld: ownership is gone, which is the more specific truth and the one an operator acts on", err)
	}
}

// TestAResidencyRemovedBetweenTheCheckAndTheWriteIsNotACleanRelease is B3: the
// same fact, eleven lines apart, used to get two answers.
//
// releasable runs before the write lock, so a residency removed in between
// reached MarkReleasing's !current branch and returned a CLEAN SUCCESS for
// "there was nothing of mine" — while the identical condition a few lines
// earlier returned ErrNothingToRelease. The seam that makes it reachable is the
// registry read releasable itself performs.
func TestAResidencyRemovedBetweenTheCheckAndTheWriteIsNotACleanRelease(t *testing.T) {
	f := newHeartbeatFixture(t)
	if err := f.beat.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	f.registry.afterGet = func() {
		f.registry.inner.RemoveByGeneration(f.key(), f.entry.Generation)
	}

	err := f.beat.BeginRelease(context.Background())
	if err == nil {
		t.Fatal("a release whose residency vanished between the check and the write reported a clean success")
	}
	if !errors.Is(err, ErrNothingToRelease) {
		t.Errorf("the failure %v does not unwrap to ErrNothingToRelease", err)
	}
	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published for a residency that is gone", published)
	}
}

// TestTheBeatAndTheReleaseAreOneWriter closes the race the split introduced at
// the other end from the one FinishRelease already reasoned about.
//
// beat reads the registry and then writes the location; beginRelease marks the
// entry releasing and then writes the SAME record under the SAME epoch, so the
// store's fence cannot order them. Measured before the lock: a beat holding a
// stale `resident` published it after beginRelease had published `releasing`,
// leaving the last durable word on a session whose admission was locally closed
// as "resident, accepting" for up to one heartbeat interval.
//
// The interleaving is driven through the afterGet seam, which puts the release
// exactly between the beat's read and its write. Whether the release wins that
// moment is scheduling; the ASSERTION is not conditional on it — once a
// releasing observation has been published no later one may say resident, which
// is true on every run and false for an unsynchronized writer.
func TestTheBeatAndTheReleaseAreOneWriter(t *testing.T) {
	f := newHeartbeatFixture(t)
	released := make(chan error, 1)
	// THE SEAM ASKS A STATE QUESTION RATHER THAN FORCING AN INTERLEAVING, which
	// is what makes it decidable. afterGet runs, by construction, after the
	// beat's registry read; if the read happened under the write lock the lock
	// is held right now and TryLock must fail. Forcing the release to make
	// progress here would deadlock the passing case; asking whether the lock is
	// held does not, and it distinguishes "the lock exists" from "the lock
	// covers the read".
	lockedAcrossTheRead := false
	f.registry.afterGet = func() {
		if f.beat.writeMu.TryLock() {
			f.beat.writeMu.Unlock()
			lockedAcrossTheRead = true
		}
		go func() { released <- f.beat.BeginRelease(context.Background()) }()
	}

	f.pulse(t)
	// A DIAGNOSTIC ARM, because this is the one blocking receive in the file
	// whose signal comes from a hook rather than from the subject's own loop: a
	// mutant that removed the beat's registry read would never run afterGet and
	// would hang here instead of reporting. No passing run reaches it.
	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("BeginRelease: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the release never ran, so the beat never read the registry")
	}
	if lockedAcrossTheRead {
		t.Error("the beat read the registry WITHOUT holding the write lock, so its publish can be based on a state another writer has already superseded; the lock existing is not the same as the lock covering the read")
	}
	f.pulse(t)

	published := f.locations.publishedAll()
	if len(published) < 2 {
		t.Fatalf("%d observations were published, want at least the beat's and the release's", len(published))
	}
	sawReleasing := false
	for i, observation := range published {
		if observation.Residency == sessionwire.SessionResidencyReleasing {
			sawReleasing = true
			continue
		}
		if sawReleasing {
			t.Errorf("observation %d says %q accepting %t after a releasing one was already published; the last durable word on a session whose admission is closed must not be that it is accepting",
				i, observation.Residency, observation.Accepting)
		}
	}
	if !sawReleasing {
		t.Fatal("no releasing observation was published, so this test asserts nothing")
	}
	// And the final record agrees with the local entry, which is the property
	// the ordering exists to protect.
	last := published[len(published)-1]
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("the residency is gone")
	}
	if last.Accepting != entry.Accepting || (entry.State == registry.StateReleasing && last.Residency != sessionwire.SessionResidencyReleasing) {
		t.Errorf("the last durable observation (%q, accepting %t) disagrees with the local entry (%q, accepting %t)",
			last.Residency, last.Accepting, entry.State, entry.Accepting)
	}
}

// TestASupersededEpochIsTheDiagnosisEvenWhenAnotherOwnerHoldsTheTeardown pins
// the CAUSE, not merely the refusal, where the two axes intersect.
//
// Recording ownershipEnded only on the winning teardown branch is BEHAVIOURALLY
// EQUIVALENT for writes — measured: BeginTeardown returns !won only when the
// generation is gone, which releasable rejects first, or when the entry is
// already claimed, which it rejects anyway — so no mutant of that shape can
// write. What it does change is what an operator is told: "this Host no longer
// owns the session" is the truth here, and "somebody else is tearing it down"
// is a diagnosis that sends them to the wrong place. The refusal is not the
// whole contract; the reason is part of it.
func TestASupersededEpochIsTheDiagnosisEvenWhenAnotherOwnerHoldsTheTeardown(t *testing.T) {
	f := newHeartbeatFixture(t, func(f *heartbeatFixture) {
		f.locations.publishErr = fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
	})
	if _, won := f.registry.inner.BeginTeardown(f.key(), f.entry.Generation); !won {
		t.Fatal("the rival could not claim teardown, so this test never reached its subject")
	}
	if !f.pulseExpectingExit(t) {
		t.Fatal("the heartbeat kept beating after a fenced write was refused")
	}
	f.awaitExit(t)
	if handed := f.teardown.handedOver(); len(handed) != 0 {
		t.Fatalf("the loser of the teardown claim handed over %v, want nothing", handed)
	}
	f.locations.mu.Lock()
	f.locations.publishErr = nil
	f.locations.mu.Unlock()

	for _, row := range []struct {
		name string
		call func() error
	}{
		{"BeginRelease", func() error { return f.beat.BeginRelease(context.Background()) }},
		{"FinishRelease", func() error { return f.beat.FinishRelease(context.Background()) }},
	} {
		err := row.call()
		if !errors.Is(err, ErrLeaseNotHeld) {
			t.Errorf("%s reported %v; the session was superseded, so ownership is gone whether or not this heartbeat won the teardown claim, and that is what an operator must be told", row.name, err)
		}
	}
	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published after the epoch was superseded", published)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("a tombstone was written after the epoch was superseded: %v", tombstones)
	}
}

// TestEveryDurableLocationWriteHappensUnderTheWriteLock states the one-writer
// rule over the CODE, because the behavioural form cannot be made decidable.
//
// Driving a release into the window between the beat's read and its write needs
// the release to make progress while the beat holds the lock — which is exactly
// what the lock prevents, so the fixture that would force the interleaving
// deadlocks the passing case. The behavioural test beside this one asserts the
// invariant the ordering protects (no resident observation after a releasing
// one) and is meaningful on every run; this one pins the mechanism.
//
// WHAT IT DOES NOT COVER: that the lock is held across the READ as well as the
// write. A function that locked, wrote, and had read the entry earlier would
// satisfy this and still publish a stale state. That gap is closed
// BEHAVIOURALLY instead, by asking at the seam whether the lock is held at the
// moment of the read — see TestTheBeatAndTheReleaseAreOneWriter — because a
// structural rule over a read has no fixed shape to match.
//
// TWO FALSE POSITIVES ARE KNOWN AND FAIL SAFE, which is the direction to fail
// in but not a reason to leave them unnamed. A lock taken inside a top-level
// nested block is rejected, because accepting nested statements is what let the
// `if false` edit through. And hoisting the write into a closure —
// `publish := func(){…}; Lock(); defer Unlock(); publish()` — is rejected,
// because firstLocationWrite finds the write inside the literal, which precedes
// the lock. That second one is a plausible refactor of finishRelease, and A
// GUARD THAT REJECTS A CORRECT REFACTOR IS A GUARD SOMEONE WEAKENS RATHER THAN
// SATISFIES: whoever makes it should teach the guard about closures, not delete
// the rule.
func TestEveryDurableLocationWriteHappensUnderTheWriteLock(t *testing.T) {
	files := parseProductionFiles(t)
	examined := 0
	for name, file := range files {
		for _, offender := range unlockedLocationWrites(file) {
			t.Errorf("%s: %s writes a durable location without taking writeMu; the beat and the release share a record and an epoch, so the store's fence cannot order them and the lock must", name, offender)
		}
		for _, declaration := range file.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && writesALocation(function) {
				examined++
			}
		}
	}
	if examined < 3 {
		t.Fatalf("%d functions writing a durable location were examined, want the beat and both release halves", examined)
	}

	for _, probe := range []struct {
		name   string
		source string
		want   int
	}{
		{
			name:   "a publish with no lock",
			source: "package p\nfunc (h *Heartbeat) beat() { h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   1,
		},
		{
			name:   "a tombstone with no lock",
			source: "package p\nfunc (h *Heartbeat) drop() { h.options.Locations.TombstoneResidency(nil, \"\", \"\", 1) }\n",
			want:   1,
		},
		{
			name:   "control: the same publish under the lock",
			source: "package p\nfunc (h *Heartbeat) beat() { h.writeMu.Lock(); defer h.writeMu.Unlock(); h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   0,
		},
		{
			// THE REALISTIC EDIT, and it walked through the earlier detector: a
			// maintainer moves the write above the lock and the guard stays
			// green while the defect it is named for is back.
			name:   "the write moved above the lock",
			source: "package p\nfunc (h *Heartbeat) beat() { h.options.Locations.PublishResidency(nil, nil); h.writeMu.Lock(); h.writeMu.Unlock() }\n",
			want:   1,
		},
		{
			name:   "a lock that never runs",
			source: "package p\nfunc (h *Heartbeat) beat() { if false { h.writeMu.Lock() }; h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   1,
		},
		{
			name:   "a lock taken on another goroutine",
			source: "package p\nfunc (h *Heartbeat) beat() { go func() { h.writeMu.Lock() }(); h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   1,
		},
		{
			name:   "a lock released before the write",
			source: "package p\nfunc (h *Heartbeat) beat() { h.writeMu.Lock(); h.writeMu.Unlock(); h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   1,
		},
		{
			name:   "control: a deferred unlock is not an early one",
			source: "package p\nfunc (h *Heartbeat) beat() { h.writeMu.Lock(); defer h.writeMu.Unlock(); h.options.Locations.PublishResidency(nil, nil) }\n",
			want:   0,
		},
		{
			// The exclusion is itself probed: a too-wide one produces exactly
			// the same green as a correct one, which has been a live defect in
			// this module twice.
			name:   "control: another type's unlocked publish is not this rule's",
			source: "package p\nfunc (m *Manager) attach() { m.locations.PublishResidency(nil, nil) }\n",
			want:   0,
		},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", probe.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parsing the probe: %v", probe.name, err)
		}
		if got := unlockedLocationWrites(parsed); len(got) != probe.want {
			t.Errorf("%s: the detector reported %v (%d), want %d", probe.name, got, len(got), probe.want)
		}
	}
}

// isHeartbeatMethod reports whether a declaration is a method on *Heartbeat.
func isHeartbeatMethod(function *ast.FuncDecl) bool {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return false
	}
	pointer, isPointer := function.Recv.List[0].Type.(*ast.StarExpr)
	if !isPointer {
		return false
	}
	name, isIdent := pointer.X.(*ast.Ident)
	return isIdent && name.Name == "Heartbeat"
}

// firstLocationWrite reports the position of the earliest durable location
// write in a function, so a lock taken after it can be told from one taken
// before.
func firstLocationWrite(function *ast.FuncDecl) token.Pos {
	earliest := token.Pos(-1)
	ast.Inspect(function, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		method, isMethod := call.Fun.(*ast.SelectorExpr)
		if !isMethod {
			return true
		}
		switch method.Sel.Name {
		case "PublishResidency", "TombstoneResidency":
			if earliest < 0 || call.Pos() < earliest {
				earliest = call.Pos()
			}
		}
		return true
	})
	return earliest
}

// writesALocation reports whether a function calls a durable location write.
func writesALocation(function *ast.FuncDecl) bool {
	found := false
	ast.Inspect(function, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		if method, ok := call.Fun.(*ast.SelectorExpr); ok {
			switch method.Sel.Name {
			case "PublishResidency", "TombstoneResidency":
				found = true
			}
		}
		return true
	})
	return found
}

// unlockedLocationWrites names the *Heartbeat methods that write a durable
// location without taking the write lock.
//
// IT IS SCOPED BY RECEIVER, NOT BY FILE, because a file name is a proxy for the
// subject and this repository has lost guards to exactly that. The subject is
// the set of writers writeMu governs, which is the methods on this type.
//
// THE MANAGER IS EXCLUDED ON ORDERING, AND ON NOTHING ELSE. Its attach
// publishes the same record and is serialized differently: one attach per key
// holds the slot, and its rollback stops the heartbeat before it tombstones,
// which the unwinder's LIFO order guarantees. That is the part that matters and
// it is true.
//
// TWO VERSIONS OF THIS COMMENT CLAIMED THE WRITERS PUBLISH THE SAME CONTENT,
// AND BOTH WERE FALSE — the second one inside the retraction of the first.
// Step 8 built `resident` as a literal and accepting as `!Draining()`, while
// the beat built residencyOf(entry.State) and entry.Accepting && !Draining():
// two of three content axes divergent, not the one the retraction admitted to.
// A clause that is true — the LIFO ordering — sat beside a clause that was not,
// lending it credibility. That is the shape this lane keeps finding, arriving
// once more inside the sentence written to stop it.
//
// SO THE DIVERGENCE IS GONE RATHER THAN DOCUMENTED. Step 8 now derives from the
// registry through the same functions the beat uses, so the two writers publish
// the same function of the same state and there is nothing left for a comment
// to be wrong about. What remains is timestamps: the two read the clock
// independently, so a step 8 publish landing after a beat carries an earlier
// ObservedAt. It is bounded by the heartbeat margin, and it is the ONLY
// remaining axis — which is a claim with a test behind it rather than an
// argument.
func unlockedLocationWrites(file *ast.File) []string {
	var offenders []string
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if !isFunction || !writesALocation(function) || !isHeartbeatMethod(function) {
			continue
		}
		// THE LOCK MUST BE UNCONDITIONAL AND MUST PRECEDE THE WRITE. Looking
		// for a Lock() call anywhere in the function was a convention dressed
		// as a mechanism, and three realistic edits walked through it: the lock
		// inside `if false`, the lock inside a `go func`, and — the one a
		// maintainer actually makes — the write moved ABOVE the lock. The rule
		// this test is named for is that the write happens under the lock, so
		// the check is over statement position and statement nesting, not over
		// the call's presence.
		lockedAt := -1
		for _, statement := range function.Body.List {
			expression, isExpression := statement.(*ast.ExprStmt)
			if !isExpression {
				continue
			}
			call, isCall := expression.X.(*ast.CallExpr)
			if !isCall {
				continue
			}
			method, isMethod := call.Fun.(*ast.SelectorExpr)
			if !isMethod || method.Sel.Name != "Lock" {
				continue
			}
			if field, ok := method.X.(*ast.SelectorExpr); ok && field.Sel.Name == "writeMu" {
				lockedAt = int(statement.Pos())
				break
			}
		}
		// AN EARLY Unlock IS THE SAME FAMILY as the write moved above the lock,
		// and it was surviving: Lock(); Unlock(); write reads as locked. With
		// statement positions already in hand the scan costs one more loop.
		unlockedAt := -1
		for _, statement := range function.Body.List {
			expression, isExpression := statement.(*ast.ExprStmt)
			if !isExpression {
				continue
			}
			call, isCall := expression.X.(*ast.CallExpr)
			if !isCall {
				continue
			}
			method, isMethod := call.Fun.(*ast.SelectorExpr)
			if !isMethod || method.Sel.Name != "Unlock" {
				continue
			}
			if field, ok := method.X.(*ast.SelectorExpr); ok && field.Sel.Name == "writeMu" {
				unlockedAt = int(statement.Pos())
				break
			}
		}
		write := int(firstLocationWrite(function))
		switch {
		case lockedAt < 0 || lockedAt > write:
			offenders = append(offenders, function.Name.Name)
		case unlockedAt >= 0 && unlockedAt < write:
			offenders = append(offenders, function.Name.Name)
		}
	}
	return offenders
}

// TestASupersededEpochOnTheRELEASEPathAlsoEndsOwnership is Y1: the third way
// ownership ends, and the one neither branch of surrender can reach.
//
// surrender runs only on the loop. The release path has its own fenced writes,
// so a BeginRelease refused for a later epoch classified nothing, and the
// FinishRelease that followed tombstoned under an epoch this Host had just been
// told was superseded — and reported a clean release. O6.1's shape makes that
// the NORMAL case, not a contrivance: BeginRelease, a slow checkpoint,
// FinishRelease, with a successor taking the lease in between and the refusal
// arriving before Lost() has propagated.
func TestASupersededEpochOnTheRELEASEPathAlsoEndsOwnership(t *testing.T) {
	f := newHeartbeatFixture(t, func(f *heartbeatFixture) {
		f.locations.publishErr = fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
	})
	if err := f.beat.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	begun := f.beat.BeginRelease(context.Background())
	if !errors.Is(begun, ErrEpochSuperseded) {
		t.Fatalf("BeginRelease reported %v, want an error unwrapping to ErrEpochSuperseded", begun)
	}
	// The refusal was SEEN. What follows is whether it was BELIEVED.
	f.locations.mu.Lock()
	f.locations.publishErr = nil
	f.locations.mu.Unlock()

	finished := f.beat.FinishRelease(context.Background())
	if finished == nil {
		t.Fatal("FinishRelease reported a clean release after this Host was told a later epoch had committed")
	}
	if !errors.Is(finished, ErrLeaseNotHeld) {
		t.Errorf("FinishRelease reported %v, want an error unwrapping to ErrLeaseNotHeld: a superseded epoch ends ownership wherever it is observed", finished)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("a tombstone was written at %v after the epoch was superseded on the release path", tombstones)
	}

	// The CONTROL, one position over: the same two calls against a store that
	// answers write both records. Without it these assertions would pass for a
	// release that never writes anything.
	g := newHeartbeatFixture(t)
	if err := g.beat.BeginRelease(context.Background()); err != nil {
		t.Fatalf("the control's BeginRelease reported %v", err)
	}
	if err := g.beat.FinishRelease(context.Background()); err != nil {
		t.Fatalf("the control's FinishRelease reported %v", err)
	}
	if len(g.locations.tombstones()) != 1 {
		t.Errorf("the control wrote %d tombstones, want 1", len(g.locations.tombstones()))
	}
}

// TestARefusedTombstoneCarriesTheStoresOwnCause is Y2.
//
// finishRelease took its Cause from releasable, which is nil on this path, so
// the one error that is NOT ambiguity survived as text only — while
// beginRelease, ten lines up, preserved it. Cause was added so a caller could
// tell a lost grant from an unreachable store with errors.Is instead of
// matching a sentence; an operator reading the dropped one sees an unreachable
// store.
func TestARefusedTombstoneCarriesTheStoresOwnCause(t *testing.T) {
	for _, row := range []struct {
		name  string
		fail  error
		wants error
	}{
		{name: "a superseded epoch", fail: fmt.Errorf("sessionstore: %w", ErrEpochSuperseded), wants: ErrEpochSuperseded},
		{name: "an unreachable store", fail: errors.New("dial tcp: connection refused"), wants: nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newHeartbeatFixture(t, func(f *heartbeatFixture) { f.tombstoneErr = row.fail })
			if err := f.beat.BeginRelease(context.Background()); err != nil {
				t.Fatalf("BeginRelease: %v", err)
			}
			err := f.beat.FinishRelease(context.Background())
			var refused *ReleaseError
			if !errors.As(err, &refused) {
				t.Fatalf("error is %T, want *ReleaseError", err)
			}
			if refused.Cause == nil {
				t.Fatal("the refusal carries no cause, so the store's own error survives as text only")
			}
			if !errors.Is(refused.Cause, row.fail) {
				t.Errorf("the refusal's cause is %v, want the store's %v", refused.Cause, row.fail)
			}
			// The DISCRIMINATOR: the two rows differ only in what the store
			// said, so a cause that were always the same value would pass one
			// and fail the other.
			if row.wants != nil && !errors.Is(err, row.wants) {
				t.Errorf("the failure does not unwrap to %v", row.wants)
			}
			if row.wants == nil && errors.Is(err, ErrEpochSuperseded) {
				t.Error("an unreachable store was reported as a superseded epoch")
			}
		})
	}
}

// TestEveryFencedWriteGoesThroughTheFence is the guard the classification rule
// was missing, and its absence is why three commits each fixed the sites they
// happened to know about.
//
// Its neighbour — one writer, one lock — had a structural guard and stayed
// right. This rule was "remember to classify the error", enforced by nothing,
// and it was applied at three sites in one file while a second writer in
// another file had four more with none. THE MUTATION EVIDENCE IS THE ARGUMENT:
// of the three sites that did classify, only one was killable — the other two
// were equivalent, because something else recorded the same fact first. Two
// thirds of the convention was already unenforced by test, which is precisely
// the state in which the next writer is added without it.
//
// It covers EVERY production file, the Manager included. The write-lock guard
// excludes the Manager with a reason; this one must not, because the rule is
// about the record and not about the type that writes it.
func TestEveryFencedWriteGoesThroughTheFence(t *testing.T) {
	files := parseProductionFiles(t)
	total := 0
	for name, file := range files {
		writes, unfenced := fencedWrites(file)
		total += writes
		for _, offender := range unfenced {
			t.Errorf("%s: %s is a fenced write that does not go through epochFence.write, so a later owner's refusal is never classified", name, offender)
		}
	}
	// FLOORED at the sites that exist, so a guard that stopped finding them
	// fails rather than passing. Six location writes; the journal fence that
	// used to be the seventh is gone, and CommitOpeningFence stays in the
	// detector's set precisely so that a reinstated one would have to pass
	// through the fence — TestNoProductionFileOpensAJournalWriter is what says
	// it may not be reinstated at all.
	if total < 6 {
		t.Fatalf("%d fenced writes were found, want at least the six this package makes", total)
	}

	for _, probe := range []struct {
		name   string
		source string
		want   int
	}{
		{
			name:   "a publish outside the fence",
			source: "package p\nfunc (m *Manager) attach() { m.locations.PublishResidency(nil, nil) }\n",
			want:   1,
		},
		{
			name:   "a tombstone outside the fence",
			source: "package p\nfunc (m *Manager) roll() { m.locations.TombstoneResidency(nil, \"\", \"\", 1) }\n",
			want:   1,
		},
		{
			name:   "a journal fence outside the fence",
			source: "package p\nfunc (m *Manager) open() { m.journal.CommitOpeningFence(nil, \"\", \"\", 1) }\n",
			want:   1,
		},
		{
			name:   "control: the same publish inside it",
			source: "package p\nfunc (m *Manager) attach() { fence.write(func() error { return m.locations.PublishResidency(nil, nil) }) }\n",
			want:   0,
		},
		{
			name:   "control: inside it through a field",
			source: "package p\nfunc (h *Heartbeat) beat() { h.fence.write(func() error { return h.options.Locations.PublishResidency(nil, nil) }) }\n",
			want:   0,
		},
		{
			// ANY method named write used to satisfy this, which made the
			// detector a convention about naming rather than a mechanism.
			name:   "a write method that is not the fence",
			source: "package p\nfunc (m *Manager) attach() { m.log.write(func() error { return m.locations.PublishResidency(nil, nil) }) }\n",
			want:   1,
		},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", probe.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parsing the probe: %v", probe.name, err)
		}
		if _, unfenced := fencedWrites(parsed); len(unfenced) != probe.want {
			t.Errorf("%s: the detector reported %v (%d), want %d", probe.name, unfenced, len(unfenced), probe.want)
		}
	}
}

// fencedWrites returns how many fenced writes a file makes and which of them do
// not go through epochFence.write.
func fencedWrites(file *ast.File) (int, []string) {
	fenced := map[string]bool{
		"PublishResidency":   true,
		"TombstoneResidency": true,
		"CommitOpeningFence": true,
	}
	total := 0
	var unfenced []string
	var stack []ast.Node
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		defer func() { stack = append(stack, node) }()
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		method, isMethod := call.Fun.(*ast.SelectorExpr)
		if !isMethod || !fenced[method.Sel.Name] {
			return true
		}
		total++
		for _, ancestor := range stack {
			outer, isOuter := ancestor.(*ast.CallExpr)
			if !isOuter {
				continue
			}
			wrapper, ok := outer.Fun.(*ast.SelectorExpr)
			if !ok || wrapper.Sel.Name != "write" {
				continue
			}
			// THE RECEIVER MUST BE A FENCE. Accepting any method named write
			// made the detector a convention about naming — the same argument
			// this package applied to the classifier one level up — so
			// `m.log.write(func() error { return …Publish… })` read as fenced.
			// Without type information the receiver is checked by SPELLING,
			// and TestFenceIsTheOnlyThingSpelledFence is what makes that sound
			// rather than a convention: it pins the biconditional over this
			// package's declarations, so "spelled fence" and "is an
			// *epochFence" are the same set. Both directions were live — a
			// fence under another name is a false positive, a non-fence called
			// fence is a false NEGATIVE — and only the second is unsafe.
			switch receiver := wrapper.X.(type) {
			case *ast.Ident:
				if spelledFence(receiver.Name) {
					return true
				}
			case *ast.SelectorExpr:
				if spelledFence(receiver.Sel.Name) {
					return true
				}
			}
		}
		unfenced = append(unfenced, method.Sel.Name)
		return true
	})
	return total, unfenced
}

// TestFenceIsTheOnlyThingSpelledFence converts the fence detector's naming
// CONVENTION into a MECHANISM, which is the argument this package made one
// level up and then did not apply to its own guard.
//
// TestEveryFencedWriteGoesThroughTheFence decides "is this wrapper a fence" by
// the receiver's spelling, because an AST guard cannot name a type without type
// information and taking that dependency is a release-graph event for one
// check. Spelling is sound only if the two directions hold, and both were live:
// an *epochFence named otherwise is a false positive, and a non-fence spelled
// `fence` is a false NEGATIVE, which is the unsafe one.
//
// So this pins the biconditional over the package's own declarations, and the
// claim is EXACTLY AS WIDE AS THE ENUMERATION AND NO WIDER — which is this
// lane's recurring defect applied to the mechanism built to end it. What
// fenceBindings enumerates is three binding forms: struct FIELDS, function
// PARAMETERS, and short variable declarations from newEpochFence. So the claim
// is "every field, parameter and newEpochFence local of type *epochFence is
// spelled fence, and nothing of those three forms is spelled fence without
// being one". It is not a claim about bindings in general.
//
// THREE FORMS ESCAPE IT, measured at zero violations detected: a package-level
// `var guard *epochFence`, a named result `func mk() (guard *epochFence)`, and
// an ALIAS `guard := h.fence` — the last being the most plausible edit by some
// distance, since it needs no new declaration style at all. The unsafe
// direction has a corner too: a package-level `var fence *sync.Mutex` satisfies
// this vacuously and would be read as a fence by the write guard.
//
// The detector is deliberately NOT widened. The floor below and the probe rows
// bound what a miss can cost, and a guard that grows a case per syntax form is
// one nobody can say the reach of. A reader who adds one of those three forms
// should extend the enumeration; a reader who trusts this sentence beyond the
// three forms it names has been told not to.
func TestFenceIsTheOnlyThingSpelledFence(t *testing.T) {
	files := parseProductionFiles(t)
	bindings := 0
	for name, file := range files {
		for _, binding := range fenceBindings(file) {
			bindings++
			switch {
			case binding.isFence && !spelledFence(binding.name):
				t.Errorf("%s: %s is an *epochFence spelled %q; the write guard decides by spelling, so a fence under another name is a fenced write it cannot see", name, binding.where, binding.name)
			case !binding.isFence && spelledFence(binding.name):
				t.Errorf("%s: %s is spelled \"fence\" and is not an *epochFence; the write guard would read writes through it as fenced when they are not", name, binding.where)
			}
		}
	}
	// FLOORED over the three forms enumerated above: the field on Heartbeat,
	// the local in attach, and the field on OwnershipRequest at least. A floor
	// is what keeps a detector that stopped matching from passing silently; it
	// is not evidence that the enumeration is complete.
	if bindings < 3 {
		t.Fatalf("%d bindings were examined, want at least the three this package declares", bindings)
	}

	for _, probe := range []struct {
		name   string
		source string
		want   int
	}{
		{
			name:   "an epochFence under another name",
			source: "package p\ntype H struct{ guard *epochFence }\n",
			want:   1,
		},
		{
			name:   "something else called fence",
			source: "package p\ntype H struct{ fence *sync.Mutex }\n",
			want:   1,
		},
		{
			name:   "a local from newEpochFence under another name",
			source: "package p\nfunc f() { guard := newEpochFence(nil); _ = guard }\n",
			want:   1,
		},
		{
			name:   "control: both directions satisfied",
			source: "package p\ntype H struct{ fence *epochFence }\nfunc f() { fence := newEpochFence(nil); _ = fence }\n",
			want:   0,
		},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", probe.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parsing the probe: %v", probe.name, err)
		}
		violations := 0
		for _, binding := range fenceBindings(parsed) {
			if binding.isFence != spelledFence(binding.name) {
				violations++
			}
		}
		if violations != probe.want {
			t.Errorf("%s: the detector reported %d violations, want %d", probe.name, violations, probe.want)
		}
	}
}

// spelledFence reports whether a name is the fence spelling, ignoring the
// capital Go uses for export. OwnershipRequest.Fence and Heartbeat.fence are
// the same word and must satisfy the same rule.
func spelledFence(name string) bool { return strings.EqualFold(name, "fence") }

// fenceBinding is one name bound to a type, for the biconditional above.
type fenceBinding struct {
	name    string
	where   string
	isFence bool
}

// fenceBindings enumerates the declarations that bind a name to a type this
// guard cares about: struct fields, function parameters, and short variable
// declarations from newEpochFence.
func fenceBindings(file *ast.File) []fenceBinding {
	var bindings []fenceBinding
	isFenceType := func(expr ast.Expr) bool {
		pointer, isPointer := expr.(*ast.StarExpr)
		if !isPointer {
			return false
		}
		name, isIdent := pointer.X.(*ast.Ident)
		return isIdent && name.Name == "epochFence"
	}
	named := func(field *ast.Field, where string) {
		for _, name := range field.Names {
			fence := isFenceType(field.Type)
			if !fence && !spelledFence(name.Name) {
				continue
			}
			bindings = append(bindings, fenceBinding{name: name.Name, where: where + " " + name.Name, isFence: fence})
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch declaration := node.(type) {
		case *ast.StructType:
			for _, field := range declaration.Fields.List {
				named(field, "field")
			}
		case *ast.FuncDecl:
			if declaration.Type.Params != nil {
				for _, field := range declaration.Type.Params.List {
					named(field, "parameter of "+declaration.Name.Name)
				}
			}
		case *ast.AssignStmt:
			for i, right := range declaration.Rhs {
				if i >= len(declaration.Lhs) {
					break
				}
				target, isIdent := declaration.Lhs[i].(*ast.Ident)
				if !isIdent {
					continue
				}
				call, isCall := right.(*ast.CallExpr)
				fromFence := false
				if isCall {
					if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "newEpochFence" {
						fromFence = true
					}
				}
				if !fromFence && !spelledFence(target.Name) {
					continue
				}
				bindings = append(bindings, fenceBinding{name: target.Name, where: "local " + target.Name, isFence: fromFence})
			}
		}
		return true
	})
	return bindings
}

// TestTheHeartbeatWritesThroughTheGrantsFenceNotACopy is the kill for a second
// fence per grant, and it needed the write to come from the OTHER side.
//
// An earlier attempt ended the fence the REQUEST carried, which is the
// Manager's own object either way, so it could not tell one fence from two.
// What distinguishes them is a refusal observed by one party being visible to
// the other: here the Manager's write is refused for a later epoch — closing no
// channel, which is the case the fence exists for — and the heartbeat must
// surrender on its next beat rather than publish under an epoch it has been
// told is stale.
func TestTheHeartbeatWritesThroughTheGrantsFenceNotACopy(t *testing.T) {
	f := newHeartbeatFixture(t)

	// The Manager's own fenced write, refused. Lost() stays open.
	if err := f.fence.write(func() error {
		return fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
	}); !errors.Is(err, ErrEpochSuperseded) {
		t.Fatalf("the seeded refusal reported %v", err)
	}
	select {
	case <-f.lease.lost:
		t.Fatal("the lease closed, so this test did not exercise the path that closes no channel")
	default:
	}

	if !f.pulseExpectingExit(t) {
		t.Fatal("the heartbeat kept beating after the grant's fence recorded a superseded epoch; it is writing through a fence of its own")
	}
	f.awaitExit(t)

	if published := len(f.locations.publishedAll()); published != 0 {
		t.Errorf("%d observations were published under an epoch a successor has superseded", published)
	}
	handed := f.teardown.handedOver()
	if len(handed) != 1 || handed[0].Reason != LossReasonEpochSuperseded {
		t.Fatalf("the loss handed over %v, want exactly one epoch_superseded", handed)
	}
	// And the release path refuses too, for the same fact through the same
	// object.
	if err := f.beat.BeginRelease(context.Background()); !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("BeginRelease reported %v, want ErrLeaseNotHeld", err)
	}
}
