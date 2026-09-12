package compose

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
	"github.com/looprig/host/internal/testkit"
)

const (
	testAgent    = sessionwire.AgentID("agent-a")
	testCompat   = department.CompatibilityID("runtime-a")
	testHostID   = sessionwire.HostID("host-a")
	testEndpoint = sessionwire.InternalEndpoint("ws://10.0.0.1:7100/hostlink")
	testGen      = uint64(4)
)

// ---------------------------------------------------------------------------
// The clock
// ---------------------------------------------------------------------------

// fakeClock is the composition's one time source under test.
//
// NO TEST IN THIS PACKAGE SLEEPS. Every bound the composition holds — the
// advertisement heartbeat, the drain's grace, the warm countdown, the
// compatibility timeout and its gate poll — is taken from this, so a test
// advances time by releasing a channel rather than by waiting for one.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeWarmTimer
	waiters map[time.Duration][]chan time.Time

	// registered and armedWarm are the rendezvous a test waits on instead of
	// sleeping: one announces that something has armed a bound, the other that
	// a warm countdown was created.
	registered chan time.Duration
	armedWarm  chan time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now:        time.Unix(1_700_000_000, 0).UTC(),
		waiters:    map[time.Duration][]chan time.Time{},
		registered: make(chan time.Duration, 256),
		armedWarm:  make(chan time.Duration, 64),
	}
}

// Now is the frozen instant.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After hands out a channel filed under its duration, so a test can fire one
// bound without firing every bound.
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	waiter := make(chan time.Time, 1)
	c.waiters[d] = append(c.waiters[d], waiter)
	select {
	case c.registered <- d:
	default:
	}
	return waiter
}

// fire releases every channel currently waiting on one duration.
func (c *fakeClock) fire(d time.Duration) int {
	c.mu.Lock()
	waiting := c.waiters[d]
	c.waiters[d] = nil
	instant := c.now
	c.mu.Unlock()
	for _, waiter := range waiting {
		waiter <- instant
	}
	return len(waiting)
}

// NewWarmTimer records the arming, which is what the warm TTL test reads.
func (c *fakeClock) NewWarmTimer(d time.Duration) residency.WarmTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeWarmTimer{armed: d, expiry: make(chan time.Time, 1), notify: c.armedWarm}
	c.timers = append(c.timers, timer)
	return timer
}

// constructions reports the duration every warm timer was CREATED for.
func (c *fakeClock) constructions() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	durations := make([]time.Duration, 0, len(c.timers))
	for _, timer := range c.timers {
		durations = append(durations, timer.arming())
	}
	return durations
}

// rearmings reports every duration a warm countdown was ARMED for, across every
// timer.
func (c *fakeClock) rearmings() []time.Duration {
	c.mu.Lock()
	timers := append([]*fakeWarmTimer(nil), c.timers...)
	c.mu.Unlock()
	var durations []time.Duration
	for _, timer := range timers {
		durations = append(durations, timer.armings()...)
	}
	return durations
}

// fakeWarmTimer is one armed warm countdown.
type fakeWarmTimer struct {
	mu     sync.Mutex
	armed  time.Duration
	resets []time.Duration
	stops  int
	expiry chan time.Time

	// notify announces an arming, so a test blocks on the event instead of
	// polling for it.
	notify chan time.Duration
}

// C is the expiry channel.
func (t *fakeWarmTimer) C() <-chan time.Time { return t.expiry }

// Stop records a cancellation and reports that a live arming was cancelled.
func (t *fakeWarmTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stops++
	return true
}

// Reset records a fresh arming.
//
// THE RESETS ARE KEPT SEPARATELY FROM THE CONSTRUCTION, and the separation is
// what lets a test tell the two paths apart: the releaser creates a timer for
// the TTL at Watch and STOPS it immediately, then arms it with Reset only when a
// session is observed idle. A fixture that recorded one duration could not
// distinguish "the TTL reached the constructor" from "an idle observation armed
// the countdown", and only the second is the behaviour a warm release turns on.
func (t *fakeWarmTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.armed = d
	t.resets = append(t.resets, d)
	select {
	case t.notify <- d:
	default:
	}
	return true
}

// armings is every duration this timer was re-armed for.
func (t *fakeWarmTimer) armings() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.resets...)
}

// arming is the duration this timer was last armed for.
func (t *fakeWarmTimer) arming() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.armed
}

// ---------------------------------------------------------------------------
// The durable seams
// ---------------------------------------------------------------------------

// recorder is the ordered trace every fake writes into, so a test asserts on
// what happened in what order rather than on a count per fake.
type recorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *recorder) record(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *recorder) trace() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.steps...)
}

// indexOf reports the position of a step, or -1.
func (r *recorder) indexOf(step string) int {
	for position, recorded := range r.trace() {
		if recorded == step {
			return position
		}
	}
	return -1
}

// fakeLease is one residency grant.
type fakeLease struct {
	trace *recorder
	epoch residency.ResidencyEpoch
	lost  chan struct{}
}

func (l *fakeLease) Epoch() residency.ResidencyEpoch { return l.epoch }
func (l *fakeLease) Lost() <-chan struct{}           { return l.lost }
func (l *fakeLease) Release(context.Context) error {
	l.trace.record("lease.release")
	return nil
}

// fakeStore stands in for every durable seam the released store satisfies or,
// where it does not, for the one a deployment must supply.
type fakeStore struct {
	trace *recorder

	mu     sync.Mutex
	leases map[registry.Key]*fakeLease
	grants map[registry.Key]*fakeGrant
	rows   []sessionwire.HostLinkRegistryObservation
	hold   chan struct{}
}

func newFakeStore(trace *recorder) *fakeStore {
	return &fakeStore{
		trace:  trace,
		leases: map[registry.Key]*fakeLease{},
		grants: map[registry.Key]*fakeGrant{},
	}
}

func (s *fakeStore) AcquireSessionLease(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (residency.Lease, error) {
	s.trace.record("lease.acquire")
	lease := &fakeLease{trace: s.trace, epoch: 9, lost: make(chan struct{})}
	s.mu.Lock()
	s.leases[registry.Key{TenantID: tenant, SessionID: session}] = lease
	s.mu.Unlock()
	return lease, nil
}

func (s *fakeStore) LoadSessionState(context.Context, sessionwire.TenantID, sessionwire.SessionID) (residency.SessionState, error) {
	return residency.SessionState{Namespace: "tenant/session", CompatibilityID: testCompat, RigSessionID: testRigSessionID}, nil
}

func (s *fakeStore) PublishResidency(_ context.Context, observation sessionwire.HostLinkRegistryObservation) error {
	s.mu.Lock()
	hold := s.hold
	s.mu.Unlock()
	if hold != nil && observation.Residency == sessionwire.SessionResidencyReleasing {
		<-hold
	}
	s.mu.Lock()
	s.rows = append(s.rows, observation)
	s.mu.Unlock()
	s.trace.record("locations.publish")
	return nil
}

func (s *fakeStore) TombstoneResidency(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64) error {
	s.trace.record("locations.tombstone")
	return nil
}

// published returns every registry row written so far.
func (s *fakeStore) published() []sessionwire.HostLinkRegistryObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sessionwire.HostLinkRegistryObservation(nil), s.rows...)
}

func (s *fakeStore) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "/workspace", nil
}

func (s *fakeStore) ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	s.trace.record("workspace.release")
	return nil
}

func (s *fakeStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

// openSession is the composition's SessionOpener.
func (s *fakeStore) openSession(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (JournalGrant, error) {
	s.trace.record("journal.open")
	grant := &fakeGrant{trace: s.trace}
	s.mu.Lock()
	s.grants[registry.Key{TenantID: tenant, SessionID: session}] = grant
	s.mu.Unlock()
	return grant, nil
}

// fakeGrant is one session's journal writer.
type fakeGrant struct {
	trace    *recorder
	mu       sync.Mutex
	released int
}

func (g *fakeGrant) AppendApplicationPrefix(context.Context, sessionwire.TenantID, sessionwire.SessionID, commands.Prefix) error {
	return nil
}

func (g *fakeGrant) Release(context.Context) error {
	g.mu.Lock()
	g.released++
	g.mu.Unlock()
	g.trace.record("journal.release")
	return nil
}

func (g *fakeGrant) releaseCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.released
}

// fakeInbox is the durable per-session command inbox.
type fakeInbox struct {
	mu      sync.Mutex
	records []commands.Command
}

func (i *fakeInbox) ListOrdered(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64, int) ([]commands.Command, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]commands.Command(nil), i.records...), nil
}

// fakeCursors is the durable consumption cursor.
type fakeCursors struct {
	mu     sync.Mutex
	cursor uint64
}

func (c *fakeCursors) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor, nil
}

func (c *fakeCursors) SaveCursor(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, order uint64, _ uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = order
	return nil
}

// fakeCommands is the private command-record half an applier reads and writes.
// Nothing in these tests drives an application, so every method reports an
// absent record rather than inventing one.
type fakeCommands struct{}

func (fakeCommands) LoadCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID) (commands.Record, error) {
	return commands.Record{}, errors.New("no command")
}

func (fakeCommands) LoadPayload(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID) (commands.Payload, error) {
	return commands.Payload{}, errors.New("no payload")
}

func (fakeCommands) FindApplication(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID) (commands.Application, error) {
	return commands.Application{}, nil
}

func (fakeCommands) LoadGate(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.GateID) (commands.Gate, bool, error) {
	return commands.Gate{}, false, nil
}

func (fakeCommands) ClaimCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID, commands.Claim) (uint64, error) {
	return 0, nil
}

func (fakeCommands) BeginApplying(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID, commands.Applying) (uint64, error) {
	return 0, nil
}

func (fakeCommands) CompleteCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID, commands.Completion) error {
	return nil
}

func (fakeCommands) RejectCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.CommandID, commands.Rejection) error {
	return nil
}

// fakeDirectory is the durable target directory.
type fakeDirectory struct {
	trace *recorder
	ttl   time.Duration

	mu         sync.Mutex
	published  []service.Advertisement
	withdrawn  []service.Advertisement
	publishErr error
}

func (d *fakeDirectory) MaxAdvertisementTTL() time.Duration { return d.ttl }

func (d *fakeDirectory) PublishTarget(_ context.Context, advertisement service.Advertisement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.publishErr != nil {
		return d.publishErr
	}
	d.published = append(d.published, advertisement)
	d.trace.record("directory.publish")
	return nil
}

func (d *fakeDirectory) WithdrawTarget(_ context.Context, advertisement service.Advertisement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.withdrawn = append(d.withdrawn, advertisement)
	d.trace.record("directory.withdraw")
	return nil
}

func (d *fakeDirectory) withdrawals() []service.Advertisement {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]service.Advertisement(nil), d.withdrawn...)
}

func (d *fakeDirectory) publications() []service.Advertisement {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]service.Advertisement(nil), d.published...)
}

// fakeCheckpointer is the product's release checkpoint.
type fakeCheckpointer struct {
	trace *recorder
	err   error
}

func (c *fakeCheckpointer) Checkpoint(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	c.trace.record("checkpoint")
	return c.err
}

// fakeAuth accepts one credential.
type fakeAuth struct{}

func (fakeAuth) VerifyTenant(_ context.Context, _ sessionwire.TenantID, credential string) error {
	if credential != "service-credential" {
		return errors.New("invalid credential")
	}
	return nil
}

// fakeWorkStates is the optional work-state source.
type fakeWorkStates struct {
	mu     sync.Mutex
	states map[registry.Key]residency.WorkState
}

func (w *fakeWorkStates) WorkState(key registry.Key) (residency.WorkState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	state, known := w.states[key]
	return state, known
}

func (w *fakeWorkStates) setState(key registry.Key, state residency.WorkState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.states == nil {
		w.states = map[registry.Key]residency.WorkState{}
	}
	w.states[key] = state
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

// fixture is one composed Host over fakes.
type fixture struct {
	t           *testing.T
	trace       *recorder
	clock       *fakeClock
	store       *fakeStore
	directory   *fakeDirectory
	rig         *testkit.FakeRig
	runtime     *controllableSession
	work        *fakeWorkStates
	host        *host.Host
	hostOptions host.Options
	svc         *Service
}

// newFixture composes a pooled Host over fakes and returns it unstarted.
func newFixture(t *testing.T, adjust ...func(*Options, *host.Options)) *fixture {
	t.Helper()

	trace := &recorder{}
	clock := newFakeClock()
	store := newFakeStore(trace)
	directory := &fakeDirectory{trace: trace, ttl: 15 * time.Minute}
	runtimeSession := newControllableSession(testRigSessionID)
	rig := &testkit.FakeRig{Session: runtimeSession}
	work := &fakeWorkStates{}

	target, err := department.NewRigTarget(rig, testCompat, department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	})
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	dept, err := department.New([]department.Registration{{AgentID: testAgent, Target: target}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}

	hostOptions := host.Options{
		HostID:            testHostID,
		InternalEndpoint:  testEndpoint,
		IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
		Department:        dept,
		SessionStore:      store,
		Workspaces:        store,
		Clock:             hostClock{clock},
		Auth:              fakeAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           defaultFixtureWarmTTL,
		RegistryHeartbeat: 10 * time.Second,
		RegistryExpiry:    60 * time.Second,
		ClaimTTL:          5 * time.Second,
		ApplyDeadline:     30 * time.Second,
		CommandQueueSize:  16,
		ReconcileInterval: time.Minute,
		ReconcileBatch:    32,
	}
	composeOptions := Options{
		HostGeneration:       testGen,
		Clock:                clock,
		Leases:               store,
		Durable:              store,
		Locations:            store,
		Workspaces:           store,
		OpenSession:          store.openSession,
		Inbox:                &fakeInbox{},
		Cursors:              &fakeCursors{},
		Records:              fakeCommands{},
		Applications:         fakeCommands{},
		Gates:                fakeCommands{},
		InboxWrites:          fakeCommands{},
		Targets:              directory,
		Checkpointer:         &fakeCheckpointer{trace: trace},
		Auth:                 fakeAuth{},
		MaxBindingsPerLink:   4,
		MaxBindings:          8,
		MaxTenantLinks:       3,
		Grace:                30 * time.Second,
		IdleBoundary:         10 * time.Second,
		PublishBound:         5 * time.Second,
		CompatibilityTimeout: 20 * time.Second,
		WorkPoll:             time.Second,
		WorkStates:           work,
	}
	for _, change := range adjust {
		change(&composeOptions, &hostOptions)
	}

	built, err := host.New(hostOptions)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	composeOptions.Host = built
	composed, err := New(composeOptions)
	if err != nil {
		t.Fatalf("compose.New: %v", err)
	}
	return &fixture{
		t: t, trace: trace, clock: clock, store: store, directory: directory,
		rig: rig, runtime: runtimeSession, work: work, host: built, hostOptions: hostOptions, svc: composed,
	}
}

// start starts the composed Host and registers its stop.
func (f *fixture) start() {
	f.t.Helper()
	if err := f.svc.Start(f.t.Context()); err != nil {
		f.t.Fatalf("Start: %v", err)
	}
}

// attach makes one session resident.
func (f *fixture) attach(tenant sessionwire.TenantID, session sessionwire.SessionID) residency.Residency {
	f.t.Helper()
	held, err := f.svc.Attach(f.t.Context(), residency.Request{
		TenantID:  tenant,
		SessionID: session,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenant, ActorID: "actor-a"},
	})
	if err != nil {
		f.t.Fatalf("Attach(%s/%s): %v", tenant, session, err)
	}
	return held
}

// hostClock satisfies host.Clock over the fake, so the Host and the composition
// read one timeline.
type hostClock struct {
	inner *fakeClock
}

func (c hostClock) Now() time.Time                       { return c.inner.Now() }
func (c hostClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// testRigSessionID is Harness's identity for the session under test, minted
// once so a restore and a create name the same runtime.
var testRigSessionID = func() uuid.UUID {
	id, err := uuid.New()
	if err != nil {
		panic(err)
	}
	return id
}()

// ---------------------------------------------------------------------------
// Fixture controls
// ---------------------------------------------------------------------------

// fireWhenWaiting blocks until something is waiting on a duration and then
// releases it.
//
// IT IS A RENDEZVOUS AND NOT A POLL, which is what lets this package hold to
// "no test sleeps" while still driving a loop that arms its bound inside a
// select. A test that fired blind would race the loop's own arming and pass or
// fail on scheduling.
func (c *fakeClock) fireWhenWaiting(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case armed := <-c.registered:
			if armed != d {
				continue
			}
			if c.fire(d) == 0 {
				continue
			}
			return
		case <-deadline.C:
			t.Fatalf("nothing armed a %v bound", d)
		}
	}
}

// holdReleasing makes the store block the first durable `releasing` observation
// until releaseHold is called, so a test can stand between the drain's
// acknowledgement and its per-session work.
func (s *fakeStore) holdReleasing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hold = make(chan struct{})
}

// releaseHold lets a held observation through.
func (s *fakeStore) releaseHold() {
	s.mu.Lock()
	hold := s.hold
	s.hold = nil
	s.mu.Unlock()
	if hold != nil {
		close(hold)
	}
}

// grantFor returns the journal grant taken for one session.
func (s *fakeStore) grantFor(key registry.Key) *fakeGrant {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grants[key]
}

// controllableSession is the launched runtime every composition test drives.
//
// IT IS NOT testkit.FullSession, and the difference is the one capability these
// tests must control: that fixture's WaitIdle returns nil immediately, which
// makes four of the compatibility wait's five endings unreachable — a session
// that is always idle can never reach a gate, never time out and never be
// stopped mid-wait. Everything else is the same six capabilities.
type controllableSession struct {
	id uuid.UUID

	mu       sync.Mutex
	released int
	holding  chan struct{}
	stopped  chan struct{}
	epoch    uint64
	held     bool
}

func newControllableSession(id uuid.UUID) *controllableSession {
	return &controllableSession{id: id, stopped: make(chan struct{}), epoch: 1, held: true}
}

// ID is Harness's identity for this session.
func (s *controllableSession) ID() uuid.UUID { return s.id }

// HoldIdle makes WaitIdle block until the context ends.
func (s *controllableSession) HoldIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holding = make(chan struct{})
}

// WaitIdle returns immediately unless the session is being held non-idle.
func (s *controllableSession) WaitIdle(ctx context.Context) error {
	s.mu.Lock()
	holding := s.holding
	s.mu.Unlock()
	if holding == nil {
		return ctx.Err()
	}
	select {
	case <-holding:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GoIdle releases a held session.
func (s *controllableSession) GoIdle() {
	s.mu.Lock()
	holding := s.holding
	s.holding = nil
	s.mu.Unlock()
	if holding != nil {
		close(holding)
	}
}

// Done reports when the runtime has stopped answering.
func (s *controllableSession) Done() <-chan struct{} { return s.stopped }

// Stop makes the runtime stop answering.
func (s *controllableSession) Stop() { close(s.stopped) }

// ReleaseResidency records a nonterminal release.
func (s *controllableSession) ReleaseResidency(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released++
	return nil
}

// Released reports how many times residency was released.
func (s *controllableSession) Released() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released
}

// SubscribeCommitted returns a closed stream, so a relay ends at once.
func (s *controllableSession) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	published := make(chan sessionwire.EnduringPublication)
	close(published)
	return published, nil
}

// ApplyCommand accepts any admitted command.
func (s *controllableSession) ApplyCommand(context.Context, department.RuntimeCommand) error {
	return nil
}

// LeaseEpoch is the RUNTIME's journal grant, never Host's residency grant.
func (s *controllableSession) LeaseEpoch() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.held
}

// defaultFixtureWarmTTL is the warm TTL every fixture Host carries unless a
// test changes it. It is named so a test asserting a DIFFERENT value can state
// that it differs, which is what stops that assertion from passing against a
// composition that ignored the Host.
const defaultFixtureWarmTTL = 90 * time.Second

// hostWithExpiry rebuilds the fixture's Host with a different registry expiry.
func hostWithExpiry(f *fixture, expiry time.Duration) (*host.Host, error) {
	options := f.hostOptions
	options.RegistryExpiry = expiry
	options.RegistryHeartbeat = expiry / host.MinHeartbeatsBeforeExpiry
	return host.New(options)
}

// awaitRearming blocks until a warm countdown is armed and returns its duration.
func (f *fixture) awaitRearming(t *testing.T) time.Duration {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if armed := f.clock.rearmings(); len(armed) != 0 {
			return armed[0]
		}
		select {
		case <-f.clock.armedWarm:
		case <-deadline.C:
			t.Fatal("no warm countdown was armed")
			return 0
		}
	}
}

// count reports how many times a step was recorded.
func (r *recorder) count(step string) int {
	total := 0
	for _, recorded := range r.trace() {
		if recorded == step {
			total++
		}
	}
	return total
}

// loseLease closes a session's residency grant loss channel.
func (s *fakeStore) loseLease(key registry.Key) {
	s.mu.Lock()
	lease := s.leases[key]
	s.mu.Unlock()
	if lease != nil {
		close(lease.lost)
	}
}

// awaitOutcome runs a compatibility wait, performs one action once the wait is
// demonstrably in flight, and returns what the wait reported.
//
// THE RENDEZVOUS IS THE WAIT'S OWN ARMING AND NOT A SLEEP. AwaitSessionIdle arms
// its timeout and then its poll before it selects, so observing both
// registrations on the clock is proof that the wait is inside the select and
// that firing either bound will reach it. Acting earlier is the race this helper
// exists to remove: a runtime stopped, or a grant lost, before the wait has
// taken its handle is a DIFFERENT scenario — the heartbeat tears the residency
// down and the wait then correctly reports that this Host holds no such session.
func (f *fixture) awaitOutcome(t *testing.T, ctx context.Context, act func()) (CompatibilityOutcome, error) {
	t.Helper()
	type result struct {
		outcome CompatibilityOutcome
		err     error
	}
	results := make(chan result, 1)
	go func() {
		outcome, err := f.svc.AwaitSessionIdle(ctx, keyA)
		results <- result{outcome: outcome, err: err}
	}()

	f.clock.awaitRegistration(t, f.svc.options.CompatibilityTimeout)
	f.clock.awaitRegistration(t, f.svc.options.WorkPoll)
	act()

	select {
	case answer := <-results:
		return answer.outcome, answer.err
	case <-time.After(10 * time.Second):
		t.Fatal("the compatibility wait never returned")
		return "", nil
	}
}

// fireIfRearmed waits briefly for a fresh arming of watched, and fires fallback
// only if one appears.
//
// It is the deterministic alternative to firing two bounds at once. A wait that
// returned at the first bound arms nothing further, so nothing is fired and the
// fallback stays unreachable; a wait that went round its loop arms the watched
// bound again, which is the observable difference.
func (c *fakeClock) fireIfRearmed(watched, fallback time.Duration) {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case armed := <-c.registered:
			if armed != watched {
				continue
			}
			c.fire(fallback)
			return
		case <-deadline.C:
			return
		}
	}
}

// awaitRegistration blocks until something arms a bound of this duration.
func (c *fakeClock) awaitRegistration(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case armed := <-c.registered:
			if armed == d {
				return
			}
		case <-deadline.C:
			t.Fatalf("nothing armed a %v bound", d)
		}
	}
}

// bindRequest is one Factory route request for a resident session.
func (f *fixture) bindRequest(tenant sessionwire.TenantID, session sessionwire.SessionID, epoch uint64) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               tenant,
		SessionID:              session,
		HostID:                 f.host.ID(),
		HostGeneration:         testGen,
		LeaseEpoch:             epoch,
		RuntimeCompatibilityID: string(testCompat),
		IdempotencyKey:         "idem-" + string(session),
	}
}
