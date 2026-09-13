package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

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

// waiting reports how many channels are currently filed under one duration.
//
// THE COUNT IS THE RENDEZVOUS AND THE REGISTRATION ANNOUNCEMENT IS NOT, when
// two goroutines arm the same bound. The compatibility wait and the work
// sampler both arm WorkPoll on this one clock, so a test that waited for a
// registration ANNOUNCEMENT could be released by the sampler's and then fire a
// bound the wait had not yet taken a handle on — the wait would never see it.
// A waiter count discriminates: the wait's own handle is the (n+1)th.
func (c *fakeClock) waiting(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters[d])
}

// awaitWaiters blocks until at least want channels are filed under a duration.
func (c *fakeClock) awaitWaiters(t *testing.T, d time.Duration, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if c.waiting(d) >= want {
			return
		}
		select {
		case <-c.registered:
		case <-deadline.C:
			t.Fatalf("only %d of %d waiters ever armed a %v bound", c.waiting(d), want, d)
		}
	}
}

// waitForWaiters is awaitWaiters without a testing.T, for use off the test
// goroutine. It reports whether the count was reached before the deadline.
func (c *fakeClock) waitForWaiters(d time.Duration, want int, within time.Duration) bool {
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	for {
		if c.waiting(d) >= want {
			return true
		}
		select {
		case <-c.registered:
		case <-deadline.C:
			return false
		}
	}
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
	mu      sync.Mutex
	steps   []string
	watcher func(step string)
}

// watch installs an observer called with each step AS IT IS RECORDED.
//
// It is how a test asserts on state that only exists WHILE a step is running —
// "the tenant transports were still open when the drain checkpointed" is a
// statement about an instant, and no after-the-fact trace can carry it. The
// observer runs outside the recorder's lock so that it may read the
// composition, and it must not record into this recorder.
func (r *recorder) watch(observe func(step string)) {
	r.mu.Lock()
	r.watcher = observe
	r.mu.Unlock()
}

func (r *recorder) record(step string) {
	r.mu.Lock()
	r.steps = append(r.steps, step)
	watcher := r.watcher
	r.mu.Unlock()
	if watcher != nil {
		watcher(step)
	}
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
	modes  map[registry.Key]protocolMode
	rows   []sessionwire.HostLinkRegistryObservation
	hold   chan struct{}
}

// protocolMode is the immutable catalog binding sessionstore pins on a session,
// modelled here because Host's behaviour differs by it and no fixture in this
// package used to vary it.
//
// A FAKE THAT ANSWERS BOTH MODES THE SAME WAY IS LOOSER THAN THE STORE, and it
// was: this fixture's opener minted a grant for anything it was asked about, so
// every composed attach test ran against a store that cannot exist. The released
// store binds the mode at AcquireResidency and then refuses the two families
// against each other, and both refusals are modelled below.
type protocolMode string

const (
	// modeDisposition is what AcquireResidency pins, and therefore the only
	// mode a Host can ever hold a session in.
	modeDisposition protocolMode = "disposition"

	// modeLegacy is a session bound to the other family before this Host saw
	// it. AcquireResidency refuses it outright, so a composed Host fails at
	// step 2 and takes nothing.
	modeLegacy protocolMode = "legacy"
)

func newFakeStore(trace *recorder) *fakeStore {
	return &fakeStore{
		trace:  trace,
		leases: map[registry.Key]*fakeLease{},
		modes:  map[registry.Key]protocolMode{},
	}
}

// bind pins one session's catalog protocol mode. An unbound session is in
// disposition mode, which is what AcquireResidency would pin for it.
func (s *fakeStore) bind(key registry.Key, mode protocolMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modes[key] = mode
}

// mode reports the binding a session carries.
func (s *fakeStore) mode(key registry.Key) protocolMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bound, held := s.modes[key]; held {
		return bound
	}
	return modeDisposition
}

// AcquireSessionLease grants residency, and REFUSES A SESSION BOUND TO THE OTHER
// FAMILY.
//
// That is finding F16(a) modelled rather than restated: AcquireResidency reads
// the immutable catalog binding and refuses anything that is not
// ProtocolModeDisposition with catalogInvalid("binding.protocol_mode"). A fake
// that granted one would let a composed attach proceed past a step the released
// store stops, and every assertion after it would be about a Host that cannot
// exist.
func (s *fakeStore) AcquireSessionLease(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (residency.Lease, error) {
	s.trace.record("lease.acquire")
	key := registry.Key{TenantID: tenant, SessionID: session}
	if mode := s.mode(key); mode != modeDisposition {
		return nil, fmt.Errorf("catalog invalid (binding.protocol_mode): the session is bound to %s and residency is granted only in disposition mode", mode)
	}
	lease := &fakeLease{trace: s.trace, epoch: 9, lost: make(chan struct{})}
	s.mu.Lock()
	s.leases[key] = lease
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
//
// IT CARRIES BOTH HIGH-WATER MARKS, for the reason regression_test.go's
// durableCommands now does: sessionstore fences the epoch and then the order,
// and a fake modelling only the order cannot produce either refusal. A fake
// looser than its dependency makes every test over it silent about the cases the
// dependency actually has.
type fakeCursors struct {
	mu          sync.Mutex
	cursor      uint64
	cursorEpoch uint64
}

func (c *fakeCursors) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor, nil
}

// SaveCursor takes the EPOCH FIRST AND THE ORDER SECOND. commands.CursorWrites
// declares SaveCursor(ctx, tenant, session, epoch, order); both are uint64, so
// a transposition compiles. This double had the two the wrong way round and
// therefore recorded the lease epoch as the consumed acceptance order. No test
// in this file read the value back, so nothing failed — which is exactly why it
// survived, and why a double whose stored value nobody reads is worth getting
// right anyway: the next test to read it inherits the defect.
func (c *fakeCursors) SaveCursor(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, epoch uint64, order uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch < c.cursorEpoch {
		return fmt.Errorf("fakeCursors: epoch %d is below the committed %d", epoch, c.cursorEpoch)
	}
	if order < c.cursor {
		return fmt.Errorf("fakeCursors: order %d is below the committed %d", order, c.cursor)
	}
	c.cursorEpoch, c.cursor = epoch, order
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

// fakeAuth is the service authenticator, and it HONOURS THE TENANT IT IS ASKED
// ABOUT.
//
// A service credential is minted FOR one tenant, so presenting tenant-a's
// credential to a transport asking about tenant-b fails here exactly as it
// fails against a real authenticator. The first version of this fake took one
// credential and ignored the tenant argument entirely, and that is the reason
// a whole class of defect below it was invisible: a fake looser than the
// dependency it stands for cannot defend anything downstream of it. With the
// tenant discarded, the transport's own TenantID could be latched to a
// constant — a tenant-a credential reaching tenant-b's Multiplexer — and the
// entire suite stayed green.
//
// It records what it was asked, because "the transport authenticated this
// connection as the tenant on its path" is a statement about the ARGUMENT and
// not only about the outcome.
type fakeAuth struct {
	mu       sync.Mutex
	verified []string
}

// credentialFor is the service credential minted for one tenant.
func credentialFor(tenant sessionwire.TenantID) string {
	return "service-credential-for-" + string(tenant)
}

func (a *fakeAuth) VerifyTenant(_ context.Context, tenant sessionwire.TenantID, credential string) error {
	a.mu.Lock()
	a.verified = append(a.verified, string(tenant)+":"+credential)
	a.mu.Unlock()
	if credential != credentialFor(tenant) {
		return errors.New("this credential was not minted for that tenant")
	}
	return nil
}

// verifications reports every (tenant, credential) pair a transport asked
// about, in order.
func (a *fakeAuth) verifications() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.verified...)
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
	auth        *fakeAuth
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
	auth := &fakeAuth{}

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
		Auth:              auth,
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
		Inbox:                &fakeInbox{},
		Cursors:              &fakeCursors{},
		Targets:              directory,
		Checkpointer:         &fakeCheckpointer{trace: trace},
		Auth:                 auth,
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
		rig: rig, runtime: runtimeSession, work: work, auth: auth, host: built, hostOptions: hostOptions, svc: composed,
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
// THE RENDEZVOUS IS THE WAIT'S OWN HANDLE AND NOT A SLEEP. AwaitSessionIdle
// arms its timeout and then its poll before it selects, so this clock holding
// one MORE waiter on each of those bounds than it held before the wait started
// is proof that the wait is inside the select and that firing either bound will
// reach it. Acting earlier is one of the two races this helper removes: a
// runtime stopped, or a grant lost, before the wait has taken its handle is a
// DIFFERENT scenario — the heartbeat tears the residency down and the wait then
// correctly reports that this Host holds no such session.
//
// IT COUNTS WAITERS AND DOES NOT WATCH ANNOUNCEMENTS, because the work sampler
// arms WorkPoll on this same clock. An announcement-based rendezvous could be
// released by the SAMPLER'S arming while the wait had taken no handle at all,
// and the test would then fire a bound into a channel nobody was reading. That
// was a real intermittent failure at roughly two runs in a thousand.
//
// A fallback bound, if one is named, is released only once the wait has GONE
// ROUND ITS LOOP — that is, once the WorkPoll waiter count is back to what it
// was immediately before act fired it. That number is derived and not chosen:
// it is the sampler's arm plus the wait's, so a wait that returned instead can
// only ever bring it back to the sampler's alone. Firing both bounds at once
// would instead leave the select with two ready cases, and Go picks among those
// uniformly at random, so it would be flaky in the direction that HIDES a
// defect.
func (f *fixture) awaitOutcome(t *testing.T, ctx context.Context, act func(), fallback ...time.Duration) (CompatibilityOutcome, error) {
	t.Helper()
	if len(fallback) > 1 {
		t.Fatalf("awaitOutcome takes at most one fallback bound, got %d", len(fallback))
	}
	type result struct {
		outcome CompatibilityOutcome
		err     error
	}
	results := make(chan result, 1)

	timeoutWaiters := f.clock.waiting(f.svc.options.CompatibilityTimeout) + 1
	pollWaiters := f.clock.waiting(f.svc.options.WorkPoll) + 1
	go func() {
		outcome, err := f.svc.AwaitSessionIdle(ctx, keyA)
		results <- result{outcome: outcome, err: err}
	}()

	f.clock.awaitWaiters(t, f.svc.options.CompatibilityTimeout, timeoutWaiters)
	f.clock.awaitWaiters(t, f.svc.options.WorkPoll, pollWaiters)
	act()

	if len(fallback) == 1 {
		rearmed := make(chan struct{})
		go func() {
			if f.clock.waitForWaiters(f.svc.options.WorkPoll, pollWaiters, 5*time.Second) {
				close(rearmed)
			}
		}()
		select {
		case answer := <-results:
			return answer.outcome, answer.err
		case <-rearmed:
			f.clock.fire(fallback[0])
		}
	}

	select {
	case answer := <-results:
		return answer.outcome, answer.err
	case <-time.After(10 * time.Second):
		t.Fatal("the compatibility wait never returned")
		return "", nil
	}
}

// hostLinkConnect dials one tenant's HostLink endpoint over a real WebSocket
// and sends the Centrifuge connect frame carrying a service credential. It
// reports whether the connection was accepted.
//
// IT HAS TO BE A REAL UPGRADE. VerifyTenant is reached only from the
// transport's OnConnecting handler, which runs after the WebSocket handshake,
// so an ordinary GET — which is what the routing test makes — authenticates
// nothing at all. That is precisely why a latched transport TenantID was
// invisible: the only assertions over the per-tenant endpoints were a routing
// status code and a pointer comparison, and neither reaches the authenticator.
func hostLinkConnect(t *testing.T, serverURL string, tenant sessionwire.TenantID, credential string) bool {
	t.Helper()
	endpoint := "ws" + strings.TrimPrefix(serverURL, "http") + HostLinkPathPrefix + string(tenant)
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if err != nil {
		t.Fatalf("dial %s (response %#v): %v", endpoint, response, err)
	}
	defer connection.Close()

	negotiation, err := json.Marshal(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		t.Fatalf("marshal negotiation request: %v", err)
	}
	command := struct {
		ID      uint32 `json:"id"`
		Connect struct {
			Token string          `json:"token"`
			Data  json.RawMessage `json:"data"`
		} `json:"connect"`
	}{ID: 1}
	command.Connect.Token = credential
	command.Connect.Data = negotiation
	frame, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal connect frame: %v", err)
	}
	// The deadlines below bound a REAL network read against a real embedded
	// transport, which is the one thing in this package the fake clock does not
	// drive. They are failure bounds and nothing in the test waits for them.
	if err := connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("write connect frame: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, payload, err := connection.ReadMessage()
	if err != nil {
		var closed *websocket.CloseError
		if errors.As(err, &closed) {
			return false
		}
		t.Fatalf("read connect reply: %v", err)
	}
	var reply struct {
		Connect *json.RawMessage `json:"connect"`
		Error   *struct {
			Code uint32 `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &reply); err != nil {
		t.Fatalf("decode connect reply %q: %v", payload, err)
	}
	return reply.Connect != nil && reply.Error == nil
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
