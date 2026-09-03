package commands

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Fixture constants
// ---------------------------------------------------------------------------

const (
	// testTenant, testSession and testAgent are the identities every fixture
	// runs under.
	testTenant  = sessionwire.TenantID("tenant-orchestration")
	testSession = sessionwire.SessionID("session-inbox")
	testAgent   = sessionwire.AgentID("agent-reviewer")

	// testEpoch is the lease epoch the consumer writes its cursor under. It is
	// DELIBERATELY NEITHER 0 NOR 1 and is distinct from every acceptance order
	// in this file, so a cursor stamped with the wrong number is visible.
	testEpoch uint64 = 37

	// testBatch is host.ReconcileBatch for the fixture, and it is small so a
	// full page is reachable in a table.
	testBatch = 3

	// testReconcileInterval is host.ReconcileInterval for the fixture.
	testReconcileInterval = 23 * time.Second
)

// testClockAt is the instant every fixture clock starts at.
var testClockAt = time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// The manual clock
// ---------------------------------------------------------------------------

// manualClock is a host.Clock whose timers fire only when a test says so.
//
// NO SLEEPING AND NO POLLING. Every wait below is a blocking receive on
// something the subject itself produces: the loop asking for its next timer.
// The only time.After in this file is a DIAGNOSTIC on the failure arm.
//
// The timers are hand-built as &time.Timer{C: ch}, which is the only way to
// hand back a timer a test controls. SUCH A TIMER PANICS ON Stop, so the loop
// must never call it.
type manualClock struct {
	mu        sync.Mutex
	now       time.Time
	requested []time.Duration

	created chan chan time.Time
	current chan time.Time
}

// newManualClock returns a clock whose timers a test drives.
func newManualClock(at time.Time) *manualClock {
	return &manualClock{now: at, created: make(chan chan time.Time, 64)}
}

// Now reports the fixture instant.
func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer records the duration asked for and returns a timer the test fires.
func (c *manualClock) NewTimer(d time.Duration) *time.Timer {
	c.mu.Lock()
	c.requested = append(c.requested, d)
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.created <- ch
	return &time.Timer{C: ch}
}

// requests returns every duration a timer was asked for, in order.
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
		return
	}
	select {
	case ch := <-c.created:
		c.current = ch
	case <-time.After(10 * time.Second):
		t.Fatal("the consumer never asked for a timer")
	}
}

// fire releases the timer the loop is parked on.
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

// listCall records one ListOrdered request, so a test can assert the page was
// BOUNDED and was asked for from the durable cursor rather than from zero.
type listCall struct {
	TenantID   sessionwire.TenantID
	SessionID  sessionwire.SessionID
	AfterOrder uint64
	Limit      int
}

// fakeInbox is an Inbox that answers from a scripted script of pages.
type fakeInbox struct {
	mu sync.Mutex

	// all, when non-empty, makes this inbox answer like a CONFORMING STORE:
	// the records strictly after the requested order, truncated to the limit.
	// A scripted page sequence cannot express "the second caller saw the first
	// caller's effect", which is the whole subject of a concurrency test.
	all []Command

	// pages is consumed one entry per call. The LAST entry is repeated once
	// exhausted, so a loop test does not have to script every idle pass.
	pages [][]Command
	// errs is consumed alongside pages; a nil entry means the page is returned.
	errs []error

	calls []listCall
}

// ListOrdered returns the next scripted page.
func (f *fakeInbox) ListOrdered(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, after uint64, limit int) ([]Command, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, listCall{TenantID: tenant, SessionID: session, AfterOrder: after, Limit: limit})
	index := len(f.calls) - 1
	if len(f.all) > 0 {
		page := []Command{}
		for _, record := range f.all {
			if record.AcceptedOrder > after && len(page) < limit {
				page = append(page, record)
			}
		}
		return page, nil
	}
	var err error
	if index < len(f.errs) {
		err = f.errs[index]
	} else if len(f.errs) > 0 {
		err = f.errs[len(f.errs)-1]
	}
	if err != nil {
		return nil, err
	}
	if len(f.pages) == 0 {
		return nil, nil
	}
	if index >= len(f.pages) {
		index = len(f.pages) - 1
	}
	return append([]Command(nil), f.pages[index]...), nil
}

// requests returns every ListOrdered call, in order.
func (f *fakeInbox) requests() []listCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]listCall(nil), f.calls...)
}

// saveCall records one durable cursor write.
type saveCall struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Epoch     uint64
	Order     uint64
}

// fakeCursors is a Cursors that keeps one durable cursor in memory.
type fakeCursors struct {
	mu sync.Mutex

	stored uint64
	// loads is the scripted sequence of values LoadCursor returns. When empty
	// the stored value is returned, which is what a conforming store does.
	loads    []uint64
	loadErr  error
	saveErr  error
	loadHits int
	saves    []saveCall
}

// LoadCursor returns the durable consumption cursor.
func (f *fakeCursors) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return 0, f.loadErr
	}
	index := f.loadHits
	f.loadHits++
	if index < len(f.loads) {
		return f.loads[index], nil
	}
	return f.stored, nil
}

// SaveCursor advances the durable consumption cursor.
func (f *fakeCursors) SaveCursor(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, epoch uint64, order uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves = append(f.saves, saveCall{TenantID: tenant, SessionID: session, Epoch: epoch, Order: order})
	if f.saveErr != nil {
		return f.saveErr
	}
	f.stored = order
	return nil
}

// written returns every durable cursor write, in order.
func (f *fakeCursors) written() []saveCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]saveCall(nil), f.saves...)
}

// scriptedOutcome is one Process verdict a test scripts by CommandID.
type scriptedOutcome struct {
	outcome Outcome
	err     error
}

// fakeProcessor is a Processor that records what it was handed and answers
// from a script keyed by CommandID.
//
// Its DEFAULT is a terminal applied outcome, so a test that cares only about
// ordering does not have to script every record — and a test about blocking
// must state the block, which is the direction that cannot pass by accident.
type fakeProcessor struct {
	mu sync.Mutex

	script  map[sessionwire.CommandID]scriptedOutcome
	handled []Command
	// before, when set, runs before each Process answers. It is how a test
	// cancels a context or loses a grant partway through a page.
	before func(Command)
}

// Process records the command and answers from the script.
func (f *fakeProcessor) Process(_ context.Context, command Command) (Outcome, error) {
	f.mu.Lock()
	before := f.before
	f.mu.Unlock()
	if before != nil {
		before(command)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handled = append(f.handled, command)
	if scripted, ok := f.script[command.CommandID]; ok {
		return scripted.outcome, scripted.err
	}
	return Outcome{State: StateApplied}, nil
}

// processed returns every command handed to Process, in order.
func (f *fakeProcessor) processed() []Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Command(nil), f.handled...)
}

// processedIDs returns the CommandIDs handed to Process, in order.
func (f *fakeProcessor) processedIDs() []sessionwire.CommandID {
	ids := []sessionwire.CommandID{}
	for _, command := range f.processed() {
		ids = append(ids, command.CommandID)
	}
	return ids
}

// fakeFence is a Fence a test can end from the outside.
type fakeFence struct {
	mu sync.Mutex

	lost   chan struct{}
	writes int
}

// newFakeFence returns a held fence.
func newFakeFence() *fakeFence { return &fakeFence{lost: make(chan struct{})} }

// Held reports the typed reason ownership is gone, or nil.
func (f *fakeFence) Held() error {
	select {
	case <-f.lost:
		return errTestGrantGone
	default:
		return nil
	}
}

// Lost closes when ownership is gone.
func (f *fakeFence) Lost() <-chan struct{} { return f.lost }

// Write performs one fenced write.
func (f *fakeFence) Write(run func() error) error {
	if err := f.Held(); err != nil {
		return err
	}
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return run()
}

// close closes the loss channel, which is the OTHER way ownership ends.
func (f *fakeFence) close() { close(f.lost) }

// fencedWrites reports how many writes went through the fence.
func (f *fakeFence) fencedWrites() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

// errTestGrantGone is what the fake fence reports when the grant is gone.
var errTestGrantGone = errors.New("commands_test: the grant is gone")

// errTestStore is an ambiguous store failure.
var errTestStore = errors.New("commands_test: the store is unreachable")

// ---------------------------------------------------------------------------
// Host fixture
// ---------------------------------------------------------------------------

// stubTarget is the minimum department.LaunchTarget a Host needs to exist.
type stubTarget struct{}

// CompatibilityID identifies the stub build.
func (stubTarget) CompatibilityID() department.CompatibilityID { return "rig-test-1" }

// Capabilities declares a pooled-capable target of weight one.
func (stubTarget) Capabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

// Create is never called by this package's tests.
func (stubTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("commands_test: the stub target launches nothing")
}

// Restore is never called by this package's tests.
func (stubTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("commands_test: the stub target launches nothing")
}

// stubSessionStore satisfies host.SessionStore.
type stubSessionStore struct{}

// LoadSession is never called by this package's tests.
func (stubSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, errors.New("commands_test: the stub session store loads nothing")
}

// stubWorkspaces satisfies host.WorkspaceProvider.
type stubWorkspaces struct{}

// EnsureWorkspace is never called by this package's tests.
func (stubWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", errors.New("commands_test: the stub workspace provider materializes nothing")
}

// stubAuth satisfies host.AuthVerifier.
type stubAuth struct{}

// VerifyTenant accepts every credential.
func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// newTestHost builds the Host whose clock, reconcile interval and reconcile
// batch the consumer derives everything from.
func newTestHost(t *testing.T, clock host.Clock) *host.Host {
	t.Helper()
	dept, err := department.New([]department.Registration{{AgentID: testAgent, Target: stubTarget{}}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	built, err := host.New(host.Options{
		HostID:            sessionwire.HostID("host-inbox"),
		TenantID:          testTenant,
		InternalEndpoint:  sessionwire.InternalEndpoint("ws://10.0.0.7:9443/hostlink"),
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      stubSessionStore{},
		Workspaces:        stubWorkspaces{},
		Clock:             clock,
		Auth:              stubAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    31 * time.Second,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: testReconcileInterval,
		ReconcileBatch:    testBatch,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	return built
}

// consumerFixture is one consumer and everything it was built from.
type consumerFixture struct {
	t         *testing.T
	clock     *manualClock
	host      *host.Host
	inbox     *fakeInbox
	cursors   *fakeCursors
	processor *fakeProcessor
	fence     *fakeFence
	consumer  *Consumer
}

// newConsumerFixture builds a consumer over fakes, applying each configure
// function to the fixture BEFORE the consumer is constructed.
func newConsumerFixture(t *testing.T, configure ...func(*consumerFixture)) *consumerFixture {
	t.Helper()
	clock := newManualClock(testClockAt)
	f := &consumerFixture{
		t:         t,
		clock:     clock,
		inbox:     &fakeInbox{},
		cursors:   &fakeCursors{},
		processor: &fakeProcessor{script: map[sessionwire.CommandID]scriptedOutcome{}},
		fence:     newFakeFence(),
	}
	f.host = newTestHost(t, clock)
	for _, apply := range configure {
		apply(f)
	}
	consumer, err := NewConsumer(Options{
		Host:       f.host,
		Key:        registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch,
		Inbox:      f.inbox,
		Cursors:    f.cursors,
		Processor:  f.processor,
		Fence:      f.fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	f.consumer = consumer
	return f
}

// command builds one inbox record at an acceptance order.
func command(order uint64, state State) Command {
	return Command{
		TenantID:      testTenant,
		SessionID:     testSession,
		CommandID:     sessionwire.CommandID(fmt.Sprintf("v1:command-%d", order)),
		AcceptedOrder: order,
		State:         state,
	}
}

// commandID is the CommandID command() gives an acceptance order.
func commandID(order uint64) sessionwire.CommandID {
	return sessionwire.CommandID(fmt.Sprintf("v1:command-%d", order))
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// TestNewConsumerRefusesAnIncompleteComposition asserts each collaborator is
// required, because a consumer missing one would fail at its first pass rather
// than at construction, by which time it owns a resident session.
func TestNewConsumerRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	clock := newManualClock(testClockAt)
	built := newTestHost(t, clock)
	complete := func() Options {
		return Options{
			Host:       built,
			Key:        registry.Key{TenantID: testTenant, SessionID: testSession},
			LeaseEpoch: testEpoch,
			Inbox:      &fakeInbox{},
			Cursors:    &fakeCursors{},
			Processor:  &fakeProcessor{},
			Fence:      newFakeFence(),
		}
	}
	if _, err := NewConsumer(complete()); err != nil {
		t.Fatalf("the complete composition was refused: %v", err)
	}

	for _, testCase := range []struct {
		name   string
		field  string
		mutate func(*Options)
	}{
		{"no host", "Host", func(o *Options) { o.Host = nil }},
		{"no inbox", "Inbox", func(o *Options) { o.Inbox = nil }},
		{"no cursors", "Cursors", func(o *Options) { o.Cursors = nil }},
		{"no processor", "Processor", func(o *Options) { o.Processor = nil }},
		{"no fence", "Fence", func(o *Options) { o.Fence = nil }},
		{"no tenant", "Key", func(o *Options) { o.Key.TenantID = "" }},
		{"no session", "Key", func(o *Options) { o.Key.SessionID = "" }},
		{"zero epoch", "LeaseEpoch", func(o *Options) { o.LeaseEpoch = 0 }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			options := complete()
			testCase.mutate(&options)
			_, err := NewConsumer(options)
			var invalid *InvalidConsumerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("want *InvalidConsumerOptionsError, got %v", err)
			}
			if invalid.Field != testCase.field {
				t.Errorf("Field = %q, want %q", invalid.Field, testCase.field)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// One pass over the durable order
// ---------------------------------------------------------------------------

// TestReconcileConsumesFromTheDurableCursor is attach-time reconciliation: the
// pass resumes from the durable cursor rather than from the beginning of the
// session's history, and it advances the cursor to the last record it consumed.
func TestReconcileConsumesFromTheDurableCursor(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.cursors.stored = 12
		f.inbox.pages = [][]Command{{command(13, StatePending), command(14, StatePending)}, nil}
	})

	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if want := []sessionwire.CommandID{commandID(13), commandID(14)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v", f.processor.processedIDs(), want)
	}
	requests := f.inbox.requests()
	if len(requests) != 1 {
		t.Fatalf("ListOrdered was called %d times, want 1", len(requests))
	}
	if requests[0].AfterOrder != 12 {
		t.Errorf("ListOrdered after = %d, want the durable cursor 12; a pass that starts at zero rescans history the cursor exists to skip", requests[0].AfterOrder)
	}
	if requests[0].TenantID != testTenant || requests[0].SessionID != testSession {
		t.Errorf("ListOrdered scope = (%q, %q), want (%q, %q)", requests[0].TenantID, requests[0].SessionID, testTenant, testSession)
	}
	if result.Cursor != 14 {
		t.Errorf("result cursor = %d, want 14", result.Cursor)
	}
	if result.Examined != 2 {
		t.Errorf("examined = %d, want 2", result.Examined)
	}
	if result.Consumed != 2 {
		t.Errorf("consumed = %d, want 2", result.Consumed)
	}
	writes := f.cursors.written()
	if len(writes) != 1 {
		t.Fatalf("the cursor was written %d times, want exactly 1 for the whole pass", len(writes))
	}
	if writes[0].Order != 14 {
		t.Errorf("cursor written at %d, want 14", writes[0].Order)
	}
	if writes[0].Epoch != testEpoch {
		t.Errorf("cursor written under epoch %d, want the lease epoch %d", writes[0].Epoch, testEpoch)
	}
}

// TestReconcileRequestsABoundedPage asserts the page limit is the Host's
// configured reconcile batch and nothing wider.
func TestReconcileRequestsABoundedPage(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.cursors.stored = 4
		f.inbox.pages = [][]Command{nil}
	})
	if _, err := f.consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	requests := f.inbox.requests()
	if len(requests) != 1 {
		t.Fatalf("ListOrdered was called %d times, want 1", len(requests))
	}
	if requests[0].Limit != f.host.ReconcileBatch() {
		t.Errorf("limit = %d, want the Host's ReconcileBatch %d", requests[0].Limit, f.host.ReconcileBatch())
	}
}

// TestReconcileReportsMoreWhenThePageWasFull asserts a pass that consumed a
// full page reports that more durable work may remain, which is what lets the
// loop converge without waiting a whole interval per page.
func TestReconcileReportsMoreWhenThePageWasFull(t *testing.T) {
	t.Parallel()

	full := []Command{command(1, StatePending), command(2, StatePending), command(3, StatePending)}
	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{full, {command(4, StatePending)}, nil}
	})

	first, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if !first.More {
		t.Errorf("More = false after a full page of %d; the loop would wait a whole interval per page", len(full))
	}
	second, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if second.More {
		t.Errorf("More = true after a short page; there is nothing more to fetch")
	}
	if got := f.inbox.requests()[1].AfterOrder; got != 3 {
		t.Errorf("the second page was asked for after %d, want 3", got)
	}
}

// TestReconcileAdvancesPastTerminalRecordsWithoutTheProcessor asserts an
// applied or rejected record needs nothing: §10.4 makes those mutually
// exclusive terminal CAS states, so re-driving one into the applier would be
// work with no possible effect.
func TestReconcileAdvancesPastTerminalRecordsWithoutTheProcessor(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StateApplied), command(2, StateRejected)}}
	})
	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if handled := f.processor.processedIDs(); len(handled) != 0 {
		t.Errorf("the processor was handed %v; a terminal record has nothing left to apply", handled)
	}
	if result.Cursor != 2 {
		t.Errorf("cursor = %d, want 2", result.Cursor)
	}
}

// TestReconcileAdvancesPastAnOwnedApplicationPrefix asserts the second half of
// the cursor rule: a command that is NOT terminal still lets the cursor past it
// once its recoverable application prefix is durably owned.
func TestReconcileAdvancesPastAnOwnedApplicationPrefix(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending)}}
		f.processor.script[commandID(1)] = scriptedOutcome{outcome: Outcome{State: StateApplying, PrefixOwned: true}}
	})
	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Blocked != nil {
		t.Fatalf("blocked at %+v; an owned application prefix is recoverable and does not block the cursor", result.Blocked)
	}
	if result.Cursor != 2 {
		t.Errorf("cursor = %d, want 2", result.Cursor)
	}
}

// TestReconcileDoesNotSkipAPendingPredecessor is step 4's other half, and the
// "silently" in it: a command that is neither terminal nor prefix-owned stops
// the pass AT that record, its successors are not applied, the cursor does not
// move, and the pass NAMES the record it stopped at.
func TestReconcileDoesNotSkipAPendingPredecessor(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending), command(3, StatePending)}}
		f.processor.script[commandID(2)] = scriptedOutcome{outcome: Outcome{State: StateClaimed}}
	})
	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if want := []sessionwire.CommandID{commandID(1), commandID(2)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v; a successor of a pending predecessor must not be applied", f.processor.processedIDs(), want)
	}
	if result.Cursor != 1 {
		t.Errorf("cursor = %d, want 1; the cursor may not pass a command that is neither terminal nor prefix-owned", result.Cursor)
	}
	if result.Blocked == nil {
		t.Fatal("Blocked is nil; a pass that stopped at a pending predecessor and reported nothing is exactly the silent skip step 4 forbids")
	}
	if result.Blocked.CommandID != commandID(2) {
		t.Errorf("blocked at %q, want %q", result.Blocked.CommandID, commandID(2))
	}
	if result.Blocked.AcceptedOrder != 2 {
		t.Errorf("blocked order = %d, want 2", result.Blocked.AcceptedOrder)
	}
	if result.Blocked.State != StateClaimed {
		t.Errorf("blocked state = %q, want %q", result.Blocked.State, StateClaimed)
	}
	if result.More {
		t.Error("More = true; a pass that stopped at a blocker has not consumed its page")
	}
}

// TestReconcileStopsAtAFailedCommandAndCarriesTheCause asserts a Process error
// blocks the pass in the same way and preserves the cause, so an operator can
// tell a claim contention from an unreachable store.
func TestReconcileStopsAtAFailedCommandAndCarriesTheCause(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending)}}
		f.processor.script[commandID(1)] = scriptedOutcome{err: errTestStore}
	})
	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Blocked == nil {
		t.Fatal("Blocked is nil")
	}
	if !errors.Is(result.Blocked.Cause, errTestStore) {
		t.Errorf("cause = %v, want %v", result.Blocked.Cause, errTestStore)
	}
	if len(f.cursors.written()) != 0 {
		t.Errorf("the cursor was written %v; nothing before the blocker advanced", f.cursors.written())
	}
}

// ---------------------------------------------------------------------------
// A non-conforming page
// ---------------------------------------------------------------------------

// TestReconcileRefusesANonConformingPage asserts the consumer validates the
// immutable order it is handed BEFORE applying any of it. Each row is a way a
// store can break the ordering contract, and in every one nothing is processed:
// applying a prefix of a page that is already known to be wrong would be
// applying commands in an order the session never accepted.
func TestReconcileRefusesANonConformingPage(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		cursor uint64
		page   []Command
		reason PageProblem
		index  int
	}{
		{
			name:   "an order that goes backwards",
			page:   []Command{command(2, StatePending), command(1, StatePending)},
			reason: PageProblemNotMonotonic,
			index:  1,
		},
		{
			name:   "a repeated order",
			page:   []Command{command(2, StatePending), command(2, StatePending)},
			reason: PageProblemNotMonotonic,
			index:  1,
		},
		{
			name:   "a record at the cursor",
			cursor: 5,
			page:   []Command{command(5, StatePending)},
			reason: PageProblemNotAfterCursor,
			index:  0,
		},
		{
			name:   "a record below the cursor",
			cursor: 5,
			page:   []Command{command(4, StatePending)},
			reason: PageProblemNotAfterCursor,
			index:  0,
		},
		{
			name:   "a zero acceptance order",
			page:   []Command{command(0, StatePending)},
			reason: PageProblemNotAfterCursor,
			index:  0,
		},
		{
			name:   "another session's record",
			page:   []Command{{TenantID: testTenant, SessionID: "session-elsewhere", CommandID: commandID(1), AcceptedOrder: 1, State: StatePending}},
			reason: PageProblemForeignSession,
			index:  0,
		},
		{
			name:   "another tenant's record",
			page:   []Command{{TenantID: "tenant-elsewhere", SessionID: testSession, CommandID: commandID(1), AcceptedOrder: 1, State: StatePending}},
			reason: PageProblemForeignSession,
			index:  0,
		},
		{
			name:   "an empty command identity",
			page:   []Command{{TenantID: testTenant, SessionID: testSession, CommandID: "", AcceptedOrder: 1, State: StatePending}},
			reason: PageProblemInvalidCommandID,
			index:  0,
		},
		{
			name:   "an unknown durable state",
			page:   []Command{command(1, State("wedged"))},
			reason: PageProblemUnknownState,
			index:  0,
		},
		{
			name:   "a page wider than the limit",
			page:   []Command{command(1, StatePending), command(2, StatePending), command(3, StatePending), command(4, StatePending)},
			reason: PageProblemOverLimit,
			index:  -1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newConsumerFixture(t, func(f *consumerFixture) {
				f.cursors.stored = testCase.cursor
				f.inbox.pages = [][]Command{testCase.page}
			})
			_, err := f.consumer.Reconcile(context.Background())
			var page *PageError
			if !errors.As(err, &page) {
				t.Fatalf("want *PageError, got %v", err)
			}
			if page.Problem != testCase.reason {
				t.Errorf("Problem = %q, want %q", page.Problem, testCase.reason)
			}
			if page.Index != testCase.index {
				t.Errorf("Index = %d, want %d; a reader has to be told WHICH record broke the page", page.Index, testCase.index)
			}
			if handled := f.processor.processedIDs(); len(handled) != 0 {
				t.Errorf("the processor was handed %v from a page already known to be non-conforming", handled)
			}
			if writes := f.cursors.written(); len(writes) != 0 {
				t.Errorf("the cursor was written %v from a non-conforming page", writes)
			}
		})
	}
}

// TestReconcileRefusesADurableCursorThatWentBackwards asserts a store that
// loses the cursor is refused rather than replayed: this Host is the only
// writer under its fence, so a lower value than the one it durably wrote is the
// store contradicting an acknowledged write.
func TestReconcileRefusesADurableCursorThatWentBackwards(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending)}, nil}
		f.cursors.loads = []uint64{0, 1}
	})
	if _, err := f.consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	_, err := f.consumer.Reconcile(context.Background())
	var regressed *CursorRegressionError
	if !errors.As(err, &regressed) {
		t.Fatalf("want *CursorRegressionError, got %v", err)
	}
	if regressed.Loaded != 1 || regressed.Held != 2 {
		t.Errorf("loaded/held = %d/%d, want 1/2", regressed.Loaded, regressed.Held)
	}
}

// ---------------------------------------------------------------------------
// The fence
// ---------------------------------------------------------------------------

// TestReconcileWritesTheCursorThroughTheFence asserts the durable cursor write
// is a fenced write, so a Host that has lost the session lease cannot advance a
// cursor a successor now owns.
func TestReconcileWritesTheCursorThroughTheFence(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending)}}
	})
	if _, err := f.consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.fence.fencedWrites() != 1 {
		t.Errorf("%d writes went through the fence, want 1", f.fence.fencedWrites())
	}
}

// TestReconcileRefusesToRunUnderALostGrant asserts a pass that begins after the
// grant is gone reads nothing and writes nothing.
func TestReconcileRefusesToRunUnderALostGrant(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending)}}
	})
	f.fence.close()
	_, err := f.consumer.Reconcile(context.Background())
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("want the grant-gone cause, got %v", err)
	}
	if len(f.inbox.requests()) != 0 {
		t.Errorf("the inbox was read %d times under a lost grant", len(f.inbox.requests()))
	}
	if len(f.cursors.written()) != 0 {
		t.Errorf("the cursor was written under a lost grant")
	}
}

// TestReconcileStopsWhenTheGrantIsLostMidPage asserts the fence is consulted
// between records and not only at the top of the pass: a grant lost while a
// page is being applied stops the pass before the next command is claimed.
func TestReconcileStopsWhenTheGrantIsLostMidPage(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending), command(3, StatePending)}}
	})
	f.processor.before = func(c Command) {
		if c.AcceptedOrder == 1 {
			f.fence.close()
		}
	}
	_, err := f.consumer.Reconcile(context.Background())
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("want the grant-gone cause, got %v", err)
	}
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v; a command must not be claimed under a grant already known to be gone", f.processor.processedIDs(), want)
	}
}

// TestReconcileDoesNotAdvanceWhenTheCursorWriteFails asserts a refused cursor
// write leaves the consumer's own cursor where it was, so the very next pass
// re-lists from the last durably acknowledged order rather than from a position
// no store agreed to.
func TestReconcileDoesNotAdvanceWhenTheCursorWriteFails(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.cursors.stored = 8
		f.cursors.saveErr = errTestStore
		f.inbox.pages = [][]Command{{command(9, StatePending)}, {command(9, StatePending)}}
	})
	if _, err := f.consumer.Reconcile(context.Background()); !errors.Is(err, errTestStore) {
		t.Fatalf("want the store failure, got %v", err)
	}
	f.cursors.mu.Lock()
	f.cursors.saveErr = nil
	f.cursors.mu.Unlock()
	if _, err := f.consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	requests := f.inbox.requests()
	if len(requests) != 2 {
		t.Fatalf("ListOrdered was called %d times, want 2", len(requests))
	}
	if requests[1].AfterOrder != 8 {
		t.Errorf("the second page was asked for after %d, want the last acknowledged cursor 8", requests[1].AfterOrder)
	}
}

// TestReconcileHonoursCancellation asserts a cancelled context stops the pass
// between records and reports the cancellation, rather than draining the page.
func TestReconcileHonoursCancellation(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending), command(3, StatePending)}}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.processor.before = func(c Command) {
		if c.AcceptedOrder == 1 {
			cancel()
		}
	}
	_, err := f.consumer.Reconcile(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v", f.processor.processedIDs(), want)
	}
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

// TestRunReconcilesBeforeArmingTheInterval is attach-time reconciliation as the
// loop performs it: an owning Host reconciles its inbox ON ATTACH, so the first
// pass must happen before any timer is armed. A loop that waited would leave a
// command accepted before the attach unapplied for a whole interval.
func TestRunReconcilesBeforeArmingTheInterval(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending)}, nil}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v before the first timer, want %v", f.processor.processedIDs(), want)
	}
	if got := f.clock.requests(); len(got) != 1 || got[0] != testReconcileInterval {
		t.Errorf("timers requested %v, want exactly one of %v", got, testReconcileInterval)
	}
}

// TestRunReconcilesPeriodicallyWhileResident asserts the resident consumer
// keeps reconciling on the Host's bounded interval with no wake signal at all,
// which is what makes correctness independent of HostLink delivery.
func TestRunReconcilesPeriodicallyWhileResident(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{nil, {command(1, StatePending)}, nil}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	f.clock.fire(t)
	f.clock.waitForTimer(t)
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v after one interval with no hint, want %v", f.processor.processedIDs(), want)
	}
	for _, requested := range f.clock.requests() {
		if requested != testReconcileInterval {
			t.Errorf("a timer was requested for %v, want the Host's ReconcileInterval %v", requested, testReconcileInterval)
		}
	}
}

// TestRunWakesOnAHint asserts the low-latency path: a hint carrying an admitted
// CommandID wakes the durable consumer before the interval elapses.
func TestRunWakesOnAHint(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{nil, {command(1, StatePending)}, nil}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	f.consumer.Hint(commandID(1))
	f.clock.current = nil
	f.clock.waitForTimer(t)
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v after a hint and no timer, want %v", f.processor.processedIDs(), want)
	}
}

// TestHintsDoNotSelectWork is step 3 stated as a test: a hint is a WAKE and
// carries no authority over what is consumed or in what order. Hints arrive
// here in the reverse of the acceptance order and for a command the inbox does
// not hold at all; the pass still consumes the durable order, and consumes
// nothing the durable page did not contain.
func TestHintsDoNotSelectWork(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending)}, nil}
	})
	f.consumer.Hint(commandID(99))
	f.consumer.Hint(commandID(2))
	f.consumer.Hint(commandID(1))

	if _, err := f.consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if want := []sessionwire.CommandID{commandID(1), commandID(2)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v; the durable acceptance order decides, not the order hints arrived in", f.processor.processedIDs(), want)
	}
}

// TestDuplicateHintsApplyACommandOnce asserts duplicate HostLink delivery is
// harmless: the same CommandID hinted repeatedly, including after it has been
// consumed, produces no second application, because the page is asked for after
// the durable cursor.
func TestDuplicateHintsApplyACommandOnce(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending)}, nil}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	for range 5 {
		f.consumer.Hint(commandID(1))
		f.clock.current = nil
		f.clock.waitForTimer(t)
	}
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v after five duplicate hints, want %v", f.processor.processedIDs(), want)
	}
}

// TestHintNeverBlocks holds the half of Hint's contract that is about the
// CALLER rather than about the consumer. A HostLink RPC handler calls it; a
// Hint that blocked while the consumer was busy would convert a slow pass into
// stalled transport, which is the failure a one-slot wake exists to avoid.
//
// The claim is exactly this: more hints than the wake can hold, with nothing
// draining it, all return. It says nothing about delivery — a dropped hint is
// the design, because the durable order is the queue.
func TestHintNeverBlocks(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		for range 16 {
			f.consumer.Hint(commandID(1))
		}
	}()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("Hint blocked with no consumer draining the wake; a HostLink handler would stall on a busy session")
	}
}

// TestRunContinuesImmediatelyAfterAFullPage asserts a full page is followed by
// another pass with no timer in between, so a Host resuming with a long backlog
// converges at the store's pace rather than one page per interval.
func TestRunContinuesImmediatelyAfterAFullPage(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{
			{command(1, StatePending), command(2, StatePending), command(3, StatePending)},
			{command(4, StatePending)},
			nil,
		}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	want := []sessionwire.CommandID{commandID(1), commandID(2), commandID(3), commandID(4)}
	if !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v before the first timer, want %v", f.processor.processedIDs(), want)
	}
	if got := f.clock.requests(); len(got) != 1 {
		t.Errorf("timers requested %v, want exactly one; a full page must not cost an interval", got)
	}
}

// TestRunStopsWithoutRunningAnotherPass covers the ONE PATH on which the
// loop's leading precedence select is the only stop check there is. A pass that
// consumed a full page continues immediately, skipping the post-pass select
// entirely, so without that leading check a stopped consumer runs another whole
// pass and applies another command.
//
// ONLY THE Stop ROW REACHES THAT PATH, and saying so is the point of having
// three rows rather than one. Reconcile cannot see Stop at all, so Stop is the
// only reason a full page can be followed by another one; the other two are
// caught INSIDE the pass, before the second command of the first page, which is
// why they consume one command rather than three and pay for one armed timer
// that Stop does not. A single "the loop exits" assertion would have been true
// of all three and would have distinguished none of them.
//
// The costs below are what each path genuinely does, not a shared triple:
//
//	Stop                one list, three applied, no timer
//	cancellation        one list, one applied, one timer
//	the grant is lost   one list, one applied, one timer
func TestRunStopsWithoutRunningAnotherPass(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		// stop ends the consumer while the first pass is inside the Processor.
		stop func(*consumerFixture, context.CancelFunc)
		// wantProcessed is how many commands may be applied in total.
		wantProcessed int
		// wantListed is how many ListOrdered calls the store may see.
		wantListed int
		// wantTimers is how many timers the loop may arm.
		wantTimers int
		reason     string
	}{
		{
			name:          "after Stop",
			stop:          func(f *consumerFixture, _ context.CancelFunc) { f.consumer.Stop() },
			wantProcessed: 3,
			wantListed:    1,
			wantTimers:    0,
			reason:        "Reconcile cannot see Stop, so the pass runs to the end of its full page and the LEADING select is the only thing that stops a second page being listed and a fourth command being APPLIED",
		},
		{
			name:          "after cancellation",
			stop:          func(_ *consumerFixture, cancel context.CancelFunc) { cancel() },
			wantProcessed: 1,
			wantListed:    1,
			wantTimers:    1,
			reason:        "the pass itself refuses before the SECOND command of the page, so only one is applied, and the loop leaves through the post-pass select having armed one timer",
		},
		{
			name:          "after the grant is lost",
			stop:          func(f *consumerFixture, _ context.CancelFunc) { f.fence.close() },
			wantProcessed: 1,
			wantListed:    1,
			wantTimers:    1,
			reason:        "the fence refuses before the SECOND command of the page, for the same cost as cancellation but by the other of the two mechanisms",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newConsumerFixture(t, func(f *consumerFixture) {
				f.inbox.all = []Command{
					command(1, StatePending), command(2, StatePending),
					command(3, StatePending), command(4, StatePending),
				}
			})
			entered := make(chan struct{})
			release := make(chan struct{})
			admit := make(chan struct{}, 1)
			admit <- struct{}{}
			f.processor.before = func(Command) {
				select {
				case <-admit:
					close(entered)
					<-release
				default:
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := f.run(t, ctx)

			<-entered
			testCase.stop(f, cancel)
			close(release)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the loop did not leave")
			}

			if got := len(f.processor.processedIDs()); got != testCase.wantProcessed {
				t.Errorf("processed %v (%d), want %d: %s", f.processor.processedIDs(), got, testCase.wantProcessed, testCase.reason)
			}
			if got := len(f.inbox.requests()); got != testCase.wantListed {
				t.Errorf("ListOrdered was called %d times, want %d: %s", got, testCase.wantListed, testCase.reason)
			}
			if got := len(f.clock.requests()); got != testCase.wantTimers {
				t.Errorf("%d timers were armed, want %d: %s", got, testCase.wantTimers, testCase.reason)
			}
		})
	}
}

// TestRunLeavesWhenTheGrantIsLost asserts the loop stops on the same signal the
// heartbeat does, rather than going on reading an inbox a successor now owns.
func TestRunLeavesWhenTheGrantIsLost(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) { f.inbox.pages = [][]Command{nil} })
	done := f.run(t, context.Background())

	f.clock.waitForTimer(t)
	f.fence.close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not leave after the grant was lost")
	}
}

// TestRunLeavesOnCancellation asserts the session context ends the loop.
func TestRunLeavesOnCancellation(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) { f.inbox.pages = [][]Command{nil} })
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(t, ctx)

	f.clock.waitForTimer(t)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not leave after its context was cancelled")
	}
}

// TestStopEndsTheLoopAndIsRepeatable asserts Stop is idempotent, because a
// rollback and a release may both reach it.
func TestStopEndsTheLoopAndIsRepeatable(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) { f.inbox.pages = [][]Command{nil} })
	done := f.run(t, context.Background())

	f.clock.waitForTimer(t)
	f.consumer.Stop()
	f.consumer.Stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not leave after Stop")
	}
}

// TestRunSurvivesAFailedPass asserts an unreachable store costs one interval
// and not the consumer: the next tick reconciles again.
func TestRunSurvivesAFailedPass(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{nil, {command(1, StatePending)}, nil}
		f.inbox.errs = []error{errTestStore, nil}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	f.clock.fire(t)
	f.clock.waitForTimer(t)
	if want := []sessionwire.CommandID{commandID(1)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v after a failed pass and one interval, want %v", f.processor.processedIDs(), want)
	}
	if f.consumer.Failures() != 1 {
		t.Errorf("Failures = %d, want 1; a pass that failed must be countable", f.consumer.Failures())
	}
}

// TestLastPassIsObservable asserts the loop's own verdict is readable, which is
// what stops a blocked predecessor from being silent when nobody is calling
// Reconcile directly.
func TestLastPassIsObservable(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending), command(2, StatePending)}}
		f.processor.script[commandID(1)] = scriptedOutcome{outcome: Outcome{State: StatePending}}
	})
	done := f.run(t, context.Background())
	defer f.stop(t, done)

	f.clock.waitForTimer(t)
	last := f.consumer.LastPass()
	if last.Blocked == nil {
		t.Fatal("LastPass reported no blocker; the loop discards its result, so this accessor is the only reader")
	}
	if last.Blocked.AcceptedOrder != 1 {
		t.Errorf("blocked order = %d, want 1", last.Blocked.AcceptedOrder)
	}
}

// TestReconcileReportsAFailedCursorLoad asserts an unreadable cursor stops the
// pass before anything is listed: a pass that defaulted the cursor to zero on a
// store failure would re-drive the session's entire accepted history.
func TestReconcileReportsAFailedCursorLoad(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.cursors.loadErr = errTestStore
		f.inbox.pages = [][]Command{{command(1, StatePending)}}
	})
	if _, err := f.consumer.Reconcile(context.Background()); !errors.Is(err, errTestStore) {
		t.Fatalf("want the store failure, got %v", err)
	}
	if len(f.inbox.requests()) != 0 {
		t.Errorf("the inbox was listed %d times with no cursor to list from", len(f.inbox.requests()))
	}
}

// TestConcurrentPassesDoNotApplyACommandTwice holds the one-pass-at-a-time
// rule. Reconcile is exported for attach-time reconciliation and the resident
// loop calls it too, so two callers is the ordinary composition; two passes
// over one cursor would list the same page and hand the same command to the
// Processor twice.
//
// The assertion is DECIDABLE rather than timed: the second pass either parks on
// the busy slot or runs to completion, exactly one of those happens, and a
// select between them settles it.
func TestConcurrentPassesDoNotApplyACommandTwice(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.all = []Command{command(1, StatePending), command(2, StatePending)}
	})
	// THE GATE ADMITS ONE CALLER AND DOES NOT BLOCK THE OTHERS, and that is
	// not a detail. A sync.Once here makes every later caller wait on the
	// first's Do, so an unserialized second pass parks inside the Processor
	// instead of running to completion — and the test then fails on its
	// ten-second "neither parked nor returned" diagnostic rather than on the
	// arm that names the defect. Measured against two mutations that deleted
	// the serialization.
	entered := make(chan struct{})
	release := make(chan struct{})
	admit := make(chan struct{}, 1)
	admit <- struct{}{}
	f.processor.before = func(Command) {
		select {
		case <-admit:
			close(entered)
			<-release
		default:
		}
	}
	parked := make(chan struct{})
	f.consumer.mu.Lock()
	f.consumer.parked = func() { close(parked) }
	f.consumer.mu.Unlock()

	first := make(chan error, 1)
	go func() {
		_, err := f.consumer.Reconcile(context.Background())
		first <- err
	}()
	<-entered

	second := make(chan error, 1)
	go func() {
		_, err := f.consumer.Reconcile(context.Background())
		second <- err
	}()
	select {
	case <-parked:
	case err := <-second:
		t.Fatalf("the second pass ran to completion (%v) while the first was inside the Processor; both read the same cursor and both list the same page", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the second pass neither parked nor returned")
	}
	close(release)

	if err := <-first; err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if want := []sessionwire.CommandID{commandID(1), commandID(2)}; !equalIDs(f.processor.processedIDs(), want) {
		t.Errorf("processed %v, want %v; a command applied twice is exactly-once turned into a scheduling accident", f.processor.processedIDs(), want)
	}
	requests := f.inbox.requests()
	if len(requests) != 2 {
		t.Fatalf("ListOrdered was called %d times, want 2", len(requests))
	}
	if requests[1].AfterOrder != 2 {
		t.Errorf("the second pass listed after %d, want 2; it must see the first pass's acknowledged cursor", requests[1].AfterOrder)
	}
}

// ---------------------------------------------------------------------------
// Structural guard
// ---------------------------------------------------------------------------

// TestEveryDurableCursorWriteGoesThroughTheFence holds the ONE mechanism rule
// for this package's single durable write. It is a structural check rather than
// a behavioural one because a behavioural test can only see the writes that
// exist today: the seventh caller of SaveCursor is the one that will forget.
//
// The claim is EXACTLY its enumeration and no wider: every call whose callee is
// a selector named SaveCursor, in a production file of this package, is
// lexically inside a call whose callee is a selector named Write. A write
// smuggled through a function value assigned elsewhere is not covered.
func TestEveryDurableCursorWriteGoesThroughTheFence(t *testing.T) {
	t.Parallel()

	t.Run("the package obeys it", func(t *testing.T) {
		t.Parallel()
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, "consumer.go", nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse consumer.go: %v", err)
		}
		found, unfenced := cursorWrites(fset, parsed)
		if found == 0 {
			t.Fatal("consumer.go contains no SaveCursor call at all, so this guard is vacuous")
		}
		if len(unfenced) != 0 {
			t.Errorf("these SaveCursor calls are not inside a fenced write, so a cursor could advance under a lease a successor has taken: %s", strings.Join(unfenced, "; "))
		}
	})

	// The guard is PROBED with declarations that genuinely violate it, rather
	// than by mutating the assertion: an assertion that survives because
	// nothing in the package breaks the property is a fact about the package,
	// not about the guard.
	for _, testCase := range []struct {
		name     string
		source   string
		found    int
		unfenced int
	}{
		{
			name:     "a fenced write",
			source:   "package p\nfunc f() { fence.Write(func() error { return c.SaveCursor(ctx, t, s, e, o) }) }\n",
			found:    1,
			unfenced: 0,
		},
		{
			name:     "a bare write",
			source:   "package p\nfunc f() { _ = c.SaveCursor(ctx, t, s, e, o) }\n",
			found:    1,
			unfenced: 1,
		},
		{
			name:     "a write inside some other call",
			source:   "package p\nfunc f() { log(func() error { return c.SaveCursor(ctx, t, s, e, o) }) }\n",
			found:    1,
			unfenced: 1,
		},
		{
			name:     "a second write beside a fenced one",
			source:   "package p\nfunc f() { fence.Write(func() error { return c.SaveCursor(ctx, t, s, e, o) }); _ = c.SaveCursor(ctx, t, s, e, o) }\n",
			found:    2,
			unfenced: 1,
		},
		{
			name:     "no write at all",
			source:   "package p\nfunc f() { _ = c.LoadCursor(ctx, t, s) }\n",
			found:    0,
			unfenced: 0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			fset := token.NewFileSet()
			parsed, err := parser.ParseFile(fset, "probe.go", testCase.source, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse the probe: %v", err)
			}
			found, unfenced := cursorWrites(fset, parsed)
			if found != testCase.found {
				t.Errorf("found %d SaveCursor calls, want %d", found, testCase.found)
			}
			if len(unfenced) != testCase.unfenced {
				t.Errorf("reported %d unfenced writes %v, want %d", len(unfenced), unfenced, testCase.unfenced)
			}
		})
	}
}

// cursorWrites returns how many SaveCursor calls a file contains and the
// positions of those not lexically inside a Write call.
func cursorWrites(fset *token.FileSet, file *ast.File) (int, []string) {
	var (
		found    int
		unfenced []string
		fenced   int
	)
	var walk func(node ast.Node) bool
	walk = func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := selectorName(call.Fun)
		switch name {
		case "Write":
			fenced++
			for _, argument := range call.Args {
				ast.Inspect(argument, walk)
			}
			fenced--
			return false
		case "SaveCursor":
			found++
			if fenced == 0 {
				unfenced = append(unfenced, fset.Position(call.Pos()).String())
			}
		}
		return true
	}
	ast.Inspect(file, walk)
	return found, unfenced
}

// selectorName returns the method name a call expression selects, or "".
func selectorName(fun ast.Expr) string {
	selector, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return selector.Sel.Name
}

// TestHintCarriesNoAuthority holds step 3 as a MECHANISM rather than as a
// convention: Consumer.Hint's CommandID parameter is UNNAMED, so no body can
// read it and no future edit can start selecting work by the identity HostLink
// happened to deliver. Naming it is not forbidden by the compiler, which is
// exactly why the guard is here — the day someone names it to log it is the day
// the next person routes on it.
//
// The claim is exactly its enumeration: Hint's parameter list, in consumer.go,
// binds no name. It says nothing about any other method.
func TestHintCarriesNoAuthority(t *testing.T) {
	t.Parallel()

	t.Run("the method obeys it", func(t *testing.T) {
		t.Parallel()
		parsed, err := parser.ParseFile(token.NewFileSet(), "consumer.go", nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse consumer.go: %v", err)
		}
		named, found := hintParameterNames(parsed)
		if !found {
			t.Fatal("consumer.go declares no method named Hint, so this guard is vacuous")
		}
		if len(named) != 0 {
			t.Errorf("Hint binds %v; a named parameter can be read, and reading it is consuming by HostLink arrival identity", named)
		}
	})

	for _, testCase := range []struct {
		name   string
		source string
		found  bool
		named  []string
	}{
		{
			name:   "an unnamed parameter",
			source: "package p\nfunc (c *Consumer) Hint(sessionwire.CommandID) {}\n",
			found:  true,
		},
		{
			name:   "a named parameter",
			source: "package p\nfunc (c *Consumer) Hint(id sessionwire.CommandID) {}\n",
			found:  true,
			named:  []string{"id"},
		},
		{
			name:   "a blank parameter",
			source: "package p\nfunc (c *Consumer) Hint(_ sessionwire.CommandID) {}\n",
			found:  true,
			named:  []string{"_"},
		},
		{
			name:   "no Hint at all",
			source: "package p\nfunc (c *Consumer) Nudge(id sessionwire.CommandID) {}\n",
			found:  false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", testCase.source, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse the probe: %v", err)
			}
			named, found := hintParameterNames(parsed)
			if found != testCase.found {
				t.Fatalf("found = %v, want %v", found, testCase.found)
			}
			if strings.Join(named, ",") != strings.Join(testCase.named, ",") {
				t.Errorf("named = %v, want %v", named, testCase.named)
			}
		})
	}
}

// hintParameterNames returns every name Hint's parameter list binds, and
// whether a method named Hint was found at all.
func hintParameterNames(file *ast.File) ([]string, bool) {
	names := []string{}
	found := false
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "Hint" || function.Recv == nil {
			continue
		}
		found = true
		for _, parameter := range function.Type.Params.List {
			for _, name := range parameter.Names {
				names = append(names, name.Name)
			}
		}
	}
	return names, found
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

// run starts the loop and returns a channel closed when it has left.
func (f *consumerFixture) run(t *testing.T, ctx context.Context) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.consumer.Run(ctx)
	}()
	return done
}

// stop ends the loop and waits for it, so no test leaves a goroutine reading
// fakes another test is asserting over.
func (f *consumerFixture) stop(t *testing.T, done <-chan struct{}) {
	t.Helper()
	f.consumer.Stop()
	if f.clock.current != nil {
		f.clock.current = nil
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not leave after Stop")
	}
}

// equalIDs reports whether two CommandID sequences are identical.
func equalIDs(got, want []sessionwire.CommandID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
