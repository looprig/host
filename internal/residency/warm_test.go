package residency

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

// fakeWarmClock hands out timers a test fires by hand. NOTHING IN THIS FILE
// SLEEPS: a warm TTL measured against the wall clock is a test whose duration
// is its own assertion, and this lane has already paid for one of those.
type fakeWarmClock struct {
	mu     sync.Mutex
	timers []*fakeWarmTimer
}

func (c *fakeWarmClock) NewWarmTimer(d time.Duration) WarmTimer {
	timer := &fakeWarmTimer{fired: make(chan time.Time), armedFor: d}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timers = append(c.timers, timer)
	return timer
}

// only returns the single timer this clock handed out, failing if there is not
// exactly one. A releaser holding two timers for one session would reset one
// and wait on the other, which is a bug no assertion about durations can see.
func (c *fakeWarmClock) only(t *testing.T) *fakeWarmTimer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers) != 1 {
		t.Fatalf("the clock handed out %d timers, want exactly 1", len(c.timers))
	}
	return c.timers[0]
}

// fakeWarmTimer is a timer whose expiry a test causes. The send on fired is
// BLOCKING, so a test that fires it knows the releaser has taken the tick
// rather than guessing.
type fakeWarmTimer struct {
	fired chan time.Time

	mu       sync.Mutex
	armedFor time.Duration
	resets   []time.Duration
	stops    int
}

func (t *fakeWarmTimer) C() <-chan time.Time { return t.fired }

func (t *fakeWarmTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stops++
	return true
}

func (t *fakeWarmTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.armedFor = d
	t.resets = append(t.resets, d)
	return true
}

// expire delivers a tick and waits for the releaser to take it, BOUNDED.
//
// The wait is what makes the assertion after it meaningful — an unsynchronised
// send would let a test read the trace before the release had run — and the
// bound is what makes a releaser that has STOPPED WATCHING a failed assertion
// rather than a hung package. A mutation probe here measured the difference:
// with an unbounded send, dropping the watch after an aborted attempt killed
// the suite by the 10-minute test timeout, which is not an assertion kill and
// names nothing.
//
// It takes a testing.TB and reports with Errorf, so it is safe to call from a
// goroutine.
func (t *fakeWarmTimer) expire(tb testing.TB) {
	tb.Helper()
	select {
	case t.fired <- time.Time{}:
	case <-time.After(5 * time.Second):
		tb.Errorf("the releaser never took the warm expiry; it is not watching this session")
	}
}

func (t *fakeWarmTimer) resetsSeen() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.resets...)
}

func (t *fakeWarmTimer) stopsSeen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stops
}

// warmTrace records every collaborator call in order, which is what the
// six-step algorithm's ORDER assertions read. A set would pass for any
// permutation and the order is the whole of §9.3.
type warmTrace struct {
	mu    sync.Mutex
	steps []string
}

func (tr *warmTrace) record(step string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.steps = append(tr.steps, step)
}

func (tr *warmTrace) recorded() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.steps...)
}

// index returns the position of a step, or -1.
func (tr *warmTrace) index(step string) int {
	for i, seen := range tr.recorded() {
		if seen == step {
			return i
		}
	}
	return -1
}

// fakeWarmSession is a resident session narrowed to the ordered steps a warm
// release takes it through. Each method records itself and may be made to fail
// INDEPENDENTLY, so a test that makes one step fault cannot pass because a
// different step also stopped.
type fakeWarmSession struct {
	key        registry.Key
	generation uint64
	trace      *warmTrace
	lost       chan struct{}

	mu sync.Mutex
	// errs is the failure to return from each named step.
	errs map[string]error
	// block is closed by a test to release a step that is holding the
	// release open, so the intermediate registry state can be sampled.
	block map[string]chan struct{}
}

func newFakeWarmSession(key registry.Key, generation uint64, trace *warmTrace) *fakeWarmSession {
	return &fakeWarmSession{
		key:        key,
		generation: generation,
		trace:      trace,
		lost:       make(chan struct{}),
		errs:       map[string]error{},
		block:      map[string]chan struct{}{},
	}
}

func (s *fakeWarmSession) Key() registry.Key     { return s.key }
func (s *fakeWarmSession) Generation() uint64    { return s.generation }
func (s *fakeWarmSession) Lost() <-chan struct{} { return s.lost }

func (s *fakeWarmSession) step(name string) error {
	s.trace.record(name)
	s.mu.Lock()
	gate, blocked := s.block[name]
	err := s.errs[name]
	s.mu.Unlock()
	if blocked {
		<-gate
	}
	return err
}

func (s *fakeWarmSession) BeginRelease(context.Context) error     { return s.step("begin_release") }
func (s *fakeWarmSession) Checkpoint(context.Context) error       { return s.step("checkpoint") }
func (s *fakeWarmSession) ReleaseResidency(context.Context) error { return s.step("release_residency") }
func (s *fakeWarmSession) FinishRelease(context.Context) error    { return s.step("finish_release") }
func (s *fakeWarmSession) ReleaseLease(context.Context) error     { return s.step("release_lease") }
func (s *fakeWarmSession) DropState(context.Context) error        { return s.step("drop_state") }

func (s *fakeWarmSession) failAt(step string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs[step] = err
}

// holdAt makes a step block until the returned func is called.
func (s *fakeWarmSession) holdAt(step string) func() {
	gate := make(chan struct{})
	s.mu.Lock()
	s.block[step] = gate
	s.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

// tracingWarmRegistry is the REAL registry with the one call the warm release
// makes to it recorded in order.
//
// It is a wrapper and NOT a fake registry. The property under test is what the
// registry's own writer does to a row — {resident, accepting:false} is
// reachable only because registry.StopAdmitting produces it — and a fake looser
// than the real type is this program's class-4 defect. What the wrapper adds is
// position in the trace, which is the only thing a delegating call cannot show.
type tracingWarmRegistry struct {
	index *registry.Registry
	trace *warmTrace
}

func (r tracingWarmRegistry) StopAdmitting(key registry.Key, generation uint64) (registry.Entry, bool) {
	r.trace.record("registry.stop_admitting")
	return r.index.StopAdmitting(key, generation)
}

// fakeWarmInbox is the durable inbox, re-read at step 1.
type fakeWarmInbox struct {
	trace *warmTrace

	mu       sync.Mutex
	accepted bool
	err      error
	reads    int
}

func (i *fakeWarmInbox) AcceptedWork(context.Context, registry.Key) (bool, error) {
	i.trace.record("inbox.reread")
	i.mu.Lock()
	defer i.mu.Unlock()
	i.reads++
	return i.accepted, i.err
}

func (i *fakeWarmInbox) readCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.reads
}

// fakeConsumption counts the wakes an aborted release owes.
type fakeConsumption struct {
	trace *warmTrace

	mu    sync.Mutex
	wakes int
}

func (c *fakeConsumption) Wake(registry.Key) {
	c.trace.record("consumption.wake")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wakes++
}

func (c *fakeConsumption) wakeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wakes
}

// fakeWarmAdmissions is the Host's admission ledger, narrowed. It counts the
// credit a release owes: a warm release that forgot it would leak this Host's
// capacity one session at a time, which no assertion about the registry sees.
type fakeWarmAdmissions struct {
	trace *warmTrace

	mu       sync.Mutex
	released []registry.Key
}

func (a *fakeWarmAdmissions) Admit(registry.Key, sessionwire.AgentID) error { return nil }
func (a *fakeWarmAdmissions) Draining() bool                                { return false }

func (a *fakeWarmAdmissions) Release(key registry.Key) bool {
	a.trace.record("admissions.release")
	a.mu.Lock()
	defer a.mu.Unlock()
	a.released = append(a.released, key)
	return true
}

func (a *fakeWarmAdmissions) credited() []registry.Key {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]registry.Key(nil), a.released...)
}

// recordingWarmObserver collects outcomes and lets a test wait for one without
// polling.
type recordingWarmObserver struct {
	mu       sync.Mutex
	outcomes []WarmOutcome
	arrived  chan WarmOutcome
}

func newWarmObserver() *recordingWarmObserver {
	return &recordingWarmObserver{arrived: make(chan WarmOutcome, 8)}
}

func (o *recordingWarmObserver) WarmRelease(outcome WarmOutcome) {
	o.mu.Lock()
	o.outcomes = append(o.outcomes, outcome)
	o.mu.Unlock()
	o.arrived <- outcome
}

// await returns the next outcome, failing rather than hanging if none arrives.
func (o *recordingWarmObserver) await(t *testing.T) WarmOutcome {
	t.Helper()
	select {
	case outcome := <-o.arrived:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("no warm outcome arrived")
		return WarmOutcome{}
	}
}

func (o *recordingWarmObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.outcomes)
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const warmTestTTL = 97 * time.Second

type warmFixture struct {
	t           *testing.T
	clock       *fakeWarmClock
	index       *registry.Registry
	trace       *warmTrace
	inbox       *fakeWarmInbox
	consumption *fakeConsumption
	admissions  *fakeWarmAdmissions
	observer    *recordingWarmObserver
	releaser    *WarmReleaser
	session     *fakeWarmSession
	key         registry.Key
}

// newWarmFixture installs one resident session in a REAL registry — not a fake
// one — because the property under test is what the registry's own writers do
// to a row, and a fake looser than the registry is this program's class-4
// defect.
func newWarmFixture(t *testing.T, configure ...func(*WarmOptions)) *warmFixture {
	t.Helper()
	trace := &warmTrace{}
	clock := &fakeWarmClock{}
	index := registry.New(&warmRegistryClock{})
	key := registry.Key{TenantID: testTenant, SessionID: testSession}
	entry, installed := index.Insert(key, registry.Admission{
		AgentID:         testAgent,
		Target:          nil,
		CompatibilityID: testCompat,
		Runtime:         nil,
		LeaseEpoch:      uint64(FirstResidencyEpoch),
	})
	if !installed {
		t.Fatal("the fixture residency was not installed")
	}

	f := &warmFixture{
		t:           t,
		clock:       clock,
		index:       index,
		trace:       trace,
		inbox:       &fakeWarmInbox{trace: trace},
		consumption: &fakeConsumption{trace: trace},
		admissions:  &fakeWarmAdmissions{trace: trace},
		observer:    newWarmObserver(),
		key:         key,
	}
	f.session = newFakeWarmSession(key, entry.Generation, trace)

	options := WarmOptions{
		Clock:       clock,
		TTL:         warmTestTTL,
		Registry:    tracingWarmRegistry{index: index, trace: trace},
		Inbox:       f.inbox,
		Consumption: f.consumption,
		Admissions:  f.admissions,
		Observer:    f.observer,
	}
	for _, apply := range configure {
		apply(&options)
	}
	releaser, err := NewWarmReleaser(options)
	if err != nil {
		t.Fatalf("NewWarmReleaser: %v", err)
	}
	t.Cleanup(releaser.Stop)
	f.releaser = releaser
	if err := releaser.Watch(f.session); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	return f
}

// warmRegistryClock is the registry's own time source. It never advances,
// because nothing in this file depends on registry staleness.
type warmRegistryClock struct{}

func (warmRegistryClock) Now() time.Time { return time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC) }

// entry reads the live registry row.
func (f *warmFixture) entry() registry.Entry {
	f.t.Helper()
	entry, held := f.index.Get(f.key)
	if !held {
		f.t.Fatal("the fixture residency is gone")
	}
	return entry
}

// ---------------------------------------------------------------------------
// The algorithm, in order
// ---------------------------------------------------------------------------

// TestAnIdleSessionIsReleasedInTheSpecifiedOrder is §9.3 and 04-host.md's O6.1
// algorithm, asserted as a SEQUENCE.
//
// The order is the whole of the contract and a set would satisfy every
// permutation of it. Three of the orderings are separately load-bearing and
// have their own tests below — the inbox re-read before anything is taken,
// the durable `releasing` mark before the checkpoint, and the tombstone before
// the lease release — but a caller reading this file should be able to see the
// shape in one place.
func TestAnIdleSessionIsReleasedInTheSpecifiedOrder(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)

	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeReleased {
		t.Fatalf("outcome = %q (%s), want %q", outcome.Kind, outcome.Reason, WarmOutcomeReleased)
	}
	if len(outcome.Failures) != 0 {
		t.Errorf("a clean release recorded failures: %v", outcome.Failures)
	}

	want := []string{
		"inbox.reread",
		"registry.stop_admitting",
		"begin_release",
		"checkpoint",
		"release_residency",
		"finish_release",
		"release_lease",
		"admissions.release",
		"drop_state",
	}
	got := f.trace.recorded()
	if len(got) != len(want) {
		t.Fatalf("the release ran\n  %v\nwant\n  %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the release ran\n  %v\nwant\n  %v", got, want)
		}
	}
}

// TestTheWarmTimerIsArmedByIdleAndCancelledByWork is "arm a resettable timer,
// cancel/reset on accepted work".
//
// The RESET half is the one with a positive observable: a session that goes
// idle, works, and goes idle again must get a WHOLE TTL from the second idle
// and not the remainder of the first. The fake timer records what it was reset
// to, so a releaser that armed the timer once and never re-armed it is visible.
func TestTheWarmTimerIsArmedByIdleAndCancelledByWork(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	timer := f.clock.only(t)

	// Watch alone arms nothing: a session that has never been observed idle is
	// not on a countdown to release.
	if resets := timer.resetsSeen(); len(resets) != 0 {
		t.Fatalf("Watch armed the timer %v; only a durable whole-session idle may", resets)
	}
	if timer.stopsSeen() == 0 {
		t.Error("Watch did not stop the timer it was handed, so the session is counting down before it has been observed idle")
	}

	f.releaser.Observe(f.key, WorkStateIdle)
	f.releaser.Observe(f.key, WorkStateWorking)
	f.releaser.Observe(f.key, WorkStateIdle)

	resets := timer.resetsSeen()
	if len(resets) != 2 {
		t.Fatalf("the timer was reset %d times, want 2: each idle observation re-arms it", len(resets))
	}
	for i, d := range resets {
		if d != warmTestTTL {
			t.Errorf("reset %d armed for %s, want the configured TTL %s: a partial remainder is not a warm window", i, d, warmTestTTL)
		}
	}
}

// TestAGateWaitIsNotWholeSessionIdle is 04-host.md in terms: "A waiting
// resident gate is not whole-session idle and therefore is not warm-evicted by
// this plan."
//
// IT IS ENCODED IN THE TYPE rather than left to the caller. A boolean idle
// signal would make this a rule about who calls it — and the one caller who
// reports a gate wait as idle releases a session with a human waiting on it.
// WorkState names the gate wait so the releaser can refuse it, and the
// assertion is that the timer is CANCELLED, not merely not armed: a gate that
// opens after an idle observation must not inherit that observation's arming.
func TestAGateWaitIsNotWholeSessionIdle(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	timer := f.clock.only(t)

	f.releaser.Observe(f.key, WorkStateIdle)
	stopsBefore := timer.stopsSeen()
	f.releaser.Observe(f.key, WorkStateGateWaiting)

	if timer.stopsSeen() <= stopsBefore {
		t.Error("a gate wait did not cancel the warm timer, so a session with a human waiting on it is on a countdown to release")
	}
	if resets := timer.resetsSeen(); len(resets) != 1 {
		t.Errorf("the timer was reset %d times, want 1: a gate wait must not arm it", len(resets))
	}

	// The control: a stale tick from the cancelled arming releases nothing.
	timer.expire(t)
	f.releaser.Observe(f.key, WorkStateWorking) // a barrier: Observe takes the same lock the tick did
	if f.inbox.readCount() != 0 {
		t.Error("a cancelled warm timer still ran a release")
	}
	if f.observer.count() != 0 {
		t.Error("a cancelled warm timer produced an outcome")
	}
}

// ---------------------------------------------------------------------------
// Step 1: the durable re-read
// ---------------------------------------------------------------------------

// TestWorkAcceptedSinceIdleAbortsTheReleaseAndWakesConsumption is step 1, and
// the wake is the half that is easy to leave out.
//
// The releaser is the only thing that knows this session stopped being a
// release candidate. A consumer that went quiet because the session was idle
// has no reason to look again on its own, so an abort that merely returned
// would leave accepted, durable work sitting in the inbox of a session that is
// resident, accepting and doing nothing.
func TestWorkAcceptedSinceIdleAbortsTheReleaseAndWakesConsumption(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.inbox.accepted = true
	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)

	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeAborted {
		t.Fatalf("outcome = %q (%s), want %q", outcome.Kind, outcome.Reason, WarmOutcomeAborted)
	}
	if f.consumption.wakeCount() != 1 {
		t.Errorf("consumption was woken %d times, want 1", f.consumption.wakeCount())
	}
	// NOTHING WAS TAKEN. The abort happens before step 2, so admission is
	// untouched and no durable write has been made.
	if entry := f.entry(); !entry.Accepting || entry.State != registry.StateResident {
		t.Errorf("the residency is (%v, %q) after an aborted release, want (accepting, resident)", entry.Accepting, entry.State)
	}
	for _, step := range []string{"registry.stop_admitting", "begin_release", "checkpoint", "release_residency", "finish_release", "release_lease", "drop_state"} {
		if f.trace.index(step) >= 0 {
			t.Errorf("an aborted release ran %q", step)
		}
	}
	if credited := f.admissions.credited(); len(credited) != 0 {
		t.Errorf("an aborted release credited the admission ledger: %v", credited)
	}
}

// TestAnUnreadableInboxAbortsTheRelease is the fail-closed half of step 1, and
// it is a DECISION rather than an accident of error handling.
//
// A store that will not answer has said nothing about whether work was
// accepted. Releasing on that silence releases a session that may hold accepted
// durable work; declining to release costs a warm runtime that stays resident
// until the next idle observation, and the session is going nowhere. The
// asymmetry is the whole reason this is not "record and continue" like the
// steps after it: those run when the release is already committed, and this one
// runs before anything has been taken.
func TestAnUnreadableInboxAbortsTheRelease(t *testing.T) {
	t.Parallel()

	unreachable := errors.New("the orchestration store is unreachable")
	f := newWarmFixture(t)
	f.inbox.err = unreachable
	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)

	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeAborted {
		t.Fatalf("outcome = %q (%s), want %q", outcome.Kind, outcome.Reason, WarmOutcomeAborted)
	}
	if len(outcome.Failures) != 1 || outcome.Failures[0].Step != WarmStepInbox {
		t.Fatalf("failures = %v, want one at %q", outcome.Failures, WarmStepInbox)
	}
	if !errors.Is(outcome.Failures[0].Err, unreachable) {
		t.Errorf("the failure does not carry the store's error: %v", outcome.Failures[0].Err)
	}
	if entry := f.entry(); !entry.Accepting {
		t.Error("an unreadable inbox stopped admission anyway")
	}
	if f.consumption.wakeCount() != 1 {
		t.Errorf("consumption was woken %d times, want 1: an unread inbox may hold work", f.consumption.wakeCount())
	}
}

// TestAnAbortedReleaseIsRetriedOnTheNextIdle holds that an abort is not a
// terminal verdict on the session. The runbook's step 1 says "abort THIS
// release attempt"; a releaser that stopped watching would leave the session
// resident forever, which is the leak the warm TTL exists to prevent.
func TestAnAbortedReleaseIsRetriedOnTheNextIdle(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.inbox.accepted = true
	f.releaser.Observe(f.key, WorkStateIdle)
	timer := f.clock.only(t)
	timer.expire(t)
	if outcome := f.observer.await(t); outcome.Kind != WarmOutcomeAborted {
		t.Fatalf("the first attempt = %q, want aborted", outcome.Kind)
	}

	// The work is applied and the session goes idle again.
	f.inbox.mu.Lock()
	f.inbox.accepted = false
	f.inbox.mu.Unlock()
	f.releaser.Observe(f.key, WorkStateWorking)
	f.releaser.Observe(f.key, WorkStateIdle)
	timer.expire(t)

	if outcome := f.observer.await(t); outcome.Kind != WarmOutcomeReleased {
		t.Fatalf("the second attempt = %q (%s), want released", outcome.Kind, outcome.Reason)
	}
	if f.inbox.readCount() != 2 {
		t.Errorf("the inbox was re-read %d times, want 2: every attempt re-reads", f.inbox.readCount())
	}
}

// ---------------------------------------------------------------------------
// Step 2: the racing command
// ---------------------------------------------------------------------------

// TestACommandRacingStepTwoFindsAResidentSessionNotAdmitting is the race the
// runbook names: "A command accepted after step 1 races step 2 and receives
// typed not_admitting".
//
// The observable is the REGISTRY ROW SAMPLED MID-RELEASE, held open at
// BeginRelease, and it is the pair {resident, accepting:false}. That row was
// unreachable before this task — Accepting was a strict function of State — and
// making it reachable is what lets a racing command be refused before any
// durable `releasing` observation has been written. Sampling after the release
// finished would prove nothing: every step sets accepting false eventually.
//
// The wire class a Factory sees for this row is asserted where a Factory
// reaches it, in internal/realtime/hostlink: TestReleasingAndNotAdmittingDoNot-
// MaskEachOther. This package owns the row; that one owns the refusal.
func TestACommandRacingStepTwoFindsAResidentSessionNotAdmitting(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	resume := f.session.holdAt("begin_release")
	f.releaser.Observe(f.key, WorkStateIdle)
	go f.clock.only(t).expire(t)

	// Wait for the release to reach the held step.
	deadline := time.Now().Add(5 * time.Second)
	for f.trace.index("begin_release") < 0 {
		if time.Now().After(deadline) {
			t.Fatal("the release never reached BeginRelease")
		}
		time.Sleep(time.Millisecond)
	}

	entry := f.entry()
	if entry.Accepting {
		t.Error("admission was still open when the durable releasing mark was being written; a command accepted here would be applied to a runtime that is going away")
	}
	if entry.State != registry.StateResident {
		t.Errorf("State = %q, want %q: admission is stopped BEFORE the residency state moves, so a command racing this window is refused not_admitting rather than releasing", entry.State, registry.StateResident)
	}

	resume()
	if outcome := f.observer.await(t); outcome.Kind != WarmOutcomeReleased {
		t.Fatalf("outcome = %q (%s), want released", outcome.Kind, outcome.Reason)
	}
}

// TestAReplacedResidencyIsNotReleased holds the generation rule at step 2.
//
// A warm timer armed for one residency and firing after that residency has been
// replaced under the same key would otherwise stop admission on, and release,
// somebody else's session. The registry refuses it, and this asserts the
// releaser takes the refusal as a REASON TO STOP rather than continuing into
// the five steps that follow.
func TestAReplacedResidencyIsNotReleased(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	if !f.index.RemoveByGeneration(f.key, f.session.generation) {
		t.Fatal("the fixture residency could not be removed")
	}
	replacement, installed := f.index.Insert(f.key, registry.Admission{
		AgentID:         testAgent,
		CompatibilityID: testCompat,
		LeaseEpoch:      uint64(FirstResidencyEpoch) + 1,
	})
	if !installed || replacement.Generation == f.session.generation {
		t.Fatal("the replacement residency did not take a new generation")
	}

	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)

	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeAbandoned {
		t.Fatalf("outcome = %q (%s), want %q", outcome.Kind, outcome.Reason, WarmOutcomeAbandoned)
	}
	if held := f.entry(); !held.Accepting || held.State != registry.StateResident {
		t.Errorf("the REPLACEMENT residency is (%v, %q); a stale warm timer acted on it", held.Accepting, held.State)
	}
	for _, step := range []string{"begin_release", "checkpoint", "release_residency", "finish_release", "release_lease", "drop_state"} {
		if f.trace.index(step) >= 0 {
			t.Errorf("a stale warm timer ran %q against a residency it does not own", step)
		}
	}
	if credited := f.admissions.credited(); len(credited) != 0 {
		t.Errorf("a stale warm timer credited the ledger for a residency it does not own: %v", credited)
	}
}

// ---------------------------------------------------------------------------
// The two orderings §9.3 makes normative
// ---------------------------------------------------------------------------

// TestTheDurableReleasingMarkPrecedesTheCheckpoint is §9.3 step 1 before step
// 2, and the reason the two halves of a release are separate methods.
//
// A checkpoint can take longer than the registry expiry. Committing it while
// the session is still advertised resident and accepting means a Factory routes
// new work onto a runtime that is being serialized out from under it.
func TestTheDurableReleasingMarkPrecedesTheCheckpoint(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)
	f.observer.await(t)

	begin, checkpoint := f.trace.index("begin_release"), f.trace.index("checkpoint")
	if begin < 0 || checkpoint < 0 {
		t.Fatalf("the release ran %v; both steps must appear", f.trace.recorded())
	}
	if begin > checkpoint {
		t.Errorf("the checkpoint ran before the durable releasing mark: %v", f.trace.recorded())
	}
	if stop := f.trace.index("registry.stop_admitting"); stop > begin {
		t.Errorf("admission was stopped after the releasing mark, so a command could be accepted into a session already advertised releasing: %v", f.trace.recorded())
	}
}

// TestTheTombstonePrecedesTheLeaseReleaseWhichPrecedesDroppingState is §9.3
// steps 4, 5 and 6, and the order is what stops a successor writing under a
// route this Host has not removed.
//
// The lease is what fences the durable record. Releasing it before the
// epoch-fenced tombstone is written lets another owner acquire the session and
// begin writing while a live §15 route still points here; and dropping local
// state before the lease is released leaves this Host holding a grant it can no
// longer renew or reason about.
func TestTheTombstonePrecedesTheLeaseReleaseWhichPrecedesDroppingState(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.releaser.Observe(f.key, WorkStateIdle)
	f.clock.only(t).expire(t)
	f.observer.await(t)

	finish := f.trace.index("finish_release")
	lease := f.trace.index("release_lease")
	drop := f.trace.index("drop_state")
	if finish < 0 || lease < 0 || drop < 0 {
		t.Fatalf("the release ran %v; all three steps must appear", f.trace.recorded())
	}
	if !(finish < lease && lease < drop) {
		t.Errorf("the release ran %v, want finish_release before release_lease before drop_state", f.trace.recorded())
	}
	// The admission credit belongs to step 6 with the rest of the in-memory
	// state, and NOT before the lease release: a Host that credited its
	// capacity while still holding the grant would admit a replacement session
	// it has no room for.
	if credit := f.trace.index("admissions.release"); credit < lease {
		t.Errorf("the admission weight was credited back at position %d, before the lease release at %d: %v", credit, lease, f.trace.recorded())
	}
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

// TestAFailedStepAfterTheReleasingMarkIsRecordedAndTheReleaseContinues is the
// deliberate DEVIATION from "abort on failure", and it is stated rather than
// implied.
//
// Before step 2 a failure aborts, because nothing has been taken. After it the
// residency has been published `releasing` and the registry offers no way to
// take that back — there is no ResumeAdmitting and no MarkResident, by design,
// because un-releasing a session a Factory has already stopped routing to is a
// second protocol with its own races. So the release is committed, and stopping
// halfway turns one unreleased resource into several: a live route with no
// owner, a lease nobody will renew, and a Host that believes it still holds a
// session it has stopped serving.
//
// THE COST IS REAL AND IS THE RIGHT WAY ROUND. A failed required checkpoint
// loses a runtime's in-memory continuation, and the durable session and its
// journal remain authoritative, so the price is a rehydration rather than lost
// work. internal/lifecycle's drain makes the same trade for the same reason.
func TestAFailedStepAfterTheReleasingMarkIsRecordedAndTheReleaseContinues(t *testing.T) {
	t.Parallel()

	for _, step := range []struct {
		name string
		want WarmStep
	}{
		{name: "begin_release", want: WarmStepBeginRelease},
		{name: "checkpoint", want: WarmStepCheckpoint},
		{name: "release_residency", want: WarmStepReleaseResidency},
		{name: "finish_release", want: WarmStepFinishRelease},
		{name: "release_lease", want: WarmStepReleaseLease},
		{name: "drop_state", want: WarmStepDropState},
	} {
		t.Run(step.name, func(t *testing.T) {
			t.Parallel()
			fault := errors.New("the " + step.name + " step refused")
			f := newWarmFixture(t)
			f.session.failAt(step.name, fault)
			f.releaser.Observe(f.key, WorkStateIdle)
			f.clock.only(t).expire(t)

			outcome := f.observer.await(t)
			if outcome.Kind != WarmOutcomeReleased {
				t.Fatalf("outcome = %q (%s), want %q: a recorded failure does not stop the release", outcome.Kind, outcome.Reason, WarmOutcomeReleased)
			}
			if len(outcome.Failures) != 1 {
				t.Fatalf("failures = %v, want exactly one", outcome.Failures)
			}
			if outcome.Failures[0].Step != step.want {
				t.Errorf("the failure names step %q, want %q", outcome.Failures[0].Step, step.want)
			}
			if !errors.Is(outcome.Failures[0].Err, fault) {
				t.Errorf("the failure does not carry the step's error: %v", outcome.Failures[0].Err)
			}
			// EVERY LATER STEP STILL RAN. This is the assertion the "record and
			// continue" claim actually rests on; without it a release that
			// stopped at the first fault would pass every line above.
			steps := []string{"begin_release", "checkpoint", "release_residency", "finish_release", "release_lease", "drop_state"}
			for _, later := range steps {
				if f.trace.index(later) < 0 {
					t.Errorf("%q did not run after %q failed: %v", later, step.name, f.trace.recorded())
				}
			}
			if credited := f.admissions.credited(); len(credited) != 1 {
				t.Errorf("the admission weight was credited %d times, want 1: a failed step must not leak this Host's capacity", len(credited))
			}
		})
	}
}

// TestALostLeaseAbandonsTheWarmPath is Host's REACTION to losing a residency
// grant, and it is deliberately not a claim about takeover.
//
// memstore has no TTL and no takeover, so nothing in this module can stage the
// successor half; a real one needs pgstore behind PGSTORE_TEST_DSN. What is
// staged here is the only part Host owns: the grant's Lost() channel closes and
// the warm path STOPS, because release under a lost grant would write fenced
// records under an epoch somebody else has superseded. residency.Heartbeat
// already owns the teardown handoff for this case — see surrender and
// TeardownObserver — and a warm release racing it would be a second release
// path with no checkpoint, no lease release and no tombstone.
func TestALostLeaseAbandonsTheWarmPath(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	f.releaser.Observe(f.key, WorkStateIdle)
	close(f.session.lost)

	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeAbandoned {
		t.Fatalf("outcome = %q (%s), want %q", outcome.Kind, outcome.Reason, WarmOutcomeAbandoned)
	}
	if !strings.Contains(outcome.Reason, "grant") {
		t.Errorf("the reason %q does not name the lost grant", outcome.Reason)
	}
	for _, step := range []string{"inbox.reread", "registry.stop_admitting", "begin_release", "finish_release", "release_lease", "drop_state"} {
		if f.trace.index(step) >= 0 {
			t.Errorf("the warm path ran %q under a lost grant", step)
		}
	}
	if entry := f.entry(); !entry.Accepting || entry.State != registry.StateResident {
		t.Errorf("the registry row is (%v, %q); the warm path wrote to a residency it no longer owns", entry.Accepting, entry.State)
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// TestNewWarmReleaserRefusesAnUnusableConfiguration covers the zero and long
// TTL rows the runbook asks for, and records where the two answers come from.
//
// ZERO IS REFUSED, not treated as "release on the idle edge". host.Options
// already refuses a non-positive WarmTTL — validateShape lists it with the
// other durations — so accepting one here would define a behaviour for a
// configuration no Host can be constructed with, which is a behaviour nothing
// can reach and nothing can test end to end. A deployment that wants no warm
// window releases on idle by not watching the session at all.
//
// LONG IS ORDINARY and has no ceiling. A dedicated Host configures a long warm
// window, and this releaser is the same protocol in dedicated mode as in pooled
// — O6.2's step 2 says so — so there is no branch here to test, only the
// absence of an invented bound.
func TestNewWarmReleaserRefusesAnUnusableConfiguration(t *testing.T) {
	t.Parallel()

	valid := func() WarmOptions {
		return WarmOptions{
			Clock:       &fakeWarmClock{},
			TTL:         warmTestTTL,
			Registry:    registry.New(&warmRegistryClock{}),
			Inbox:       &fakeWarmInbox{trace: &warmTrace{}},
			Consumption: &fakeConsumption{trace: &warmTrace{}},
			Admissions:  &fakeWarmAdmissions{trace: &warmTrace{}},
		}
	}
	for _, row := range []struct {
		name  string
		spoil func(*WarmOptions)
		field string
	}{
		{name: "no clock", spoil: func(o *WarmOptions) { o.Clock = nil }, field: "Clock"},
		{name: "no registry", spoil: func(o *WarmOptions) { o.Registry = nil }, field: "Registry"},
		{name: "no inbox", spoil: func(o *WarmOptions) { o.Inbox = nil }, field: "Inbox"},
		{name: "no consumption", spoil: func(o *WarmOptions) { o.Consumption = nil }, field: "Consumption"},
		{name: "no admissions", spoil: func(o *WarmOptions) { o.Admissions = nil }, field: "Admissions"},
		{name: "zero ttl", spoil: func(o *WarmOptions) { o.TTL = 0 }, field: "TTL"},
		{name: "negative ttl", spoil: func(o *WarmOptions) { o.TTL = -time.Second }, field: "TTL"},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			options := valid()
			row.spoil(&options)
			releaser, err := NewWarmReleaser(options)
			if releaser != nil {
				t.Error("a releaser was returned alongside the refusal")
			}
			var invalid *InvalidManagerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("NewWarmReleaser = %v, want an *InvalidManagerOptionsError", err)
			}
			if invalid.Field != row.field {
				t.Errorf("the refusal names field %q, want %q", invalid.Field, row.field)
			}
		})
	}

	t.Run("a long warm window is accepted and armed for what it was given", func(t *testing.T) {
		t.Parallel()
		const long = 30 * 24 * time.Hour
		f := newWarmFixture(t, func(o *WarmOptions) { o.TTL = long })
		f.releaser.Observe(f.key, WorkStateIdle)
		resets := f.clock.only(t).resetsSeen()
		if len(resets) != 1 || resets[0] != long {
			t.Errorf("the timer was armed for %v, want one arming of %s", resets, long)
		}
	})

	t.Run("the observer is optional", func(t *testing.T) {
		t.Parallel()
		options := valid()
		options.Observer = nil
		if _, err := NewWarmReleaser(options); err != nil {
			t.Fatalf("NewWarmReleaser without an observer = %v, want acceptance", err)
		}
	})
}

// TestWatchAndForgetAreIdempotent holds the composition rules a caller needs:
// one watch per key, a second is refused rather than silently replacing the
// first, and Forget stops the watch without releasing anything.
func TestWatchAndForgetAreIdempotent(t *testing.T) {
	t.Parallel()

	f := newWarmFixture(t)
	if err := f.releaser.Watch(f.session); err == nil {
		t.Error("a second Watch for the same key was accepted; two timers for one session is a release racing itself")
	}
	f.releaser.Forget(f.key)
	f.releaser.Forget(f.key)

	// Forget is not a release: nothing was checkpointed, tombstoned or dropped.
	if steps := f.trace.recorded(); len(steps) != 0 {
		t.Errorf("Forget ran %v, want nothing", steps)
	}
	if entry := f.entry(); !entry.Accepting {
		t.Error("Forget stopped admission")
	}
	// A forgotten key is watchable again.
	if err := f.releaser.Watch(f.session); err != nil {
		t.Errorf("Watch after Forget = %v, want acceptance", err)
	}
}

// TestForgetAndStopEndTheWatchGoroutineAndNotJustItsTimer is the checklist
// question this task's review protocol asks of every stop call site, applied to
// the two that end a watch: "is the observable sampled AT ALL, per statement?"
//
// It was not, and the omission was a real leak. Both Forget and Stop disarm the
// timer — which every existing assertion about arming already covered — and
// nothing sampled whether the goroutine waiting on that timer ever exited. A
// pooled Host that watches and forgets a session per placement accumulates one
// parked goroutine per forgotten session for the life of the process.
//
// TWO OBSERVABLES, because the two statements are separately deletable:
// goroutine count for the exit, and "a fired timer releases nothing" for the
// disarming. Stop's bound is what makes the first an assertion rather than a
// package-wide hang, which this lane has ruled is not a kill.
func TestForgetAndStopEndTheWatchGoroutineAndNotJustItsTimer(t *testing.T) {
	// NOT parallel: it counts goroutines, and a sibling test's would be
	// indistinguishable from a leaked one.
	for _, row := range []struct {
		name string
		end  func(*warmFixture)
	}{
		{name: "Forget", end: func(f *warmFixture) { f.releaser.Forget(f.key) }},
		{name: "Stop", end: func(f *warmFixture) { f.releaser.Stop() }},
	} {
		t.Run(row.name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			f := newWarmFixture(t)
			f.releaser.Observe(f.key, WorkStateIdle)

			done := make(chan struct{})
			go func() { defer close(done); row.end(f) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not return; it is waiting on a watch goroutine that never ends", row.name)
			}

			// The goroutine is gone. NumGoroutine settles without a sleep
			// because Stop waits, and Forget's watch exits on a closed channel
			// it is already selecting on; a bounded poll covers the scheduling
			// gap without asserting on a duration.
			deadline := time.Now().Add(5 * time.Second)
			for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if after := runtime.NumGoroutine(); after > before {
				t.Errorf("%d goroutines before and %d after %s; the watch goroutine outlived the watch", before, after, row.name)
			}

			// And the timer is disarmed: a tick that arrives anyway releases
			// nothing. Nobody is reading the channel now, so this must not
			// block.
			select {
			case f.clock.only(t).fired <- time.Time{}:
			default:
			}
			if f.inbox.readCount() != 0 {
				t.Errorf("a release ran after %s", row.name)
			}
			if f.observer.count() != 0 {
				t.Errorf("an outcome was produced after %s", row.name)
			}
		})
	}
}
