package residency

import (
	"context"
	"errors"
	"fmt"

	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
	"github.com/looprig/host/internal/testkit"
)

// ---------------------------------------------------------------------------
// The step trace
// ---------------------------------------------------------------------------

// trace is the ORDERING INSTRUMENT, and it records what the COLLABORATORS were
// asked to do rather than what the manager says it did. A manager that appended
// its own step names would be asserting its own commentary; every entry here is
// written by the fake that received the call.
//
// WHAT IT CANNOT SEE, stated because the ordering assertion would otherwise
// read as covering nine steps when it observes seven. Step 1's AgentID and
// compatibility validation and step 5's capability validation reach no
// collaborator that could record a call — they are decisions over values
// already held. Step 1's position is pinned instead by the tests asserting an
// EMPTY trace on a refusal, and step 5's by runtime.done, which only the
// liveness check calls.
type trace struct {
	mu    sync.Mutex
	steps []string
}

func (t *trace) record(step string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.steps = append(t.steps, step)
}

func (t *trace) recorded() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.steps...)
}

func (t *trace) indexOf(step string) int {
	for i, recorded := range t.recorded() {
		if recorded == step {
			return i
		}
	}
	return -1
}

func (t *trace) count(step string) int {
	total := 0
	for _, recorded := range t.recorded() {
		if recorded == step {
			total++
		}
	}
	return total
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------
//
// EVERY FAKE REFUSES A DEAD CONTEXT, and that is the instrument for one
// specific claim rather than realism for its own sake: a rollback that ran on
// the REQUEST context would silently release nothing once that request was
// cancelled, and the released counters would read zero for a reason no
// assertion could distinguish from "the manager never tried". Refusing and
// recording the refusal turns that into a named failure.

var errDeadContext = errors.New("fake: called with a context that is already done")

// fakeLease is one granted lease.
type fakeLease struct {
	trace *trace
	epoch ResidencyEpoch
	lost  chan struct{}

	mu         sync.Mutex
	released   int
	deadCalls  int
	releaseErr error
}

func (l *fakeLease) Epoch() ResidencyEpoch { return l.epoch }
func (l *fakeLease) Lost() <-chan struct{} { return l.lost }
func (l *fakeLease) releasedCount() int    { l.mu.Lock(); defer l.mu.Unlock(); return l.released }

func (l *fakeLease) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ctx.Err() != nil {
		l.deadCalls++
		l.trace.record("lease.release-on-dead-context")
		return errDeadContext
	}
	l.trace.record("lease.release")
	if l.releaseErr != nil {
		return l.releaseErr
	}
	l.released++
	return nil
}

// FirstResidencyEpoch is the first RESIDENCY epoch fakeLeases mints.
//
// IT IS 1000 AND NOT 1, AND THAT IS THE POINT OF O3.4 RATHER THAN A DETAIL. Host
// holds two grants from two different issuers — this one, and the journal grant
// the RUNTIME holds, which testkit mints from testkit.FirstJournalEpoch = 1. Every
// fake in this module used to start at 1, so a Host that fed its residency epoch
// to a journal reader produced the right number by coincidence and every test
// passed. harness's own capability doc names that coincidence in terms: "each
// counter starting at 1 under a fresh in-memory backend is an accident of initial
// conditions, not a relationship". Separating the bases is what turns a future
// confusion of the two into a failing assertion instead of a green suite.
const FirstResidencyEpoch ResidencyEpoch = 1000

// fakeLeases grants leases and records every grant.
type fakeLeases struct {
	trace *trace

	mu        sync.Mutex
	nextEpoch ResidencyEpoch
	// exclusive models a real Leaser: a second grant for a held session is
	// refused with ErrLeaseHeld.
	exclusive bool
	held      map[sessionwire.SessionID]bool
	granted   []*fakeLease
	err       error
	zeroEpoch bool
	nilLease  bool
	// releaseErr is given to every lease this store grants, so a test can make
	// the ROLLBACK itself fail.
	releaseErr error
}

func (s *fakeLeases) AcquireSessionLease(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		s.trace.record("lease.acquire-on-dead-context")
		return nil, errDeadContext
	}
	s.trace.record("lease.acquire")
	if s.err != nil {
		return nil, s.err
	}
	if s.exclusive && s.held[session] {
		return nil, ErrLeaseHeld
	}
	if s.nilLease {
		return nil, nil
	}
	s.held[session] = true
	if s.nextEpoch == 0 {
		s.nextEpoch = FirstResidencyEpoch - 1
	}
	s.nextEpoch++
	epoch := s.nextEpoch
	if s.zeroEpoch {
		epoch = 0
	}
	lease := &fakeLease{trace: s.trace, epoch: epoch, lost: make(chan struct{}), releaseErr: s.releaseErr}
	s.granted = append(s.granted, lease)
	return lease, nil
}

// heldCount reports how many granted leases have not been released.
func (s *fakeLeases) heldCount() int {
	s.mu.Lock()
	granted := append([]*fakeLease(nil), s.granted...)
	s.mu.Unlock()
	held := 0
	for _, lease := range granted {
		if lease.releasedCount() == 0 {
			held++
		}
	}
	return held
}

func (s *fakeLeases) grantedCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.granted) }

// fakeJournal records opening fences.
type fakeJournal struct {
	trace *trace

	mu     sync.Mutex
	fences int
	err    error
}

// CommitOpeningFence records that a fence happened and NOT the epoch it was
// stamped with, because the seam no longer carries one.
//
// THE FAKE WAS LOOSER THAN THE DEPENDENCY IN EXACTLY THIS PLACE. It accepted an
// epoch a caller chose and recorded it, so the whole package could assert that
// Host stamped the journal fence with Host's own lease epoch — a write the
// released store refuses on the one seam where it can be compared
// (JournalWriter refuses an application prefix whose LeaseEpoch a caller chose)
// and which, in disposition mode, no session accepts at all.
func (j *fakeJournal) CommitOpeningFence(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if ctx.Err() != nil {
		j.trace.record("journal.fence-on-dead-context")
		return errDeadContext
	}
	j.trace.record("journal.fence")
	if j.err != nil {
		return j.err
	}
	j.fences++
	return nil
}

func (j *fakeJournal) committed() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fences
}

// fakeDurable serves durable session state.
type fakeDurable struct {
	trace *trace

	mu    sync.Mutex
	state SessionState
	err   error
	calls int
}

func (d *fakeDurable) LoadSessionState(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) (SessionState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ctx.Err() != nil {
		d.trace.record("durable.load-on-dead-context")
		return SessionState{}, errDeadContext
	}
	d.trace.record("durable.load")
	d.calls++
	if d.err != nil {
		return SessionState{}, d.err
	}
	return d.state, nil
}

// fakeWorkspaces materializes and releases workspaces.
type fakeWorkspaces struct {
	trace *trace

	mu         sync.Mutex
	root       string
	ensured    int
	released   int
	deadCalls  int
	ensureErr  error
	releaseErr error
}

func (w *fakeWorkspaces) EnsureWorkspace(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ctx.Err() != nil {
		w.trace.record("workspace.ensure-on-dead-context")
		return "", errDeadContext
	}
	w.trace.record("workspace.ensure")
	if w.ensureErr != nil {
		return "", w.ensureErr
	}
	w.ensured++
	return w.root, nil
}

func (w *fakeWorkspaces) ReleaseWorkspace(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ctx.Err() != nil {
		w.deadCalls++
		w.trace.record("workspace.release-on-dead-context")
		return errDeadContext
	}
	w.trace.record("workspace.release")
	if w.releaseErr != nil {
		return w.releaseErr
	}
	w.released++
	return nil
}

func (w *fakeWorkspaces) counts() (ensured, released int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ensured, w.released
}

// fakeLocations records the durable residency projection.
type fakeLocations struct {
	trace *trace

	mu           sync.Mutex
	published    []sessionwire.HostLinkRegistryObservation
	tombstoned   []uint64
	publishErr   error
	tombstoneErr error
	// failOnCall, when positive, refuses exactly that publish and no other,
	// counted from one. THE COUNT IS PER FIXTURE, NOT PER ATTACH: it spans every
	// session, which is fine for a single-key test and a trap for a
	// multi-session one, where the Nth publish is not the Nth publish of the
	// attach under test.
	//
	// It names ONE call rather than "everything after the
	// nth" because a test that needs the winner's resident publish to fail
	// usually needs a LATER attach to succeed, and a fake that fails forever
	// after the first refusal cannot express the difference.
	failOnCall int
	calls      int
}

func (l *fakeLocations) PublishResidency(ctx context.Context, observation sessionwire.HostLinkRegistryObservation) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ctx.Err() != nil {
		l.trace.record("location.publish-on-dead-context")
		return errDeadContext
	}
	l.trace.record("location.publish:" + string(observation.Residency))
	l.calls++
	if l.publishErr != nil && (l.failOnCall == 0 || l.calls == l.failOnCall) {
		return l.publishErr
	}
	l.published = append(l.published, observation)
	return nil
}

func (l *fakeLocations) TombstoneResidency(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, epoch uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ctx.Err() != nil {
		l.trace.record("location.tombstone-on-dead-context")
		return errDeadContext
	}
	l.trace.record("location.tombstone")
	if l.tombstoneErr != nil {
		return l.tombstoneErr
	}
	l.tombstoned = append(l.tombstoned, epoch)
	return nil
}

// live reports the observations that were published and not tombstoned. It is
// the NEGATIVE assertion's instrument: a rolled-back attach must leave none.
func (l *fakeLocations) live() []sessionwire.HostLinkRegistryObservation {
	l.mu.Lock()
	defer l.mu.Unlock()
	tombstoned := map[uint64]bool{}
	for _, epoch := range l.tombstoned {
		tombstoned[epoch] = true
	}
	var live []sessionwire.HostLinkRegistryObservation
	for _, observation := range l.published {
		if !tombstoned[observation.LeaseEpoch] {
			live = append(live, observation)
		}
	}
	return live
}

func (l *fakeLocations) publishedAll() []sessionwire.HostLinkRegistryObservation {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sessionwire.HostLinkRegistryObservation(nil), l.published...)
}

func (l *fakeLocations) tombstones() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.tombstoned...)
}

// fakeOwnershipHandle is one started ownership.
type fakeOwnershipHandle struct {
	trace   *trace
	stopErr error
	mu      sync.Mutex
	stopped int
}

func (h *fakeOwnershipHandle) Stop(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Err() != nil {
		h.trace.record("ownership.stop-on-dead-context")
		return errDeadContext
	}
	h.trace.record("ownership.stop")
	if h.stopErr != nil {
		return h.stopErr
	}
	h.stopped++
	return nil
}

func (h *fakeOwnershipHandle) stoppedCount() int { h.mu.Lock(); defer h.mu.Unlock(); return h.stopped }

// fakeOwnership begins inbox/event/heartbeat ownership.
type fakeOwnership struct {
	trace *trace

	mu       sync.Mutex
	started  []OwnershipRequest
	contexts []context.Context
	handles  []*fakeOwnershipHandle
	err      error
	stopErr  error
	// nilHandle makes BeginOwnership report success and return nothing.
	nilHandle bool
	// inWindow runs while the attach is between step 6 and step 9: the
	// registry entry exists and the session is not attached yet. It is the only
	// seam that reaches that window, and no other collaborator is called inside
	// it.
	inWindow func()

	// withRequest runs in the same window and is handed the request, which is
	// how a test reaches the attach's own fence — the object a real heartbeat
	// starting here would go on to write through.
	withRequest func(OwnershipRequest)
}

func (o *fakeOwnership) BeginOwnership(ctx context.Context, request OwnershipRequest) (OwnershipHandle, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ctx.Err() != nil {
		o.trace.record("ownership.begin-on-dead-context")
		return nil, errDeadContext
	}
	o.trace.record("ownership.begin")
	hook := o.inWindow
	o.inWindow = nil
	if hook != nil {
		// Released before the hook runs: the hook re-enters the Manager, and
		// holding a fake's lock across that would be a deadlock this fake
		// invented rather than a property of the subject.
		o.mu.Unlock()
		hook()
		o.mu.Lock()
	}
	if withRequest := o.withRequest; withRequest != nil {
		o.withRequest = nil
		o.mu.Unlock()
		withRequest(request)
		o.mu.Lock()
	}
	if o.err != nil {
		return nil, o.err
	}
	if o.nilHandle {
		return nil, nil
	}
	o.started = append(o.started, request)
	o.contexts = append(o.contexts, ctx)
	handle := &fakeOwnershipHandle{trace: o.trace, stopErr: o.stopErr}
	o.handles = append(o.handles, handle)
	return handle, nil
}

func (o *fakeOwnership) startedRequests() []OwnershipRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]OwnershipRequest(nil), o.started...)
}

func (o *fakeOwnership) startedContexts() []context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]context.Context(nil), o.contexts...)
}

// runningCount reports how many started ownerships have not been stopped.
func (o *fakeOwnership) runningCount() int {
	o.mu.Lock()
	handles := append([]*fakeOwnershipHandle(nil), o.handles...)
	o.mu.Unlock()
	running := 0
	for _, handle := range handles {
		if handle.stoppedCount() == 0 {
			running++
		}
	}
	return running
}

// tracingAdmissions records admission calls and DELEGATES to the real ledger.
//
// The delegate is *service.CapacityPublisher, deliberately, because "consume
// the existing ledger" is a claim a stub would let this package make falsely:
// a fake Admit that returns nil charges nothing, so a manager that never called
// the real one would pass every capacity assertion. See
// TestTheAdmissionLedgerIsTheOneServiceAlreadyOwns.
type tracingAdmissions struct {
	trace *trace
	inner Admissions

	mu            sync.Mutex
	admits        int
	releases      int
	refuseRelease bool
}

func (a *tracingAdmissions) Admit(key registry.Key, agent sessionwire.AgentID) error {
	a.mu.Lock()
	a.admits++
	a.mu.Unlock()
	a.trace.record("admit")
	return a.inner.Admit(key, agent)
}

func (a *tracingAdmissions) Release(key registry.Key) bool {
	a.mu.Lock()
	a.releases++
	refuse := a.refuseRelease
	a.mu.Unlock()
	a.trace.record("admission.release")
	if refuse {
		// The ledger reporting "this session was not charged". It is the shape
		// a double credit takes, and the only way to reach the branch that
		// consumes the bool.
		return false
	}
	return a.inner.Release(key)
}

// Draining delegates, so a test that begins a drain on the REAL ledger changes
// what the Manager advertises. A stub returning false would let a Manager that
// never consulted the flag pass.
func (a *tracingAdmissions) Draining() bool { return a.inner.Draining() }

func (a *tracingAdmissions) counts() (admits, releases int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.admits, a.releases
}

// fakeRuntime is a department.Runtime whose lifecycle calls are counted.
//
// It carries a Shutdown method that is NOT part of department.Runtime and must
// never be called. That is the only way to assert "nonterminal" as a property
// rather than as a spelling: a manager that reached for a terminal teardown by
// type assertion would find one here, and the assertion that it is never used
// is what makes the H4.1 distinction observable from outside.
type fakeRuntime struct {
	trace     *trace
	sessionID sessionwire.SessionID
	agentID   sessionwire.AgentID
	done      chan struct{}

	mu           sync.Mutex
	released     int
	shutdowns    int
	deadCalls    int
	releaseErr   error
	journalEpoch uint64
	journalHeld  bool
	epochReads   int
}

// testJournalEpoch is the epoch a fixture runtime reports as ITS OWN journal
// grant. It is nowhere near FirstResidencyEpoch or testEpoch: the two grants have
// different issuers and a test that could not tell them apart would be testing
// nothing. This is the value residency.Residency.JournalEpoch must carry, and the
// residency epoch is the value it must not.
const testJournalEpoch uint64 = 3

// LeaseEpoch reports the runtime's journal grant and counts the reads, so a test
// can tell a value that was READ from one that was remembered.
func (r *fakeRuntime) LeaseEpoch() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epochReads++
	return r.journalEpoch, r.journalHeld
}

// journalEpochReads reports how many times the runtime was asked for its grant.
func (r *fakeRuntime) journalEpochReads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epochReads
}

func (r *fakeRuntime) SessionID() sessionwire.SessionID { return r.sessionID }
func (r *fakeRuntime) AgentID() sessionwire.AgentID     { return r.agentID }

func (r *fakeRuntime) WaitIdle(context.Context) error { return nil }

func (r *fakeRuntime) Done() <-chan struct{} {
	r.trace.record("runtime.done")
	return r.done
}

func (r *fakeRuntime) ReleaseResidency(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() != nil {
		r.deadCalls++
		r.trace.record("runtime.release-on-dead-context")
		return errDeadContext
	}
	r.trace.record("runtime.release")
	if r.releaseErr != nil {
		return r.releaseErr
	}
	r.released++
	return nil
}

// Shutdown is the TERMINAL teardown and must never be reached from here.
func (r *fakeRuntime) Shutdown(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdowns++
	r.trace.record("runtime.shutdown")
	return nil
}

func (r *fakeRuntime) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	published := make(chan sessionwire.EnduringPublication)
	close(published)
	return published, nil
}

func (r *fakeRuntime) ApplyCommand(context.Context, department.RuntimeCommand) error { return nil }

func (r *fakeRuntime) counts() (released, shutdowns int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.released, r.shutdowns
}

// fakeTarget is a department.LaunchTarget with an observable launch.
type fakeTarget struct {
	trace         *trace
	compatibility department.CompatibilityID
	capabilities  department.Capabilities

	mu         sync.Mutex
	compatCall int
	// driftAfter, when positive, makes CompatibilityID answer driftTo from
	// that call onwards, so a test can model a target that reports a different
	// build each time it is asked. A LaunchTarget is caller code and may:
	// see internal/service's launchable for the panic a live re-read once cost.
	driftAfter int
	driftTo    department.CompatibilityID
	createErr  error
	restoreErr error
	// boundSessionID, when non-empty, is the identity every produced runtime
	// reports, whatever was asked for.
	boundSessionID sessionwire.SessionID
	// bornDone, when true, produces a runtime whose Done channel is already
	// closed: a runtime that has already stopped.
	bornDone bool
	// nilRuntime, when true, reports success and returns nothing.
	nilRuntime bool
	// noJournalGrant, when true, produces a runtime that holds no single-writer
	// journal lease: harness's headless, no-persistence and not-wired-for-
	// durable-commands configurations all answer (0, false), and all three are
	// legitimate rather than failures.
	noJournalGrant bool
	// releaseErr is given to every runtime this target produces.
	releaseErr error

	creates  []department.CreateRequest
	restores []department.RestoreRequest
	contexts []context.Context
	produced []*fakeRuntime
}

func (t *fakeTarget) CompatibilityID() department.CompatibilityID {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compatCall++
	if t.driftAfter > 0 && t.compatCall > t.driftAfter {
		return t.driftTo
	}
	return t.compatibility
}

// driftFromNow lets ONE more read answer the registered build and every read
// after that answer a different one, which is exactly the shape a snapshot must
// survive: the step-1 read is the snapshot, and any re-read is the defect. The
// threshold is taken from the live call count rather than written as a literal,
// so the test does not encode how many times constructing a Department and a
// CapacityPublisher happened to read it.
func (t *fakeTarget) driftFromNow(to department.CompatibilityID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.driftAfter = t.compatCall + 1
	t.driftTo = to
}

func (t *fakeTarget) Capabilities() department.Capabilities {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.capabilities
}

// redeclare changes what the target answers from now on. It is how a test
// models the thing a LaunchTarget is entitled to do and a snapshot exists to
// survive: answer differently on the next call.
func (t *fakeTarget) redeclare(compatibility department.CompatibilityID, capabilities department.Capabilities) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compatibility = compatibility
	t.driftAfter = 0
	t.capabilities = capabilities
}

func (t *fakeTarget) newRuntime(session sessionwire.SessionID, agent sessionwire.AgentID) *fakeRuntime {
	if t.boundSessionID != "" {
		session = t.boundSessionID
	}
	// THE RUNTIME'S OWN GRANT, and noJournalGrant is how a test models the
	// legitimate "this session is not wired to a lease that reports an epoch"
	// answer rather than a failure.
	runtime := &fakeRuntime{
		trace: t.trace, sessionID: session, agentID: agent,
		done: make(chan struct{}), releaseErr: t.releaseErr,
		journalEpoch: testJournalEpoch, journalHeld: !t.noJournalGrant,
	}
	if t.noJournalGrant {
		runtime.journalEpoch = 0
	}
	if t.bornDone {
		close(runtime.done)
	}
	t.produced = append(t.produced, runtime)
	return runtime
}

func (t *fakeTarget) Create(ctx context.Context, request department.CreateRequest) (department.Runtime, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx.Err() != nil {
		t.trace.record("target.create-on-dead-context")
		return nil, errDeadContext
	}
	t.trace.record("target.create")
	t.creates = append(t.creates, request)
	t.contexts = append(t.contexts, ctx)
	if t.createErr != nil {
		return nil, t.createErr
	}
	if t.nilRuntime {
		return nil, nil
	}
	return t.newRuntime(request.SessionID, request.AgentID), nil
}

func (t *fakeTarget) Restore(ctx context.Context, request department.RestoreRequest) (department.Runtime, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx.Err() != nil {
		t.trace.record("target.restore-on-dead-context")
		return nil, errDeadContext
	}
	t.trace.record("target.restore")
	t.restores = append(t.restores, request)
	t.contexts = append(t.contexts, ctx)
	if t.restoreErr != nil {
		return nil, t.restoreErr
	}
	if t.nilRuntime {
		return nil, nil
	}
	return t.newRuntime(request.SessionID, request.AgentID), nil
}

func (t *fakeTarget) producedRuntimes() []*fakeRuntime {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*fakeRuntime(nil), t.produced...)
}

func (t *fakeTarget) createRequests() []department.CreateRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]department.CreateRequest(nil), t.creates...)
}

func (t *fakeTarget) restoreRequests() []department.RestoreRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]department.RestoreRequest(nil), t.restores...)
}

func (t *fakeTarget) launchContexts() []context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]context.Context(nil), t.contexts...)
}

// spyRegistry delegates to the real registry and can interleave a COMPETING
// insert immediately before the manager's own, which is the only deterministic
// way to reach the registry-loser branch. See LocalRegistry.
type spyRegistry struct {
	inner        *registry.Registry
	trace        *trace
	mu           sync.Mutex
	beforeInsert func()
	// afterGet runs once, after the next read. It is the seam that puts a
	// heartbeat between reading the registry and writing the record, which is
	// the only place a lease can be lost mid-beat.
	afterGet func()
}

func (s *spyRegistry) Get(key registry.Key) (registry.Entry, bool) {
	entry, held := s.inner.Get(key)
	s.mu.Lock()
	hook := s.afterGet
	s.afterGet = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return entry, held
}

func (s *spyRegistry) Insert(key registry.Key, admission registry.Admission) (registry.Entry, bool) {
	s.mu.Lock()
	hook := s.beforeInsert
	s.beforeInsert = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.trace.record("registry.insert")
	return s.inner.Insert(key, admission)
}

// MarkReleasing and BeginTeardown are recorded but Get is NOT, deliberately:
// Get is on the attach hot path and recording it would put an entry in every
// manager test's expected sequence for a read that decides nothing.
func (s *spyRegistry) MarkReleasing(key registry.Key, generation uint64) (registry.Entry, bool) {
	s.trace.record("registry.releasing")
	return s.inner.MarkReleasing(key, generation)
}

func (s *spyRegistry) BeginTeardown(key registry.Key, generation uint64) (registry.Entry, bool) {
	s.trace.record("registry.teardown")
	return s.inner.BeginTeardown(key, generation)
}

func (s *spyRegistry) RemoveByGeneration(key registry.Key, generation uint64) bool {
	s.trace.record("registry.remove")
	return s.inner.RemoveByGeneration(key, generation)
}

// The spy satisfies both seams, so the heartbeat tests drive the same registry
// the manager tests do.
var (
	_ LocalRegistry     = (*spyRegistry)(nil)
	_ HeartbeatRegistry = (*spyRegistry)(nil)
)

// ---------------------------------------------------------------------------
// Host fixture
// ---------------------------------------------------------------------------

type stubSessionStore struct{}

func (stubSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

type stubHostWorkspaces struct{}

func (stubHostWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", nil
}

type stubAuth struct{}

func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// fixedClock is a time source pinned to a distinctive instant, so a projection
// field carrying the wrong time is not accidentally equal to the right one.
type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time                       { return c.at }
func (c fixedClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

const (
	testTenant        sessionwire.TenantID         = "tenant-9f3"
	testSession       sessionwire.SessionID        = "session-4d2"
	testAgent         sessionwire.AgentID          = "coder"
	testHost          sessionwire.HostID           = "host-7c1"
	testCompat        department.CompatibilityID   = "rig-2026-09-a1"
	testEndpoint      sessionwire.InternalEndpoint = "wss://host-7c1.internal.example:8443/hostlink"
	testWorkspaceRoot                              = "/var/lib/host/workspaces/session-4d2"
	testNamespace                                  = "tenant-9f3/session-4d2/objects"
	testGeneration    uint64                       = 41
	testWeight        uint64                       = 3
	testCapacity      uint64                       = 8
	testExpiry                                     = 31 * time.Second
	// testFirstEpoch is the first RESIDENCY epoch a fixture grant mints, and it
	// is deliberately NOT 1. The first minted epoch, the first registry
	// generation and the first of anything else counted from zero are all 1, so
	// a fixture granting epoch 1 would let a manager that fenced the journal
	// with a literal, or with the generation, pass every assertion about the
	// epoch. Measured: with epoch 1 a hard-coded fence value survived the whole
	// suite.
	//
	// IT IS NOW FirstResidencyEpoch, AND THE DISTANCE FROM testJournalEpoch IS
	// THE ASSERTION O3.4 EXISTS FOR. The runtime's journal grant is minted by a
	// different issuer; the fixture runtime reports 3 and this mints from 1000.
	// A Host that copied one into the other's slot is now a 997-apart value
	// mismatch rather than a coincidence of two counters that both started at 1.
	testFirstEpoch = FirstResidencyEpoch
)

var testClockAt = time.Date(2026, 9, 2, 14, 5, 6, 0, time.UTC)

// testRigSessionID is Harness's identity for the restored session. It is a
// FIXED value rather than a fresh one, so an assertion that it was propagated
// cannot pass because both sides called the same generator.
var testRigSessionID = uuid.MustParse("6f1c2b84-9a3d-4c17-b0e5-2d7f8a4c1e93")

// fixture is one wired Manager and every fake it was built from.
type fixture struct {
	t          *testing.T
	trace      *trace
	host       *host.Host
	target     *fakeTarget
	registry   *spyRegistry
	publisher  *service.CapacityPublisher
	admissions *tracingAdmissions
	leases     *fakeLeases
	journal    *fakeJournal
	durable    *fakeDurable
	workspaces *fakeWorkspaces
	locations  *fakeLocations
	ownership  *fakeOwnership
	manager    *Manager

	// targetOverride replaces the fake target in the Department, so a test can
	// run the same manager over department.NewRigTarget's real adapter.
	targetOverride department.LaunchTarget

	// fixedSession, when set, builds a DEDICATED Host bound to that session.
	fixedSession sessionwire.SessionID

	// afterBuild runs once the Host, the ledger and the Manager exist, for the
	// rows that must configure something built by newFixture rather than
	// something handed to it.
	afterBuild func(*fixture)
}

func newFixture(t *testing.T, configure ...func(*fixture)) *fixture {
	t.Helper()
	steps := &trace{}
	f := &fixture{
		t:     t,
		trace: steps,
		target: &fakeTarget{
			trace:         steps,
			compatibility: testCompat,
			capabilities: department.Capabilities{
				SupportsPooled:    true,
				SupportsDedicated: true,
				RequiresWorkspace: true,
				AdmissionWeight:   testWeight,
				CaptureSafety:     department.CaptureSafetyStreaming,
			},
		},
		registry:   &spyRegistry{inner: registry.New(fixedClock{at: testClockAt}), trace: steps},
		leases:     &fakeLeases{trace: steps, nextEpoch: testFirstEpoch - 1, held: map[sessionwire.SessionID]bool{}},
		journal:    &fakeJournal{trace: steps},
		durable:    &fakeDurable{trace: steps, state: SessionState{Exists: true, Namespace: testNamespace, CompatibilityID: testCompat, RigSessionID: testRigSessionID, HasCheckpoint: true, CheckpointSequence: 91}},
		workspaces: &fakeWorkspaces{trace: steps, root: testWorkspaceRoot},
		locations:  &fakeLocations{trace: steps},
		ownership:  &fakeOwnership{trace: steps},
	}
	for _, apply := range configure {
		apply(f)
	}

	var target department.LaunchTarget = f.target
	if f.targetOverride != nil {
		target = f.targetOverride
	}
	registrations := []department.Registration{{AgentID: testAgent, Target: target}}
	dept, err := department.New(registrations)
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	placement, capacity := sessionwire.HostPlacementPooled, testCapacity
	if f.fixedSession != "" {
		placement, capacity = sessionwire.HostPlacementDedicated, 1
	}
	built, err := host.New(host.Options{
		HostID:            testHost,
		TenantID:          testTenant,
		InternalEndpoint:  testEndpoint,
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      stubSessionStore{},
		Workspaces:        stubHostWorkspaces{},
		Clock:             fixedClock{at: testClockAt},
		Auth:              stubAuth{},
		Placement:         placement,
		Capacity:          capacity,
		FixedSessionID:    f.fixedSession,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: 5 * time.Second,
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
	f.admissions = &tracingAdmissions{trace: steps, inner: publisher}

	manager, err := NewManager(Options{
		Host:           built,
		HostGeneration: testGeneration,
		Registry:       f.registry,
		Admissions:     f.admissions,
		Leases:         f.leases,
		Journal:        f.journal,
		Durable:        f.durable,
		Workspaces:     f.workspaces,
		Locations:      f.locations,
		Ownership:      f.ownership,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	f.manager = manager
	t.Cleanup(manager.Close)
	if f.afterBuild != nil {
		f.afterBuild(f)
	}
	return f
}

func (f *fixture) key() registry.Key {
	return registry.Key{TenantID: testTenant, SessionID: testSession}
}

func (f *fixture) request(mode Mode) Request { return f.requestFor(testSession, mode) }

// requestWithoutBuild omits the placed-on build, for the tests in which the
// target's build changes underneath the attach and pinning it in the request
// would refuse the very case being measured.
func (f *fixture) requestWithoutBuild(mode Mode) Request {
	request := f.requestFor(testSession, mode)
	request.CompatibilityID = ""
	return request
}

func (f *fixture) requestFor(session sessionwire.SessionID, mode Mode) Request {
	return Request{
		TenantID:        testTenant,
		SessionID:       session,
		AgentID:         testAgent,
		Mode:            mode,
		CompatibilityID: testCompat,
		Principal: Principal{
			TenantID: testTenant,
			ActorID:  "factory/service-01",
			TraceID:  "trace-b7e",
		},
	}
}

// assertNothingHeld is the NEGATIVE assertion every failure row makes. The
// interesting half of this task is here: not that an error was returned, but
// that nothing survived it.
// assertRouteRemoved holds the two clauses about the DURABLE ROUTE: nothing is
// left live, and a route that was published was removed by a tombstone rather
// than by erasure.
//
// IT IS SEPARATE FROM THE MAIN HELPER BECAUSE ONE ROW LEGITIMATELY DIFFERS,
// and the earlier remedy for that row — skipping the whole helper — silently
// dropped seven checks it passes. An opt-out that takes unrelated coverage with
// it is how a row stops asserting things nobody notices. A failure whose grant
// was superseded must NOT tombstone: the write would be one from a holder that
// no longer owns the record, so the route is deliberately left to expire and
// named in Unreleased instead. That row asserts everything else, and asserts
// the naming in place of these two.
func (f *fixture) assertRouteRemoved(t *testing.T) {
	t.Helper()
	if live := f.locations.live(); len(live) != 0 {
		t.Errorf("%d durable residency projection(s) still live after a failed attach; want a tombstone instead", len(live))
	}
	if published, tombstones := f.locations.publishedAll(), f.locations.tombstones(); len(published) > 0 && len(tombstones) == 0 {
		t.Errorf("%d projection(s) were published and none tombstoned; §10.1 removes a route with an EXPIRED epoch-fenced tombstone rather than by erasing the fencing high-water mark", len(published))
	}
}

// assertNothingHeld is the NEGATIVE assertion every failure row makes: not that
// an error came back, but that nothing survived it.
//
// It covers the lease grants, the admission charge, the local registry entry,
// the watcher, the runtime's exactly-one nonterminal release and never-terminal
// teardown, the workspace materialization balance, the launched context's
// cancellation, and the count of rollback calls made on a context that was
// already done. The durable route is assertRouteRemoved's, and the split is
// explained there.
func (f *fixture) assertNothingHeld(t *testing.T) {
	t.Helper()
	if held := f.leases.heldCount(); held != 0 {
		t.Errorf("%d granted lease(s) are still held after a failed attach; every one must be released", held)
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != 0 {
		t.Errorf("the admission ledger still charges %d after a failed attach; want 0", consumed)
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds a residency after a failed attach")
	}
	if running := f.ownership.runningCount(); running != 0 {
		t.Errorf("%d ownership(s) still running after a failed attach; a failed attach starts no watcher", running)
	}
	for i, runtime := range f.target.producedRuntimes() {
		released, shutdowns := runtime.counts()
		if released != 1 {
			t.Errorf("runtime %d was released %d times after a failed attach; want exactly one nonterminal ReleaseResidency", i, released)
		}
		if shutdowns != 0 {
			t.Errorf("runtime %d was shut down %d times; residency release is NONTERMINAL and must never reach a terminal teardown", i, shutdowns)
		}
	}
	ensured, released := f.workspaces.counts()
	if ensured != released {
		t.Errorf("%d workspace(s) were materialized and %d released after a failed attach; the local materialization must not survive", ensured, released)
	}
	// THE EIGHTH RESOURCE, and the one that had no guard: deleting the session
	// context's compensation passed the whole suite, leaking a context.WithCancel
	// child on the manager root for the process lifetime and leaving the
	// released runtime with a live context.
	for i, ctx := range f.target.launchContexts() {
		if ctx.Err() == nil {
			t.Errorf("the context runtime %d was launched on is still live after a failed attach; a released runtime keeps a live context and the cancel leaks on the manager root", i)
		}
	}
	if dead := f.trace.count("lease.release-on-dead-context") + f.trace.count("workspace.release-on-dead-context") + f.trace.count("runtime.release-on-dead-context") + f.trace.count("location.tombstone-on-dead-context") + f.trace.count("ownership.stop-on-dead-context"); dead != 0 {
		t.Errorf("%d rollback call(s) were made on a context that was already done; a rollback that runs on the request context releases nothing once the caller has gone", dead)
	}
}

// createSequence is the observable order of a successful create. It is spelled
// out as one literal rather than assembled from parts, because a sequence
// assembled by the same helper the manager uses would agree with a reordered
// manager.
var createSequence = []string{
	"admit",
	"lease.acquire",
	"journal.fence",
	"durable.load",
	"workspace.ensure",
	"target.create",
	"runtime.done",
	"registry.insert",
	"location.publish:attaching",
	"ownership.begin",
	"location.publish:resident",
}

var restoreSequence = []string{
	"admit",
	"lease.acquire",
	"journal.fence",
	"durable.load",
	"workspace.ensure",
	"target.restore",
	"runtime.done",
	"registry.insert",
	"location.publish:attaching",
	"ownership.begin",
	"location.publish:resident",
}

func requireSteps(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the attach sequence was\n  %v\nwant\n  %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// The sequence
// ---------------------------------------------------------------------------

func TestAttachCreatesUnderTheLeaseInTheSpecifiedOrder(t *testing.T) {
	f := newFixture(t)

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	requireSteps(t, f.trace.recorded(), createSequence)

	if !residency.Attached {
		t.Error("the first attach reports Attached false; it is the call that established the residency")
	}
	if residency.Key != f.key() {
		t.Errorf("residency key is %+v, want %+v", residency.Key, f.key())
	}
	if residency.LeaseEpoch != testFirstEpoch {
		t.Errorf("residency reports lease epoch %d, want the granted %d", residency.LeaseEpoch, testFirstEpoch)
	}
	if residency.CompatibilityID != testCompat {
		t.Errorf("residency reports compatibility %q, want %q", residency.CompatibilityID, testCompat)
	}
	if residency.Runtime == nil {
		t.Fatal("residency carries no runtime")
	}
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("the local registry holds no residency after a successful attach")
	}
	if entry.Generation != residency.Generation {
		t.Errorf("residency reports generation %d and the registry holds %d", residency.Generation, entry.Generation)
	}
	if ResidencyEpoch(entry.LeaseEpoch) != residency.LeaseEpoch {
		t.Errorf("the registry entry carries lease epoch %d and the residency reports %d", entry.LeaseEpoch, residency.LeaseEpoch)
	}
	if entry.Runtime != residency.Runtime {
		t.Error("the registry holds a different runtime from the one reported to the caller")
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != testWeight {
		t.Errorf("the admission ledger charges %d, want the target's weight %d", consumed, testWeight)
	}
	// ONE FENCE, AND NO EPOCH ON IT. This assertion used to read "the fence
	// carries exactly the lease epoch", which was the fusion stated as a
	// requirement: the fence is a JOURNAL record and Host holds no journal
	// grant, so the writer stamps its own. What is left to assert is that the
	// fence happened exactly once, before hydration; the epoch is no longer
	// Host's to get right or wrong.
	if fences := f.journal.committed(); fences != 1 {
		t.Errorf("the attach committed %d opening fences, want exactly 1", fences)
	}

	// THE TWO GRANTS, SIDE BY SIDE. The residency epoch is Host's own and comes
	// from the lease; the journal epoch is the RUNTIME's and comes from the
	// runtime. They are 997 apart in this fixture because they are minted by
	// different issuers, and the whole of O3.4 is that the two are no longer one
	// number playing both roles.
	if !residency.JournalEpochHeld {
		t.Error("the residency reports no journal grant, and the fixture runtime holds one")
	}
	if residency.JournalEpoch != JournalEpoch(testJournalEpoch) {
		t.Errorf("residency reports journal epoch %d, want the runtime's %d", residency.JournalEpoch, testJournalEpoch)
	}
	if uint64(residency.JournalEpoch) == uint64(residency.LeaseEpoch) {
		t.Errorf("the two epochs are both %d, so this fixture cannot tell the residency grant from the journal grant", residency.JournalEpoch)
	}

	// The create request the target actually received. This is the CONSUMER's
	// copy: asserting the manager's own fields would prove it can remember a
	// value, not that the value reached the launch.
	creates := f.target.createRequests()
	if len(creates) != 1 {
		t.Fatalf("the target was asked to create %d times, want 1", len(creates))
	}
	want := department.CreateRequest{
		TenantID:      testTenant,
		SessionID:     testSession,
		AgentID:       testAgent,
		Placement:     sessionwire.HostPlacementPooled,
		WorkspaceRoot: testWorkspaceRoot,
		Storage:       department.StorageContext{Namespace: testNamespace},
	}
	if creates[0] != want {
		t.Errorf("the target received\n  %+v\nwant\n  %+v", creates[0], want)
	}
}

func TestAttachRestoresUnderTheLeaseInTheSpecifiedOrder(t *testing.T) {
	f := newFixture(t)

	residency, err := f.manager.Attach(context.Background(), f.request(ModeRestore))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	requireSteps(t, f.trace.recorded(), restoreSequence)

	restores := f.target.restoreRequests()
	if len(restores) != 1 {
		t.Fatalf("the target was asked to restore %d times, want 1", len(restores))
	}
	want := department.RestoreRequest{
		TenantID:        testTenant,
		SessionID:       testSession,
		AgentID:         testAgent,
		Placement:       sessionwire.HostPlacementPooled,
		WorkspaceRoot:   testWorkspaceRoot,
		Storage:         department.StorageContext{Namespace: testNamespace},
		CompatibilityID: testCompat,
		RigSessionID:    testRigSessionID,
	}
	if restores[0] != want {
		t.Errorf("the target received\n  %+v\nwant\n  %+v", restores[0], want)
	}
	if !residency.Attached {
		t.Error("the restoring attach reports Attached false")
	}
}

// TestTheOpeningFenceIsCommittedBeforeAnyHydration states step 3's ordering as
// its own claim.
//
// It overlaps the whole-sequence assertion above deliberately: that one fails
// for any reordering at all, so it cannot tell a reader WHICH rule was broken,
// and the fence rule is the one with a correctness argument behind it. Its
// probe is a manager that fences after hydration; the whole-sequence test dies
// too, which is the point of having both.
func TestTheOpeningFenceIsCommittedBeforeAnyHydration(t *testing.T) {
	f := newFixture(t)
	if _, err := f.manager.Attach(context.Background(), f.request(ModeRestore)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	fence := f.trace.indexOf("journal.fence")
	if fence < 0 {
		t.Fatal("no opening fence was committed")
	}
	for _, hydration := range []string{"durable.load", "workspace.ensure", "target.restore"} {
		at := f.trace.indexOf(hydration)
		if at < 0 {
			t.Fatalf("hydration step %q never ran, so this test asserts nothing about it", hydration)
		}
		if at < fence {
			t.Errorf("%q ran at %d, before the opening fence at %d; a hydration that precedes the fence leaves two processes believing they own the journal", hydration, at, fence)
		}
	}
	if lease := f.trace.indexOf("lease.acquire"); lease > fence {
		t.Errorf("the fence was committed at %d before the lease was acquired at %d; the fence is stamped with the lease epoch", fence, lease)
	}
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

func TestAttachIsIdempotentForAResidentSession(t *testing.T) {
	f := newFixture(t)
	first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("first Attach: %v", err)
	}
	before := f.trace.recorded()

	second, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("second Attach: %v", err)
	}

	if second.Attached {
		t.Error("the second attach reports Attached true; it established nothing")
	}
	if second.Generation != first.Generation {
		t.Errorf("the second attach reports generation %d and the first %d; a repeated attach must report the residency that exists", second.Generation, first.Generation)
	}
	if second.Runtime != first.Runtime {
		t.Error("the second attach reports a different runtime")
	}
	if second.LeaseEpoch != first.LeaseEpoch {
		t.Errorf("the second attach reports epoch %d and the first %d", second.LeaseEpoch, first.LeaseEpoch)
	}
	requireSteps(t, f.trace.recorded(), before)
	if granted := f.leases.grantedCount(); granted != 1 {
		t.Errorf("%d leases were granted across two attaches of one session, want 1", granted)
	}
	if produced := len(f.target.producedRuntimes()); produced != 1 {
		t.Errorf("%d runtimes were constructed across two attaches of one session, want 1", produced)
	}
	if started := len(f.ownership.startedRequests()); started != 1 {
		t.Errorf("%d ownerships were started across two attaches of one session, want 1", started)
	}
}

// TestAnIdempotentAttachRefusesADifferentAgent is the OTHER DIRECTION of the
// rule above. "The same request returns the same residency" is only half a
// rule; without this row a manager that ignored the agent entirely would be
// indistinguishable from one that matched it.
func TestAnIdempotentAttachRefusesADifferentAgent(t *testing.T) {
	f := newFixture(t)
	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
		t.Fatalf("first Attach: %v", err)
	}
	before := f.trace.recorded()

	request := f.request(ModeCreate)
	request.AgentID = "reviewer"
	_, err := f.manager.Attach(context.Background(), request)
	if err == nil {
		t.Fatal("attaching a resident session under a different agent was accepted")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepValidate {
		t.Errorf("the refusal names step %q, want %q", attach.Step, StepValidate)
	}
	requireSteps(t, f.trace.recorded(), before)
}

// ---------------------------------------------------------------------------
// Races
// ---------------------------------------------------------------------------

func TestConcurrentColdAttachInstallsExactlyOneResidency(t *testing.T) {
	const attachers = 8
	f := newFixture(t)

	start := make(chan struct{})
	results := make([]Residency, attachers)
	failures := make([]error, attachers)
	var waiting, done sync.WaitGroup
	waiting.Add(attachers)
	done.Add(attachers)
	for i := range attachers {
		go func() {
			defer done.Done()
			waiting.Done()
			<-start
			results[i], failures[i] = f.manager.Attach(context.Background(), f.request(ModeRestore))
		}()
	}
	// A CLOSED CHANNEL, not a sleep and not a buffered send. Every goroutine is
	// parked on the same receive and is released by one close, so the race is
	// real on every run rather than on the runs where a sleep happened to be
	// long enough.
	waiting.Wait()
	close(start)
	done.Wait()

	attached, generations := 0, map[uint64]int{}
	for i, err := range failures {
		if err != nil {
			t.Fatalf("attacher %d failed: %v", i, err)
		}
		generations[results[i].Generation]++
		if results[i].Attached {
			attached++
		}
	}
	if attached != 1 {
		t.Errorf("%d of %d concurrent attaches reported Attached true, want exactly one winner", attached, attachers)
	}
	if len(generations) != 1 {
		t.Errorf("the concurrent attaches reported %d distinct generations %v, want one residency", len(generations), generations)
	}
	if produced := len(f.target.producedRuntimes()); produced != 1 {
		t.Errorf("%d runtimes were constructed for one cold session, want 1", produced)
	}
	if started := len(f.ownership.startedRequests()); started != 1 {
		t.Errorf("%d ownerships were started, want 1; a loser starts no watcher", started)
	}
	if granted := f.leases.grantedCount(); granted != 1 {
		t.Errorf("%d leases were granted for one cold session, want 1", granted)
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != testWeight {
		t.Errorf("the admission ledger charges %d after %d concurrent attaches, want one weight %d", consumed, attachers, testWeight)
	}
	if live := f.locations.live(); len(live) != 2 {
		t.Errorf("%d durable projections are live, want the attaching and resident pair of one residency", len(live))
	}
}

// TestTheRegistryLoserReleasesItsRuntimeNonterminally is the bug named by O3.1
// step 5, made deterministic.
//
// The competing entry is installed by the spy immediately before the manager's
// own Insert, which is the only way to reach the losing branch with certainty:
// the concurrent test above exercises the same code but cannot GUARANTEE two
// runtimes exist, and a coin-flip guard over the one defect this task exists to
// fix is not a guard.
func TestTheRegistryLoserReleasesItsRuntimeNonterminally(t *testing.T) {
	f := newFixture(t)
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	f.registry.beforeInsert = func() {
		f.registry.inner.Insert(f.key(), registry.Admission{
			AgentID:         testAgent,
			Target:          f.target,
			CompatibilityID: testCompat,
			Runtime:         rival,
			LeaseEpoch:      99,
		})
	}

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("losing the registry race must report the residency that exists, not an error: %v", err)
	}
	if residency.Attached {
		t.Error("the loser reports Attached true")
	}
	if residency.Runtime != department.Runtime(rival) {
		t.Error("the loser reports its own runtime rather than the one actually installed")
	}

	produced := f.target.producedRuntimes()
	if len(produced) != 1 {
		t.Fatalf("%d runtimes were constructed, want the loser's 1", len(produced))
	}
	released, shutdowns := produced[0].counts()
	if released != 1 {
		t.Errorf("the losing runtime was released %d times, want exactly one ReleaseResidency; leaving it live is the bug this test exists for", released)
	}
	if shutdowns != 0 {
		t.Errorf("the losing runtime was shut down %d times; release is NONTERMINAL and Shutdown durably appends SessionStopped", shutdowns)
	}
	if f.trace.indexOf("runtime.shutdown") >= 0 {
		t.Error("a terminal teardown appears in the trace")
	}
	if held := f.leases.heldCount(); held != 0 {
		t.Errorf("the loser still holds %d lease grant(s)", held)
	}
	if started := len(f.ownership.startedRequests()); started != 0 {
		t.Errorf("the loser started %d ownership(s), want 0", started)
	}
	if published := f.locations.publishedAll(); len(published) != 0 {
		t.Errorf("the loser wrote %d durable projection(s), want 0", len(published))
	}
}

// TestTheRegistryLoserLeavesTheWinnersSharedResourcesAlone is the half of the
// loser's rollback that is NOT symmetric, and it is a real over-release hazard
// rather than a hypothetical one.
//
// The admission ledger and the workspace are keyed by (TenantID, SessionID),
// not by attempt: service.Admit is documented as idempotent for a repeated
// (key, agent), and EnsureWorkspace materializes ONE root per session. So a
// loser that "released everything it took" would credit back the WINNER's
// charge and delete the WINNER's workspace — the resident session would then be
// uncharged, and this Host would admit past its capacity with no test in either
// package able to see it. Own resources are released; shared ones are not.
func TestTheRegistryLoserLeavesTheWinnersSharedResourcesAlone(t *testing.T) {
	f := newFixture(t)
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	f.registry.beforeInsert = func() {
		f.registry.inner.Insert(f.key(), registry.Admission{
			AgentID: testAgent, Target: f.target, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
		})
	}
	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if consumed := f.publisher.ConsumedWeight(); consumed != testWeight {
		t.Errorf("the admission ledger charges %d after the loser rolled back, want the resident session's %d still charged", consumed, testWeight)
	}
	ensured, released := f.workspaces.counts()
	if ensured != 1 {
		t.Fatalf("%d workspaces were materialized, want 1", ensured)
	}
	if released != 0 {
		t.Errorf("the loser released the session workspace %d time(s); that root is the resident runtime's", released)
	}
	// The ledger was not merely left charged, it was never CREDITED. Without
	// this row a manager that released and re-admitted would look identical.
	if _, releases := f.admissions.counts(); releases != 0 {
		t.Errorf("the loser credited the admission ledger back %d time(s); that charge is the resident session's", releases)
	}
}

func TestAnExclusiveLeaseRefusesTheSecondAttach(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.leases.exclusive = true })
	// The first grant is taken by something that is not this manager — another
	// Host process, which is the case an exclusive lease exists for.
	if _, err := f.leases.AcquireSessionLease(context.Background(), testTenant, testSession); err != nil {
		t.Fatalf("seeding the held lease: %v", err)
	}
	before := f.leases.heldCount()

	_, err := f.manager.Attach(context.Background(), f.request(ModeRestore))
	if err == nil {
		t.Fatal("an attach whose lease is held elsewhere was accepted")
	}
	if !errors.Is(err, ErrLeaseHeld) {
		t.Errorf("error %v does not unwrap to ErrLeaseHeld", err)
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepLease {
		t.Errorf("the refusal names step %q, want %q", attach.Step, StepLease)
	}
	code, ok := attach.HostLinkCode()
	if !ok || code != sessionwire.HostLinkErrorEpochMismatch {
		t.Errorf("the refusal reports HostLink code (%q, %t), want (%q, true): §15 says a held lease means the registry Factory routed from was stale", code, ok, sessionwire.HostLinkErrorEpochMismatch)
	}
	if held := f.leases.heldCount(); held != before {
		t.Errorf("%d leases are held, want the %d that were held before the refused attach", held, before)
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != 0 {
		t.Errorf("the admission ledger still charges %d after a refused attach", consumed)
	}
	if produced := len(f.target.producedRuntimes()); produced != 0 {
		t.Errorf("%d runtimes were constructed for a session this Host could not lease", produced)
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry holds a residency for a session this Host could not lease")
	}
}

// ---------------------------------------------------------------------------
// Failure injection after every sequence step
// ---------------------------------------------------------------------------

// TestFailureAtEverySequenceStepReleasesEverythingItTook is the bulk of this
// task, and its interesting assertions are the NEGATIVE ones: no lease left
// held, no watcher started, no registry entry, no runtime still live, no live
// projection, no workspace left materialized.
//
// The rows ENUMERATE the sequence rather than sampling it. Each names the steps
// that must NOT have run, because "an error came back" is satisfied by a
// manager that failed for the wrong reason at the wrong place.
func TestFailureAtEverySequenceStepReleasesEverythingItTook(t *testing.T) {
	sentinel := errors.New("injected")

	for _, row := range []struct {
		name      string
		mode      Mode
		configure func(*fixture)
		wantStep  Step
		wantCode  sessionwire.HostLinkErrorCode
		forbidden []string
	}{
		{
			name:      "1 host admission refused",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.afterBuild = func(f *fixture) { f.publisher.BeginDrain() } },
			wantStep:  StepValidate,
			wantCode:  sessionwire.HostLinkErrorNotAdmitting,
			forbidden: []string{"lease.acquire", "journal.fence", "durable.load", "workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "2 lease refused",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.leases.err = sentinel },
			wantStep:  StepLease,
			forbidden: []string{"journal.fence", "durable.load", "workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "2 lease granted with an epoch Core refuses",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.leases.zeroEpoch = true },
			wantStep:  StepLease,
			forbidden: []string{"journal.fence", "durable.load", "workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "2 lease reported without a grant",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.leases.nilLease = true },
			wantStep:  StepLease,
			forbidden: []string{"journal.fence", "durable.load", "workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "3 opening fence refused",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.journal.err = sentinel },
			wantStep:  StepFence,
			forbidden: []string{"durable.load", "workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 durable state unreadable",
			mode:      ModeRestore,
			configure: func(f *fixture) { f.durable.err = sentinel },
			wantStep:  StepHydrate,
			forbidden: []string{"workspace.ensure", "target.restore", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 restore of a session with no durable state",
			mode:      ModeRestore,
			configure: func(f *fixture) { f.durable.state.Exists = false },
			wantStep:  StepHydrate,
			wantCode:  sessionwire.HostLinkErrorRuntimeUnavailable,
			forbidden: []string{"workspace.ensure", "target.restore", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 restore onto a different runtime build",
			mode:      ModeRestore,
			configure: func(f *fixture) { f.durable.state.CompatibilityID = "rig-2026-04-z9" },
			wantStep:  StepHydrate,
			wantCode:  sessionwire.HostLinkErrorRuntimeMismatch,
			forbidden: []string{"workspace.ensure", "target.restore", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name: "4 restore of a target that requires a checkpoint it has not got",
			mode: ModeRestore,
			configure: func(f *fixture) {
				f.target.capabilities.RequiresCheckpoint = true
				f.durable.state.HasCheckpoint = false
			},
			wantStep:  StepHydrate,
			wantCode:  sessionwire.HostLinkErrorRuntimeUnavailable,
			forbidden: []string{"workspace.ensure", "target.restore", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 the durable store returns no object namespace",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.durable.state = SessionState{Exists: false} },
			wantStep:  StepHydrate,
			forbidden: []string{"workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name: "4 create over durable state written by a different build",
			mode: ModeCreate,
			configure: func(f *fixture) {
				f.durable.state = SessionState{Exists: true, Namespace: testNamespace, CompatibilityID: "rig-2026-04-z9"}
			},
			wantStep:  StepHydrate,
			wantCode:  sessionwire.HostLinkErrorRuntimeMismatch,
			forbidden: []string{"workspace.ensure", "target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 workspace cannot be materialized",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.workspaces.ensureErr = sentinel },
			wantStep:  StepHydrate,
			forbidden: []string{"target.create", "registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 the target refuses to launch",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.target.createErr = sentinel },
			wantStep:  StepHydrate,
			forbidden: []string{"registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "4 the target reports success and returns nothing",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.target.nilRuntime = true },
			wantStep:  StepHydrate,
			forbidden: []string{"registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "5 the launched runtime is bound to another session",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.target.boundSessionID = "session-somebody-elses" },
			wantStep:  StepCapabilities,
			wantCode:  sessionwire.HostLinkErrorRuntimeMismatch,
			forbidden: []string{"registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "5 the launched runtime has already stopped",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.target.bornDone = true },
			wantStep:  StepCapabilities,
			wantCode:  sessionwire.HostLinkErrorRuntimeUnavailable,
			forbidden: []string{"registry.insert", "location.publish:attaching", "ownership.begin"},
		},
		{
			name:      "7 the durable projection is refused",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.locations.publishErr = sentinel },
			wantStep:  StepPublish,
			forbidden: []string{"ownership.begin", "location.publish:resident"},
		},
		{
			name:      "8 ownership will not start",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.ownership.err = sentinel },
			wantStep:  StepOwnership,
			forbidden: []string{"location.publish:resident"},
		},
		{
			name:      "8 ownership reports success and returns no handle",
			mode:      ModeCreate,
			configure: func(f *fixture) { f.ownership.nilHandle = true },
			wantStep:  StepOwnership,
			forbidden: []string{"location.publish:resident"},
		},
		{
			name: "9 the resident projection is refused",
			mode: ModeCreate,
			configure: func(f *fixture) {
				f.locations.publishErr = sentinel
				f.locations.failOnCall = 2
			},
			wantStep:  StepAttached,
			forbidden: nil,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t, row.configure)

			_, err := f.manager.Attach(context.Background(), f.request(row.mode))
			if err == nil {
				t.Fatal("the injected failure was not reported")
			}
			var attach *AttachError
			if !errors.As(err, &attach) {
				t.Fatalf("error is %T, want *AttachError", err)
			}
			if attach.Step != row.wantStep {
				t.Errorf("the failure names step %q, want %q (trace: %v)", attach.Step, row.wantStep, f.trace.recorded())
			}
			if attach.Code != row.wantCode {
				t.Errorf("the failure carries HostLink code %q, want %q", attach.Code, row.wantCode)
			}
			for _, forbidden := range row.forbidden {
				if at := f.trace.indexOf(forbidden); at >= 0 {
					t.Errorf("step %q ran at index %d, after the failure; a failed step must not be followed by the next one (trace: %v)", forbidden, at, f.trace.recorded())
				}
			}
			f.assertNothingHeld(t)
			f.assertRouteRemoved(t)
			if _, live := f.manager.SessionContext(f.key()); live {
				t.Error("a session context survives a failed attach")
			}
		})
	}
}

// TestRollbackDoesNotRunOnTheRequestContext is the failure-injection suite's
// missing dimension: every row above uses a live request context, so a manager
// that rolled back on the REQUEST's context would pass all of them.
func TestRollbackDoesNotRunOnTheRequestContext(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.ownership.err = errors.New("injected") })

	// Cancelled BEFORE the attach, which is the strongest form: if any part of
	// the sequence or its rollback derives from this context, nothing works at
	// all and the fakes say so by name.
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := f.manager.Attach(requestCtx, f.request(ModeCreate)); err == nil {
		t.Fatal("the injected ownership failure was not reported")
	}
	for _, step := range f.trace.recorded() {
		if strings.HasSuffix(step, "-on-dead-context") {
			t.Errorf("%q: the manager passed a context that was already done; §8.3 separates request lifetime from session residency", step)
		}
	}
	f.assertNothingHeld(t)
	f.assertRouteRemoved(t)
}

// ---------------------------------------------------------------------------
// The session root context
// ---------------------------------------------------------------------------

// requestOnlyKey is a value a caller might attach to its own request context:
// an auth token, a tenant-scoped database handle, a credential.
type requestOnlyKey struct{}

func TestTheSessionContextCarriesOnlyApprovedFields(t *testing.T) {
	f := newFixture(t)
	requestCtx, cancel := context.WithDeadline(context.WithValue(context.Background(), requestOnlyKey{}, "bearer-token-do-not-retain"), time.Now().Add(time.Millisecond))
	defer cancel()

	residency, err := f.manager.Attach(requestCtx, f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	sessionCtx, live := f.manager.SessionContext(residency.Key)
	if !live {
		t.Fatal("the manager holds no session context for an attached residency")
	}

	if carried := sessionCtx.Value(requestOnlyKey{}); carried != nil {
		t.Errorf("the session context carries the request's value %v; approved fields are a WHITELIST and everything else must be unreachable rather than stripped", carried)
	}
	if deadline, ok := sessionCtx.Deadline(); ok {
		t.Errorf("the session context inherited the request's deadline of %v; a residency does not end when a request does", deadline)
	}
	principal, ok := PrincipalFrom(sessionCtx)
	if !ok {
		t.Fatal("the session context carries no principal")
	}
	want := f.request(ModeCreate).Principal
	if principal != want {
		t.Errorf("the session carries principal %+v, want %+v", principal, want)
	}
	// The ownership was begun on the session context, not the request's. Without
	// this the whitelist could hold for a context nothing downstream uses.
	contexts := f.ownership.startedContexts()
	if len(contexts) != 1 {
		t.Fatalf("%d ownerships were started, want 1", len(contexts))
	}
	if carried := contexts[0].Value(requestOnlyKey{}); carried != nil {
		t.Errorf("ownership was begun on a context carrying the request's %v", carried)
	}
	if carried, ok := PrincipalFrom(contexts[0]); !ok || carried != want {
		t.Errorf("ownership was begun on a context carrying principal (%+v, %t), want (%+v, true)", carried, ok, want)
	}
}

func TestTheSessionAndItsRuntimeOutliveTheRequest(t *testing.T) {
	f := newFixture(t)
	requestCtx, cancel := context.WithCancel(context.Background())

	residency, err := f.manager.Attach(requestCtx, f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	cancel()

	sessionCtx, live := f.manager.SessionContext(residency.Key)
	if !live {
		t.Fatal("the manager holds no session context for an attached residency")
	}
	if err := sessionCtx.Err(); err != nil {
		t.Errorf("cancelling the request ended the session context: %v", err)
	}
	for i, ctx := range f.ownership.startedContexts() {
		if err := ctx.Err(); err != nil {
			t.Errorf("cancelling the request ended ownership context %d: %v", i, err)
		}
	}
	for i, ctx := range f.target.launchContexts() {
		if err := ctx.Err(); err != nil {
			t.Errorf("cancelling the request ended the context the runtime was launched on (%d): %v", i, err)
		}
	}
	if _, held := f.registry.Get(residency.Key); !held {
		t.Error("cancelling the request removed the residency from the registry")
	}
	if runtimes := f.target.producedRuntimes(); len(runtimes) == 1 {
		if released, _ := runtimes[0].counts(); released != 0 {
			t.Errorf("cancelling the request released the runtime %d time(s)", released)
		}
	}

	// And the manager's own root still governs it, which is what makes the
	// lifetime OWNED rather than merely detached.
	f.manager.Close()
	if err := sessionCtx.Err(); err == nil {
		t.Error("closing the manager left the session context live; the root is meant to be the one lifetime owner")
	}
}

// ---------------------------------------------------------------------------
// Structural guards over the whitelist
// ---------------------------------------------------------------------------

// nonStringShapedFields returns the fields of a struct type that could carry
// something other than a string, which is what a whitelist must not have.
func nonStringShapedFields(subject reflect.Type) []string {
	var offenders []string
	for i := range subject.NumField() {
		field := subject.Field(i)
		if field.Type.Kind() != reflect.String {
			offenders = append(offenders, field.Name+" ("+field.Type.Kind().String()+")")
		}
	}
	return offenders
}

// smugglingPrincipal is the SYNTHETIC FIXTURE that violates the rule.
//
// It exists because Principal already satisfies it, and a guard asserting a
// property its only subject already has is a guard with an untested detector by
// construction: the assertion would pass if the check were `return nil`. This
// type is the missing negative row, and `any` is the exact hole being closed —
// it would let a caller hand a session an arbitrary value, the request context
// among them.
type smugglingPrincipal struct {
	TenantID sessionwire.TenantID
	Payload  any
}

func TestPrincipalCarriesOnlyStringShapedFields(t *testing.T) {
	subject := reflect.TypeOf(Principal{})
	if subject.NumField() == 0 {
		t.Fatal("Principal has no fields, so this guard examines nothing")
	}
	if offenders := nonStringShapedFields(subject); len(offenders) != 0 {
		t.Errorf("Principal carries non-string-shaped field(s) %v; a whitelist of approved values must be unable to carry an arbitrary one", offenders)
	}

	// The detector, proved on a type that breaks the rule.
	violating := nonStringShapedFields(reflect.TypeOf(smugglingPrincipal{}))
	if len(violating) != 1 || !strings.HasPrefix(violating[0], "Payload") {
		t.Errorf("the detector reported %v for a struct carrying an `any` field, want exactly the Payload field; a check that cannot fail is not a check", violating)
	}
}

// contextViolations reports production code that could give a session a
// request's lifetime or a request's values.
//
// TWO RULES, and both are WHITELISTS. R1 lists the context constructors this
// package may use at all, so context.WithoutCancel — which retains every value
// on the context it is handed, auth among them — is refused for being absent
// from the list rather than for being named in a ban. R2 refuses any
// context.With* call whose parent is a context.Context PARAMETER of the
// enclosing function, which is the structural spelling of "a session is never
// derived from a request".
//
// R2 FOLLOWS ONE LEVEL OF LOCAL ALIASING — `c := ctx` then
// `context.WithValue(c, …)` — because that is the likeliest maintainer edit of
// the shapes that defeat a bare identifier comparison, and it escaped until it
// was measured.
//
// WHAT IT DOES NOT COVER: a request context stored in a struct field and read
// back, and a helper that takes its parent as an `any`. Neither is refused
// here; what closes them today is that Principal cannot carry a context (see
// TestPrincipalCarriesOnlyStringShapedFields) and that R1 leaves only three
// constructors to reach.
func contextViolations(file *ast.File) []string {
	permitted := map[string]bool{"Background": true, "WithCancel": true, "WithValue": true}
	var violations []string
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if !isFunction {
			continue
		}
		requestParams := map[string]bool{}
		if function.Type.Params != nil {
			for _, param := range function.Type.Params.List {
				selector, isSelector := param.Type.(*ast.SelectorExpr)
				if !isSelector || selector.Sel.Name != "Context" {
					continue
				}
				for _, name := range param.Names {
					requestParams[name.Name] = true
				}
			}
		}
		// One level of local aliasing: any local assigned FROM a request
		// parameter is itself a request context for the purposes of R2.
		ast.Inspect(function, func(node ast.Node) bool {
			assignment, isAssignment := node.(*ast.AssignStmt)
			if !isAssignment || len(assignment.Lhs) != len(assignment.Rhs) {
				return true
			}
			for i, right := range assignment.Rhs {
				source, sourceIsIdent := right.(*ast.Ident)
				target, targetIsIdent := assignment.Lhs[i].(*ast.Ident)
				if sourceIsIdent && targetIsIdent && requestParams[source.Name] {
					requestParams[target.Name] = true
				}
			}
			return true
		})
		ast.Inspect(function, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			pkg, isIdent := selector.X.(*ast.Ident)
			if !isIdent || pkg.Name != "context" {
				return true
			}
			if !permitted[selector.Sel.Name] {
				violations = append(violations, function.Name.Name+" calls context."+selector.Sel.Name)
			}
			if len(call.Args) == 0 {
				return true
			}
			parent, parentIsIdent := call.Args[0].(*ast.Ident)
			if parentIsIdent && requestParams[parent.Name] {
				violations = append(violations, function.Name.Name+" derives context."+selector.Sel.Name+" from the request parameter "+parent.Name)
			}
			return true
		})
	}
	return violations
}

func parseProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fileSet := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		files[name] = parsed
	}
	if len(files) == 0 {
		t.Fatal("no production file was parsed, so this guard is vacuous")
	}
	return files
}

func TestProductionCodeNeverDerivesASessionFromARequestContext(t *testing.T) {
	files := parseProductionFiles(t)
	withValueCalls := 0
	for name, file := range files {
		for _, violation := range contextViolations(file) {
			t.Errorf("%s: %s", name, violation)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector || selector.Sel.Name != "WithValue" {
				return true
			}
			if pkg, isIdent := selector.X.(*ast.Ident); isIdent && pkg.Name == "context" {
				withValueCalls++
			}
			return true
		})
	}
	// FLOORED, because a guard that examines nothing passes. One site, and one
	// only: a second context.WithValue is a second whitelist.
	if withValueCalls != 1 {
		t.Errorf("production code installs %d context values, want exactly 1; the principal is the only approved one and a second site is a second whitelist", withValueCalls)
	}

	// The detector, proved on sources that break each rule, plus a control one
	// position over that must stay clean.
	for _, probe := range []struct {
		name   string
		source string
		want   int
	}{
		{
			name:   "withoutcancel over a request context",
			source: "package p\nimport \"context\"\nfunc f(ctx context.Context) context.Context { return context.WithoutCancel(ctx) }\n",
			want:   2,
		},
		{
			name:   "a value installed on a request context",
			source: "package p\nimport \"context\"\ntype k struct{}\nfunc f(ctx context.Context) context.Context { return context.WithValue(ctx, k{}, 1) }\n",
			want:   1,
		},
		{
			name:   "a value installed on a local alias of a request context",
			source: "package p\nimport \"context\"\ntype k struct{}\nfunc f(ctx context.Context) context.Context { c := ctx; return context.WithValue(c, k{}, 1) }\n",
			want:   1,
		},
		{
			name:   "control: a value installed on a root this function did not receive",
			source: "package p\nimport \"context\"\ntype k struct{}\nvar root context.Context\nfunc f() context.Context { return context.WithValue(root, k{}, 1) }\n",
			want:   0,
		},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", probe.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parsing the probe: %v", probe.name, err)
		}
		if got := contextViolations(parsed); len(got) != probe.want {
			t.Errorf("%s: the detector reported %v (%d), want %d violations", probe.name, got, len(got), probe.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshots and the projection
// ---------------------------------------------------------------------------

// TestTheCompatibilityIDIsSnapshottedOnceAndNotReRead holds the rule
// internal/service learned the hard way: a LaunchTarget is caller code and may
// answer differently on every call, so a value VALIDATED at step 1 does not
// bind the value RE-READ at step 4 or step 7.
func TestTheCompatibilityIDIsSnapshottedOnceAndNotReRead(t *testing.T) {
	const drifted department.CompatibilityID = "rig-2027-01-drifted"
	f := newFixture(t)
	f.target.driftFromNow(drifted)

	residency, err := f.manager.Attach(context.Background(), f.request(ModeRestore))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if residency.CompatibilityID != testCompat {
		t.Errorf("the residency reports build %q, want the snapshotted %q", residency.CompatibilityID, testCompat)
	}
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("no residency was installed")
	}
	if entry.CompatibilityID != testCompat {
		t.Errorf("the registry recorded build %q, want the snapshotted %q", entry.CompatibilityID, testCompat)
	}
	restores := f.target.restoreRequests()
	if len(restores) != 1 {
		t.Fatalf("%d restores, want 1", len(restores))
	}
	if restores[0].CompatibilityID != testCompat {
		t.Errorf("the restore was asked for build %q, want the snapshotted %q", restores[0].CompatibilityID, testCompat)
	}
	for i, observation := range f.locations.publishedAll() {
		if department.CompatibilityID(observation.RuntimeCompatibilityID) != testCompat {
			t.Errorf("projection %d advertises build %q, want the snapshotted %q; a drifting id would silently write to a different record", i, observation.RuntimeCompatibilityID, testCompat)
		}
	}
	if started := f.ownership.startedRequests(); len(started) == 1 && started[0].CompatibilityID != testCompat {
		t.Errorf("ownership was begun for build %q, want the snapshotted %q", started[0].CompatibilityID, testCompat)
	}
}

func TestThePublishedProjectionIsTheEpochFencedObservation(t *testing.T) {
	f := newFixture(t)
	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	published := f.locations.publishedAll()
	if len(published) != 2 {
		t.Fatalf("%d projections were published, want the attaching/resident pair of §9's state machine", len(published))
	}
	if published[0].Residency != sessionwire.SessionResidencyAttaching {
		t.Errorf("the first projection reports residency %q, want %q", published[0].Residency, sessionwire.SessionResidencyAttaching)
	}
	if published[0].Accepting {
		t.Error("the attaching projection reports Accepting true; nothing may be routed to a session whose ownership has not started")
	}
	if published[1].Residency != sessionwire.SessionResidencyResident {
		t.Errorf("the second projection reports residency %q, want %q", published[1].Residency, sessionwire.SessionResidencyResident)
	}
	if !published[1].Accepting {
		t.Error("the resident projection reports Accepting false")
	}

	want := sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               testTenant,
		SessionID:              testSession,
		HostID:                 testHost,
		HostGeneration:         testGeneration,
		AgentID:                testAgent,
		RuntimeCompatibilityID: string(testCompat),
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       testEndpoint,
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             uint64(residency.LeaseEpoch),
		ObservedAt:             testClockAt,
		ExpiresAt:              testClockAt.Add(testExpiry),
	}
	if published[1] != want {
		t.Errorf("the resident projection is\n  %+v\nwant\n  %+v", published[1], want)
	}
	// Core is the authority on what a reader will accept, so the record is
	// validated by Core rather than by this test's idea of one.
	for i, observation := range published {
		if err := observation.Validate(); err != nil {
			t.Errorf("projection %d is one Core refuses: %v", i, err)
		}
	}
	if epoch := published[1].LeaseEpoch; epoch == 0 || epoch != uint64(residency.LeaseEpoch) {
		t.Errorf("the projection carries lease epoch %d, want the residency's non-zero %d", epoch, residency.LeaseEpoch)
	}
}

// ---------------------------------------------------------------------------
// Refusals that take nothing
// ---------------------------------------------------------------------------

func TestAttachRefusesAnInvalidRequestBeforeTakingAnything(t *testing.T) {
	oversized := ActorID(strings.Repeat("a", MaxPrincipalFieldBytes+1))
	for _, row := range []struct {
		name     string
		mutate   func(*Request)
		wantCode sessionwire.HostLinkErrorCode
	}{
		{name: "another tenant's session", mutate: func(r *Request) { r.TenantID = "tenant-other"; r.Principal.TenantID = "tenant-other" }},
		{name: "a principal acting for another tenant", mutate: func(r *Request) { r.Principal.TenantID = "tenant-other" }},
		{name: "no session id", mutate: func(r *Request) { r.SessionID = "" }},
		{name: "no agent id", mutate: func(r *Request) { r.AgentID = "" }},
		{name: "an agent this Department does not register", mutate: func(r *Request) { r.AgentID = "reviewer" }, wantCode: sessionwire.HostLinkErrorRuntimeUnavailable},
		{name: "a build this Host does not launch", mutate: func(r *Request) { r.CompatibilityID = "rig-2019-01" }, wantCode: sessionwire.HostLinkErrorRuntimeMismatch},
		{name: "no mode", mutate: func(r *Request) { r.Mode = "" }},
		{name: "an unknown mode", mutate: func(r *Request) { r.Mode = "resume" }},
		{name: "no actor", mutate: func(r *Request) { r.Principal.ActorID = "" }},
		{name: "an oversized actor", mutate: func(r *Request) { r.Principal.ActorID = oversized }},
		{name: "an oversized trace", mutate: func(r *Request) { r.Principal.TraceID = TraceID(oversized) }},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t)
			request := f.request(ModeCreate)
			row.mutate(&request)

			_, err := f.manager.Attach(context.Background(), request)
			if err == nil {
				t.Fatal("the invalid request was accepted")
			}
			var attach *AttachError
			if !errors.As(err, &attach) {
				t.Fatalf("error is %T, want *AttachError", err)
			}
			if attach.Step != StepValidate {
				t.Errorf("the refusal names step %q, want %q", attach.Step, StepValidate)
			}
			if attach.Code != row.wantCode {
				t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, row.wantCode)
			}
			// NOTHING was taken: validation precedes the ledger and the lease.
			if steps := f.trace.recorded(); len(steps) != 0 {
				t.Errorf("a refused request reached %v; step 1 precedes every collaborator", steps)
			}
			f.assertNothingHeld(t)
			f.assertRouteRemoved(t)
		})
	}
}

func TestADedicatedHostRefusesASessionItIsNotBoundTo(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.fixedSession = testSession
		f.target.capabilities.AdmissionWeight = 1
	})

	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
		t.Fatalf("the dedicated Host refused the session it is bound to: %v", err)
	}
	before := f.trace.recorded()

	_, err := f.manager.Attach(context.Background(), f.requestFor("session-not-mine", ModeCreate))
	if err == nil {
		t.Fatal("a dedicated Host accepted a session it is not bound to")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepValidate {
		t.Errorf("the refusal names step %q, want %q", attach.Step, StepValidate)
	}
	if attach.Code != sessionwire.HostLinkErrorRuntimeMismatch {
		t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, sessionwire.HostLinkErrorRuntimeMismatch)
	}
	requireSteps(t, f.trace.recorded(), before)
}

func TestAWorkspaceIsMaterializedOnlyWhenTheTargetRequiresOne(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.target.capabilities.RequiresWorkspace = false })

	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if at := f.trace.indexOf("workspace.ensure"); at >= 0 {
		t.Errorf("a workspace was materialized at %d for a target that declares it needs none", at)
	}
	creates := f.target.createRequests()
	if len(creates) != 1 {
		t.Fatalf("%d creates, want 1", len(creates))
	}
	if creates[0].WorkspaceRoot != "" {
		t.Errorf("the launch carries workspace root %q, want none", creates[0].WorkspaceRoot)
	}
	// The other direction is the default fixture, and it is asserted in the
	// sequence test: without that row this one would pass for a manager that
	// never materialized a workspace at all.
}

// ---------------------------------------------------------------------------
// The admission ledger is service's, not a second one
// ---------------------------------------------------------------------------

// The Host's ONE admission ledger satisfies this package's Admissions as
// written. internal/service names O6.1/O6.3 as required consumers of that
// ledger rather than builders of a second one, and this is the compile-time
// half of honouring it.
var _ Admissions = (*service.CapacityPublisher)(nil)

// TestTheAdmissionLedgerIsTheOneServiceAlreadyOwns is the behavioural half.
//
// The compile-time assertion above proves the SHAPE fits. It does not prove the
// manager calls it: a manager that admitted into a map of its own would satisfy
// the interface and pass every other test in this file, because nothing else
// reads the ledger's own capacity arithmetic. Here the Host's capacity is
// exhausted through real attaches and the refusal comes from the real ledger.
func TestTheAdmissionLedgerIsTheOneServiceAlreadyOwns(t *testing.T) {
	f := newFixture(t)
	for _, session := range []sessionwire.SessionID{"session-a", "session-b"} {
		if _, err := f.manager.Attach(context.Background(), f.requestFor(session, ModeCreate)); err != nil {
			t.Fatalf("attaching %s: %v", session, err)
		}
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != 2*testWeight {
		t.Fatalf("the ledger charges %d after two attaches of weight %d, want %d", consumed, testWeight, 2*testWeight)
	}

	// Capacity 8, weight 3: the third session does not fit, and the ledger is
	// the only thing that knows it.
	_, err := f.manager.Attach(context.Background(), f.requestFor("session-c", ModeCreate))
	if err == nil {
		t.Fatal("a third session was admitted past this Host's capacity")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Code != sessionwire.HostLinkErrorNoCapacity {
		t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, sessionwire.HostLinkErrorNoCapacity)
	}
	var refused *service.AdmissionRefusedError
	if !errors.As(err, &refused) {
		t.Errorf("the refusal does not unwrap to service's own *AdmissionRefusedError, so it did not come from the Host's ledger: %v", err)
	}
	if consumed := f.publisher.ConsumedWeight(); consumed != 2*testWeight {
		t.Errorf("the ledger charges %d after a refused attach, want the %d still resident", consumed, 2*testWeight)
	}
}

// ---------------------------------------------------------------------------
// The real embedded rig target
// ---------------------------------------------------------------------------

func rigTarget(t *testing.T, session department.RigSession, capabilities department.Capabilities) department.LaunchTarget {
	t.Helper()
	target, err := department.NewRigTarget(&testkit.FakeRig{Session: session}, testCompat, capabilities)
	if err != nil {
		t.Fatalf("department.NewRigTarget: %v", err)
	}
	return target
}

func testCapabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		RequiresWorkspace: true,
		AdmissionWeight:   testWeight,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

func TestAttachDrivesTheRealEmbeddedRigTarget(t *testing.T) {
	session := testkit.NewFullSession(testRigSessionID)
	f := newFixture(t, func(f *fixture) {
		f.targetOverride = rigTarget(t, session, testCapabilities())
	})

	residency, err := f.manager.Attach(context.Background(), f.request(ModeRestore))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if residency.Runtime == nil {
		t.Fatal("no runtime was reported")
	}
	if residency.Runtime.SessionID() != testSession {
		t.Errorf("the adapted runtime reports session %q, want %q", residency.Runtime.SessionID(), testSession)
	}
	rig, ok := department.RigSessionID(residency.Runtime)
	if !ok || rig != testRigSessionID {
		t.Errorf("the adapted runtime reports Harness identity (%v, %t), want (%v, true)", rig, ok, testRigSessionID)
	}
	if released := session.Released(); released != 0 {
		t.Errorf("a successful attach released residency %d time(s)", released)
	}
}

// TestARuntimeWithoutNonterminalReleaseIsRefusedBeforeInstallation is Host's
// answer to the state H4.1 actually shipped.
//
// H4.1 exported the interfaces and H4.2 supplies the nonterminal release, so a
// live Harness session satisfies WaitIdle and Done today and returns ok ==
// false on a Releaser assertion. HOST'S DECISION IS ALREADY MADE, AND IT IS
// STRUCTURAL RATHER THAN A POLICY THIS PACKAGE PICKED: department.Runtime
// EMBEDS Releaser, so a value that does not satisfy it is not a Runtime, and
// O1.2's adapter refuses to produce one — before publication, naming every
// missing capability. There is therefore no code path on which this manager
// holds a runtime it cannot release nonterminally, and no place for it to
// invent what a release means. The attach is refused, the reason is recorded,
// and everything taken so far is released.
//
// The alternative, releasing terminally with a recorded reason, was NOT taken:
// Shutdown durably appends SessionStopped, so a Host that could not attach
// would end a session that is still resumable, and it would do so by inventing
// H4.2's answer one repository early.
func TestARuntimeWithoutNonterminalReleaseIsRefusedBeforeInstallation(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.targetOverride = rigTarget(t, testkit.NewSessionWithout(testRigSessionID, testkit.CapabilityReleaser), testCapabilities())
	})

	_, err := f.manager.Attach(context.Background(), f.request(ModeRestore))
	if err == nil {
		t.Fatal("a session that cannot be released nonterminally was attached")
	}
	var incapable *department.IncapableRuntimeError
	if !errors.As(err, &incapable) {
		t.Fatalf("error %v does not unwrap to *department.IncapableRuntimeError", err)
	}
	if len(incapable.Missing) != 1 || incapable.Missing[0] != "Releaser" {
		t.Errorf("the refusal names missing capabilities %v, want exactly [Releaser]", incapable.Missing)
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepHydrate {
		t.Errorf("the refusal names step %q, want %q", attach.Step, StepHydrate)
	}
	// NO HostLink code, deliberately: see launchCode. Every Host running this
	// build fails identically, so re-running placement is a stampede.
	if attach.Code != "" {
		t.Errorf("the refusal carries HostLink code %q, want none", attach.Code)
	}
	if at := f.trace.indexOf("registry.insert"); at >= 0 {
		t.Errorf("the incapable runtime reached the registry at %d", at)
	}
	f.assertNothingHeld(t)
	f.assertRouteRemoved(t)
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewManagerRefusesAnIncompleteConfiguration(t *testing.T) {
	f := newFixture(t)
	complete := Options{
		Host:           f.host,
		HostGeneration: testGeneration,
		Registry:       f.registry,
		Admissions:     f.admissions,
		Leases:         f.leases,
		Journal:        f.journal,
		Durable:        f.durable,
		Workspaces:     f.workspaces,
		Locations:      f.locations,
		Ownership:      f.ownership,
	}
	// The control: the complete configuration is accepted, so every row below
	// fails for the field it removes rather than for something already missing.
	accepted, err := NewManager(complete)
	if err != nil {
		t.Fatalf("the complete configuration was refused: %v", err)
	}
	accepted.Close()

	for _, row := range []struct {
		field  string
		remove func(*Options)
	}{
		{"Host", func(o *Options) { o.Host = nil }},
		{"HostGeneration", func(o *Options) { o.HostGeneration = 0 }},
		{"Registry", func(o *Options) { o.Registry = nil }},
		{"Admissions", func(o *Options) { o.Admissions = nil }},
		{"Leases", func(o *Options) { o.Leases = nil }},
		{"Journal", func(o *Options) { o.Journal = nil }},
		{"Durable", func(o *Options) { o.Durable = nil }},
		{"Workspaces", func(o *Options) { o.Workspaces = nil }},
		{"Locations", func(o *Options) { o.Locations = nil }},
		{"Ownership", func(o *Options) { o.Ownership = nil }},
	} {
		t.Run(row.field, func(t *testing.T) {
			options := complete
			row.remove(&options)
			manager, err := NewManager(options)
			if err == nil {
				manager.Close()
				t.Fatalf("a configuration without %s was accepted", row.field)
			}
			var invalid *InvalidManagerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("error is %T, want *InvalidManagerOptionsError", err)
			}
			if invalid.Field != row.field {
				t.Errorf("the refusal names field %q, want %q", invalid.Field, row.field)
			}
			if manager != nil {
				t.Error("a refused configuration returned a Manager; there is no partially valid one")
			}
		})
	}
}

// TestTheSessionRecordHoldsWhatO32WillNeed is a WHITE-BOX assertion, and it is
// here because the alternative is worse.
//
// sessionRecord's generation and ownership handle are written by an attach and
// read by nobody in this task: O3.2's heartbeat and O6.1's release are what
// consume them. A field nothing reads is a claim nothing checks — the previous
// task lost four guards to exactly that shape — so rather than leave them
// unprobed, or discard the handle and leave O3.2 with no way to stop what this
// package started, the record is asserted directly.
func TestTheSessionRecordHoldsWhatO32WillNeed(t *testing.T) {
	f := newFixture(t)
	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	f.manager.mu.Lock()
	record, held := f.manager.sessions[f.key()]
	f.manager.mu.Unlock()
	if !held {
		t.Fatal("no session record was kept for an attached residency")
	}
	if record.generation != residency.Generation {
		t.Errorf("the record holds generation %d and the residency reports %d; a late caller must not be able to act on a residency that has been replaced", record.generation, residency.Generation)
	}
	if record.ownership == nil {
		t.Fatal("the record holds no ownership handle, so nothing this attach started could ever be stopped")
	}
	started := f.ownership.startedRequests()
	if len(started) != 1 {
		t.Fatalf("%d ownerships were started, want 1", len(started))
	}
	if started[0].Generation != residency.Generation {
		t.Errorf("ownership was begun for generation %d, want %d", started[0].Generation, residency.Generation)
	}
	if started[0].LeaseEpoch != residency.LeaseEpoch || started[0].Runtime != residency.Runtime {
		t.Errorf("ownership was begun for epoch %d and a %T, want epoch %d and the residency's runtime", started[0].LeaseEpoch, started[0].Runtime, residency.LeaseEpoch)
	}
	if record.cancel == nil {
		t.Error("the record holds no cancel, so the session lifetime has no owner")
	}
}

// ---------------------------------------------------------------------------
// The 6-to-9 window
// ---------------------------------------------------------------------------

// TestNothingIsReportableAsAttachedInsideTheInstallWindow is the deterministic
// half of the guard over a live escape.
//
// registry.Insert marks an entry resident and accepting at step 6, and the
// attach can still fail at 7, 8 or 9 and take it away again. A fast path gated
// on the REGISTRY therefore returned success, carrying the winner's runtime, to
// a caller arriving inside that window — and when the winner then rolled back,
// that caller was left holding a Runtime whose ReleaseResidency had already
// been called and whose lease was gone. The erroring call took nothing; the
// SUCCEEDING one held a corpse.
//
// WHAT THIS TEST COVERS AND WHAT IT DOES NOT: it pins the PREDICATE — inside
// the window the registry says resident and attachedResidency says not
// attached, so the two are observably different things. The CALL SITE is pinned
// twice over: behaviourally by
// TestASecondAttachCannotReturnFromInsideTheInstallWindow, and structurally by
// TestAttachReadsResidencyOnlyThroughTheAttachedPredicate, which additionally
// refuses any OTHER direct registry read creeping into Attach.
func TestNothingIsReportableAsAttachedInsideTheInstallWindow(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.ownership.err = errors.New("injected")
	})
	var (
		registrySaysResident  bool
		predicateSaysAttached bool
		sessionContextLive    bool
	)
	f.ownership.inWindow = func() {
		_, registrySaysResident = f.registry.Get(f.key())
		_, predicateSaysAttached = f.manager.attachedResidency(f.key())
		_, sessionContextLive = f.manager.SessionContext(f.key())
	}

	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err == nil {
		t.Fatal("the injected ownership failure was not reported")
	}
	if !registrySaysResident {
		t.Fatal("the registry did not hold the entry inside the window, so this test never reached its subject")
	}
	if predicateSaysAttached {
		t.Error("the predicate the fast path uses reported ATTACHED inside the install window; a caller taking that path would be handed a runtime this attach is about to release")
	}
	if sessionContextLive {
		t.Error("a session context was reportable inside the install window")
	}
	f.assertNothingHeld(t)
	f.assertRouteRemoved(t)
}

// TestAttachReadsResidencyOnlyThroughTheAttachedPredicate states the rule over
// the CODE: Attach reads residency through attachedResidency and reaches the
// registry through nothing else.
//
// IT IS NOT THE BEHAVIOURAL GUARD, and an earlier version of this comment
// claimed no behavioural guard was possible — that a second Attach in the
// install window either coin-flips or deadlocks the passing case. That was true
// of one technique and false of the space:
// TestASecondAttachCannotReturnFromInsideTheInstallWindow decides it by making
// PARKING observable, so exactly one of parked-or-returned happens and a select
// between them is total. What this one adds is different and worth keeping: the
// behavioural guard pins the ONE read it drives, and this pins that NO OTHER
// direct registry read creeps into Attach — including on paths no test reaches.
func TestAttachReadsResidencyOnlyThroughTheAttachedPredicate(t *testing.T) {
	files := parseProductionFiles(t)
	examined := 0
	for name, file := range files {
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Name.Name != "Attach" {
				continue
			}
			examined++
			for _, reached := range registryFieldSelections(function) {
				t.Errorf("%s: Attach reads m.registry.%s directly; a registry entry exists from step 6 and may still be withdrawn, so residency must be read through attachedResidency", name, reached)
			}
		}
	}
	if examined != 1 {
		t.Fatalf("%d functions named Attach were examined, want exactly 1; this guard is vacuous otherwise", examined)
	}

	// The detector, on a source that breaks the rule, and a control one
	// position over that must stay clean.
	for _, probe := range []struct {
		name   string
		source string
		want   int
	}{
		{
			name:   "a direct registry read in Attach",
			source: "package p\ntype M struct{ registry any }\nfunc (m *M) Attach() { m.registry.Get(1) }\n",
			want:   1,
		},
		{
			name:   "control: the same read in a different method",
			source: "package p\ntype M struct{ registry any }\nfunc (m *M) attachedResidency() { m.registry.Get(1) }\nfunc (m *M) Attach() { m.attachedResidency() }\n",
			want:   0,
		},
	} {
		parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", probe.source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parsing the probe: %v", probe.name, err)
		}
		found := 0
		for _, declaration := range parsed.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "Attach" {
				found += len(registryFieldSelections(function))
			}
		}
		if found != probe.want {
			t.Errorf("%s: the detector reported %d direct registry reads, want %d", probe.name, found, probe.want)
		}
	}
}

// registryFieldSelections returns the methods called on the receiver's registry
// field inside one function.
func registryFieldSelections(function *ast.FuncDecl) []string {
	var reached []string
	ast.Inspect(function, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		method, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector {
			return true
		}
		field, isField := method.X.(*ast.SelectorExpr)
		if !isField || field.Sel.Name != "registry" {
			return true
		}
		if receiver, isIdent := field.X.(*ast.Ident); isIdent && receiver.Name == "m" {
			reached = append(reached, method.Sel.Name)
		}
		return true
	})
	return reached
}

// TestNoSuccessfulAttachEverReportsAReleasedRuntime is the end-to-end form of
// the same property, and it is the universal invariant rather than a race the
// test has to win: EVERY attach that returns nil must report a live runtime
// with running ownership, whichever branch it took.
//
// It drives a second Attach into the install window through the ownership hook
// while the winner is failing at step 9, and asserts the invariant whichever
// branch that second call took. The DECIDABLE form of the same window is
// TestASecondAttachCannotReturnFromInsideTheInstallWindow; this one is the
// end-to-end statement of what a caller is guaranteed.
func TestNoSuccessfulAttachEverReportsAReleasedRuntime(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.locations.publishErr = errors.New("injected")
		f.locations.failOnCall = 2
	})

	type outcome struct {
		residency Residency
		err       error
	}
	second := make(chan outcome, 1)
	f.ownership.inWindow = func() {
		go func() {
			residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
			second <- outcome{residency, err}
		}()
	}

	_, winnerErr := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if winnerErr == nil {
		t.Fatal("the winner's injected step 9 failure was not reported")
	}
	result := <-second

	if result.err != nil {
		// A refusal is a perfectly good outcome here; there is nothing to check.
		return
	}
	if result.residency.Runtime == nil {
		t.Fatal("a successful attach reported no runtime")
	}
	for _, runtime := range f.target.producedRuntimes() {
		if department.Runtime(runtime) != result.residency.Runtime {
			continue
		}
		if released, _ := runtime.counts(); released != 0 {
			t.Errorf("a successful attach reported a runtime whose ReleaseResidency has been called %d time(s): the caller was handed a corpse", released)
		}
	}
	if _, live := f.manager.SessionContext(result.residency.Key); !live {
		t.Error("a successful attach left no session context, so it did not reach step 9")
	}
	if running := f.ownership.runningCount(); running == 0 {
		t.Error("a successful attach reported a residency with no ownership running")
	}
}

// ---------------------------------------------------------------------------
// The warm path is validated like the cold one
// ---------------------------------------------------------------------------

// TestAResidentSessionIsValidatedLikeAColdOne is the THIRD direction of the
// idempotency rule, and the one that had no row.
//
// The first two are "the same request returns the same residency" and "a
// different agent is refused". The third is every OTHER rule step 1 holds: a
// caller naming an unknown mode, no actor, or a runtime build this Host does
// not launch was refused cold and ACCEPTED WARM, because the fast path ran
// before validation and checked only the agent.
func TestAResidentSessionIsValidatedLikeAColdOne(t *testing.T) {
	for _, row := range []struct {
		name     string
		mutate   func(*Request)
		wantCode sessionwire.HostLinkErrorCode
	}{
		{name: "an unknown mode", mutate: func(r *Request) { r.Mode = "resume" }},
		{name: "no actor", mutate: func(r *Request) { r.Principal.ActorID = "" }},
		{name: "a principal acting for another tenant", mutate: func(r *Request) { r.Principal.TenantID = "tenant-other" }},
		{name: "a build this Host does not launch", mutate: func(r *Request) { r.CompatibilityID = "rig-1999-nonsense" }, wantCode: sessionwire.HostLinkErrorRuntimeMismatch},
		{name: "an agent this Department does not register", mutate: func(r *Request) { r.AgentID = "reviewer" }, wantCode: sessionwire.HostLinkErrorRuntimeUnavailable},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t)
			first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
			if err != nil {
				t.Fatalf("the cold attach failed: %v", err)
			}
			before := f.trace.recorded()

			// The CONTROL: the same request unmutated is still accepted warm,
			// so each row below fails for the field it changes.
			if again, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil || again.Generation != first.Generation {
				t.Fatalf("the unmutated warm attach reported (%+v, %v), want the resident residency", again, err)
			}

			request := f.request(ModeCreate)
			row.mutate(&request)
			_, err = f.manager.Attach(context.Background(), request)
			if err == nil {
				t.Fatal("the invalid request was accepted against a resident session")
			}
			var attach *AttachError
			if !errors.As(err, &attach) {
				t.Fatalf("error is %T, want *AttachError", err)
			}
			if attach.Step != StepValidate {
				t.Errorf("the refusal names step %q, want %q", attach.Step, StepValidate)
			}
			if attach.Code != row.wantCode {
				t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, row.wantCode)
			}
			requireSteps(t, f.trace.recorded(), before)
			if entry, held := f.registry.Get(f.key()); !held || entry.Generation != first.Generation {
				t.Error("the refused warm attach disturbed the resident session")
			}
		})
	}
}

// TestAResidentSessionIsRefusedForARuntimeBuildItIsNotRunning holds that the
// comparison is against the RESIDENT entry rather than against the current
// target, which is the only comparison that means anything once a target has
// been upgraded under a live session.
func TestAResidentSessionIsRefusedForARuntimeBuildItIsNotRunning(t *testing.T) {
	const upgraded department.CompatibilityID = "rig-2027-06-upgraded"
	f := newFixture(t)
	resident, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("the cold attach failed: %v", err)
	}
	// The target is upgraded under the resident session. Its runtime is still
	// the old build, and the registry recorded that.
	f.target.driftFromNow(upgraded)
	f.target.driftAfter = 0
	f.target.compatibility = upgraded

	upgradedPlacement := f.request(ModeCreate)
	upgradedPlacement.CompatibilityID = upgraded
	_, err = f.manager.Attach(context.Background(), upgradedPlacement)
	if err == nil {
		t.Fatal("a caller placed on the upgraded build was accepted onto a runtime built by the old one")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Code != sessionwire.HostLinkErrorRuntimeMismatch {
		t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, sessionwire.HostLinkErrorRuntimeMismatch)
	}

	// THE FOURTH DIRECTION, and the one that had no row: the build the resident
	// runtime was ACTUALLY BUILT BY must be ACCEPTED. Refusing the upgraded
	// value is half a rule, and the missing half is where a regression lived —
	// a warm attach was compared against the CURRENT target as well as against
	// the entry, so after an upgrade the two checks between them refused every
	// non-empty CompatibilityID and the only correct placement was the one that
	// could not be expressed.
	residentPlacement := f.request(ModeCreate)
	residentPlacement.CompatibilityID = testCompat
	warm, err := f.manager.Attach(context.Background(), residentPlacement)
	if err != nil {
		t.Fatalf("a caller placed on the RESIDENT build %q was refused: %v", testCompat, err)
	}
	if warm.Attached {
		t.Error("the warm attach reports Attached true")
	}
	if warm.Generation != resident.Generation {
		t.Errorf("the warm attach reported generation %d, want the resident %d", warm.Generation, resident.Generation)
	}
	if warm.CompatibilityID != testCompat {
		t.Errorf("the warm attach reported build %q, want the resident %q", warm.CompatibilityID, testCompat)
	}
}

// ---------------------------------------------------------------------------
// A rollback that cannot release says so
// ---------------------------------------------------------------------------

// TestARollbackThatCannotReleaseNamesWhatIsStillHeld closes the case the
// package's own guarantee was silent about.
//
// "An Attach that returns an error took nothing that is still held" is false
// when a compensation itself fails, and until this test the failure was
// swallowed: a refused TombstoneResidency leaves a LIVE ROUTE while the attach
// reports failure, so Factory keeps sending work to a Host that owns nothing,
// and nothing anywhere records why. Every knob exercised here existed on the
// fakes and was set by no test, which is what made the gap invisible.
func TestARollbackThatCannotReleaseNamesWhatIsStillHeld(t *testing.T) {
	stuck := errors.New("the store is unreachable")
	f := newFixture(t, func(f *fixture) {
		f.locations.publishErr = errors.New("injected")
		f.locations.failOnCall = 2
		f.locations.tombstoneErr = stuck
		f.ownership.stopErr = stuck
		f.workspaces.releaseErr = stuck
		f.leases.releaseErr = stuck
		f.target.releaseErr = stuck
	})

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the injected step 9 failure was not reported")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepAttached {
		t.Errorf("the failure names step %q, want %q", attach.Step, StepAttached)
	}
	for _, want := range []string{"residency tombstone", "ownership", "runtime residency", "workspace", "session lease"} {
		found := false
		for _, unreleased := range attach.Unreleased {
			if strings.HasPrefix(unreleased, want+": ") {
				found = true
			}
		}
		if !found {
			t.Errorf("the failure does not name %q among what it could not release: %v", want, attach.Unreleased)
		}
	}
	if !strings.Contains(err.Error(), "could not release") {
		t.Errorf("the error text says nothing about what is still held: %q", err.Error())
	}

	// EVERY compensation was attempted, not just the ones before the first
	// failure. Abandoning the rest would turn one leaked resource into five.
	for _, attempted := range []string{"location.tombstone", "ownership.stop", "runtime.release", "workspace.release", "lease.release"} {
		if f.trace.indexOf(attempted) < 0 {
			t.Errorf("the rollback never attempted %q; a compensation that fails must not abandon the ones after it", attempted)
		}
	}
	// And the registry entry, whose removal cannot fail, is gone regardless.
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds a residency after a failed attach")
	}
}

// TestAStaleSessionRecordIsNotReportableAsAttached closes the generation half
// of the fast-path predicate, which nothing else reached.
//
// A SYNTHETIC FIXTURE IS REQUIRED because the subject already satisfies the
// rule on every path this Manager takes: while the key slot is held no attach
// of that key can replace the entry, so a record and its entry never disagree.
// The fixture builds the disagreement directly — the residency is replaced
// under the same key by another writer to the shared registry, leaving this
// Manager's record naming a generation that is gone — and a predicate ignoring
// the generation then reports ATTACHED for a residency this Manager does not
// own, handing a caller a runtime it never launched and would never release.
//
// THE ASSERTED END STATE IS FIXTURE-DEPENDENT AND IS NOT A CLAIM ABOUT
// PRODUCTION. It launches, loses and releases only because the default
// fakeLeases is NON-EXCLUSIVE. Against a real exclusive SessionLeases — which
// the interface promises, and which this Manager still holds the earlier grant
// from — the same path is refused at step 2 with epoch_mismatch and never
// reaches the loser branch at all. What is being pinned here is the PREDICATE:
// that a stale record does not short-circuit. Where the fall-through then ends
// is the lease store's business.
func TestAStaleSessionRecordIsNotReportableAsAttached(t *testing.T) {
	f := newFixture(t)
	first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if !f.registry.inner.RemoveByGeneration(f.key(), first.Generation) {
		t.Fatal("the seeded residency could not be removed")
	}
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	replacement, installed := f.registry.inner.Insert(f.key(), registry.Admission{
		AgentID: testAgent, Target: f.target, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
	})
	if !installed || replacement.Generation == first.Generation {
		t.Fatalf("the replacement was not installed under a new generation (installed=%t, generation=%d)", installed, replacement.Generation)
	}
	launchesBefore := len(f.target.producedRuntimes())

	second, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("second Attach: %v", err)
	}

	if second.Generation != replacement.Generation {
		t.Errorf("the attach reported generation %d, want the residency that actually exists, %d", second.Generation, replacement.Generation)
	}
	// THE DISCRIMINATOR. Reporting the replacement is right either way; what
	// separates a stale record honoured from a stale record ignored is whether
	// the fast path was taken. A predicate that ignored the generation returns
	// straight from the record and launches nothing; the correct one falls
	// through to the slot, launches, loses the registry race and releases.
	launched := f.target.producedRuntimes()
	if len(launched) != launchesBefore+1 {
		t.Fatalf("%d runtimes were launched by the second attach, want 1: a stale record was reported as attached without the key slot ever being taken", len(launched)-launchesBefore)
	}
	if released, shutdowns := launched[len(launched)-1].counts(); released != 1 || shutdowns != 0 {
		t.Errorf("the losing runtime was released %d times and shut down %d times, want exactly one nonterminal release", released, shutdowns)
	}
}

// TestASecondAttachCannotReturnFromInsideTheInstallWindow decides the escape
// F1 named, without a race and without a timing assertion.
//
// The move that makes it decidable is making PARKING OBSERVABLE. A second
// Attach arriving while the winner sits between step 6 and step 9 does exactly
// one of two things — it parks on the key slot, which is correct, or it returns
// from inside the window, which is the escape — so a select between a
// "parked" signal and the call's own result is total. No sleep, no deadline,
// and no deadlock in the passing case, because the passing case SIGNALS before
// it blocks.
//
// An earlier version of this file asserted that this test could not exist. That
// claim was about one technique, not about the space, and the precedent against
// it is in this package already: LocalRegistry is an interface for exactly this
// reason, and its own comment says a coin-flip guard over the bug a change
// exists to fix is not a guard.
func TestASecondAttachCannotReturnFromInsideTheInstallWindow(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		// The winner fails at step 9, so a residency reported from inside the
		// window would be one that is about to be withdrawn.
		f.locations.publishErr = errors.New("injected")
		f.locations.failOnCall = 2
	})

	parked := make(chan struct{}, 1)
	f.manager.mu.Lock()
	f.manager.parked = func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	}
	f.manager.mu.Unlock()

	type outcome struct {
		residency Residency
		err       error
	}
	returned := make(chan outcome, 1)
	f.ownership.inWindow = func() {
		go func() {
			residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
			returned <- outcome{residency, err}
		}()
		select {
		case <-parked:
			// Correct: the second attach is waiting for the slot and will see
			// whatever this attach actually leaves behind.
		case escaped := <-returned:
			t.Errorf("a second Attach RETURNED from inside the install window (attached=%t, err=%v); the winner is about to withdraw that residency, so the caller has been handed a runtime that is about to be released",
				escaped.residency.Attached, escaped.err)
			returned <- escaped
		}
	}

	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err == nil {
		t.Fatal("the winner's injected step 9 failure was not reported")
	}
	second := <-returned
	if second.err == nil {
		// Having waited for the slot, the second attach ran cold and won. Its
		// residency must be a real one, not the withdrawn one.
		if _, live := f.manager.SessionContext(second.residency.Key); !live {
			t.Error("the second attach reported success without reaching step 9")
		}
	}
}

// TestALosingAttachThatCannotReleaseIsNotReportedAsSuccess is F4's rule applied
// on the path beside the one F4 fixed.
//
// Losing the registry race is not a failure — the session is resident and the
// caller is told so. Losing it and then FAILING TO RELEASE is a failure, and
// reporting it as an idempotent success leaves a live runtime and a lease
// nobody will renew while telling the caller the attach was fine and the
// operator nothing at all.
func TestALosingAttachThatCannotReleaseIsNotReportedAsSuccess(t *testing.T) {
	stuck := errors.New("the lease store is unreachable")
	f := newFixture(t, func(f *fixture) {
		f.leases.releaseErr = stuck
		f.target.releaseErr = stuck
	})
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	f.registry.beforeInsert = func() {
		f.registry.inner.Insert(f.key(), registry.Admission{
			AgentID: testAgent, Target: f.target, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
		})
	}

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("a loser that could not release what it took reported an idempotent success")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepInstall {
		t.Errorf("the failure names step %q, want %q", attach.Step, StepInstall)
	}
	for _, want := range []string{"runtime residency", "session lease"} {
		found := false
		for _, unreleased := range attach.Unreleased {
			if strings.HasPrefix(unreleased, want+": ") {
				found = true
			}
		}
		if !found {
			t.Errorf("the failure does not name %q among what it could not release: %v", want, attach.Unreleased)
		}
	}
	// The SHARED half is still the winner's and is still not named, because the
	// loser never had it to give back.
	for _, forbidden := range []string{"admission", "workspace"} {
		for _, unreleased := range attach.Unreleased {
			if strings.HasPrefix(unreleased, forbidden+": ") {
				t.Errorf("the loser reported %q as unreleased; that resource is the winner's and the loser must not touch it", forbidden)
			}
		}
	}
	// The CONTROL: with releases that succeed, the same race is an ordinary
	// idempotent success. Without this row the test above would pass for a
	// manager that failed every loser.
	g := newFixture(t)
	otherRival := &fakeRuntime{trace: g.trace, sessionID: testSession, agentID: testAgent, done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	g.registry.beforeInsert = func() {
		g.registry.inner.Insert(g.key(), registry.Admission{
			AgentID: testAgent, Target: g.target, CompatibilityID: testCompat, Runtime: otherRival, LeaseEpoch: 99,
		})
	}
	if residency, err := g.manager.Attach(context.Background(), g.request(ModeCreate)); err != nil || residency.Attached {
		t.Fatalf("a loser whose releases succeeded reported (attached=%t, %v), want an idempotent success", residency.Attached, err)
	}
}

// ---------------------------------------------------------------------------
// The snapshot is taken inside the slot
// ---------------------------------------------------------------------------

// TestAParkedAttachLaunchesOnTheTargetAsItIsWhenItRuns is B1, and it needs a
// fixture nobody had written: a target that changes its answers while an attach
// is parked behind another one.
//
// A snapshot taken BEFORE the slot is a snapshot of answers the target gave
// before this attach waited. It drives the compatibility comparison, the
// workspace and checkpoint requirements, the launch, the registry entry and
// both §15 projections — so a target that grew a RequiresWorkspace in the
// meantime got a runtime launched with an EMPTY WORKSPACE ROOT and no workspace
// compensation recorded, and a target that changed build published a route for
// a build the Host is not running. The second is snapshotTarget's own stated
// reason for existing, defeated by taking the snapshot on the wrong side of the
// lock.
//
// The winner is made to fail at step 9 so the parked attach runs COLD when it
// wakes; if the winner succeeded the parked call would take the warm path and
// launch nothing, which is a different rule.
func TestAParkedAttachLaunchesOnTheTargetAsItIsWhenItRuns(t *testing.T) {
	const upgraded department.CompatibilityID = "rig-2027-11-v2"
	f := newFixture(t, func(f *fixture) {
		f.locations.publishErr = errors.New("injected")
		f.locations.failOnCall = 2
		// The target starts declaring that it needs NO workspace, so a stale
		// snapshot launches without one.
		f.target.capabilities.RequiresWorkspace = false
	})
	// Both builds must be restorable, so the durable state is created rather
	// than restored and the build comparison does not mask the subject.
	f.durable.state = SessionState{Exists: false, Namespace: testNamespace}

	parked := make(chan struct{}, 1)
	f.manager.mu.Lock()
	f.manager.parked = func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	}
	f.manager.mu.Unlock()

	type outcome struct {
		residency Residency
		err       error
	}
	returned := make(chan outcome, 1)
	f.ownership.inWindow = func() {
		go func() {
			residency, err := f.manager.Attach(context.Background(), f.requestWithoutBuild(ModeCreate))
			returned <- outcome{residency, err}
		}()
		<-parked
		// The parked attach has not read the target yet. Everything it is about
		// to decide must come from these answers, not the ones the winner saw.
		f.target.redeclare(upgraded, department.Capabilities{
			SupportsPooled:    true,
			SupportsDedicated: true,
			RequiresWorkspace: true,
			AdmissionWeight:   testWeight,
			CaptureSafety:     department.CaptureSafetyStreaming,
		})
	}

	if _, err := f.manager.Attach(context.Background(), f.requestWithoutBuild(ModeCreate)); err == nil {
		t.Fatal("the winner's injected step 9 failure was not reported")
	}
	second := <-returned
	if second.err != nil {
		t.Fatalf("the parked attach failed: %v", second.err)
	}

	launched := f.target.createRequests()
	if len(launched) != 2 {
		t.Fatalf("%d launches, want the winner's and the parked attach's", len(launched))
	}
	parkedLaunch := launched[1]
	if parkedLaunch.WorkspaceRoot != testWorkspaceRoot {
		t.Errorf("the parked attach launched with workspace root %q, though the target it launched declares RequiresWorkspace; a stale snapshot both skips the materialization and records no compensation for it", parkedLaunch.WorkspaceRoot)
	}
	if second.residency.CompatibilityID != upgraded {
		t.Errorf("the residency reports build %q while the target launches %q", second.residency.CompatibilityID, upgraded)
	}
	entry, held := f.registry.Get(f.key())
	if !held || entry.CompatibilityID != upgraded {
		t.Errorf("the registry recorded build %q, want %q", entry.CompatibilityID, upgraded)
	}
	for i, observation := range f.locations.live() {
		if department.CompatibilityID(observation.RuntimeCompatibilityID) != upgraded {
			t.Errorf("live projection %d advertises build %q while the Host runs %q: a route for a build it is not running", i, observation.RuntimeCompatibilityID, upgraded)
		}
	}
}

// ---------------------------------------------------------------------------
// Hydration fails closed
// ---------------------------------------------------------------------------

// TestAColdCreateIsLaunchedWithItsDurableNamespace is B2. Every fixture seeded
// Exists:true with a namespace even for create rows, so the genuinely cold
// case — the ONLY case a create is — was never exercised.
func TestAColdCreateIsLaunchedWithItsDurableNamespace(t *testing.T) {
	f := newFixture(t)
	f.durable.state = SessionState{Exists: false, Namespace: testNamespace}

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("a genuinely cold create was refused: %v", err)
	}
	if !residency.Attached {
		t.Error("the cold create reports Attached false")
	}
	launched := f.target.createRequests()
	if len(launched) != 1 {
		t.Fatalf("%d launches, want 1", len(launched))
	}
	if launched[0].Storage.Namespace != testNamespace {
		t.Errorf("the runtime was launched with object namespace %q, want %q; the empty prefix scopes every object the session writes to nothing", launched[0].Storage.Namespace, testNamespace)
	}
}

// ---------------------------------------------------------------------------
// The loser's third exit
// ---------------------------------------------------------------------------

// TestALoserWhoseRivalIsADifferentSessionNamesWhatIsStillHeld is B3.
//
// The loser has THREE exits, not two. Besides "released cleanly, report the
// residency" and "could not release, report that", there is "the residency that
// won is not the one this request describes" — and on that exit the shared
// admission and workspace have already been skipped and are genuinely still
// held. Returning existingResidency's refusal unchanged named step "validate"
// for a failure at step 6, carried no Unreleased, and left an admission charge
// and a materialized workspace behind in silence: the package's own headline
// guarantee, falsified.
//
// Unwinding with shared=true is NOT the remedy. This Manager may be the
// winner's charger — service.Admit is idempotent per (key, agent) — so
// crediting the charge back would uncharge the resident session.
func TestALoserWhoseRivalIsADifferentSessionNamesWhatIsStillHeld(t *testing.T) {
	f := newFixture(t)
	rival := &fakeRuntime{trace: f.trace, sessionID: testSession, agentID: "reviewer", done: make(chan struct{}), journalEpoch: testJournalEpoch, journalHeld: true}
	f.registry.beforeInsert = func() {
		f.registry.inner.Insert(f.key(), registry.Admission{
			AgentID: "reviewer", Target: f.target, CompatibilityID: testCompat, Runtime: rival, LeaseEpoch: 99,
		})
	}

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("losing to a residency this request does not describe was reported as a success")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepInstall {
		t.Errorf("the failure names step %q, want %q; it happened at the registry install, not during validation", attach.Step, StepInstall)
	}
	if attach.Code != sessionwire.HostLinkErrorRuntimeMismatch {
		t.Errorf("the failure carries HostLink code %q, want %q", attach.Code, sessionwire.HostLinkErrorRuntimeMismatch)
	}
	for _, want := range []string{"admission", "workspace"} {
		found := false
		for _, held := range attach.Unreleased {
			if strings.HasPrefix(held, want+": ") {
				found = true
			}
		}
		if !found {
			t.Errorf("the failure does not name %q among what is still held: %v", want, attach.Unreleased)
		}
	}
	// And they ARE still held, which is why they must be named rather than
	// released: the resident session is charged for them.
	if consumed := f.publisher.ConsumedWeight(); consumed != testWeight {
		t.Errorf("the ledger charges %d, want the resident session's %d still charged", consumed, testWeight)
	}
	if ensured, released := f.workspaces.counts(); ensured != 1 || released != 0 {
		t.Errorf("%d workspaces materialized and %d released; that root is the resident runtime's", ensured, released)
	}
	// The loser's OWN resources are gone, as on every other loser exit.
	if held := f.leases.heldCount(); held != 0 {
		t.Errorf("the loser still holds %d lease grant(s)", held)
	}
	for i, runtime := range f.target.producedRuntimes() {
		if released, shutdowns := runtime.counts(); released != 1 || shutdowns != 0 {
			t.Errorf("runtime %d was released %d times and shut down %d times, want one nonterminal release", i, released, shutdowns)
		}
	}
}

// ---------------------------------------------------------------------------
// Shutdown does not disable the rollback
// ---------------------------------------------------------------------------

// TestClosingTheManagerDuringAnAttachStillRollsItBack is N2.
//
// Compensations used to run on the manager root, which Close cancels, so a
// Close arriving mid-attach released nothing and left a live §15 route with no
// owner and a lease nobody would renew. That is §8.3's failure one level up:
// the request context is kept out of the sequence precisely so a rollback
// cannot be disabled by something else ending, and the manager's own lifetime
// is something else ending.
func TestClosingTheManagerDuringAnAttachStillRollsItBack(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.ownership.err = errors.New("injected") })
	f.ownership.inWindow = func() { f.manager.Close() }

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the injected ownership failure was not reported")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if len(attach.Unreleased) != 0 {
		t.Errorf("closing the manager mid-attach left %v unreleased; the rollback must not depend on this process still running", attach.Unreleased)
	}
	f.assertNothingHeld(t)
	f.assertRouteRemoved(t)
}

// TestACompensationThatReportsItDidNothingIsNamed covers the two rungs whose
// result is a bool rather than an error.
//
// A ledger release reporting "this session was not charged" means something
// else credited it back first, which is the double credit that lets a Host
// admit past its capacity; a registry removal reporting the same means the
// residency was replaced under this attach's generation. Both were discarded.
// Neither is reachable through the ordinary fakes, so the ledger's answer is
// forced.
func TestACompensationThatReportsItDidNothingIsNamed(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.ownership.err = errors.New("injected")
		f.afterBuild = func(f *fixture) { f.admissions.refuseRelease = true }
	})

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the injected ownership failure was not reported")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	named := false
	for _, unreleased := range attach.Unreleased {
		if strings.HasPrefix(unreleased, "admission: ") {
			named = true
		}
	}
	if !named {
		t.Errorf("a ledger release reporting that nothing was charged was not named: %v", attach.Unreleased)
	}
	// The CONTROL: with a ledger that credits normally, the same failure names
	// nothing. Without this row the assertion above would pass for a manager
	// that named the admission unconditionally.
	g := newFixture(t, func(g *fixture) { g.ownership.err = errors.New("injected") })
	_, err = g.manager.Attach(context.Background(), g.request(ModeCreate))
	if err == nil {
		t.Fatal("the control's injected ownership failure was not reported")
	}
	var control *AttachError
	if !errors.As(err, &control) {
		t.Fatalf("control error is %T, want *AttachError", err)
	}
	if len(control.Unreleased) != 0 {
		t.Errorf("a clean rollback named %v as unreleased", control.Unreleased)
	}
}

// TestAnAttachThatFinishesOnADrainingHostDoesNotAdvertiseAccepting closes the
// Manager's half of the one-drain-flag rule.
//
// internal/service names two sources of "draining" as the defect — a Host that
// stops accepting in one place while advertising Accepting from the other — and
// the Manager WAS that second place: step 9 passed accepting as a literal true
// while the heartbeat ANDs the entry's flag with the Host's. Admit refuses new
// sessions once draining, but an already-admitted attach spans the lease, the
// journal fence and the whole of hydration, so a drain lands inside it
// routinely rather than by contrivance.
//
// The drain is begun through the REAL ledger, mid-attach, at the one seam that
// puts it between admission and the resident publish.
func TestAnAttachThatFinishesOnADrainingHostDoesNotAdvertiseAccepting(t *testing.T) {
	f := newFixture(t)
	f.registry.afterGet = nil
	f.ownership.inWindow = func() { f.publisher.BeginDrain() }

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if !residency.Attached {
		t.Fatal("the attach did not complete, so this test never reached its subject")
	}

	published := f.locations.publishedAll()
	if len(published) != 2 {
		t.Fatalf("%d observations were published, want the attaching/resident pair", len(published))
	}
	if published[1].Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("the second observation reports %q, want resident", published[1].Residency)
	}
	if published[1].Accepting {
		t.Error("an attach that finished on a draining Host advertised an accepting route; the drain flag has one source and this is a reader of it")
	}

	// The CONTROL, one position over: without the drain the same attach
	// advertises accepting. Without this row the assertion above would pass for
	// a Manager that never advertised accepting at all.
	g := newFixture(t)
	if _, err := g.manager.Attach(context.Background(), g.request(ModeCreate)); err != nil {
		t.Fatalf("the control's Attach: %v", err)
	}
	control := g.locations.publishedAll()
	if len(control) != 2 || !control[1].Accepting {
		t.Errorf("the control published %d observations with resident accepting %t, want 2 and true", len(control), control[len(control)-1].Accepting)
	}
}

// TestASupersededEpochDuringAnAttachStopsTheRollbackTombstoning is the
// Manager's half of the rule the heartbeat has enforced three times.
//
// Step 9's publish is refused for a later epoch — SEEN — and the unwinder then
// wrote the tombstone at that same epoch anyway. The consequence is worse here
// than on the handle: a tombstone is a route REMOVAL, so under any store that
// fences removals less strictly than publishes it takes the SUCCESSOR's route
// away. The heartbeat guards its equivalent by generation; the unwinder had no
// equivalent at all.
func TestASupersededEpochDuringAnAttachStopsTheRollbackTombstoning(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.locations.publishErr = fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
		f.locations.failOnCall = 2
	})

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the refused resident publish was not reported")
	}
	if !errors.Is(err, ErrEpochSuperseded) {
		t.Fatalf("the failure %v does not unwrap to ErrEpochSuperseded, so it was never seen", err)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("the rollback tombstoned at %v after the store said a later epoch had committed; a tombstone is a route removal and this one may be the successor's", tombstones)
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	named := false
	for _, unreleased := range attach.Unreleased {
		if strings.HasPrefix(unreleased, "residency tombstone: ") {
			named = true
		}
	}
	if !named {
		t.Errorf("the failure does not name the tombstone among what it could not write: %v; a route left to expire is an operator's business", attach.Unreleased)
	}
	// Everything ELSE still comes back. Refusing one fenced write must not
	// abandon the rest of the ladder.
	if consumed := f.publisher.ConsumedWeight(); consumed != 0 {
		t.Errorf("the ledger still charges %d", consumed)
	}
	if held := f.leases.heldCount(); held != 0 {
		t.Errorf("%d lease grant(s) are still held", held)
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds the residency")
	}

	// The CONTROL, one position over: the same failure from an UNREACHABLE
	// store does tombstone, because nothing has said the epoch is stale.
	g := newFixture(t, func(g *fixture) {
		g.locations.publishErr = errors.New("dial tcp: connection refused")
		g.locations.failOnCall = 2
	})
	if _, err := g.manager.Attach(context.Background(), g.request(ModeCreate)); err == nil {
		t.Fatal("the control's injected failure was not reported")
	}
	if tombstones := g.locations.tombstones(); len(tombstones) != 1 {
		t.Errorf("the control wrote %d tombstones, want 1: an unreachable store says nothing about who owns the record", len(tombstones))
	}
}

// TestAnAttachWhoseLeaseClosesMidWayDoesNotReportItselfAttached closes the half
// of Lease.Lost() that O3.1 disclosed and O3.2 closed only for the heartbeat.
//
// Step 4 of the runbook says lease loss stops admission; the Manager is the
// other place admission is granted, and it never consulted Lost() at all — so a
// grant closing mid-attach still ended in an accepting route published under a
// lease this Host does not hold, with Attached: true returned to the caller.
func TestAnAttachWhoseLeaseClosesMidWayDoesNotReportItselfAttached(t *testing.T) {
	f := newFixture(t)
	f.ownership.inWindow = func() {
		f.leases.mu.Lock()
		granted := append([]*fakeLease(nil), f.leases.granted...)
		f.leases.mu.Unlock()
		if len(granted) != 1 {
			t.Errorf("%d leases were granted, want 1", len(granted))
			return
		}
		close(granted[0].lost)
	}

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatalf("an attach whose lease closed mid-way reported itself attached: %+v", residency)
	}
	if !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("the failure %v does not unwrap to ErrLeaseNotHeld", err)
	}
	for _, observation := range f.locations.publishedAll() {
		if observation.Accepting {
			t.Errorf("an accepting route was published under a lease this Host does not hold: %+v", observation)
		}
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("the rollback tombstoned at %v under a lost grant", tombstones)
	}
	if held := f.leases.heldCount(); held != 0 {
		t.Errorf("%d lease grant(s) are still held", held)
	}
	if _, held := f.registry.Get(f.key()); held {
		t.Error("the local registry still holds a residency whose lease is gone")
	}
}

// TestAJournalFenceRefusedByALaterOwnerEndsTheAttach is §10.1's OTHER fencing
// mechanism, which had no typed error at all and so could not be classified
// even in principle.
//
// The journal is fenced by an in-stream ownership record rather than an epoch
// column, and a successor's committed fence makes this writer's sequence
// permanently stale — "even if it has not observed Lease.Lost()". That is the
// same fact ErrEpochSuperseded carries, arriving through the mechanism nobody's
// frame included.
func TestAJournalFenceRefusedByALaterOwnerEndsTheAttach(t *testing.T) {
	f := newFixture(t, func(f *fixture) {
		f.journal.err = fmt.Errorf("sessionstore: %w", ErrFenceConflict)
	})

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the refused journal fence was not reported")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepFence {
		t.Errorf("the failure names step %q, want %q", attach.Step, StepFence)
	}
	if !errors.Is(err, ErrFenceConflict) {
		t.Errorf("the failure %v does not unwrap to ErrFenceConflict", err)
	}
	f.assertNothingHeld(t)
	f.assertRouteRemoved(t)

	// THE DISCRIMINATOR: a fence refused by a LATER OWNER must be classified,
	// so nothing later in this attach writes under the stale epoch. Reaching a
	// tombstone at all would mean the classification was dropped.
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("a tombstone was written at %v after a later owner's fence refused this one", tombstones)
	}
}

// TestTheAttachAndTheHeartbeatPublishTheSameFunctionOfTheSameState is B2.
//
// The two writers share the record and their epoch, so the store cannot order
// them; what keeps them from contradicting each other is publishing the SAME
// FUNCTION of the SAME STATE. Step 9 used to build `resident` as a literal and
// accepting as !Draining(), while the beat builds residencyOf(entry.State) and
// entry.Accepting && !Draining() — so a residency moved to releasing between
// step 8 and step 9 had that state overwritten with resident and accepting.
//
// The registry is moved through the inWindow seam, which is by construction
// between the install and the resident publish.
func TestTheAttachAndTheHeartbeatPublishTheSameFunctionOfTheSameState(t *testing.T) {
	f := newFixture(t)
	f.ownership.inWindow = func() {
		entry, held := f.registry.Get(f.key())
		if !held {
			t.Error("no residency inside the install window")
			return
		}
		f.registry.inner.MarkReleasing(f.key(), entry.Generation)
	}

	if _, err := f.manager.Attach(context.Background(), f.request(ModeCreate)); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	published := f.locations.publishedAll()
	last := published[len(published)-1]
	entry, held := f.registry.Get(f.key())
	if !held {
		t.Fatal("the residency is gone")
	}
	if last.Accepting != entry.Accepting {
		t.Errorf("step 9 published Accepting %t while the registry says %t", last.Accepting, entry.Accepting)
	}
	if entry.State == registry.StateReleasing && last.Residency != sessionwire.SessionResidencyReleasing {
		t.Errorf("step 9 published residency %q while the registry says %q; the two writers must publish the same function of the same state", last.Residency, entry.State)
	}

	// The CONTROL, one position over: with the registry left alone the same
	// attach publishes resident and accepting. Without it these assertions
	// would pass for a Manager that always published releasing.
	g := newFixture(t)
	if _, err := g.manager.Attach(context.Background(), g.request(ModeCreate)); err != nil {
		t.Fatalf("the control's Attach: %v", err)
	}
	control := g.locations.publishedAll()
	final := control[len(control)-1]
	if final.Residency != sessionwire.SessionResidencyResident || !final.Accepting {
		t.Errorf("the control published (%q, accepting %t), want resident and true", final.Residency, final.Accepting)
	}
}

// TestTheHeartbeatsFenceIsTheAttachsFence is the shared-fence hole, which an
// earlier disclosure called equivalent on a reason that was false.
//
// The heartbeat starts at step 8 and TWO fenced writes follow it inside the
// same attach: step 9's publish and the unwinder's tombstone. So a beat refused
// with ErrEpochSuperseded ends the heartbeat's fence WITHOUT closing Lost() —
// which is the case the fence exists for — and with a fence of its own the
// Manager knows nothing and tombstones at an epoch a successor has superseded.
// A tombstone is a route removal, and the route may be the successor's.
//
// The hook stands in for that beat exactly: one refused write through the very
// object a real heartbeat starting here is handed.
func TestTheHeartbeatsFenceIsTheAttachsFence(t *testing.T) {
	f := newFixture(t)
	f.ownership.withRequest = func(request OwnershipRequest) {
		if request.Fence == nil {
			t.Error("ownership was begun with no fence")
			return
		}
		_ = request.Fence.write(func() error {
			return fmt.Errorf("sessionstore: %w", ErrEpochSuperseded)
		})
	}

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("the attach reported success although its grant had been superseded by a write the heartbeat made")
	}
	if !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("the failure %v does not unwrap to ErrLeaseNotHeld; the heartbeat's refusal must be the Manager's too", err)
	}
	if tombstones := f.locations.tombstones(); len(tombstones) != 0 {
		t.Errorf("the rollback tombstoned at %v under an epoch a successor has superseded", tombstones)
	}
	// Lost() is OPEN throughout, which is the point: this is the path that
	// closes no channel, and a fence that only watched the channel would have
	// let both later writes through.
	f.leases.mu.Lock()
	granted := append([]*fakeLease(nil), f.leases.granted...)
	f.leases.mu.Unlock()
	if len(granted) != 1 {
		t.Fatalf("%d leases were granted, want 1", len(granted))
	}
	select {
	case <-granted[0].lost:
		t.Fatal("the lease reported itself lost, so this test did not exercise the path that closes no channel")
	default:
	}

	// THIS ROW ASSERTS EVERYTHING assertNothingHeld ASSERTS, and swaps only the
	// two clauses about the route. assertRouteRemoved is the pair that cannot
	// hold here — refusing the tombstone is the correct act, so the route is
	// deliberately left to expire — and the naming assertion below stands in
	// for them. An earlier version skipped the whole helper for that reason and
	// silently dropped seven checks this row passes: the watcher count, the
	// runtime released-once and never-shut-down counts, the workspace balance,
	// the launched-context cancellation, and the rollback-on-a-dead-context
	// count. A hand-rolled substitute is not the same thing as the helper, and
	// the difference is invisible until somebody looks.
	f.assertNothingHeld(t)
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	named := false
	for _, unreleased := range attach.Unreleased {
		if strings.HasPrefix(unreleased, "residency tombstone: ") {
			named = true
		}
	}
	if !named {
		t.Errorf("a route was left live and unnamed: %v; §18.2 leaves it to registry expiry, and an operator must be told which", attach.Unreleased)
	}
	if _, live := f.manager.SessionContext(f.key()); live {
		t.Error("a session context survives a failed attach")
	}
}

// TestAResidencyRemovedBeforeStep9IsNotReportedAttached pins the branch B2's
// fix introduced: step 9 re-reads the registry, so it can find the residency
// gone or replaced.
func TestAResidencyRemovedBeforeStep9IsNotReportedAttached(t *testing.T) {
	f := newFixture(t)
	f.ownership.inWindow = func() {
		entry, held := f.registry.Get(f.key())
		if !held {
			t.Error("no residency inside the install window")
			return
		}
		f.registry.inner.RemoveByGeneration(f.key(), entry.Generation)
	}

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err == nil {
		t.Fatal("an attach whose residency vanished before step 9 reported itself attached")
	}
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("error is %T, want *AttachError", err)
	}
	if attach.Step != StepAttached {
		t.Errorf("the failure names step %q, want %q", attach.Step, StepAttached)
	}
	// The entry-removal compensation cannot remove what is already gone, and it
	// says so rather than reporting a clean rollback.
	named := false
	for _, unreleased := range attach.Unreleased {
		if strings.HasPrefix(unreleased, "registry entry: ") {
			named = true
		}
	}
	if !named {
		t.Errorf("the rollback does not name the registry entry among what it could not release: %v", attach.Unreleased)
	}
	if _, live := f.manager.SessionContext(f.key()); live {
		t.Error("a session context survives an attach that never reached step 9")
	}
}

// ---------------------------------------------------------------------------
// O3.4: the two grants
// ---------------------------------------------------------------------------

// TestTheTwoEpochDomainsAreDistinctTypes pins the MECHANISM that keeps the two
// grants apart, which is the thing a value assertion cannot hold on its own.
//
// IT IS HERE BECAUSE THE VALUES AGREE BY ACCIDENT ALL THE TIME. Two fresh
// per-session counters under a memory backend both start at 1, so an attach that
// fed Host's residency epoch to a journal reader produced the right number and
// every test passed — that is exactly how the fusion survived twelve tasks.
// Separate defined types make the substitution a COMPILE failure, which no
// coincidence of initial conditions can mask, and the fake seeding below makes it
// a value failure as well. Neither alone is enough: a maintainer collapsing both
// types to uint64 "for simplicity" defeats the compiler, and this test is what
// then fails.
func TestTheTwoEpochDomainsAreDistinctTypes(t *testing.T) {
	residencyType := reflect.TypeFor[ResidencyEpoch]()
	journalType := reflect.TypeFor[JournalEpoch]()

	if residencyType == journalType {
		t.Fatal("ResidencyEpoch and JournalEpoch are the same type, so nothing stops one being used as the other")
	}
	if residencyType.AssignableTo(journalType) || journalType.AssignableTo(residencyType) {
		t.Error("the two epoch types are assignable to one another, so the confusion O3.4 exists to prevent compiles silently")
	}
	// THE CONTROL: they are distinct because they are DEFINED types, not because
	// their kinds differ. Both are uint64 underneath and are meant to be, so a
	// test asserting "different kinds" would be asserting the wrong thing.
	if residencyType.Kind() != reflect.Uint64 || journalType.Kind() != reflect.Uint64 {
		t.Errorf("the epoch kinds are %v and %v, want both uint64", residencyType.Kind(), journalType.Kind())
	}

	// AND THE PRODUCERS ANSWER IN THE RIGHT DOMAIN. residency.Lease is Host's own
	// grant; the runtime's journal grant reaches Host only through the reported
	// value on Residency, which carries the other type.
	leaseEpoch, ok := reflect.TypeFor[Lease]().MethodByName("Epoch")
	if !ok {
		t.Fatal("Lease has no Epoch method")
	}
	if got := leaseEpoch.Type.Out(0); got != residencyType {
		t.Errorf("Lease.Epoch returns %v, want ResidencyEpoch: a Lease answering in the journal domain is the fusion restated", got)
	}
	reported, ok := reflect.TypeFor[Residency]().FieldByName("JournalEpoch")
	if !ok {
		t.Fatal("Residency has no JournalEpoch field")
	}
	if reported.Type != journalType {
		t.Errorf("Residency.JournalEpoch is %v, want JournalEpoch", reported.Type)
	}
	held, ok := reflect.TypeFor[Residency]().FieldByName("LeaseEpoch")
	if !ok {
		t.Fatal("Residency has no LeaseEpoch field")
	}
	if held.Type != residencyType {
		t.Errorf("Residency.LeaseEpoch is %v, want ResidencyEpoch", held.Type)
	}
}

// TestTheJournalEpochComesFromTheRuntimeAndNotTheLease is the VALUE half, and it
// is the assertion the different fake bases exist for.
//
// The fixture's lease mints 1000 and the fixture's runtime reports 3, so the two
// answers cannot be confused for one another by any reading. A manager that filled
// JournalEpoch from the lease would report 1000; one that filled LeaseEpoch from
// the runtime would report 3. Both are caught, in the same call.
func TestTheJournalEpochComesFromTheRuntimeAndNotTheLease(t *testing.T) {
	f := newFixture(t)

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if residency.LeaseEpoch != testFirstEpoch {
		t.Errorf("the residency epoch is %d, want the lease's %d", residency.LeaseEpoch, testFirstEpoch)
	}
	if residency.JournalEpoch != JournalEpoch(testJournalEpoch) {
		t.Errorf("the journal epoch is %d, want the runtime's %d", residency.JournalEpoch, testJournalEpoch)
	}
	if !residency.JournalEpochHeld {
		t.Error("the runtime holds a journal grant and the residency reports none")
	}
	if runtime := f.target.producedRuntimes(); len(runtime) != 1 {
		t.Fatalf("the target produced %d runtimes, want 1", len(runtime))
	} else if reads := runtime[0].journalEpochReads(); reads == 0 {
		t.Error("the runtime was never asked for its journal grant, so the reported epoch came from somewhere else")
	}
}

// TestARuntimeThatHoldsNoJournalGrantStillAttaches is the OTHER direction, and it
// is a refusal this package deliberately does NOT make.
//
// harness gates its report on the lease still being held and answers (0, false)
// for a headless session, one without persistence, and one simply not wired for
// durable commands. Those are legitimate configurations, so an attach that refused
// them would turn supported deployments into attach failures. What Host must never
// do is substitute its own grant — and the assertion that matters is that the
// residency epoch is UNCHANGED while the journal one is absent.
//
// IT IS ALSO THE CONTROL FOR THE TEST ABOVE. Without a row in which something
// legitimately reports no epoch, "JournalEpochHeld is true" could be satisfied by
// a constant.
func TestARuntimeThatHoldsNoJournalGrantStillAttaches(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.target.noJournalGrant = true })

	residency, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	if err != nil {
		t.Fatalf("Attach refused a runtime that reports no journal grant: %v", err)
	}
	if residency.JournalEpochHeld {
		t.Error("the residency reports a journal grant the runtime says it does not hold")
	}
	if residency.JournalEpoch != 0 {
		t.Errorf("the journal epoch is %d, want 0 when no grant is held", residency.JournalEpoch)
	}
	// THE POINT OF THE WHOLE TASK, stated as an assertion: an absent journal
	// grant does not become Host's residency grant.
	if uint64(residency.JournalEpoch) == uint64(residency.LeaseEpoch) {
		t.Errorf("with no journal grant the residency epoch %d was reported as the journal epoch", residency.LeaseEpoch)
	}
	if residency.LeaseEpoch != testFirstEpoch {
		t.Errorf("the residency epoch is %d, want the lease's %d", residency.LeaseEpoch, testFirstEpoch)
	}
}

// TestARefusedLeaseAcquisitionIsUnwoundByTheAttach is F16(b).
//
// sessionstore's AcquireResidency can return NO GRANT and still leave a provider
// lease and a Store admission held; until a release succeeds the admission is
// retained and Close may time out. (Lease, error) cannot express that, so every
// fake in this module read a non-nil error as nothing acquired and leaked it.
//
// THE OBLIGATION IS OWNED BY THE UNWINDER, and the two rows are what makes that a
// mechanism rather than a call. When the retry succeeds the refusal names nothing
// still held; when it fails, the ladder that reports what it could not give back
// reports this too — which an inline retry could not do, because its failure would
// vanish into the error text.
func TestARefusedLeaseAcquisitionIsUnwoundByTheAttach(t *testing.T) {
	for _, row := range []struct {
		name       string
		cleanupErr error
		unreleased bool
	}{
		{name: "the retry succeeds", cleanupErr: nil, unreleased: false},
		{name: "the retry fails too", cleanupErr: errors.New("the provider is unreachable"), unreleased: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			var retries int
			f := newFixture(t, func(f *fixture) {
				f.leases.err = &LeaseCleanupError{
					Cause: errors.New("the acquisition was cancelled"),
					Cleanup: func(context.Context) error {
						retries++
						return row.cleanupErr
					},
				}
			})

			_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
			var attach *AttachError
			if !errors.As(err, &attach) {
				t.Fatalf("Attach = %v, want *AttachError", err)
			}
			if attach.Step != StepLease {
				t.Errorf("the refusal names step %q, want %q", attach.Step, StepLease)
			}
			if retries != 1 {
				t.Fatalf("the unwinder retried the release %d times, want exactly 1", retries)
			}
			// THE CAUSE SURVIVES EITHER WAY, so a caller can still tell WHY the
			// acquisition was refused.
			var owed *LeaseCleanupError
			if !errors.As(err, &owed) {
				t.Error("the refusal no longer carries the store's cleanup error as its cause")
			}

			named := slices.ContainsFunc(attach.Unreleased, func(held string) bool {
				return strings.Contains(held, "session lease")
			})
			if named != row.unreleased {
				t.Errorf("Unreleased = %v, want it to name the refused lease: %t", attach.Unreleased, row.unreleased)
			}
			if !row.unreleased && len(attach.Unreleased) != 0 {
				t.Errorf("a refusal whose cleanup succeeded still reports %v as held", attach.Unreleased)
			}
		})
	}
}

// A PLAIN REFUSAL OWES NOTHING, which is the control that keeps the test above
// from passing on an unwinder that owns a cleanup for every failed acquisition.
func TestAPlainLeaseRefusalOwesNoRelease(t *testing.T) {
	f := newFixture(t, func(f *fixture) { f.leases.err = errors.New("the lease store is unreachable") })

	_, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
	var attach *AttachError
	if !errors.As(err, &attach) {
		t.Fatalf("Attach = %v, want *AttachError", err)
	}
	if len(attach.Unreleased) != 0 {
		t.Errorf("a refusal that took nothing reports %v as still held", attach.Unreleased)
	}
}
