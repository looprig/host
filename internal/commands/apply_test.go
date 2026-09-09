package commands

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Fixture constants
// ---------------------------------------------------------------------------

const (
	// testHostID is the Host identity newTestHost builds under, and the owner a
	// resumable gate must name.
	testHostID = sessionwire.HostID("host-inbox")

	// testOtherEpoch is a lease epoch that is NOT this applier's and is lower
	// than it, so a claim under it is a predecessor's rather than a successor's.
	testOtherEpoch uint64 = testEpoch - 1

	// testLaterEpoch is a lease epoch later than this applier's.
	testLaterEpoch uint64 = testEpoch + 1

	// testGate is the gate a gate response answers.
	testGate = sessionwire.GateID("gate-permission-1")

	// testClaimTTL and testApplyDeadline mirror newTestHost's options. They are
	// restated because the assertions are about the values the applier derives
	// from the Host, and deriving the expectation from the same accessor would
	// assert nothing.
	testClaimTTL      = 11 * time.Second
	testApplyDeadline = 47 * time.Second

	// testRevision is the revision the fixture's record is read at. It is
	// deliberately neither zero nor one, so a transition that named a constant
	// or forgot to carry the previous CAS's answer is visible.
	testRevision uint64 = 12

	// testEffectSeq and testEffectEventID are the journal effect the fixture's
	// runtime commits.
	testEffectSeq     uint64 = 907
	testEffectEventID        = sessionwire.EventID("event-effect-907")
)

// testRuntimeCommandID is the once-allocated RuntimeCommandID the winning
// acceptance record carries. Nothing in production mints one.
var testRuntimeCommandID = uuid.MustParse("6f1b0f8e-4f1a-4f7e-9b2a-2f3c4d5e6f70")

// testOtherRuntimeCommandID is a DIFFERENT allocation, for the correlation
// checks. A prefix naming it is evidence about another application.
var testOtherRuntimeCommandID = uuid.MustParse("11111111-2222-3333-4444-555555555555")

// errTestRuntime is what the fake runtime fails a command with.
var errTestRuntime = errors.New("commands_test: the runtime refused the command")

// The refusals the tightened fake store answers with, one per precondition the
// released sessionstore actually enforces. They are separate values because a
// test that could not tell a revision conflict from a state conflict could not
// tell a transition that was refused for the right reason from one that was
// refused for any reason at all.
var (
	errTestRevisionConflict = errors.New("commands_test: the expected revision is not the record's")
	errTestStateConflict    = errors.New("commands_test: the record is not in a state this transition is admitted from")
	errTestEpochFenced      = errors.New("commands_test: the epoch is below the record's high-water mark")
	errTestClaimHeld        = errors.New("commands_test: a live claim holds this command")
	errTestClaimLost        = errors.New("commands_test: the claim is not this caller's, or is no longer live")
	errTestDeadlinePassed   = errors.New("commands_test: the apply deadline has passed")
	errTestNoEvidence       = errors.New("commands_test: the journal does not support this settlement")
	errTestUnrevisioned     = errors.New("commands_test: zero is not a revision any provider assigns")
	errTestInvalidResult    = errors.New("commands_test: the terminal result is not usable")
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// storedCommand is one durable inbox record as the fake store holds it: the
// record, its private body, the due_at flag §10.4's terminal writes clear, and
// what the session's journal proves about it.
type storedCommand struct {
	record      Record
	payload     Payload
	due         bool
	application Application
}

// completeCall and rejectCall record one terminal settlement each. They are two
// types because the two settlements are two methods carrying different
// evidence, which is the mechanism behind "applied and rejected are mutually
// exclusive": no value and no call can be both.
type completeCall struct {
	CommandID sessionwire.CommandID
	Revision  uint64
	Epoch     uint64
	Effect    Effect
}

type rejectCall struct {
	CommandID sessionwire.CommandID
	Revision  uint64
	Epoch     uint64
	Detail    sessionwire.ErrorDetail
}

// fakeStore is CommandRecords, Applications, Gates, InboxWrites and
// JournalWrites over one map.
//
// IT ENFORCES THE PRECONDITIONS THE RELEASED STORE ENFORCES, and that is the
// whole reason it is this long. A fake more permissive than the implementation
// it stands for converts a contract violation into a passing test: the previous
// one accepted a terminal write from any non-terminal state and needed no
// result, so a `claimed -> applied` transition that sessionstore v0.6.0 refuses
// outright — CompleteCommand admits only StateApplying, and requires a result
// naming a durable journal event — read as green. Every refusal below is a rule
// in inbox_claim.go, not a rule this test bench invented.
type fakeStore struct {
	mu    sync.Mutex
	clock *manualClock

	commands map[sessionwire.CommandID]*storedCommand
	gates    map[sessionwire.GateID]Gate

	// journalEpoch is the lease epoch the journal writer stamps onto an
	// appended prefix. It is the WRITER's, never the caller's.
	journalEpoch uint64

	// errs maps an operation name to the error it fails with.
	errs map[string]error

	// after maps an operation name to a hook run once it has been recorded. IT
	// IS HOW A GRANT ENDS MID-PROTOCOL: the durable writes of one application
	// are separated by instants, and the fence must be consulted at each of
	// them rather than once at the top.
	after map[string]func()

	// calls is every operation in order, which is what the ordering assertions
	// — payload after claim, prefix before the runtime — are made from.
	calls []string

	claims     []Claim
	applyings  []Applying
	prefixes   []Prefix
	completed  []completeCall
	rejections []rejectCall
}

// newFakeStore returns an empty store on a clock.
func newFakeStore(clock *manualClock) *fakeStore {
	return &fakeStore{
		clock:        clock,
		commands:     map[sessionwire.CommandID]*storedCommand{},
		gates:        map[sessionwire.GateID]Gate{},
		journalEpoch: testEpoch,
		errs:         map[string]error{},
		after:        map[string]func(){},
	}
}

// record notes an operation and returns the scripted error for it.
func (s *fakeStore) record(op string) error {
	s.calls = append(s.calls, op)
	if hook := s.after[op]; hook != nil {
		hook()
	}
	return s.errs[op]
}

// claimLive mirrors the store's own liveness test, against the store's clock.
func (s *fakeStore) claimLive(stored *storedCommand) bool {
	return s.clock.Now().Before(stored.record.ClaimExpiresAt)
}

// LoadCommand returns the durable record.
func (s *fakeStore) LoadCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("LoadCommand"); err != nil {
		return Record{}, err
	}
	stored, ok := s.commands[command]
	if !ok {
		return Record{}, errors.New("commands_test: no such command")
	}
	return stored.record, nil
}

// LoadPayload returns the private payload.
func (s *fakeStore) LoadPayload(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID) (Payload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("LoadPayload"); err != nil {
		return Payload{}, err
	}
	stored, ok := s.commands[command]
	if !ok {
		return Payload{}, errors.New("commands_test: no such command")
	}
	return stored.payload, nil
}

// FindApplication reports what the journal proves about a command.
func (s *fakeStore) FindApplication(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID) (Application, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("FindApplication"); err != nil {
		return Application{}, err
	}
	stored, ok := s.commands[command]
	if !ok {
		return Application{}, errors.New("commands_test: no such command")
	}
	return stored.application, nil
}

// LoadGate returns the durable gate.
func (s *fakeStore) LoadGate(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, gate sessionwire.GateID) (Gate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("LoadGate"); err != nil {
		return Gate{}, false, err
	}
	found, ok := s.gates[gate]
	return found, ok, nil
}

// AppendApplicationPrefix appends the correlation record to the journal.
//
// THE EPOCH IS THE WRITER'S. The caller supplies no lease epoch and could not:
// the released journal writer refuses a record that names one and stamps its
// own grant instead. A prefix whose runtime identity is not the record's breaks
// the durable mapping, which is what the conflicted outcome reports.
func (s *fakeStore) AppendApplicationPrefix(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, prefix Prefix) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("AppendApplicationPrefix"); err != nil {
		return err
	}
	s.prefixes = append(s.prefixes, prefix)
	stored, ok := s.commands[prefix.CommandID]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	stored.application.CommandID = prefix.CommandID
	stored.application.RuntimeCommandID = prefix.RuntimeCommandID
	stored.application.PrefixEpoch = s.journalEpoch
	if prefix.RuntimeCommandID != stored.record.RuntimeCommandID {
		stored.application.Outcome = ApplicationConflicted
		return nil
	}
	// A prefix at the tip with its writer possibly alive is exactly what the
	// journal cannot yet resolve. It becomes committed when the effect lands.
	stored.application.Outcome = ApplicationUnresolved
	return nil
}

// commitEffect is what the RUNTIME does: it appends the public event that
// carried the command's effect, which turns the correlation committed.
func (s *fakeStore) commitEffect(command sessionwire.CommandID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.commands[command]
	if !ok {
		return
	}
	stored.application.Outcome = ApplicationCommitted
	stored.application.EffectEventID = testEffectEventID
	stored.application.EffectSeq = testEffectSeq
}

// ClaimCommand CASes a record into claimed. Its refusals are inbox_claim.go's,
// in the order that file states them.
func (s *fakeStore) ClaimCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, claim Claim) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("ClaimCommand"); err != nil {
		return 0, err
	}
	s.claims = append(s.claims, claim)
	stored, ok := s.commands[command]
	if !ok {
		return 0, errors.New("commands_test: no such command")
	}
	switch {
	// Zero is not a revision any provider assigns, so it cannot be one a caller
	// read, and a compare-and-swap naming it would be unconditional.
	case claim.ExpectedRevision == 0:
		return 0, errTestUnrevisioned
	case stored.record.Revision != claim.ExpectedRevision:
		return 0, errTestRevisionConflict
	case stored.record.State.Terminal():
		return 0, errTestStateConflict
	// An applying command is not claimable AT ANY EPOCH: resuming one is
	// continuation, not a claim.
	case stored.record.State == StateApplying:
		return 0, errTestStateConflict
	case claim.Epoch < stored.record.ClaimEpoch:
		return 0, errTestEpochFenced
	case !s.clock.Now().Before(stored.record.ApplyDeadline):
		return 0, errTestDeadlinePassed
	// A CLAIM CANNOT BE RENEWED: an equal epoch may not take a live claim.
	case s.claimLive(stored) && claim.Epoch == stored.record.ClaimEpoch:
		return 0, errTestClaimHeld
	}
	stored.record.State = StateClaimed
	stored.record.ClaimEpoch = claim.Epoch
	stored.record.ClaimExpiresAt = claim.ExpiresAt
	stored.record.Revision++
	return stored.record.Revision, nil
}

// BeginApplying CASes a claimed record into applying. Only the holder of a LIVE
// claim at the claim's own epoch may make it.
func (s *fakeStore) BeginApplying(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, applying Applying) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("BeginApplying"); err != nil {
		return 0, err
	}
	s.applyings = append(s.applyings, applying)
	stored, ok := s.commands[command]
	if !ok {
		return 0, errors.New("commands_test: no such command")
	}
	switch {
	// Zero is not a revision any provider assigns, so it cannot be one a caller
	// read, and a compare-and-swap naming it would be unconditional.
	case applying.ExpectedRevision == 0:
		return 0, errTestUnrevisioned
	case stored.record.Revision != applying.ExpectedRevision:
		return 0, errTestRevisionConflict
	case stored.record.State != StateClaimed:
		return 0, errTestStateConflict
	case applying.Epoch != stored.record.ClaimEpoch:
		return 0, errTestClaimLost
	case !s.claimLive(stored):
		return 0, errTestClaimLost
	}
	stored.record.State = StateApplying
	stored.record.ClaimExpiresAt = applying.ExpiresAt
	stored.record.Revision++
	return stored.record.Revision, nil
}

// CompleteCommand settles an applying command as applied.
//
// THREE PRECONDITIONS, AND THE FIRST IS THE ONE THAT WAS MISSING: the record
// must be APPLYING, so `claimed -> applied` is refused here as the released
// store refuses it. The result must name a durable journal event, and a caller
// whose epoch is not the claim's must name the event the correlation found
// rather than any event it preferred.
func (s *fakeStore) CompleteCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, completion Completion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("CompleteCommand"); err != nil {
		return err
	}
	s.completed = append(s.completed, completeCall{
		CommandID: command, Revision: completion.ExpectedRevision, Epoch: completion.Epoch, Effect: completion.Effect,
	})
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	switch {
	case completion.ExpectedRevision == 0:
		return errTestUnrevisioned
	case stored.record.Revision != completion.ExpectedRevision:
		return errTestRevisionConflict
	case completion.Effect.CompletedAt.IsZero() || completion.Effect.EventID == "" || completion.Effect.JournalSeq == 0:
		return errTestInvalidResult
	case stored.record.State != StateApplying:
		return errTestStateConflict
	case completion.Epoch < stored.record.ClaimEpoch:
		return errTestEpochFenced
	case completion.Epoch != stored.record.ClaimEpoch && !namesEffect(stored.application, completion.Effect):
		return errTestNoEvidence
	}
	stored.record.State = StateApplied
	stored.record.Revision++
	stored.due = false
	return nil
}

// RejectCommand settles a command with a durable typed reason.
func (s *fakeStore) RejectCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, rejection Rejection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("RejectCommand"); err != nil {
		return err
	}
	s.rejections = append(s.rejections, rejectCall{
		CommandID: command, Revision: rejection.ExpectedRevision, Epoch: rejection.Epoch, Detail: rejection.Detail,
	})
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	if rejection.ExpectedRevision == 0 {
		return errTestUnrevisioned
	}
	if stored.record.Revision != rejection.ExpectedRevision {
		return errTestRevisionConflict
	}
	if err := rejection.Detail.Validate(); err != nil {
		return errTestInvalidResult
	}
	if stored.record.State.Terminal() {
		return errTestStateConflict
	}
	if rejection.Epoch != 0 && rejection.Epoch < stored.record.ClaimEpoch {
		return errTestEpochFenced
	}
	recovering := false
	switch {
	case s.claimLive(stored):
		if rejection.Epoch != stored.record.ClaimEpoch {
			return errTestClaimHeld
		}
	case stored.record.State == StateApplying:
		if rejection.Epoch <= stored.record.ClaimEpoch {
			return errTestClaimLost
		}
		recovering = true
	}
	if !stored.application.provesNoEffect() {
		return errTestNoEvidence
	}
	if recovering && !stored.application.fences(stored.record.ClaimEpoch) {
		return errTestNoEvidence
	}
	stored.record.State = StateRejected
	stored.record.Revision++
	stored.due = false
	return nil
}

// namesEffect is the store's successor rule: the outcome must be committed and
// the result must be the correlated event.
func namesEffect(application Application, effect Effect) bool {
	return application.Outcome == ApplicationCommitted &&
		effect.EventID == application.EffectEventID &&
		effect.JournalSeq == application.EffectSeq
}

// operations returns the operation log.
func (s *fakeStore) operations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// stateOf returns the durable state and due flag of a command.
func (s *fakeStore) stateOf(command sessionwire.CommandID) (State, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := s.commands[command]
	return stored.record.State, stored.due
}

// completions and settledRejections return the terminal settlements the store
// ACCEPTED, as distinct from the ones it was asked for.
func (s *fakeStore) completions() []completeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]completeCall(nil), s.completed...)
}

func (s *fakeStore) settledRejections() []rejectCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rejectCall(nil), s.rejections...)
}

// fakeRuntime is department.CommandApplier, recording exactly what crossed the
// seam.
//
// onApply is what makes this bench honest: a real runtime's application ENDS IN
// A DURABLE JOURNAL EFFECT, and a command is applied because that event exists
// rather than because a method returned nil. A test that wants a runtime which
// commits nothing clears it.
type fakeRuntime struct {
	mu      sync.Mutex
	applied []department.RuntimeCommand
	err     error
	before  func()
	onApply func(sessionwire.CommandID)
}

// ApplyCommand records the command and returns the scripted error.
func (r *fakeRuntime) ApplyCommand(_ context.Context, command department.RuntimeCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.before != nil {
		r.before()
	}
	r.applied = append(r.applied, command)
	if r.err != nil {
		return r.err
	}
	if r.onApply != nil {
		r.onApply(command.CommandID)
	}
	return nil
}

// commands returns every command the runtime was driven with.
func (r *fakeRuntime) commands() []department.RuntimeCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]department.RuntimeCommand(nil), r.applied...)
}

// ---------------------------------------------------------------------------
// Applier fixture
// ---------------------------------------------------------------------------

// applierFixture is one Applier and everything it was built from.
type applierFixture struct {
	t       *testing.T
	clock   *manualClock
	host    *host.Host
	store   *fakeStore
	runtime *fakeRuntime
	fence   *fakeFence
	epoch   uint64
	applier *Applier
}

// newApplierFixture builds an applier over one pending input command, applying
// each configure function BEFORE the applier is constructed.
func newApplierFixture(t *testing.T, configure ...func(*applierFixture)) *applierFixture {
	t.Helper()
	clock := newManualClock(testClockAt)
	f := &applierFixture{
		t:       t,
		clock:   clock,
		store:   newFakeStore(clock),
		runtime: &fakeRuntime{},
		fence:   newFakeFence(),
		epoch:   testEpoch,
	}
	f.runtime.onApply = f.store.commitEffect
	f.host = newTestHost(t, clock)
	f.put(KindInput, StatePending)
	for _, apply := range configure {
		apply(f)
	}
	applier, err := NewApplier(ApplierOptions{
		Host:         f.host,
		Key:          registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch:   f.epoch,
		Records:      f.store,
		Applications: f.store,
		Gates:        f.store,
		Writes:       f.store,
		Journal:      f.store,
		Runtime:      f.runtime,
		Fence:        f.fence,
	})
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	f.applier = applier
	return f
}

// put stores the fixture's single command at acceptance order 1, in a kind and
// a state, due, with a private body and no journal correlation.
func (f *applierFixture) put(kind Kind, state State) *storedCommand {
	stored := &storedCommand{
		record: Record{
			TenantID:         testTenant,
			SessionID:        testSession,
			CommandID:        commandID(1),
			RuntimeCommandID: testRuntimeCommandID,
			Kind:             kind,
			State:            state,
			AcceptedOrder:    1,
			Revision:         testRevision,
			ApplyDeadline:    testClockAt.Add(testApplyDeadline),
		},
		payload:     Payload{Body: []byte(`{"blocks":[]}`)},
		due:         true,
		application: Application{Outcome: ApplicationAbsent},
	}
	if kind == KindGateResponse {
		stored.payload = Payload{Body: gateResponseBody(f.t, testGate)}
		f.store.gates[testGate] = Gate{GateID: testGate, Open: true, OwnerHostID: testHostID, OwnerEpoch: testEpoch}
	}
	f.store.commands[commandID(1)] = stored
	return stored
}

// gateResponseBody is the private body of a gate response: Core's own
// GateResponseRequest, which is the record §9.4's backstop reads the gate
// identity out of.
func gateResponseBody(t *testing.T, gate sessionwire.GateID) []byte {
	t.Helper()
	body, err := json.Marshal(sessionwire.GateResponseRequest{
		CommandEnvelope:     sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(1)},
		SessionID:           testSession,
		GateID:              gate,
		Action:              "approve",
		Values:              map[string]json.RawMessage{"choice": json.RawMessage(`"allow"`)},
		ExpectedOpenEventID: sessionwire.EventID("event-gate-open-1"),
	})
	if err != nil {
		t.Fatalf("marshal the gate response body: %v", err)
	}
	return body
}

// stored returns the fixture's single command record.
func (f *applierFixture) stored() *storedCommand { return f.store.commands[commandID(1)] }

// process runs the applier over the fixture's single command.
func (f *applierFixture) process() (Outcome, error) {
	f.t.Helper()
	return f.applier.Process(context.Background(), command(1, f.stored().record.State))
}

// refusalOf returns the ApplyRefusal an error carries, or "".
func refusalOf(err error) ApplyRefusal {
	var apply *ApplyError
	if errors.As(err, &apply) {
		return apply.Refusal
	}
	return ""
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// TestNewApplierRefusesAnIncompleteComposition asserts every collaborator is
// required at construction, because an applier missing one would fail partway
// through a durable protocol rather than before it started.
func TestNewApplierRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	complete := func(t *testing.T) ApplierOptions {
		t.Helper()
		store := newFakeStore(newManualClock(testClockAt))
		return ApplierOptions{
			Host:         newTestHost(t, newManualClock(testClockAt)),
			Key:          registry.Key{TenantID: testTenant, SessionID: testSession},
			LeaseEpoch:   testEpoch,
			Records:      store,
			Applications: store,
			Gates:        store,
			Writes:       store,
			Journal:      store,
			Runtime:      &fakeRuntime{},
			Fence:        newFakeFence(),
		}
	}
	if _, err := NewApplier(complete(t)); err != nil {
		t.Fatalf("the complete composition was refused: %v", err)
	}
	for _, testCase := range []struct {
		name   string
		damage func(*ApplierOptions)
		field  string
	}{
		{"no host", func(o *ApplierOptions) { o.Host = nil }, "Host"},
		{"no tenant", func(o *ApplierOptions) { o.Key.TenantID = "" }, "Key"},
		{"no session", func(o *ApplierOptions) { o.Key.SessionID = "" }, "Key"},
		{"a zero epoch", func(o *ApplierOptions) { o.LeaseEpoch = 0 }, "LeaseEpoch"},
		{"no records", func(o *ApplierOptions) { o.Records = nil }, "Records"},
		{"no applications", func(o *ApplierOptions) { o.Applications = nil }, "Applications"},
		{"no gates", func(o *ApplierOptions) { o.Gates = nil }, "Gates"},
		{"no writes", func(o *ApplierOptions) { o.Writes = nil }, "Writes"},
		{"no journal", func(o *ApplierOptions) { o.Journal = nil }, "Journal"},
		{"no runtime", func(o *ApplierOptions) { o.Runtime = nil }, "Runtime"},
		{"no fence", func(o *ApplierOptions) { o.Fence = nil }, "Fence"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			options := complete(t)
			testCase.damage(&options)
			applier, err := NewApplier(options)
			if applier != nil {
				t.Fatal("an applier was returned for an incomplete composition")
			}
			var invalid *InvalidConsumerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("got %v, want *InvalidConsumerOptionsError", err)
			}
			if invalid.Field != testCase.field {
				t.Errorf("the refusal names %q, want %q", invalid.Field, testCase.field)
			}
		})
	}
}

// TestNewApplierRefusesAHostThatDidNotComeFromHostNew holds the same rule
// NewConsumer holds, for the accessors an APPLIER reads.
//
// ENUMERATE CONSTRUCTIONS, NOT INTENTIONS: &host.Host{} compiles from any
// package however unexported its fields are, and it yields a nil Clock, an empty
// ID and a ZERO ClaimTTL — and a zero claim TTL takes a claim that has already
// expired, which the store then refuses to let this applier begin applying
// under.
func TestNewApplierRefusesAHostThatDidNotComeFromHostNew(t *testing.T) {
	t.Parallel()

	store := newFakeStore(newManualClock(testClockAt))
	applier, err := NewApplier(ApplierOptions{
		Host:         &host.Host{},
		Key:          registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch:   testEpoch,
		Records:      store,
		Applications: store,
		Gates:        store,
		Writes:       store,
		Journal:      store,
		Runtime:      &fakeRuntime{},
		Fence:        newFakeFence(),
	})
	if applier != nil {
		t.Fatal("an applier was built over a Host with no clock, no identity and a zero claim TTL")
	}
	var unusable *UnusableHostError
	if !errors.As(err, &unusable) {
		t.Fatalf("got %v, want *UnusableHostError", err)
	}
	want := []string{"Clock", "ClaimTTL", "ID"}
	if !reflect.DeepEqual(unusable.Accessors, want) {
		t.Errorf("the refusal names %v, want every unusable accessor %v; naming only the first would leave two checks unexercised by any construction", unusable.Accessors, want)
	}
}

// ---------------------------------------------------------------------------
// The whole protocol
// ---------------------------------------------------------------------------

// TestAnInputCommandIsClaimedAppliedAndCompleted asserts §10.4's protocol in the
// order it is written: the record, then the journal, then the claim, then the
// private payload, then applying, then the correlation, then the runtime, then
// the journal again, then the terminal completion.
//
// THE ORDER IS THE ASSERTION. Every pair in this sequence has a crash between it
// and its neighbour that the recovery rules answer differently, so a reordering
// is not a style change: a payload loaded before the claim is work done for a
// command another Host owns, and a prefix appended after the runtime call is
// missing in the one case it exists for.
func TestAnInputCommandIsClaimedAppliedAndCompleted(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	want := []string{
		"LoadCommand",
		"FindApplication",
		"ClaimCommand",
		"LoadPayload",
		"BeginApplying",
		"AppendApplicationPrefix",
		"FindApplication",
		"CompleteCommand",
	}
	if got := f.store.operations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the durable protocol ran as %v, want %v", got, want)
	}
	if claims := f.store.claims; len(claims) != 1 {
		t.Fatalf("the record was claimed %d times, want once", len(claims))
	}
	claim := f.store.claims[0]
	if claim.Epoch != testEpoch {
		t.Errorf("the claim was taken under epoch %d, want the lease epoch %d", claim.Epoch, testEpoch)
	}
	if want := testClockAt.Add(testClaimTTL); !claim.ExpiresAt.Equal(want) {
		t.Errorf("the claim expires at %v, want now plus the Host's claim TTL %v", claim.ExpiresAt, want)
	}
	if prefixes := f.store.prefixes; len(prefixes) != 1 ||
		prefixes[0] != (Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, Kind: KindInput}) {
		t.Errorf("the application prefix is %+v, want one correlating this command with its own runtime allocation", prefixes)
	}
	completions := f.store.completions()
	if len(completions) != 1 {
		t.Fatalf("the command was completed %d times, want once", len(completions))
	}
	// THE EFFECT COMES FROM THE JOURNAL. A completion names the durable event
	// that carried the command's effect; one composed by the applier would be a
	// claim that something happened with nothing to point at, and the store
	// refuses it.
	wantEffect := Effect{CompletedAt: testClockAt, EventID: testEffectEventID, JournalSeq: testEffectSeq}
	if completions[0].Effect != wantEffect {
		t.Errorf("the completion recorded %+v, want the correlated effect %+v", completions[0].Effect, wantEffect)
	}
	if completions[0].Epoch != testEpoch {
		t.Errorf("the completion was stamped with epoch %d, want %d", completions[0].Epoch, testEpoch)
	}
}

// TestEveryTransitionNamesTheRevisionItRead asserts each compare-and-swap names
// the revision the caller decided on — the record's for the claim, and each
// previous CAS's answer for the ones after it.
//
// A TRANSITION THAT NAMED NO REVISION WOULD BE A BLIND WRITE dressed as a CAS,
// and two Hosts that both read a pending record would both succeed. Carrying the
// answer forward is the half that is easy to lose: an applier that re-used the
// record's original revision for every step would be refused by the store on its
// second write, and by nothing here before this test existed.
func TestEveryTransitionNamesTheRevisionItRead(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(f.store.claims) != 1 || len(f.store.applyings) != 1 || len(f.store.completed) != 1 {
		t.Fatalf("the protocol ran as %v, which is not one of each transition", f.store.operations())
	}
	if got := f.store.claims[0].ExpectedRevision; got != testRevision {
		t.Errorf("the claim named revision %d, want the revision the record was read at %d", got, testRevision)
	}
	if got := f.store.applyings[0].ExpectedRevision; got != testRevision+1 {
		t.Errorf("BeginApplying named revision %d, want the claim's answer %d", got, testRevision+1)
	}
	if got := f.store.completed[0].Revision; got != testRevision+2 {
		t.Errorf("the completion named revision %d, want BeginApplying's answer %d", got, testRevision+2)
	}
}

// TestTheRuntimeReceivesTheMappingAndTheBody is blocking finding 1's regression
// test, and it is a whole-value comparison for the reason department's own
// restore test is: an assertion on one field passes against a seam that carries
// only that field.
//
// The previous seam took sessionwire.CommandEnvelope, which released Core
// defines as exactly a wire version and a public CommandID. So the
// RuntimeCommandID §16 requires Host to forward was loaded, used in the local
// prefix, and never sent; and the command's substance could not be sent either,
// because the inbox payload is private to Factory and Host and SessionStore
// imports only Core and storage. Harness received a bare identity for an input
// command and had no way to obtain the blocks.
func TestTheRuntimeReceivesTheMappingAndTheBody(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	body := f.stored().payload.Body
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	driven := f.runtime.commands()
	if len(driven) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(driven))
	}
	want := department.RuntimeCommand{
		CommandID:        commandID(1),
		RuntimeCommandID: testRuntimeCommandID,
		Kind:             string(KindInput),
		Payload:          body,
	}
	if !reflect.DeepEqual(driven[0], want) {
		t.Errorf("the runtime received %+v, want %+v", driven[0], want)
	}
}

// TestTheRuntimeCommandCarriesEverythingARuntimeNeeds pins the SHAPE of the
// seam, which the value comparison above cannot.
//
// A test that only compares values agrees with any seam wide enough to hold
// them; what made the old seam wrong was its type. This fails if a member is
// removed — which is exactly how the mapping and the body stopped crossing —
// and it fails if one is added, which is the prompt to decide whether Host
// should be filling it.
func TestTheRuntimeCommandCarriesEverythingARuntimeNeeds(t *testing.T) {
	t.Parallel()

	command := reflect.TypeOf(department.RuntimeCommand{})
	var members []string
	for index := 0; index < command.NumField(); index++ {
		members = append(members, command.Field(index).Name)
	}
	want := []string{"CommandID", "RuntimeCommandID", "Kind", "Payload", "PayloadRef"}
	if !reflect.DeepEqual(members, want) {
		t.Errorf("department.RuntimeCommand carries %v, want %v", members, want)
	}
}

// TestAReferencedBodyCrossesAsAReference asserts Host passes an object
// reference on rather than dereferencing it.
//
// §10.1 gives a private body an independent immutable object reference once it
// exceeds its inline threshold, and the runtime resolves it through its own
// object read. Host has no object seam at all — which is the mechanism, since a
// dereference it cannot perform is one it cannot accidentally add — so what this
// asserts is that the reference is not silently dropped on the way.
func TestAReferencedBodyCrossesAsAReference(t *testing.T) {
	t.Parallel()

	reference := sessionwire.ObjectReference{ObjectID: "object-77f"}
	f := newApplierFixture(t, func(f *applierFixture) {
		f.stored().payload = Payload{Ref: reference}
	})
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	driven := f.runtime.commands()
	if len(driven) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(driven))
	}
	if driven[0].PayloadRef != reference {
		t.Errorf("the runtime received the reference %+v, want %+v", driven[0].PayloadRef, reference)
	}
	if len(driven[0].Payload) != 0 {
		t.Errorf("the runtime received %d inline bytes beside a reference, and at most one of the two is ever set", len(driven[0].Payload))
	}
}

// TestACommandTheRuntimeCommittedNothingForIsNotApplied is the other half of
// blocking finding 1, and the one that makes the seam's correctness observable
// rather than merely asserted.
//
// A command is applied because a durable journal effect exists, not because a
// method returned nil. The runtime here returns success while committing
// nothing — which is exactly what the old two-field seam produced for every
// input command, since Harness could not obtain the blocks — and the applier
// must refuse to record it, leaving the command applying and DUE for a later
// pass.
func TestACommandTheRuntimeCommittedNothingForIsNotApplied(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) { f.runtime.onApply = nil })
	outcome, err := f.process()
	if refusalOf(err) != RefusalNoCommittedEffect {
		t.Fatalf("got %v, want a %q refusal", err, RefusalNoCommittedEffect)
	}
	if completions := f.store.completions(); len(completions) != 0 {
		t.Errorf("the command was completed as %+v with no effect in the journal", completions)
	}
	state, due := f.store.stateOf(commandID(1))
	if state != StateApplying {
		t.Errorf("the record is %q, want %q", state, StateApplying)
	}
	if !due {
		t.Error("the record is no longer due, so no reconciler will ever finish it")
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though a correlated prefix was appended before the call")
	}
}

// TestEveryKindIsDrivenIntoTheRuntime replaces a deviation that was refused by
// the store this runs on.
//
// An earlier version settled create and restore as applied WITHOUT driving the
// runtime, on the ground that residency is their consequence and had already
// happened. That was sound about the consequence and skipped what the durable
// record may legally say: `claimed -> applied` is not an edge — §10.4 writes the
// chain through applying, and sessionstore's CompleteCommand admits only an
// applying record and requires a result naming the journal event that carried
// the effect. For a create settled by residency alone no such event exists, so
// no adapter could have supplied one.
func TestEveryKindIsDrivenIntoTheRuntime(t *testing.T) {
	t.Parallel()

	for _, kind := range []Kind{KindCreate, KindRestore, KindInput, KindInterrupt} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				stored := f.put(kind, StatePending)
				if kind != KindInput {
					stored.payload = Payload{}
				}
			})
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != StateApplied {
				t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
			}
			driven := f.runtime.commands()
			if len(driven) != 1 {
				t.Fatalf("the runtime was driven %d times, want once", len(driven))
			}
			if driven[0].Kind != string(kind) {
				t.Errorf("the runtime received kind %q, want %q", driven[0].Kind, kind)
			}
			if completions := f.store.completions(); len(completions) != 1 {
				t.Errorf("the command was completed %d times, want once", len(completions))
			}
		})
	}
}

// TestAResidentGateResponseIsApplied asserts the ordinary gate case: the durable
// gate is open and still names this Host and this residency, so the response is
// driven into the runtime like any other command.
func TestAResidentGateResponseIsApplied(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) { f.put(KindGateResponse, StatePending) })
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(driven))
	}
	operations := f.store.operations()
	claim, gate := indexOf(operations, "ClaimCommand"), indexOf(operations, "LoadGate")
	if claim < 0 || gate < 0 || gate < claim {
		t.Errorf("the operations ran as %v; the durable gate is read AFTER the claim, because the recheck is work this applier does on a command it owns", operations)
	}
}

// TestAGateResponseThatLostTheReleaseRaceIsRejectedAsNotResumable is §9.4's
// backstop.
//
// FACTORY NORMALLY REJECTS A COLD RESPONSE BEFORE THE INBOX EXISTS, so every row
// here is a RACE: the gate was resumable when Factory admitted the command and is
// not by the time this Host claims it. The rejection is durable and typed, the
// runtime is never driven, and no application prefix is appended — a rejection
// rests on the journal proving that no effect committed, so a correlation
// appended first would make the settlement it needs impossible.
func TestAGateResponseThatLostTheReleaseRaceIsRejectedAsNotResumable(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		gate func(*applierFixture)
	}{
		{
			name: "the gate is gone",
			gate: func(f *applierFixture) { delete(f.store.gates, testGate) },
		},
		{
			name: "the gate is resolved",
			gate: func(f *applierFixture) {
				gate := f.store.gates[testGate]
				gate.Open = false
				f.store.gates[testGate] = gate
			},
		},
		{
			name: "another Host owns it",
			gate: func(f *applierFixture) {
				gate := f.store.gates[testGate]
				gate.OwnerHostID = sessionwire.HostID("host-elsewhere")
				f.store.gates[testGate] = gate
			},
		},
		{
			name: "it was opened under an earlier residency",
			gate: func(f *applierFixture) {
				gate := f.store.gates[testGate]
				gate.OwnerEpoch = testOtherEpoch
				f.store.gates[testGate] = gate
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				f.put(KindGateResponse, StatePending)
				testCase.gate(f)
			})
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != StateRejected {
				t.Fatalf("outcome state = %q, want %q", outcome.State, StateRejected)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v for a gate it cannot resume", driven)
			}
			if prefixes := f.store.prefixes; len(prefixes) != 0 {
				t.Errorf("an application prefix %+v was appended for a command that is being rejected", prefixes)
			}
			rejections := f.store.settledRejections()
			if len(rejections) != 1 {
				t.Fatalf("the command was rejected %d times, want once", len(rejections))
			}
			if code := rejections[0].Detail.Code; code != sessionwire.ErrorCodeGateNotResumable {
				t.Errorf("the rejection code is %q, want %q", code, sessionwire.ErrorCodeGateNotResumable)
			}
			if rejections[0].Detail.Message == "" {
				t.Error("the rejection carries no message, so an operator cannot tell which of the four races was lost")
			}
		})
	}
}

// TestAGateResponseThisHostCannotReadIsRefused asserts §9.4's backstop refuses
// rather than guesses.
//
// Host reads ONE record out of a private body — Core's own GateResponseRequest,
// because the gate's identity is not a member of the inbox record and the check
// is impossible without it. A body it cannot read that record out of, or one
// stored behind an object reference it does not dereference, leaves the check
// undecidable; assuming the gate is fine would drive a response into a runtime
// that cannot resume it.
func TestAGateResponseThisHostCannotReadIsRefused(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		payload func(*testing.T) Payload
		refusal ApplyRefusal
	}{
		{
			name:    "a body behind an object reference",
			payload: func(*testing.T) Payload { return Payload{Ref: sessionwire.ObjectReference{ObjectID: "object-1"}} },
			refusal: RefusalReferencedGateResponse,
		},
		{
			name:    "a body that is not a gate response",
			payload: func(*testing.T) Payload { return Payload{Body: []byte(`{"blocks":[]}`)} },
			refusal: RefusalUnreadableGateResponse,
		},
		{
			name: "a body naming another command",
			payload: func(t *testing.T) Payload {
				body, err := json.Marshal(sessionwire.GateResponseRequest{
					CommandEnvelope:     sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(9)},
					SessionID:           testSession,
					GateID:              testGate,
					Action:              "approve",
					Values:              map[string]json.RawMessage{"choice": json.RawMessage(`"allow"`)},
					ExpectedOpenEventID: sessionwire.EventID("event-gate-open-1"),
				})
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				return Payload{Body: body}
			},
			refusal: RefusalCorrelation,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				stored := f.put(KindGateResponse, StatePending)
				stored.payload = testCase.payload(t)
			})
			_, err := f.process()
			if refusalOf(err) != testCase.refusal {
				t.Fatalf("got %v, want a %q refusal", err, testCase.refusal)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v", driven)
			}
			if rejections := f.store.settledRejections(); len(rejections) != 0 {
				t.Errorf("the command was durably rejected as %+v for a check this Host could not run", rejections)
			}
		})
	}
}

// TestAnEffectMustNameADurableEvent covers Effect.Validate over every way an
// effect can fail to name one.
//
// IT IS TESTED DIRECTLY BECAUSE THE TYPE IS EXPORTED. Effect{} compiles from any
// package, and the adapter that will map a store's own result type into this one
// is exactly the caller that can build a partial value — so the invariant is a
// property of the type rather than of the one path inside this package that
// happens to fill every member. Routing every row through Process would also
// have hidden two of them: the first failing member returns, so a value missing
// all three exercises one arm.
func TestAnEffectMustNameADurableEvent(t *testing.T) {
	t.Parallel()

	whole := Effect{CompletedAt: testClockAt, EventID: testEffectEventID, JournalSeq: testEffectSeq}
	if err := whole.Validate(); err != nil {
		t.Fatalf("a complete effect was refused: %v", err)
	}
	for _, testCase := range []struct {
		name   string
		effect Effect
	}{
		{"no completion instant", Effect{EventID: testEffectEventID, JournalSeq: testEffectSeq}},
		{"no public event", Effect{CompletedAt: testClockAt, JournalSeq: testEffectSeq}},
		{"an invalid public event", Effect{CompletedAt: testClockAt, EventID: sessionwire.EventID(strings.Repeat("e", 4096)), JournalSeq: testEffectSeq}},
		{"no journal sequence", Effect{CompletedAt: testClockAt, EventID: testEffectEventID}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := testCase.effect.Validate()
			if refusalOf(err) != RefusalInvalidEffect {
				t.Fatalf("got %v, want a %q refusal", err, RefusalInvalidEffect)
			}
		})
	}
}

// TestAppliedAndRejectedAreMutuallyExclusive holds step 5 as TWO MECHANISMS
// rather than a value shape.
//
// The first is the seam: applied and rejected are separate methods carrying
// different evidence — a journal effect for one, a stable typed code for the
// other — so no single call can commit both. An earlier version made this a
// property of one Result value with an unexported state field, which was true
// and much weaker, because it said nothing about what the store would accept.
// The second is the record: a settled command is terminal, and the store refuses
// the other settlement against it. Both directions are asserted, because a guard
// that only refused a second write of the same kind would let a completed
// command be rejected.
func TestAppliedAndRejectedAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	t.Run("a completed command cannot then be rejected", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t)
		if _, err := f.process(); err != nil {
			t.Fatalf("Process: %v", err)
		}
		record := f.stored().record
		if record.State != StateApplied {
			t.Fatalf("the record is %q, want %q", record.State, StateApplied)
		}
		_, err := f.applier.reject(context.Background(), record, record.Revision,
			sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeCommandRejected, Message: "late"})
		if !errors.Is(err, errTestStateConflict) {
			t.Fatalf("rejecting an applied command reported %v, want the store's state conflict", err)
		}
		if state, _ := f.store.stateOf(commandID(1)); state != StateApplied {
			t.Errorf("the record is now %q; a terminal state was overwritten", state)
		}
	})

	t.Run("a rejected command cannot then be completed", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t, func(f *applierFixture) {
			f.put(KindGateResponse, StatePending)
			delete(f.store.gates, testGate)
		})
		if _, err := f.process(); err != nil {
			t.Fatalf("Process: %v", err)
		}
		record := f.stored().record
		if record.State != StateRejected {
			t.Fatalf("the record is %q, want %q", record.State, StateRejected)
		}
		_, err := f.applier.complete(context.Background(), record, record.Revision, Application{
			Outcome: ApplicationCommitted, EffectEventID: testEffectEventID, EffectSeq: testEffectSeq,
		})
		if !errors.Is(err, errTestStateConflict) {
			t.Fatalf("completing a rejected command reported %v, want the store's state conflict", err)
		}
		if state, _ := f.store.stateOf(commandID(1)); state != StateRejected {
			t.Errorf("the record is now %q; a terminal state was overwritten", state)
		}
	})
}

// TestATerminalCommandBecomesNotDue holds step 5's second half. A record still
// being worked on stays due, because a due page is what a reconciler finds
// outstanding work in; a terminal settlement is the write that clears it.
func TestATerminalCommandBecomesNotDue(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.store.errs["CompleteCommand"] = errTestStore
	if _, err := f.process(); !errors.Is(err, errTestStore) {
		t.Fatalf("the refused completion reported %v, want the store failure", err)
	}
	if _, due := f.store.stateOf(commandID(1)); !due {
		t.Fatal("a command whose terminal write did not commit is no longer due, so no reconciler will ever finish it")
	}

	f.store.mu.Lock()
	delete(f.store.errs, "CompleteCommand")
	f.store.mu.Unlock()
	if _, err := f.process(); err != nil {
		t.Fatalf("the recovery pass reported %v", err)
	}
	state, due := f.store.stateOf(commandID(1))
	if state != StateApplied {
		t.Errorf("the record is %q, want %q", state, StateApplied)
	}
	if due {
		t.Error("a terminal record is still due, so it goes on appearing in every due page forever")
	}
}

// TestThePrivatePayloadIsLoadedAfterTheClaim holds step 4's first half: the
// payload is loaded from SessionStore once this applier owns the command, so a
// Host does not read a command another Host is working on.
func TestThePrivatePayloadIsLoadedAfterTheClaim(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	operations := f.store.operations()
	claim, payload := indexOf(operations, "ClaimCommand"), indexOf(operations, "LoadPayload")
	if claim < 0 || payload < 0 {
		t.Fatalf("the operations ran as %v, which contains no claim or no payload load", operations)
	}
	if payload < claim {
		t.Errorf("the operations ran as %v; the private payload is loaded AFTER the claim", operations)
	}
}

// TestTheRuntimeCommandIDComesFromTheWinningRecord asserts the mapping is
// LOADED, not minted: §10.4 lets two replicas propose different UUIDs while
// racing one public ID and makes only the winner's usable, so an applier that
// generated its own would correlate an effect with a mapping no retry receives.
func TestTheRuntimeCommandIDComesFromTheWinningRecord(t *testing.T) {
	t.Parallel()

	t.Run("the prefix and the runtime both carry the record's allocation", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t, func(f *applierFixture) {
			f.stored().record.RuntimeCommandID = testOtherRuntimeCommandID
		})
		if _, err := f.process(); err != nil {
			t.Fatalf("Process: %v", err)
		}
		if prefixes := f.store.prefixes; len(prefixes) != 1 || prefixes[0].RuntimeCommandID != testOtherRuntimeCommandID {
			t.Errorf("the prefix correlates %+v, want the record's own allocation %v", prefixes, testOtherRuntimeCommandID)
		}
		if driven := f.runtime.commands(); len(driven) != 1 || driven[0].RuntimeCommandID != testOtherRuntimeCommandID {
			t.Errorf("the runtime received %+v, want the record's own allocation %v", driven, testOtherRuntimeCommandID)
		}
	})

	t.Run("an unallocated mapping is refused before anything is written", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t, func(f *applierFixture) {
			f.stored().record.RuntimeCommandID = uuid.UUID{}
		})
		_, err := f.process()
		if refusalOf(err) != RefusalCorrelation {
			t.Fatalf("got %v, want a %q refusal", err, RefusalCorrelation)
		}
		if writes := f.fence.fencedWrites(); writes != 0 {
			t.Errorf("%d durable writes were made for a record no prefix could correlate", writes)
		}
	})
}

// TestProcessRefusesARecordThatIsNotTheOneItAskedFor asserts the keyed read is
// checked, for the reason validatePage checks a page: this applier is about to
// compare-and-swap a durable record, and a store that answered with somebody
// else's would have it write onto a command it decided nothing about.
func TestProcessRefusesARecordThatIsNotTheOneItAskedFor(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		corrupt func(*Record)
		refusal ApplyRefusal
	}{
		{"another tenant", func(r *Record) { r.TenantID = "tenant-elsewhere" }, RefusalForeignRecord},
		{"another session", func(r *Record) { r.SessionID = "session-elsewhere" }, RefusalForeignRecord},
		{"another command", func(r *Record) { r.CommandID = commandID(9) }, RefusalForeignRecord},
		{"another acceptance order", func(r *Record) { r.AcceptedOrder = 4 }, RefusalForeignRecord},
		{"a state this package cannot reason about", func(r *Record) { r.State = State("half-applied") }, RefusalUnknownState},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t)
			asked := f.stored().record
			testCase.corrupt(&f.stored().record)
			_, err := f.applier.Process(context.Background(), command(asked.AcceptedOrder, asked.State))
			if refusalOf(err) != testCase.refusal {
				t.Fatalf("got %v, want a %q refusal", err, testCase.refusal)
			}
			if writes := f.fence.fencedWrites(); writes != 0 {
				t.Errorf("%d durable writes were made against a record this applier refused", writes)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v", driven)
			}
		})
	}
}

// TestAnAlreadyTerminalRecordIsReportedWithoutWriting asserts the case the page
// cannot see: the record was non-terminal when the page was listed and somebody
// — a predecessor, or Factory's deadline reconciler — settled it in between.
func TestAnAlreadyTerminalRecordIsReportedWithoutWriting(t *testing.T) {
	t.Parallel()

	for _, state := range []State{StateApplied, StateRejected} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t)
			f.stored().record.State = state
			outcome, err := f.applier.Process(context.Background(), command(1, StatePending))
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != state {
				t.Errorf("outcome state = %q, want the durable %q", outcome.State, state)
			}
			if writes := f.fence.fencedWrites(); writes != 0 {
				t.Errorf("%d durable writes were made against a terminal record", writes)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v for a command that is already terminal", driven)
			}
		})
	}
}

// TestAPayloadTheCommandCannotBeAppliedWithoutIsRefused covers both halves of
// the payload check: a kind whose body is required and absent, and a payload
// whose two forms disagree with the store's own one-of-two invariant.
func TestAPayloadTheCommandCannotBeAppliedWithoutIsRefused(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		prepare func(*applierFixture)
	}{
		{"input with no body at all", func(f *applierFixture) { f.put(KindInput, StatePending).payload = Payload{} }},
		{"a gate response with no body at all", func(f *applierFixture) { f.put(KindGateResponse, StatePending).payload = Payload{} }},
		{"a body and a reference at once", func(f *applierFixture) {
			f.put(KindInput, StatePending).payload = Payload{
				Body: []byte(`{"blocks":[]}`),
				Ref:  sessionwire.ObjectReference{ObjectID: "object-2"},
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, testCase.prepare)
			_, err := f.process()
			if refusalOf(err) != RefusalMissingPayload {
				t.Fatalf("got %v, want a %q refusal", err, RefusalMissingPayload)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v", driven)
			}
			if completions := f.store.completions(); len(completions) != 0 {
				t.Errorf("the command was completed as %+v", completions)
			}
		})
	}
}

// TestAStoreFailureStopsTheProtocolWhereItHappened asserts each read and each
// fenced write is a place the protocol can stop, and that stopping there leaves
// the runtime undriven whenever the failure came before the effect.
func TestAStoreFailureStopsTheProtocolWhereItHappened(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"LoadCommand", "FindApplication", "ClaimCommand", "LoadPayload", "BeginApplying", "AppendApplicationPrefix"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t)
			f.store.errs[operation] = errTestStore
			_, err := f.process()
			if !errors.Is(err, errTestStore) {
				t.Fatalf("got %v, want the store failure to be carried", err)
			}
			if refusalOf(err) != RefusalStore {
				t.Errorf("the refusal is %q, want %q", refusalOf(err), RefusalStore)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v after %s failed", driven, operation)
			}
		})
	}
}

// TestALostGrantStopsTheApplicationBeforeItClaims asserts the fence is consulted
// by the applier's own writes and not only by the consumer's cursor write: an
// applier whose grant ended claims nothing, because a claim under a lease a
// successor holds is work taken away from the Host that owns it.
func TestALostGrantStopsTheApplicationBeforeItClaims(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		lose func(*fakeFence)
	}{
		{"the lease channel closed", func(f *fakeFence) { f.close() }},
		{"a store refused a write under a superseded epoch", func(f *fakeFence) { f.end() }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t)
			testCase.lose(f.fence)
			_, err := f.process()
			if !errors.Is(err, errTestGrantGone) {
				t.Fatalf("got %v, want the grant-gone cause", err)
			}
			if claims := f.store.claims; len(claims) != 0 {
				t.Errorf("the command was claimed %+v under a grant already known to be gone", claims)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v", driven)
			}
		})
	}
}

// TestEachFencedWriteIsRefusedWhenTheGrantEndsBeforeIt asserts the fence is
// consulted at EVERY durable write of one application, not once at the top.
//
// THE STRUCTURAL GUARD IS NOT ENOUGH ON ITS OWN, and measuring it is what showed
// that: unfencing a write one at a time was killed by
// TestEveryDurableWriteGoesThroughTheFence and by NOTHING ELSE, because the only
// behavioural lost-grant test loses the grant before the first write and never
// reaches the later ones. A guard whose subject is a naming convention is exactly
// the guard that stops covering a write the day somebody renames a seam.
//
// THE GRANT ENDS THE WAY A SELECT CANNOT SEE — a store refusing a write under a
// superseded epoch, which closes no channel — because that is the path a
// mid-protocol loss actually arrives on.
//
// THE TERMINAL WRITES ARE NOT ROWS HERE, AND THE REASON IS THE POINT. A row for
// the terminal write was VACUOUS: with the grant ending after the prefix, the
// check before the runtime call returns first, so the settlement is never
// attempted for a reason that has nothing to do with its fencing — the assertion
// passed against a build whose terminal write was unfenced. Each has its own test
// standing where nothing else can return first.
func TestEachFencedWriteIsRefusedWhenTheGrantEndsBeforeIt(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name      string
		prepare   func(*applierFixture)
		ends      string
		forbidden string
	}{
		{name: "before the record enters applying", ends: "ClaimCommand", forbidden: "BeginApplying"},
		{name: "before the application prefix", ends: "BeginApplying", forbidden: "AppendApplicationPrefix"},
		{
			// The rejection's own row. It is reachable only on the gate path,
			// because that is the one place this applier decides to settle a
			// command without applying it — and without it, the terminal
			// rejection had no behavioural cover at all.
			name: "before a terminal rejection",
			prepare: func(f *applierFixture) {
				f.put(KindGateResponse, StatePending)
				delete(f.store.gates, testGate)
			},
			ends:      "LoadGate",
			forbidden: "RejectCommand",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var prepare []func(*applierFixture)
			if testCase.prepare != nil {
				prepare = append(prepare, testCase.prepare)
			}
			f := newApplierFixture(t, prepare...)
			f.store.after[testCase.ends] = func() { f.fence.end() }
			_, err := f.process()
			if !errors.Is(err, errTestGrantGone) {
				t.Fatalf("got %v, want the grant-gone cause", err)
			}
			operations := f.store.operations()
			if indexOf(operations, testCase.ends) < 0 {
				t.Fatalf("the operations ran as %v, which never reached %s, so this row asserts nothing", operations, testCase.ends)
			}
			if indexOf(operations, testCase.forbidden) >= 0 {
				t.Errorf("the operations ran as %v; %s committed under a grant that had already ended", operations, testCase.forbidden)
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v under a grant that had already ended", driven)
			}
		})
	}
}

// TestTheRuntimeIsNotDrivenAfterTheGrantEnds asserts the one check in the apply
// path that is not a write.
//
// Every other step is a durable write the fence refuses on this Host's behalf.
// The runtime call is not: it is the one step no successor can undo, so the grant
// is consulted immediately before it and a Host that has lost the session stops
// rather than driving a runtime it is about to lose. The prefix it already
// appended is what lets the next owner finish the command.
//
// THE ASSERTION IS THE RUNTIME AND NOT THE SETTLEMENT. The settlement is also
// absent on this path, and asserting it here would assert nothing: this check
// returns first, so it is unreached whether or not it is fenced.
func TestTheRuntimeIsNotDrivenAfterTheGrantEnds(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.store.after["AppendApplicationPrefix"] = func() { f.fence.end() }
	outcome, err := f.process()
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("got %v, want the grant-gone cause", err)
	}
	operations := f.store.operations()
	if indexOf(operations, "AppendApplicationPrefix") < 0 {
		t.Fatalf("the operations ran as %v, which never appended a prefix, so this test asserts nothing", operations)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v under a grant that had already ended", driven)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though the prefix was appended and is what the next owner finishes from")
	}
}

// TestTheCompletionIsRefusedWhenTheGrantEndsDuringTheRuntimeCall is the row its
// table sibling cannot hold.
//
// The grant ends INSIDE the runtime call, which is the only instant between the
// application prefix and the settlement at which nothing else returns first: the
// check before the call has already passed, and the effect has already been
// driven. What the fence must still refuse is the terminal write.
//
// THE PASS REPORTS NO ERROR, and that is the documented behaviour rather than a
// hole. The effect is durable in the journal, so the command is recoverable
// whatever the reason the completion did not land, which is exactly
// Outcome.PrefixOwned's meaning.
func TestTheCompletionIsRefusedWhenTheGrantEndsDuringTheRuntimeCall(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.runtime.before = func() { f.fence.end() }
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Fatalf("the runtime was driven %d times, want once: the grant was held when the call began, and this test asserts nothing if it never ran", len(driven))
	}
	if completions := f.store.completions(); len(completions) != 0 {
		t.Errorf("the completion %+v committed under a grant that had ended", completions)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though the effect is durable in the journal")
	}
	if outcome.State.Terminal() {
		t.Errorf("outcome state = %q, which reports a terminal state no store acknowledged", outcome.State)
	}
}

// TestAConsumerWritesNoCursorWhenTheGrantEndedDuringACompletion is the half of
// the rule above that lives at the composition, and it is what makes the silent
// arm safe rather than merely documented.
//
// complete reports a lost grant as an owned prefix with no error, which the
// cursor rule treats as consumable — so on its own that would advance the cursor
// past a command whose settlement never landed. It does not, and the mechanism
// is the one the applier shares with the consumer: the cursor write goes through
// the SAME fence, so a pass that ended with the grant gone writes nothing and
// the record is re-derived by whoever owns the session next.
func TestAConsumerWritesNoCursorWhenTheGrantEndedDuringACompletion(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.runtime.before = func() { f.fence.end() }
	cursors := &fakeCursors{}
	consumer, err := NewConsumer(Options{
		Host: f.host, Key: registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch, Inbox: &fakeInbox{pages: [][]Command{{command(1, StatePending)}}},
		Cursors: cursors, Processor: f.applier, Fence: f.fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	result, err := consumer.Reconcile(context.Background())
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("Reconcile reported %v, want the grant-gone cause", err)
	}
	if writes := cursors.written(); len(writes) != 0 {
		t.Errorf("the cursor was written %+v by a pass whose grant had ended", writes)
	}
	if result.Cursor != 0 {
		t.Errorf("the pass reports cursor %d, want the unacknowledged 0", result.Cursor)
	}
	if state, due := f.store.stateOf(commandID(1)); state != StateApplying || !due {
		t.Errorf("the record is %q (due %v), want an applying record still due for the next owner", state, due)
	}
}

// TestARecordWithNoRevisionIsRefusedBeforeAnyTransition holds the rule
// Record.Revision's own documentation states, on the side that can act on it.
//
// Zero is not a revision any provider assigns, so it cannot be one this applier
// read, and a compare-and-swap naming it is unconditional — two Hosts that both
// saw a pending record would both succeed. THE STORE REFUSES IT, and that is not
// enough on its own: a Host willing to issue the request is a Host whose
// exactly-once property is being enforced by somebody else's input validation.
// Both halves are asserted here, because the fake-only version of this check
// would have left the production side unguarded and green.
func TestARecordWithNoRevisionIsRefusedBeforeAnyTransition(t *testing.T) {
	t.Parallel()

	t.Run("the applier will not issue one", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t, func(f *applierFixture) { f.stored().record.Revision = 0 })
		_, err := f.process()
		if refusalOf(err) != RefusalUnrevisionedRecord {
			t.Fatalf("got %v, want a %q refusal", err, RefusalUnrevisionedRecord)
		}
		if writes := f.fence.fencedWrites(); writes != 0 {
			t.Errorf("%d durable writes named a revision no provider assigns", writes)
		}
		if driven := f.runtime.commands(); len(driven) != 0 {
			t.Errorf("the runtime was driven with %+v", driven)
		}
	})

	t.Run("the store would refuse one", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t)
		_, err := f.store.ClaimCommand(context.Background(), testTenant, testSession, commandID(1), Claim{
			ExpectedRevision: 0, Epoch: testEpoch, ExpiresAt: unexpired,
		})
		if !errors.Is(err, errTestUnrevisioned) {
			t.Fatalf("the store accepted an unrevisioned claim: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// The consumer's own reader
// ---------------------------------------------------------------------------

// TestTheApplierIsTheSeamTheConsumerConsumes runs a real Applier behind a real
// Consumer, which is the only place the two halves of O4 meet.
//
// It is not a duplicate of the protocol test above: what it establishes is that
// the Outcome an applier reports is one the CURSOR RULE accepts, so a command
// this package applied is one the consumer's cursor passes. A processor that
// applied correctly and reported an outcome the rule rejects would leave every
// session wedged on its first command, and no test on either side of the seam
// would have said so.
func TestTheApplierIsTheSeamTheConsumerConsumes(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	inbox := &fakeInbox{pages: [][]Command{{command(1, StatePending)}}}
	cursors := &fakeCursors{}
	consumer, err := NewConsumer(Options{
		Host: f.host, Key: registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch, Inbox: inbox, Cursors: cursors, Processor: f.applier, Fence: f.fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	result, err := consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Blocked != nil {
		t.Fatalf("the pass stopped at %+v, so an applied command did not satisfy the cursor rule", result.Blocked)
	}
	if result.Consumed != 1 {
		t.Errorf("consumed = %d, want 1", result.Consumed)
	}
	if result.Cursor != 1 {
		t.Errorf("the cursor advanced to %d, want the applied command's acceptance order 1", result.Cursor)
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Errorf("the runtime was driven %d times, want once", len(driven))
	}
}

// ---------------------------------------------------------------------------
// The one fencing mechanism
// ---------------------------------------------------------------------------

// TestEveryDurableWriteGoesThroughTheFence holds the ONE MECHANISM rule for
// every durable write this package makes, and it is the widened form of a guard
// that used to cover SaveCursor in one file.
//
// THE SEVENTH CALLER IS WHAT IT IS FOR. O4.1 recorded that a Processor's claim
// and terminal write would arrive OUTSIDE the old guard; they now go through the
// same Fence.Write the cursor does, and this is what keeps them there when the
// next write is added by somebody who never read that note.
//
// THE SUBJECT IS DERIVED, NOT LISTED. The guard collects the methods of every
// interface declared in a production file of this package whose name ends in
// "Writes", and requires every call to one of those names, in any production
// file, to be lexically inside a call to Write. A method added to InboxWrites is
// covered the moment it is declared, and JournalWrites — a whole second write
// seam, over a different durable object — needed no edit here at all, which is
// what the convention buys.
//
// THE CLAIM IS EXACTLY THAT ENUMERATION AND NO WIDER. A durable write declared on
// an interface named otherwise is not covered, and neither is a write smuggled
// through a function value assigned elsewhere.
//
// THAT LIMIT WAS MEASURED, and what it costs was measured with it. Renaming a
// write seam out of the convention SURVIVES this guard. What that no longer costs
// is today's writes: each has a behavioural kill of its own as well, so the
// rename alone unfences nothing any test would miss. What it does cost is the
// forward claim, which is the whole reason a structural guard exists — the NEXT
// write added to a renamed seam would be unguarded and this test would not say
// so. That is the residual, and it is a rename in a diff rather than a silent
// omission.
func TestEveryDurableWriteGoesThroughTheFence(t *testing.T) {
	t.Parallel()

	t.Run("the package obeys it", func(t *testing.T) {
		t.Parallel()
		fset := token.NewFileSet()
		files := parseProductionFiles(t, fset)
		if len(files) == 0 {
			t.Fatal("no production file was parsed, so this guard is vacuous")
		}
		seams, methods := fencedWriteMethods(files)
		if len(seams) == 0 {
			t.Fatal("no interface named …Writes is declared, so this guard has no subject")
		}
		if len(methods) == 0 {
			t.Fatalf("the seams %v declare no method, so this guard is vacuous", seams)
		}
		called := map[string]int{}
		var unfenced []string
		for _, file := range files {
			found, escaped := fencedWriteCalls(fset, file, methods)
			for method, count := range found {
				called[method] += count
			}
			unfenced = append(unfenced, escaped...)
		}
		for method := range methods {
			if called[method] == 0 {
				t.Errorf("no production file calls %s, so the guard's claim about it is vacuous: either it is dead, or it is called by a name this guard cannot see", method)
			}
		}
		if len(unfenced) != 0 {
			sort.Strings(unfenced)
			t.Errorf("these durable writes are not inside a fenced write, so they could commit under a lease a successor has taken: %s", strings.Join(unfenced, "; "))
		}
	})

	// The collector is PROBED with declarations that genuinely violate it,
	// rather than by mutating the assertion: an assertion that survives because
	// nothing in the package breaks the property is a fact about the package,
	// not about the guard.
	t.Run("the seams it collects", func(t *testing.T) {
		t.Parallel()
		for _, testCase := range []struct {
			name    string
			source  string
			seams   []string
			methods []string
		}{
			{
				name:    "an interface named …Writes",
				source:  "package p\ntype InboxWrites interface {\n\tClaimCommand(int) error\n\tCompleteCommand(int) error\n}\n",
				seams:   []string{"InboxWrites"},
				methods: []string{"ClaimCommand", "CompleteCommand"},
			},
			{
				name:    "a reading seam beside it",
				source:  "package p\ntype InboxReads interface{ LoadCommand(int) error }\ntype InboxWrites interface{ ClaimCommand(int) error }\n",
				seams:   []string{"InboxWrites"},
				methods: []string{"ClaimCommand"},
			},
			{
				name:    "two write seams",
				source:  "package p\ntype InboxWrites interface{ ClaimCommand(int) error }\ntype JournalWrites interface{ AppendApplicationPrefix(int) error }\n",
				seams:   []string{"InboxWrites", "JournalWrites"},
				methods: []string{"AppendApplicationPrefix", "ClaimCommand"},
			},
			{
				name:    "an embedded write seam contributes its own name only",
				source:  "package p\ntype InboxWrites interface{ ClaimCommand(int) error }\ntype Inbox interface {\n\tInboxWrites\n\tLoadCommand(int) error\n}\n",
				seams:   []string{"InboxWrites"},
				methods: []string{"ClaimCommand"},
			},
			{
				name:   "no write seam at all",
				source: "package p\ntype Inbox interface{ LoadCommand(int) error }\n",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				fset := token.NewFileSet()
				parsed, err := parser.ParseFile(fset, "probe.go", testCase.source, parser.ParseComments)
				if err != nil {
					t.Fatalf("parse the probe: %v", err)
				}
				seams, methods := fencedWriteMethods(map[string]*ast.File{"probe.go": parsed})
				if !reflect.DeepEqual(seams, testCase.seams) {
					t.Errorf("collected the seams %v, want %v", seams, testCase.seams)
				}
				var got []string
				for method := range methods {
					got = append(got, method)
				}
				sort.Strings(got)
				want := testCase.methods
				sort.Strings(want)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("collected the methods %v, want %v", got, want)
				}
			})
		}
	})

	t.Run("the calls it refuses", func(t *testing.T) {
		t.Parallel()
		methods := map[string]bool{"ClaimCommand": true, "SaveCursor": true}
		for _, testCase := range []struct {
			name     string
			source   string
			found    int
			unfenced int
		}{
			{
				name:     "a fenced write",
				source:   "package p\nfunc f() { fence.Write(func() error { return w.ClaimCommand(ctx) }) }\n",
				found:    1,
				unfenced: 0,
			},
			{
				name:     "a bare write",
				source:   "package p\nfunc f() { _ = w.ClaimCommand(ctx) }\n",
				found:    1,
				unfenced: 1,
			},
			{
				name:     "a write inside some other call",
				source:   "package p\nfunc f() { log(func() error { return w.SaveCursor(ctx) }) }\n",
				found:    1,
				unfenced: 1,
			},
			{
				name:     "a second write beside a fenced one",
				source:   "package p\nfunc f() { fence.Write(func() error { return w.ClaimCommand(ctx) }); _ = w.SaveCursor(ctx) }\n",
				found:    2,
				unfenced: 1,
			},
			{
				name:     "a call that is not a write",
				source:   "package p\nfunc f() { _ = r.LoadCommand(ctx) }\n",
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
				found, unfenced := fencedWriteCalls(fset, parsed, methods)
				total := 0
				for _, count := range found {
					total += count
				}
				if total != testCase.found {
					t.Errorf("found %d durable writes, want %d", total, testCase.found)
				}
				if len(unfenced) != testCase.unfenced {
					t.Errorf("reported %d unfenced writes %v, want %d", len(unfenced), unfenced, testCase.unfenced)
				}
			})
		}
	})
}

// parseProductionFiles parses every non-test Go file of this package.
func parseProductionFiles(t *testing.T, fset *token.FileSet) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	files := map[string]*ast.File{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files[name] = parsed
	}
	return files
}

// fencedWriteMethods returns the fenced-write seams a file set declares, sorted,
// and the set of method names they declare directly.
//
// An EMBEDDED interface contributes nothing here, and that is not a gap: the
// embedded seam is itself collected under its own name, so its methods are in
// the set either way, and reading them twice would only make a caller's floor
// harder to state.
func fencedWriteMethods(files map[string]*ast.File) ([]string, map[string]bool) {
	var seams []string
	methods := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			declared, ok := spec.Type.(*ast.InterfaceType)
			if !ok || !strings.HasSuffix(spec.Name.Name, "Writes") {
				return true
			}
			seams = append(seams, spec.Name.Name)
			for _, field := range declared.Methods.List {
				for _, name := range field.Names {
					methods[name.Name] = true
				}
			}
			return true
		})
	}
	sort.Strings(seams)
	return seams, methods
}

// fencedWriteCalls returns how many times a file calls each durable write, and
// the positions of the calls not lexically inside a Write call.
func fencedWriteCalls(fset *token.FileSet, file *ast.File, methods map[string]bool) (map[string]int, []string) {
	found := map[string]int{}
	var (
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
		if name == "Write" {
			fenced++
			for _, argument := range call.Args {
				ast.Inspect(argument, walk)
			}
			fenced--
			return false
		}
		if methods[name] {
			found[name]++
			if fenced == 0 {
				unfenced = append(unfenced, name+" at "+fset.Position(call.Pos()).String())
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

// indexOf returns the position of the first occurrence of a value, or -1.
func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}
