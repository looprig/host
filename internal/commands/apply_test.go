package commands

import (
	"context"
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
)

// testRuntimeCommandID is the once-allocated RuntimeCommandID the winning
// CreateOrdered record carries. Nothing in production mints one.
var testRuntimeCommandID = uuid.MustParse("6f1b0f8e-4f1a-4f7e-9b2a-2f3c4d5e6f70")

// testOtherRuntimeCommandID is a DIFFERENT allocation, for the correlation
// checks. A prefix naming it is evidence about another application.
var testOtherRuntimeCommandID = uuid.MustParse("11111111-2222-3333-4444-555555555555")

// errTestRuntime is what the fake runtime fails a command with.
var errTestRuntime = errors.New("commands_test: the runtime refused the command")

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// storedCommand is one durable inbox record as the fake store holds it,
// including the due_at flag §10.4's terminal CAS clears.
type storedCommand struct {
	record  Record
	payload Payload
	due     bool
}

// finalCall records one terminal CAS.
type finalCall struct {
	CommandID sessionwire.CommandID
	Epoch     uint64
	Result    Result
}

// fakeStore is CommandRecords, Gates and InboxWrites over one map.
//
// IT IS ONE FAKE FOR THREE SEAMS ON PURPOSE: they are three views of one
// durable aggregate, and a fake that let a claim and a load disagree about the
// record would make the exactly-once assertions vacuous.
type fakeStore struct {
	mu sync.Mutex

	commands map[sessionwire.CommandID]*storedCommand
	prefixes map[sessionwire.CommandID]Prefix
	gates    map[sessionwire.GateID]Gate

	// errs maps an operation name to the error it fails with.
	errs map[string]error

	// calls is every operation in order, which is what the ordering assertions
	// — payload after claim, prefix before the runtime — are made from.
	calls []string

	claims     []Claim
	writtenPre []Prefix
	finalized  []finalCall
}

// newFakeStore returns an empty store.
func newFakeStore() *fakeStore {
	return &fakeStore{
		commands: map[sessionwire.CommandID]*storedCommand{},
		prefixes: map[sessionwire.CommandID]Prefix{},
		gates:    map[sessionwire.GateID]Gate{},
		errs:     map[string]error{},
	}
}

// record notes an operation and returns the scripted error for it.
func (s *fakeStore) record(op string) error {
	s.calls = append(s.calls, op)
	return s.errs[op]
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

// LoadApplicationPrefix reports the durable correlation record.
func (s *fakeStore) LoadApplicationPrefix(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID) (Prefix, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("LoadApplicationPrefix"); err != nil {
		return Prefix{}, false, err
	}
	prefix, ok := s.prefixes[command]
	return prefix, ok, nil
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

// ClaimCommand CASes the record into claimed, refusing a claim whose expected
// state or claim epoch no longer matches.
func (s *fakeStore) ClaimCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, claim Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("ClaimCommand"); err != nil {
		return err
	}
	s.claims = append(s.claims, claim)
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	if stored.record.State != claim.ExpectedState || stored.record.ClaimEpoch != claim.ExpectedClaimEpoch {
		return errTestClaimConflict
	}
	stored.record.State = StateClaimed
	stored.record.ClaimEpoch = claim.Epoch
	stored.record.ClaimExpiresAt = claim.ExpiresAt
	return nil
}

// BeginApplying CASes a claimed record into applying.
func (s *fakeStore) BeginApplying(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, epoch uint64, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("BeginApplying"); err != nil {
		return err
	}
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	if stored.record.State != StateClaimed || stored.record.ClaimEpoch != epoch {
		return errTestClaimConflict
	}
	stored.record.State = StateApplying
	stored.record.ClaimExpiresAt = expiresAt
	return nil
}

// RecordApplicationPrefix commits the private correlation record.
func (s *fakeStore) RecordApplicationPrefix(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, prefix Prefix) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("RecordApplicationPrefix"); err != nil {
		return err
	}
	s.writtenPre = append(s.writtenPre, prefix)
	s.prefixes[prefix.CommandID] = prefix
	return nil
}

// FinalizeCommand performs the terminal CAS, which is the ONLY thing in this
// fake that clears due_at.
func (s *fakeStore) FinalizeCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, epoch uint64, result Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.record("FinalizeCommand"); err != nil {
		return err
	}
	s.finalized = append(s.finalized, finalCall{CommandID: command, Epoch: epoch, Result: result})
	if err := result.Validate(); err != nil {
		return err
	}
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	if stored.record.State.Terminal() {
		return errTestClaimConflict
	}
	stored.record.State = result.State()
	stored.due = false
	return nil
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

// terminalResults returns every terminal CAS the store accepted.
func (s *fakeStore) terminalResults() []finalCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]finalCall(nil), s.finalized...)
}

// errTestClaimConflict is a refused compare-and-swap.
var errTestClaimConflict = errors.New("commands_test: the compare-and-swap was refused")

// fakeRuntime is department.CommandApplier, recording exactly what crossed the
// seam.
type fakeRuntime struct {
	mu        sync.Mutex
	envelopes []sessionwire.CommandEnvelope
	err       error
}

// ApplyCommand records the envelope and returns the scripted error.
func (r *fakeRuntime) ApplyCommand(_ context.Context, envelope sessionwire.CommandEnvelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envelopes = append(r.envelopes, envelope)
	return r.err
}

// applied returns every envelope the runtime was driven with.
func (r *fakeRuntime) applied() []sessionwire.CommandEnvelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sessionwire.CommandEnvelope(nil), r.envelopes...)
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
		store:   newFakeStore(),
		runtime: &fakeRuntime{},
		fence:   newFakeFence(),
		epoch:   testEpoch,
	}
	f.host = newTestHost(t, clock)
	f.put(KindInput, StatePending)
	for _, apply := range configure {
		apply(f)
	}
	applier, err := NewApplier(ApplierOptions{
		Host:       f.host,
		Key:        registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: f.epoch,
		Records:    f.store,
		Writes:     f.store,
		Gates:      f.store,
		Runtime:    f.runtime,
		Fence:      f.fence,
	})
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	f.applier = applier
	return f
}

// put stores the fixture's single command at acceptance order 1, in a kind and
// a state, with a due deadline and a payload.
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
			ApplyDeadline:    testClockAt.Add(testApplyDeadline),
		},
		payload: Payload{Body: []byte(`{"blocks":[]}`)},
		due:     true,
	}
	if kind == KindGateResponse {
		stored.payload.GateID = testGate
		f.store.gates[testGate] = Gate{GateID: testGate, Open: true, OwnerHostID: testHostID, OwnerEpoch: testEpoch}
	}
	f.store.commands[commandID(1)] = stored
	return stored
}

// stored returns the fixture's single command record.
func (f *applierFixture) stored() *storedCommand { return f.store.commands[commandID(1)] }

// process runs the applier over the fixture's single command.
func (f *applierFixture) process() (Outcome, error) {
	f.t.Helper()
	return f.applier.Process(context.Background(), command(1, f.stored().record.State))
}

// refusal returns the ApplyRefusal an error carries, or "".
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
		store := newFakeStore()
		return ApplierOptions{
			Host:       newTestHost(t, newManualClock(testClockAt)),
			Key:        registry.Key{TenantID: testTenant, SessionID: testSession},
			LeaseEpoch: testEpoch,
			Records:    store,
			Writes:     store,
			Gates:      store,
			Runtime:    &fakeRuntime{},
			Fence:      newFakeFence(),
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
		{"no writes", func(o *ApplierOptions) { o.Writes = nil }, "Writes"},
		{"no gates", func(o *ApplierOptions) { o.Gates = nil }, "Gates"},
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
// package however unexported its fields are, and it yields a nil Clock, an
// empty ID and a ZERO ClaimTTL — and a zero claim TTL claims a command whose
// claim has already expired, so the next reader takes it over mid-application.
func TestNewApplierRefusesAHostThatDidNotComeFromHostNew(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	applier, err := NewApplier(ApplierOptions{
		Host:       &host.Host{},
		Key:        registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch,
		Records:    store,
		Writes:     store,
		Gates:      store,
		Runtime:    &fakeRuntime{},
		Fence:      newFakeFence(),
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

// TestAnInputCommandIsClaimedAppliedAndFinalized asserts §10.4's protocol in
// the order it is written: claim, then the private payload, then applying, then
// the correlation, then the runtime, then the terminal CAS.
//
// THE ORDER IS THE ASSERTION. Every pair in this sequence has a crash between
// it and its neighbour that the recovery rules answer differently, so a
// reordering is not a style change: a payload loaded before the claim is work
// done for a command another Host owns, and a prefix written after the runtime
// call is missing in the one case it exists for.
func TestAnInputCommandIsClaimedAppliedAndFinalized(t *testing.T) {
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
		"LoadApplicationPrefix",
		"ClaimCommand",
		"LoadPayload",
		"BeginApplying",
		"RecordApplicationPrefix",
		"FinalizeCommand",
	}
	if got := f.store.operations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the durable protocol ran as %v, want %v", got, want)
	}
	if applied := f.runtime.applied(); len(applied) != 1 || applied[0].CommandID != commandID(1) {
		t.Fatalf("the runtime was driven with %v, want exactly one envelope for %q", applied, commandID(1))
	}
	if version := f.runtime.applied()[0].Version; version != sessionwire.CurrentWireVersion {
		t.Errorf("the envelope carries wire version %q, want %q", version, sessionwire.CurrentWireVersion)
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
	if claim.ExpectedState != StatePending || claim.ExpectedClaimEpoch != 0 {
		t.Errorf("the claim expected (%q, %d), want the record as it was read (%q, 0); a claim that expects nothing cannot be refused, and two Hosts would both apply", claim.ExpectedState, claim.ExpectedClaimEpoch, StatePending)
	}
	if prefixes := f.store.writtenPre; len(prefixes) != 1 || prefixes[0] != (Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testEpoch}) {
		t.Errorf("the application prefix is %v, want one correlating both identities with the lease epoch", prefixes)
	}
}

// TestTheClaimNeverExtendsTheApplyDeadline holds §10.4's sentence: a claim has
// a bounded TTL of its own and the command's outer bound does not move.
func TestTheClaimNeverExtendsTheApplyDeadline(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	before := f.stored().record.ApplyDeadline
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if after := f.stored().record.ApplyDeadline; !after.Equal(before) {
		t.Errorf("the apply deadline moved from %v to %v", before, after)
	}
	if len(f.store.claims) != 1 {
		t.Fatalf("the record was claimed %d times, want once", len(f.store.claims))
	}
	if claim := f.store.claims[0]; !claim.ExpiresAt.Before(before) {
		t.Errorf("the claim expires at %v, which is not inside the apply deadline %v", claim.ExpiresAt, before)
	}
}

// TestAnInterruptCommandIsAppliedWithoutAPayload asserts the kind whose whole
// content is its identity still reaches the runtime.
func TestAnInterruptCommandIsAppliedWithoutAPayload(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		stored := f.put(KindInterrupt, StatePending)
		stored.payload = Payload{}
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(applied))
	}
}

// TestACreateOrRestoreIsSatisfiedByResidency asserts the two kinds whose
// consequence is the runtime's existence are finalized WITHOUT driving it.
//
// A Host consumes a session's inbox only while it holds that session's runtime,
// so a create or restore that reaches here has already had its consequence.
// Driving it into the runtime would ask a live session to be created again, and
// entering `applying` would claim an effect that never begins.
func TestACreateOrRestoreIsSatisfiedByResidency(t *testing.T) {
	t.Parallel()

	for _, kind := range []Kind{KindCreate, KindRestore} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) { f.put(kind, StatePending) })
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != StateApplied {
				t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
			}
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v for a command its own existence satisfied", applied)
			}
			for _, operation := range f.store.operations() {
				if operation == "RecordApplicationPrefix" || operation == "BeginApplying" {
					t.Errorf("%s ran for a command that drives no runtime, so it correlates an effect that never begins", operation)
				}
			}
			if results := f.store.terminalResults(); len(results) != 1 || results[0].Result.State() != StateApplied {
				t.Errorf("the terminal results are %v, want one applied", results)
			}
		})
	}
}

// TestAResidentGateResponseIsApplied asserts the ordinary gate case: the
// durable gate is open and still names this Host and this residency, so the
// response is driven into the runtime like any other command.
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
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(applied))
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
// FACTORY NORMALLY REJECTS A COLD RESPONSE BEFORE THE INBOX EXISTS, so every
// row here is a RACE: the gate was resumable when Factory admitted the command
// and is not by the time this Host claims it. The rejection is durable and
// typed, and the runtime is never driven — a runtime that was released and
// relaunched cannot resume what the gate suspended, because continuation is not
// implemented.
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
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v for a gate it cannot resume", applied)
			}
			results := f.store.terminalResults()
			if len(results) != 1 {
				t.Fatalf("the terminal CAS ran %d times, want once", len(results))
			}
			reason, _, rejected := results[0].Result.Rejection()
			if !rejected {
				t.Fatalf("the terminal result is %q, want a rejection", results[0].Result.State())
			}
			if reason != sessionwire.ErrorCodeGateNotResumable {
				t.Errorf("the rejection reason is %q, want %q", reason, sessionwire.ErrorCodeGateNotResumable)
			}
		})
	}
}

// TestAppliedAndRejectedAreMutuallyExclusive holds step 5's first half as a
// property of the VALUE, not of the code paths that build one.
//
// The claim is exactly its enumeration: a Result carries one state, so no
// value of the type can report both — and the two constructors are the only way
// to set that state from outside this package. What a caller CAN build is
// Result{}, because an empty composite literal compiles however unexported the
// fields are, so the zero value is refused rather than defaulted.
func TestAppliedAndRejectedAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	applied := Applied()
	if applied.State() != StateApplied {
		t.Errorf("Applied().State() = %q, want %q", applied.State(), StateApplied)
	}
	if _, _, rejected := applied.Rejection(); rejected {
		t.Error("an applied result reports a rejection as well, so the two are not exclusive")
	}
	if err := applied.Validate(); err != nil {
		t.Errorf("Applied() is invalid: %v", err)
	}

	rejection := Rejected(sessionwire.ErrorCodeGateNotResumable, "the gate is gone")
	if rejection.State() != StateRejected {
		t.Errorf("Rejected().State() = %q, want %q", rejection.State(), StateRejected)
	}
	reason, message, rejected := rejection.Rejection()
	if !rejected || reason != sessionwire.ErrorCodeGateNotResumable || message != "the gate is gone" {
		t.Errorf("Rejected() reports (%q, %q, %v)", reason, message, rejected)
	}

	for _, testCase := range []struct {
		name   string
		result Result
	}{
		{"the zero value", Result{}},
		{"a rejection with no typed reason", Rejected("", "")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if err := testCase.result.Validate(); err == nil {
				t.Fatal("Validate accepted a result that is not one of the two terminal states")
			}
		})
	}
}

// TestAnInvalidResultIsRefusedBeforeItIsWritten asserts the validation above
// has a reader in the write path: finalization refuses rather than committing a
// terminal state the store would have to interpret.
func TestAnInvalidResultIsRefusedBeforeItIsWritten(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	outcome, err := f.applier.finalize(context.Background(), f.stored().record, Result{}, false)
	if refusalOf(err) != RefusalInvalidResult {
		t.Fatalf("got %v, want a %q refusal", err, RefusalInvalidResult)
	}
	if outcome.State == StateApplied || outcome.State == StateRejected {
		t.Errorf("outcome state = %q, which reports a terminal state nothing committed", outcome.State)
	}
	if results := f.store.terminalResults(); len(results) != 0 {
		t.Errorf("the store was asked to commit %v", results)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d fenced writes were spent on a result that could not be committed", writes)
	}
}

// TestATerminalCommandBecomesNotDue holds step 5's second half. A record that
// is still being worked on stays due, because a due page is what a reconciler
// finds outstanding work in; the terminal CAS is the one write that clears it,
// which is why FinalizeCommand takes no due_at.
func TestATerminalCommandBecomesNotDue(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.store.errs["FinalizeCommand"] = errTestStore
	if _, err := f.process(); err != nil {
		t.Fatalf("the pass reported %v, and an owned prefix is not a failure", err)
	}
	if _, due := f.store.stateOf(commandID(1)); !due {
		t.Fatal("a command whose terminal CAS did not commit is no longer due, so no reconciler will ever finish it")
	}

	f.store.mu.Lock()
	delete(f.store.errs, "FinalizeCommand")
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

// TestThePrivatePayloadIsLoadedAfterTheClaimAndNeverReachesTheRuntime holds
// step 4 in both of its halves.
//
// The first half is an ORDER: the payload is loaded from SessionStore once this
// applier owns the command. The second is a TYPE: what crosses into the runtime
// is a sessionwire.CommandEnvelope, and the reflection below is what keeps that
// an argument rather than an assertion — the day Core adds a body-bearing
// member to the envelope, this test says so before somebody fills it in.
func TestThePrivatePayloadIsLoadedAfterTheClaimAndNeverReachesTheRuntime(t *testing.T) {
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
		t.Errorf("the operations ran as %v; the private payload is loaded AFTER the claim, so a Host does not read another Host's command", operations)
	}

	envelope := reflect.TypeOf(sessionwire.CommandEnvelope{})
	var members []string
	for index := 0; index < envelope.NumField(); index++ {
		members = append(members, envelope.Field(index).Name)
	}
	want := []string{"Version", "CommandID"}
	if !reflect.DeepEqual(members, want) {
		t.Errorf("sessionwire.CommandEnvelope now carries %v, want %v; the runtime seam takes this type BECAUSE it has nowhere to put a private payload, and a new member ends that argument", members, want)
	}
}

// TestTheRuntimeCommandIDComesFromTheWinningRecord asserts the mapping is
// LOADED, not minted: §10.4 lets two replicas propose different UUIDs while
// racing one public ID and makes only the winner's usable, so an applier that
// generated its own would correlate an effect with a mapping no retry receives.
func TestTheRuntimeCommandIDComesFromTheWinningRecord(t *testing.T) {
	t.Parallel()

	t.Run("the prefix carries the record's allocation", func(t *testing.T) {
		t.Parallel()
		f := newApplierFixture(t, func(f *applierFixture) {
			f.stored().record.RuntimeCommandID = testOtherRuntimeCommandID
		})
		if _, err := f.process(); err != nil {
			t.Fatalf("Process: %v", err)
		}
		if prefixes := f.store.writtenPre; len(prefixes) != 1 || prefixes[0].RuntimeCommandID != testOtherRuntimeCommandID {
			t.Errorf("the prefix correlates %v, want the record's own allocation %v", prefixes, testOtherRuntimeCommandID)
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
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v", applied)
			}
		})
	}
}

// TestAnAlreadyTerminalRecordIsReportedWithoutWriting asserts the case the page
// cannot see: the record was non-terminal when the page was listed and somebody
// — a predecessor, or Factory's deadline reconciler — finished it in between.
// §10.4 never re-opens a terminal record, so the only thing left is to tell the
// consumer what it became, which is what lets its cursor pass.
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
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v for a command that is already terminal", applied)
			}
		})
	}
}

// TestAMissingPrivatePayloadIsRefused asserts a kind whose payload is required
// does not reach the runtime without one.
func TestAMissingPrivatePayloadIsRefused(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		prepare func(*applierFixture)
	}{
		{"input with no body", func(f *applierFixture) { f.put(KindInput, StatePending).payload = Payload{} }},
		{"a gate response with no gate", func(f *applierFixture) {
			stored := f.put(KindGateResponse, StatePending)
			stored.payload.GateID = ""
		}},
		{"a gate response with no body", func(f *applierFixture) {
			stored := f.put(KindGateResponse, StatePending)
			stored.payload.Body = nil
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, testCase.prepare)
			_, err := f.process()
			if refusalOf(err) != RefusalMissingPayload {
				t.Fatalf("got %v, want a %q refusal", err, RefusalMissingPayload)
			}
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v", applied)
			}
			if results := f.store.terminalResults(); len(results) != 0 {
				t.Errorf("the command was made terminal as %v, and a payload this Host could not read is not evidence about the command", results)
			}
		})
	}
}

// TestAStoreFailureStopsTheProtocolWhereItHappened asserts each read and each
// fenced write is a place the protocol can stop, and that stopping there leaves
// the runtime undriven whenever the failure came before the effect.
func TestAStoreFailureStopsTheProtocolWhereItHappened(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"LoadCommand", "LoadApplicationPrefix", "ClaimCommand", "LoadPayload", "BeginApplying", "RecordApplicationPrefix"} {
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
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v after %s failed", applied, operation)
			}
		})
	}
}

// TestALostGrantStopsTheApplicationBeforeItClaims asserts the fence is
// consulted by the applier's own writes and not only by the consumer's cursor
// write: an applier whose grant ended claims nothing, because a claim under a
// lease a successor holds is work taken away from the Host that owns it.
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
				t.Errorf("the command was claimed %v under a grant already known to be gone", claims)
			}
			if applied := f.runtime.applied(); len(applied) != 0 {
				t.Errorf("the runtime was driven with %v", applied)
			}
		})
	}
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

	clock := newManualClock(testClockAt)
	built := newTestHost(t, clock)
	store := newFakeStore()
	store.commands[commandID(1)] = &storedCommand{
		record: Record{
			TenantID: testTenant, SessionID: testSession, CommandID: commandID(1),
			RuntimeCommandID: testRuntimeCommandID, Kind: KindInput, State: StatePending,
			AcceptedOrder: 1, ApplyDeadline: testClockAt.Add(testApplyDeadline),
		},
		payload: Payload{Body: []byte(`{"blocks":[]}`)},
		due:     true,
	}
	fence := newFakeFence()
	runtime := &fakeRuntime{}
	applier, err := NewApplier(ApplierOptions{
		Host: built, Key: registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch, Records: store, Writes: store, Gates: store, Runtime: runtime, Fence: fence,
	})
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	inbox := &fakeInbox{pages: [][]Command{{command(1, StatePending)}}}
	cursors := &fakeCursors{}
	consumer, err := NewConsumer(Options{
		Host: built, Key: registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch, Inbox: inbox, Cursors: cursors, Processor: applier, Fence: fence,
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
	if applied := runtime.applied(); len(applied) != 1 {
		t.Errorf("the runtime was driven %d times, want once", len(applied))
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
// and terminal CAS would arrive OUTSIDE the old guard; they now go through the
// same Fence.Write the cursor does, and this is what keeps them there when the
// eighth write is added by somebody who never read that note.
//
// THE SUBJECT IS DERIVED, NOT LISTED. The guard collects the methods of every
// interface declared in a production file of this package whose name ends in
// "Writes", and requires every call to one of those names, in any production
// file, to be lexically inside a call to Write. So a method added to InboxWrites
// or CursorWrites is covered the moment it is declared, with no list to update
// — which is the failure mode a list has.
//
// THE CLAIM IS EXACTLY THAT ENUMERATION AND NO WIDER. A durable write declared
// on an interface named otherwise is not covered, and neither is a write
// smuggled through a function value assigned elsewhere. The naming convention is
// what makes the guard extend itself, and it is stated on InboxWrites where
// somebody adding a seam will read it.
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
				source:  "package p\ntype InboxWrites interface {\n\tClaimCommand(int) error\n\tFinalizeCommand(int) error\n}\n",
				seams:   []string{"InboxWrites"},
				methods: []string{"ClaimCommand", "FinalizeCommand"},
			},
			{
				name:    "a reading seam beside it",
				source:  "package p\ntype InboxReads interface{ LoadCommand(int) error }\ntype InboxWrites interface{ ClaimCommand(int) error }\n",
				seams:   []string{"InboxWrites"},
				methods: []string{"ClaimCommand"},
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
