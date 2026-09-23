package commands

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// The disposition store double
// ---------------------------------------------------------------------------
//
// IT IS MODELLED ON THE RELEASED EDGES AND NOT ON WHAT AN APPLIER WANTS. Every
// refusal below is one sessionstore v0.9.0 actually makes, in the order it makes
// it, because a fake looser than the store is how a composed Host was last found
// to be green against a store that would have refused it at StepFence. In
// particular: the claim edge derives its epoch from a GRANT and the request
// carries none; BeginDispositionAttempt fences the residency to EQUAL the
// claim's; and SettleDispositionCommand chooses the terminal arm from EVIDENCE
// the caller does not supply.

// dispositionErrors are the refusals the fake makes, named so a test asserts on
// a reason rather than on a string.
var (
	errDispRevision   = errors.New("commands_test: revision conflict")
	errDispState      = errors.New("commands_test: state conflict")
	errDispFence      = errors.New("commands_test: residency below the record's high-water mark")
	errDispClaimLost  = errors.New("commands_test: the claim has lapsed")
	errDispDeadline   = errors.New("commands_test: the apply deadline has passed")
	errDispNoEvidence = errors.New("commands_test: no disposition evidence")
	errDispAttempt    = errors.New("commands_test: the record has a durably authorized attempt")
)

// storedDisposition is one durable record plus the runtime disposition the
// bound journal holds for it, which is the evidence the store settles from.
type storedDisposition struct {
	record DispositionRecord

	// evidence is the runtime's durable disposition, or "" when the journal
	// holds none. It is deliberately NOT a member of the record: the record and
	// the evidence live in different stores, and a fake that kept one value for
	// both could not express the failure the whole protocol is built around —
	// an authorized attempt whose evidence is unreadable.
	evidence string
}

// fakeDispositionStore is DispositionRecords and DispositionWrites.
type fakeDispositionStore struct {
	mu       sync.Mutex
	clock    *manualClock
	commands map[sessionwire.CommandID]*storedDisposition
	payloads map[sessionwire.CommandID]Payload

	// grantEpoch is the residency the GRANT this store issued carries. It is
	// the store's, not the caller's: the claim edge takes a grant and derives
	// the epoch, so a test that wants a claim at another epoch changes THIS.
	grantEpoch uint64

	calls   []string
	errs    map[string]error
	claims  []DispositionClaim
	begins  []DispositionAttempt
	settles []DispositionSettlement
	rejects []DispositionRejection
}

func newFakeDispositionStore(clock *manualClock) *fakeDispositionStore {
	return &fakeDispositionStore{
		clock:      clock,
		commands:   map[sessionwire.CommandID]*storedDisposition{},
		payloads:   map[sessionwire.CommandID]Payload{},
		grantEpoch: testEpoch,
		errs:       map[string]error{},
	}
}

func (s *fakeDispositionStore) note(op string) error {
	s.calls = append(s.calls, op)
	return s.errs[op]
}

func (s *fakeDispositionStore) operations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// LoadDispositionCommand returns the record as it is now.
func (s *fakeDispositionStore) LoadDispositionCommand(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID,
) (DispositionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("LoadDispositionCommand"); err != nil {
		return DispositionRecord{}, err
	}
	stored, ok := s.commands[command]
	if !ok {
		return DispositionRecord{}, errors.New("commands_test: no such command")
	}
	return stored.record, nil
}

// LoadDispositionPayload returns the private body.
func (s *fakeDispositionStore) LoadDispositionPayload(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID,
) (Payload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("LoadDispositionPayload"); err != nil {
		return Payload{}, err
	}
	return s.payloads[command], nil
}

// ClaimDisposition is the released claim edge's refusal order, which is the
// order in which its answers become PERMANENT.
func (s *fakeDispositionStore) ClaimDisposition(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, claim DispositionClaim,
) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("ClaimDisposition"); err != nil {
		return 0, err
	}
	s.claims = append(s.claims, claim)
	stored, ok := s.commands[command]
	if !ok {
		return 0, errors.New("commands_test: no such command")
	}
	if claim.ExpectedRevision == 0 || stored.record.Revision != claim.ExpectedRevision {
		return 0, errDispRevision
	}
	if stored.record.State.Terminal() {
		return 0, errDispState
	}
	if stored.record.hasAttempt() {
		return 0, errDispAttempt
	}
	if s.grantEpoch < stored.record.ClaimResidencyEpoch {
		return 0, errDispFence
	}
	if !s.clock.Now().Before(stored.record.ApplyDeadline) {
		return 0, errDispDeadline
	}
	stored.record.State = StateClaimed
	stored.record.ClaimResidencyEpoch = s.grantEpoch
	stored.record.ClaimExpiresAt = claim.ExpiresAt
	stored.record.Revision++
	return stored.record.Revision, nil
}

// BeginAttempt is BeginDispositionAttempt's refusal order.
func (s *fakeDispositionStore) BeginAttempt(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, attempt DispositionAttempt,
) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("BeginAttempt"); err != nil {
		return 0, err
	}
	s.begins = append(s.begins, attempt)
	stored, ok := s.commands[command]
	if !ok {
		return 0, errors.New("commands_test: no such command")
	}
	if attempt.ExpectedRevision == 0 || stored.record.Revision != attempt.ExpectedRevision {
		return 0, errDispRevision
	}
	if attempt.AttemptID == "" || attempt.JournalEpoch == 0 || attempt.ResidencyEpoch == 0 || attempt.StartedAt.IsZero() {
		return 0, errors.New("commands_test: the attempt is not well formed")
	}
	if stored.record.State != StateClaimed {
		return 0, errDispState
	}
	// THE FENCE IS EQUALITY AND NOT ORDERING, which is the released edge's own
	// rule: an attempt may not raise the record's high-water mark, so a
	// residency that is not the claim's has not claimed this command.
	if attempt.ResidencyEpoch < stored.record.ClaimResidencyEpoch {
		return 0, errDispFence
	}
	if attempt.ResidencyEpoch != stored.record.ClaimResidencyEpoch {
		return 0, errDispState
	}
	if !s.clock.Now().Before(stored.record.ClaimExpiresAt) {
		return 0, errDispClaimLost
	}
	stored.record.State = StateApplying
	stored.record.AttemptID = attempt.AttemptID
	stored.record.AttemptJournalEpoch = attempt.JournalEpoch
	stored.record.AttemptResidencyEpoch = attempt.ResidencyEpoch
	stored.record.Revision++
	return stored.record.Revision, nil
}

// SettleDisposition settles from the EVIDENCE the bound journal holds, and the
// caller supplies none of it.
func (s *fakeDispositionStore) SettleDisposition(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, settlement DispositionSettlement,
) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("SettleDisposition"); err != nil {
		return "", err
	}
	s.settles = append(s.settles, settlement)
	stored, ok := s.commands[command]
	if !ok {
		return "", errors.New("commands_test: no such command")
	}
	if settlement.ExpectedRevision == 0 || stored.record.Revision != settlement.ExpectedRevision {
		return "", errDispRevision
	}
	if stored.record.State.Terminal() {
		return stored.record.State, nil
	}
	if !stored.record.hasAttempt() {
		return "", errDispState
	}
	// The fence runs BEFORE the evidence read, for the released reason: a
	// superseded caller must be told it is superseded, not sent to look for
	// evidence under a lease that no longer exists.
	if settlement.ResidencyEpoch < stored.record.ClaimResidencyEpoch {
		return "", errDispFence
	}
	switch stored.evidence {
	case "":
		return "", errDispNoEvidence
	case "applied", "no_op":
		stored.record.State = StateApplied
	case "refused", "not_applied":
		stored.record.State = StateRejected
	default:
		return "", errors.New("commands_test: unknown disposition kind")
	}
	stored.record.Revision++
	return stored.record.State, nil
}

// RejectDisposition is RejectDispositionCommand's refusal order: a record with
// an attempt is never rejected here, a terminal record with none is this edge's
// own earlier answer, and a live claim admits only its own holder.
func (s *fakeDispositionStore) RejectDisposition(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, command sessionwire.CommandID, rejection DispositionRejection,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.note("RejectDisposition"); err != nil {
		return err
	}
	s.rejects = append(s.rejects, rejection)
	stored, ok := s.commands[command]
	if !ok {
		return errors.New("commands_test: no such command")
	}
	if stored.record.hasAttempt() {
		return errDispAttempt
	}
	if stored.record.State.Terminal() {
		return nil
	}
	if rejection.ExpectedRevision == 0 || stored.record.Revision != rejection.ExpectedRevision {
		return errDispRevision
	}
	if stored.record.State == StateClaimed && rejection.ResidencyEpoch != stored.record.ClaimResidencyEpoch &&
		s.clock.Now().Before(stored.record.ClaimExpiresAt) {
		return errDispState
	}
	stored.record.State = StateRejected
	stored.record.Revision++
	return nil
}

// ---------------------------------------------------------------------------
// The other doubles
// ---------------------------------------------------------------------------

// countingAttemptIDs mints a distinct identity per call, which is the ONE
// property a minter must have.
type countingAttemptIDs struct {
	mu     sync.Mutex
	prefix string
	n      int
	err    error

	// fixed, when set, is returned instead of a minted identity. It is how a
	// test supplies an identity a conforming minter would never produce, which
	// is the only way to reach the applier's own validation of one.
	//
	// IT IS A POINTER BECAUSE THE EMPTY IDENTITY IS ONE OF THE CASES. A plain
	// string gated on `!= ""` could express every bad identity except the one
	// that matters most — a minter that answered "" with a nil error — and a
	// fixture that cannot reach a case is a fixture that reports it as covered.
	fixed *AttemptID
}

func (m *countingAttemptIDs) NewAttemptID() (AttemptID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	if m.fixed != nil {
		return *m.fixed, nil
	}
	m.n++
	return AttemptID(m.prefix + "-" + string(rune('0'+m.n))), nil
}

// stubJournalEpochs is department.LeaseEpochReporter, whose TWO results a
// caller must branch on.
type stubJournalEpochs struct {
	epoch uint64
	held  bool
}

func (s stubJournalEpochs) LeaseEpoch() (uint64, bool) { return s.epoch, s.held }

// recordingCloser is AttemptClosers.
type recordingCloser struct {
	mu     sync.Mutex
	closed []AttemptID
	err    error

	// onClose runs after a successful closure, which is where a faithful double
	// writes the not_applied evidence the closure produced.
	onClose func(sessionwire.CommandID)
}

func (c *recordingCloser) CloseAttempt(
	_ context.Context, command sessionwire.CommandID, _ uuid.UUID, _ Kind, attempt AttemptID, _ uint64,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = append(c.closed, attempt)
	if c.err != nil {
		return c.err
	}
	if c.onClose != nil {
		c.onClose(command)
	}
	return nil
}

func (c *recordingCloser) closures() []AttemptID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]AttemptID(nil), c.closed...)
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

type dispositionFixture struct {
	t        *testing.T
	clock    *manualClock
	host     *hostconfig.Host
	store    *fakeDispositionStore
	runtime  *fakeRuntime
	journal  stubJournalEpochs
	attempts *countingAttemptIDs
	closer   *recordingCloser
	fence    *fakeFence
	gates    *fakeGates
	epoch    uint64
	applier  *DispositionApplier

	// stranded records every Stranded report, in order.
	stranded []sessionwire.CommandID
	causes   []error
}

const testJournalEpoch uint64 = 41

func newDispositionFixture(t *testing.T, configure ...func(*dispositionFixture)) *dispositionFixture {
	t.Helper()
	clock := newManualClock(testClockAt)
	f := &dispositionFixture{
		t:        t,
		clock:    clock,
		store:    newFakeDispositionStore(clock),
		runtime:  &fakeRuntime{},
		journal:  stubJournalEpochs{epoch: testJournalEpoch, held: true},
		attempts: &countingAttemptIDs{prefix: "attempt"},
		closer:   &recordingCloser{},
		fence:    newFakeFence(),
		epoch:    testEpoch,
	}
	// A REAL DISPATCH ENDS IN A DURABLE DISPOSITION, so the default runtime
	// writes one. A test that wants a runtime which recorded nothing clears it,
	// and that is the case the whole evidence boundary exists for.
	f.runtime.onApply = func(command sessionwire.CommandID) { f.setEvidence(command, "applied") }
	f.host = newTestHost(t, clock)
	f.put(KindInput, StatePending)
	for _, apply := range configure {
		apply(f)
	}
	f.rebuild()
	return f
}

// rebuild constructs the applier from the fixture's current collaborators.
func (f *dispositionFixture) rebuild() {
	t := f.t
	t.Helper()
	// A TYPED NIL IS NOT AN ABSENT COLLABORATOR, and the fixture must not
	// manufacture one: assigning a nil *recordingCloser into an AttemptClosers
	// field produces a NON-nil interface holding a nil pointer, which would
	// panic at the first call instead of taking the "no closer" arm. A
	// composition passes a real nil, so the fixture does too.
	var closer AttemptClosers
	if f.closer != nil {
		closer = f.closer
	}
	var gates Gates
	if f.gates != nil {
		gates = f.gates
	}
	applier, err := NewDispositionApplier(DispositionApplierOptions{
		Host:           f.host,
		Key:            registry.Key{TenantID: testTenant, SessionID: testSession},
		ResidencyEpoch: f.epoch,
		Records:        f.store,
		Writes:         f.store,
		Runtime:        f.runtime,
		JournalEpochs:  f.journal,
		Attempts:       f.attempts,
		Closer:         closer,
		Gates:          gates,
		Fence:          f.fence,
		Stranded: func(command sessionwire.CommandID, cause error) {
			f.stranded = append(f.stranded, command)
			f.causes = append(f.causes, cause)
		},
	})
	if err != nil {
		t.Fatalf("NewDispositionApplier: %v", err)
	}
	f.applier = applier
}

func (f *dispositionFixture) put(kind Kind, state State) *storedDisposition {
	stored := &storedDisposition{
		record: DispositionRecord{
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
	}
	f.store.commands[commandID(1)] = stored
	f.store.payloads[commandID(1)] = Payload{Body: []byte(`{"blocks":[]}`)}
	return stored
}

func (f *dispositionFixture) setEvidence(command sessionwire.CommandID, kind string) {
	f.store.commands[command].evidence = kind
}

func (f *dispositionFixture) stored() *storedDisposition { return f.store.commands[commandID(1)] }

func (f *dispositionFixture) process() (Outcome, error) {
	f.t.Helper()
	return f.applier.Process(context.Background(), command(1, f.stored().record.State))
}

// ---------------------------------------------------------------------------
// The whole path
// ---------------------------------------------------------------------------

// THE ACCEPTANCE CASE: a pending command is claimed, an attempt is authorized,
// the dispatch carries the attempt identity, and the command settles applied
// from the evidence that dispatch produced.
//
// EVERY ASSERTION IS ABOUT A DURABLE CONSEQUENCE and not about a method
// returning nil: the edges in the order the protocol fixes them, the identity
// the runtime actually received, and the state the STORE chose.
func TestDispositionApplyClaimsAuthorizesDispatchesAndSettles(t *testing.T) {
	f := newDispositionFixture(t)

	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("Outcome.State = %q, want %q", outcome.State, StateApplied)
	}
	if !outcome.PrefixOwned {
		t.Error("Outcome.PrefixOwned = false, want true for a command whose attempt was durably authorized")
	}
	if got := f.stored().record.State; got != StateApplied {
		t.Errorf("the durable record is %q, want %q", got, StateApplied)
	}
	want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "BeginAttempt", "SettleDisposition"}
	if got := f.store.operations(); !equalStrings(got, want) {
		t.Errorf("the store saw %v, want %v", got, want)
	}
	driven := f.runtime.commands()
	if len(driven) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(driven))
	}
	if driven[0].AttemptID == "" {
		t.Error("the dispatch carried no attempt identity, so harness writes no disposition and nothing can ever settle it")
	}
	if AttemptID(driven[0].AttemptID) != f.stored().record.AttemptID {
		t.Errorf("the dispatch carried attempt %q and the record authorized %q", driven[0].AttemptID, f.stored().record.AttemptID)
	}
}

// THE ATTEMPT IS AUTHORIZED BEFORE THE DISPATCH AND NEVER AFTER IT. A dispatch
// that preceded its attempt would put a real effect behind a record that
// authorized nothing, and no settler could ever match it — which is exactly what
// the dispatch boundary this replaces existed to prevent.
func TestDispositionAttemptIsDurableBeforeTheDispatch(t *testing.T) {
	var stateAtDispatch State
	var attemptAtDispatch AttemptID
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		f.runtime.before = func() {
			stateAtDispatch = f.stored().record.State
			attemptAtDispatch = f.stored().record.AttemptID
		}
	})
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if stateAtDispatch != StateApplying {
		t.Errorf("the record was %q when the runtime was driven, want %q", stateAtDispatch, StateApplying)
	}
	if attemptAtDispatch == "" {
		t.Error("the record named no attempt when the runtime was driven")
	}
}

// THE ATTEMPT NAMES THE RUNTIME'S OWN JOURNAL GRANT AND NOT THIS HOST'S
// RESIDENCY. The two are different authorities over different stores and are
// never compared; a Host that copied one into the other would author an attempt
// whose evidence can never verify. The fixture keeps them at different values on
// purpose — two fresh counters agreeing early in a session's life is an accident,
// not a relationship, and a fixture where they agreed could not see this at all.
func TestDispositionAttemptNamesTheRuntimeGrantAndTheHostResidency(t *testing.T) {
	f := newDispositionFixture(t)
	if _, err := f.process(); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(f.store.begins) != 1 {
		t.Fatalf("the store saw %d attempts, want one", len(f.store.begins))
	}
	begin := f.store.begins[0]
	if begin.JournalEpoch != testJournalEpoch {
		t.Errorf("the attempt's JournalEpoch = %d, want the runtime's grant %d", begin.JournalEpoch, testJournalEpoch)
	}
	if begin.ResidencyEpoch != testEpoch {
		t.Errorf("the attempt's ResidencyEpoch = %d, want this Host's residency %d", begin.ResidencyEpoch, testEpoch)
	}
}

// A RUNTIME HOLDING NO JOURNAL GRANT IS REFUSED BEFORE ANY DURABLE WRITE, and
// `held` is what is branched on rather than the number. The second row is the
// control that makes it a mechanism: a reporter holding a grant whose epoch is
// zero would pass a check that read only the number, and the store would then
// refuse the attempt after a claim was already durable.
func TestDispositionRefusesARuntimeHoldingNoJournalGrant(t *testing.T) {
	for _, row := range []struct {
		name    string
		journal stubJournalEpochs
		refused bool
	}{
		{"no grant at all", stubJournalEpochs{epoch: 0, held: false}, true},
		{"a stale number with no grant", stubJournalEpochs{epoch: 41, held: false}, true},
		{"a held grant", stubJournalEpochs{epoch: 41, held: true}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) { f.journal = row.journal })
			_, err := f.process()
			if row.refused {
				if refusalOf(err) != RefusalNoJournalGrant {
					t.Fatalf("Process = %v, want %q", err, RefusalNoJournalGrant)
				}
				for _, op := range f.store.operations() {
					if op == "BeginAttempt" {
						t.Error("an attempt was authorized for a runtime holding no journal grant")
					}
				}
				if len(f.runtime.commands()) != 0 {
					t.Error("the runtime was driven with no journal grant")
				}
				return
			}
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
		})
	}
}

// A DISPATCH THE RUNTIME COULD NOT RECORD A DISPOSITION FOR IS ITS OWN REFUSAL,
// because nothing durable was written and the command MAY be re-offered — which
// is not what a transport failure means. The control row is an ordinary
// transport failure at the same seam: the two must part company, or a Host
// either strands a re-offerable command or re-drives a real effect.
func TestDispositionSeparatesAnUnsupportedDispositionFromATransportFailure(t *testing.T) {
	for _, row := range []struct {
		name    string
		err     error
		refusal ApplyRefusal
	}{
		{"the runtime cannot record a disposition", ErrDispositionUnsupported, RefusalDispositionUnsupported},
		{"the runtime failed", errors.New("commands_test: connection reset"), RefusalRuntime},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) { f.runtime.err = row.err })
			_, err := f.process()
			if refusalOf(err) != row.refusal {
				t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), row.refusal)
			}
			if got := f.stored().record.State; got != StateApplying {
				t.Errorf("the durable record is %q, want it left at %q with its revision unmoved", got, StateApplying)
			}
		})
	}
}

// AN UNSETTLEABLE COMMAND BLOCKS THE PASS AND IS NOT CONCLUDED ABOUT. Absence is
// not a disposition: the record stays applying at an unmoved revision, and a
// Host that re-dispatched on an unreadable journal would re-drive an effect that
// may already have committed.
func TestDispositionLeavesACommandApplyingWhenNoEvidenceExists(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) { f.runtime.onApply = nil })

	_, err := f.process()
	if refusalOf(err) != RefusalEvidenceUnavailable {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalEvidenceUnavailable)
	}
	if got := f.stored().record.State; got != StateApplying {
		t.Errorf("the durable record is %q, want %q", got, StateApplying)
	}
	if len(f.runtime.commands()) != 1 {
		t.Errorf("the runtime was driven %d times, want exactly one authorized attempt", len(f.runtime.commands()))
	}
}

// THE STORE CHOOSES THE TERMINAL ARM AND THIS HOST REPORTS IT. All four released
// kinds are rowed, because a Host that mapped them itself would be authoring an
// outcome from a vocabulary it does not own — and the two REJECTING kinds are
// authored by different grants, so a Host that collapsed them would report a
// live runtime's own refusal as a successor's tombstone.
func TestDispositionReportsTheTerminalArmTheStoreChose(t *testing.T) {
	for _, row := range []struct {
		kind string
		want State
	}{
		{"applied", StateApplied},
		{"no_op", StateApplied},
		{"refused", StateRejected},
		{"not_applied", StateRejected},
	} {
		t.Run(row.kind, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				f.runtime.onApply = func(command sessionwire.CommandID) { f.setEvidence(command, row.kind) }
			})
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != row.want {
				t.Errorf("Outcome.State = %q, want %q", outcome.State, row.want)
			}
			if got := f.stored().record.State; got != row.want {
				t.Errorf("the durable record is %q, want %q", got, row.want)
			}
		})
	}
}

// A TERMINAL RECORD IS REPORTED AND NEVER RE-OPENED. The page said it was not
// terminal, which means somebody settled it in between; the only thing left is
// to tell the consumer what it became.
func TestDispositionReportsATerminalRecordWithoutWriting(t *testing.T) {
	for _, state := range []State{StateApplied, StateRejected} {
		t.Run(string(state), func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) { f.put(KindInput, state) })
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.State != state {
				t.Errorf("Outcome.State = %q, want %q", outcome.State, state)
			}
			if got := f.store.operations(); !equalStrings(got, []string{"LoadDispositionCommand"}) {
				t.Errorf("the store saw %v, want only the record read", got)
			}
			if len(f.runtime.commands()) != 0 {
				t.Error("a terminal command was driven into the runtime")
			}
		})
	}
}

// EVERY DURABLE WRITE IS FENCED. Losing the grant immediately before each one
// must refuse it, which is the behavioural half of the structural guard: the
// guard proves the CALL is lexically inside a fence, and this proves the fence
// is the one that matters.
func TestDispositionRefusesEveryWriteUnderALostGrant(t *testing.T) {
	for _, row := range []struct {
		name  string
		setup func(*dispositionFixture)
		write string
	}{
		{"the claim", func(f *dispositionFixture) {}, "ClaimDisposition"},
		{"the attempt", func(f *dispositionFixture) { f.put(KindInput, StateClaimed) }, "BeginAttempt"},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				row.setup(f)
				if row.write == "BeginAttempt" {
					stored := f.stored()
					stored.record.ClaimResidencyEpoch = testEpoch
					stored.record.ClaimExpiresAt = testClockAt.Add(time.Minute)
				}
			})
			f.fence.end()
			if _, err := f.process(); err == nil {
				t.Fatal("Process succeeded under a lost grant")
			}
			for _, op := range f.store.operations() {
				if op == row.write {
					t.Errorf("%s reached the store under a lost grant", row.write)
				}
			}
		})
	}
}

// A PREDECESSOR'S STRANDED ATTEMPT IS CLOSED BY A SUCCESSOR AND THEN SETTLED.
// The attempt names a journal grant STRICTLY BELOW the one this runtime holds,
// which is the only shape a successor may tombstone, and the closure is what
// produces the not_applied evidence the settlement then reads.
func TestDispositionClosesAPredecessorsStrandedAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.ClaimExpiresAt = testClockAt.Add(time.Minute)
		stored.record.AttemptID = "a-predecessors-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch - 1
		stored.record.AttemptResidencyEpoch = testEpoch
		f.closer.onClose = func(command sessionwire.CommandID) { f.setEvidence(command, "not_applied") }
	})

	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateRejected {
		t.Errorf("Outcome.State = %q, want %q", outcome.State, StateRejected)
	}
	if got := f.closer.closures(); len(got) != 1 || got[0] != "a-predecessors-attempt" {
		t.Errorf("the closer saw %v, want the predecessor's one attempt", got)
	}
	if len(f.runtime.commands()) != 0 {
		t.Error("a stranded attempt was re-dispatched, which would re-drive an effect that may already have committed")
	}
}

// D1 (CLAUDE_RESULT_I2.1.md "Fix round"): A SUCCESSOR MUST SETTLE FROM A
// PREDECESSOR'S ALREADY-COMMITTED DISPOSITION WITHOUT EVER NEEDING THE
// CLOSURE TO SUCCEED, and once it has, the predecessor's own late settle
// attempt — reaching the store after the fact, exactly as a paused write
// resumed post-takeover does — must be refused rather than allowed to
// overwrite the successor's outcome.
//
// THE SCENARIO: a predecessor's runtime committed the attempt's disposition
// (durable "applied" evidence) and started the effect, but the predecessor
// died or was paused before its own settle reached the store. A successor
// with a higher residency epoch is asked to apply the still-`applying`
// record — no NEW claim happens here: ClaimDispositionCommand never accepts a
// record that is already `applying`, so ClaimResidencyEpoch already sits at
// the successor's residency from whichever claim put the record into
// `applying` in the first place. The closer is wired to refuse with
// ErrEnduringEffect, exactly as harness <= v0.36.0's AttemptCloser always did
// once ANY effect had committed under the attempt: this is deliberately NOT
// relying on harness v0.37.0's ClosureResult.AlreadyDisposed leniency,
// because the fix must hold independent of that pairing.
//
// BEFORE THE FIX (close-before-settle): the successor called the closer
// first, the closer refused (an effect is already committed), and Process
// returned RefusalEnduringEffect with the record left `applying` forever —
// host_sessions_command_blocked stuck at 1, every later command on the
// session queued behind it, and the closer's refusal reached even though the
// evidence needed no closure at all.
//
// AFTER THE FIX (settle-before-close): the successor settles straight from
// the predecessor's already-durable evidence, the closer is never consulted,
// and the command stream is unblocked.
func TestASuccessorSettlesAPredecessorsAlreadyDisposedAttemptWithoutTheCloser(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.Revision = testRevision
		// ClaimResidencyEpoch already sits at the successor's residency —
		// from the claim that authorized this attempt, before the record
		// went to `applying`, not from any new claim happening now (see the
		// scenario comment above: ClaimDispositionCommand never accepts an
		// already-`applying` record). This mark plays no role in refusing
		// the predecessor's stale settle below; only the revision
		// compare-and-swap does (see the assertion at the end of this test).
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.ClaimExpiresAt = testClockAt.Add(time.Minute)
		// The attempt is the PREDECESSOR's: an earlier journal grant, and a
		// residency below the successor's own (its claim, at authorize time).
		stored.record.AttemptID = "a-predecessors-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch - 1
		stored.record.AttemptResidencyEpoch = testOtherEpoch
		// THE CRUX OF D1: the predecessor's runtime already committed the
		// disposition before this applier is ever asked about the command.
		f.setEvidence(commandID(1), "applied")
		// If the closure were called at all, it would refuse — modelling
		// every harness release's behaviour once an effect has committed
		// under the attempt (v0.37.0's AlreadyDisposed only changes what
		// happens for the ATTEMPT'S OWN disposition on the closure path; this
		// fixture proves the fix without depending on that leniency).
		f.closer.err = ErrEnduringEffect
	})

	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v, want the successor to settle from the predecessor's already-durable evidence without ever calling the closer", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("Outcome.State = %q, want %q", outcome.State, StateApplied)
	}
	if got := f.stored().record.State; got != StateApplied {
		t.Errorf("the durable record is %q, want %q: the command stream must be unblocked", got, StateApplied)
	}
	if got := f.closer.closures(); len(got) != 0 {
		t.Errorf("the closer was called %v; settling from already-durable evidence must never need it", got)
	}
	settledRevision := f.stored().record.Revision

	// THE PREDECESSOR'S OWN LATE SETTLE NOW LANDS, exactly as a write paused
	// before the takeover and resumed afterward does (CLAUDE_RESULT_I2.1.md's
	// Pause/Resume fixture). It carries what the predecessor itself observed
	// before it went stale: the pre-settlement revision, and its OWN
	// (superseded) residency epoch — never the successor's.
	_, err = f.store.SettleDisposition(context.Background(), testTenant, testSession, commandID(1), DispositionSettlement{
		ExpectedRevision: testRevision,
		ResidencyEpoch:   testOtherEpoch,
	})
	if err == nil {
		t.Fatal("the predecessor's stale settle succeeded; it must be refused by the record's revision compare-and-swap alone, now that the successor's settle has already moved it")
	}
	if got := f.stored().record.State; got != StateApplied {
		t.Errorf("after the stale settle attempt the durable record is %q, want it to remain %q as the successor left it", got, StateApplied)
	}
	if got := f.stored().record.Revision; got != settledRevision {
		t.Errorf("the stale settle attempt moved the revision from %d to %d; the successor's outcome must be the one that stands", settledRevision, got)
	}
}

// A CLOSURE REFUSED FOR AN ENDURING EFFECT IS TERMINAL AND MUST NOT BE RETRIED
// INTO A TOMBSTONE. The predecessor's effect committed and only its evidence is
// missing; the pass blocks and an operator has to look. The control is that the
// record is not settled either way.
func TestDispositionRefusesToTombstoneACommittedEffect(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.AttemptID = "a-predecessors-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch - 1
		stored.record.AttemptResidencyEpoch = testEpoch
		f.closer.err = ErrEnduringEffect
	})

	_, err := f.process()
	if refusalOf(err) != RefusalEnduringEffect {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalEnduringEffect)
	}
	if got := f.stored().record.State; got != StateApplying {
		t.Errorf("the durable record is %q, want it left at %q", got, StateApplying)
	}
}

// THIS HOST'S OWN ATTEMPT IS SETTLED AND NEVER CLOSED. A closure is a
// SUCCESSOR's conclusion about a runtime that is gone; a runtime closing its own
// attempt would be tombstoning work it may itself have done. The discriminator
// is the journal grant: equal is this runtime's own, strictly below is a
// predecessor's, and the negative row is what makes that a comparison rather
// than a reachability claim.
func TestDispositionNeverClosesItsOwnAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.AttemptID = "this-runtimes-own-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch
		stored.record.AttemptResidencyEpoch = testEpoch
		stored.evidence = "applied"
	})

	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("Outcome.State = %q, want %q", outcome.State, StateApplied)
	}
	if got := f.closer.closures(); len(got) != 0 {
		t.Errorf("the closer saw %v, want none: a runtime may not close its own attempt", got)
	}
}

// THE NEGATIVE ROW THAT MAKES THE DISCRIMINATOR A COMPARISON AGAIN. Under
// settle-first (D1), the previous test's own-attempt row settles from durable
// evidence before settleOrRecover's `AttemptJournalEpoch >= journalEpoch` check
// is ever reached, so it no longer distinguishes that comparison from mere
// reachability — mutating `>=` to `>` survives the suite (CLAUDE_REVIEW_HOST_V071_FINAL.md,
// N1). This row removes the evidence, so settle fails and Process must reach
// the guard with something for it to actually decide. The closer here carries
// no error, so it WOULD succeed if called: only the guard stops it, not an
// absence of anything to close.
func TestDispositionNeverClosesItsOwnAttemptWithNoEvidence(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.AttemptID = "this-runtimes-own-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch
		stored.record.AttemptResidencyEpoch = testEpoch
		// stored.evidence is left at its zero value: the journal holds none.
	})

	_, err := f.process()
	if refusalOf(err) != RefusalEvidenceUnavailable {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalEvidenceUnavailable)
	}
	if got := f.closer.closures(); len(got) != 0 {
		t.Errorf("the closer saw %v, want none: a runtime may not close its own attempt even with no evidence to settle from and a closer that would succeed", got)
	}
}

// A RECORD THAT IS NOT THE ONE ASKED FOR IS REFUSED. Three rows, because a page
// is a snapshot and a record read fresh may disagree with it in three separable
// ways; each is a different comparison, and a single row would credit one guard
// for all three.
func TestDispositionRefusesAForeignRecord(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*DispositionRecord)
	}{
		{"another tenant", func(r *DispositionRecord) { r.TenantID = "another-tenant" }},
		{"another session", func(r *DispositionRecord) { r.SessionID = "another-session" }},
		{"another acceptance order", func(r *DispositionRecord) { r.AcceptedOrder = 99 }},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) { row.mutate(&f.stored().record) })
			_, err := f.process()
			if refusalOf(err) != RefusalForeignRecord {
				t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalForeignRecord)
			}
			if got := f.store.operations(); !equalStrings(got, []string{"LoadDispositionCommand"}) {
				t.Errorf("the store saw %v, want only the record read", got)
			}
		})
	}
}

// A RECORD CARRYING NO PROVIDER REVISION IS REFUSED BEFORE ANY WRITE. Zero is
// not a revision any provider assigns, so a compare-and-swap naming it is
// unconditional; the store refuses it as an invalid request, and this Host
// refuses to ISSUE it, which is the half a fake cannot supply.
func TestDispositionRefusesAnUnrevisionedRecord(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) { f.stored().record.Revision = 0 })
	_, err := f.process()
	if refusalOf(err) != RefusalUnrevisionedRecord {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalUnrevisionedRecord)
	}
	if got := f.store.operations(); !equalStrings(got, []string{"LoadDispositionCommand"}) {
		t.Errorf("the store saw %v, want only the record read", got)
	}
}

// AN UNKNOWN KIND IS REFUSED AND NEVER REJECTED. A newer Factory may admit a
// kind a newer Host applies and this one does not; a durable rejection here
// would destroy work a newer Host in a rolling deployment would have done.
func TestDispositionRefusesAnUnknownKindWithoutRejectingIt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) { f.stored().record.Kind = "a-kind-from-the-future" })
	_, err := f.process()
	if refusalOf(err) != RefusalUnsupportedKind {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalUnsupportedKind)
	}
	if got := f.stored().record.State; got != StatePending {
		t.Errorf("the durable record is %q, want it left %q for a Host that understands the kind", got, StatePending)
	}
}

// A MINTER THAT CANNOT MINT REFUSES BEFORE THE ATTEMPT IS WRITTEN. An attempt
// authorized under an empty identity would be an attempt no evidence could ever
// name, and the released type refuses one — after a durable write.
func TestDispositionRefusesAnUnmintableAttemptIdentity(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		f.attempts.err = errors.New("commands_test: no entropy")
	})
	_, err := f.process()
	if refusalOf(err) != RefusalInvalidAttempt {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalInvalidAttempt)
	}
	for _, op := range f.store.operations() {
		if op == "BeginAttempt" {
			t.Error("an attempt was authorized with no identity")
		}
	}
}

// AN IDENTITY EITHER DOWNSTREAM MODULE WOULD REFUSE IS REFUSED HERE FIRST, and
// the rows are the three separable comparisons Validate makes. The control is a
// well-formed identity at the boundary length, which a mutant that refused
// everything would fail.
func TestAttemptIDValidate(t *testing.T) {
	for _, row := range []struct {
		name    string
		id      AttemptID
		refused bool
	}{
		{"empty", "", true},
		{"one byte over the bound", AttemptID(strings.Repeat("a", MaxAttemptIDBytes+1)), true},
		{"exactly at the bound", AttemptID(strings.Repeat("a", MaxAttemptIDBytes)), false},
		{"invalid UTF-8", AttemptID("\xff\xfe"), true},
		{"ordinary", "attempt-1", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := row.id.Validate()
			if row.refused && err == nil {
				t.Fatal("Validate accepted an identity a downstream module refuses")
			}
			if !row.refused && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if row.refused && refusalOf(err) != RefusalInvalidAttempt {
				t.Errorf("Validate = %v (%q), want %q", err, refusalOf(err), RefusalInvalidAttempt)
			}
		})
	}
}

// OPTIONS ARE VALIDATED, and each required collaborator is its own row: an
// absent one is a composition that would panic at the first command rather than
// refuse at construction.
func TestNewDispositionApplierRefusesIncompleteOptions(t *testing.T) {
	complete := func() DispositionApplierOptions {
		clock := newManualClock(testClockAt)
		store := newFakeDispositionStore(clock)
		return DispositionApplierOptions{
			Host:           newTestHost(t, clock),
			Key:            registry.Key{TenantID: testTenant, SessionID: testSession},
			ResidencyEpoch: testEpoch,
			Records:        store,
			Writes:         store,
			Runtime:        &fakeRuntime{},
			JournalEpochs:  stubJournalEpochs{epoch: 1, held: true},
			Attempts:       &countingAttemptIDs{prefix: "a"},
			Fence:          newFakeFence(),
		}
	}
	if _, err := NewDispositionApplier(complete()); err != nil {
		t.Fatalf("NewDispositionApplier refused complete options: %v", err)
	}
	for _, row := range []struct {
		name   string
		mutate func(*DispositionApplierOptions)
	}{
		{"Host", func(o *DispositionApplierOptions) { o.Host = nil }},
		{"Key", func(o *DispositionApplierOptions) { o.Key = registry.Key{} }},
		{"ResidencyEpoch", func(o *DispositionApplierOptions) { o.ResidencyEpoch = 0 }},
		{"Records", func(o *DispositionApplierOptions) { o.Records = nil }},
		{"Writes", func(o *DispositionApplierOptions) { o.Writes = nil }},
		{"Runtime", func(o *DispositionApplierOptions) { o.Runtime = nil }},
		{"JournalEpochs", func(o *DispositionApplierOptions) { o.JournalEpochs = nil }},
		{"Attempts", func(o *DispositionApplierOptions) { o.Attempts = nil }},
		{"Fence", func(o *DispositionApplierOptions) { o.Fence = nil }},
	} {
		t.Run(row.name, func(t *testing.T) {
			options := complete()
			row.mutate(&options)
			if _, err := NewDispositionApplier(options); err == nil {
				t.Fatalf("NewDispositionApplier accepted options with no %s", row.name)
			}
		})
	}
}

// THE CLOSER IS OPTIONAL AND ITS ABSENCE IS A BLOCKED PASS, never a conclusion.
// A composition with no closer simply cannot recover a predecessor's stranded
// attempt; what it must NOT do is decide the command's fate without one.
func TestDispositionBlocksAStrandedAttemptWithNoCloser(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.AttemptID = "a-predecessors-attempt"
		stored.record.AttemptJournalEpoch = testJournalEpoch - 1
		stored.record.AttemptResidencyEpoch = testEpoch
		f.closer = nil
	})
	_, err := f.process()
	if refusalOf(err) != RefusalNoAttemptCloser {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalNoAttemptCloser)
	}
	if got := f.stored().record.State; got != StateApplying {
		t.Errorf("the durable record is %q, want it left at %q", got, StateApplying)
	}
}

// AND A CLOSER THAT REPORTS THE SAME ABSENCE AS A REFUSAL GETS THE SAME ANSWER.
//
// THERE ARE TWO ROUTES TO "NO CLOSER" AND ONLY ONE IS A NIL. A composed runtime
// reaches this applier through department's wrapper, which DECLARES CloseAttempt
// for every runtime it wraps — so an absent capability cannot arrive as a failed
// assertion and arrives as ErrNoAttemptCloser instead. A mutant collapsing that
// arm into the generic store failure SURVIVED until this row existed, and the
// cost of the collapse is diagnostic rather than durable: the pass blocks either
// way, but an operator is sent to look for a broken store instead of a runtime
// that simply cannot write a closure.
//
// THE CONTROL IS AN ORDINARY CLOSURE FAILURE at the same seam, which must NOT
// be reported as a missing capability.
func TestDispositionReportsAMissingCloserWhateverRouteItArrivesBy(t *testing.T) {
	for _, row := range []struct {
		name    string
		err     error
		refusal ApplyRefusal
	}{
		{"the runtime reports no closure capability", ErrNoAttemptCloser, RefusalNoAttemptCloser},
		{"the closure failed", errors.New("commands_test: the journal is unreachable"), RefusalStore},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				stored := f.put(KindInput, StateApplying)
				stored.record.ClaimResidencyEpoch = testEpoch
				stored.record.AttemptID = "a-predecessors-attempt"
				stored.record.AttemptJournalEpoch = testJournalEpoch - 1
				stored.record.AttemptResidencyEpoch = testEpoch
				f.closer.err = row.err
			})
			_, err := f.process()
			if refusalOf(err) != row.refusal {
				t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), row.refusal)
			}
			if got := f.stored().record.State; got != StateApplying {
				t.Errorf("the durable record is %q, want it left at %q", got, StateApplying)
			}
		})
	}
}

// equalStrings compares two operation logs.
func equalStrings(got, want []string) bool {
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

// THE PRODUCTION MINTER NEVER REPEATS, which is the ONE property an attempt
// identity has to have: a collision would let one attempt's durable disposition
// settle another attempt's command, which is exactly the correlation the
// identity exists to make impossible. The count is large enough that a mutant
// returning a constant, or a counter reset per call, dies.
func TestUUIDAttemptIDsNeverRepeats(t *testing.T) {
	minter := UUIDAttemptIDs{}
	seen := map[AttemptID]bool{}
	for i := 0; i < 2048; i++ {
		id, err := minter.NewAttemptID()
		if err != nil {
			t.Fatalf("NewAttemptID: %v", err)
		}
		if problem := id.Validate(); problem != nil {
			t.Fatalf("the minted identity is one a downstream module refuses: %v", problem)
		}
		if seen[id] {
			t.Fatalf("NewAttemptID repeated %q after %d mints", id, i)
		}
		seen[id] = true
	}
}

// AN IDENTITY THE MINTER PRODUCED IS STILL VALIDATED, and this row exists
// because a mutant deleting that check SURVIVED the rest of this file.
//
// The production minter cannot produce a bad identity — UUIDAttemptIDs returns a
// canonical UUID — so a reachability argument would retire the guard as
// unreachable. The seam is an INTERFACE: a deployment, a future minter, or a
// wrapper can return anything, and the two things that would then happen are
// both durable. An empty identity is written into the immutable attempt by the
// store and then REFUSED by harness at Admitted.Validate, leaving a command
// applying under an attempt no evidence can ever be about; an over-long one is
// refused by whichever module bounds it first. Refusing before the attempt edge
// is what keeps both off the record.
func TestDispositionRefusesAnIdentityTheMinterShouldNotHaveProduced(t *testing.T) {
	for _, row := range []struct {
		name string
		id   AttemptID
	}{
		{"empty", ""},
		{"over the bound", AttemptID(strings.Repeat("a", MaxAttemptIDBytes+1))},
		{"not valid UTF-8", AttemptID("\xff\xfe")},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				id := row.id
				f.attempts.fixed = &id
			})
			_, err := f.process()
			if refusalOf(err) != RefusalInvalidAttempt {
				t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalInvalidAttempt)
			}
			for _, op := range f.store.operations() {
				if op == "BeginAttempt" {
					t.Error("an attempt was authorized under an identity a downstream module refuses")
				}
			}
			if len(f.runtime.commands()) != 0 {
				t.Error("the runtime was driven under an identity no evidence could ever name")
			}
		})
	}
	// THE CONTROL. The same path with a well-formed identity must reach the
	// attempt edge, or the three rows above are satisfied by an applier that
	// refuses every identity.
	f := newDispositionFixture(t)
	if _, err := f.process(); err != nil {
		t.Fatalf("Process with a well-formed identity: %v", err)
	}
}

// AN APPLYING RECORD THAT NAMES NO ATTEMPT IS REFUSED, and this row exists for
// the same reason the one above does: a mutant disabling the guard SURVIVED.
//
// THE STORE DOES NOT WRITE THIS SHAPE — validateDispositionState requires an
// attempt for an applying record, and among non-terminal records an attempt
// exists exactly when the state is applying. That is precisely why "no
// production producer" is not an argument for deleting the guard: a record that
// disagrees with itself is corruption or a foreign writer, and the two things an
// applier could do with one are to settle it (there is nothing to settle from)
// or to re-dispatch it (there may be an effect behind it). It does neither.
func TestDispositionRefusesAnApplyingRecordThatNamesNoAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateApplying)
		stored.record.ClaimResidencyEpoch = testEpoch
		// and no attempt at all, which is the shape under test
	})
	_, err := f.process()
	if refusalOf(err) != RefusalUnknownState {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalUnknownState)
	}
	if got := f.stored().record.State; got != StateApplying {
		t.Errorf("the durable record is %q, want it untouched at %q", got, StateApplying)
	}
	if len(f.runtime.commands()) != 0 {
		t.Error("a record that disagrees with itself was dispatched")
	}
	if got := f.closer.closures(); len(got) != 0 {
		t.Errorf("the closer was asked to tombstone an attempt that does not exist: %v", got)
	}
}

// THE ATTEMPT'S EXISTENCE IS THE IDENTITY AND NOT THE EPOCH, and this row exists
// because a mutant swapping the two SURVIVED every other test in this file.
//
// WHY THEY WERE INDISTINGUISHABLE: every fixture sets both members or neither,
// so a reader consulting either agreed with all of them — the classic
// structural blind spot, where the varied axis is the VALUE and the unvaried one
// is which FIELD is consulted. The record below sets them in disagreement.
//
// NEITHER FIELD'S DISAGREEMENT IS A SHAPE THE STORE WRITES: an attempt carries a
// non-zero journal grant by the store's own validator. That is the point. The
// two members are written by one immutable transition and can only disagree
// through corruption or a foreign writer, and what must not happen then is for
// an applier to conclude "there was no attempt" about a record that names one —
// because the only thing to do with that conclusion is to dispatch again, on top
// of an effect that may already have committed.
//
// IT IS ASSERTED THROUGH PrefixOwned, which is the one place a caller can read
// the answer: §10.4 gives that field the meaning "the command's recoverable
// application is durably committed", and for the disposition family the attempt
// IS that commitment.
func TestTheAttemptsExistenceIsItsIdentityAndNotItsGrant(t *testing.T) {
	for _, row := range []struct {
		name       string
		attempt    AttemptID
		grant      uint64
		wantPrefix bool
	}{
		{"both present, as the store writes them", "an-attempt", testJournalEpoch, true},
		{"neither, as a pre-dispatch rejection leaves them", "", 0, false},
		{"an identity with no grant", "an-attempt", 0, true},
		{"a grant with no identity", "", testJournalEpoch, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				stored := f.put(KindInput, StateApplied)
				stored.record.AttemptID = row.attempt
				stored.record.AttemptJournalEpoch = row.grant
			})
			outcome, err := f.process()
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if outcome.PrefixOwned != row.wantPrefix {
				t.Errorf("Outcome.PrefixOwned = %v, want %v", outcome.PrefixOwned, row.wantPrefix)
			}
		})
	}
}

// TestASuccessorTakesAPredecessorsClaimBeforeItsAttempt: a command claimed by a
// predecessor that crashed is `claimed` under ANOTHER residency. Treating it as
// this Host's own claim sent it straight to BeginAttempt, which the store
// fences to EQUAL the claim's residency and refuses — every pass, forever
// (measured on two composed Hosts with a gate answer claimed by the crashed
// one). A successor claims it first, which the released claim edge allows.
func TestASuccessorTakesAPredecessorsClaimBeforeItsAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateClaimed)
		stored.record.ClaimResidencyEpoch = testEpoch - 1
		stored.record.ClaimExpiresAt = testClockAt.Add(time.Minute)
	})
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v, %v), want the successor to claim and apply", outcome, err)
	}
	want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "BeginAttempt", "SettleDisposition"}
	if got := f.store.operations(); !equalStrings(got, want) {
		t.Fatalf("the store saw %v, want %v", got, want)
	}
	if got := f.stored().record.ClaimResidencyEpoch; got != testEpoch {
		t.Fatalf("the claim is held at residency %d, want this Host's %d", got, testEpoch)
	}
}

// TestAClaimThisHostHoldsResumesAtTheAttempt is the control: a claim at this
// Host's own residency is resumed without a second claim.
func TestAClaimThisHostHoldsResumesAtTheAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		stored := f.put(KindInput, StateClaimed)
		stored.record.ClaimResidencyEpoch = testEpoch
		stored.record.ClaimExpiresAt = testClockAt.Add(time.Minute)
	})
	if outcome, err := f.process(); err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v, %v)", outcome, err)
	}
	want := []string{"LoadDispositionCommand", "LoadDispositionPayload", "BeginAttempt", "SettleDisposition"}
	if got := f.store.operations(); !equalStrings(got, want) {
		t.Fatalf("the store saw %v, want %v", got, want)
	}
}

// D3. A RUNTIME THAT FAILS A COMMAND AFTER ITS ATTEMPT AND RECORDS NOTHING HAS
// STRANDED IT: the attempt names this runtime's own grant, so no pass under this
// runtime may close it, and every later command queues behind it. The
// composition is told once, with the runtime's failure, so it can hand the
// session to a successor; the record itself is left applying, unconcluded.
func TestDispositionReportsAnAttemptTheRuntimeStranded(t *testing.T) {
	runtimeErr := errors.New("commands_test: the journal append failed")
	f := newDispositionFixture(t, func(f *dispositionFixture) { f.runtime.err = runtimeErr })

	_, err := f.process()
	if refusalOf(err) != RefusalRuntime {
		t.Fatalf("Process = %v (%q), want %q", err, refusalOf(err), RefusalRuntime)
	}
	if len(f.stranded) != 1 || f.stranded[0] != f.stored().record.CommandID || !errors.Is(f.causes[0], runtimeErr) {
		t.Fatalf("stranded reports = %v (causes %v), want exactly this command with the runtime's failure", f.stranded, f.causes)
	}
	if got := f.stored().record.State; got != StateApplying {
		t.Errorf("the durable record is %q, want it left %q for a successor", got, StateApplying)
	}
}

// The controls: a runtime that failed the effect but RECORDED a disposition has
// stranded nothing — the store settles it now — and neither has a runtime that
// wrote nothing durable at all (it may be re-offered), nor a pass the Host
// itself cancelled.
func TestDispositionReportsNothingStrandedWhenTheCommandCanStillSettle(t *testing.T) {
	runtimeErr := errors.New("commands_test: the effect failed")
	t.Run("the runtime recorded refused", func(t *testing.T) {
		f := newDispositionFixture(t)
		f.runtime.err = runtimeErr
		f.runtime.before = func() { f.setEvidence(f.stored().record.CommandID, "refused") }
		outcome, err := f.process()
		if err != nil || outcome.State != StateRejected {
			t.Fatalf("Process = (%q, %v), want the store's rejected settlement", outcome.State, err)
		}
		if len(f.stranded) != 0 {
			t.Errorf("stranded reported %v for a settleable command", f.stranded)
		}
	})
	t.Run("the runtime cannot record a disposition", func(t *testing.T) {
		f := newDispositionFixture(t, func(f *dispositionFixture) { f.runtime.err = ErrDispositionUnsupported })
		if _, err := f.process(); refusalOf(err) != RefusalDispositionUnsupported {
			t.Fatalf("Process = %v, want %q", err, RefusalDispositionUnsupported)
		}
		if len(f.stranded) != 0 {
			t.Errorf("stranded reported %v for a command nothing durable was written for", f.stranded)
		}
	})
	t.Run("the pass was cancelled", func(t *testing.T) {
		f := newDispositionFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		f.runtime.err = runtimeErr
		f.runtime.before = cancel
		if _, err := f.applier.Process(ctx, command(1, f.stored().record.State)); refusalOf(err) != RefusalRuntime {
			t.Fatalf("Process = %v, want %q", err, RefusalRuntime)
		}
		if len(f.stranded) != 0 {
			t.Errorf("stranded reported %v for a pass the Host cancelled", f.stranded)
		}
	})
}
