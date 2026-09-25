package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	harnessjournal "github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/runtimecommand"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

const (
	// THE NAMES ARE PREFIXED, and that is a guard's doing rather than a style
	// rule: TestDocCommentsNameTheirOwnDeclaration collects declaration names
	// across the whole module and reports every doc-comment line that OPENS with
	// one. Declaring `settlementTenant` and `settlementSession` here made "settlementTenant" and "settlementSession" the
	// first word of a sentence in twenty unrelated files a defect.
	settlementTenant  = sessionwire.TenantID("tenant-settlement")
	settlementSession = sessionwire.SessionID("session-settlement")
	settlementBinding = "binding-journal-1"
	settlementCommand = sessionwire.CommandID("command-settlement-1")

	// targetAcceptedOrder is the order `admit` leaves the target at, after two
	// fillers. It is stated once and asserted in newWorld's own probe below
	// rather than repeated as a literal at every call site.
	targetAcceptedOrder = 3
)

// ---------------------------------------------------------------------------
// The real composition
// ---------------------------------------------------------------------------

// world is both released stores, wired the way a deployment wires them.
type world struct {
	t *testing.T

	// orchestration holds the catalog, the disposition inbox and the residency.
	orchestration *sessionstore.Store
	adapted       *sessionstoreadapter.Store

	// journal is harness's store, on its OWN backend, and it is both the writer
	// of the disposition frame and the reader the settlement resolves through.
	journal   *harnessstore.Store
	rigID     uuid.UUID
	log       *harnessstore.RuntimeCommandLog
	lease     harnessjournal.Lease
	sessionJ  harnessjournal.SessionJournal
	runtimeID uuid.UUID
}

func newWorld(t *testing.T) *world { return newWorldWithReaders(t, nil) }

// newWorldWithReaders builds the world with a caller-chosen routing table, which
// is how a WIRING failure is reproduced: the stores are healthy and the table is
// wrong, which is exactly the shape an operator cannot diagnose from a collapsed
// refusal code.
func newWorldWithReaders(t *testing.T, readers func(*world) map[string]sessionstore.DispositionEvidenceReader) *world {
	t.Helper()

	// (1) HARNESS'S STORE, ON ITS OWN BACKEND AND ITS OWN LAYOUT. A single
	// backend for both is refused by harness itself at the first catalog write,
	// because a legacy scope that accepted the disposition mode would let two
	// authorities name one journal.
	// WithTenant IS NOT COSMETIC AND THIS TEST MEASURED IT. A harness Store files
	// ONE settlementTenant's sessions for its whole life and REFUSES a settlement evidence
	// request naming a different one, because answering it from this keyspace
	// would be answering the wrong question confidently. Opened at harness's
	// DEFAULT settlementTenant, every settlement here failed with
	// "invalid TenantID" — so a deployment's orchestration settlementTenant and its journal
	// store's settlementTenant must agree, and that is a wiring obligation no type enforces.
	journalStore, err := harnessstore.Open(memstore.New(), harnessstore.WithTenant(settlementTenant))
	if err != nil {
		t.Fatalf("open the harness journal store: %v", err)
	}
	rigID, err := uuid.New()
	if err != nil {
		t.Fatalf("mint a rig id: %v", err)
	}
	// THE RUNTIME'S GRANT IS DELIBERATELY NOT 1, and the throwaway lease below is
	// how a real one gets there.
	//
	// A QUALITY GATE MEASURED FOUR INDEPENDENTLY-SOURCED COUNTERS IN THIS FIXTURE
	// AND FOUND ALL FOUR EQUAL TO 1 — the residency epoch, the journal epoch, the
	// record's Revision and its AcceptedOrder. Three field-swap mutants in
	// internal/sessionstoreadapter survived the WHOLE MODULE at exit 0 as a
	// result, because this fixture was that package's only driver. The unit rows
	// in disposition_edges_test.go are what kill them; this is the other half,
	// because AN INTEGRATION FIXTURE IS THE WORST PLACE FOR A VALUE TO BE
	// DEGENERATE: it is simultaneously the only driver of several packages, so
	// one coincidence here blinds every layer at once while each layer's own
	// suite reports green.
	//
	// Acquiring and RELEASING a first grant makes the second one strictly later,
	// which is the store's own monotonicity rather than a number this test chose.
	// Acquiring and RELEASING grants makes the next one strictly later, which is
	// the store's own monotonicity rather than a number this test chose. Three
	// throwaways put the runtime's grant at 4, clear of Host's residency (2), the
	// record's first revision (1) and the target's acceptance order (3).
	for i := 0; i < 3; i++ {
		throwaway, err := journalStore.AcquireLease(t.Context(), rigID)
		if err != nil {
			t.Fatalf("acquire throwaway journal lease %d: %v", i, err)
		}
		if err := throwaway.Release(context.WithoutCancel(t.Context())); err != nil {
			t.Fatalf("release throwaway journal lease %d: %v", i, err)
		}
	}
	lease, err := journalStore.AcquireLease(t.Context(), rigID)
	if err != nil {
		t.Fatalf("acquire the runtime's journal lease: %v", err)
	}
	sessionJ, err := journalStore.OpenJournal(t.Context(), rigID, lease)
	if err != nil {
		t.Fatalf("open the runtime's journal: %v", err)
	}
	log, err := journalStore.OpenRuntimeCommandLog(rigID, sessionJ)
	if err != nil {
		t.Fatalf("open the runtime command log: %v", err)
	}

	// (2) THE EVIDENCE ROUTER, over the one journal store this deployment has.
	// It is built BEFORE the orchestration store because the orchestration store
	// has to be OPENED with it: a store with no reader refuses every settlement.
	table := map[string]sessionstore.DispositionEvidenceReader{settlementBinding: journalStore}
	if readers != nil {
		table = readers(&world{t: t, journal: journalStore, rigID: rigID})
	}
	router, err := sessionstoreadapter.NewEvidenceRouter(table)
	if err != nil {
		t.Fatalf("build the evidence router: %v", err)
	}

	// (3) THE ORCHESTRATION STORE, opened with that router.
	orchestration, err := sessionstore.Open(t.Context(), memstore.New(),
		sessionstore.WithDispositionEvidence(router))
	if err != nil {
		t.Fatalf("open the orchestration store: %v", err)
	}
	t.Cleanup(func() { _ = orchestration.Close(context.WithoutCancel(t.Context())) })
	adapted, err := sessionstoreadapter.New(orchestration)
	if err != nil {
		t.Fatalf("adapt the orchestration store: %v", err)
	}

	w := &world{
		t: t, orchestration: orchestration, adapted: adapted,
		journal: journalStore, rigID: rigID, log: log, lease: lease, sessionJ: sessionJ,
		runtimeID: uuid.MustParse("44444444-4444-4444-4444-444444444444"),
	}
	w.createSession()
	// AND HOST'S RESIDENCY IS MOVED OFF 1 THE SAME WAY, for the same reason: with
	// the record's first revision also 1, a transposition between the two was
	// invisible to this whole module. It runs after the catalog entry exists,
	// because AcquireResidency reads the immutable binding and refuses a session
	// that has none.
	warmup, err := adapted.AcquireSessionLease(t.Context(), settlementTenant, settlementSession)
	if err != nil {
		t.Fatalf("acquire the throwaway residency grant: %v", err)
	}
	if err := warmup.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("release the throwaway residency grant: %v", err)
	}
	return w
}

// sessionBinding is the immutable pin that joins the two stores.
//
// RuntimeSessionID IS THE RIG'S UUID AND NOTHING ELSE. It is the only thing that
// addresses the journal — harness derives "sessions/<uuid>" from it — and the
// orchestration SessionID is deliberately not used for it, because the two
// identity spaces never have to agree.
func (w *world) sessionBinding() sessionstore.SessionBinding {
	return sessionstore.SessionBinding{
		StorageBindingID: settlementBinding,
		BindingVersion:   "v1",
		RuntimeSessionID: w.rigID.String(),
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
}

func (w *world) createSession() {
	w.t.Helper()
	now := time.Now().UTC()
	if _, _, err := w.orchestration.CreateCatalogEntry(w.t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID: settlementTenant, SessionID: settlementSession, AgentID: "agent-settlement",
		RuntimeCompatibilityID: "compat-1",
		CreatedAt:              now, LastActiveAt: now,
		State:            sessionwire.SessionStateRunning,
		Residency:        sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled,
		IdempotencyKey:   "idem-settlement",
		Binding:          w.sessionBinding(),
	}); err != nil {
		w.t.Fatalf("create the disposition catalog entry: %v", err)
	}
}

// admit puts one pending command in the disposition inbox, as Factory would.
//
// IT ADMITS TWO FILLERS FIRST, so the target's immutable AcceptedOrder is 3 and
// not 1. That is the same degeneracy the journal grant above escapes, on the
// other pair: with AcceptedOrder and Revision both 1, a swap between them at the
// adapter's read edge was invisible to this whole module.
func (w *world) admit(id sessionwire.CommandID) {
	w.t.Helper()
	for _, filler := range []sessionwire.CommandID{"command-filler-1", "command-filler-2"} {
		w.admitOne(filler)
	}
	w.admitOne(id)
}

// admitOne admits exactly one command.
func (w *world) admitOne(id sessionwire.CommandID) {
	w.t.Helper()
	now := time.Now().UTC()
	body, err := json.Marshal(sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id},
		SessionID:       settlementSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"hello"}]`),
	})
	if err != nil {
		w.t.Fatal(err)
	}
	if _, _, err := w.orchestration.AdmitDispositionCommand(w.t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: id, Binding: w.sessionBinding(),
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(w.runtimeID.String()),
		Kind:                     sessionstore.CommandKind(commands.KindInput),
		Payload:                  body,
		AcceptedAt:               now, ApplyDeadline: now.Add(time.Hour),
	}); err != nil {
		w.t.Fatalf("admit %q: %v", id, err)
	}
}

// applier builds Host's REAL disposition applier over both released stores.
func (w *world) applier(runtime department.CommandApplier) *commands.DispositionApplier {
	w.t.Helper()
	lease, err := w.adapted.AcquireSessionLease(w.t.Context(), settlementTenant, settlementSession)
	if err != nil {
		w.t.Fatalf("acquire residency: %v", err)
	}
	w.t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(w.t.Context())) })

	// THE WRITER IS BOUND TO THE GRANT AND NOT TO AN EPOCH. This is the plumbing
	// the released claim edge forces: ClaimDispositionCommand takes a
	// *ResidencyGrant, the grant is private to the adapter's lease, and a caller
	// cannot name one.
	writer, err := w.adapted.DispositionWriterFor(lease)
	if err != nil {
		w.t.Fatalf("bind the disposition writer to the residency grant: %v", err)
	}

	built, err := hostconfig.New(hostconfig.Options{
		HostID:            "host-settlement",
		InternalEndpoint:  "ws://10.0.0.1:7100",
		IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
		Department:        testDepartment(w.t),
		SessionStore:      w.adapted,
		Workspaces:        stubWorkspaces{},
		Clock:             systemClock{},
		Auth:              stubAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           time.Minute,
		RegistryHeartbeat: time.Second,
		RegistryExpiry:    time.Minute,
		ClaimTTL:          30 * time.Second,
		ApplyDeadline:     time.Hour,
		CommandQueueSize:  8,
		ReconcileInterval: time.Minute,
		ReconcileBatch:    16,
	})
	if err != nil {
		w.t.Fatalf("hostconfig.New: %v", err)
	}

	applier, err := commands.NewDispositionApplier(commands.DispositionApplierOptions{
		Host:           built,
		Key:            registry.Key{TenantID: settlementTenant, SessionID: settlementSession},
		ResidencyEpoch: uint64(lease.Epoch()),
		Records:        w.adapted,
		Writes:         writer,
		Runtime:        runtime,
		// THE RUNTIME'S OWN JOURNAL GRANT, read from the harness lease this
		// world actually holds. It is a DIFFERENT number from the residency
		// above and comes from a different authority; the assertions below check
		// which one reached the durable attempt.
		JournalEpochs: journalGrant{lease: w.lease},
		Attempts:      commands.UUIDAttemptIDs{},
		Fence:         passthroughFence{},
	})
	if err != nil {
		w.t.Fatalf("NewDispositionApplier: %v", err)
	}
	return applier
}

// stateOf reads the command's durable state back out of the released store.
func (w *world) stateOf(id sessionwire.CommandID) sessionstore.InboxState {
	w.t.Helper()
	entry, err := w.orchestration.GetDispositionCommand(w.t.Context(), sessionstore.GetDispositionCommandRequest{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: id,
	})
	if err != nil {
		w.t.Fatalf("read %q back: %v", id, err)
	}
	return entry.Record.State
}

// attemptOf reads the immutable attempt the store authorized.
func (w *world) attemptOf(id sessionwire.CommandID) *sessionstore.DispositionAttempt {
	w.t.Helper()
	entry, err := w.orchestration.GetDispositionCommand(w.t.Context(), sessionstore.GetDispositionCommandRequest{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: id,
	})
	if err != nil {
		w.t.Fatalf("read %q back: %v", id, err)
	}
	return entry.Record.Attempt
}

// ---------------------------------------------------------------------------
// The runtime double, writing REAL harness records
// ---------------------------------------------------------------------------

// conformingRuntime is a runtime that does exactly what the released writer
// does: it appends the application prefix, and THEN, as a separate bodiless
// frame, the disposition.
//
// THE TWO APPENDS ARE TWO FRAMES AND CANNOT BE ONE. SessionJournal.Append takes
// exactly one record, encodes one body, frames one envelope and does one
// AppendDefinite; there is no batch, so no effect a runtime performs can share a
// frame with its disposition. A double that wrote them together would be
// measuring a shape the released store cannot produce.
type conformingRuntime struct {
	t     *testing.T
	log   *harnessstore.RuntimeCommandLog
	lease harnessjournal.Lease

	// kind is what the runtime records. "" means it records NOTHING, which is
	// the case the whole evidence boundary exists for.
	kind runtimecommand.DispositionKind

	seen []department.RuntimeCommand
}

func (r *conformingRuntime) ApplyCommand(ctx context.Context, command department.RuntimeCommand) error {
	r.seen = append(r.seen, command)

	admitted := runtimecommand.Admitted{
		CommandID:        runtimecommand.CommandID(command.CommandID),
		RuntimeCommandID: command.RuntimeCommandID,
		Kind:             runtimecommand.Kind(command.Kind),
		LeaseEpoch:       r.lease.Epoch(),
		AttemptID:        runtimecommand.AttemptID(command.AttemptID),
		Principal:        command.Principal,
	}
	if admitted.Kind == runtimecommand.KindCreate || admitted.Kind == runtimecommand.KindInput {
		admitted.Metadata = command.Metadata
	}
	// THE BLOCKS ARE DECODED HERE BECAUSE THEY ARE DECODED THERE. Host hands the
	// private body across opaque — it is not the semantic validator of a command
	// body — and internal/harnessadapter's configured block decoder turns it into
	// content before admission. The released Admitted.Validate refuses an input
	// carrying no content, so a double that skipped this step would be admitting
	// a command the production path cannot.
	if admitted.Kind == runtimecommand.KindInput {
		admitted.Blocks = []content.Block{&content.TextBlock{Text: string(command.Payload)}}
	}
	if err := admitted.Validate(); err != nil {
		return err
	}
	if _, err := r.log.AppendCommandApplication(ctx, admitted.Application()); err != nil {
		return err
	}
	if r.kind == "" {
		return nil
	}
	// DispositionFor IS THE RELEASED CONSTRUCTOR, called rather than restated, so
	// the frame this test writes and the frame harness writes cannot drift.
	if _, err := r.log.AppendCommandDisposition(ctx, admitted.DispositionFor(r.kind, r.lease.Epoch())); err != nil {
		return err
	}
	return nil
}

// journalGrant reports the RUNTIME's journal epoch, and branches on the grant
// still being held exactly as department.LeaseEpochReporter requires.
type journalGrant struct {
	lease harnessjournal.Lease
}

func (g journalGrant) LeaseEpoch() (uint64, bool) {
	if g.lease == nil || !g.lease.Valid() {
		return 0, false
	}
	return g.lease.Epoch(), true
}

// passthroughFence is a held fence. The fencing behaviour is measured in
// internal/commands against a fence a test can END; what this package measures
// is the durable protocol across two released stores.
type passthroughFence struct{}

func (passthroughFence) Held() error                  { return nil }
func (passthroughFence) Lost() <-chan struct{}        { return nil }
func (passthroughFence) Write(run func() error) error { return run() }

// ---------------------------------------------------------------------------
// The acceptance case
// ---------------------------------------------------------------------------

// TestACommandAppliesAndSettlesAcrossBothReleasedStores is the acceptance
// criterion for the whole settlement path.
//
// EVERY DURABLE BYTE IS WRITTEN BY RELEASED CODE. The catalog entry, the
// disposition inbox record, the residency grant, the claim, the immutable
// attempt and the terminal settlement are sessionstore v0.9.0's; the application
// prefix and the kind-5 disposition frame are harness v0.34.0's, appended
// through its own RuntimeCommandLog under a lease harness issued. What this test
// supplies is Host: the applier, the grant plumbing and the evidence router.
//
// SEVEN THINGS ARE ASSERTED, and each is a link that did not exist before this
// release train: the runtime was driven exactly once; the dispatch carried an
// attempt identity; that identity is the one the STORE wrote immutably; the
// attempt names the RUNTIME's journal grant rather than Host's residency; the
// command reached `applied`; the arm was chosen by the store from evidence
// rather than asserted by Host; and the settlement resolved a journal in a
// DIFFERENT store through the settlementBinding.
func TestACommandAppliesAndSettlesAcrossBothReleasedStores(t *testing.T) {
	w := newWorld(t)
	w.admit(settlementCommand)

	runtime := &conformingRuntime{t: t, log: w.log, lease: w.lease, kind: runtimecommand.DispositionApplied}
	applier := w.applier(runtime)

	outcome, err := applier.Process(t.Context(), commands.Command{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: settlementCommand,
		AcceptedOrder: targetAcceptedOrder, State: commands.StatePending,
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// (a) THE RUNTIME WAS DRIVEN EXACTLY ONCE.
	if len(runtime.seen) != 1 {
		t.Fatalf("the runtime was driven %d times, want exactly one authorized dispatch", len(runtime.seen))
	}

	// (b) AND THE DISPATCH CARRIED AN ATTEMPT IDENTITY. An empty one means
	// harness writes no disposition AT ALL, by design, and nothing could settle.
	if runtime.seen[0].AttemptID == "" {
		t.Fatal("the dispatch carried no attempt identity, so no kind-5 frame is written and nothing can ever settle it")
	}

	// (c) IT IS THE IDENTITY THE STORE WROTE IMMUTABLY. A dispatch carrying some
	// other well-formed identity would produce evidence about an attempt this
	// record never authorized, and the verifier would refuse it.
	attempt := w.attemptOf(settlementCommand)
	if attempt == nil {
		t.Fatal("the durable record authorized no attempt")
	}
	if string(attempt.AttemptID) != runtime.seen[0].AttemptID {
		t.Errorf("the store authorized attempt %q and the dispatch carried %q", attempt.AttemptID, runtime.seen[0].AttemptID)
	}

	// (d) THE ATTEMPT NAMES THE RUNTIME'S JOURNAL GRANT. sessionstore says a
	// residency epoch "must never be compared with, or used as, a journal
	// epoch", and the verifier checks the record's claimed grant against the
	// journal's nearest preceding opening fence — so a Host that copied its own
	// residency in here would author an attempt no evidence could verify.
	if uint64(attempt.JournalEpoch) != w.lease.Epoch() {
		t.Errorf("the attempt names journal grant %d, want the runtime's own %d", attempt.JournalEpoch, w.lease.Epoch())
	}

	// (e) THE COMMAND SETTLED APPLIED, in the released store's own vocabulary.
	if got := w.stateOf(settlementCommand); got != sessionstore.InboxStateApplied {
		t.Errorf("the durable record is %q, want %q", got, sessionstore.InboxStateApplied)
	}
	if outcome.State != commands.StateApplied {
		t.Errorf("Outcome.State = %q, want %q", outcome.State, commands.StateApplied)
	}
	if !outcome.PrefixOwned {
		t.Error("Outcome.PrefixOwned = false for a command whose attempt is durably authorized")
	}
}

// TestTheStoreChoosesTheTerminalArmFromTheRuntimesOwnFrame is the discrimination
// (e) above cannot make on its own.
//
// A SINGLE `applied` ROW IS EQUALLY EXPLAINED BY A HOST THAT ASSERTS `applied`.
// What separates the two is a runtime that writes a DIFFERENT kind: the command
// then settles REJECTED, and no code in Host chose that. The two rejecting kinds
// are both rowed because they are authored by different grants — a refusal is a
// live runtime's own answer and a closure is a successor's conclusion — and a
// Host that collapsed them would report one as the other.
func TestTheStoreChoosesTheTerminalArmFromTheRuntimesOwnFrame(t *testing.T) {
	for _, row := range []struct {
		kind runtimecommand.DispositionKind
		want sessionstore.InboxState
	}{
		{runtimecommand.DispositionApplied, sessionstore.InboxStateApplied},
		{runtimecommand.DispositionNoOp, sessionstore.InboxStateApplied},
		{runtimecommand.DispositionRefused, sessionstore.InboxStateRejected},
	} {
		t.Run(string(row.kind), func(t *testing.T) {
			w := newWorld(t)
			w.admit(settlementCommand)
			runtime := &conformingRuntime{t: t, log: w.log, lease: w.lease, kind: row.kind}
			applier := w.applier(runtime)

			outcome, err := applier.Process(t.Context(), commands.Command{
				TenantID: settlementTenant, SessionID: settlementSession, CommandID: settlementCommand,
				AcceptedOrder: targetAcceptedOrder, State: commands.StatePending,
			})
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if got := w.stateOf(settlementCommand); got != row.want {
				t.Errorf("the durable record is %q, want %q", got, row.want)
			}
			if string(outcome.State) != string(row.want) {
				t.Errorf("Outcome.State = %q, want %q", outcome.State, row.want)
			}
		})
	}
}

// TestACommandWithNoDurableDispositionStaysApplying is the NEGATIVE the
// acceptance case needs, and it is the one row a green-only suite cannot see.
//
// THE RUNTIME IS DRIVEN AND THE EFFECT PREFIX IS WRITTEN; only the disposition
// frame is missing, which is exactly the crash window the design names. The
// released settlement therefore has nothing to verify and REFUSES, the record
// stays applying at an unmoved revision, and Host reports a refusal rather than
// a conclusion. Without this row, every assertion in the acceptance case is
// equally satisfied by a Host that settles on its own confidence.
func TestACommandWithNoDurableDispositionStaysApplying(t *testing.T) {
	w := newWorld(t)
	w.admit(settlementCommand)

	runtime := &conformingRuntime{t: t, log: w.log, lease: w.lease} // records nothing
	applier := w.applier(runtime)

	_, err := applier.Process(t.Context(), commands.Command{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: settlementCommand,
		AcceptedOrder: targetAcceptedOrder, State: commands.StatePending,
	})
	var refusal *commands.ApplyError
	if !errors.As(err, &refusal) {
		t.Fatalf("Process = %T (%v), want a *commands.ApplyError", err, err)
	}
	if refusal.Refusal != commands.RefusalEvidenceUnavailable {
		t.Errorf("Process refused with %q, want %q", refusal.Refusal, commands.RefusalEvidenceUnavailable)
	}
	// THE RUNTIME WAS STILL DRIVEN, which is what makes this the dangerous case
	// rather than a dispatch that never happened: there may be a real effect
	// behind this record, and re-driving it is what a Host must never do.
	if len(runtime.seen) != 1 {
		t.Errorf("the runtime was driven %d times, want one", len(runtime.seen))
	}
	if got := w.stateOf(settlementCommand); got != sessionstore.InboxStateApplying {
		t.Errorf("the durable record is %q, want it left %q", got, sessionstore.InboxStateApplying)
	}
}

// TestSettlementResolvesTheJournalThroughTheBindingAndNotTheSessionID is the
// indirection the whole two-store composition rests on.
//
// THE ORCHESTRATION SessionID IS NOT A UUID AND ADDRESSES NO JOURNAL. harness
// derives "sessions/<uuid>" from Binding.RuntimeSessionID and says req.SessionID
// "is deliberately not used to address anything here". This asserts the
// consequence positively: settlement succeeded above while the two identity
// spaces disagreed, and it fails when the settlementBinding names a journal that is not
// this runtime's.
func TestSettlementResolvesTheJournalThroughTheBindingAndNotTheSessionID(t *testing.T) {
	w := newWorld(t)

	// The premise, asserted rather than assumed: the two identities differ, and
	// the orchestration one is not even parseable as a UUID. A fixture where
	// they agreed could not see a reader that routed on the wrong one.
	if _, err := uuid.Parse(string(settlementSession)); err == nil {
		t.Fatalf("the orchestration settlementSession id %q parses as a UUID; this test's premise is that it does not", settlementSession)
	}
	if w.sessionBinding().RuntimeSessionID == string(settlementSession) {
		t.Fatal("the settlementBinding names the orchestration settlementSession id, so this test could not tell the two apart")
	}

	// A router registered for a DIFFERENT settlementBinding cannot answer, and the
	// settlement is refused rather than answered emptily.
	router, err := sessionstoreadapter.NewEvidenceRouter(
		map[string]sessionstore.DispositionEvidenceReader{"binding-somebody-elses": w.journal})
	if err != nil {
		t.Fatalf("build the router: %v", err)
	}
	_, err = router.ReadDispositionEvidence(t.Context(), sessionstore.DispositionEvidenceRequest{
		TenantID: settlementTenant, SessionID: settlementSession, CommandID: settlementCommand,
		Binding: w.sessionBinding(),
		Attempt: sessionstore.DispositionAttempt{AttemptID: "attempt-x"},
	})
	if !errors.Is(err, sessionstoreadapter.ErrUnroutableBinding) {
		t.Fatalf("ReadDispositionEvidence = %v, want the unroutable-settlementBinding refusal", err)
	}
}

// ---------------------------------------------------------------------------
// The host.Host this package builds is a CARRIER for three accessors
// ---------------------------------------------------------------------------
//
// NewDispositionApplier reads exactly Clock, ClaimTTL and ID off it, and refuses
// a Host missing any of them. Everything else here is what host.New validates on
// the way to producing one, so the values are the smallest legal ones rather
// than a configuration anybody should copy.

// systemClock is the real clock, because the claim expiry this applier writes is
// evaluated against the RELEASED STORE's clock and a fake one here would author
// a claim the store reads as already lapsed.
type systemClock struct{}

func (systemClock) Now() time.Time                       { return time.Now().UTC() }
func (systemClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// stubWorkspaces materializes nothing. No runtime is launched here, so no
// workspace is ever asked for; the seam is required by host.New and is satisfied
// with a value that would be caught the moment one was.
type stubWorkspaces struct{}

func (stubWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", errors.New("settlement_test: this package materializes no workspace")
}

// stubAuth refuses everything. Nothing in this package authenticates.
type stubAuth struct{}

func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error {
	return errors.New("settlement_test: this package authenticates nothing")
}

// testDepartment is the smallest department host.New accepts. No runtime is ever
// launched through it: this package drives the applier directly, because what it
// measures is the DURABLE protocol rather than the attach.
func testDepartment(t *testing.T) *department.Department {
	t.Helper()
	target, err := department.NewRigTarget(unusedRig{}, "compat-1", department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	})
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	dept, err := department.New([]department.Registration{{AgentID: "agent-settlement", Target: target}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	return dept
}

// unusedRig refuses to launch, which is honest: a rig this package launched
// would be a second runtime beside the one writing the journal records, and the
// two would disagree about which grant holds the stream.
type unusedRig struct{}

func (unusedRig) NewSession(context.Context, department.RigCreateRequest) (department.RigSession, error) {
	return nil, errUnusedRig
}

func (unusedRig) RestoreSession(context.Context, uuid.UUID, department.RigRestoreRequest) (department.RigSession, error) {
	return nil, errUnusedRig
}

var errUnusedRig = errors.New("settlement_test: this package launches no runtime")

// TestTheFixturesFourCountersAreFourDifferentNumbers is the premise every other
// test in this package rests on, asserted once and in one place.
//
// IT EXISTS BECAUSE THE OPPOSITE WAS TRUE AND NOBODY NOTICED. A quality gate
// instrumented `newWorld`, read the four numbers at the moment `Process` is
// called, and got `1, 1, 1, 1` — four independently-sourced counters, all equal.
// Three field-swap mutants in `internal/sessionstoreadapter` survived the whole
// module at exit 0 on the strength of it, and that package's own suite never
// touched the code they mutated: this fixture was its only driver.
//
// A PROBE IS NOT A TEST AND THAT IS THE POINT OF WRITING IT DOWN. The gate's
// measurement was a `t.Logf` that vanished with the gate. This is the same
// measurement as an assertion, so the day one of these four collapses onto
// another, THIS fails rather than the suite quietly going blind.
func TestTheFixturesFourCountersAreFourDifferentNumbers(t *testing.T) {
	w := newWorld(t)
	w.admit(settlementCommand)

	lease, err := w.adapted.AcquireSessionLease(t.Context(), settlementTenant, settlementSession)
	if err != nil {
		t.Fatalf("acquire residency: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })

	record, err := w.adapted.LoadDispositionCommand(t.Context(), settlementTenant, settlementSession, settlementCommand)
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}

	counters := map[string]uint64{
		"Host's residency epoch":      uint64(lease.Epoch()),
		"the runtime's journal epoch": w.lease.Epoch(),
		"the record's Revision":       record.Revision,
		"the record's AcceptedOrder":  record.AcceptedOrder,
	}
	seen := map[uint64]string{}
	for name, value := range counters {
		if value == 0 {
			t.Errorf("%s is 0, which no provider assigns", name)
		}
		if other, clash := seen[value]; clash {
			t.Errorf("%s and %s are both %d; a swap between them is invisible to every test in this package", name, other, value)
		}
		seen[value] = name
	}

	// AND THE ACCEPTANCE ORDER IS THE ONE EVERY OTHER TEST NAMES. A fixture whose
	// order drifted from the literal those tests pass would refuse every command
	// as foreign, which is a confusing way to discover an off-by-two.
	if record.AcceptedOrder != targetAcceptedOrder {
		t.Errorf("the target's AcceptedOrder is %d and this package's tests name %d", record.AcceptedOrder, targetAcceptedOrder)
	}
}

// TestAWiringFailureIsNotReportedAsAMissingDisposition is the classification a
// spec gate demanded and a quality gate then measured Host was already able to
// make and was discarding.
//
// FOUR CAUSES COLLAPSED TO ONE REFUSAL CODE, and only one of them is the
// runtime's fault. An operator reading `evidence_unavailable` was pointed at a
// runtime that wrote nothing, when the cause could equally be a store this Host
// is holding a TYPED error about:
//
//	benign — the runtime wrote no disposition           → the runtime's business
//	unroutable — no reader registered for the binding    → HOST's own sentinel
//	tenant mismatch — the reader refuses to resolve      → a released typed error
//	mis-keyed router — a healthy store, wrong journal    → genuinely ambiguous
//
// THE COLLAPSE HAPPENED AT ONE PLACE and cost nothing to undo: `settle` mapped
// every settlement failure to one refusal. Classifying leaks nothing — a refusal
// code names no tenant, session, binding or body — and needs no upstream API.
//
// THE FOURTH CASE IS LEFT COLLAPSED ON PURPOSE, and that is not an omission. A
// mis-keyed router points at a store that is healthy, at the correct tenant, and
// truthfully reporting that it holds no such record — which is the same answer
// the benign case gives, correctly. It needs a CONSTRUCTION-time answer (a
// journal store declaring which bindings it serves), which no released API
// offers. A `Tenant()` accessor would NOT close it either: that store is at the
// right tenant.
func TestAWiringFailureIsNotReportedAsAMissingDisposition(t *testing.T) {
	for _, row := range []struct {
		name    string
		readers func(*world) map[string]sessionstore.DispositionEvidenceReader
		refusal commands.ApplyRefusal

		// wantCause is the typed error the refusal must still CARRY. The code
		// says "the wiring"; this says which piece of it.
		wantCause func(error) bool
	}{
		{
			"the binding has no registered reader",
			func(w *world) map[string]sessionstore.DispositionEvidenceReader {
				return map[string]sessionstore.DispositionEvidenceReader{"binding-somebody-elses": w.journal}
			},
			commands.RefusalEvidenceUnroutable,
			func(err error) bool {
				var unroutable *sessionstoreadapter.UnroutableBindingError
				return errors.As(err, &unroutable) && unroutable.Binding == settlementBinding
			},
		},
		{
			"the registered reader serves another tenant",
			func(w *world) map[string]sessionstore.DispositionEvidenceReader {
				other, err := harnessstore.Open(memstore.New(), harnessstore.WithTenant("tenant-somebody-elses"))
				if err != nil {
					t.Fatalf("open the other tenant's journal store: %v", err)
				}
				return map[string]sessionstore.DispositionEvidenceReader{settlementBinding: other}
			},
			commands.RefusalEvidenceUnroutable,
			func(err error) bool {
				var binding *harnessstore.DispositionBindingError
				return errors.As(err, &binding) && binding.Field == "TenantID"
			},
		},
		{
			"the runtime wrote no disposition",
			nil, // the ordinary router; the runtime simply records nothing
			commands.RefusalEvidenceUnavailable,
			// AND THE BENIGN ROW CARRIES NEITHER, which is what stops the two
			// checks above from being satisfied by a cause that always matches.
			func(err error) bool {
				var unroutable *sessionstoreadapter.UnroutableBindingError
				var binding *harnessstore.DispositionBindingError
				return err != nil && !errors.As(err, &unroutable) && !errors.As(err, &binding)
			},
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			w := newWorldWithReaders(t, row.readers)
			w.admit(settlementCommand)
			runtime := &conformingRuntime{t: t, log: w.log, lease: w.lease}
			if row.refusal == commands.RefusalEvidenceUnroutable {
				// The wiring rows dispatch a runtime that DOES write, so the
				// refusal below cannot be explained by a missing frame.
				runtime.kind = runtimecommand.DispositionApplied
			}
			applier := w.applier(runtime)

			_, err := applier.Process(t.Context(), commands.Command{
				TenantID: settlementTenant, SessionID: settlementSession, CommandID: settlementCommand,
				AcceptedOrder: targetAcceptedOrder, State: commands.StatePending,
			})
			var refusal *commands.ApplyError
			if !errors.As(err, &refusal) {
				t.Fatalf("Process = %T (%v), want a *commands.ApplyError", err, err)
			}
			if refusal.Refusal != row.refusal {
				t.Errorf("Process refused with %q, want %q", refusal.Refusal, row.refusal)
			}
			// AND THE CAUSE SURVIVES, WHICH IS THE HALF THE CODE CANNOT CARRY.
			// A re-gate showed the classification's PAYLOAD was unpinned:
			// replacing errors.Join(sentinel, err) with a bare sentinel kept
			// every refusal code correct and threw away the typed error that
			// names WHICH binding or WHICH tenant. The code tells an operator
			// the fault is in the wiring; only the cause tells them where, and a
			// diagnosis that stops at "somewhere in the routing table" is the
			// diagnosis this whole classification exists to improve on.
			if row.wantCause != nil && !row.wantCause(refusal.Cause) {
				t.Errorf("the refusal's cause is %v, which no longer carries the typed error naming what is mis-wired", refusal.Cause)
			}
			// EVERY OUTCOME IS STILL SAFE. Classification changes the diagnosis
			// and must not change the durable answer: the command stays applying
			// at an unmoved revision in all three rows.
			if got := w.stateOf(settlementCommand); got != sessionstore.InboxStateApplying {
				t.Errorf("the durable record is %q, want it left %q", got, sessionstore.InboxStateApplying)
			}
		})
	}
}
