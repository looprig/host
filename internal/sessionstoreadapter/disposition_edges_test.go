package sessionstoreadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// This file drives the DISPOSITION EDGE against the released store, and it
// exists because a quality gate proved the edge had no driver in its own package
// at all.
//
// A PANIC PROBE AT THE TOP OF EACH OF THE FOUR FUNCTIONS LEFT THIS PACKAGE'S
// SUITE AT EXIT 0. `LoadDispositionCommand`, `ClaimDisposition`, `BeginAttempt`
// and `SettleDisposition` were reached only transitively, from a fixture in
// `internal/settlement` — and in that fixture the residency epoch, the journal
// epoch, `Revision` and `AcceptedOrder` were **all 1**. Three field-swap mutants
// therefore survived the WHOLE MODULE at exit 0:
//
//	E3 — the two epochs swapped at the WRITE edge
//	E4 — the two epochs swapped at the READ-BACK, which feeds the takeover
//	     discriminator `record.AttemptJournalEpoch < journalEpoch`
//	E5 — AcceptedOrder and Revision swapped at the READ edge, which feeds the
//	     immutable-order guard AND every compare-and-swap's ExpectedRevision
//
// THE LESSON IS STRUCTURAL AND IS WORTH MORE THAN THE THREE MUTANTS. An
// INTEGRATION FIXTURE IS THE WORST PLACE FOR A VALUE TO BE DEGENERATE, because it
// is simultaneously the only driver of several packages: one coincidence there
// blinds every layer at once, and each layer's own suite reports green. That is
// the standing "the constant is almost always 1" rule landing on its most
// expensive instance.
//
// SO THE FOUR NUMBERS HERE ARE FOUR DIFFERENT NUMBERS, and the test ASSERTS that
// they are before it asserts anything else. A premise that is not checked is a
// premise that decays — and it very nearly did: a re-gate deleted the residency
// warm-up in `newEdgeWorld` and the suite stayed at exit 0, turning a
// supersession row into a silent `t.Skip`. That guard is now FATAL, and
// `TestTheEdgeFixturesCountersAreFourDifferentNumbers` states the property once.
//
// THE `classifyInbox` FAMILY IS SWEPT HERE RATHER THAN PATCHED ONE SITE AT A
// TIME, which is the OTHER process lesson of this file. The settlement site was
// fixed because a mutation tripped over it, and the identical site three
// functions away survived another whole round. Every call site, its reachable
// producer, and whether it is rowed or declared equivalent is enumerated in
// `TestALiveGrantIsNeverBelowTheRecordsHighWaterMark`, whose job is to make the
// two equivalence declarations FALSIFIABLE rather than merely argued. **Finding
// an instance is not sweeping the family.**

const (
	edgeTenant  = sessionwire.TenantID("tenant-edges")
	edgeSession = sessionwire.SessionID("session-edges")
	edgeBinding = "binding-edges"

	// edgeJournalEpoch is the RUNTIME's grant, chosen far from every other
	// counter here so a transposition cannot land on a plausible value.
	edgeJournalEpoch uint64 = 41
)

// edgeWorld is one released store with a disposition session and four admitted
// commands.
type edgeWorld struct {
	t       *testing.T
	store   *sessionstore.Store
	adapted *sessionstoreadapter.Store
	writer  commands.DispositionWrites
	lease   residency.Lease

	residencyEpoch uint64
}

func newEdgeWorld(t *testing.T) *edgeWorld {
	t.Helper()
	store, err := sessionstore.Open(t.Context(), memstore.New())
	if err != nil {
		t.Fatalf("open the released store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.WithoutCancel(t.Context())) })
	adapted, err := sessionstoreadapter.New(store)
	if err != nil {
		t.Fatalf("adapt the store: %v", err)
	}

	binding := sessionstore.SessionBinding{
		StorageBindingID: edgeBinding,
		BindingVersion:   "v1",
		RuntimeSessionID: "11111111-2222-3333-4444-555555555555",
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
	now := time.Now().UTC()
	if _, _, err := store.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID: edgeTenant, SessionID: edgeSession, AgentID: "agent-edges",
		RuntimeCompatibilityID: "compat-1",
		CreatedAt:              now, LastActiveAt: now,
		State:            sessionwire.SessionStateRunning,
		Residency:        sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled,
		IdempotencyKey:   "idem-edges",
		Binding:          binding,
	}); err != nil {
		t.Fatalf("create the disposition catalog entry: %v", err)
	}
	// FOUR COMMANDS SO THE TARGET'S ACCEPTANCE ORDER IS FOUR. Only the last is
	// used; the first three exist to move the order away from the revision,
	// which is what E5 needs in order to be visible at all.
	for _, id := range []sessionwire.CommandID{"command-1", "command-2", "command-3", "command-4"} {
		if _, _, err := store.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
			TenantID: edgeTenant, SessionID: edgeSession, CommandID: id, Binding: binding,
			ProposedRuntimeCommandID: "44444444-4444-4444-4444-444444444444",
			Kind:                     sessionstore.CommandKind(commands.KindInput),
			Payload:                  []byte(`{"blocks":[]}`),
			AcceptedAt:               now, ApplyDeadline: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("admit %q: %v", id, err)
		}
	}

	// THE RESIDENCY GRANT IS MOVED OFF 1, for two reasons at once: it keeps the
	// four counters distinct (see the file header), and it creates a strictly
	// LOWER epoch, without which a superseded settlement cannot be expressed at
	// all. Acquiring and releasing is the store's own monotonicity rather than a
	// number this test chose.
	warmup, err := adapted.AcquireSessionLease(t.Context(), edgeTenant, edgeSession)
	if err != nil {
		t.Fatalf("acquire the throwaway residency grant: %v", err)
	}
	if err := warmup.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("release the throwaway residency grant: %v", err)
	}
	lease, err := adapted.AcquireSessionLease(t.Context(), edgeTenant, edgeSession)
	if err != nil {
		t.Fatalf("acquire residency: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
	// THE WARM-UP'S EFFECT IS ASSERTED, NOT ASSUMED, AND THIS GUARD IS THE POINT
	// OF THE ROUND THAT ADDED IT.
	//
	// A re-gate deleted the warm-up above and the suite stayed at EXIT 0: the
	// residency epoch fell back to 1, the supersession row lost its "strictly
	// lower epoch" premise and turned into a SILENT t.Skip, and the whole
	// four-distinct-counters property — the thing this file exists to establish —
	// evaporated without a single failure. That is the degeneracy this file was
	// written to fix, reappearing one level up in the fixture that fixes it.
	//
	// SO THE SKIP IS GONE AND THIS IS A FATAL. A fixture that stops exercising
	// its case must FAIL LOUDLY, never skip: a skipped row and a passing row are
	// the same colour on every dashboard anybody reads.
	if uint64(lease.Epoch()) < 2 {
		t.Fatalf("the residency grant is %d; the warm-up acquire/release above is what puts it above 1, "+
			"and without it this file has no strictly lower epoch to express a supersession from "+
			"and its four counters are no longer four different numbers", lease.Epoch())
	}
	writer, err := adapted.DispositionWriterFor(lease)
	if err != nil {
		t.Fatalf("bind the writer to the grant: %v", err)
	}
	return &edgeWorld{t: t, store: store, adapted: adapted, writer: writer, lease: lease, residencyEpoch: uint64(lease.Epoch())}
}

// TestTheDispositionEdgeMapsEveryFieldToTheRightOne is the unit row the three
// surviving swap mutants demanded.
//
// IT READS BACK THROUGH BOTH SIDES: the released store DIRECTLY for what the
// write edge stored, and the adapter for what the read edge reports.
//
// THE REASON FIRST GIVEN FOR THAT WAS WRONG, AND CORRECTING IT MATTERS. The
// claim was that reading only through the adapter would let a write-edge swap and
// a read-back swap CANCEL and report the right answer from a wrong record. For
// the EPOCH pair that cancellation is unshippable and the both-sides read never
// even reaches it: with the write-edge swap applied, the released store refuses
// `BeginDispositionAttempt` OUTRIGHT — `inbox claim_lost (residency_epoch)` —
// because the attempt's residency is fenced to EQUAL the claim's, so a swapped
// pair never becomes a durable record at all.
//
// THE DESIGN IS STILL LOAD-BEARING, FOR THE OTHER PAIR. `AcceptedOrder` and
// `Revision` are both read-edge fields with no store-side fence between them, so
// a mis-mapping there IS storable and IS cancellable, and only a direct read of
// the store separates "the adapter reports the right numbers" from "the adapter
// reports the right numbers about a record it mis-read". A right design defended
// by a wrong mechanism is one refactor away from being deleted as redundant,
// which is why the mechanism is named rather than asserted.
func TestTheDispositionEdgeMapsEveryFieldToTheRightOne(t *testing.T) {
	w := newEdgeWorld(t)
	const target = sessionwire.CommandID("command-4")

	// ------------------------------------------------------------------
	// The read edge: AcceptedOrder and Revision are different numbers
	// ------------------------------------------------------------------
	record, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}
	// THE PREMISE, ASSERTED. If these two ever coincide, every assertion below
	// about which is which becomes vacuous and this test starts reporting green
	// about a mapping it cannot see.
	if record.AcceptedOrder == record.Revision {
		t.Fatalf("AcceptedOrder and Revision are both %d; a swap between them is invisible and this test proves nothing", record.Revision)
	}
	entry, err := w.store.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{
		TenantID: edgeTenant, SessionID: edgeSession, CommandID: target,
	})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	if record.AcceptedOrder != entry.AcceptedOrder {
		t.Errorf("the adapter reports AcceptedOrder %d, the store holds %d", record.AcceptedOrder, entry.AcceptedOrder)
	}
	if record.Revision != entry.Revision {
		t.Errorf("the adapter reports Revision %d, the store holds %d", record.Revision, entry.Revision)
	}

	// ------------------------------------------------------------------
	// The claim edge: the returned revision is the STORE's new one
	// ------------------------------------------------------------------
	claimed, err := w.writer.ClaimDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionClaim{
		ExpectedRevision: record.Revision,
		ExpiresAt:        time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimDisposition: %v", err)
	}
	if claimed == record.Revision {
		t.Errorf("ClaimDisposition returned the revision it was HANDED (%d); a compare-and-swap naming it would be unconditional", claimed)
	}

	// ------------------------------------------------------------------
	// The write edge: the two epochs land in their own members
	// ------------------------------------------------------------------
	//
	// THIS IS THE ASSERTION E3 FALSIFIED. sessionstore declares JournalEpoch and
	// ResidencyEpoch as two types precisely so an attempt that kept one number
	// could not tell a successor which authority was held; the conversions in
	// the adapter are the only place the two vocabularies meet, so a swap there
	// is a swap of two authorities.
	if w.residencyEpoch == edgeJournalEpoch {
		t.Fatalf("the residency epoch and the journal epoch are both %d; the write edge's swap is invisible", w.residencyEpoch)
	}
	applying, err := w.writer.BeginAttempt(t.Context(), edgeTenant, edgeSession, target, commands.DispositionAttempt{
		ExpectedRevision: claimed,
		AttemptID:        "attempt-edges",
		JournalEpoch:     edgeJournalEpoch,
		ResidencyEpoch:   w.residencyEpoch,
		StartedAt:        time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("BeginAttempt: %v", err)
	}
	stored, err := w.store.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{
		TenantID: edgeTenant, SessionID: edgeSession, CommandID: target,
	})
	if err != nil {
		t.Fatalf("GetDispositionCommand after the attempt: %v", err)
	}
	if stored.Record.Attempt == nil {
		t.Fatal("the store recorded no attempt")
	}
	if uint64(stored.Record.Attempt.JournalEpoch) != edgeJournalEpoch {
		t.Errorf("the store holds JournalEpoch %d, want the RUNTIME's grant %d", stored.Record.Attempt.JournalEpoch, edgeJournalEpoch)
	}
	if uint64(stored.Record.Attempt.ResidencyEpoch) != w.residencyEpoch {
		t.Errorf("the store holds ResidencyEpoch %d, want HOST's grant %d", stored.Record.Attempt.ResidencyEpoch, w.residencyEpoch)
	}

	// ------------------------------------------------------------------
	// The read-back: and THIS is what the takeover discriminator reads
	// ------------------------------------------------------------------
	//
	// THIS IS THE ASSERTION E4 FALSIFIED, and it is the higher-consequence of
	// the two: `record.AttemptJournalEpoch < journalEpoch` decides whether a
	// runtime closes a PREDECESSOR's attempt or its own, so a swap here makes a
	// successor either tombstone its own live attempt or block a predecessor's
	// forever.
	after, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand after the attempt: %v", err)
	}
	if after.AttemptJournalEpoch != edgeJournalEpoch {
		t.Errorf("the adapter reports AttemptJournalEpoch %d, want the runtime's grant %d", after.AttemptJournalEpoch, edgeJournalEpoch)
	}
	if after.AttemptResidencyEpoch != w.residencyEpoch {
		t.Errorf("the adapter reports AttemptResidencyEpoch %d, want Host's grant %d", after.AttemptResidencyEpoch, w.residencyEpoch)
	}
	if after.AttemptID != "attempt-edges" {
		t.Errorf("the adapter reports AttemptID %q, want the one the attempt named", after.AttemptID)
	}
	if after.Revision != applying {
		t.Errorf("the adapter reports Revision %d and BeginAttempt returned %d", after.Revision, applying)
	}
	if after.AcceptedOrder != record.AcceptedOrder {
		t.Errorf("the immutable acceptance order moved from %d to %d across a transition", record.AcceptedOrder, after.AcceptedOrder)
	}
	if after.State != commands.StateApplying {
		t.Errorf("the adapter reports state %q, want %q", after.State, commands.StateApplying)
	}
	if after.ClaimResidencyEpoch != w.residencyEpoch {
		t.Errorf("the adapter reports ClaimResidencyEpoch %d, want %d", after.ClaimResidencyEpoch, w.residencyEpoch)
	}

	// ------------------------------------------------------------------
	// The settlement edge, driven — its two same-typed arguments are not
	// interchangeable and a store with no reader refuses rather than settles
	// ------------------------------------------------------------------
	if _, err := w.writer.SettleDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionSettlement{
		ExpectedRevision: applying,
		ResidencyEpoch:   w.residencyEpoch,
	}); err == nil {
		t.Fatal("SettleDisposition settled a command against a store configured with no evidence reader")
	}
	// AND THE RECORD DID NOT MOVE. A refused settlement that had already written
	// something would be the overwrite the whole protocol exists to prevent.
	unmoved, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand after the refused settlement: %v", err)
	}
	if unmoved.Revision != applying || unmoved.State != commands.StateApplying {
		t.Errorf("the refused settlement left the record at %q revision %d, want %q revision %d",
			unmoved.State, unmoved.Revision, commands.StateApplying, applying)
	}
}

// TestASupersededSettlementReachesTheFenceAsASupersession is the classification
// row a surviving mutant demanded.
//
// DELETING `classifyInbox` FROM THE SETTLEMENT EDGE SURVIVED THE WHOLE MODULE.
// That call is what promotes the store's `InboxErrorEpoch` — a transition refused
// below the record's committed high-water mark, which means a LATER lease has
// taken the session — into `residency.ErrEpochSuperseded`. Every disposition
// write goes through `commands.Fence.Write`, and `residency.epochFence` records
// ownership as gone for exactly that sentinel and `ErrFenceConflict`, treating
// anything else as an ambiguous store failure it RETRIES. So an unclassified
// supersession leaves this Host believing it still owns a session a successor
// holds, retrying once per reconcile interval, forever.
//
// THE GAP PREDATES THE WIRING CLASSIFIER AND WAS MADE VISIBLE BY IT. The edge
// called `classifyInbox` unconditionally before; nothing drove it, so nothing
// noticed. Splitting the function is what turned an untested call into a
// deletable one.
//
// THE CONTROL IS A SETTLEMENT AT THE CLAIM'S OWN RESIDENCY, which must NOT be
// reported as a supersession — it fails for want of evidence, which is a
// different fault with a different repair.
func TestASupersededSettlementReachesTheFenceAsASupersession(t *testing.T) {
	w := newEdgeWorld(t)
	const target = sessionwire.CommandID("command-4")

	record, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}
	claimed, err := w.writer.ClaimDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionClaim{
		ExpectedRevision: record.Revision,
		ExpiresAt:        time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimDisposition: %v", err)
	}
	applying, err := w.writer.BeginAttempt(t.Context(), edgeTenant, edgeSession, target, commands.DispositionAttempt{
		ExpectedRevision: claimed,
		AttemptID:        "attempt-superseded",
		JournalEpoch:     edgeJournalEpoch,
		ResidencyEpoch:   w.residencyEpoch,
		StartedAt:        time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("BeginAttempt: %v", err)
	}
	_, err = w.writer.SettleDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionSettlement{
		ExpectedRevision: applying,
		ResidencyEpoch:   w.residencyEpoch - 1,
	})
	if err == nil {
		t.Fatal("a settlement below the claim's high-water mark was accepted")
	}
	if !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("SettleDisposition = %v, want it to carry residency.ErrEpochSuperseded; "+
			"the fence classifies exactly that sentinel, so anything else is retried forever", err)
	}

	// THE CONTROL. At the claim's own residency the settlement still fails — this
	// store has no evidence reader — but NOT as a supersession.
	_, err = w.writer.SettleDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionSettlement{
		ExpectedRevision: applying,
		ResidencyEpoch:   w.residencyEpoch,
	})
	if err == nil {
		t.Fatal("a settlement succeeded against a store with no evidence reader")
	}
	if errors.Is(err, residency.ErrEpochSuperseded) {
		t.Errorf("a settlement at the claim's own residency was reported as superseded: %v", err)
	}
}

// TestTheEdgeFixturesCountersAreFourDifferentNumbers is this file's premise,
// asserted in one place rather than assumed in every test.
//
// IT IS THE SAME GUARD internal/settlement CARRIES, AND FOR THE SAME REASON ONE
// LEVEL DOWN. A re-gate showed the fixture's residency warm-up could be deleted
// at exit 0, collapsing the residency epoch back onto the record's first
// revision and turning a supersession row into a silent skip. A fixture whose
// distinguishing values have quietly collapsed reports green about mappings it
// can no longer see, which is exactly the defect the rest of this file exists to
// have caught.
func TestTheEdgeFixturesCountersAreFourDifferentNumbers(t *testing.T) {
	w := newEdgeWorld(t)
	record, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, "command-4")
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}
	counters := map[string]uint64{
		"Host's residency epoch":      w.residencyEpoch,
		"the runtime's journal epoch": edgeJournalEpoch,
		"the record's Revision":       record.Revision,
		"the record's AcceptedOrder":  record.AcceptedOrder,
	}
	seen := map[uint64]string{}
	for name, value := range counters {
		if value == 0 {
			t.Errorf("%s is 0, which no provider assigns", name)
		}
		if other, clash := seen[value]; clash {
			t.Errorf("%s and %s are both %d; a swap between them is invisible to every test in this file", name, other, value)
		}
		seen[value] = name
	}
}

// TestASupersededAttemptReachesTheFenceAsASupersession is W7 — the sibling of
// the supersession row above, at the OTHER write edge, found by a re-gate after
// three of us had passed over the family.
//
// THE CONSEQUENCE IS IDENTICAL AND THE EDGE IS DIFFERENT. `BeginDispositionAttempt`
// refuses a residency below the claim's high-water mark with
// `InboxErrorEpoch` on `residency_epoch`, and `classifyInbox` is what turns that
// into the sentinel `commands.Fence.Write` reads. Without it this Host treats a
// supersession as an ambiguous store failure and retries a session a successor
// holds, forever — the same failure the settlement row prevents, one edge over.
//
// FINDING THE INSTANCE IS NOT SWEEPING THE FAMILY, which is the process lesson
// this row carries: the settlement site was fixed because a mutation tripped over
// it, and the identical site three functions away was left alone. The file header
// now enumerates every site and says which are rowed and which are equivalent.
func TestASupersededAttemptReachesTheFenceAsASupersession(t *testing.T) {
	w := newEdgeWorld(t)
	const target = sessionwire.CommandID("command-4")

	record, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}
	claimed, err := w.writer.ClaimDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionClaim{
		ExpectedRevision: record.Revision,
		ExpiresAt:        time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimDisposition: %v", err)
	}

	// BELOW THE CLAIM'S OWN MARK. The claim was written under the grant's
	// residency; anything lower is superseded permanently.
	_, err = w.writer.BeginAttempt(t.Context(), edgeTenant, edgeSession, target, commands.DispositionAttempt{
		ExpectedRevision: claimed,
		AttemptID:        "attempt-superseded",
		JournalEpoch:     edgeJournalEpoch,
		ResidencyEpoch:   w.residencyEpoch - 1,
		StartedAt:        time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("an attempt below the claim's high-water mark was authorized")
	}
	if !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("BeginAttempt = %v, want it to carry residency.ErrEpochSuperseded; "+
			"the fence classifies exactly that sentinel, so anything else is retried forever", err)
	}

	// THE CONTROL, AND IT IS TWO ROWS RATHER THAN ONE, because the edge has two
	// refusals and only the first is a supersession.
	//
	// ABOVE the claim's residency is NOT superseded — it "merely has not claimed
	// this command", which the store reports as claim_lost. A Host that read that
	// as a supersession would give up a session it still holds.
	_, err = w.writer.BeginAttempt(t.Context(), edgeTenant, edgeSession, target, commands.DispositionAttempt{
		ExpectedRevision: claimed,
		AttemptID:        "attempt-above",
		JournalEpoch:     edgeJournalEpoch,
		ResidencyEpoch:   w.residencyEpoch + 1,
		StartedAt:        time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("an attempt at a residency that never claimed this command was authorized")
	}
	if errors.Is(err, residency.ErrEpochSuperseded) {
		t.Errorf("an attempt ABOVE the claim's residency was reported as superseded: %v", err)
	}

	// And the claim's own residency is admitted, so neither refusal above is a
	// property of the edge refusing everything.
	if _, err := w.writer.BeginAttempt(t.Context(), edgeTenant, edgeSession, target, commands.DispositionAttempt{
		ExpectedRevision: claimed,
		AttemptID:        "attempt-ok",
		JournalEpoch:     edgeJournalEpoch,
		ResidencyEpoch:   w.residencyEpoch,
		StartedAt:        time.Now().UTC(),
	}); err != nil {
		t.Fatalf("BeginAttempt at the claim's own residency: %v", err)
	}
}

// TestALiveGrantIsNeverBelowTheRecordsHighWaterMark pins the PREMISE that three
// declared equivalents rest on, so the declarations are falsifiable rather than
// merely argued.
//
// THE FAMILY, SWEPT. `classifyInbox` does exactly one thing: promote
// `sessionstore.InboxErrorEpoch` into `residency.ErrEpochSuperseded`, the
// sentinel `commands.Fence.Write` reads. Dropping it is undetectable unless some
// input reaches that code. Host's disposition edge has FOUR call sites, and the
// released store has exactly THREE reachable producers of that code:
//
//	site                      producer                                  status
//	SettleDisposition         disposition_settlement.go:536             ROWED
//	BeginAttempt              disposition_settlement.go:444             ROWED
//	ClaimDisposition          disposition_claim.go:306                  EQUIVALENT, below
//	LoadDispositionCommand    none — GetDispositionCommand has no fence EQUIVALENT
//	LoadDispositionPayload    none — same read                          EQUIVALENT
//
// THE TWO READS ARE EQUIVALENT BY CONSTRUCTION. `GetDispositionCommand` derives
// a scope, reads the catalog and does one `OrderedIndex.Get`; it compares no
// epoch and calls none of the three fences, so no input to it can produce the one
// code `classifyInbox` transforms.
//
// THE CLAIM EDGE IS EQUIVALENT BY LEASE MONOTONICITY, WHICH IS WHAT THIS TEST
// MEASURES. `ClaimDispositionCommand` reaches `dispositionRecordHighWater` with a
// residency it derived from a `*ResidencyGrant` — *"the epoch comes off the grant
// and from nowhere else"* — and the record's mark is a residency derived the same
// way from an EARLIER grant for the same session. One grant is live at a time and
// epochs rise, so a live grant is never below the mark and that comparison cannot
// refuse. A caller has no way to hand it a lower number, which is the same
// property that made the grant plumbing structural in the first place.
//
// SO THE EQUIVALENCE IS CONDITIONAL, AND THE CONDITION IS ASSERTED HERE. If a
// provider ever issued a live grant below a mark it had already issued, this test
// fails and those two declarations must be re-opened — which is the whole point
// of writing a premise down instead of reasoning about it once.
func TestALiveGrantIsNeverBelowTheRecordsHighWaterMark(t *testing.T) {
	w := newEdgeWorld(t)
	const target = sessionwire.CommandID("command-4")

	record, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand: %v", err)
	}
	claimed, err := w.writer.ClaimDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionClaim{
		ExpectedRevision: record.Revision,
		ExpiresAt:        time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimDisposition: %v", err)
	}
	marked, err := w.adapted.LoadDispositionCommand(t.Context(), edgeTenant, edgeSession, target)
	if err != nil {
		t.Fatalf("LoadDispositionCommand after the claim: %v", err)
	}
	if marked.ClaimResidencyEpoch != w.residencyEpoch {
		t.Fatalf("the claim recorded residency %d, want the grant's own %d", marked.ClaimResidencyEpoch, w.residencyEpoch)
	}

	// A SUCCESSOR'S GRANT, TAKEN THE ONLY WAY THE STORE ALLOWS. Releasing and
	// re-acquiring is what a real failover does; the point is the direction the
	// epoch moves.
	if err := w.lease.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("release the first grant: %v", err)
	}
	successor, err := w.adapted.AcquireSessionLease(t.Context(), edgeTenant, edgeSession)
	if err != nil {
		t.Fatalf("acquire the successor grant: %v", err)
	}
	t.Cleanup(func() { _ = successor.Release(context.WithoutCancel(t.Context())) })

	if uint64(successor.Epoch()) <= marked.ClaimResidencyEpoch {
		t.Fatalf("the successor's LIVE grant is %d and the record's mark is already %d; "+
			"a live grant below a mark makes ClaimDisposition's high-water check reachable, "+
			"and the equivalence declared for it must be re-opened",
			successor.Epoch(), marked.ClaimResidencyEpoch)
	}

	// AND THE SUCCESSOR CAN ACTUALLY CLAIM, which is the behavioural half: the
	// high-water check admits it rather than merely not refusing it in theory.
	successorWriter, err := w.adapted.DispositionWriterFor(successor)
	if err != nil {
		t.Fatalf("bind the successor's writer: %v", err)
	}
	if _, err := successorWriter.ClaimDisposition(t.Context(), edgeTenant, edgeSession, target, commands.DispositionClaim{
		ExpectedRevision: claimed,
		ExpiresAt:        time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("a successor holding a strictly higher grant could not claim: %v", err)
	}
}

// TestTheRejectEdgeRejectsUnderTheLiveClaimsResidency drives RejectDisposition
// through the released store while this Host's claim is LIVE (spec gate S7).
// The released edge admits only the live claim's holder, so the residency the
// adapter passes must be the claim's own: one that passed zero, or any other
// number, would be refused as claim_held and the command would wait out the
// claim's TTL.
func TestTheRejectEdgeRejectsUnderTheLiveClaimsResidency(t *testing.T) {
	w := newEdgeWorld(t)
	entry, err := w.store.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{TenantID: edgeTenant, SessionID: edgeSession, CommandID: "command-4"})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := w.writer.ClaimDisposition(t.Context(), edgeTenant, edgeSession, "command-4", commands.DispositionClaim{
		ExpectedRevision: entry.Revision, ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ClaimDisposition: %v", err)
	}
	if err := w.writer.RejectDisposition(t.Context(), edgeTenant, edgeSession, "command-4", commands.DispositionRejection{
		ExpectedRevision: revision, ResidencyEpoch: w.residencyEpoch,
	}); err != nil {
		t.Fatalf("RejectDisposition under the live claim's residency: %v", err)
	}
	after, err := w.store.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{TenantID: edgeTenant, SessionID: edgeSession, CommandID: "command-4"})
	if err != nil {
		t.Fatal(err)
	}
	if after.Record.State != sessionstore.InboxStateRejected || after.Record.Attempt != nil {
		t.Fatalf("after the reject the record is %q with attempt %+v, want rejected with none", after.Record.State, after.Record.Attempt)
	}
}
