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
// premise that decays.

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
	writer, err := adapted.DispositionWriterFor(lease)
	if err != nil {
		t.Fatalf("bind the writer to the grant: %v", err)
	}
	return &edgeWorld{t: t, store: store, adapted: adapted, writer: writer, residencyEpoch: uint64(lease.Epoch())}
}

// TestTheDispositionEdgeMapsEveryFieldToTheRightOne is the unit row the three
// surviving swap mutants demanded.
//
// IT READS BACK THROUGH BOTH SIDES. The released store is consulted DIRECTLY for
// what the write edge stored, and the adapter for what the read edge reports, so
// a swap in either direction is visible: reading only through the adapter would
// let a write-edge swap and a read-back swap cancel each other out and report the
// right answer from a record that is wrong.
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
	// THE PREMISE. A claim at residency 1 cannot be settled from BELOW 1 unless
	// there is a below, so the world's own grant must be above the floor.
	if w.residencyEpoch < 2 {
		t.Skipf("the residency grant is %d; there is no strictly lower epoch to settle from", w.residencyEpoch)
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
