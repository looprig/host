package compose

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// The double, held to the released store
// ---------------------------------------------------------------------------
//
// O7.2 ran the composed idempotency coverage against durableCommands, a double
// in regression_test.go, because sessionstore v0.6.0 satisfied neither
// commands.Inbox nor commands.Cursors and there was nothing else to run it
// against. Its owed list says the obligation that creates: "when the sessionstore
// release lands, the adapter must be written AND the composed test re-pointed at
// it — a green test against a double is not evidence about the store."
//
// WHAT RE-POINTING CAN AND CANNOT REACH TODAY, stated before the test rather
// than discovered after it. The composed idempotency test drives a whole attach:
// HostLink bind, delivery, the applier, the runtime, the journal. That path
// CANNOT run against one released session, and the reason is structural rather
// than incidental — finding C-2:
//
//   - the new stream and cursor are over the DISPOSITION family, which is the
//     only family a Host can reach, because AcquireResidency pins
//     ProtocolModeDisposition;
//   - sessionstoreadapter's record read and its four compare-and-swap
//     transitions are over the LEGACY family (GetCommand, ClaimCommand,
//     BeginApplyingCommand, CompleteCommand, RejectCommand);
//   - and a disposition-bound session REFUSES OpenSession outright, which
//     TestTheResidencyGrantAndTheJournalGrantAreDifferentLeases already measures,
//     so the applier's journal grant cannot even be taken.
//
// So the composed attach is not re-pointable until the disposition-family
// transition adapter exists. What IS re-pointable, and what the idempotency
// claim actually rests on, is the CONSUMPTION MECHANISM: list strictly after a
// durable cursor, advance the cursor, list again. That is three methods, both of
// Host's re-pointed seams, and it is the whole of "a second delivery of a
// CommandID whose order the cursor has passed cannot be listed at all".
//
// THIS FILE THEREFORE RUNS THE MECHANISM AGAINST BOTH AND REQUIRES THEM TO
// AGREE. A differential is stronger than re-pointing one test would have been:
// re-pointing asks "does the store also pass", and this asks "is the double
// telling the truth about the store", which is the question a green test against
// a double leaves open. It found two divergences on its first run; both were in
// the double and both are fixed rather than accommodated.

// cursorStep is one call in the sequence, and the answer both implementations
// must give.
type cursorStep struct {
	what string

	list      bool
	after     uint64
	limit     int
	wantIDs   []sessionwire.CommandID
	wantOrder uint64

	save      bool
	saveEpoch uint64
	saveOrder uint64

	load bool

	// wantRefused is true when the call must FAIL. It is deliberately not a
	// specific error: the two implementations cannot produce the same error
	// value and the claim is about the DECISION, not about its spelling.
	wantRefused bool
}

// inboxUnderTest is the pair of seams this differential drives.
type inboxUnderTest struct {
	inbox   commands.Inbox
	cursors commands.Cursors
}

// runCursorSteps drives one sequence and reports every disagreement with the
// expectation, rather than stopping at the first.
func runCursorSteps(t *testing.T, name string, subject inboxUnderTest, key registry.Key, steps []cursorStep) {
	t.Helper()
	ctx := context.Background()

	for index, step := range steps {
		label := fmt.Sprintf("%s: step %d (%s)", name, index, step.what)
		switch {
		case step.list:
			page, err := subject.inbox.ListOrdered(ctx, key.TenantID, key.SessionID, step.after, step.limit)
			if step.wantRefused {
				if err == nil {
					t.Errorf("%s: the list was accepted, want a refusal", label)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s: %v", label, err)
				continue
			}
			var ids []sessionwire.CommandID
			for _, record := range page {
				ids = append(ids, record.CommandID)
			}
			if !equalCommandIDs(ids, step.wantIDs) {
				t.Errorf("%s: the page is %v, want %v", label, ids, step.wantIDs)
			}

		case step.save:
			err := subject.cursors.SaveCursor(ctx, key.TenantID, key.SessionID, step.saveEpoch, step.saveOrder)
			if step.wantRefused && err == nil {
				t.Errorf("%s: SaveCursor(epoch=%d, order=%d) was ACCEPTED, want a refusal",
					label, step.saveEpoch, step.saveOrder)
			}
			if !step.wantRefused && err != nil {
				t.Errorf("%s: SaveCursor(epoch=%d, order=%d): %v", label, step.saveEpoch, step.saveOrder, err)
			}

		case step.load:
			cursor, err := subject.cursors.LoadCursor(ctx, key.TenantID, key.SessionID)
			if step.wantRefused {
				if err == nil {
					t.Errorf("%s: LoadCursor was accepted (%d), want a refusal", label, cursor)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s: %v", label, err)
				continue
			}
			if cursor != step.wantOrder {
				t.Errorf("%s: LoadCursor = %d, want %d", label, cursor, step.wantOrder)
			}
		}
	}
}

func equalCommandIDs(got, want []sessionwire.CommandID) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// theConsumptionSequence is the mechanism the idempotency claim rests on, plus
// the two refusals a consumer must design around.
//
// IT IS ONE SEQUENCE DRIVEN TWICE rather than two tests that happen to look
// alike, because a differential in which each side has its own script is not a
// differential.
func theConsumptionSequence() []cursorStep {
	return []cursorStep{
		{what: "a fresh session has consumed nothing", load: true, wantOrder: 0},
		{what: "the head page, bounded by the caller's limit", list: true, after: 0, limit: 2,
			wantIDs: []sessionwire.CommandID{"command-a", "command-b"}},
		{what: "consume through the second command", save: true, saveEpoch: 5, saveOrder: 2},
		{what: "the cursor reads back as the ORDER, not the epoch", load: true, wantOrder: 2},

		// THE IDEMPOTENCE ITSELF. Listing from the cursor cannot see a command
		// the cursor has passed, which is why a second delivery of command-a
		// can never be applied twice.
		{what: "listing from the cursor cannot see a consumed command", list: true, after: 2, limit: 10,
			wantIDs: []sessionwire.CommandID{"command-c"}},
		{what: "consume the tail", save: true, saveEpoch: 5, saveOrder: 3},
		{what: "the exhausted stream is an empty page", list: true, after: 3, limit: 10},

		// THE SAME EPOCH SAVES MANY TIMES, and an EQUAL order is admitted so a
		// save retried after an ambiguous outcome succeeds.
		{what: "an equal save is admitted", save: true, saveEpoch: 5, saveOrder: 3},

		// REFUSAL 1: a position behind the committed one. InboxErrorOrder, which
		// means re-read and retry. The double used to treat it as a silent no-op.
		{what: "a stale position is REFUSED", save: true, saveEpoch: 5, saveOrder: 2, wantRefused: true},
		{what: "and the committed position did not move", load: true, wantOrder: 3},

		// REFUSAL 2: an epoch below the record's high-water mark. The double had
		// no epoch fence at all, so it accepted this and advanced the cursor.
		{what: "a superseded epoch is REFUSED", save: true, saveEpoch: 4, saveOrder: 9, wantRefused: true},
		{what: "and the committed position did not move", load: true, wantOrder: 3},
	}
}

// TestTheReleasedStoreRunsTheConsumptionMechanismTheIdempotencyClaimRestsOn is
// the re-point: the same sequence, against sessionstore v0.7.0.
func TestTheReleasedStoreRunsTheConsumptionMechanismTheIdempotencyClaimRestsOn(t *testing.T) {
	t.Parallel()

	released, err := sessionstore.Open(t.Context(), memstore.New())
	if err != nil {
		t.Fatalf("open released store: %v", err)
	}
	t.Cleanup(func() { _ = released.Close(context.WithoutCancel(t.Context())) })
	adapted, err := sessionstoreadapter.New(released)
	if err != nil {
		t.Fatalf("adapt store: %v", err)
	}

	binding := sessionstore.SessionBinding{
		StorageBindingID: "binding-1",
		BindingVersion:   "v1",
		RuntimeSessionID: "runtime-session-1",
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
	now := time.Now().UTC()
	if _, _, err := released.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID: tenantA, SessionID: sessionA, AgentID: testAgent,
		RuntimeCompatibilityID: string(testCompat),
		CreatedAt:              now, LastActiveAt: now,
		State:     sessionwire.SessionStateRunning,
		Residency: sessionwire.SessionResidencyCold,

		DesiredPlacement: sessionwire.HostPlacementPooled,
		IdempotencyKey:   "idem-differential",
		Binding:          binding,
	}); err != nil {
		t.Fatalf("create disposition catalog entry: %v", err)
	}
	for _, id := range []sessionwire.CommandID{"command-a", "command-b", "command-c"} {
		if _, _, err := released.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
			TenantID: tenantA, SessionID: sessionA, CommandID: id, Binding: binding,
			ProposedRuntimeCommandID: "44444444-4444-4444-4444-444444444444",
			Kind:                     sessionstore.CommandKind(commands.KindInput),
			Payload:                  []byte(`{"blocks":[]}`),
			AcceptedAt:               now, ApplyDeadline: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("admit %q: %v", id, err)
		}
	}

	// THE ACCEPTANCE ORDERS MUST BE 1, 2, 3 for the sequence's literal bounds to
	// mean what they say. This is asserted rather than assumed: the store
	// documents the order as increasing and NOT contiguous, so a provider that
	// allocated 10, 20, 30 would make every bound below a different test.
	page, err := adapted.ListOrdered(t.Context(), tenantA, sessionA, 0, 10)
	if err != nil {
		t.Fatalf("reading the acceptance orders: %v", err)
	}
	if len(page) != 3 || page[0].AcceptedOrder != 1 || page[1].AcceptedOrder != 2 || page[2].AcceptedOrder != 3 {
		t.Fatalf("the store allocated orders %+v; this sequence's literal bounds assume 1, 2, 3", page)
	}

	runCursorSteps(t, "released store",
		inboxUnderTest{inbox: adapted, cursors: adapted},
		registry.Key{TenantID: tenantA, SessionID: sessionA},
		theConsumptionSequence())
}

// TestTheInboxDoubleAnswersTheConsumptionMechanismExactlyAsTheStoreDoes is the
// other half of the differential.
//
// A DOUBLE LOOSER THAN ITS DEPENDENCY IS THE DEFECT CLASS THIS REPOSITORY KEEPS
// FINDING, and this is the arm that reports it. Two divergences were found on
// the first run of this file and both were in the double:
//
//  1. a SaveCursor below the committed order was a silent no-op, where the store
//     refuses with InboxErrorOrder. A consumer that saved a stale position was
//     told it had succeeded.
//  2. the double had NO EPOCH FENCE AT ALL, so a save from a superseded lease
//     was accepted and advanced the cursor. The property a real store enforces —
//     that a successor's mark cannot be walked back by a predecessor — was not
//     modelled, and the composed idempotency test therefore established nothing
//     about it.
//
// Both are fixed in the double rather than accommodated here. The expectation
// above is the store's behaviour and was not weakened to meet the fake.
func TestTheInboxDoubleAnswersTheConsumptionMechanismExactlyAsTheStoreDoes(t *testing.T) {
	t.Parallel()

	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	double := newDurableCommands(key)
	for _, id := range []sessionwire.CommandID{"command-a", "command-b", "command-c"} {
		double.accept(t, id, `{"blocks":[]}`)
	}

	runCursorSteps(t, "the double", inboxUnderTest{inbox: double, cursors: double}, key, theConsumptionSequence())
}

// TestTheDifferentialCanSeeADivergence is the CONTROL over the comparison
// itself.
//
// Both tests above pass by finding nothing, which is the shape that passes just
// as well when the harness asserts nothing. This drives the SAME sequence over a
// deliberately wrong implementation — one that ignores the bound and lists
// everything, which is precisely the defect the idempotency claim is about — and
// requires the harness to report it.
func TestTheDifferentialCanSeeADivergence(t *testing.T) {
	t.Parallel()

	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	double := newDurableCommands(key)
	for _, id := range []sessionwire.CommandID{"command-a", "command-b", "command-c"} {
		double.accept(t, id, `{"blocks":[]}`)
	}

	probe := &testing.T{}
	runCursorSteps(probe, "an inbox that ignores its bound",
		inboxUnderTest{inbox: unboundedInbox{inner: double}, cursors: double}, key, theConsumptionSequence())
	if !probe.Failed() {
		t.Fatal("the differential accepted an inbox that lists every command regardless of the cursor, which is the exact defect the idempotency claim is about")
	}
}

// unboundedInbox is the wrong implementation the control drives.
type unboundedInbox struct {
	inner commands.Inbox
}

func (i unboundedInbox) ListOrdered(
	ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, _ uint64, limit int,
) ([]commands.Command, error) {
	return i.inner.ListOrdered(ctx, tenant, session, 0, limit)
}

var _ commands.Inbox = unboundedInbox{}

// errStaleDoubleSave is what the double reports for a refusal, spelled here so
// the double's own refusals are distinguishable in a failure message.
var errStaleDoubleSave = errors.New("durableCommands: the save is behind a committed mark")
