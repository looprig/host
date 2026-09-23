package sessionstoreadapter_test

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// The seams sessionstore v0.7.0 finally satisfies
// ---------------------------------------------------------------------------
//
// EVERY TEST IN THIS FILE RUNS AGAINST A DISPOSITION SESSION, and that is the
// configuration rather than a detail. sessionstore v0.7.0's own release note
// says the new stream and cursor are over the DISPOSITION family and that "the
// legacy inbox is unreachable to a Host", because AcquireResidency pins
// ProtocolModeDisposition. A round of tests written against a legacy session
// would pass its own assertions and say nothing about the configuration Host
// actually runs in — which is the failure v0.7.0's own round 1 made, with twenty
// green tests on a legacy fixture.
//
// THE COMPILE-TIME ASSERTIONS ARE THE POINT OF THE FIRST TWO LINES. Until this
// task the composition root had to be handed both seams by a product; now the
// adapted store satisfies them, and a drift in either shape is a build failure
// rather than a test failure.

var (
	_ commands.Inbox   = (*sessionstoreadapter.Store)(nil)
	_ commands.Cursors = (*sessionstoreadapter.Store)(nil)
)

// admitDispositionCommand makes one disposition command durable.
func admitDispositionCommand(
	t *testing.T, released *sessionstore.Store, id sessionwire.CommandID, kind string,
) sessionstore.DispositionInboxEntry {
	t.Helper()
	now := time.Now().UTC()
	entry, _, err := released.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID:  testTenant,
		SessionID: testSession,
		CommandID: id,
		Binding: sessionstore.SessionBinding{
			StorageBindingID: "binding-1",
			BindingVersion:   "v1",
			RuntimeSessionID: "runtime-session-1",
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(testRuntimeCommandID),
		Kind:                     sessionstore.CommandKind(kind),
		Payload:                  []byte(`{"blocks":[]}`),
		AcceptedAt:               now,
		ApplyDeadline:            now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("admit disposition command %q: %v", id, err)
	}
	return entry
}

// ---------------------------------------------------------------------------
// ListOrdered
// ---------------------------------------------------------------------------

// TestListOrderedReadsTheSessionStreamStrictlyAfterTheBoundAndBoundedByTheLimit
// is the Inbox contract measured against the released store rather than against
// a double that was written from the same sentence.
func TestListOrderedReadsTheSessionStreamStrictlyAfterTheBoundAndBoundedByTheLimit(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	var orders []uint64
	var runtimeIDs []string
	for _, id := range []sessionwire.CommandID{"command-a", "command-b", "command-c"} {
		entry := admitDispositionCommand(t, released, id, string(commands.KindInput))
		orders = append(orders, entry.AcceptedOrder)
		runtimeIDs = append(runtimeIDs, string(entry.Record.Descriptor.RuntimeCommandID))
	}
	if orders[0] >= orders[1] || orders[1] >= orders[2] {
		t.Fatalf("the store allocated acceptance orders %v, which are not strictly increasing; every assertion below reads them", orders)
	}

	// THE WHOLE STREAM, from the head. Zero is the store's own spelling for the
	// head and no provider allocates order zero.
	page, err := adapted.ListOrdered(t.Context(), testTenant, testSession, 0, 10)
	if err != nil {
		t.Fatalf("ListOrdered from the head: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("the head page holds %d commands, want 3: %+v", len(page), page)
	}
	for index, record := range page {
		if record.AcceptedOrder != orders[index] {
			t.Errorf("page[%d].AcceptedOrder = %d, want the store's %d", index, record.AcceptedOrder, orders[index])
		}
		if record.TenantID != testTenant || record.SessionID != testSession {
			t.Errorf("page[%d] claims %s/%s, want %s/%s", index, record.TenantID, record.SessionID, testTenant, testSession)
		}
		if record.State != commands.StatePending {
			t.Errorf("page[%d].State = %q, want pending; admission produces nothing else", index, record.State)
		}
		// W1: the live tail's projection names the admitted command in place
		// of this runtime identity, so the listing must carry it.
		if record.RuntimeCommandID.IsZero() || record.RuntimeCommandID.String() != runtimeIDs[index] {
			t.Errorf("page[%d].RuntimeCommandID = %s, want the store's %s", index, record.RuntimeCommandID, runtimeIDs[index])
		}
	}
	if page[0].CommandID != "command-a" || page[2].CommandID != "command-c" {
		t.Errorf("the page is %q…%q, want command-a…command-c", page[0].CommandID, page[2].CommandID)
	}

	// STRICTLY AFTER. The bound's own row must not come back, which is the half
	// a consumer's idempotence rests on: a cursor at order N asks for N+1 and up,
	// and a store that answered inclusively would re-apply command N every pass.
	after, err := adapted.ListOrdered(t.Context(), testTenant, testSession, orders[0], 10)
	if err != nil {
		t.Fatalf("ListOrdered after the first order: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("the page after order %d holds %d commands, want 2: %+v", orders[0], len(after), after)
	}
	if after[0].AcceptedOrder != orders[1] {
		t.Errorf("the first row after order %d is at %d, want %d", orders[0], after[0].AcceptedOrder, orders[1])
	}

	// THE LIMIT IS THE CALLER'S.
	bounded, err := adapted.ListOrdered(t.Context(), testTenant, testSession, 0, 2)
	if err != nil {
		t.Fatalf("ListOrdered with limit 2: %v", err)
	}
	if len(bounded) != 2 {
		t.Fatalf("a limit of 2 returned %d commands", len(bounded))
	}

	// AND THE EXHAUSTED STREAM IS AN EMPTY PAGE AND NOT A FAILURE, which is what
	// a caught-up consumer reads on every pass.
	empty, err := adapted.ListOrdered(t.Context(), testTenant, testSession, orders[2], 10)
	if err != nil {
		t.Fatalf("ListOrdered past the tail: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("the page past the tail holds %d commands: %+v", len(empty), empty)
	}
}

// TestListOrderedRefusesASessionWithNoDispositionCatalog is the store's
// fail-closed answer, carried to Host as a NAMED refusal.
//
// THE CONTROL IS THE LEGACY SESSION. createSession writes no binding at all,
// which the store preserves as the legacy v1 record — so this is not "a session
// that does not exist", it is a session that exists and is bound to the other
// protocol, and it is the arm that a test using only a missing session would
// never reach.
func TestListOrderedRefusesASessionWithNoDispositionCatalog(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	_, err := adapted.ListOrdered(t.Context(), testTenant, testSession, 0, 10)
	var refusal *sessionstoreadapter.NoDispositionCatalogError
	if !errors.As(err, &refusal) {
		t.Fatalf("ListOrdered against a legacy session = %v, want a NoDispositionCatalogError", err)
	}
	if refusal.TenantID != testTenant || refusal.SessionID != testSession {
		t.Errorf("the refusal names %s/%s, want %s/%s", refusal.TenantID, refusal.SessionID, testTenant, testSession)
	}
	requireNotAnOwnershipStatement(t, "ListOrdered", err)
}

// ---------------------------------------------------------------------------
// LoadCursor
// ---------------------------------------------------------------------------

// TestLoadCursorAnswersZeroForASessionThatHasRecordedNothing pins the ONE case
// in which zero is the answer: a session that EXISTS in the disposition catalog
// and has never saved a cursor.
func TestLoadCursorAnswersZeroForASessionThatHasRecordedNothing(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	cursor, err := adapted.LoadCursor(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadCursor on a fresh session: %v", err)
	}
	if cursor != 0 {
		t.Fatalf("LoadCursor = %d, want 0 for a session that has recorded nothing", cursor)
	}
}

// TestLoadCursorRefusesASessionWithNoDispositionCatalogRatherThanAnsweringZero
// is FINDING C-1 made executable, and it is the reason Host's Cursors doc
// changed rather than the store's behaviour being papered over.
//
// The two states are not the same state and must not share an answer. Zero means
// "this session exists and has consumed nothing", and a consumer acts on it by
// listing from the head. A session with no disposition catalog can be consumed
// by nobody, and answering it with zero would send every pass to the head of a
// stream this Host has no authority over, forever, silently.
func TestLoadCursorRefusesASessionWithNoDispositionCatalogRatherThanAnsweringZero(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	cursor, err := adapted.LoadCursor(t.Context(), testTenant, testSession)
	var refusal *sessionstoreadapter.NoDispositionCatalogError
	if !errors.As(err, &refusal) {
		t.Fatalf("LoadCursor against a legacy session = (%d, %v), want a NoDispositionCatalogError", cursor, err)
	}
	if cursor != 0 {
		t.Errorf("the refusal also reported cursor %d; a refused read must report no position", cursor)
	}
	requireNotAnOwnershipStatement(t, "LoadCursor", err)

	// AND AGAINST A SESSION THAT NEVER EXISTED AT ALL, which fails earlier, at
	// the keyspace, and is the same refusal to a caller.
	absent, err := adapted.LoadCursor(t.Context(), testTenant, "session-that-never-existed")
	if !errors.As(err, &refusal) {
		t.Fatalf("LoadCursor against an absent session = (%d, %v), want a NoDispositionCatalogError", absent, err)
	}
}

// ---------------------------------------------------------------------------
// SaveCursor
// ---------------------------------------------------------------------------

// TestSaveCursorRecordsThePositionAndLoadCursorReadsItBack is the round trip,
// and it is written to READ THE VALUE BACK for the reason this repository has
// now had three transpositions: a test that only writes cannot tell an epoch
// from an order. The two arguments here are DELIBERATELY DIFFERENT NUMBERS.
func TestSaveCursorRecordsThePositionAndLoadCursorReadsItBack(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	const epoch, order = uint64(7), uint64(3)
	if err := adapted.SaveCursor(t.Context(), testTenant, testSession, epoch, order); err != nil {
		t.Fatalf("SaveCursor(epoch=%d, order=%d): %v", epoch, order, err)
	}

	cursor, err := adapted.LoadCursor(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if cursor != order {
		t.Fatalf("LoadCursor = %d, want the saved ORDER %d; %d would be the saved EPOCH", cursor, order, epoch)
	}

	// The durable record is asked directly as well, so the round trip is not two
	// adapter bugs cancelling: the store's own record must carry the epoch in the
	// epoch member and the order in the order member.
	entry, err := released.LoadDispositionCommandCursor(t.Context(), sessionstore.LoadDispositionCommandCursorRequest{
		TenantID:  testTenant,
		SessionID: testSession,
	})
	if err != nil {
		t.Fatalf("LoadDispositionCommandCursor: %v", err)
	}
	if entry.Cursor.LeaseEpoch != epoch {
		t.Errorf("the durable record holds LeaseEpoch %d, want %d", entry.Cursor.LeaseEpoch, epoch)
	}
	if entry.Cursor.ConsumedOrder != order {
		t.Errorf("the durable record holds ConsumedOrder %d, want %d", entry.Cursor.ConsumedOrder, order)
	}
}

// TestSaveCursorReportsASupersededEpochAsTheOwnershipSentinel is the obligation
// commands.CursorWrites states as a contract rather than a courtesy.
//
// residency.epochFence records ownership as gone for exactly
// residency.ErrEpochSuperseded and residency.ErrFenceConflict. An adapter that
// returned the store's *InboxError untouched would leave this Host believing it
// still owns a session a successor has taken, retrying once per
// ReconcileInterval and counting each refusal as store trouble.
func TestSaveCursorReportsASupersededEpochAsTheOwnershipSentinel(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	if err := adapted.SaveCursor(t.Context(), testTenant, testSession, 9, 4); err != nil {
		t.Fatalf("the first SaveCursor: %v", err)
	}

	err := adapted.SaveCursor(t.Context(), testTenant, testSession, 8, 5)
	if !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("SaveCursor below the committed epoch = %v, want ErrEpochSuperseded", err)
	}
	// The store's own error survives as the cause, so a reader still sees which
	// mark refused the write.
	var inboxErr *sessionstore.InboxError
	if !errors.As(err, &inboxErr) {
		t.Fatalf("the sentinel discarded the store's error: %v", err)
	}
	if inboxErr.Code != sessionstore.InboxErrorEpoch {
		t.Fatalf("the cause carries code %q, want %q", inboxErr.Code, sessionstore.InboxErrorEpoch)
	}
}

// TestSaveCursorDoesNotReportAStalePositionAsALostSession is v0.7.0's SECOND
// consumer obligation, and it is a negative claim with a positive control.
//
// InboxErrorOrder is the twentieth code in the vocabulary and it means RE-READ
// AND RETRY: the caller's position is behind the committed one, which is a
// statement about its DATA and not about its ownership. Mapping it onto
// ErrEpochSuperseded would make a consumer that retried an ambiguous save
// surrender a session it still holds — silently, and permanently, since the
// fence never reopens.
func TestSaveCursorDoesNotReportAStalePositionAsALostSession(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	// The committed position is 6 at epoch 3.
	if err := adapted.SaveCursor(t.Context(), testTenant, testSession, 3, 6); err != nil {
		t.Fatalf("the first SaveCursor: %v", err)
	}

	// The SAME epoch with a LOWER order. The epoch fence admits it — an equal
	// epoch is admitted, because one grant saves many times — so the refusal that
	// comes back is the ORDER fence's and nothing else.
	err := adapted.SaveCursor(t.Context(), testTenant, testSession, 3, 5)
	if err == nil {
		t.Fatal("a save below the committed position was accepted, so the rest of this test is unfalsifiable")
	}
	var inboxErr *sessionstore.InboxError
	if !errors.As(err, &inboxErr) {
		t.Fatalf("SaveCursor below the committed order = %v, want an *InboxError", err)
	}
	if inboxErr.Code != sessionstore.InboxErrorOrder {
		t.Fatalf("the refusal carries code %q, want %q; this test's whole subject is the order fence", inboxErr.Code, sessionstore.InboxErrorOrder)
	}

	// THE CLAIM. It is not an ownership statement, in either sentinel.
	requireNotAnOwnershipStatement(t, "SaveCursor below the committed order", err)

	// THE CONTROL, and it is what makes the negative above falsifiable: the same
	// assertion run over the EPOCH refusal finds the sentinel. So
	// requireNotAnOwnershipStatement can see a sentinel when one is there.
	superseded := adapted.SaveCursor(t.Context(), testTenant, testSession, 2, 9)
	if !errors.Is(superseded, residency.ErrEpochSuperseded) {
		t.Fatalf("the control did not produce an ownership sentinel: %v", superseded)
	}

	// AND THE SESSION IS STILL CONSUMABLE, which is the consequence the negative
	// claim exists to protect: the committed position is unchanged and a save at
	// or above it still lands.
	if err := adapted.SaveCursor(t.Context(), testTenant, testSession, 3, 7); err != nil {
		t.Fatalf("a legitimate save after a stale one was refused: %v", err)
	}
	cursor, err := adapted.LoadCursor(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if cursor != 7 {
		t.Fatalf("LoadCursor = %d, want 7", cursor)
	}
}

// TestSaveCursorRefusesASessionWithNoDispositionCatalogWithoutSurrenderingIt is
// the arm the task names as the failure mode the retarget existed to avoid: a
// *CatalogError reaching residency.epochFence unclassified.
//
// The fence recognises two sentinels and treats everything else as an ambiguous
// STORE failure — which is retried once per ReconcileInterval, forever. A
// binding refusal is not ambiguous and is not transient: it is permanent and it
// is an operator's problem, so it is given a name of its own and is explicitly
// NOT an ownership statement.
func TestSaveCursorRefusesASessionWithNoDispositionCatalogWithoutSurrenderingIt(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	err := adapted.SaveCursor(t.Context(), testTenant, testSession, 3, 1)
	var refusal *sessionstoreadapter.NoDispositionCatalogError
	if !errors.As(err, &refusal) {
		t.Fatalf("SaveCursor against a legacy session = %v, want a NoDispositionCatalogError", err)
	}
	requireNotAnOwnershipStatement(t, "SaveCursor", err)

	// THE STORE'S OWN ERROR SURVIVES AS THE CAUSE. A refusal that discarded it
	// would tell an operator that something is wrong with the binding and not
	// WHICH member the store refused.
	var catalogErr *sessionstore.CatalogError
	if !errors.As(err, &catalogErr) {
		t.Fatalf("the refusal discarded the store's *CatalogError: %v", err)
	}
	if catalogErr.Field == "" {
		t.Errorf("the store's refusal names no field, so the classification below cannot be checked")
	}
}

// requireNotAnOwnershipStatement is the shared negative assertion.
//
// It is separate so that the control in
// TestSaveCursorDoesNotReportAStalePositionAsALostSession drives THIS function
// over an error that IS an ownership statement. A negative
// assertion whose helper is never shown finding anything is a negative assertion
// that cannot fail.
func requireNotAnOwnershipStatement(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: there is no error to classify, so this assertion is vacuous", what)
	}
	for _, sentinel := range []error{
		residency.ErrEpochSuperseded,
		residency.ErrFenceConflict,
		residency.ErrLeaseHeld,
		residency.ErrLeaseNotHeld,
	} {
		if errors.Is(err, sentinel) {
			t.Fatalf("%s surrendered ownership (%v) for a failure that says nothing about it: %v", what, sentinel, err)
		}
	}
}
