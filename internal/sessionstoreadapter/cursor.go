package sessionstoreadapter

import (
	"context"
	"errors"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
)

// This file binds the two seams sessionstore v0.7.0 finally satisfies:
// commands.Inbox's ordered per-session listing and commands.Cursors' durable
// consumption cursor. Until v0.7.0 a deployment had to supply both itself, and
// cmd/host's Bootstrap injected a pair that refused.
//
// THEY ARE OVER THE DISPOSITION FAMILY AND NOT THE LEGACY INBOX, and that is a
// property of the session rather than a choice made here. A Host takes residency
// through AcquireResidency, which reads the immutable catalog binding and
// refuses anything that is not ProtocolModeDisposition — so a session this Host
// can hold at all is disposition-bound, and the legacy inbox is unreachable to
// it. The released module's own note says as much: a legacy-bound cursor fails
// with catalog conflict on binding.protocol_mode.
//
// WHAT THIS PACKAGE STILL DOES NOT BIND, said plainly rather than left to be
// discovered: inbox.go's record read and its four compare-and-swap transitions
// are over the LEGACY vocabulary (GetCommand, ClaimCommand,
// BeginApplyingCommand, CompleteCommand, RejectCommand), and the disposition
// family has its own (GetDispositionCommand, BeginDispositionAttempt,
// SettleDispositionCommand). The two halves of Host's command path therefore
// name two different families of the same store, and no session can serve both.
// That is finding C-2 and it is reported upward rather than papered over here;
// see the task result's owed list.

// ---------------------------------------------------------------------------
// commands.Inbox
// ---------------------------------------------------------------------------

// ListOrdered returns at most limit records strictly after afterOrder, in the
// store's immutable acceptance order.
//
// THE BOUND IS THE CALLER'S AND THE ORDERING IS THE STORE'S, and nothing here
// sorts, filters or re-ranks the page: the released call returns rows in
// strictly increasing AcceptedOrder, every one of them above the bound, and a
// page this adapter re-ordered would be Host inferring an order rather than
// reading one.
//
// IT FAILS CLOSED ON A ROW IT CANNOT VOUCH FOR, and the failure is the store's
// rather than this adapter's: ListSessionDispositionCommands refuses a page
// containing a row that does not decode, disagrees with its filing, or carries
// another session's identities, because a consumer handed a page with a row
// quietly missing would advance its durable cursor PAST the missing command.
// Nothing here weakens that into a skip, and nothing here has anywhere to record
// one if it wanted to.
func (s *Store) ListOrdered(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	afterOrder uint64,
	limit int,
) ([]commands.Command, error) {
	page, err := s.store.ListSessionDispositionCommands(ctx, sessionstore.ListSessionDispositionCommandsRequest{
		TenantID:   tenant,
		SessionID:  session,
		AfterOrder: afterOrder,
		Limit:      limit,
	})
	if err != nil {
		return nil, classifyDispositionRead(tenant, session, err)
	}

	records := make([]commands.Command, 0, len(page.Commands))
	for _, entry := range page.Commands {
		state, err := adaptState(entry.Record.State)
		if err != nil {
			return nil, err
		}
		// AN UNPARSEABLE RUNTIME ID IS LEFT ZERO RATHER THAN FAILING THE PAGE:
		// the consumer does not read it, and failing here would stop a
		// session's command stream over a field only the public projection
		// uses. A zero id maps nothing, so the projection omits it.
		runtimeID, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil {
			runtimeID = uuid.UUID{}
		}
		records = append(records, commands.Command{
			TenantID:         entry.Record.Descriptor.TenantID,
			SessionID:        entry.Record.Descriptor.SessionID,
			CommandID:        entry.Record.Descriptor.CommandID,
			AcceptedOrder:    entry.AcceptedOrder,
			State:            state,
			RuntimeCommandID: runtimeID,
		})
	}
	return records, nil
}

// ---------------------------------------------------------------------------
// commands.Cursors
// ---------------------------------------------------------------------------

// LoadCursor returns the greatest acceptance order durably consumed for a
// session that EXISTS in the disposition catalog, or zero when none has been
// recorded.
//
// THE QUALIFICATION IS THE WHOLE OF FINDING C-1 and it is why the seam's own
// documentation moved to meet this. A session with no disposition catalog —
// absent, or bound to another protocol — is REFUSED here rather than answered
// about, because the two states are not one state:
//
//   - zero means "this session exists and has consumed nothing", and a consumer
//     acts on it by listing from the head of the stream;
//   - a missing or wrongly-bound catalog means the stream cannot be read by this
//     Host at all, and answering it with zero would send every pass to the head
//     of a stream it has no authority over, forever, reporting accepted work that
//     it can never consume.
//
// The released reader draws the same line for the same reason and says so: "a
// caller must not translate it into 'no commands'".
func (s *Store) LoadCursor(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (uint64, error) {
	entry, err := s.store.LoadDispositionCommandCursor(ctx, sessionstore.LoadDispositionCommandCursorRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		return 0, classifyDispositionRead(tenant, session, err)
	}
	return entry.Cursor.ConsumedOrder, nil
}

// SaveCursor records the cursor under the writer's lease epoch.
//
// THE PARAMETER ORDER IS epoch THEN order, which is commands.CursorWrites'
// declared order, and both are uint64 — the shape this repository has now got
// wrong three times. TestSaveCursorRecordsThePositionAndLoadCursorReadsItBack
// reads the durable record's two members back separately for that reason, and
// TestNoImplementationTransposesAdjacentSameTypedParameters holds the
// declaration.
//
// EVERY REFUSAL IS CLASSIFIED, AND THERE ARE THREE KINDS. This call goes through
// commands.Fence.Write, and residency.epochFence records ownership as gone for
// exactly residency.ErrEpochSuperseded and residency.ErrFenceConflict, treating
// everything else as an ambiguous store failure that is retried:
//
//  1. InboxErrorEpoch is the store refusing a write below the record's committed
//     high-water mark — a later lease has taken the session. It becomes
//     ErrEpochSuperseded, which is terminal and correct.
//
//  2. InboxErrorOrder is v0.7.0's twentieth code and means RE-READ AND RETRY. It
//     is a statement about the caller's DATA and not about its ownership, and it
//     is deliberately left alone: mapping it onto ErrEpochSuperseded would make
//     a consumer that retried an ambiguous save surrender a session it still
//     holds, permanently, since the fence never reopens.
//
//  3. A *CatalogError or *KeyspaceError is the session itself being one this
//     store will not vouch for. It reaches the fence as neither sentinel — which
//     is right — but it is NOT ambiguous and NOT transient, so leaving it
//     unclassified would put a permanent misconfiguration on a retry loop that
//     runs once per ReconcileInterval forever with nothing naming the cause. It
//     becomes NoDispositionCatalogError, which names the session and carries the
//     store's own refusal.
func (s *Store) SaveCursor(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	epoch uint64,
	order uint64,
) error {
	_, err := s.store.SaveDispositionCommandCursor(ctx, sessionstore.SaveDispositionCommandCursorRequest{
		TenantID:      tenant,
		SessionID:     session,
		LeaseEpoch:    epoch,
		ConsumedOrder: order,
	})
	if err == nil {
		return nil
	}
	// THE CATALOG ARM IS CHECKED FIRST, and the ordering is not arbitrary: a
	// binding refusal is a statement about the SESSION and reaches the fence as
	// neither sentinel either way, but classifyInbox would pass it through
	// unnamed. Naming it first is what stops a permanent misconfiguration being
	// retried as ambiguous store trouble.
	if isDispositionCatalogRefusal(err) {
		return &NoDispositionCatalogError{TenantID: tenant, SessionID: session, Cause: err}
	}
	return classifyInbox(err)
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

// classifyDispositionRead names the one refusal a disposition-family read makes
// that is about the SESSION rather than about the request or the provider.
//
// It returns the store's error unchanged for everything else, which keeps this a
// narrowing rather than a rewrite: an adapter that wrapped every catalog failure
// would report a provider outage as a binding problem, which is the
// wider-than-its-probe sentence this repository's reviews keep finding.
func classifyDispositionRead(tenant sessionwire.TenantID, session sessionwire.SessionID, err error) error {
	if err == nil {
		return nil
	}
	if !isDispositionCatalogRefusal(err) {
		return err
	}
	return &NoDispositionCatalogError{TenantID: tenant, SessionID: session, Cause: err}
}

// isDispositionCatalogRefusal reports the ways the store says it will not vouch
// for a session's DISPOSITION catalog.
//
// FOUR CODES, IN THREE SHAPES, and the set is closed over the pinned module's
// declarations by codeset_test.go rather than being a list somebody remembers to
// widen:
//
//   - KeyspaceBindingNotFound — the session has NEVER existed, so deriving its
//     scope fails before the catalog is reached at all. This is finding F13's
//     arm and it is the commonest of the four.
//   - CatalogErrorNotFound and CatalogErrorDeleted — the scope is bound and the
//     record is gone.
//   - CatalogErrorInvalid — the record EXISTS and is bound to another protocol.
//     This is the arm a test using only a missing session never reaches, and it
//     is the one v0.7.0's note is about.
//
// CatalogErrorInvalid IS WIDER THAN "WRONG PROTOCOL" AND THAT IS ACCEPTED
// DELIBERATELY. The store spells the protocol refusal catalogInvalid with field
// binding.protocol_mode, and narrowing on the FIELD would make this guard depend
// on a string the released module declares no vocabulary for and changes without
// a version bump. Every other invalid a disposition read can raise is also a
// statement that this store will not serve this session's disposition catalog,
// so the wider arm reports the same conclusion for the same reason. The cause is
// carried, so a reader always sees which member was refused.
func isDispositionCatalogRefusal(err error) bool {
	var keyspaceErr *sessionstore.KeyspaceError
	if errors.As(err, &keyspaceErr) {
		return keyspaceErr.Code == sessionstore.KeyspaceBindingNotFound
	}
	var catalogErr *sessionstore.CatalogError
	if !errors.As(err, &catalogErr) {
		return false
	}
	switch catalogErr.Code {
	case sessionstore.CatalogErrorNotFound,
		sessionstore.CatalogErrorDeleted,
		sessionstore.CatalogErrorInvalid:
		return true
	default:
		return false
	}
}

// NoDispositionCatalogError reports a session this store will not answer
// disposition-family questions about.
//
// IT IS NOT AN OWNERSHIP STATEMENT, and that is asserted rather than assumed:
// it unwraps to the store's own error and to nothing in residency's sentinel
// vocabulary, so residency.epochFence leaves the grant alone when one reaches it
// through a fenced write. What it IS is a permanent refusal with a name, which
// is what separates it from the ambiguous store failure the fence would
// otherwise retry forever.
type NoDispositionCatalogError struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Cause     error
}

func (e *NoDispositionCatalogError) Error() string {
	return "sessionstoreadapter: session " + strconv.Quote(string(e.TenantID)) + "/" +
		strconv.Quote(string(e.SessionID)) +
		" has no disposition catalog this store will vouch for, so its command stream and consumption cursor cannot be read: " +
		e.Cause.Error()
}

func (e *NoDispositionCatalogError) Unwrap() error { return e.Cause }
