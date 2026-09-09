package sessionstoreadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/residency"
)

// UnavailableMemberError reports a member of a Host record that the released
// store holds no source for.
//
// IT IS A REFUSAL AND NOT A ZERO VALUE, and that is the whole of findings F4 and
// F5. A hydration handed Namespace="" scopes every object the launched runtime
// writes to nothing and reports success; a restore handed the zero
// RigSessionID asks Harness to restore a session that does not exist. Both are
// silent in a fake that fills the member in and loud here, which is the
// direction this task exists to make loud.
type UnavailableMemberError struct {
	Member string
	Reason string
}

func (e *UnavailableMemberError) Error() string {
	return "sessionstoreadapter: " + strconv.Quote(e.Member) +
		" has no source in the released session store: " + e.Reason
}

// NamespaceLayout answers the durable object prefix a session's runtime writes
// under.
//
// IT IS A PARAMETER RATHER THAN A DELEGATION BECAUSE F4 SAYS IT HAS TO BE.
// residency.SessionState documents Namespace as read from the store "because the
// layout is SessionStore's", and sessionstore v0.6.0 publishes no layout,
// prefix or namespace accessor — the keyspace derivation is unexported. So the
// adapter cannot answer this member by delegating, and it will not answer it by
// inventing one either: a prefix chosen here would be a second authority for a
// layout the store already owns, and the two would disagree the first time
// either moved. A composition root that has an answer states it; one that does
// not gets UnavailableMemberError rather than an empty string.
type NamespaceLayout func(sessionwire.TenantID, sessionwire.SessionID) string

// RigSessionIDs answers Harness's own identity for a session Host created.
//
// IT IS A PARAMETER FOR F5'S REASON, which is F4's reason with a different
// owner. residency.SessionState.RigSessionID is documented as something Host
// records at create and cannot derive, and no released record holds it:
// CatalogRecord has no member for it and SessionPointerKind is a closed set of
// three roles, none of them a runtime identity. Until something durable does,
// the composition root that made the record answers here — and one that recorded
// nothing gets UnavailableMemberError instead of uuid.UUID{}, which is what
// Rig.RestoreSession would otherwise be handed.
type RigSessionIDs func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uuid.UUID, error)

// Option configures an adapted store.
type Option func(*Store)

// WithNamespaceLayout supplies the object prefix LoadSessionState reports.
func WithNamespaceLayout(layout NamespaceLayout) Option {
	return func(s *Store) { s.namespace = layout }
}

// WithRigSessionIDs supplies the Harness identity LoadSessionState reports for
// an existing session.
func WithRigSessionIDs(ids RigSessionIDs) Option {
	return func(s *Store) { s.rigSessionIDs = ids }
}

// LoadSessionState reports the durable state a hydration reads.
//
// A session with no catalog record is Exists=false and is the create path; a
// session with one is the restore path, and the restore path CANNOT BE SERVED by
// the released store — see F5. It refuses rather than answering with a zero
// RigSessionID, because the caller's next act is to hand that UUID to Harness.
func (s *Store) LoadSessionState(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (residency.SessionState, error) {
	if s.namespace == nil {
		return residency.SessionState{}, &UnavailableMemberError{
			Member: "Namespace",
			Reason: "no namespace layout was supplied; see WithNamespaceLayout",
		}
	}
	state := residency.SessionState{Namespace: s.namespace(tenant, session)}

	entry, err := s.store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	switch {
	case err == nil:
	case isCatalogAbsent(err):
		// A session with no durable record still reports a namespace: the
		// prefix is a property of the identities, not of the state.
		return state, nil
	default:
		return residency.SessionState{}, err
	}

	state.Exists = true
	state.CompatibilityID = department.CompatibilityID(entry.Record.RuntimeCompatibilityID)
	state.HasCheckpoint = entry.Record.Checkpoint.JournalSeq != 0
	state.CheckpointSequence = entry.Record.Checkpoint.JournalSeq

	if s.rigSessionIDs == nil {
		return residency.SessionState{}, &UnavailableMemberError{
			Member: "RigSessionID",
			Reason: "the catalog record holds no Harness session identity and no session pointer kind names one; see WithRigSessionIDs",
		}
	}
	rigSessionID, err := s.rigSessionIDs(ctx, tenant, session)
	if err != nil {
		return residency.SessionState{}, err
	}
	state.RigSessionID = rigSessionID
	return state, nil
}

// LoadSession returns the durable record for a session as opaque bytes.
//
// THE BYTES ARE THIS ADAPTER'S, NOT THE STORE'S, which is finding F1. No
// released call answers with an encoded record, so the encoding below is a
// choice; it is the catalog record's canonical JSON so that a reader debugging a
// failure sees the record rather than a length. NOTHING MAY PARSE IT: a caller
// that needs a member reads residency.SessionState, which is the seam that
// declares its members.
func (s *Store) LoadSession(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) ([]byte, error) {
	entry, err := s.store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(entry.Record)
}

// isCatalogAbsent reports the three ways the store says a session has no record.
//
// THREE, AND THEY ARE NOT ALL CATALOG ERRORS — finding F13, which this adapter's
// first test measured rather than predicted. A session that has NEVER existed
// does not reach the catalog at all: deriving its scope fails first, and the
// caller gets *KeyspaceError{binding_not_found}. CatalogErrorNotFound is a
// session whose scope is bound but whose record is gone, and CatalogErrorDeleted
// is one whose record was removed. A hydration treats all three identically —
// there is nothing to restore from — and an adapter matching only the catalog
// codes reports the commonest case, a brand new session, as a store failure.
//
// This is precisely what a fake cannot teach: residency's fakeDurable answers
// SessionState{Exists:false} and has no error to classify, so no test on it
// could distinguish the three, and none had to.
func isCatalogAbsent(err error) bool {
	var keyspaceErr *sessionstore.KeyspaceError
	if errors.As(err, &keyspaceErr) && keyspaceErr.Code == sessionstore.KeyspaceBindingNotFound {
		return true
	}
	var catalogErr *sessionstore.CatalogError
	if !errors.As(err, &catalogErr) {
		return false
	}
	return catalogErr.Code == sessionstore.CatalogErrorNotFound ||
		catalogErr.Code == sessionstore.CatalogErrorDeleted
}
