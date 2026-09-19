package sessionstoreadapter

import (
	"context"
	"errors"
	"io"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/journal"
	harnesssessionstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/residency"
)

// harnessJournal is the read side of the released harness session store that
// answers "is there a conversation under this runtime id". *harness
// sessionstore.Store satisfies it, and so does any value embedding one.
type harnessJournal interface {
	OpenCatalog(...harnesssessionstore.CatalogOption) *harnesssessionstore.Catalog
	OpenInternalEventReplayer(uuid.UUID, harnesssessionstore.ReplayRequest) (journal.EventReplayer, error)
}

// The released store is the journal. A drift is a build failure here rather
// than every create in production silently reporting Unknown.
var _ harnessJournal = (*harnesssessionstore.Store)(nil)

// RuntimeJournals is satisfied by the router.
var _ RuntimeJournals = (*TenantEvidenceRouter)(nil)

// RuntimeJournal reports whether the journal store serving the session's
// tenant and storage binding already holds a conversation under runtimeID.
//
// THE STORE IS THE SETTLEMENT READER, BECAUSE IT HAS TO BE THE SAME JOURNAL.
// The reader registered for (tenant, binding) is the store the runtime's
// disposition evidence is read from, which is the journal the runtime writes;
// asking any other store would answer about a different journal.
//
// UNKNOWN IS AN ANSWER, NOT AN ERROR, when there is no way to look: no reader
// for the binding, or a reader that is not a harness store. A create is then
// refused upstream rather than launched. An error is reserved for a look that
// failed.
//
// THE CATALOG IS A SHORTCUT AND THE LEDGER IS THE AUTHORITY. harness folds its
// catalog after each append, best-effort, so an entry proves a conversation and
// its absence proves nothing: a crash between an append and the fold, or a
// swallowed KV error, leaves a journal with no entry. So an entry answers
// Present with one KV read, and anything else — absent, or unreadable —
// falls through to the journal itself, where any event at all is a
// conversation. (A journal holding only non-event records has no conversation
// to lose; a create over it is not a restart.)
func (r *TenantEvidenceRouter) RuntimeJournal(
	ctx context.Context,
	tenant sessionwire.TenantID,
	binding sessionstore.SessionBinding,
	runtimeID uuid.UUID,
) (residency.RuntimeJournal, error) {
	reader, served := r.readers[EvidenceKey{TenantID: tenant, StorageBindingID: binding.StorageBindingID}]
	if !served {
		return residency.RuntimeJournalUnknown, nil
	}
	store, ok := reader.(harnessJournal)
	if !ok {
		return residency.RuntimeJournalUnknown, nil
	}
	if _, present, err := store.OpenCatalog().ReadMeta(ctx, runtimeID); err == nil && present {
		return residency.RuntimeJournalPresent, nil
	}
	replayer, err := store.OpenInternalEventReplayer(runtimeID, harnesssessionstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		return residency.RuntimeJournalUnknown, err
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		return residency.RuntimeJournalUnknown, err
	}
	defer cursor.Close()
	_, _, err = cursor.Next(ctx)
	switch {
	case err == nil:
		return residency.RuntimeJournalPresent, nil
	case errors.Is(err, io.EOF):
		return residency.RuntimeJournalAbsent, nil
	default:
		return residency.RuntimeJournalUnknown, err
	}
}
