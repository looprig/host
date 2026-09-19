package sessionstoreadapter

import (
	"errors"

	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/residency"
)

// Store is the released session store, adapted to Host's seams.
//
// It holds the store rather than embedding it, so nothing in Host reaches a
// released method this package has not audited. Every method below is on this
// type or on a value it produces.
type Store struct {
	store *sessionstore.Store

	namespace     NamespaceLayout
	rigSessionIDs RigSessionIDs
	journals      RuntimeJournals
	gateJournals  GateJournals
}

// New adapts an open store.
//
// It refuses a nil store rather than deferring the nil dereference to the first
// durable call. A composition root that got this wrong would otherwise discover
// it inside an attach that has already taken an admission.
func New(store *sessionstore.Store, options ...Option) (*Store, error) {
	if store == nil {
		return nil, ErrNoStore
	}
	adapted := &Store{store: store}
	for _, option := range options {
		if option == nil {
			return nil, ErrNilOption
		}
		option(adapted)
	}
	return adapted, nil
}

// ErrNoStore is the refusal New returns when it is handed nothing to adapt.
var ErrNoStore = errors.New("sessionstoreadapter: no session store to adapt")

// ErrNilOption is the refusal New returns for a nil option, rather than the nil
// dereference that would otherwise happen while applying it.
var ErrNilOption = errors.New("sessionstoreadapter: a nil option was supplied")

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------
//
// THE SENTINELS ARE THE CONTRACT. residency.epochFence records ownership as gone
// for exactly residency.ErrEpochSuperseded and residency.ErrFenceConflict and
// treats every other error as an ambiguous store failure, so an adapter that
// reported a superseded epoch as a typed sessionstore error would leave this
// Host believing it still owns a session a successor has taken. Each function
// below turns one released vocabulary into that one.

// classifyRegistry maps a registry failure onto the sentinel residency fences
// on, preserving the store's error as the cause.
//
// RegistryErrorEpoch is the store refusing a write below a record's committed
// high-water mark, which is exactly what ErrEpochSuperseded means. Nothing else
// in the registry vocabulary is an ownership statement: not_found, expired and
// released describe the record, and backend, malformed, version and too_large
// describe the attempt.
func classifyRegistry(err error) error {
	if err == nil {
		return nil
	}
	var registryErr *sessionstore.RegistryError
	if errors.As(err, &registryErr) && registryErr.Code == sessionstore.RegistryErrorEpoch {
		return errors.Join(residency.ErrEpochSuperseded, err)
	}
	return err
}

// classifyJournal maps a journal failure onto the residency sentinels.
//
// The three ownership codes are NOT interchangeable and are not collapsed here.
// lease_held is another holder refusing the grant, which is ErrLeaseHeld and is
// what an attach retries; fenced is a successor's committed sequence refusing
// this writer's CAS, which is ErrFenceConflict; lease_lost is the grant itself
// having gone, which carries the same fact the mutable records call
// ErrEpochSuperseded. Reporting all three as one would erase the distinction
// residency.LossReason exists to render.
func classifyJournal(err error) error {
	if err == nil {
		return nil
	}
	var journalErr *sessionstore.JournalError
	if !errors.As(err, &journalErr) {
		return err
	}
	switch journalErr.Code {
	case sessionstore.JournalErrorLeaseHeld:
		return errors.Join(residency.ErrLeaseHeld, err)
	case sessionstore.JournalErrorFenced:
		return errors.Join(residency.ErrFenceConflict, err)
	case sessionstore.JournalErrorLeaseLost:
		return errors.Join(residency.ErrEpochSuperseded, err)
	default:
		return err
	}
}

// classifyInbox maps an inbox failure onto the residency sentinels.
//
// EXACTLY ONE CODE IS AN OWNERSHIP STATEMENT. InboxErrorEpoch is the store
// refusing a transition below the record's committed high-water mark, which
// means a later lease has taken the session: it is ErrEpochSuperseded, and it
// must arrive at commands.Fence.Write as that sentinel or this Host goes on
// retrying a session it no longer owns.
//
// THE OTHERS ARE DELIBERATELY LEFT ALONE, and that is worth stating because
// three of them have a Host refusal with the same name. claim_held, deadline and
// terminal reach the applier as RefusalStore, while ApplyRefusal declares
// RefusalClaimHeld, RefusalDeadlinePassed and a terminal path — because the
// applier reaches those refusals from its OWN precondition checks against a
// record it has read, and only loses the race to this error. Promoting the
// store's code into the same refusal would make a lost CAS indistinguishable
// from a precondition the applier evaluated, which is §5's identical-error mask.
// The distinction is reported here rather than resolved: it is a Host defect
// only if a lost race should be diagnosable, and that is O5/O6's decision.
func classifyInbox(err error) error {
	if err == nil {
		return nil
	}
	var inboxErr *sessionstore.InboxError
	if errors.As(err, &inboxErr) && inboxErr.Code == sessionstore.InboxErrorEpoch {
		return errors.Join(residency.ErrEpochSuperseded, err)
	}
	return err
}
