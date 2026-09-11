package sessionstoreadapter

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host/internal/residency"
)

// ResidencyLease is Host's own residency grant over one session.
//
// IT IS NOT THE JOURNAL GRANT, and keeping the two types apart in this package is
// the point of O3.4 rather than a filing decision. Grant, in journal.go, wraps a
// JournalWriter: its Epoch is the runtime-facing journal epoch, and it no longer
// satisfies residency.Lease at all — the compiler now refuses the substitution
// that used to be a comment. This one wraps a *sessionstore.ResidencyGrant, whose
// own doc says the epoch it mints "must never be compared with, or used as, a
// journal epoch" and that "residency loss does not fence a journal".
//
// IT IS WHAT FINALLY DISCHARGES THE OTHER HALF OF F3. residency.Lease.Lost is a
// channel a heartbeat and a drain supervisor select on, and against the journal
// grant this package had to close it itself from the classification of a write
// Host made — an echo of Host's own traffic, silent for an idle session.
// ResidencyGrant.Lost is Storage's lease loss channel, forwarded unmodified:
// "the provider's actual notification, never inferred from journal events,
// release errors or caller cancellation". F3 stands for the JOURNAL grant, which
// still publishes no loss signal.
//
// WHAT IT IS NOT IS A TAKEOVER GUARANTEE. Liveness is the provider's:
// ResidencyGrant's doc says memstore provides neither TTL nor crash takeover, so
// under this module's own tests the only thing that closes this channel is a
// release or a deliberately revocable wrapper. A real takeover needs pgstore.
type ResidencyLease struct {
	grant *sessionstore.ResidencyGrant
}

// ResidencyLease is a residency.Lease. The assertion is here rather than in a
// test so a drift in either shape is a compile failure.
var _ residency.Lease = (*ResidencyLease)(nil)

// Store is a residency.SessionLeases.
var _ residency.SessionLeases = (*Store)(nil)

// AcquireSessionLease takes the session's residency grant.
//
// THE SESSION MUST BE IN DISPOSITION MODE, and that is the store's rule rather
// than this adapter's: AcquireResidency reads the immutable catalog binding and
// refuses anything that is not ProtocolModeDisposition with
// catalogInvalid("binding.protocol_mode"), binding the session scope mode as a
// side effect. It is finding F16(a) and it is not softened here — a legacy-mode
// session simply has no residency grant to take, and papering over that would
// hand Host a lease the store never issued.
func (s *Store) AcquireSessionLease(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (residency.Lease, error) {
	grant, err := s.store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		// A TYPED NIL WOULD BE WORSE THAN THE ERROR. Returning `grant` here
		// would hand back a non-nil residency.Lease holding a nil pointer, and
		// residency's step 2 tests `lease == nil` to catch a store that reports
		// success and grants nothing — so the refusal would be read as a grant
		// and dereferenced at Epoch().
		return nil, classifyResidency(err)
	}
	return &ResidencyLease{grant: grant}, nil
}

// Epoch is this grant's residency epoch, in Host's own domain.
func (l *ResidencyLease) Epoch() residency.ResidencyEpoch {
	return residency.ResidencyEpoch(l.grant.Epoch())
}

// Lost is the PROVIDER's loss signal for this grant, forwarded unmodified.
func (l *ResidencyLease) Lost() <-chan struct{} { return l.grant.Lost() }

// Release returns this grant to the provider. It is not termination: the session
// remains durable and resumable.
//
// Failure is retryable and does not pretend cleanup completed: until a release
// succeeds the Store admission is retained and Close may time out.
//
// IT DOES NOT GO THROUGH classifyResidency, AND THE OMISSION IS THE DECISION.
// That classifier exists to turn an ACQUISITION vocabulary into Host's ownership
// sentinels, and neither of its arms is a statement a release can make: you cannot
// lose contention to another holder by handing a grant back, so ErrLeaseHeld here
// would tell residency's fence this grant was superseded when it merely could not
// be returned — and ResidencyAcquireCleanupError is raised inside AcquireResidency
// and never by Release. Wrapping the call was a no-op on every input this method
// can produce, which measured as an unkillable mutation; a transformation that
// cannot fire is worse than none, because it reads as a rule being applied. The
// store's own error therefore survives verbatim, and the caller that owns it is
// residency's unwinder, which reports a failed release in AttachError.Unreleased
// rather than classifying it.
func (l *ResidencyLease) Release(ctx context.Context) error {
	return l.grant.Release(ctx)
}

// classifyResidency maps a residency failure onto Host's sentinels and, for the
// one refusal that still owes a release, onto the error that can say so.
//
// THE CLEANUP ARM IS CHECKED FIRST AND THAT ORDERING IS LOAD-BEARING.
// ResidencyAcquireCleanupError unwraps to the joined refusal AND the provider
// cleanup failure, so a contention refusal wrapped in one would match the
// LeaseHeld arm too — and reporting it as a plain ErrLeaseHeld would discard the
// release obligation, which is the leak F16(b) names. Matching the obligation
// first and carrying the whole error as the cause keeps both facts: a caller
// still reaches ErrLeaseHeld through errors.Is on the cause.
//
// CONTENTION IS storage.LeaseHeldError, NOT A JOURNAL CODE. ResidencyError's own
// doc says Cause "preserves provider errors, including storage.LeaseHeldError for
// contention", and the journal vocabulary classifyJournal reads does not appear on
// this path at all — the grant is taken through storage.Leaser directly.
func classifyResidency(err error) error {
	if err == nil {
		return nil
	}
	var cleanup *sessionstore.ResidencyAcquireCleanupError
	if errors.As(err, &cleanup) {
		return &residency.LeaseCleanupError{Cause: err, Cleanup: cleanup.Release}
	}
	var held *storage.LeaseHeldError
	if errors.As(err, &held) {
		return errors.Join(residency.ErrLeaseHeld, err)
	}
	return err
}
