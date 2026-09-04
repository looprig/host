package sessionstoreadapter

import (
	"context"
	"errors"
	"strconv"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
)

// Grant is one open, epoch-fenced single-writer grant over a session's journal.
//
// IT IS ONE OBJECT WHERE HOST DECLARES TWO SEAMS, which is finding F2 and the
// largest structural difference this task found. residency models the lease
// acquisition and the opening fence as independent steps, so an attach can hold
// a lease whose fence was refused. Store.OpenJournal acquires the
// lease, reads the tip exactly once, commits the opening fence at that tip and —
// on every failure after the grant — releases the grant before returning. There
// is no state in which the released store holds a lease and no fence, so this
// type cannot offer one and does not pretend to.
type Grant struct {
	writer *sessionstore.JournalWriter

	tenant  sessionwire.TenantID
	session sessionwire.SessionID

	mu   sync.Mutex
	lost chan struct{}
	done bool
}

// OpenSession takes the lease and commits the opening fence in one call.
//
// A CALLER STILL SEQUENCES ITS ROLLBACK AROUND TWO OUTCOMES, and they are the
// two the store distinguishes: ErrLeaseHeld means another holder has the
// session and nothing was taken, ErrFenceConflict means this grant was spent
// against a successor's committed sequence and the store has already released
// it. Neither leaves a lease for the caller to unwind, which is the property
// residency's rollback ladder must be read against.
func (s *Store) OpenSession(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (*Grant, error) {
	writer, err := s.store.OpenJournal(ctx, sessionstore.OpenJournalRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		return nil, classifyJournal(err)
	}
	return &Grant{
		writer:  writer,
		tenant:  tenant,
		session: session,
		lost:    make(chan struct{}),
	}, nil
}

// Epoch is the fencing epoch of this grant's lease.
func (g *Grant) Epoch() uint64 { return g.writer.Epoch() }

// Lost closes when this grant is known to be gone.
//
// IT IS NOT THE STORE'S SIGNAL, AND THERE IS NO STORE SIGNAL — finding F3. The
// released module exposes no lease-loss channel and no renewal callback; a
// JournalWriter learns it lost the stream by latching the failure of an append
// IT made. So this channel closes when a call THROUGH THIS ADAPTER classified as
// a lost or fenced grant, and it is SILENT FOR AN IDLE SESSION: a Host that
// stops writing stops learning. residency.Lease.Lost documents itself as "the
// FAST guard only" with the non-rebasing journal CAS as the hard backstop, and
// against this store the fast guard is strictly weaker than that sentence
// implies — it is not a timer, it is an echo of this Host's own writes.
func (g *Grant) Lost() <-chan struct{} { return g.lost }

// Release drops the grant. It is not termination: the session remains durable
// and resumable.
func (g *Grant) Release(ctx context.Context) error {
	err := g.writer.Close(ctx)
	g.end()
	return classifyJournal(err)
}

// AppendApplicationPrefix commits the private correlation record before the
// runtime-visible effect begins.
//
// THE LEASE EPOCH IS NOT A PARAMETER AND IS NOT SET HERE. The released writer
// refuses a prefix whose LeaseEpoch a caller chose and stamps its own grant onto
// the record, which is exactly what commands.Prefix's doc says it wants: the
// epoch on a prefix is the epoch that actually held the stream. The two agree,
// so the field is left zero deliberately rather than by omission.
func (g *Grant) AppendApplicationPrefix(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	prefix commands.Prefix,
) error {
	// THE SEAM CARRIES IDENTITIES THE GRANT ALREADY FIXES, so they are checked
	// rather than passed on. A grant is opened for one session and the store
	// takes no identities on Append; forwarding a foreign pair would append this
	// command's prefix to a stream that is not its session's, and the store
	// could not see it. commands has one applier per residency, so a mismatch is
	// a composition fault and is reported as one.
	if tenant != g.tenant || session != g.session {
		return &ForeignGrantError{
			GrantTenant:   g.tenant,
			GrantSession:  g.session,
			CalledTenant:  tenant,
			CalledSession: session,
		}
	}
	_, err := g.writer.Append(ctx, sessionstore.Envelope{
		Kind:             sessionstore.EnvelopeKindApplicationPrefix,
		CommandID:        prefix.CommandID,
		RuntimeCommandID: prefix.RuntimeCommandID,
		CommandKind:      string(prefix.Kind),
	})
	classified := classifyJournal(err)
	// MEASURED, AND UNKILLABLE TODAY. Removing this closure changes no test:
	// TestLostReportsOnlyWhatThisGrantDid says why, and a mutation that never
	// calls g.end here survives the whole suite. Neither sessionstore nor
	// memstore offers a way to expire a live grant's lease, and while the grant
	// is live every rival open is refused at the lease — so no test in this
	// module can construct an append that comes back fenced.
	//
	// It is kept rather than deleted because it is the ONLY thing that would
	// ever close Lost against a real provider: finding F3 is that the released
	// store publishes no ownership signal, so a fenced write is the single
	// observation this adapter gets. Deleting it would leave residency's
	// heartbeat and drain supervisor selecting on a channel that never closes at
	// all. What holds it meanwhile is classifyJournal's own arms, which ARE
	// killable (see the lease_held and epoch mutations recorded for O3.3).
	if errors.Is(classified, residency.ErrEpochSuperseded) || errors.Is(classified, residency.ErrFenceConflict) {
		g.end()
	}
	return classified
}

// end closes the loss channel once.
func (g *Grant) end() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done {
		return
	}
	g.done = true
	close(g.lost)
}

// ForeignGrantError reports a durable write routed to a grant that does not own
// the session it names.
type ForeignGrantError struct {
	GrantTenant   sessionwire.TenantID
	GrantSession  sessionwire.SessionID
	CalledTenant  sessionwire.TenantID
	CalledSession sessionwire.SessionID
}

func (e *ForeignGrantError) Error() string {
	return "sessionstoreadapter: a grant over session " +
		strconv.Quote(string(e.GrantTenant)) + "/" + strconv.Quote(string(e.GrantSession)) +
		" was asked to write for " +
		strconv.Quote(string(e.CalledTenant)) + "/" + strconv.Quote(string(e.CalledSession))
}
