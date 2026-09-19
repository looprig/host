package sessionstoreadapter

import (
	"context"
	"errors"
	"io"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
	harnesssessionstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/residency"
)

// ErrGateOwnerUnavailable is the refusal LoadGate returns for a gate that
// exists.
//
// THE BACKSTOP IS DECIDED FROM MEMBERS THE PROJECTION DOES NOT CARRY — finding
// F10. commands.Gate holds OwnerHostID and OwnerEpoch, and §9.4's release-race
// backstop resumes a gate response only while both still name this Host's
// current residency. Store.ReadGates answers with sessionwire.GatePage, whose
// GateProjection carries the gate id, kind, prompt, opening event and sequence,
// deadline and answerability — and names no owning Host and no lease epoch.
//
// ANSWERING WITH ZERO OWNERS WOULD BE WORSE THAN REFUSING, which is why this
// exists rather than a partially filled Gate. A zero HostID matches no live
// residency, so a caller comparing it against its own would refuse every gate
// response — but a caller comparing OwnerEpoch against zero, or one that treats
// "no owner" as "not owned by anyone else", would resume a gate whose runtime
// was released and relaunched. The seam cannot express "I know the gate is open
// and I do not know who owns it", so it refuses.
var ErrGateOwnerUnavailable = errors.New(
	"sessionstoreadapter: the released gate projection names no owning Host or lease epoch")

// LoadGate returns the durable gate and whether it exists.
//
// A gate that is NOT open is answerable soundly: absence from the page is the
// whole answer, and the owner members are documented as the state a gate was
// opened under. A gate that IS open is refused; see ErrGateOwnerUnavailable.
func (s *Store) LoadGate(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	gate sessionwire.GateID,
) (commands.Gate, bool, error) {
	page, err := s.store.ReadGates(ctx, sessionstore.ReadGatesRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		return commands.Gate{}, false, err
	}
	for _, projection := range page.Gates {
		if projection.GateID != gate {
			continue
		}
		return commands.Gate{}, false, &GateOwnerUnavailableError{GateID: gate}
	}
	return commands.Gate{}, false, nil
}

// GateOwnerUnavailableError names the gate whose owner could not be read.
type GateOwnerUnavailableError struct {
	GateID sessionwire.GateID
}

func (e *GateOwnerUnavailableError) Error() string {
	return ErrGateOwnerUnavailable.Error() + ", so gate " + strconv.Quote(string(e.GateID)) +
		" cannot be decided"
}

func (e *GateOwnerUnavailableError) Unwrap() error { return ErrGateOwnerUnavailable }

// ---------------------------------------------------------------------------
// Gate publication, bound to one session's residency grant
// ---------------------------------------------------------------------------

// GateJournals resolves the runtime journal a session's gates are folded from.
// TenantEvidenceRouter satisfies it: the journal store registered for a
// (tenant, storage binding) is the one the runtime writes, which is the only
// journal whose GateOpened events describe this session's gates.
type GateJournals interface {
	GateJournal(tenant sessionwire.TenantID, storageBindingID string) (EventReplayers, bool)
}

// EventReplayers opens a positioned replay of one runtime session's PUBLIC
// events. *harness sessionstore.Store satisfies it.
type EventReplayers interface {
	OpenEventReplayer(uuid.UUID, harnesssessionstore.ReplayRequest) (journal.EventReplayer, error)
}

// The released harness store is a gate journal. A drift is a build failure.
var _ EventReplayers = (*harnesssessionstore.Store)(nil)

// GateJournals is satisfied by the router.
var _ GateJournals = (*TenantEvidenceRouter)(nil)

// GateJournal returns the journal store serving (tenant, binding), when it can
// replay events.
func (r *TenantEvidenceRouter) GateJournal(tenant sessionwire.TenantID, storageBindingID string) (EventReplayers, bool) {
	reader, served := r.readers[EvidenceKey{TenantID: tenant, StorageBindingID: storageBindingID}]
	if !served {
		return nil, false
	}
	replayers, ok := reader.(EventReplayers)
	return replayers, ok
}

// WithGateJournals supplies the journals gates are folded from. Without it,
// GateSessionFor refuses.
func WithGateJournals(journals GateJournals) Option {
	return func(s *Store) { s.gateJournals = journals }
}

// ErrNoGateJournals is GateSessionFor's refusal for a store built without
// WithGateJournals.
var ErrNoGateJournals = errors.New("sessionstoreadapter: no gate journals were supplied; see WithGateJournals")

// GateSession is one resident session's durable gate surface, bound to the
// residency GRANT this store issued for it.
//
// THE GRANT IS HELD AND NEVER PASSED, for the claim edge's reason: sessionstore
// v0.12.0's gate writes on a disposition session take a *ResidencyGrant and
// refuse a bare LeaseEpoch, because the grant's epoch becomes the projection's
// residency mark and a caller-chosen number could lock every later holder out.
type GateSession struct {
	store    *sessionstore.Store
	grant    *sessionstore.ResidencyGrant
	journals GateJournals
	tenant   sessionwire.TenantID
	session  sessionwire.SessionID

	// Set by Scope, from the one binding read.
	runtime uuid.UUID
	replay  EventReplayers
}

// GateSession is the publisher's seam.
var _ gates.Session = (*GateSession)(nil)

// GateSessionFor binds one session's gate writes to the residency grant this
// store issued for it. It refuses a lease this package did not issue, exactly
// as DispositionWriterFor does, and returns the seam rather than the concrete
// type so its refusal is a real nil.
func (s *Store) GateSessionFor(lease residency.Lease, tenant sessionwire.TenantID, session sessionwire.SessionID) (gates.Session, error) {
	held, ok := lease.(*ResidencyLease)
	if !ok || held == nil || held.grant == nil {
		return nil, ErrForeignLease
	}
	if s.gateJournals == nil {
		return nil, ErrNoGateJournals
	}
	return &GateSession{store: s.store, grant: held.grant, journals: s.gateJournals, tenant: tenant, session: session}, nil
}

// Scope reads the session's catalog record once: the Core identity the
// projection is answered under, and the runtime identity its events carry, from
// the SAME read of the immutable binding.
func (g *GateSession) Scope(ctx context.Context) (gates.Scope, error) {
	entry, err := g.store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: g.tenant, SessionID: g.session})
	if err != nil {
		return gates.Scope{}, err
	}
	record := entry.Record
	runtimeID, err := uuid.Parse(record.Binding.RuntimeSessionID)
	if err == nil && runtimeID.IsZero() {
		err = errors.New("the zero UUID names no session")
	}
	if err != nil {
		return gates.Scope{}, &RuntimeSessionIDError{TenantID: g.tenant, SessionID: g.session, Cause: err}
	}
	replay, served := g.journals.GateJournal(record.TenantID, record.Binding.StorageBindingID)
	if !served {
		return gates.Scope{}, &UnroutableBindingError{TenantID: string(record.TenantID), Binding: record.Binding.StorageBindingID}
	}
	g.runtime, g.replay = runtimeID, replay
	return gates.Scope{
		TenantID:         record.TenantID,
		SessionID:        record.SessionID,
		AgentID:          record.AgentID,
		RuntimeSessionID: runtimeID,
	}, nil
}

// Projected returns the gates the durable projection holds open.
func (g *GateSession) Projected(ctx context.Context) ([]sessionwire.GateProjection, error) {
	entry, err := g.store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: g.tenant, SessionID: g.session})
	if err != nil {
		return nil, err
	}
	return append([]sessionwire.GateProjection(nil), entry.Record.OpenGates...), nil
}

// Open projects one gate under the bound grant.
func (g *GateSession) Open(ctx context.Context, projection sessionwire.GateProjection) error {
	_, err := g.store.OpenGate(ctx, sessionstore.OpenGateRequest{
		TenantID:  g.tenant,
		SessionID: g.session,
		Gate:      projection,
		Residency: g.grant,
	})
	return classifyGateWrite(err)
}

// Resolve removes one gate from the projection under the bound grant.
func (g *GateSession) Resolve(ctx context.Context, gate sessionwire.GateID) error {
	_, err := g.store.ResolveGate(ctx, sessionstore.ResolveGateRequest{
		TenantID:  g.tenant,
		SessionID: g.session,
		GateID:    gate,
		Residency: g.grant,
	})
	return classifyGateWrite(err)
}

// Replay visits the runtime session's public events from the inclusive
// sequence from.
func (g *GateSession) Replay(ctx context.Context, from uint64, visit func(event.Event, uint64) error) error {
	if g.replay == nil {
		return errors.New("sessionstoreadapter: Replay before Scope; the runtime identity is read from the binding")
	}
	replayer, err := g.replay.OpenEventReplayer(g.runtime, harnesssessionstore.ReplayRequest{FromSeq: from})
	if err != nil {
		return err
	}
	cursor, err := replayer.Open(ctx, journal.ReplayRequest{SessionID: g.runtime, From: journal.Beginning()})
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		ev, seq, err := cursor.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := visit(ev, seq); err != nil {
			return err
		}
	}
}

// classifyGateWrite maps a gate write's two ownership-and-permanence answers
// onto the Host's sentinels and leaves the rest alone.
//
// CatalogErrorEpoch is the mark above this grant: a successor has written, and
// it is residency.ErrEpochSuperseded. CatalogErrorDeleted on the gate intent is
// sessionstore's booked F2 residue — a stale resolve tombstoned this gate's
// deadline intent, and "a tombstoned intent cannot be re-published" — so the
// gate can never be projected under that identity: gates.ErrUnpublishable.
func classifyGateWrite(err error) error {
	var catalog *sessionstore.CatalogError
	if !errors.As(err, &catalog) {
		return err
	}
	switch {
	case catalog.Code == sessionstore.CatalogErrorEpoch:
		return errors.Join(residency.ErrEpochSuperseded, err)
	case catalog.Code == sessionstore.CatalogErrorDeleted && catalog.Field == "gate_intent":
		return errors.Join(gates.ErrUnpublishable, err)
	default:
		return err
	}
}
