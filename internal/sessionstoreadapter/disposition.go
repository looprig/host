package sessionstoreadapter

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	harnesssessionstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
)

// This file binds the DISPOSITION family's command lifecycle: the claim edge
// sessionstore v0.8.0 published, and the attempt and settlement edges v0.9.0
// completed with an evidence reader.
//
// IT IS NOT inbox.go's COUNTERPART METHOD FOR METHOD, and the differences are
// the protocol rather than an accident of naming:
//
//   - THE CLAIM EDGE TAKES A GRANT, NOT AN EPOCH. ClaimDispositionCommand's
//     request carries a *sessionstore.ResidencyGrant and derives the epoch from
//     it, refusing anything that is not a live-looking grant THAT store issued
//     for THAT session. So a caller cannot name one, and commands.DispositionClaim
//     has no epoch member. That is why the writer below is bound to ONE SESSION's
//     grant rather than being a method on the adapted store: a per-call grant
//     parameter would be a grant a caller chose, which is the thing the released
//     edge exists to make impossible.
//   - SETTLEMENT SUPPLIES NO OUTCOME. The store derives the expected descriptor
//     from its own immutable record, obtains the evidence through its configured
//     reader and verifies it before the write. The adapter reports the arm the
//     STORE chose and never composes one.
//   - THERE IS NO HOST-AUTHORED REJECTION. See commands/disposition.go.

// ---------------------------------------------------------------------------
// commands.DispositionRecords
// ---------------------------------------------------------------------------

// LoadDispositionCommand returns the full disposition record for one command.
func (s *Store) LoadDispositionCommand(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (commands.DispositionRecord, error) {
	entry, err := s.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID:  tenant,
		SessionID: session,
		CommandID: command,
	})
	if err != nil {
		return commands.DispositionRecord{}, classifyInbox(err)
	}
	return adaptDispositionRecord(entry)
}

// LoadDispositionPayload returns the private body or its object reference.
//
// IT IS A SECOND CALL AGAINST A STORE THAT ANSWERS BOTH IN ONE, exactly as
// LoadPayload is, and for the same reason: commands declares two methods so that
// "the payload is loaded AFTER the claim" is a thing a test can see. The
// ordering is a Host discipline, not a store-enforced one.
func (s *Store) LoadDispositionPayload(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (commands.Payload, error) {
	entry, err := s.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID:  tenant,
		SessionID: session,
		CommandID: command,
	})
	if err != nil {
		return commands.Payload{}, classifyInbox(err)
	}
	payload := commands.Payload{Body: entry.Record.Descriptor.Payload}
	if entry.Record.Descriptor.PayloadObject != nil {
		payload.Ref = entry.Record.Descriptor.PayloadObject.Reference
	}
	return payload, nil
}

var _ commands.DispositionRecords = (*Store)(nil)

// ---------------------------------------------------------------------------
// commands.DispositionWrites, bound to one session's residency grant
// ---------------------------------------------------------------------------

// DispositionWriter is one session's disposition transitions, bound to the
// residency GRANT they are authorized under.
//
// THE GRANT IS HELD RATHER THAN PASSED, and that is the structural half of the
// claim edge's rule. sessionstore's ClaimDispositionCommand takes a
// *ResidencyGrant and derives the residency epoch from it — "the epoch comes off
// the grant and from nowhere else, so no number a caller chose can reach the
// record's mark" — and a writer that took a grant per call would be handing that
// choice back to a caller. Binding it once, to the session the grant is for, is
// what makes the rule hold for every edge here at once.
//
// THE RESIDENCY EPOCH IS STILL PASSED AT THE OTHER TWO EDGES, and that is not an
// inconsistency. BeginDispositionAttempt fences its epoch to EQUAL the claim's
// own, so it cannot raise the record's mark; SettleDispositionCommand records
// its epoch as settlement CONTEXT and compares it with no journal epoch. Only
// the claim edge writes the mark, so only the claim edge is gated on a grant.
type DispositionWriter struct {
	store *sessionstore.Store
	grant *sessionstore.ResidencyGrant
}

// DispositionWriter is the seam the applier writes through.
var _ commands.DispositionWrites = (*DispositionWriter)(nil)

// DispositionWriterFor binds one session's disposition writes to the residency
// grant this store issued for it.
//
// IT TAKES A residency.Lease AND ASSERTS, rather than taking a
// *sessionstore.ResidencyGrant, because the composition holds a Lease: the grant
// is private to ResidencyLease and reaches no caller, which is the property O3.4
// established and which this must not undo. The assertion is the narrowest way
// to recover it, and the refusal below is what stops a foreign Lease — a fake,
// or a future second implementation — from producing a writer with no grant at
// all.
// IT RETURNS THE SEAM RATHER THAN THE CONCRETE TYPE, and the refusal path
// returns an explicit nil for it. A `(*DispositionWriter, error)` signature
// would put a TYPED NIL into every caller's interface field on the error path —
// a non-nil interface holding nothing — and the composition's own check is
// `writer == nil`. That is the same defect the closer's absence arm has, found
// once already in this task's own fixture, and it is worth not shipping twice.
func (s *Store) DispositionWriterFor(lease residency.Lease) (commands.DispositionWrites, error) {
	held, ok := lease.(*ResidencyLease)
	if !ok || held == nil || held.grant == nil {
		return nil, ErrForeignLease
	}
	return &DispositionWriter{store: s.store, grant: held.grant}, nil
}

// ErrForeignLease is the refusal DispositionWriterFor returns for a lease this
// package did not issue.
//
// IT IS A REFUSAL RATHER THAN A DEGRADED WRITER. A writer with no grant could
// not claim anything, and would discover that at the first command of a session
// this Host had already taken residency of — which is the failure mode the
// composition root exists to make impossible.
var ErrForeignLease = errors.New("sessionstoreadapter: this residency lease was not issued by this store, so it carries no grant a disposition claim could be derived from")

// ClaimDisposition CASes a pending record into claimed under the bound grant.
func (w *DispositionWriter) ClaimDisposition(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	claim commands.DispositionClaim,
) (uint64, error) {
	entry, _, err := w.store.ClaimDispositionCommand(ctx, sessionstore.ClaimDispositionCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: claim.ExpectedRevision,
		Residency:        w.grant,
		ClaimExpiresAt:   claim.ExpiresAt,
	})
	if err != nil {
		return 0, classifyInbox(err)
	}
	// THE IDEMPOTENT ARM IS NOT AN ERROR AND ITS REVISION IS THE RIGHT ONE. A
	// replay of a live claim writes nothing and returns the STORED entry, whose
	// revision is what the attempt edge must then name; discarding it and
	// reporting a failure would turn a durable claim this Host already holds
	// into a blocked pass.
	return entry.Revision, nil
}

// BeginAttempt CASes a claimed record into applying and records the attempt.
func (w *DispositionWriter) BeginAttempt(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	attempt commands.DispositionAttempt,
) (uint64, error) {
	entry, err := w.store.BeginDispositionAttempt(ctx, sessionstore.BeginDispositionAttemptRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: attempt.ExpectedRevision,
		AttemptID:        sessionstore.DispositionAttemptID(attempt.AttemptID),
		// THE TWO EPOCHS CROSS AS TWO TYPES. sessionstore declares JournalEpoch
		// and ResidencyEpoch separately so that an attempt which kept one number
		// could not tell a successor which authority was held; the conversions
		// here are the only place the two vocabularies meet, and each takes its
		// value from its own source.
		JournalEpoch:   sessionstore.JournalEpoch(attempt.JournalEpoch),
		ResidencyEpoch: sessionstore.ResidencyEpoch(attempt.ResidencyEpoch),
		StartedAt:      attempt.StartedAt,
	})
	if err != nil {
		return 0, classifyInbox(err)
	}
	return entry.Revision, nil
}

// SettleDisposition settles an applying command from durable evidence and
// reports the terminal state the STORE chose.
func (w *DispositionWriter) SettleDisposition(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	settlement commands.DispositionSettlement,
) (commands.State, error) {
	entry, _, err := w.store.SettleDispositionCommand(ctx, sessionstore.SettleDispositionCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: settlement.ExpectedRevision,
		ResidencyEpoch:   sessionstore.ResidencyEpoch(settlement.ResidencyEpoch),
	})
	if err != nil {
		return "", classifySettlement(err)
	}
	// THE SECOND RESULT IS DISCARDED DELIBERATELY. It reports whether THIS call
	// wrote the terminal record, and false with a nil error is the idempotent
	// arm — a record already terminal at that revision, returned as it stands
	// with no evidence read. Both answers settle the command; a caller that
	// treated the replay as a failure would block a pass on a command that is
	// already done.
	return adaptState(entry.Record.State)
}

// RejectDisposition rejects a claimed command before any attempt was
// authorized. The released edge refuses a record carrying an attempt and admits
// only the live claim's holder, so the residency passed is this Host's own and
// cannot move any mark.
func (w *DispositionWriter) RejectDisposition(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	rejection commands.DispositionRejection,
) error {
	_, _, err := w.store.RejectDispositionCommand(ctx, sessionstore.RejectDispositionCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: rejection.ExpectedRevision,
		ResidencyEpoch:   sessionstore.ResidencyEpoch(rejection.ResidencyEpoch),
	})
	// THE SECOND RESULT IS DISCARDED for SettleDisposition's reason: false with
	// a nil error is the idempotent arm, a record this edge already rejected.
	return classifyInbox(err)
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// adaptDispositionRecord translates one released disposition entry into the
// aggregate an applier reads.
//
// THE ABSENT MEMBERS ARE LEFT ZERO RATHER THAN ZERO-FILLED FROM A SIBLING. A
// record with no claim and a record whose claim is at residency zero are the
// same value here, which is safe because every consumer of these members first
// reads the STATE: a pending record has no claim by the store's own validator,
// and commands.DispositionRecord.hasAttempt reads the identity rather than the
// epoch, so an attempt at journal epoch zero — which the store refuses to write
// — could not be mistaken for an absent one.
func adaptDispositionRecord(entry sessionstore.DispositionInboxEntry) (commands.DispositionRecord, error) {
	state, err := adaptState(entry.Record.State)
	if err != nil {
		return commands.DispositionRecord{}, err
	}
	descriptor := entry.Record.Descriptor
	runtimeCommandID, err := adaptRuntimeCommandID(descriptor.RuntimeCommandID)
	if err != nil {
		return commands.DispositionRecord{}, err
	}
	record := commands.DispositionRecord{
		TenantID:         descriptor.TenantID,
		SessionID:        descriptor.SessionID,
		CommandID:        descriptor.CommandID,
		RuntimeCommandID: runtimeCommandID,
		Kind:             commands.Kind(descriptor.Kind),
		State:            state,
		AcceptedOrder:    entry.AcceptedOrder,
		Revision:         entry.Revision,
		ApplyDeadline:    entry.Record.ApplyDeadline,
	}
	if claim := entry.Record.Claim; claim != nil {
		record.ClaimResidencyEpoch = uint64(claim.ResidencyEpoch)
		record.ClaimExpiresAt = claim.ExpiresAt
	}
	if attempt := entry.Record.Attempt; attempt != nil {
		record.AttemptID = commands.AttemptID(attempt.AttemptID)
		record.AttemptJournalEpoch = uint64(attempt.JournalEpoch)
		record.AttemptResidencyEpoch = uint64(attempt.ResidencyEpoch)
	}
	return record, nil
}

// classifySettlement marks the settlement failures that are this DEPLOYMENT's
// wiring rather than the runtime's silence.
//
// EVERY OUTCOME IT TOUCHES IS ALREADY SAFE, so this changes a diagnosis and never
// a durable answer: all four causes leave the command applying at an unmoved
// revision, and that is unchanged. What changes is who an operator is sent to
// look at. `evidence_unavailable` points at the runtime, which is right when the
// runtime recorded nothing and expensively wrong when the cause is the routing
// table — because by then a real effect has already been committed and the
// command is stranded.
//
// TWO CAUSES ARE NAMEABLE AND BOTH ALREADY ARRIVE TYPED. The information was
// being discarded by one unconditional mapping upstream, not missing:
//
//   - *UnroutableBindingError — HOST's own, raised by the router when no reader
//     is registered for the session's storage binding;
//   - *harnesssessionstore.DispositionBindingError — the RELEASED reader refusing
//     to RESOLVE the request at all: another tenant, a non-disposition binding,
//     or a RuntimeSessionID that is not a UUID. It is raised BEFORE any journal
//     read, which is exactly what separates it from an empty journal.
//
// THE THIRD IS LEFT ALONE ON PURPOSE. A router pointed at the WRONG journal store
// names a store that is healthy, at the correct tenant, and truthfully reporting
// that it holds no such record — the same answer the benign case gives, and
// correctly. Closing that needs a CONSTRUCTION-time answer (a journal store
// declaring which storage bindings it serves) that no released API offers, and a
// harness Tenant() accessor would not help: that store is at the right tenant.
//
// WHY THIS FILE NAMES harness AT ALL, since nothing else in this package does.
// The refusal is a released typed error and its producer is the only thing that
// can name the type; the alternative is matching a message, and a message is not
// an API. It is a SINGLE symbol used in a SINGLE errors.As, and the cost is
// stated rather than hidden: a deployment whose journal store is not harness's
// needs its resolve-refusal added here, and until it is, that store's wiring
// failures fall back to evidence_unavailable — which is the safe direction,
// because the fallback under-claims rather than mislabels.
func classifySettlement(err error) error {
	if err == nil {
		return nil
	}
	var unroutable *UnroutableBindingError
	if errors.As(err, &unroutable) {
		return errors.Join(commands.ErrEvidenceUnroutable, err)
	}
	var binding *harnesssessionstore.DispositionBindingError
	if errors.As(err, &binding) {
		return errors.Join(commands.ErrEvidenceUnroutable, err)
	}
	// EVERYTHING ELSE GOES THROUGH THE ORDINARY INBOX CLASSIFIER, which is what
	// this call site used to do unconditionally. Dropping it would lose
	// InboxErrorEpoch's promotion to ErrEpochSuperseded and leave this Host
	// retrying a session a successor has taken.
	return classifyInbox(err)
}
