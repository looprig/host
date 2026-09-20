package commands

import (
	"context"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/gateresponse"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// DispositionApplier
// ---------------------------------------------------------------------------

// DispositionApplierOptions configures a DispositionApplier.
type DispositionApplierOptions struct {
	// Host supplies the clock, the claim TTL and this Host's identity.
	Host *hostconfig.Host

	// Key is the session whose commands this applier applies.
	Key registry.Key

	// ResidencyEpoch is HOST's grant over the session. It is settlement and
	// authorization CONTEXT and is never used as a journal epoch.
	ResidencyEpoch uint64

	Records DispositionRecords
	Writes  DispositionWrites

	// Runtime is Host's control path into the resident runtime.
	Runtime department.CommandApplier

	// JournalEpochs reports the RUNTIME's own grant over its journal. It is a
	// separate collaborator from Runtime because it is a separate capability,
	// discovered by assertion in department, and because the two answers must be
	// visibly sourced differently: an attempt naming this Host's residency as a
	// journal epoch is an attempt no evidence could ever verify.
	JournalEpochs department.LeaseEpochReporter

	// Attempts mints the identity of each authorized dispatch.
	Attempts AttemptIDs

	// Closer drives a successor's recovery closure. IT IS OPTIONAL: a
	// composition without one blocks on a predecessor's stranded attempt rather
	// than concluding anything about it.
	Closer AttemptClosers

	// Gates reads the durable gate projection a gate_response is checked
	// against before its attempt. OPTIONAL: a composition without one blocks
	// every gate_response (RefusalNoGateReader) and applies every other kind.
	Gates Gates

	// Fence is the lease-epoch guard every durable write goes through.
	Fence Fence
}

// DispositionApplier claims, authorizes, dispatches and settles one command at a
// time in the disposition family. It is the Processor a composed Host runs, and
// it replaced the commands.NoDispatch refusal, deleted in the same change,
// when harness gained an attempt-aware journal writer.
type DispositionApplier struct {
	host      *hostconfig.Host
	key       registry.Key
	residency uint64
	records   DispositionRecords
	writes    DispositionWrites
	runtime   department.CommandApplier
	journal   department.LeaseEpochReporter
	attempts  AttemptIDs
	closer    AttemptClosers
	gates     Gates
	fence     Fence
}

// The disposition applier is the seam the consumer hands commands to.
var _ Processor = (*DispositionApplier)(nil)

// NewDispositionApplier validates options and returns a DispositionApplier.
func NewDispositionApplier(options DispositionApplierOptions) (*DispositionApplier, error) {
	if options.Host == nil {
		return nil, &InvalidConsumerOptionsError{Field: "Host", Reason: "must be set; the clock, the claim TTL and this Host's identity all come from it"}
	}
	// THE ACCESSORS THIS TYPE READS, AND ONLY THOSE, for NewApplier's reason:
	// &host.Host{} compiles from any package with none of them set, and a zero
	// claim TTL claims a command that has already expired.
	var unusable []string
	if options.Host.Clock() == nil {
		unusable = append(unusable, "Clock")
	}
	if options.Host.ClaimTTL() <= 0 {
		unusable = append(unusable, "ClaimTTL")
	}
	if options.Host.ID() == "" {
		unusable = append(unusable, "ID")
	}
	if len(unusable) > 0 {
		return nil, &UnusableHostError{Accessors: unusable}
	}
	if options.Key.TenantID == "" || options.Key.SessionID == "" {
		return nil, &InvalidConsumerOptionsError{Field: "Key", Reason: "must name both a tenant and a session; every SessionStore operation is scoped by the pair"}
	}
	if options.ResidencyEpoch == 0 {
		return nil, &InvalidConsumerOptionsError{Field: "ResidencyEpoch", Reason: "must be non-zero; a settlement stamped with a zero residency is unfenced"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Records", options.Records != nil},
		{"Writes", options.Writes != nil},
		{"Runtime", options.Runtime != nil},
		{"JournalEpochs", options.JournalEpochs != nil},
		{"Attempts", options.Attempts != nil},
		{"Fence", options.Fence != nil},
	} {
		if !required.present {
			return nil, &InvalidConsumerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	// Closer is NOT in that list, and the omission is the decision: it is
	// optional in harness for the reason it is optional here, and a composition
	// without one is refused at the one command that needs it rather than at
	// construction.
	return &DispositionApplier{
		host:      options.Host,
		key:       options.Key,
		residency: options.ResidencyEpoch,
		records:   options.Records,
		writes:    options.Writes,
		runtime:   options.Runtime,
		journal:   options.JournalEpochs,
		attempts:  options.Attempts,
		closer:    options.Closer,
		gates:     options.Gates,
		fence:     options.Fence,
	}, nil
}

// Process claims, authorizes, dispatches, recovers or settles one command.
//
// IT READS THE RECORD FRESH. The Command it is handed is the ordering
// decision's three fields, taken from a page that may be a whole pass old, and
// every transition below is a compare-and-swap against the record as it is now.
//
// THE FOUR ARMS ARE THE FOUR DURABLE STATES AND NOTHING ELSE. A terminal record
// is reported; an applying record has a durably authorized attempt and is
// settled or recovered; a claimed record resumes at the attempt when the claim
// is this Host's, and is claimed first when it is another residency's; a
// pending record starts at the claim.
func (a *DispositionApplier) Process(ctx context.Context, command Command) (Outcome, error) {
	record, err := a.records.LoadDispositionCommand(ctx, a.key.TenantID, a.key.SessionID, command.CommandID)
	if err != nil {
		return Outcome{State: command.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: command.CommandID,
			Reason:    "the durable record could not be read",
			Cause:     err,
		}
	}
	if problem := a.checkDispositionRecord(record, command); problem != nil {
		return Outcome{State: command.State}, problem
	}
	// A TERMINAL RECORD IS REPORTED, NOT WRITTEN. The page said it was not, so
	// somebody settled it in between; §10.4 never re-opens a terminal record.
	if record.State.Terminal() {
		return Outcome{State: record.State, PrefixOwned: record.hasAttempt()}, nil
	}
	switch record.State {
	case StateApplying:
		return a.settleOrRecover(ctx, record)
	case StateClaimed:
		// A CLAIM IS THIS HOST'S ONLY AT THIS HOST'S RESIDENCY. One held under
		// another — a predecessor that crashed mid-command — is taken first:
		// BeginAttempt fences its residency to EQUAL the claim's, so resuming
		// straight at the attempt was refused on every pass, forever. The
		// claim edge lets a later residency take a claim over and refuses an
		// earlier one, so this is safe in both directions.
		//
		// AND A CLAIM IS THIS HOST'S ONLY WHILE IT IS LIVE (spec gate M2). One
		// at this Host's residency that has lapsed — the pass blocked, on a
		// gate this Host did not yet own or an unreadable projection, for
		// longer than the claim TTL — is refused by BeginAttempt as lost, on
		// every pass, until Factory's deadline sweep rejects the command. The
		// store admits a new claim at the same residency over a lapsed one, so
		// it is re-taken first.
		//
		// N6, RECORDED AND NOT FIXED. The liveness test compares the STORE's
		// recorded expiry with THIS HOST's clock, and the two are different
		// clocks: ClaimExpiresAt was computed here as now+ClaimTTL and then
		// durably stored, so any skew between a Host and its peers reappears
		// here as an error in the comparison. The two directions cost different
		// things and neither is a correctness defect. A Host running FAST
		// declares its own live claim lapsed and re-takes it, which the store
		// admits at the same residency: one extra write. A Host running SLOW
		// treats a lapsed claim as live and goes straight to BeginAttempt,
		// which refuses it as lost -- the `claim_lost` loop -- until the skew
		// closes or the TTL is exceeded by more than the skew. Fixing it needs
		// a store-issued deadline this applier could compare against a
		// store-issued now, which is a released-module change; the honest thing
		// at this seam is to say which clock is being read.
		if record.ClaimResidencyEpoch != a.residency || !record.ClaimExpiresAt.After(a.now()) {
			return a.claimThenDispatch(ctx, record)
		}
		return a.authorizeAndDispatch(ctx, record, record.Revision)
	default:
		return a.claimThenDispatch(ctx, record)
	}
}

// checkDispositionRecord refuses a record this applier may not act on, and every
// refusal here happens BEFORE any durable write.
//
// THE ORDER IS THE ORDER THE ANSWERS BECOME PERMANENT, which is the released
// store's own discipline restated on this side. A foreign record is not this
// session's at any revision; an unrevisioned one cannot be compare-and-swapped
// at all; an unknown state cannot be reasoned about; an unknown kind is left for
// a Host that understands it.
func (a *DispositionApplier) checkDispositionRecord(record DispositionRecord, command Command) error {
	switch {
	case record.TenantID != a.key.TenantID || record.SessionID != a.key.SessionID:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason:    "the durable record names another tenant or another session",
		}
	case record.CommandID != command.CommandID:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason:    "the durable record names another command",
		}
	case record.AcceptedOrder != command.AcceptedOrder:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason:    "the durable record's immutable acceptance order is not the one the page reported",
		}
	case record.Revision == 0:
		return &ApplyError{
			Refusal:   RefusalUnrevisionedRecord,
			CommandID: command.CommandID,
			Reason:    "the durable record carries no provider revision, so no compare-and-swap could name one",
		}
	case !record.State.known():
		return &ApplyError{
			Refusal:   RefusalUnknownState,
			CommandID: command.CommandID,
			Reason:    "the durable record is in a state this Host cannot reason about",
		}
	case !record.Kind.known():
		// REFUSED AND NOT REJECTED. A newer Factory may admit a kind a newer
		// Host applies; a durable rejection here would destroy work a newer Host
		// in a rolling deployment would have done.
		return &ApplyError{
			Refusal:   RefusalUnsupportedKind,
			CommandID: command.CommandID,
			Reason:    "this Host cannot apply commands of this kind, and leaves the record for one that can",
		}
	default:
		return nil
	}
}

// claimThenDispatch takes the claim and continues into the attempt.
//
// THE CLAIM NAMES NO EPOCH. The concrete edge holds the residency GRANT this
// store issued for this session and derives the epoch from it, so no number
// chosen here can reach the record's high-water mark.
func (a *DispositionApplier) claimThenDispatch(ctx context.Context, record DispositionRecord) (Outcome, error) {
	var revision uint64
	err := a.fence.Write(func() error {
		claimed, err := a.writes.ClaimDisposition(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, DispositionClaim{
			ExpectedRevision: record.Revision,
			ExpiresAt:        a.now().Add(a.host.ClaimTTL()),
		})
		revision = claimed
		return err
	})
	if err != nil {
		return Outcome{State: record.State}, a.storeRefusal(record, "the command could not be claimed", err)
	}
	return a.authorizeAndDispatch(ctx, record, revision)
}

// authorizeAndDispatch mints an attempt, records it immutably, drives the
// runtime with it, and settles.
//
// THE PAYLOAD IS LOADED AFTER THE CLAIM AND BEFORE THE ATTEMPT, which is the one
// ordering the two seams exist to make visible: a body loaded before the claim
// would be a private read this Host had no claim to make, and a body loaded
// after the attempt would leave an authorized dispatch with nothing to dispatch.
func (a *DispositionApplier) authorizeAndDispatch(ctx context.Context, record DispositionRecord, revision uint64) (Outcome, error) {
	payload, err := a.records.LoadDispositionPayload(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the command's private body could not be read",
			Cause:     err,
		}
	}
	// A GATE RESPONSE IS CHECKED HERE, AFTER THE CLAIM AND BEFORE THE ATTEMPT,
	// because this is the last point at which a rejection is still possible:
	// once an attempt exists only the runtime's evidence settles the command.
	if record.Kind == KindGateResponse {
		rejected, problem := a.checkGateResponse(ctx, record, revision, payload)
		if problem != nil {
			return Outcome{State: StateClaimed}, problem
		}
		if rejected {
			return Outcome{State: StateRejected}, nil
		}
	}
	// THE RUNTIME'S OWN GRANT, AND `held` IS WHAT IS BRANCHED ON. No pinned
	// provider zeroes a released lease's epoch, so reading the number alone
	// would stamp a live-looking dead value into an immutable attempt.
	journalEpoch, held := a.journal.LeaseEpoch()
	if !held {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalNoJournalGrant,
			CommandID: record.CommandID,
			Reason:    "this runtime holds no single-writer journal lease, so there is no grant an attempt could name",
		}
	}
	attempt, err := a.attempts.NewAttemptID()
	if err != nil {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalInvalidAttempt,
			CommandID: record.CommandID,
			Reason:    "no attempt identity could be minted, and an attempt with no identity is one no evidence could ever name",
			Cause:     err,
		}
	}
	if problem := attempt.Validate(); problem != nil {
		return Outcome{State: StateClaimed}, problem
	}

	var applying uint64
	err = a.fence.Write(func() error {
		next, err := a.writes.BeginAttempt(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, DispositionAttempt{
			ExpectedRevision: revision,
			AttemptID:        attempt,
			JournalEpoch:     journalEpoch,
			ResidencyEpoch:   a.residency,
			StartedAt:        a.now(),
		})
		applying = next
		return err
	})
	if err != nil {
		return Outcome{State: StateClaimed}, a.storeRefusal(record, "the dispatch attempt could not be authorized", err)
	}

	// FROM HERE THE COMMAND IS `applying` AND IS CLOSED BY EVIDENCE. Every
	// return below therefore reports PrefixOwned: the attempt is durable and
	// recoverable by whoever holds the session next, which is exactly what that
	// field means.
	if problem := a.dispatch(ctx, record, payload, attempt); problem != nil {
		return Outcome{State: StateApplying, PrefixOwned: true}, problem
	}
	return a.settle(ctx, record, applying)
}

// checkGateResponse answers, before the attempt, every question Host can answer
// about a gate response, and reports whether it rejected the command.
//
// THREE ANSWERS, AND THEY ARE DIFFERENT KINDS OF ANSWER:
//
//   - A BODY NO HOST COULD EVER APPLY is rejected: stored by reference,
//     malformed, naming another session or command, a gate identity harness
//     never mints, or — for a gate
//     the projection holds and this Host owns — an expected-open version that
//     is not the projected one. The body is immutable and every Host reads the
//     same bytes, and harness would refuse it only after the attempt.
//   - A LIMIT OF THIS HOST blocks with nothing written: an unreadable
//     projection, no gate reader, or a gate whose residency mark is not this
//     Host's grant
//     (below it the fencing write has not landed; above it a successor wrote).
//   - EVERYTHING ELSE IS THE RUNTIME'S. A gate the projection no longer holds
//     is dispatched — by the Host that owns the projection's mark, and only by
//     it: harness is the authority on whether it is open, and
//     settles an answer to a closed gate as no_op. Rejecting on the projection
//     would race the publisher and could destroy a valid answer.
//
// OWNERSHIP IS DECIDED BY RESIDENCY EPOCH ONLY. The projection's mark is the
// residency of the last Host to write a gate; it is compared with this Host's
// residency grant and never with a journal epoch.
func (a *DispositionApplier) checkGateResponse(ctx context.Context, record DispositionRecord, revision uint64, payload Payload) (bool, error) {
	// A BODY STORED BY REFERENCE IS REJECTED, NOT BLOCKED (spec gate C1,
	// quality gate F5). This Host does not dereference a private object and
	// harness's admitted command has no reference member, so no released Host
	// can apply it — and a block holds the session's WHOLE command stream,
	// interrupts included, until Factory's deadline sweep rejects the answer
	// anyway. factory v0.5.0 refuses such a body at admission; this is for
	// every other path.
	if len(payload.Body) == 0 && payload.Ref != (sessionwire.ObjectReference{}) {
		return a.reject(ctx, record, revision)
	}
	if a.gates == nil {
		return false, &ApplyError{
			Refusal:   RefusalNoGateReader,
			CommandID: record.CommandID,
			Reason:    "this composition supplied no durable gate reader, so the gate cannot be checked before the attempt",
		}
	}
	response, err := gateresponse.Decode(payload.Body, a.key.SessionID, record.CommandID)
	if err != nil {
		return a.reject(ctx, record, revision)
	}
	gate, found, err := a.gates.LoadGate(ctx, a.key.TenantID, a.key.SessionID, response.Request.GateID)
	if err != nil {
		return false, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the durable gate could not be read, so the gate response could not be checked",
			Cause:     err,
		}
	}
	// OWNERSHIP IS THE PROJECTION'S MARK, WHETHER OR NOT THE GATE IS STILL
	// PROJECTED. A gate this Host's successor has already resolved is not
	// projected, and a predecessor that skipped the check on that arm began an
	// attempt its successor then closed as not_applied, losing the answer.
	if gate.OwnerEpoch != a.residency {
		return false, &ApplyError{
			Refusal:   RefusalGateNotOwned,
			CommandID: record.CommandID,
			Reason:    "the durable gate's residency mark is not this Host's grant, so this Host cannot yet say it holds the gate",
		}
	}
	if !found || !gate.Open {
		return false, nil
	}
	request := response.Request
	if (request.ExpectedOpenEventID != "" && request.ExpectedOpenEventID != gate.OpenedEventID) ||
		(request.ExpectedOpenJournalSeq != 0 && request.ExpectedOpenJournalSeq != gate.OpenedJournalSeq) {
		return a.reject(ctx, record, revision)
	}
	return false, nil
}

// reject durably rejects a claimed command before any attempt.
func (a *DispositionApplier) reject(ctx context.Context, record DispositionRecord, revision uint64) (bool, error) {
	err := a.fence.Write(func() error {
		return a.writes.RejectDisposition(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, DispositionRejection{
			ExpectedRevision: revision,
			ResidencyEpoch:   a.residency,
		})
	})
	if err != nil {
		return false, a.storeRefusal(record, "the gate response no Host could apply could not be rejected", err)
	}
	return true, nil
}

// dispatch drives the runtime with the authorized attempt's identity.
//
// THE TWO FAILURES ARE NOT ONE. A runtime that cannot record a disposition wrote
// nothing durable, so the command may be re-offered; a transport failure says
// the opposite, and collapsing them either strands a re-offerable command or
// re-drives a real effect.
func (a *DispositionApplier) dispatch(ctx context.Context, record DispositionRecord, payload Payload, attempt AttemptID) error {
	err := a.runtime.ApplyCommand(ctx, department.RuntimeCommand{
		CommandID:        record.CommandID,
		RuntimeCommandID: record.RuntimeCommandID,
		Kind:             string(record.Kind),
		Payload:          payload.Body,
		PayloadRef:       payload.Ref,
		AttemptID:        string(attempt),
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrDispositionUnsupported):
		return &ApplyError{
			Refusal:   RefusalDispositionUnsupported,
			CommandID: record.CommandID,
			Reason:    "the runtime's durable log cannot record a disposition, so nothing was dispatched and the command may be re-offered",
			Cause:     err,
		}
	default:
		return &ApplyError{
			Refusal:   RefusalRuntime,
			CommandID: record.CommandID,
			Reason:    "the runtime refused or failed the command after its attempt was durably authorized",
			Cause:     err,
		}
	}
}

// settle asks the store to settle from evidence and reports the arm IT chose.
//
// THIS HOST SUPPLIES NO OUTCOME. The store derives what it expects from its own
// immutable record, obtains the evidence through its configured reader and
// verifies it before the write.
func (a *DispositionApplier) settle(ctx context.Context, record DispositionRecord, revision uint64) (Outcome, error) {
	var settled State
	err := a.fence.Write(func() error {
		state, err := a.writes.SettleDisposition(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, DispositionSettlement{
			ExpectedRevision: revision,
			ResidencyEpoch:   a.residency,
		})
		settled = state
		return err
	})
	if err != nil {
		// ABSENCE IS NOT A DISPOSITION. The record stays applying at an unmoved
		// revision and the pass blocks; a Host that concluded "nothing happened"
		// from an unreadable journal would re-drive an effect that may already
		// have committed.
		//
		// BUT WHICH ABSENCE IT IS, IS INFORMATION HOST HAS AND WAS DISCARDING.
		// This mapping used to be unconditional, so a WIRING failure — a binding
		// with no registered reader, or a reader that refuses to resolve the
		// session — read identically to "the runtime recorded nothing", and an
		// operator was sent to look at a runtime that had done its job. The
		// durable answer is the same either way and must be; only the diagnosis
		// changes.
		refusal := RefusalEvidenceUnavailable
		reason := "the command's durable runtime disposition could not be read or verified, so it stays applying"
		if errors.Is(err, ErrEvidenceUnroutable) {
			refusal = RefusalEvidenceUnroutable
			reason = "this deployment has no settlement evidence reader that can resolve this session's journal, so the command stays applying with its effect already taken"
		}
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   refusal,
			CommandID: record.CommandID,
			Reason:    reason,
			Cause:     err,
		}
	}
	return Outcome{State: settled, PrefixOwned: true}, nil
}

// settleOrRecover handles an applying record, which is one whose attempt is
// already durable.
//
// THE DISCRIMINATOR IS THE JOURNAL GRANT AND NOT THE RESIDENCY. An attempt
// naming the grant this runtime holds is THIS runtime's own and is settled: a
// runtime closing its own attempt would tombstone work it may itself have done.
// An attempt naming a STRICTLY EARLIER grant belongs to a runtime that is gone,
// and only a successor may close one — which is the same comparison harness's
// own closer makes on the other side.
func (a *DispositionApplier) settleOrRecover(ctx context.Context, record DispositionRecord) (Outcome, error) {
	if !record.hasAttempt() {
		// An applying record with no attempt is a shape the store's own
		// validator refuses, so reaching it means the record disagrees with
		// itself. Nothing is concluded from it.
		return Outcome{State: StateApplying}, &ApplyError{
			Refusal:   RefusalUnknownState,
			CommandID: record.CommandID,
			Reason:    "the durable record is applying and names no attempt, which is a shape the store does not write",
		}
	}
	journalEpoch, held := a.journal.LeaseEpoch()
	if !held {
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalNoJournalGrant,
			CommandID: record.CommandID,
			Reason:    "this runtime holds no journal grant, so it can neither be the attempt's author nor close it",
		}
	}
	if record.AttemptJournalEpoch < journalEpoch {
		if problem := a.close(ctx, record); problem != nil {
			return Outcome{State: StateApplying, PrefixOwned: true}, problem
		}
	}
	return a.settle(ctx, record, record.Revision)
}

// close drives a successor's recovery closure for a predecessor's attempt.
func (a *DispositionApplier) close(ctx context.Context, record DispositionRecord) error {
	if a.closer == nil {
		return &ApplyError{
			Refusal:   RefusalNoAttemptCloser,
			CommandID: record.CommandID,
			Reason:    "a predecessor's attempt is stranded and this composition has no recovery closer, so nothing may be concluded about it",
		}
	}
	err := a.closer.CloseAttempt(ctx, record.CommandID, record.RuntimeCommandID, record.Kind, record.AttemptID, record.AttemptJournalEpoch)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNoAttemptCloser):
		// THE SAME ANSWER AS A NIL CLOSER, BY A DIFFERENT ROUTE. A composed
		// runtime reaches this applier through a wrapper that declares the
		// method for every runtime, so "no closer" arrives as a refusal rather
		// than as a failed assertion. Both mean the same thing and both must
		// block rather than conclude.
		return &ApplyError{
			Refusal:   RefusalNoAttemptCloser,
			CommandID: record.CommandID,
			Reason:    "a predecessor's attempt is stranded and this runtime offers no recovery closure, so nothing may be concluded about it",
			Cause:     err,
		}
	case errors.Is(err, ErrEnduringEffect):
		// NOT RETRYABLE, AND NEVER RETRIED INTO A TOMBSTONE. The predecessor's
		// effect committed and only its evidence is missing.
		return &ApplyError{
			Refusal:   RefusalEnduringEffect,
			CommandID: record.CommandID,
			Reason:    "the runtime's journal holds a durable effect for this command, so its stranded attempt may not be closed",
			Cause:     err,
		}
	default:
		return &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the predecessor's stranded attempt could not be closed",
			Cause:     err,
		}
	}
}

// storeRefusal wraps a durable-transition failure.
//
// IT DOES NOT CLASSIFY THE STORE'S VOCABULARY, and that is deliberate rather
// than unfinished: the concrete edge in internal/sessionstoreadapter maps the
// released inbox codes onto this package's sentinels, so a classification here
// would be a second authority free to drift from it.
func (a *DispositionApplier) storeRefusal(record DispositionRecord, what string, err error) error {
	var apply *ApplyError
	if errors.As(err, &apply) {
		return err
	}
	return &ApplyError{
		Refusal:   RefusalStore,
		CommandID: record.CommandID,
		Reason:    what,
		Cause:     err,
	}
}

// now is the composition's instant.
func (a *DispositionApplier) now() time.Time { return a.host.Clock().Now() }

// dispositionCommandID keeps the sessionwire import honest for the seam
// signatures above; it is the identity this applier scopes every call by.
var _ = sessionwire.CommandID("")
