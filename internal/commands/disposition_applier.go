package commands

import (
	"context"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// DispositionApplier
// ---------------------------------------------------------------------------

// DispositionApplierOptions configures a DispositionApplier.
type DispositionApplierOptions struct {
	// Host supplies the clock, the claim TTL and this Host's identity.
	Host *host.Host

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

	// Fence is the lease-epoch guard every durable write goes through.
	Fence Fence
}

// DispositionApplier claims, authorizes, dispatches and settles one command at a
// time in the disposition family. It is the Processor a composed Host runs, and
// it replaced the commands.NoDispatch refusal, deleted in the same change,
// when harness gained an attempt-aware journal writer.
type DispositionApplier struct {
	host      *host.Host
	key       registry.Key
	residency uint64
	records   DispositionRecords
	writes    DispositionWrites
	runtime   department.CommandApplier
	journal   department.LeaseEpochReporter
	attempts  AttemptIDs
	closer    AttemptClosers
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
// settled or recovered; a claimed record already holds this Host's claim and
// resumes at the attempt; a pending record starts at the claim.
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
