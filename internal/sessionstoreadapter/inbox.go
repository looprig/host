package sessionstoreadapter

import (
	"context"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
)

// LoadCommand returns the full inbox record for one command.
func (s *Store) LoadCommand(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (commands.Record, error) {
	entry, err := s.store.GetCommand(ctx, sessionstore.GetCommandRequest{
		TenantID:  tenant,
		SessionID: session,
		CommandID: command,
	})
	if err != nil {
		return commands.Record{}, err
	}
	return adaptRecord(entry)
}

// LoadPayload returns the private body or its object reference.
//
// IT IS A SECOND CALL AGAINST A STORE THAT ANSWERS BOTH IN ONE, which is finding
// F9. commands declares two methods so that "the payload is loaded AFTER the
// claim" is a thing a test can see; the released record carries Payload and
// PayloadRef inline, so this reads the same record again. The ordering the two
// seams express is therefore a Host discipline here and not a store-enforced
// one — a fake can refuse an unclaimed payload read and the store cannot.
func (s *Store) LoadPayload(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (commands.Payload, error) {
	entry, err := s.store.GetCommand(ctx, sessionstore.GetCommandRequest{
		TenantID:  tenant,
		SessionID: session,
		CommandID: command,
	})
	if err != nil {
		return commands.Payload{}, err
	}
	return commands.Payload{Body: entry.Record.Payload, Ref: entry.Record.PayloadRef}, nil
}

// FindApplication correlates the journal against the command's own durable
// identities.
func (s *Store) FindApplication(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (commands.Application, error) {
	found, err := s.store.FindCommandApplication(ctx, sessionstore.FindCommandApplicationRequest{
		TenantID:  tenant,
		SessionID: session,
		CommandID: command,
	})
	if err != nil {
		return commands.Application{}, err
	}

	outcome, err := adaptOutcome(found.Outcome)
	if err != nil {
		return commands.Application{}, err
	}

	application := commands.Application{
		CommandID:        found.CommandID,
		Outcome:          outcome,
		PrefixEpoch:      found.PrefixEpoch,
		EffectEventID:    found.EffectEventID,
		EffectSeq:        found.EffectSeq,
		SupersedingEpoch: found.SupersedingEpoch,
	}
	// An absent application names no runtime identity, and the store leaves it
	// empty rather than zero-filling it. Parsing "" would refuse the one outcome
	// that is not a failure.
	if found.RuntimeCommandID != "" {
		runtimeCommandID, err := adaptRuntimeCommandID(found.RuntimeCommandID)
		if err != nil {
			return commands.Application{}, err
		}
		application.RuntimeCommandID = runtimeCommandID
	}
	return application, nil
}

// ClaimCommand CASes a record into claimed and returns its new revision.
func (s *Store) ClaimCommand(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	claim commands.Claim,
) (uint64, error) {
	entry, err := s.store.ClaimCommand(ctx, sessionstore.ClaimCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: claim.ExpectedRevision,
		LeaseEpoch:       claim.Epoch,
		ClaimExpiresAt:   claim.ExpiresAt,
	})
	if err != nil {
		return 0, classifyInbox(err)
	}
	return entry.Revision, nil
}

// BeginApplying CASes a claimed record into applying and returns its new
// revision.
func (s *Store) BeginApplying(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	applying commands.Applying,
) (uint64, error) {
	entry, err := s.store.BeginApplyingCommand(ctx, sessionstore.BeginApplyingCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: applying.ExpectedRevision,
		LeaseEpoch:       applying.Epoch,
		ClaimExpiresAt:   applying.ExpiresAt,
	})
	if err != nil {
		return 0, classifyInbox(err)
	}
	return entry.Revision, nil
}

// CompleteCommand settles an applying command as applied.
func (s *Store) CompleteCommand(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	completion commands.Completion,
) error {
	_, err := s.store.CompleteCommand(ctx, sessionstore.CompleteCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: completion.ExpectedRevision,
		LeaseEpoch:       completion.Epoch,
		Result: sessionstore.CommandResult{
			CompletedAt: completion.Effect.CompletedAt,
			EventID:     completion.Effect.EventID,
			JournalSeq:  completion.Effect.JournalSeq,
		},
	})
	return classifyInbox(err)
}

// RejectCommand settles a command with a durable typed reason.
func (s *Store) RejectCommand(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	rejection commands.Rejection,
) error {
	_, err := s.store.RejectCommand(ctx, sessionstore.RejectCommandRequest{
		TenantID:         tenant,
		SessionID:        session,
		CommandID:        command,
		ExpectedRevision: rejection.ExpectedRevision,
		LeaseEpoch:       rejection.Epoch,
		Rejection:        rejection.Detail,
	})
	return classifyInbox(err)
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// adaptRecord translates one released inbox entry into the aggregate an applier
// reads.
func adaptRecord(entry sessionstore.InboxEntry) (commands.Record, error) {
	state, err := adaptState(entry.Record.State)
	if err != nil {
		return commands.Record{}, err
	}
	runtimeCommandID, err := adaptRuntimeCommandID(entry.Record.RuntimeCommandID)
	if err != nil {
		return commands.Record{}, err
	}
	return commands.Record{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		RuntimeCommandID: runtimeCommandID,
		Kind:             commands.Kind(entry.Record.Kind),
		State:            state,
		AcceptedOrder:    entry.AcceptedOrder,
		Revision:         entry.Revision,
		ApplyDeadline:    entry.Record.ApplyDeadline,
		ClaimEpoch:       entry.Record.Claim.LeaseEpoch,
		ClaimExpiresAt:   entry.Record.Claim.ExpiresAt,
	}, nil
}

// adaptState maps the store's durable state onto Host's.
//
// IT IS AN EXPLICIT MAP AND NOT A CONVERSION, though every member happens to
// spell the same. A conversion would make the two vocabularies one type by
// accident of spelling, so the day either side adds a state the other has never
// heard of, a string would walk through here and be reasoned about as if it were
// known. commands.State.known refuses an unknown state for exactly that reason;
// this refuses it one layer earlier, where the store's name is still available
// to name in the failure.
func adaptState(state sessionstore.InboxState) (commands.State, error) {
	switch state {
	case sessionstore.InboxStatePending:
		return commands.StatePending, nil
	case sessionstore.InboxStateClaimed:
		return commands.StateClaimed, nil
	case sessionstore.InboxStateApplying:
		return commands.StateApplying, nil
	case sessionstore.InboxStateApplied:
		return commands.StateApplied, nil
	case sessionstore.InboxStateRejected:
		return commands.StateRejected, nil
	default:
		return "", &UnmappedValueError{Vocabulary: "sessionstore.InboxState", Value: string(state)}
	}
}

// adaptOutcome maps what the journal proves onto Host's five-valued vocabulary.
func adaptOutcome(outcome sessionstore.CommandApplicationOutcome) (commands.ApplicationOutcome, error) {
	switch outcome {
	case sessionstore.CommandApplicationAbsent:
		return commands.ApplicationAbsent, nil
	case sessionstore.CommandApplicationCommitted:
		return commands.ApplicationCommitted, nil
	case sessionstore.CommandApplicationAbandoned:
		return commands.ApplicationAbandoned, nil
	case sessionstore.CommandApplicationUnresolved:
		return commands.ApplicationUnresolved, nil
	case sessionstore.CommandApplicationConflicted:
		return commands.ApplicationConflicted, nil
	default:
		return "", &UnmappedValueError{Vocabulary: "sessionstore.CommandApplicationOutcome", Value: string(outcome)}
	}
}

// adaptRuntimeCommandID parses the store's opaque runtime identity into the UUID
// Host's record declares.
//
// THE STORE DELIBERATELY IMPOSES NO UUID GRAMMAR — finding F11 — and validates
// this member as bounded opaque UTF-8, because the durable MAPPING is what the
// record makes authoritative rather than the shape of the identity. Host's
// Record and department.RuntimeCommand both declare a uuid.UUID, so a record
// written by an allocator that is not Harness cannot be represented here at all.
// It is refused rather than zeroed: a zero runtime identity is the one value
// runtimecommand.Admitted.Validate rejects outright, so zeroing would move the
// failure to the far side of a durable claim.
func adaptRuntimeCommandID(id sessionstore.RuntimeCommandID) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.UUID{}, &UnmappedValueError{
			Vocabulary: "sessionstore.RuntimeCommandID",
			Value:      string(id),
			Cause:      err,
		}
	}
	return parsed, nil
}

// UnmappedValueError reports a released value Host has no member for.
type UnmappedValueError struct {
	Vocabulary string
	Value      string
	Cause      error
}

func (e *UnmappedValueError) Error() string {
	message := "sessionstoreadapter: " + e.Vocabulary + " value " + strconv.Quote(e.Value) +
		" has no counterpart in Host's vocabulary"
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *UnmappedValueError) Unwrap() error { return e.Cause }
