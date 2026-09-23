package harnessadapter

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/createbody"
	"github.com/looprig/host/internal/gateresponse"
)

// boundSession is one launched harness session, expressed as the capabilities
// Host consumes.
//
// EVERY CAPABILITY IS A PUBLISHED HARNESS INTERFACE, held by the fields below,
// and none of them is spelled again here. Declaring a private structural twin
// would have been the weaker of the two available things: a private interface
// cannot produce a compile failure against a foreign type, so a harness change
// that moved a method would arrive as a silent ok == false and an
// IncapableSessionError at run time. Naming session.IdleWaiter and its siblings
// makes the same change a build failure, which is the property this package
// claims and now has.
type boundSession struct {
	controller session.SessionController

	idle       session.IdleWaiter
	live       session.Liveness
	releaser   session.Releaser
	committed  session.CommittedPublicEventSource
	leaseEpoch session.LeaseEpochReporter
	faults     session.PersistenceFaultReporter
	abandoner  session.ResidencyAbandoner

	tenant  sessionwire.TenantID
	session sessionwire.SessionID

	decode BlockDecoder
}

// ID is Harness's identity for the session.
func (s *boundSession) ID() uuid.UUID { return s.controller.SessionID() }

// WaitIdle blocks until the runtime has no work in flight.
func (s *boundSession) WaitIdle(ctx context.Context) error { return s.idle.WaitIdle(ctx) }

// Done closes once the runtime has begun tearing down.
func (s *boundSession) Done() <-chan struct{} { return s.live.Done() }

// ReleaseResidency releases residency without terminating the session.
func (s *boundSession) ReleaseResidency(ctx context.Context) error {
	return s.releaser.ReleaseResidency(ctx)
}

// LeaseEpoch reports the journal single-writer lease epoch THIS runtime holds,
// and whether it holds one. It is a straight forward of harness's capability.
//
// FORWARDING IT IS THE POINT AND NOT A FORMALITY. harness's own doc names the
// hazard: "a wrapper around a live session that does not forward the method
// silently opts its wrapped session out". boundSession is exactly such a wrapper,
// and a Host reading a silent (0, false) would conclude the runtime holds no
// journal grant and refuse every command it could in fact apply.
func (s *boundSession) LeaseEpoch() (uint64, bool) { return s.leaseEpoch.LeaseEpoch() }

// PersistenceFaulted closes when the runtime latches a terminal persistence
// fault. It is a straight forward of harness's capability.
func (s *boundSession) PersistenceFaulted() <-chan struct{} { return s.faults.PersistenceFaulted() }

// PersistenceFault reports the latched fault, or nil.
func (s *boundSession) PersistenceFault() error { return s.faults.PersistenceFault() }

// AbandonResidency gives the runtime up crash-equivalently, writing nothing.
func (s *boundSession) AbandonResidency(ctx context.Context) error {
	return s.abandoner.AbandonResidency(ctx)
}

// ErrResumeUnsupported is the refusal SubscribeCommitted returns for a non-empty
// resume point.
//
// FINDING H3. event.EventFilter selects loops and event classes and carries no
// position; nothing else in the released subscription API does either. Serving a
// positioned subscription from the live tail instead would open a gap between
// the caller's last durable event and the first live one, and neither side could
// see it — the caller believes it resumed, and the stream believes it started.
var ErrResumeUnsupported = errors.New(
	"harnessadapter: the released subscription cannot be positioned after an event")

// SubscribeCommitted delivers committed public journal events.
//
// The resume point must be empty; see ErrResumeUnsupported. The returned channel
// closes when the subscription ends, whether by the caller's context, by an
// intentional close, or by a hub-forced loss.
func (s *boundSession) SubscribeCommitted(
	ctx context.Context,
	after sessionwire.EventID,
) (<-chan sessionwire.EnduringPublication, error) {
	if after != "" {
		return nil, ErrResumeUnsupported
	}
	subscription, err := s.committed.SubscribeCommittedPublicEvents(event.EventFilter{
		Enduring: event.LoopScope{All: true},
	})
	if err != nil {
		return nil, err
	}

	published := make(chan sessionwire.EnduringPublication, committedEgressBuffer)
	go s.pump(ctx, subscription, published)
	return published, nil
}

// committedEgressBuffer is how many publications this adapter will hold for a
// Host consumer that has stopped taking them.
//
// IT IS HARNESS'S OWN NUMBER, not one chosen here: hub.defaultEgressBuffer is
// 256 and is the capacity of the buffer on the other side of this pump, whose
// doc says "one slow subscriber must never block a publisher or another
// subscriber". Matching it means Host adds one buffer of the same depth rather
// than a second, differently-sized answer to the same question.
const committedEgressBuffer = 256

// pump translates deliveries into publications until the subscription or the
// caller's context ends.
//
// IT DROPS A DELIVERY THAT IS NOT A COMMITTED PUBLICATION, and that is finding
// H4 rather than a filter. event.Delivery's three publication members are
// populated only for a live delivery whose durable append both committed a frame
// and stored a canonical public body; an ephemeral or private delivery has them
// at their zero values, and a zero EventID is not a publication Core would
// accept. Delivery.Committed is the released predicate for exactly that, so the
// decision is the module's rather than a second reading of its fields.
func (s *boundSession) pump(
	ctx context.Context,
	subscription event.Subscription,
	published chan<- sessionwire.EnduringPublication,
) {
	defer close(published)
	defer func() { _ = subscription.Close() }()

	deliveries := subscription.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, open := <-deliveries:
			if !open {
				return
			}
			if !delivery.Committed() {
				continue
			}
			publication := sessionwire.EnduringPublication{
				TenantID:       s.tenant,
				SessionID:      s.session,
				EventID:        sessionwire.EventID(delivery.EventID),
				JournalSeq:     delivery.JournalSeq,
				CoveredThrough: delivery.CoveredThrough,
				Body:           delivery.PublicBody,
			}
			// A NON-BLOCKING SEND, AND THE OVERFLOW ENDS THE PUMP. See
			// committedEgressBuffer and
			// TestThePumpAbsorbsABurstThenGivesUpRatherThanBlockingItsProducer.
			// A blocking send made this goroutine — the hub's only reader —
			// stop reading, which spends the hub's own egress budget and gets
			// the subscription failed there instead of here.
			//
			// THERE IS NO DROP BRANCH BECAUSE THERE IS NOTHING DROPPABLE. The
			// hub's class-aware policy drops Ephemeral and fails Enduring; the
			// committed stream never carries an Ephemeral delivery, the filter
			// opened above is enduring-only, and Delivery.Committed has already
			// excluded everything else. Every publication reaching this send is
			// enduring, so the only conforming policy is to stop — a skipped
			// one would leave the consumer a hole in coverage it is about to
			// persist as a cursor, which is the failure the committed stream
			// exists to prevent.
			select {
			case published <- publication:
			default:
				return
			}
		}
	}
}

// ApplyCommand applies one admitted runtime command.
//
// THE HOST KINDS AND BOTH PAYLOAD FORMS ARE NARROWER HERE THAN AT THE SEAM,
// and each refusal is a finding rather than a policy: H6 for the two kinds
// runtimecommand.Kind does not name, H7 for the decode an input requires,
// H8 for the object reference that has nowhere to go, and H5 for the lease epoch
// the seam does not carry.
func (s *boundSession) ApplyCommand(ctx context.Context, command department.RuntimeCommand) error {
	provider, ok := s.controller.(runtimecommand.Provider)
	if !ok {
		return &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "the harness session declares no runtime-command capability",
		}
	}
	// THE TWO-RESULT FORM IS CONSULTED, not assumed. runtimecommand.Provider
	// reports ok=false with a nil Applier for a session with no durable,
	// deduplicating application-prefix log, and its own documentation says the
	// point of the second result is that a single-result form would hand back an
	// applier that fails only after Host had acknowledged the command.
	applier, ok := provider.RuntimeCommands()
	if !ok || applier == nil {
		return &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "the harness session has no durable application-prefix log",
		}
	}

	admitted, err := s.admit(command)
	if err != nil {
		return err
	}
	disposition, err := applier.ApplyRuntimeCommand(ctx, admitted)
	if err != nil {
		return classifyDispatch(disposition, err)
	}
	return nil
}

// classifyDispatch marks the ONE dispatch failure that licenses a re-offer.
//
// *runtimecommand.DispositionUnsupportedError is raised BEFORE any durable
// write, so nothing happened and the command may be offered to another Host; a
// transport failure says the opposite, and a caller that could not tell them
// apart would either strand a re-offerable command or re-drive a real effect.
// Host is explicitly told to act on this, which is why harness moved the type
// into its public package — recognising a refusal by its message text is not an
// API.
//
// THE RELEASED ERROR ALWAYS SURVIVES. The sentinel is JOINED rather than
// substituted, so a caller that needs the identities harness named still reaches
// them through errors.As, and every other failure crosses untouched.
//
// THE DISPOSITION IS READ TOO (D3 gate F1). harness returns a NON-zero
// PrefixSequence alongside an error when the application prefix committed before
// the failure, and a zero one when nothing durable was written. Host needs that
// fact to tell a gate answer whose GateResolved may be durable (keep the runtime)
// from a command that never reached the journal (give the runtime up), so it is
// joined as department.ErrPrefixCommitted rather than discarded.
func classifyDispatch(disposition runtimecommand.Disposition, err error) error {
	var unsupported *runtimecommand.DispositionUnsupportedError
	if errors.As(err, &unsupported) {
		return errors.Join(department.ErrDispositionUnsupported, err)
	}
	if disposition.PrefixSequence != 0 {
		return errors.Join(department.ErrPrefixCommitted, err)
	}
	return err
}

// CloseAttempt writes the recovery closure for a PREDECESSOR's stranded attempt.
//
// THE CAPABILITY IS DISCOVERED ON THE RELEASED APPLIER AND NOT ADVERTISED
// UNCONDITIONALLY. runtimecommand.AttemptCloser is segregated from Applier for
// the reason department.AttemptCloser is segregated from Runtime, and the
// released contract says an implementation that cannot honour it must refuse AT
// THE CALL rather than advertise a closure it cannot write. That is what this
// does: the method exists on every bound session, and a session whose applier is
// not a closer is refused here.
//
// THE AUTHOR GRANT IS NOT A PARAMETER AND IS NEVER SUPPLIED. The released closer
// stamps it from the live lease it holds, because a caller-supplied author epoch
// would be a caller-authored proof, and a tombstone is the one thing that must
// never be one. What crosses is the ATTEMPT's grant, read from the durable
// record, which is what the closer's strictly-later comparison is made against.
func (s *boundSession) CloseAttempt(
	ctx context.Context,
	command sessionwire.CommandID,
	runtimeCommand uuid.UUID,
	kind string,
	attempt string,
	attemptJournalEpoch uint64,
) error {
	provider, ok := s.controller.(runtimecommand.Provider)
	if !ok {
		return &UnsupportedCommandError{
			CommandID: command,
			Kind:      kind,
			Reason:    "the harness session declares no runtime-command capability, so it has no recovery closure to offer",
		}
	}
	applier, ok := provider.RuntimeCommands()
	if !ok || applier == nil {
		return &UnsupportedCommandError{
			CommandID: command,
			Kind:      kind,
			Reason:    "the harness session has no durable application-prefix log, so it has no recovery closure to offer",
		}
	}
	closer, ok := applier.(runtimecommand.AttemptCloser)
	if !ok {
		return &UnsupportedCommandError{
			CommandID: command,
			Kind:      kind,
			Reason:    "this harness session's applier is not a recovery closer, so a predecessor's stranded attempt cannot be closed here",
		}
	}
	closure := runtimecommand.Closure{
		CommandID:           runtimecommand.CommandID(command),
		RuntimeCommandID:    runtimeCommand,
		Kind:                runtimecommand.Kind(kind),
		AttemptID:           runtimecommand.AttemptID(attempt),
		AttemptJournalEpoch: attemptJournalEpoch,
	}
	// VALIDATED BEFORE THE CLOSER, on the released type's own rule rather than a
	// restatement of it: a closure that cannot name an attempt is one that could
	// tombstone the wrong command, and calling Validate here rather than
	// re-deriving its conditions is what keeps the two from drifting.
	if err := closure.Validate(); err != nil {
		return err
	}
	if _, err := closer.CloseAttempt(ctx, closure); err != nil {
		return classifyClosure(err)
	}
	return nil
}

// The bound session is the segregated closure capability.
var _ department.AttemptCloser = (*boundSession)(nil)

// classifyClosure marks the ONE closure refusal a caller must never retry.
//
// *runtimecommand.EnduringEffectError means the predecessor's effect COMMITTED
// and only its evidence is missing, so a retry that became a tombstone would
// destroy it. The two authorization refusals are ordinary failures and are left
// alone: conflating them would send an operator looking for the wrong failure,
// and — worse — would make a transient "no live grant" look terminal.
func classifyClosure(err error) error {
	var enduring *runtimecommand.EnduringEffectError
	if errors.As(err, &enduring) {
		return errors.Join(department.ErrEnduringEffect, err)
	}
	return err
}

// admit translates one Host command into the released admitted command, or
// reports why it cannot be translated.
func (s *boundSession) admit(command department.RuntimeCommand) (runtimecommand.Admitted, error) {
	// SOURCED FROM THE RUNTIME'S OWN GRANT, and `held` is what is branched on.
	// harness gates the report on the lease still being Valid(), so a released or
	// lost lease answers (0, false) rather than the stale number it used to hold;
	// no pinned provider zeroes a dead lease's Epoch(), so reading the epoch alone
	// would stamp a live-looking dead value that the equality check at
	// ApplyRuntimeCommand would then reject as merely stale.
	epoch, held := s.leaseEpoch.LeaseEpoch()
	if !held {
		return runtimecommand.Admitted{}, &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "this runtime holds no single-writer journal lease, so there is no epoch an admitted command could name",
		}
	}
	if command.PayloadRef != (sessionwire.ObjectReference{}) {
		return runtimecommand.Admitted{}, &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "the released admitted command has no object-reference payload and no object reader",
		}
	}

	admitted := runtimecommand.Admitted{
		CommandID:        runtimecommand.CommandID(command.CommandID),
		RuntimeCommandID: command.RuntimeCommandID,
		Kind:             runtimecommand.Kind(command.Kind),
		LeaseEpoch:       epoch,
		// CARRIED, NOT MINTED, AND NOT DEFAULTED. An empty AttemptID crosses as
		// empty, which is the released type's own optional shape and means
		// harness writes no disposition frame; a value this adapter chose would
		// name an attempt the durable record never authorized, and harness would
		// then write evidence about it that no settlement could ever match.
		// Admitted.Validate bounds a non-empty one, and it is called below, so
		// an identity the released type refuses is refused BEFORE the applier.
		AttemptID: runtimecommand.AttemptID(command.AttemptID),
	}
	if !admitted.Kind.Valid() {
		return runtimecommand.Admitted{}, &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "harness applies only create, input, interrupt, restore and gate_response commands",
		}
	}
	if admitted.Kind == runtimecommand.KindGateResponse {
		// THE SAME DECODE THE APPLIER RAN BEFORE THE ATTEMPT, so a body refused
		// here was already rejected there and this refusal is unreachable from
		// a conforming applier. It names the bound session's Core identity,
		// which is what the stored body must name.
		response, err := gateresponse.Decode(command.Payload, s.session, command.CommandID)
		if err != nil {
			return runtimecommand.Admitted{}, err
		}
		admitted.GateResponse = &gate.GateResponse{
			GateID: gate.ID(response.GateID),
			Action: response.Request.Action,
			Values: response.Request.Values,
			// ALWAYS THE USER. A classifier source is refused by the session
			// on this path, and nothing Factory admits is a policy's answer.
			Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
		}
	}
	// A CREATE CARRIES THE SESSION'S FIRST MESSAGE, AND IT IS THE ONE KIND THE
	// GATE ABOVE ADMITS WHOSE BODY IS NOT THE SHAPE THE DECODER READS. Core
	// stores a create as a CreateRequest, whose Blocks member is omitempty, so
	// handing the payload straight to the input decoder would fail on every
	// create and reading no payload at all would drop the first message in
	// silence -- harness's own KindCreate doc names that second failure, because
	// an undecoded payload and an absent one are the same value on its side of
	// this seam and it cannot tell them apart.
	if admitted.Kind == runtimecommand.KindCreate {
		body, err := createbody.FirstMessage(command.Payload)
		if err != nil {
			return runtimecommand.Admitted{}, err
		}
		if len(body) != 0 {
			if s.decode == nil {
				return runtimecommand.Admitted{}, &UnsupportedCommandError{
					CommandID: command.CommandID,
					Kind:      command.Kind,
					Reason:    "no block decoder was bound, and a create carrying a first message must carry decoded content",
				}
			}
			blocks, err := s.decode(body)
			if err != nil {
				return runtimecommand.Admitted{}, err
			}
			admitted.Blocks = blocks
		}
	}
	// A RESTORE CARRIES NOTHING AND IS NOT LISTED HERE FOR THAT REASON.
	// Core's RestoreRequest has no blocks member, and Admitted.Validate refuses
	// a restore that carries any, so there is no arm to write: the kind crosses
	// with the identities and the epoch and nothing else. harness gates the
	// input preparation on the PAYLOAD rather than on the kind, so a restore
	// never borrows the input path's loop refusals.
	if admitted.Kind == runtimecommand.KindInput {
		if s.decode == nil {
			return runtimecommand.Admitted{}, &UnsupportedCommandError{
				CommandID: command.CommandID,
				Kind:      command.Kind,
				Reason:    "no block decoder was bound, and an input command must carry decoded content",
			}
		}
		blocks, err := s.decode(command.Payload)
		if err != nil {
			return runtimecommand.Admitted{}, err
		}
		admitted.Blocks = blocks
	}
	if err := admitted.Validate(); err != nil {
		return runtimecommand.Admitted{}, err
	}
	return admitted, nil
}
