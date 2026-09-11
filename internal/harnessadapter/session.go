package harnessadapter

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
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
// FOUR OF THE FIVE HOST KINDS AND BOTH PAYLOAD FORMS ARE NARROWER HERE THAN AT
// THE SEAM, and each refusal is a finding rather than a policy: H6 for the three
// kinds runtimecommand.Kind does not name, H7 for the decode an input requires,
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
	if _, err := applier.ApplyRuntimeCommand(ctx, admitted); err != nil {
		return err
	}
	return nil
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
	}
	if !admitted.Kind.Valid() {
		return runtimecommand.Admitted{}, &UnsupportedCommandError{
			CommandID: command.CommandID,
			Kind:      command.Kind,
			Reason:    "harness applies only input and interrupt commands",
		}
	}
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
