package compose

import (
	"context"
	"errors"
	"log/slog"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// clockAdapter narrows the composition's Clock to the two one-method time
// sources registry and lifecycle declare.
//
// Both are satisfied by the SAME value, which is the property worth having: a
// composition free to satisfy them separately could hand the registry a real
// clock and the drain a fake one, and every bound compared across the two would
// be comparing different timelines.
type clockAdapter struct {
	clock Clock
}

var (
	_ registry.Clock  = clockAdapter{}
	_ lifecycle.Clock = clockAdapter{}
)

// Now is the composition's instant.
func (c clockAdapter) Now() time.Time { return c.clock.Now() }

// After is the composition's bound.
func (c clockAdapter) After(d time.Duration) <-chan time.Time { return c.clock.After(d) }

// warmClockAdapter narrows the composition's Clock to the warm countdown.
type warmClockAdapter struct {
	clock Clock
}

var _ residency.WarmClock = warmClockAdapter{}

// NewWarmTimer arms one session's warm countdown.
func (c warmClockAdapter) NewWarmTimer(d time.Duration) residency.WarmTimer {
	return c.clock.NewWarmTimer(d)
}

// ---------------------------------------------------------------------------
// Ownership: the heartbeat, the command consumer and the live tail
// ---------------------------------------------------------------------------

// sessionOwnership is the composition's residency.Ownership.
//
// residency sequences ownership LAST in an attach, after the epoch is fenced
// and the runtime exists, because all three things it starts write under the
// lease epoch. This adds the two internal/residency cannot know about — the
// durable command consumer and the live event relay — to the heartbeat its
// inner factory starts, and hands back one handle that stops all three.
type sessionOwnership struct {
	service *Service
	inner   residency.Ownership
}

var _ residency.Ownership = (*sessionOwnership)(nil)

// BeginOwnership starts the heartbeat, the consumer and the tail.
//
// THE ORDER IS HEARTBEAT, THEN CONSUMER, THEN TAIL, and it is the reverse of
// the order they stop in. The heartbeat is what publishes this Host's claim on
// the session, so a consumer that began applying commands before the claim was
// visible would be acting on a session no reader knew this Host held. The tail
// is last because it is the only one of the three that is purely outbound: a
// relay running with nothing to relay costs nothing, while the reverse — a
// consumer running with no relay — silently drops the events its own
// applications produce.
func (o *sessionOwnership) BeginOwnership(ctx context.Context, request residency.OwnershipRequest) (residency.OwnershipHandle, error) {
	handle, err := o.inner.BeginOwnership(ctx, request)
	if err != nil {
		return nil, err
	}
	halves, ok := handle.(releaseHalves)
	if !ok {
		_ = handle.Stop(ctx)
		return nil, errNoReleaseHalves
	}
	work, err := o.service.beginWork(ctx, request)
	if err != nil {
		_ = handle.Stop(ctx)
		return nil, err
	}
	if err := o.service.trackResident(request, halves, work); err != nil {
		work.stop()
		_ = handle.Stop(ctx)
		return nil, err
	}
	return handle, nil
}

// beginWork starts one session's durable command consumer, its applier and the
// live event relay.
//
// THE PROCESSOR IS THE DISPOSITION APPLIER, AND THAT IS WHAT REPLACED THE
// BOUNDARY. The removed commands.NoDispatch refused before any durable seam because harness
// could not write attempt-aware evidence: a command dispatched under the old
// writer committed an application prefix naming no attempt, the settlement
// verifier requires the attempt's identity, and no honest reader could supply
// one — so such a command sat `applying` forever with a real effect behind it.
// harness v0.34.0 writes the attempt-bearing disposition frame and sessionstore
// v0.9.0 verifies it, so there is now a settlement for a dispatch to reach, and
// the refusal is gone rather than relaxed.
//
// commands.Applier IS STILL NOT WIRED HERE, and the reason it was not before is
// unchanged: it is the LEGACY family's protocol, and a Host takes residency
// through AcquireResidency, which pins ProtocolModeDisposition. Both appliers
// compile against this composition; only one of them names edges a session this
// Host can hold actually has.
//
// THE WRITER IS BOUND TO THE SESSION'S GRANT AND THE ATTACH FAILS WITHOUT ONE.
// A Host holding residency it cannot write under is the shape that produced the
// boundary in the first place, so it is refused here rather than discovered at
// the first command.
func (s *Service) beginWork(ctx context.Context, request residency.OwnershipRequest) (*sessionWork, error) {
	guard := request.Guard()
	lease := s.leaseFor(request.Key)
	if lease == nil {
		return nil, errNoRecordedLease
	}
	writer, err := s.options.Writers.DispositionWriterFor(lease)
	if err != nil {
		return nil, err
	}
	// THE RUNTIME IS ASKED FOR ITS CLOSER RATHER THAN REQUIRED TO HAVE ONE.
	// A missing closer blocks recovery of a predecessor's attempt without
	// refusing ordinary attach. A department wrapper always has the method, so
	// its explicit availability report overrides the interface assertion.
	closer, available := attemptCloserFor(request.Runtime)
	// This is the session's composition point: only now has the launch target
	// returned a runtime, and the warning can name the affected session.
	if !available {
		s.options.logger().LogAttrs(ctx, slog.LevelWarn, "host: runtime cannot close a predecessor's stranded attempt",
			slog.String("condition", "attempt_closer_unavailable"),
			slog.String("tenant_id", string(request.Key.TenantID)),
			slog.String("session_id", string(request.Key.SessionID)))
	}
	applier, err := commands.NewDispositionApplier(commands.DispositionApplierOptions{
		Host:           s.options.Host,
		Key:            request.Key,
		ResidencyEpoch: uint64(request.LeaseEpoch),
		Records:        s.options.Records,
		Writes:         writer,
		Runtime:        request.Runtime,
		// THE JOURNAL EPOCH COMES FROM THE RUNTIME AND NEVER FROM request.LeaseEpoch,
		// which is this Host's RESIDENCY. The two are different authorities over
		// different stores; an attempt stamped with the wrong one names a grant no
		// evidence could ever verify, and the numbers agreeing early in a session's
		// life is an accident of two fresh counters rather than a relationship.
		JournalEpochs: request.Runtime,
		Attempts:      commands.UUIDAttemptIDs{},
		Closer:        closerOrNil(closer),
		Gates:         s.options.GateReads,
		Fence:         guard,
	})
	if err != nil {
		return nil, err
	}
	consumer, err := commands.NewConsumer(commands.Options{
		Host:       s.options.Host,
		Key:        request.Key,
		LeaseEpoch: uint64(request.LeaseEpoch),
		Inbox:      s.options.Inbox,
		Cursors:    s.options.Cursors,
		Processor:  applier,
		Fence:      guard,
	})
	if err != nil {
		return nil, err
	}
	link, err := s.links.resolve(request.Key.TenantID)
	if err != nil {
		return nil, err
	}
	tail, err := link.tails.Publish(ctx, request.Key, request.Runtime)
	if err != nil {
		return nil, err
	}
	// THE GATE PUBLISHER IS BOUND TO THE SAME GRANT, and a Host that cannot
	// bind one refuses the attach rather than holding a session whose gates no
	// Factory could ever see. It starts BEFORE the consumer so its fencing
	// write is under way when the first gate response is considered; the
	// consumer re-checks a gate it could not yet own when the publisher
	// converges (OnConverged hints it).
	var publisher *gates.Publisher
	if s.options.Gates != nil {
		session, err := s.options.Gates.GateSessionFor(lease, request.Key.TenantID, request.Key.SessionID)
		if err != nil {
			tail.Stop()
			return nil, err
		}
		publisher, err = gates.Start(ctx, gates.Options{
			Session:     session,
			Hints:       request.Runtime,
			After:       s.options.Clock.After,
			Retry:       s.options.gateRetry(),
			OnConverged: func() { consumer.Hint("") },
			Logger:      s.options.logger(),
		})
		if err != nil {
			tail.Stop()
			return nil, err
		}
	}
	s.recordConsumer(request.Key, consumer)
	go consumer.Run(ctx)
	return &sessionWork{stop: func() {
		tail.Stop()
		if publisher != nil && !publisher.Stop() {
			s.options.logger().Warn("host: the gate publisher did not stop within its bound; a store call is ignoring cancellation",
				"tenant_id", string(request.Key.TenantID), "session_id", string(request.Key.SessionID))
		}
		consumer.Stop()
		s.forgetConsumer(request.Key)
		// AND EVERY ROUTE TO THIS SESSION GOES WITH IT.
		//
		// It is NOT redundant with Tails.invalidate, which drops routes only
		// when a tail is LOST or REFUSED; relay deliberately treats Host's own
		// stop as neither, because "a cancellation races the close, and
		// attributing Host's own stop to a loss would invalidate routes nobody
		// lost". That reasoning is about the CAUSE recorded on the tail. The
		// route is a different question: the session is no longer resident
		// here, whoever ended it.
		//
		// IT USED TO BE DONE BY A SIDE EFFECT. Until the drain stopped closing
		// transports, every release was immediately followed by a transport
		// close that dropped the link's routes with it. A link-drained Host now
		// stays up, so a binding released this way would outlive its residency
		// on a Host that is still answering. Measured before this line existed.
		//
		// IT IS SCOPED TO THE SESSION AND NOT TO THE LINK. InvalidateSession
		// takes the key; dropping the link's whole table would cut a Factory's
		// routes to every OTHER session it holds on the same connection. See
		// TestReleasingOneSessionLeavesEveryOtherSessionSRoutesAlone.
		link.mux.InvalidateSession(request.Key)
	}}, nil
}

// attemptCloserFor asks both the method set and, when present, the wrapper's
// actual forwarded capability. A reporter cannot invent a missing method.
func attemptCloserFor(runtime any) (department.AttemptCloser, bool) {
	closer, ok := runtime.(department.AttemptCloser)
	if !ok {
		return nil, false
	}
	if reporter, ok := runtime.(interface{ AttemptCloserAvailable() bool }); ok && !reporter.AttemptCloserAvailable() {
		return nil, false
	}
	return closer, true
}

// errNoRecordedLease is the refusal beginWork makes when the composition has no
// grant recorded for a session it is starting ownership of.
//
// IT IS NOT AN IMPOSSIBLE STATE DRESSED AS A CHECK. leaseRecorder is the ONLY
// route from the grant to this composition — residency.Manager hands onward an
// OwnershipRequest carrying an epoch and a fence but not the Lease — so a change
// that stopped recording, or that recorded under a different key, would produce
// exactly this. The alternative is a nil lease reaching the writer factory and
// being refused there as "foreign", which names the wrong fault.
var errNoRecordedLease = errors.New("compose: no residency grant is recorded for this session, so no disposition writer can be bound to it")

// closerOrNil converts an absent capability into a real nil interface.
//
// A TYPED NIL IS NOT AN ABSENT COLLABORATOR. A failed type assertion yields a
// nil department.AttemptCloser, but assigning it through a second interface
// field can produce a NON-nil interface holding nothing, and the applier's
// "no closer" arm tests for nil. Making the conversion explicit is what stops a
// missing capability from becoming a panic at the one command that needs it.
func closerOrNil(closer department.AttemptCloser) commands.AttemptClosers {
	if closer == nil {
		return nil
	}
	return &closerAdapter{closer: closer}
}

// closerAdapter restates department's two-string closure in commands' own
// vocabulary, which is where a mis-ordered pair of same-typed parameters becomes
// a compile failure instead of a wrong tombstone.
type closerAdapter struct {
	closer department.AttemptCloser
}

var _ commands.AttemptClosers = (*closerAdapter)(nil)

// CloseAttempt forwards one closure.
func (a *closerAdapter) CloseAttempt(
	ctx context.Context,
	command sessionwire.CommandID,
	runtimeCommand uuid.UUID,
	kind commands.Kind,
	attempt commands.AttemptID,
	attemptJournalEpoch uint64,
) error {
	return a.closer.CloseAttempt(ctx, command, runtimeCommand, string(kind), string(attempt), attemptJournalEpoch)
}

// trackResident records the session this Host now holds, in the shape the drain
// and the warm release take it through.
// trackResident returns an error rather than swallowing one, and the two it can
// get back mean opposite things.
//
// ErrWarmReleaserStopped is ORDINARY during a drain — the releaser has been
// stopped and a session attached afterwards is simply not warm-watched, which
// is the correct outcome rather than a failure. ErrWarmSessionWatched is a
// DEFECT: it means this Host already holds a watch for the same key, so a second
// residency has been installed under a live one, and an attach that continued
// past it would leave two objects believing they own one session's release.
func (s *Service) trackResident(request residency.OwnershipRequest, halves releaseHalves, work *sessionWork) error {
	held := &resident{
		key:        request.Key,
		agent:      request.AgentID,
		generation: request.Generation,
		runtime:    request.Runtime,
		lease:      s.leaseFor(request.Key),
		halves:     halves,
		work:       work,
		checkpoint: s.options.Checkpointer.Checkpoint,
		workspaces: s.options.Workspaces,
	}
	s.mu.Lock()
	s.sessions[request.Key] = held
	s.mu.Unlock()
	if err := s.warm.Watch(warmSession{resident: held}); err != nil && !errors.Is(err, residency.ErrWarmReleaserStopped) {
		s.forget(request.Key, request.Generation)
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// The seams the composition itself satisfies
// ---------------------------------------------------------------------------

// ConsumerFor resolves the durable inbox consumer that owns a session, for the
// HostLink command delivery path.
func (s *Service) ConsumerFor(key registry.Key) (hostlink.CommandConsumer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	consumer, held := s.consumers[key]
	return consumer, held
}

var _ hostlink.CommandConsumers = (*Service)(nil)

// recordConsumer registers a session's consumer.
func (s *Service) recordConsumer(key registry.Key, consumer *commands.Consumer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumers == nil {
		s.consumers = map[registry.Key]*commands.Consumer{}
	}
	s.consumers[key] = consumer
}

// BlockedSessions counts the resident sessions whose durable command consumer
// has stopped at a command.
//
// IT IS THE FIRST PRODUCTION READER OF Consumer.LastPass. That method's own doc
// calls itself "the loop's only reader … without this a blocked predecessor is
// reported to nobody", and until this existed a composed binary was the nobody:
// a blocked pass returns a nil error, so Failures() never moves, and a Host
// wedged in its designed steady state read healthy on every exported signal.
//
// IT COUNTS SESSIONS AND NOT COMMANDS, deliberately. A pass reports the ONE
// command it stopped at, so a per-command count would be a count of ones; how
// many sessions are not advancing is the question an operator is actually
// asking, and it is the one this can answer honestly.
func (s *Service) BlockedSessions() uint64 {
	s.mu.Lock()
	consumers := make([]*commands.Consumer, 0, len(s.consumers))
	for _, consumer := range s.consumers {
		consumers = append(consumers, consumer)
	}
	s.mu.Unlock()
	var blocked uint64
	for _, consumer := range consumers {
		if consumer.LastPass().Blocked != nil {
			blocked++
		}
	}
	return blocked
}

var _ lifecycle.BlockedConsumers = (*Service)(nil)

// forgetConsumer removes a session's consumer.
func (s *Service) forgetConsumer(key registry.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumers, key)
}

// AcceptedWork reports whether the durable inbox holds work for this session
// that is accepted and not yet terminal.
//
// IT IS A DURABLE READ and it is the whole point of the warm release's re-read:
// a command accepted by another component since this session went idle is
// visible here and would not be in any local queue depth.
func (s *Service) AcceptedWork(ctx context.Context, key registry.Key) (bool, error) {
	cursor, err := s.cursorFor(ctx, key)
	if err != nil {
		return false, err
	}
	records, err := s.options.Inbox.ListOrdered(ctx, key.TenantID, key.SessionID, cursor, 1)
	if err != nil {
		return false, err
	}
	return len(records) > 0, nil
}

// cursorFor reads the durable consumption cursor.
//
// IT PROPAGATES AND NO LONGER SWALLOWS, which is finding C-1 and a reversal of
// what this function used to do. The old reasoning was that zero is the
// conservative answer — the re-read starts from the beginning, reports work
// where there may be none, and the session is NOT released, which is the safe
// side. That holds for a TRANSIENT read failure and does not hold for the
// refusal sessionstore v0.7.0 actually makes: a session with no disposition
// catalog, absent or bound to another protocol, is refused rather than answered
// about, permanently. Swallowing it holds such a session resident on every warm
// release attempt for the whole life of the process, on a Host that cannot
// consume one command in it, with nothing anywhere naming the cause.
//
// FAILING IS NOT ABANDONING THE WARM RELEASE. WarmReleaser's step 1 already
// aborts on an unreadable inbox and records WarmFailure{Step: WarmStepInbox},
// so the refusal reaches an operator with the session named — which is strictly
// more than the silent non-release the zero produced. A transient failure gets
// the same abort it always got, and the next tick tries again.
//
// ZERO IS NOT A FALLBACK POSITION. It is the answer for a session that EXISTS
// and has consumed nothing, and it is the store that draws that line: "a caller
// must not translate [the refusal] into 'no commands'".
func (s *Service) cursorFor(ctx context.Context, key registry.Key) (uint64, error) {
	return s.options.Cursors.LoadCursor(ctx, key.TenantID, key.SessionID)
}

// Wake resumes durable command consumption for a session whose release was
// aborted.
func (s *Service) Wake(key registry.Key) {
	s.mu.Lock()
	consumer, held := s.consumers[key]
	s.mu.Unlock()
	if held {
		consumer.Hint("")
	}
}

// WarmRelease records one warm release attempt's outcome.
//
// It forwards to the metrics collector and, on a release that completed, drops
// the composition's own handle on the session. A composition that kept it would
// go on offering a released session to the drain, which would then take a
// second session through a release protocol that has already run.
func (s *Service) WarmRelease(outcome residency.WarmOutcome) {
	s.metrics.WarmRelease(outcome)
	if outcome.Kind == residency.WarmOutcomeReleased {
		s.forget(outcome.Key, outcome.Generation)
	}
}

// ResidencyLost is handed a residency whose ownership is gone.
//
// THE RUNTIME ARRIVES UNRELEASED, by that seam's contract, and this is where
// the composition stops offering it. It does NOT run the release protocol: the
// grant is gone, so every fenced write in that protocol would be refused or,
// worse, accepted under an epoch a successor has superseded. What is owed is
// dropping the local machinery, which is what the runtime handle and the
// consumer are.
func (s *Service) ResidencyLost(ctx context.Context, lost residency.LostResidency) {
	s.mu.Lock()
	held, present := s.sessions[lost.Key]
	s.mu.Unlock()
	if present && held.generation == lost.Generation {
		held.stopWork()
	}
	s.warm.Forget(lost.Key)
	s.forget(lost.Key, lost.Generation)
}

// forget drops the composition's handle on one residency, fenced by generation
// so a late caller cannot remove the residency that REPLACED it.
func (s *Service) forget(key registry.Key, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, present := s.sessions[key]; present && held.generation == generation {
		delete(s.sessions, key)
	}
}

// leaseFor returns the residency grant recorded for a key by the lease recorder.
func (s *Service) leaseFor(key registry.Key) residency.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leases[key]
}

// fenceGates makes the projection-fencing gate write under a fresh grant, when
// this composition publishes gates.
func (s *Service) fenceGates(ctx context.Context, lease residency.Lease, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	if s.options.Gates == nil {
		return nil
	}
	fence, err := s.options.Gates.GateSessionFor(lease, tenant, session)
	if err != nil {
		return err
	}
	return fence.Resolve(ctx, gates.FenceGateID)
}

// leaseRecorder wraps the durable lease seam and remembers what it granted.
//
// THE COMPOSITION HAS NO OTHER WAY TO REACH THE GRANT, and that is a shape of
// the seams rather than a shortcut. residency.Manager takes the lease, and what
// it hands onward is an OwnershipRequest carrying an epoch and a fence but not
// the Lease itself; the warm release's own ReleaseLease step therefore needs an
// object nothing downstream is given. Recording it at the one place it is
// created is the narrowest fix, and it is fenced by the key so a re-attach
// replaces rather than accumulates.
type leaseRecorder struct {
	service *Service
	inner   residency.SessionLeases
}

var _ residency.SessionLeases = (*leaseRecorder)(nil)

// AcquireSessionLease takes the grant and records it.
//
// A REFUSAL RECORDS NOTHING, including the one refusal that still owes a
// release: LeaseCleanupError carries its own Release and the caller that owns
// the retry is residency's unwinder, not this.
//
// AND IT FENCES THE GATE PROJECTION, BEFORE THE RUNTIME IS RESTORED (quality
// gate F2). The residency grant exists from here, so one gate write under it
// raises the projection's mark past any predecessor now, while the runtime is
// still unlaunched. Before, the mark moved only when the publisher started,
// after the restore: in between, a predecessor that was not dead still passed
// its ownership check on an answer, began its attempt, lost the journal to this
// Host's restore, and the user's answer settled rejected/not_applied. Now that
// predecessor's check fails first and it never begins one. A failed fence
// releases the grant and refuses the attach.
func (r *leaseRecorder) AcquireSessionLease(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (residency.Lease, error) {
	lease, err := r.inner.AcquireSessionLease(ctx, tenant, session)
	if err != nil {
		return nil, err
	}
	if err := r.service.fenceGates(ctx, lease, tenant, session); err != nil {
		if releaseErr := lease.Release(context.WithoutCancel(ctx)); releaseErr != nil {
			return nil, &residency.LeaseCleanupError{Cause: errors.Join(err, releaseErr), Cleanup: lease.Release}
		}
		return nil, err
	}
	r.service.mu.Lock()
	if r.service.leases == nil {
		r.service.leases = map[registry.Key]residency.Lease{}
	}
	r.service.leases[registry.Key{TenantID: tenant, SessionID: session}] = lease
	r.service.mu.Unlock()
	return lease, nil
}
