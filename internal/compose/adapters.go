package compose

import (
	"context"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/commands"
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
// The journal grant, held for the life of a residency
// ---------------------------------------------------------------------------

// journalFencer is the composition's residency.JournalFencer.
//
// IT KEEPS WHAT THE FENCE PRODUCED, which is the whole reason it is not a thin
// forward. The released adapter's OpenSession takes the session's journal grant
// AND commits the opening fence in one call, and residency's seam reports only
// whether the fence committed — so a composition that forwarded and discarded
// would drop the one object an applier needs to append a correlation record,
// and the grant would stay held with nobody able to release it.
type journalFencer struct {
	service *Service
}

var _ residency.JournalFencer = (*journalFencer)(nil)

// journal returns this Service's fencer.
func (s *Service) journal() *journalFencer { return &journalFencer{service: s} }

// CommitOpeningFence opens the session and retains its journal grant.
//
// A REFUSED STASH RELEASES THE GRANT IT JUST TOOK, and that half is as
// load-bearing as the refusal. Returning while still holding the grant would
// trade the stranded grant stashGrant now refuses for a leaked one nobody has a
// handle to — the same defect with a different name — and the released store
// retains a Store admission for a live journal writer, so Close would wait on it
// forever.
func (f *journalFencer) CommitOpeningFence(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	grant, err := f.service.options.OpenSession(ctx, tenant, session)
	if err != nil {
		return err
	}
	key := registry.Key{TenantID: tenant, SessionID: session}
	if err := f.service.stashGrant(key, grant); err != nil {
		return errors.Join(err, grant.Release(ctx))
	}
	return nil
}

// stashGrant records the journal grant an opening fence produced, to be taken
// by the ownership that follows it in the same attach.
//
// IT REFUSES A SECOND GRANT FOR ONE KEY rather than overwriting the first, and
// the guard is HERE rather than only at the caller for the reason O7.1's
// cross-tenant bypass is a live lesson in this module: that defect survived its
// own guard because the guard was a caller's discipline asserted in another
// package, with the object that depended on it asserting nothing. The map is
// this package's, the invariant is about the map, and so the check is on the map.
//
// IT IS DEFENCE IN DEPTH AND NOT A LIVE FIX, said plainly. residency.Manager's
// per-key attach slot means one opening per key today, so nothing in this module
// can reach the refusal through an attach; the two tests that drive it call the
// fencer directly. What changes is that the property no longer depends on a
// promise made somewhere else.
func (s *Service) stashGrant(key registry.Key, grant JournalGrant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingGrants == nil {
		s.pendingGrants = map[registry.Key]JournalGrant{}
	}
	if _, held := s.pendingGrants[key]; held {
		return &PendingGrantHeldError{Key: key}
	}
	s.pendingGrants[key] = grant
	return nil
}

// PendingGrantHeldError reports a second opening fence for a session whose first
// grant has not yet been taken by an ownership.
type PendingGrantHeldError struct {
	Key registry.Key
}

func (e *PendingGrantHeldError) Error() string {
	return "compose: session " + string(e.Key.TenantID) + "/" + string(e.Key.SessionID) +
		" already holds an untaken journal grant, so a second opening fence would strand it"
}

// takeGrant removes and returns the grant stashed for a key.
//
// IT IS A TAKE AND NOT A READ. The grant belongs to exactly one residency, and
// leaving it behind would let a later attach of the same session adopt a grant
// the previous attach is still holding — which is the shape of a double release.
func (s *Service) takeGrant(key registry.Key) (JournalGrant, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	grant, held := s.pendingGrants[key]
	delete(s.pendingGrants, key)
	return grant, held
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
	work, grant, err := o.service.beginWork(ctx, request)
	if err != nil {
		_ = handle.Stop(ctx)
		return nil, err
	}
	if err := o.service.trackResident(request, halves, work, grant); err != nil {
		work.stop()
		_ = handle.Stop(ctx)
		return nil, err
	}
	return handle, nil
}

// beginWork starts one session's durable command consumer and live event relay.
func (s *Service) beginWork(ctx context.Context, request residency.OwnershipRequest) (*sessionWork, JournalGrant, error) {
	grant, held := s.takeGrant(request.Key)
	if !held {
		return nil, nil, errors.New("compose: the attach committed no opening fence, so this session holds no journal grant to write under")
	}
	guard := request.Guard()
	applier, err := commands.NewApplier(commands.ApplierOptions{
		Host:         s.options.Host,
		Key:          request.Key,
		LeaseEpoch:   uint64(request.LeaseEpoch),
		Records:      s.options.Records,
		Applications: s.options.Applications,
		Gates:        s.options.Gates,
		Writes:       s.options.InboxWrites,
		Journal:      grant,
		Runtime:      request.Runtime,
		Fence:        guard,
	})
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	link, err := s.links.resolve(request.Key.TenantID)
	if err != nil {
		return nil, nil, err
	}
	tail, err := link.tails.Publish(ctx, request.Key, request.Runtime)
	if err != nil {
		return nil, nil, err
	}
	s.recordConsumer(request.Key, consumer)
	go consumer.Run(ctx)
	return &sessionWork{stop: func() {
		tail.Stop()
		consumer.Stop()
		s.forgetConsumer(request.Key)
	}}, grant, nil
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
func (s *Service) trackResident(request residency.OwnershipRequest, halves releaseHalves, work *sessionWork, grant JournalGrant) error {
	held := &resident{
		key:        request.Key,
		agent:      request.AgentID,
		generation: request.Generation,
		runtime:    request.Runtime,
		lease:      s.leaseFor(request.Key),
		journal:    grant,
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
func (r *leaseRecorder) AcquireSessionLease(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (residency.Lease, error) {
	lease, err := r.inner.AcquireSessionLease(ctx, tenant, session)
	if err != nil {
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
