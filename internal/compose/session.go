package compose

import (
	"context"
	"errors"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// releaseHalves is the pair residency.Heartbeat offers and
// residency.OwnershipHandle does not name.
//
// Ownership returns a handle whose only method is Stop, so a composition that
// held the interface alone could not mark a residency releasing. The heartbeat
// already carries the two halves — its own documentation records that they were
// split for this caller rather than fused — so the composition asserts for them
// rather than rebuilding the durable release it would otherwise have to.
type releaseHalves interface {
	residency.OwnershipHandle

	BeginRelease(context.Context) error
	FinishRelease(context.Context) error
}

// errNoReleaseHalves is what a composition reports when its Ownership returns a
// handle that cannot mark a residency releasing.
//
// It is a REFUSAL AT ATTACH rather than a failure at drain. A Host that
// discovered this at shutdown would have accepted every session it holds under
// a release protocol it cannot perform, and the discovery would arrive at the
// moment there is nothing left to do about it.
var errNoReleaseHalves = errors.New("compose: the ownership handle cannot begin or finish a release, so this residency could never be released without terminating it")

// resident is everything the composition holds about one session it has made
// resident.
//
// IT IS ONE OBJECT BEHIND TWO SEAMS, and the seams disagree; see releaseSession
// and warmSession. The fields are fixed at attach and the mutable part is the
// once-guards, because every step below is reachable from both a drain and a
// warm release and a session released twice must not release a lease twice.
type resident struct {
	key        registry.Key
	agent      sessionwire.AgentID
	generation uint64
	runtime    department.Runtime
	lease      residency.Lease
	journal    JournalGrant
	halves     releaseHalves
	work       *sessionWork
	checkpoint func(context.Context, sessionwire.TenantID, sessionwire.SessionID) error
	workspaces residency.Workspaces

	once struct {
		mu        sync.Mutex
		workDone  bool
		stopped   bool
		leaseGone bool
		dropped   bool
	}

	// The four steps above these are guarded ON ATTEMPT; these four are guarded
	// ON SUCCESS, and the split is a decision rather than an oversight.
	//
	// A DESTRUCTIVE STEP LATCHES ON ATTEMPT. stopWork, stopOwnership,
	// releaseLease and dropState hand something back, and a step that errored
	// may have handed it back anyway; retrying one of those is a release of
	// somebody else's grant if the session has since been re-acquired, which is
	// worse than a release that never retries.
	//
	// A DURABLE OR IDEMPOTENT STEP LATCHES ON SUCCESS, so that this layer never
	// turns a failed step into a nil return of its own. That matters most for
	// FinishRelease: internal/lifecycle reports `drained` unless FinishRelease
	// failed, because Core defines that state as "the observed scope has
	// finished release" and a Factory DELETES a dedicated workload on it. A
	// guard here that latched on attempt would let a warm release whose
	// tombstone write failed turn a later drain's FinishRelease into a silent
	// success, and the Host would report `drained` with nothing written.
	//
	// TWO OF THESE FOUR ARE BELT OVER AN EXISTING BRACE, AND THAT IS MEASURED
	// RATHER THAN ASSUMED. residency.Heartbeat — the only production
	// releaseHalves — already guards BeginRelease and FinishRelease itself
	// (heartbeat.go:408 and :433) and latches on ATTEMPT, returning its first
	// verdict forever; issuing halves.FinishRelease twice writes ONE tombstone,
	// which is why doubling that call is an equivalent mutation here and not a
	// live defect. What these two guards buy is independence from that: releaseHalves
	// is an INTERFACE and its contract states no such property, so a composition
	// that relied on one implementation's internals would be relying on
	// something nothing holds it to. They do not weaken the dependency's own
	// guard, which is the stricter of the two.
	//
	// The other two are load-bearing today. Checkpoint is the PRODUCT's seam and
	// ReleaseResidency is the runtime's, and neither promises idempotence;
	// without these guards a warm countdown firing during a drain checkpoints
	// twice and releases the runtime twice, and internal/lifecycle's own drain
	// test already treats a second runtime release as wrong.
	//
	// Neither seam retries a step within its own sequence — both run each step
	// exactly once and record the failure — so these guards only ever fire
	// ACROSS the two seams, which is the race they exist for.
	checkpointOnce onceOnSuccess
	beginOnce      onceOnSuccess
	finishOnce     onceOnSuccess
	residencyOnce  onceOnSuccess
}

// onceOnSuccess runs an action at most once SUCCESSFULLY, serializing concurrent
// callers so that a warm release and a drain reaching the same step at the same
// instant run it once between them rather than twice.
type onceOnSuccess struct {
	mu   sync.Mutex
	done bool
}

// run performs the action unless a previous call already completed it.
func (o *onceOnSuccess) run(action func() error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done {
		return nil
	}
	if err := action(); err != nil {
		return err
	}
	o.done = true
	return nil
}

// sessionWork is the per-session machinery ownership started: the durable
// command consumer and the live event tail.
type sessionWork struct {
	stop func()
}

// Key identifies the session.
func (r *resident) Key() registry.Key { return r.key }

// Generation is the registry generation this residency was installed under.
func (r *resident) Generation() uint64 { return r.generation }

// Lost closes when this Host's residency grant is gone.
func (r *resident) Lost() <-chan struct{} { return r.lease.Lost() }

// WaitIdle blocks until the runtime has no work in flight.
func (r *resident) WaitIdle(ctx context.Context) error { return r.runtime.WaitIdle(ctx) }

// BeginRelease marks the residency releasing, durably, without terminating it,
// once.
func (r *resident) BeginRelease(ctx context.Context) error {
	return r.beginOnce.run(func() error { return r.halves.BeginRelease(ctx) })
}

// Checkpoint commits the checkpoints the release requires, through the
// product's seam.
//
// THERE IS NO FALLBACK AND THERE MUST NOT BE ONE. host.Checkpointer has no
// implementation in this module because the state being committed is the
// runtime's and the workspace's, and Host holds neither; a composition that
// treated an absent checkpointer as success would report a drained Host whose
// sessions had lost whatever was not already durable. The constructor refuses a
// composition without one, so this field is never nil.
func (r *resident) Checkpoint(ctx context.Context) error {
	return r.checkpointOnce.run(func() error {
		return r.checkpoint(ctx, r.key.TenantID, r.key.SessionID)
	})
}

// ReleaseResidency stops this session's process-local work and releases the
// runtime, nonterminally.
//
// THE CONSUMER AND THE TAIL STOP FIRST, AND THE ORDER IS THE POINT. A consumer
// still running would go on applying admitted commands to a runtime that has
// been released, and a tail would go on relaying a stream nobody owns. The
// HEARTBEAT is deliberately not stopped here: the release is not finished, and
// the two remaining durable writes — the tombstone and, on a drain, the lease
// release — go through it.
func (r *resident) ReleaseResidency(ctx context.Context) error {
	r.stopWork()
	return r.residencyOnce.run(func() error { return r.runtime.ReleaseResidency(ctx) })
}

// stopWork ends the consumer and the tail, once.
func (r *resident) stopWork() {
	r.once.mu.Lock()
	defer r.once.mu.Unlock()
	if r.once.workDone {
		return
	}
	r.once.workDone = true
	if r.work != nil && r.work.stop != nil {
		r.work.stop()
	}
}

// finishRelease writes the epoch-fenced tombstone, once.
//
// It is the step BOTH SEAMS name FinishRelease and neither means the same thing
// by; see the two views below. What is common to them is this one durable
// write, so the guard lives here rather than in either view.
func (r *resident) finishRelease(ctx context.Context) error {
	return r.finishOnce.run(func() error { return r.halves.FinishRelease(ctx) })
}

// stopOwnership ends the heartbeat, once.
func (r *resident) stopOwnership(ctx context.Context) error {
	r.once.mu.Lock()
	stop := !r.once.stopped
	r.once.stopped = true
	r.once.mu.Unlock()
	if !stop {
		return nil
	}
	return r.halves.Stop(ctx)
}

// releaseLease hands back the residency grant and the journal grant, once.
//
// BOTH GRANTS, AND THE JOURNAL ONE IS THE HALF THAT IS EASY TO LOSE. A session
// holds two: Host's residency lease, whose loss a heartbeat observes, and the
// runtime's journal grant, taken by the same attach and released by nobody
// else. Leaving the second held would leave the session unattachable elsewhere
// for a reason nothing in the registry or the lease records.
func (r *resident) releaseLease(ctx context.Context) error {
	r.once.mu.Lock()
	release := !r.once.leaseGone
	r.once.leaseGone = true
	r.once.mu.Unlock()
	if !release {
		return nil
	}
	var failures []error
	if r.journal != nil {
		if err := r.journal.Release(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if err := r.lease.Release(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

// dropState drops the local workspace materialization, once. It never deletes
// durable state: ReleaseWorkspace is the local half by that seam's contract.
func (r *resident) dropState(ctx context.Context) error {
	r.once.mu.Lock()
	drop := !r.once.dropped
	r.once.dropped = true
	r.once.mu.Unlock()
	if !drop {
		return nil
	}
	return r.workspaces.ReleaseWorkspace(ctx, r.key.TenantID, r.key.SessionID)
}

// ---------------------------------------------------------------------------
// The two release seams, and where they disagree
// ---------------------------------------------------------------------------

// releaseSession is one resident session behind internal/lifecycle's drain
// seam.
//
// IT EXISTS BECAUSE THE TWO SEAMS GIVE ONE METHOD NAME TWO MEANINGS, which is a
// finding rather than a filing decision and is written here because a reader
// who assumed they agreed would produce a Host that leaks a lease on every
// drain. lifecycle.Session documents FinishRelease as "writes the epoch-fenced
// tombstone AND RELEASES THE LEASE"; residency.WarmSession documents its own
// FinishRelease as "writes the epoch-fenced tombstone and removes the visible
// Host registry entry", and gives the lease its own ReleaseLease and the local
// state its own DropState. One type cannot satisfy both under one name, so the
// composition supplies two views of one session and folds the warm path's last
// two steps into the drain path's last one. The ORDER inside the fold is the
// warm path's: tombstone, then heartbeat, then lease, then local state.
type releaseSession struct {
	*resident
}

// releaseSession is a lifecycle.Session.
var _ lifecycle.Session = releaseSession{}

// FinishRelease writes the tombstone, ends the heartbeat, releases both grants
// and drops local state, which is what this seam's FinishRelease means.
func (s releaseSession) FinishRelease(ctx context.Context) error {
	var failures []error
	if err := s.finishRelease(ctx); err != nil {
		failures = append(failures, err)
	}
	if err := s.stopOwnership(ctx); err != nil {
		failures = append(failures, err)
	}
	if err := s.releaseLease(ctx); err != nil {
		failures = append(failures, err)
	}
	if err := s.dropState(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

// warmSession is the same resident session behind internal/residency's warm
// release seam, where the last three steps are named separately.
type warmSession struct {
	*resident
}

// warmSession is a residency.WarmSession.
var _ residency.WarmSession = warmSession{}

// FinishRelease writes the epoch-fenced tombstone and ends the heartbeat. It
// does NOT release the lease: this seam gives that its own step.
func (s warmSession) FinishRelease(ctx context.Context) error {
	if err := s.finishRelease(ctx); err != nil {
		return err
	}
	return s.stopOwnership(ctx)
}

// ReleaseLease drops the residency grant and the journal grant.
func (s warmSession) ReleaseLease(ctx context.Context) error { return s.releaseLease(ctx) }

// DropState drops in-memory state and the local workspace materialization.
func (s warmSession) DropState(ctx context.Context) error { return s.dropState(ctx) }
