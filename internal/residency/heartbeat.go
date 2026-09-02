package residency

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// What a heartbeat needs
// ---------------------------------------------------------------------------

// ErrEpochSuperseded is what a Locations implementation returns when a LATER
// epoch has already committed for the record it was asked to write.
//
// It is not a store failure and must not be retried. §10.1 makes SessionStore
// reject an epoch lower than the greatest committed for a record, so observing
// this is observing that somebody else now owns the session — the same fact
// Lease.Lost() reports, arriving by the other of the two paths. The loss signal
// is the fast guard; a rejected fenced write is the backstop, and this Host may
// see either first.
var ErrEpochSuperseded = errors.New("residency: a later lease epoch has already committed for this record")

// DrainState reports whether this Host has begun graceful drain.
//
// IT IS THE HOST'S ONE DRAIN FLAG AND NOT A SECOND ONE. internal/service owns
// it and its documentation names this task's consumers explicitly: two sources
// of "draining" is a Host that stops accepting in one place while still
// advertising Accepting from the other, and neither package's tests could see
// it. *service.CapacityPublisher satisfies this as written.
type DrainState interface {
	Draining() bool
}

// HeartbeatRegistry is the local index a heartbeat reads and, on loss or
// release, claims.
//
// BeginTeardown is the whole reason this is not just a reader. It is
// single-owner and its ownership is RECORDED rather than inferred from state,
// because StateDraining is re-enterable and a second caller finding the state
// already draining must be told it does not own the teardown. That is exactly
// the "one teardown owner" this task needs, already built and already tested,
// so nothing here invents a second notion of ownership.
type HeartbeatRegistry interface {
	Get(registry.Key) (registry.Entry, bool)
	MarkReleasing(registry.Key, uint64) (registry.Entry, bool)
	BeginTeardown(registry.Key, uint64) (registry.Entry, bool)
	RemoveByGeneration(registry.Key, uint64) bool
}

// The concrete registry satisfies the seam.
var _ HeartbeatRegistry = (*registry.Registry)(nil)

// LossReason names how ownership of a session was found to be gone.
type LossReason string

const (
	// LossReasonLeaseLost reports Lease.Lost() closing: renewal failure,
	// expiry, or the leaser observing a later epoch.
	LossReasonLeaseLost LossReason = "lease_lost"

	// LossReasonEpochSuperseded reports a fenced write refused because a later
	// epoch has committed. It is the same fact by the other path.
	LossReasonEpochSuperseded LossReason = "epoch_superseded"
)

// LostResidency describes a residency this Host no longer owns.
type LostResidency struct {
	Key             registry.Key
	AgentID         sessionwire.AgentID
	CompatibilityID department.CompatibilityID
	LeaseEpoch      uint64
	Generation      uint64
	Runtime         department.Runtime
	Reason          LossReason
}

// TeardownObserver is handed a residency whose ownership is gone, AT MOST ONCE
// per residency, by whichever caller won registry.BeginTeardown.
//
// What it then does is O6.1's: this package guarantees the handoff happens once
// and does not invent the release protocol behind it. In particular the runtime
// is handed over UNRELEASED — releasing it here would be a second release path
// with no checkpoint, no lease release and no tombstone.
type TeardownObserver interface {
	ResidencyLost(context.Context, LostResidency)
}

// ---------------------------------------------------------------------------
// Options and construction
// ---------------------------------------------------------------------------

// HeartbeatOptions configures the ownership factory.
type HeartbeatOptions struct {
	// Host supplies the identity, endpoint, placement, clock, heartbeat
	// interval and registry expiry every observation is derived from. Nothing
	// here restates a value host.New already checked.
	Host *host.Host

	// HostGeneration is this Host process's incarnation identity, which Core
	// requires to be non-zero on every HostLink record.
	HostGeneration uint64

	Registry  HeartbeatRegistry
	Locations Locations

	// Drain is the Host's ONE drain flag. See DrainState.
	Drain DrainState

	// Teardown receives a residency whose ownership is gone, once.
	Teardown TeardownObserver
}

// HeartbeatOwnership begins the durable-location half of a residency's
// ownership. It is the Ownership the residency Manager consumes.
type HeartbeatOwnership struct {
	options HeartbeatOptions
}

// HeartbeatOwnership is an Ownership. The assertion is here rather than in a
// test so a drift is a compile failure.
var _ Ownership = (*HeartbeatOwnership)(nil)

// NewHeartbeatOwnership validates options and returns the factory.
func NewHeartbeatOwnership(options HeartbeatOptions) (*HeartbeatOwnership, error) {
	if options.Host == nil {
		return nil, &InvalidManagerOptionsError{Field: "Host", Reason: "must be set; there is nothing to advertise without one"}
	}
	if options.HostGeneration == 0 {
		return nil, &InvalidManagerOptionsError{Field: "HostGeneration", Reason: "must be non-zero; Core refuses every HostLink record carrying a zero generation"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Registry", options.Registry != nil},
		{"Locations", options.Locations != nil},
		{"Drain", options.Drain != nil},
		{"Teardown", options.Teardown != nil},
	} {
		if !required.present {
			return nil, &InvalidManagerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	return &HeartbeatOwnership{options: options}, nil
}

// BeginOwnership starts one residency's heartbeat under the SESSION context.
func (o *HeartbeatOwnership) BeginOwnership(ctx context.Context, request OwnershipRequest) (OwnershipHandle, error) {
	if request.Lease == nil {
		return nil, &InvalidManagerOptionsError{
			Field:  "Lease",
			Reason: "must be set; a heartbeat that cannot observe Lease.Lost() publishes a route under a lease it may no longer hold",
		}
	}
	beat := &Heartbeat{
		options:    o.options,
		key:        request.Key,
		agent:      request.AgentID,
		compat:     request.CompatibilityID,
		epoch:      request.LeaseEpoch,
		generation: request.Generation,
		runtime:    request.Runtime,
		lease:      request.Lease,
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	go beat.run(ctx)
	return beat, nil
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

// Heartbeat maintains one residency's epoch-fenced Host location.
type Heartbeat struct {
	options    HeartbeatOptions
	key        registry.Key
	agent      sessionwire.AgentID
	compat     department.CompatibilityID
	epoch      uint64
	generation uint64
	runtime    department.Runtime
	lease      Lease

	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}

	releaseMu  sync.Mutex
	released   bool
	releaseErr error

	mu       sync.Mutex
	beats    int
	failures int
}

// THE LOOP CARRIES NO "lost" FLAG, and it did until an audit deleted every rung
// in turn and found nothing failed. A field written on surrender and read by
// nobody is a claim nothing checks — the same shape as the session-context rung
// whose deletion passed O3.1's entire suite. What ownership was lost is
// reported where it can be acted on: once, to the teardown observer.

// Heartbeat is an OwnershipHandle.
var _ OwnershipHandle = (*Heartbeat)(nil)

// Stop ends the heartbeat loop and waits for it to leave. It WRITES NOTHING:
// the caller that stops a heartbeat is either rolling back an attach, which
// writes a tombstone on its own path, or releasing residency, which is Release.
func (h *Heartbeat) Stop(context.Context) error {
	h.stopOnce.Do(func() { close(h.stopped) })
	<-h.done
	return nil
}

// Release performs the durable-location half of §9.3's graceful release: mark
// the entry releasing and stop admission, publish that state under the held
// epoch, write the expired epoch-fenced tombstone, and drop the local entry.
//
// IT STOPS THE LOOP FIRST AND WAITS FOR IT. A beat racing the release would
// write `resident` after `releasing`, and the last writer wins on a record
// whose whole purpose is telling a router what is true now. Stopping first is
// not a nicety, and it costs nothing: the loop selects on the stop signal, so
// nothing waits for a timer.
//
// It CONTINUES past a failed write, for the reason the attach rollback does:
// abandoning the rest turns one stale record into two. What could not be
// written is named on the returned *ReleaseError, because a release that
// reports success while leaving a live route with no owner is the worst outcome
// available and must not be indistinguishable from a clean one.
func (h *Heartbeat) Release(ctx context.Context) error {
	_ = h.Stop(ctx)

	// RELEASE HAPPENS ONCE, and the lock is what makes that true rather than
	// the generation check below. MEASURED: two concurrent Releases both passed
	// MarkReleasing — which sets state and, unlike BeginTeardown, records no
	// ownership — before either removed the entry, so both published and both
	// tombstoned. The writes are idempotent at the store, so nothing broke, but
	// a release that runs twice is a release whose ordering guarantees hold
	// only by luck.
	//
	// A repeat returns the FIRST verdict rather than re-attempting. The local
	// half cannot be redone once the entry is gone, and the durable half has
	// already been reported; re-driving the writes belongs to §15's idempotent
	// cleanup and its bounded due reconciler, not to a caller retrying a method
	// that would find nothing to release.
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	if h.released {
		return h.releaseErr
	}
	h.released = true
	h.releaseErr = h.release(ctx)
	return h.releaseErr
}

// release is the body, run at most once.
func (h *Heartbeat) release(ctx context.Context) error {
	entry, current := h.options.Registry.MarkReleasing(h.key, h.generation)
	if !current {
		// The residency is gone or has been replaced under this generation.
		// There is nothing of this heartbeat's to release, and writing under a
		// generation that is gone is what the check exists to prevent.
		return nil
	}

	var unwritten []string
	observation := h.observation(entry, sessionwire.SessionResidencyReleasing, false)
	if err := h.options.Locations.PublishResidency(ctx, observation); err != nil {
		unwritten = append(unwritten, "releasing observation: "+err.Error())
	}
	// §10.1: the route is removed by an EXPIRED epoch-fenced tombstone, never
	// by erasing the fencing high-water mark, so a later epoch is still
	// rejected after the logical record is gone.
	if err := h.options.Locations.TombstoneResidency(ctx, h.key.TenantID, h.key.SessionID, h.epoch); err != nil {
		unwritten = append(unwritten, "residency tombstone: "+err.Error())
	}
	h.options.Registry.RemoveByGeneration(h.key, h.generation)

	if len(unwritten) > 0 {
		return &ReleaseError{Key: h.key, Unwritten: unwritten}
	}
	return nil
}

// Beats reports how many observations this heartbeat has published.
func (h *Heartbeat) Beats() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beats
}

// ConsecutiveFailures reports how many publishes in a row the store has
// refused for a reason that is not a superseded epoch.
func (h *Heartbeat) ConsecutiveFailures() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failures
}

// run is the loop.
//
// THE TIMER IS NEVER STOPPED, and that is a cost rather than an oversight.
// host.Clock hands back a *time.Timer, and the only test double that can hand
// back a timer a test fires is &time.Timer{C: ch}, which PANICS on Stop. So an
// unfired timer outlives the loop by at most one interval. The remedy when it
// matters is host.Clock gaining a channel-returning After, not a Stop this
// package cannot exercise.
//
// LEASE LOSS DOMINATES A READY TIMER, and that is the one ordering rule in
// here. Go's select chooses uniformly among ready cases, so a lease that closed
// at the same moment a beat came due would publish an accepting route under a
// lease this Host no longer holds — a coin flip, half the time, on the one
// record ownership is advertised through. The re-check before beating makes
// that decidable rather than random.
func (h *Heartbeat) run(ctx context.Context) {
	defer close(h.done)
	for {
		timer := h.options.Host.Clock().NewTimer(h.options.Host.RegistryHeartbeat())
		select {
		case <-ctx.Done():
			return
		case <-h.stopped:
			return
		case <-h.lease.Lost():
			h.surrender(ctx, LossReasonLeaseLost)
			return
		case <-timer.C:
			// Precedence, not a race: every reason to stop is consulted before
			// a due beat is allowed to write anything.
			select {
			case <-ctx.Done():
				return
			case <-h.stopped:
				return
			case <-h.lease.Lost():
				h.surrender(ctx, LossReasonLeaseLost)
				return
			default:
			}
			if !h.beat(ctx) {
				return
			}
		}
	}
}

// beat publishes one observation and reports whether the loop should continue.
func (h *Heartbeat) beat(ctx context.Context) bool {
	entry, current := h.options.Registry.Get(h.key)
	if !current || entry.Generation != h.generation {
		// The residency was removed or replaced under this heartbeat. It is not
		// this heartbeat's to refresh and not its to tear down: whoever
		// replaced it owns it now.
		return false
	}
	// RE-CHECKED HERE, immediately before the write, and this is the check that
	// matters rather than the one in the loop. The loop's re-check stops a beat
	// that was due at the same moment the lease closed; this one closes the
	// window between reading the registry and writing the record, which is
	// where a lease can be lost while a beat is already in flight. A publish
	// under a lost grant is the one thing a fenced record must never receive.
	select {
	case <-h.lease.Lost():
		h.surrender(ctx, LossReasonLeaseLost)
		return false
	default:
	}
	observation := h.observation(entry, residencyOf(entry.State), entry.Accepting && !h.options.Drain.Draining())
	if err := h.options.Locations.PublishResidency(ctx, observation); err != nil {
		if errors.Is(err, ErrEpochSuperseded) {
			// NOT AMBIGUITY AND NOT RETRIED. A refused fence is the backstop
			// §10.1 describes: somebody else owns the session now, which is the
			// same fact Lost() reports by the other path.
			h.surrender(ctx, LossReasonEpochSuperseded)
			return false
		}
		// AMBIGUITY IS NOT LOSS. The store said nothing about who owns the
		// session; the LEASE decides that and is still held. Retrying on the
		// next beat is the whole response — the entry expires on its own if the
		// store never answers, and readers treat an expired entry as absent.
		h.mu.Lock()
		h.failures++
		h.mu.Unlock()
		return true
	}
	h.mu.Lock()
	h.beats++
	h.failures = 0
	h.mu.Unlock()
	return true
}

// surrender stops admission and hands the residency to exactly one teardown
// owner.
//
// IT WRITES NOTHING DURABLE. Both paths here mean this Host's epoch may already
// have been superseded, and a writer that has lost its grant must not touch a
// fenced record — not even to tombstone it, because the tombstone is itself an
// epoch-fenced write and a successor's higher epoch is the thing that must
// stand. §18.2 is explicit that registry EXPIRY removes the stale route. The
// tombstone belongs to the graceful path, where the lease is still held.
//
// The single owner is registry.BeginTeardown's, not a second notion: it records
// ownership rather than inferring it from a re-enterable state, and it stops
// admission in the same atomic step, which is what "stops admission
// immediately" needs.
func (h *Heartbeat) surrender(ctx context.Context, reason LossReason) {
	entry, won := h.options.Registry.BeginTeardown(h.key, h.generation)
	if !won {
		// Either the residency has been replaced under this heartbeat, or
		// somebody already owns the teardown. In both cases admission is
		// already closed and this is not the owner.
		return
	}
	// The runtime is handed over UNRELEASED: releasing it here would be a
	// second release path with no checkpoint, no lease release and no
	// tombstone. O6.1 owns what happens next.
	h.options.Teardown.ResidencyLost(ctx, LostResidency{
		Key:             h.key,
		AgentID:         h.agent,
		CompatibilityID: h.compat,
		LeaseEpoch:      h.epoch,
		Generation:      entry.Generation,
		Runtime:         h.runtime,
		Reason:          reason,
	})
}

// observation derives one epoch-fenced §15 record.
//
// The residency and accepting state are ARGUMENTS rather than read here,
// because the release path publishes a state the registry has only just been
// moved to and the beat path publishes the state it found. Both carry the same
// epoch and generation, which is the half that is never a choice.
func (h *Heartbeat) observation(entry registry.Entry, residency sessionwire.SessionResidency, accepting bool) sessionwire.HostLinkRegistryObservation {
	now := h.options.Host.Clock().Now()
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               h.key.TenantID,
		SessionID:              h.key.SessionID,
		HostID:                 h.options.Host.ID(),
		HostGeneration:         h.options.HostGeneration,
		AgentID:                entry.AgentID,
		RuntimeCompatibilityID: string(entry.CompatibilityID),
		Placement:              h.options.Host.Placement(),
		InternalEndpoint:       h.options.Host.InternalEndpoint(),
		Residency:              residency,
		Accepting:              accepting,
		LeaseEpoch:             h.epoch,
		ObservedAt:             now,
		ExpiresAt:              now.Add(h.options.Host.RegistryExpiry()),
	}
}

// residencyOf maps a local residency state onto the wire member a router reads.
//
// A draining state has NO wire member of its own — Core accepts only attaching,
// resident and releasing on the observation — and a session being torn down is
// releasing as far as a router cares. The default arm is the half that matters:
// listing the states that mean "still serving" and defaulting everything else
// to releasing means a state added later fails safe rather than advertising a
// route to a session that is going away.
func residencyOf(state registry.ResidencyState) sessionwire.SessionResidency {
	switch state {
	case registry.StateResident:
		return sessionwire.SessionResidencyResident
	default:
		return sessionwire.SessionResidencyReleasing
	}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// ReleaseError reports a release that could not write everything it owed.
type ReleaseError struct {
	Key registry.Key

	// Unwritten names each durable write that failed, in the order attempted,
	// for the reason AttachError.Unreleased exists: a release that reports
	// failure and a release that leaves a live route with no owner must not be
	// indistinguishable.
	Unwritten []string
}

func (e *ReleaseError) Error() string {
	return "residency: releasing the location of session " + strconv.Quote(string(e.Key.SessionID)) +
		" could not write " + strings.Join(e.Unwritten, "; ")
}
