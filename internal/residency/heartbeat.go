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
		settled:    make(chan struct{}),
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

	// settled closes once the loop has left AND any teardown handoff has
	// returned. done closes first, so a handle is never held hostage by an
	// observer; settled is what a caller waits on to know the handoff is over.
	settled chan struct{}

	releaseMu sync.Mutex
	begun     bool
	beginErr  error
	finished  bool
	finishErr error

	mu       sync.Mutex
	beats    int
	failures int
}

// THE LOOP CARRIES NO "lost" FLAG, and it did until an audit deleted every rung
// in turn and found nothing failed. A field written on surrender and read by
// nobody is a claim nothing checks — the same shape as the session-context rung
// whose deletion passed O3.1's entire suite. What ownership was lost is
// reported where it can be acted on: once, to the teardown observer.

// lostSignal is the lease-loss channel, or a nil channel when there is no
// lease at all.
//
// A NIL CHANNEL BLOCKS FOREVER IN A SELECT, which is exactly "this grant has
// not been lost" and is why this is not a defensive shrug. BeginOwnership
// refuses a request without a lease, so the nil case is unreachable in
// production — and that is the point: without this, the mutation that removes
// that refusal kills the guard by PANICKING on a nil interface before the test
// can compare anything, and a kill by panic is not an assertion kill. With it,
// the same mutation dies on the comparison the test actually makes.
func (h *Heartbeat) lostSignal() <-chan struct{} {
	if h.lease == nil {
		return nil
	}
	return h.lease.Lost()
}

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

// BeginRelease is §9.3 step 1: mark the entry releasing, stop admission, and
// write that state under the held epoch.
//
// IT IS HALF OF A RELEASE ON PURPOSE, and the split is what O6.1 needs rather
// than a refinement of it. §9.3 and 04-host.md put the mark-releasing and the
// durable `releasing` observation BEFORE checkpoints and ReleaseResidency, and
// the tombstone and removal AFTER them. A single fused call leaves a caller two
// bad options: checkpoint with admission still open, or drive MarkReleasing and
// a durable write itself and become a SECOND durable-location writer — the exact
// defect this package refuses for the drain flag. There is one writer, and it
// offers the two halves the protocol actually has.
//
// THE LOOP KEEPS RUNNING between the halves, deliberately. A checkpoint can
// take longer than the registry expiry, and a residency that stopped beating at
// step 1 would have its route expire mid-release and read as `cold` while this
// Host still holds the lease and the runtime. The beats in between carry
// `releasing` and Accepting false, because that is what the entry now says.
func (h *Heartbeat) BeginRelease(ctx context.Context) error {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	if h.begun {
		return h.beginErr
	}
	h.begun = true
	h.beginErr = h.beginRelease(ctx)
	return h.beginErr
}

// FinishRelease is §9.3 steps 4 and 5' location half: stop the loop, write the
// expired epoch-fenced tombstone, and drop the local entry.
//
// IT STOPS THE LOOP FIRST AND WAITS FOR IT. A beat racing the tombstone would
// re-publish a route after it was removed, and the last writer wins on a record
// whose whole purpose is telling a router what is true now. Stopping first
// costs nothing: the loop selects on the stop signal, so nothing waits for a
// timer.
//
// It CONTINUES past a failed write, for the reason the attach rollback does:
// abandoning the rest turns one stale record into two. What could not be
// written is named on the returned *ReleaseError, because a release that
// reports success while leaving a live route with no owner is the worst outcome
// available and must not be indistinguishable from a clean one.
func (h *Heartbeat) FinishRelease(ctx context.Context) error {
	_ = h.Stop(ctx)

	// EACH HALF HAPPENS ONCE, and the lock is what makes that true rather than
	// the generation check. MEASURED on the fused version: two concurrent
	// releases both passed MarkReleasing — which sets state and, unlike
	// BeginTeardown, records no ownership — before either removed the entry, so
	// both published and both tombstoned. The writes are idempotent at the
	// store, so nothing broke, but a release that runs twice is a release whose
	// ordering guarantees hold only by luck.
	//
	// A repeat returns the FIRST verdict rather than re-attempting. The local
	// half cannot be redone once the entry is gone, and the durable half has
	// already been reported; re-driving the writes belongs to §15's idempotent
	// cleanup and its bounded due reconciler, not to a caller retrying a method
	// that would find nothing to release.
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	if h.finished {
		return h.finishErr
	}
	h.finished = true
	h.finishErr = h.finishRelease(ctx)
	return h.finishErr
}

// beginRelease is BeginRelease's body, run at most once.
func (h *Heartbeat) beginRelease(ctx context.Context) error {
	entry, current := h.options.Registry.MarkReleasing(h.key, h.generation)
	if !current {
		// The residency is gone or has been replaced under this generation.
		// There is nothing of this heartbeat's to release, and writing under a
		// generation that is gone would remove a route its REPLACEMENT
		// installed.
		return nil
	}
	if err := h.leaseHeld(); err != nil {
		return &ReleaseError{Key: h.key, Unwritten: []string{"releasing observation: " + err.Error()}, Cause: err}
	}
	observation := h.observation(entry, sessionwire.SessionResidencyReleasing, false)
	if err := h.options.Locations.PublishResidency(ctx, observation); err != nil {
		return &ReleaseError{Key: h.key, Unwritten: []string{"releasing observation: " + err.Error()}, Cause: err}
	}
	return nil
}

// finishRelease is FinishRelease's body, run at most once.
func (h *Heartbeat) finishRelease(ctx context.Context) error {
	if entry, current := h.options.Registry.Get(h.key); !current || entry.Generation != h.generation {
		return nil
	}

	var unwritten []string
	if err := h.leaseHeld(); err != nil {
		unwritten = append(unwritten, "residency tombstone: "+err.Error())
	} else if err := h.options.Locations.TombstoneResidency(ctx, h.key.TenantID, h.key.SessionID, h.epoch); err != nil {
		// §10.1: the route is removed by an EXPIRED epoch-fenced tombstone,
		// never by erasing the fencing high-water mark, so a later epoch is
		// still rejected after the logical record is gone.
		unwritten = append(unwritten, "residency tombstone: "+err.Error())
	}

	// The LOCAL entry goes whether or not the durable write happened. Leaving it
	// would make this Host believe it still holds a session it has released.
	h.options.Registry.RemoveByGeneration(h.key, h.generation)

	if len(unwritten) > 0 {
		return &ReleaseError{Key: h.key, Unwritten: unwritten, Cause: h.leaseHeld()}
	}
	return nil
}

// leaseHeld reports ErrLeaseNotHeld when this heartbeat's grant is gone.
//
// IT IS CHECKED ON THE OBJECT, not only inside surrender, and that distinction
// was a live escape. The rule "a holder that has lost its grant must not touch a
// fenced record — not even to tombstone it" was enforced on the loop's path and
// nowhere else, so a teardown owner reaching this handle after a loss — which is
// the intended O6.1 path, since Manager.sessions carries the handle even though
// LostResidency does not — published a `releasing` observation and a tombstone
// under a lease this Host no longer held, and got nil back. A real store fences
// both, but the whole argument for writing nothing after a loss is that the
// holder must not ATTEMPT it.
func (h *Heartbeat) leaseHeld() error {
	select {
	case <-h.lostSignal():
		return ErrLeaseNotHeld
	default:
		return nil
	}
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
	lost := h.loop(ctx)

	// THE HANDLE IS FREED BEFORE THE OBSERVER RUNS, and that ordering is a
	// bound rather than a tidiness. ResidencyLost is arbitrary caller code on
	// this goroutine; Stop waits on done and FinishRelease calls Stop, so an
	// observer that blocked would wedge every caller of this handle — O3.1's
	// attach rollback among them, which is the one path that must never be able
	// to hang. Closing done first means a wedged observer costs one goroutine
	// and nothing else.
	close(h.done)
	if lost != nil {
		h.options.Teardown.ResidencyLost(ctx, *lost)
	}
	close(h.settled)
}

// loop runs until ownership ends, and returns the residency to hand over if
// this heartbeat is the one that must hand it over.
func (h *Heartbeat) loop(ctx context.Context) *LostResidency {
	for {
		timer := h.options.Host.Clock().NewTimer(h.options.Host.RegistryHeartbeat())
		select {
		case <-ctx.Done():
			return nil
		case <-h.stopped:
			return nil
		case <-h.lostSignal():
			return h.surrender(LossReasonLeaseLost)
		case <-timer.C:
			// Precedence, not a race: every reason to stop is consulted before
			// a due beat is allowed to write anything.
			select {
			case <-ctx.Done():
				return nil
			case <-h.stopped:
				return nil
			case <-h.lostSignal():
				return h.surrender(LossReasonLeaseLost)
			default:
			}
			keep, lost := h.beat(ctx)
			if !keep {
				return lost
			}
		}
	}
}

// beat publishes one observation and reports whether the loop should continue.
func (h *Heartbeat) beat(ctx context.Context) (bool, *LostResidency) {
	entry, current := h.options.Registry.Get(h.key)
	if !current || entry.Generation != h.generation {
		// The residency was removed or replaced under this heartbeat. It is not
		// this heartbeat's to refresh and not its to tear down: whoever
		// replaced it owns it now.
		return false, nil
	}
	// RE-CHECKED HERE, immediately before the write, and this is the check that
	// matters rather than the one in the loop. The loop's re-check stops a beat
	// that was due at the same moment the lease closed; this one closes the
	// window between reading the registry and writing the record, which is
	// where a lease can be lost while a beat is already in flight. A publish
	// under a lost grant is the one thing a fenced record must never receive.
	select {
	case <-h.lostSignal():
		return false, h.surrender(LossReasonLeaseLost)
	default:
	}
	// FOR O4 AND O5, BECAUSE THE FLAG HAS NO OTHER READER YET: this
	// observation is currently the ONLY consumer of registry.Entry.Accepting in
	// this module, so the local half of "a lost lease stops admission
	// immediately" is a flag nothing enforces. Whoever admits a command or a
	// HostLink bind must read it, or step 4's guarantee becomes the
	// claim-nothing-checks shape this lane keeps finding.
	observation := h.observation(entry, residencyOf(entry.State), entry.Accepting && !h.options.Drain.Draining())
	if err := h.options.Locations.PublishResidency(ctx, observation); err != nil {
		if errors.Is(err, ErrEpochSuperseded) {
			// NOT AMBIGUITY AND NOT RETRIED. A refused fence is the backstop
			// §10.1 describes: somebody else owns the session now, which is the
			// same fact Lost() reports by the other path.
			return false, h.surrender(LossReasonEpochSuperseded)
		}
		// AMBIGUITY IS NOT LOSS. The store said nothing about who owns the
		// session; the LEASE decides that and is still held. Retrying on the
		// next beat is the whole response — the entry expires on its own if the
		// store never answers, and readers treat an expired entry as absent.
		h.mu.Lock()
		h.failures++
		h.mu.Unlock()
		return true, nil
	}
	h.mu.Lock()
	h.beats++
	h.failures = 0
	h.mu.Unlock()
	return true, nil
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
// admission in the same atomic step.
//
// "IMMEDIATELY" IS LOCAL AND ONLY EVENTUAL DURABLY, which is worth stating
// because the unqualified phrase overclaims. The loop exits here without
// writing, so the durable record goes on advertising Accepting true until it
// expires — up to one RegistryExpiry. That is the deliberate consequence of
// writing nothing under a lost grant, and §18.2 assigns the cleanup to registry
// expiry and to Factory's bounded due reconciler rather than to this Host.
func (h *Heartbeat) surrender(reason LossReason) *LostResidency {
	entry, won := h.options.Registry.BeginTeardown(h.key, h.generation)
	if !won {
		// Either the residency has been replaced under this heartbeat, or
		// somebody already owns the teardown. In both cases admission is
		// already closed and this is not the owner.
		return nil
	}
	// The runtime is handed over UNRELEASED: releasing it here would be a
	// second release path with no checkpoint, no lease release and no
	// tombstone. O6.1 owns what happens next.
	return &LostResidency{
		Key:             h.key,
		AgentID:         h.agent,
		CompatibilityID: h.compat,
		LeaseEpoch:      h.epoch,
		Generation:      entry.Generation,
		Runtime:         h.runtime,
		Reason:          reason,
	}
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

// ErrLeaseNotHeld is the cause a release carries when it wrote nothing because
// this Host no longer holds the session lease. It is not a store failure and
// there is nothing to retry: the route expires on its own, and §15's bounded
// due reconciler is what cleans up after a Host that stopped owning a session.
var ErrLeaseNotHeld = errors.New("residency: this Host no longer holds the session lease")

// ReleaseError reports a release that could not write everything it owed.
type ReleaseError struct {
	Key registry.Key

	// Cause carries the typed reason, so a caller can tell a lost grant from an
	// unreachable store with errors.Is instead of matching text.
	Cause error

	// Unwritten names each durable write that failed, in the order attempted,
	// for the reason AttachError.Unreleased exists: a release that reports
	// failure and a release that leaves a live route with no owner must not be
	// indistinguishable.
	Unwritten []string
}

// Unwrap returns the typed reason this release wrote nothing, if any.
func (e *ReleaseError) Unwrap() error { return e.Cause }

func (e *ReleaseError) Error() string {
	return "residency: releasing the location of session " + strconv.Quote(string(e.Key.SessionID)) +
		" could not write " + strings.Join(e.Unwritten, "; ")
}
