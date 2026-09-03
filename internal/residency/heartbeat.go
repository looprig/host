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

// ErrFenceConflict is what a JournalFencer returns when its sequence
// compare-and-swap is refused.
//
// §10.1 HAS TWO FENCING MECHANISMS AND THIS IS THE OTHER ONE. The journal is
// fenced by an in-stream ownership record rather than an epoch column: a
// successor's committed fence permanently makes the predecessor's tracked
// sequence stale, so "every later append from the predecessor fails with a
// typed conflict EVEN IF IT HAS NOT OBSERVED Lease.Lost()". That is the same
// meaning ErrEpochSuperseded carries on the mutable records, and this module
// had no sentinel for it at all — so the one fenced write that is not a
// location write could not be classified even in principle.
var ErrFenceConflict = errors.New("residency: the journal fence was refused by a later owner's committed sequence")

// epochFence is the ONE mechanism every fenced write goes through.
//
// IT IS A MECHANISM AND NOT A CONVENTION, and the difference is the whole
// reason this type exists. The rule was "remember to classify the error", and
// remembering was enforced by nothing: it was applied at three sites in one
// file while a fourth writer in another file had three more, none classified.
// Its neighbour — one writer, one lock — had a structural guard and stayed
// right. A rule you can forget is a rule that will be forgotten by whoever adds
// the seventh write.
//
// So a write goes through write(): it refuses before the call when ownership is
// already gone, and it records ownership as gone when the store or the ledger
// says a later owner has committed. Both fencing mechanisms of §10.1 end
// ownership, because both mean the same thing.
type epochFence struct {
	lease Lease

	mu    sync.Mutex
	ended bool
}

// newEpochFence returns the fence for one lease grant.
func newEpochFence(lease Lease) *epochFence { return &epochFence{lease: lease} }

// lost is the grant's loss channel, or a nil channel when there is no grant.
//
// A NIL CHANNEL BLOCKS FOREVER IN A SELECT, which is exactly "this grant has not
// been lost". Every construction path refuses a missing lease, so the nil case
// is unreachable — and that is the point: without it, the mutation removing
// that refusal kills its guard by PANICKING before the test can compare
// anything, and a kill by panic is not an assertion kill.
func (f *epochFence) lost() <-chan struct{} {
	if f.lease == nil {
		return nil
	}
	return f.lease.Lost()
}

// held reports ErrLeaseNotHeld when this grant no longer owns the session,
// by either of the two ways that can be known.
func (f *epochFence) held() error {
	f.mu.Lock()
	ended := f.ended
	f.mu.Unlock()
	if ended {
		return ErrLeaseNotHeld
	}
	select {
	case <-f.lost():
		return ErrLeaseNotHeld
	default:
		return nil
	}
}

// end records that ownership is gone for a reason other than a refused write —
// Lost() closing, observed on the loop.
func (f *epochFence) end() {
	f.mu.Lock()
	f.ended = true
	f.mu.Unlock()
}

// write performs one fenced write: refuse if ownership is already gone, run it,
// and classify what came back.
func (f *epochFence) write(run func() error) error {
	if err := f.held(); err != nil {
		return err
	}
	err := run()
	if errors.Is(err, ErrEpochSuperseded) || errors.Is(err, ErrFenceConflict) {
		f.end()
	}
	return err
}

// MEASURED, AND ONE ARM IS EQUIVALENT TODAY. Removing ErrFenceConflict from the
// classification above changes no observable behaviour: the journal fence is
// the only journal write this package makes, it happens at step 3, and nothing
// fenced follows it in an attach that failed there — the rollback ladder at
// that point holds an admission, a context and a lease, none of them fenced. It
// is kept because the arm becomes load-bearing the moment there is a second
// journal write, which O4's inbox consumption is, and because the alternative
// is a sentinel that exists and is never consulted. What DOES hold it today is
// the structural guard: the write must be routed through here whether or not
// the classification can yet be observed.

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

	// Admissions is the Host's ONE ledger, and its drain flag with it.
	//
	// IT IS THE WHOLE LEDGER RATHER THAN A NARROWER DrainState, which this
	// package used to declare. That interface was a strict subset, so a
	// composition could satisfy the two independently and hand the Manager one
	// object and the heartbeat another — moving "two sources of draining" from
	// a code possibility to a COMPOSITION possibility, which is the defect the
	// Workspaces note in manager.go describes. Naming the same type in both
	// places is what makes them the same object by construction.
	Admissions Admissions

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
		{"Admissions", options.Admissions != nil},
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
		fence:      newEpochFence(request.Lease),
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

	// fence is the one mechanism every fenced write goes through. It also
	// carries whether ownership has ended, which used to be a field here read
	// by a method beside it — three sites, one file, and a fourth writer
	// elsewhere with none.
	fence *epochFence

	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}

	// settled closes once the loop has left AND any teardown handoff has
	// returned. done closes first, so a handle is never held hostage by an
	// observer; settled is what a caller waits on to know the handoff is over.
	settled chan struct{}

	// writeMu serializes everything that reads the residency and then writes
	// its durable location: the beat, and beginRelease. See beat.
	writeMu sync.Mutex

	releaseMu sync.Mutex
	begun     bool
	beginErr  error
	finished  bool
	finishErr error

	mu sync.Mutex

	beats    int
	failures int

	// teardownOwned records that THIS heartbeat won registry.BeginTeardown,
	// which is what distinguishes "the claim is ours" from "a rival holds it".
	teardownOwned bool
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
	if _, err := h.releasable(); err != nil {
		return &ReleaseError{Key: h.key, Unwritten: []string{"releasing observation: " + err.Error()}, Cause: err}
	}

	// ONE WRITER. See writeMu: the beat and this share a record and an epoch,
	// so the store's fence cannot order them and the lock must.
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	entry, current := h.options.Registry.MarkReleasing(h.key, h.generation)
	if !current {
		return nil
	}
	observation := h.observation(entry, sessionwire.SessionResidencyReleasing, false)
	if err := h.fence.write(func() error { return h.options.Locations.PublishResidency(ctx, observation) }); err != nil {
		return &ReleaseError{Key: h.key, Unwritten: []string{"releasing observation: " + err.Error()}, Cause: err}
	}
	return nil
}

// finishRelease is FinishRelease's body, run at most once.
func (h *Heartbeat) finishRelease(ctx context.Context) error {
	_, err := h.releasable()
	switch {
	case errors.Is(err, ErrNothingToRelease):
		return &ReleaseError{Key: h.key, Unwritten: []string{"residency tombstone: " + err.Error()}, Cause: err}
	case errors.Is(err, ErrTeardownOwnedElsewhere):
		// The local entry is NOT removed. It is the claimed owner's.
		return &ReleaseError{Key: h.key, Unwritten: []string{"residency tombstone: " + err.Error()}, Cause: err}
	}

	// ONE WRITER, uniformly. This half has already stopped the loop, so there
	// is nothing to contend with — and taking the lock anyway is the point:
	// "every durable location write happens under writeMu" is one rule with one
	// mechanism, which a guard can check, rather than two mechanisms that a
	// reader has to know are equivalent.
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	var unwritten []string
	cause := err
	if err != nil {
		unwritten = append(unwritten, "residency tombstone: "+err.Error())
	} else if err := h.fence.write(func() error {
		return h.options.Locations.TombstoneResidency(ctx, h.key.TenantID, h.key.SessionID, h.epoch)
	}); err != nil {
		// Y2: the STORE'S error becomes the cause. It was taken from
		// releasable, which is nil on this path, so the one error that is not
		// ambiguity — a superseded epoch — survived as text only while
		// beginRelease ten lines up preserved it. Cause exists precisely so a
		// caller tells a lost grant from an unreachable store with errors.Is
		// rather than by matching a sentence.
		cause = err
		// §10.1: the route is removed by an EXPIRED epoch-fenced tombstone,
		// never by erasing the fencing high-water mark, so a later epoch is
		// still rejected after the logical record is gone.
		unwritten = append(unwritten, "residency tombstone: "+err.Error())
	}

	// The LOCAL entry goes whether or not the durable write happened. Leaving it
	// would make this Host believe it still holds a session it has released.
	h.options.Registry.RemoveByGeneration(h.key, h.generation)

	if len(unwritten) > 0 {
		return &ReleaseError{Key: h.key, Unwritten: unwritten, Cause: cause}
	}
	return nil
}

// leaseHeld reports ErrLeaseNotHeld when this heartbeat's grant is gone.
//
// IT DELEGATES, because the rule is the fence's. Both ways ownership can be
// known to be gone live there: Lost() closing, and a fenced write refused by a
// later owner. Lost() fires on renewal failure or expiry and does NOT prove a
// successor exists; a refused fenced write IS that proof, and it closes no
// channel — so a handle consulting only the channel went on writing under an
// epoch it had been told was superseded.
func (h *Heartbeat) leaseHeld() error { return h.fence.held() }

// teardownIsOurs reports whether a teardown claim on the entry is this
// heartbeat's own.
func (h *Heartbeat) teardownIsOurs() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.teardownOwned
}

// releasable reports whether this handle may still act on the residency, or the
// typed reason it may not.
//
// THE ORDER OF THE THREE CHECKS IS THE CONTRACT. A generation that is gone
// means there is nothing of this heartbeat's, and it is checked first because
// every later answer would be about somebody else's residency. A lost grant is
// checked before a rival's teardown claim because it is the more specific
// truth: when this heartbeat surrendered it claimed the teardown itself, so the
// claim it would otherwise report is its own.
func (h *Heartbeat) releasable() (registry.Entry, error) {
	entry, current := h.options.Registry.Get(h.key)
	if !current || entry.Generation != h.generation {
		// Nothing of this heartbeat's. Writing under a generation that is gone
		// would remove a route its REPLACEMENT installed.
		return registry.Entry{}, ErrNothingToRelease
	}
	if err := h.leaseHeld(); err != nil {
		return entry, err
	}
	if entry.TeardownOwned && !h.teardownIsOurs() {
		// registry.BeginTeardown records ownership rather than inferring it
		// from a re-enterable state, and this package built it that way so a
		// second owner would not have to be guessed at. Reading it is the other
		// half of that: a rival holding the claim owns the release, and
		// removing the local entry out from under it is the failure the claim
		// exists to prevent.
		return entry, ErrTeardownOwnedElsewhere
	}
	return entry, nil
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
		case <-h.fence.lost():
			return h.surrender(LossReasonLeaseLost)
		case <-timer.C:
			// Precedence, not a race: every reason to stop is consulted before
			// a due beat is allowed to write anything.
			select {
			case <-ctx.Done():
				return nil
			case <-h.stopped:
				return nil
			case <-h.fence.lost():
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
	// ONE WRITER, and this lock is what makes that claim true rather than
	// nearly true. The beat reads the registry and then writes the location;
	// beginRelease marks the entry releasing and then writes the same record
	// under the SAME epoch, so the store's fence cannot order the two. Without
	// this, a beat that had already read `resident` published it after
	// beginRelease had published `releasing`, and the last durable word on a
	// session whose admission was locally closed was "resident, accepting" for
	// up to one heartbeat interval. FinishRelease's doc reasons about exactly
	// this hazard against the tombstone; its twin here was missed.
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

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
	if err := h.leaseHeld(); err != nil {
		return false, h.surrender(LossReasonLeaseLost)
	}
	// FOR O4 AND O5, BECAUSE THE FLAG HAS NO OTHER READER YET: this
	// observation is currently the ONLY consumer of registry.Entry.Accepting in
	// this module, so the local half of "a lost lease stops admission
	// immediately" is a flag nothing enforces. Whoever admits a command or a
	// HostLink bind must read it, or step 4's guarantee becomes the
	// claim-nothing-checks shape this lane keeps finding.
	observation := h.observation(entry, residencyOf(entry.State), entry.Accepting && !h.options.Admissions.Draining())
	if err := h.fence.write(func() error { return h.options.Locations.PublishResidency(ctx, observation) }); err != nil {
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
	// RECORDED ON BOTH BRANCHES, because ownership is gone either way. Whether
	// this heartbeat won the teardown claim decides who performs the release;
	// it decides nothing about whether this Host still owns the session, and
	// recording it only on the winning branch would leave a loser writing
	// fenced records under an epoch somebody else has superseded.
	h.fence.end()
	h.mu.Lock()
	h.teardownOwned = won
	h.mu.Unlock()
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

// ErrTeardownOwnedElsewhere is the cause a release carries when another caller
// holds registry.BeginTeardown's single-owner claim on the residency. That
// owner performs the release; this handle writes nothing and removes nothing.
var ErrTeardownOwnedElsewhere = errors.New("residency: another caller owns the teardown of this residency")

// ErrNothingToRelease reports that the residency this handle was created for is
// gone or has been replaced under it.
//
// IT IS RETURNED RATHER THAN COLLAPSED TO NIL, and the distinction is §9.3's.
// Step 1 is a precondition for step 3: a caller that reads nil from
// BeginRelease and proceeds to checkpoint is checkpointing a residency whose
// admission this handle did not stop, because there was nothing here to stop.
// Success and no-op must not be indistinguishable — the argument this file
// makes for ReleaseError, applied to the one return that did not follow it.
var ErrNothingToRelease = errors.New("residency: this heartbeat's residency is gone or has been replaced")

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
