// Package registry indexes the sessions a Host currently holds resident.
//
// IT IS AN OPTIMIZATION AND NOTHING MORE. This index exists so a Host need not
// read durable state to answer "do I hold this session", and a wrong or stale
// answer must waste a request at worst. Concretely: a stale entry may cost a
// caller a round trip, and may never let a stale Host advance the journal, a
// mutable projection, a continuation or checkpoint pointer, or a command claim.
// Nothing this package decides is durable, which is what makes a lost,
// duplicated or reordered registry update survivable.
//
// THE RULE THIS PACKAGE ENFORCES, and it is only half the sentence: the
// registry may READ and STORE a lease epoch so a caller can compare it, and may
// never DECIDE anything from one. TestRegistryNeverGatesOnTheLeaseEpoch holds
// that structurally, by whitelisting the positions the field may appear in
// rather than by guessing at the shapes a decision can take.
//
// THE OTHER HALF IS NOT ENFORCED HERE AND CANNOT BE. "Every mutating caller
// separately validates the durable lease epoch" is a rule about CALLERS; this
// package has none yet, and its API gives a caller nothing to validate against.
// Whoever writes the first caller owns that half, and should not read the guard
// below as having covered it.
package registry

import (
	"sort"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
)

// Key identifies one resident session.
//
// It is (TenantID, SessionID) and NEVER a bare SessionID. SessionIDs are
// client-supplied opaque strings — Core mints none of them — so two tenants
// naming a session identically is an ordinary event, not an attack, and a
// bare-SessionID index would hand one tenant the other's runtime handle. The
// composite key is the reason that cannot happen, which is why the tests
// construct the collision rather than asserting the struct's shape.
type Key struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ResidencyState is what the Host is currently doing with a resident session.
type ResidencyState string

const (
	// StateResident reports a session held and able to accept work.
	StateResident ResidencyState = "resident"

	// StateReleasing reports residency release under way. The session remains
	// resumable; this is not termination.
	StateReleasing ResidencyState = "releasing"

	// StateDraining reports teardown under way, owned by exactly one caller.
	StateDraining ResidencyState = "draining"
)

// Clock is the time source, so last-activity and staleness are testable
// without sleeping.
type Clock interface {
	Now() time.Time
}

// Entry is a COPY of one registry row. Mutating it changes nothing.
type Entry struct {
	Key     Key
	AgentID sessionwire.AgentID

	// Target is the LaunchTarget that produced this runtime, kept so
	// reconciliation can tell a runtime launched by the current Department from
	// one launched by a Department that has since changed.
	Target department.LaunchTarget

	// CompatibilityID is the runtime build actually launched, which is not
	// necessarily Target.CompatibilityID() any more: a target upgraded under a
	// resident session still holds a runtime built by the old one. Both are
	// stored because the difference is the thing reconciliation acts on.
	CompatibilityID department.CompatibilityID

	// Runtime is the live handle.
	Runtime department.Runtime

	// LeaseEpoch is the durable lease epoch this residency was established
	// under. It is CARRIED, never consulted: see the package comment.
	LeaseEpoch uint64

	// Generation is this registry's own monotonic identity for the residency.
	// Removal and teardown are keyed by it so a late caller cannot act on a
	// residency that has since been replaced under the same Key.
	Generation uint64

	State ResidencyState

	// Accepting reports whether THIS SESSION may take new work. It is NOT a
	// function of State: StopAdmitting closes it while the residency is still
	// resident, which is the state a warm release passes through between
	// closing admission and writing its durable `releasing` observation.
	//
	// It was a strict function of State until O6.1 — Insert set
	// {resident, true} and both MarkReleasing and BeginTeardown set false — and
	// the consequence was recorded across this module: a reader consulting
	// State and Accepting together would have had one decide every reachable
	// case and the other decide none. That is no longer true, and the three
	// facts a caller can now tell apart are a resident session that has stopped
	// admitting, a releasing one, and a Host-wide drain, which live in three
	// different places and have three different repairs.
	Accepting bool

	LastActivity time.Time

	// TeardownOwned reports whether some caller has already claimed teardown.
	TeardownOwned bool
}

// Admission is what a caller supplies to claim residency for a session.
type Admission struct {
	AgentID         sessionwire.AgentID
	Target          department.LaunchTarget
	CompatibilityID department.CompatibilityID
	Runtime         department.Runtime
	LeaseEpoch      uint64
}

// Registry is the tenant-aware index of resident sessions.
type Registry struct {
	mu             sync.Mutex
	entries        map[Key]*Entry
	clock          Clock
	nextGeneration uint64
}

// New returns an empty Registry.
func New(clock Clock) *Registry {
	return &Registry{entries: map[Key]*Entry{}, clock: clock}
}

// Insert claims residency for a key. It returns the entry now held and whether
// THIS call was the one that established it.
//
// A loser is told the residency that ACTUALLY EXISTS rather than an error, so
// it can act on it without a second lookup — and because the alternative
// invites a re-read that races with the removal it is trying to avoid.
func (r *Registry) Insert(key Key, admission Admission) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, held := r.entries[key]; held {
		return *existing, false
	}
	r.nextGeneration++
	entry := &Entry{
		Key:             key,
		AgentID:         admission.AgentID,
		Target:          admission.Target,
		CompatibilityID: admission.CompatibilityID,
		Runtime:         admission.Runtime,
		LeaseEpoch:      admission.LeaseEpoch,
		Generation:      r.nextGeneration,
		State:           StateResident,
		Accepting:       true,
		LastActivity:    r.clock.Now(),
	}
	r.entries[key] = entry
	return *entry, true
}

// Get returns a copy of the entry for a key.
func (r *Registry) Get(key Key) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, held := r.entries[key]
	if !held {
		return Entry{}, false
	}
	return *entry, true
}

// current returns the entry for key if its generation still matches. It is the
// one place staleness is decided, so a late caller holding a generation that
// has been replaced cannot act through ANY mutating method rather than through
// all but the one somebody forgot.
func (r *Registry) current(key Key, generation uint64) (*Entry, bool) {
	entry, held := r.entries[key]
	if !held || entry.Generation != generation {
		return nil, false
	}
	return entry, true
}

// StopAdmitting closes a residency to new work WITHOUT moving its state.
//
// IT IS NOT A WEAKER MarkReleasing AND MUST NOT BE FOLDED INTO ONE. Warm
// release stops admission first and marks the residency releasing second,
// because the reverse order publishes a `releasing` observation while this Host
// is still accepting commands into the runtime that observation says is going
// away. The intermediate state is {resident, accepting:false}: the route is
// still live for the bindings that hold it, and no new work enters.
//
// It is IDEMPOTENT and takes the same generation rule as every other mutating
// method, so a late warm timer cannot close admission on the residency that
// REPLACED the one it was armed for.
func (r *Registry) StopAdmitting(key Key, generation uint64) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.current(key, generation)
	if !ok {
		return Entry{}, false
	}
	entry.Accepting = false
	entry.LastActivity = r.clock.Now()
	return *entry, true
}

// MarkReleasing moves a residency to StateReleasing and stops it accepting.
func (r *Registry) MarkReleasing(key Key, generation uint64) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.current(key, generation)
	if !ok {
		return Entry{}, false
	}
	entry.State = StateReleasing
	entry.Accepting = false
	entry.LastActivity = r.clock.Now()
	return *entry, true
}

// ResumeResident returns a residency a release had begun on to StateResident
// and accepting, under the same generation.
//
// IT IS THE ONE WAY BACK, AND IT IS NARROW ON PURPOSE. A release that has
// published `releasing` cannot in general be taken back — a Factory may already
// have stopped routing to it — so nothing here offered this until a warm
// release found a runtime that REFUSED to release. Such a session never stopped
// being live under this Host's grant: no tombstone was written and no successor
// could attach. Leaving it `releasing` made it unanswerable through Factory
// (no reusable owner, bind and wake refused) until the Host drained. So a warm
// release may revert, and only that: a residency claimed for teardown or
// draining is refused, because its owner is the drain and not the warm path.
func (r *Registry) ResumeResident(key Key, generation uint64) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.current(key, generation)
	if !ok || entry.TeardownOwned || (entry.State != StateReleasing && entry.State != StateResident) {
		return Entry{}, false
	}
	entry.State = StateResident
	entry.Accepting = true
	entry.LastActivity = r.clock.Now()
	return *entry, true
}

// BeginTeardown claims teardown for a residency. Exactly one caller wins.
//
// The claim is recorded on the entry rather than inferred from the state,
// because StateDraining is reachable and re-enterable while ownership is a
// once-only fact: a second caller finding the state already draining must be
// told it does NOT own the teardown, not that teardown is under way.
func (r *Registry) BeginTeardown(key Key, generation uint64) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.current(key, generation)
	if !ok {
		return Entry{}, false
	}
	if entry.TeardownOwned {
		return *entry, false
	}
	entry.TeardownOwned = true
	entry.State = StateDraining
	entry.Accepting = false
	entry.LastActivity = r.clock.Now()
	return *entry, true
}

// Touch records activity against a residency.
func (r *Registry) Touch(key Key, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.current(key, generation)
	if !ok {
		return false
	}
	entry.LastActivity = r.clock.Now()
	return true
}

// RemoveByGeneration removes a residency only if the generation still matches.
func (r *Registry) RemoveByGeneration(key Key, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.current(key, generation); !ok {
		return false
	}
	delete(r.entries, key)
	return true
}

// RemoveStale removes every NON-RESIDENT entry last active before the cutoff,
// and returns what it removed.
//
// Resident sessions are never reaped, however quiet. A session that is holding
// work and simply has not been touched is not the registry's to end — deciding
// that would be an index adjudicating residency, which is the whole thing this
// package promises not to do. Only a session already on its way out, and idle
// since the cutoff, is swept.
func (r *Registry) RemoveStale(before time.Time) []Key {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed []Key
	for key, entry := range r.entries {
		if entry.State == StateResident || !entry.LastActivity.Before(before) {
			continue
		}
		delete(r.entries, key)
		removed = append(removed, key)
	}
	sortKeys(removed)
	return removed
}

// Snapshot returns a copy of every entry, ordered so callers see a stable
// sequence rather than map iteration order.
func (r *Registry) Snapshot() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]Entry, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, *entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return lessKey(entries[i].Key, entries[j].Key)
	})
	return entries
}

// Len reports how many residencies are held.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// lessKey orders keys by tenant then session, which is the order a human reads
// a Host's residencies in.
func lessKey(a, b Key) bool {
	if a.TenantID != b.TenantID {
		return a.TenantID < b.TenantID
	}
	return a.SessionID < b.SessionID
}

func sortKeys(keys []Key) {
	sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i], keys[j]) })
}
