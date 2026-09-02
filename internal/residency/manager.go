// Package residency creates and restores sessions under the durable session
// lease, in one fixed order, and releases everything it took when any step of
// that order fails.
//
// THE ORDER IS THE CONTRACT, not an implementation detail, and every step is
// there because the one before it must already have happened:
//
//  1. validate AgentID/runtime compatibility and Host admission;
//  2. obtain the SessionStore lease and its new epoch;
//  3. commit the opening journal fence before hydration;
//  4. construct/restore the runtime and its workspace/checkpoint;
//  5. validate the required runtime capabilities;
//  6. atomically install the local registry winner;
//  7. write the epoch-fenced durable Host registry/residency projection;
//  8. begin inbox/event/heartbeat ownership;
//  9. only then report attached.
//
// Step 3 is the one most easily read as bookkeeping. It is not: spec §10.1
// fences the journal with an IN-STREAM ownership record rather than an epoch
// column, and a writer's first append is that fence, committed by sequence CAS
// against the tip it read immediately beforehand. Committing it BEFORE
// hydration is what makes a predecessor's every later append fail even if the
// predecessor has not yet observed Lease.Lost(). Hydrating first would leave a
// window in which two processes both believe they own the stream and neither
// has staked it.
//
// Nothing here is durable except the fence and the projection, and neither is
// this package's to adjudicate: the LEASE is authoritative, the local registry
// is an optimization, and the projection is a routing hint Factory may find
// stale. What this package guarantees is narrower and is the thing a caller can
// rely on: an Attach that returns an error took nothing that is still held.
package residency

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------
//
// Each is a NARROW LOCAL interface, for the reason department.Rig and
// host.SessionStore are: naming a looprig module in go.mod is the same decision
// as depending on it, and publishedLooprigVersions is where that decision is
// recorded. Released harness v0.30.2 has none of H4.1's capabilities, and
// sessionstore v0.1.0 implements neither the epoch-fenced projection nor the
// in-stream journal fence this package needs. These describe what Host
// requires; the concrete edges are later tasks.

// Lease is one granted, epoch-fenced session lease.
type Lease interface {
	// Epoch is the lease epoch this grant owns. It is never zero: Core rejects
	// a zero lease_epoch on the registry observation, so a store that grants
	// one has granted something Host cannot publish under.
	Epoch() uint64

	// Lost closes when the grant is no longer held, whether by renewal
	// failure, expiry, or observation of a later epoch. It is the FAST guard
	// only; the non-rebasing journal CAS is the hard backstop.
	Lost() <-chan struct{}

	// Release drops the grant. It is not termination: the session remains
	// durable and resumable.
	Release(context.Context) error
}

// SessionLeases grants exclusive epoch-fenced leases over a session.
type SessionLeases interface {
	// AcquireSessionLease grants the lease for one session, or reports
	// ErrLeaseHeld if another holder has it.
	AcquireSessionLease(context.Context, sessionwire.TenantID, sessionwire.SessionID) (Lease, error)
}

// JournalFencer commits the opening in-stream ownership record.
type JournalFencer interface {
	// CommitOpeningFence appends the fence stamped with the lease epoch,
	// committed by sequence CAS against the tip read immediately beforehand.
	CommitOpeningFence(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64) error
}

// SessionState is the durable state a hydration reads. It is deliberately not
// host.SessionStore's opaque []byte: this package needs the fields, and
// decoding them here would put a wire format in a state machine.
type SessionState struct {
	// Exists reports whether the session has durable state at all.
	Exists bool

	// Namespace is the tenant- and session-scoped object prefix every launched
	// runtime writes under. It is read from the store rather than derived here
	// because the layout is SessionStore's, not Host's.
	Namespace string

	// CompatibilityID is the runtime build that wrote the durable state. A
	// restore onto a different build is refused.
	CompatibilityID department.CompatibilityID

	// RigSessionID is Harness's identity for the session, which Host recorded
	// at create and cannot derive. A restore that does not carry it has
	// nothing to restore from.
	RigSessionID uuid.UUID

	// HasCheckpoint and CheckpointSequence report the active workspace
	// checkpoint. A target declaring RequiresCheckpoint may not be restored
	// without one.
	HasCheckpoint      bool
	CheckpointSequence uint64
}

// DurableStore reads the durable session state a hydration needs.
type DurableStore interface {
	// LoadSessionState returns the durable state for a session.
	LoadSessionState(context.Context, sessionwire.TenantID, sessionwire.SessionID) (SessionState, error)
}

// Workspaces materializes and un-materializes session workspaces.
//
// It is WIDER THAN host.WorkspaceProvider by exactly one method, and the extra
// one is why the interface is declared here rather than reused. That type has
// only EnsureWorkspace, which is the half a successful attach needs; a FAILED
// attach needs the other half, or every rolled-back attach leaves a
// materialized workspace behind and a Host that refuses a hundred attaches
// fills its disk. ReleaseWorkspace drops the LOCAL MATERIALIZATION ONLY: the
// durable session, its objects and its checkpoints are untouched, which is what
// makes calling it on a rollback safe.
type Workspaces interface {
	// EnsureWorkspace makes a workspace available and returns its root.
	EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error)

	// ReleaseWorkspace drops the local materialization. It never deletes
	// durable state.
	ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error
}

// FOR O6.1, AND IT IS THE DEFECT CLASS internal/service ALREADY NAMED. host.New
// validates a host.WorkspaceProvider that this manager CANNOT USE, because it
// has no ReleaseWorkspace. So a Host today can be constructed with one workspace
// provider and its residency manager wired to another, and nothing checks that
// they are the same thing — two sources of "the workspace", exactly as two
// sources of "consumed" would be two admission ledgers. The composition root
// must either widen host.WorkspaceProvider to carry the release half, or hand
// this manager the very value host.New validated. Do not resolve it by leaving
// both.

// Locations writes the epoch-fenced durable Host registry projection of §15.
type Locations interface {
	// PublishResidency writes the observation under its lease epoch. The
	// implementation rejects an epoch lower than the greatest already
	// committed for that record.
	PublishResidency(context.Context, sessionwire.HostLinkRegistryObservation) error

	// TombstoneResidency writes the EXPIRED epoch-fenced tombstone of §10.1.
	// It removes the route without erasing the fencing high-water mark, which
	// is why a rollback calls this rather than deleting the record.
	TombstoneResidency(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64) error
}

// OwnershipHandle is one session's running inbox/event/heartbeat ownership.
type OwnershipHandle interface {
	// Stop ends the ownership this handle started.
	Stop(context.Context) error
}

// OwnershipRequest describes the residency whose ownership is beginning.
type OwnershipRequest struct {
	Key             registry.Key
	AgentID         sessionwire.AgentID
	CompatibilityID department.CompatibilityID
	LeaseEpoch      uint64
	Generation      uint64
	Runtime         department.Runtime
}

// Ownership begins the durable inbox consumption, event fan-out and heartbeat
// a resident session owns. O3.2 and O4.1 implement it; this package only
// sequences it, and sequences it LAST, because every one of those three writes
// under the lease epoch and none of them may start before the epoch is fenced
// and the runtime exists.
type Ownership interface {
	// BeginOwnership starts ownership under the SESSION context, not the
	// request's, and returns the handle that ends it.
	BeginOwnership(context.Context, OwnershipRequest) (OwnershipHandle, error)
}

// Admissions is the Host's admission ledger.
//
// IT IS DELIBERATELY NOT A SECOND LEDGER. internal/service already owns the
// only one, and its documentation names this task as a required consumer:
// two sources of "consumed" is an over-admission bug that neither package's
// tests could see, because each would be internally consistent while the Host
// admitted past its capacity. *service.CapacityPublisher satisfies this
// interface as written, and a test in this package asserts that rather than
// leaving it to a comment.
type Admissions interface {
	// Admit charges one session's admission weight. It is idempotent for a
	// repeated (key, agent).
	Admit(registry.Key, sessionwire.AgentID) error

	// Release credits an admitted session's weight back.
	Release(registry.Key) bool
}

// LocalRegistry is the local residency index this manager installs into.
//
// It is an interface over the concrete *registry.Registry for ONE reason worth
// stating, because "for testing" would be the wrong one: the registry race is
// the substance of this task, and the losing branch is reachable only when two
// runtimes have already been constructed. A concurrent test can produce that
// but cannot GUARANTEE it, and a coin-flip guard over the one bug this task
// exists to fix is not a guard. The seam lets a test interleave the competing
// insert deterministically; the concurrent test then runs anyway, over the same
// code, under -race.
type LocalRegistry interface {
	Get(registry.Key) (registry.Entry, bool)
	Insert(registry.Key, registry.Admission) (registry.Entry, bool)
	RemoveByGeneration(registry.Key, uint64) bool
}

// The concrete registry satisfies the seam. A drift here is a compile failure
// rather than a test that quietly stops covering the real type.
var _ LocalRegistry = (*registry.Registry)(nil)

// ---------------------------------------------------------------------------
// Identities carried into the session root context
// ---------------------------------------------------------------------------

// MaxPrincipalFieldBytes bounds each principal field, mirroring the bound Core
// puts on the identities it accepts on the wire.
const MaxPrincipalFieldBytes = sessionwire.MaxIDBytes

// ActorID identifies who an attach acts for: a Factory service identity or an
// end user. It is declared here because Core v0.7.0 has no such type.
type ActorID string

// TraceID correlates an attach with the request that caused it.
type TraceID string

// Principal is the WHITELIST of request-scoped values a session may carry.
//
// It is a whitelist expressed as a TYPE rather than as a list of keys to strip,
// and that is the whole security property. A session context is derived from
// this Manager's own root — never from a request context — and the only value
// installed on it is one of these. So an auth token, a bearer credential, a
// database handle or a cancellation a caller happened to attach to its request
// context is not "dropped": it was never reachable. There is no
// context.WithoutCancel anywhere in this package, and a test parses the
// production files to keep it that way.
//
// EVERY FIELD IS STRING-SHAPED, and a test asserts it over the reflected type
// rather than over this sentence. A field of interface, map, pointer or channel
// kind would reopen exactly the hole the type closes: `any` would let a caller
// hand the session an arbitrary value, including the request context itself.
type Principal struct {
	// TenantID is the tenant the attach acts within. It must equal the
	// request's tenant; a principal for one tenant may not attach another's
	// session.
	TenantID sessionwire.TenantID

	// ActorID is who the attach acts for. It is REQUIRED: an attach with no
	// actor is an attach nothing can be attributed to.
	ActorID ActorID

	// TraceID correlates the attach with its cause. It is optional.
	TraceID TraceID
}

// principalKey is the unexported key the principal is installed under, so no
// other package can install or overwrite one.
type principalKey struct{}

// PrincipalFrom returns the approved principal a session context carries.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok
}

// ---------------------------------------------------------------------------
// Requests and results
// ---------------------------------------------------------------------------

// Mode selects whether an attach creates a session or restores one.
type Mode string

const (
	// ModeCreate launches a new session.
	ModeCreate Mode = "create"

	// ModeRestore relaunches one over existing durable state.
	ModeRestore Mode = "restore"
)

// Request is one attach request.
type Request struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	AgentID   sessionwire.AgentID
	Mode      Mode

	// CompatibilityID is the runtime build Factory placed this session on. It
	// is OPTIONAL, and when set it must equal the target's own. Checking it in
	// step 1 refuses a mis-placed session before a lease is taken; the durable
	// state's build is checked again in step 4, against a different source.
	CompatibilityID department.CompatibilityID

	// Principal is the approved subset of the caller's identity that may reach
	// the session root context. See Principal.
	Principal Principal
}

// Residency is what an attach reports.
type Residency struct {
	Key             registry.Key
	AgentID         sessionwire.AgentID
	CompatibilityID department.CompatibilityID
	LeaseEpoch      uint64
	Generation      uint64
	Runtime         department.Runtime

	// Attached reports whether THIS call established the residency. False
	// means the session was already resident and this call took nothing.
	Attached bool
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Step names the sequence step an attach failed at.
type Step string

const (
	StepValidate     Step = "validate"
	StepLease        Step = "lease"
	StepFence        Step = "fence"
	StepHydrate      Step = "hydrate"
	StepCapabilities Step = "capabilities"
	StepInstall      Step = "install"
	StepPublish      Step = "publish"
	StepOwnership    Step = "ownership"
	StepAttached     Step = "attached"
)

// ErrLeaseHeld is the cause a SessionLeases implementation returns when another
// holder owns the lease. §15 calls this outcome LeaseHeld and says what it
// means: the registry Factory routed from was stale.
var ErrLeaseHeld = errors.New("residency: the session lease is held by another owner")

// AttachError reports a refused or failed attach.
//
// Step names WHERE it failed, which is what a Host operator needs and what no
// wrapped cause carries. Code is the HostLink refusal class Factory branches on
// and is EMPTY when the failure is not a placement outcome — a store that
// failed is not a reason to re-run placement, and giving it a code would tell
// Factory to move a session that has nowhere better to go.
type AttachError struct {
	Step   Step
	Code   sessionwire.HostLinkErrorCode
	Key    registry.Key
	Reason string
	Cause  error

	// Unreleased names every compensation that itself failed, so a caller can
	// tell "the attach failed and took nothing" from "the attach failed and
	// something is still held". The second is an operator's problem — a live
	// route with no owner, a lease nobody will renew — and it must not be
	// indistinguishable from the first.
	Unreleased []string
}

func (e *AttachError) Error() string {
	message := "residency: attaching session " + strconv.Quote(string(e.Key.SessionID)) +
		" failed at " + string(e.Step) + ": " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	if len(e.Unreleased) > 0 {
		message += " (and the rollback could not release " + strings.Join(e.Unreleased, "; ") + ")"
	}
	return message
}

// Unwrap returns the typed error a lower layer produced, if any.
func (e *AttachError) Unwrap() error { return e.Cause }

// HostLinkCode returns the refusal class Factory branches on, and whether this
// failure has one at all. It is a two-value accessor rather than an ErrorCode
// method because the empty code is a real state and a bare accessor would hand
// a caller a value Core refuses.
func (e *AttachError) HostLinkCode() (sessionwire.HostLinkErrorCode, bool) {
	return e.Code, e.Code != ""
}

// InvalidManagerOptionsError reports an option a Manager may not run with.
type InvalidManagerOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidManagerOptionsError) Error() string {
	return "residency: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

// Options configures a Manager.
type Options struct {
	// Host is the validated Host whose identity, endpoint, placement, tenant,
	// registry expiry and clock the projection is derived from. Nothing here
	// re-states a value host.New already checked.
	Host *host.Host

	// HostGeneration is this Host process's incarnation identity. Core
	// requires it to be non-zero on every HostLink record, and it belongs to
	// the run rather than to the configuration, which is why it is not on
	// host.Options.
	HostGeneration uint64

	// Registry is the local residency index. It is an optimization; see
	// LocalRegistry and the registry package comment.
	Registry LocalRegistry

	// Admissions is the Host's ONE admission ledger. See Admissions.
	Admissions Admissions

	Leases     SessionLeases
	Journal    JournalFencer
	Durable    DurableStore
	Workspaces Workspaces
	Locations  Locations
	Ownership  Ownership
}

// sessionRecord is the process-local state a residency owns that the registry
// deliberately does not carry: the session root context and the ownership
// handle. Keeping it here rather than widening registry.Entry is the difference
// between an index and a lifetime owner.
type sessionRecord struct {
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	ownership  OwnershipHandle
}

// Manager creates and restores sessions under the durable session lease.
type Manager struct {
	host       *host.Host
	generation uint64

	registry   LocalRegistry
	admissions Admissions
	leases     SessionLeases
	journal    JournalFencer
	durable    DurableStore
	workspaces Workspaces
	locations  Locations
	ownership  Ownership

	// root is THE ONE explicit root session context. Every session context is
	// derived from it and from nothing else, which is what makes a session's
	// lifetime independent of the request that started it and makes a
	// request's values unreachable from a session.
	root       context.Context
	cancelRoot context.CancelFunc

	mu       sync.Mutex
	sessions map[registry.Key]*sessionRecord

	// attaching serializes attaches of one key. See Manager.acquireKey.
	attaching map[registry.Key]chan struct{}

	// parked, when set, is called immediately before an attach parks on a busy
	// key slot. It exists so that PARKING IS OBSERVABLE, which is what turns
	// "a second attach must not return from inside the install window" from a
	// race into a decidable question: exactly one of parked-or-returned
	// happens, so a select between the two is deterministic. It is unexported
	// and set only from inside this package, always under mu.
	parked func()
}

// NewManager validates options and returns a Manager.
func NewManager(options Options) (*Manager, error) {
	if options.Host == nil {
		return nil, &InvalidManagerOptionsError{Field: "Host", Reason: "must be set; there is nothing to attach to without one"}
	}
	if options.HostGeneration == 0 {
		return nil, &InvalidManagerOptionsError{Field: "HostGeneration", Reason: "must be non-zero; Core refuses every HostLink record carrying a zero generation"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Registry", options.Registry != nil},
		{"Admissions", options.Admissions != nil},
		{"Leases", options.Leases != nil},
		{"Journal", options.Journal != nil},
		{"Durable", options.Durable != nil},
		{"Workspaces", options.Workspaces != nil},
		{"Locations", options.Locations != nil},
		{"Ownership", options.Ownership != nil},
	} {
		if !required.present {
			return nil, &InvalidManagerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	root, cancel := context.WithCancel(context.Background())
	return &Manager{
		host:       options.Host,
		generation: options.HostGeneration,
		registry:   options.Registry,
		admissions: options.Admissions,
		leases:     options.Leases,
		journal:    options.Journal,
		durable:    options.Durable,
		workspaces: options.Workspaces,
		locations:  options.Locations,
		ownership:  options.Ownership,
		root:       root,
		cancelRoot: cancel,
		sessions:   map[registry.Key]*sessionRecord{},
		attaching:  map[registry.Key]chan struct{}{},
	}, nil
}

// SessionContext returns the root context of a resident session.
func (m *Manager) SessionContext(key registry.Key) (context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, held := m.sessions[key]
	if !held {
		return nil, false
	}
	return record.ctx, true
}

// Close cancels this Manager's root session context, which cancels every
// session derived from it and therefore every ownership started under one.
//
// IT IS NOT RESIDENCY RELEASE, and the difference is deliberate rather than an
// omission. Release is a PROTOCOL — mark releasing, checkpoint, remove the
// registry entry, tombstone the projection, release the lease — and it belongs
// to O3.2 and O6.1. Doing half of it here would create a second release path
// that writes no tombstone and releases no lease, which is worse than doing
// none of it: the durable projection would outlive the process that wrote it
// and Factory would route to a Host that had stopped listening. Close is for
// process shutdown after that protocol has run, or for a test.
//
// FOR O3.2/O6.1, WITH O3.1's REACHABILITY STATED: m.sessions is never pruned by
// this package, because nothing here ends a residency. If a record is ever
// orphaned — its registry entry replaced under the same key by another writer,
// which needs a second writer to that registry and so cannot happen in O3.1 —
// the record stays forever holding a live session context and an ownership
// handle, and every later attach for that key launches a runtime and discards
// it. Release owns the pruning, under the same mutex that writes the record.
func (m *Manager) Close() {
	m.cancelRoot()
}

// Attach creates or restores a session under its lease and reports the
// residency. It is the state machine this package's doc comment describes.
//
// THE REQUEST CONTEXT IS NOT PROPAGATED, and that is the point rather than an
// oversight. Spec §8.3 separates request lifetime from session residency: a
// runtime launched on the caller's context dies when the caller disconnects,
// and a rollback run on it releases nothing once the caller has gone — which is
// the failure mode a rolled-back attach can least afford. Every collaborator
// below is called on the SESSION context, derived from this Manager's root, and
// every rollback on the root itself.
//
// TWO COSTS, and they COMPOUND, which is why they are written together rather
// than one each in their own place. A caller cannot cancel an attach; and
// attaches of one key are serialized, so one hung Create or Restore parks every
// later attacher for that key on the slot with no timeout and no way for any of
// them to abandon. The bound is the collaborators' own deadlines, and that
// bound now covers the QUEUE and not just the one call. If it stops being
// enough the answer is a deadline owned by this package — a bound on the whole
// sequence, applied to the session context — and not a return to the request's
// lifetime, which would reintroduce the failure the paragraph above describes.
func (m *Manager) Attach(requestCtx context.Context, request Request) (Residency, error) {
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}

	// STEP 1 RUNS FIRST, INCLUDING ON THE WARM PATH. An earlier version put the
	// idempotency check ahead of validation, so a request naming an unknown
	// mode, no actor, or a runtime build this Host does not launch was REFUSED
	// COLD AND ACCEPTED WARM. Step 1 says validate the AgentID AND the runtime
	// compatibility; checking only the agent on the resident path is half of a
	// rule. Validation reaches no collaborator, so running it first still
	// leaves "a refused request took nothing" true.
	target, err := m.validate(key, request)
	if err != nil {
		return Residency{}, err
	}

	// THE FAST IDEMPOTENCY PATH IS GATED ON THE SESSION RECORD, which exists
	// only from step 9. Gating it on the REGISTRY was a live escape: Insert
	// marks an entry resident and accepting at step 6, so a concurrent Attach
	// arriving in the 6-to-9 window bypassed the serialization entirely and
	// returned success carrying the winner's runtime — which the winner then
	// released when its own step 7, 8 or 9 failed. The erroring call took
	// nothing and the SUCCEEDING one was left holding a corpse: a Runtime whose
	// ReleaseResidency had already been called and whose lease was gone.
	//
	// The record is written last, under the same mutex, so it reports attached
	// only for a residency that reached step 9.
	if existing, attached := m.attachedResidency(key); attached {
		return existingResidency(existing, key, request)
	}

	// ONE ATTACH PER KEY AT A TIME. Without this, N concurrent cold attaches
	// each acquire a lease, construct a runtime and race the registry, and N-1
	// of them then throw a runtime away — which is the behaviour step 5 calls a
	// bug, merely tidied up afterwards rather than avoided. Serializing turns
	// "concurrent cold restore" into one launch and N-1 idempotent hits.
	//
	// The RE-READ after the slot is held is what makes that true; without it
	// every waiter proceeds to launch its own runtime in turn. It uses the same
	// predicate as the fast path, so a residency that is merely half-installed
	// is never reported to anyone.
	release := m.acquireKey(key)
	defer release()
	if existing, attached := m.attachedResidency(key); attached {
		return existingResidency(existing, key, request)
	}
	return m.attach(key, request, target)
}

// attachedResidency reports the residency of a session this Manager has
// ATTACHED — one that reached step 9 — and nothing else.
//
// It is deliberately narrower than "the registry holds an entry". A registry
// entry exists from step 6, and between 6 and 9 the attach that installed it
// may still fail and take it away again; an entry installed by anything other
// than this Manager has no session record here at all and is left to the
// registry-loser branch, which knows how to give back what it took.
func (m *Manager) attachedResidency(key registry.Key) (registry.Entry, bool) {
	m.mu.Lock()
	record, known := m.sessions[key]
	m.mu.Unlock()
	if !known {
		return registry.Entry{}, false
	}
	entry, resident := m.registry.Get(key)
	if !resident || entry.Generation != record.generation {
		return registry.Entry{}, false
	}
	return entry, true
}

// acquireKey serializes attaches of one session and returns the release. A
// waiter parks on the holder's channel rather than spinning.
func (m *Manager) acquireKey(key registry.Key) func() {
	for {
		m.mu.Lock()
		if waiting, busy := m.attaching[key]; busy {
			parked := m.parked
			m.mu.Unlock()
			if parked != nil {
				parked()
			}
			<-waiting
			continue
		}
		held := make(chan struct{})
		m.attaching[key] = held
		m.mu.Unlock()
		return func() {
			m.mu.Lock()
			delete(m.attaching, key)
			m.mu.Unlock()
			close(held)
		}
	}
}

// existingResidency reports a residency that already exists, refusing a caller
// whose request does not describe it.
//
// BOTH HALVES OF STEP 1'S IDENTITY RULE ARE CHECKED HERE, against the RESIDENT
// entry rather than against the current launch target: a target upgraded under
// a resident session still holds a runtime built by the old one, so the build
// the caller was placed on must be compared with the build actually running.
// The tenant needs no check because it is half of the key.
func existingResidency(entry registry.Entry, key registry.Key, request Request) (Residency, error) {
	if entry.AgentID != request.AgentID {
		return Residency{}, &AttachError{
			Step:   StepValidate,
			Code:   sessionwire.HostLinkErrorRuntimeMismatch,
			Key:    key,
			Reason: "the session is resident as agent " + strconv.Quote(string(entry.AgentID)) + " and cannot also be attached as " + strconv.Quote(string(request.AgentID)),
		}
	}
	if request.CompatibilityID != "" && request.CompatibilityID != entry.CompatibilityID {
		return Residency{}, &AttachError{
			Step:   StepValidate,
			Code:   sessionwire.HostLinkErrorRuntimeMismatch,
			Key:    key,
			Reason: "the session is resident on runtime " + strconv.Quote(string(entry.CompatibilityID)) + " and the request was placed on " + strconv.Quote(string(request.CompatibilityID)),
		}
	}
	return Residency{
		Key:             entry.Key,
		AgentID:         entry.AgentID,
		CompatibilityID: entry.CompatibilityID,
		LeaseEpoch:      entry.LeaseEpoch,
		Generation:      entry.Generation,
		Runtime:         entry.Runtime,
		Attached:        false,
	}, nil
}

// compensation is one thing an attach took and must give back.
//
// The SHARED flag is not a refinement, it is a correctness rule with a measured
// failure behind it. The admission ledger and the workspace are keyed by
// (TenantID, SessionID) rather than by attempt — service.Admit is idempotent
// for a repeated (key, agent) and EnsureWorkspace materializes one root per
// session — so an attach that loses the registry race and "releases everything
// it took" credits back the WINNER's admission and deletes the WINNER's
// workspace. The resident session would then be uncharged and this Host would
// admit past its capacity, with neither package's tests able to see it. A
// failed attach with no winner releases both; a registry loser releases only
// what is its own.
//
// THE PRECONDITION, stated because it is not enforced here: leaving a shared
// resource to the winner is right only if the winner CHARGED it. Every path
// through this package admits before it inserts, so it holds today; a residency
// installed into the registry by code that did not call Admit would strand the
// loser's charge instead. If a second writer to this registry ever appears,
// that is the assumption to revisit.
type compensation struct {
	shared bool
	name   string
	run    func(context.Context) error
}

// unwinder holds the compensations of one in-flight attach, released in
// reverse order.
type unwinder struct{ actions []compensation }

// own records a compensation for a resource belonging to this attempt alone.
func (u *unwinder) own(name string, run func(context.Context) error) {
	u.actions = append(u.actions, compensation{name: name, run: run})
}

// shared records a compensation for a resource keyed by the session, which a
// registry loser must leave to the winner.
func (u *unwinder) shared(name string, run func(context.Context) error) {
	u.actions = append(u.actions, compensation{shared: true, name: name, run: run})
}

// unwind releases what was taken, most recent first, and NAMES WHAT IT COULD
// NOT GIVE BACK.
//
// A compensation can itself fail, and the failure is not cosmetic: a
// tombstone write that is refused leaves a LIVE ROUTE while the attach
// reports failure, so Factory keeps sending work to a Host that owns nothing.
// Swallowing that made the package's own guarantee — "an Attach that returns an
// error took nothing that is still held" — unfalsifiable in exactly the case
// where it is false. Every failure is named on the returned AttachError.
//
// It CONTINUES past a failure rather than stopping. The compensations are
// independent, and abandoning the rest because one failed would turn one leaked
// resource into five.
func (u *unwinder) unwind(ctx context.Context, includeShared bool) []string {
	var unreleased []string
	for i := len(u.actions) - 1; i >= 0; i-- {
		if u.actions[i].shared && !includeShared {
			continue
		}
		if err := u.actions[i].run(ctx); err != nil {
			unreleased = append(unreleased, u.actions[i].name+": "+err.Error())
		}
	}
	return unreleased
}

// attach is the nine-step sequence. It is one function on purpose: the ORDER is
// the contract, and a sequence split across methods is a sequence a reader has
// to reassemble before they can check it.
func (m *Manager) attach(key registry.Key, request Request, target snapshotTarget) (Residency, error) {
	// -- 1. AgentID, runtime compatibility, and Host admission ---------------
	//
	// The identity half ran in Attach, so that the WARM path is validated by
	// the same code. What is left of step 1 is the COLD-path compatibility
	// comparison — see Manager.validate for why it is not shared — and
	// admission, which must not be charged for a request that was going to be
	// refused anyway.
	if request.CompatibilityID != "" && request.CompatibilityID != target.compatibility {
		return Residency{}, &AttachError{
			Step:   StepValidate,
			Code:   sessionwire.HostLinkErrorRuntimeMismatch,
			Key:    key,
			Reason: "the session was placed on runtime " + strconv.Quote(string(request.CompatibilityID)) + " and this Host launches " + strconv.Quote(string(target.compatibility)),
		}
	}
	capabilities := target.capabilities

	if err := m.admissions.Admit(key, request.AgentID); err != nil {
		return Residency{}, &AttachError{
			Step:   StepValidate,
			Code:   admissionCode(err),
			Key:    key,
			Reason: "this Host would not admit the session",
			Cause:  err,
		}
	}
	unwound := &unwinder{}
	unwound.shared("admission", func(context.Context) error { m.admissions.Release(key); return nil })

	// The ONE explicit root session context, and the only place one is made.
	sessionCtx, cancelSession := m.newSessionContext(request.Principal)
	unwound.own("session context", func(context.Context) error { cancelSession(); return nil })

	fail := func(step Step, code sessionwire.HostLinkErrorCode, reason string, cause error) (Residency, error) {
		// The rollback runs on the MANAGER ROOT: not on the request, which may
		// already be cancelled, and not on the session context, which this
		// very rollback is cancelling.
		unreleased := unwound.unwind(m.root, true)
		return Residency{}, &AttachError{Step: step, Code: code, Key: key, Reason: reason, Cause: cause, Unreleased: unreleased}
	}

	// -- 2. the SessionStore lease and its new epoch -------------------------
	lease, err := m.leases.AcquireSessionLease(sessionCtx, key.TenantID, key.SessionID)
	switch {
	case err != nil:
		code := sessionwire.HostLinkErrorCode("")
		if errors.Is(err, ErrLeaseHeld) {
			// §15: LeaseHeld means the registry Factory routed from was stale,
			// so the binding is invalidated and placement re-run. A store that
			// merely failed gets NO code: telling Factory to move a session
			// that has nowhere better to go turns an outage into a stampede.
			code = sessionwire.HostLinkErrorEpochMismatch
		}
		return fail(StepLease, code, "the session lease could not be acquired", err)
	case lease == nil:
		return fail(StepLease, "", "the lease store reported success and granted nothing", nil)
	}
	unwound.own("session lease", lease.Release)

	epoch := lease.Epoch()
	if epoch == 0 {
		// Refused HERE rather than at step 7. Core rejects a zero lease_epoch
		// on the registry observation, so a Host that carried one this far
		// would discover it having already fenced the journal and launched a
		// runtime.
		return fail(StepLease, "", "the lease was granted with epoch 0, which Core refuses on every fenced record", nil)
	}

	// -- 3. the opening journal fence, BEFORE hydration ----------------------
	if err := m.journal.CommitOpeningFence(sessionCtx, key.TenantID, key.SessionID, epoch); err != nil {
		return fail(StepFence, "", "the opening journal fence could not be committed", err)
	}
	// A COMMITTED FENCE IS NOT COMPENSATED, and that is the design rather than
	// an omission: §10.1 makes a fence permanent on purpose, so that a
	// successor's fence — not a predecessor's tidying up — is what makes the
	// predecessor's sequence stale. There is nothing to undo and undoing it
	// would be the bug.

	// -- 4. hydration: durable state, workspace, runtime ---------------------
	state, err := m.durable.LoadSessionState(sessionCtx, key.TenantID, key.SessionID)
	if err != nil {
		return fail(StepHydrate, "", "the durable session state could not be read", err)
	}
	if request.Mode == ModeRestore {
		switch {
		case !state.Exists:
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "the session has no durable state to restore from", nil)
		case state.CompatibilityID != target.compatibility:
			// Fail closed BEFORE the launch, for O1.2's reason: deciding this
			// after the target has produced a session spends the resource the
			// check exists to protect. The target checks it again against the
			// value it is handed; this check is against the DURABLE state,
			// which is a different source.
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeMismatch,
				"the durable state was written by runtime "+strconv.Quote(string(state.CompatibilityID))+" and this Host launches "+strconv.Quote(string(target.compatibility)), nil)
		case capabilities.RequiresCheckpoint && !state.HasCheckpoint:
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "the target requires a checkpoint and the session has none", nil)
		}
	}

	var workspaceRoot string
	if capabilities.RequiresWorkspace {
		workspaceRoot, err = m.workspaces.EnsureWorkspace(sessionCtx, key.TenantID, key.SessionID)
		if err != nil {
			return fail(StepHydrate, "", "the session workspace could not be materialized", err)
		}
		unwound.shared("workspace", func(ctx context.Context) error {
			return m.workspaces.ReleaseWorkspace(ctx, key.TenantID, key.SessionID)
		})
	}

	storage := department.StorageContext{Namespace: state.Namespace}
	var runtime department.Runtime
	if request.Mode == ModeCreate {
		runtime, err = target.target.Create(sessionCtx, department.CreateRequest{
			TenantID:      key.TenantID,
			SessionID:     key.SessionID,
			AgentID:       request.AgentID,
			Placement:     m.host.Placement(),
			WorkspaceRoot: workspaceRoot,
			Storage:       storage,
		})
	} else {
		runtime, err = target.target.Restore(sessionCtx, department.RestoreRequest{
			TenantID:        key.TenantID,
			SessionID:       key.SessionID,
			AgentID:         request.AgentID,
			Placement:       m.host.Placement(),
			WorkspaceRoot:   workspaceRoot,
			Storage:         storage,
			CompatibilityID: target.compatibility,
			RigSessionID:    state.RigSessionID,
		})
	}
	switch {
	case err != nil:
		return fail(StepHydrate, launchCode(err), "the runtime could not be launched", err)
	case runtime == nil:
		return fail(StepHydrate, "", "the launch target reported success and produced no runtime", nil)
	}
	unwound.own("runtime residency", runtime.ReleaseResidency)

	// -- 5. the required runtime capabilities --------------------------------
	//
	// The five CAPABILITY assertions are department's, made before it will
	// produce a Runtime at all, and they cannot be repeated here: Runtime
	// EMBEDS them, so a value that is missing one is not a Runtime. What is
	// left for this step is what a type cannot state — that the runtime is
	// bound to the session that was asked for, and that it has not already
	// stopped.
	if runtime.SessionID() != key.SessionID || runtime.AgentID() != request.AgentID {
		return fail(StepCapabilities, sessionwire.HostLinkErrorRuntimeMismatch,
			"the launched runtime is bound to session "+strconv.Quote(string(runtime.SessionID()))+" as agent "+strconv.Quote(string(runtime.AgentID()))+", which is not what was attached", nil)
	}
	select {
	case <-runtime.Done():
		return fail(StepCapabilities, sessionwire.HostLinkErrorRuntimeUnavailable, "the launched runtime has already stopped", nil)
	default:
	}

	// -- 6. the atomic local registry winner ---------------------------------
	entry, won := m.registry.Insert(key, registry.Admission{
		AgentID:         request.AgentID,
		Target:          target.target,
		CompatibilityID: target.compatibility,
		Runtime:         runtime,
		LeaseEpoch:      epoch,
	})
	if !won {
		// THE LOSER. It releases its OWN lease, runtime and context — the
		// runtime through the NONTERMINAL ReleaseResidency, never a terminal
		// teardown, because the session it was launched for is resident
		// elsewhere and remains resumable — and leaves the shared admission and
		// workspace to the winner. Leaving the runtime live is the bug this
		// branch exists to fix.
		if unreleased := unwound.unwind(m.root, false); len(unreleased) > 0 {
			// LOSING IS NOT A FAILURE; LEAKING IS. A loser whose own
			// ReleaseResidency or lease release failed holds a live runtime and
			// a lease nobody will renew, and reporting the residency with a nil
			// error would tell the caller the attach was fine and tell the
			// operator nothing at all. This is the rule fail() applies, applied
			// on the path beside it.
			return Residency{}, &AttachError{
				Step:       StepInstall,
				Key:        key,
				Reason:     "the session is resident under another attach and this one could not release what it took",
				Unreleased: unreleased,
			}
		}
		return existingResidency(entry, key, request)
	}
	unwound.own("registry entry", func(context.Context) error {
		m.registry.RemoveByGeneration(key, entry.Generation)
		return nil
	})

	// -- 7. the epoch-fenced durable residency projection --------------------
	observation := m.observation(key, request.AgentID, target.compatibility, epoch, sessionwire.SessionResidencyAttaching, false)
	if err := m.locations.PublishResidency(sessionCtx, observation); err != nil {
		return fail(StepPublish, "", "the durable residency projection could not be written", err)
	}
	unwound.own("residency tombstone", func(ctx context.Context) error {
		// §10.1: the route is removed by an EXPIRED epoch-fenced tombstone,
		// never by erasing the fencing high-water mark.
		return m.locations.TombstoneResidency(ctx, key.TenantID, key.SessionID, epoch)
	})

	// -- 8. inbox, event and heartbeat ownership -----------------------------
	handle, err := m.ownership.BeginOwnership(sessionCtx, OwnershipRequest{
		Key:             key,
		AgentID:         request.AgentID,
		CompatibilityID: target.compatibility,
		LeaseEpoch:      epoch,
		Generation:      entry.Generation,
		Runtime:         runtime,
	})
	switch {
	case err != nil:
		return fail(StepOwnership, "", "inbox, event and heartbeat ownership could not be started", err)
	case handle == nil:
		// The SAME RULE the lease and the runtime get, applied in the third
		// place it belongs. An accepted nil handle attaches the session with
		// nothing O3.2 can stop and nothing O6.1 can release, which is a leak
		// reported as a success.
		return fail(StepOwnership, "", "ownership reported success and returned no handle, so nothing it started could ever be stopped", nil)
	}
	unwound.own("ownership", handle.Stop)

	// -- 9. only then, attached ----------------------------------------------
	//
	// §9's state machine is cold -> ATTACHING -> resident, and the second
	// fenced write is what moves it. Publishing `resident` at step 7 would
	// advertise an accepting route before its inbox ownership existed;
	// publishing only `attaching` would leave every session attaching until
	// O3.2's first heartbeat, which Factory does not route to at all.
	//
	// WHAT `attaching` IS AND IS NOT VISIBLE FOR, so a later reader does not
	// "fix" it: the projection is written at step 7, which is AFTER hydration,
	// so `attaching` is externally observable only across the short 7-to-9
	// window and never during the slow part of an attach. §9.1's prose sketch
	// orders "Host registers observed location" before construction, but the
	// task's sequence governs and is the safer one — a crash during hydration
	// leaves no observation at all, which projects `cold`, which is correct.
	// Moving the first publish earlier would advertise a route for a session
	// that may never exist.
	resident := m.observation(key, request.AgentID, target.compatibility, epoch, sessionwire.SessionResidencyResident, true)
	if err := m.locations.PublishResidency(sessionCtx, resident); err != nil {
		return fail(StepAttached, "", "the resident residency projection could not be written", err)
	}

	m.mu.Lock()
	m.sessions[key] = &sessionRecord{generation: entry.Generation, ctx: sessionCtx, cancel: cancelSession, ownership: handle}
	m.mu.Unlock()

	return Residency{
		Key:             key,
		AgentID:         request.AgentID,
		CompatibilityID: target.compatibility,
		LeaseEpoch:      epoch,
		Generation:      entry.Generation,
		Runtime:         runtime,
		Attached:        true,
	}, nil
}

// snapshotTarget is the launch target and the two values it was asked for ONCE.
//
// The snapshot is the point. A LaunchTarget is an interface a caller
// implements, so Capabilities() and CompatibilityID() are arbitrary code that
// may answer differently every call: the value VALIDATED at step 1 does not
// bind the value RE-READ at step 4 or step 7. internal/service learned this as
// a divide-by-zero on the heartbeat path; here a drifting compatibility id
// would publish a route for a build the Host is not running.
type snapshotTarget struct {
	target        department.LaunchTarget
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
}

// validate holds step 1: every rule that must hold before this Host takes
// anything at all. It reaches no collaborator, which is what makes the claim
// that a refusal took nothing observable rather than asserted.
func (m *Manager) validate(key registry.Key, request Request) (snapshotTarget, error) {
	refuse := func(code sessionwire.HostLinkErrorCode, reason string, cause error) (snapshotTarget, error) {
		return snapshotTarget{}, &AttachError{Step: StepValidate, Code: code, Key: key, Reason: reason, Cause: cause}
	}
	if request.TenantID != m.host.TenantID() {
		return refuse("", "this Host serves tenant "+strconv.Quote(string(m.host.TenantID()))+" and the request names "+strconv.Quote(string(request.TenantID)), nil)
	}
	if err := request.SessionID.Validate(); err != nil {
		return refuse("", "the session identity is not a permitted sessionwire identity: "+err.Error(), err)
	}
	if fixed := m.host.FixedSessionID(); fixed != "" && fixed != request.SessionID {
		return refuse(sessionwire.HostLinkErrorRuntimeMismatch,
			"this dedicated Host is bound to session "+strconv.Quote(string(fixed))+" and cannot hold another", nil)
	}
	if err := request.AgentID.Validate(); err != nil {
		return refuse("", "the agent identity is not a permitted sessionwire identity: "+err.Error(), err)
	}
	switch request.Mode {
	case ModeCreate, ModeRestore:
	default:
		return refuse("", "the attach mode "+strconv.Quote(string(request.Mode))+" is neither create nor restore", nil)
	}
	if err := request.Principal.validate(request.TenantID); err != nil {
		return refuse("", err.Error(), err)
	}
	target, err := m.host.Department().Target(request.AgentID)
	if err != nil {
		return refuse(sessionwire.HostLinkErrorRuntimeUnavailable, "this Host's Department registers no launch target for that agent", err)
	}
	snapshot := snapshotTarget{target: target, compatibility: target.CompatibilityID(), capabilities: target.Capabilities()}
	// Re-validated against what the target answered JUST NOW, for the reason
	// the snapshot exists: department.New validated what it answered at
	// registration, and those are not the same two answers.
	if err := snapshot.compatibility.Validate(); err != nil {
		return refuse(sessionwire.HostLinkErrorRuntimeUnavailable, "the launch target reports an unusable compatibility id", err)
	}
	if err := snapshot.capabilities.Validate(); err != nil {
		return refuse(sessionwire.HostLinkErrorRuntimeUnavailable, "the launch target reports capabilities it could not have been registered with", err)
	}
	// THE REQUEST'S OWN COMPATIBILITY IS NOT COMPARED HERE, and the reason is a
	// regression this comparison caused when it was. Against a RESIDENT session
	// there are two builds in play — the one the current target launches and
	// the one the resident runtime was actually built by — and after an upgrade
	// they differ. Comparing against the CURRENT target on the warm path
	// refused the RESIDENT value, so between the two checks every non-empty
	// CompatibilityID was refused and the only correct placement was the one
	// that could not be expressed. The comparison a warm caller needs is
	// against the entry, and existingResidency owns it; this one belongs to the
	// cold path, where the target about to be launched is the only build there
	// is, and attach makes it before taking anything.
	return snapshot, nil
}

// validate holds every rule a Principal must satisfy. It is a method on the
// whitelist so a new approved field has one place to be checked.
func (p Principal) validate(tenant sessionwire.TenantID) error {
	if p.TenantID != tenant {
		return errors.New("the principal acts for tenant " + strconv.Quote(string(p.TenantID)) + " and the request names " + strconv.Quote(string(tenant)))
	}
	if p.ActorID == "" {
		return errors.New("the principal names no actor, so the attach could not be attributed to anyone")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"actor", string(p.ActorID)},
		{"trace", string(p.TraceID)},
	} {
		if len(field.value) > MaxPrincipalFieldBytes {
			return errors.New("the principal's " + field.name + " is longer than the " + strconv.Itoa(MaxPrincipalFieldBytes) + " bytes Core permits an identity")
		}
		if !utf8.ValidString(field.value) {
			return errors.New("the principal's " + field.name + " is not valid UTF-8")
		}
	}
	return nil
}

// newSessionContext derives a session from THIS MANAGER'S ROOT and installs the
// approved principal on it. It takes no context argument, which is what makes
// deriving a session from a request unrepresentable here rather than merely
// absent: there is no request context in scope to derive from.
func (m *Manager) newSessionContext(principal Principal) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithValue(m.root, principalKey{}, principal))
}

// observation derives the epoch-fenced durable projection of §15.
func (m *Manager) observation(key registry.Key, agent sessionwire.AgentID, compatibility department.CompatibilityID, epoch uint64, residency sessionwire.SessionResidency, accepting bool) sessionwire.HostLinkRegistryObservation {
	now := m.host.Clock().Now()
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               key.TenantID,
		SessionID:              key.SessionID,
		HostID:                 m.host.ID(),
		HostGeneration:         m.generation,
		AgentID:                agent,
		RuntimeCompatibilityID: string(compatibility),
		Placement:              m.host.Placement(),
		InternalEndpoint:       m.host.InternalEndpoint(),
		Residency:              residency,
		Accepting:              accepting,
		LeaseEpoch:             epoch,
		ObservedAt:             now,
		ExpiresAt:              now.Add(m.host.RegistryExpiry()),
	}
}

// admissionCode carries service's own HostLink refusal class through, rather
// than re-deciding it here. Two places deciding what "no capacity" means is how
// the two drift.
func admissionCode(err error) sessionwire.HostLinkErrorCode {
	var refused *service.AdmissionRefusedError
	if errors.As(err, &refused) {
		return refused.Code
	}
	return ""
}

// launchCode maps department's typed launch failures onto the class Factory
// branches on, and maps almost none of them.
//
// ONLY a compatibility mismatch gets a code, because only it is a PLACEMENT
// outcome: the request is well formed and another Host may be able to serve it.
// A rig that refused to launch, one that reported success and produced nothing,
// and one whose session is missing a capability Host requires are all HOST
// DEFECTS — a broken build or a misconfigured target — and department's own
// ruling on RigLaunchError and IncapableRuntimeError is that giving them a wire
// code tells a client to retry something only an operator can fix. Re-placing
// a session onto the next Host running the same broken build is a stampede,
// not a recovery. This defers to that ruling rather than making a second one.
func launchCode(err error) sessionwire.HostLinkErrorCode {
	var mismatch *department.CompatibilityMismatchError
	if errors.As(err, &mismatch) {
		return sessionwire.HostLinkErrorRuntimeMismatch
	}
	return ""
}
