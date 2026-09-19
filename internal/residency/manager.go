// Package residency creates and restores sessions under the durable session
// lease, in one fixed order, and releases everything it took when any step of
// that order fails.
//
// THE ORDER IS THE CONTRACT, not an implementation detail, and every step is
// there because the one before it must already have happened:
//
//  1. validate AgentID/runtime compatibility and Host admission;
//  2. obtain the SessionStore lease and its new epoch;
//  3. construct/restore the runtime and its workspace/checkpoint;
//  4. validate the required runtime capabilities and read the runtime's own
//     journal grant;
//  5. atomically install the local registry winner;
//  6. write the epoch-fenced durable Host registry/residency projection;
//  7. begin inbox/event/heartbeat ownership;
//  8. only then report attached.
//
// THERE USED TO BE A STEP BETWEEN 2 AND 3, and its removal is the one thing a
// reader of an older revision of this file must know. An opening in-stream
// journal fence was committed BEFORE hydration, on §10.1's reasoning that a
// predecessor's later appends must fail even before it observes Lease.Lost().
// That reasoning is sound and it is not Host's to act on. The fence is a
// JOURNAL write, Host does not hold the journal grant, and the released store
// binds a journal writer to ProtocolModeLegacy while AcquireResidency pins
// ProtocolModeDisposition — so the fence conflicted on binding.protocol_mode for
// EVERY session a Host can hold, and no composed Host reached step 7 at all.
// Under Bundle B, where disposition is the only supported mode, Host never opens
// that writer; the journal epoch it needs is the RUNTIME'S, read at step 4
// through department.LeaseEpochReporter, and the runtime's own composition root
// is what stakes the stream. Do not reinstate it here.
//
// Nothing here is durable except the projection, and that is not this package's
// to adjudicate: the LEASE is authoritative, the local registry is an
// optimization, and the projection is a routing hint Factory may find stale.
// What this package guarantees is narrower and is the thing a caller can rely
// on: an Attach that returns an error took nothing that is still held.
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

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
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
// recorded. THE REASON IS THE go.mod DECISION, NOT AN ABSENCE UPSTREAM, and an
// earlier version of this comment gave the wrong one: it said released harness
// v0.30.2 "has none of H4.1's capabilities", which was already false when
// harnessadapter bound to v0.31.0 — session.IdleWaiter, session.Liveness and
// session.Releaser are all exported and are asserted on by name there.
// sessionstore v0.6.0 does publish a residency grant with Epoch, Lost and
// Release, but in a SEPARATE epoch domain that must not stamp a journal fence,
// and JournalWriter still exposes no loss channel; see findings F3 and F15 in
// internal/sessionstoreadapter. So no released type satisfies Lease as declared
// below. These describe what Host requires; the concrete edges are later tasks.

// ResidencyEpoch is HOST'S OWN orchestration grant over a session, and it is a
// defined type rather than a uint64 so that the other epoch in this system cannot
// be put where one is wanted.
//
// THE TWO EPOCHS ARE DIFFERENT GRANTS WITH DIFFERENT HOLDERS. This one is minted
// by the lease Host takes in the session's residency namespace; it authorizes
// Host to publish the §15 route and to fence its own registry writes, and it
// authorizes NO journal write and no command application.
// sessionstore.ResidencyEpoch states the rule in terms — it "must never be
// compared with, or used as, a journal epoch" — and JournalEpoch below is the
// other one. That the two often agree early in a session's life is an accident of
// two fresh counters both starting at 1; this package's fakes are seeded from
// different bases precisely so that accident cannot pass for a relationship.
type ResidencyEpoch uint64

// JournalEpoch is THE RUNTIME'S grant over its own agent journal, surfaced to
// Host as a value it reads and never as one it chooses.
//
// HOST DOES NOT HOLD THIS LEASE. It is minted by the storage lease the runtime's
// composition root acquired, and the only route out of a live runtime is
// department.LeaseEpochReporter. Host reads it in order to STAMP work the runtime
// will fence — harness compares an admitted command's epoch for equality against
// the lease it holds, and a disposition attempt records the journal and residency
// epochs as separate members whose settlement fence orders the journal one. A
// residency epoch copied into either slot produces work that is refused, or an
// attempt no evidence can ever match.
type JournalEpoch uint64

// Lease is HOST'S OWN residency grant, and nothing else.
//
// IT USED TO BE BOTH GRANTS AT ONCE. Epoch() was documented as the value
// published as the registry observation's lease_epoch AND as the value stamped
// into the in-stream journal fence, which is two grants in two epoch domains
// behind one uint64. Against the LEGACY shared-backend store that was accurate —
// Store.OpenJournal hands out one number that really does play every role — but
// legacy is not a supported deployment target, and a disposition-mode session
// refuses the fusion outright: Store.AcquireResidency rejects a legacy session
// with catalog invalid and Store.OpenJournal rejects a disposition session with
// catalog conflict on binding.protocol_mode. The journal epoch is now
// JournalEpoch, read from the runtime; see finding F15 in
// internal/sessionstoreadapter.
//
// *sessionstore.ResidencyGrant satisfies this shape with the wrap in
// internal/sessionstoreadapter, which is also what finally gives Lost() a real
// provider signal rather than an echo of Host's own writes (finding F3).
type Lease interface {
	// Epoch is the RESIDENCY epoch this grant owns. It is never zero: Core
	// rejects a zero lease_epoch on the registry observation, so a store that
	// grants one has granted something Host cannot publish under.
	Epoch() ResidencyEpoch

	// Lost closes when the grant is no longer held, whether by renewal
	// failure, expiry, or observation of a later epoch. It is the FAST guard
	// only; the non-rebasing journal CAS is the hard backstop.
	Lost() <-chan struct{}

	// Release drops the grant. It is not termination: the session remains
	// durable and resumable.
	Release(context.Context) error
}

// SessionLeases grants exclusive epoch-fenced residency leases over a session.
type SessionLeases interface {
	// AcquireSessionLease grants the lease for one session, or reports
	// ErrLeaseHeld if another holder has it.
	//
	// A REFUSAL MAY STILL OWE A RELEASE, which is finding F16(b) and the reason
	// LeaseCleanupError exists. (Lease, error) cannot say "refused, and you
	// still owe a release", so every fake in this module read a non-nil error
	// as nothing acquired and LEAKED the case the released store actually
	// produces. An implementation in that state wraps the refusal in a
	// *LeaseCleanupError; a caller reaches it with errors.As and owns the retry.
	AcquireSessionLease(context.Context, sessionwire.TenantID, sessionwire.SessionID) (Lease, error)
}

// LeaseCleanupError is a refused acquisition that still holds a provider lease
// and, against sessionstore, a Store admission. Until a release succeeds the
// admission is retained and Store.Close may time out.
//
// IT CARRIES A RELEASE AND NOT A LEASE, deliberately. sessionstore's
// ResidencyAcquireCleanupError "exposes no ownership capability" and neither does
// this: the caller owns a cleanup obligation, not a grant, and handing back
// something with Epoch() and Lost() would invite a caller to attach under a lease
// the store has already refused it.
type LeaseCleanupError struct {
	// Cause is the refusal, joined with the cleanup failure by an
	// implementation that has both.
	Cause error

	// Cleanup retries the release. It may be nil only in a value nobody built
	// through a store; Release treats that as nothing owed.
	Cleanup func(context.Context) error
}

func (e *LeaseCleanupError) Error() string {
	message := "residency: the refused lease acquisition still holds a grant that must be released"
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *LeaseCleanupError) Unwrap() error { return e.Cause }

// Release retries the cleanup this refusal owes. It is safe on a nil Cleanup so
// that an unwinder can own the obligation without first interrogating it.
func (e *LeaseCleanupError) Release(ctx context.Context) error {
	if e.Cleanup == nil {
		return nil
	}
	return e.Cleanup(ctx)
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
	//
	// IT IS REQUIRED WHETHER OR NOT THE SESSION EXISTS. The prefix is a
	// property of the identities, not of the durable state, so a store that
	// answers Exists=false must still answer with the namespace that session's
	// objects will live under. An empty one is refused before the launch: a
	// runtime handed the empty prefix scopes every object it writes to nothing,
	// and it does so having reported success.
	Namespace string

	// CompatibilityID is the runtime build that wrote the durable state. A
	// restore onto a different build is refused.
	CompatibilityID department.CompatibilityID

	// RigSessionID is Harness's identity for the session: the runtime session
	// id the session's immutable durable binding names (Factory derives it
	// at create; it is NOT the Core session id). A restore that does not carry
	// it has nothing to restore from, and a create launches under it so the
	// journal the runtime writes is the one the binding names. Zero arises
	// only for a session with NO catalog record (Exists=false), and a create
	// then lets the rig mint one. A record without a binding (legacy) gets the
	// deprecated RigSessionIDs collaborator's answer, or an error.
	RigSessionID uuid.UUID

	// RuntimeJournal reports whether the runtime's own journal already holds
	// a conversation under RigSessionID. It is what decides a create: see
	// RuntimeJournal. The zero value is RuntimeJournalUnknown, which refuses a
	// create, so a store that forgot to answer fails closed. A legacy record
	// (no binding) is never probed, so it stays Unknown and every create over
	// one is refused; v0.2.1 launched it. No shipped path creates a legacy
	// record a Host could hold (residency is disposition-only).
	RuntimeJournal RuntimeJournal

	// HasCheckpoint and CheckpointSequence report the active workspace
	// checkpoint. A target declaring RequiresCheckpoint may not be restored
	// without one.
	HasCheckpoint      bool
	CheckpointSequence uint64
}

// RuntimeJournal is what the runtime's own journal holds for a session's
// runtime identity.
//
// IT EXISTS BECAUSE A CREATE CANNOT BE TRUSTED TO MEAN "NEW". Factory sends
// every attach as create (both inputs its attach mode is chosen from are dead
// on a disposition record), and harness does not verify that a session id it
// is asked to create under is fresh: naming a session whose journal exists
// re-opens THAT stream, appends a second SessionStarted and restores nothing,
// with no error. So a Host that launched every create fresh silently restarted
// every re-placed session's conversation. The journal is the authority; the
// attach mode is only a request.
type RuntimeJournal int

const (
	// RuntimeJournalUnknown means nothing established whether a journal
	// exists. A create is REFUSED on it rather than launched, because
	// launching is exactly the silent restart when the answer would have been
	// "present". It is the zero value on purpose.
	RuntimeJournalUnknown RuntimeJournal = iota
	// RuntimeJournalAbsent means the runtime has no conversation under the
	// session's runtime identity, so a create launches a new one under it.
	RuntimeJournalAbsent
	// RuntimeJournalPresent means a conversation exists, so a create restores
	// it instead of starting over.
	RuntimeJournalPresent
)

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
	LeaseEpoch      ResidencyEpoch
	Generation      uint64
	Runtime         department.Runtime

	// Fence is the attach's OWN epochFence, handed on rather than rebuilt.
	//
	// IT IS THE GRANT'S FENCE AND NOT MERELY ITS LEASE, and the difference is
	// what "the one mechanism" has to mean. Building a second fence over the
	// same lease gave two objects that shared Lost() and did NOT share the
	// classification: a supersession one of them observed was invisible to the
	// other, so the Manager could know the epoch was stale while the heartbeat
	// went on publishing under it. One grant, one fence.
	//
	// IT CLOSES A REAL HOLE, and an earlier version of this comment called it
	// equivalent on a reason that was simply wrong — that a heartbeat ends its
	// own fence only after the attach has returned. It does not: the heartbeat
	// starts at step 7 and TWO fenced writes follow it inside the same attach.
	// So a beat refused with ErrEpochSuperseded ends the heartbeat's fence
	// without closing Lost(); step 8 then fails for some other reason; and the
	// unwinder's tombstone goes through a Manager fence that knows nothing, at
	// an epoch a successor has already superseded. That closure's own comment
	// calls that the worst case in the file, because a tombstone is a route
	// removal and the route may be the successor's.
	Fence *epochFence
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

	// Draining reports whether this Host has begun graceful drain.
	//
	// IT IS ON THE LEDGER RATHER THAN BESIDE IT because they are the same
	// object and the same rule: internal/service owns exactly one of each, and
	// two sources of "draining" is a Host that stops accepting in one place
	// while advertising Accepting from the other. This Manager WAS that second
	// place — step 8 published accepting as a literal true — so an attach
	// admitted a moment before a drain began went on to advertise an accepting
	// route on a draining Host. The window is not narrow: Admit refuses new
	// sessions once draining, but an already-admitted attach spans the lease,
	// and the whole of hydration.
	Draining() bool
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
	// state's build is checked again in step 3, against a different source.
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
	Generation      uint64
	Runtime         department.Runtime

	// LeaseEpoch is HOST'S residency grant, the value published as the registry
	// observation's lease_epoch.
	LeaseEpoch ResidencyEpoch

	// JournalEpoch is THE RUNTIME'S grant, read from the launched runtime and
	// never derived from LeaseEpoch. Its type differs from LeaseEpoch's so the
	// two cannot be assigned across without a conversion a reviewer can see.
	//
	// JournalEpochHeld is the half a consumer must branch on. A runtime that is
	// headless, has no persistence, or is simply not wired for durable commands
	// legitimately holds no journal lease, and one whose lease has been released
	// or lost reports absence rather than the number it used to hold. Zero is
	// therefore not "no epoch" — false is.
	JournalEpoch     JournalEpoch
	JournalEpochHeld bool

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
	Host *hostconfig.Host

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
	host       *hostconfig.Host
	generation uint64

	registry   LocalRegistry
	admissions Admissions
	leases     SessionLeases
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
// process shutdown after that protocol has run, or for a test. An attach in
// flight when it arrives still rolls back completely: see the rollback context
// in attach, which Close deliberately cannot cancel.
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
//
// THE SLOT ALSO BARGES: acquireKey deletes the map entry before closing the
// channel it woke everyone on, so a fresh arrival can take the slot ahead of a
// waiter that has been parked longer. There is no correctness consequence —
// every waiter re-reads the residency and either finds it or attaches — but
// there is no fairness bound either, so a hot key under sustained arrivals can
// starve one waiter indefinitely. A queue would fix it and is not worth its
// complexity until an attach is slow enough for the starvation to be visible.
func (m *Manager) Attach(requestCtx context.Context, request Request) (Residency, error) {
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}

	// STEP 1 RUNS FIRST, INCLUDING ON THE WARM PATH. An earlier version put the
	// idempotency check ahead of validation, so a request naming an unknown
	// mode, no actor, or a runtime build this Host does not launch was REFUSED
	// COLD AND ACCEPTED WARM. Step 1 says validate the AgentID AND the runtime
	// compatibility; checking only the agent on the resident path is half of a
	// rule. Validation reaches no collaborator, so running it first still
	// leaves "a refused request took nothing" true.
	if err := m.validateRequest(key, request); err != nil {
		return Residency{}, err
	}

	// THE FAST IDEMPOTENCY PATH IS GATED ON THE SESSION RECORD, which exists
	// only from step 8. Gating it on the REGISTRY was a live escape: Insert
	// marks an entry resident and accepting at step 5, so a concurrent Attach
	// arriving in the 5-to-8 window bypassed the serialization entirely and
	// returned success carrying the winner's runtime — which the winner then
	// released when its own step 6, 7 or 8 failed. The erroring call took
	// nothing and the SUCCEEDING one was left holding a corpse: a Runtime whose
	// ReleaseResidency had already been called and whose lease was gone.
	//
	// The record is written last, under the same mutex, so it reports attached
	// only for a residency that reached step 8.
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

	// THE SNAPSHOT IS TAKEN HERE, inside the slot, and nowhere else. See
	// resolveTarget: taken before the wait, a parked attach launches on answers
	// the target gave before it parked.
	target, err := m.resolveTarget(key, request)
	if err != nil {
		return Residency{}, err
	}
	return m.attach(key, request, target)
}

// attachedResidency reports the residency of a session this Manager has
// ATTACHED — one that reached step 8 — and nothing else.
//
// It is deliberately narrower than "the registry holds an entry". A registry
// entry exists from step 5, and between 5 and 8 the attach that installed it
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
	// THE JOURNAL EPOCH IS READ LIVE ON THE WARM PATH TOO, from the resident
	// runtime rather than from the registry entry. The registry records Host's
	// residency grant, which is fixed for the life of the residency; the
	// runtime's journal grant is not Host's to cache, and a cached copy would be
	// the same fusion in a slower form.
	journalEpoch, journalHeld := entry.Runtime.LeaseEpoch()
	return Residency{
		Key:              entry.Key,
		AgentID:          entry.AgentID,
		CompatibilityID:  entry.CompatibilityID,
		LeaseEpoch:       ResidencyEpoch(entry.LeaseEpoch),
		Generation:       entry.Generation,
		Runtime:          entry.Runtime,
		JournalEpoch:     JournalEpoch(journalEpoch),
		JournalEpochHeld: journalHeld,
		Attached:         false,
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

// sharedHeld names the shared compensations this attach recorded and did not
// run, for a caller that must be told they are still held. It is the loser's
// half of the same honesty unwind provides for a failure.
//
// THE WORDING IS ONE DEGREE MORE CONFIDENT THAN THE SITUATION, and the report
// is honest where the sentence is not. "Not this call's to release" is exactly
// right when the winner charged the ledger, which is every path through this
// package. It is WRONG in the case a second registry writer makes reachable:
// a rival installed by code that never went through the ledger leaves the
// charge genuinely this call's, and skipping it strands the charge rather than
// handing it over. Naming it is still the right report — something is held and
// somebody must know — but a reader should not take the phrase as a claim about
// who owns it.
func (u *unwinder) sharedHeld() []string {
	var held []string
	for _, action := range u.actions {
		if action.shared {
			held = append(held, action.name+": held by the resident session and not this call's to release")
		}
	}
	return held
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
	unwound.shared("admission", func(context.Context) error {
		// The bool is CONSUMED. This attach charged the ledger, so a release
		// reporting that nothing was charged means something else credited it
		// back first — which is the double-credit that lets a Host admit past
		// its capacity, and it must not be discarded on the one path that would
		// have noticed.
		if !m.admissions.Release(key) {
			return errors.New("the ledger reports this session was not charged, so something else credited it back first")
		}
		return nil
	})

	// The ONE explicit root session context, and the only place one is made.
	sessionCtx, cancelSession := m.newSessionContext(request.Principal)
	unwound.own("session context", func(context.Context) error { cancelSession(); return nil })

	// THE ROLLBACK CONTEXT IS NOT THE MANAGER ROOT, and the reason is §8.3's,
	// one level up. Compensations ran on m.root, which Close cancels — so a
	// Close arriving during an in-flight attach disabled the entire rollback
	// and left a live §15 route with no owner and a lease nobody would renew.
	// That is the same "a rollback on a dead context releases nothing" failure
	// the request context is kept out of the sequence to avoid. A release must
	// happen whether or not this process is shutting down, so the rollback runs
	// on a context nothing here can cancel. It is not the request's either,
	// which may already be done.
	//
	// It carries NO principal and no deadline. It is not a session: nothing
	// downstream of it works on the tenant's behalf, it only gives back what
	// was taken, and installing an identity on a context with no lifetime owner
	// would be the opposite of what the whitelist is for.
	rollbackCtx := context.Background()

	fail := func(step Step, code sessionwire.HostLinkErrorCode, reason string, cause error) (Residency, error) {
		unreleased := unwound.unwind(rollbackCtx, true)
		return Residency{}, &AttachError{Step: step, Code: code, Key: key, Reason: reason, Cause: cause, Unreleased: unreleased}
	}

	// -- 2. the SessionStore lease and its new epoch -------------------------
	lease, err := m.leases.AcquireSessionLease(sessionCtx, key.TenantID, key.SessionID)
	switch {
	case err != nil:
		// F16(b). A REFUSAL CAN STILL OWE A RELEASE, and (Lease, error) cannot
		// say so. sessionstore's AcquireResidency returns no grant and yet
		// leaves a provider lease and a Store admission held when its own
		// rollback failed; until a release succeeds the admission is retained
		// and Close may time out. The obligation is OWNED BY THE UNWINDER
		// rather than retried inline, so the one ladder that reports what it
		// could not give back reports this too — an inline retry that failed
		// would vanish into the error text and out of Unreleased.
		var cleanup *LeaseCleanupError
		if errors.As(err, &cleanup) {
			unwound.own("refused session lease", cleanup.Release)
		}
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

	// THE ONE MECHANISM, for this attach's three fenced writes. All of them
	// were unclassified: a step 8 publish refused for a later epoch was seen
	// and then the unwinder tombstoned at the same epoch anyway — and a
	// tombstone is a ROUTE REMOVAL, so under a store that fences removals less
	// strictly than publishes that takes the SUCCESSOR's route away. The
	// heartbeat guards its equivalent by generation; the unwinder had nothing.
	//
	// It also closes the other half O3.1 disclosed and this program has been
	// chasing since: the Manager never consulted Lost() at all, so a grant
	// closing mid-attach still ended in an accepting route published under a
	// lease this Host does not hold, and Attached: true returned to the caller.
	fence := newEpochFence(lease)

	epoch := lease.Epoch()
	if epoch == 0 {
		// Refused HERE rather than at step 6. Core rejects a zero lease_epoch
		// on the registry observation, so a Host that carried one this far
		// would discover it having already launched a runtime.
		return fail(StepLease, "", "the lease was granted with epoch 0, which Core refuses on every fenced record", nil)
	}

	// -- 3. hydration: durable state, workspace, runtime ---------------------
	//
	// HYDRATION FOLLOWS THE LEASE DIRECTLY, with no journal write between them.
	// See the sequence at the top of this file for why the fence that used to
	// sit here is gone and must not come back.
	state, err := m.durable.LoadSessionState(sessionCtx, key.TenantID, key.SessionID)
	if err != nil {
		return fail(StepHydrate, "", "the durable session state could not be read", err)
	}
	// THE NAMESPACE IS REQUIRED IN BOTH MODES, and a genuinely cold create is
	// the case that made this necessary rather than pedantic. A create has no
	// durable state, so a store answering Exists=false also answered
	// Namespace="" — and the runtime was launched with the EMPTY PREFIX every
	// one of that session's durable objects is scoped by. It succeeded, which
	// is the worst available outcome. The namespace is a property of the
	// IDENTITIES and not of the state, which is why SessionState.Namespace is
	// documented as required whether or not the session exists; enforcing it
	// here is what makes that documentation a rule.
	if state.Namespace == "" {
		return fail(StepHydrate, "", "the durable store returned no object namespace for this session, and a runtime launched under the empty prefix would scope every object it writes to nothing", nil)
	}
	// THE BUILD CHECK IS NOT RESTORE-ONLY. It was, and a create over durable
	// state written by a different runtime proceeded and then published the new
	// build as the route for that session. Whether a create should meet
	// existing state at all is Factory's idempotency question — §16 binds the
	// create identity in SessionStore — but if it does, the build must still
	// agree. A session with state and no recorded build lands here too, and
	// fails closed.
	if state.Exists && state.CompatibilityID != target.compatibility {
		return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeMismatch,
			"the durable state was written by runtime "+strconv.Quote(string(state.CompatibilityID))+" and this Host launches "+strconv.Quote(string(target.compatibility)), nil)
	}
	if request.Mode == ModeRestore {
		switch {
		case !state.Exists:
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "the session has no durable state to restore from", nil)
		case capabilities.RequiresCheckpoint && !state.HasCheckpoint:
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "the target requires a checkpoint and the session has none", nil)
		}
	}

	// A CREATE IS A REQUEST AND THE RUNTIME JOURNAL DECIDES IT. See
	// RuntimeJournal for why the mode alone cannot be obeyed: a create over a
	// conversation that already exists is launched as a RESTORE of it, and a
	// create whose journal cannot be established is refused rather than
	// launched, because launching is the silent restart.
	//
	// THE REFUSALS BELOW ARE runtime_unavailable, the code this Host already
	// gives a restore it cannot perform (no durable state, a required
	// checkpoint missing): the journal is Unknown (no journal store serves the
	// binding — Compose refuses a reader that is not a harness store, so this
	// is a binding the table does not serve), or a restore needs a checkpoint
	// the session has not got. It is chosen for what it SAYS, not for what
	// factory v0.3.0 does with it: v0.3.0 treats runtime_unavailable exactly as
	// it treats no_capacity — try the next candidate, and retry on the next
	// sweep — and only COUNTS it, never logs it. runtime_mismatch would claim a
	// build problem, epoch_mismatch a stale binding, no_capacity and
	// not_admitting this Host's occupancy; none of those is what happened.
	//
	// A JOURNAL READ THAT FAILS NEVER REACHES THIS SWITCH. LoadSessionState
	// returns the error, and the attach is refused above with the EMPTY,
	// unclassified code, as every other durable-read failure is (factory
	// v0.3.0 reads that as ErrAttachFailed and also tries the next candidate).
	//
	// WHAT THIS DOES NOT CLOSE. The journal is read under the residency lease,
	// before the launch. A Host that reads Absent, stalls inside the launch and
	// LOSES its lease can still create over a conversation a successor began
	// in the meantime (measured: a second SessionStarted; the conversation
	// survived the next restore). No check Host can make closes it, because
	// the stall is inside the launch; it needs harness to create-if-absent
	// under its own journal lease. Booked as a harness follow-up.
	launch := request.Mode
	if request.Mode == ModeCreate && !state.RigSessionID.IsZero() {
		switch state.RuntimeJournal {
		case RuntimeJournalAbsent:
		case RuntimeJournalPresent:
			if capabilities.RequiresCheckpoint && !state.HasCheckpoint {
				return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "a create names a session whose runtime journal already holds a conversation, and restoring it requires a checkpoint the session has not got; it is refused rather than started over", nil)
			}
			launch = ModeRestore
		default:
			return fail(StepHydrate, sessionwire.HostLinkErrorRuntimeUnavailable, "a create names a runtime session whose journal this Host cannot inspect, so it cannot tell a new session from one it would silently restart; it is refused rather than launched", nil)
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
	if launch == ModeCreate {
		runtime, err = target.target.Create(sessionCtx, department.CreateRequest{
			TenantID:      key.TenantID,
			SessionID:     key.SessionID,
			AgentID:       request.AgentID,
			Placement:     m.host.Placement(),
			WorkspaceRoot: workspaceRoot,
			Storage:       storage,
			RigSessionID:  state.RigSessionID,
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

	// -- 4. the required runtime capabilities --------------------------------
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

	// THE RUNTIME'S OWN JOURNAL GRANT, read here and nowhere else in this
	// sequence. It is a capability of the launched runtime, so it cannot be read
	// before step 3 and must not be guessed before then — which is one of the two
	// reasons the attach-time journal fence could not soundly exist here: it ran
	// BEFORE hydration and so before any runtime grant was readable.
	//
	// A RUNTIME REPORTING NO GRANT IS NOT REFUSED. harness gates the report on
	// the lease still being held and answers (0, false) for a headless session,
	// one without persistence, one not wired for durable commands, and one whose
	// lease is already gone. The first three are legitimate configurations, and
	// the fourth is caught by the Done() check above, so refusing here would
	// turn supported deployments into attach failures.
	//
	// WHAT HOST MUST NEVER DO IS SUBSTITUTE ITS OWN GRANT — AND THE TYPE SYSTEM
	// DOES NOT STOP IT. An earlier version of this comment said the value below
	// "cannot be substituted: JournalEpoch and ResidencyEpoch are different
	// types". Go converts freely between named numeric types, so
	// `journalEpoch := JournalEpoch(epoch)` compiles, and a mutation doing
	// exactly that was measured compiling. The distinct types make the
	// substitution VISIBLE — it cannot happen by assignment, only by a
	// conversion a reviewer can see — and what REFUSES it is
	// TestTheJournalEpochComesFromTheRuntimeAndNotTheLease plus four others,
	// which can only refuse it because the fixtures keep the two epochs
	// deliberately distinct — this package mints residency epochs from 1000 and
	// its runtime reports 3; the composed suite uses 9 and 1. Credit the tests
	// and the fixture seeding, not the compiler.
	reportedEpoch, journalHeld := runtime.LeaseEpoch()
	journalEpoch := JournalEpoch(reportedEpoch)

	// -- 5. the atomic local registry winner ---------------------------------
	entry, won := m.registry.Insert(key, registry.Admission{
		AgentID:         request.AgentID,
		Target:          target.target,
		CompatibilityID: target.compatibility,
		Runtime:         runtime,
		LeaseEpoch:      uint64(epoch),
	})
	if !won {
		// THE LOSER. It releases its OWN lease, runtime and context — the
		// runtime through the NONTERMINAL ReleaseResidency, never a terminal
		// teardown, because the session it was launched for is resident
		// elsewhere and remains resumable — and leaves the shared admission and
		// workspace to the winner. Leaving the runtime live is the bug this
		// branch exists to fix.
		unreleased := unwound.unwind(rollbackCtx, false)
		residency, mismatch := existingResidency(entry, key, request)
		if mismatch != nil {
			// THE THIRD EXIT, and it is a FAILING one. The residency that won
			// is not the one this request describes, so there is nothing to
			// report as an idempotent success — and the shared admission charge
			// and workspace have already been skipped, deliberately, because
			// this Manager may be the winner's charger and crediting them back
			// would uncharge a resident session. So they are genuinely STILL
			// HELD and this call cannot safely give them back. Returning
			// existingResidency's error as-is named step "validate", carried no
			// Unreleased, and left an admission charge and a materialized
			// workspace behind in silence.
			var refusal *AttachError
			code := sessionwire.HostLinkErrorCode("")
			if errors.As(mismatch, &refusal) {
				code = refusal.Code
			}
			return Residency{}, &AttachError{
				Step:       StepInstall,
				Code:       code,
				Key:        key,
				Reason:     "the session is resident under an attach this request does not describe",
				Cause:      mismatch,
				Unreleased: append(unreleased, unwound.sharedHeld()...),
			}
		}
		if len(unreleased) > 0 {
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
		return residency, nil
	}
	unwound.own("registry entry", func(context.Context) error {
		// Consumed for the same reason: false means the generation this attach
		// installed is no longer the one held, so the residency was replaced
		// under it and this rollback removed nothing.
		if !m.registry.RemoveByGeneration(key, entry.Generation) {
			return errors.New("the residency was replaced under this generation, so nothing was removed")
		}
		return nil
	})

	// -- 6. the epoch-fenced durable residency projection --------------------
	observation := m.observation(key, request.AgentID, target.compatibility, epoch, sessionwire.SessionResidencyAttaching, false)
	if err := fence.write(func() error { return m.locations.PublishResidency(sessionCtx, observation) }); err != nil {
		return fail(StepPublish, "", "the durable residency projection could not be written", err)
	}
	unwound.own("residency tombstone", func(ctx context.Context) error {
		// §10.1: the route is removed by an EXPIRED epoch-fenced tombstone,
		// never by erasing the fencing high-water mark.
		//
		// THROUGH THE FENCE, and here that matters more than anywhere else in
		// this file. A tombstone is a route REMOVAL, so writing one after the
		// store has said a later epoch committed does not merely fail
		// harmlessly under a strict store — under any store that fences
		// removals less strictly than publishes it takes the SUCCESSOR's route
		// away. Refusing leaves this Host's own record to expire, which §18.2
		// assigns to registry expiry and Factory's due reconciler.
		return fence.write(func() error {
			return m.locations.TombstoneResidency(ctx, key.TenantID, key.SessionID, uint64(epoch))
		})
	})

	// -- 7. inbox, event and heartbeat ownership -----------------------------
	handle, err := m.ownership.BeginOwnership(sessionCtx, OwnershipRequest{
		Key:             key,
		AgentID:         request.AgentID,
		CompatibilityID: target.compatibility,
		LeaseEpoch:      epoch,
		Generation:      entry.Generation,
		Runtime:         runtime,
		Fence:           fence,
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

	// -- 8. only then, attached ----------------------------------------------
	//
	// §9's state machine is cold -> ATTACHING -> resident, and the second
	// fenced write is what moves it. Publishing `resident` at step 6 would
	// advertise an accepting route before its inbox ownership existed;
	// publishing only `attaching` would leave every session attaching until
	// O3.2's first heartbeat, which Factory does not route to at all.
	//
	// WHAT `attaching` IS AND IS NOT VISIBLE FOR, so a later reader does not
	// "fix" it: the projection is written at step 6, which is AFTER hydration,
	// so `attaching` is externally observable only across the short 6-to-8
	// window and never during the slow part of an attach. §9.1's prose sketch
	// orders "Host registers observed location" before construction, but the
	// task's sequence governs and is the safer one — a crash during hydration
	// leaves no observation at all, which projects `cold`, which is correct.
	// Moving the first publish earlier would advertise a route for a session
	// that may never exist.
	// DERIVED FROM THE REGISTRY, exactly as a heartbeat derives it, and not
	// from what this attach believes it just installed.
	//
	// The two writers share this record and their epoch, so the store cannot
	// order them; what keeps them from contradicting each other is publishing
	// the SAME FUNCTION of the SAME STATE. This step used to build `resident`
	// as a literal and accepting as `!Draining()` while the beat built
	// residencyOf(entry.State) and entry.Accepting && !Draining() — two of the
	// three content axes divergent, so a heartbeat that had already surrendered
	// between step 7 and here would have its releasing, not-accepting record
	// overwritten with resident and accepting.
	live, stillHeld := m.registry.Get(key)
	if !stillHeld || live.Generation != entry.Generation {
		return fail(StepAttached, "", "the residency was removed or replaced before it could be reported attached", nil)
	}
	resident := m.observation(key, request.AgentID, target.compatibility, epoch,
		residencyOf(live.State), live.Accepting && !m.admissions.Draining())
	if err := fence.write(func() error { return m.locations.PublishResidency(sessionCtx, resident) }); err != nil {
		return fail(StepAttached, "", "the resident residency projection could not be written", err)
	}

	m.mu.Lock()
	m.sessions[key] = &sessionRecord{generation: entry.Generation, ctx: sessionCtx, cancel: cancelSession, ownership: handle}
	m.mu.Unlock()

	return Residency{
		Key:              key,
		AgentID:          request.AgentID,
		CompatibilityID:  target.compatibility,
		LeaseEpoch:       epoch,
		Generation:       entry.Generation,
		Runtime:          runtime,
		JournalEpoch:     journalEpoch,
		JournalEpochHeld: journalHeld,
		Attached:         true,
	}, nil
}

// snapshotTarget is the launch target and the two values it was asked for ONCE.
//
// The snapshot is the point. A LaunchTarget is an interface a caller
// implements, so Capabilities() and CompatibilityID() are arbitrary code that
// may answer differently every call: the value VALIDATED at step 1 does not
// bind the value RE-READ at step 3 or step 6. internal/service learned this as
// a divide-by-zero on the heartbeat path; here a drifting compatibility id
// would publish a route for a build the Host is not running.
type snapshotTarget struct {
	target        department.LaunchTarget
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
}

// validateRequest holds the half of step 1 that is about the REQUEST: the
// identities, the placement binding, the mode and the principal. It reaches no
// collaborator, which is what makes the claim that a refusal took nothing
// observable rather than asserted.
//
// IT RUNS AHEAD OF THE WARM FAST PATH, so a resident session is validated by
// the same rules as a cold one. An earlier version put the idempotency check
// first and a request naming an unknown mode, no actor, or an agent this
// Department does not register was refused cold and accepted warm.
//
// It looks the agent up and DISCARDS the target. That lookup is the cost of
// keeping the unknown-agent rule on both paths, and it is a map read; what it
// must not do is snapshot, for the reason resolveTarget states.
func (m *Manager) validateRequest(key registry.Key, request Request) error {
	refuse := func(code sessionwire.HostLinkErrorCode, reason string, cause error) error {
		return &AttachError{Step: StepValidate, Code: code, Key: key, Reason: reason, Cause: cause}
	}
	// THE TENANT IS VALIDATED, NOT COMPARED, and that is human gate H8 answered
	// 2026-09-04 as option (a). This used to refuse any request whose tenant
	// differed from a fixed Options.TenantID; that field is gone, because a
	// Host that is tenant-exclusive BY CONSTRUCTION makes spec §12's isolation
	// class unreadable and forces one Host Deployment per tenant. The rule did
	// not disappear, it moved to the two places that can hold it correctly:
	// Factory placement, which §12 makes the enforcer, and this Host's ONE
	// admission ledger, whose lock is the only Host-wide critical section an
	// admission passes through and which refuses a second tenant unless the
	// advertised class is cross_tenant_isolated.
	//
	// WHAT REMAINS HERE IS THE IDENTITY RULE, and it is not a leftover. The
	// comparison was implicitly bounding the tenant too — an over-long or
	// invalid-UTF-8 tenant could not equal a validated Options.TenantID — so
	// removing it without this would have let an identity Core rejects on every
	// HostLink record reach the lease, the workspace prefix and the §15
	// projection. host.Options delegated this same rule to Core for its own
	// fields; a tenant now enters this Host here instead.
	if err := request.TenantID.Validate(); err != nil {
		return refuse("", "the tenant identity is not a permitted sessionwire identity: "+err.Error(), err)
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
	if _, err := m.host.Department().Target(request.AgentID); err != nil {
		return refuse(sessionwire.HostLinkErrorRuntimeUnavailable, "this Host's Department registers no launch target for that agent", err)
	}
	return nil
}

// resolveTarget takes the snapshot, and it is called ONLY WITH THE KEY SLOT
// HELD. That is the whole of its contract and it is not an optimisation.
//
// The snapshot drives the compatibility comparison, the workspace and
// checkpoint requirements, the launch, the registry entry and both §15
// projections. Taken before the slot, an attach that then PARKS behind another
// one uses answers the target gave before it waited — so a target that grew a
// RequiresWorkspace in the meantime got a runtime launched with an empty
// workspace root and no workspace compensation recorded, and a target that
// changed build published a route for a build the Host is not running. That
// second sentence is snapshotTarget's own stated reason for existing,
// reintroduced by taking the snapshot on the wrong side of the lock.
//
// The fix is NOT a second validate after the slot: two reads of a drifting
// target is the exact defect the snapshot type exists to prevent, and it breaks
// the guard that says so. One read, inside the slot.
//
// THE WINDOW IT DOES NOT CLOSE, because one read cannot: a target that
// redeclares between this snapshot and the launch is still launched on the
// snapshot. That window is IRREDUCIBLE under a one-read snapshot and what moved
// the slot changed is what it CONTAINS — no wait for another attach remains
// inside it, because AcquireSessionLease reports ErrLeaseHeld rather than
// waiting. The residual lands on the CREATE path: a restore reaches
// rigTarget.Restore, which compares the id it is handed against the target's
// own and fails closed, so a build that drifted mid-hydration is caught there.
// A create has no equivalent comparison and would launch on the stale id.
func (m *Manager) resolveTarget(key registry.Key, request Request) (snapshotTarget, error) {
	refuse := func(code sessionwire.HostLinkErrorCode, reason string, cause error) (snapshotTarget, error) {
		return snapshotTarget{}, &AttachError{Step: StepValidate, Code: code, Key: key, Reason: reason, Cause: cause}
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
func (m *Manager) observation(key registry.Key, agent sessionwire.AgentID, compatibility department.CompatibilityID, epoch ResidencyEpoch, residency sessionwire.SessionResidency, accepting bool) sessionwire.HostLinkRegistryObservation {
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
		// THE ONE CONVERSION TO THE WIRE. Core's observation carries a plain
		// uint64 and the record it fences is Host's own registry route, so the
		// residency epoch is the right value and this is the only place it is
		// stripped of its type.
		LeaseEpoch: uint64(epoch),
		ObservedAt: now,
		ExpiresAt:  now.Add(m.host.RegistryExpiry()),
	}
}

// Observe reports the §15 projection of a residency this Manager currently
// holds, as it stands NOW, and whether it holds one at all.
//
// IT IS THE ATTACH RPC'S ACCEPTED REPLY BODY. Core requires an accepted
// hostlink.attach to answer with the HostLinkRegistryObservation the following
// bind needs, and this is the ONE derivation of that record — the same
// `observation` the sequence publishes durably at its publish step — read from
// the registry rather than rebuilt from the attach result. The difference
// matters on the warm path: an attach that found the session already resident
// took nothing, and the entry it found may have moved on (a warm release may
// have closed admission, or begun), so the reply says what the registry says
// and not what the establishing call once saw. A router reading this and a
// router reading the durable projection are reading one derivation.
//
// The second result is false for a session this Manager does not hold, which a
// caller reaches only in the window between an attach returning and the
// residency being released; it is a fact and not a refusal, and the caller
// decides what to do with it.
func (m *Manager) Observe(key registry.Key) (sessionwire.HostLinkRegistryObservation, bool) {
	entry, resident := m.registry.Get(key)
	if !resident {
		return sessionwire.HostLinkRegistryObservation{}, false
	}
	return m.observation(key, entry.AgentID, entry.CompatibilityID, ResidencyEpoch(entry.LeaseEpoch), residencyOf(entry.State), entry.Accepting), true
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
