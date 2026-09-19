// Package department defines the immutable set of launch targets a Host serves.
//
// A Department is a REGISTRY, fixed at construction: it maps an agent identity
// to the target that can launch a runtime for it, and it never changes
// afterwards. Residency, admission, command consumption and HostLink are other
// tasks; this package answers one question, "what may this Host launch, and
// under what constraints", and answers it the same way for the life of the
// process.
//
// Nothing here is a Factory type. A Runtime is a composition of narrow Harness
// session capabilities that Host requires, declared on Host's side.
package department

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// CompatibilityID identifies the runtime build a target launches, so a restore
// onto an incompatible runtime is refused before it starts.
//
// It is declared HERE and not taken from Core because Core v0.7.0 spells this
// as a bare string field on the HostLink records
// (HostLinkCapacityReport.RuntimeCompatibilityID and its siblings) and exports
// no named type for it. Host needs the name to state its own contract; when
// Core promotes the field to a named type this becomes a one-line alias and the
// conversion at the wire edge disappears. The validation rule below is Core's,
// restated rather than imported because the function that holds it,
// validateHostLinkOpaque, is unexported.
type CompatibilityID string

// MaxCompatibilityIDBytes mirrors sessionwire.MaxIDBytes, which is what Core
// enforces on the wire field this value is written to. Naming it separately
// would let the two drift; it is derived.
const MaxCompatibilityIDBytes = sessionwire.MaxIDBytes

// Validate reports whether the identifier is a permitted opaque runtime
// identity: non-empty, at most MaxCompatibilityIDBytes bytes, valid UTF-8.
func (id CompatibilityID) Validate() error {
	switch {
	case len(id) == 0:
		return &InvalidCompatibilityIDError{Reason: "must not be empty"}
	case len(id) > MaxCompatibilityIDBytes:
		return &InvalidCompatibilityIDError{Reason: "must be at most " + strconv.Itoa(MaxCompatibilityIDBytes) + " bytes"}
	case !utf8.ValidString(string(id)):
		return &InvalidCompatibilityIDError{Reason: "must be valid UTF-8"}
	default:
		return nil
	}
}

// CaptureSafety describes how a target's highest-output tool returns its
// output, which is what decides whether the target may share a Host.
type CaptureSafety string

const (
	// CaptureSafetyUnknown NAMES the unknown case for a target that wants to
	// say so explicitly. It is NOT the zero value of CaptureSafety — that is
	// the empty string — and do not branch on it to find an undescribed
	// target. A Capabilities nobody filled in carries "", not this, and
	// `if c.CaptureSafety == CaptureSafetyUnknown` walks straight past exactly
	// the case it looks like it covers.
	//
	// Ask PoolingPermitted instead. It lists the SAFE values and defaults
	// everything else to unsafe, so the empty string, this constant and any
	// value a future Core adds are all refused without anyone editing a branch.
	CaptureSafetyUnknown CaptureSafety = "unknown"

	// CaptureSafetyStreaming reports that high-output tools stream their
	// output, so peak memory does not scale with output size.
	CaptureSafetyStreaming CaptureSafety = "streaming"

	// CaptureSafetyBoundedMaterialized reports that output is materialized in
	// memory but under a limit the target enforces.
	CaptureSafetyBoundedMaterialized CaptureSafety = "bounded_materialized"

	// CaptureSafetyUnboundedMaterialized reports that output is materialized in
	// memory with no enforced limit.
	CaptureSafetyUnboundedMaterialized CaptureSafety = "unbounded_materialized"
)

// Capabilities is what a target declares about how it may be placed and what it
// needs. It is a value: a target returns a copy and no caller can reach into
// one target's declaration through another.
type Capabilities struct {
	// SupportsPooled and SupportsDedicated declare the admission models the
	// target can run under. At least one must be true.
	SupportsPooled    bool
	SupportsDedicated bool

	// RequiresWorkspace declares that a launch needs a materialized workspace.
	RequiresWorkspace bool

	// RequiresCheckpoint declares that a restore needs a checkpoint to exist.
	RequiresCheckpoint bool

	// AdmissionWeight is this target's cost against a Host's capacity, in the
	// same units the capacity report uses. It must be at least one: a target
	// that costs nothing admits without bound.
	AdmissionWeight uint64

	// CaptureSafety describes the target's highest-output tool.
	CaptureSafety CaptureSafety
}

// Validate holds every rule about a Capabilities value, in one place, so the
// registry and the rig-target constructor cannot disagree about what a legal
// declaration is.
//
// It was extracted in O1.2, which is a REFACTOR OF O1.1's code from a later
// task. The trigger was real — NewRigTarget needed the same rules and restating
// them would have been the two-site shape this file has spent several rounds
// removing — but the consequence crossed a task boundary and is recorded here
// rather than left to be discovered: the rules gained a typed cause,
// *InvalidCapabilitiesError, and O1.1's assertion that a capability rule
// unwraps to nil moved to duplicate registration.
func (c Capabilities) Validate() error {
	if c.AdmissionWeight == 0 {
		return &InvalidCapabilitiesError{
			Code:   DefinitionErrorCodeZeroAdmissionWeight,
			Reason: "admission weight is zero, so this target would admit without bound",
		}
	}
	if !c.SupportsPooled && !c.SupportsDedicated {
		return &InvalidCapabilitiesError{
			Code:   DefinitionErrorCodeNoPlacement,
			Reason: "declares no placement, so it could never be launched",
		}
	}
	// The capture-safety rule is enforced by REJECTION rather than by silently
	// dropping the pooled claim. A silent narrowing produces exactly the same
	// green as a correct declaration, which is how a too-wide exclusion hides;
	// making the author write SupportsDedicated only is a visible diff.
	//
	// This is STRICTER THAN SPEC and the difference has consequences downstream
	// that O5 must know about. §11.3 says such a target "is dedicated-only" — a
	// narrowing — and §7's posture is degrade-visibly, so rejecting is a choice
	// the spec did not make. Both behaviours exist: PoolingPermitted still
	// narrows on a bare Capabilities. But because every construction path
	// refuses the combination, SupportsPooled == PoolingPermitted() holds for
	// every target inside a Department, which means THE NARROWING IS
	// UNREACHABLE THROUGH THE REGISTRY — a placement path reading
	// PoolingPermitted off a registered target can never observe it doing work.
	// The other consequence is operational: a target whose tools regress from
	// streaming to unbounded turns a Host that should have run it
	// dedicated-only into one that refuses to start.
	if c.SupportsPooled && !c.PoolingPermitted() {
		return &InvalidCapabilitiesError{
			Code: DefinitionErrorCodePooledUnsafeCapture,
			Reason: "declares pooled support with " + string(c.CaptureSafety) +
				" capture, which is dedicated-only until the target gains streaming capture",
		}
	}
	return nil
}

// PoolingPermitted reports whether the target may share a Host.
//
// An unknown or unbounded materialized high-output tool makes the target
// DEDICATED-ONLY until it gains streaming capture, regardless of what
// SupportsPooled declares: the declaration is about what the target can do, and
// this is about what its capture behaviour makes safe.
func (c Capabilities) PoolingPermitted() bool {
	if !c.SupportsPooled {
		return false
	}
	switch c.CaptureSafety {
	case CaptureSafetyStreaming, CaptureSafetyBoundedMaterialized:
		return true
	default:
		// The default arm is the half of this rule that matters, and it is
		// wider than "CaptureSafetyUnknown". The zero value of a Capabilities
		// is CaptureSafety(""), not CaptureSafetyUnknown — the constant is a
		// NAME for the unknown case, not the zero value of the type — so a
		// struct nobody filled in arrives here as the empty string. Both land
		// on the unsafe side, and so does any value a future Core or a typo
		// introduces. Listing the safe cases and defaulting to unsafe is what
		// makes that true; enumerating the unsafe ones would not.
		return false
	}
}

// PermittedPlacements returns the admission models this target may be placed
// under, in a deterministic sorted order.
func (c Capabilities) PermittedPlacements() []sessionwire.HostPlacement {
	var placements []sessionwire.HostPlacement
	if c.SupportsDedicated {
		placements = append(placements, sessionwire.HostPlacementDedicated)
	}
	if c.PoolingPermitted() {
		placements = append(placements, sessionwire.HostPlacementPooled)
	}
	// Sorted so the determinism is a property of the VALUE rather than of the
	// order these two ifs happen to be written in.
	//
	// This call is UNKILLABLE today and the honest reading is that it buys
	// nothing yet: "dedicated" sorts before "pooled", so the sort agrees with
	// the written order and removing it changes no answer any test can see. It
	// is kept because the alternative is an invariant that holds by accident of
	// two constants' spelling. TestPlacementConstantsStillCollateAsAssumed is
	// the tripwire that keeps that from being an untestable claim: if Core ever
	// renames either constant into a different collating position, it fires and
	// sends the next reader here.
	slices.Sort(placements)
	return placements
}

// LaunchTarget creates and restores runtimes for one agent.
type LaunchTarget interface {
	// CompatibilityID identifies the runtime build this target launches.
	CompatibilityID() CompatibilityID

	// Capabilities returns how this target may be placed and what it needs.
	Capabilities() Capabilities

	// Create launches a new runtime.
	Create(context.Context, CreateRequest) (Runtime, error)

	// Restore relaunches a runtime over existing durable state.
	Restore(context.Context, RestoreRequest) (Runtime, error)
}

// StorageContext scopes every durable object a launched session writes.
//
// It is a struct with one field rather than a bare string because the thing
// being passed is a CONTEXT, and the next thing it needs — a retention class, a
// provider hint — is a field here rather than a second parameter threaded
// through every signature between Host and the Rig.
type StorageContext struct {
	// Namespace is the durable prefix the session's objects live under. It is
	// opaque to this package and is not parsed.
	Namespace string
}

// CreateRequest is a Host-side launch request.
//
// It is NOT sessionwire.CreateRequest. That record is a client's durable
// command envelope — a wire type with a CommandID, a version and canonical JSON
// blocks — and a launch is neither a command nor a wire record: it is Host
// asking a target for a process, and the fields it needs (a workspace root, a
// resolved placement) have no place on the wire.
type CreateRequest struct {
	TenantID      sessionwire.TenantID
	SessionID     sessionwire.SessionID
	AgentID       sessionwire.AgentID
	Placement     sessionwire.HostPlacement
	WorkspaceRoot string
	Storage       StorageContext

	// RigSessionID is HARNESS'S identity to launch the new session under: the
	// runtime session id the session's immutable durable binding names, which
	// Factory derived at create and which is NOT the sessionwire SessionID. A
	// target must launch under exactly this id (harness: rig.WithSessionID),
	// because the binding is the only record of where the session's journal
	// lives; a session launched under a minted id writes a journal nothing
	// can find again. Host sends a create only after establishing that no
	// journal exists under it. Zero means the durable record named none — only
	// a record with no binding — and the target mints one.
	RigSessionID uuid.UUID
}

// RestoreRequest is a Host-side relaunch request over existing durable state.
type RestoreRequest struct {
	TenantID      sessionwire.TenantID
	SessionID     sessionwire.SessionID
	AgentID       sessionwire.AgentID
	Placement     sessionwire.HostPlacement
	WorkspaceRoot string
	Storage       StorageContext

	// CompatibilityID is the runtime build the durable state was written by. A
	// target whose own CompatibilityID differs must refuse the restore.
	CompatibilityID CompatibilityID

	// RigSessionID is HARNESS'S identity for the session being restored, which
	// Host recorded when it created the session and cannot derive: the two
	// identity spaces are independent, so a restore that does not carry this
	// has nothing to restore FROM. It is a field rather than something the
	// adapter reconstructs, because reconstructing it would mean deriving a
	// UUID from an opaque sessionwire string, which is the conflation this
	// package exists to prevent.
	RigSessionID uuid.UUID
}

// ---------------------------------------------------------------------------
// Runtime: segregated Harness session capabilities
// ---------------------------------------------------------------------------
//
// These are declared on HOST'S side rather than imported, because H4.1 has not
// landed and Host is not blocked on it. Each is the narrowest interface a Host
// caller actually needs, so a drain loop takes a Releaser and its fake
// implements one method.
//
// AN ADAPTER IS MANDATORY, whatever H4.1 ends up naming. Harness's Session
// identifies itself with core/uuid.UUID and these interfaces identify a runtime
// with the sessionwire identities Host and Factory exchange, which are opaque
// strings and not UUIDs by contract. No naming outcome makes a Harness session
// satisfy Identity directly, so O1.2 writes the adapter. An earlier version of
// this comment said a Harness session would "either satisfy these or the
// difference is a real disagreement worth seeing"; that was already false when
// it was written, and the whole point of stating an expectation in a comment is
// that it can be checked.
//
// The three lifecycle shapes below are H4.1's, taken verbatim from the program
// runbook rather than invented here. Declaring the same capability with a
// different shape would turn a mechanical adapter into a semantic one, and both
// differences would have cost something real: Done() is a broadcast a drain
// supervisor can select on where a poll is not, and ReleaseResidency carries
// the distinction H4.1 exists to protect — residency release is NONTERMINAL and
// is not Shutdown, which durably appends SessionStopped. A bare Release loses
// that at the Host boundary, which is the one place it most needs to survive.

// Identity reports the identities a runtime is bound to.
//
// It is NOT called Controller. It carries two getters and controls nothing;
// Host's control path is CommandApplier, which applies an admitted command.
// Naming a pure accessor after the thing it is adjacent to is how a type ends
// up with a promise its methods do not keep.
type Identity interface {
	SessionID() sessionwire.SessionID
	AgentID() sessionwire.AgentID
}

// IdleWaiter blocks until the runtime has no work in flight. H4.1's shape.
type IdleWaiter interface {
	WaitIdle(context.Context) error
}

// Liveness reports when the runtime has stopped answering. H4.1's shape: a
// channel closed once, which any number of drain supervisors can select on,
// rather than a poll each of them has to run.
type Liveness interface {
	Done() <-chan struct{}
}

// Releaser releases RESIDENCY, which is nonterminal: the session remains
// resumable and no SessionStopped is appended. H4.1's shape, including the
// name, because the name is the part that carries the distinction.
type Releaser interface {
	ReleaseResidency(context.Context) error
}

// PublicationSubscriber delivers committed public journal events after the
// given event, so a subscriber can join the live path with a bounded journal
// read. Only COMMITTED public events cross this seam.
type PublicationSubscriber interface {
	SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error)
}

// RuntimeCommand is one admitted command as Host hands it to a runtime.
//
// IT IS NOT sessionwire.CommandEnvelope, AND THAT WAS A DEFECT RATHER THAN A
// CHOICE. This seam took the envelope, which released Core defines as exactly a
// wire version and a public CommandID — so the two things a runtime needs in
// order to DO anything never crossed it. §16 requires Host to forward the
// RuntimeCommandID into the Harness API and H3.1 states the same contract from
// the other side, and the command's substance could not cross either: the inbox
// payload is private to Factory and Host, and SessionStore imports only Core and
// storage, so a runtime handed a bare CommandID has no way to obtain the blocks
// it is being asked to apply. An input command applied across that seam was a
// durable no-op.
//
// THE PAYLOAD MAY BE A REFERENCE INSTEAD OF BYTES, and Host does not
// dereference it. §10.1 gives a private body an independent immutable object
// reference once it exceeds its inline threshold, and at most one of the two is
// ever set; the runtime resolves the reference through its own object read
// rather than having the bytes copied through Host's memory.
//
// WHAT STILL DOES NOT CROSS is anything Host decoded. Host is not the semantic
// validator of a command body — Harness is — so the bytes travel opaque.
type RuntimeCommand struct {
	// CommandID is the public, retry-stable identity.
	CommandID sessionwire.CommandID

	// RuntimeCommandID is the once-allocated mapping from the winning
	// acceptance record. It is what correlates the runtime's own durable effect
	// with the command that asked for it, which is why §16 forwards it rather
	// than letting each side derive one.
	RuntimeCommandID uuid.UUID

	// Kind is the command kind the acceptance record carries, opaque here.
	Kind string

	// Payload and PayloadRef are the private body. At most one is set, and
	// neither is set for a command that has none.
	Payload    []byte
	PayloadRef sessionwire.ObjectReference

	// AttemptID is the immutable identity of the ONE dispatch the durable
	// record authorized, and it is what makes this dispatch settleable.
	//
	// IT IS THE STORE'S AND IS NEVER MINTED ON THIS SIDE OF THE SEAM.
	// BeginDispositionAttempt writes it immutably before any dispatch happens,
	// so a runtime's durable disposition can name the attempt it is about and a
	// settler can verify the two agree. A value invented here would name an
	// attempt no evidence could ever be about.
	//
	// IT IS A STRING RATHER THAN A TYPED IDENTITY for the reason Kind is: this
	// seam is Host's control path into ANY runtime, and typing it would put
	// either sessionstore's or harness's vocabulary in department's own. It
	// carries harness's grammar — bounded opaque UTF-8 — because both modules
	// bound it the same way, and the adapter validates rather than assumes.
	//
	// EMPTY IS A LEGITIMATE VALUE AND MEANS NO ATTEMPT WAS AUTHORIZED. harness
	// writes no disposition frame for one, by design, leaving a legacy
	// journal's bytes unchanged. A Host driving the disposition family always
	// sets it; a caller that does not is asking for the legacy behaviour and
	// gets it.
	AttemptID string
}

// CommandApplier applies one admitted runtime command. This is Host's control
// path into a runtime.
type CommandApplier interface {
	ApplyCommand(context.Context, RuntimeCommand) error
}

// ErrDispositionUnsupported reports a dispatch refused because the runtime's
// durable log cannot record a command disposition.
//
// IT IS DECLARED HERE, IN THE SEAM, BECAUSE BOTH SIDES MUST NAME IT. The
// adapter raises it and the applier acts on it, and neither may import the
// other; a refusal recognisable only by its message text is a refusal a caller
// cannot distinguish from a transport failure, which is precisely the mistake
// harness exported its own error type to stop.
//
// NOTHING DURABLE WAS WRITTEN, so the command MAY be re-offered — which a
// transport failure does not license, because a transport failure says the
// opposite: something may have happened.
var ErrDispositionUnsupported = errors.New("department: this runtime's durable log cannot record a command disposition, so nothing was dispatched and nothing durable was written")

// ErrEnduringEffect reports a recovery closure refused because the runtime's
// journal holds an enduring event caused by the command being closed.
//
// IT IS TERMINAL FOR THE CLOSURE AND IS NEVER RETRYABLE. The predecessor's
// effect committed and only its evidence is missing, so a tombstone over it is
// the one error this protocol cannot recover from. A caller that retried this
// into a closure would destroy a real effect.
var ErrEnduringEffect = errors.New("department: the runtime's journal holds an enduring effect for this command, so its attempt may not be closed")

// ErrNoAttemptCloser reports a runtime that offers no recovery closure.
//
// IT EXISTS BECAUSE A WRAPPER CANNOT BE ABSENT. rigRuntime forwards an optional
// capability, and a Go wrapper that declares the method satisfies the interface
// for every runtime it wraps — so a caller's type assertion stops being able to
// say "this one has none". The distinction moves from the assertion to this
// sentinel, and a caller that needs it must read the error rather than the
// assertion.
//
// A BLOCKED PASS IS THE CORRECT RESPONSE, never a conclusion: a composition that
// cannot close a predecessor's stranded attempt must leave the command where it
// is, not decide its fate without the evidence a closure would have produced.
var ErrNoAttemptCloser = errors.New("department: this runtime offers no recovery closure, so a predecessor's stranded attempt cannot be closed")

// AttemptCloser writes the recovery closure for an attempt a PREVIOUS runtime
// never finished, under this runtime's own strictly later journal grant.
//
// IT IS DISCOVERED BY ASSERTION AND IS DELIBERATELY NOT PART OF Runtime, which
// is the shape harness's runtimecommand.AttemptCloser has for the same reason:
// folding a recovery-only capability into the ordinary control path would make
// every implementer of that path — the composed adapter, every test double —
// carry a method only a successor ever calls. A composition whose runtime does
// not satisfy it blocks on a stranded attempt rather than concluding anything.
type AttemptCloser interface {
	// CloseAttempt durably records that the named attempt was not applied.
	//
	// AttemptJournalEpoch is the grant the ATTEMPT was authorized under, read
	// from the durable record. The author grant is NOT a parameter: the closer
	// stamps that from the live lease it holds, because a caller-supplied author
	// epoch would be a caller-authored proof, and a tombstone is the one thing
	// that must never be one.
	CloseAttempt(ctx context.Context, command sessionwire.CommandID, runtimeCommand uuid.UUID, kind string, attempt string, attemptJournalEpoch uint64) error
}

// LeaseEpochReporter reports the JOURNAL single-writer lease epoch the RUNTIME
// holds. It is harness v0.33.0's session.LeaseEpochReporter shape, verbatim, for
// the reason every other shape in this block is verbatim: a capability declared
// here with a different signature turns a mechanical adapter into a semantic one.
//
// IT IS NOT HOST'S GRANT AND MUST NEVER BE SOURCED FROM ONE. A deployment has two
// monotonic per-session counters — Host's residency grant and the runtime's
// journal grant — issued by different holders over different namespaces.
// sessionstore.ResidencyEpoch says in terms that it "must never be compared with,
// or used as, a journal epoch", and harness checks an admitted command's
// LeaseEpoch for EQUALITY against the lease the runtime itself holds. A Host that
// answered this from its own lease would produce work that check rejects, or —
// worse, in disposition mode — an attempt no evidence can ever match. That the
// two numbers often agree early in a session's life is an accident of two fresh
// counters both starting at 1, not a relationship.
//
// THE TWO RESULTS ARE THE CONTRACT. held reports whether the runtime holds a
// lease that reports an epoch AT ALL; the epoch is meaningful only when held is
// true and is zero otherwise. A caller must branch on held: no pinned provider
// zeroes a released lease's epoch, so reading the number alone hands back a
// live-looking dead value once the lease is gone.
type LeaseEpochReporter interface {
	LeaseEpoch() (epoch uint64, held bool)
}

// Runtime is a live agent runtime, expressed as the composition of the narrow
// capabilities Host consumes. It is deliberately a composition and not one wide
// interface: nothing here should have to fake six methods to test one.
//
// LeaseEpochReporter IS REQUIRED RATHER THAN DISCOVERED, and that is a deviation
// from the sentence on harness's own capability — "a caller MUST treat a false ok
// as 'this session does not report a lease epoch', not as an error" — so it is
// argued rather than assumed. That sentence is about the ASSERTION; the
// conditional half of the capability is the `held` result, which Host does branch
// on and never treats as an error. harness pins `var _ session.LeaseEpochReporter
// = (*Session)(nil)` and states there is no configuration under which its Session
// lacks the METHOD, so requiring it refuses nothing harness produces. What it does
// refuse is a WRAPPER that fails to forward it — the hazard harness's own doc
// names, where a wrapped live session is silently opted out and every later epoch
// read answers a plausible zero. Host already requires Releaser on the same
// argument, so this is the established rule here and not one invented for it.
type Runtime interface {
	Identity
	IdleWaiter
	Liveness
	Releaser
	PublicationSubscriber
	CommandApplier
	LeaseEpochReporter
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// DefinitionErrorCode is a stable, machine-readable reason a definition could
// not become a Department.
//
// It exists because Reason is free text and two rules had already collided in
// it: the empty Department said "registers no launch target" and a nil target
// said "no launch target", so a test matching the substring passed for both
// inputs — it was green for the wrong reason and would have stayed green if the
// nil-target rule were deleted. A caller branching on a rule needs something
// that cannot be made ambiguous by rewording a sentence.
type DefinitionErrorCode string

const (
	// DefinitionErrorCodeNoRegistrations reports a Department with no
	// registrations at all.
	DefinitionErrorCodeNoRegistrations DefinitionErrorCode = "no_registrations"

	// DefinitionErrorCodeDuplicateAgent reports an agent registered more than
	// once.
	DefinitionErrorCodeDuplicateAgent DefinitionErrorCode = "duplicate_agent"

	// DefinitionErrorCodeInvalidAgentID reports an agent identity sessionwire
	// would refuse. The cause is the *sessionwire.IDValidationError.
	DefinitionErrorCodeInvalidAgentID DefinitionErrorCode = "invalid_agent_id"

	// DefinitionErrorCodeNoTarget reports a registration with a nil target.
	DefinitionErrorCodeNoTarget DefinitionErrorCode = "no_target"

	// DefinitionErrorCodeInvalidCompatibilityID reports a runtime identity Host
	// may not write to the wire. The cause is the
	// *InvalidCompatibilityIDError.
	DefinitionErrorCodeInvalidCompatibilityID DefinitionErrorCode = "invalid_compatibility_id"

	// DefinitionErrorCodeZeroAdmissionWeight reports a target that would admit
	// without bound.
	DefinitionErrorCodeZeroAdmissionWeight DefinitionErrorCode = "zero_admission_weight"

	// DefinitionErrorCodeNoPlacement reports a target that declares neither
	// pooled nor dedicated support.
	DefinitionErrorCodeNoPlacement DefinitionErrorCode = "no_placement"

	// DefinitionErrorCodePooledUnsafeCapture reports a target declaring pooled
	// support whose capture behaviour makes it dedicated-only.
	DefinitionErrorCodePooledUnsafeCapture DefinitionErrorCode = "pooled_unsafe_capture"
)

// InvalidDepartmentError reports a definition that could not become a
// Department. AgentID names the registration at fault and is empty when the
// fault is the Department as a whole.
//
// Code is what a caller branches on. Reason is for a human and may be reworded
// freely. Cause carries the typed error a lower layer produced — Core's
// *sessionwire.IDValidationError, or this package's
// *InvalidCompatibilityIDError — so errors.As reaches it instead of a caller
// having to substring-match a sentence this type has already changed twice.
type InvalidDepartmentError struct {
	Code    DefinitionErrorCode
	AgentID sessionwire.AgentID
	Reason  string
	Cause   error
}

// Unwrap returns the typed error this one was built from, if any.
func (e *InvalidDepartmentError) Unwrap() error { return e.Cause }

func (e *InvalidDepartmentError) Error() string {
	if e.AgentID == "" {
		return "department: invalid definition: " + e.Reason
	}
	return "department: invalid registration for agent " + strconv.Quote(string(e.AgentID)) + ": " + e.Reason
}

// InvalidCapabilitiesError reports a Capabilities value no target may declare.
//
// It exists so the rules have ONE site. department.New validates a registered
// target's capabilities and NewRigTarget validates a target it is about to
// build; a rule applied in one and not the other is not a rule, and this file
// has spent several rounds removing exactly that shape. Code is the same
// DefinitionErrorCode the registry reports, so the two paths are not merely
// consistent but observably identical.
type InvalidCapabilitiesError struct {
	Code   DefinitionErrorCode
	Reason string
}

func (e *InvalidCapabilitiesError) Error() string {
	return "department: invalid capabilities: " + e.Reason
}

// InvalidCompatibilityIDError reports a runtime identity Host may not write to
// the wire field it is destined for.
type InvalidCompatibilityIDError struct {
	Reason string
}

func (e *InvalidCompatibilityIDError) Error() string {
	return "department: invalid compatibility id: " + e.Reason
}

// UnknownAgentError reports a lookup for an agent this Department does not
// register.
//
// It carries sessionwire.ErrorCodeRuntimeUnavailable rather than an
// invalid-request code, and the distinction is the point: the request was well
// formed, and this Host simply has no runtime for that agent. A client branches
// on the code, and telling it its request was malformed sends it to fix
// something that is not broken.
type UnknownAgentError struct {
	AgentID sessionwire.AgentID
}

func (e *UnknownAgentError) Error() string {
	return "department: no launch target registered for agent " + strconv.Quote(string(e.AgentID))
}

// ErrorCode returns the stable public code a client branches on.
func (e *UnknownAgentError) ErrorCode() sessionwire.ErrorCode {
	return sessionwire.ErrorCodeRuntimeUnavailable
}
