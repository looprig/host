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
	"slices"
	"strconv"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
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
	// CaptureSafetyUnknown is the zero value and the safe default: a target
	// that has not described its capture behaviour is treated as if it could
	// materialize without bound.
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
	// Do not read more into this than it does today: "dedicated" sorts before
	// "pooled", so the sort currently agrees with the written order and
	// reordering the ifs would not change the answer. What the sort buys is
	// that it would keep agreeing if Core renamed either constant into a
	// different collating position, and the tests pin the VALUES rather than
	// the positions, so such a rename is caught either way.
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
}

// RestoreRequest is a Host-side relaunch request over existing durable state.
type RestoreRequest struct {
	TenantID      sessionwire.TenantID
	SessionID     sessionwire.SessionID
	AgentID       sessionwire.AgentID
	Placement     sessionwire.HostPlacement
	WorkspaceRoot string

	// CompatibilityID is the runtime build the durable state was written by. A
	// target whose own CompatibilityID differs must refuse the restore.
	CompatibilityID CompatibilityID
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

// CommandApplier applies one admitted runtime command. This is Host's control
// path into a runtime.
type CommandApplier interface {
	ApplyCommand(context.Context, sessionwire.CommandEnvelope) error
}

// Runtime is a live agent runtime, expressed as the composition of the narrow
// capabilities Host consumes. It is deliberately a composition and not one wide
// interface: nothing here should have to fake six methods to test one.
type Runtime interface {
	Identity
	IdleWaiter
	Liveness
	Releaser
	PublicationSubscriber
	CommandApplier
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// InvalidDepartmentError reports a definition that could not become a
// Department. AgentID names the registration at fault and is empty when the
// fault is the Department as a whole.
type InvalidDepartmentError struct {
	AgentID sessionwire.AgentID
	Reason  string
}

func (e *InvalidDepartmentError) Error() string {
	if e.AgentID == "" {
		return "department: invalid definition: " + e.Reason
	}
	return "department: invalid registration for agent " + strconv.Quote(string(e.AgentID)) + ": " + e.Reason
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
