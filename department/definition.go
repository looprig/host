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
		// CaptureSafetyUnknown is the zero value and lands here, which is the
		// half of this rule that matters: a Capabilities nobody filled in is
		// dedicated-only rather than accidentally poolable.
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
	// Sorted rather than appended in a fixed order, so the determinism is a
	// property of the value and not of the order these two ifs happen to be
	// written in. Reordering the ifs must not change the answer.
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
// implements one method. When H4.1 lands, a Harness session either satisfies
// these or the difference is a real disagreement worth seeing.

// Controller reports the identities a runtime is bound to.
type Controller interface {
	SessionID() sessionwire.SessionID
	AgentID() sessionwire.AgentID
}

// IdleWaiter blocks until the runtime has no work in flight.
type IdleWaiter interface {
	WaitIdle(context.Context) error
}

// Liveness reports whether the runtime is still answering.
type Liveness interface {
	Alive(context.Context) error
}

// Releaser releases the runtime's resources. It is the drain path.
type Releaser interface {
	Release(context.Context) error
}

// PublicationSubscriber delivers committed public journal events after the
// given event, so a subscriber can join the live path with a bounded journal
// read. Only COMMITTED public events cross this seam.
type PublicationSubscriber interface {
	SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error)
}

// CommandApplier applies one admitted runtime command.
type CommandApplier interface {
	ApplyCommand(context.Context, sessionwire.CommandEnvelope) error
}

// Runtime is a live agent runtime, expressed as the composition of the narrow
// capabilities Host consumes. It is deliberately a composition and not one wide
// interface: nothing here should have to fake six methods to test one.
type Runtime interface {
	Controller
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
