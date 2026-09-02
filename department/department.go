package department

import (
	"maps"
	"slices"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Registration binds one agent identity to the target that launches it.
//
// New takes a SLICE of these rather than a map, and the reason is that a map
// cannot carry the fault the constructor must reject: duplicate constant keys
// in a map literal are a compile error, and duplicate computed keys silently
// last-wins. A map-taking constructor would receive an already-deduplicated
// input and report a clean Department built from an ambiguous definition — it
// could never see the duplicate, so it could never reject it. The slice is the
// smallest input shape that can express the mistake.
type Registration struct {
	AgentID sessionwire.AgentID
	Target  LaunchTarget
}

// Department is the immutable set of launch targets a Host serves.
//
// The map is UNEXPORTED and the type is a struct rather than a named map type,
// which is the difference between an immutable registry and a mutable one with
// a comment saying not to write to it. A named map type has no way to refuse
// d[agent] = other, and no way to stop a caller ranging it and getting a
// different order every time.
//
// The immutability is of the REGISTRY, and that is the whole of the claim. New
// copies the registrations out of the caller's slice, so nothing the caller
// does to that slice afterwards reaches inside; it does not and cannot copy the
// TARGETS, which are interface values a caller may still hold and whose
// implementations may carry state. A LaunchTarget is a factory and has to be
// shared to be useful. What is fixed here is which agent maps to which target,
// how many there are, and the order they are listed in.
type Department struct {
	targets map[sessionwire.AgentID]LaunchTarget

	// agents is the sorted listing, computed once. AgentIDs hands out a copy of
	// it; recomputing per call would be the same answer, but storing it makes
	// "deterministic" a property of the value rather than of every caller
	// getting the sort right.
	agents []sessionwire.AgentID
}

// New validates every registration and returns an immutable Department.
func New(registrations []Registration) (*Department, error) {
	// NOT REQUIRED BY SPEC. §7 asks only for a finite mapping; rejecting the
	// empty one is this implementation's choice and it forbids a Host
	// configured later. Sound and cheap to reverse, recorded so the next reader
	// does not mistake it for a contract.
	if len(registrations) == 0 {
		return nil, &InvalidDepartmentError{
			Code:   DefinitionErrorCodeNoRegistrations,
			Reason: "a Department registers no launch target, so this Host can serve nothing",
		}
	}

	targets := make(map[sessionwire.AgentID]LaunchTarget, len(registrations))
	for _, registration := range registrations {
		if err := validateRegistration(registration); err != nil {
			return nil, err
		}
		if _, duplicate := targets[registration.AgentID]; duplicate {
			return nil, &InvalidDepartmentError{
				Code:    DefinitionErrorCodeDuplicateAgent,
				AgentID: registration.AgentID,
				Reason:  "registered more than once, so which target launches it depends on ordering",
			}
		}
		targets[registration.AgentID] = registration.Target
	}

	return &Department{targets: targets, agents: slices.Sorted(maps.Keys(targets))}, nil
}

// validateRegistration holds every rule one registration must satisfy. It is
// one function so New has one place to call and a new rule has one site.
func validateRegistration(registration Registration) error {
	if err := registration.AgentID.Validate(); err != nil {
		// Cause, not string concatenation. Core documents
		// IDValidationError.Code as stable precisely so a caller can tell
		// "too_long" from "invalid_utf8" without matching Error(); folding it
		// into a sentence here is what destroys that.
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodeInvalidAgentID,
			AgentID: registration.AgentID,
			Reason:  "agent identity is not a permitted sessionwire identity: " + err.Error(),
			Cause:   err,
		}
	}
	if registration.Target == nil {
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodeNoTarget,
			AgentID: registration.AgentID,
			Reason:  "no launch target",
		}
	}
	if err := registration.Target.CompatibilityID().Validate(); err != nil {
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodeInvalidCompatibilityID,
			AgentID: registration.AgentID,
			Reason:  "compatibility id is unusable: " + err.Error(),
			Cause:   err,
		}
	}

	capabilities := registration.Target.Capabilities()
	// Also not required by spec, and also kept: a target that costs nothing
	// against capacity admits without bound, and zero is the zero value rather
	// than a deliberate declaration.
	if capabilities.AdmissionWeight == 0 {
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodeZeroAdmissionWeight,
			AgentID: registration.AgentID,
			Reason:  "admission weight is zero, so this target would admit without bound",
		}
	}
	if !capabilities.SupportsPooled && !capabilities.SupportsDedicated {
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodeNoPlacement,
			AgentID: registration.AgentID,
			Reason:  "declares no placement, so it could never be launched",
		}
	}
	// The capture-safety rule is enforced by REJECTION rather than by silently
	// dropping the pooled claim. A silent narrowing produces exactly the same
	// green as a correct declaration, which is how a too-wide exclusion hides;
	// making the author write SupportsDedicated only is a visible diff.
	//
	// This is STRICTER THAN SPEC and the difference has consequences downstream
	// that O1.2 and O5 must know about. §11.3 says such a target "is
	// dedicated-only" — a narrowing — and §7's posture is degrade-visibly, so
	// rejecting is a choice the spec did not make. Both behaviours exist:
	// PoolingPermitted still narrows on a bare Capabilities. But because New
	// refuses the combination, SupportsPooled == PoolingPermitted() holds for
	// every target inside a constructed Department, which means THE NARROWING
	// IS UNREACHABLE THROUGH THE REGISTRY — a placement path that reads
	// PoolingPermitted off a registered target can never observe it doing
	// work, and a test written against the registry cannot exercise it.
	// The other consequence is operational: a target whose tools regress from
	// streaming to unbounded turns a Host that should have run it
	// dedicated-only into one that refuses to start. Fail-loud is defensible
	// and deliberate; it is not what §11.3 asks for.
	if capabilities.SupportsPooled && !capabilities.PoolingPermitted() {
		return &InvalidDepartmentError{
			Code:    DefinitionErrorCodePooledUnsafeCapture,
			AgentID: registration.AgentID,
			Reason: "declares pooled support with " + string(capabilities.CaptureSafety) +
				" capture, which is dedicated-only until the target gains streaming capture",
		}
	}
	return nil
}

// Target returns the launch target registered for agent, or *UnknownAgentError
// carrying sessionwire.ErrorCodeRuntimeUnavailable if this Department does not
// register one.
func (d *Department) Target(agent sessionwire.AgentID) (LaunchTarget, error) {
	target, ok := d.targets[agent]
	if !ok {
		return nil, &UnknownAgentError{AgentID: agent}
	}
	return target, nil
}

// AgentIDs returns every registered agent identity in ascending order.
func (d *Department) AgentIDs() []sessionwire.AgentID {
	// A copy, because returning d.agents hands the caller a slice whose backing
	// array is the Department's own listing: one append within capacity or one
	// index write and the registry's view of itself has changed.
	return slices.Clone(d.agents)
}

// Len reports how many agents this Department registers.
func (d *Department) Len() int { return len(d.targets) }
