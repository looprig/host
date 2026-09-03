package department_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// stubTarget is a LaunchTarget that records nothing and launches nothing. The
// Department is a REGISTRY: every assertion here is about what it accepts, what
// it hands back, and what it refuses to share, so a target that did real work
// would only add ways for a test to pass for the wrong reason.
type stubTarget struct {
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
}

func (t stubTarget) CompatibilityID() department.CompatibilityID { return t.compatibility }
func (t stubTarget) Capabilities() department.Capabilities       { return t.capabilities }

func (t stubTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

func (t stubTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

// pooledCapabilities is the ordinary shape: streaming capture, so pooling is
// permitted, and both placements declared.
func pooledCapabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:     true,
		SupportsDedicated:  true,
		RequiresWorkspace:  true,
		RequiresCheckpoint: false,
		AdmissionWeight:    1,
		CaptureSafety:      department.CaptureSafetyStreaming,
	}
}

// target returns a stub launch target declaring the given runtime build.
func target(compatibility string) stubTarget {
	return stubTarget{compatibility: department.CompatibilityID(compatibility), capabilities: pooledCapabilities()}
}

// registration binds an agent identity to a stub target for the given build.
func registration(agent, compatibility string) department.Registration {
	return department.Registration{AgentID: sessionwire.AgentID(agent), Target: target(compatibility)}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRejectsAnEmptyDepartment(t *testing.T) {
	t.Parallel()

	_, err := department.New(nil)
	var invalid *department.InvalidDepartmentError
	if !errors.As(err, &invalid) {
		t.Fatalf("New(nil) error = %v, want *InvalidDepartmentError", err)
	}
	if invalid.AgentID != "" {
		t.Errorf("AgentID = %q, want empty: the fault is the Department, not one agent", invalid.AgentID)
	}
	// The CODE, not the Reason. This assertion used to match the substring
	// "no launch target", which the NIL-TARGET message also contains — so it
	// passed for a different input than the one it names and would have stayed
	// green with this rule deleted.
	if invalid.Code != department.DefinitionErrorCodeNoRegistrations {
		t.Errorf("Code = %q, want %q", invalid.Code, department.DefinitionErrorCodeNoRegistrations)
	}
	if _, err := department.New([]department.Registration{}); !errors.As(err, &invalid) {
		t.Errorf("New(empty slice) error = %v, want *InvalidDepartmentError", err)
	}
}

// TestNewRejectsADuplicateAgentID is why the constructor takes a SLICE.
//
// A map literal cannot express this input: duplicate constant keys are a
// compile error and duplicate computed keys silently last-wins, so a
// map-taking constructor could never see the duplicate it is supposed to
// reject — it would receive an already-deduplicated map and report a clean
// Department built from an ambiguous definition. The slice is the smallest
// input that can CARRY the fault.
func TestNewRejectsADuplicateAgentID(t *testing.T) {
	t.Parallel()

	_, err := department.New([]department.Registration{
		registration("reviewer", "runtime-a"),
		registration("planner", "runtime-a"),
		registration("reviewer", "runtime-b"),
	})
	var invalid *department.InvalidDepartmentError
	if !errors.As(err, &invalid) {
		t.Fatalf("New with a duplicate AgentID error = %v, want *InvalidDepartmentError", err)
	}
	if invalid.AgentID != "reviewer" {
		t.Errorf("AgentID = %q, want \"reviewer\"", invalid.AgentID)
	}
	if invalid.Code != department.DefinitionErrorCodeDuplicateAgent {
		t.Errorf("Code = %q, want %q", invalid.Code, department.DefinitionErrorCodeDuplicateAgent)
	}
}

func TestNewRejectsAnInvalidRegistration(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnknown

	noPlacement := pooledCapabilities()
	noPlacement.SupportsPooled = false
	noPlacement.SupportsDedicated = false

	zeroWeight := pooledCapabilities()
	zeroWeight.AdmissionWeight = 0

	tests := []struct {
		name         string
		registration department.Registration
		agent        sessionwire.AgentID
		code         department.DefinitionErrorCode
	}{
		{
			name:         "empty agent id",
			registration: department.Registration{AgentID: "", Target: target("runtime-a")},
			agent:        "",
			code:         department.DefinitionErrorCodeInvalidAgentID,
		},
		{
			name:         "nil target",
			registration: department.Registration{AgentID: "reviewer", Target: nil},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeNoTarget,
		},
		{
			name:         "empty compatibility id",
			registration: department.Registration{AgentID: "reviewer", Target: target("")},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeInvalidCompatibilityID,
		},
		{
			name:         "over-long compatibility id",
			registration: department.Registration{AgentID: "reviewer", Target: target(strings.Repeat("x", sessionwire.MaxIDBytes+1))},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeInvalidCompatibilityID,
		},
		{
			// The UTF-8 arm had no row at all, so deleting it from Validate
			// left the suite green while the length arm's control died.
			name:         "compatibility id that is not valid UTF-8",
			registration: department.Registration{AgentID: "reviewer", Target: target("runtime-\xff\xfe")},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeInvalidCompatibilityID,
		},
		{
			name:         "pooled with unsafe capture",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: unsafe}},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodePooledUnsafeCapture,
		},
		{
			// A target that costs nothing against capacity admits without
			// bound, which is a capacity guard that reports success while
			// enforcing nothing. Zero is also the ZERO VALUE, so this is the
			// forgotten-field case rather than a deliberate one.
			name:         "zero admission weight",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: zeroWeight}},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeZeroAdmissionWeight,
		},
		{
			name:         "no placement at all",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: noPlacement}},
			agent:        "reviewer",
			code:         department.DefinitionErrorCodeNoPlacement,
		},
	}

	if len(tests) == 0 {
		t.Fatal("no registration rule is exercised")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := department.New([]department.Registration{tt.registration})
			var invalid *department.InvalidDepartmentError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidDepartmentError", err)
			}
			if invalid.AgentID != tt.agent {
				t.Errorf("AgentID = %q, want %q", invalid.AgentID, tt.agent)
			}
			if invalid.Code != tt.code {
				t.Errorf("Code = %q, want %q", invalid.Code, tt.code)
			}
			if invalid.Reason == "" {
				t.Error("Reason is empty; the code is for branching and the reason is for the person reading the failure")
			}
		})
	}
}

// TestNewPreservesTypedCauses is the repository's own rule applied here:
// return typed errors and preserve causes with Unwrap. Folding a cause into a
// sentence destroys it — Core documents IDValidationError.Code as "stable for
// callers that need to distinguish validation failures without matching Error",
// and a caller that has to substring-match "too_long" is doing exactly what
// that sentence exists to prevent.
//
// The compatibility-id arm is the sharper case: *InvalidCompatibilityIDError is
// EXPORTED and New is the only production path that constructs one, so without
// Unwrap it was an exported type no consumer could ever match.
func TestNewPreservesTypedCauses(t *testing.T) {
	t.Parallel()

	t.Run("an agent identity carries Core's validation code", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			agent sessionwire.AgentID
			code  sessionwire.IDValidationCode
		}{
			{name: "empty", agent: "", code: sessionwire.IDValidationCodeEmpty},
			{name: "too long", agent: sessionwire.AgentID(strings.Repeat("a", sessionwire.MaxIDBytes+1)), code: sessionwire.IDValidationCodeTooLong},
			{name: "invalid utf8", agent: sessionwire.AgentID("\xff\xfe"), code: sessionwire.IDValidationCodeInvalidUTF8},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := department.New([]department.Registration{{AgentID: tt.agent, Target: target("runtime-a")}})
				var validation *sessionwire.IDValidationError
				if !errors.As(err, &validation) {
					t.Fatalf("New error = %v; errors.As did not reach *sessionwire.IDValidationError", err)
				}
				if validation.Code != tt.code {
					t.Errorf("IDValidationError.Code = %q, want %q", validation.Code, tt.code)
				}
			})
		}
	})

	t.Run("a compatibility id carries this package's typed cause", func(t *testing.T) {
		t.Parallel()
		_, err := department.New([]department.Registration{{AgentID: "reviewer", Target: target("")}})
		var invalidID *department.InvalidCompatibilityIDError
		if !errors.As(err, &invalidID) {
			t.Fatalf("New error = %v; errors.As did not reach *department.InvalidCompatibilityIDError, which no consumer could then match", err)
		}
		if invalidID.Reason == "" {
			t.Error("InvalidCompatibilityIDError.Reason is empty")
		}
	})

	t.Run("a rule with no lower-layer cause unwraps to nil", func(t *testing.T) {
		t.Parallel()
		// Unwrap must not invent a cause. A rule this package decides ON ITS
		// OWN has nothing underneath it, and saying otherwise would make
		// errors.Is match by accident.
		//
		// The rule chosen here is duplicate registration, and the choice
		// matters: the CAPABILITY rules used to qualify and no longer do,
		// because they moved into Capabilities.Validate so the registry and
		// NewRigTarget could not disagree about them. That extraction gave them
		// a real typed cause, *InvalidCapabilitiesError, which
		// TestNewRigTargetSharesTheDepartmentsCapabilityRules now relies on. A
		// rule with a genuine cause is the wrong subject for an assertion that
		// there is none.
		_, err := department.New([]department.Registration{
			registration("reviewer", "runtime-a"),
			registration("reviewer", "runtime-b"),
		})
		var invalid *department.InvalidDepartmentError
		if !errors.As(err, &invalid) {
			t.Fatalf("New error = %v, want *InvalidDepartmentError", err)
		}
		if unwrapped := errors.Unwrap(invalid); unwrapped != nil {
			t.Errorf("Unwrap() = %v, want nil", unwrapped)
		}
	})
}

// TestDefinitionErrorCodesAreDistinct is what makes the codes worth having. Two
// rules sharing a code is the same defect as two rules sharing a Reason
// substring, which is the defect that produced this type: a test naming one
// rule passes for the other, and deleting a rule stays green.
func TestDefinitionErrorCodesAreDistinct(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnknown
	noPlacement := pooledCapabilities()
	noPlacement.SupportsPooled, noPlacement.SupportsDedicated = false, false
	zeroWeight := pooledCapabilities()
	zeroWeight.AdmissionWeight = 0

	definitions := [][]department.Registration{
		nil,
		{registration("reviewer", "runtime-a"), registration("reviewer", "runtime-b")},
		{{AgentID: "", Target: target("runtime-a")}},
		{{AgentID: "reviewer", Target: nil}},
		{{AgentID: "reviewer", Target: target("")}},
		{{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: zeroWeight}}},
		{{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: noPlacement}}},
		{{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: unsafe}}},
	}

	seen := map[department.DefinitionErrorCode]int{}
	for i, definition := range definitions {
		_, err := department.New(definition)
		var invalid *department.InvalidDepartmentError
		if !errors.As(err, &invalid) {
			t.Fatalf("definition %d: New error = %v, want *InvalidDepartmentError", i, err)
		}
		if invalid.Code == "" {
			t.Errorf("definition %d: Code is empty", i)
		}
		if first, duplicate := seen[invalid.Code]; duplicate {
			t.Errorf("definitions %d and %d both report code %q; a code shared by two rules cannot tell them apart", first, i, invalid.Code)
		}
		seen[invalid.Code] = i
	}
	// Floored: one rejected definition per rule, so deleting a row is not a
	// silent green.
	if len(seen) != len(definitions) {
		t.Errorf("saw %d distinct codes over %d definitions", len(seen), len(definitions))
	}
}

// TestCaptureSafetyMakesATargetDedicatedOnly holds the Harness capture-safety
// rule, in both directions. An unknown or unbounded materialized high-output
// tool cannot be pooled until the target gains streaming capture, and the
// constructor REJECTS a target that declares pooling anyway rather than
// silently narrowing it. A silent narrowing produces exactly the same green as
// a correct declaration, which is the standing hazard of this repository.
func TestCaptureSafetyMakesATargetDedicatedOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		safety     department.CaptureSafety
		wantPooled bool
	}{
		// The ZERO VALUE of the field, which is CaptureSafety("") and NOT
		// CaptureSafetyUnknown — the constant is a name for the unknown case,
		// not the type's zero. A Capabilities nobody filled in arrives with the
		// empty string, so this row is the forgotten-field case and it is the
		// one the default arm exists for. Without it the guarantee is a comment:
		// a mutant adding `case CaptureSafety(""): return true` survived the
		// whole suite while every named-constant row still passed.
		{name: "zero value", safety: "", wantPooled: false},
		{name: "streaming", safety: department.CaptureSafetyStreaming, wantPooled: true},
		{name: "bounded materialized", safety: department.CaptureSafetyBoundedMaterialized, wantPooled: true},
		{name: "unbounded materialized", safety: department.CaptureSafetyUnboundedMaterialized, wantPooled: false},
		{name: "unknown", safety: department.CaptureSafetyUnknown, wantPooled: false},
		// A value from a future Core, or a typo. The default arm must refuse
		// anything it was not told is safe; enumerating the unsafe ones would
		// let this through.
		{name: "unrecognised", safety: "some_future_capture_mode", wantPooled: false},
	}

	// The two rows that carry the default arm's guarantee are asserted BY NAME,
	// not by counting. A count cannot know which rows matter: deleting the
	// zero-value row and editing 6 to 5 in the same commit passed, which is a
	// diff nobody would look at twice. Deleting it now leaves nothing to edit
	// except the assertion that says what was lost, which is a visibly
	// different thing to do in review.
	//
	// A weak guard is not kept beside a strong one — the row count is gone,
	// because a weak check invites satisfying the weak check.
	covered := map[department.CaptureSafety]bool{}
	for _, tt := range tests {
		covered[tt.safety] = true
	}
	if !covered[""] {
		t.Fatal("no zero-value row: the default arm's guarantee that an UNFILLED Capabilities is unsafe is untested, and a mutant treating the empty string as poolable survived this table before that row existed")
	}
	declared := []department.CaptureSafety{
		department.CaptureSafetyUnknown,
		department.CaptureSafetyStreaming,
		department.CaptureSafetyBoundedMaterialized,
		department.CaptureSafetyUnboundedMaterialized,
	}
	unrecognised := false
	for safety := range covered {
		if safety != "" && !slices.Contains(declared, safety) {
			unrecognised = true
		}
	}
	if !unrecognised {
		t.Fatal("no row carries a value outside the declared constants: nothing tests that the default arm refuses what it was never told is safe, which is what a future Core value or a typo would be")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// SupportsPooled is true in EVERY row, which is the only way the
			// capture-safety branch is reached: a row that declares
			// SupportsPooled false short-circuits before the switch, so a table
			// that varies both fields together tests the declaration and never
			// the rule. That is how a mutant treating the ZERO VALUE as
			// poolable survived this assertion the first time it was written.
			capabilities := department.Capabilities{
				SupportsPooled:    true,
				SupportsDedicated: true,
				AdmissionWeight:   1,
				CaptureSafety:     tt.safety,
			}
			if got := capabilities.PoolingPermitted(); got != tt.wantPooled {
				t.Errorf("PoolingPermitted() with SupportsPooled and %s capture = %v, want %v", tt.safety, got, tt.wantPooled)
			}

			// N3: a POOLED-ONLY target, the only shape that exercises the
			// dedicated arm's condition. Every other row here declares
			// SupportsDedicated, so making that append unconditional survived
			// the whole suite — and because New refuses pooled + unsafe
			// capture, this unit table is the only coverage PermittedPlacements
			// will ever get through any path.
			pooledOnly := capabilities
			pooledOnly.SupportsDedicated = false
			wantPooledOnly := []sessionwire.HostPlacement(nil)
			if tt.wantPooled {
				wantPooledOnly = []sessionwire.HostPlacement{sessionwire.HostPlacementPooled}
			}
			if got := pooledOnly.PermittedPlacements(); !slices.Equal(got, wantPooledOnly) {
				t.Errorf("PermittedPlacements() without dedicated support and %s capture = %v, want %v", tt.safety, got, wantPooledOnly)
			}

			// The declaration still governs: safe capture does not pool a
			// target that never asked to be pooled.
			undeclared := capabilities
			undeclared.SupportsPooled = false
			if undeclared.PoolingPermitted() {
				t.Errorf("PoolingPermitted() with %s capture and no pooled support = true, want false", tt.safety)
			}
			if got := undeclared.PermittedPlacements(); !slices.Equal(got, []sessionwire.HostPlacement{sessionwire.HostPlacementDedicated}) {
				t.Errorf("PermittedPlacements() without pooled support = %v, want [dedicated]", got)
			}

			placements := capabilities.PermittedPlacements()
			if !slices.IsSorted(placements) {
				t.Errorf("PermittedPlacements() = %v, want a deterministic sorted order", placements)
			}
			wantPlacements := []sessionwire.HostPlacement{sessionwire.HostPlacementDedicated}
			if tt.wantPooled {
				wantPlacements = []sessionwire.HostPlacement{sessionwire.HostPlacementDedicated, sessionwire.HostPlacementPooled}
			}
			if !slices.Equal(placements, wantPlacements) {
				t.Errorf("PermittedPlacements() = %v, want %v", placements, wantPlacements)
			}

			// The registry agrees with the descriptor: what PoolingPermitted
			// refuses, New refuses to register as pooled.
			_, err := department.New([]department.Registration{{
				AgentID: "reviewer",
				Target:  stubTarget{compatibility: "runtime-a", capabilities: department.Capabilities{SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1, CaptureSafety: tt.safety}},
			}})
			if tt.wantPooled && err != nil {
				t.Errorf("New with %s capture error = %v, want nil", tt.safety, err)
			}
			if !tt.wantPooled && err == nil {
				t.Errorf("New with %s capture succeeded while declaring pooled support; that is the silent narrowing this rejects", tt.safety)
			}
		})
	}
}

// TestPlacementConstantsStillCollateAsAssumed is a tripwire, not a rule.
//
// PermittedPlacements sorts, and the sort is unkillable because "dedicated"
// already sorts before "pooled" — so the code and its comment currently rest on
// a coincidence of spelling. Rather than leave a justification that is an
// untestable claim about a hypothetical, which this repository's standing rule
// forbids, the hypothetical is checked: if Core renames either constant into a
// different collating position this fails, and whoever fixes it re-reads why
// the sort is there.
func TestPlacementConstantsStillCollateAsAssumed(t *testing.T) {
	t.Parallel()

	if !(sessionwire.HostPlacementDedicated < sessionwire.HostPlacementPooled) {
		t.Fatalf("Core now collates %q after %q; PermittedPlacements' sort is no longer a no-op, which is exactly why it is there. Re-read its comment before changing anything",
			sessionwire.HostPlacementDedicated, sessionwire.HostPlacementPooled)
	}
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

func TestTargetReturnsRuntimeUnavailableForAnUnknownAgent(t *testing.T) {
	t.Parallel()

	registry, err := department.New([]department.Registration{registration("reviewer", "runtime-a")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := registry.Target("reviewer"); err != nil {
		t.Fatalf("Target(reviewer) error = %v, want nil", err)
	}

	_, err = registry.Target("planner")
	var unknown *department.UnknownAgentError
	if !errors.As(err, &unknown) {
		t.Fatalf("Target(planner) error = %v, want *UnknownAgentError", err)
	}
	if unknown.AgentID != "planner" {
		t.Errorf("AgentID = %q, want \"planner\"", unknown.AgentID)
	}
	if got := unknown.ErrorCode(); got != sessionwire.ErrorCodeRuntimeUnavailable {
		t.Errorf("ErrorCode() = %q, want %q", got, sessionwire.ErrorCodeRuntimeUnavailable)
	}
	// The public code is what a client branches on, and an unknown agent must
	// not be reported as a malformed request: the request was well formed and
	// this Host has no runtime for it.
	if strings.Contains(unknown.Error(), "invalid") {
		t.Errorf("Error() = %q; an unknown agent is not an invalid request", unknown.Error())
	}
}

// ---------------------------------------------------------------------------
// Immutability
// ---------------------------------------------------------------------------

// TestDepartmentDoesNotAliasItsInput is the defensive-copy probe. The caller
// keeps the slice it passed; mutating it afterwards must not reach inside.
func TestDepartmentDoesNotAliasItsInput(t *testing.T) {
	t.Parallel()

	registrations := []department.Registration{
		registration("reviewer", "runtime-a"),
		registration("planner", "runtime-b"),
	}
	registry, err := department.New(registrations)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	registrations[0].AgentID = "impostor"
	registrations[0].Target = target("runtime-substituted")
	registrations[1] = registration("planner", "runtime-substituted")

	found, err := registry.Target("reviewer")
	if err != nil {
		t.Fatalf("Target(reviewer) after mutating the caller's slice: %v", err)
	}
	if got := found.CompatibilityID(); got != "runtime-a" {
		t.Errorf("reviewer compatibility = %q, want \"runtime-a\"; the Department aliased its input", got)
	}
	if _, err := registry.Target("impostor"); err == nil {
		t.Error("Target(impostor) succeeded; mutating the caller's slice registered a new agent")
	}
	planner, err := registry.Target("planner")
	if err != nil {
		t.Fatalf("Target(planner): %v", err)
	}
	if got := planner.CompatibilityID(); got != "runtime-b" {
		t.Errorf("planner compatibility = %q, want \"runtime-b\"", got)
	}
}

// TestAgentIDsCannotBeMutatedIntoTheDepartment is the other half: a listing the
// caller can write to is a mutable view of the registry unless it is a copy.
func TestAgentIDsCannotBeMutatedIntoTheDepartment(t *testing.T) {
	t.Parallel()

	registry, err := department.New([]department.Registration{
		registration("planner", "runtime-a"),
		registration("reviewer", "runtime-b"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first := registry.AgentIDs()
	if len(first) != 2 {
		t.Fatalf("AgentIDs() = %q, want two", first)
	}
	for i := range first {
		first[i] = "clobbered"
	}
	first = append(first, "appended")
	_ = first

	second := registry.AgentIDs()
	if !slices.Equal(second, []sessionwire.AgentID{"planner", "reviewer"}) {
		t.Errorf("AgentIDs() after mutating an earlier listing = %q, want [planner reviewer]", second)
	}
	if _, err := registry.Target("clobbered"); err == nil {
		t.Error("Target(clobbered) succeeded; the listing was a window into the registry")
	}
}

// TestCompatibilityIDBoundIsExact is the off-by-one row the table did not have.
// `len(id) > Max` mutated to `>=` survived the whole suite, because nothing
// registered an id at exactly the limit; Core's own ids_test.go has such a case
// and this restatement of Core's rule did not.
func TestCompatibilityIDBoundIsExact(t *testing.T) {
	t.Parallel()

	atLimit := department.CompatibilityID(strings.Repeat("x", department.MaxCompatibilityIDBytes))
	if err := atLimit.Validate(); err != nil {
		t.Errorf("Validate() at exactly %d bytes = %v, want nil", department.MaxCompatibilityIDBytes, err)
	}
	overLimit := department.CompatibilityID(strings.Repeat("x", department.MaxCompatibilityIDBytes+1))
	if err := overLimit.Validate(); err == nil {
		t.Errorf("Validate() at %d bytes = nil, want an error", department.MaxCompatibilityIDBytes+1)
	}

	// The bound is BYTES, not runes, which is what Core enforces on the wire
	// field this is written to. A rune-counting restatement would accept an id
	// Core rejects.
	multibyte := department.CompatibilityID(strings.Repeat("\u00e9", department.MaxCompatibilityIDBytes))
	if err := multibyte.Validate(); err == nil {
		t.Errorf("Validate() of %d two-byte runes = nil; the bound is bytes, not runes", department.MaxCompatibilityIDBytes)
	}
}

// TestCompatibilityIDAgreesWithCore is the drift guard the restatement needs.
//
// MaxCompatibilityIDBytes is DERIVED from sessionwire.MaxIDBytes, so the
// constant cannot drift. The three RULES are hand-copied from Core's
// unexported validateID and nothing stopped them drifting, which is the same
// class of defect one level down. Comparing verdicts against a Core identity
// that shares validateID makes a Core rule change fail here instead of
// silently letting Host write an id Core will reject.
func TestCompatibilityIDAgreesWithCore(t *testing.T) {
	t.Parallel()

	values := []string{
		"",
		"runtime-a",
		strings.Repeat("x", department.MaxCompatibilityIDBytes-1),
		strings.Repeat("x", department.MaxCompatibilityIDBytes),
		strings.Repeat("x", department.MaxCompatibilityIDBytes+1),
		"\xff\xfe",
		"runtime-\xff",
		"\u00e9",
		strings.Repeat("\u00e9", department.MaxCompatibilityIDBytes),
	}
	for _, value := range values {
		hostRejects := department.CompatibilityID(value).Validate() != nil
		coreRejects := sessionwire.AgentID(value).Validate() != nil
		if hostRejects != coreRejects {
			t.Errorf("for %q: department.CompatibilityID rejects = %v, Core's identity rule rejects = %v. The restatement has drifted from validateID", value, hostRejects, coreRejects)
		}
	}
	if len(values) == 0 {
		t.Fatal("no value was compared")
	}
}

// TestRegisteredCapabilitiesAreReportedUnchanged pins that the registry is a
// registry: what a target declares is what a caller reads back, field for
// field. Two fields here — RequiresWorkspace and RequiresCheckpoint — are
// carried and not validated, which is exactly the kind of field that gets
// dropped by a constructor that rebuilds a struct instead of storing it.
func TestRegisteredCapabilitiesAreReportedUnchanged(t *testing.T) {
	t.Parallel()

	declared := department.Capabilities{
		SupportsPooled:     false,
		SupportsDedicated:  true,
		RequiresWorkspace:  true,
		RequiresCheckpoint: true,
		AdmissionWeight:    7,
		CaptureSafety:      department.CaptureSafetyUnboundedMaterialized,
	}
	registry, err := department.New([]department.Registration{{
		AgentID: "reviewer",
		Target:  stubTarget{compatibility: "runtime-a", capabilities: declared},
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	found, err := registry.Target("reviewer")
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	if got := found.Capabilities(); got != declared {
		t.Errorf("Capabilities() = %+v, want %+v", got, declared)
	}
	if got := found.CompatibilityID(); got != "runtime-a" {
		t.Errorf("CompatibilityID() = %q, want \"runtime-a\"", got)
	}
}

// ---------------------------------------------------------------------------
// Deterministic listing
// ---------------------------------------------------------------------------

// TestAgentIDsAreSortedNotMapOrdered uses enough agents, registered in
// DESCENDING order, that neither map-iteration order nor insertion order can
// pass by luck.
//
// The size is the point. Go randomizes map iteration, but with three or four
// keys a map-ordered implementation returns the sorted order often enough that
// a green run proves nothing. With 32 keys the chance of a map walk landing on
// sorted order is 1/32!, and registering them backwards means an
// insertion-ordered implementation is exactly reversed rather than accidentally
// right. Repeating the call also catches an implementation that is unstable
// without being unsorted.
func TestAgentIDsAreSortedNotMapOrdered(t *testing.T) {
	t.Parallel()

	const count = 32
	var (
		registrations []department.Registration
		want          []sessionwire.AgentID
	)
	for i := count - 1; i >= 0; i-- {
		agent := sessionwire.AgentID("agent-" + strconv.Itoa(100+i))
		registrations = append(registrations, department.Registration{AgentID: agent, Target: target("runtime-a")})
	}
	for i := range count {
		want = append(want, sessionwire.AgentID("agent-"+strconv.Itoa(100+i)))
	}

	registry, err := department.New(registrations)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for attempt := range 64 {
		got := registry.AgentIDs()
		if !slices.Equal(got, want) {
			t.Fatalf("AgentIDs() on attempt %d = %q, want %q", attempt, got, want)
		}
	}
	if registry.Len() != count {
		t.Errorf("Len() = %d, want %d", registry.Len(), count)
	}
}

// ---------------------------------------------------------------------------
// Boundary
// ---------------------------------------------------------------------------

// TestRuntimeIsSatisfiedBySegregatedCapabilities pins that Runtime is a
// COMPOSITION of narrow interfaces rather than one wide one, so a caller
// needing only liveness can take Liveness and a fake needs only that method.
// It also pins that the whole set is satisfiable outside this package, which is
// what "Host defines what it requires on Host's side" has to mean while H4.1 is
// unlanded — and that the three lifecycle shapes are H4.1's, so a Harness
// session differs from them only by identity type and not by shape.
func TestRuntimeIsSatisfiedBySegregatedCapabilities(t *testing.T) {
	t.Parallel()

	var runtime department.Runtime = fakeRuntime{}
	var (
		_ department.IdleWaiter            = runtime
		_ department.Liveness              = runtime
		_ department.Releaser              = runtime
		_ department.PublicationSubscriber = runtime
		_ department.CommandApplier        = runtime
		_ department.Identity              = runtime
	)
	if got := runtime.SessionID(); got != "session-1" {
		t.Errorf("SessionID() = %q, want \"session-1\"", got)
	}

	// Liveness is a BROADCAST, not a poll, and the difference is the reason
	// H4.1 chose this shape: any number of drain supervisors select on the same
	// channel and none of them has to ask. Exercising a receive pins that the
	// method returns something select-able rather than merely declaring a
	// channel type.
	select {
	case <-runtime.Done():
	default:
		t.Error("Done() did not report a stopped runtime; a Liveness a supervisor cannot select on is a poll with extra steps")
	}

	if err := runtime.ReleaseResidency(t.Context()); err != nil {
		t.Errorf("ReleaseResidency() = %v, want nil", err)
	}
}

// harnessH41LifecycleShapes is the H4.1 contract, transcribed from the program
// runbook (03-harness.md, "Add separate interfaces") and NOT read back from the
// code it checks. It is a literal for the same reason an oracle is a literal:
// derived from the implementation it would agree with anything.
//
//	type IdleWaiter interface { WaitIdle(context.Context) error }
//	type Liveness   interface { Done() <-chan struct{} }
//	type Releaser   interface { ReleaseResidency(context.Context) error }
var harnessH41LifecycleShapes = map[string][]string{
	"IdleWaiter": {"WaitIdle func(context.Context) error"},
	"Liveness":   {"Done func() <-chan struct {}"},
	"Releaser":   {"ReleaseResidency func(context.Context) error"},
}

// hostRuntimeMethodSet is the whole of Runtime: every method name and signature
// Host requires of a runtime, sorted, spelled out.
var hostRuntimeMethodSet = []string{
	"AgentID func() v1.AgentID",
	"ApplyCommand func(context.Context, department.RuntimeCommand) error",
	"Done func() <-chan struct {}",
	"ReleaseResidency func(context.Context) error",
	"SessionID func() v1.SessionID",
	"SubscribeCommitted func(context.Context, v1.EventID) (<-chan v1.EnduringPublication, error)",
	"WaitIdle func(context.Context) error",
}

// TestRuntimeMethodSetMatchesTheH41Contract pins the interface SHAPES, which
// nothing else here does.
//
// The satisfiability assertions in the test above look like they hold these,
// and they do not. They catch only an UNCOORDINATED edit: rename a method in
// definition.go alone and the compiler complains, but nobody renames a method
// uncoordinatedly. A gopls rename touches the interface and the fake in one
// action, both sides agree, and every compile-time assertion in this package
// passes while the H4.1 alignment that CLAUDE.md and two commit messages rest
// on is gone. Measured: renaming the release method to Release across
// definition.go and this file left the suite green, as did widening Liveness
// with a Stopped() bool implemented on the fake in the same edit.
//
// The rewording above is not cosmetic: an earlier draft opened a line with a
// method name, and TestDocCommentsNameTheirOwnDeclaration reported it, because
// that method — being on a test fake — has no doc comment of its own. That is
// the guard behaving as specified rather than a false positive to suppress, and
// it is worth knowing it reaches method names in prose, not only declarations.
//
// An interface's method set is fully reachable by reflection with NO
// implementation at all, which is what makes this the right guard: the earlier
// claim that "nothing about Runtime is assertion-testable until O1.2's adapter
// exists" was wrong, and wrong in the direction that leaves a documented
// guarantee unenforced.
func TestRuntimeMethodSetMatchesTheH41Contract(t *testing.T) {
	t.Parallel()

	byName := map[string]reflect.Type{
		"IdleWaiter": reflect.TypeFor[department.IdleWaiter](),
		"Liveness":   reflect.TypeFor[department.Liveness](),
		"Releaser":   reflect.TypeFor[department.Releaser](),
	}
	if len(harnessH41LifecycleShapes) != len(byName) {
		t.Fatalf("the transcribed H4.1 contract names %d interfaces and this test resolves %d", len(harnessH41LifecycleShapes), len(byName))
	}
	for name, want := range harnessH41LifecycleShapes {
		typ, ok := byName[name]
		if !ok {
			t.Errorf("H4.1 specifies %s and this package does not declare it", name)
			continue
		}
		if got := methodSet(typ); !slices.Equal(got, want) {
			t.Errorf("%s method set = %q, want H4.1's %q. A different SHAPE for the same capability turns O1.2's adapter from mechanical into semantic; if the divergence is deliberate, change the runbook or say so here rather than letting the two drift silently", name, got, want)
		}
	}

	got := methodSet(reflect.TypeFor[department.Runtime]())
	if !slices.Equal(got, hostRuntimeMethodSet) {
		t.Errorf("Runtime method set = %q, want %q", got, hostRuntimeMethodSet)
	}
	// Floored, because a Runtime that required nothing would satisfy an
	// empty expectation and every assertion above would be vacuous.
	if len(got) == 0 || len(hostRuntimeMethodSet) == 0 {
		t.Fatal("Runtime has no methods, or the expectation is empty; this guard would assert nothing")
	}
}

// methodSet returns "Name signature" for every method of an interface type, in
// the sorted order reflect reports, so a comparison sees a rename, a signature
// change, an addition and a removal alike.
func methodSet(typ reflect.Type) []string {
	methods := make([]string, 0, typ.NumMethod())
	for i := range typ.NumMethod() {
		method := typ.Method(i)
		methods = append(methods, method.Name+" "+method.Type.String())
	}
	return methods
}

type fakeRuntime struct{}

func (fakeRuntime) SessionID() sessionwire.SessionID { return "session-1" }
func (fakeRuntime) AgentID() sessionwire.AgentID     { return "reviewer" }
func (fakeRuntime) WaitIdle(context.Context) error   { return nil }
func (fakeRuntime) Done() <-chan struct{} {
	stopped := make(chan struct{})
	close(stopped)
	return stopped
}

func (fakeRuntime) ReleaseResidency(context.Context) error { return nil }

func (fakeRuntime) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, nil
}

func (fakeRuntime) ApplyCommand(context.Context, department.RuntimeCommand) error { return nil }
