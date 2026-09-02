package department_test

import (
	"context"
	"errors"
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

func target(compatibility string) stubTarget {
	return stubTarget{compatibility: department.CompatibilityID(compatibility), capabilities: pooledCapabilities()}
}

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
	if !strings.Contains(invalid.Reason, "no launch target") {
		t.Errorf("Reason = %q, want it to say the Department registers no launch target", invalid.Reason)
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
	if !strings.Contains(invalid.Reason, "registered more than once") {
		t.Errorf("Reason = %q, want it to say the agent is registered more than once", invalid.Reason)
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
		reason       string
	}{
		{
			name:         "empty agent id",
			registration: department.Registration{AgentID: "", Target: target("runtime-a")},
			agent:        "",
			reason:       "agent identity",
		},
		{
			name:         "nil target",
			registration: department.Registration{AgentID: "reviewer", Target: nil},
			agent:        "reviewer",
			reason:       "no launch target",
		},
		{
			name:         "empty compatibility id",
			registration: department.Registration{AgentID: "reviewer", Target: target("")},
			agent:        "reviewer",
			reason:       "compatibility",
		},
		{
			name:         "over-long compatibility id",
			registration: department.Registration{AgentID: "reviewer", Target: target(strings.Repeat("x", sessionwire.MaxIDBytes+1))},
			agent:        "reviewer",
			reason:       "compatibility",
		},
		{
			name:         "pooled with unsafe capture",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: unsafe}},
			agent:        "reviewer",
			reason:       "dedicated-only",
		},
		{
			// A target that costs nothing against capacity admits without
			// bound, which is a capacity guard that reports success while
			// enforcing nothing. Zero is also the ZERO VALUE, so this is the
			// forgotten-field case rather than a deliberate one.
			name:         "zero admission weight",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: zeroWeight}},
			agent:        "reviewer",
			reason:       "admission weight",
		},
		{
			name:         "no placement at all",
			registration: department.Registration{AgentID: "reviewer", Target: stubTarget{compatibility: "runtime-a", capabilities: noPlacement}},
			agent:        "reviewer",
			reason:       "no placement",
		},
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
			if !strings.Contains(invalid.Reason, tt.reason) {
				t.Errorf("Reason = %q, want it to mention %q", invalid.Reason, tt.reason)
			}
		})
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
		safety     department.CaptureSafety
		wantPooled bool
	}{
		{safety: department.CaptureSafetyStreaming, wantPooled: true},
		{safety: department.CaptureSafetyBoundedMaterialized, wantPooled: true},
		{safety: department.CaptureSafetyUnboundedMaterialized, wantPooled: false},
		{safety: department.CaptureSafetyUnknown, wantPooled: false},
	}

	for _, tt := range tests {
		t.Run(string(tt.safety), func(t *testing.T) {
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
// unlanded.
func TestRuntimeIsSatisfiedBySegregatedCapabilities(t *testing.T) {
	t.Parallel()

	var runtime department.Runtime = fakeRuntime{}
	var (
		_ department.IdleWaiter            = runtime
		_ department.Liveness              = runtime
		_ department.Releaser              = runtime
		_ department.PublicationSubscriber = runtime
		_ department.CommandApplier        = runtime
		_ department.Controller            = runtime
	)
	if got := runtime.SessionID(); got != "session-1" {
		t.Errorf("SessionID() = %q, want \"session-1\"", got)
	}
}

type fakeRuntime struct{}

func (fakeRuntime) SessionID() sessionwire.SessionID { return "session-1" }
func (fakeRuntime) AgentID() sessionwire.AgentID     { return "reviewer" }
func (fakeRuntime) WaitIdle(context.Context) error   { return nil }
func (fakeRuntime) Alive(context.Context) error      { return nil }
func (fakeRuntime) Release(context.Context) error    { return nil }

func (fakeRuntime) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, nil
}

func (fakeRuntime) ApplyCommand(context.Context, sessionwire.CommandEnvelope) error { return nil }
