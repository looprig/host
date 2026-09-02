package department_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/testkit"
)

// rigSessionUUID is the Harness identity the fake hands back. It is a literal
// with no zero bytes so an assertion cannot pass against uuid.UUID{}.
var rigSessionUUID = uuid.MustParse("6f1c9e2a-3b47-4d58-9a10-2c7e5f8b4d63")

// launchRequest is the create request every test starts from. EVERY field is
// distinct from its zero value, because the standing trap in this program is a
// fixture whose default already equals the value being asserted: such a test
// passes against an adapter that propagates nothing.
func launchRequest() department.CreateRequest {
	return department.CreateRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
	}
}

func restoreRequest(compatibility department.CompatibilityID) department.RestoreRequest {
	return department.RestoreRequest{
		TenantID:        "tenant-9f3",
		SessionID:       "session-71c",
		AgentID:         "reviewer",
		Placement:       sessionwire.HostPlacementDedicated,
		WorkspaceRoot:   "/srv/workspaces/71c",
		Storage:         department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
		CompatibilityID: compatibility,
		RigSessionID:    rigSessionUUID,
	}
}

func rigTarget(t *testing.T, rig department.Rig) department.LaunchTarget {
	t.Helper()
	target, err := department.NewRigTarget(rig, "rig-2026-09", pooledCapabilities())
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	return target
}

// ---------------------------------------------------------------------------
// Propagation, asserted against the consumer
// ---------------------------------------------------------------------------

// TestCreatePropagatesTheLaunchContextToTheRig asserts against the RIG, which
// is what consumes these values, and not against the adapter's own getters.
//
// A getter assertion proves an adapter can remember a value. It does not prove
// the value reached the thing that needs it, and the Harness lane has just paid
// for that distinction: a record written into the wrong envelope kind was
// invisible to the released reader that exists to read it, and round-tripping
// it through the writer's own decode proved nothing at all.
func TestCreatePropagatesTheLaunchContextToTheRig(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)
	request := launchRequest()

	if _, err := target.Create(t.Context(), request); err != nil {
		t.Fatalf("Create: %v", err)
	}

	creates := rig.Creates()
	if len(creates) != 1 {
		t.Fatalf("the rig was asked for %d sessions, want exactly 1", len(creates))
	}
	got := creates[0]
	// The expectation is spelled out in LITERALS, not built by converting the
	// request the adapter was given. Deriving the oracle from the same
	// operation the implementation performs makes the two agree by
	// construction: if the adapter converts and the test converts, a dropped
	// field is dropped identically on both sides and the assertion cannot see
	// it. An oracle restates the answer; it does not recompute it.
	want := department.RigCreateRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
	}
	if got != want {
		t.Errorf("the rig received %+v, want %+v", got, want)
	}
	// The fixture-default control. If any asserted field equalled its zero
	// value, the assertion above would pass against an adapter that propagates
	// nothing, which is how three probes were lost in a sibling lane this week.
	if want == (department.RigCreateRequest{}) {
		t.Fatal("the launch request is entirely zero-valued, so the assertion above cannot distinguish propagation from silence")
	}
	for name, zero := range map[string]bool{
		"TenantID":      want.TenantID == "",
		"SessionID":     want.SessionID == "",
		"AgentID":       want.AgentID == "",
		"Placement":     want.Placement == "",
		"WorkspaceRoot": want.WorkspaceRoot == "",
		"Storage":       want.Storage == department.StorageContext{},
	} {
		if zero {
			t.Errorf("fixture field %s is zero-valued; an assertion on it proves nothing", name)
		}
	}
}

func TestRestorePropagatesTheLaunchContextAndTheHarnessIdentity(t *testing.T) {
	t.Parallel()

	session := testkit.NewFullSession(rigSessionUUID)
	rig := &testkit.FakeRig{Session: session}
	target := rigTarget(t, rig)

	created, err := target.Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	harnessID, ok := department.RigSessionID(created)
	if !ok {
		t.Fatal("RigSessionID did not recognise a runtime this package adapted")
	}
	if harnessID != rigSessionUUID {
		t.Fatalf("RigSessionID = %v, want %v", harnessID, rigSessionUUID)
	}

	if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restores := rig.Restores()
	if len(restores) != 1 {
		t.Fatalf("the rig was asked to restore %d sessions, want exactly 1", len(restores))
	}
	// A WHOLE-VALUE oracle, spelled in literals, exactly as the create test
	// does — and it is needed MORE here, not less. Create hands the rig a
	// conversion, so a forgotten field stops the build; Restore hands it a
	// field-by-field literal, because RestoreRequest carries two fields a rig
	// never sees and the two types are therefore not convertible. Nothing but
	// this comparison stands between a deleted line and a silently dropped
	// field: probing Storage and TenantID alone left deleting Placement and
	// WorkspaceRoot green, which is "propagate workspace context" gone.
	wantRestore := department.RigRestoreRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
	}
	if restores[0] != wantRestore {
		t.Errorf("the rig received %+v, want %+v", restores[0], wantRestore)
	}
	for name, zero := range map[string]bool{
		"TenantID":      wantRestore.TenantID == "",
		"SessionID":     wantRestore.SessionID == "",
		"AgentID":       wantRestore.AgentID == "",
		"Placement":     wantRestore.Placement == "",
		"WorkspaceRoot": wantRestore.WorkspaceRoot == "",
		"Storage":       wantRestore.Storage == department.StorageContext{},
	} {
		if zero {
			t.Errorf("fixture field %s is zero-valued; an assertion on it proves nothing", name)
		}
	}
	// Harness's identity for the session must reach the rig, because a restore
	// has nothing to restore FROM without it. A zero UUID here is the defect
	// this assertion exists for: the adapter passed uuid.UUID{} and every other
	// test in this file still passed, because none of them looked.
	ids := rig.RestoredIDs()
	if len(ids) != 1 || ids[0] != rigSessionUUID {
		t.Errorf("the rig was asked to restore %v, want %v", ids, rigSessionUUID)
	}
	if rigSessionUUID.IsZero() {
		t.Fatal("the fixture UUID is the zero value, so the assertion above cannot tell a propagated identity from an unset one")
	}
}

// TestAdaptedRuntimeCarriesHostIdentitiesNotHarnessOnes is the adapter O1.1
// said was mandatory, asserted rather than promised. Host's identities come
// from the REQUEST and Harness's UUID is kept beside them: nothing is parsed
// from one into the other, which is what makes the two identity spaces
// independent rather than merely differently spelled.
func TestAdaptedRuntimeCarriesHostIdentitiesNotHarnessOnes(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := runtime.SessionID(); got != "session-71c" {
		t.Errorf("SessionID() = %q, want the request's sessionwire identity", got)
	}
	if got := runtime.AgentID(); got != "reviewer" {
		t.Errorf("AgentID() = %q, want the request's agent identity", got)
	}
	if got := string(runtime.SessionID()); got == rigSessionUUID.String() {
		t.Error("SessionID() returned Harness's UUID; the two identity spaces are not the same one spelled differently")
	}
}

// ---------------------------------------------------------------------------
// Fail closed on compatibility mismatch
// ---------------------------------------------------------------------------

// TestRestoreFailsClosedOnCompatibilityMismatch is an ORDERING claim as much as
// an error claim: the refusal must happen with nothing launched. An adapter
// that starts a session and then notices the mismatch has already spent the
// resource it was supposed to protect.
func TestRestoreFailsClosedOnCompatibilityMismatch(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)

	runtime, err := target.Restore(t.Context(), restoreRequest("rig-2026-08"))
	var mismatch *department.CompatibilityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Restore onto a different runtime build error = %v, want *CompatibilityMismatchError", err)
	}
	if runtime != nil {
		t.Error("Restore returned a runtime alongside its error")
	}
	if rig.Launches() != 0 {
		t.Errorf("the rig was asked to launch %d times; a fail-closed mismatch must be decided before anything is launched", rig.Launches())
	}
	if mismatch.Durable != "rig-2026-08" || mismatch.Target != "rig-2026-09" {
		t.Errorf("mismatch = %+v, want it to name both the durable build and this target's", mismatch)
	}
	if got := mismatch.ErrorCode(); got != sessionwire.ErrorCodeRuntimeUnavailable {
		t.Errorf("ErrorCode() = %q, want %q", got, sessionwire.ErrorCodeRuntimeUnavailable)
	}

	// The control: the SAME request with the matching build launches, so the
	// refusal is the compatibility id and not the request being malformed.
	if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); err != nil {
		t.Fatalf("Restore with the matching build: %v", err)
	}
	if rig.Launches() != 1 {
		t.Errorf("the matching restore launched %d times, want 1", rig.Launches())
	}
}

// TestRestoreWithNoDeclaredCompatibilityFailsClosed covers the ZERO VALUE of
// the field, which is the forgotten-field case rather than a deliberate one: a
// RestoreRequest nobody filled in must not restore onto an arbitrary runtime.
func TestRestoreWithNoDeclaredCompatibilityFailsClosed(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	_, err := rigTarget(t, rig).Restore(t.Context(), restoreRequest(""))
	var mismatch *department.CompatibilityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Restore with an empty compatibility id error = %v, want *CompatibilityMismatchError", err)
	}
	if rig.Launches() != 0 {
		t.Error("the rig was launched for a restore with no declared runtime build")
	}
}

// ---------------------------------------------------------------------------
// Capability rejection
// ---------------------------------------------------------------------------

// TestLaunchRejectsASessionMissingACapability walks every capability Host
// requires and proves each one is individually load-bearing.
//
// The rejection is BEFORE publication in the sense this task can actually
// prove: no Runtime value is returned, so there is nothing for a caller to
// register. Asserting that also requires the control below — the same rig with
// a complete session succeeds — or "returns an error" would be satisfied by an
// adapter that rejects everything.
func TestLaunchRejectsASessionMissingACapability(t *testing.T) {
	t.Parallel()

	if len(testkit.AllCapabilities) != 5 {
		t.Fatalf("testkit lists %d capabilities, want the 5 Host requires", len(testkit.AllCapabilities))
	}

	for _, capability := range testkit.AllCapabilities {
		t.Run(string(capability), func(t *testing.T) {
			t.Parallel()
			rig := &testkit.FakeRig{Session: testkit.NewSessionWithout(rigSessionUUID, capability)}
			target := rigTarget(t, rig)

			runtime, err := target.Create(t.Context(), launchRequest())
			var incapable *department.IncapableRuntimeError
			if !errors.As(err, &incapable) {
				t.Fatalf("Create with a session missing %s error = %v, want *IncapableRuntimeError", capability, err)
			}
			if runtime != nil {
				t.Error("Create returned a runtime alongside its error; there must be nothing to publish")
			}
			if !slices.Contains(incapable.Missing, string(capability)) {
				t.Errorf("Missing = %q, want it to name %s", incapable.Missing, capability)
			}
			if len(incapable.Missing) != 1 {
				t.Errorf("Missing = %q, want exactly the one capability withheld", incapable.Missing)
			}
			if incapable.AgentID != "reviewer" || incapable.SessionID != "session-71c" {
				t.Errorf("error = %+v, want it to name the agent and session", incapable)
			}

			// Restore rejects the same session for the same reason, so the
			// check is not on one path only.
			if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); !errors.As(err, &incapable) {
				t.Errorf("Restore with a session missing %s error = %v, want *IncapableRuntimeError", capability, err)
			}
		})
	}

	t.Run("control: a complete session is accepted", func(t *testing.T) {
		t.Parallel()
		rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
		runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
		if err != nil {
			t.Fatalf("Create with a complete session: %v", err)
		}
		if runtime == nil {
			t.Fatal("Create returned no runtime and no error")
		}
	})
}

// TestIncapableRuntimeErrorNamesEveryMissingCapability holds that a session
// short of several capabilities costs one round trip to diagnose rather than
// five. An error naming only the first is a guard that makes you run it again.
func TestIncapableRuntimeErrorNamesEveryMissingCapability(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewBareSession(rigSessionUUID)}
	_, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	var incapable *department.IncapableRuntimeError
	if !errors.As(err, &incapable) {
		t.Fatalf("Create error = %v, want *IncapableRuntimeError", err)
	}
	want := make([]string, 0, len(testkit.AllCapabilities))
	for _, capability := range testkit.AllCapabilities {
		want = append(want, string(capability))
	}
	slices.Sort(want)
	got := slices.Clone(incapable.Missing)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("Missing = %q, want all five: %q", got, want)
	}
	for _, capability := range want {
		if !strings.Contains(incapable.Error(), capability) {
			t.Errorf("Error() = %q, want it to name %s", incapable.Error(), capability)
		}
	}
}

// ---------------------------------------------------------------------------
// Construction and rig failure
// ---------------------------------------------------------------------------

func TestNewRigTargetRejectsAnUnusableTarget(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnknown

	tests := []struct {
		name          string
		rig           department.Rig
		compatibility department.CompatibilityID
		capabilities  department.Capabilities
	}{
		{name: "nil rig", rig: nil, compatibility: "rig-2026-09", capabilities: pooledCapabilities()},
		{name: "empty compatibility id", rig: &testkit.FakeRig{}, compatibility: "", capabilities: pooledCapabilities()},
		{name: "pooled with unsafe capture", rig: &testkit.FakeRig{}, compatibility: "rig-2026-09", capabilities: unsafe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			target, err := department.NewRigTarget(tt.rig, tt.compatibility, tt.capabilities)
			if err == nil {
				t.Fatal("NewRigTarget accepted an unusable target")
			}
			if target != nil {
				t.Error("NewRigTarget returned a target alongside its error; an unusable target must not reach a Registration, let alone a Department")
			}
		})
	}
}

// TestNewRigTargetSharesTheDepartmentsCapabilityRules is the one-site check.
// NewRigTarget and department.New both refuse the same Capabilities, because
// the rules live in Capabilities.Validate and neither restates them. Two sites
// applying the same rule separately is the shape this package has already spent
// several rounds removing.
func TestNewRigTargetSharesTheDepartmentsCapabilityRules(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnboundedMaterialized
	noPlacement := pooledCapabilities()
	noPlacement.SupportsPooled, noPlacement.SupportsDedicated = false, false
	zeroWeight := pooledCapabilities()
	zeroWeight.AdmissionWeight = 0

	for _, capabilities := range []department.Capabilities{unsafe, noPlacement, zeroWeight} {
		_, targetErr := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", capabilities)
		_, registryErr := department.New([]department.Registration{{
			AgentID: "reviewer",
			Target:  stubTarget{compatibility: "rig-2026-09", capabilities: capabilities},
		}})
		if (targetErr == nil) != (registryErr == nil) {
			t.Errorf("for %+v NewRigTarget error = %v but department.New error = %v; the two disagree about the same rule", capabilities, targetErr, registryErr)
		}
		var invalid *department.InvalidCapabilitiesError
		if !errors.As(targetErr, &invalid) {
			t.Errorf("NewRigTarget error = %v; errors.As did not reach *InvalidCapabilitiesError", targetErr)
		}
	}

	// The control: a legal Capabilities is accepted by both.
	if _, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", pooledCapabilities()); err != nil {
		t.Errorf("NewRigTarget with legal capabilities: %v", err)
	}
}

// TestLaunchWrapsARigFailureWithoutLosingTheCause keeps the repository rule
// that a package-level API returns a typed error preserving its cause.
func TestLaunchWrapsARigFailureWithoutLosingTheCause(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{NewErr: testkit.ErrRigRefused, RestoreErr: testkit.ErrRigRefused}
	target := rigTarget(t, rig)

	for _, launch := range []struct {
		name string
		run  func() (department.Runtime, error)
	}{
		{name: "create", run: func() (department.Runtime, error) { return target.Create(t.Context(), launchRequest()) }},
		{name: "restore", run: func() (department.Runtime, error) {
			return target.Restore(t.Context(), restoreRequest("rig-2026-09"))
		}},
	} {
		t.Run(launch.name, func(t *testing.T) {
			runtime, err := launch.run()
			var launchErr *department.RigLaunchError
			if !errors.As(err, &launchErr) {
				t.Fatalf("%s error = %v, want *RigLaunchError", launch.name, err)
			}
			if !errors.Is(err, testkit.ErrRigRefused) {
				t.Errorf("%s error does not unwrap to the rig's own cause", launch.name)
			}
			if runtime != nil {
				t.Error("a failed launch returned a runtime")
			}
			if launchErr.Operation == "" {
				t.Error("the error does not say which operation failed")
			}
		})
	}
}

// TestLaunchRejectsASessionTheRigDidNotReturn covers the rig that reports
// success and hands back nothing, which is a contract violation this package
// must not turn into a nil dereference three layers away.
func TestLaunchRejectsASessionTheRigDidNotReturn(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err == nil {
		t.Fatal("Create accepted a rig that returned no session and no error")
	}
	if runtime != nil {
		t.Error("Create returned a runtime for a session that does not exist")
	}

	// The TYPE and the CAUSE, not merely that something failed. Asserting only
	// "an error came back" is what let a real mutant live: delete the nil check
	// AND move session.ID() below the capability discovery, and a nil session
	// becomes IncapableRuntimeError{Missing: all five} — because a type
	// assertion on a nil interface returns ok=false. No panic, suite green, and
	// a diagnosis that sends the reader to look for five missing methods on a
	// session that was never returned.
	//
	// This is why "removing the guard only panics" was the wrong reading here.
	// It holds only when nothing can be moved above the dereference, and here
	// the dereference is reorderable.
	var launchErr *department.RigLaunchError
	if !errors.As(err, &launchErr) {
		t.Fatalf("Create error = %v, want *RigLaunchError", err)
	}
	if !errors.Is(err, department.ErrNoRigSession) {
		t.Errorf("Create error does not unwrap to ErrNoRigSession, so it cannot be told from a rig that refused")
	}
	var incapable *department.IncapableRuntimeError
	if errors.As(err, &incapable) {
		t.Errorf("a rig that returned no session was diagnosed as an incapable one: %v", incapable)
	}
}

// TestRigTargetExposesItsDeclaredCompatibilityAndCapabilities asserts the
// ACCESSORS, which nothing did.
//
// "Expose the declared compatibility id" is a named property of this task and
// it had no test: a mutant returning a constant from CompatibilityID left the
// whole suite green, because every other test reached t.compatibility through
// Restore's comparison, which is a different code path. The two uses are what
// make the gap matter rather than merely exist — HostLink advertises this value
// as RuntimeCompatibilityID and Restore refuses a durable state that disagrees
// with it, so an accessor out of step with the comparison is a Host advertising
// a build it will then refuse to restore.
func TestRigTargetExposesItsDeclaredCompatibilityAndCapabilities(t *testing.T) {
	t.Parallel()

	declared := department.Capabilities{
		SupportsPooled:     false,
		SupportsDedicated:  true,
		RequiresWorkspace:  true,
		RequiresCheckpoint: true,
		AdmissionWeight:    7,
		CaptureSafety:      department.CaptureSafetyUnboundedMaterialized,
	}
	target, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", declared)
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	if got := target.CompatibilityID(); got != "rig-2026-09" {
		t.Errorf("CompatibilityID() = %q, want the declared build", got)
	}
	if got := target.Capabilities(); got != declared {
		t.Errorf("Capabilities() = %+v, want %+v", got, declared)
	}
	// The control: a target declared with a DIFFERENT build reports that one,
	// so the assertion above is reading the declaration and not a constant that
	// happens to match the fixture.
	other, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2025-01", declared)
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	if got := other.CompatibilityID(); got != "rig-2025-01" {
		t.Errorf("CompatibilityID() = %q, want the second target's declared build", got)
	}
}

// TestARigTargetSurvivesRegistration is the closest this task gets to step 2's
// "before registry publication": a real rig target — not the stub every other
// registry test uses — goes into a Department and is read back out.
//
// It matters beyond coverage. O2.2's registry entry stores the compatibility id
// READ OFF THE TARGET, so the value that reaches the registry is the accessor's
// and not Restore's. Registering a stub proves the registry works; registering
// this proves the thing the registry will actually hold.
func TestARigTargetSurvivesRegistration(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)

	registry, err := department.New([]department.Registration{{AgentID: "reviewer", Target: target}})
	if err != nil {
		t.Fatalf("New with a rig target: %v", err)
	}
	registered, err := registry.Target("reviewer")
	if err != nil {
		t.Fatalf("Target(reviewer): %v", err)
	}
	if got := registered.CompatibilityID(); got != "rig-2026-09" {
		t.Errorf("the registered target reports build %q, want the one it was constructed with. This is the value O2.2 stores and HostLink advertises", got)
	}
	if got := registered.Capabilities(); got != pooledCapabilities() {
		t.Errorf("the registered target reports %+v, want %+v", got, pooledCapabilities())
	}

	// And it still launches once registered, so registration does not hand back
	// something merely shaped like the target.
	runtime, err := registered.Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create through the registry: %v", err)
	}
	if runtime.SessionID() != "session-71c" {
		t.Errorf("SessionID() = %q through the registry", runtime.SessionID())
	}
}

// ---------------------------------------------------------------------------
// The capabilities reach through
// ---------------------------------------------------------------------------

// TestAdaptedRuntimeForwardsToTheSession asserts against the SESSION for each
// capability, for the same reason the propagation tests do: a Runtime that
// answers WaitIdle itself, or subscribes to its own empty channel, satisfies
// the interface and does nothing.
func TestAdaptedRuntimeForwardsToTheSession(t *testing.T) {
	t.Parallel()

	session := testkit.NewFullSession(rigSessionUUID)
	rig := &testkit.FakeRig{Session: session}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := runtime.ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("ReleaseResidency: %v", err)
	}
	if session.Released() != 1 {
		t.Errorf("the session saw %d releases, want 1", session.Released())
	}

	envelope := sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-4b2"}
	if err := runtime.ApplyCommand(t.Context(), envelope); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	if applied := session.Applied(); len(applied) != 1 || applied[0].CommandID != "command-4b2" {
		t.Errorf("the session applied %+v, want the envelope the runtime was given", applied)
	}

	if _, err := runtime.SubscribeCommitted(t.Context(), "event-88"); err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}
	if after := session.SubscribedAfter(); !slices.Equal(after, []sessionwire.EventID{"event-88"}) {
		t.Errorf("the session was subscribed from %q, want [event-88]", after)
	}

	// Liveness is the session's channel, not one the adapter made: closing the
	// session's must be observable through the runtime.
	select {
	case <-runtime.Done():
		t.Fatal("Done() reported a stopped runtime before the session stopped")
	default:
	}
	session.Stop()
	select {
	case <-runtime.Done():
	default:
		t.Error("the session stopped and Done() did not report it; the adapter is not forwarding the session's channel")
	}

	// A cancelled context must reach the session rather than be swallowed.
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.WaitIdle(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitIdle with a cancelled context = %v, want context.Canceled", err)
	}
}
