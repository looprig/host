package harnessruntime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
)

// probeReleaser records the context a release ran under and fails on demand.
type probeReleaser struct {
	err error

	mu       sync.Mutex
	released int
	ctxErr   error
}

func (r *probeReleaser) ReleaseResidency(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released++
	r.ctxErr = ctx.Err()
	return r.err
}

func (r *probeReleaser) releaseSeen() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.released, r.ctxErr
}

// recoveryController is a full controller with a probe releaser and a
// configurable applier and fault channel.
type recoveryController struct {
	controllerBase
	idlePart
	livenessPart
	*probeReleaser
	committedPart
	leaseEpochPart
	faultsPart
	applierPart
}

func newRecoveryController(applier applierPart, faulted chan struct{}) *recoveryController {
	return &recoveryController{
		controllerBase: controllerBase{id: newFullController(nil, nil, nil).id},
		livenessPart:   livenessPart{done: make(chan struct{})},
		probeReleaser:  &probeReleaser{},
		committedPart:  committedPart{available: true, subscription: newFakeSubscription(nil)},
		leaseEpochPart: leaseEpochPart{epoch: 7, held: true},
		faultsPart:     faultsPart{faulted: faulted},
		applierPart:    applier,
	}
}

// noEpochController is recoveryController without a LeaseEpochReporter: a live,
// releasable session bind must refuse.
type noEpochController struct {
	controllerBase
	idlePart
	livenessPart
	*probeReleaser
	committedPart
	faultsPart
}

var (
	_ session.SessionController = (*recoveryController)(nil)
	_ session.Releaser          = (*noEpochController)(nil)
)

func recoveryTarget(t *testing.T, controller session.SessionController) department.LaunchTarget {
	t.Helper()
	target, err := Target(stubRigs{launcher: &fakeLauncher{controller: controller}}, "build-7", conformanceCapabilities())
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	return target
}

// A TARGET'S DECLARATION IS VERIFIED AT BIND, NOT ASSUMED. Target declares
// both recovery capabilities, so a session that cannot honour one — an applier
// that is not a runtimecommand.AttemptCloser, no runtime-command applier at
// all, or a nil fault channel — is refused and released rather than admitted
// under a false declaration.
func TestTargetRefusesASessionThatCannotHonourItsRecoveryDeclaration(t *testing.T) {
	for _, row := range []struct {
		name    string
		applier applierPart
		faulted chan struct{}
		missing string
	}{
		{"an applier that is not a closer", applierPart{available: true}, make(chan struct{}), "runtimecommand.AttemptCloser"},
		{"no runtime-command applier", applierPart{available: false}, make(chan struct{}), "runtimecommand.AttemptCloser"},
		{"a nil fault channel", applierPart{available: true, closer: &recordingAttemptCloser{}}, nil, "PersistenceFaulted"},
	} {
		t.Run(row.name, func(t *testing.T) {
			controller := newRecoveryController(row.applier, row.faulted)
			_, err := recoveryTarget(t, controller).Create(t.Context(), department.CreateRequest{
				TenantID: testTenant, SessionID: testSession, AgentID: "agent", RigSessionID: controller.id,
			})
			var incapable *IncapableSessionError
			if !errors.As(err, &incapable) {
				t.Fatalf("Create = %v, want *IncapableSessionError", err)
			}
			if !slices.ContainsFunc(incapable.Missing, func(m string) bool { return strings.Contains(m, row.missing) }) {
				t.Errorf("Missing = %q, want it to name %s", incapable.Missing, row.missing)
			}
			if released, _ := controller.releaseSeen(); released != 1 {
				t.Errorf("the refused session was released %d times, want once", released)
			}
		})
	}

	t.Run("control: a session honouring both binds", func(t *testing.T) {
		controller := newRecoveryController(applierPart{available: true, closer: &recordingAttemptCloser{}}, make(chan struct{}))
		runtime, err := recoveryTarget(t, controller).Create(t.Context(), department.CreateRequest{
			TenantID: testTenant, SessionID: testSession, AgentID: "agent", RigSessionID: controller.id,
		})
		if err != nil || runtime == nil {
			t.Fatalf("Create = (%v, %v), want a runtime", runtime, err)
		}
		if released, _ := controller.releaseSeen(); released != 0 {
			t.Errorf("an accepted session was released %d times", released)
		}
	})
}

// AN ADAPTER BUILT WITH New REPORTS WHAT IT CAN HONOUR, so a product that
// declares recovery itself on department.NewRigTarget is held to the same
// truth: the department refuses a declaration the session cannot keep.
func TestABoundSessionReportsItsRealRecoveryAvailability(t *testing.T) {
	controller := newRecoveryController(applierPart{available: true}, nil)
	adapter := newAdapter(t, stubRigs{launcher: &fakeLauncher{controller: controller}})
	bound, err := adapter.NewSession(t.Context(), department.RigCreateRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("an undeclared adapter refused the session: %v", err)
	}
	reporter := bound.(interface {
		AttemptCloserAvailable() bool
		PersistenceFaultsAvailable() bool
	})
	if reporter.AttemptCloserAvailable() || reporter.PersistenceFaultsAvailable() {
		t.Fatal("the bound session reports recovery its harness session cannot provide")
	}

	caps := conformanceCapabilities()
	caps.Recovery = department.Recovery{AttemptCloser: true, PersistenceFaults: true}
	target, err := department.NewRigTarget(adapter, "build-7", caps)
	if err != nil {
		t.Fatal(err)
	}
	_, err = target.Create(t.Context(), department.CreateRequest{TenantID: testTenant, SessionID: testSession, AgentID: "agent"})
	var incapable *department.IncapableRuntimeError
	if !errors.As(err, &incapable) || !slices.Equal(incapable.Missing, []string{"declared AttemptCloser absent", "declared PersistenceFaults absent"}) {
		t.Fatalf("Create = %v, want the department to refuse both declarations", err)
	}
}

// A BIND REFUSAL KEEPS OWNERSHIP OF THE LAUNCHED SESSION. The session holds its
// journal lease and Host registers cleanup only after a successful launch, so
// bind releases it — under a context the launch's cancellation cannot reach —
// and reports a release that failed.
func TestABindRefusalReleasesTheLaunchedSession(t *testing.T) {
	newController := func(releaseErr error) *noEpochController {
		return &noEpochController{
			controllerBase: controllerBase{id: newFullController(nil, nil, nil).id},
			livenessPart:   livenessPart{done: make(chan struct{})},
			probeReleaser:  &probeReleaser{err: releaseErr},
			committedPart:  committedPart{available: true, subscription: newFakeSubscription(nil)},
			faultsPart:     faultsPart{faulted: make(chan struct{})},
		}
	}

	t.Run("under a cancelled launch context", func(t *testing.T) {
		controller := newController(nil)
		adapter := newAdapter(t, stubRigs{launcher: &fakeLauncher{controller: controller}})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := adapter.NewSession(ctx, department.RigCreateRequest{TenantID: testTenant, SessionID: testSession})
		var incapable *IncapableSessionError
		if !errors.As(err, &incapable) || !slices.Contains(incapable.Missing, "session.LeaseEpochReporter") {
			t.Fatalf("NewSession = %v, want an IncapableSessionError naming LeaseEpochReporter", err)
		}
		released, ctxErr := controller.releaseSeen()
		if released != 1 {
			t.Fatalf("the refused session was released %d times, want once", released)
		}
		if ctxErr != nil {
			t.Errorf("the release ran under a done context (%v)", ctxErr)
		}
	})

	t.Run("a failed release is reported", func(t *testing.T) {
		failure := errors.New("lease store unavailable")
		controller := newController(failure)
		adapter := newAdapter(t, stubRigs{launcher: &fakeLauncher{controller: controller}})
		_, err := adapter.RestoreSession(t.Context(), controller.id, department.RigRestoreRequest{TenantID: testTenant, SessionID: testSession})
		var incapable *IncapableSessionError
		if !errors.As(err, &incapable) || !errors.Is(err, failure) {
			t.Fatalf("RestoreSession = %v, want the refusal and the release failure", err)
		}
	})
}

var _ runtimecommand.AttemptCloser = (*recordingAttemptCloser)(nil)
