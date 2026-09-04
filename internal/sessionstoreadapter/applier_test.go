package sessionstoreadapter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// The one seam this package cannot delegate
// ---------------------------------------------------------------------------

// grantFence is commands.Fence over an adapted grant.
//
// IT IS WRITTEN HERE RATHER THAN SHIPPED, and that is O7.1's carry-forward
// answered rather than discharged. O7.1 asks for a mechanical Fence adapter
// because residency.epochFence's methods are unexported, and offers O3.3 the
// option of discharging it "by delegating both to sessionstore". THAT OPTION IS
// NOT AVAILABLE: sessionstore v0.1.0 publishes no ownership signal to delegate
// to — the grant's Lost channel here is the adapter's own, closed by a write
// this Host made (finding F3) — so a Fence delegating to the released store
// would report ownership loss strictly later than epochFence does, and never at
// all for an idle session. The adapter therefore stays owed to O7.1 over
// residency.epochFence, and this type exists only to exercise the applier.
type grantFence struct {
	grant *sessionstoreadapter.Grant

	mu    sync.Mutex
	ended bool
}

func (f *grantFence) Held() error {
	f.mu.Lock()
	ended := f.ended
	f.mu.Unlock()
	if ended {
		return residency.ErrLeaseNotHeld
	}
	select {
	case <-f.grant.Lost():
		return residency.ErrLeaseNotHeld
	default:
		return nil
	}
}

func (f *grantFence) Lost() <-chan struct{} { return f.grant.Lost() }

func (f *grantFence) Write(run func() error) error {
	if err := f.Held(); err != nil {
		return err
	}
	err := run()
	if errors.Is(err, residency.ErrEpochSuperseded) || errors.Is(err, residency.ErrFenceConflict) {
		f.mu.Lock()
		f.ended = true
		f.mu.Unlock()
	}
	return err
}

// ---------------------------------------------------------------------------
// Host and runtime fixtures
// ---------------------------------------------------------------------------

type fixedClock struct{}

func (fixedClock) Now() time.Time                       { return time.Now().UTC() }
func (fixedClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

type stubTarget struct{}

func (stubTarget) CompatibilityID() department.CompatibilityID {
	return department.CompatibilityID(testCompat)
}
func (stubTarget) Capabilities() department.Capabilities {
	return department.Capabilities{SupportsPooled: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming}
}
func (stubTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("sessionstoreadapter_test: the stub target launches nothing")
}
func (stubTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("sessionstoreadapter_test: the stub target launches nothing")
}

type stubWorkspaces struct{}

func (stubWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", errors.New("sessionstoreadapter_test: the stub workspace provider materializes nothing")
}

type stubSessionStore struct{}

func (stubSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, errors.New("sessionstoreadapter_test: the stub session store loads nothing")
}

type stubAuth struct{}

func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// recordingRuntime accepts every command and records it, exactly as
// testkit's applier part does.
type recordingRuntime struct {
	mu      sync.Mutex
	applied []department.RuntimeCommand
}

func (r *recordingRuntime) ApplyCommand(_ context.Context, command department.RuntimeCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, command)
	return nil
}

func newApplierHost(t *testing.T) *host.Host {
	t.Helper()
	dept, err := department.New([]department.Registration{{AgentID: testAgent, Target: stubTarget{}}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	built, err := host.New(host.Options{
		HostID:            sessionwire.HostID("host-adapter"),
		TenantID:          testTenant,
		InternalEndpoint:  sessionwire.InternalEndpoint("ws://10.0.0.7:9443/hostlink"),
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      stubSessionStore{},
		Workspaces:        stubWorkspaces{},
		Clock:             fixedClock{},
		Auth:              stubAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    31 * time.Second,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: 3 * time.Second,
		ReconcileBatch:    16,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	return built
}

// ---------------------------------------------------------------------------
// The whole apply protocol, against the released store
// ---------------------------------------------------------------------------

// THIS IS THE TEST O3.3 EXISTS FOR. commands.Applier's every durable step —
// the record read, the claim, the payload load, the applying transition and the
// application prefix — runs against sessionstore v0.1.0 rather than a fake, and
// each one is exercised in the order §10.4 fixes.
//
// IT ENDS IN A REFUSAL, AND THE REFUSAL IS THE FINDING. The applier settles a
// command only against a correlated public event in THIS session's journal, and
// nothing wrote one: the runtime this test hands it records the command and
// returns, exactly as testkit's fake does, and against the released modules the
// real runtime would be no better — harness commits its own records under
// ("local", <harness uuid>) and not under Host's (TenantID, SessionID). See
// harnessadapter's TestHarnessAndHostDoNotShareASessionScope. So the durable
// evidence the settlement requires cannot exist, and RefusalNoCommittedEffect is
// the correct answer to a real question rather than a fixture artefact.
func TestApplierRunsTheWholeProtocolAgainstTheReleasedStore(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	admitCommand(t, released, "command-a", string(commands.KindInterrupt))

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	runtime := &recordingRuntime{}
	applier, err := commands.NewApplier(commands.ApplierOptions{
		Host:         newApplierHost(t),
		Key:          registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch:   grant.Epoch(),
		Records:      adapted,
		Applications: adapted,
		Gates:        adapted,
		Writes:       adapted,
		Journal:      grant,
		Runtime:      runtime,
		Fence:        &grantFence{grant: grant},
	})
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}

	outcome, err := applier.Process(t.Context(), commands.Command{
		TenantID:      testTenant,
		SessionID:     testSession,
		CommandID:     "command-a",
		AcceptedOrder: 1,
		State:         commands.StatePending,
	})

	if len(runtime.applied) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(runtime.applied))
	}
	if !outcome.PrefixOwned {
		t.Fatal("the application prefix was committed but the outcome did not report it owned")
	}
	if outcome.State != commands.StateApplying {
		t.Fatalf("outcome state = %q, want applying", outcome.State)
	}
	var applyErr *commands.ApplyError
	if !errors.As(err, &applyErr) {
		t.Fatalf("Process = %v, want an ApplyError", err)
	}
	if applyErr.Refusal != commands.RefusalNoCommittedEffect {
		t.Fatalf("refusal = %q, want %q", applyErr.Refusal, commands.RefusalNoCommittedEffect)
	}

	// The durable record is where the protocol left it: applying, under this
	// grant's epoch, with a prefix in the journal. That is exactly the state a
	// successor's recovery reads.
	record, err := adapted.LoadCommand(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("LoadCommand: %v", err)
	}
	if record.State != commands.StateApplying {
		t.Fatalf("durable state = %q, want applying", record.State)
	}
	if record.ClaimEpoch != grant.Epoch() {
		t.Fatalf("claim epoch = %d, want the grant's %d", record.ClaimEpoch, grant.Epoch())
	}
	application, err := adapted.FindApplication(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("FindApplication: %v", err)
	}
	if application.Outcome == commands.ApplicationAbsent {
		t.Fatal("the journal reports no prefix for a command the applier drove")
	}
}
