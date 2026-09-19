package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnesstest"
	"github.com/looprig/host/internal/testkit"
)

// ---------------------------------------------------------------------------
// The exported blueprint, driven over a REAL session store
// ---------------------------------------------------------------------------
//
// Every test here composes through host.Compose over a memstore-backed
// sessionstore — the released store, not a fake — because the property B1
// exists for is "a consumer outside this module can run a Host", and a fake
// store would be this module's. Where a test needs to act as the OTHER party
// (Factory seeding the catalog, another Host holding a lease) it opens a second
// sessionstore.Store over the SAME backend, which is exactly what those parties
// do in production.

const (
	composeTenant  = sessionwire.TenantID("tenant-a")
	composeSession = sessionwire.SessionID("session-a")
	composeAgent   = sessionwire.AgentID("agent-a")
	composeCompat  = department.CompatibilityID("runtime-a")
	composeSecret  = "factory-service-secret"
	composeBinding = "binding-1"
)

type acceptingAuth struct{}

func (acceptingAuth) VerifyTenant(_ context.Context, _ sessionwire.TenantID, credential string) error {
	if credential != composeSecret {
		return errors.New("bad credential")
	}
	return nil
}

type inertWorkspaces struct{}

func (inertWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "/tmp/workspace", nil
}
func (inertWorkspaces) ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

type inertCheckpointer struct{}

func (inertCheckpointer) Checkpoint(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

type stubEvidence struct{}

func (stubEvidence) ReadDispositionEvidence(context.Context, sessionstore.DispositionEvidenceRequest) (sessionstore.DispositionEvidence, error) {
	return sessionstore.DispositionEvidence{}, errors.New("no evidence in this test")
}

// composePublishingSession keeps the real runtime capabilities from the
// fixture, but leaves its committed-publication tail open. The ordinary
// FullSession intentionally returns a closed channel because most composition
// tests exercise lifecycle unwinds; the HostLink round trip needs a live tail
// so a publication can cross the composed relay and reach a real subscriber.
type composePublishingSession struct {
	*testkit.FullSession
	published chan sessionwire.EnduringPublication
}

func (s *composePublishingSession) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return s.published, nil
}

// composeFixture is one blueprint's inputs, with the backend shared so a
// test can act as another party over it.
type composeFixture struct {
	backend *storage.Composite
	rig     *testkit.FakeRig
	session *testkit.FullSession

	// journal is the runtime's journal store for (composeTenant,
	// composeBinding): a REAL harness session store, because an attach now
	// reads it to decide whether the session's conversation already exists.
	// The fake rig never writes to it, so every session here is new to it.
	journal *harnessstore.Store
	// journalBackend is the storage under journal, so a test can open a
	// second reader over the same ledger.
	journalBackend *storage.Composite
}

func newComposeFixture(t testing.TB) *composeFixture {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	session := testkit.NewFullSession(id)
	journalBackend := harnesstest.Backend(t)
	return &composeFixture{
		backend:        memstore.New(),
		rig:            &testkit.FakeRig{Session: session},
		session:        session,
		journal:        harnesstest.Store(t, journalBackend, composeTenant),
		journalBackend: journalBackend,
	}
}

func (f *composeFixture) registrar(t testing.TB) host.Registrar {
	t.Helper()
	return host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		target, err := department.NewRigTarget(f.rig, composeCompat, department.Capabilities{
			SupportsPooled:    true,
			SupportsDedicated: true,
			AdmissionWeight:   1,
			CaptureSafety:     department.CaptureSafetyStreaming,
		})
		if err != nil {
			return nil, err
		}
		return []department.Registration{{AgentID: composeAgent, Target: target}}, nil
	})
}

func (f *composeFixture) blueprint(t testing.TB) host.Composition {
	t.Helper()
	return host.Composition{
		Options: host.Options{
			HostID:            "host-a",
			InternalEndpoint:  "ws://10.0.0.1:7100",
			IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
			Placement:         sessionwire.HostPlacementPooled,
			Capacity:          4,
			WarmTTL:           90 * time.Second,
			RegistryHeartbeat: 10 * time.Second,
			RegistryExpiry:    60 * time.Second,
			ClaimTTL:          5 * time.Second,
			ApplyDeadline:     30 * time.Second,
			CommandQueueSize:  16,
			ReconcileInterval: time.Minute,
			ReconcileBatch:    32,
		},
		Generation:           4,
		Link:                 host.LinkOptions{MaxBindingsPerLink: 4, MaxBindings: 8, MaxTenantLinks: 3},
		Drain:                host.DrainOptions{Grace: 30 * time.Second, IdleBoundary: 10 * time.Second, PublishBound: 5 * time.Second},
		CompatibilityTimeout: 20 * time.Second,
		WorkPoll:             time.Second,
		Collaborators: host.Collaborators{
			Backend:       f.backend,
			JournalStores: map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: composeTenant, StorageBindingID: composeBinding}: f.journal},
			Registrar:     f.registrar(t),
			Checkpointer:  inertCheckpointer{},
			Auth:          acceptingAuth{},
			Workspaces:    inertWorkspaces{},
			NamespaceLayout: func(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
				return string(tenant) + "/" + string(session)
			},
		},
	}
}

// otherParty opens a SECOND store over the same backend: Factory seeding the
// catalog, or another Host taking a lease.
func (f *composeFixture) otherParty(t testing.TB) *sessionstore.Store {
	t.Helper()
	other, err := sessionstore.Open(t.Context(), f.backend)
	if err != nil {
		t.Fatalf("open the other party's store over the shared backend: %v", err)
	}
	t.Cleanup(func() { _ = other.Close(context.Background()) })
	return other
}

// seedDispositionSession is Factory's create: the catalog record a Host's
// AcquireResidency will grant residency over. Its binding names the fixture's
// runtime session, as Factory's derived runtime id names the runtime's.
func (f *composeFixture) seedDispositionSession(t *testing.T, store *sessionstore.Store, session sessionwire.SessionID) {
	t.Helper()
	now := time.Now().UTC()
	if _, _, err := store.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID:               composeTenant,
		SessionID:              session,
		AgentID:                composeAgent,
		RuntimeCompatibilityID: string(composeCompat),
		CreatedAt:              now,
		LastActiveAt:           now,
		State:                  sessionwire.SessionStateRunning,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "idem-" + string(session),
		Binding: sessionstore.SessionBinding{
			StorageBindingID: composeBinding,
			BindingVersion:   "v1",
			RuntimeSessionID: f.session.ID().String(),
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
	}); err != nil {
		t.Fatalf("seed the catalog: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

// TestComposeRefusesWhatItCannotRun holds every construction rule as a typed
// refusal naming its field, with the untouched fixture accepted as the control
// (and closed again).
func TestComposeRefusesWhatItCannotRun(t *testing.T) {
	control := newComposeFixture(t)
	service, err := host.Compose(t.Context(), control.blueprint(t))
	if err != nil {
		t.Fatalf("the control blueprint was refused: %v", err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := service.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	for _, test := range []struct {
		field string
		spoil func(*host.Composition)
	}{
		{"Options.Department", func(c *host.Composition) { c.Options.Department = &department.Department{} }},
		{"Options.SessionStore", func(c *host.Composition) { c.Options.SessionStore = facadeStore{} }},
		{"Options.Workspaces", func(c *host.Composition) { c.Options.Workspaces = inertWorkspaces{} }},
		{"Options.Clock", func(c *host.Composition) { c.Options.Clock = facadeClock{} }},
		{"Options.Auth", func(c *host.Composition) { c.Options.Auth = acceptingAuth{} }},
		{"Generation", func(c *host.Composition) { c.Generation = 0 }},
		{"Collaborators.Backend", func(c *host.Composition) { c.Collaborators.Backend = nil }},
		{"Collaborators.JournalStores", func(c *host.Composition) { c.Collaborators.JournalStores = nil }},
		{"Collaborators.Registrar", func(c *host.Composition) { c.Collaborators.Registrar = nil }},
		{"Collaborators.Checkpointer", func(c *host.Composition) { c.Collaborators.Checkpointer = nil }},
		{"Collaborators.Auth", func(c *host.Composition) { c.Collaborators.Auth = nil }},
		{"Collaborators.Workspaces", func(c *host.Composition) { c.Collaborators.Workspaces = nil }},
		{"Collaborators.NamespaceLayout", func(c *host.Composition) { c.Collaborators.NamespaceLayout = nil }},
		{"Collaborators.JournalStores", func(c *host.Composition) {
			c.Collaborators.JournalStores = map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: "", StorageBindingID: "b"}: stubEvidence{}}
		}},
		{"Collaborators.StoreOptions", func(c *host.Composition) {
			// A caller's own evidence option: the store refuses two, so the
			// router Compose installed is never silently replaced. This is
			// condition (a) made unrepresentable.
			c.Collaborators.StoreOptions = []sessionstore.Option{sessionstore.WithDispositionEvidence(stubEvidence{})}
		}},
		{"Collaborators.Registrar", func(c *host.Composition) {
			c.Collaborators.Registrar = host.RegistrarFunc(func(context.Context) ([]department.Registration, error) { return nil, errors.New("no agents") })
		}},
		{"Link.MaxBindings", func(c *host.Composition) { c.Link.MaxBindings = 0 }},
		{"Drain.Grace", func(c *host.Composition) { c.Drain.Grace = 0 }},
		{"WorkPoll", func(c *host.Composition) { c.WorkPoll = 0 }},
	} {
		t.Run(test.field, func(t *testing.T) {
			f := newComposeFixture(t)
			blueprint := f.blueprint(t)
			test.spoil(&blueprint)
			service, err := host.Compose(t.Context(), blueprint)
			if err == nil {
				_, _ = service.Stop(t.Context())
				t.Fatalf("a blueprint with %s spoiled was accepted", test.field)
			}
			var invalid *host.InvalidCompositionError
			if !errors.As(err, &invalid) {
				t.Fatalf("Compose = %T %v, want *host.InvalidCompositionError", err, err)
			}
			if invalid.Field != test.field {
				t.Fatalf("refusal names %q, want %q (%v)", invalid.Field, test.field, err)
			}
		})
	}

	// An Options rule is refused as the Options rule, through the same
	// constructor a v0.1.0 caller knows.
	f := newComposeFixture(t)
	blueprint := f.blueprint(t)
	blueprint.Options.Capacity = 0
	_, err = host.Compose(t.Context(), blueprint)
	var invalidOptions *host.InvalidOptionsError
	if !errors.As(err, &invalidOptions) || invalidOptions.Code != host.OptionErrorCodeNotPositive {
		t.Fatalf("a bad Option was refused %v, want *host.InvalidOptionsError not_positive", err)
	}
}

// ---------------------------------------------------------------------------
// The whole path, in process and over the link
// ---------------------------------------------------------------------------

// TestAComposedHostRunsTheAttachAndLiveLinkRoundtrip is B1's composed
// acceptance: a caller outside this module seeds a real SessionStore, reaches
// a real HostLink, makes the session resident, and uses the returned
// observation to bind and receive a committed publication.
func TestAComposedHostRunsTheAttachAndLiveLinkRoundtrip(t *testing.T) {
	f := newComposeFixture(t)
	other := f.otherParty(t)
	f.seedDispositionSession(t, other, composeSession)
	runtime := &composePublishingSession{
		FullSession: f.session,
		published:   make(chan sessionwire.EnduringPublication),
	}
	f.rig.Session = runtime

	service, err := host.Compose(t.Context(), f.blueprint(t))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if service.Ready() || service.Live() {
		t.Fatal("a composed, unstarted Service reports ready or live")
	}
	if service.Host().ID() != "host-a" || service.Department() == nil {
		t.Fatalf("Host() = %v, Department() = %v", service.Host(), service.Department())
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !service.Ready() || !service.Live() {
		t.Fatal("a started Service is not ready and live")
	}

	server := httptest.NewServer(service.Handler())
	defer server.Close()
	link := dialHostLink(t, server.URL, composeTenant)
	defer link.Close()

	// THE ATTACH IS OVER THE REAL HOSTLINK. Its body is the observation the
	// caller must use for the next step, rather than a value reconstructed from
	// the request or from Host-local state.
	attach := rpcOver(t, link, 2, sessionwire.HostLinkMethodAttach, sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: composeTenant, SessionID: composeSession,
		HostID: "host-a", HostGeneration: 4, AgentID: composeAgent, RuntimeCompatibilityID: string(composeCompat),
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory", IdempotencyKey: "attach-1",
	})
	if attach.Error != nil {
		t.Fatalf("attach over HostLink: %#v", *attach.Error)
	}
	if attach.RPC == nil || len(attach.RPC.Data) == 0 || string(attach.RPC.Data) == "null" {
		t.Fatalf("attach returned no observation: %#v", attach)
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(attach.RPC.Data, &observation); err != nil {
		t.Fatalf("decode attach observation %s: %v", attach.RPC.Data, err)
	}
	if observation.HostID != "host-a" || observation.HostGeneration != 4 {
		t.Fatalf("observation names host %q generation %d, want host-a at 4", observation.HostID, observation.HostGeneration)
	}
	if observation.TenantID != composeTenant || observation.SessionID != composeSession {
		t.Fatalf("observation names %s/%s, want %s/%s", observation.TenantID, observation.SessionID, composeTenant, composeSession)
	}
	if observation.Residency != sessionwire.SessionResidencyResident || !observation.Accepting {
		t.Fatalf("observation reports %s/accepting=%v, want resident and accepting", observation.Residency, observation.Accepting)
	}
	if observation.LeaseEpoch == 0 {
		t.Fatal("attach observation carries no residency lease epoch")
	}

	// THE OTHER PARTY READS THE SAME REGISTRATION FROM THE MEMSTORE. This
	// checks both the durable resident state and the observation's fencing
	// epoch, not merely the reply Host constructed in memory.
	entry, err := other.GetHostRegistration(t.Context(), sessionstore.GetHostRegistrationRequest{
		TenantID: composeTenant, SessionID: composeSession,
	})
	if err != nil {
		t.Fatalf("read resident registration: %v", err)
	}
	if entry.Registration.Route == nil || entry.Registration.Route.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("durable registration route = %+v, want resident", entry.Registration.Route)
	}
	durable, err := entry.Registration.Observation()
	if err != nil {
		t.Fatalf("project durable registration: %v", err)
	}
	if durable.LeaseEpoch != observation.LeaseEpoch {
		t.Fatalf("durable observation epoch = %d, wire attach epoch = %d", durable.LeaseEpoch, observation.LeaseEpoch)
	}

	// BIND WITH THE EPOCH THE ATTACH RETURNED, THEN SUBSCRIBE TO THE SESSION'S
	// CHANNEL. A command is a channel RPC, not a HostLink method constant.
	bind := rpcOver(t, link, 3, sessionwire.HostLinkMethodBind, sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: composeTenant, SessionID: composeSession,
		HostID: "host-a", HostGeneration: 4, LeaseEpoch: observation.LeaseEpoch,
		RuntimeCompatibilityID: string(composeCompat), IdempotencyKey: "bind-1",
	})
	assertAcceptedRPC(t, bind, "bind")
	channel := sessionwire.HostLinkChannel(composeTenant, composeSession)
	subscribe := sendOver(t, link, map[string]any{
		"id": 4, "subscribe": map[string]any{"channel": channel},
	}, 4)
	if subscribe.Error != nil {
		t.Fatalf("subscribe to %q: %#v", channel, *subscribe.Error)
	}
	command := rpcOver(t, link, 5, channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-1"})
	assertAcceptedRPC(t, command, "channel command")

	publication := sessionwire.EnduringPublication{
		TenantID: composeTenant, SessionID: composeSession,
		EventID: "event-1", JournalSeq: 1, CoveredThrough: 1,
		Body: json.RawMessage(`{"kind":"published"}`),
	}
	select {
	case runtime.published <- publication:
	case <-time.After(5 * time.Second):
		t.Fatal("Host did not subscribe the resident runtime's committed publication tail")
	}
	got := awaitPublicationOver(t, link, channel)
	if got.EventID != publication.EventID || got.JournalSeq != publication.JournalSeq || string(got.Body) != string(publication.Body) {
		t.Fatalf("publication receipt = %+v, want %+v", got, publication)
	}

	// THE CONTROLS: the attach launched exactly one runtime, and the request
	// was the CREATE shape carried all the way to the rig.
	if launches := f.rig.Launches(); launches != 1 {
		t.Fatalf("the rig launched %d times, want 1", launches)
	}
	if creates := f.rig.Creates(); len(creates) != 1 || creates[0].SessionID != composeSession {
		t.Fatalf("the launch was %+v, want exactly one CREATE of %s", creates, composeSession)
	}

	// THE METRICS ARE THIS HOST'S: served, and not the default registry's.
	recorder := httptest.NewRecorder()
	service.Routes().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "go_goroutines") {
		t.Fatalf("/metrics answered %d with go_goroutines=%v; want 200 and only this Host's series", recorder.Code, strings.Contains(recorder.Body.String(), "go_goroutines"))
	}

	report, err := service.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if report.State != sessionwire.HostLinkDrainStateDrained || len(report.Failures) != 0 {
		t.Fatalf("Stop reported %+v, want drained with no failures", report)
	}
	if released := f.session.Released(); released != 1 {
		t.Fatalf("drain released the runtime %d times, want 1", released)
	}
	_, err = other.GetHostRegistration(t.Context(), sessionstore.GetHostRegistrationRequest{
		TenantID: composeTenant, SessionID: composeSession,
	})
	if err == nil {
		t.Fatal("registration remained routable after drain")
	}
	var released *sessionstore.RegistryError
	if !errors.As(err, &released) || released.Code != sessionstore.RegistryErrorReleased {
		t.Fatalf("registration after drain = %v, want a released tombstone", err)
	}
	if service.Live() || service.Ready() {
		t.Fatal("a stopped Service still reports live or ready")
	}
}

// TestAnAttachAgainstAnotherHoldersLeaseCarriesThatHoldersEpoch is the
// cross-store form of the epoch obligation: ANOTHER Host, over the same
// backend, holds the residency, and this Host's Attach reports epoch_mismatch
// naming that holder's epoch exactly — read from the released store, not a
// fake. It is the two-Host race `tests` will drive, one half at a time.
func TestAnAttachAgainstAnotherHoldersLeaseCarriesThatHoldersEpoch(t *testing.T) {
	f := newComposeFixture(t)
	other := f.otherParty(t)
	f.seedDispositionSession(t, other, composeSession)
	holder, err := other.AcquireResidency(t.Context(), sessionstore.AcquireResidencyRequest{TenantID: composeTenant, SessionID: composeSession})
	if err != nil {
		t.Fatalf("the other Host could not take the lease: %v", err)
	}
	defer func() { _ = holder.Release(context.Background()) }()

	service, err := host.Compose(t.Context(), f.blueprint(t))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	defer service.Stop(context.Background()) //nolint:errcheck // cleanup
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err = service.Attach(t.Context(), host.AttachRequest{
		TenantID: composeTenant, SessionID: composeSession, AgentID: composeAgent,
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "test-service",
	})
	var refused *host.AttachError
	if !errors.As(err, &refused) {
		t.Fatalf("Attach against a held lease = %v, want *host.AttachError", err)
	}
	code, published := refused.HostLinkCode()
	if !published || code != sessionwire.HostLinkErrorEpochMismatch {
		t.Fatalf("refusal class = (%q, %v), want epoch_mismatch", code, published)
	}
	if refused.CurrentLeaseEpoch != uint64(holder.Epoch()) || holder.Epoch() == 0 {
		t.Fatalf("CurrentLeaseEpoch = %d, want the other holder's %d", refused.CurrentLeaseEpoch, holder.Epoch())
	}
	if f.rig.Launches() != 0 {
		t.Fatal("a refused lease still launched a runtime")
	}
	// THE CONTROL: release the other holder and the same attach gets PAST the
	// lease and completes. The refusal above was therefore about the holder
	// and not about this Host.
	if err := holder.Release(t.Context()); err != nil {
		t.Fatalf("release the other holder: %v", err)
	}
	// Keep the composed runtime's committed tail live for the successful
	// post-contention attach. The first half of this test intentionally uses
	// the real store holder; this control proves the later result is a genuine
	// attach rather than another lease refusal.
	f.rig.Session = &composePublishingSession{
		FullSession: f.session,
		published:   make(chan sessionwire.EnduringPublication),
	}
	residency, err := service.Attach(t.Context(), host.AttachRequest{
		TenantID: composeTenant, SessionID: composeSession, AgentID: composeAgent,
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "test-service",
	})
	if err != nil {
		t.Fatalf("after the holder released, Attach = %v, want success", err)
	}
	if residency.LeaseEpoch == 0 || residency.SessionID != composeSession {
		t.Fatalf("after the holder released, Attach returned %+v, want a resident session with an epoch", residency)
	}
	if f.rig.Launches() != 1 {
		t.Fatalf("after the holder released the rig launched %d times, want 1", f.rig.Launches())
	}
}

// TestRunServesTheProbesAndStopsOnCancellation drives the whole binary path
// through the exported Run: it serves /readyz and /healthz on the listener it
// was handed, and returns nil once the context is cancelled and the drain has
// run.
func TestRunServesTheProbesAndStopsOnCancellation(t *testing.T) {
	f := newComposeFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx, f.blueprint(t), host.ListenOptions{Listener: listener}) }()

	base := "http://" + listener.Addr().String()
	awaitProbe(t, base+"/readyz", http.StatusOK, "accepting")
	awaitProbe(t, base+"/healthz", http.StatusOK, "live")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancellation, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func awaitProbe(t *testing.T, url string, status int, body string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(url) //nolint:gosec // test URL on loopback
		if err == nil {
			payload, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode == status && strings.TrimSpace(string(payload)) == body {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s answered %d %q, want %d %q", url, response.StatusCode, payload, status, body)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("%s: %v", url, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// A minimal HostLink client
// ---------------------------------------------------------------------------

type linkReply struct {
	ID  uint32 `json:"id"`
	RPC *struct {
		Data json.RawMessage `json:"data"`
	} `json:"rpc,omitempty"`
	Connect *json.RawMessage `json:"connect,omitempty"`
	Error   *struct {
		Code    uint32 `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

var linkMu sync.Mutex

func rpcHasEmptyAcceptedBody(reply linkReply) bool {
	return reply.RPC == nil || len(reply.RPC.Data) == 0 || string(reply.RPC.Data) == "null"
}

func assertAcceptedRPC(t *testing.T, reply linkReply, operation string) {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("%s returned a transport error: %#v", operation, *reply.Error)
	}
	if !rpcHasEmptyAcceptedBody(reply) {
		t.Fatalf("%s returned RPC data %s, want the empty accepted shape", operation, reply.RPC.Data)
	}
}

func TestRPCEmptyAcceptanceRejectsAValidHostLinkRefusalBody(t *testing.T) {
	refusal, err := json.Marshal(sessionwire.HostLinkError{
		Code:              sessionwire.HostLinkErrorEpochMismatch,
		CurrentLeaseEpoch: 7,
	})
	if err != nil {
		t.Fatalf("marshal refusal: %v", err)
	}
	var decoded sessionwire.HostLinkError
	if err := json.Unmarshal(refusal, &decoded); err != nil {
		t.Fatalf("the regression body is not a valid HostLinkError: %v", err)
	}
	reply := linkReply{
		RPC: &struct {
			Data json.RawMessage `json:"data"`
		}{Data: refusal},
	}
	if rpcHasEmptyAcceptedBody(reply) {
		t.Fatalf("a valid HostLink refusal body %s was classified as an accepted empty RPC", refusal)
	}
}

func dialHostLink(t *testing.T, serverURL string, tenant sessionwire.TenantID) *websocket.Conn {
	t.Helper()
	// Derived exactly as a Factory derives it: Core's function over the base.
	endpoint, err := sessionwire.HostLinkEndpoint(sessionwire.InternalEndpoint("ws"+strings.TrimPrefix(serverURL, "http")), tenant)
	if err != nil {
		t.Fatalf("derive the HostLink endpoint: %v", err)
	}
	connection, response, err := websocket.DefaultDialer.Dial(string(endpoint), http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if err != nil {
		t.Fatalf("dial %s (response %#v): %v", endpoint, response, err)
	}
	negotiation, _ := json.Marshal(sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	reply := sendOver(t, connection, map[string]any{
		"id":      1,
		"connect": map[string]any{"token": composeSecret, "data": json.RawMessage(negotiation), "name": "test-factory"},
	}, 1)
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect reply = %#v", reply)
	}
	return connection
}

func sendOver(t testing.TB, connection *websocket.Conn, command any, id uint32) linkReply {
	t.Helper()
	linkMu.Lock()
	defer linkMu.Unlock()
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	for {
		_, frame, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read reply %d: %v", id, err)
		}
		var reply linkReply
		if err := json.Unmarshal(frame, &reply); err != nil {
			t.Fatalf("decode %s: %v", frame, err)
		}
		if reply.ID == id {
			return reply
		}
		// A push (no id) may interleave; keep reading for our reply.
	}
}

func rpcOver(t *testing.T, connection *websocket.Conn, id uint32, method string, body any) linkReply {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return sendOver(t, connection, map[string]any{"id": id, "rpc": map[string]any{"method": method, "data": json.RawMessage(data)}}, id)
}

func awaitPublicationOver(t *testing.T, connection *websocket.Conn, channel string) sessionwire.EnduringPublication {
	t.Helper()
	linkMu.Lock()
	defer linkMu.Unlock()
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("publication deadline: %v", err)
	}
	for {
		_, frame, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read publication: %v", err)
		}
		for _, line := range strings.Split(string(frame), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || line == "{}" {
				continue
			}
			var envelope struct {
				Push *struct {
					Channel string `json:"channel"`
					Pub     *struct {
						Data json.RawMessage `json:"data"`
					} `json:"pub"`
				} `json:"push,omitempty"`
			}
			if err := json.Unmarshal([]byte(line), &envelope); err != nil || envelope.Push == nil || envelope.Push.Pub == nil || envelope.Push.Channel != channel {
				continue
			}
			var publication sessionwire.EnduringPublication
			if err := json.Unmarshal(envelope.Push.Pub.Data, &publication); err != nil {
				t.Fatalf("decode publication %s: %v", envelope.Push.Pub.Data, err)
			}
			return publication
		}
	}
}
