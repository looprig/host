package hostlink_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	centrifugeclient "github.com/centrifugal/centrifuge-go"
	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/hub"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnessadapter"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// Only the controller's launch and unrelated command methods are stubs. The
// producer is a real Harness hub backed by a real committed event journal.
type liveBridgeController struct {
	session.SessionController
	id   uuid.UUID
	hub  *hub.Hub
	done chan struct{}
}

func (c *liveBridgeController) SessionID() uuid.UUID { return c.id }
func (c *liveBridgeController) SubscribeEvents(filter event.EventFilter) (event.Subscription, error) {
	return c.hub.SubscribeEvents(filter)
}
func (c *liveBridgeController) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return c, c.hub.CommittedPublicEventsSupported()
}
func (c *liveBridgeController) SubscribeCommittedPublicEvents(filter event.EventFilter) (event.Subscription, error) {
	return c.hub.SubscribeCommittedPublicEvents(filter)
}
func (*liveBridgeController) WaitIdle(context.Context) error         { return nil }
func (c *liveBridgeController) Done() <-chan struct{}                { return c.done }
func (*liveBridgeController) ReleaseResidency(context.Context) error { return nil }
func (*liveBridgeController) LeaseEpoch() (uint64, bool)             { return 1, true }
func (*liveBridgeController) PersistenceFaulted() <-chan struct{}    { return nil }
func (*liveBridgeController) PersistenceFault() error                { return nil }
func (*liveBridgeController) AbandonResidency(context.Context) error { return nil }

type liveBridgeLauncher struct{ controller session.SessionController }

func (l liveBridgeLauncher) NewSession(context.Context, ...rig.SessionOption) (session.SessionController, error) {
	return l.controller, nil
}
func (l liveBridgeLauncher) RestoreSession(context.Context, uuid.UUID) (session.SessionController, error) {
	return l.controller, nil
}

type liveBridgeRigs struct{ launcher harnessadapter.Launcher }

func (r liveBridgeRigs) RigForCreate(context.Context, department.RigCreateRequest) (harnessadapter.Launcher, error) {
	return r.launcher, nil
}
func (r liveBridgeRigs) RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (harnessadapter.Launcher, error) {
	return r.launcher, nil
}

type liveBridgeSession struct {
	key     registry.Key
	runtime department.PublicationSubscriber
	hub     *hub.Hub
	rigID   uuid.UUID
	factory *event.Factory
}

func newLiveBridgeSession(t *testing.T, tenant sessionwire.TenantID) *liveBridgeSession {
	t.Helper()
	rigID, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	store, err := harnessstore.Open(memstore.New(), harnessstore.WithTenant(tenant))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(t.Context(), rigID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
	sessionJournal, err := store.OpenJournal(t.Context(), rigID, lease)
	if err != nil {
		t.Fatal(err)
	}
	appender, err := journal.NewJournalEventAppenderChecked(sessionJournal)
	if err != nil || !appender.SupportsCommittedPublicBodies() {
		t.Fatalf("committed Harness appender = %v, err %v", appender, err)
	}
	sessionHub := hub.New(rigID, hub.WithAppender(appender))
	controller := &liveBridgeController{id: rigID, hub: sessionHub, done: make(chan struct{})}
	adapter, err := harnessadapter.New(liveBridgeRigs{launcher: liveBridgeLauncher{controller: controller}})
	if err != nil {
		t.Fatal(err)
	}
	key := registry.Key{TenantID: tenant, SessionID: sessionwire.SessionID(rigID.String())}
	launched, err := adapter.NewSession(t.Context(), department.RigCreateRequest{TenantID: tenant, SessionID: key.SessionID, AgentID: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	runtime, ok := launched.(department.PublicationSubscriber)
	if !ok {
		t.Fatalf("adapted Harness runtime %T lacks publications", launched)
	}
	return &liveBridgeSession{key: key, runtime: runtime, hub: sessionHub, rigID: rigID, factory: event.NewFactory(uuid.New, time.Now)}
}

type acceptingHostLinkAuth struct{}

func (acceptingHostLinkAuth) VerifyTenant(_ context.Context, _ sessionwire.TenantID, token string) error {
	if token != "integration-credential" {
		return errors.New("bad token")
	}
	return nil
}

type openHostAdmission struct{}

func (openHostAdmission) Draining() bool { return false }

type unusedCommandConsumers struct{}

func (unusedCommandConsumers) ConsumerFor(registry.Key) (hostlink.CommandConsumer, bool) {
	return nil, false
}

func TestHarnessLiveTextArrivesOnConcreteHostLinkInOrder(t *testing.T) {
	live := newLiveBridgeSession(t, "tenant-alpha")
	const hostID = sessionwire.HostID("host-live-text")
	const compatibility = "runtime-live-text"
	index := registry.New(frozenClock{at: time.Now()})
	if _, inserted := index.Insert(live.key, registry.Admission{
		AgentID: "agent-a", CompatibilityID: compatibility, LeaseEpoch: 1,
	}); !inserted {
		t.Fatal("insert live residency")
	}
	mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID: live.key.TenantID, HostID: hostID, HostGeneration: 1,
		Residencies: index, Admission: openHostAdmission{}, Consumers: unusedCommandConsumers{},
		MaxBindingsPerLink: 1, MaxBindings: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := hostlink.NewCentrifugeServer(hostlink.Config{
		TenantID: live.key.TenantID, Authenticator: acceptingHostLinkAuth{}, Multiplexer: mux,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Errorf("close HostLink: %v", err)
		}
	})

	connectData, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := centrifugeclient.NewJsonClient(strings.Replace(httpServer.URL, "http://", "ws://", 1)+"?format=json", centrifugeclient.Config{
		Token: "integration-credential", Data: connectData,
	})
	t.Cleanup(client.Close)
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugeclient.ConnectedEvent) { connected <- struct{}{} })
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("Go Centrifuge client did not connect")
	}
	bind, err := json.Marshal(sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: live.key.TenantID,
		SessionID: live.key.SessionID, HostID: hostID, HostGeneration: 1,
		LeaseEpoch: 1, RuntimeCompatibilityID: compatibility, IdempotencyKey: "live-bind",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := client.RPC(ctx, hostlink.MethodBind, bind); err != nil {
		t.Fatalf("bind over Go Centrifuge client: %v", err)
	}
	channel := hostlink.ChannelFor(live.key)
	subscription, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatal(err)
	}
	subscribed := make(chan struct{}, 1)
	publications := make(chan []byte, 3)
	subscription.OnSubscribed(func(centrifugeclient.SubscribedEvent) { subscribed <- struct{}{} })
	subscription.OnPublication(func(ev centrifugeclient.PublicationEvent) {
		publications <- append([]byte(nil), ev.Data...)
	})
	if err := subscription.Subscribe(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("Go Centrifuge client did not subscribe")
	}

	ids := publicbody.Identities{RuntimeSessionID: live.rigID, SessionID: live.key.SessionID}
	rewrite := publicbody.Projection(ids)
	tails, err := service.NewTails(service.TailOptions{Publications: server, Routes: mux})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), live.key, live.runtime, rewrite, rewrite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	header, err := live.factory.Stamp(event.Header{Coordinates: identity.Coordinates{
		SessionID: live.rigID, LoopID: uuid.MustParse("11111111-2222-3333-4444-555555555555"), TurnID: uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.hub.PublishEventChecked(t.Context(), event.TokenDelta{Header: header, Chunk: &content.TextChunk{Text: "visible preview"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-publications:
		if !bytes.Contains(frame, []byte(`"type":"ephemeral_publication"`)) || !bytes.Contains(frame, []byte("visible preview")) {
			t.Fatalf("first frame = %s, want live text", frame)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Go Centrifuge client did not receive live text")
	}
	enduringHeader, err := live.factory.Stamp(event.Header{Coordinates: identity.Coordinates{SessionID: live.rigID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.hub.PublishEventChecked(t.Context(), event.SessionStarted{Header: enduringHeader}); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-publications:
		if !bytes.Contains(frame, []byte(`"event_id"`)) {
			t.Fatalf("second frame = %s, want enduring event", frame)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Go Centrifuge client did not receive enduring event")
	}
}
