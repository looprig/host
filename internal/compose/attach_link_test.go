package compose

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// The attach RPC, composed: the one production path that makes a session
// resident on a Host a Factory reaches over HostLink
// ---------------------------------------------------------------------------
//
// Until host v0.2.0 compose.Service.Attach had no non-test caller and a
// bootstrapped Host could never make a session resident (B4). Every test here
// drives hostlink.attach over a REAL authenticated link into the composed
// Service, so what is asserted is the whole path a Factory takes and not the
// Multiplexer's ladder or the manager's sequence on their own — both of which
// are held by their own packages.

// attachedObservation issues one attach RPC and decodes its accepted body.
func attachedObservation(t *testing.T, link *factoryLink, id uint32, request sessionwire.HostLinkAttachRequest) sessionwire.HostLinkRegistryObservation {
	t.Helper()
	reply := link.rpc(id, hostlink.MethodAttach, request)
	if reply.Error != nil {
		t.Fatalf("attach failed at the transport: %#v", *reply.Error)
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 || string(reply.RPC.Data) == "null" {
		t.Fatalf("accepted attach returned no body: %#v", reply)
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(reply.RPC.Data, &observation); err != nil {
		t.Fatalf("accepted attach body %s is not a Core registry observation: %v", reply.RPC.Data, err)
	}
	return observation
}

// refusedAttachOverLink issues one attach RPC and returns the Core class it was
// refused with, failing if it was accepted or failed at the transport.
func refusedAttachOverLink(t *testing.T, link *factoryLink, id uint32, request sessionwire.HostLinkAttachRequest) sessionwire.HostLinkError {
	t.Helper()
	reply := link.rpc(id, hostlink.MethodAttach, request)
	refusal, published := refusalOf(t, reply)
	if !published {
		t.Fatalf("attach was not refused with a Core class: %#v", reply)
	}
	return refusal
}

// TestAnAttachOverHostLinkMakesTheSessionResidentAndBindable is the round
// trip B4 was missing: a Factory attaches over the link, binds with the epoch
// the reply carried, subscribes, delivers a command, and receives the
// session's live publication — with no in-process call anywhere.
func TestAnAttachOverHostLinkMakesTheSessionResidentAndBindable(t *testing.T) {
	runtime := newPublishingSession()
	f := newFixture(t)
	f.rig.Session = runtime
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	// NOTHING IS RESIDENT BEFORE THE RPC, so the observation below is the
	// attach's doing and not the fixture's.
	if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
		t.Fatalf("the registry holds %d residencies before any attach", len(entries))
	}

	observation := attachedObservation(t, link, 2, f.attachRequest(tenantA, sessionA))

	// THE BODY IS THE §15 OBSERVATION A BIND NEEDS, member by member.
	if observation.HostID != f.host.ID() || observation.HostGeneration != testGen {
		t.Errorf("observation names host %q generation %d, want this Host %q at %d", observation.HostID, observation.HostGeneration, f.host.ID(), testGen)
	}
	if observation.TenantID != tenantA || observation.SessionID != sessionA {
		t.Errorf("observation names %s/%s, want %s/%s", observation.TenantID, observation.SessionID, tenantA, sessionA)
	}
	if observation.LeaseEpoch != 9 {
		// 9 is the fixture store's grant. It is what the durable projection
		// carries and what a bind must present.
		t.Errorf("observation lease_epoch = %d, want the store's grant 9", observation.LeaseEpoch)
	}
	if observation.Residency != sessionwire.SessionResidencyResident || !observation.Accepting {
		t.Errorf("observation reports %s/accepting=%v, want resident and accepting", observation.Residency, observation.Accepting)
	}
	if observation.AgentID != testAgent || observation.RuntimeCompatibilityID != string(testCompat) {
		t.Errorf("observation names agent %q build %q, want %q %q", observation.AgentID, observation.RuntimeCompatibilityID, testAgent, testCompat)
	}
	if observation.Placement != f.host.Placement() || observation.InternalEndpoint != f.host.InternalEndpoint() {
		t.Errorf("observation carries placement %q endpoint %q, want the Host's", observation.Placement, observation.InternalEndpoint)
	}
	// AND IT IS THE SAME DERIVATION THE MANAGER PUBLISHED DURABLY: the last
	// row the store holds for this key agrees on every routing member.
	var durable *sessionwire.HostLinkRegistryObservation
	for _, row := range f.store.published() {
		if row.TenantID == tenantA && row.SessionID == sessionA {
			copied := row
			durable = &copied
		}
	}
	if durable == nil {
		t.Fatal("the store holds no durable observation for the attached session")
	}
	if durable.LeaseEpoch != observation.LeaseEpoch || durable.HostID != observation.HostID ||
		durable.HostGeneration != observation.HostGeneration || durable.Residency != observation.Residency ||
		durable.Accepting != observation.Accepting || durable.InternalEndpoint != observation.InternalEndpoint {
		t.Errorf("the reply %+v disagrees with the durable projection %+v", observation, *durable)
	}

	// THE COMPOSITION OWNS THE SESSION: tracked, with a running consumer.
	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	if _, running := f.svc.ConsumerFor(key); !running {
		t.Fatal("no durable command consumer is registered for the session the RPC attached")
	}

	// BIND WITH THE RETURNED EPOCH, THEN DELIVER, THEN RECEIVE — the three
	// things Core says the observation exists to make possible.
	accepted(t, link.rpc(3, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, observation.LeaseEpoch)))
	channel := hostlink.ChannelFor(key)
	if reply := link.subscribe(4, channel); reply.Error != nil {
		t.Fatalf("subscribe on the attached session's channel was refused: %#v", *reply.Error)
	}
	accepted(t, link.rpc(5, channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}))
	runtime.commit(t, enduring(key, 1, `{"kind":"first"}`))
	if got := link.awaitPushes(1); got[0].Channel != channel {
		t.Fatalf("publication arrived on %q, want %q", got[0].Channel, channel)
	}

	// THE CONTROL: a bind with an epoch other than the one the reply carried
	// is refused, so the accepted bind above was a statement about the value
	// the attach returned and not about any epoch at all.
	stale := refusedBind(t, link.rpc(6, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, observation.LeaseEpoch+1)))
	if stale.Code != sessionwire.HostLinkErrorEpochMismatch || stale.CurrentLeaseEpoch != observation.LeaseEpoch {
		t.Fatalf("the control bind was refused %#v, want epoch_mismatch naming %d", stale, observation.LeaseEpoch)
	}
}

func refusedBind(t *testing.T, reply wireReply) sessionwire.HostLinkError {
	t.Helper()
	refusal, published := refusalOf(t, reply)
	if !published {
		t.Fatalf("bind was not refused with a Core class: %#v", reply)
	}
	return refusal
}

// TestTwoAttachesForOneSessionOverHostLinkSettleOnOneResidency is the race a
// Factory with two replicas can produce, driven over two real links at once.
//
// THE MANAGER'S PER-KEY SERIALIZATION IS WHAT IS BEING MEASURED, from the wire:
// both callers are answered with the SAME observation, exactly one runtime is
// launched, and a third attach — serial, after both — is answered identically
// again. The wire carries no Attached member, so "idempotent" is asserted as
// one launch and one registry row rather than as a flag.
func TestTwoAttachesForOneSessionOverHostLinkSettleOnOneResidency(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	first := dialFactoryLink(t, server.URL, tenantA)
	second := dialFactoryLink(t, server.URL, tenantA)

	start := make(chan struct{})
	var parked, done sync.WaitGroup
	results := make([]sessionwire.HostLinkRegistryObservation, 2)
	for index, link := range []*factoryLink{first, second} {
		parked.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			parked.Done()
			<-start
			results[index] = attachedObservation(t, link, 2, f.attachRequest(tenantA, sessionA))
		}()
	}
	parked.Wait()
	close(start)
	done.Wait()

	if results[0] != results[1] {
		t.Fatalf("the two racing attaches were answered differently:\n%+v\n%+v", results[0], results[1])
	}
	if results[0].LeaseEpoch == 0 {
		t.Fatal("the shared observation carries no lease epoch")
	}
	if launches := f.rig.Launches(); launches != 1 {
		t.Errorf("the rig was asked to launch %d times for two attaches of one session, want 1", launches)
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 1 {
		t.Errorf("the registry holds %d residencies, want 1", len(entries))
	}

	// THE SERIAL REPEAT is the warm path from the wire: same observation, no
	// new launch.
	again := attachedObservation(t, first, 3, f.attachRequest(tenantA, sessionA))
	if again != results[0] {
		t.Fatalf("a repeated attach was answered %+v, want the existing residency %+v", again, results[0])
	}
	if launches := f.rig.Launches(); launches != 1 {
		t.Errorf("the repeated attach launched again (%d launches)", launches)
	}

	// THE CONTROL for the counts: a different session launches once more.
	other := attachedObservation(t, second, 3, f.attachRequest(tenantA, "session-control"))
	if other.SessionID != "session-control" || f.rig.Launches() != 2 {
		t.Fatalf("the control attach answered %+v with %d launches; the counts above cannot distinguish one from any number", other, f.rig.Launches())
	}
}

// TestAnAttachRefusedByAnotherHoldersLeaseCarriesThatHoldersEpoch is the
// obligation Core placed on this Host: epoch_mismatch must carry the OTHER
// holder's epoch, and Core refuses the class with a zero. The epoch is
// asserted EXACTLY — 42, which is neither this fixture's own grant (9) nor a
// zero — because "not zero" would pass a build that published the wrong
// number.
//
// The control is the shape of contention a different SessionLeases could
// produce: the sentinel with no provider error in the chain. The Host then
// has no epoch to publish and answers a different, publishable class rather
// than an invalid body.
func TestAnAttachRefusedByAnotherHoldersLeaseCarriesThatHoldersEpoch(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	f.store.holdElsewhere(key, 42)
	refusal := refusedAttachOverLink(t, link, 2, f.attachRequest(tenantA, sessionA))
	if refusal.Code != sessionwire.HostLinkErrorEpochMismatch {
		t.Fatalf("refusal class = %q, want epoch_mismatch", refusal.Code)
	}
	if refusal.CurrentLeaseEpoch != 42 {
		t.Fatalf("current_lease_epoch = %d, want the other holder's 42", refusal.CurrentLeaseEpoch)
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
		t.Fatalf("a refused attach left %d residencies", len(entries))
	}
	if launches := f.rig.Launches(); launches != 0 {
		t.Fatalf("a lease refused elsewhere still launched %d runtimes", launches)
	}

	// THE CONTROL: contention with no provider epoch in the chain.
	other := sessionwire.SessionID("session-opaque")
	f.store.holdElsewhereWithoutEpoch(registry.Key{TenantID: tenantA, SessionID: other})
	downgraded := refusedAttachOverLink(t, link, 3, f.attachRequest(tenantA, other))
	if downgraded.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("a held lease with no readable holder epoch was refused %q, want runtime_unavailable (Core refuses epoch_mismatch with a zero epoch)", downgraded.Code)
	}
	if downgraded.CurrentLeaseEpoch != 0 {
		t.Fatalf("the downgraded refusal carries epoch %d out of nowhere", downgraded.CurrentLeaseEpoch)
	}
}

// TestAnAttachNamingAnotherHostIncarnationTakesNoLease is Core's fence:
// "a Host that is not that incarnation must refuse the attach before it
// acquires any lease". The lease acquisition is the store trace's
// `lease.acquire`, and its absence is measured against a control that shows
// the same trace records one for the correct tuple.
func TestAnAttachNamingAnotherHostIncarnationTakesNoLease(t *testing.T) {
	for _, test := range []struct {
		name    string
		perturb func(*sessionwire.HostLinkAttachRequest)
	}{
		{name: "another host", perturb: func(r *sessionwire.HostLinkAttachRequest) { r.HostID = "host-somebody-else" }},
		{name: "an earlier incarnation", perturb: func(r *sessionwire.HostLinkAttachRequest) { r.HostGeneration = testGen - 1 }},
		{name: "a later incarnation", perturb: func(r *sessionwire.HostLinkAttachRequest) { r.HostGeneration = testGen + 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			f.start()
			server := f.serve()
			link := dialFactoryLink(t, server.URL, tenantA)

			request := f.attachRequest(tenantA, sessionA)
			test.perturb(&request)
			refusal := refusedAttachOverLink(t, link, 2, request)
			if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
				t.Fatalf("refusal class = %q, want runtime_unavailable, the class a bind to another Host earns", refusal.Code)
			}
			if got := f.trace.indexOf("lease.acquire"); got != -1 {
				t.Fatalf("a mis-addressed attach reached the lease store (trace %v); Core requires the fence BEFORE any lease", f.trace.trace())
			}
			if launches := f.rig.Launches(); launches != 0 {
				t.Fatalf("a mis-addressed attach launched %d runtimes", launches)
			}
			if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
				t.Fatalf("a mis-addressed attach left %d residencies", len(entries))
			}

			// THE CONTROL: the correct tuple, on the same link, reaches the
			// lease store exactly once.
			attachedObservation(t, link, 3, f.attachRequest(tenantA, sessionA))
			if got := f.trace.indexOf("lease.acquire"); got == -1 {
				t.Fatalf("the control attach never reached the lease store, so the absence above is the harness's and not the fence's")
			}
		})
	}
}

// TestAnAttachForAForeignTenantIsRefusedBeforeAnyLease is R-1 applied to the
// attach: every link is authenticated for one tenant, and a request naming
// another is refused with the same uninformative class a foreign bind gets,
// before the store is consulted about anything.
func TestAnAttachForAForeignTenantIsRefusedBeforeAnyLease(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	foreign := f.attachRequest(tenantB, sessionA)
	refusal := refusedAttachOverLink(t, link, 2, foreign)
	if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("a foreign tenant's attach was refused %q, want runtime_unavailable", refusal.Code)
	}
	if got := f.trace.indexOf("lease.acquire"); got != -1 {
		t.Fatalf("a foreign tenant's attach reached the lease store (trace %v)", f.trace.trace())
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
		t.Fatalf("a foreign tenant's attach left %d residencies", len(entries))
	}

	// THE CONTROL: the same session under the link's own tenant attaches.
	observation := attachedObservation(t, link, 3, f.attachRequest(tenantA, sessionA))
	if observation.TenantID != tenantA {
		t.Fatalf("the control attached %s, want %s", observation.TenantID, tenantA)
	}
	// AND TENANT B, ON ITS OWN LINK, IS NOT AFFECTED: the refusal was about the
	// link's tenant, not about tenant B's existence or standing.
	linkB := dialFactoryLink(t, server.URL, tenantB)
	if other := attachedObservation(t, linkB, 2, f.attachRequest(tenantB, sessionA)); other.TenantID != tenantB {
		t.Fatalf("tenant B's own attach answered %s", other.TenantID)
	}
}

// TestAnAttachWhoseLaunchFailsCarriesNoClass preserves the manager's
// launchCode ruling on the wire: a runtime that refuses to launch is a Host
// defect, not a placement outcome, so the RPC fails at the transport with an
// INTERNAL error and never publishes a HostLinkError a Factory would re-place
// on. The lease taken for the attempt is released.
func TestAnAttachWhoseLaunchFailsCarriesNoClass(t *testing.T) {
	f := newFixture(t)
	f.rig.NewErr = errors.New("the rig refused to launch")
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	reply := link.rpc(2, hostlink.MethodAttach, f.attachRequest(tenantA, sessionA))
	if reply.Error == nil {
		body := ""
		if reply.RPC != nil {
			body = string(reply.RPC.Data)
		}
		t.Fatalf("a failed launch was answered with a body %q, want a transport error", body)
	}
	if reply.Error.Code != 100 {
		t.Fatalf("a failed launch answered transport code %d, want 100 (internal, this Host's fault)", reply.Error.Code)
	}
	if acquired, released := f.trace.indexOf("lease.acquire"), f.trace.indexOf("lease.release"); acquired == -1 || released < acquired {
		t.Fatalf("the failed attach's lease was not released after acquisition (trace %v)", f.trace.trace())
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
		t.Fatalf("a failed launch left %d residencies", len(entries))
	}
}

// TestTheAttachModeMappingCoversCoresEnumeration holds the composition's mode
// switch to Core's two members and to the manager's two, and refuses a third.
func TestTheAttachModeMappingCoversCoresEnumeration(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		wire sessionwire.HostLinkAttachMode
		want residency.Mode
	}{
		{sessionwire.HostLinkAttachModeCreate, residency.ModeCreate},
		{sessionwire.HostLinkAttachModeRestore, residency.ModeRestore},
	} {
		got, err := attachMode(row.wire)
		if err != nil || got != row.want {
			t.Errorf("attachMode(%q) = (%q, %v), want (%q, nil)", row.wire, got, err, row.want)
		}
	}
	if _, err := attachMode("resume"); err == nil {
		t.Fatal("a mode Core does not define was mapped rather than refused")
	}
	var refusal *hostlink.AttachRefusal
	if _, err := attachMode(""); !errors.As(err, &refusal) || refusal.Code != "" {
		t.Fatalf("an unmapped mode returned %v, want an AttachRefusal with no class", err)
	}
}

// TestARepeatedAttachReportsTheRegistryAsItStands is the warm path's reply:
// an attach that finds the session already resident takes nothing, and the
// observation it answers with is the registry's CURRENT row — admission closed
// by a warm release, or a residency already releasing — rather than the row
// the establishing call once saw. A Factory that bound on a stale "accepting"
// would deliver into a session that refuses.
func TestARepeatedAttachReportsTheRegistryAsItStands(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)
	key := registry.Key{TenantID: tenantA, SessionID: sessionA}

	first := attachedObservation(t, link, 2, f.attachRequest(tenantA, sessionA))
	if first.Residency != sessionwire.SessionResidencyResident || !first.Accepting {
		t.Fatalf("the establishing attach reports %s/accepting=%v, want resident and accepting", first.Residency, first.Accepting)
	}
	entry, held := f.svc.registry.Get(key)
	if !held {
		t.Fatal("the registry holds no row for the attached session")
	}

	// A warm release has closed admission: still resident, no longer taking
	// work. The repeated attach says so.
	if _, ok := f.svc.registry.StopAdmitting(key, entry.Generation); !ok {
		t.Fatal("StopAdmitting refused the current generation")
	}
	closed := attachedObservation(t, link, 3, f.attachRequest(tenantA, sessionA))
	if closed.Residency != sessionwire.SessionResidencyResident || closed.Accepting {
		t.Fatalf("after admission closed the repeated attach reports %s/accepting=%v, want resident and NOT accepting", closed.Residency, closed.Accepting)
	}
	if closed.LeaseEpoch != first.LeaseEpoch {
		t.Fatalf("the repeated attach reports epoch %d, want the residency's %d", closed.LeaseEpoch, first.LeaseEpoch)
	}

	// The residency has begun releasing. The repeated attach says that too.
	if _, ok := f.svc.registry.MarkReleasing(key, entry.Generation); !ok {
		t.Fatal("MarkReleasing refused the current generation")
	}
	releasing := attachedObservation(t, link, 4, f.attachRequest(tenantA, sessionA))
	if releasing.Residency != sessionwire.SessionResidencyReleasing || releasing.Accepting {
		t.Fatalf("after release began the repeated attach reports %s/accepting=%v, want releasing and not accepting", releasing.Residency, releasing.Accepting)
	}
	if launches := f.rig.Launches(); launches != 1 {
		t.Fatalf("the repeated attaches launched again (%d launches); the warm path takes nothing", launches)
	}
}
