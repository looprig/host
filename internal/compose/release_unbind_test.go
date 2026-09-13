package compose

import (
	"slices"
	"testing"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// This file is the unbind that SHIPPED WITH the drain's transport change, and
// the dependency is the whole reason it exists.
//
// BEFORE that change the drain closed every tenant transport as its last step,
// and closing a transport drops its links' routes as a side effect — so a
// session released by a drain left no binding behind, and nothing needed to say
// so. The transport now survives a link-initiated drain. Measured on the
// composed Host before this unbind existed: after a drain released the fixed
// session, `Replicas(keyA)` still returned the bound link.
//
// WHY A DANGLING ROUTE IS NOT COSMETIC. A binding is a Factory replica's claim
// to route work to a session ON THIS HOST. One outliving the residency means a
// Factory holds a route to a session this Host does not hold, on a Host that is
// still up and still answering, so the claim is not even self-correcting by
// disconnection. `Tails.invalidate` already calls InvalidateSession when a tail
// is LOST — this is the same drop for the case where Host itself ended it,
// which `relay` deliberately does NOT treat as a loss.

// TestReleasingASessionDropsEveryRouteToIt is the property, over a drain that
// leaves the transport open.
func TestReleasingASessionDropsEveryRouteToIt(t *testing.T) {
	runtime := newPublishingSession()
	f := dedicatedDrainFixture(t)
	f.rig.Session = runtime
	f.start()
	server := f.serve()
	f.attach(tenantA, sessionA)

	link := dialFactoryLink(t, server.URL, tenantA)
	accepted(t, link.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))
	owner, err := f.svc.links.resolve(tenantA)
	if err != nil {
		t.Fatalf("resolving the owning tenant's link: %v", err)
	}
	// NON-VACUITY: there is a route to drop. Without this the assertion below
	// is satisfied by a bind that never took effect.
	if replicas := owner.mux.Replicas(keyA); len(replicas) != 1 {
		t.Fatalf("%d replicas are bound to the fixed session before the drain, want 1: there is no route for the release to drop", len(replicas))
	}

	_ = drainObservationOf(t, link.rpc(3, hostlink.MethodDrain, fixedSessionDrainRequest("unbind")))
	f.svc.drainer.Wait()

	if replicas := owner.mux.Replicas(keyA); len(replicas) != 0 {
		t.Errorf("%v still route to the fixed session after the drain released it: a Factory replica holds a binding to a session this Host no longer holds", replicas)
	}
	// AND THE DROP IS THE RELEASE'S, NOT THE TRANSPORT'S. If the link had
	// closed, the routes would have gone with it and the assertion above would
	// be measuring the old behaviour under a new name.
	if live := len(f.svc.links.all()); live == 0 {
		t.Fatal("no tenant link survived the drain, so the unbind asserted above may be the transport close rather than the release")
	}
}

// TestReleasingOneSessionLeavesEveryOtherSessionSRoutesAlone is the key-scoping
// row, and it is the one that makes the test above a statement about a SESSION
// rather than about a link.
//
// InvalidateSession takes a key and an unbind that ignored it would satisfy the
// test above perfectly while dropping every route the link holds — a blast
// radius of one link instead of one session. It is measured on a POOLED fixture
// because a dedicated Host is pinned to one resident session and therefore has
// no second binding for a too-wide unbind to damage.
func TestReleasingOneSessionLeavesEveryOtherSessionSRoutesAlone(t *testing.T) {
	f := newFixture(t)
	f.rig.Session = newPublishingSession()
	f.start()
	server := f.serve()
	f.attach(tenantA, sessionA)
	f.attach(tenantA, sessionB)

	keyB := registry.Key{TenantID: tenantA, SessionID: sessionB}
	link := dialFactoryLink(t, server.URL, tenantA)
	accepted(t, link.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))
	accepted(t, link.rpc(3, hostlink.MethodBind, f.bindRequest(tenantA, sessionB, 9)))
	owner, err := f.svc.links.resolve(tenantA)
	if err != nil {
		t.Fatalf("resolving the tenant's link: %v", err)
	}
	if len(owner.mux.Replicas(keyA)) != 1 || len(owner.mux.Replicas(keyB)) != 1 {
		t.Fatalf("the two sessions are bound %d and %d times, want one each", len(owner.mux.Replicas(keyA)), len(owner.mux.Replicas(keyB)))
	}

	held := f.svc.ResidentSessions()
	index := slices.IndexFunc(held, func(session lifecycle.Session) bool { return session.Key() == keyA })
	if index == -1 {
		t.Fatalf("session-a is not resident, so nothing here releases it: %v", held)
	}
	if err := held[index].ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("releasing session-a: %v", err)
	}

	if replicas := owner.mux.Replicas(keyA); len(replicas) != 0 {
		t.Errorf("%v still route to the released session", replicas)
	}
	if replicas := owner.mux.Replicas(keyB); len(replicas) != 1 {
		t.Errorf("releasing session-a left %d replicas routing to session-b, want 1: THE UNBIND IS SCOPED TO THE LINK RATHER THAN TO THE SESSION, so releasing one session cuts a Factory's routes to every other session on the same connection", len(replicas))
	}
}
