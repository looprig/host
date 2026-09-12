package compose

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"

	"net/http"

	"github.com/looprig/host/internal/realtime/hostlink"
)

// stubLinks builds a links over a counting builder, so a guard can watch what
// the composition allocates without driving a whole Host.
//
// IT USES THE REAL links AND THE REAL Multiplexer. Only the transport and the
// relay are omitted, because neither participates in the property under test and
// building a Centrifuge node per tenant would make a bound-checking test a
// resource test.
func stubLinks(t *testing.T, max int) (*links, *int) {
	t.Helper()
	built := 0
	residencies := stubResidencies{}
	set := &links{
		clock:    stubClock{},
		max:      max,
		byTenant: map[sessionwire.TenantID]*tenantLink{},
	}
	set.build = func(tenant sessionwire.TenantID) (*tenantLink, error) {
		built++
		mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
			TenantID:           tenant,
			HostID:             "host-a",
			HostGeneration:     4,
			Residencies:        residencies,
			Admission:          stubAdmission{},
			Consumers:          stubConsumers{},
			MaxBindingsPerLink: 2,
			MaxBindings:        2,
		})
		if err != nil {
			return nil, err
		}
		return &tenantLink{tenant: tenant, mux: mux, server: stubServer{}}, nil
	}
	return set, &built
}

// TestOneMultiplexerPerTenantAndNeverOneShared is the composition-level guard
// for human gate H8.
//
// H8, answered 2026-09-04, DELETED host.Options.TenantID: a pooled Host
// advertising cross_tenant_isolated admits several tenants, and the admission
// rule is the isolation class derived from the admitted ledger, never latched to
// whoever arrived first. Everything below the composition already obeys that —
// internal/residency enforces it on the durable path, internal/service in the
// ledger — and NEITHER CAN SEE THIS. A composition that built one Multiplexer
// and handed it to every tenant would reverse H8 with every one of those tests
// still green, which is the reversal O6.2 measured as unguarded.
//
// The property asserted is a BIJECTION and not merely "more than one exists":
// every live tenant maps to a Multiplexer no other tenant maps to, and the
// number of distinct Multiplexers equals the number of distinct tenants. A
// single shared table fails the first half; a composition latched to the first
// tenant fails the second.
func TestOneMultiplexerPerTenantAndNeverOneShared(t *testing.T) {
	t.Parallel()

	tenants := []sessionwire.TenantID{"tenant-a", "tenant-b", "tenant-c"}
	set, built := stubLinks(t, len(tenants))

	for _, tenant := range tenants {
		if _, err := set.resolve(tenant); err != nil {
			t.Fatalf("resolve(%q): %v", tenant, err)
		}
	}

	// The non-vacuity guard. Without it every assertion below is satisfied by a
	// composition that allocated nothing at all.
	if *built != len(tenants) {
		t.Fatalf("the composition built %d tenant links for %d tenants, want one each", *built, len(tenants))
	}

	owners := map[*hostlink.Multiplexer][]sessionwire.TenantID{}
	for _, tenant := range tenants {
		link, held := set.lookup(tenant)
		if !held {
			t.Fatalf("no link is held for %q", tenant)
		}
		owners[link.mux] = append(owners[link.mux], tenant)
	}
	if len(owners) != len(tenants) {
		t.Fatalf("%d tenants share %d Multiplexers, want one each: %v", len(tenants), len(owners), owners)
	}
	for mux, sharing := range owners {
		if len(sharing) != 1 {
			t.Errorf("Multiplexer %p serves %v, want exactly one tenant", mux, sharing)
		}
	}
}

// TestResolvingTheSameTenantTwiceReusesItsMultiplexer is the other half of the
// bijection, and it is a separate test because it fails for the opposite reason.
//
// A composition that built a fresh Multiplexer per REQUEST would satisfy the
// guard above — every tenant would still have a table of its own — while
// leaking a routing table and a transport on every reconnect, and while losing
// every binding a reconnecting Factory expects to still be there.
func TestResolvingTheSameTenantTwiceReusesItsMultiplexer(t *testing.T) {
	t.Parallel()

	set, built := stubLinks(t, 2)
	first, err := set.resolve("tenant-a")
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	second, err := set.resolve("tenant-a")
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if first != second || first.mux != second.mux {
		t.Errorf("resolving one tenant twice produced two links (%p, %p)", first, second)
	}
	if *built != 1 {
		t.Errorf("the composition built %d links for one tenant, want 1", *built)
	}
}

// TestTheTenantBoundIsACapacityAnswerAndNotALatch keeps MaxTenantLinks from
// becoming the fixed tenant H8 removed.
//
// The distinction is which tenant is refused. A latch refuses every tenant but
// the first, forever; a capacity bound refuses whoever asks when there is no
// room, and stops refusing as soon as an idle link is reclaimed. Both halves are
// asserted, because the first alone is also true of a latch with a limit of one.
func TestTheTenantBoundIsACapacityAnswerAndNotALatch(t *testing.T) {
	t.Parallel()

	set, _ := stubLinks(t, 1)
	if _, err := set.resolve("tenant-a"); err != nil {
		t.Fatalf("first tenant: %v", err)
	}

	// A second tenant is refused for CAPACITY only because the first link is
	// reclaimable; to make the refusal reachable at all, the first must be busy.
	// It is not, so the second tenant is admitted and the first is reclaimed.
	if _, err := set.resolve("tenant-b"); err != nil {
		t.Fatalf("a bound at one refused a second tenant while the first link held no bindings: %v", err)
	}
	if _, held := set.lookup("tenant-a"); held {
		t.Error("the reclaimed link is still held")
	}
	if _, held := set.lookup("tenant-b"); !held {
		t.Error("the admitted tenant holds no link")
	}
}

// TestAnInvalidTenantIsRefusedBeforeAnythingIsAllocated closes the other half of
// the bound: the tenant on the path is an unauthenticated claim, so a caller
// naming arbitrary strings must not be able to make this Host allocate for each.
func TestAnInvalidTenantIsRefusedBeforeAnythingIsAllocated(t *testing.T) {
	t.Parallel()

	set, built := stubLinks(t, 4)
	if _, err := set.resolve(""); err == nil {
		t.Fatal("an empty tenant was accepted")
	}
	if *built != 0 {
		t.Errorf("a refused tenant built %d links, want 0", *built)
	}
}

// TestTheTenantPathIsExactlyOneSegment pins the routing rule.
//
// A path with a further segment is REFUSED rather than truncated, because
// /hostlink/tenant-a/tenant-b resolving to tenant-a is the tolerance that turns
// into a confused-deputy report later.
func TestTheTenantPathIsExactlyOneSegment(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		path   string
		tenant string
		ok     bool
	}{
		{path: "/hostlink/tenant-a", tenant: "tenant-a", ok: true},
		{path: "/hostlink/", ok: false},
		{path: "/hostlink", ok: false},
		{path: "/hostlink/tenant-a/tenant-b", ok: false},
		{path: "/other/tenant-a", ok: false},
	} {
		tenant, ok := tenantFromPath(test.path)
		if ok != test.ok || tenant != test.tenant {
			t.Errorf("tenantFromPath(%q) = (%q, %v), want (%q, %v)", test.path, tenant, ok, test.tenant, test.ok)
		}
	}
}

// stubClock is a frozen clock for the links tests.
type stubClock struct{}

func (stubClock) Now() time.Time { return time.Unix(1_700_000_000, 0).UTC() }

// stubResidencies holds nothing resident.
type stubResidencies struct{}

func (stubResidencies) Get(registry.Key) (registry.Entry, bool) { return registry.Entry{}, false }

// stubAdmission is not draining.
type stubAdmission struct{}

func (stubAdmission) Draining() bool { return false }

// stubConsumers resolves no consumer.
type stubConsumers struct{}

func (stubConsumers) ConsumerFor(registry.Key) (hostlink.CommandConsumer, bool) { return nil, false }

// stubServer is a transport that does nothing and closes cleanly.
type stubServer struct{}

func (stubServer) Handler() http.Handler        { return nil }
func (stubServer) Publish(string, []byte) error { return nil }
func (stubServer) Close(context.Context) error  { return nil }
