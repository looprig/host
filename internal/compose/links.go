package compose

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/service"
)

// HostLinkPathPrefix is the path every HostLink connection arrives under. The
// segment after it is the tenant the connection claims, which the server then
// AUTHENTICATES; a path segment is a claim and never a credential.
const HostLinkPathPrefix = "/hostlink/"

// tenantLink is everything one authenticated tenant's connections share: a
// routing table, a transport, and the live event relay that publishes through
// it.
//
// THE THREE ARE PER TENANT TOGETHER AND NOT SEPARATELY. The Multiplexer is per
// tenant because H8 made the tenant the LINK'S rather than the Host's; the
// transport is per tenant because hostlink.Config authenticates every
// connection against one TenantID; and the relay is per tenant because
// service.Publications takes a channel and a payload and nothing else, so a
// single relay across tenants would have no way to choose which transport a
// publication left through. Splitting any one of them from the other two is how
// a cross-tenant delivery becomes reachable.
type tenantLink struct {
	tenant sessionwire.TenantID
	mux    *hostlink.Multiplexer
	server hostlink.Server
	tails  *service.Tails

	lastUsed time.Time
}

// errTooManyTenants is what an unauthenticated caller gets once this Host holds
// as many tenant links as it is willing to allocate.
var errTooManyTenants = errors.New("compose: this Host already holds its configured number of tenant links")

// links is this Host's set of per-tenant HostLink endpoints.
//
// IT IS THE COMPOSITION-LEVEL GUARD FOR H8 and it is the reason this type
// exists at all rather than a single Multiplexer field on the service. H8,
// answered 2026-09-04, deleted host.Options.TenantID: a pooled Host advertising
// cross_tenant_isolated admits several tenants, and the admission rule is the
// isolation class, derived from the admitted ledger and never latched. A
// composition holding ONE Multiplexer would reverse that here, silently, because
// nothing below the composition can see it — internal/residency enforces
// isolation on the durable path and internal/service on the ledger, and neither
// has any idea how many routing tables exist above them.
//
// So the invariant this type holds is a BIJECTION: one live Multiplexer per
// authenticated tenant, never shared and never latched to whichever tenant
// connected first.
type links struct {
	// clock is narrowed to the instant, which is all a reclaim needs.
	clock registry.Clock
	max   int
	build func(sessionwire.TenantID) (*tenantLink, error)

	mu       sync.Mutex
	byTenant map[sessionwire.TenantID]*tenantLink
	closed   bool
}

// resolve returns the tenant's link, building one if this Host holds none.
//
// THE TENANT IS NOT AUTHENTICATED YET at this point and cannot be: the
// credential arrives inside the WebSocket connect frame, which is after the
// upgrade. That is why the bound exists — an anonymous caller naming a fresh
// tenant on each request would otherwise make this Host allocate a transport
// per request — and why the identity is validated with Core's own rule before
// anything is built. What it decides is which authenticated tenant this
// connection COULD be; hostlink.Config decides whether it is.
func (l *links) resolve(tenant sessionwire.TenantID) (*tenantLink, error) {
	if err := tenant.Validate(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errors.New("compose: this Host is closed")
	}
	if existing, held := l.byTenant[tenant]; held {
		existing.lastUsed = l.clock.Now()
		return existing, nil
	}
	if len(l.byTenant) >= l.max && !l.reclaimLocked() {
		return nil, errTooManyTenants
	}
	built, err := l.build(tenant)
	if err != nil {
		return nil, err
	}
	built.lastUsed = l.clock.Now()
	l.byTenant[tenant] = built
	return built, nil
}

// reclaimLocked frees one slot by closing the least recently used link that
// holds no bindings, and reports whether it freed one.
//
// IT RECLAIMS ONLY AN EMPTY LINK, which is what makes it safe to do under
// pressure rather than on a timer. A Multiplexer with no bindings routes
// nothing and holds no session state; closing its transport drops connections
// that have bound nothing, and a Factory reconnects. A link holding even one
// binding is never reclaimed, so a busy tenant cannot be evicted by a stranger
// connecting.
//
// The bound is therefore on ALLOCATION and not on tenancy. A Host at its limit
// with every link busy refuses a new tenant rather than displacing a working
// one, and that refusal is a capacity answer, not the latched-tenant answer H8
// removed.
func (l *links) reclaimLocked() bool {
	var (
		victim *tenantLink
		key    sessionwire.TenantID
	)
	for tenant, link := range l.byTenant {
		if link.mux.Len() != 0 {
			continue
		}
		if victim == nil || link.lastUsed.Before(victim.lastUsed) {
			victim, key = link, tenant
		}
	}
	if victim == nil {
		return false
	}
	delete(l.byTenant, key)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = victim.server.Close(ctx)
	return true
}

// lookup returns a tenant's link without building one.
func (l *links) lookup(tenant sessionwire.TenantID) (*tenantLink, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	link, held := l.byTenant[tenant]
	return link, held
}

// all returns every live link.
func (l *links) all() []*tenantLink {
	l.mu.Lock()
	defer l.mu.Unlock()
	live := make([]*tenantLink, 0, len(l.byTenant))
	for _, link := range l.byTenant {
		live = append(live, link)
	}
	return live
}

// Close closes every tenant transport and refuses further resolution.
//
// It is the composition's lifecycle.Link, and it closes LAST in the drain for
// that seam's reason: Factory reaches the bounded drain-status observation
// through these connections, so closing them when the drain began would leave
// it inferring completion from a disconnect.
func (l *links) Close(ctx context.Context) error {
	l.mu.Lock()
	l.closed = true
	live := make([]*tenantLink, 0, len(l.byTenant))
	for _, link := range l.byTenant {
		live = append(live, link)
	}
	l.byTenant = map[sessionwire.TenantID]*tenantLink{}
	l.mu.Unlock()

	var failures []error
	for _, link := range live {
		if err := link.server.Close(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// handler routes an inbound HostLink connection to its tenant's transport.
//
// THE TENANT COMES FROM THE PATH AND THE CREDENTIAL DECIDES. A connection
// arriving at /hostlink/{tenant} is handed the transport configured for that
// tenant, and that transport's authenticator is called with that same tenant on
// every physical connection including reconnects; a caller naming a tenant it
// cannot authenticate for reaches a server that disconnects it. There is no
// path by which a connection reaches a Multiplexer built for a different
// tenant, because the tenant selects the transport before the transport
// authenticates it.
func (l *links) handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		name, ok := tenantFromPath(request.URL.Path)
		if !ok {
			http.Error(writer, "HostLink requires a tenant path segment", http.StatusNotFound)
			return
		}
		link, err := l.resolve(sessionwire.TenantID(name))
		if err != nil {
			if errors.Is(err, errTooManyTenants) {
				http.Error(writer, "this Host holds no room for another tenant", http.StatusServiceUnavailable)
				return
			}
			http.Error(writer, "HostLink requires a valid tenant", http.StatusBadRequest)
			return
		}
		link.server.Handler().ServeHTTP(writer, request)
	})
}

// tenantFromPath extracts the tenant segment.
//
// It refuses a path with a FURTHER segment rather than ignoring the remainder,
// because /hostlink/tenant-a/tenant-b naming tenant-a is the kind of tolerance
// that becomes a confused-deputy report later. One segment, exactly.
func tenantFromPath(path string) (string, bool) {
	rest, found := strings.CutPrefix(path, HostLinkPathPrefix)
	if !found || rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}
