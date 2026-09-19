package host_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
)

// ---------------------------------------------------------------------------
// The one guard that cannot go blind: Host's REAL router against Core's
// derivation
// ---------------------------------------------------------------------------
//
// Core's sessionwire.HostLinkEndpoint(base, tenant) claims to produce
// byte-for-byte the address Host's router resolves back to the same tenant,
// and every Factory dials exactly what it returns. Core cannot import Host, so
// its own router is a copy pinned by goldens recorded from host v0.2.1: it
// would stay green if Host's routing moved. THIS file is where that claim is
// checked against the thing it is about — host.Service.Routes(), composed over
// a real session store, served over a real listener and reached by a real
// WebSocket client — and the observable is not a status code but WHICH TENANT
// THE CONNECTION AUTHENTICATES AS, read at the injected AuthVerifier. That is
// the tenant whose Multiplexer the connection landed in.

// goldenEndpoints is Core's testdata/hostlink_endpoint_goldens.json.
type goldenEndpoints struct {
	Base sessionwire.InternalEndpoint `json:"base"`
	Rows []struct {
		ID           string `json:"id"`
		TenantHex    string `json:"tenant_hex"`
		SameTenant   bool   `json:"router_same_tenant"`
		Endpoint     string `json:"endpoint"`
		Refusal      string `json:"refusal"`
		RouterStatus int    `json:"router_status"`
	} `json:"rows"`
}

// loadCoreGoldens reads the goldens FROM THE PINNED CORE MODULE, not from a
// copy in this repository, so a Core bump re-runs this guard against that
// release's list and a stale copy cannot drift. It refuses a replaced or
// workspace-resolved Core for the reason pinnedSessionstoreDir does.
func loadCoreGoldens(tb testing.TB) goldenEndpoints {
	tb.Helper()
	const modulePath = "github.com/looprig/core"
	var stderr strings.Builder
	cmd := exec.Command("go", "list", "-m", "-json", modulePath)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		tb.Fatalf("go list -m -json %s: %v: %s", modulePath, err, stderr.String())
	}
	var module struct {
		Main    bool
		Version string
		Dir     string
		Replace *struct{ Path string }
	}
	if err := json.Unmarshal(out, &module); err != nil {
		tb.Fatalf("parse go list output: %v", err)
	}
	if module.Replace != nil || module.Main || module.Version == "" || module.Dir == "" {
		tb.Fatalf("core resolves to %+v; this guard reads the RELEASED module's goldens. Rerun with GOWORK=off", module)
	}
	data, err := os.ReadFile(filepath.Join(module.Dir, "sessionwire", "v1", "testdata", "hostlink_endpoint_goldens.json"))
	if err != nil {
		tb.Fatalf("core %s ships no HostLinkEndpoint goldens: %v", module.Version, err)
	}
	var goldens goldenEndpoints
	if err := json.Unmarshal(data, &goldens); err != nil {
		tb.Fatalf("decode goldens: %v", err)
	}
	// THE GUARD MUST REACH ITS SUBJECT. An empty or one-sided list passes
	// every row it has.
	routed, refused := 0, 0
	for _, row := range goldens.Rows {
		if row.Endpoint != "" && row.Refusal == "" {
			routed++
		}
		if row.Refusal == string(sessionwire.HostLinkEndpointCodeUnroutableTenant) {
			refused++
		}
	}
	if routed < 40 || refused < 5 {
		tb.Fatalf("core %s goldens hold %d routed and %d unroutable rows; the guard would prove nothing", module.Version, routed, refused)
	}
	return goldens
}

// recordingAuth is the injected AuthVerifier, instrumented. It accepts the
// fixture credential for any tenant and records the tenant each connection
// was authenticated FOR, which is the tenant whose transport it reached.
type recordingAuth struct {
	mu   sync.Mutex
	seen []sessionwire.TenantID
}

// VerifyTenant records the tenant and accepts the fixture credential.
func (a *recordingAuth) VerifyTenant(_ context.Context, tenant sessionwire.TenantID, credential string) error {
	if credential != composeSecret {
		return errors.New("bad credential")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = append(a.seen, tenant)
	return nil
}

// take returns the tenants recorded since the last call and forgets them.
func (a *recordingAuth) take() []sessionwire.TenantID {
	a.mu.Lock()
	defer a.mu.Unlock()
	seen := a.seen
	a.seen = nil
	return seen
}

// routedHost is a composed, started Service whose REAL Routes() is served by
// a real net/http server. Its advertised base is the goldens' base, so every
// derived endpoint can be compared with the golden byte for byte.
//
// THE TRANSPORT IS IN MEMORY AND EVERYTHING ABOVE IT IS REAL. The server is an
// ordinary http.Server and the client an ordinary gorilla/websocket Dialer
// given the derived endpoint VERBATIM, exactly as a Factory would dial it; only
// the dialler's socket is a net.Pipe handed to the server's listener. A TCP
// listener was measured running this machine out of ephemeral ports within a
// second of fuzzing ("connect: can't assign requested address"), which is a
// failure of the harness and not of the router.
type routedHost struct {
	base   sessionwire.InternalEndpoint
	auth   *recordingAuth
	dialer *websocket.Dialer
}

// newRoutedHost composes and starts a Service advertising base and serves its
// Routes() over an in-memory listener.
func newRoutedHost(tb testing.TB, base sessionwire.InternalEndpoint) *routedHost {
	tb.Helper()
	fixture := newComposeFixture(tb)
	auth := &recordingAuth{}
	blueprint := fixture.blueprint(tb)
	blueprint.Options.InternalEndpoint = base
	blueprint.Collaborators.Auth = auth
	service, err := host.Compose(context.Background(), blueprint)
	if err != nil {
		tb.Fatalf("Compose: %v", err)
	}
	if err := service.Start(context.Background()); err != nil {
		tb.Fatalf("Start: %v", err)
	}
	listener := newPipeListener()
	server := &http.Server{Handler: service.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	tb.Cleanup(func() {
		_ = server.Close()
		_, _ = service.Stop(context.Background())
	})
	if got := service.Host().InternalEndpoint(); got != base {
		tb.Fatalf("the Service advertises %q, want the configured base %q", got, base)
	}
	return &routedHost{
		base:   service.Host().InternalEndpoint(),
		auth:   auth,
		dialer: &websocket.Dialer{NetDialContext: listener.DialContext, HandshakeTimeout: 5 * time.Second},
	}
}

// reach dials a derived endpoint verbatim, completes the HostLink connect, and
// reports the tenants the connection was authenticated for. ok is false when
// the router refused the upgrade.
func (r *routedHost) reach(tb testing.TB, endpoint sessionwire.InternalEndpoint) ([]sessionwire.TenantID, bool) {
	tb.Helper()
	connection, response, err := r.dialer.Dial(string(endpoint), http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return r.auth.take(), false
	}
	defer connection.Close()
	negotiation, _ := json.Marshal(sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
	reply := sendOver(tb, connection, map[string]any{
		"id":      1,
		"connect": map[string]any{"token": composeSecret, "data": json.RawMessage(negotiation), "name": "test-factory"},
	}, 1)
	if reply.Connect == nil || reply.Error != nil {
		tb.Fatalf("connect over %q refused: %#v", endpoint, reply)
	}
	return r.auth.take(), true
}

// pipeListener is a net.Listener whose connections are net.Pipe halves.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

// newPipeListener returns an open listener with no pending connection.
func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

// Accept returns the next dialled pipe half.
func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Close stops Accept and every later dial.
func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// Addr names the in-memory transport.
func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

// DialContext hands the server one half of a fresh pipe and returns the other.
func (l *pipeListener) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		_, _ = server.Close(), client.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_, _ = server.Close(), client.Close()
		return nil, ctx.Err()
	}
}

// pipeAddr is the in-memory transport's address.
type pipeAddr struct{}

// Network reports "pipe".
func (pipeAddr) Network() string { return "pipe" }

// String reports "pipe".
func (pipeAddr) String() string { return "pipe" }

// TestRoutesResolveEveryGoldenEndpointToItsTenant is the goldens half: for
// every tenant Core's list derives an endpoint for, the endpoint Host's OWN
// advertised base derives is byte-identical to the golden, and dialling it on
// the real router authenticates the connection as exactly that tenant. For
// every tenant Core refuses as unroutable, the naive spelling really does not
// reach that tenant on this router — Core's refusal is sound for this Host.
func TestRoutesResolveEveryGoldenEndpointToItsTenant(t *testing.T) {
	goldens := loadCoreGoldens(t)
	routed := newRoutedHost(t, goldens.Base)
	reached := 0
	for _, row := range goldens.Rows {
		raw, err := hex.DecodeString(row.TenantHex)
		if err != nil {
			t.Fatalf("%s: tenant_hex: %v", row.ID, err)
		}
		tenant := sessionwire.TenantID(raw)
		endpoint, deriveErr := sessionwire.HostLinkEndpoint(routed.base, tenant)
		switch {
		case row.Refusal == "" && row.Endpoint != "":
			if deriveErr != nil {
				t.Errorf("%s: HostLinkEndpoint(advertised base) = %v, want %q", row.ID, deriveErr, row.Endpoint)
				continue
			}
			if string(endpoint) != row.Endpoint {
				t.Errorf("%s: derived %q from the advertised base, golden %q", row.ID, endpoint, row.Endpoint)
			}
			seen, ok := routed.reach(t, endpoint)
			if !ok || len(seen) != 1 || seen[0] != tenant {
				t.Errorf("%s: dialling %q on Routes() authenticated as %q (upgraded %t), want exactly [%q]", row.ID, endpoint, seen, ok, tenant)
				continue
			}
			reached++
		case row.Refusal == string(sessionwire.HostLinkEndpointCodeUnroutableTenant):
			var refusal *sessionwire.HostLinkEndpointError
			if !errors.As(deriveErr, &refusal) || refusal.Code != sessionwire.HostLinkEndpointCodeUnroutableTenant {
				t.Errorf("%s: HostLinkEndpoint = (%q, %v), want unroutable_tenant", row.ID, endpoint, deriveErr)
				continue
			}
			naive := sessionwire.InternalEndpoint(string(routed.base) + host.HostLinkPathPrefix + url.PathEscape(string(tenant)))
			seen, _ := routed.reach(t, naive)
			for _, got := range seen {
				if got == tenant {
					t.Errorf("%s: Core refuses %q as unroutable, but the naive spelling %q reached it on Routes()", row.ID, tenant, naive)
				}
			}
		}
	}
	if reached < 40 {
		t.Fatalf("only %d golden endpoints reached their tenant; the guard did not reach its subject", reached)
	}
}

// FuzzRoutesResolveHostLinkEndpointToItsTenant is the fuzz half: for every
// tenant Core derives an endpoint for from this Host's advertised base, the
// real router authenticates the connection as that tenant and no other.
func FuzzRoutesResolveHostLinkEndpointToItsTenant(f *testing.F) {
	goldens := loadCoreGoldens(f)
	for _, row := range goldens.Rows {
		raw, err := hex.DecodeString(row.TenantHex)
		if err != nil {
			f.Fatalf("%s: tenant_hex: %v", row.ID, err)
		}
		f.Add(string(raw))
	}
	routed := newRoutedHost(f, goldens.Base)
	f.Fuzz(func(t *testing.T, tenant string) {
		endpoint, err := sessionwire.HostLinkEndpoint(routed.base, sessionwire.TenantID(tenant))
		if err != nil {
			return
		}
		seen, ok := routed.reach(t, endpoint)
		if !ok || len(seen) != 1 || seen[0] != sessionwire.TenantID(tenant) {
			t.Fatalf("dialling %q on Routes() authenticated as %q (upgraded %t), want exactly [%q]", endpoint, seen, ok, tenant)
		}
	})
}

// TestTheHostLinkPrefixIsCores pins the exported prefix to Core's, the value
// every deriving Factory builds addresses with. A Host serving any other
// prefix is unreachable from every Factory, so this is not a style rule.
func TestTheHostLinkPrefixIsCores(t *testing.T) {
	t.Parallel()
	if host.HostLinkPathPrefix != sessionwire.HostLinkPathPrefix {
		t.Fatalf("host.HostLinkPathPrefix = %q, Core's sessionwire.HostLinkPathPrefix = %q", host.HostLinkPathPrefix, sessionwire.HostLinkPathPrefix)
	}
}
