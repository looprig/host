package hostlink_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// The three collaborator interfaces are satisfied by the production types they
// were narrowed from, and the assertions live here rather than in production so
// hostlink does not name internal/commands, internal/registry's concrete type
// or internal/service at build time.
//
// The CommandConsumer line is the load-bearing one. It says the ONLY method a
// HostLink command delivery can reach on the durable consumer is Hint, whose
// single parameter is a CommandID — so §3's "CommandID only" is a property of
// the type rather than of the caller's restraint.
var (
	_ hostlink.Residencies     = (*registry.Registry)(nil)
	_ hostlink.Admission       = (*service.CapacityPublisher)(nil)
	_ hostlink.CommandConsumer = (*commands.Consumer)(nil)
)

const (
	testHostID     = sessionwire.HostID("host-alpha")
	testGeneration = uint64(4)
	testEpoch      = uint64(7)
	testRuntime    = "runtime-build-1"
	testSession    = sessionwire.SessionID("session-alpha")
	otherSession   = sessionwire.SessionID("session-beta")
	thirdSession   = sessionwire.SessionID("session-gamma")
	linkA          = hostlink.LinkID("link-a")
	linkB          = hostlink.LinkID("link-b")
)

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time { return c.at }

// recordingResidencies wraps the REAL registry rather than reimplementing it.
//
// A hand-written residency fake would be free to answer with a row the registry
// cannot produce — a resident entry that is not accepting, say — and a bind
// tested against that answer would be tested against a state that does not
// exist. Delegating keeps the oracle exactly as tight as production, and the
// only thing added is a record of which keys were looked up, which is what the
// cross-tenant test needs.
type recordingResidencies struct {
	inner   *registry.Registry
	mu      sync.Mutex
	lookups []registry.Key
}

func (r *recordingResidencies) Get(key registry.Key) (registry.Entry, bool) {
	r.mu.Lock()
	r.lookups = append(r.lookups, key)
	r.mu.Unlock()
	return r.inner.Get(key)
}

func (r *recordingResidencies) lookedUp() []registry.Key {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registry.Key(nil), r.lookups...)
}

type stubAdmission struct {
	mu       sync.Mutex
	draining bool
}

func (a *stubAdmission) Draining() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.draining
}

func (a *stubAdmission) beginDrain() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.draining = true
}

type stubConsumer struct {
	mu    sync.Mutex
	hints []sessionwire.CommandID
}

func (c *stubConsumer) Hint(id sessionwire.CommandID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hints = append(c.hints, id)
}

func (c *stubConsumer) recorded() []sessionwire.CommandID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sessionwire.CommandID(nil), c.hints...)
}

type stubConsumers struct {
	mu        sync.Mutex
	consumers map[registry.Key]*stubConsumer
}

func (c *stubConsumers) ConsumerFor(key registry.Key) (hostlink.CommandConsumer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	consumer, running := c.consumers[key]
	if !running {
		return nil, false
	}
	return consumer, true
}

type fixture struct {
	t           *testing.T
	registry    *registry.Registry
	residencies *recordingResidencies
	admission   *stubAdmission
	consumers   *stubConsumers
	mux         *hostlink.Multiplexer
}

func residencyKey(session sessionwire.SessionID) registry.Key {
	return registry.Key{TenantID: testTenant, SessionID: session}
}

// newFixture builds a Multiplexer over a real registry holding three resident
// sessions, with a two-per-link and three-per-Host binding budget.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	index := registry.New(frozenClock{at: time.Unix(1_700_000_000, 0).UTC()})
	residencies := &recordingResidencies{inner: index}
	admission := &stubAdmission{}
	consumers := &stubConsumers{consumers: map[registry.Key]*stubConsumer{}}
	for _, session := range []sessionwire.SessionID{testSession, otherSession, thirdSession} {
		if _, inserted := index.Insert(residencyKey(session), registry.Admission{
			AgentID:         sessionwire.AgentID("agent-alpha"),
			CompatibilityID: testRuntime,
			LeaseEpoch:      testEpoch,
		}); !inserted {
			t.Fatalf("fixture failed to insert %q", session)
		}
		consumers.consumers[residencyKey(session)] = &stubConsumer{}
	}
	mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID:           testTenant,
		HostID:             testHostID,
		HostGeneration:     testGeneration,
		Residencies:        residencies,
		Admission:          admission,
		Consumers:          consumers,
		MaxBindingsPerLink: 2,
		MaxBindings:        3,
	})
	if err != nil {
		t.Fatalf("NewMultiplexer: %v", err)
	}
	return &fixture{t: t, registry: index, residencies: residencies, admission: admission, consumers: consumers, mux: mux}
}

// bindRequest returns the fully valid ownership tuple for a session. Every
// refusal row below perturbs exactly one thing about this value or about the
// state it is validated against.
func bindRequest(session sessionwire.SessionID) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               testTenant,
		SessionID:              session,
		HostID:                 testHostID,
		HostGeneration:         testGeneration,
		LeaseEpoch:             testEpoch,
		RuntimeCompatibilityID: testRuntime,
		IdempotencyKey:         "bind-attempt-1",
	}
}

func unbindRequest(session sessionwire.SessionID) sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version:        sessionwire.CurrentWireVersion,
		TenantID:       testTenant,
		SessionID:      session,
		HostID:         testHostID,
		HostGeneration: testGeneration,
		LeaseEpoch:     testEpoch,
		IdempotencyKey: "unbind-attempt-1",
	}
}

func mustBind(t *testing.T, mux *hostlink.Multiplexer, link hostlink.LinkID, session sessionwire.SessionID) hostlink.Binding {
	t.Helper()
	binding, err := mux.Bind(link, bindRequest(session))
	if err != nil {
		t.Fatalf("bind %q on %q: %v", session, link, err)
	}
	return binding
}

func refusalOf(t *testing.T, err error) *hostlink.BindError {
	t.Helper()
	if err == nil {
		t.Fatal("expected a refusal, got success")
	}
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("error = %T %v, want *hostlink.BindError", err, err)
	}
	return refusal
}

// ---------------------------------------------------------------------------
// Multiplexing
// ---------------------------------------------------------------------------

func TestOneLinkCarriesManySessionBindings(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	first := mustBind(t, f.mux, linkA, testSession)
	second := mustBind(t, f.mux, linkA, otherSession)

	if first.Channel == second.Channel {
		t.Fatalf("two sessions minted one channel %q", first.Channel)
	}
	if got := f.mux.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	bindings := f.mux.LinkBindings(linkA)
	if len(bindings) != 2 {
		t.Fatalf("link bindings = %d, want 2", len(bindings))
	}
	// Each binding routes independently: a delivery on one channel wakes one
	// consumer and leaves the other's log empty.
	deliver(t, f.mux, linkA, first.Channel, "command-one")
	if got := f.consumers.consumers[residencyKey(testSession)].recorded(); len(got) != 1 || got[0] != "command-one" {
		t.Fatalf("first session hints = %v, want one command-one", got)
	}
	if got := f.consumers.consumers[residencyKey(otherSession)].recorded(); len(got) != 0 {
		t.Fatalf("second session hints = %v, want none", got)
	}
}

func TestManyFactoryReplicasBindOneSession(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	a := mustBind(t, f.mux, linkA, testSession)
	b := mustBind(t, f.mux, linkB, testSession)

	if a.Channel != b.Channel {
		t.Fatalf("replicas of one session got channels %q and %q", a.Channel, b.Channel)
	}
	if got := f.mux.Replicas(residencyKey(testSession)); !slices.Equal(got, []hostlink.LinkID{linkA, linkB}) {
		t.Fatalf("Replicas = %v, want both links", got)
	}
	if got := f.mux.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 routes for one session", got)
	}
	// One session, two routes, and each route may be used on its own link.
	deliver(t, f.mux, linkA, a.Channel, "from-replica-a")
	deliver(t, f.mux, linkB, b.Channel, "from-replica-b")
	if got := f.consumers.consumers[residencyKey(testSession)].recorded(); !slices.Equal(got, []sessionwire.CommandID{"from-replica-a", "from-replica-b"}) {
		t.Fatalf("hints = %v, want one from each replica", got)
	}
}

func TestBindAndUnbindAreIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	first := mustBind(t, f.mux, linkA, testSession)
	repeat, err := f.mux.Bind(linkA, bindRequest(testSession))
	if err != nil {
		t.Fatalf("repeat bind: %v", err)
	}
	if repeat != first {
		t.Fatalf("repeat bind = %#v, want the identical binding %#v", repeat, first)
	}
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len after repeat bind = %d, want 1", got)
	}

	// A repeat carrying a NEW idempotency key is still one route, and the
	// recorded key moves. HostLink does not replay a cached acceptance, so the
	// second attempt re-read current ownership: three lookups for three binds.
	renamed := bindRequest(testSession)
	renamed.IdempotencyKey = "bind-attempt-2"
	updated, err := f.mux.Bind(linkA, renamed)
	if err != nil {
		t.Fatalf("renamed bind: %v", err)
	}
	if updated.IdempotencyKey != "bind-attempt-2" || updated.Channel != first.Channel {
		t.Fatalf("renamed bind = %#v, want the same channel with the new key", updated)
	}
	if got := len(f.residencies.lookedUp()); got != 3 {
		t.Fatalf("residency lookups = %d, want 3; every bind re-validates", got)
	}

	if err := f.mux.Unbind(linkA, unbindRequest(testSession)); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if got := f.mux.Len(); got != 0 {
		t.Fatalf("Len after unbind = %d, want 0", got)
	}
	if err := f.mux.Unbind(linkA, unbindRequest(testSession)); err != nil {
		t.Fatalf("repeat unbind: %v", err)
	}
	if err := f.mux.Unbind(linkA, unbindRequest(thirdSession)); err != nil {
		t.Fatalf("unbind of a route never held: %v", err)
	}
	if got := f.mux.Len(); got != 0 {
		t.Fatalf("Len after repeat unbind = %d, want 0", got)
	}
	// The unbind released the budget rather than merely hiding the route: a
	// fresh bind succeeds, which a leaked count would eventually refuse.
	mustBind(t, f.mux, linkA, testSession)
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

// TestEachBindRefusalHasASoleCause is a SINGLE-FAULT matrix, and that shape is
// the point rather than the coverage.
//
// Every row starts from a fixture whose bind is proved to succeed inside the
// same subtest, then changes exactly one thing — one request field, or one
// piece of Host state — and asserts one refusal. The success control and the
// refusal therefore differ in one respect, which is what it means for a check
// to be the SOLE one on its path: no other check can be firing, because with
// that one perturbation removed nothing fires at all.
//
// The refusals are pairwise distinct even where the wire class is not. Four
// paths publish runtime_unavailable on purpose, so asserting only the Core code
// would let any of the four stand in for the others; asserting the local
// Refusal is what keeps them apart.
func TestEachBindRefusalHasASoleCause(t *testing.T) {
	t.Parallel()

	otherTenant := sessionwire.TenantID("tenant-other")
	for _, test := range []struct {
		name    string
		perturb func(*fixture, *sessionwire.HostLinkBindRequest)
		refusal hostlink.Refusal
		code    sessionwire.HostLinkErrorCode
		check   func(*testing.T, *hostlink.BindError)
	}{
		{
			name:    "wrong tenant",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.TenantID = otherTenant },
			refusal: hostlink.RefusalForeignTenant,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "wrong session",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.SessionID = "session-absent" },
			refusal: hostlink.RefusalUnknownSession,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "wrong host",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.HostID = "host-beta" },
			refusal: hostlink.RefusalForeignHost,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "wrong host generation",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.HostGeneration = testGeneration + 1 },
			refusal: hostlink.RefusalStaleHostGeneration,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "wrong epoch",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.LeaseEpoch = testEpoch + 1 },
			refusal: hostlink.RefusalEpochMismatch,
			code:    sessionwire.HostLinkErrorEpochMismatch,
			check: func(t *testing.T, refusal *hostlink.BindError) {
				wire, _ := refusal.HostLinkError()
				if wire.CurrentLeaseEpoch != testEpoch {
					t.Fatalf("current_lease_epoch = %d, want the residency's %d", wire.CurrentLeaseEpoch, testEpoch)
				}
			},
		},
		{
			name:    "wrong runtime",
			perturb: func(_ *fixture, r *sessionwire.HostLinkBindRequest) { r.RuntimeCompatibilityID = "runtime-build-2" },
			refusal: hostlink.RefusalRuntimeMismatch,
			code:    sessionwire.HostLinkErrorRuntimeMismatch,
			check: func(t *testing.T, refusal *hostlink.BindError) {
				wire, _ := refusal.HostLinkError()
				if wire.RuntimeCompatibilityID != testRuntime {
					t.Fatalf("runtime_compatibility_id = %q, want the launched %q", wire.RuntimeCompatibilityID, testRuntime)
				}
			},
		},
		{
			name: "releasing residency",
			perturb: func(f *fixture, _ *sessionwire.HostLinkBindRequest) {
				entry, held := f.registry.Get(residencyKey(testSession))
				if !held {
					f.t.Fatal("fixture residency vanished")
				}
				if _, marked := f.registry.MarkReleasing(entry.Key, entry.Generation); !marked {
					f.t.Fatal("MarkReleasing refused the fixture residency")
				}
			},
			refusal: hostlink.RefusalReleasing,
			code:    sessionwire.HostLinkErrorReleasing,
		},
		{
			name:    "host not admitting",
			perturb: func(f *fixture, _ *sessionwire.HostLinkBindRequest) { f.admission.beginDrain() },
			refusal: hostlink.RefusalHostNotAdmitting,
			code:    sessionwire.HostLinkErrorNotAdmitting,
		},
		{
			name: "link binding budget spent",
			perturb: func(f *fixture, _ *sessionwire.HostLinkBindRequest) {
				mustBind(f.t, f.mux, linkA, otherSession)
				mustBind(f.t, f.mux, linkA, thirdSession)
			},
			refusal: hostlink.RefusalNoLinkCapacity,
			code:    sessionwire.HostLinkErrorNoCapacity,
		},
		{
			name: "host binding budget spent",
			perturb: func(f *fixture, _ *sessionwire.HostLinkBindRequest) {
				mustBind(f.t, f.mux, linkB, testSession)
				mustBind(f.t, f.mux, linkB, otherSession)
				mustBind(f.t, f.mux, "link-c", thirdSession)
			},
			refusal: hostlink.RefusalNoHostCapacity,
			code:    sessionwire.HostLinkErrorNoCapacity,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// The control. Same fixture recipe, same request, no perturbation.
			control := newFixture(t)
			if _, err := control.mux.Bind(linkA, bindRequest(testSession)); err != nil {
				t.Fatalf("unperturbed control bind failed, so this row proves nothing: %v", err)
			}

			f := newFixture(t)
			request := bindRequest(testSession)
			test.perturb(f, &request)
			refusal := refusalOf(t, mustFailBind(t, f.mux, linkA, request))
			if refusal.Refusal != test.refusal {
				t.Fatalf("refusal = %q, want %q", refusal.Refusal, test.refusal)
			}
			wire, published := refusal.HostLinkError()
			if !published {
				t.Fatalf("refusal %q published no Core class", refusal.Refusal)
			}
			if wire.Code != test.code {
				t.Fatalf("wire code = %q, want %q", wire.Code, test.code)
			}
			if err := wire.Validate(); err != nil {
				t.Fatalf("wire record is not a valid Core record: %v", err)
			}
			if test.check != nil {
				test.check(t, refusal)
			}
		})
	}
}

func mustFailBind(t *testing.T, mux *hostlink.Multiplexer, link hostlink.LinkID, request sessionwire.HostLinkBindRequest) error {
	t.Helper()
	binding, err := mux.Bind(link, request)
	if err == nil {
		t.Fatalf("bind succeeded and returned %#v, want a refusal", binding)
	}
	return err
}

// TestRefusalsAreDistinguishableWhereTheWireIsNot pins the deliberate
// narrowing rather than leaving it to be read off the table above.
//
// Seven of the twelve refusals share a Core class with at least one other —
// five publish runtime_unavailable and two publish no_capacity — so the local
// value is the only thing keeping those paths apart, and a duplicated string
// would silently merge two of them. Every constant is listed here, which is
// what makes each one's VALUE read by something: comparing the enum symbols
// alone would survive two of them being spelled identically.
func TestRefusalsAreDistinguishableWhereTheWireIsNot(t *testing.T) {
	t.Parallel()

	all := []hostlink.Refusal{
		hostlink.RefusalForeignTenant,
		hostlink.RefusalUnknownSession,
		hostlink.RefusalForeignHost,
		hostlink.RefusalStaleHostGeneration,
		hostlink.RefusalEpochMismatch,
		hostlink.RefusalRuntimeMismatch,
		hostlink.RefusalReleasing,
		hostlink.RefusalHostNotAdmitting,
		hostlink.RefusalNoLinkCapacity,
		hostlink.RefusalNoHostCapacity,
		hostlink.RefusalNotBound,
		hostlink.RefusalMalformedRequest,
	}
	seen := map[hostlink.Refusal]bool{}
	for _, refusal := range all {
		if refusal == "" {
			t.Fatal("a refusal has the empty value, which is the absence of a refusal")
		}
		if seen[refusal] {
			t.Fatalf("refusal %q appears twice, so two paths are indistinguishable", refusal)
		}
		seen[refusal] = true
	}
	if len(seen) != len(all) {
		t.Fatalf("distinct refusals = %d, want %d", len(seen), len(all))
	}
	// The subset that shares one Core class is where a duplicate would do the
	// most damage, so it is named rather than left implicit.
	for _, group := range [][]hostlink.Refusal{
		{hostlink.RefusalForeignTenant, hostlink.RefusalUnknownSession, hostlink.RefusalForeignHost, hostlink.RefusalStaleHostGeneration, hostlink.RefusalNotBound},
		{hostlink.RefusalNoLinkCapacity, hostlink.RefusalNoHostCapacity},
	} {
		for index, refusal := range group {
			if slices.Index(group, refusal) != index {
				t.Fatalf("refusal %q is repeated inside one wire class", refusal)
			}
		}
	}
}

// TestForeignTenantBindReadsNoResidencyAtAll is the non-leak half of the
// wrong-tenant refusal, and it is a probe rather than a tautology: the same
// oracle IS consulted on every other refusal path, and the assertion below is
// over a count of its calls.
func TestForeignTenantBindReadsNoResidencyAtAll(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	foreign := bindRequest(testSession)
	foreign.TenantID = "tenant-other"
	refusal := refusalOf(t, mustFailBind(t, f.mux, linkA, foreign))
	if refusal.Refusal != hostlink.RefusalForeignTenant {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalForeignTenant)
	}
	if got := f.residencies.lookedUp(); len(got) != 0 {
		t.Fatalf("residency lookups on the foreign-tenant path = %v, want none", got)
	}

	// The control that makes the count above mean something: a wrong-session
	// bind for the RIGHT tenant does reach the oracle, so zero is a property of
	// the tenant check and not of the harness.
	absent := bindRequest("session-absent")
	if _, err := f.mux.Bind(linkA, absent); err == nil {
		t.Fatal("bind for an unheld session succeeded")
	}
	if got := f.residencies.lookedUp(); len(got) != 1 || got[0] != residencyKey("session-absent") {
		t.Fatalf("residency lookups after the control = %v, want exactly the control's key", got)
	}
}

func TestMalformedBindRequestIsRefusedWithNoWireClass(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	invalid := bindRequest(testSession)
	invalid.LeaseEpoch = 0
	refusal := refusalOf(t, mustFailBind(t, f.mux, linkA, invalid))
	if refusal.Refusal != hostlink.RefusalMalformedRequest {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalMalformedRequest)
	}
	if wire, published := refusal.HostLinkError(); published {
		t.Fatalf("malformed request published Core class %q; Core's classes are statements about ownership", wire.Code)
	}
	if got := f.residencies.lookedUp(); len(got) != 0 {
		t.Fatalf("a request Core refuses reached the residency oracle: %v", got)
	}
}

func TestMultiplexerOptionsAreValidated(t *testing.T) {
	t.Parallel()

	valid := hostlink.MultiplexerOptions{
		TenantID:           testTenant,
		HostID:             testHostID,
		HostGeneration:     testGeneration,
		Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
		Admission:          &stubAdmission{},
		Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
		MaxBindingsPerLink: 1,
		MaxBindings:        1,
	}
	if _, err := hostlink.NewMultiplexer(valid); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	for _, test := range []struct {
		field  string
		spoil  func(*hostlink.MultiplexerOptions)
		expect string
	}{
		{field: "TenantID", spoil: func(o *hostlink.MultiplexerOptions) { o.TenantID = "" }},
		{field: "HostID", spoil: func(o *hostlink.MultiplexerOptions) { o.HostID = "" }},
		{field: "HostGeneration", spoil: func(o *hostlink.MultiplexerOptions) { o.HostGeneration = 0 }},
		{field: "Residencies", spoil: func(o *hostlink.MultiplexerOptions) { o.Residencies = nil }},
		{field: "Admission", spoil: func(o *hostlink.MultiplexerOptions) { o.Admission = nil }},
		{field: "Consumers", spoil: func(o *hostlink.MultiplexerOptions) { o.Consumers = nil }},
		{field: "MaxBindingsPerLink", spoil: func(o *hostlink.MultiplexerOptions) { o.MaxBindingsPerLink = 0 }},
		{field: "MaxBindings", spoil: func(o *hostlink.MultiplexerOptions) { o.MaxBindings = 0 }},
	} {
		t.Run(test.field, func(t *testing.T) {
			options := valid
			test.spoil(&options)
			mux, err := hostlink.NewMultiplexer(options)
			if err == nil {
				t.Fatalf("spoiled %s was accepted and returned %#v", test.field, mux)
			}
			var invalid *hostlink.InvalidMultiplexerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("error = %T %v, want *hostlink.InvalidMultiplexerOptionsError", err, err)
			}
			if invalid.Field != test.field {
				t.Fatalf("reported field = %q, want %q", invalid.Field, test.field)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// A bind validates ownership and never grants it
// ---------------------------------------------------------------------------

// TestBindingChangesNothingAboutResidency runs every bind path this package
// has, successful and refused, against a REAL registry and compares the whole
// index before and after.
//
// The comparison is over registry.Snapshot, which carries the state, the
// accepting flag, the generation, the lease epoch and the last-activity stamp,
// so a bind that installed, marked, touched or removed anything would move it.
// The clock is frozen, so an unchanged stamp is evidence rather than luck.
func TestBindingChangesNothingAboutResidency(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	before := f.registry.Snapshot()
	if len(before) != 3 {
		t.Fatalf("fixture snapshot = %d entries, want 3; an empty one would compare equal to anything", len(before))
	}

	mustBind(t, f.mux, linkA, testSession)
	mustBind(t, f.mux, linkB, testSession)
	for _, spoil := range []func(*sessionwire.HostLinkBindRequest){
		func(r *sessionwire.HostLinkBindRequest) { r.TenantID = "tenant-other" },
		func(r *sessionwire.HostLinkBindRequest) { r.SessionID = "session-absent" },
		func(r *sessionwire.HostLinkBindRequest) { r.HostID = "host-beta" },
		func(r *sessionwire.HostLinkBindRequest) { r.HostGeneration = testGeneration + 1 },
		func(r *sessionwire.HostLinkBindRequest) { r.LeaseEpoch = testEpoch + 1 },
		func(r *sessionwire.HostLinkBindRequest) { r.RuntimeCompatibilityID = "runtime-build-2" },
	} {
		request := bindRequest(otherSession)
		spoil(&request)
		if _, err := f.mux.Bind(linkA, request); err == nil {
			t.Fatal("a spoiled bind succeeded")
		}
	}
	if err := f.mux.Unbind(linkA, unbindRequest(testSession)); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	f.mux.CloseLink(linkB)

	if after := f.registry.Snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("residency changed across binding traffic:\nbefore %#v\nafter  %#v", before, after)
	}

	// The control. Snapshot equality is only evidence if it can fail, and one
	// registry mutation is enough to show it does.
	if _, inserted := f.registry.Insert(residencyKey("session-delta"), registry.Admission{LeaseEpoch: 1}); !inserted {
		t.Fatal("control insert was refused")
	}
	if after := f.registry.Snapshot(); reflect.DeepEqual(before, after) {
		t.Fatal("snapshot comparison did not notice a registry insert, so it proves nothing above")
	}
}

// TestCollaboratorInterfacesExposeOnlyReads derives the method sets from the
// types instead of naming the methods a reviewer happened to think of, so a
// mutator added to any of the three fails here rather than passing unnoticed.
func TestCollaboratorInterfacesExposeOnlyReads(t *testing.T) {
	t.Parallel()

	keyType := reflect.TypeOf(registry.Key{})
	entryType := reflect.TypeOf(registry.Entry{})
	boolType := reflect.TypeOf(false)
	commandIDType := reflect.TypeOf(sessionwire.CommandID(""))

	for _, test := range []struct {
		name    string
		iface   reflect.Type
		method  string
		params  []reflect.Type
		results []reflect.Type
	}{
		{
			name:    "Residencies",
			iface:   reflect.TypeOf((*hostlink.Residencies)(nil)).Elem(),
			method:  "Get",
			params:  []reflect.Type{keyType},
			results: []reflect.Type{entryType, boolType},
		},
		{
			name:    "Admission",
			iface:   reflect.TypeOf((*hostlink.Admission)(nil)).Elem(),
			method:  "Draining",
			results: []reflect.Type{boolType},
		},
		{
			name:   "CommandConsumer",
			iface:  reflect.TypeOf((*hostlink.CommandConsumer)(nil)).Elem(),
			method: "Hint",
			params: []reflect.Type{commandIDType},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.iface.NumMethod(); got != 1 {
				var names []string
				for index := range got {
					names = append(names, test.iface.Method(index).Name)
				}
				t.Fatalf("%s has %d methods (%s), want exactly one; a second method is a seam this package promised not to have", test.name, got, strings.Join(names, ", "))
			}
			method := test.iface.Method(0)
			if method.Name != test.method {
				t.Fatalf("%s's method = %q, want %q", test.name, method.Name, test.method)
			}
			signature := method.Type
			if got := signature.NumIn(); got != len(test.params) {
				t.Fatalf("%s.%s takes %d parameters, want %d", test.name, method.Name, got, len(test.params))
			}
			for index, want := range test.params {
				if got := signature.In(index); got != want {
					t.Fatalf("%s.%s parameter %d = %s, want %s", test.name, method.Name, index, got, want)
				}
			}
			if got := signature.NumOut(); got != len(test.results) {
				t.Fatalf("%s.%s returns %d results, want %d", test.name, method.Name, got, len(test.results))
			}
			for index, want := range test.results {
				if got := signature.Out(index); got != want {
					t.Fatalf("%s.%s result %d = %s, want %s", test.name, method.Name, index, got, want)
				}
			}
		})
	}
}

// TestProductionHoldsNoMutableRegistryHandle is the structural half of "a bind
// never grants ownership": the package cannot mutate residency because it never
// names anything that could.
//
// The file set is DERIVED from the package directory and the used-symbol set is
// derived from the type checker, so a future file, or a future line in an
// existing one, is inside the guard without anybody remembering to add it. The
// second half of the test proves the guard can fail, by type-checking the same
// package with one synthetic file that does hold a *registry.Registry and
// calls Insert on it.
func TestProductionHoldsNoMutableRegistryHandle(t *testing.T) {
	t.Parallel()

	packageDir := packageDirectory(t)
	const registryPath = "github.com/looprig/host/internal/registry"

	// The read-only surface of internal/registry. Every entry is a type, a
	// field or a constant; not one is a function, a constructor or a method,
	// which is what makes holding this list equivalent to holding no mutator.
	allowed := map[string]bool{
		"Key": true, "TenantID": true, "SessionID": true,
		"Entry": true, "LeaseEpoch": true, "CompatibilityID": true, "State": true,
		"ResidencyState": true, "StateResident": true, "StateReleasing": true, "StateDraining": true,
	}

	used := registrySymbolsUsedBy(t, packageDir, registryPath, nil)
	if len(used) == 0 {
		t.Fatal("no internal/registry symbol was used at all, so this guard is vacuous")
	}
	for _, symbol := range []string{"Key", "Entry"} {
		if !slices.Contains(used, symbol) {
			t.Fatalf("the package does not use %q, so the whitelist is not being exercised", symbol)
		}
	}
	for _, symbol := range used {
		if !allowed[symbol] {
			t.Errorf("production names %s.%s, which is outside the read-only surface a bind may hold. A bind VALIDATES ownership and never grants it; reaching a mutator, a constructor or the concrete *registry.Registry breaks that structurally", registryPath, symbol)
		}
	}

	control := `package hostlink

import "github.com/looprig/host/internal/registry"

func mutableRegistryHandleControl(index *registry.Registry, subject registry.Key) {
	_, _ = index.Insert(subject, registry.Admission{})
}
`
	controlUsed := registrySymbolsUsedBy(t, packageDir, registryPath, []byte(control))
	for _, symbol := range []string{"Registry", "Insert", "Admission"} {
		if !slices.Contains(controlUsed, symbol) {
			t.Fatalf("the control file names %s.%s and the guard did not see it, so the guard above cannot fail", registryPath, symbol)
		}
		if allowed[symbol] {
			t.Fatalf("%s.%s is on the whitelist, so the control would be accepted", registryPath, symbol)
		}
	}
}

// registrySymbolsUsedBy type-checks the package's production files, plus an
// optional synthetic control, and returns every internal/registry name they
// use, sorted and deduplicated.
func registrySymbolsUsedBy(t *testing.T, packageDir, registryPath string, control []byte) []string {
	t.Helper()

	entries, err := os.ReadDir(packageDir)
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	set := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(set, filepath.Join(packageDir, entry.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatal("registry-handle guard found zero production Go files")
	}
	if control != nil {
		file, err := parser.ParseFile(set, "mutable_registry_handle_control.go", control, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse control: %v", err)
		}
		files = append(files, file)
	}

	exports := dependencyExports(t, packageDir)
	lookup := func(path string) (io.ReadCloser, error) {
		export, ok := exports[path]
		if !ok || export == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(export)
	}
	information := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	if _, err := (&types.Config{Importer: importer.ForCompiler(set, "gc", lookup)}).Check("github.com/looprig/host/internal/realtime/hostlink", set, files, information); err != nil {
		t.Fatalf("type-check production package: %v", err)
	}

	seen := map[string]bool{}
	for _, object := range information.Uses {
		pkg := object.Pkg()
		if pkg == nil || pkg.Path() != registryPath {
			continue
		}
		seen[object.Name()] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// ---------------------------------------------------------------------------
// Command delivery
// ---------------------------------------------------------------------------

func deliver(t *testing.T, mux *hostlink.Multiplexer, link hostlink.LinkID, channel string, id sessionwire.CommandID) {
	t.Helper()
	if err := mux.Deliver(link, channel, sessionwire.HostLinkCommandDelivery{CommandID: id}); err != nil {
		t.Fatalf("deliver %q on %q: %v", id, channel, err)
	}
}

// TestCommandDeliveryCarriesACommandIDAndNothingElse proves the "CommandID
// only" rule three ways, because each way alone leaves a hole: the record has
// one field, an extra JSON member is refused by Core's own fail-closed decoder,
// and the only method the delivery can reach on the consumer takes a CommandID
// and returns nothing.
func TestCommandDeliveryCarriesACommandIDAndNothingElse(t *testing.T) {
	t.Parallel()

	delivery := reflect.TypeOf(sessionwire.HostLinkCommandDelivery{})
	if got := delivery.NumField(); got != 1 {
		t.Fatalf("HostLinkCommandDelivery has %d fields, want 1", got)
	}
	if field := delivery.Field(0); field.Type != reflect.TypeOf(sessionwire.CommandID("")) {
		t.Fatalf("its sole field %s is a %s, want sessionwire.CommandID", field.Name, field.Type)
	}

	for _, body := range []string{
		`{"command_id":"command-one","payload":"secret"}`,
		`{"command_id":"command-one","session_id":"session-alpha"}`,
		`{"command_id":"command-one","input":{"text":"hello"}}`,
	} {
		var decoded sessionwire.HostLinkCommandDelivery
		if err := json.Unmarshal([]byte(body), &decoded); err == nil {
			t.Fatalf("a delivery carrying an extra member was accepted: %s decoded to %#v", body, decoded)
		}
	}

	hint, found := reflect.TypeOf((*hostlink.CommandConsumer)(nil)).Elem().MethodByName("Hint")
	if !found {
		t.Fatal("CommandConsumer has no Hint method")
	}
	if hint.Type.NumIn() != 1 || hint.Type.NumOut() != 0 {
		t.Fatalf("Hint has signature %s, want one parameter and no result", hint.Type)
	}
}

func TestDuplicateCommandDeliveriesAreHarmless(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	binding := mustBind(t, f.mux, linkA, testSession)
	for range 3 {
		deliver(t, f.mux, linkA, binding.Channel, "command-one")
	}
	consumer := f.consumers.consumers[residencyKey(testSession)]
	if got := consumer.recorded(); !slices.Equal(got, []sessionwire.CommandID{"command-one", "command-one", "command-one"}) {
		t.Fatalf("hints = %v, want the same wake three times", got)
	}
	// A duplicate is a repeated WAKE and never a repeated application: nothing
	// in this package can reach anything but Hint, and the binding is unchanged
	// after three of them.
	if got := f.mux.LinkBindings(linkA); len(got) != 1 || got[0] != binding {
		t.Fatalf("bindings after duplicates = %#v, want the one unchanged binding", got)
	}
}

// TestDeliveryResolvesTheChannelByRouteAndNeverByShape is the transport-side
// echo of "parsing a channel never grants access": the refused channel below is
// a perfectly well-formed one that this Host itself minted for a session that
// is resident and bound — on the other link.
func TestDeliveryResolvesTheChannelByRouteAndNeverByShape(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	mine := mustBind(t, f.mux, linkA, testSession)
	theirs := mustBind(t, f.mux, linkB, otherSession)
	if theirs.Channel != hostlink.ChannelFor(residencyKey(otherSession)) {
		t.Fatalf("channel %q is not the one ChannelFor mints", theirs.Channel)
	}

	err := f.mux.Deliver(linkA, theirs.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"})
	refusal := refusalOf(t, err)
	if refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalNotBound)
	}
	if got := f.consumers.consumers[residencyKey(otherSession)].recorded(); len(got) != 0 {
		t.Fatalf("the other session was woken through a route its link did not hold: %v", got)
	}
	// The control: the same link, the same session's consumer, through the
	// route it does hold.
	deliver(t, f.mux, linkA, mine.Channel, "command-one")
	if got := f.consumers.consumers[residencyKey(testSession)].recorded(); len(got) != 1 {
		t.Fatalf("hints on the held route = %v, want one", got)
	}
	// And the same channel over the link that DOES hold it, which is what makes
	// the refusal above a statement about the route rather than the channel.
	deliver(t, f.mux, linkB, theirs.Channel, "command-two")
	if got := f.consumers.consumers[residencyKey(otherSession)].recorded(); len(got) != 1 {
		t.Fatalf("hints over the holding link = %v, want one", got)
	}
}

func TestDeliveryWithoutARunningConsumerIsRefused(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	binding := mustBind(t, f.mux, linkA, testSession)
	delete(f.consumers.consumers, residencyKey(testSession))
	refusal := refusalOf(t, f.mux.Deliver(linkA, binding.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}))
	if refusal.Refusal != hostlink.RefusalUnknownSession {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalUnknownSession)
	}
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

// TestChannelsAreInjectiveAcrossSeparatorCollisions enumerates a small space of
// identifier pairs rather than testing one, because the property is universally
// quantified over keys and a fixed fixture cannot defend one: the pairs below
// are chosen so that a plain-concatenation channel would collide.
func TestChannelsAreInjectiveAcrossSeparatorCollisions(t *testing.T) {
	t.Parallel()

	tenants := []sessionwire.TenantID{"a", "a.b", "a.b.c", "a:b", "a#b", "ab", "a-b", "a_b"}
	sessions := []sessionwire.SessionID{"c", "b.c", "c.d", "b:c", "b#c", "bc", "b-c", "b_c"}
	channels := map[string]registry.Key{}
	for _, tenant := range tenants {
		for _, session := range sessions {
			subject := registry.Key{TenantID: tenant, SessionID: session}
			channel := hostlink.ChannelFor(subject)
			if !strings.HasPrefix(channel, hostlink.ChannelPrefix) {
				t.Fatalf("channel %q for %#v lacks the prefix", channel, subject)
			}
			if previous, collided := channels[channel]; collided {
				t.Fatalf("keys %#v and %#v both mint channel %q", previous, subject, channel)
			}
			channels[channel] = subject
		}
	}
	if len(channels) != len(tenants)*len(sessions) {
		t.Fatalf("minted %d distinct channels for %d keys", len(channels), len(tenants)*len(sessions))
	}
	// Anti-vacuity for the choice of space: at least one pair in it does
	// collide under the naive spelling this encoding exists to rule out.
	naive := map[string]bool{}
	collisions := 0
	for _, tenant := range tenants {
		for _, session := range sessions {
			plain := string(tenant) + "." + string(session)
			if naive[plain] {
				collisions++
			}
			naive[plain] = true
		}
	}
	if collisions == 0 {
		t.Fatal("no pair in the enumerated space collides under plain concatenation, so the space does not exercise injectivity")
	}
}

// TestMaxChannelBytesBoundsEveryLegalKey checks the derivation against the
// widest key Core permits, so the node's channel ceiling cannot be a number
// that merely happened to fit the fixtures.
func TestMaxChannelBytesBoundsEveryLegalKey(t *testing.T) {
	t.Parallel()

	widest := registry.Key{
		TenantID:  sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes)),
		SessionID: sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes)),
	}
	if err := widest.TenantID.Validate(); err != nil {
		t.Fatalf("the widest tenant Core permits is invalid: %v", err)
	}
	if err := widest.SessionID.Validate(); err != nil {
		t.Fatalf("the widest session Core permits is invalid: %v", err)
	}
	channel := hostlink.ChannelFor(widest)
	if len(channel) != hostlink.MaxChannelBytes {
		t.Fatalf("widest channel is %d bytes, want MaxChannelBytes = %d", len(channel), hostlink.MaxChannelBytes)
	}
	// The ceiling is above Centrifuge's own default, which is the reason
	// NewCentrifugeServer raises it rather than leaving it alone.
	if hostlink.MaxChannelBytes <= 255 {
		t.Fatalf("MaxChannelBytes = %d, which no longer exceeds Centrifuge's 255 default; the node override is now untested", hostlink.MaxChannelBytes)
	}
	if want := len(hostlink.ChannelPrefix) + 1 + 2*base64.RawURLEncoding.EncodedLen(sessionwire.MaxIDBytes); hostlink.MaxChannelBytes != want {
		t.Fatalf("MaxChannelBytes = %d, want the derived %d", hostlink.MaxChannelBytes, want)
	}
}

// ---------------------------------------------------------------------------
// Link close and link loss
// ---------------------------------------------------------------------------

// TestClosingOneLinkLeavesEveryOtherReplicaWorking is written so that no
// assertion in it is a negative made after a state-destroying action.
//
// Closing a link removes routes, so "link B still has a binding" would pass
// against a build that had never installed one. Every survival claim below is
// therefore POSITIVE and end-to-end: link B's route still wakes its consumer
// after link A is gone, and the counter it increments was zero before. The
// closing control is at the end — close B too and the same delivery is refused
// — which is what shows the delivery assertion can fail at all.
func TestClosingOneLinkLeavesEveryOtherReplicaWorking(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	a := mustBind(t, f.mux, linkA, testSession)
	b := mustBind(t, f.mux, linkB, testSession)
	consumer := f.consumers.consumers[residencyKey(testSession)]
	if got := consumer.recorded(); len(got) != 0 {
		t.Fatalf("consumer already woken before any delivery: %v", got)
	}

	dropped := f.mux.CloseLink(linkA)
	if len(dropped) != 1 || dropped[0] != a {
		t.Fatalf("CloseLink returned %#v, want exactly link A's one binding", dropped)
	}

	// POSITIVE: the surviving replica's route still works.
	deliver(t, f.mux, linkB, b.Channel, "after-the-loss")
	if got := consumer.recorded(); !slices.Equal(got, []sessionwire.CommandID{"after-the-loss"}) {
		t.Fatalf("hints after closing link A = %v, want the surviving replica's one wake", got)
	}
	if got := f.mux.Replicas(residencyKey(testSession)); !slices.Equal(got, []hostlink.LinkID{linkB}) {
		t.Fatalf("Replicas = %v, want only link B", got)
	}
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	// The runtime is untouched, which here means the residency row is byte-for-
	// byte what it was: this package can only read it.
	entry, held := f.registry.Get(residencyKey(testSession))
	if !held || entry.State != registry.StateResident || !entry.Accepting {
		t.Fatalf("residency after a link loss = %#v, want the resident row unchanged", entry)
	}

	// The closed link is gone: its route no longer resolves, and rebinding is
	// how a replica comes back.
	if refusal := refusalOf(t, f.mux.Deliver(linkA, a.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "ignored"})); refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("delivery on the closed link = %q, want %q", refusal.Refusal, hostlink.RefusalNotBound)
	}
	if f.mux.CloseLink(linkA) != nil {
		t.Fatal("closing an already closed link returned bindings")
	}

	// The control for the positive assertion above.
	f.mux.CloseLink(linkB)
	if refusal := refusalOf(t, f.mux.Deliver(linkB, b.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "ignored"})); refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("delivery after closing link B = %q, want %q; the survival assertion above would pass vacuously", refusal.Refusal, hostlink.RefusalNotBound)
	}
	if got := consumer.recorded(); len(got) != 1 {
		t.Fatalf("hints = %v, want still exactly one; a refused delivery must wake nothing", got)
	}
}

func TestClosingOneLinkLeavesItsOtherSessionsAlone(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	mustBind(t, f.mux, linkA, testSession)
	mustBind(t, f.mux, linkA, otherSession)
	surviving := mustBind(t, f.mux, linkB, thirdSession)

	dropped := f.mux.CloseLink(linkA)
	if len(dropped) != 2 {
		t.Fatalf("CloseLink dropped %d bindings, want both of link A's", len(dropped))
	}
	deliver(t, f.mux, linkB, surviving.Channel, "still-routed")
	if got := f.consumers.consumers[residencyKey(thirdSession)].recorded(); !slices.Equal(got, []sessionwire.CommandID{"still-routed"}) {
		t.Fatalf("the untouched link's route = %v, want one wake", got)
	}
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	// The budget the closed link held came back: the Host cap is three and two
	// more binds now fit.
	mustBind(t, f.mux, linkB, testSession)
	mustBind(t, f.mux, "link-c", otherSession)
}

// TestInvalidatingOneSessionDropsEveryReplicaOfItAndNothingElse holds the
// session-scoped invalidation a lost live tail performs.
//
// The two claims are opposite and are asserted separately, because one of them
// is a negative made after a state-destroying action and would pass against a
// build that never installed the route it claims survived. So the SURVIVAL
// claims are positive and end-to-end — the surviving routes still wake their
// consumers, from a counter that was zero — and the closing control at the end
// shows the delivery assertion can fail at all.
func TestInvalidatingOneSessionDropsEveryReplicaOfItAndNothingElse(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	a := mustBind(t, f.mux, linkA, testSession)
	b := mustBind(t, f.mux, linkB, testSession)
	sameLink := mustBind(t, f.mux, linkA, otherSession)
	if got := f.mux.Len(); got != 3 {
		t.Fatalf("Len before invalidation = %d, want 3", got)
	}

	dropped := f.mux.InvalidateSession(residencyKey(testSession))
	if !slices.Equal(dropped, []hostlink.Binding{a, b}) {
		t.Fatalf("InvalidateSession returned %#v, want both replicas of the session in link order", dropped)
	}
	if got := f.mux.Replicas(residencyKey(testSession)); got != nil {
		t.Fatalf("Replicas after invalidation = %v, want none", got)
	}
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len after invalidation = %d, want only the untouched session's route", got)
	}

	// POSITIVE: the SAME LINK's route to a DIFFERENT session still works. This
	// is the claim that separates invalidating a session from closing a link.
	other := f.consumers.consumers[residencyKey(otherSession)]
	if got := other.recorded(); len(got) != 0 {
		t.Fatalf("the untouched session's consumer was already woken: %v", got)
	}
	deliver(t, f.mux, linkA, sameLink.Channel, "still-routed")
	if got := other.recorded(); !slices.Equal(got, []sessionwire.CommandID{"still-routed"}) {
		t.Fatalf("hints on the untouched session = %v, want one wake", got)
	}

	// The invalidated routes are gone on BOTH links, not just the first one the
	// map iteration happened to reach.
	if refusal := refusalOf(t, f.mux.Deliver(linkA, a.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "ignored"})); refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("delivery on link A's invalidated route = %q, want %q", refusal.Refusal, hostlink.RefusalNotBound)
	}
	if refusal := refusalOf(t, f.mux.Deliver(linkB, b.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "ignored"})); refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("delivery on link B's invalidated route = %q, want %q", refusal.Refusal, hostlink.RefusalNotBound)
	}
	// The residency is untouched: this package can only read it, and a tail
	// that stopped being deliverable is not a session that stopped existing.
	entry, held := f.registry.Get(residencyKey(testSession))
	if !held || entry.State != registry.StateResident {
		t.Fatalf("residency after invalidation = %#v, want the resident row unchanged", entry)
	}
	// Rebinding is how a replica comes back, which is what makes this a reset
	// rather than a revocation.
	mustBind(t, f.mux, linkA, testSession)

	// Repeating it is safe, and a session nobody routes to invalidates nothing.
	f.mux.InvalidateSession(residencyKey(testSession))
	if got := f.mux.InvalidateSession(residencyKey(testSession)); got != nil {
		t.Fatalf("re-invalidating = %#v, want nothing", got)
	}
	if got := f.mux.InvalidateSession(residencyKey(thirdSession)); got != nil {
		t.Fatalf("invalidating an unrouted session = %#v, want nothing", got)
	}

	// The control for the positive survival assertion above.
	f.mux.InvalidateSession(residencyKey(otherSession))
	if refusal := refusalOf(t, f.mux.Deliver(linkA, sameLink.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "ignored"})); refusal.Refusal != hostlink.RefusalNotBound {
		t.Fatalf("delivery after invalidating the other session = %q, want %q; the survival assertion above would pass vacuously", refusal.Refusal, hostlink.RefusalNotBound)
	}
	if got := other.recorded(); len(got) != 1 {
		t.Fatalf("hints = %v, want still exactly one; a refused delivery must wake nothing", got)
	}
	if got := f.mux.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0; every route was invalidated", got)
	}
}

// ---------------------------------------------------------------------------
// Over the wire
// ---------------------------------------------------------------------------

type rpcReply struct {
	ID  uint32 `json:"id"`
	RPC *struct {
		Data json.RawMessage `json:"data"`
	} `json:"rpc,omitempty"`
	Subscribe *json.RawMessage `json:"subscribe,omitempty"`
	Error     *struct {
		Code    uint32 `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func sendCommand(t *testing.T, connection *websocket.Conn, id uint32, command any) rpcReply {
	t.Helper()
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write command: %v", err)
	}
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, response, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	var reply rpcReply
	if err := json.Unmarshal(response, &reply); err != nil {
		t.Fatalf("decode reply %q: %v", response, err)
	}
	if reply.ID != id {
		t.Fatalf("reply id = %d, want %d (%s)", reply.ID, id, response)
	}
	return reply
}

func rpc(t *testing.T, connection *websocket.Conn, id uint32, method string, body any) rpcReply {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal rpc body: %v", err)
	}
	return sendCommand(t, connection, id, map[string]any{
		"id":  id,
		"rpc": map[string]any{"method": method, "data": json.RawMessage(data)},
	})
}

func acceptedRPC(t *testing.T, reply rpcReply) {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("rpc was refused at the transport: %#v", *reply.Error)
	}
	if reply.RPC != nil && len(reply.RPC.Data) != 0 && string(reply.RPC.Data) != "null" {
		t.Fatalf("accepted rpc returned a body %s, want none", reply.RPC.Data)
	}
}

func refusedRPC(t *testing.T, reply rpcReply) sessionwire.HostLinkError {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("rpc failed at the transport instead of publishing a Core class: %#v", *reply.Error)
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 {
		t.Fatalf("refused rpc returned no body: %#v", reply)
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply.RPC.Data, &refusal); err != nil {
		t.Fatalf("refusal body %s is not a Core HostLinkError: %v", reply.RPC.Data, err)
	}
	return refusal
}

// TestBindingRPCsSpeakCoreRecordsOverTheLink drives the whole path a Factory
// takes: bind, subscribe, deliver a command, and be refused with a Core class.
func TestBindingRPCsSpeakCoreRecordsOverTheLink(t *testing.T) {
	f := newFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}

	acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, bindRequest(testSession)))
	channel := hostlink.ChannelFor(residencyKey(testSession))
	if got := f.mux.Replicas(residencyKey(testSession)); len(got) != 1 {
		t.Fatalf("Replicas after a wire bind = %v, want one link", got)
	}

	// The subscription follows the binding, and a channel the link did not bind
	// is refused even though it names a resident session.
	if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
		t.Fatalf("subscribe to a bound channel was refused: %#v", *reply.Error)
	}
	unbound := sendCommand(t, connection, 4, map[string]any{"id": 4, "subscribe": map[string]any{"channel": hostlink.ChannelFor(residencyKey(otherSession))}})
	if unbound.Error == nil {
		t.Fatal("subscribe to an unbound channel succeeded")
	}

	// A command RPC names its binding in the method and carries a CommandID and
	// nothing else in its body.
	acceptedRPC(t, rpc(t, connection, 5, channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}))
	acceptedRPC(t, rpc(t, connection, 6, channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}))
	if got := f.consumers.consumers[residencyKey(testSession)].recorded(); !slices.Equal(got, []sessionwire.CommandID{"command-one", "command-one"}) {
		t.Fatalf("hints = %v, want the duplicate wake through the link", got)
	}

	stale := bindRequest(otherSession)
	stale.LeaseEpoch = testEpoch + 1
	refusal := refusedRPC(t, rpc(t, connection, 7, hostlink.MethodBind, stale))
	if refusal.Code != sessionwire.HostLinkErrorEpochMismatch || refusal.CurrentLeaseEpoch != testEpoch {
		t.Fatalf("refusal = %#v, want an epoch mismatch naming the current epoch", refusal)
	}

	acceptedRPC(t, rpc(t, connection, 8, hostlink.MethodUnbind, unbindRequest(testSession)))
	if got := f.mux.Len(); got != 0 {
		t.Fatalf("Len after a wire unbind = %d, want 0", got)
	}
}

// TestLosingOneLinkLeavesTheOtherReplicaRouted is the end-to-end form of the
// link-loss property, over two real connections.
//
// The survival claim is positive and is made THROUGH the surviving link: after
// the first connection is dropped, a command RPC on the second still wakes the
// consumer, and its recorded wake was absent beforehand. The control at the end
// drops the second connection too and shows the same RPC then fails.
func TestLosingOneLinkLeavesTheOtherReplicaRouted(t *testing.T) {
	f := newFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	first := dial(t, httpServer.URL, "")
	defer first.Close()
	if reply := connect(t, first, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("first replica failed to connect")
	}
	second := dial(t, httpServer.URL, "")
	defer second.Close()
	if reply := connect(t, second, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("second replica failed to connect")
	}

	acceptedRPC(t, rpc(t, first, 2, hostlink.MethodBind, bindRequest(testSession)))
	acceptedRPC(t, rpc(t, second, 2, hostlink.MethodBind, bindRequest(testSession)))
	waitForReplicas(t, f.mux, residencyKey(testSession), 2)
	consumer := f.consumers.consumers[residencyKey(testSession)]
	if got := consumer.recorded(); len(got) != 0 {
		t.Fatalf("consumer woken before any delivery: %v", got)
	}

	first.Close()
	waitForReplicas(t, f.mux, residencyKey(testSession), 1)

	channel := hostlink.ChannelFor(residencyKey(testSession))
	acceptedRPC(t, rpc(t, second, 3, channel, sessionwire.HostLinkCommandDelivery{CommandID: "after-the-loss"}))
	if got := consumer.recorded(); !slices.Equal(got, []sessionwire.CommandID{"after-the-loss"}) {
		t.Fatalf("hints after the first link was lost = %v, want the survivor's one wake", got)
	}
	entry, held := f.registry.Get(residencyKey(testSession))
	if !held || entry.State != registry.StateResident {
		t.Fatalf("residency after a link loss = %#v, want the resident row untouched", entry)
	}

	// The control. Drop the survivor too and the identical RPC is refused, so
	// the assertion above was capable of failing.
	second.Close()
	waitForReplicas(t, f.mux, residencyKey(testSession), 0)
	third := dial(t, httpServer.URL, "")
	defer third.Close()
	if reply := connect(t, third, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("third connection failed to connect")
	}
	refusal := refusedRPC(t, rpc(t, third, 2, channel, sessionwire.HostLinkCommandDelivery{CommandID: "unrouted"}))
	if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("delivery over an unbound link = %q, want runtime_unavailable", refusal.Code)
	}
	if got := consumer.recorded(); len(got) != 1 {
		t.Fatalf("hints = %v, want still one; a link that never bound must wake nothing", got)
	}
}

// waitForReplicas waits for a disconnect to be observed, which is asynchronous
// on the server side.
func waitForReplicas(t *testing.T, mux *hostlink.Multiplexer, subject registry.Key, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := mux.Replicas(subject)
		if len(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Replicas = %v, want %d after waiting", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// The heartbeat mechanism is the Host's, not the client's
// ---------------------------------------------------------------------------

// TestClientCannotSelectTheFramePingPongMechanism settles the key O5.1 handed
// over undecided, and settles it by refusing it.
//
// centrifuge@v0.38.0 handler_websocket.go:142 reads cf_ws_frame_ping_pong from
// the query and, when it is "true", hands the transport a PingPongConfig of
// {-1,-1}. A zero-configured HostLink sends no PingPongConfig of its own, so on
// that path the transport's would win and the connect reply would carry neither
// ping nor pong: the client, not the operator, would have chosen the liveness
// mechanism. The control below is what makes that concrete rather than
// asserted — the same zero-configured server does advertise a heartbeat when
// nothing names the key.
//
// The gate is on PRESENCE, so "false" is refused too, and the third row is the
// evidence that it is presence and not the value: a DIFFERENT unknown query
// name carrying the same value is admitted.
func TestClientCannotSelectTheFramePingPongMechanism(t *testing.T) {
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{})
	defer closeServers(t, server, httpServer)

	for _, suffix := range []string{
		"?format=json&cf_ws_frame_ping_pong=true",
		"?format=json&cf_ws_frame_ping_pong=false",
		"?format=json&cf_ws_frame_ping_pong=",
		"?cf_protocol=json&cf_ws_frame_ping_pong=true",
		"?cf_ws_frame_ping_pong=true",
	} {
		t.Run(suffix, func(t *testing.T) {
			dialer := websocket.Dialer{Subprotocols: []string{"centrifuge-json"}}
			connection, response, err := dialer.Dial(wsURL(httpServer.URL)+suffix, nil)
			assertUpgradeRejected(t, connection, response, err)
		})
	}

	// The control for "presence, not value": an unknown key the selector does
	// not list is admitted at the same value.
	t.Run("unlisted key at the same value", func(t *testing.T) {
		dialer := websocket.Dialer{Subprotocols: []string{"centrifuge-json"}}
		connection, response, err := dialer.Dial(wsURL(httpServer.URL)+"?format=json&cf_ws_frame_ping_pong_unlisted=true", nil)
		if err != nil {
			t.Fatalf("unlisted key was rejected: %v (response %#v)", err, response)
		}
		connection.Close()
	})

	// The control for the refusal being worth making: with the key absent, this
	// zero-configured server DOES advertise an application-level heartbeat, so
	// the thing being protected exists.
	t.Run("zero configuration still heartbeats", func(t *testing.T) {
		connection := dial(t, httpServer.URL, "")
		defer connection.Close()
		reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}})
		if reply.Connect == nil || reply.Connect.Ping == 0 || !reply.Connect.Pong {
			t.Fatalf("zero-configured connect heartbeat = %#v, want a non-zero ping and pong", reply.Connect)
		}
	})
}

// ---------------------------------------------------------------------------
// The node's channel limits are HostLink's, not Centrifuge's defaults
// ---------------------------------------------------------------------------

// customServer builds a server and Multiplexer over an explicit tenant and
// budget, for the two tests whose whole subject is a limit the shared fixture
// cannot reach.
func customServer(t *testing.T, tenant sessionwire.TenantID, sessions []sessionwire.SessionID, perLink int) (*hostlink.Multiplexer, *httptest.Server) {
	t.Helper()
	index := registry.New(frozenClock{at: time.Unix(1_700_000_000, 0).UTC()})
	consumers := &stubConsumers{consumers: map[registry.Key]*stubConsumer{}}
	for _, session := range sessions {
		subject := registry.Key{TenantID: tenant, SessionID: session}
		if _, inserted := index.Insert(subject, registry.Admission{CompatibilityID: testRuntime, LeaseEpoch: testEpoch}); !inserted {
			t.Fatalf("insert %q", session)
		}
		consumers.consumers[subject] = &stubConsumer{}
	}
	mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID:           tenant,
		HostID:             testHostID,
		HostGeneration:     testGeneration,
		Residencies:        &recordingResidencies{inner: index},
		Admission:          &stubAdmission{},
		Consumers:          consumers,
		MaxBindingsPerLink: perLink,
		MaxBindings:        perLink,
	})
	if err != nil {
		t.Fatalf("NewMultiplexer: %v", err)
	}
	server, err := hostlink.NewCentrifugeServer(hostlink.Config{
		TenantID:      tenant,
		Authenticator: &recordingAuthenticator{wantToken: testCredential},
		Multiplexer:   mux,
	})
	if err != nil {
		t.Fatalf("NewCentrifugeServer: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() { closeServers(t, server, httpServer) })
	return mux, httpServer
}

func longBind(tenant sessionwire.TenantID, session sessionwire.SessionID) sessionwire.HostLinkBindRequest {
	request := bindRequest(session)
	request.TenantID = tenant
	return request
}

// TestTheWidestLegalSessionIsStillRoutable is the reader for the node's
// ChannelMaxLength override.
//
// Centrifuge defaults that ceiling to 255 and refuses a longer channel at
// subscribe, and two maximum-length Core identifiers encode to far more than
// that, so without the override a legal session would be bindable and then
// unsubscribable — a failure that appears one round trip after the acceptance.
func TestTheWidestLegalSessionIsStillRoutable(t *testing.T) {
	widestTenant := sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes))
	widestSession := sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes))
	mux, httpServer := customServer(t, widestTenant, []sessionwire.SessionID{widestSession}, 1)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("connect failed")
	}
	acceptedRPC(t, rpc(t, connection, 2, hostlink.MethodBind, longBind(widestTenant, widestSession)))

	channel := hostlink.ChannelFor(registry.Key{TenantID: widestTenant, SessionID: widestSession})
	if len(channel) <= 255 {
		t.Fatalf("the widest channel is %d bytes, which Centrifuge's default already admits; this test no longer exercises the override", len(channel))
	}
	if reply := sendCommand(t, connection, 3, map[string]any{"id": 3, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
		t.Fatalf("subscribing to the widest legal channel was refused: %#v", *reply.Error)
	}
	if got := mux.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

// TestOneLinkMayHoldMoreChannelsThanCentrifugeAllowsByDefault is the reader for
// the node's ClientChannelLimit override.
//
// Centrifuge defaults it to 128. Left alone it would be a SECOND answer to "how
// many sessions may one link hold", silently different from the Multiplexer's
// and arriving one round trip late: the bind would be accepted and the
// subscribe refused. The count below is one over that default, which is the
// smallest number that can tell the two answers apart.
func TestOneLinkMayHoldMoreChannelsThanCentrifugeAllowsByDefault(t *testing.T) {
	const centrifugeDefaultChannelLimit = 128
	const wanted = centrifugeDefaultChannelLimit + 1

	tenant := sessionwire.TenantID("tenant-wide")
	sessions := make([]sessionwire.SessionID, 0, wanted)
	for index := range wanted {
		sessions = append(sessions, sessionwire.SessionID(fmt.Sprintf("session-%03d", index)))
	}
	mux, httpServer := customServer(t, tenant, sessions, wanted)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("connect failed")
	}
	id := uint32(1)
	for _, session := range sessions {
		id++
		acceptedRPC(t, rpc(t, connection, id, hostlink.MethodBind, longBind(tenant, session)))
		id++
		channel := hostlink.ChannelFor(registry.Key{TenantID: tenant, SessionID: session})
		if reply := sendCommand(t, connection, id, map[string]any{"id": id, "subscribe": map[string]any{"channel": channel}}); reply.Error != nil {
			t.Fatalf("subscribe for %q of %d was refused: %#v", session, wanted, *reply.Error)
		}
	}
	if got := mux.Len(); got != wanted {
		t.Fatalf("Len = %d, want %d", got, wanted)
	}
}

// ---------------------------------------------------------------------------
// Unbind and dispatch edges
// ---------------------------------------------------------------------------

// TestUnbindReportsAForeignTenantOrHost pins two checks that are NOT the same
// kind of thing, and an earlier version of this comment got that wrong by
// offering one justification for both.
//
// The TENANT check is equivalent for EFFECT. ChannelFor is injective over
// (TenantID, SessionID), so a request naming another tenant computes a channel
// this link cannot be holding, and with that check alone deleted the removal is
// a measured no-op. What it buys is that a Factory pointed at the wrong tenant
// is TOLD so, instead of watching its unbinds silently succeed forever.
//
// The HOST check is LOAD-BEARING for the removal, and injectivity says nothing
// about it: the addressed Host is not a component of registry.Key and so not a
// component of the channel. With that check alone deleted, an unbind addressed
// to another Host REMOVES this Host's route — measured, the held route falls
// from 1 to 0 — and the reader of the difference is the removal itself. That
// property is asserted below AHEAD of the refusal, because inspecting the
// refusal first stops the test at the missing error and never reaches the
// routing table, which is how the false claim above survived.
func TestUnbindReportsAForeignTenantOrHost(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	mustBind(t, f.mux, linkA, testSession)

	foreignTenant := unbindRequest(testSession)
	foreignTenant.TenantID = "tenant-other"
	if refusal := refusalOf(t, f.mux.Unbind(linkA, foreignTenant)); refusal.Refusal != hostlink.RefusalForeignTenant {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalForeignTenant)
	}
	foreignHost := unbindRequest(testSession)
	foreignHost.HostID = "host-beta"
	foreignHostErr := f.mux.Unbind(linkA, foreignHost)
	// Ahead of the refusal, deliberately: this is the load-bearing half, and a
	// refusal assertion first would stop here and never reach the table.
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len = %d after an unbind addressed to another Host, want the route still held", got)
	}
	if refusal := refusalOf(t, foreignHostErr); refusal.Refusal != hostlink.RefusalForeignHost {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalForeignHost)
	}
	malformed := unbindRequest(testSession)
	malformed.LeaseEpoch = 0
	if refusal := refusalOf(t, f.mux.Unbind(linkA, malformed)); refusal.Refusal != hostlink.RefusalMalformedRequest {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalMalformedRequest)
	}
	// The route the refused unbinds named is still held, so none of them acted.
	if got := f.mux.Len(); got != 1 {
		t.Fatalf("Len = %d, want the binding still held", got)
	}
	// And an unbind naming a stale epoch still works, because a Factory that
	// has just been told its epoch is stale must be able to drop its route.
	stale := unbindRequest(testSession)
	stale.LeaseEpoch = testEpoch + 1
	if err := f.mux.Unbind(linkA, stale); err != nil {
		t.Fatalf("unbind at a stale epoch: %v", err)
	}
	if got := f.mux.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

// TestAnUnboundLinkLearnsNothingFromItsOwnMalformedBody pins the order inside
// dispatch: the binding is resolved before the body is decoded, so a link with
// no route gets the same answer whatever it sent.
func TestAnUnboundLinkLearnsNothingFromItsOwnMalformedBody(t *testing.T) {
	f := newFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatal("connect failed")
	}
	channel := hostlink.ChannelFor(residencyKey(testSession))

	wellFormed := refusedRPC(t, rpc(t, connection, 2, channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}))
	garbage := refusedRPC(t, sendCommand(t, connection, 3, map[string]any{
		"id":  3,
		"rpc": map[string]any{"method": channel, "data": map[string]any{"command_id": "command-one", "payload": "secret"}},
	}))
	if wellFormed.Code != sessionwire.HostLinkErrorRuntimeUnavailable || garbage != wellFormed {
		t.Fatalf("unbound link got %#v for a valid body and %#v for a malformed one; they must be identical", wellFormed, garbage)
	}

	// The control: once the link HOLDS the route, the malformed body is
	// distinguished, so the identity above is the unbound link's answer and not
	// the dispatcher's only answer.
	acceptedRPC(t, rpc(t, connection, 4, hostlink.MethodBind, bindRequest(testSession)))
	bad := sendCommand(t, connection, 5, map[string]any{
		"id":  5,
		"rpc": map[string]any{"method": channel, "data": map[string]any{"command_id": "command-one", "payload": "secret"}},
	})
	if bad.Error == nil {
		t.Fatalf("a malformed body on a bound route was accepted: %#v", bad)
	}
	if got := f.consumers.consumers[residencyKey(testSession)].recorded(); len(got) != 0 {
		t.Fatalf("a malformed body woke the consumer: %v", got)
	}
}
