package service_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// fakeClock is the time source every test in this file runs on.
//
// It is a FAKE and not a stub: Advance moves it, so a heartbeat, an expiry and
// a crash are all expressed as arithmetic on a value this test controls. That
// is what removes the race by construction — nothing here waits for a real
// instant to arrive, so there is nothing to sleep on and nothing to poll.
//
// Now is mutex-guarded because TestPublisherIsSafeUnderConcurrentUse runs
// it from several goroutines under -race.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	nows   int
	timers int
}

// newFakeClock starts the clock at a NON-ZERO instant.
//
// The instant matters. Core's validateHostLinkTimes rejects a zero ObservedAt,
// so a fixture defaulting to time.Time{} would make every advertisement in this
// file invalid for a reason unrelated to what each test is about — and
// TestPublishRefusesAnObservationCoreWouldReject, which feeds the zero time
// deliberately, would be indistinguishable from every other test.
func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 2, 11, 22, 33, 44, time.UTC)}
}

// Now returns the current fake instant and counts the call.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nows++
	return c.now
}

// NewTimer counts the call and returns a real timer.
//
// Nothing in capacity.go calls it — TestPublishConsultsOnlyTheClocksNow asserts
// the count stays zero — so the returned timer is never received from. It is a
// real one anyway rather than nil, so that a future caller gets a working timer
// instead of a nil dereference that would present as a crash rather than as the
// assertion failure that count is there to produce.
func (c *fakeClock) NewTimer(d time.Duration) *time.Timer {
	c.mu.Lock()
	c.timers++
	c.mu.Unlock()
	return time.NewTimer(d)
}

// Advance moves the clock forward.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Counts reports how many times each Clock method was called.
func (c *fakeClock) Counts() (nows, timers int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nows, c.timers
}

// frozenClock is a Clock stuck at whatever instant it was given, including the
// zero time. It exists for the one test that needs an observation Core refuses.
type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time                       { return c.at }
func (c frozenClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// inertStore, inertWorkspaces and inertAuth satisfy the collaborators host.New
// requires and that this file never exercises.
type inertStore struct{}

func (inertStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

type inertWorkspaces struct{}

func (inertWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", nil
}

type inertAuth struct{}

func (inertAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// testTarget is a LaunchTarget whose compatibility identity, admission weight
// and placement support are all set per row, because all three are published.
type testTarget struct {
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
}

func (t testTarget) CompatibilityID() department.CompatibilityID { return t.compatibility }
func (t testTarget) Capabilities() department.Capabilities       { return t.capabilities }

func (testTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("test target launches nothing")
}

func (testTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("test target launches nothing")
}

// pooledCapabilities is a capability set both placements accept.
func pooledCapabilities(weight uint64) department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   weight,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

// registration builds one Department entry with a per-agent compatibility ID,
// so a test can tell one published row from another by more than its agent.
func registration(agent sessionwire.AgentID, capabilities department.Capabilities) department.Registration {
	return department.Registration{
		AgentID: agent,
		Target:  testTarget{compatibility: department.CompatibilityID("rig-" + string(agent) + "-2026-09"), capabilities: capabilities},
	}
}

func testDepartment(t *testing.T, registrations ...department.Registration) *department.Department {
	t.Helper()
	if len(registrations) == 0 {
		registrations = []department.Registration{registration("reviewer", pooledCapabilities(1))}
	}
	built, err := department.New(registrations)
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	return built
}

// pooledOptions is a valid pooled Host configuration. Every duration is a
// distinct value so an assertion that reads the wrong one cannot agree by
// accident, and RegistryExpiry is deliberately not a round multiple of
// RegistryHeartbeat.
func pooledOptions(t *testing.T, clock hostconfig.Clock) hostconfig.Options {
	t.Helper()
	return hostconfig.Options{
		HostID:            "host-7c1",
		InternalEndpoint:  "wss://host-7c1.internal.example:8443",
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        testDepartment(t),
		SessionStore:      inertStore{},
		Workspaces:        inertWorkspaces{},
		Clock:             clock,
		Auth:              inertAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          8,
		WarmTTL:           97 * time.Second,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    31 * time.Second,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: 23 * time.Second,
		ReconcileBatch:    129,
	}
}

// testTenant is the tenant this package's keys name. It is a TEST constant and
// no longer a Host one: since H8 a Host is constructed with no tenant at all,
// so the fixture must name the tenant it admits rather than read it back off
// the configuration.
const testTenant sessionwire.TenantID = "tenant-9f3"

// testHostGeneration is a non-zero incarnation identity. Core rejects zero.
const testHostGeneration = uint64(9)

func newHost(t *testing.T, options hostconfig.Options) *hostconfig.Host {
	t.Helper()
	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("hostconfig.New: %v", err)
	}
	return built
}

func newPublisher(t *testing.T, options hostconfig.Options) *service.CapacityPublisher {
	t.Helper()
	publisher, err := service.NewCapacityPublisher(service.CapacityOptions{
		Host:           newHost(t, options),
		HostGeneration: testHostGeneration,
	})
	if err != nil {
		t.Fatalf("NewCapacityPublisher: %v", err)
	}
	return publisher
}

func publish(t *testing.T, publisher *service.CapacityPublisher) []service.Advertisement {
	t.Helper()
	advertisements, err := publisher.Publish()
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return advertisements
}

// byAgent indexes a publication so a test can name the row it means.
func byAgent(t *testing.T, advertisements []service.Advertisement) map[sessionwire.AgentID]service.Advertisement {
	t.Helper()
	indexed := make(map[sessionwire.AgentID]service.Advertisement, len(advertisements))
	for _, advertisement := range advertisements {
		if _, duplicate := indexed[advertisement.Report.AgentID]; duplicate {
			t.Fatalf("two advertisements for agent %q in one publication", advertisement.Report.AgentID)
		}
		indexed[advertisement.Report.AgentID] = advertisement
	}
	return indexed
}

// ---------------------------------------------------------------------------
// Initial publish: cardinality, and every field surviving derivation
// ---------------------------------------------------------------------------

// TestInitialPublishDerivesOneAdvertisementPerLaunchTarget holds the cardinality
// rule at three sizes, including the multi-target size the runbook names.
//
// One agent is the size that HIDES a hardcoded single row, which is exactly the
// survivor host.TestDepartmentCardinalityIsNotConstrained was written for one
// layer down. The four-agent row is what makes "one per target" a claim rather
// than a coincidence.
func TestInitialPublishDerivesOneAdvertisementPerLaunchTarget(t *testing.T) {
	t.Parallel()

	for _, agents := range [][]sessionwire.AgentID{
		{"reviewer"},
		{"planner", "reviewer"},
		{"critic", "planner", "reviewer", "summariser"},
	} {
		t.Run(strconv.Itoa(len(agents))+" targets", func(t *testing.T) {
			t.Parallel()
			registrations := make([]department.Registration, 0, len(agents))
			for i, agent := range agents {
				registrations = append(registrations, registration(agent, pooledCapabilities(uint64(i+1))))
			}
			options := pooledOptions(t, newFakeClock())
			options.Department = testDepartment(t, registrations...)

			advertisements := publish(t, newPublisher(t, options))
			if len(advertisements) != len(agents) {
				t.Fatalf("Publish returned %d advertisements, want one per LaunchTarget (%d)", len(advertisements), len(agents))
			}
			indexed := byAgent(t, advertisements)
			for _, agent := range agents {
				if _, published := indexed[agent]; !published {
					t.Errorf("no advertisement for registered agent %q", agent)
				}
			}
			// Stable keys separate the rows. Without this a derivation that
			// produced N identical records would satisfy the count.
			keys := make([]string, 0, len(advertisements))
			for _, advertisement := range advertisements {
				keys = append(keys, advertisement.StableKey)
			}
			slices.Sort(keys)
			if len(slices.Compact(keys)) != len(agents) {
				t.Errorf("stable keys %v are not distinct across %d targets", keys, len(agents))
			}
		})
	}
}

// TestEveryPublishedFieldSurvivesDerivation asserts each field of the record
// individually, rather than asserting the record type declares them.
//
// A derivation that dropped one field would leave it at its zero value, and a
// whole-struct comparison against a hand-built expectation is only as good as
// the expectation. Each row below names the field and the SOURCE it must have
// come from, so a field taking its value from the wrong option fails here.
func TestEveryPublishedFieldSurvivesDerivation(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(2)))
	publisher := newPublisher(t, options)

	advertisements := publish(t, publisher)
	if len(advertisements) != 1 {
		t.Fatalf("Publish returned %d advertisements, want 1", len(advertisements))
	}
	got := advertisements[0]
	observed := clock.Now()

	for _, field := range []struct {
		name string
		got  any
		want any
	}{
		{"Report.Version", got.Report.Version, sessionwire.CurrentWireVersion},
		{"Report.HostID", got.Report.HostID, options.HostID},
		{"Report.HostGeneration", got.Report.HostGeneration, testHostGeneration},
		{"Report.AgentID", got.Report.AgentID, sessionwire.AgentID("reviewer")},
		{"Report.RuntimeCompatibilityID", got.Report.RuntimeCompatibilityID, "rig-reviewer-2026-09"},
		{"Report.Placement", got.Report.Placement, options.Placement},
		{"Report.InternalEndpoint", got.Report.InternalEndpoint, options.InternalEndpoint},
		{"Report.IsolationClass", got.Report.IsolationClass, options.IsolationClass},
		{"Report.Accepting", got.Report.Accepting, true},
		// Capacity 8, admission weight 2: four more of this target fit.
		{"Report.AvailableCapacity", got.Report.AvailableCapacity, uint64(4)},
		{"Report.ObservedAt", got.Report.ObservedAt, observed},
		{"Report.ExpiresAt", got.Report.ExpiresAt, observed.Add(options.RegistryExpiry)},
		// The LITERAL, not the constant. Comparing the field to the constant is
		// self-referential — a probe set the namespace to "x" and every
		// assertion including the storage-name grammar check still passed — so
		// the value itself is pinned here. It is two segments and host-scoped
		// so that a Host's target directory cannot collide with another
		// producer's records in the same backend.
		{"Namespace", got.Namespace, "host/target-directory"},
		{"AdvertisementNamespace", service.AdvertisementNamespace, "host/target-directory"},
		{"Rank", got.Rank, int64(4)},
		{"Ranked", got.Ranked, true},
		{"DueAt", got.DueAt, observed.Add(options.RegistryExpiry)},
		{"Tombstone", got.Tombstone, false},
	} {
		if fmt.Sprint(field.got) != fmt.Sprint(field.want) {
			t.Errorf("%s = %v, want %v", field.name, field.got, field.want)
		}
	}
	if got.StableKey == "" {
		t.Error("StableKey is empty, so the record has no identity to CAS against")
	}
	// The PREFIX literal, for the reason the namespace literal is pinned: a
	// probe set AdvertisementNamespace to "x" and every assertion passed,
	// because they all compared the field to the constant. This is the same
	// shape one field over, so it gets the same treatment.
	if !strings.HasPrefix(got.RankingScope, "launchtarget/") {
		t.Errorf("RankingScope = %q, want the two-segment form launchtarget/<digest> so a Host's ranking scopes cannot collide with another producer's in the same backend", got.RankingScope)
	}
	if got.RankingScope == "launchtarget/" {
		t.Error("RankingScope is the bare prefix, so every LaunchTarget would rank in one scope")
	}
	if err := got.Report.Validate(); err != nil {
		t.Errorf("published report is not a valid sessionwire record: %v", err)
	}
}

// TestStableKeyAndRankingScopeSeparateWhatTheSpecSeparates holds the two
// identities against each other, in both directions.
//
// The stable key is (agent, runtime compatibility, placement, host); the ranking
// scope is that tuple WITHOUT the host, because ranking compares Hosts against
// each other for the same LaunchTarget. A single row cannot distinguish those:
// asserting only that two agents differ would pass for a scope that included the
// host id, and asserting only that two hosts share a scope would pass for a
// scope that was a constant.
func TestStableKeyAndRankingScopeSeparateWhatTheSpecSeparates(t *testing.T) {
	t.Parallel()

	base := func(t *testing.T, mutate func(*hostconfig.Options)) map[sessionwire.AgentID]service.Advertisement {
		t.Helper()
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t,
			registration("planner", pooledCapabilities(1)),
			registration("reviewer", pooledCapabilities(1)),
		)
		mutate(&options)
		return byAgent(t, publish(t, newPublisher(t, options)))
	}

	first := base(t, func(*hostconfig.Options) {})
	otherHost := base(t, func(o *hostconfig.Options) {
		o.HostID = "host-b42"
		o.InternalEndpoint = "wss://host-b42.internal.example:8443"
	})
	dedicated := base(t, func(o *hostconfig.Options) {
		o.Placement = sessionwire.HostPlacementDedicated
		o.Capacity = 1
		o.FixedSessionID = "session-71c"
	})

	if first["planner"].RankingScope == first["reviewer"].RankingScope {
		t.Error("two different LaunchTargets share a ranking scope, so Factory would rank a planner against a reviewer")
	}
	if first["planner"].StableKey == first["reviewer"].StableKey {
		t.Error("two different LaunchTargets share a stable key, so one would overwrite the other")
	}
	if first["reviewer"].RankingScope != otherHost["reviewer"].RankingScope {
		t.Error("the same LaunchTarget on two Hosts has different ranking scopes, so they could never be compared for placement")
	}
	if first["reviewer"].StableKey == otherHost["reviewer"].StableKey {
		t.Error("two Hosts advertising the same LaunchTarget share a stable key, so each would overwrite the other's advertisement")
	}
	if first["reviewer"].RankingScope == dedicated["reviewer"].RankingScope {
		t.Error("pooled and dedicated advertisements for one agent share a ranking scope, so a pooled candidate could be ranked against a dedicated one")
	}

	// The agent and the compatibility id must BOTH be in the tuple, and neither
	// of the rows above shows that: the fixture derives each target's
	// compatibility id FROM its agent, so the two move together and a
	// derivation using only one of them is indistinguishable. A probe measured
	// exactly that — dropping the agent from the scope survived every assertion
	// above. These two rows break the fixture's coupling in each direction.
	shared := byAgent(t, publish(t, newPublisher(t, func() hostconfig.Options {
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t,
			department.Registration{AgentID: "planner", Target: testTarget{compatibility: "rig-shared-2026-09", capabilities: pooledCapabilities(1)}},
			department.Registration{AgentID: "reviewer", Target: testTarget{compatibility: "rig-shared-2026-09", capabilities: pooledCapabilities(1)}},
		)
		return options
	}())))
	if shared["planner"].RankingScope == shared["reviewer"].RankingScope {
		t.Error("two agents served by ONE runtime build share a ranking scope, so the agent identity is missing from the tuple and a planner would be ranked against a reviewer")
	}
	if shared["planner"].StableKey == shared["reviewer"].StableKey {
		t.Error("two agents served by ONE runtime build share a stable key, so one advertisement would overwrite the other")
	}

	upgraded := base(t, func(o *hostconfig.Options) {
		o.Department = testDepartment(t,
			department.Registration{AgentID: "reviewer", Target: testTarget{compatibility: "rig-reviewer-2026-10", capabilities: pooledCapabilities(1)}},
		)
	})
	if first["reviewer"].RankingScope == upgraded["reviewer"].RankingScope {
		t.Error("one agent on two runtime builds shares a ranking scope, so the compatibility id is missing from the tuple and a restore could be ranked onto an incompatible build")
	}
	if first["reviewer"].StableKey == upgraded["reviewer"].StableKey {
		t.Error("one agent on two runtime builds shares a stable key, so an upgraded Host would overwrite its own previous advertisement rather than publish a new one")
	}
	// The tuple's members are OPAQUE and may contain whatever byte the encoding
	// uses as a separator, so a separator alone cannot distinguish them. These
	// two Departments differ only in where the colon falls, and a derivation
	// that joined the members without a length prefix would hash them
	// identically and give two different LaunchTargets one record.
	colonInAgent := byAgent(t, publish(t, newPublisher(t, func() hostconfig.Options {
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t, department.Registration{AgentID: "a:b", Target: testTarget{compatibility: "c", capabilities: pooledCapabilities(1)}})
		return options
	}())))
	colonInCompatibility := byAgent(t, publish(t, newPublisher(t, func() hostconfig.Options {
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t, department.Registration{AgentID: "a", Target: testTarget{compatibility: "b:c", capabilities: pooledCapabilities(1)}})
		return options
	}())))
	if colonInAgent["a:b"].StableKey == colonInCompatibility["a"].StableKey {
		t.Error(`agent "a:b" with build "c" and agent "a" with build "b:c" derived one stable key, so the tuple encoding is ambiguous for identities containing its separator`)
	}
	if colonInAgent["a:b"].RankingScope == colonInCompatibility["a"].RankingScope {
		t.Error(`agent "a:b" with build "c" and agent "a" with build "b:c" derived one ranking scope, so the tuple encoding is ambiguous for identities containing its separator`)
	}

	// The scope must be usable as a storage name. The grammar is one or more
	// lower-case segments joined by '/', and an agent id is an opaque
	// sessionwire string that need not obey it, so the derivation may not
	// simply interpolate one.
	opaque := base(t, func(o *hostconfig.Options) {
		o.Department = testDepartment(t, registration("Rev/iewer Ünicode", pooledCapabilities(1)))
	})
	for agent, advertisement := range opaque {
		assertStorageName(t, "RankingScope for agent "+string(agent), advertisement.RankingScope)
		assertStorageName(t, "Namespace", advertisement.Namespace)
		if n := len(advertisement.StableKey); n == 0 || n > 256 {
			t.Errorf("StableKey is %d bytes, want 1..256", n)
		}
	}
}

// assertStorageName checks the storage name grammar: one or more '/'-joined
// segments, each starting [a-z0-9] and continuing [a-z0-9_.-], at most 512
// bytes. It is restated rather than imported because host does not depend on
// the storage module.
func assertStorageName(t *testing.T, what, name string) {
	t.Helper()
	if name == "" || len(name) > 512 {
		t.Errorf("%s = %q is not 1..512 bytes", what, name)
		return
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" {
			t.Errorf("%s = %q has an empty segment", what, name)
			return
		}
		for i := 0; i < len(segment); i++ {
			c := segment[i]
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			case i > 0 && (c == '_' || c == '.' || c == '-'):
			default:
				t.Errorf("%s = %q violates the storage name grammar at byte %d of segment %q", what, name, i, segment)
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

// TestHeartbeatMovesRankAndExpiryInOneRecord holds the atomicity the spec asks
// for in the only form this layer can hold it: rank and expiry are fields of ONE
// derived record, so a writer that CASes it writes both or neither.
//
// What this does NOT cover: the CAS itself. Nothing here writes to a
// SessionStore, so "atomically" is an assertion about the shape handed to the
// writer and not about the write. The writer is a later task and owns that half.
func TestHeartbeatMovesRankAndExpiryInOneRecord(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(1)))
	publisher := newPublisher(t, options)

	before := publish(t, publisher)[0]

	// One heartbeat interval passes and one session is admitted, so BOTH the
	// expiry and the rank have moved by the next publication. Moving only one
	// would leave the other's assertion satisfied by the first publication.
	clock.Advance(options.RegistryHeartbeat)
	if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "session-1"}, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	after := publish(t, publisher)[0]

	if after.StableKey != before.StableKey {
		t.Fatalf("heartbeat changed the stable key (%q -> %q), so it would insert a second record rather than refresh one", before.StableKey, after.StableKey)
	}
	if after.RankingScope != before.RankingScope {
		t.Errorf("heartbeat changed the ranking scope (%q -> %q)", before.RankingScope, after.RankingScope)
	}
	wantObserved := before.Report.ObservedAt.Add(options.RegistryHeartbeat)
	if !after.Report.ObservedAt.Equal(wantObserved) {
		t.Errorf("ObservedAt = %v, want %v: the heartbeat must re-observe at the current instant", after.Report.ObservedAt, wantObserved)
	}
	if !after.Report.ExpiresAt.Equal(wantObserved.Add(options.RegistryExpiry)) {
		t.Errorf("ExpiresAt = %v, want %v: the expiry must move with the observation", after.Report.ExpiresAt, wantObserved.Add(options.RegistryExpiry))
	}
	if !after.DueAt.Equal(after.Report.ExpiresAt) {
		t.Errorf("DueAt = %v, want ExpiresAt %v: the record is made due at its expiry", after.DueAt, after.Report.ExpiresAt)
	}
	if after.Rank != before.Rank-1 {
		t.Errorf("Rank = %d, want %d: rank is available capacity and one session was admitted", after.Rank, before.Rank-1)
	}
	if after.Rank != int64(after.Report.AvailableCapacity) {
		t.Errorf("Rank = %d but AvailableCapacity = %d: the record is ranked BY available capacity", after.Rank, after.Report.AvailableCapacity)
	}
}

// TestExpiryIsDerivedFromTheValidatedOptions holds that the expiry comes from
// RegistryExpiry and is not recomputed from the heartbeat or from a constant.
//
// Three scales, because one scale tests an arithmetic coincidence: at a single
// (heartbeat, expiry) pair a derivation using heartbeat*MinHeartbeatsBeforeExpiry
// instead of RegistryExpiry is indistinguishable whenever the fixture happens to
// satisfy the margin exactly. The rows below deliberately include one pair that
// meets the margin EXACTLY and two that exceed it.
func TestExpiryIsDerivedFromTheValidatedOptions(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name      string
		heartbeat time.Duration
		expiry    time.Duration
	}{
		{"exactly at the margin", 4 * time.Second, 12 * time.Second},
		{"well above the margin", 5 * time.Second, 31 * time.Second},
		{"minutes, not seconds", 90 * time.Second, 17 * time.Minute},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			options := pooledOptions(t, clock)
			options.RegistryHeartbeat = row.heartbeat
			options.RegistryExpiry = row.expiry
			advertisement := publish(t, newPublisher(t, options))[0]

			want := advertisement.Report.ObservedAt.Add(row.expiry)
			if !advertisement.Report.ExpiresAt.Equal(want) {
				t.Errorf("ExpiresAt = %v, want ObservedAt+RegistryExpiry = %v", advertisement.Report.ExpiresAt, want)
			}
		})
	}
}

// TestAdvertisementSurvivesTheMissedHeartbeatsItsMarginBuys is the other side of
// the expiry rule: the margin host.MinHeartbeatsBeforeExpiry validates is a
// promise about how many heartbeats may be lost, and the advertisement must
// actually keep it.
//
// The boundary is asserted in BOTH directions at each scale: one tick before the
// expiry it is still live, at the expiry it is gone.
func TestAdvertisementSurvivesTheMissedHeartbeatsItsMarginBuys(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name      string
		heartbeat time.Duration
		expiry    time.Duration
	}{
		{"exactly at the margin", 4 * time.Second, 12 * time.Second},
		{"well above the margin", 5 * time.Second, 31 * time.Second},
		{"minutes, not seconds", 90 * time.Second, 17 * time.Minute},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			options := pooledOptions(t, clock)
			options.RegistryHeartbeat = row.heartbeat
			options.RegistryExpiry = row.expiry
			advertisement := publish(t, newPublisher(t, options))[0]
			observed := advertisement.Report.ObservedAt

			// A missed heartbeat and its retry are both still inside the
			// expiry. That is what the margin is FOR.
			missable := hostconfig.MinHeartbeatsBeforeExpiry - 1
			for missed := 1; missed <= missable; missed++ {
				at := observed.Add(time.Duration(missed) * row.heartbeat)
				if advertisement.Expired(at) {
					t.Errorf("Expired(%v) = true after %d missed heartbeats, want false: the margin promises %d",
						at, missed, missable)
				}
			}
			if advertisement.Expired(observed.Add(row.expiry - time.Nanosecond)) {
				t.Error("Expired one nanosecond before ExpiresAt = true, want false")
			}
			if !advertisement.Expired(observed.Add(row.expiry)) {
				t.Error("Expired at exactly ExpiresAt = false, want true: a reader treats a lapsed record as absent")
			}
			if !advertisement.Expired(observed.Add(row.expiry + time.Nanosecond)) {
				t.Error("Expired one nanosecond after ExpiresAt = false, want true")
			}
		})
	}
}

// TestCrashedHostAdvertisementLapsesWithoutAFurtherPublish models the crash: a
// Host that stops publishing leaves its last record behind, and that record must
// become absent to a reader on its own.
//
// The crash is expressed as "no further call to Publish", which is the whole of
// what a crash is at this layer. Nothing is torn down, nothing is signalled.
func TestCrashedHostAdvertisementLapsesWithoutAFurtherPublish(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	publisher := newPublisher(t, options)
	last := publish(t, publisher)[0]

	// A live Host heartbeats here. This one does not.
	clock.Advance(options.RegistryExpiry)

	if !last.Expired(clock.Now()) {
		t.Error("a crashed Host's last advertisement has not lapsed at its expiry, so Factory would keep placing onto it")
	}
	// The control: had the Host heartbeated at the same instant, the refreshed
	// record would still be live. Without this row, an Expired that always
	// returned true would pass.
	refreshed := publish(t, publisher)[0]
	if refreshed.Expired(clock.Now()) {
		t.Error("a freshly published advertisement is already expired")
	}
	if !refreshed.Ranked || refreshed.Tombstone {
		t.Error("a crash-lapsed record must not be confused with a drained one: recovery republishes a ranked, non-tombstoned record")
	}
}

// TestRankSaturatesRatherThanWrapping holds the conversion at the top of its
// range, where a bare int64(available) inverts the ordering it exists to
// provide.
//
// Rank is signed and capacity is unsigned, and host.New puts no ceiling on
// Capacity, so this is reachable configuration rather than a hypothetical.
// Measured before the clamp existed: Capacity MaxUint64 gave Rank -1, which
// under descending-rank paging puts the emptiest Host LAST. Core validates the
// report either way, because AvailableCapacity is uint64 and only the ORDERING
// breaks — nothing downstream would have reported it.
//
// Three scales with the boundary as a row in both directions: below it the
// value passes through, at it the value passes through, above it the value
// saturates. One scale would only test that some large number came back.
func TestRankSaturatesRatherThanWrapping(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name     string
		capacity uint64
		wantRank int64
	}{
		{"ordinary", 8, 8},
		{"one below the signed ceiling", math.MaxInt64 - 1, math.MaxInt64 - 1},
		{"exactly the signed ceiling", math.MaxInt64, math.MaxInt64},
		{"one above the signed ceiling", uint64(math.MaxInt64) + 1, math.MaxInt64},
		{"the unsigned ceiling", math.MaxUint64, math.MaxInt64},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t, newFakeClock())
			options.Capacity = row.capacity
			options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(1)))
			advertisement := publish(t, newPublisher(t, options))[0]

			if advertisement.Rank != row.wantRank {
				t.Errorf("Rank = %d at capacity %d, want %d", advertisement.Rank, row.capacity, row.wantRank)
			}
			if advertisement.Rank < 0 {
				t.Errorf("Rank = %d is negative, so descending-rank paging would put the emptiest Host last", advertisement.Rank)
			}
			// The report is unaffected: AvailableCapacity is uint64 and Core
			// accepts the whole range, so the clamp must not narrow what a
			// reader is told about actual capacity.
			if advertisement.Report.AvailableCapacity != row.capacity {
				t.Errorf("AvailableCapacity = %d at capacity %d and weight 1, want %d: the rank clamp must not narrow the observation", advertisement.Report.AvailableCapacity, row.capacity, row.capacity)
			}
			if err := advertisement.Report.Validate(); err != nil {
				t.Errorf("report is not a valid sessionwire record: %v", err)
			}
		})
	}

	// Ordering is what the rank is FOR, so it is asserted directly rather than
	// inferred from the rows above: a larger Host never ranks below a smaller
	// one, including across the saturation boundary where two capacities share
	// a rank.
	rankAt := func(capacity uint64) int64 {
		options := pooledOptions(t, newFakeClock())
		options.Capacity = capacity
		options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(1)))
		return publish(t, newPublisher(t, options))[0].Rank
	}
	ascending := []uint64{1, 8, math.MaxInt64 - 1, math.MaxInt64, uint64(math.MaxInt64) + 1, math.MaxUint64}
	for i := 1; i < len(ascending); i++ {
		if rankAt(ascending[i]) < rankAt(ascending[i-1]) {
			t.Errorf("capacity %d ranks below capacity %d", ascending[i], ascending[i-1])
		}
	}
	if rankAt(8) >= rankAt(math.MaxInt64) {
		t.Error("a small Host does not rank below a huge one, so the ordering the rank exists to provide is gone")
	}
}

// ---------------------------------------------------------------------------
// Admission consumption and release
// ---------------------------------------------------------------------------

// TestAvailableCapacityTracksAdmissionAtThreeScales holds the admission
// threshold with the boundary as a row in both directions.
//
// Three scales because one tests an arithmetic coincidence: at capacity 8 and
// weight 1, "capacity - consumed" and "capacity / weight - consumed" and
// "(capacity - consumed) / weight" all agree. The weighted rows separate them,
// and the dedicated row pins the single-slot case Core independently constrains.
func TestAvailableCapacityTracksAdmissionAtThreeScales(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name      string
		capacity  uint64
		weight    uint64
		placement sessionwire.HostPlacement
	}{
		{"dedicated, one slot", 1, 1, sessionwire.HostPlacementDedicated},
		{"pooled, weight above one", 8, 3, sessionwire.HostPlacementPooled},
		{"pooled, thousands", 1000, 7, sessionwire.HostPlacementPooled},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			clock := newFakeClock()
			options := pooledOptions(t, clock)
			options.Capacity = row.capacity
			options.Placement = row.placement
			if row.placement == sessionwire.HostPlacementDedicated {
				options.FixedSessionID = "session-71c"
			}
			options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(row.weight)))
			publisher := newPublisher(t, options)

			admits := row.capacity / row.weight
			for admitted := uint64(0); admitted <= admits; admitted++ {
				advertisement := publish(t, publisher)[0]
				want := admits - admitted
				if advertisement.Report.AvailableCapacity != want {
					t.Fatalf("after %d admissions AvailableCapacity = %d, want %d", admitted, advertisement.Report.AvailableCapacity, want)
				}
				if advertisement.Rank != int64(want) {
					t.Fatalf("after %d admissions Rank = %d, want %d", admitted, advertisement.Rank, want)
				}
				if err := advertisement.Report.Validate(); err != nil {
					t.Fatalf("after %d admissions the report is not a valid sessionwire record: %v", admitted, err)
				}
				key := registry.Key{TenantID: testTenant, SessionID: sessionwire.SessionID("session-" + strconv.FormatUint(admitted, 10))}
				err := publisher.Admit(key, "reviewer")
				if admitted < admits {
					// The row BELOW the boundary: still admissible.
					if err != nil {
						t.Fatalf("admission %d of %d refused: %v", admitted+1, admits, err)
					}
					continue
				}
				// The row AT the boundary: the first refusal.
				if err == nil {
					t.Fatalf("admission %d was accepted, want refusal: capacity %d at weight %d admits %d", admitted+1, row.capacity, row.weight, admits)
				}
				var refused *service.AdmissionRefusedError
				if !errors.As(err, &refused) {
					t.Fatalf("Admit error is %T, want *service.AdmissionRefusedError", err)
				}
				if refused.Code != sessionwire.HostLinkErrorNoCapacity {
					t.Fatalf("Admit refusal code = %q, want %q", refused.Code, sessionwire.HostLinkErrorNoCapacity)
				}
			}
			// A full Host still ADVERTISES. Core documents a zero
			// AvailableCapacity as a valid observation Factory uses to avoid
			// new placement while maintaining existing HostLinks, so "full" is
			// not "gone" and is not "not accepting".
			full := publish(t, publisher)[0]
			if full.Report.AvailableCapacity != 0 {
				t.Errorf("AvailableCapacity = %d on a full Host, want 0", full.Report.AvailableCapacity)
			}
			if !full.Report.Accepting {
				t.Error("a full Host advertises Accepting=false, but zero available capacity is a valid accepting observation and only drain stops acceptance")
			}
			if !full.Ranked || full.Tombstone {
				t.Error("a full Host's advertisement is unranked or tombstoned, which is drain's meaning and not fullness's")
			}
		})
	}
}

// TestReleaseReturnsExactlyTheWeightItCharged holds the other direction of the
// admission ledger, including the two ways a release can be wrong.
func TestReleaseReturnsExactlyTheWeightItCharged(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	options.Capacity = 9
	options.Department = testDepartment(t,
		registration("reviewer", pooledCapabilities(2)),
		registration("planner", pooledCapabilities(3)),
	)
	publisher := newPublisher(t, options)

	reviewer := registry.Key{TenantID: testTenant, SessionID: "session-r"}
	planner := registry.Key{TenantID: testTenant, SessionID: "session-p"}
	for _, admission := range []struct {
		key   registry.Key
		agent sessionwire.AgentID
	}{{reviewer, "reviewer"}, {planner, "planner"}} {
		if err := publisher.Admit(admission.key, admission.agent); err != nil {
			t.Fatalf("Admit(%v): %v", admission.key, err)
		}
	}
	if got := publisher.ConsumedWeight(); got != 5 {
		t.Fatalf("ConsumedWeight = %d after admitting weights 2 and 3, want 5", got)
	}

	// Releasing the PLANNER must return 3, not 2 and not the whole ledger. A
	// release that returned a fixed weight, or the first weight, or everything,
	// each fails a different one of the three assertions below.
	if !publisher.Release(planner) {
		t.Fatal("Release of an admitted key reported nothing released")
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d after releasing the weight-3 session, want 2", got)
	}
	indexed := byAgent(t, publish(t, publisher))
	if got := indexed["reviewer"].Report.AvailableCapacity; got != (9-2)/2 {
		t.Errorf("reviewer AvailableCapacity = %d, want %d", got, (9-2)/2)
	}
	if got := indexed["planner"].Report.AvailableCapacity; got != (9-2)/3 {
		t.Errorf("planner AvailableCapacity = %d, want %d", got, (9-2)/3)
	}

	// A second release of the same key must not credit the weight twice, and an
	// unknown key must credit nothing at all. Either would let a Host admit
	// beyond its capacity, which is the failure the ledger exists to prevent.
	if publisher.Release(planner) {
		t.Error("releasing an already-released key reported a release")
	}
	if publisher.Release(registry.Key{TenantID: testTenant, SessionID: "never-admitted"}) {
		t.Error("releasing a key that was never admitted reported a release")
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d after two spurious releases, want 2", got)
	}
	if !publisher.Release(reviewer) {
		t.Error("Release of the remaining admitted key reported nothing released")
	}
	if got := publisher.ConsumedWeight(); got != 0 {
		t.Errorf("ConsumedWeight = %d after releasing everything, want 0", got)
	}
}

// TestAdmissionLedgerIsKeyedByTenantAndSession holds that the ledger key is the
// composite the registry uses, not a bare SessionID.
//
// Session identities are client-supplied opaque strings, so two tenants naming a
// session identically is ordinary. A bare-SessionID ledger would treat the
// second tenant's admission as a duplicate of the first and charge nothing.
func TestAdmissionLedgerIsKeyedByTenantAndSession(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Capacity = 4
	// Cross-tenant-isolated, because since H8 that is exactly the Host this
	// case describes: one that legitimately holds two tenants at once. The
	// LEDGER must not be the place a tenant assumption is enforced silently —
	// charging once for two distinct keys is an over-admission whichever
	// tenants they name — and under the exclusive class the refusal would
	// arrive before the charge and this property would go untested.
	options.IsolationClass = sessionwire.HostIsolationClassCrossTenantIsolated
	publisher := newPublisher(t, options)

	for _, tenant := range []sessionwire.TenantID{testTenant, "tenant-other"} {
		if err := publisher.Admit(registry.Key{TenantID: tenant, SessionID: "same-name"}, "reviewer"); err != nil {
			t.Fatalf("Admit for tenant %q: %v", tenant, err)
		}
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d, want 2: two distinct (tenant, session) keys were admitted", got)
	}
}

// TestRepeatedAdmissionOfOneKeyChargesOnce holds the ledger's idempotence and
// the one case it must refuse instead.
func TestRepeatedAdmissionOfOneKeyChargesOnce(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Department = testDepartment(t,
		registration("reviewer", pooledCapabilities(2)),
		registration("planner", pooledCapabilities(3)),
	)
	publisher := newPublisher(t, options)
	key := registry.Key{TenantID: testTenant, SessionID: "session-r"}

	for attempt := 1; attempt <= 3; attempt++ {
		if err := publisher.Admit(key, "reviewer"); err != nil {
			t.Fatalf("re-admitting the same key under the same agent, attempt %d: %v", attempt, err)
		}
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d after three identical admissions, want 2", got)
	}
	// A release must then actually release: an idempotent admit that recorded
	// the key three times would need three releases.
	if !publisher.Release(key) || publisher.ConsumedWeight() != 0 {
		t.Errorf("one release after three identical admissions left ConsumedWeight = %d, want 0", publisher.ConsumedWeight())
	}

	if err := publisher.Admit(key, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	err := publisher.Admit(key, "planner")
	if err == nil {
		t.Fatal("re-admitting one key under a DIFFERENT agent was accepted, so the ledger holds a weight that matches neither agent")
	}
	var conflict *service.AdmissionConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("Admit error is %T, want *service.AdmissionConflictError", err)
	}
	if conflict.Admitted != "reviewer" || conflict.Requested != "planner" {
		t.Errorf("conflict = admitted %q requested %q, want reviewer/planner", conflict.Admitted, conflict.Requested)
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d after a refused conflicting admission, want 2: a refusal charges nothing", got)
	}
}

// TestAdmissionRefusesAnAgentThisDepartmentDoesNotRegister holds that the ledger
// cannot be charged for a target this Host cannot launch.
func TestAdmissionRefusesAnAgentThisDepartmentDoesNotRegister(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	publisher := newPublisher(t, options)

	err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "not-registered")
	if err == nil {
		t.Fatal("admitting an unregistered agent was accepted")
	}
	var refused *service.AdmissionRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("Admit error is %T, want *service.AdmissionRefusedError", err)
	}
	if refused.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Errorf("refusal code = %q, want %q", refused.Code, sessionwire.HostLinkErrorRuntimeUnavailable)
	}
	var unknown *department.UnknownAgentError
	if !errors.As(err, &unknown) {
		t.Errorf("refusal does not unwrap to *department.UnknownAgentError, so a caller cannot reach the Department's own answer")
	}
	if got := publisher.ConsumedWeight(); got != 0 {
		t.Errorf("ConsumedWeight = %d after a refused admission, want 0", got)
	}
	// The control, one position over: the registered agent is admitted.
	if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "reviewer"); err != nil {
		t.Errorf("the registered agent was refused too, so the assertion above proves nothing: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Placement compatibility
// ---------------------------------------------------------------------------

// TestATargetThatCannotRunHereIsAdvertisedButNotAccepting holds the rule in both
// directions and for both placements.
//
// The row remains, because the cardinality rule is one advertisement per
// LaunchTarget and dropping the row would make a Department's targets and its
// advertisements disagree. What changes is Accepting, which is the field Factory
// filters on.
func TestATargetThatCannotRunHereIsAdvertisedButNotAccepting(t *testing.T) {
	t.Parallel()

	pooledOnly := department.Capabilities{
		SupportsPooled:  true,
		AdmissionWeight: 1,
		CaptureSafety:   department.CaptureSafetyStreaming,
	}
	dedicatedOnly := department.Capabilities{
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyUnboundedMaterialized,
	}

	for _, row := range []struct {
		name           string
		placement      sessionwire.HostPlacement
		isolation      sessionwire.HostIsolationClass
		wantAccepting  map[sessionwire.AgentID]bool
		wantRefusedFor sessionwire.AgentID
	}{
		{
			name:           "pooled Host",
			placement:      sessionwire.HostPlacementPooled,
			isolation:      sessionwire.HostIsolationClassTenantExclusive,
			wantAccepting:  map[sessionwire.AgentID]bool{"poolable": true, "dedicatable": false},
			wantRefusedFor: "dedicatable",
		},
		{
			name:           "dedicated Host",
			placement:      sessionwire.HostPlacementDedicated,
			isolation:      sessionwire.HostIsolationClassCrossTenantIsolated,
			wantAccepting:  map[sessionwire.AgentID]bool{"poolable": false, "dedicatable": true},
			wantRefusedFor: "poolable",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t, newFakeClock())
			options.Placement = row.placement
			options.IsolationClass = row.isolation
			if row.placement == sessionwire.HostPlacementDedicated {
				options.Capacity = 1
				options.FixedSessionID = "session-71c"
			}
			options.Department = testDepartment(t,
				registration("poolable", pooledOnly),
				registration("dedicatable", dedicatedOnly),
			)
			publisher := newPublisher(t, options)

			indexed := byAgent(t, publish(t, publisher))
			if len(indexed) != 2 {
				t.Fatalf("published %d advertisements, want one per LaunchTarget (2): an unplaceable target is still a registered target", len(indexed))
			}
			for agent, want := range row.wantAccepting {
				advertisement, published := indexed[agent]
				if !published {
					t.Fatalf("no advertisement for %q", agent)
				}
				if advertisement.Report.Accepting != want {
					t.Errorf("%q Accepting = %v, want %v under %s placement", agent, advertisement.Report.Accepting, want, row.placement)
				}
				// THE THIRD STATE, and the one a probe found undetermined: this
				// Host is not draining, so a guard that read Ranked as "not
				// draining" and one that read it as "is a candidate" agree
				// everywhere else and disagree exactly here. An unplaceable
				// target is unranked, because it can never become a candidate
				// on this Host; a FULL one stays ranked, because the next
				// heartbeat may find room, and TestAvailableCapacityTracks...
				// holds that side.
				if advertisement.Ranked != want {
					t.Errorf("%q Ranked = %v, want %v: an unplaceable target must not occupy a slot in the ranked page Factory then has to discard, and a placeable one must", agent, advertisement.Ranked, want)
				}
				if advertisement.Tombstone {
					t.Errorf("%q is tombstoned on a Host that is not draining, so an unplaceable target is indistinguishable from a drained one", agent)
				}
				if !want && advertisement.Report.AvailableCapacity != 0 {
					t.Errorf("%q advertises %d available capacity though it cannot be placed here", agent, advertisement.Report.AvailableCapacity)
				}
				if !want && advertisement.Rank != 0 {
					t.Errorf("%q has rank %d though it cannot be placed here", agent, advertisement.Rank)
				}
				if advertisement.Report.IsolationClass != row.isolation {
					t.Errorf("%q IsolationClass = %q, want %q", agent, advertisement.Report.IsolationClass, row.isolation)
				}
				if advertisement.Report.Placement != row.placement {
					t.Errorf("%q Placement = %q, want %q", agent, advertisement.Report.Placement, row.placement)
				}
				if err := advertisement.Report.Validate(); err != nil {
					t.Errorf("%q report is not a valid sessionwire record: %v", agent, err)
				}
			}
			err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, row.wantRefusedFor)
			if err == nil {
				t.Fatalf("admitting %q onto a %s Host was accepted, though the target does not support that placement", row.wantRefusedFor, row.placement)
			}
			var refused *service.AdmissionRefusedError
			if !errors.As(err, &refused) {
				t.Fatalf("Admit error is %T, want *service.AdmissionRefusedError", err)
			}
			if refused.Code != sessionwire.HostLinkErrorRuntimeMismatch {
				t.Errorf("refusal code = %q, want %q", refused.Code, sessionwire.HostLinkErrorRuntimeMismatch)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Drain
// ---------------------------------------------------------------------------

// TestDrainUnranksAndTombstonesEveryAdvertisement holds graceful drain across a
// multi-target Department, so a drain that stopped after the first row fails.
func TestDrainUnranksAndTombstonesEveryAdvertisement(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	options.Capacity = 12
	options.Department = testDepartment(t,
		registration("critic", pooledCapabilities(1)),
		registration("planner", pooledCapabilities(3)),
		registration("reviewer", pooledCapabilities(2)),
	)
	publisher := newPublisher(t, options)

	before := publish(t, publisher)
	for _, advertisement := range before {
		if !advertisement.Ranked || advertisement.Tombstone || !advertisement.Report.Accepting {
			t.Fatalf("%q is not ranked, accepting and untombstoned BEFORE drain, so the assertions after drain prove nothing", advertisement.Report.AgentID)
		}
		if advertisement.Rank == 0 {
			t.Fatalf("%q has rank 0 before drain, so the post-drain rank assertion is satisfied by the pre-drain state", advertisement.Report.AgentID)
		}
	}

	if publisher.Draining() {
		t.Fatal("a fresh publisher reports draining")
	}
	publisher.BeginDrain()
	if !publisher.Draining() {
		t.Fatal("BeginDrain did not begin a drain")
	}

	after := publish(t, publisher)
	if len(after) != len(before) {
		t.Fatalf("drain published %d advertisements, want %d: a tombstone is a record, not a deletion", len(after), len(before))
	}
	indexed := byAgent(t, after)
	for _, agent := range options.Department.AgentIDs() {
		advertisement, published := indexed[agent]
		if !published {
			t.Fatalf("drain dropped %q entirely, leaving whatever was published for it before to expire on its own", agent)
		}
		if advertisement.Ranked {
			t.Errorf("%q is still ranked after drain, so Factory would page it as a candidate", agent)
		}
		if advertisement.Rank != 0 {
			t.Errorf("%q has rank %d after drain, want 0", agent, advertisement.Rank)
		}
		if !advertisement.Tombstone {
			t.Errorf("%q is not tombstoned after drain", agent)
		}
		if advertisement.Report.Accepting {
			t.Errorf("%q still advertises Accepting after drain", agent)
		}
		if advertisement.Report.AvailableCapacity != 0 {
			t.Errorf("%q advertises %d available capacity after drain, want 0", agent, advertisement.Report.AvailableCapacity)
		}
		// A tombstone still expires. Without observed and expiry the record is
		// not a valid sessionwire observation and no due reconciler can reap it.
		if !advertisement.DueAt.Equal(advertisement.Report.ExpiresAt) {
			t.Errorf("%q tombstone DueAt = %v, want its expiry %v", agent, advertisement.DueAt, advertisement.Report.ExpiresAt)
		}
		if err := advertisement.Report.Validate(); err != nil {
			t.Errorf("%q tombstone is not a valid sessionwire record: %v", agent, err)
		}
		if advertisement.StableKey != indexed[agent].StableKey {
			t.Errorf("%q tombstone has a different stable key from its advertisement", agent)
		}
	}
	for i, advertisement := range after {
		if advertisement.StableKey != before[i].StableKey {
			t.Errorf("tombstone %d has stable key %q, want the advertised key %q it must overwrite", i, advertisement.StableKey, before[i].StableKey)
		}
	}

	// Drain stops NEW admission and keeps the existing ledger, which is the
	// difference between draining and being emptied.
	err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "reviewer")
	if err == nil {
		t.Fatal("a draining Host accepted a new admission")
	}
	var refused *service.AdmissionRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("Admit error is %T, want *service.AdmissionRefusedError", err)
	}
	if refused.Code != sessionwire.HostLinkErrorNotAdmitting {
		t.Errorf("refusal code = %q, want %q", refused.Code, sessionwire.HostLinkErrorNotAdmitting)
	}
}

// TestDrainReleasesTheSessionsItStillHolds holds that drain does not lose the
// ledger: a session admitted before drain can still be released during it.
func TestDrainReleasesTheSessionsItStillHolds(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(2)))
	publisher := newPublisher(t, options)
	key := registry.Key{TenantID: testTenant, SessionID: "session-r"}

	if err := publisher.Admit(key, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	publisher.BeginDrain()
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Errorf("ConsumedWeight = %d immediately after BeginDrain, want 2: drain stops admission, it does not evict", got)
	}
	if !publisher.Release(key) {
		t.Error("a session admitted before drain could not be released during it")
	}
	if got := publisher.ConsumedWeight(); got != 0 {
		t.Errorf("ConsumedWeight = %d after releasing during drain, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// What Host publishes, and what it does not
// ---------------------------------------------------------------------------

// TestPublishedAgentsAreExactlyTheDepartmentsRegisteredAgents is step 3 of the
// task, stated as a WHITELIST over the published rows rather than as a search
// for a second catalogue.
//
// It covers BOTH halves of that step at once, because both are the same
// property: no row whose agent the Department does not register (which is what a
// second static catalogue or a copied Rig subagent entry would be), and no
// registered agent missing (which is what a Host publishing something else
// INSTEAD would be). The set equality is what makes it a whitelist.
//
// WHAT IT DOES NOT COVER: this is a claim about the rows Publish returns. It
// says nothing about another package publishing something else, and nothing
// about a caller adding rows after Publish returns.
// TestPublishIsTheOnlyPublishingSurface covers this package at the API level;
// nothing covers either of the other two.
func TestPublishedAgentsAreExactlyTheDepartmentsRegisteredAgents(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Department = testDepartment(t,
		registration("critic", pooledCapabilities(1)),
		registration("planner", pooledCapabilities(2)),
		registration("reviewer", pooledCapabilities(3)),
	)
	publisher := newPublisher(t, options)

	registered := options.Department.AgentIDs()
	// Each phase takes the SUBTEST's *testing.T. t.Fatalf calls FailNow, which
	// must run on the goroutine of the test it belongs to; closing over the
	// parent t and failing inside t.Run is undefined, and it was reachable
	// exactly when Admit failed — the moment a confusing outcome costs most.
	for _, phase := range []struct {
		name string
		act  func(*testing.T)
	}{
		{"initial publish", func(*testing.T) {}},
		{"after admission", func(t *testing.T) {
			t.Helper()
			if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "planner"); err != nil {
				t.Fatalf("Admit: %v", err)
			}
		}},
		{"after drain", func(*testing.T) { publisher.BeginDrain() }},
	} {
		t.Run(phase.name, func(t *testing.T) {
			phase.act(t)
			var published []sessionwire.AgentID
			for _, advertisement := range publish(t, publisher) {
				published = append(published, advertisement.Report.AgentID)
			}
			slices.Sort(published)
			if !slices.Equal(published, registered) {
				t.Errorf("published agents %v, want exactly the Department's registered agents %v: everything outside that set is a second catalogue", published, registered)
			}
		})
	}
	// The control the whitelist needs: an agent the Department DOES register
	// appears, so "the sets are equal" is not satisfied by both being empty.
	if len(registered) != 3 {
		t.Fatalf("the fixture registers %d agents, want 3: a set equality over an empty set is vacuous", len(registered))
	}
}

// TestPublishIsTheOnlyPublishingSurface is the API-level half of the same
// whitelist: this package's production FILES are enumerated, every exported
// top-level DECLARATION in them is enumerated — function, method, type, const
// and var — and exactly one of them yields a record.
//
// It is written as "enumerate what may exist and report everything else" rather
// than as "prove no second catalogue exists", because the second is unprovable
// from inside a test. A new exported declaration fails here until it is listed,
// and one that yields an Advertisement fails the publishing-surface assertion
// whether or not it is listed.
//
// EVERY WIDENING HERE WAS FORCED BY A PROBE, and the two it took are worth
// naming because they are the same mistake at two levels. Scoped to one file
// name, a second publishing surface in a NEW FILE of this package escaped.
// Scoped to *ast.FuncDecl, a second publishing surface declared as a func-typed
// VAR escaped. Neither was hypothetical; both were measured, and both now fail.
// The lesson each time was that the guard named a proxy for its subject rather
// than the subject.
//
// WHAT IT DOES NOT COVER: other PACKAGES; anything a caller does to the slice
// after Publish returns; and a record reached through a LOCALLY RENAMED TYPE.
//
// The third is a floor rather than an oversight, and it is measured: a sink
// parameter typed `*[]record`, where `type record = Advertisement`, passes,
// while the identical sink typed `*[]Advertisement` fails — the difference is
// purely the rename. This guard matches identifiers, and every syntactic
// name-based guard has that floor. Closing it means go/types with a real
// importer, which is a far heavier instrument for a payload that requires an
// alias to be introduced on purpose. Unlike the four escapes above, this is not
// a position the guard never looked at; it is indirection. Documented rather
// than closed, deliberately.
//
// Its two instruments are separately controlled by
// TestTheStructuralGuardsSeeWhatTheyClaimTo, because arms of both are not
// exercised by this package's real declarations.
func TestPublishIsTheOnlyPublishingSurface(t *testing.T) {
	t.Parallel()

	// The enumerated set. A DECLARATION here is permitted to exist; only Publish
	// is permitted to yield a record. Types and constants are listed alongside
	// functions because a probe showed why: while this walked only *ast.FuncDecl
	// a second publishing surface declared as a func-typed package var — `var
	// StaticCatalogue = func(*CapacityPublisher) []Advertisement { ... }`
	// returning a row for an invented agent — was neither enumerated nor tested,
	// and the package stayed green. The guard is over declarations now, so the
	// declaration KIND cannot be the escape hatch.
	permitted := map[string]bool{
		"Advertisement":                      true,
		"AdvertisementNamespace":             true,
		"CapacityOptions":                    true,
		"CapacityPublisher":                  true,
		"AdmissionRefusedError":              true,
		"AdmissionConflictError":             true,
		"AdvertisementError":                 true,
		"InvalidCapacityOptionsError":        true,
		"NewCapacityPublisher":               true,
		"Advertisement.Expired":              true,
		"CapacityPublisher.Publish":          true,
		"CapacityPublisher.Admit":            true,
		"CapacityPublisher.Release":          true,
		"CapacityPublisher.Own":              true,
		"CapacityPublisher.ReleaseOwned":     true,
		"CapacityPublisher.BeginDrain":       true,
		"CapacityPublisher.Draining":         true,
		"CapacityPublisher.ConsumedWeight":   true,
		"AdmissionRefusedError.Error":        true,
		"AdmissionRefusedError.Unwrap":       true,
		"AdmissionConflictError.Error":       true,
		"AdvertisementError.Error":           true,
		"AdvertisementError.Unwrap":          true,
		"InvalidCapacityOptionsError.Error":  true,
		"InvalidCapacityOptionsError.Unwrap": true,

		// v0.4.0. A SENTINEL, NOT A SURFACE: it classifies a directory write a
		// newer incarnation of this HostID has superseded, so the drain can tell
		// "not my row any more" from "the directory is down". It yields no record.
		"ErrTargetGenerationSuperseded": true,

		// events.go, O5.3. THE LIVE TAIL IS NOT A SECOND CATALOGUE and this
		// enumeration is where that is decided rather than assumed: a Tail
		// carries one session's committed journal events onto one HostLink
		// channel and cannot express an Advertisement at all — Publish returns
		// *Tail, and the publishing assertion below is what checks that rather
		// than this list. Listing them is the visible diff the guard exists to
		// force.
		"Publications":                  true,
		"Routes":                        true,
		"TailOptions":                   true,
		"InvalidTailOptionsError":       true,
		"InvalidTailOptionsError.Error": true,
		"Tails":                         true,
		"Tails.Publish":                 true,
		"NewTails":                      true,
		"TailEnd":                       true,
		"TailEndRunning":                true,
		"TailEndStopped":                true,
		"TailEndLost":                   true,
		"TailEndRefused":                true,
		"Tail":                          true,
		"Tail.Key":                      true,
		"Tail.Channel":                  true,
		"Tail.Done":                     true,
		"Tail.Stop":                     true,
		"Tail.End":                      true,
		"Tail.Published":                true,
		"Tail.EphemeralDrops":           true,
		"ErrForeignPublication":         true,

		// v0.10.0, finding W1. The same live tail with each body's private
		// runtime identities rewritten first; it returns *Tail like Publish.
		"Projector":              true,
		"Tails.PublishProjected": true,
	}

	// The FILE SET is enumerated too, and that is not tidiness. A probe measured
	// the gap: with only "capacity.go" parsed, a second exported publishing
	// surface added in a new file of THIS SAME PACKAGE survived every assertion
	// below, while the identical payload in capacity.go was caught. A whitelist
	// that names its own subject is the fix.
	files := packageFiles(t, false)
	if !slices.Equal(files, []string{"capacity.go", "events.go"}) {
		t.Errorf("this package's production files are %v, want exactly [capacity.go events.go]: a second file is a second place to publish from, and everything below reads only the enumerated ones", files)
	}

	var found, publishing []string
	for _, name := range files {
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, declaration := range parsed.Decls {
			for _, exported := range exportedDeclarations(declaration) {
				found = append(found, exported.name)
				if !permitted[exported.name] {
					t.Errorf("%s exports %s, which is not in the enumerated set: Host publishes one advertisement per LaunchTarget and nothing else", name, exported.name)
				}
				if exported.publishes {
					publishing = append(publishing, exported.name)
				}
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("no exported function was found, so this guard reached nothing")
	}
	slices.Sort(found)
	for name := range permitted {
		if !slices.Contains(found, name) {
			t.Errorf("%s is enumerated but does not exist; the enumeration must describe the file, not a plan for it", name)
		}
	}
	if !slices.Equal(publishing, []string{"CapacityPublisher.Publish"}) {
		t.Errorf("functions yielding an Advertisement are %v, want exactly [CapacityPublisher.Publish]", publishing)
	}
}

// packageFiles lists this package's Go source files in sorted order, optionally
// including test files. It is what makes the two structural guards above name
// their own subject rather than a file they hope is the only one.
func packageFiles(t *testing.T, includeTests bool) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if !includeTests && strings.HasSuffix(name, "_test.go") {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		t.Fatal("the package directory holds no Go file, so this guard reached nothing")
	}
	slices.Sort(names)
	return names
}

// exportedDeclaration is one exported top-level declaration and whether it
// yields an Advertisement.
type exportedDeclaration struct {
	name      string
	publishes bool
}

// exportedDeclarations returns every exported top-level declaration a
// declaration node introduces.
//
// It covers FUNCTIONS, METHODS, TYPES, CONSTANTS and VARIABLES, because the
// declaration kind was itself an escape: a func-typed exported var is a
// publishing surface that no walk over *ast.FuncDecl can see. A value
// declaration publishes if its type or its value mentions Advertisement, which
// reaches the results of a function literal assigned to it. A TYPE declaration
// never counts as publishing — the Advertisement struct is not a surface — so
// the check is deliberately not applied to it.
func exportedDeclarations(declaration ast.Decl) []exportedDeclaration {
	switch typed := declaration.(type) {
	case *ast.FuncDecl:
		if !typed.Name.IsExported() {
			return nil
		}
		name := typed.Name.Name
		if typed.Recv != nil && len(typed.Recv.List) == 1 {
			name = receiverTypeName(typed.Recv.List[0].Type) + "." + name
		}
		// typed.Type, not typed.Type.Results. A record reaches a caller through
		// a PARAMETER just as well as through a return — a second catalogue
		// appended to a caller-supplied sink keeps a whitelisted name AND a
		// whitelisted result type, and a probe confirmed it passed the whole
		// package. The receiver is deliberately outside this: FuncType covers
		// parameters and results only, so Advertisement's own methods are not
		// counted as surfaces for having an Advertisement receiver.
		return []exportedDeclaration{{name: name, publishes: mentionsAdvertisement(typed.Type)}}
	case *ast.GenDecl:
		var exported []exportedDeclaration
		for _, specification := range typed.Specs {
			switch spec := specification.(type) {
			case *ast.TypeSpec:
				if spec.Name.IsExported() {
					exported = append(exported, exportedDeclaration{name: spec.Name.Name})
				}
			case *ast.ValueSpec:
				publishes := mentionsAdvertisement(spec.Type)
				for _, value := range spec.Values {
					publishes = publishes || mentionsAdvertisement(value)
				}
				for _, identifier := range spec.Names {
					if identifier.IsExported() {
						exported = append(exported, exportedDeclaration{name: identifier.Name, publishes: publishes})
					}
				}
			}
		}
		return exported
	}
	return nil
}

// receiverTypeName returns the bare type name of a method receiver.
//
// It falls back to the printed expression rather than to a placeholder, because
// a placeholder would collapse every unrecognised receiver onto one name and
// the enumeration compares names. No permitted declaration in this package has
// a receiver that reaches the fallback; a generic one would.
func receiverTypeName(expression ast.Expr) string {
	if star, isPointer := expression.(*ast.StarExpr); isPointer {
		expression = star.X
	}
	if identifier, isIdentifier := expression.(*ast.Ident); isIdentifier {
		return identifier.Name
	}
	return types.ExprString(expression)
}

// mentionsAdvertisement reports whether a syntax node names Advertisement
// anywhere inside it, including in the results of a function literal.
func mentionsAdvertisement(node ast.Node) bool {
	if node == nil || reflect.ValueOf(node).IsNil() {
		return false
	}
	mentioned := false
	ast.Inspect(node, func(inner ast.Node) bool {
		if identifier, isIdentifier := inner.(*ast.Ident); isIdentifier && identifier.Name == "Advertisement" {
			mentioned = true
		}
		return true
	})
	return mentioned
}

// TestEveryExportedStructCarriesExactlyTheEnumeratedFields is the same
// whitelist over the shapes this package exports. A field added to any of them
// fails here until it is enumerated, which is the visible diff a silent
// addition would not be.
//
// EVERY exported struct, not just Advertisement, and that widening was forced
// like the others: an []Advertisement field added to CapacityOptions passed
// while the identical field on Advertisement failed. CapacityOptions is input
// and a static catalogue there would be inert, which is exactly why it is the
// defensible-looking face of a class whose other face — a caller-supplied sink
// PARAMETER — is a live output path. Both are the same mistake, so both are
// closed by the same change of shape rather than one being written up.
func TestEveryExportedStructCarriesExactlyTheEnumeratedFields(t *testing.T) {
	t.Parallel()

	// The file set again, rather than a literal name. Today the other guard's
	// enumeration happens to keep this package to one production file, but a
	// guard that depends on a neighbouring guard's assertion reverts silently
	// when that one is relaxed, and the diff would be at the other site.
	files := packageFiles(t, false)
	if !slices.Equal(files, []string{"capacity.go", "events.go"}) {
		t.Fatalf("this package's production files are %v, want exactly [capacity.go events.go]", files)
	}
	fileSet := token.NewFileSet()
	var structs []string
	parsedFiles := make([]*ast.File, 0, len(files))
	for _, name := range files {
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		parsedFiles = append(parsedFiles, parsed)
		structs = append(structs, exportedStructNames(t, parsed)...)
	}
	slices.Sort(structs)
	want := map[string][]string{
		"Advertisement":               {"DueAt", "Namespace", "Rank", "Ranked", "RankingScope", "Report", "StableKey", "Tombstone"},
		"CapacityOptions":             {"Host", "HostGeneration"},
		"CapacityPublisher":           nil,
		"AdmissionRefusedError":       {"AgentID", "Cause", "Code", "Reason"},
		"AdmissionConflictError":      {"Admitted", "Key", "Requested"},
		"AdvertisementError":          {"AgentID", "Cause", "Reason"},
		"InvalidCapacityOptionsError": {"Cause", "Field", "Reason"},

		// events.go, O5.3. Tails and Tail carry NO exported field on purpose:
		// a Tail is a handle, and an exported field on one would be a second,
		// unsynchronized way to read state its own accessors take a lock for.
		"TailOptions":             {"Publications", "Routes"},
		"InvalidTailOptionsError": {"Field", "Reason"},
		"Tails":                   nil,
		"Tail":                    nil,
	}

	if !slices.Equal(structs, slices.Sorted(maps.Keys(want))) {
		t.Errorf("this package's exported structs are %v, want exactly %v", structs, slices.Sorted(maps.Keys(want)))
	}
	// fieldsOf searches EVERY parsed file for the named struct, because the
	// struct set is now drawn from more than one and looking in only the file a
	// loop happens to hold would report an empty field list for every type
	// declared in the other one — which reads exactly like "exports nothing".
	fieldsOf := func(name string) []string {
		for _, parsed := range parsedFiles {
			if slices.Contains(exportedStructNames(t, parsed), name) {
				return structFieldNames(t, parsed, name)
			}
		}
		t.Fatalf("%s is enumerated but is declared in none of %v", name, files)
		return nil
	}
	for _, name := range structs {
		enumerated, listed := want[name]
		if !listed {
			continue
		}
		fields := fieldsOf(name)
		if enumerated == nil {
			// A nil enumeration means "exports NOTHING", which is stronger than
			// a list and is why these entries are not spelled out: their fields
			// are private state, and enumerating private state would make the
			// whitelist track every refactor of it. CapacityPublisher, Tails and
			// Tail are all in this class.
			for _, field := range fields {
				if field != "" && field[0] >= 'A' && field[0] <= 'Z' {
					t.Errorf("%s exports field %s; its state is not part of what this package publishes", name, field)
				}
			}
			continue
		}
		if !slices.Equal(fields, enumerated) {
			t.Errorf("%s fields = %v, want exactly %v", name, fields, enumerated)
		}
	}
}

// exportedStructNames returns every exported struct type declared in a file, in
// sorted order. It is what lets the field whitelist name its subject as "the
// shapes this package exports" rather than one type somebody remembered.
func exportedStructNames(t *testing.T, file *ast.File) []string {
	t.Helper()
	var names []string
	for _, declaration := range file.Decls {
		general, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, isType := specification.(*ast.TypeSpec)
			if !isType || !typeSpec.Name.IsExported() {
				continue
			}
			if _, isStruct := typeSpec.Type.(*ast.StructType); isStruct {
				names = append(names, typeSpec.Name.Name)
			}
		}
	}
	if len(names) == 0 {
		t.Fatal("no exported struct type was found, so this guard reached nothing")
	}
	slices.Sort(names)
	return names
}

// structFieldNames returns the sorted field names of a named struct type.
func structFieldNames(t *testing.T, file *ast.File, name string) []string {
	t.Helper()
	var names []string
	found := false
	for _, declaration := range file.Decls {
		general, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, isType := specification.(*ast.TypeSpec)
			if !isType || typeSpec.Name.Name != name {
				continue
			}
			structType, isStruct := typeSpec.Type.(*ast.StructType)
			if !isStruct {
				t.Fatalf("%s is not a struct type", name)
			}
			found = true
			for _, field := range structType.Fields.List {
				// An EMBEDDED field carries no name, so collecting only
				// field.Names skips it — and its own fields are published
				// through the outer struct all the same. A probe embedded a
				// struct holding a SubagentCatalogue and this guard stayed
				// green, which is the field-whitelist bypass step 3 exists to
				// prevent, landing on a literal subagent catalogue. Embedding
				// is recorded under a name that cannot match the enumeration,
				// so it fails rather than disappears.
				if len(field.Names) == 0 {
					names = append(names, "embedded "+types.ExprString(field.Type))
					continue
				}
				for _, fieldName := range field.Names {
					names = append(names, fieldName.Name)
				}
			}
		}
	}
	if !found {
		t.Fatalf("no struct type named %s was found, so this guard reached nothing", name)
	}
	slices.Sort(names)
	return names
}

// TestTheStructuralGuardsSeeWhatTheyClaimTo is a POSITIVE CONTROL for the two
// helpers the whitelists above are built from.
//
// It exists because both helpers have arms that the real subject does not
// exercise, so removing them changes nothing and the mutation survives:
// Advertisement embeds nothing, so structFieldNames' embedded arm never runs,
// and no exported var of this package yields an Advertisement, so
// exportedDeclarations' value-publishing arm never runs. A guard whose
// detecting code is only reached by the payload it is meant to detect is a
// guard nobody has tested — measured, not assumed: a probe deleting the
// embedded arm passed the whole package.
//
// The subject here is a source snippet rather than this package, deliberately.
// The claim being controlled is "the helper can SEE these shapes", and that is
// answered by handing it the shapes; adding them to Advertisement itself would
// mean publishing a field to test a test.
//
// WHAT IT DOES NOT COVER: it says nothing about what this package declares. The
// two whitelists above own that, and this owns only their instruments.
func TestTheStructuralGuardsSeeWhatTheyClaimTo(t *testing.T) {
	t.Parallel()

	const source = `package sample

// StaticCatalogue is a publishing surface that is not a FuncDecl.
var StaticCatalogue = func() []Advertisement { return nil }

// SinkCatalogue delivers records through a PARAMETER, keeping a plain result.
func SinkCatalogue(sink *[]Advertisement) bool { return false }

// PlainFunction names no record in either position.
func PlainFunction(n int) bool { return false }

// PlainConstant mentions no record.
const PlainConstant = 3

// Embedding carries a field with no name of its own.
type Embedding struct {
	Named string
	embeddedPart
}
`
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "sample.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing the sample: %v", err)
	}

	// Each row is a SHAPE the enumeration must classify. The negative rows make
	// this instrument test SELF-CONTAINED — it decides both answers without
	// depending on another test to catch a check that reports everything.
	//
	// They are not what stops that mutant, and an earlier version of this
	// comment claimed they were. Measured: an always-true publishing check dies
	// three times over — on the PlainConstant row here, and twice in
	// TestPublishIsTheOnlyPublishingSurface, which reports every declaration in
	// the package as publishing. No mutant kills on PlainFunction alone. The
	// row stays because relying on redundancy elsewhere is exactly what this
	// file refuses to do everywhere else.
	wantPublishing := map[string]bool{
		"StaticCatalogue": true,  // a record through a func-typed var
		"SinkCatalogue":   true,  // a record through a caller-supplied PARAMETER
		"PlainFunction":   false, // no record in either position
		"PlainConstant":   false, // no record at all
	}
	seen := map[string]bool{}
	for _, declaration := range parsed.Decls {
		for _, exported := range exportedDeclarations(declaration) {
			want, interesting := wantPublishing[exported.name]
			if !interesting {
				continue
			}
			seen[exported.name] = true
			if exported.publishes != want {
				t.Errorf("exportedDeclarations classified %s as publishes=%v, want %v", exported.name, exported.publishes, want)
			}
		}
	}
	for name := range wantPublishing {
		if !seen[name] {
			t.Errorf("exportedDeclarations never reported %s, so its row proves nothing", name)
		}
	}

	if got := exportedStructNames(t, parsed); !slices.Equal(got, []string{"Embedding"}) {
		t.Errorf("exportedStructNames = %v, want [Embedding]: the field whitelist enumerates the structs it finds, so a struct it cannot find is a shape nobody enumerated", got)
	}

	fields := structFieldNames(t, parsed, "Embedding")
	if !slices.Equal(fields, []string{"Named", "embedded embeddedPart"}) {
		t.Errorf("structFieldNames over an embedding struct = %v, want [Named \"embedded embeddedPart\"]: an embedded field carries no name, and its own fields are published through the outer struct all the same", fields)
	}
}

// driftingTarget is a LaunchTarget whose answers change after registration.
//
// It is not a contrived fixture. department's own type documentation says a
// Department "does not and cannot copy the TARGETS, which are interface values
// a caller may still hold and whose implementations may carry state", so this
// is the behaviour the accepted design permits, and department.New validates
// only the answers it was given at registration.
type driftingTarget struct {
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
}

func (t *driftingTarget) CompatibilityID() department.CompatibilityID { return t.compatibility }
func (t *driftingTarget) Capabilities() department.Capabilities       { return t.capabilities }

func (*driftingTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("drifting target launches nothing")
}

func (*driftingTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("drifting target launches nothing")
}

// TestATargetThatChangesItsAnswersCannotBreakThePeriodicPath holds the snapshot.
//
// The fault it pins was a PANIC, not a wrong value: a target reporting admission
// weight 1 at registration and 0 afterwards divided by zero inside Publish, on
// the heartbeat, in a file where every other fault is a typed refusal. The
// compatibility id is the same shape with a quieter consequence — it is an input
// to StableKey, so a drifting one would start writing to a different record and
// orphan the live advertisement until it expired, reporting nothing.
//
// Both directions are here: a target already broken AT construction is refused,
// and a target that breaks AFTERWARDS is survived on the snapshotted values.
func TestATargetThatChangesItsAnswersCannotBreakThePeriodicPath(t *testing.T) {
	t.Parallel()

	newDrifting := func(t *testing.T) (*driftingTarget, hostconfig.Options) {
		t.Helper()
		target := &driftingTarget{compatibility: "rig-drift-2026-09", capabilities: pooledCapabilities(2)}
		options := pooledOptions(t, newFakeClock())
		options.Capacity = 8
		options.Department = testDepartment(t, department.Registration{AgentID: "reviewer", Target: target})
		return target, options
	}

	// TWO weight rows, and WHICH ONE IS LOAD-BEARING DEPENDS ON THE MUTANT.
	// Both shapes were measured, because an earlier version of this comment
	// asserted one of them as though it were the only one.
	//
	// A NARROW mutant re-reads only the DIVISOR and leaves `accepting` on the
	// snapshot. The zero row then reaches the division and PANICS — a crash,
	// which this program does not count as a kill — and the 2->4 row is the
	// only assertion kill.
	//
	// A WHOLE-BINDING mutant re-reads `capabilities` itself. Now the zero row
	// never reaches the divisor at all: department.Capabilities{} also zeroes
	// SupportsPooled and SupportsDedicated, so placementSupported is false,
	// `accepting` is false, and the division is skipped. It kills by assertion
	// instead, reporting available capacity 4 -> 0.
	//
	// So the 2->4 row is the one that kills BOTH shapes by assertion, and the
	// zero row is the only one that reaches the divide-by-zero that was
	// actually reported. Neither is redundant, and neither covers the other.
	t.Run("a different weight arriving after construction", func(t *testing.T) {
		t.Parallel()
		target, options := newDrifting(t)
		publisher := newPublisher(t, options)
		before := publish(t, publisher)[0]

		// 8/2 = 4 before, 8/4 = 2 if re-read. Neither is zero, so nothing
		// crashes and the difference is a value a test can compare.
		target.capabilities = pooledCapabilities(4)

		after := publish(t, publisher)[0]
		if after.Report.AvailableCapacity != 4 || before.Report.AvailableCapacity != 4 {
			t.Errorf("AvailableCapacity was %d and is %d; both must be 4, the snapshotted weight-2 answer at capacity 8", before.Report.AvailableCapacity, after.Report.AvailableCapacity)
		}
		if after.Rank != before.Rank {
			t.Errorf("Rank moved from %d to %d because a target changed its mind", before.Rank, after.Rank)
		}
		if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "reviewer"); err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if got := publisher.ConsumedWeight(); got != 2 {
			t.Errorf("ConsumedWeight = %d, want the snapshotted weight 2: admission must charge what the Host was validated against", got)
		}
	})

	t.Run("a zero weight arriving after construction", func(t *testing.T) {
		t.Parallel()
		target, options := newDrifting(t)
		publisher := newPublisher(t, options)
		before := publish(t, publisher)[0]

		target.capabilities = department.Capabilities{}

		// Would panic with integer divide by zero if the weight were re-read.
		after := publish(t, publisher)[0]
		if after.Report.AvailableCapacity != before.Report.AvailableCapacity {
			t.Errorf("AvailableCapacity moved from %d to %d because a target changed its mind; the snapshot is what the Host was validated against", before.Report.AvailableCapacity, after.Report.AvailableCapacity)
		}
		if got := before.Report.AvailableCapacity; got != 4 {
			t.Errorf("AvailableCapacity = %d at capacity 8 and weight 2, want 4: the fixture must exercise a weight above one or a divisor fault is invisible", got)
		}
		// Admission charges the snapshotted weight too, and a zero weight there
		// would admit without bound — which is the rule Capabilities.Validate
		// exists to enforce and could no longer see.
		if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "reviewer"); err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if got := publisher.ConsumedWeight(); got != 2 {
			t.Errorf("ConsumedWeight = %d, want the snapshotted weight 2", got)
		}
	})

	t.Run("a compatibility id arriving after construction", func(t *testing.T) {
		t.Parallel()
		target, options := newDrifting(t)
		publisher := newPublisher(t, options)
		before := publish(t, publisher)[0]

		target.compatibility = "rig-drift-2026-12"

		after := publish(t, publisher)[0]
		if after.StableKey != before.StableKey {
			t.Errorf("the heartbeat's stable key moved from %q to %q, so it would insert a new record and leave the live advertisement to expire unrefreshed", before.StableKey, after.StableKey)
		}
		if after.RankingScope != before.RankingScope {
			t.Errorf("the heartbeat's ranking scope moved from %q to %q", before.RankingScope, after.RankingScope)
		}
		if after.Report.RuntimeCompatibilityID != before.Report.RuntimeCompatibilityID {
			t.Errorf("RuntimeCompatibilityID moved from %q to %q without the key moving, so the record would advertise a build its own key does not describe", before.Report.RuntimeCompatibilityID, after.Report.RuntimeCompatibilityID)
		}
	})

	t.Run("already broken at construction", func(t *testing.T) {
		t.Parallel()
		// The CAUSE is asserted, not just the refusal, and that is what makes
		// these two rows different rules rather than one rule twice.
		//
		// Measured: deleting the constructor's compatibility re-check entirely
		// left the whole package green. Every input CompatibilityID.Validate
		// rejects, the trial Publish rejects too — department documents
		// MaxCompatibilityIDBytes as DERIVED from the wire bound Core enforces
		// on the same field — so the row passed through Core's refusal and
		// would have kept passing with the check gone. It was a row named after
		// a check it did not reach. Pinning the cause type fixes that, and what
		// the check then earns its place for is message quality: it names the
		// AGENT and its target where Core's path names only a wire field.
		//
		// The asymmetry is real and worth keeping straight: the CAPABILITIES
		// re-check is load-bearing on its own, because Core has no opinion on
		// AdmissionWeight and nothing downstream would reject a zero.
		for _, row := range []struct {
			name      string
			spoil     func(*driftingTarget)
			wantCause func(error) bool
			causeName string
		}{
			{
				name:      "zero admission weight",
				spoil:     func(target *driftingTarget) { target.capabilities = department.Capabilities{} },
				causeName: "*department.InvalidCapabilitiesError",
				wantCause: func(err error) bool {
					var invalid *department.InvalidCapabilitiesError
					return errors.As(err, &invalid)
				},
			},
			{
				name:      "empty compatibility id",
				spoil:     func(target *driftingTarget) { target.compatibility = "" },
				causeName: "*department.InvalidCompatibilityIDError",
				wantCause: func(err error) bool {
					var invalid *department.InvalidCompatibilityIDError
					return errors.As(err, &invalid)
				},
			},
		} {
			t.Run(row.name, func(t *testing.T) {
				t.Parallel()
				target, options := newDrifting(t)
				// Registration succeeded on the healthy answers; the target
				// changes them before the publisher snapshots. This is the case
				// department.New has already been past and cannot revisit.
				row.spoil(target)
				_, err := service.NewCapacityPublisher(service.CapacityOptions{
					Host:           newHost(t, options),
					HostGeneration: testHostGeneration,
				})
				var invalid *service.InvalidCapacityOptionsError
				if !errors.As(err, &invalid) {
					t.Fatalf("NewCapacityPublisher = %v (%T), want *service.InvalidCapacityOptionsError: a snapshot nobody validated is the panic in a different place", err, err)
				}
				if invalid.Field != "Host" {
					t.Errorf("refusal field = %q, want %q", invalid.Field, "Host")
				}
				if !row.wantCause(err) {
					t.Errorf("refusal does not unwrap to %s: %v — so this row is passing through some LATER refusal and would stay green with the check it is named after deleted", row.causeName, err)
				}
			})
		}
		// The control one position over: the same target, unbroken, is accepted.
		_, options := newDrifting(t)
		if _, err := service.NewCapacityPublisher(service.CapacityOptions{
			Host:           newHost(t, options),
			HostGeneration: testHostGeneration,
		}); err != nil {
			t.Errorf("the healthy target was refused too, so the refusals above prove nothing: %v", err)
		}
	})
}

// TestTheSnapshotCopiesEverythingItsSourcesCanHold holds the two properties of
// OTHER packages' types that the snapshot silently depends on.
//
// It is a test and not a paragraph because neither package owes this one
// stability, and a comment cannot fail when one of them changes.
//
// FIRST: department.Capabilities must hold no reference field. The snapshot
// copies the struct, so a slice, map or pointer inside it would be copied as a
// HEADER pointing at memory the LaunchTarget still owns — the target could keep
// mutating it and the "snapshot" would follow, making the whole fix cosmetic
// while every drift test kept passing. That failure is invisible from inside
// this package, which is exactly why it is pinned here.
//
// SECOND: host.Host must expose no field. StableKey and RankingScope are
// derived once from Placement() and ID() while Report.Placement and
// Report.HostID are read live on every heartbeat — a snapshot/live pair for one
// value, inert only while the Host cannot change underneath it. An exported
// field would make it mutable by any holder and turn the pair into the same
// class of bug the LaunchTarget snapshot exists to prevent.
//
// WHAT IT DOES NOT COVER: it checks the SHAPE of both types, not their
// behaviour. A host.Host accessor that computed a different answer each call
// would satisfy this and break the pair; nothing here would see it.
func TestTheSnapshotCopiesEverythingItsSourcesCanHold(t *testing.T) {
	t.Parallel()

	// The CONTROL first. department.Capabilities holds no reference field today,
	// so the classifier's rejecting arm is never reached by the real subject —
	// and a probe widening that arm to accept slices, maps and pointers passed
	// the whole package. A guard whose detecting code only the payload reaches
	// is a guard nobody has tested, which is the third time that shape has
	// turned up in this file.
	type referenceBearing struct {
		Fine    bool
		Slice   []string
		Map     map[string]int
		Pointer *int
	}
	if got := referenceFields(reflect.TypeOf(referenceBearing{})); !slices.Equal(got, []string{"Map", "Pointer", "Slice"}) {
		t.Errorf("referenceFields over a struct carrying a slice, a map and a pointer = %v, want all three named", got)
	}

	if got := referenceFields(reflect.TypeOf(department.Capabilities{})); len(got) != 0 {
		t.Errorf("department.Capabilities carries reference field(s) %v; the publisher SNAPSHOTS this struct by copying it, so a reference field is shared with the LaunchTarget that owns it and the snapshot would follow the target's later changes — the fix would be cosmetic while every drift test kept passing",
			got)
	}

	// Controlled the same way and for the same reason: host.Host exports
	// nothing today, so the reporting arm is unreached by the real subject and
	// disabling it passed the package. Measured, like the two before it.
	type exportBearing struct {
		Exported   int
		AlsoPublic string
		unexported bool
	}
	// The unexported field is assigned so it is a USE — staticcheck reports an
	// untouched one — and it carries the negative half of the control: the
	// classifier must EXCLUDE it, not merely list what it finds. Without the
	// arity check, an implementation returning every field would pass the
	// positive half.
	bearing := reflect.TypeOf(exportBearing{unexported: true})
	if bearing.NumField() != 3 {
		t.Fatalf("the control struct has %d fields, want 3: two exported and one not", bearing.NumField())
	}
	if got := exportedFieldNames(bearing); !slices.Equal(got, []string{"AlsoPublic", "Exported"}) {
		t.Errorf("exportedFieldNames over a struct with two exported fields and one unexported = %v, want exactly the two exported", got)
	}

	built := reflect.TypeOf(hostconfig.Host{})
	if built.NumField() == 0 {
		t.Fatal("hostconfig.Host has no fields, so this guard reached nothing")
	}
	if got := exportedFieldNames(built); len(got) != 0 {
		t.Errorf("hostconfig.Host exports field(s) %v; the advertisement's keys are derived from Placement() and ID() ONCE while the report reads them every heartbeat, and that snapshot/live pair is only safe while a holder cannot change the Host underneath it",
			got)
	}
}

// referenceFields names every field of a struct whose kind makes a copy of the
// struct share memory with the original, in sorted order.
func referenceFields(structure reflect.Type) []string {
	var shared []string
	for i := 0; i < structure.NumField(); i++ {
		field := structure.Field(i)
		switch field.Type.Kind() {
		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
		default:
			shared = append(shared, field.Name)
		}
	}
	slices.Sort(shared)
	return shared
}

// exportedFieldNames names every exported field of a struct, in sorted order.
func exportedFieldNames(structure reflect.Type) []string {
	var exported []string
	for i := 0; i < structure.NumField(); i++ {
		if field := structure.Field(i); field.IsExported() {
			exported = append(exported, field.Name)
		}
	}
	slices.Sort(exported)
	return exported
}

// TestExportedErrorsDoNotPanicWithoutACause holds the one thing every exported
// error type owes a caller that did not build it the way this file does.
//
// Error() is what a log line calls, so a nil dereference there crashes at the
// moment something has already gone wrong — the worst place to crash. Every
// exported type is a row rather than the one that was found wanting, because
// the guard is only worth having if it is over the shape and not the instance.
func TestExportedErrorsDoNotPanicWithoutACause(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name string
		err  error
	}{
		{"AdvertisementError", &service.AdvertisementError{AgentID: "reviewer", Reason: "unpublishable"}},
		{"AdmissionRefusedError", &service.AdmissionRefusedError{AgentID: "reviewer", Reason: "refused"}},
		{"AdmissionConflictError", &service.AdmissionConflictError{Admitted: "a", Requested: "b"}},
		{"InvalidCapacityOptionsError", &service.InvalidCapacityOptionsError{Field: "Host", Reason: "unset"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			// RECOVERED, so the failure is an assertion naming the type rather
			// than a panic that fails the whole package and says only which
			// goroutine died. The defect being guarded IS a panic, so without
			// this the only possible kill is a crash.
			message, panicked := errorText(row.err)
			if panicked != nil {
				t.Fatalf("Error() panicked with %v; a nil cause must not crash the log line that reports the failure", panicked)
			}
			if message == "" {
				t.Error("Error() returned nothing, so a log line would say nothing")
			}
			if !strings.Contains(message, "host/service") {
				t.Errorf("Error() = %q, want it to name the package it came from", message)
			}
			if unwrapped := errors.Unwrap(row.err); unwrapped != nil {
				t.Errorf("errors.Unwrap = %v on an error built with no cause, want nil", unwrapped)
			}
		})
	}
}

// errorText calls Error() and reports any panic instead of propagating it.
func errorText(err error) (message string, panicked any) {
	defer func() { panicked = recover() }()
	return err.Error(), nil
}

// TestNoInjectedCallHappensUnderTheLock holds the reentrancy hazard
// structurally, because the only behavioural symptom is a DEADLOCK.
//
// sync.Mutex is not reentrant. Every collaborator this package reaches through
// the Host — Clock, Department, LaunchTarget — is caller-supplied code, and a
// Clock whose Now called back into Draining, ConsumedWeight, Admit or Release
// would hang the Host rather than misbehave visibly. A test that provoked it
// would be killed by a hang, which this program does not count as a kill and
// which in CI reports a timeout rather than a cause; a structural guard fails
// with a file and a line.
//
// The rule is narrow and exact: no method call reached through p.host may
// appear lexically inside a region where p.mu is held. A deferred unlock makes
// the region run to the end of the function, which is the shape every method
// here uses.
//
// WHAT IT DOES NOT COVER: a call reached through a HELPER invoked under the
// lock, and any deadlock not involving p.mu. It is a guard over one lexical
// pattern and does not prove the package deadlock-free.
func TestNoInjectedCallHappensUnderTheLock(t *testing.T) {
	t.Parallel()

	files := packageFiles(t, false)
	regions := 0
	for _, name := range files {
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, declaration := range parsed.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Body == nil {
				continue
			}
			from, to, held := lockedRegion(function)
			if !held {
				continue
			}
			regions++
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall || call.Pos() < from || call.Pos() > to {
					return true
				}
				if reachesHost(call.Fun) {
					t.Errorf("%s: %s calls through p.host at %s, inside the region where p.mu is held; sync.Mutex is not reentrant, so a collaborator that called back into this publisher would deadlock the Host",
						name, function.Name.Name, fileSet.Position(call.Pos()))
				}
				return true
			})
		}
	}
	if regions == 0 {
		t.Fatal("no locked region was found, so this guard reached nothing")
	}
	// Every mutating and reading entry point takes the lock. Fewer regions than
	// that would mean the guard is inspecting a subset and passing on the rest.
	if regions < 6 {
		t.Errorf("found %d locked regions, want at least 6 — Publish, Admit, Release, BeginDrain, Draining and ConsumedWeight all take p.mu", regions)
	}
}

// lockedRegion returns the byte range of a function in which p.mu is held.
//
// A deferred unlock runs the region to the end of the function; an explicit one
// ends it at that call. Only the first lock in a function is considered, which
// is all this package has and is stated rather than assumed.
func lockedRegion(function *ast.FuncDecl) (from, to token.Pos, held bool) {
	deferred := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.DeferStmt:
			if isMutexCall(typed.Call, "Unlock") {
				deferred = true
			}
			return false
		case *ast.CallExpr:
			if isMutexCall(typed, "Lock") && !held {
				from, held = typed.Pos(), true
			}
			if isMutexCall(typed, "Unlock") && held && to == token.NoPos {
				to = typed.Pos()
			}
		}
		return true
	})
	if !held {
		return token.NoPos, token.NoPos, false
	}
	if deferred || to == token.NoPos {
		to = function.Body.End()
	}
	return from, to, true
}

// isMutexCall reports whether a call is p.mu.<name>().
func isMutexCall(call *ast.CallExpr, name string) bool {
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector || selector.Sel.Name != name {
		return false
	}
	inner, isInner := selector.X.(*ast.SelectorExpr)
	return isInner && inner.Sel.Name == "mu"
}

// reachesHost reports whether an expression selects through p.host.
func reachesHost(expression ast.Expr) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		selector, isSelector := node.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "host" {
			return true
		}
		if identifier, isIdentifier := selector.X.(*ast.Ident); isIdentifier && identifier.Name == "p" {
			found = true
		}
		return true
	})
	return found
}

// ---------------------------------------------------------------------------
// The clock, and the absence of waiting
// ---------------------------------------------------------------------------

// TestPublishConsultsOnlyTheClocksNow holds that every instant in an
// advertisement comes from the injected Clock, and that nothing in this package
// arms a timer.
//
// This is what makes the fake clock sufficient. A derivation calling time.Now
// directly would produce an ObservedAt the fake never returned; one arming a
// timer would introduce a real-time wait a fake cannot control.
func TestPublishConsultsOnlyTheClocksNow(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	options := pooledOptions(t, clock)
	publisher := newPublisher(t, options)

	nowsBefore, timersBefore := clock.Counts()
	if timersBefore != 0 {
		t.Errorf("NewCapacityPublisher armed %d timers", timersBefore)
	}
	advertisement := publish(t, publisher)[0]
	if err := publisher.Admit(registry.Key{TenantID: testTenant, SessionID: "s"}, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	publisher.Release(registry.Key{TenantID: testTenant, SessionID: "s"})
	publisher.BeginDrain()
	_ = publish(t, publisher)

	nowsAfter, timersAfter := clock.Counts()
	if timersAfter != 0 {
		t.Errorf("the publisher armed %d timers; nothing here waits, so nothing here needs one", timersAfter)
	}
	if nowsAfter <= nowsBefore {
		t.Error("Publish never called Clock.Now, so its observation instant came from somewhere this test cannot control")
	}
	if !advertisement.Report.ObservedAt.Equal(clock.Now()) {
		t.Errorf("ObservedAt = %v but the fake clock reads %v: the observation did not come from the injected Clock", advertisement.Report.ObservedAt, clock.Now())
	}
}

// waitPolicy is how much of the no-waiting ban one file is held to.
type waitPolicy uint8

const (
	// banEverything forbids real-time calls, select statements and channel
	// receives alike. It is the advertisement half's policy: a fake clock
	// removes the wait by construction, so there is nothing left to receive.
	banEverything waitPolicy = iota

	// banRealTimeOnly forbids the real-time calls and permits select and
	// receive. It is the LIVE TAIL's policy, and the narrowing is a statement
	// about what that code IS rather than a concession: relaying a live stream
	// is receiving from a channel, so banning the receive would ban the
	// feature. What must still never appear there is a TIMER — a tail that
	// slept, ticked or timed out would be inventing a schedule of its own on
	// top of a stream whose pace is the runtime's.
	banRealTimeOnly

	// noBan holds a file to nothing. Only the live tail's TEST is in this
	// class: every deadline in it is a bounded assertion that replaces a
	// construct which would otherwise HANG, and a hang is not an assertion
	// failure — it reports nothing and takes the package's other tests with it.
	noBan
)

// TestFilesSleepOrPollExactlyWhereTheyAreAllowedTo holds the structural half of
// the same property, per file, over every file of this package.
//
// It bans, by parsed structure and not by text search, the constructs through
// which a wait can enter: time.Sleep, time.After, time.Tick, time.NewTicker,
// time.AfterFunc, runtime.Gosched, any select statement, and any channel
// receive.
//
// IT IS PER-FILE BECAUSE THE PACKAGE HAS TWO HALVES AND THEY ARE OPPOSITE. The
// advertisement half derives a record from an injected clock and never waits;
// the live tail (events.go, O5.3) relays a stream, which is a channel receive by
// definition. A single blanket ban would have to be either wrong for one half or
// absent for both, and the version of this guard that predated the tail was the
// first — it failed on events.go the moment the file existed.
//
// THE EXCLUSION IS ITSELF TESTED, which is the rule this repository has twice
// been bitten for skipping: policies is a total map over the file set, asserted
// equal to it, so a NEW file lands in no class and fails here rather than
// inheriting the loosest one. The narrowing is per file and per construct, so
// events.go is still held to the timer ban that matters for it.
//
// WHAT IT DOES NOT COVER: a wait reached through a helper in another PACKAGE,
// and a busy loop that spins on a value without receiving.
func TestFilesSleepOrPollExactlyWhereTheyAreAllowedTo(t *testing.T) {
	t.Parallel()

	banned := map[string]bool{
		"time.Sleep": true, "time.After": true, "time.Tick": true,
		"time.NewTicker": true, "time.AfterFunc": true, "runtime.Gosched": true,
	}
	policies := map[string]waitPolicy{
		"capacity.go":         banEverything,
		"capacity_test.go":    banEverything,
		"events.go":           banRealTimeOnly,
		"events_test.go":      noBan,
		"live_events_test.go": noBan,
	}
	files := packageFiles(t, true)
	if !slices.Equal(files, slices.Sorted(maps.Keys(policies))) {
		t.Fatalf("this package's files are %v, want exactly %v: every file is held to a named policy, and a file in none of them would be held to nothing by accident", files, slices.Sorted(maps.Keys(policies)))
	}
	inspected := 0
	strictFiles := 0
	for _, name := range files {
		policy := policies[name]
		if policy == banEverything {
			strictFiles++
		}
		if policy == noBan {
			continue
		}
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				inspected++
				selector, isSelector := typed.Fun.(*ast.SelectorExpr)
				if !isSelector {
					return true
				}
				pkg, isIdentifier := selector.X.(*ast.Ident)
				if !isIdentifier {
					return true
				}
				if qualified := pkg.Name + "." + selector.Sel.Name; banned[qualified] {
					t.Errorf("%s calls %s at %s: nothing in this file waits on real time", name, qualified, fileSet.Position(typed.Pos()))
				}
			case *ast.SelectStmt:
				if policy == banEverything {
					t.Errorf("%s has a select statement at %s: the one coin-flip test this program has shipped was a select with two ready cases", name, fileSet.Position(typed.Pos()))
				}
			case *ast.UnaryExpr:
				if typed.Op == token.ARROW && policy == banEverything {
					t.Errorf("%s receives from a channel at %s: a fake clock removes the wait, so there is nothing to receive", name, fileSet.Position(typed.Pos()))
				}
			}
			return true
		})
	}
	if inspected == 0 {
		t.Fatal("no call expression was inspected, so this guard reached nothing")
	}
	// The floor for the loosest class: at least one file is still held to the
	// whole ban. Without it, moving every file to noBan would leave this test
	// green while asserting nothing at all.
	if strictFiles == 0 {
		t.Fatal("no file is held to the full ban, so this guard permits everything it was written to forbid")
	}
}

// ---------------------------------------------------------------------------
// Refusals at construction
// ---------------------------------------------------------------------------

// TestNewCapacityPublisherRefusesWhatItCannotPublish holds the construction
// rules, each with an accepted control one position over.
func TestNewCapacityPublisherRefusesWhatItCannotPublish(t *testing.T) {
	t.Parallel()

	t.Run("no host", func(t *testing.T) {
		t.Parallel()
		_, err := service.NewCapacityPublisher(service.CapacityOptions{HostGeneration: testHostGeneration})
		var invalid *service.InvalidCapacityOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("NewCapacityPublisher without a Host = %v (%T), want *service.InvalidCapacityOptionsError", err, err)
		}
		if invalid.Field != "Host" {
			t.Errorf("refusal field = %q, want %q", invalid.Field, "Host")
		}
	})

	t.Run("zero host generation", func(t *testing.T) {
		t.Parallel()
		built := newHost(t, pooledOptions(t, newFakeClock()))
		// Zero is the value Core refuses on the capacity report, so a publisher
		// built with it could never publish anything. The boundary is asserted
		// in both directions: one is accepted.
		_, err := service.NewCapacityPublisher(service.CapacityOptions{Host: built})
		var invalid *service.InvalidCapacityOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("NewCapacityPublisher with generation 0 = %v (%T), want *service.InvalidCapacityOptionsError", err, err)
		}
		var validation *sessionwire.RequestValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("the refusal does not unwrap to Core's *RequestValidationError, so a caller cannot tell which wire field Core objected to")
		}
		if validation.Field != "host_generation" {
			t.Errorf("Core objected to field %q, want %q", validation.Field, "host_generation")
		}
		// The OPTION named, not just Core's wire field. This was hard-coded to
		// HostGeneration for both causes the constructor documents, so the
		// Clock refusal below sent an operator to a knob that was already
		// correct. Two rows, because one cannot tell a derived answer from a
		// constant that happens to match.
		if invalid.Field != "HostGeneration" {
			t.Errorf("refusal field = %q, want %q", invalid.Field, "HostGeneration")
		}
		if _, err := service.NewCapacityPublisher(service.CapacityOptions{Host: built, HostGeneration: 1}); err != nil {
			t.Errorf("generation 1 was refused too, so the assertion above proves nothing: %v", err)
		}
	})

	t.Run("an observation Core would reject", func(t *testing.T) {
		t.Parallel()
		// A Clock stuck at the zero time. Core's validateHostLinkTimes reports
		// observed_at as missing, so this Host would publish records no reader
		// could accept; the derivation must refuse rather than emit them.
		options := pooledOptions(t, frozenClock{})
		_, err := service.NewCapacityPublisher(service.CapacityOptions{
			Host:           newHost(t, options),
			HostGeneration: testHostGeneration,
		})
		var invalid *service.InvalidCapacityOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("NewCapacityPublisher on a zero-time Clock = %v (%T), want *service.InvalidCapacityOptionsError", err, err)
		}
		var validation *sessionwire.RequestValidationError
		if !errors.As(err, &validation) || validation.Field != "observed_at" {
			t.Errorf("Core's objection = %v, want an observed_at validation error", err)
		}
		if invalid.Field != "Host" {
			t.Errorf("refusal field = %q, want %q: the instant comes from the Host's Clock, and naming HostGeneration here sends the operator to a knob that is already correct", invalid.Field, "Host")
		}
		// The control: the same Clock one instant later is accepted.
		options.Clock = frozenClock{at: time.Unix(1, 0)}
		if _, err := service.NewCapacityPublisher(service.CapacityOptions{
			Host:           newHost(t, options),
			HostGeneration: testHostGeneration,
		}); err != nil {
			t.Errorf("a non-zero frozen Clock was refused too, so the assertion above proves nothing: %v", err)
		}
	})
}

// TestPublishRefusesAnObservationCoreWouldReject holds the same rule at the
// publishing boundary rather than at construction.
//
// A publisher whose Clock returns the zero time only later — nothing forbids it,
// the Clock is an injected interface — must refuse rather than emit a record no
// reader can accept.
func TestPublishRefusesAnObservationCoreWouldReject(t *testing.T) {
	t.Parallel()

	clock := &switchableClock{now: time.Date(2026, 9, 2, 11, 22, 33, 44, time.UTC)}
	options := pooledOptions(t, clock)
	publisher := newPublisher(t, options)
	if _, err := publisher.Publish(); err != nil {
		t.Fatalf("the first Publish failed, so the refusal below proves nothing: %v", err)
	}

	clock.now = time.Time{}
	advertisements, err := publisher.Publish()
	if err == nil {
		t.Fatal("Publish emitted an advertisement observed at the zero time, which Core rejects as a missing observed_at")
	}
	if advertisements != nil {
		t.Errorf("Publish returned a non-nil slice of %d advertisements alongside its error; the result must be nil so a caller cannot write it anyway", len(advertisements))
	}
	var validation *sessionwire.RequestValidationError
	if !errors.As(err, &validation) || validation.Field != "observed_at" {
		t.Errorf("Publish error = %v, want an observed_at validation error", err)
	}
}

// switchableClock is a Clock whose instant a test rewrites between calls. It is
// not mutex-guarded and is used only by the single-goroutine test above.
type switchableClock struct{ now time.Time }

func (c *switchableClock) Now() time.Time                       { return c.now }
func (c *switchableClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestPublisherIsSafeUnderConcurrentUse runs the mutating and derived paths
// together under -race.
//
// It asserts a RESULT rather than a schedule: whatever interleaving occurs,
// every session admitted and not released is still charged at the end, and every
// publication in flight was a valid record. There is no synchronisation here
// beyond WaitGroup, because there is nothing to wait FOR.
func TestPublisherIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Capacity = 64
	options.Department = testDepartment(t,
		registration("planner", pooledCapabilities(2)),
		registration("reviewer", pooledCapabilities(1)),
	)
	publisher := newPublisher(t, options)

	const sessions = 16
	var group sync.WaitGroup
	for i := 0; i < sessions; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			key := registry.Key{TenantID: testTenant, SessionID: sessionwire.SessionID("session-" + strconv.Itoa(i))}
			if err := publisher.Admit(key, "reviewer"); err != nil {
				t.Errorf("Admit: %v", err)
				return
			}
			if i%2 == 0 {
				publisher.Release(key)
			}
		}(i)
		group.Add(1)
		go func() {
			defer group.Done()
			advertisements, err := publisher.Publish()
			if err != nil {
				t.Errorf("Publish: %v", err)
				return
			}
			for _, advertisement := range advertisements {
				if err := advertisement.Report.Validate(); err != nil {
					t.Errorf("concurrent publication is not a valid sessionwire record: %v", err)
				}
			}
		}()
	}
	group.Wait()

	if got := publisher.ConsumedWeight(); got != sessions/2 {
		t.Errorf("ConsumedWeight = %d, want %d: half the weight-1 sessions were released", got, sessions/2)
	}
}

// TestTheIsolationClassIsThePooledAdmissionRule is human gate H8, answered
// 2026-09-04 as option (a): drop the fixed TenantID.
//
// Host was constructed with a TenantID and the residency manager refused every
// attach naming another, so a pooled deployment needed one Host Deployment per
// tenant and IsolationClass — the field spec §12 says decides this — had no
// reader that could matter. It has one now, and it is HERE rather than in the
// residency manager for a reason the fix depends on: this is the Host's ONE
// admission ledger and its lock is the only Host-wide critical section an
// admission passes through. The manager's per-key slot cannot order two
// attaches for two DIFFERENT keys, so a tenant rule enforced there would be
// decided by two concurrent readers of the same empty registry.
//
// A Host WITHOUT the class is tenant-exclusive by PLACEMENT, not by
// construction: spec §12 says "Factory placement enforces the restriction".
// What is held here is the local backstop, and the exclusivity is a property of
// what is currently resident — so it is acquired by the first admission and
// GIVEN UP when the last one is released, which is what makes a warm release
// the thing that frees a pooled Host for another tenant.
func TestTheIsolationClassIsThePooledAdmissionRule(t *testing.T) {
	t.Parallel()

	first := registry.Key{TenantID: "tenant-9f3", SessionID: "session-a"}
	second := registry.Key{TenantID: "tenant-other", SessionID: "session-b"}

	t.Run("cross_tenant_isolated admits two tenants", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t, newFakeClock())
		options.IsolationClass = sessionwire.HostIsolationClassCrossTenantIsolated
		publisher := newPublisher(t, options)

		if err := publisher.Admit(first, "reviewer"); err != nil {
			t.Fatalf("Admit(%v) = %v, want acceptance", first, err)
		}
		if err := publisher.Admit(second, "reviewer"); err != nil {
			t.Fatalf("a cross-tenant-isolated Host refused a second tenant: %v", err)
		}
	})

	t.Run("tenant_exclusive refuses a second tenant", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t, newFakeClock())
		options.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
		publisher := newPublisher(t, options)

		if err := publisher.Admit(first, "reviewer"); err != nil {
			t.Fatalf("Admit(%v) = %v, want acceptance", first, err)
		}
		// The SAME tenant's second session is unaffected: exclusivity is about
		// tenants, not about how many sessions one tenant may hold.
		sibling := registry.Key{TenantID: first.TenantID, SessionID: "session-sibling"}
		if err := publisher.Admit(sibling, "reviewer"); err != nil {
			t.Fatalf("a tenant-exclusive Host refused the SAME tenant's second session: %v", err)
		}

		err := publisher.Admit(second, "reviewer")
		var refused *service.AdmissionRefusedError
		if !errors.As(err, &refused) {
			t.Fatalf("Admit(%v) = %v, want an *AdmissionRefusedError", second, err)
		}
		if refused.Code != sessionwire.HostLinkErrorNotAdmitting {
			t.Errorf("refusal code = %q, want %q", refused.Code, sessionwire.HostLinkErrorNotAdmitting)
		}
		if !strings.Contains(refused.Reason, string(first.TenantID)) {
			t.Errorf("the refusal %q does not name the tenant that holds this Host", refused.Reason)
		}
	})

	t.Run("the isolation refusal precedes the target lookup", func(t *testing.T) {
		t.Parallel()
		// A MUTATION SURVIVED THIS FILE UNTIL THIS ROW EXISTED. Moving the
		// isolation check below the byAgent lookup passed the whole suite,
		// because no case presented BOTH a foreign tenant and an agent this
		// Department does not register — and the two refusals carry different
		// Codes, which is the value a Factory branches on. not_admitting means
		// re-run placement; runtime_unavailable means this Host cannot serve
		// the agent at all, which is a statement about the Department that is
		// false here and which also answers a question about another tenant's
		// placement that the refusal has no business answering.
		options := pooledOptions(t, newFakeClock())
		options.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
		publisher := newPublisher(t, options)
		if err := publisher.Admit(first, "reviewer"); err != nil {
			t.Fatalf("Admit(%v) = %v, want acceptance", first, err)
		}

		err := publisher.Admit(second, "not-registered")
		var refused *service.AdmissionRefusedError
		if !errors.As(err, &refused) {
			t.Fatalf("Admit = %v, want an *AdmissionRefusedError", err)
		}
		if refused.Code != sessionwire.HostLinkErrorNotAdmitting {
			t.Errorf("refusal code = %q, want %q: the isolation rule is decided before this Host looks the agent up", refused.Code, sessionwire.HostLinkErrorNotAdmitting)
		}
	})

	t.Run("releasing the last session frees the Host for another tenant", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t, newFakeClock())
		options.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
		publisher := newPublisher(t, options)

		if err := publisher.Admit(first, "reviewer"); err != nil {
			t.Fatalf("Admit(%v) = %v, want acceptance", first, err)
		}
		if err := publisher.Admit(second, "reviewer"); err == nil {
			t.Fatal("a second tenant was admitted while the first was resident")
		}
		if !publisher.Release(first) {
			t.Fatal("Release reported the first session was not admitted")
		}
		// This is the warm release's consequence, and it is why exclusivity is
		// derived from the ledger rather than latched at the first admission: a
		// latched value would hold this Host to a tenant that has no session on
		// it, forever.
		if err := publisher.Admit(second, "reviewer"); err != nil {
			t.Fatalf("the Host stayed bound to a tenant with nothing resident: %v", err)
		}
	})
}

// TestACreditBelongsToTheResidencyThatOwnsTheCharge is booked finding B2. The
// ledger is keyed by session and Admit is idempotent, so a SUCCESSOR attach
// admitting the session between the old residency's registry removal and its
// credit found the old charge, took it as its own and charged nothing — and
// the old path's credit then uncharged the successor: over-admission by one.
// The charge is now OWNED by a residency generation: a re-admission claims it
// for the attach in flight, and only the owning generation's credit returns it.
func TestACreditBelongsToTheResidencyThatOwnsTheCharge(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t, newFakeClock())
	options.Capacity = 2
	options.Department = testDepartment(t, registration("reviewer", pooledCapabilities(2)))
	publisher := newPublisher(t, options)
	key := registry.Key{TenantID: testTenant, SessionID: "session-r"}

	// The old residency, generation 7, charged and owned.
	if err := publisher.Admit(key, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if !publisher.Own(key, 7) {
		t.Fatal("Own of a fresh charge refused")
	}
	// THE RACE: the successor's step 1 lands before the old path's credit.
	if err := publisher.Admit(key, "reviewer"); err != nil {
		t.Fatalf("the successor's Admit: %v", err)
	}
	if publisher.ReleaseOwned(key, 7) {
		t.Fatal("the old residency's credit returned a charge the successor had claimed")
	}
	if got := publisher.ConsumedWeight(); got != 2 {
		t.Fatalf("ConsumedWeight = %d after the stale credit, want 2: the successor is resident and uncharged", got)
	}
	// The successor installs as generation 8 and owns the charge; a stale
	// credit still does nothing, and its own credit returns it exactly once.
	if !publisher.Own(key, 8) {
		t.Fatal("the successor could not own the charge it claimed")
	}
	if publisher.Own(key, 7) {
		t.Fatal("a stale generation took the charge back from its owner")
	}
	if publisher.ReleaseOwned(key, 7) || publisher.ConsumedWeight() != 2 {
		t.Fatal("a stale credit returned the successor's owned charge")
	}
	if !publisher.ReleaseOwned(key, 8) || publisher.ConsumedWeight() != 0 {
		t.Fatalf("the owner's credit = %d consumed, want 0", publisher.ConsumedWeight())
	}
	if publisher.ReleaseOwned(key, 8) {
		t.Fatal("a second credit of the same residency returned weight twice")
	}

	// And the ordinary order: the old credit lands first, the successor
	// charges afresh.
	if err := publisher.Admit(key, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	publisher.Own(key, 9)
	if !publisher.ReleaseOwned(key, 9) || publisher.ConsumedWeight() != 0 {
		t.Fatal("an owner's credit before any successor did not return the charge")
	}
}
