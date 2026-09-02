package service_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
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

	"github.com/looprig/host"
	"github.com/looprig/host/department"
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
func pooledOptions(t *testing.T, clock host.Clock) host.Options {
	t.Helper()
	return host.Options{
		HostID:            "host-7c1",
		TenantID:          "tenant-9f3",
		InternalEndpoint:  "wss://host-7c1.internal.example:8443/hostlink",
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

// testHostGeneration is a non-zero incarnation identity. Core rejects zero.
const testHostGeneration = uint64(9)

func newHost(t *testing.T, options host.Options) *host.Host {
	t.Helper()
	built, err := host.New(options)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	return built
}

func newPublisher(t *testing.T, options host.Options) *service.CapacityPublisher {
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

	base := func(t *testing.T, mutate func(*host.Options)) map[sessionwire.AgentID]service.Advertisement {
		t.Helper()
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t,
			registration("planner", pooledCapabilities(1)),
			registration("reviewer", pooledCapabilities(1)),
		)
		mutate(&options)
		return byAgent(t, publish(t, newPublisher(t, options)))
	}

	first := base(t, func(*host.Options) {})
	otherHost := base(t, func(o *host.Options) {
		o.HostID = "host-b42"
		o.InternalEndpoint = "wss://host-b42.internal.example:8443/hostlink"
	})
	dedicated := base(t, func(o *host.Options) {
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
	shared := byAgent(t, publish(t, newPublisher(t, func() host.Options {
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

	upgraded := base(t, func(o *host.Options) {
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
	colonInAgent := byAgent(t, publish(t, newPublisher(t, func() host.Options {
		options := pooledOptions(t, newFakeClock())
		options.Department = testDepartment(t, department.Registration{AgentID: "a:b", Target: testTarget{compatibility: "c", capabilities: pooledCapabilities(1)}})
		return options
	}())))
	colonInCompatibility := byAgent(t, publish(t, newPublisher(t, func() host.Options {
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
	opaque := base(t, func(o *host.Options) {
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
	if err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "session-1"}, "reviewer"); err != nil {
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
			missable := host.MinHeartbeatsBeforeExpiry - 1
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
				key := registry.Key{TenantID: options.TenantID, SessionID: sessionwire.SessionID("session-" + strconv.FormatUint(admitted, 10))}
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

	reviewer := registry.Key{TenantID: options.TenantID, SessionID: "session-r"}
	planner := registry.Key{TenantID: options.TenantID, SessionID: "session-p"}
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
	if publisher.Release(registry.Key{TenantID: options.TenantID, SessionID: "never-admitted"}) {
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
	publisher := newPublisher(t, options)

	// A Host is tenant-scoped, so the second key is a key this Host would never
	// see in production. It is here because the LEDGER must not be the place
	// that assumption is enforced silently: charging once for two distinct keys
	// is an over-admission whichever tenant they name.
	for _, tenant := range []sessionwire.TenantID{options.TenantID, "tenant-other"} {
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
	key := registry.Key{TenantID: options.TenantID, SessionID: "session-r"}

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

	err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, "not-registered")
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
	if err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, "reviewer"); err != nil {
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
			err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, row.wantRefusedFor)
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
	err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, "reviewer")
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
	key := registry.Key{TenantID: options.TenantID, SessionID: "session-r"}

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
	for _, phase := range []struct {
		name string
		act  func()
	}{
		{"initial publish", func() {}},
		{"after admission", func() {
			if err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, "planner"); err != nil {
				t.Fatalf("Admit: %v", err)
			}
		}},
		{"after drain", publisher.BeginDrain},
	} {
		t.Run(phase.name, func(t *testing.T) {
			phase.act()
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
// WHAT IT DOES NOT COVER: other PACKAGES, and anything a caller does to the
// slice after Publish returns. Its two instruments are separately controlled by
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
	}

	// The FILE SET is enumerated too, and that is not tidiness. A probe measured
	// the gap: with only "capacity.go" parsed, a second exported publishing
	// surface added in a new file of THIS SAME PACKAGE survived every assertion
	// below, while the identical payload in capacity.go was caught. A whitelist
	// that names its own subject is the fix.
	files := packageFiles(t, false)
	if !slices.Equal(files, []string{"capacity.go"}) {
		t.Errorf("this package's production files are %v, want exactly [capacity.go]: a second file is a second place to publish from, and everything below reads only the enumerated ones", files)
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
		return []exportedDeclaration{{name: name, publishes: mentionsAdvertisement(typed.Type.Results)}}
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
func receiverTypeName(expression ast.Expr) string {
	if star, isPointer := expression.(*ast.StarExpr); isPointer {
		expression = star.X
	}
	if identifier, isIdentifier := expression.(*ast.Ident); isIdentifier {
		return identifier.Name
	}
	return "?"
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

// TestAdvertisementCarriesExactlyTheEnumeratedFields is the same whitelist over
// the record's shape. A field added to what Host publishes fails here until it
// is enumerated, which is the visible diff a silent addition would not be.
func TestAdvertisementCarriesExactlyTheEnumeratedFields(t *testing.T) {
	t.Parallel()

	// The file set again, rather than a literal name. Today the other guard's
	// enumeration happens to keep this package to one production file, but a
	// guard that depends on a neighbouring guard's assertion reverts silently
	// when that one is relaxed, and the diff would be at the other site.
	files := packageFiles(t, false)
	if !slices.Equal(files, []string{"capacity.go"}) {
		t.Fatalf("this package's production files are %v, want exactly [capacity.go]", files)
	}
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, files[0], nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", files[0], err)
	}
	got := structFieldNames(t, parsed, "Advertisement")
	want := []string{"DueAt", "Namespace", "Rank", "Ranked", "RankingScope", "Report", "StableKey", "Tombstone"}
	if !slices.Equal(got, want) {
		t.Errorf("Advertisement fields = %v, want exactly %v", got, want)
	}
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

	var sawCatalogue, sawConstant bool
	for _, declaration := range parsed.Decls {
		for _, exported := range exportedDeclarations(declaration) {
			switch exported.name {
			case "StaticCatalogue":
				sawCatalogue = true
				if !exported.publishes {
					t.Error("a func-typed exported var yielding []Advertisement was not counted as a publishing surface, so a second catalogue declared as a variable would escape the enumeration")
				}
			case "PlainConstant":
				sawConstant = true
				if exported.publishes {
					t.Error("an exported constant mentioning no record was counted as a publishing surface, so the publishing check reports everything and distinguishes nothing")
				}
			}
		}
	}
	if !sawCatalogue || !sawConstant {
		t.Fatalf("exportedDeclarations reported neither the var (%v) nor the const (%v); it reached nothing", sawCatalogue, sawConstant)
	}

	fields := structFieldNames(t, parsed, "Embedding")
	if !slices.Equal(fields, []string{"Named", "embedded embeddedPart"}) {
		t.Errorf("structFieldNames over an embedding struct = %v, want [Named \"embedded embeddedPart\"]: an embedded field carries no name, and its own fields are published through the outer struct all the same", fields)
	}
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
	if err := publisher.Admit(registry.Key{TenantID: options.TenantID, SessionID: "s"}, "reviewer"); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	publisher.Release(registry.Key{TenantID: options.TenantID, SessionID: "s"})
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

// TestNothingInThisPackageSleepsOrPolls holds the structural half of the same
// property over every file of this package.
//
// It bans, by parsed structure and not by text search, every construct through
// which a wait could enter: time.Sleep, time.After, time.Tick, time.NewTicker,
// runtime.Gosched, any select statement, and any channel receive. A fake clock
// removes the race by construction, and needing any of these back would be a
// design signal rather than a test-tuning problem.
//
// WHAT IT DOES NOT COVER: a wait reached through a helper in another PACKAGE,
// and a busy loop that spins on a value without receiving. Neither is present
// today and neither is detectable here. The file set is enumerated, so a new
// file of this package fails rather than escaping — which it did before the
// enumeration was added, measured by probe.
func TestNothingInThisPackageSleepsOrPolls(t *testing.T) {
	t.Parallel()

	banned := map[string]bool{
		"time.Sleep": true, "time.After": true, "time.Tick": true,
		"time.NewTicker": true, "time.AfterFunc": true, "runtime.Gosched": true,
	}
	files := packageFiles(t, true)
	if !slices.Equal(files, []string{"capacity.go", "capacity_test.go"}) {
		t.Errorf("this package's files are %v, want exactly [capacity.go capacity_test.go]: a construct banned here is banned in this package, and the ban reaches only the files it names", files)
	}
	inspected := 0
	for _, name := range files {
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
					t.Errorf("%s calls %s at %s: nothing in this package waits on real time", name, qualified, fileSet.Position(typed.Pos()))
				}
			case *ast.SelectStmt:
				t.Errorf("%s has a select statement at %s: the one coin-flip test this program has shipped was a select with two ready cases", name, fileSet.Position(typed.Pos()))
			case *ast.UnaryExpr:
				if typed.Op == token.ARROW {
					t.Errorf("%s receives from a channel at %s: a fake clock removes the wait, so there is nothing to receive", name, fileSet.Position(typed.Pos()))
				}
			}
			return true
		})
	}
	if inspected == 0 {
		t.Fatal("no call expression was inspected, so this guard reached nothing")
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
			key := registry.Key{TenantID: options.TenantID, SessionID: sessionwire.SessionID("session-" + strconv.Itoa(i))}
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
