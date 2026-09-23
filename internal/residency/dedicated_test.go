package residency

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// O6.2 — dedicated Host semantics
// ---------------------------------------------------------------------------
//
// This file exists because the refusal at manager.go:1475 PREDATES this task and
// an existing refusal is not this task's coverage. The audit that opened O6.2
// deleted that line and TestADedicatedHostRefusesASessionItIsNotBoundTo caught
// it — so the guard is real — but three mutations of the SAME line survived the
// whole suite, and each names a dimension that test holds fixed:
//
//	A2  `&& request.Mode == ModeCreate`   — the refusal was proven for creates only.
//	A3  `&& request.TenantID == "tenant-9f3"` — proven for the fixture's one tenant.
//	A4  refuse the fixed session under any tenant but "tenant-9f3" — i.e. put back
//	    the tenant exclusivity H8 removed. NOTHING in the suite noticed.
//
// A4 is the one that matters. H8 deleted Options.TenantID, so a dedicated Host
// has NO CONFIGURED TENANT and its tenant is whatever its one admission
// establishes. That is a behaviour change with no guard, and a test that pinned
// a particular tenant would be asserting a rule that no longer exists. The rows
// below vary the tenant precisely so they cannot.

// arbitraryTenants are tenants the fixture has never seen, plus the one it has.
//
// The fixture's own testTenant is INCLUDED rather than replaced: "any tenant"
// includes the ordinary one, and a row set that only used strangers would leave
// the common case asserted nowhere in this file.
var arbitraryTenants = []sessionwire.TenantID{
	testTenant,
	"tenant-somebody-else",
	"t",
	"tenant-0000000000000000000000000000",
}

// requestForTenant is requestFor with the tenant moved.
//
// The PRINCIPAL moves with it. Principal.validate refuses a principal acting for
// a different tenant than the request names, and it runs AFTER the fixed-session
// check — so a row that moved only request.TenantID would still be refused, just
// at a later rule, and would report "a dedicated Host refused a foreign tenant"
// when what it had actually built was a malformed principal.
func (f *fixture) requestForTenant(tenant sessionwire.TenantID, session sessionwire.SessionID, mode Mode) Request {
	request := f.requestFor(session, mode)
	request.TenantID = tenant
	request.Principal.TenantID = tenant
	return request
}

// TestADedicatedHostAcceptsItsFixedSessionUnderAnyTenant is H8 asserted.
//
// There is no tenant to match against, so the only tenant rule left on this path
// is the IDENTITY rule — the tenant must be a permitted sessionwire identity —
// and every value below satisfies it. The attach must therefore succeed and run
// the ordinary create sequence, not a shortened or diverted one.
func TestADedicatedHostAcceptsItsFixedSessionUnderAnyTenant(t *testing.T) {
	for _, tenant := range arbitraryTenants {
		t.Run(string(tenant), func(t *testing.T) {
			f := newFixture(t, func(f *fixture) {
				f.fixedSession = testSession
				f.target.capabilities.AdmissionWeight = 1
			})

			residency, err := f.manager.Attach(context.Background(), f.requestForTenant(tenant, testSession, ModeCreate))
			if err != nil {
				t.Fatalf("the dedicated Host refused its fixed session under tenant %q: %v", tenant, err)
			}
			if !residency.Attached {
				t.Error("the attach reports Attached false; it is the call that established the residency")
			}
			if residency.Key != (registry.Key{TenantID: tenant, SessionID: testSession}) {
				t.Errorf("residency key is %+v, want the tenant the admission established", residency.Key)
			}
			requireSteps(t, f.trace.recorded(), createSequence)
			if _, held := f.registry.Get(registry.Key{TenantID: tenant, SessionID: testSession}); !held {
				t.Error("no local registry entry for the session this dedicated Host is bound to")
			}
		})
	}
}

// TestADedicatedHostRefusesAnyOtherSessionWhateverItsTenantOrMode is step 1's
// refusal half, varied over the two dimensions the predecessor test held fixed.
//
// The refusal must be the FIXED-SESSION one — StepValidate carrying
// runtime_mismatch — and not some later rule that happens to also refuse. A row
// that only checked `err != nil` would pass against a manager that had lost this
// guard entirely and refused at the principal or the agent instead.
func TestADedicatedHostRefusesAnyOtherSessionWhateverItsTenantOrMode(t *testing.T) {
	for _, mode := range []Mode{ModeCreate, ModeRestore} {
		for _, tenant := range arbitraryTenants {
			t.Run(string(mode)+"/"+string(tenant), func(t *testing.T) {
				f := newFixture(t, func(f *fixture) {
					f.fixedSession = testSession
					f.target.capabilities.AdmissionWeight = 1
				})

				_, err := f.manager.Attach(context.Background(), f.requestForTenant(tenant, "session-not-mine", mode))
				if err == nil {
					t.Fatal("a dedicated Host accepted a session it is not bound to")
				}
				var attach *AttachError
				if !errors.As(err, &attach) {
					t.Fatalf("error is %T, want *AttachError", err)
				}
				if attach.Step != StepValidate {
					t.Errorf("the refusal names step %q, want %q", attach.Step, StepValidate)
				}
				if attach.Code != sessionwire.HostLinkErrorRuntimeMismatch {
					t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, sessionwire.HostLinkErrorRuntimeMismatch)
				}
				requireCoreEncodable(t, attach)
				// Step 1 precedes every collaborator, so the refusal took
				// nothing. This is the clause that distinguishes "refused" from
				// "refused after launching a runtime for it".
				if steps := f.trace.recorded(); len(steps) != 0 {
					t.Errorf("a refused request reached %v; step 1 precedes every collaborator", steps)
				}
				f.assertNothingHeld(t)
				f.assertRouteRemoved(t)
			})
		}
	}
}

// TestADedicatedHostRefusesASecondRuntimeForItsFixedSession covers step 1's
// third clause, and it takes THREE rows because three different readers can be
// the one that refuses and they refuse different things.
//
// The session id alone does not settle it: a residency is keyed by (tenant,
// session), so a SECOND TENANT naming the same fixed session is a DIFFERENT KEY
// and sails past manager.go:1475, which compares only the session. What stops it
// is the admission ledger — the isolation backstop when the class is
// tenant_exclusive, and capacity when it is not, since host.Options pins a
// dedicated Host to capacity 1. Naming both is the point: a reviewer who deleted
// one would otherwise believe the other was never load-bearing.
func TestADedicatedHostRefusesASecondRuntimeForItsFixedSession(t *testing.T) {
	t.Run("the same tenant re-attaching takes the resident runtime and launches no second one", func(t *testing.T) {
		f := newFixture(t, func(f *fixture) {
			f.fixedSession = testSession
			f.target.capabilities.AdmissionWeight = 1
		})

		first, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
		if err != nil {
			t.Fatalf("first attach: %v", err)
		}
		second, err := f.manager.Attach(context.Background(), f.request(ModeCreate))
		if err != nil {
			t.Fatalf("second attach: %v", err)
		}
		if second.Attached {
			t.Error("the second attach reports Attached true; it established nothing")
		}
		if second.Runtime != first.Runtime {
			t.Error("the second attach reports a different runtime from the resident one")
		}
		if made := len(f.target.producedRuntimes()); made != 1 {
			t.Errorf("%d runtimes were launched for the fixed session, want 1", made)
		}
	})

	for _, row := range []struct {
		name      string
		isolation sessionwire.HostIsolationClass
		wantCode  sessionwire.HostLinkErrorCode
	}{
		{
			name:      "a second tenant naming the same session is refused by the isolation backstop",
			isolation: sessionwire.HostIsolationClassTenantExclusive,
			wantCode:  sessionwire.HostLinkErrorNotAdmitting,
		},
		{
			name:      "a second tenant naming the same session is refused by capacity when isolation permits it",
			isolation: sessionwire.HostIsolationClassCrossTenantIsolated,
			wantCode:  sessionwire.HostLinkErrorNoCapacity,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t, func(f *fixture) {
				f.fixedSession = testSession
				f.isolation = row.isolation
				f.target.capabilities.AdmissionWeight = 1
			})

			if _, err := f.manager.Attach(context.Background(), f.requestForTenant(testTenant, testSession, ModeCreate)); err != nil {
				t.Fatalf("first attach: %v", err)
			}
			_, err := f.manager.Attach(context.Background(), f.requestForTenant("tenant-second", testSession, ModeCreate))
			if err == nil {
				t.Fatal("a dedicated Host admitted a second runtime for its fixed session")
			}
			var attach *AttachError
			if !errors.As(err, &attach) {
				t.Fatalf("error is %T, want *AttachError", err)
			}
			if attach.Code != row.wantCode {
				t.Errorf("the refusal carries HostLink code %q, want %q", attach.Code, row.wantCode)
			}
			requireCoreEncodable(t, attach)
			if made := len(f.target.producedRuntimes()); made != 1 {
				t.Errorf("%d runtimes were launched for the fixed session, want 1", made)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Step 2: the same path, not merely the same outcome
// ---------------------------------------------------------------------------

// placementObservation is everything an attach made observable, gathered so that
// the POOLED and DEDICATED runs can be compared TO EACH OTHER.
//
// That comparison is the whole of step 2 and it is why this is a struct rather
// than six assertions. Six per-mode assertions each say "dedicated mode does
// something"; none of them says the two modes traverse the same path, and a
// divergence introduced into EITHER mode leaves all six passing. Comparing the
// two observations breaks on a divergence in either direction, including one
// nobody thought to write an assertion for — a field added to this struct is
// automatically held to sameness.
//
// It carries no runtime pointer, no context and no time: those differ between
// two runs of the same scenario for reasons that are not placement, and a
// sameness assertion that cannot hold has to be dropped rather than weakened.
type placementObservation struct {
	Steps            []string
	Failure          string
	Attached         bool
	AgentID          sessionwire.AgentID
	CompatibilityID  department.CompatibilityID
	LeaseEpoch       ResidencyEpoch
	Generation       uint64
	JournalEpoch     JournalEpoch
	JournalHeld      bool
	ConsumedWeight   uint64
	RuntimesLaunched int
	LeasesHeld       int
	EntryHeld        bool
	EntryState       registry.ResidencyState
	EntryAccepting   bool
	EntryLeaseEpoch  uint64
	EntryCompat      department.CompatibilityID
	LiveProjections  int
	Published        int
	Tombstones       int
	WorkspacesMade   int
	WorkspacesFreed  int
}

// observePlacement runs one scenario under one placement and returns what it
// made observable.
//
// CAPACITY IS PINNED TO 1 IN BOTH ARMS. host.Options refuses a dedicated Host
// with any other capacity, so a pooled arm left at testCapacity would differ
// from the dedicated arm in TWO ways and a divergence caused by capacity would
// be indistinguishable from one caused by placement. Pinning it leaves exactly
// two fields different between the arms: Placement and FixedSessionID.
func observePlacement(t *testing.T, fixed sessionwire.SessionID, mode Mode, spoil func(*fixture)) placementObservation {
	t.Helper()
	f := newFixture(t, func(f *fixture) {
		f.fixedSession = fixed
		f.capacity = 1
		f.target.capabilities.AdmissionWeight = 1
		if spoil != nil {
			spoil(f)
		}
	})

	observation := placementObservation{Steps: f.trace.recorded()}
	residency, err := f.manager.Attach(context.Background(), f.request(mode))
	observation.Steps = f.trace.recorded()
	if err != nil {
		var attach *AttachError
		if !errors.As(err, &attach) {
			t.Fatalf("error is %T, want *AttachError", err)
		}
		observation.Failure = string(attach.Step) + "/" + string(attach.Code)
	} else {
		observation.Attached = residency.Attached
		observation.AgentID = residency.AgentID
		observation.CompatibilityID = residency.CompatibilityID
		observation.LeaseEpoch = residency.LeaseEpoch
		observation.Generation = residency.Generation
		observation.JournalEpoch = residency.JournalEpoch
		observation.JournalHeld = residency.JournalEpochHeld
	}
	observation.ConsumedWeight = f.publisher.ConsumedWeight()
	observation.RuntimesLaunched = len(f.target.producedRuntimes())
	observation.LeasesHeld = f.leases.heldCount()
	entry, held := f.registry.Get(f.key())
	observation.EntryHeld = held
	if held {
		observation.EntryState = entry.State
		observation.EntryAccepting = entry.Accepting
		observation.EntryLeaseEpoch = entry.LeaseEpoch
		observation.EntryCompat = entry.CompatibilityID
	}
	observation.LiveProjections = len(f.locations.live())
	observation.Published = len(f.locations.publishedAll())
	observation.Tombstones = len(f.locations.tombstones())
	observation.WorkspacesMade, observation.WorkspacesFreed = f.workspaces.counts()
	return observation
}

// TestPooledAndDedicatedTraverseTheSameAttachPath is 04-host.md's step 2 for the
// lease, the registry, the checkpoint read and the launch: "use the same lease,
// registry, inbox, HostLink, checkpoint, and release behavior as pooled mode".
//
// The assertion is EQUALITY BETWEEN THE TWO ARMS, not equality of each arm to a
// literal. A literal would have to be updated when the sequence legitimately
// changes, and the tempting way to update it — copy what the manager now does —
// is how a sameness test stops noticing anything. Comparing the arms means a
// legitimate change moves both and the test stays silent, while a change that
// moves only one breaks it, which is the only event this test is for.
//
// The literal sequences are asserted too, but as a SEPARATE clause and only
// against the pooled arm, so that "the two are the same" and "the same thing is
// the documented sequence" remain two claims a reader can tell apart.
func TestPooledAndDedicatedTraverseTheSameAttachPath(t *testing.T) {
	for _, row := range []struct {
		mode Mode
		want []string
	}{
		{mode: ModeCreate, want: createSequence},
		{mode: ModeRestore, want: restoreSequence},
	} {
		t.Run(string(row.mode), func(t *testing.T) {
			pooled := observePlacement(t, "", row.mode, nil)
			dedicated := observePlacement(t, testSession, row.mode, nil)

			if pooled.Failure != "" {
				t.Fatalf("the pooled arm failed at %s", pooled.Failure)
			}
			if !reflect.DeepEqual(pooled, dedicated) {
				t.Errorf("the two placements diverged:\n pooled    %+v\n dedicated %+v", pooled, dedicated)
			}
			requireSteps(t, pooled.Steps, row.want)
		})
	}
}

// TestPooledAndDedicatedUnwindAFailedAttachIdentically is step 2's RELEASE half.
//
// A sameness claim about the happy path is the easy half: the interesting
// divergence is a dedicated Host that declines to give something back because
// "it is the only session anyway". Each row fails the attach at a different
// depth of the sequence — before the lease, after it, and after hydration — so
// the comparison covers an unwind that owes nothing, one that owes a lease, and
// one that owes a lease and a fence.
func TestPooledAndDedicatedUnwindAFailedAttachIdentically(t *testing.T) {
	sentinel := errors.New("injected")

	for _, row := range []struct {
		name  string
		mode  Mode
		spoil func(*fixture)
	}{
		{name: "lease refused", mode: ModeCreate, spoil: func(f *fixture) { f.leases.err = sentinel }},
		{name: "workspace materialization refused", mode: ModeCreate, spoil: func(f *fixture) { f.workspaces.ensureErr = sentinel }},
		{name: "durable state unreadable", mode: ModeRestore, spoil: func(f *fixture) { f.durable.err = sentinel }},
	} {
		t.Run(row.name, func(t *testing.T) {
			pooled := observePlacement(t, "", row.mode, row.spoil)
			dedicated := observePlacement(t, testSession, row.mode, row.spoil)

			// The rows must actually fail. A spoiler that stopped working would
			// otherwise turn all three into a second copy of the happy-path
			// test, passing while asserting nothing about an unwind.
			if pooled.Failure == "" {
				t.Fatal("the pooled arm succeeded; this row is supposed to fail the attach")
			}
			if !reflect.DeepEqual(pooled, dedicated) {
				t.Errorf("the two placements unwound differently:\n pooled    %+v\n dedicated %+v", pooled, dedicated)
			}
			if pooled.LeasesHeld != 0 || pooled.ConsumedWeight != 0 || pooled.EntryHeld || pooled.LiveProjections != 0 {
				t.Errorf("the unwind left something behind: %+v", pooled)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Step 3: a long warm window is still a warm window
// ---------------------------------------------------------------------------

// dedicatedWarmTTL is a warm window nobody would call short.
const dedicatedWarmTTL = 30 * 24 * time.Hour

// hostWithWarmTTL builds a real Host at one placement so the TTL under test is
// one a Host can actually be CONSTRUCTED with.
//
// Going through host.New rather than passing the duration straight to
// WarmOptions is the point of the helper: it is what makes "a dedicated Host
// configures a long warm TTL" a statement about a configuration that exists,
// rather than about a number a test invented.
func hostWithWarmTTL(t *testing.T, fixed sessionwire.SessionID, ttl time.Duration) *hostconfig.Host {
	t.Helper()
	dept, err := department.New([]department.Registration{{AgentID: testAgent, Target: &fakeTarget{
		trace:         &trace{},
		compatibility: testCompat,
		capabilities: department.Capabilities{
			SupportsPooled:    true,
			SupportsDedicated: true,
			AdmissionWeight:   1,
			CaptureSafety:     department.CaptureSafetyStreaming,
		},
	}}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	placement, capacity := sessionwire.HostPlacementPooled, testCapacity
	if fixed != "" {
		placement, capacity = sessionwire.HostPlacementDedicated, 1
	}
	built, err := hostconfig.New(hostconfig.Options{
		HostID:            testHost,
		InternalEndpoint:  testEndpoint,
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      stubSessionStore{},
		Workspaces:        stubHostWorkspaces{},
		Clock:             fixedClock{at: testClockAt},
		Auth:              stubAuth{},
		Placement:         placement,
		Capacity:          capacity,
		FixedSessionID:    fixed,
		WarmTTL:           ttl,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    testExpiry,
		ClaimTTL:          11 * time.Second,
		ApplyDeadline:     47 * time.Second,
		CommandQueueSize:  257,
		ReconcileInterval: 23 * time.Second,
		ReconcileBatch:    129,
	})
	if err != nil {
		t.Fatalf("hostconfig.New: %v", err)
	}
	return built
}

// TestALongWarmTTLDoesNotMakeADedicatedHostDurable is 04-host.md step 3:
// "configure long warm TTL/idle policy but DO NOT EQUATE DEDICATED WITH
// DURABLE".
//
// The failure this exists to catch is an optimisation, not a bug: a dedicated
// Host holds one session for its whole life, so releasing it on idle looks like
// pointless work and "dedicated means resident until drain" is an easy thing to
// talk yourself into. It is wrong — the warm release is what commits the
// checkpoint and returns the lease, so a dedicated session that never warm-
// releases is a session whose durable state is only ever as fresh as its last
// drain.
//
// The arming clause and the release clause are BOTH required. Arming alone is
// satisfied by a releaser that arms a timer and ignores it; releasing alone
// would pass against one that ignored the configured TTL and released on the
// idle edge.
func TestALongWarmTTLDoesNotMakeADedicatedHostDurable(t *testing.T) {
	t.Parallel()

	dedicated := hostWithWarmTTL(t, testSession, dedicatedWarmTTL)
	if dedicated.Placement() != sessionwire.HostPlacementDedicated {
		t.Fatalf("the fixture Host is %q, not dedicated", dedicated.Placement())
	}
	f := newWarmFixture(t, func(o *WarmOptions) { o.TTL = dedicated.WarmTTL() })

	f.releaser.Observe(f.key, WorkStateIdle)
	resets := f.clock.only(t).resetsSeen()
	if len(resets) != 1 || resets[0] != dedicatedWarmTTL {
		t.Fatalf("the timer was armed for %v, want one arming of the dedicated Host's %s", resets, dedicatedWarmTTL)
	}

	f.clock.only(t).expire(t)
	outcome := f.observer.await(t)
	if outcome.Kind != WarmOutcomeReleased {
		t.Fatalf("outcome = %q (%s), want %q: a dedicated Host with a long TTL still warm-releases on whole-session idle", outcome.Kind, outcome.Reason, WarmOutcomeReleased)
	}
	for _, step := range []string{"registry.stop_admitting", "checkpoint", "release_residency", "release_lease", "drop_state"} {
		if f.trace.index(step) < 0 {
			t.Errorf("the release did not run %q; a dedicated session that skips it is one whose durable state is only as fresh as its last drain", step)
		}
	}
}

// TestTheWarmReleaseIsTheSameProtocolAtEitherPlacement is step 2's INBOX and
// RELEASE half, and it is expressible as sameness for a structural reason worth
// recording: WarmOptions takes a TTL, not a Host, so the releaser CANNOT branch
// on placement today. That is the property being locked in. The test is
// vacuously satisfied by the current code and would stop being vacuous the
// moment somebody hands the releaser a Host.
func TestTheWarmReleaseIsTheSameProtocolAtEitherPlacement(t *testing.T) {
	t.Parallel()

	observe := func(fixed sessionwire.SessionID) ([]string, WarmOutcomeKind) {
		built := hostWithWarmTTL(t, fixed, dedicatedWarmTTL)
		f := newWarmFixture(t, func(o *WarmOptions) { o.TTL = built.WarmTTL() })
		f.releaser.Observe(f.key, WorkStateIdle)
		f.clock.only(t).expire(t)
		outcome := f.observer.await(t)
		return f.trace.recorded(), outcome.Kind
	}

	pooledSteps, pooledKind := observe("")
	dedicatedSteps, dedicatedKind := observe(testSession)

	if pooledKind != WarmOutcomeReleased {
		t.Fatalf("the pooled arm did not release: %q", pooledKind)
	}
	if pooledKind != dedicatedKind {
		t.Errorf("the outcomes differ: pooled %q, dedicated %q", pooledKind, dedicatedKind)
	}
	if !reflect.DeepEqual(pooledSteps, dedicatedSteps) {
		t.Errorf("the warm release diverged:\n pooled    %v\n dedicated %v", pooledSteps, dedicatedSteps)
	}
}
