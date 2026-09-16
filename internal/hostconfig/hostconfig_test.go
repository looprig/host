package hostconfig_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
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
)

// ---------------------------------------------------------------------------
// Collaborator stubs
// ---------------------------------------------------------------------------

// Each stub carries an id, so two instances of the SAME type are distinguishable
// by comparison. Four distinct zero-size types would already be comparable, but
// an identity assertion that only works because the types differ is one
// refactor away from being vacuous — and the whole point of these assertions is
// that they survive a refactor of New.

// stubSessionStore is an inert SessionStore that records nothing.
type stubSessionStore struct{ id string }

func (stubSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

// stubWorkspaces is an inert WorkspaceProvider.
type stubWorkspaces struct{ id string }

func (stubWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", nil
}

// stubClock is a fixed time source.
type stubClock struct{ id string }

func (stubClock) Now() time.Time                       { return time.Unix(0, 0) }
func (stubClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// stubAuth is a verifier that accepts everything.
type stubAuth struct{ id string }

func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// stubTarget is a launch target that launches nothing.
type stubTarget struct{}

func (stubTarget) CompatibilityID() department.CompatibilityID { return "rig-2026-09" }

func (stubTarget) Capabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

func (stubTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

func (stubTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

func testDepartment(t *testing.T, agents ...sessionwire.AgentID) *department.Department {
	t.Helper()
	if len(agents) == 0 {
		agents = []sessionwire.AgentID{"reviewer"}
	}
	registrations := make([]department.Registration, 0, len(agents))
	for _, agent := range agents {
		registrations = append(registrations, department.Registration{AgentID: agent, Target: stubTarget{}})
	}
	registry, err := department.New(registrations)
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	return registry
}

// pooledOptions is a fully valid POOLED configuration.
//
// Every duration is a DISTINCT value, and that is deliberate rather than
// decorative: a table in which one side of a comparison is always the same
// constant tests the other side only, and this program has lost six probes to a
// fixture whose value equalled the thing being pinned — one of them in
// production code. No two durations here are equal, so a comparison that reads
// the wrong field cannot accidentally agree.
func pooledOptions(t *testing.T) hostconfig.Options {
	t.Helper()
	return hostconfig.Options{
		HostID:            "host-7c1",
		InternalEndpoint:  "wss://host-7c1.internal.example:8443/hostlink",
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        testDepartment(t),
		SessionStore:      stubSessionStore{id: "store-a"},
		Workspaces:        stubWorkspaces{id: "workspaces-a"},
		Clock:             stubClock{id: "clock-a"},
		Auth:              stubAuth{id: "auth-a"},
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

func dedicatedOptions(t *testing.T) hostconfig.Options {
	t.Helper()
	options := pooledOptions(t)
	options.Placement = sessionwire.HostPlacementDedicated
	options.Capacity = 1
	options.FixedSessionID = "session-71c"
	return options
}

// ---------------------------------------------------------------------------
// The happy paths, first, so every rejection below has a control
// ---------------------------------------------------------------------------

func TestNewAcceptsAValidConfiguration(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		options hostconfig.Options
	}{
		{name: "pooled", options: pooledOptions(t)},
		{name: "dedicated", options: dedicatedOptions(t)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			built, err := hostconfig.New(tt.options)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if built == nil {
				t.Fatal("New returned no Host and no error")
			}
		})
	}
}

// TestEveryEnumMemberIsAccepted is the one-sided-comparison shape in ENUM form.
//
// The fixture only ever used TenantExclusive, so deleting
// HostIsolationClassCrossTenantIsolated from the accept case — refusing every
// cross-tenant-isolated Host outright — left the suite green, while deleting
// TenantExclusive failed loudly across the file. Placement had accepted rows
// for both members; IsolationClass had one for neither.
//
// It is also the mirror of the bug the rule exists to prevent. Refusing the
// zero value turns Core's requirement into a construction-time failure instead
// of a first-advertisement one; REFUSING A VALUE CORE ACCEPTS is the same
// failure pointing the other way, and a maintainer transcribing Core's list
// would ship it green.
func TestEveryEnumMemberIsAccepted(t *testing.T) {
	t.Parallel()

	isolation := []sessionwire.HostIsolationClass{
		sessionwire.HostIsolationClassCrossTenantIsolated,
		sessionwire.HostIsolationClassTenantExclusive,
	}
	for _, class := range isolation {
		t.Run("isolation "+string(class), func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.IsolationClass = class
			built, err := hostconfig.New(options)
			if err != nil {
				t.Fatalf("New with isolation class %q = %v, want acceptance: Core accepts it, so refusing it here is a Host that cannot be built for a configuration the wire permits", class, err)
			}
			if got := built.IsolationClass(); got != class {
				t.Errorf("IsolationClass() = %q, want %q", got, class)
			}
		})
	}

	placements := []sessionwire.HostPlacement{
		sessionwire.HostPlacementPooled,
		sessionwire.HostPlacementDedicated,
	}
	for _, placement := range placements {
		t.Run("placement "+string(placement), func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.Placement = placement
			if placement == sessionwire.HostPlacementDedicated {
				options.Capacity = 1
				options.FixedSessionID = "session-71c"
			}
			if _, err := hostconfig.New(options); err != nil {
				t.Fatalf("New with placement %q = %v, want acceptance", placement, err)
			}
		})
	}

	// This floor detects someone SHORTENING the local slices, and nothing else.
	// It cannot detect Core adding a member, because the number it compares
	// against is the number it is derived from — Go exposes no way to enumerate
	// a package's constants, so no test in this module can notice a third
	// isolation class appearing upstream. Written down rather than removed,
	// because a reader would otherwise take it for the guard it resembles: when
	// Core's enums change, THIS TABLE IS UPDATED BY HAND OR NOT AT ALL.
	if len(isolation) != 2 || len(placements) != 2 {
		t.Fatal("a row was removed from this table; every member Core accepts needs an accepted row here")
	}
}

// TestIdentifiersHaveNoUndocumentedMinimumLength is the third instance of the
// class, and the one my own audit stopped short of.
//
// The rule for an identifier is NON-EMPTY. Replacing `o.HostID == ""` with
// `len(o.HostID) < 4` left the suite green, because the boundary was pinned by
// nothing except the fixture's own length — "host-7c1" is eight characters, so
// a floor of nine fails 79 assertions and a floor of four fails none. The
// reasoning in TestDurationsHaveNoUndocumentedMinimum applies verbatim: any
// floor above non-empty is a new rule owing its own constant and its own
// boundary rows, and Core's own identity rule bounds only the upper end.
func TestIdentifiersHaveNoUndocumentedMinimumLength(t *testing.T) {
	t.Parallel()

	t.Run("single-character pooled identifiers", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t)
		options.HostID = "h"
		built, err := hostconfig.New(options)
		if err != nil {
			t.Fatalf("New with single-character identifiers = %v, want acceptance: the rule is non-empty", err)
		}
		if built.ID() != "h" {
			t.Errorf("resolved %q, want h", built.ID())
		}
	})

	t.Run("single-character dedicated binding", func(t *testing.T) {
		t.Parallel()
		options := dedicatedOptions(t)
		options.HostID = "h"
		options.FixedSessionID = "s"
		built, err := hostconfig.New(options)
		if err != nil {
			t.Fatalf("New with a single-character FixedSessionID = %v, want acceptance", err)
		}
		if built.FixedSessionID() != "s" {
			t.Errorf("FixedSessionID() = %q, want s", built.FixedSessionID())
		}
	})

	// The other side stays closed at that scale, so these rows do not soften
	// the emptiness rule they are bounding.
	for name, spoil := range map[string]func(*hostconfig.Options){
		"HostID": func(o *hostconfig.Options) { o.HostID = "" },
	} {
		options := pooledOptions(t)
		options.HostID = "h"
		spoil(&options)
		if _, err := hostconfig.New(options); err == nil {
			t.Errorf("an empty %s was accepted", name)
		}
	}
}

// TestIdentifiersEnforceCoresIdentityRule closes the divergence the ceiling
// sweep turned up: Host checked only non-empty while Core bounds these three
// fields at MaxIDBytes and rejects invalid UTF-8 besides.
//
// It is the IsolationClass ruling on three more fields. Core calls
// validateHostLinkHost from SIX sites in hostlink.go — bind, unbind, capacity
// report, registry observation, and BOTH DRAIN PATHS — so an over-long HostID
// produced a Host that could neither advertise nor drain. Not merely "fails at
// first advertisement": it cannot shut down cleanly either.
//
// The rule is DELEGATED rather than restated: options.go calls Core's own
// exported Validate on each identity, so there is no copy to drift. That is why
// these rows assert Core's CODES rather than a message — a caller must be able
// to tell too_long from invalid_utf8, which is the whole reason Core documents
// IDValidationError.Code as stable.
func TestIdentifiersEnforceCoresIdentityRule(t *testing.T) {
	t.Parallel()

	overLong := strings.Repeat("x", sessionwire.MaxIDBytes+1)
	badUTF8 := "host-\xff\xfe"

	tests := []struct {
		name  string
		spoil func(*hostconfig.Options)
		field string
		code  sessionwire.IDValidationCode
	}{
		{name: "host id too long", spoil: func(o *hostconfig.Options) { o.HostID = sessionwire.HostID(overLong) }, field: "HostID", code: sessionwire.IDValidationCodeTooLong},
		{name: "host id invalid utf8", spoil: func(o *hostconfig.Options) { o.HostID = sessionwire.HostID(badUTF8) }, field: "HostID", code: sessionwire.IDValidationCodeInvalidUTF8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			tt.spoil(&options)
			built, err := hostconfig.New(options)
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}

			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Field != tt.field {
				t.Errorf("Field = %q, want %q", invalid.Field, tt.field)
			}
			if invalid.Code != hostconfig.OptionErrorCodeInvalid {
				t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeInvalid)
			}

			// The TYPED CAUSE, reachable through the exported constructor. An
			// error carrying Core's sentence but not Core's type forces a
			// caller to substring-match text Core is free to reword, which is
			// exactly what the stable Code exists to prevent.
			var validation *sessionwire.IDValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("errors.As did not reach *sessionwire.IDValidationError from New's error")
			}
			if validation.Code != tt.code {
				t.Errorf("IDValidationError.Code = %q, want %q", validation.Code, tt.code)
			}
		})
	}

	// FixedSessionID takes the same rule in the dedicated branch, where it is
	// the one placement that requires it to be present at all.
	for _, tt := range []struct {
		name  string
		value sessionwire.SessionID
		code  sessionwire.IDValidationCode
	}{
		{name: "fixed session id too long", value: sessionwire.SessionID(overLong), code: sessionwire.IDValidationCodeTooLong},
		{name: "fixed session id invalid utf8", value: sessionwire.SessionID("session-\xff"), code: sessionwire.IDValidationCodeInvalidUTF8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := dedicatedOptions(t)
			options.FixedSessionID = tt.value
			_, err := hostconfig.New(options)
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Field != "FixedSessionID" {
				t.Errorf("Field = %q, want FixedSessionID", invalid.Field)
			}
			var validation *sessionwire.IDValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("errors.As did not reach *sessionwire.IDValidationError")
			}
			if validation.Code != tt.code {
				t.Errorf("IDValidationError.Code = %q, want %q", validation.Code, tt.code)
			}
		})
	}

	// The OTHER DIRECTION, which is the lesson of this round: exactly
	// MaxIDBytes must be ACCEPTED. Without it the rule is one-sided again and a
	// bound one byte too tight would pass. TestGenerousButLegalConfigurationIsAccepted
	// carries the same values and now pins this boundary rather than merely
	// being generous; this row states the intent locally so deleting that test
	// does not silently remove the accepted side.
	t.Run("exactly MaxIDBytes is accepted", func(t *testing.T) {
		t.Parallel()
		atLimit := strings.Repeat("x", sessionwire.MaxIDBytes)
		options := dedicatedOptions(t)
		options.HostID = sessionwire.HostID(atLimit)
		options.FixedSessionID = sessionwire.SessionID(atLimit)
		built, err := hostconfig.New(options)
		if err != nil {
			t.Fatalf("New with identifiers of exactly MaxIDBytes = %v, want acceptance: Core accepts them, so refusing them here is a Host that cannot be built for a configuration the wire permits", err)
		}
		if len(built.ID()) != sessionwire.MaxIDBytes {
			t.Errorf("ID() has %d bytes, want %d", len(built.ID()), sessionwire.MaxIDBytes)
		}
	})
}

// TestIdentityRulesAreDelegatedNotRestated pins the MECHANISM, because the
// mechanism is what the comment claims.
//
// options.go asserts, at length, that it "enforces Core's rule BY CALLING IT
// and cannot drift if Core changes it". The calling is the entire argument, and
// it was unpinned: a hand restatement that is FAITHFUL — all three arms, the
// bound derived from sessionwire.MaxIDBytes, utf8.ValidString, returning Core's
// own *IDValidationError with Core's own codes — passes every behavioural test
// in this file. Q4 died only because the restatement I wrote was wrong.
//
// The live exposure is therefore one step earlier than "Core drifts". It is that
// the delegation quietly STOPS BEING a delegation: a maintainer inlining the
// check during a refactor, or deleting what reads as needless indirection,
// produces a copy that is green today and diverges the next time Core moves —
// surfacing as a Host that can neither advertise nor drain, which is the exact
// failure this delegation exists to prevent.
//
// One commit ago I added TestEachBoundExplainsItself because a stated
// expectation nothing checked is the shape this repository treats as a defect
// in its own right. A twenty-five line comment asserting delegation, with no
// assertion that delegation happens, is that shape.
//
// So: options.go must CALL Validate on each identity, and must not name Core's
// internals directly. Detection is by exclusion rather than by recognising a
// good restatement, because there is no way to tell a faithful copy from a
// delegation by looking at what it computes — only by looking at whether it
// delegates.
func TestIdentityRulesAreDelegatedNotRestated(t *testing.T) {
	t.Parallel()

	parsed, err := parser.ParseFile(token.NewFileSet(), "options.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse options.go: %v", err)
	}

	delegated := map[string]bool{}
	var borrowed []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// o.<Identity>.Validate, whether called or taken as a method value.
		if selector.Sel.Name == "Validate" {
			if field, ok := selector.X.(*ast.SelectorExpr); ok {
				delegated[field.Sel.Name] = true
			}
		}
		// sessionwire.MaxIDBytes, utf8.ValidString, IDValidationCode*: naming
		// any of these in production code means the rule is being recomputed
		// here rather than asked for.
		if pkg, ok := selector.X.(*ast.Ident); ok {
			switch {
			case pkg.Name == "utf8":
				borrowed = append(borrowed, "utf8."+selector.Sel.Name)
			case selector.Sel.Name == "MaxIDBytes",
				strings.HasPrefix(selector.Sel.Name, "IDValidationCode"):
				borrowed = append(borrowed, pkg.Name+"."+selector.Sel.Name)
			}
		}
		return true
	})

	// TenantID LEFT THIS LIST AT H8 RATHER THAN BEING DROPPED FROM IT. Host is
	// no longer constructed with a tenant, so there is no o.TenantID for
	// options.go to delegate on; the identity rule moved to
	// residency.Manager.validateRequest, which calls request.TenantID.Validate
	// on the tenant each attach names, and TestAttachValidatesTheTenantIdentity
	// in that package is this row's successor.
	for _, identity := range []string{"HostID", "FixedSessionID"} {
		if !delegated[identity] {
			t.Errorf("options.go never calls Validate on o.%s. Core's rule must be ASKED FOR: a faithful restatement passes every behavioural test in this file and diverges silently the next time Core moves. Note this guard closes REPLACEMENT, not ADDITION — a redundant check alongside the call names none of the banned symbols and passes", identity)
		}
	}
	for _, name := range borrowed {
		t.Errorf("options.go names %s directly. That is Core's rule being recomputed here; call the identity's own Validate instead", name)
	}

	// Floored: if the walk found no delegation at all the loop above would have
	// reported three failures, but a walk that visited nothing would report
	// them for the wrong reason.
	if len(parsed.Decls) == 0 {
		t.Fatal("options.go parsed to no declarations; this guard would be vacuous")
	}
}

// TestNoCollaboratorIsInvokedAtConstruction converts a property I verified once
// by hand into one the suite holds.
//
// Nothing calls a collaborator during New today, and the four ordinary stubs
// cannot show that: an invented LoadSession or VerifyTenant call SURVIVES,
// because stubSessionStore returns (nil, nil) and stubAuth returns nil. Two
// other invented calls die, but only by accident of what their stubs return —
// and a kill that depends on a fixture's return value is the same false
// all-clear that produced the Capacity mistake, one layer down.
//
// Auth is the one that matters. A New that acquires a VerifyTenant call becomes
// network- and order-dependent, and a Host that fails to construct because an
// auth service is briefly unreachable is a different component from the one
// documented here.
//
// The stubs RECORD rather than panic. A panicking stub kills these mutants too,
// but by crashing, and this repository does not count a crash as a kill by
// assertion — the recorded flag gives the same detection with a failure that
// names the method and survives -race cleanly.
func TestNoCollaboratorIsInvokedAtConstruction(t *testing.T) {
	t.Parallel()

	invoked := &invocationLog{}
	options := pooledOptions(t)
	options.SessionStore = recordingStore{log: invoked}
	options.Workspaces = recordingWorkspaces{log: invoked}
	options.Clock = recordingClock{log: invoked}
	options.Auth = recordingAuth{log: invoked}

	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New = %v; construction must not depend on a collaborator", err)
	}
	if called := invoked.calls(); len(called) != 0 {
		t.Errorf("hostconfig.New invoked %v. Construction must be pure: a New that calls Auth or SessionStore becomes network- and order-dependent, and a Host that cannot be built while a dependency is briefly unreachable is a different component from the one documented", called)
	}

	// The collaborators are STORED, not discarded — otherwise this would pass
	// against a New that dropped them on the floor and called nothing.
	if built.Auth() != options.Auth || built.SessionStore() != options.SessionStore {
		t.Error("a collaborator did not survive construction")
	}

	// And the log itself works, so "no calls" is a measurement rather than a
	// stub that records nothing.
	if err := built.Auth().VerifyTenant(t.Context(), "tenant-9f3", ""); err != nil {
		t.Fatalf("VerifyTenant: %v", err)
	}
	if called := invoked.calls(); !slices.Contains(called, "Auth.VerifyTenant") {
		t.Errorf("the invocation log recorded %v after a deliberate call; it cannot detect what it does not record", called)
	}
}

// invocationLog records which collaborator methods were called.
type invocationLog struct {
	mu    sync.Mutex
	names []string
}

// record notes one invocation.
func (l *invocationLog) record(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.names = append(l.names, name)
}

// calls returns every recorded invocation, in order.
func (l *invocationLog) calls() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.names...)
}

// recordingStore is a SessionStore that notes every call.
type recordingStore struct{ log *invocationLog }

// LoadSession records the call and succeeds.
func (s recordingStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	s.log.record("SessionStore.LoadSession")
	return nil, nil
}

// recordingWorkspaces is a WorkspaceProvider that notes every call.
type recordingWorkspaces struct{ log *invocationLog }

// EnsureWorkspace records the call and succeeds.
func (w recordingWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	w.log.record("Workspaces.EnsureWorkspace")
	return "/tmp/workspace", nil
}

// recordingClock is a Clock that notes every call.
type recordingClock struct{ log *invocationLog }

// Now records the call and returns a fixed instant.
func (c recordingClock) Now() time.Time {
	c.log.record("Clock.Now")
	return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
}

// NewTimer records the call and returns a real timer.
func (c recordingClock) NewTimer(d time.Duration) *time.Timer {
	c.log.record("Clock.NewTimer")
	return time.NewTimer(d)
}

// recordingAuth is an AuthVerifier that notes every call.
type recordingAuth struct{ log *invocationLog }

// VerifyTenant records the call and succeeds.
func (a recordingAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error {
	a.log.record("Auth.VerifyTenant")
	return nil
}

// TestDepartmentCardinalityIsNotConstrained is the same survivor shape one field
// over, and the one my ceiling sweep missed.
//
// That sweep covered every SCALAR option and stopped at the only reference-typed
// required dependency. Every Department in this file was one registration of one
// agent, so adding `if o.Department.Len() != 1` to validatePresence left the
// whole suite green — a Host that silently refused every multi-agent Department,
// with nothing to notice.
//
// It matters for what is next rather than for what is here: O2.2 and O2.3 derive
// one advertisement per LaunchTarget, so a cardinality assumption baked in at
// New would stay invisible until a two-agent Department met a real deployment.
// There is no such rule and there should not be one; this row is what says so.
func TestDepartmentCardinalityIsNotConstrained(t *testing.T) {
	t.Parallel()

	for _, agents := range [][]sessionwire.AgentID{
		{"reviewer"},
		{"reviewer", "planner"},
		{"reviewer", "planner", "summariser", "critic"},
	} {
		t.Run(strconv.Itoa(len(agents))+" agents", func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.Department = testDepartment(t, agents...)
			built, err := hostconfig.New(options)
			if err != nil {
				t.Fatalf("New with a %d-agent Department = %v, want acceptance: Host constrains the Department's CONTENTS nowhere and its SIZE nowhere either", len(agents), err)
			}
			if got := built.Department().Len(); got != len(agents) {
				t.Errorf("Department().Len() = %d, want %d", got, len(agents))
			}
			// The registry survives construction intact, not merely by count.
			for _, agent := range agents {
				if _, err := built.Department().Target(agent); err != nil {
					t.Errorf("Target(%q) after construction: %v", agent, err)
				}
			}
		})
	}
}

// TestGenerousButLegalConfigurationIsAccepted is
// TestDurationsHaveNoUndocumentedMinimum at the other end, and it exists
// because a sweep found the same shape in every remaining field: EVERY ONE IS
// FLOORED BELOW AND OPEN ABOVE, so an invented ceiling survives.
//
// Measured, each surviving on its own: a 64-byte cap on the three identifiers,
// a 1000 cap on Capacity, a seven-day cap on WarmTTL and ReconcileInterval, and
// an InternalEndpoint required to end in "/hostlink". Only the heartbeat and
// claim ceilings died, and incidentally — the overflow rows feed huge values
// and expect a margin code.
//
// Two of those are worth naming rather than counting.
//
// CAPACITY WAS PROBED LAST ROUND AND I REPORTED THE WRONG CONCLUSION. The
// commit message for 75d26d1 records an undocumented Capacity ceiling as
// "already covered by the large-capacity dedicated row". It is covered only up
// to 64, the largest fixture: a ceiling of 32 dies and 1000 survives. The
// value-equals-fixture trap produced a false all-clear inside a probe result I
// published. Core imposes no capacity ceiling at all — it rejects only
// dedicated placement above one — so any host-side ceiling is invention.
//
// INTERNALENDPOINT IS THE ENUM BUG ON THE OTHER CORE-DELEGATED FIELD.
// TestEveryEnumMemberIsAccepted exists because refusing a value Core accepts is
// as much a bug as accepting one it refuses; exactly one endpoint string was
// ever accepted anywhere in this suite, and the "/hostlink" narrowing survived
// SPECIFICALLY BECAUSE all three endpoint fixtures, valid and invalid, share
// that suffix. The generalisation landed on the enums and stopped at the field
// with identical structure. The second endpoint below differs in scheme, host
// form, port and path so that nothing about the first is load-bearing.
func TestGenerousButLegalConfigurationIsAccepted(t *testing.T) {
	t.Parallel()

	// 256 bytes is Core's own identity bound, so this is the largest identifier
	// that can reach the wire — and since Host now delegates to Core's
	// validators, it is also the largest Host will construct with. This row is
	// therefore the ACCEPTED SIDE of that bound, not merely a generous value.
	// (An earlier version of this comment called Host's non-enforcement "a
	// separate question"; it was answered two commits later and the comment was
	// left contradicting the code below it.)
	long := sessionwire.HostID(strings.Repeat("h", sessionwire.MaxIDBytes))
	longSession := sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes))

	t.Run("pooled, generous", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t)
		// GENEROUS IN THE DEPARTMENT TOO. Without this the row is generous in
		// every field except the one reference-typed dependency it carries, and
		// an invented Department ceiling above the largest fixture is invisible
		// — Len() > 4 and Len() > 8 both survived, because the cardinality test
		// stops at four agents. It is the fixture-pinned ceiling I diagnosed
		// and fixed for the scalars, recurring one level up.
		//
		// THIS ROW MITIGATES; IT DOES NOT CLOSE. The identifier rows above ARE
		// closed, because MaxIDBytes is a real upstream bound and the fixture
		// sits exactly on it — no ceiling can hide above a value Core itself
		// refuses to exceed. Department size and Capacity have NO upstream
		// bound, so any finite fixture leaves a higher ceiling invisible: this
		// dies at 32 and survives at 100. Sixty-four and one million are
		// arguments from implausibility, not proofs, and a later reader should
		// not take them for the same kind of assurance.
		crowd := make([]sessionwire.AgentID, 0, 64)
		for i := range 64 {
			crowd = append(crowd, sessionwire.AgentID("agent-"+strconv.Itoa(i)))
		}
		options.Department = testDepartment(t, crowd...)
		options.HostID = long
		// A second, structurally different endpoint: ws rather than wss, an
		// address literal rather than a name, an explicit port, and a path that
		// is not "/hostlink".
		options.InternalEndpoint = "ws://10.0.4.7:9000/link"
		options.IsolationClass = sessionwire.HostIsolationClassCrossTenantIsolated
		options.Capacity = 1_000_000
		options.WarmTTL = 30 * 24 * time.Hour
		options.ReconcileInterval = 7 * 24 * time.Hour
		options.RegistryHeartbeat = 7 * 24 * time.Hour
		options.RegistryExpiry = 30 * 24 * time.Hour
		options.ClaimTTL = 7 * 24 * time.Hour
		options.ApplyDeadline = 30 * 24 * time.Hour

		built, err := hostconfig.New(options)
		if err != nil {
			t.Fatalf("New with a generous but legal configuration = %v. Every rule here is a floor; a ceiling above it is an undocumented rule owing its own constant and its own boundary rows", err)
		}
		if built.ID() != long {
			t.Error("a long identifier did not survive construction")
		}
		if built.Capacity() != 1_000_000 {
			t.Errorf("Capacity() = %d, want 1000000", built.Capacity())
		}
		if built.InternalEndpoint() != "ws://10.0.4.7:9000/link" {
			t.Errorf("InternalEndpoint() = %q", built.InternalEndpoint())
		}
		if got := built.Department().Len(); got != len(crowd) {
			t.Errorf("Department().Len() = %d, want %d", got, len(crowd))
		}
	})

	t.Run("dedicated, generous", func(t *testing.T) {
		t.Parallel()
		options := dedicatedOptions(t)
		options.HostID = long
		options.FixedSessionID = longSession
		options.InternalEndpoint = "ws://[2001:db8::1]:9000/"
		options.WarmTTL = 30 * 24 * time.Hour
		if built, err := hostconfig.New(options); err != nil {
			t.Fatalf("New with a generous dedicated configuration = %v", err)
		} else if built.FixedSessionID() != longSession {
			t.Error("a long FixedSessionID did not survive construction")
		}
	})

	// The controls: the ceilings really are absent rather than merely
	// unreached, so each accepted value above is one an invented ceiling would
	// have refused.
	if sessionwire.MaxIDBytes <= 64 {
		t.Fatalf("MaxIDBytes = %d, so the long identifiers are not longer than the 64-byte ceiling that survived", sessionwire.MaxIDBytes)
	}
}

// TestTimingMarginsDoNotOverflow holds that the two relationships fail CLOSED on
// a duration large enough to overflow their product.
//
// Computed as a product, both failed OPEN: a ClaimTTL above MaxInt64/2 wraps
// negative, `deadline < negative` is false, and a one-second deadline under a
// 146-year claim was ACCEPTED. Nobody configures that, but this constructor's
// entire job is refusing incoherent configurations and it refused in the wrong
// direction on the one input class it could not represent.
func TestTimingMarginsDoNotOverflow(t *testing.T) {
	t.Parallel()

	t.Run("claim", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t)
		options.ClaimTTL = time.Duration(math.MaxInt64/hostconfig.MinClaimAttemptsBeforeDeadline) + 1
		options.ApplyDeadline = time.Second
		var invalid *hostconfig.InvalidOptionsError
		if _, err := hostconfig.New(options); !errors.As(err, &invalid) {
			t.Fatalf("New with an overflowing ClaimTTL = %v, want *InvalidOptionsError", err)
		} else if invalid.Code != hostconfig.OptionErrorCodeClaimMargin {
			t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeClaimMargin)
		}
	})

	t.Run("heartbeat", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t)
		options.RegistryHeartbeat = time.Duration(math.MaxInt64/hostconfig.MinHeartbeatsBeforeExpiry) + 1
		options.RegistryExpiry = time.Second
		var invalid *hostconfig.InvalidOptionsError
		if _, err := hostconfig.New(options); !errors.As(err, &invalid) {
			t.Fatalf("New with an overflowing RegistryHeartbeat = %v, want *InvalidOptionsError", err)
		} else if invalid.Code != hostconfig.OptionErrorCodeHeartbeatMargin {
			t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeHeartbeatMargin)
		}
	})

	// The control: a large duration on the OTHER side, where no overflow
	// occurs, is still accepted. Otherwise the two rows above would be
	// satisfied by a constructor that refuses anything large.
	t.Run("control: a large expiry is fine", func(t *testing.T) {
		t.Parallel()
		options := pooledOptions(t)
		options.RegistryHeartbeat = time.Hour
		options.RegistryExpiry = 24 * time.Hour
		if _, err := hostconfig.New(options); err != nil {
			t.Fatalf("New with an hourly heartbeat under a daily expiry = %v, want acceptance", err)
		}
	})
}

// TestResolvedConfigurationReportsWhatItWasGiven is the accessor half.
//
// It is separate from the validation tests on purpose. O1.2 shipped an accessor
// and a comparison that read the same field and were tested through only one of
// them, so a constant returned by the accessor survived the whole suite. Here
// the accessors are read directly, and every value asserted differs from every
// other, so no single hardcoded return can satisfy two of them.
func TestResolvedConfigurationReportsWhatItWasGiven(t *testing.T) {
	t.Parallel()

	options := dedicatedOptions(t)
	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := built.ID(); got != options.HostID {
		t.Errorf("ID() = %q, want %q", got, options.HostID)
	}
	if got := built.InternalEndpoint(); got != options.InternalEndpoint {
		t.Errorf("InternalEndpoint() = %q, want %q", got, options.InternalEndpoint)
	}
	if got := built.IsolationClass(); got != options.IsolationClass {
		t.Errorf("IsolationClass() = %q, want %q", got, options.IsolationClass)
	}
	if got := built.Placement(); got != options.Placement {
		t.Errorf("Placement() = %q, want %q", got, options.Placement)
	}
	if got := built.Capacity(); got != options.Capacity {
		t.Errorf("Capacity() = %d, want %d", got, options.Capacity)
	}
	if got := built.FixedSessionID(); got != options.FixedSessionID {
		t.Errorf("FixedSessionID() = %q, want %q", got, options.FixedSessionID)
	}

	// Every duration accessor, checked against a value distinct from all the
	// others, so a getter returning the wrong field is caught rather than
	// coinciding.
	durations := map[string]struct {
		got  time.Duration
		want time.Duration
	}{
		"WarmTTL":           {built.WarmTTL(), options.WarmTTL},
		"RegistryHeartbeat": {built.RegistryHeartbeat(), options.RegistryHeartbeat},
		"RegistryExpiry":    {built.RegistryExpiry(), options.RegistryExpiry},
		"ClaimTTL":          {built.ClaimTTL(), options.ClaimTTL},
		"ApplyDeadline":     {built.ApplyDeadline(), options.ApplyDeadline},
		"ReconcileInterval": {built.ReconcileInterval(), options.ReconcileInterval},
	}
	seen := map[time.Duration]string{}
	for name, pair := range durations {
		if pair.got != pair.want {
			t.Errorf("%s() = %v, want %v", name, pair.got, pair.want)
		}
		if other, duplicate := seen[pair.want]; duplicate {
			t.Errorf("%s and %s share the fixture value %v, so a getter reading the wrong field would not be caught", name, other, pair.want)
		}
		seen[pair.want] = name
	}
	if got := built.CommandQueueSize(); got != options.CommandQueueSize {
		t.Errorf("CommandQueueSize() = %d, want %d", got, options.CommandQueueSize)
	}
	if got := built.ReconcileBatch(); got != options.ReconcileBatch {
		t.Errorf("ReconcileBatch() = %d, want %d", got, options.ReconcileBatch)
	}
	if built.Department() != options.Department {
		t.Error("Department() did not return the registry it was given")
	}

	// IDENTITY, not nilness. The previous assertion here checked only that the
	// four collaborators were non-nil, and substituting always-permissive
	// replacements for SessionStore and Auth inside New left the whole suite
	// green — while the identical substitution for Department died immediately,
	// because Department had an identity assertion and these four did not.
	//
	// The Auth case is the one that matters: a Host answering VerifyTenant with
	// something other than the verifier its caller supplied is a tenant
	// isolation bypass, and nothing in this module would have noticed. A
	// refactor of New that wraps, decorates or defaults a field is exactly how
	// that arrives, and it arrives looking like tidying.
	//
	// This is also the general rule, and it runs opposite to the intuition that
	// a validated field needs the stronger test: ClaimTTL() returning the wrong
	// field dies in the margin test, because a value that feeds a comparison
	// gets a second witness for free. A field with NO validation rule has no
	// such witness, so its accessor is the only one there is.
	for _, collaborator := range []struct {
		name string
		got  any
		want any
	}{
		{"SessionStore", built.SessionStore(), options.SessionStore},
		{"Workspaces", built.Workspaces(), options.Workspaces},
		{"Clock", built.Clock(), options.Clock},
		{"Auth", built.Auth(), options.Auth},
	} {
		if collaborator.got == nil {
			t.Errorf("%s() returned nil", collaborator.name)
			continue
		}
		if collaborator.got != collaborator.want {
			t.Errorf("%s() = %#v, want the instance it was constructed with, %#v", collaborator.name, collaborator.got, collaborator.want)
		}
	}
}

// TestCollaboratorsAreNotSubstitutedForOneAnother proves the identity assertions
// above distinguish two instances of the SAME type, not merely two types.
func TestCollaboratorsAreNotSubstitutedForOneAnother(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t)
	options.Auth = stubAuth{id: "auth-supplied"}
	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if built.Auth() == (stubAuth{id: "auth-other"}) {
		t.Fatal("two stubAuth values with different ids compare equal, so the identity assertions above cannot see a substitution within a type")
	}
	if built.Auth() != (stubAuth{id: "auth-supplied"}) {
		t.Errorf("Auth() = %#v, want the supplied verifier", built.Auth())
	}
}

// TestHostDoesNotAliasTheCallersOptions is the immutability probe.
func TestHostDoesNotAliasTheCallersOptions(t *testing.T) {
	t.Parallel()

	options := dedicatedOptions(t)
	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	options.HostID = "impostor"
	options.Capacity = 99
	options.RegistryHeartbeat = time.Hour
	options.FixedSessionID = "session-substituted"

	if built.ID() != "host-7c1" || built.Capacity() != 1 ||
		built.RegistryHeartbeat() != 5*time.Second || built.FixedSessionID() != "session-71c" {
		t.Error("mutating the caller's Options after New changed the Host; the configuration is not resolved")
	}
}

// ---------------------------------------------------------------------------
// Required options
// ---------------------------------------------------------------------------

func TestNewRejectsAMissingOrInvalidOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spoil func(*hostconfig.Options)
		field string
		code  hostconfig.OptionErrorCode
	}{
		{name: "host id", spoil: func(o *hostconfig.Options) { o.HostID = "" }, field: "HostID", code: hostconfig.OptionErrorCodeMissing},
		{name: "internal endpoint absent", spoil: func(o *hostconfig.Options) { o.InternalEndpoint = "" }, field: "InternalEndpoint", code: hostconfig.OptionErrorCodeMissing},
		{name: "internal endpoint malformed", spoil: func(o *hostconfig.Options) { o.InternalEndpoint = "http://host/hostlink" }, field: "InternalEndpoint", code: hostconfig.OptionErrorCodeInvalid},
		{name: "isolation class unknown", spoil: func(o *hostconfig.Options) { o.IsolationClass = "shared" }, field: "IsolationClass", code: hostconfig.OptionErrorCodeUnknownEnum},
		{name: "isolation class absent", spoil: func(o *hostconfig.Options) { o.IsolationClass = "" }, field: "IsolationClass", code: hostconfig.OptionErrorCodeUnknownEnum},
		{name: "department", spoil: func(o *hostconfig.Options) { o.Department = nil }, field: "Department", code: hostconfig.OptionErrorCodeMissing},
		{name: "session store", spoil: func(o *hostconfig.Options) { o.SessionStore = nil }, field: "SessionStore", code: hostconfig.OptionErrorCodeMissing},
		{name: "workspaces", spoil: func(o *hostconfig.Options) { o.Workspaces = nil }, field: "Workspaces", code: hostconfig.OptionErrorCodeMissing},
		{name: "clock", spoil: func(o *hostconfig.Options) { o.Clock = nil }, field: "Clock", code: hostconfig.OptionErrorCodeMissing},
		{name: "auth", spoil: func(o *hostconfig.Options) { o.Auth = nil }, field: "Auth", code: hostconfig.OptionErrorCodeMissing},
		{name: "placement unknown", spoil: func(o *hostconfig.Options) { o.Placement = "burst" }, field: "Placement", code: hostconfig.OptionErrorCodeUnknownEnum},
		{name: "placement absent", spoil: func(o *hostconfig.Options) { o.Placement = "" }, field: "Placement", code: hostconfig.OptionErrorCodeUnknownEnum},
		{name: "capacity zero", spoil: func(o *hostconfig.Options) { o.Capacity = 0 }, field: "Capacity", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "warm ttl zero", spoil: func(o *hostconfig.Options) { o.WarmTTL = 0 }, field: "WarmTTL", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "warm ttl negative", spoil: func(o *hostconfig.Options) { o.WarmTTL = -time.Second }, field: "WarmTTL", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "heartbeat zero", spoil: func(o *hostconfig.Options) { o.RegistryHeartbeat = 0 }, field: "RegistryHeartbeat", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "expiry zero", spoil: func(o *hostconfig.Options) { o.RegistryExpiry = 0 }, field: "RegistryExpiry", code: hostconfig.OptionErrorCodeNotPositive},
		// The two DIVIDENDS, negative. This is the only input class where
		// validateTiming's division and the product it replaced disagree, and
		// the division is the more permissive of the two — so what makes it
		// safe is that validateShape refuses these before validateTiming ever
		// sees them. The table had a negative row only for WarmTTL, and zero is
		// safe for the division, so the class was untested.
		{name: "expiry negative", spoil: func(o *hostconfig.Options) { o.RegistryExpiry = -time.Second }, field: "RegistryExpiry", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "apply deadline negative", spoil: func(o *hostconfig.Options) { o.ApplyDeadline = -time.Second }, field: "ApplyDeadline", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "heartbeat negative", spoil: func(o *hostconfig.Options) { o.RegistryHeartbeat = -time.Second }, field: "RegistryHeartbeat", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "claim ttl negative", spoil: func(o *hostconfig.Options) { o.ClaimTTL = -time.Second }, field: "ClaimTTL", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "claim ttl zero", spoil: func(o *hostconfig.Options) { o.ClaimTTL = 0 }, field: "ClaimTTL", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "apply deadline zero", spoil: func(o *hostconfig.Options) { o.ApplyDeadline = 0 }, field: "ApplyDeadline", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "queue size zero", spoil: func(o *hostconfig.Options) { o.CommandQueueSize = 0 }, field: "CommandQueueSize", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "queue size negative", spoil: func(o *hostconfig.Options) { o.CommandQueueSize = -1 }, field: "CommandQueueSize", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "queue size above bound", spoil: func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxCommandQueueSize + 1 }, field: "CommandQueueSize", code: hostconfig.OptionErrorCodeAboveBound},
		{name: "reconcile interval zero", spoil: func(o *hostconfig.Options) { o.ReconcileInterval = 0 }, field: "ReconcileInterval", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "reconcile batch zero", spoil: func(o *hostconfig.Options) { o.ReconcileBatch = 0 }, field: "ReconcileBatch", code: hostconfig.OptionErrorCodeNotPositive},
		{name: "reconcile batch above bound", spoil: func(o *hostconfig.Options) { o.ReconcileBatch = hostconfig.MaxReconcileBatch + 1 }, field: "ReconcileBatch", code: hostconfig.OptionErrorCodeAboveBound},
	}

	if len(tests) == 0 {
		t.Fatal("no option rule is exercised")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			tt.spoil(&options)
			built, err := hostconfig.New(options)
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Field != tt.field {
				t.Errorf("Field = %q, want %q", invalid.Field, tt.field)
			}
			if invalid.Code != tt.code {
				t.Errorf("Code = %q, want %q", invalid.Code, tt.code)
			}
			if invalid.Reason == "" {
				t.Error("Reason is empty; the code is for branching and the reason is for the person reading the failure")
			}
		})
	}
}

// TestEveryRuleIsReachableThroughTheExportedConstructor holds that a caller can
// discriminate the refusals BELOW, which is the property O1.1 shipped without:
// an exported error type no consumer could match is a type that does not exist.
//
// READ THE NAME NARROWLY. Ten rules are covered and THREE ARE DELIBERATELY
// EXCLUDED: the two identity delegations (HostID, FixedSessionID)
// and, with them, the endpoint rule they would collide with. All three report
// OptionErrorCodeInvalid, so adding a spoiler for any of them would fail the
// distinctness assertion — and the correct response then is NOT to relax the
// assertion but to split the code.
//
// The exclusion is safe today because those three are discriminated by two
// routes this test does not use: the Field, and a typed cause of a different
// type — Core's *RequestValidationError for the endpoint against its
// *IDValidationError for the identities, both asserted in
// TestInvalidEndpointCarriesCoresTypedCause and
// TestIdentifiersEnforceCoresIdentityRule. Written down because a reader takes
// "every rule" at face value, believes a property that is not held, and
// discovers the collision by weakening the wrong thing.
func TestEveryRuleIsReachableThroughTheExportedConstructor(t *testing.T) {
	t.Parallel()

	spoilers := []func(*hostconfig.Options){
		func(o *hostconfig.Options) { o.HostID = "" },
		func(o *hostconfig.Options) { o.InternalEndpoint = "http://host/x" },
		func(o *hostconfig.Options) { o.IsolationClass = "shared" },
		func(o *hostconfig.Options) { o.Capacity = 0 },
		func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxCommandQueueSize + 1 },
		func(o *hostconfig.Options) { o.RegistryExpiry = o.RegistryHeartbeat },
		func(o *hostconfig.Options) { o.ApplyDeadline = o.ClaimTTL },
		func(o *hostconfig.Options) { o.Placement = sessionwire.HostPlacementDedicated; o.Capacity = 1 },
		func(o *hostconfig.Options) {
			o.Placement = sessionwire.HostPlacementDedicated
			o.FixedSessionID = "session-71c"
			o.Capacity = 4
		},
		func(o *hostconfig.Options) { o.FixedSessionID = "session-71c" },
	}

	seen := map[hostconfig.OptionErrorCode]int{}
	for i, spoil := range spoilers {
		options := pooledOptions(t)
		spoil(&options)
		_, err := hostconfig.New(options)
		var invalid *hostconfig.InvalidOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("spoiler %d: New error = %v, want *InvalidOptionsError", i, err)
		}
		if invalid.Code == "" {
			t.Errorf("spoiler %d: Code is empty", i)
		}
		if first, duplicate := seen[invalid.Code]; duplicate {
			t.Errorf("spoilers %d and %d both report %q; a code shared by two rules cannot tell them apart", first, i, invalid.Code)
		}
		seen[invalid.Code] = i
	}
	if len(seen) != len(spoilers) {
		t.Errorf("saw %d distinct codes over %d spoilers", len(seen), len(spoilers))
	}
}

// TestInvalidEndpointCarriesCoresTypedCause keeps the repository rule that a
// package-level API preserves the cause a lower layer produced.
func TestInvalidEndpointCarriesCoresTypedCause(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t)
	options.InternalEndpoint = "wss://user:secret@host/hostlink"
	_, err := hostconfig.New(options)

	var validation *sessionwire.RequestValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("New error = %v; errors.As did not reach *sessionwire.RequestValidationError. Core validates this field and documents its Code as stable so a caller need not match text", err)
	}
	if validation.Field != "internal_endpoint" {
		t.Errorf("RequestValidationError.Field = %q, want internal_endpoint", validation.Field)
	}
}

// ---------------------------------------------------------------------------
// Timing relationships
// ---------------------------------------------------------------------------

// TestHeartbeatMustBeSafelyBelowExpiry is the substance of this task, and
// "safely" is a MARGIN rather than a comparison.
//
// A heartbeat one nanosecond below the expiry satisfies `<` and loses the
// registry entry on the first scheduling hiccup — which is to say immediately,
// in production. The rule is that MinHeartbeatsBeforeExpiry whole intervals fit
// inside the expiry, and the boundary is a row in both directions so `<=`
// versus `<` is not a matter of taste.
//
// BOTH SIDES VARY. A table that moves only the heartbeat tests only the
// heartbeat: with the expiry pinned to one constant, a comparison that reads
// the wrong field can still agree.
func TestHeartbeatMustBeSafelyBelowExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		heartbeat time.Duration
		expiry    time.Duration
		accepted  bool
	}{
		{name: "exactly the margin", heartbeat: 5 * time.Second, expiry: 15 * time.Second, accepted: true},
		{name: "one nanosecond under the margin", heartbeat: 5 * time.Second, expiry: 15*time.Second - 1, accepted: false},
		{name: "comfortably over the margin", heartbeat: 5 * time.Second, expiry: 60 * time.Second, accepted: true},
		{name: "merely less than, which is the bug", heartbeat: 5 * time.Second, expiry: 5*time.Second + 1, accepted: false},
		{name: "equal", heartbeat: 5 * time.Second, expiry: 5 * time.Second, accepted: false},
		{name: "expiry below heartbeat", heartbeat: 30 * time.Second, expiry: 5 * time.Second, accepted: false},
		// The same margin at a different scale, so neither side is a constant.
		{name: "exactly the margin, larger", heartbeat: 20 * time.Second, expiry: 60 * time.Second, accepted: true},
		{name: "one under the margin, larger", heartbeat: 20 * time.Second, expiry: 60*time.Second - 1, accepted: false},
		{name: "exactly the margin, smaller", heartbeat: time.Second, expiry: 3 * time.Second, accepted: true},
		{name: "one under the margin, smaller", heartbeat: time.Second, expiry: 3*time.Second - 1, accepted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.RegistryHeartbeat = tt.heartbeat
			options.RegistryExpiry = tt.expiry
			_, err := hostconfig.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New with heartbeat %v and expiry %v = %v, want acceptance", tt.heartbeat, tt.expiry, err)
				}
				return
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New with heartbeat %v and expiry %v error = %v, want *InvalidOptionsError", tt.heartbeat, tt.expiry, err)
			}
			if invalid.Code != hostconfig.OptionErrorCodeHeartbeatMargin {
				t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeHeartbeatMargin)
			}
			if !strings.Contains(invalid.Reason, "RegistryExpiry") || !strings.Contains(invalid.Reason, "RegistryHeartbeat") {
				t.Errorf("Reason = %q, want it to name both durations so the reader can see which to change", invalid.Reason)
			}
		})
	}
}

func TestClaimTTLMustBeSafelyBelowApplyDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		claim    time.Duration
		deadline time.Duration
		accepted bool
	}{
		{name: "exactly the margin", claim: 10 * time.Second, deadline: 20 * time.Second, accepted: true},
		{name: "one nanosecond under the margin", claim: 10 * time.Second, deadline: 20*time.Second - 1, accepted: false},
		{name: "comfortably over", claim: 10 * time.Second, deadline: 90 * time.Second, accepted: true},
		{name: "merely less than, which is the bug", claim: 10 * time.Second, deadline: 10*time.Second + 1, accepted: false},
		{name: "equal", claim: 10 * time.Second, deadline: 10 * time.Second, accepted: false},
		{name: "deadline below claim", claim: 40 * time.Second, deadline: 10 * time.Second, accepted: false},
		{name: "exactly the margin, larger", claim: 45 * time.Second, deadline: 90 * time.Second, accepted: true},
		{name: "one under the margin, larger", claim: 45 * time.Second, deadline: 90*time.Second - 1, accepted: false},
		{name: "exactly the margin, smaller", claim: 2 * time.Second, deadline: 4 * time.Second, accepted: true},
		{name: "one under the margin, smaller", claim: 2 * time.Second, deadline: 4*time.Second - 1, accepted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.ClaimTTL = tt.claim
			options.ApplyDeadline = tt.deadline
			_, err := hostconfig.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New with claim %v and deadline %v = %v, want acceptance", tt.claim, tt.deadline, err)
				}
				return
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New with claim %v and deadline %v error = %v, want *InvalidOptionsError", tt.claim, tt.deadline, err)
			}
			if invalid.Code != hostconfig.OptionErrorCodeClaimMargin {
				t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeClaimMargin)
			}
			if !strings.Contains(invalid.Reason, "ApplyDeadline") || !strings.Contains(invalid.Reason, "ClaimTTL") {
				t.Errorf("Reason = %q, want it to name both durations", invalid.Reason)
			}
		})
	}
}

// TestTheMarginsAreDerivedFromTheirConstants pins that the rule follows the
// exported constant rather than a literal that happens to equal it today.
func TestTheMarginsAreDerivedFromTheirConstants(t *testing.T) {
	t.Parallel()

	if hostconfig.MinHeartbeatsBeforeExpiry < 2 {
		t.Fatalf("MinHeartbeatsBeforeExpiry = %d; a margin below 2 tolerates no missed heartbeat and is not a margin", hostconfig.MinHeartbeatsBeforeExpiry)
	}
	if hostconfig.MinClaimAttemptsBeforeDeadline < 2 {
		t.Fatalf("MinClaimAttemptsBeforeDeadline = %d; below 2 a lapsed claim has no attempt left inside the deadline", hostconfig.MinClaimAttemptsBeforeDeadline)
	}

	heartbeat := 7 * time.Second
	options := pooledOptions(t)
	options.RegistryHeartbeat = heartbeat
	options.RegistryExpiry = heartbeat*hostconfig.MinHeartbeatsBeforeExpiry - 1
	if _, err := hostconfig.New(options); err == nil {
		t.Error("an expiry one nanosecond under MinHeartbeatsBeforeExpiry intervals was accepted")
	}
	options.RegistryExpiry = heartbeat * hostconfig.MinHeartbeatsBeforeExpiry
	if _, err := hostconfig.New(options); err != nil {
		t.Errorf("an expiry of exactly MinHeartbeatsBeforeExpiry intervals was refused: %v", err)
	}

	claim := 3 * time.Second
	options = pooledOptions(t)
	options.ClaimTTL = claim
	options.ApplyDeadline = claim*hostconfig.MinClaimAttemptsBeforeDeadline - 1
	if _, err := hostconfig.New(options); err == nil {
		t.Error("a deadline one nanosecond under MinClaimAttemptsBeforeDeadline claims was accepted")
	}
	options.ApplyDeadline = claim * hostconfig.MinClaimAttemptsBeforeDeadline
	if _, err := hostconfig.New(options); err != nil {
		t.Errorf("a deadline of exactly MinClaimAttemptsBeforeDeadline claims was refused: %v", err)
	}
}

// TestSizeBoundsAcceptExactlyTheirConstant is the boundary-in-both-directions
// treatment the size bounds did not get.
//
// The timing margins got it rigorously and the sizes got nothing, which is the
// same "which branch does this test actually enter" failure in a new dress: the
// discipline was real but scoped to where I was looking. The only bound row was
// MaxCommandQueueSize+1, and that value exceeds EITHER constant — so swapping
// CommandQueueSize's bound for MaxReconcileBatch left the suite green with the
// queue limit 16x tighter than the exported constant that documents it. A
// caller who reads MaxCommandQueueSize and configures exactly that would have
// been refused by a Host whose tests all passed.
//
// The accepted-at-the-bound row is what closes it, and it only works because
// the two constants DIFFER; that is asserted rather than assumed.
func TestSizeBoundsAcceptExactlyTheirConstant(t *testing.T) {
	t.Parallel()

	if hostconfig.MaxCommandQueueSize == hostconfig.MaxReconcileBatch {
		t.Fatal("the two size bounds are equal, so swapping one for the other in validateShape would be undetectable by any value. Keep them distinct or this test cannot do its job")
	}

	tests := []struct {
		name     string
		apply    func(*hostconfig.Options)
		accepted bool
	}{
		{name: "queue at exactly its bound", apply: func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxCommandQueueSize }, accepted: true},
		{name: "queue one over its bound", apply: func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxCommandQueueSize + 1 }},
		{name: "queue at one", apply: func(o *hostconfig.Options) { o.CommandQueueSize = 1 }, accepted: true},
		// The cross rows: each size set to the OTHER bound's constant. The
		// queue accepts it because the queue's bound is larger; the batch
		// refuses it for the same reason. A swapped bound inverts both.
		{name: "queue at the batch bound", apply: func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxReconcileBatch }, accepted: true},
		{name: "batch at exactly its bound", apply: func(o *hostconfig.Options) { o.ReconcileBatch = hostconfig.MaxReconcileBatch }, accepted: true},
		{name: "batch one over its bound", apply: func(o *hostconfig.Options) { o.ReconcileBatch = hostconfig.MaxReconcileBatch + 1 }},
		{name: "batch at one", apply: func(o *hostconfig.Options) { o.ReconcileBatch = 1 }, accepted: true},
		{name: "batch at the queue bound", apply: func(o *hostconfig.Options) { o.ReconcileBatch = hostconfig.MaxCommandQueueSize }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			tt.apply(&options)
			built, err := hostconfig.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New = %v, want acceptance. A caller who reads the exported constant and configures exactly it must be accepted, or the constant is not the bound", err)
				}
				return
			}
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Code != hostconfig.OptionErrorCodeAboveBound {
				t.Errorf("Code = %q, want %q", invalid.Code, hostconfig.OptionErrorCodeAboveBound)
			}
		})
	}
}

// TestDurationsHaveNoUndocumentedMinimum is the second instance of the survivor
// class, found by auditing the rest of the file rather than by being told.
//
// The rule for a duration is POSITIVE, and nothing more. Every fixture in this
// file is on a scale of seconds, so imposing an undocumented one-second floor —
// `duration.value < time.Second` in place of `<= 0` — left the whole suite
// green. That is the same defect as the size bounds: a rejection row proves the
// upper side, and with no accepted row at the smallest legal value the lower
// side is whatever the implementation happens to say.
//
// A nanosecond heartbeat is operationally absurd, and accepting it is still the
// right answer here: this constructor validates COHERENCE, not operational
// sanity, and a minimum would be a new rule owing its own exported constant and
// its own boundary rows, exactly as the two margins do. If one is ever wanted,
// add it that way rather than by tightening this comparison.
func TestDurationsHaveNoUndocumentedMinimum(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t)
	options.WarmTTL = 1
	options.ReconcileInterval = 1
	options.RegistryHeartbeat = 1
	options.RegistryExpiry = hostconfig.MinHeartbeatsBeforeExpiry
	options.ClaimTTL = 1
	options.ApplyDeadline = hostconfig.MinClaimAttemptsBeforeDeadline

	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New with nanosecond-scale durations = %v, want acceptance: the rule is positive, and any floor above that is an undocumented rule", err)
	}
	if got := built.WarmTTL(); got != 1 {
		t.Errorf("WarmTTL() = %v, want 1ns", got)
	}
	if got := built.RegistryHeartbeat(); got != 1 {
		t.Errorf("RegistryHeartbeat() = %v, want 1ns", got)
	}

	// The other side stays closed: zero is still refused at that scale, so this
	// row does not soften the positivity rule it is bounding.
	options.WarmTTL = 0
	if _, err := hostconfig.New(options); err == nil {
		t.Error("a zero WarmTTL was accepted")
	}
}

// TestEachBoundExplainsItself holds that the two size bounds report their OWN
// rationale.
//
// They share a validation loop, and for a while they shared its sentence too:
// ReconcileBatch told the reader that "an unbounded one turns backpressure into
// memory growth", which is the queue's argument and not the batch's. That is
// user-visible text, and reverting it survived every other assertion in this
// file — a stated expectation nothing checked, which is the shape this
// repository treats as a defect in its own right.
func TestEachBoundExplainsItself(t *testing.T) {
	t.Parallel()

	reasons := map[string]string{}
	for field, spoil := range map[string]func(*hostconfig.Options){
		"CommandQueueSize": func(o *hostconfig.Options) { o.CommandQueueSize = hostconfig.MaxCommandQueueSize + 1 },
		"ReconcileBatch":   func(o *hostconfig.Options) { o.ReconcileBatch = hostconfig.MaxReconcileBatch + 1 },
	} {
		options := pooledOptions(t)
		spoil(&options)
		_, err := hostconfig.New(options)
		var invalid *hostconfig.InvalidOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("%s: New error = %v, want *InvalidOptionsError", field, err)
		}
		reasons[field] = invalid.Reason
	}

	if reasons["CommandQueueSize"] == reasons["ReconcileBatch"] {
		t.Errorf("both bounds report the same reason %q; they bound different things for different reasons and a shared loop must not become a shared explanation", reasons["CommandQueueSize"])
	}
	if !strings.Contains(reasons["CommandQueueSize"], "backpressure") {
		t.Errorf("the queue's reason %q does not give the queue's argument", reasons["CommandQueueSize"])
	}
	if !strings.Contains(reasons["ReconcileBatch"], "converge") {
		t.Errorf("the batch's reason %q does not give the batch's argument", reasons["ReconcileBatch"])
	}
}

// TestResolvedSizesSurviveAtTheBound pins that a value accepted at the bound is
// also REPORTED at the bound, since acceptance and the accessor are two code
// paths and this lane has already shipped a divergence between exactly that
// pair once.
func TestResolvedSizesSurviveAtTheBound(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t)
	options.CommandQueueSize = hostconfig.MaxCommandQueueSize
	options.ReconcileBatch = hostconfig.MaxReconcileBatch
	built, err := hostconfig.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := built.CommandQueueSize(); got != hostconfig.MaxCommandQueueSize {
		t.Errorf("CommandQueueSize() = %d, want %d", got, hostconfig.MaxCommandQueueSize)
	}
	if got := built.ReconcileBatch(); got != hostconfig.MaxReconcileBatch {
		t.Errorf("ReconcileBatch() = %d, want %d", got, hostconfig.MaxReconcileBatch)
	}
}

// ---------------------------------------------------------------------------
// Placement is a cross-field rule
// ---------------------------------------------------------------------------

// TestPlacementBindingRules exercises the pairing in BOTH directions, with a
// positive control for each, because a rejection test with no accepted
// counterpart is satisfied by a constructor that rejects everything.
func TestPlacementBindingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement sessionwire.HostPlacement
		capacity  uint64
		fixed     sessionwire.SessionID
		code      hostconfig.OptionErrorCode
	}{
		{name: "dedicated, bound, capacity one", placement: sessionwire.HostPlacementDedicated, capacity: 1, fixed: "session-71c"},
		{name: "dedicated without a fixed session", placement: sessionwire.HostPlacementDedicated, capacity: 1, fixed: "", code: hostconfig.OptionErrorCodeDedicatedFixed},
		{name: "dedicated with capacity above one", placement: sessionwire.HostPlacementDedicated, capacity: 2, fixed: "session-71c", code: hostconfig.OptionErrorCodeDedicatedCap},
		{name: "dedicated with large capacity", placement: sessionwire.HostPlacementDedicated, capacity: 64, fixed: "session-71c", code: hostconfig.OptionErrorCodeDedicatedCap},
		{name: "pooled, unbound", placement: sessionwire.HostPlacementPooled, capacity: 8, fixed: ""},
		{name: "pooled with capacity one", placement: sessionwire.HostPlacementPooled, capacity: 1, fixed: ""},
		{name: "pooled with a fixed session", placement: sessionwire.HostPlacementPooled, capacity: 8, fixed: "session-71c", code: hostconfig.OptionErrorCodePooledFixed},
		{name: "pooled with a fixed session and capacity one", placement: sessionwire.HostPlacementPooled, capacity: 1, fixed: "session-71c", code: hostconfig.OptionErrorCodePooledFixed},
	}

	accepted := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.Placement = tt.placement
			options.Capacity = tt.capacity
			options.FixedSessionID = tt.fixed

			built, err := hostconfig.New(options)
			if tt.code == "" {
				if err != nil {
					t.Fatalf("New = %v, want acceptance", err)
				}
				if built.Placement() != tt.placement || built.Capacity() != tt.capacity || built.FixedSessionID() != tt.fixed {
					t.Errorf("resolved (%q, %d, %q), want (%q, %d, %q)", built.Placement(), built.Capacity(), built.FixedSessionID(), tt.placement, tt.capacity, tt.fixed)
				}
				return
			}
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Code != tt.code {
				t.Errorf("Code = %q, want %q", invalid.Code, tt.code)
			}
		})
	}
	for _, tt := range tests {
		if tt.code == "" {
			accepted++
		}
	}
	if accepted < 3 {
		t.Errorf("the table has %d accepted rows; without positive controls these rejections are satisfied by a constructor that refuses everything", accepted)
	}
	if !slices.ContainsFunc(tests, func(tt struct {
		name      string
		placement sessionwire.HostPlacement
		capacity  uint64
		fixed     sessionwire.SessionID
		code      hostconfig.OptionErrorCode
	}) bool {
		return tt.placement == sessionwire.HostPlacementDedicated && tt.code == ""
	}) {
		t.Error("no accepted DEDICATED row: the dedicated rejections would be satisfied by refusing dedicated placement outright")
	}
}

// TestHostExposesNoTenantAccessor is a TRIP-WIRE for the most likely way human
// gate H8 gets silently undone, and it is deliberately a structural guard
// rather than a behavioural one.
//
// H8 (answered 2026-09-04, option (a)) removed the fixed tenant from this type:
// a pooled Host advertising cross_tenant_isolated may hold several tenants, and
// one advertising tenant_exclusive is exclusive by PLACEMENT rather than by
// construction, which is spec §12's own wording. The admission rule now lives
// in internal/service's ledger and the identity rule in the residency manager.
//
// THE REVERSAL PATH IS CONCRETE. hostlink.MultiplexerOptions still carries a
// TenantID — correctly, because it is the tenant a LINK authenticated as — and
// the composition root that fills it does not exist yet (O7.1). The obvious
// wrong move when writing it is to reach for a Host-wide tenant, which means
// re-adding this accessor; and a Host serving one Multiplexer for several
// tenants then refuses every cross-tenant bind with foreign_tenant, undoing H8
// while failing CLOSED and therefore quietly. The correct composition is one
// Multiplexer per authenticated tenant.
//
// This cannot make the wrong composition fail, because no composition exists
// for a guard to see; what it makes fail is the EDIT that composition would
// have to make first. Written down so a later reader does not mistake it for
// more coverage than it is.
func TestHostExposesNoTenantAccessor(t *testing.T) {
	t.Parallel()

	hostType := reflect.TypeOf((*hostconfig.Host)(nil))
	if hostType.NumMethod() == 0 {
		t.Fatal("*hostconfig.Host has no methods at all, so this guard is vacuous")
	}
	// The control: a method this type DOES have, so a guard that could never
	// find anything is distinguishable from one that found nothing wrong.
	if _, present := hostType.MethodByName("IsolationClass"); !present {
		t.Fatal("*hostconfig.Host has no IsolationClass method, so the lookup used below does not work")
	}
	for _, banned := range []string{"TenantID", "Tenant"} {
		if _, present := hostType.MethodByName(banned); present {
			t.Errorf("*hostconfig.Host exposes %s. H8 removed the fixed tenant from this type; a Host is tenant-exclusive by PLACEMENT, not by construction, and re-adding this is how a composition feeds one tenant to a Host-wide hostlink.Multiplexer and reverses H8 without any behavioural test failing. If a caller needs a tenant, it is the one the REQUEST or the LINK names", banned)
		}
	}
}
