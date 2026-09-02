package host_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
)

// ---------------------------------------------------------------------------
// Collaborator stubs
// ---------------------------------------------------------------------------

type stubSessionStore struct{}

func (stubSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

type stubWorkspaces struct{}

func (stubWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", nil
}

type stubClock struct{}

func (stubClock) Now() time.Time                       { return time.Unix(0, 0) }
func (stubClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

type stubAuth struct{}

func (stubAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

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

func testDepartment(t *testing.T) *department.Department {
	t.Helper()
	registry, err := department.New([]department.Registration{{AgentID: "reviewer", Target: stubTarget{}}})
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
func pooledOptions(t *testing.T) host.Options {
	t.Helper()
	return host.Options{
		HostID:            "host-7c1",
		TenantID:          "tenant-9f3",
		InternalEndpoint:  "wss://host-7c1.internal.example:8443/hostlink",
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        testDepartment(t),
		SessionStore:      stubSessionStore{},
		Workspaces:        stubWorkspaces{},
		Clock:             stubClock{},
		Auth:              stubAuth{},
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

func dedicatedOptions(t *testing.T) host.Options {
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
		options host.Options
	}{
		{name: "pooled", options: pooledOptions(t)},
		{name: "dedicated", options: dedicatedOptions(t)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			built, err := host.New(tt.options)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if built == nil {
				t.Fatal("New returned no Host and no error")
			}
		})
	}
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
	built, err := host.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := built.ID(); got != options.HostID {
		t.Errorf("ID() = %q, want %q", got, options.HostID)
	}
	if got := built.TenantID(); got != options.TenantID {
		t.Errorf("TenantID() = %q, want %q", got, options.TenantID)
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
	if built.SessionStore() == nil || built.Workspaces() == nil || built.Clock() == nil || built.Auth() == nil {
		t.Error("a collaborator accessor returned nil")
	}
}

// TestHostDoesNotAliasTheCallersOptions is the immutability probe.
func TestHostDoesNotAliasTheCallersOptions(t *testing.T) {
	t.Parallel()

	options := dedicatedOptions(t)
	built, err := host.New(options)
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
		spoil func(*host.Options)
		field string
		code  host.OptionErrorCode
	}{
		{name: "host id", spoil: func(o *host.Options) { o.HostID = "" }, field: "HostID", code: host.OptionErrorCodeMissing},
		{name: "tenant id", spoil: func(o *host.Options) { o.TenantID = "" }, field: "TenantID", code: host.OptionErrorCodeMissing},
		{name: "internal endpoint absent", spoil: func(o *host.Options) { o.InternalEndpoint = "" }, field: "InternalEndpoint", code: host.OptionErrorCodeMissing},
		{name: "internal endpoint malformed", spoil: func(o *host.Options) { o.InternalEndpoint = "http://host/hostlink" }, field: "InternalEndpoint", code: host.OptionErrorCodeInvalid},
		{name: "isolation class unknown", spoil: func(o *host.Options) { o.IsolationClass = "shared" }, field: "IsolationClass", code: host.OptionErrorCodeUnknownEnum},
		{name: "isolation class absent", spoil: func(o *host.Options) { o.IsolationClass = "" }, field: "IsolationClass", code: host.OptionErrorCodeUnknownEnum},
		{name: "department", spoil: func(o *host.Options) { o.Department = nil }, field: "Department", code: host.OptionErrorCodeMissing},
		{name: "session store", spoil: func(o *host.Options) { o.SessionStore = nil }, field: "SessionStore", code: host.OptionErrorCodeMissing},
		{name: "workspaces", spoil: func(o *host.Options) { o.Workspaces = nil }, field: "Workspaces", code: host.OptionErrorCodeMissing},
		{name: "clock", spoil: func(o *host.Options) { o.Clock = nil }, field: "Clock", code: host.OptionErrorCodeMissing},
		{name: "auth", spoil: func(o *host.Options) { o.Auth = nil }, field: "Auth", code: host.OptionErrorCodeMissing},
		{name: "placement unknown", spoil: func(o *host.Options) { o.Placement = "burst" }, field: "Placement", code: host.OptionErrorCodeUnknownEnum},
		{name: "placement absent", spoil: func(o *host.Options) { o.Placement = "" }, field: "Placement", code: host.OptionErrorCodeUnknownEnum},
		{name: "capacity zero", spoil: func(o *host.Options) { o.Capacity = 0 }, field: "Capacity", code: host.OptionErrorCodeNotPositive},
		{name: "warm ttl zero", spoil: func(o *host.Options) { o.WarmTTL = 0 }, field: "WarmTTL", code: host.OptionErrorCodeNotPositive},
		{name: "warm ttl negative", spoil: func(o *host.Options) { o.WarmTTL = -time.Second }, field: "WarmTTL", code: host.OptionErrorCodeNotPositive},
		{name: "heartbeat zero", spoil: func(o *host.Options) { o.RegistryHeartbeat = 0 }, field: "RegistryHeartbeat", code: host.OptionErrorCodeNotPositive},
		{name: "expiry zero", spoil: func(o *host.Options) { o.RegistryExpiry = 0 }, field: "RegistryExpiry", code: host.OptionErrorCodeNotPositive},
		{name: "claim ttl zero", spoil: func(o *host.Options) { o.ClaimTTL = 0 }, field: "ClaimTTL", code: host.OptionErrorCodeNotPositive},
		{name: "apply deadline zero", spoil: func(o *host.Options) { o.ApplyDeadline = 0 }, field: "ApplyDeadline", code: host.OptionErrorCodeNotPositive},
		{name: "queue size zero", spoil: func(o *host.Options) { o.CommandQueueSize = 0 }, field: "CommandQueueSize", code: host.OptionErrorCodeNotPositive},
		{name: "queue size negative", spoil: func(o *host.Options) { o.CommandQueueSize = -1 }, field: "CommandQueueSize", code: host.OptionErrorCodeNotPositive},
		{name: "queue size above bound", spoil: func(o *host.Options) { o.CommandQueueSize = host.MaxCommandQueueSize + 1 }, field: "CommandQueueSize", code: host.OptionErrorCodeAboveBound},
		{name: "reconcile interval zero", spoil: func(o *host.Options) { o.ReconcileInterval = 0 }, field: "ReconcileInterval", code: host.OptionErrorCodeNotPositive},
		{name: "reconcile batch zero", spoil: func(o *host.Options) { o.ReconcileBatch = 0 }, field: "ReconcileBatch", code: host.OptionErrorCodeNotPositive},
		{name: "reconcile batch above bound", spoil: func(o *host.Options) { o.ReconcileBatch = host.MaxReconcileBatch + 1 }, field: "ReconcileBatch", code: host.OptionErrorCodeAboveBound},
	}

	if len(tests) == 0 {
		t.Fatal("no option rule is exercised")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			tt.spoil(&options)
			built, err := host.New(options)
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}
			var invalid *host.InvalidOptionsError
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
// discriminate every refusal, which is the property O1.1 shipped without: an
// exported error type no consumer could match is a type that does not exist.
func TestEveryRuleIsReachableThroughTheExportedConstructor(t *testing.T) {
	t.Parallel()

	spoilers := []func(*host.Options){
		func(o *host.Options) { o.HostID = "" },
		func(o *host.Options) { o.InternalEndpoint = "http://host/x" },
		func(o *host.Options) { o.IsolationClass = "shared" },
		func(o *host.Options) { o.Capacity = 0 },
		func(o *host.Options) { o.CommandQueueSize = host.MaxCommandQueueSize + 1 },
		func(o *host.Options) { o.RegistryExpiry = o.RegistryHeartbeat },
		func(o *host.Options) { o.ApplyDeadline = o.ClaimTTL },
		func(o *host.Options) { o.Placement = sessionwire.HostPlacementDedicated; o.Capacity = 1 },
		func(o *host.Options) {
			o.Placement = sessionwire.HostPlacementDedicated
			o.FixedSessionID = "session-71c"
			o.Capacity = 4
		},
		func(o *host.Options) { o.FixedSessionID = "session-71c" },
	}

	seen := map[host.OptionErrorCode]int{}
	for i, spoil := range spoilers {
		options := pooledOptions(t)
		spoil(&options)
		_, err := host.New(options)
		var invalid *host.InvalidOptionsError
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
	_, err := host.New(options)

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
			_, err := host.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New with heartbeat %v and expiry %v = %v, want acceptance", tt.heartbeat, tt.expiry, err)
				}
				return
			}
			var invalid *host.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New with heartbeat %v and expiry %v error = %v, want *InvalidOptionsError", tt.heartbeat, tt.expiry, err)
			}
			if invalid.Code != host.OptionErrorCodeHeartbeatMargin {
				t.Errorf("Code = %q, want %q", invalid.Code, host.OptionErrorCodeHeartbeatMargin)
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
			_, err := host.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New with claim %v and deadline %v = %v, want acceptance", tt.claim, tt.deadline, err)
				}
				return
			}
			var invalid *host.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New with claim %v and deadline %v error = %v, want *InvalidOptionsError", tt.claim, tt.deadline, err)
			}
			if invalid.Code != host.OptionErrorCodeClaimMargin {
				t.Errorf("Code = %q, want %q", invalid.Code, host.OptionErrorCodeClaimMargin)
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

	if host.MinHeartbeatsBeforeExpiry < 2 {
		t.Fatalf("MinHeartbeatsBeforeExpiry = %d; a margin below 2 tolerates no missed heartbeat and is not a margin", host.MinHeartbeatsBeforeExpiry)
	}
	if host.MinClaimAttemptsBeforeDeadline < 2 {
		t.Fatalf("MinClaimAttemptsBeforeDeadline = %d; below 2 a lapsed claim has no attempt left inside the deadline", host.MinClaimAttemptsBeforeDeadline)
	}

	heartbeat := 7 * time.Second
	options := pooledOptions(t)
	options.RegistryHeartbeat = heartbeat
	options.RegistryExpiry = heartbeat*host.MinHeartbeatsBeforeExpiry - 1
	if _, err := host.New(options); err == nil {
		t.Error("an expiry one nanosecond under MinHeartbeatsBeforeExpiry intervals was accepted")
	}
	options.RegistryExpiry = heartbeat * host.MinHeartbeatsBeforeExpiry
	if _, err := host.New(options); err != nil {
		t.Errorf("an expiry of exactly MinHeartbeatsBeforeExpiry intervals was refused: %v", err)
	}

	claim := 3 * time.Second
	options = pooledOptions(t)
	options.ClaimTTL = claim
	options.ApplyDeadline = claim*host.MinClaimAttemptsBeforeDeadline - 1
	if _, err := host.New(options); err == nil {
		t.Error("a deadline one nanosecond under MinClaimAttemptsBeforeDeadline claims was accepted")
	}
	options.ApplyDeadline = claim * host.MinClaimAttemptsBeforeDeadline
	if _, err := host.New(options); err != nil {
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

	if host.MaxCommandQueueSize == host.MaxReconcileBatch {
		t.Fatal("the two size bounds are equal, so swapping one for the other in validateShape would be undetectable by any value. Keep them distinct or this test cannot do its job")
	}

	tests := []struct {
		name     string
		apply    func(*host.Options)
		accepted bool
	}{
		{name: "queue at exactly its bound", apply: func(o *host.Options) { o.CommandQueueSize = host.MaxCommandQueueSize }, accepted: true},
		{name: "queue one over its bound", apply: func(o *host.Options) { o.CommandQueueSize = host.MaxCommandQueueSize + 1 }},
		{name: "queue at one", apply: func(o *host.Options) { o.CommandQueueSize = 1 }, accepted: true},
		// The cross rows: each size set to the OTHER bound's constant. The
		// queue accepts it because the queue's bound is larger; the batch
		// refuses it for the same reason. A swapped bound inverts both.
		{name: "queue at the batch bound", apply: func(o *host.Options) { o.CommandQueueSize = host.MaxReconcileBatch }, accepted: true},
		{name: "batch at exactly its bound", apply: func(o *host.Options) { o.ReconcileBatch = host.MaxReconcileBatch }, accepted: true},
		{name: "batch one over its bound", apply: func(o *host.Options) { o.ReconcileBatch = host.MaxReconcileBatch + 1 }},
		{name: "batch at one", apply: func(o *host.Options) { o.ReconcileBatch = 1 }, accepted: true},
		{name: "batch at the queue bound", apply: func(o *host.Options) { o.ReconcileBatch = host.MaxCommandQueueSize }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			tt.apply(&options)
			built, err := host.New(options)
			if tt.accepted {
				if err != nil {
					t.Fatalf("New = %v, want acceptance. A caller who reads the exported constant and configures exactly it must be accepted, or the constant is not the bound", err)
				}
				return
			}
			if built != nil {
				t.Error("New returned a Host alongside its error")
			}
			var invalid *host.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New error = %v, want *InvalidOptionsError", err)
			}
			if invalid.Code != host.OptionErrorCodeAboveBound {
				t.Errorf("Code = %q, want %q", invalid.Code, host.OptionErrorCodeAboveBound)
			}
		})
	}
}

// TestDurationsHaveNoUndOCUMENTEDMinimum is the second instance of the survivor
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
	options.RegistryExpiry = host.MinHeartbeatsBeforeExpiry
	options.ClaimTTL = 1
	options.ApplyDeadline = host.MinClaimAttemptsBeforeDeadline

	built, err := host.New(options)
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
	if _, err := host.New(options); err == nil {
		t.Error("a zero WarmTTL was accepted")
	}
}

// TestResolvedSizesSurviveAtTheBound pins that a value accepted at the bound is
// also REPORTED at the bound, since acceptance and the accessor are two code
// paths and this lane has already shipped a divergence between exactly that
// pair once.
func TestResolvedSizesSurviveAtTheBound(t *testing.T) {
	t.Parallel()

	options := pooledOptions(t)
	options.CommandQueueSize = host.MaxCommandQueueSize
	options.ReconcileBatch = host.MaxReconcileBatch
	built, err := host.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := built.CommandQueueSize(); got != host.MaxCommandQueueSize {
		t.Errorf("CommandQueueSize() = %d, want %d", got, host.MaxCommandQueueSize)
	}
	if got := built.ReconcileBatch(); got != host.MaxReconcileBatch {
		t.Errorf("ReconcileBatch() = %d, want %d", got, host.MaxReconcileBatch)
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
		code      host.OptionErrorCode
	}{
		{name: "dedicated, bound, capacity one", placement: sessionwire.HostPlacementDedicated, capacity: 1, fixed: "session-71c"},
		{name: "dedicated without a fixed session", placement: sessionwire.HostPlacementDedicated, capacity: 1, fixed: "", code: host.OptionErrorCodeDedicatedFixed},
		{name: "dedicated with capacity above one", placement: sessionwire.HostPlacementDedicated, capacity: 2, fixed: "session-71c", code: host.OptionErrorCodeDedicatedCap},
		{name: "dedicated with large capacity", placement: sessionwire.HostPlacementDedicated, capacity: 64, fixed: "session-71c", code: host.OptionErrorCodeDedicatedCap},
		{name: "pooled, unbound", placement: sessionwire.HostPlacementPooled, capacity: 8, fixed: ""},
		{name: "pooled with capacity one", placement: sessionwire.HostPlacementPooled, capacity: 1, fixed: ""},
		{name: "pooled with a fixed session", placement: sessionwire.HostPlacementPooled, capacity: 8, fixed: "session-71c", code: host.OptionErrorCodePooledFixed},
		{name: "pooled with a fixed session and capacity one", placement: sessionwire.HostPlacementPooled, capacity: 1, fixed: "session-71c", code: host.OptionErrorCodePooledFixed},
	}

	accepted := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.Placement = tt.placement
			options.Capacity = tt.capacity
			options.FixedSessionID = tt.fixed

			built, err := host.New(options)
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
			var invalid *host.InvalidOptionsError
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
		code      host.OptionErrorCode
	}) bool {
		return tt.placement == sessionwire.HostPlacementDedicated && tt.code == ""
	}) {
		t.Error("no accepted DEDICATED row: the dedicated rejections would be satisfied by refusing dedicated placement outright")
	}
}
