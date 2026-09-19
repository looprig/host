package host_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
)

// ---------------------------------------------------------------------------
// The root package is a carrier for internal/hostconfig, held to it by test
// ---------------------------------------------------------------------------
//
// Since v0.2.0 the resolved configuration lives in internal/hostconfig and this
// package re-declares its exported surface — the same names, values and
// shapes — delegating by value. That is a duplication with a reason (see
// host.Host's doc) and duplication is how two declarations drift, so every
// axis on which they could drift is held here: the Options field set, the Host
// method set, the constants, the error codes, and every refusal reaching New.

// shape prints a type for comparison across the two packages: package
// qualifiers are erased, and an interface is printed as its sorted method set,
// so `host.SessionStore` and `hostconfig.SessionStore` compare equal exactly
// when their method sets do.
func shape(t reflect.Type) string {
	erase := func(s string) string {
		s = strings.ReplaceAll(s, "hostconfig.", "")
		return strings.ReplaceAll(s, "host.", "")
	}
	if t.Kind() != reflect.Interface {
		return erase(t.String())
	}
	methods := make([]string, 0, t.NumMethod())
	for index := range t.NumMethod() {
		method := t.Method(index)
		methods = append(methods, method.Name+erase(method.Type.String()))
	}
	sort.Strings(methods)
	return "interface{" + strings.Join(methods, "; ") + "}"
}

// TestOptionsFieldsMirrorTheResolvedConfiguration holds the two Options
// structs to the same fields, in the same order, with the same shapes. A field
// added to internal/hostconfig and not carried by Options.resolved would be
// silently dropped at construction; this is what notices.
func TestOptionsFieldsMirrorTheResolvedConfiguration(t *testing.T) {
	t.Parallel()
	exported := reflect.TypeOf(host.Options{})
	internal := reflect.TypeOf(hostconfig.Options{})
	if exported.NumField() != internal.NumField() {
		t.Fatalf("host.Options has %d fields and hostconfig.Options has %d", exported.NumField(), internal.NumField())
	}
	for index := range exported.NumField() {
		got, want := exported.Field(index), internal.Field(index)
		if got.Name != want.Name {
			t.Errorf("field %d is %q in host and %q in hostconfig", index, got.Name, want.Name)
			continue
		}
		if shape(got.Type) != shape(want.Type) {
			t.Errorf("field %s is %s in host and %s in hostconfig", got.Name, shape(got.Type), shape(want.Type))
		}
	}
	if exported.NumField() < 19 {
		t.Fatalf("host.Options has %d fields; the comparison above walked fewer than the configuration is known to have", exported.NumField())
	}
}

// TestHostMethodSetMirrorsTheResolvedConfiguration holds *host.Host to exactly
// *hostconfig.Host's methods, by name and shape. It is also the successor of
// TestHostExposesNoTenantAccessor for this package: a TenantID accessor on
// either side, and not the other, fails here.
func TestHostMethodSetMirrorsTheResolvedConfiguration(t *testing.T) {
	t.Parallel()
	exported := reflect.TypeOf((*host.Host)(nil))
	internal := reflect.TypeOf((*hostconfig.Host)(nil))
	if exported.NumMethod() != internal.NumMethod() {
		t.Fatalf("*host.Host has %d methods and *hostconfig.Host has %d", exported.NumMethod(), internal.NumMethod())
	}
	for index := range exported.NumMethod() {
		got := exported.Method(index)
		want, held := internal.MethodByName(got.Name)
		if !held {
			t.Errorf("*host.Host has %s, which *hostconfig.Host does not", got.Name)
			continue
		}
		if shape(got.Type) != shape(want.Type) {
			t.Errorf("%s is %s on host and %s on hostconfig", got.Name, shape(got.Type), shape(want.Type))
		}
		if got.Name == "TenantID" {
			t.Errorf("*host.Host exposes TenantID; H8 removed the fixed tenant")
		}
	}
	if exported.NumMethod() < 19 {
		t.Fatalf("*host.Host has %d methods; the comparison above walked fewer than the configuration is known to have", exported.NumMethod())
	}
}

// TestConstantsAndCodesMirrorTheResolvedConfiguration holds every published
// number and code to the internal one.
func TestConstantsAndCodesMirrorTheResolvedConfiguration(t *testing.T) {
	t.Parallel()
	for name, pair := range map[string][2]int{
		"MinHeartbeatsBeforeExpiry":      {host.MinHeartbeatsBeforeExpiry, hostconfig.MinHeartbeatsBeforeExpiry},
		"MinClaimAttemptsBeforeDeadline": {host.MinClaimAttemptsBeforeDeadline, hostconfig.MinClaimAttemptsBeforeDeadline},
		"MaxCommandQueueSize":            {host.MaxCommandQueueSize, hostconfig.MaxCommandQueueSize},
		"MaxReconcileBatch":              {host.MaxReconcileBatch, hostconfig.MaxReconcileBatch},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s is %d in host and %d in hostconfig", name, pair[0], pair[1])
		}
	}
	for name, pair := range map[string][2]string{
		"Missing":         {string(host.OptionErrorCodeMissing), string(hostconfig.OptionErrorCodeMissing)},
		"Invalid":         {string(host.OptionErrorCodeInvalid), string(hostconfig.OptionErrorCodeInvalid)},
		"UnknownEnum":     {string(host.OptionErrorCodeUnknownEnum), string(hostconfig.OptionErrorCodeUnknownEnum)},
		"NotPositive":     {string(host.OptionErrorCodeNotPositive), string(hostconfig.OptionErrorCodeNotPositive)},
		"AboveBound":      {string(host.OptionErrorCodeAboveBound), string(hostconfig.OptionErrorCodeAboveBound)},
		"HeartbeatMargin": {string(host.OptionErrorCodeHeartbeatMargin), string(hostconfig.OptionErrorCodeHeartbeatMargin)},
		"ClaimMargin":     {string(host.OptionErrorCodeClaimMargin), string(hostconfig.OptionErrorCodeClaimMargin)},
		"DedicatedFixed":  {string(host.OptionErrorCodeDedicatedFixed), string(hostconfig.OptionErrorCodeDedicatedFixed)},
		"DedicatedCap":    {string(host.OptionErrorCodeDedicatedCap), string(hostconfig.OptionErrorCodeDedicatedCap)},
		"PooledFixed":     {string(host.OptionErrorCodePooledFixed), string(hostconfig.OptionErrorCodePooledFixed)},
	} {
		if pair[0] != pair[1] {
			t.Errorf("OptionErrorCode%s is %q in host and %q in hostconfig", name, pair[0], pair[1])
		}
	}
}

// ---------------------------------------------------------------------------
// Delegation
// ---------------------------------------------------------------------------

type facadeStore struct{ id string }

func (facadeStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, nil
}

type facadeWorkspaces struct{ id string }

func (facadeWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", nil
}

type facadeClock struct{ id string }

func (facadeClock) Now() time.Time                       { return time.Unix(0, 0) }
func (facadeClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

type facadeAuth struct{ id string }

func (facadeAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

type facadeTarget struct{}

func (facadeTarget) CompatibilityID() department.CompatibilityID { return "runtime-facade" }
func (facadeTarget) Capabilities() department.Capabilities {
	return department.Capabilities{SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming}
}
func (facadeTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("facadeTarget launches nothing")
}
func (facadeTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("facadeTarget launches nothing")
}

func facadeOptions(t *testing.T) host.Options {
	t.Helper()
	dept, err := department.New([]department.Registration{{AgentID: "agent-facade", Target: facadeTarget{}}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	return host.Options{
		HostID:            "host-facade",
		InternalEndpoint:  "ws://10.0.0.1:7100",
		IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
		Department:        dept,
		SessionStore:      facadeStore{id: "store"},
		Workspaces:        facadeWorkspaces{id: "workspaces"},
		Clock:             facadeClock{id: "clock"},
		Auth:              facadeAuth{id: "auth"},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           90 * time.Second,
		RegistryHeartbeat: 10 * time.Second,
		RegistryExpiry:    60 * time.Second,
		ClaimTTL:          5 * time.Second,
		ApplyDeadline:     30 * time.Second,
		CommandQueueSize:  16,
		ReconcileInterval: time.Minute,
		ReconcileBatch:    32,
	}
}

// TestNewReportsWhatItWasGivenThroughTheCarrier is the positive control for
// the delegation: every accessor answers the value or the very collaborator
// the caller supplied.
func TestNewReportsWhatItWasGivenThroughTheCarrier(t *testing.T) {
	t.Parallel()
	options := facadeOptions(t)
	built, err := host.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if built.ID() != options.HostID || built.InternalEndpoint() != options.InternalEndpoint || built.IsolationClass() != options.IsolationClass {
		t.Errorf("identity accessors = (%q, %q, %q)", built.ID(), built.InternalEndpoint(), built.IsolationClass())
	}
	if built.Department() != options.Department {
		t.Error("Department is not the very value supplied")
	}
	if built.SessionStore() != options.SessionStore || built.Workspaces() != options.Workspaces ||
		built.Clock() != options.Clock || built.Auth() != options.Auth {
		t.Error("a collaborator accessor does not return the very value supplied")
	}
	if built.Placement() != options.Placement || built.Capacity() != options.Capacity || built.FixedSessionID() != "" {
		t.Errorf("placement accessors = (%q, %d, %q)", built.Placement(), built.Capacity(), built.FixedSessionID())
	}
	if built.WarmTTL() != options.WarmTTL || built.RegistryHeartbeat() != options.RegistryHeartbeat ||
		built.RegistryExpiry() != options.RegistryExpiry || built.ClaimTTL() != options.ClaimTTL ||
		built.ApplyDeadline() != options.ApplyDeadline || built.ReconcileInterval() != options.ReconcileInterval {
		t.Error("a duration accessor does not return the value supplied")
	}
	if built.CommandQueueSize() != options.CommandQueueSize || built.ReconcileBatch() != options.ReconcileBatch {
		t.Errorf("size accessors = (%d, %d)", built.CommandQueueSize(), built.ReconcileBatch())
	}
}

// TestNewDelegatesEveryRefusal drives every rule's spoiler through BOTH
// constructors and holds the exported refusal to the internal one on code,
// field, reason and cause. The spoilers are the same set the internal
// package's own reachability test uses, so a rule added there without a
// spoiler here is noticed by that test's count rather than silently.
func TestNewDelegatesEveryRefusal(t *testing.T) {
	t.Parallel()
	spoilers := []func(*host.Options){
		func(o *host.Options) { o.HostID = "" },
		func(o *host.Options) { o.SessionStore = nil },
		func(o *host.Options) { o.InternalEndpoint = "http://host/x" },
		func(o *host.Options) { o.InternalEndpoint = "ws://10.0.0.1:7100/hostlink" },
		func(o *host.Options) { o.InternalEndpoint = "ws://10.0.0.1:99999" },
		func(o *host.Options) { o.HostID = sessionwire.HostID(strings.Repeat("h", sessionwire.MaxIDBytes+1)) },
		func(o *host.Options) { o.IsolationClass = "shared" },
		func(o *host.Options) { o.Placement = "elastic" },
		func(o *host.Options) { o.Capacity = 0 },
		func(o *host.Options) { o.WarmTTL = 0 },
		func(o *host.Options) { o.CommandQueueSize = host.MaxCommandQueueSize + 1 },
		func(o *host.Options) { o.RegistryExpiry = o.RegistryHeartbeat },
		func(o *host.Options) { o.ApplyDeadline = o.ClaimTTL },
		func(o *host.Options) { o.Placement = sessionwire.HostPlacementDedicated; o.Capacity = 1 },
		func(o *host.Options) {
			o.Placement = sessionwire.HostPlacementDedicated
			o.FixedSessionID = "session-71c"
			o.Capacity = 4
		},
		func(o *host.Options) {
			o.Placement = sessionwire.HostPlacementDedicated
			o.FixedSessionID = sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes+1))
			o.Capacity = 1
		},
		func(o *host.Options) { o.FixedSessionID = "session-71c" },
	}
	seen := map[host.OptionErrorCode]bool{}
	for index, spoil := range spoilers {
		options := facadeOptions(t)
		spoil(&options)
		_, err := host.New(options)
		var exported *host.InvalidOptionsError
		if !errors.As(err, &exported) {
			t.Fatalf("spoiler %d: New = %v, want *host.InvalidOptionsError", index, err)
		}
		_, internalErr := hostconfig.New(internalOptions(options))
		var internal *hostconfig.InvalidOptionsError
		if !errors.As(internalErr, &internal) {
			t.Fatalf("spoiler %d: hostconfig.New = %v, want *hostconfig.InvalidOptionsError", index, internalErr)
		}
		if string(exported.Code) != string(internal.Code) || exported.Field != internal.Field || exported.Reason != internal.Reason {
			t.Errorf("spoiler %d: host reports (%q, %q, %q) and hostconfig (%q, %q, %q)", index,
				exported.Code, exported.Field, exported.Reason, internal.Code, internal.Field, internal.Reason)
		}
		if (exported.Cause == nil) != (internal.Cause == nil) || (internal.Cause != nil && exported.Cause.Error() != internal.Cause.Error()) {
			t.Errorf("spoiler %d: causes differ: %v vs %v", index, exported.Cause, internal.Cause)
		}
		if exported.Error() != internal.Error() {
			t.Errorf("spoiler %d: messages differ: %q vs %q", index, exported.Error(), internal.Error())
		}
		if exported.Cause != nil && !errors.Is(err, exported.Cause) {
			t.Errorf("spoiler %d: the exported refusal does not unwrap to its cause", index)
		}
		seen[exported.Code] = true
	}
	// Every code the package publishes was reached by some spoiler, so a code
	// that no rule produces any more is noticed here.
	for _, code := range []host.OptionErrorCode{
		host.OptionErrorCodeMissing, host.OptionErrorCodeInvalid, host.OptionErrorCodeUnknownEnum,
		host.OptionErrorCodeNotPositive, host.OptionErrorCodeAboveBound, host.OptionErrorCodeHeartbeatMargin,
		host.OptionErrorCodeClaimMargin, host.OptionErrorCodeDedicatedFixed, host.OptionErrorCodeDedicatedCap,
		host.OptionErrorCodePooledFixed,
	} {
		if !seen[code] {
			t.Errorf("no spoiler reached %q through New", code)
		}
	}
}

// internalOptions spells host.Options as hostconfig.Options for the comparison
// above. It is written out in the test rather than borrowed from production so
// the two spellings are independent.
func internalOptions(o host.Options) hostconfig.Options {
	return hostconfig.Options{
		HostID: o.HostID, InternalEndpoint: o.InternalEndpoint, IsolationClass: o.IsolationClass,
		Department: o.Department, SessionStore: o.SessionStore, Workspaces: o.Workspaces, Clock: o.Clock, Auth: o.Auth,
		Placement: o.Placement, Capacity: o.Capacity, FixedSessionID: o.FixedSessionID,
		WarmTTL: o.WarmTTL, RegistryHeartbeat: o.RegistryHeartbeat, RegistryExpiry: o.RegistryExpiry,
		ClaimTTL: o.ClaimTTL, ApplyDeadline: o.ApplyDeadline, CommandQueueSize: o.CommandQueueSize,
		ReconcileInterval: o.ReconcileInterval, ReconcileBatch: o.ReconcileBatch,
	}
}

// TestTheExportedConstructorRefusesANonBaseEndpointTypedly is v0.3.0's
// endpoint rule seen from outside the module: a v0.2.1-style per-tenant
// endpoint reaches Core's *HostLinkEndpointError with its Code, and an
// undiallable authority reaches host.ErrUndiallableEndpoint, so a caller
// branches on neither the Reason nor the internal package.
func TestTheExportedConstructorRefusesANonBaseEndpointTypedly(t *testing.T) {
	t.Parallel()

	options := facadeOptions(t)
	options.InternalEndpoint = "ws://10.0.0.1:7100/hostlink/tenant-a"
	_, err := host.New(options)
	var derive *sessionwire.HostLinkEndpointError
	if !errors.As(err, &derive) || derive.Code != sessionwire.HostLinkEndpointCodeBaseNamesTenant {
		t.Fatalf("New(per-tenant endpoint) = %v, want a *HostLinkEndpointError with code base_names_tenant", err)
	}

	options = facadeOptions(t)
	options.InternalEndpoint = "ws://10.0.0.1:80:90"
	_, err = host.New(options)
	if !errors.Is(err, host.ErrUndiallableEndpoint) {
		t.Fatalf("New(two ports) = %v, want host.ErrUndiallableEndpoint", err)
	}
}
