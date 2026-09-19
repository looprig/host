package hostconfig

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
)

// ---------------------------------------------------------------------------
// Collaborators Host requires
// ---------------------------------------------------------------------------
//
// Each is a NARROW LOCAL interface, for the reason department.Rig is: naming a
// looprig module in go.mod is the same decision as depending on it, and
// publishedLooprigVersions is the list that decision is recorded in. These
// describe what Host needs; the concrete edges are later tasks.

// SessionStore is the durable session state Host reads and writes.
type SessionStore interface {
	// LoadSession returns the durable record for a session, or an error the
	// caller can classify.
	LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error)
}

// WorkspaceProvider materializes session workspaces.
//
// It does NOT checkpoint them. An earlier version of this comment said it did,
// which is the doc-versus-reality drift host/CLAUDE.md exists to stop and the
// third time this lane has shipped a comment promising more than the code
// holds. Checkpointing arrives with O4.x/O6.x and will be its own method or its
// own interface; until then this says only what EnsureWorkspace does.
type WorkspaceProvider interface {
	// EnsureWorkspace makes a workspace available and returns its root.
	EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error)
}

// Clock is the time source.
//
// It is FORWARD-DECLARED for O2.x's residency, heartbeat and drain timers,
// which is why NewTimer exists with no caller yet. An earlier version of this
// comment justified it as making "the timing rules below" testable without
// sleeping; those rules are pure arithmetic over the Options and never consult
// a clock, so the justification described a task that had not landed. Requiring
// it now is deliberate — a Host that reaches residency without a Clock would
// have to be reconstructed — but it is a forward dependency and says so.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) *time.Timer
}

// AuthVerifier authenticates an inbound HostLink or command caller.
type AuthVerifier interface {
	// VerifyTenant reports whether a presented credential acts for the tenant.
	VerifyTenant(context.Context, sessionwire.TenantID, string) error
}

// ---------------------------------------------------------------------------
// Timing margins
// ---------------------------------------------------------------------------

// MinHeartbeatsBeforeExpiry is how many heartbeat intervals must fit inside the
// registry expiry.
//
// "Safely below" is a MARGIN, not `<`, and the difference is the whole rule. A
// heartbeat one nanosecond under the expiry satisfies `<` and loses the
// registry entry on the first scheduling hiccup, GC pause or network retry —
// which is to say, immediately, in production. Three means two consecutive
// heartbeats may be late or lost before Factory stops seeing this Host, which
// is the smallest number that tolerates a single miss AND the retry of it.
const MinHeartbeatsBeforeExpiry = 3

// MinClaimAttemptsBeforeDeadline is how many claim lifetimes must fit inside
// the apply deadline.
//
// A claim that lapses mid-apply is retried by another Host. If only one claim
// lifetime fits inside the deadline, the first lapse spends the entire budget
// and the command misses its deadline with no attempt left; two means a lapsed
// claim still has a whole attempt in front of it.
//
// THIS IS STRICTER THAN THE RUNBOOK, and the difference is not derived from
// anything written down. 04-host.md says "claim TTL below apply deadline" —
// merely below — and reserves the words "safely below" for the heartbeat rule
// alone. That asymmetry is deliberate on the runbook's part, so this constant
// is an engineering choice and not a reading of the spec: it will REFUSE
// DEPLOYMENTS THE RUNBOOK PERMITS, such as a 30s claim under a 45s deadline.
//
// The failure model it is chosen against: at 1.5x, a claim that lapses at the
// end of its life leaves half an attempt before the deadline, so the retry is
// guaranteed to be cut off and the command fails having consumed two Hosts. At
// 2x the retry has a full attempt. If a deployment needs the looser rule the
// runbook allows, change THIS CONSTANT — do not weaken validateTiming, which is
// what makes the boundary testable.
const MinClaimAttemptsBeforeDeadline = 2

// The two size bounds below are DELIBERATELY DIFFERENT VALUES, and a test
// asserts that they differ. They bound different things for different reasons,
// and if they ever coincided, swapping one for the other in validateShape would
// become undetectable — which is exactly the mutation that survived this file's
// first version, because the only bound row exceeded either constant.

// MaxCommandQueueSize bounds the command queue. A queue is bounded because an
// unbounded one converts backpressure into memory growth and turns a slow
// consumer into an OOM three minutes later, which is harder to diagnose than a
// refusal.
const MaxCommandQueueSize = 65536

// MaxReconcileBatch bounds one reconciliation pass, so a Host that has drifted
// far does not attempt to converge in a single unbounded sweep.
const MaxReconcileBatch = 4096

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Options configures a Host. Every field is required unless its documentation
// says otherwise; nothing here is discovered, and there is no package-global
// default Host to fall back on.
type Options struct {
	// HostID identifies this Host to Factory and in HostLink bindings.
	HostID sessionwire.HostID

	// InternalEndpoint is the credential-free HostLink BASE address Factory
	// derives each tenant's address from; see validateEndpointBase.
	InternalEndpoint sessionwire.InternalEndpoint

	// IsolationClass is advertised with capacity so Factory can place
	// cross-tenant work correctly, AND IT IS THIS HOST'S POOLED ADMISSION RULE.
	// Pooled placement alone does not establish the boundary.
	//
	// THERE IS NO TenantID BESIDE IT, and that is human gate H8, answered
	// 2026-09-04 as option (a). A Host used to be constructed with a fixed
	// tenant and the residency manager refused every request naming another, so
	// a pooled deployment needed one Host Deployment per tenant and this field
	// — which spec §12 says decides the question — had no reader that could
	// matter. A Host advertising cross_tenant_isolated may hold several tenants
	// resident; one advertising tenant_exclusive is exclusive by PLACEMENT, not
	// by construction, which is §12's own wording: "Factory placement enforces
	// the restriction". internal/service's admission ledger holds the local
	// backstop for it, atomically, because the ledger's lock is the only
	// Host-wide critical section an admission passes through.
	IsolationClass sessionwire.HostIsolationClass

	// Department is the immutable set of launch targets this Host serves.
	Department *department.Department

	// SessionStore, Workspaces, Clock and Auth are Host's collaborators.
	SessionStore SessionStore
	Workspaces   WorkspaceProvider
	Clock        Clock
	Auth         AuthVerifier

	// Placement is the admission model this Host runs under.
	Placement sessionwire.HostPlacement

	// Capacity is how many sessions this Host may hold resident. Dedicated
	// placement requires exactly one.
	Capacity uint64

	// FixedSessionID binds a dedicated Host to one session for its lifetime. It
	// is REQUIRED for dedicated placement and REJECTED for pooled.
	FixedSessionID sessionwire.SessionID

	// WarmTTL is how long a released session stays warm before drain.
	WarmTTL time.Duration

	// RegistryHeartbeat and RegistryExpiry are the advertisement lifetime.
	RegistryHeartbeat time.Duration
	RegistryExpiry    time.Duration

	// ClaimTTL and ApplyDeadline bound command application.
	ClaimTTL      time.Duration
	ApplyDeadline time.Duration

	// CommandQueueSize is the inbound command buffer.
	CommandQueueSize int

	// ReconcileInterval and ReconcileBatch bound reconciliation.
	ReconcileInterval time.Duration
	ReconcileBatch    int
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// OptionErrorCode is a stable, machine-readable reason an option was refused.
//
// It exists for the reason department.DefinitionErrorCode does: Reason is free
// text a caller must not branch on, and a constructor that refuses a dozen ways
// is useless to a caller who cannot tell which one happened without matching a
// sentence that will be reworded.
type OptionErrorCode string

const (
	OptionErrorCodeMissing         OptionErrorCode = "missing"
	OptionErrorCodeInvalid         OptionErrorCode = "invalid"
	OptionErrorCodeUnknownEnum     OptionErrorCode = "unknown_enum"
	OptionErrorCodeNotPositive     OptionErrorCode = "not_positive"
	OptionErrorCodeAboveBound      OptionErrorCode = "above_bound"
	OptionErrorCodeHeartbeatMargin OptionErrorCode = "heartbeat_margin"
	OptionErrorCodeClaimMargin     OptionErrorCode = "claim_margin"
	OptionErrorCodeDedicatedFixed  OptionErrorCode = "dedicated_requires_fixed_session"
	OptionErrorCodeDedicatedCap    OptionErrorCode = "dedicated_requires_capacity_one"
	OptionErrorCodePooledFixed     OptionErrorCode = "pooled_rejects_fixed_session"
)

// InvalidOptionsError reports an option Host may not run with.
//
// Code is what a caller branches on, Field names the option, Reason is for a
// person, and Cause carries the typed error a lower layer produced — Core's
// *RequestValidationError for an endpoint, for instance — so errors.As reaches
// it instead of a caller substring-matching a sentence.
type InvalidOptionsError struct {
	Code   OptionErrorCode
	Field  string
	Reason string
	Cause  error
}

func (e *InvalidOptionsError) Error() string {
	return "host: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Unwrap returns the typed error this one was built from, if any.
func (e *InvalidOptionsError) Unwrap() error { return e.Cause }

// missing builds the absent-required-option error, spelled once so every
// presence rule reports the same code and shape.
func missing(field string) error {
	return &InvalidOptionsError{Code: OptionErrorCodeMissing, Field: field, Reason: "must be set"}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// validate holds every option rule, in one place, so a new rule has one site
// and the constructor has one call.
//
// The order is deliberate: presence first, then per-field shape, then the
// CROSS-FIELD rules. A cross-field rule reached with a missing field would
// report the relationship when the real fault is the absence, which sends the
// reader to the wrong option.
func (o Options) validate() error {
	if err := o.validatePresence(); err != nil {
		return err
	}
	if err := o.validateShape(); err != nil {
		return err
	}
	if err := o.validateTiming(); err != nil {
		return err
	}
	return o.validatePlacement()
}

func (o Options) validatePresence() error {
	if o.HostID == "" {
		return missing("HostID")
	}
	if o.InternalEndpoint == "" {
		return missing("InternalEndpoint")
	}
	if o.Department == nil {
		return missing("Department")
	}
	if o.SessionStore == nil {
		return missing("SessionStore")
	}
	if o.Workspaces == nil {
		return missing("Workspaces")
	}
	if o.Clock == nil {
		return missing("Clock")
	}
	if o.Auth == nil {
		return missing("Auth")
	}
	return nil
}

func (o Options) validateShape() error {
	if err := validateEndpointBase(o.InternalEndpoint); err != nil {
		return err
	}
	// DELEGATED to Core, not restated. HostID.Validate and SessionID.Validate
	// are exported and each calls Core's validateID, so this enforces Core's
	// rule BY CALLING IT and cannot drift if Core changes it. TenantID took the
	// same treatment until H8 removed the field; the rule did not go with it,
	// it MOVED — residency.Manager.validateRequest now calls
	// request.TenantID.Validate on the tenant each attach names, which is where
	// a tenant now enters this Host.
	//
	// TWO of validateID's three arms are reachable here: over MaxIDBytes and
	// invalid UTF-8. The EMPTY arm is delegated but SHADOWED, because
	// validatePresence runs first and reports OptionErrorCodeMissing — which is
	// the better error, and deliberately so. Core's generic "empty" would tell
	// an operator less than "HostID must be set", and for FixedSessionID the
	// shadowing code is DedicatedRequiresFixedSession, which names the actual
	// rule. An earlier version of this comment claimed all three arms in Core's
	// own order; the layering is right and the sentence was not.
	//
	// O1.2's CompatibilityID.Validate had to hand-restate those three arms
	// because Core exports no named type for that field, and a copy has nothing
	// keeping it honest — that restatement needs a drift test and has one.
	// Here there is a type to delegate to, so there is no copy to keep honest.
	//
	// This is the IsolationClass ruling applied to the identifiers, and the
	// consequence is worse than "fails at first advertisement". Core calls
	// validateHostLinkHost from SIX sites in hostlink.go — HostLinkBindRequest,
	// HostLinkUnbindRequest, HostLinkCapacityReport, HostLinkRegistryObservation,
	// HostLinkDrainRequest and HostLinkDrainObservation — so an over-long HostID
	// produced a Host that could neither ADVERTISE nor DRAIN. It could not shut
	// down cleanly, which is the state you least want to discover in production.
	// Enforcing it here turns that into a failure at New, where the operator is
	// still holding the configuration.
	for _, identity := range []struct {
		field    string
		validate func() error
	}{
		{"HostID", o.HostID.Validate},
	} {
		if err := identity.validate(); err != nil {
			return &InvalidOptionsError{
				Code:   OptionErrorCodeInvalid,
				Field:  identity.field,
				Reason: "is not a permitted sessionwire identity: " + err.Error(),
				Cause:  err,
			}
		}
	}
	switch o.IsolationClass {
	case sessionwire.HostIsolationClassCrossTenantIsolated, sessionwire.HostIsolationClassTenantExclusive:
	default:
		// The zero value lands here, and this is REQUIRED rather than a
		// judgement about defaults. Core's validateHostIsolationClass
		// (sessionwire/v1/hostlink.go) accepts only these two constants and
		// returns invalid_field for anything else including the empty string;
		// HostLinkCapacityReport.Validate calls it, and decodeHostIsolationClass
		// requires the wire member outright. A Host that defaulted this field
		// could not publish a capacity report, so refusing it here turns a
		// failure at first advertisement into a failure at construction.
		return &InvalidOptionsError{
			Code:   OptionErrorCodeUnknownEnum,
			Field:  "IsolationClass",
			Reason: "must be " + string(sessionwire.HostIsolationClassCrossTenantIsolated) + " or " + string(sessionwire.HostIsolationClassTenantExclusive) + "; Core rejects any other value on the capacity report, so a Host without one could never advertise",
		}
	}
	switch o.Placement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
	default:
		return &InvalidOptionsError{
			Code:   OptionErrorCodeUnknownEnum,
			Field:  "Placement",
			Reason: "must be " + string(sessionwire.HostPlacementPooled) + " or " + string(sessionwire.HostPlacementDedicated),
		}
	}
	if o.Capacity == 0 {
		return &InvalidOptionsError{Code: OptionErrorCodeNotPositive, Field: "Capacity", Reason: "must be at least one; a Host that can hold nothing resident cannot serve"}
	}
	for _, duration := range []struct {
		field string
		value time.Duration
	}{
		{"WarmTTL", o.WarmTTL},
		{"RegistryHeartbeat", o.RegistryHeartbeat},
		{"RegistryExpiry", o.RegistryExpiry},
		{"ClaimTTL", o.ClaimTTL},
		{"ApplyDeadline", o.ApplyDeadline},
		{"ReconcileInterval", o.ReconcileInterval},
	} {
		if duration.value <= 0 {
			return &InvalidOptionsError{Code: OptionErrorCodeNotPositive, Field: duration.field, Reason: "must be a positive duration"}
		}
	}
	// Each bound carries ITS OWN rationale. Sharing the loop is fine; sharing
	// the sentence was not, and it was user-visible: ReconcileBatch reported
	// the queue's backpressure argument, which is not why a reconciliation
	// sweep is bounded.
	for _, size := range []struct {
		field  string
		value  int
		bound  int
		reason string
	}{
		{"CommandQueueSize", o.CommandQueueSize, MaxCommandQueueSize, "an unbounded queue turns backpressure into memory growth, so a slow consumer becomes an OOM rather than a refusal"},
		{"ReconcileBatch", o.ReconcileBatch, MaxReconcileBatch, "a Host that has drifted far must not try to converge in one unbounded sweep, which would starve everything else it owes"},
	} {
		if size.value <= 0 {
			return &InvalidOptionsError{Code: OptionErrorCodeNotPositive, Field: size.field, Reason: "must be positive"}
		}
		if size.value > size.bound {
			return &InvalidOptionsError{
				Code:   OptionErrorCodeAboveBound,
				Field:  size.field,
				Reason: "must be at most " + strconv.Itoa(size.bound) + "; " + size.reason,
			}
		}
	}
	return nil
}

// validateTiming holds the two relationships that are the point of this
// constructor. Both are MARGINS, not comparisons: see MinHeartbeatsBeforeExpiry
// and MinClaimAttemptsBeforeDeadline for why `<` is the bug rather than the
// rule.
func (o Options) validateTiming() error {
	// DIVIDE, do not multiply. The product overflows: a heartbeat above
	// MaxInt64/MinHeartbeatsBeforeExpiry wraps negative, `expiry < negative` is
	// false, and the configuration is ACCEPTED — a constructor whose whole job
	// is refusing incoherent configurations, failing open on the one input
	// class it cannot represent.
	//
	// PRECONDITION: the DIVIDEND must be non-negative. Only the dividend; the
	// divisor's sign is irrelevant. `e/N < h` and `e < h*N` agree for every
	// input except a negative dividend — at e=-5, h=-1, N=3 the product says
	// refuse and the division says accept — and in every divergent case the
	// division is the MORE PERMISSIVE form. Zero is safe.
	//
	// validateShape guarantees it by refusing non-positive durations, and it
	// runs first. THE HAZARD IS NOT A REORDER: swapping the phases changes
	// which error a caller gets, never whether they get one, because
	// validateShape still runs and still refuses. The hazard is a NEW CALLER —
	// this is an unexported method, and any future O2.x code in this package
	// can call it directly, where nothing has established the precondition.
	// Call validate, or establish it yourself.
	if o.RegistryExpiry/MinHeartbeatsBeforeExpiry < o.RegistryHeartbeat {
		return &InvalidOptionsError{
			Code:  OptionErrorCodeHeartbeatMargin,
			Field: "RegistryExpiry",
			Reason: "RegistryExpiry (" + o.RegistryExpiry.String() + ") must be at least " +
				strconv.Itoa(MinHeartbeatsBeforeExpiry) + " times RegistryHeartbeat (" + o.RegistryHeartbeat.String() +
				"), so a missed heartbeat and its retry both fit before Factory stops seeing this Host",
		}
	}
	if o.ApplyDeadline/MinClaimAttemptsBeforeDeadline < o.ClaimTTL {
		return &InvalidOptionsError{
			Code:  OptionErrorCodeClaimMargin,
			Field: "ApplyDeadline",
			Reason: "ApplyDeadline (" + o.ApplyDeadline.String() + ") must be at least " +
				strconv.Itoa(MinClaimAttemptsBeforeDeadline) + " times ClaimTTL (" + o.ClaimTTL.String() +
				"), so a claim that lapses mid-apply still leaves a whole attempt inside the deadline",
		}
	}
	return nil
}

// validatePlacement holds the cross-field binding rule. Dedicated placement is
// a Host built for ONE session: without the binding it is a pooled Host wearing
// the wrong label, and with capacity above one it would admit a second session
// it has no identity for.
func (o Options) validatePlacement() error {
	if o.Placement == sessionwire.HostPlacementDedicated {
		if o.FixedSessionID == "" {
			return &InvalidOptionsError{
				Code:   OptionErrorCodeDedicatedFixed,
				Field:  "FixedSessionID",
				Reason: "dedicated placement is a binding to one session and must name it",
			}
		}
		// FixedSessionID is a SessionID and takes the same rule, delegated the
		// same way. It is checked here rather than in validateShape because
		// only the dedicated branch requires it to be present at all; the
		// pooled branch requires the opposite.
		if err := o.FixedSessionID.Validate(); err != nil {
			return &InvalidOptionsError{
				Code:   OptionErrorCodeInvalid,
				Field:  "FixedSessionID",
				Reason: "is not a permitted sessionwire identity: " + err.Error(),
				Cause:  err,
			}
		}
		if o.Capacity != 1 {
			return &InvalidOptionsError{
				Code:   OptionErrorCodeDedicatedCap,
				Field:  "Capacity",
				Reason: "dedicated placement admits exactly one session, so capacity must be 1",
			}
		}
		return nil
	}
	if o.FixedSessionID != "" {
		return &InvalidOptionsError{
			Code:   OptionErrorCodePooledFixed,
			Field:  "FixedSessionID",
			Reason: "pooled placement accepts whatever Factory sends and must not be bound to one session",
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// The product seam
// ---------------------------------------------------------------------------
//
// Host is a GENERIC runtime host and the agents it serves are a product's. The
// two types below are the two a product supplies FROM OUTSIDE THIS MODULE, and
// they are declared here rather than in the composition for one reason:
// internal/ is not importable from outside this module, so a seam declared
// there could only ever be satisfied by this module's own binary.
//
// THEY ARE NOT YET THE WHOLE OF WHAT A PRODUCT SUPPLIES, and saying so would
// be false in this same commit. cmd/host's Bootstrap asks for five more — an
// authenticator, a workspace provider, an inbox, a cursor store and the store
// itself — and four of those are typed in internal/, so a product cannot
// satisfy Bootstrap at all today. That is tracked against the sessionstore
// release the inbox and cursor seams are waiting on; see cmd/host/main.go.

// Registrar produces the Department registrations one deployment serves.
//
// IT IS CALLED ONCE, AT STARTUP, AND ITS RESULT IS IMMUTABLE. Department.New
// validates the registrations and fixes them for the life of the process, so a
// Registrar that answered differently later would be answering a question
// nobody asks again. It takes a context because building a launch target may
// require I/O — reading a model catalogue, resolving a runtime build — and a
// startup that can block must be cancellable.
type Registrar interface {
	Register(context.Context) ([]department.Registration, error)
}

// RegistrarFunc adapts a function to Registrar.
type RegistrarFunc func(context.Context) ([]department.Registration, error)

// Register calls f.
func (f RegistrarFunc) Register(ctx context.Context) ([]department.Registration, error) {
	return f(ctx)
}

// Checkpointer commits the checkpoints a nonterminal release requires.
//
// IT HAS NO IMPLEMENTATION IN THIS MODULE AND THAT IS A STATEMENT ABOUT WHERE
// THE KNOWLEDGE LIVES, not an omission to be filled in with a no-op. A release
// checkpoint is the runtime's conversation state and the workspace's contents,
// and Host holds neither: department.Runtime exposes no checkpoint method and
// WorkspaceProvider materializes a workspace without ever committing one. A
// composition that silently treated the step as done would report a drained
// Host whose sessions had lost whatever was not already durable, which is the
// one drain failure that destroys work.
//
// So the seam is REQUIRED rather than optional. A Host composed without one
// does not start.
type Checkpointer interface {
	Checkpoint(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}

// ---------------------------------------------------------------------------
// The internal endpoint is a BASE
// ---------------------------------------------------------------------------

// ErrUndiallableEndpoint is the cause of an InternalEndpoint refusal whose
// authority no client could dial — an empty port ("ws://h:"), a port outside
// 1..65535 ("ws://h:99999", "ws://h:0"), or more than one port
// ("ws://h:80:90") — although Core's InternalEndpoint.Validate and
// HostLinkEndpoint both accept it. Reach it with errors.Is.
var ErrUndiallableEndpoint = errors.New("host: the internal endpoint's authority cannot be dialled")

// endpointProbeTenant is the tenant validateEndpointBase derives with. It is
// the shortest legal, routable tenant, so the only refusals HostLinkEndpoint
// can report for it are about the BASE — including too_long, which for a
// one-byte tenant means no tenant at all fits on this base.
const endpointProbeTenant sessionwire.TenantID = "t"

// validateEndpointBase holds host v0.3.0's reading of InternalEndpoint.
//
// THE VALUE IS A BASE, NOT AN ADDRESS. Host serves each tenant's HostLink at
// sessionwire.HostLinkPathPrefix plus that tenant, and Core owns the one
// derivation a Factory uses to reach it, sessionwire.HostLinkEndpoint(base,
// tenant). So the rule is Core's, applied BY CALLING IT rather than restated:
// a base Core would refuse to derive from — invalid_base, base_names_tenant
// (the v0.2.1 per-tenant spelling ".../hostlink" or ".../hostlink/<tenant>"),
// base_not_bare (any other path, an ingress prefix, an empty fragment), or one
// so long that no tenant fits (too_long) — is refused here, loudly at startup,
// instead of being advertised and then refused by every Factory that reads it.
// The *sessionwire.HostLinkEndpointError is carried as the cause so a caller
// branches on its Code; for invalid_base it wraps Core's
// *RequestValidationError in turn.
//
// THEN THE AUTHORITY MUST BE DIALLABLE, which Core does not check (core spec
// F4): Validate accepts "ws://h:", "ws://h:99999" and "ws://h:80:90", and
// HostLinkEndpoint appends to them faithfully. Each is an operator error that
// would otherwise surface only as a Factory dial failure.
func validateEndpointBase(endpoint sessionwire.InternalEndpoint) error {
	if _, err := sessionwire.HostLinkEndpoint(endpoint, endpointProbeTenant); err != nil {
		return &InvalidOptionsError{
			Code:   OptionErrorCodeInvalid,
			Field:  "InternalEndpoint",
			Reason: "is not a usable HostLink base address (a ws or wss scheme and an authority, nothing else; each tenant's address is derived from it): " + err.Error(),
			Cause:  err,
		}
	}
	if err := dialableAuthority(endpoint); err != nil {
		return &InvalidOptionsError{
			Code:   OptionErrorCodeInvalid,
			Field:  "InternalEndpoint",
			Reason: err.Error(),
			Cause:  err,
		}
	}
	return nil
}

// dialableAuthority refuses an authority no client could dial. It runs after
// HostLinkEndpoint accepted the base, so the base parses and has a host.
func dialableAuthority(endpoint sessionwire.InternalEndpoint) error {
	parsed, err := url.Parse(string(endpoint))
	if err != nil {
		return errors.Join(ErrUndiallableEndpoint, err)
	}
	authority := parsed.Host
	if strings.HasSuffix(authority, ":") {
		return errors.Join(ErrUndiallableEndpoint, errors.New("the authority names an empty port"))
	}
	if parsed.Port() == "" {
		return nil
	}
	// SplitHostPort refuses a second port ("h:80:90") as too many colons, and
	// accepts a bracketed IPv6 literal with one.
	if _, _, err := net.SplitHostPort(authority); err != nil {
		return errors.Join(ErrUndiallableEndpoint, err)
	}
	port, err := strconv.ParseUint(parsed.Port(), 10, 16)
	if err != nil || port == 0 {
		return errors.Join(ErrUndiallableEndpoint, errors.New("the port must be a decimal number from 1 to 65535"))
	}
	return nil
}
