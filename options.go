package host

import (
	"context"
	"strconv"
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

	// TenantID scopes every session this Host may hold resident.
	TenantID sessionwire.TenantID

	// InternalEndpoint is the credential-free WebSocket address Factory dials.
	InternalEndpoint sessionwire.InternalEndpoint

	// IsolationClass is advertised with capacity so Factory can place
	// cross-tenant work correctly. Pooled placement alone does not establish it.
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
	if o.TenantID == "" {
		return missing("TenantID")
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
	// Core owns this rule and reports a typed *RequestValidationError whose
	// Code it documents as stable. The cause is carried rather than folded into
	// a sentence, so a caller can tell a credential-bearing endpoint from a
	// malformed one without matching text.
	if err := o.InternalEndpoint.Validate(); err != nil {
		return &InvalidOptionsError{
			Code:   OptionErrorCodeInvalid,
			Field:  "InternalEndpoint",
			Reason: "is not a usable HostLink address: " + err.Error(),
			Cause:  err,
		}
	}
	// DELEGATED to Core, not restated. HostID.Validate, TenantID.Validate and
	// SessionID.Validate are exported and each calls Core's validateID, so this
	// enforces Core's rule BY CALLING IT: empty, over MaxIDBytes, and invalid
	// UTF-8, in Core's own order, and it cannot drift if Core changes them.
	//
	// O1.2's CompatibilityID.Validate had to hand-restate those three arms
	// because Core exports no named type for that field, and a copy has nothing
	// keeping it honest — that restatement needs a drift test and has one.
	// Here there is a type to delegate to, so there is no copy to keep honest.
	//
	// This is the IsolationClass ruling applied to the identifiers. Core calls
	// validateHostLinkHost from three sites in hostlink.go, so a 300-byte
	// HostID constructed fine and failed at FIRST ADVERTISEMENT on every path
	// that advertises. Enforcing it here turns that into a failure at New,
	// where the operator is still holding the configuration.
	for _, identity := range []struct {
		field    string
		validate func() error
	}{
		{"HostID", o.HostID.Validate},
		{"TenantID", o.TenantID.Validate},
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
