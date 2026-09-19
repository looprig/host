package host

import (
	"context"
	"errors"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
)

// ---------------------------------------------------------------------------
// Collaborators Host requires
// ---------------------------------------------------------------------------
//
// Each is a NARROW LOCAL interface, for the reason department.Rig is: naming a
// looprig module in go.mod is the same decision as depending on it. These
// describe what Host needs; internal/sessionstoreadapter binds the released
// store to them. They are declared here, in the exported package, and again in
// internal/hostconfig with identical method sets — a value of one satisfies
// the other — and TestOptionsFieldsMirrorTheResolvedConfiguration holds the
// two declarations to each other.

// SessionStore is the durable session state Host reads and writes.
type SessionStore interface {
	// LoadSession returns the durable record for a session, or an error the
	// caller can classify.
	LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error)
}

// WorkspaceProvider materializes session workspaces.
//
// It does NOT checkpoint them; see Checkpointer for that half. Nor does it
// release them: the composition needs the release half too, which is
// Workspaces, and it is Workspaces a composition takes.
type WorkspaceProvider interface {
	// EnsureWorkspace makes a workspace available and returns its root.
	EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error)
}

// Clock is the time source.
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
// registry expiry. "Safely below" is a MARGIN, not `<`: three means two
// consecutive heartbeats may be late or lost before Factory stops seeing this
// Host. The rule is internal/hostconfig's; this is its published value.
const MinHeartbeatsBeforeExpiry = hostconfig.MinHeartbeatsBeforeExpiry

// MinClaimAttemptsBeforeDeadline is how many claim lifetimes must fit inside
// the apply deadline, so a claim that lapses mid-apply still leaves a whole
// attempt inside the deadline. It is stricter than the runbook's "below";
// see internal/hostconfig for the failure model it is chosen against.
const MinClaimAttemptsBeforeDeadline = hostconfig.MinClaimAttemptsBeforeDeadline

// MaxCommandQueueSize bounds the command queue: an unbounded one converts
// backpressure into memory growth.
const MaxCommandQueueSize = hostconfig.MaxCommandQueueSize

// MaxReconcileBatch bounds one reconciliation pass, so a Host that has drifted
// far does not attempt to converge in a single unbounded sweep.
const MaxReconcileBatch = hostconfig.MaxReconcileBatch

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Options configures a Host. Every field is required unless its documentation
// says otherwise; nothing here is discovered, and there is no package-global
// default Host to fall back on.
type Options struct {
	// HostID identifies this Host to Factory and in HostLink bindings.
	HostID sessionwire.HostID

	// InternalEndpoint is this Host's credential-free HostLink BASE address:
	// a ws or wss scheme and an authority, with no path. Factory derives each
	// tenant's address from it with sessionwire.HostLinkEndpoint(base,
	// tenant), which is base + HostLinkPathPrefix + the escaped tenant. As of
	// v0.3.0 a value HostLinkEndpoint would refuse (including the v0.2.1
	// per-tenant spelling ".../hostlink/<tenant>") or one whose authority
	// cannot be dialled is refused by New; see ErrUndiallableEndpoint.
	InternalEndpoint sessionwire.InternalEndpoint

	// IsolationClass is advertised with capacity so Factory can place
	// cross-tenant work correctly, AND IT IS THIS HOST'S POOLED ADMISSION RULE.
	//
	// THERE IS NO TenantID BESIDE IT, and that is human gate H8, answered
	// 2026-09-04: a Host advertising cross_tenant_isolated may hold several
	// tenants resident; one advertising tenant_exclusive is exclusive by
	// PLACEMENT, not by construction. The admission ledger holds the local
	// backstop.
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

// resolved is the internal spelling of these options, member by member.
//
// IT IS WRITTEN OUT AND NOT CONVERTED, because the two structs are not
// convertible: their collaborator fields are interfaces declared in two
// packages, identical in shape and distinct in identity. A field added to
// either struct and not carried here would be silently dropped, which is why
// TestOptionsFieldsMirrorTheResolvedConfiguration holds the two field sets to
// each other by reflection.
func (o Options) resolved() hostconfig.Options {
	return hostconfig.Options{
		HostID:            o.HostID,
		InternalEndpoint:  o.InternalEndpoint,
		IsolationClass:    o.IsolationClass,
		Department:        o.Department,
		SessionStore:      o.SessionStore,
		Workspaces:        o.Workspaces,
		Clock:             o.Clock,
		Auth:              o.Auth,
		Placement:         o.Placement,
		Capacity:          o.Capacity,
		FixedSessionID:    o.FixedSessionID,
		WarmTTL:           o.WarmTTL,
		RegistryHeartbeat: o.RegistryHeartbeat,
		RegistryExpiry:    o.RegistryExpiry,
		ClaimTTL:          o.ClaimTTL,
		ApplyDeadline:     o.ApplyDeadline,
		CommandQueueSize:  o.CommandQueueSize,
		ReconcileInterval: o.ReconcileInterval,
		ReconcileBatch:    o.ReconcileBatch,
	}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// OptionErrorCode is a stable, machine-readable reason an option was refused.
// Reason is free text a caller must not branch on; this is what it branches on.
type OptionErrorCode string

const (
	OptionErrorCodeMissing         OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeMissing)
	OptionErrorCodeInvalid         OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeInvalid)
	OptionErrorCodeUnknownEnum     OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeUnknownEnum)
	OptionErrorCodeNotPositive     OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeNotPositive)
	OptionErrorCodeAboveBound      OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeAboveBound)
	OptionErrorCodeHeartbeatMargin OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeHeartbeatMargin)
	OptionErrorCodeClaimMargin     OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeClaimMargin)
	OptionErrorCodeDedicatedFixed  OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeDedicatedFixed)
	OptionErrorCodeDedicatedCap    OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodeDedicatedCap)
	OptionErrorCodePooledFixed     OptionErrorCode = OptionErrorCode(hostconfig.OptionErrorCodePooledFixed)
)

// ErrUndiallableEndpoint is the cause of an InternalEndpoint refusal whose
// authority no client could dial — an empty port, a port outside 1..65535, or
// more than one port — although Core's InternalEndpoint.Validate and
// sessionwire.HostLinkEndpoint accept it. Reach it with errors.Is. A base Core
// itself refuses carries a *sessionwire.HostLinkEndpointError instead, whose
// Code names the reason.
var ErrUndiallableEndpoint = hostconfig.ErrUndiallableEndpoint

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

// exportOptionsError re-spells the internal refusal as this package's, member
// by member, so a caller's errors.As on *InvalidOptionsError keeps working and
// the Cause it unwraps to is the same value the internal rule produced.
func exportOptionsError(err error) error {
	var invalid *hostconfig.InvalidOptionsError
	if !errors.As(err, &invalid) {
		return err
	}
	return &InvalidOptionsError{
		Code:   OptionErrorCode(invalid.Code),
		Field:  invalid.Field,
		Reason: invalid.Reason,
		Cause:  invalid.Cause,
	}
}

// ---------------------------------------------------------------------------
// The product seam
// ---------------------------------------------------------------------------
//
// Host is a GENERIC runtime host and the agents it serves are a product's. The
// types below are what a product supplies FROM OUTSIDE THIS MODULE; Compose
// takes them in Collaborators.

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
// and Host holds neither. A composition that silently treated the step as done
// would report a drained Host whose sessions had lost whatever was not already
// durable, which is the one drain failure that destroys work. So the seam is
// REQUIRED rather than optional: a Host composed without one does not start.
type Checkpointer interface {
	Checkpoint(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}
