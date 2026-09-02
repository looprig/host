package department

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// Rig is what Host requires of a Harness Rig: something that creates a session
// and restores one. It is a LOCAL interface, and that is a decision worth its
// paragraph rather than a shrug.
//
// Host does not import github.com/looprig/harness, for two independent reasons
// either of which would be sufficient. First, the published harness (v0.30.2)
// does not have H4.1's capabilities at all — no WaitIdle, no Done, no
// ReleaseResidency — so naming it would buy an adapter over a type that cannot
// satisfy Runtime. Second, naming a looprig module in go.mod is the same
// decision as depending on it, and publishedLooprigVersions has no harness
// entry; adding one is a release-ordering commitment, not an import.
//
// So this follows the pattern CLAUDE.md already prescribes for drain: a narrow
// local interface plus a fake, with the concrete edge deferred until the
// module and the capability both exist. What is narrow here is deliberate —
// rig.Rig's real NewSession takes variadic SessionOption and returns
// session.SessionController, and Host wants neither. Host wants a session it
// can identify and interrogate.
type Rig interface {
	// NewSession launches a session for a create request.
	NewSession(context.Context, RigCreateRequest) (RigSession, error)

	// RestoreSession relaunches a session over existing durable state. The UUID
	// is Harness's identity for it, which is not Host's; see rigRuntime.
	RestoreSession(context.Context, uuid.UUID, RigRestoreRequest) (RigSession, error)
}

// RigSession is the minimum a launched session must offer. Everything else Host
// needs is discovered by type assertion, because Harness's session type is wide
// and H4.1's capabilities are added to implementations rather than required of
// them: a session either has them or this package refuses to wrap it.
type RigSession interface {
	// ID is Harness's identity for the session, a UUID.
	ID() uuid.UUID
}

// RigCreateRequest is what Host hands a Rig to launch a session. It carries
// Host's identities and the storage and workspace context the session needs.
type RigCreateRequest struct {
	TenantID      sessionwire.TenantID
	SessionID     sessionwire.SessionID
	AgentID       sessionwire.AgentID
	Placement     sessionwire.HostPlacement
	WorkspaceRoot string
	Storage       StorageContext
}

// RigRestoreRequest is RigCreateRequest for a relaunch over durable state.
type RigRestoreRequest struct {
	TenantID      sessionwire.TenantID
	SessionID     sessionwire.SessionID
	AgentID       sessionwire.AgentID
	Placement     sessionwire.HostPlacement
	WorkspaceRoot string
	Storage       StorageContext
}

// NewRigTarget adapts a Rig into a LaunchTarget for one agent.
//
// It validates BEFORE returning a target, so an unusable target cannot reach a
// Registration and therefore cannot reach a Department: the rejection happens
// with nothing registered rather than with a registry holding something that
// will fail on first use.
func NewRigTarget(rig Rig, compatibility CompatibilityID, capabilities Capabilities) (LaunchTarget, error) {
	if rig == nil {
		return nil, &InvalidDepartmentError{Code: DefinitionErrorCodeNoTarget, Reason: "no rig to launch sessions with"}
	}
	if err := compatibility.Validate(); err != nil {
		return nil, &InvalidDepartmentError{
			Code:   DefinitionErrorCodeInvalidCompatibilityID,
			Reason: "compatibility id is unusable: " + err.Error(),
			Cause:  err,
		}
	}
	if err := capabilities.Validate(); err != nil {
		var invalid *InvalidCapabilitiesError
		if !errors.As(err, &invalid) {
			return nil, &InvalidDepartmentError{Reason: err.Error(), Cause: err}
		}
		return nil, &InvalidDepartmentError{Code: invalid.Code, Reason: invalid.Reason, Cause: err}
	}
	return &rigTarget{rig: rig, compatibility: compatibility, capabilities: capabilities}, nil
}

type rigTarget struct {
	rig           Rig
	compatibility CompatibilityID
	capabilities  Capabilities
}

// CompatibilityID reports the runtime build this target launches.
//
// It is the value HostLink advertises as RuntimeCompatibilityID and the value
// Restore compares a durable state's build against, and those two uses are the
// reason it has its own assertion rather than being inferred from the mismatch
// test: an accessor that disagrees with what Restore compares against is a Host
// advertising a build it will then refuse to restore. A mutant returning a
// constant here once left the whole suite green, because every test reached
// t.compatibility through Restore instead.
func (t *rigTarget) CompatibilityID() CompatibilityID { return t.compatibility }

// Capabilities reports what this target declared at construction.
func (t *rigTarget) Capabilities() Capabilities { return t.capabilities }

// Create launches a new session through the rig and adapts it into a Runtime.
func (t *rigTarget) Create(ctx context.Context, request CreateRequest) (Runtime, error) {
	// A CONVERSION, not a field-by-field literal, and the difference is a
	// guard rather than a style. RigCreateRequest is structurally identical to
	// CreateRequest today, so a literal that forgot a field would compile and
	// silently stop propagating it — which is exactly the defect J2 probes for.
	// The conversion cannot forget one: the day someone adds a field to either
	// type alone, this line stops compiling and the question "does the rig need
	// this too?" gets asked instead of answered by omission.
	session, err := t.rig.NewSession(ctx, RigCreateRequest(request))
	if err != nil {
		return nil, &RigLaunchError{AgentID: request.AgentID, SessionID: request.SessionID, Operation: "create", Cause: err}
	}
	return adaptRigSession(request.SessionID, request.AgentID, session)
}

// Restore relaunches a session over existing durable state, refusing a state
// written by a different runtime build before anything is launched.
func (t *rigTarget) Restore(ctx context.Context, request RestoreRequest) (Runtime, error) {
	// FAIL CLOSED, and fail closed BEFORE launching. Deciding this after the
	// rig has produced a session spends the resource the check exists to
	// protect, so the ordering is part of the rule rather than an efficiency.
	// The zero value of the field lands here too: a RestoreRequest nobody
	// filled in does not match any target's compatibility id, because a target
	// with an empty one cannot be constructed.
	if request.CompatibilityID != t.compatibility {
		return nil, &CompatibilityMismatchError{
			AgentID:   request.AgentID,
			SessionID: request.SessionID,
			Durable:   request.CompatibilityID,
			Target:    t.compatibility,
		}
	}
	// A field-by-field LITERAL, unlike Create, and the difference matters
	// enough to write down. RestoreRequest carries two fields a rig has no
	// business seeing — the compatibility id Host checks above, and Harness's
	// own session identity, which travels as its own argument — so the two
	// types are not structurally identical and a conversion is not available.
	//
	// That means the compile-time protection Create gets is ABSENT here: a
	// field deleted from this literal compiles and silently stops propagating.
	// The guard is therefore the test, which compares the whole received value
	// against an independently spelled expectation rather than probing a field
	// or two. Deleting Placement and WorkspaceRoot from this literal once left
	// the entire suite green.
	session, err := t.rig.RestoreSession(ctx, request.RigSessionID, RigRestoreRequest{
		TenantID:      request.TenantID,
		SessionID:     request.SessionID,
		AgentID:       request.AgentID,
		Placement:     request.Placement,
		WorkspaceRoot: request.WorkspaceRoot,
		Storage:       request.Storage,
	})
	if err != nil {
		return nil, &RigLaunchError{AgentID: request.AgentID, SessionID: request.SessionID, Operation: "restore", Cause: err}
	}
	return adaptRigSession(request.SessionID, request.AgentID, session)
}

// adaptRigSession discovers the capabilities Host requires and refuses a
// session short of any of them.
//
// It reports EVERY missing capability rather than the first, because a session
// short of three should cost one diagnosis rather than three. The refusal
// returns a nil Runtime: there is nothing for a caller to register, which is
// the strongest form of "rejected before publication" this seam can offer —
// publication is a later task's, and what this one can guarantee is that
// nothing reaches it.
func adaptRigSession(sessionID sessionwire.SessionID, agentID sessionwire.AgentID, session RigSession) (Runtime, error) {
	if isNilSession(session) {
		return nil, &RigLaunchError{
			AgentID:   agentID,
			SessionID: sessionID,
			Operation: "launch",
			Cause:     ErrNoRigSession,
		}
	}

	adapted := &rigRuntime{sessionID: sessionID, agentID: agentID, rigID: session.ID()}
	var missing []string
	if capability, ok := session.(IdleWaiter); ok {
		adapted.IdleWaiter = capability
	} else {
		missing = append(missing, "IdleWaiter")
	}
	if capability, ok := session.(Liveness); ok {
		adapted.Liveness = capability
	} else {
		missing = append(missing, "Liveness")
	}
	if capability, ok := session.(Releaser); ok {
		adapted.Releaser = capability
	} else {
		missing = append(missing, "Releaser")
	}
	if capability, ok := session.(PublicationSubscriber); ok {
		adapted.PublicationSubscriber = capability
	} else {
		missing = append(missing, "PublicationSubscriber")
	}
	if capability, ok := session.(CommandApplier); ok {
		adapted.CommandApplier = capability
	} else {
		missing = append(missing, "CommandApplier")
	}
	if len(missing) > 0 {
		return nil, &IncapableRuntimeError{AgentID: agentID, SessionID: sessionID, Missing: missing}
	}
	return adapted, nil
}

// rigRuntime is the adapter O1.1 said would be mandatory.
//
// It exists because the identity types cannot be reconciled by naming: Harness
// identifies a session with a uuid.UUID and Host identifies a runtime with the
// sessionwire identities it exchanges with Factory, which are opaque strings
// and not UUIDs by contract. The translation is not a cast — Host's identities
// come from the REQUEST, and Harness's UUID is kept beside them rather than
// parsed into one or derived from the other.
type rigRuntime struct {
	sessionID sessionwire.SessionID
	agentID   sessionwire.AgentID
	rigID     uuid.UUID

	IdleWaiter
	Liveness
	Releaser
	PublicationSubscriber
	CommandApplier
}

func (r *rigRuntime) SessionID() sessionwire.SessionID { return r.sessionID }
func (r *rigRuntime) AgentID() sessionwire.AgentID     { return r.agentID }

// RigSessionID reports Harness's identity for this runtime. It is the method
// the package-level RigSessionID asserts for, so a wrapper that forwards it
// keeps working.
func (r *rigRuntime) RigSessionID() uuid.UUID { return r.rigID }

// RigSessionID reports Harness's UUID for a runtime this package adapted, for
// callers that must correlate with Harness's own records. It is deliberately
// not part of Runtime: Host's contracts speak sessionwire identities.
//
// PROVISIONAL. It is a *rigRuntime type assertion on a public API, which is an
// escape hatch rather than a contract, and it exists today because it is the
// only way to assert create-side identity translation from outside this
// package. O2.2 is the task that will actually need to correlate with Harness's
// records; when it does, decide then whether this stays an assertion-shaped
// helper or becomes a field on something the registry holds. Do not build on it
// before that decision.
func RigSessionID(runtime Runtime) (uuid.UUID, bool) {
	// An INTERFACE assertion, not runtime.(*rigRuntime). The concrete assertion
	// breaks the day anything wraps a Runtime — a residency decorator, a
	// metrics wrapper, a test spy — and it breaks by returning false, which
	// reads as "not one of ours" when it means "your wrapper ate it". A wrapper
	// that forwards this one method keeps working.
	reporter, ok := runtime.(interface{ RigSessionID() uuid.UUID })
	if !ok {
		return uuid.UUID{}, false
	}
	return reporter.RigSessionID(), true
}

// isNilSession reports whether a rig handed back nothing, in either of the two
// ways Go allows.
//
// A nil INTERFACE is what a fake produces and what an obvious `return nil, nil`
// produces. A TYPED NIL — a non-nil interface holding a nil *Session — is what
// a real rig produces, from the commonest bug in the language:
//
//	var session *Session
//	if err := launch(&session); err != nil { return nil, err }
//	return session, nil   // non-nil interface, nil pointer
//
// The guard covered only the first, so a typed nil panicked at session.ID() —
// the exact "nil dereference three layers away" this check's own comment claims
// to prevent, arriving by the path a real implementation actually takes rather
// than the one a test double does.
func isNilSession(session RigSession) bool {
	if session == nil {
		return true
	}
	value := reflect.ValueOf(session)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return value.IsNil()
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// IncapableRuntimeError reports a launched session missing capabilities Host
// requires. Missing names every one of them rather than the first, because a
// session short of three capabilities should cost one round trip to find out.
type IncapableRuntimeError struct {
	AgentID   sessionwire.AgentID
	SessionID sessionwire.SessionID
	Missing   []string
}

func (e *IncapableRuntimeError) Error() string {
	return "department: the rig session for agent " + strconv.Quote(string(e.AgentID)) +
		" is missing capabilities Host requires: " + strings.Join(e.Missing, ", ")
}

// CompatibilityMismatchError reports a restore onto a runtime build that did
// not write the durable state.
type CompatibilityMismatchError struct {
	AgentID   sessionwire.AgentID
	SessionID sessionwire.SessionID
	Durable   CompatibilityID
	Target    CompatibilityID
}

func (e *CompatibilityMismatchError) Error() string {
	return "department: session " + strconv.Quote(string(e.SessionID)) +
		" was written by runtime " + strconv.Quote(string(e.Durable)) +
		" and this target launches " + strconv.Quote(string(e.Target))
}

// ErrorCode returns the stable public code a client branches on. A restore onto
// an incompatible runtime is not an invalid request: the request is well formed
// and this Host has no runtime that can serve it.
func (e *CompatibilityMismatchError) ErrorCode() sessionwire.ErrorCode {
	return sessionwire.ErrorCodeRuntimeUnavailable
}

// RigLaunchError wraps a failure the Rig itself reported, preserving the cause.
//
// It carries no ErrorCode, and neither does IncapableRuntimeError, while
// CompatibilityMismatchError and UnknownAgentError do. That is deliberate and
// worth saying, because every other code choice in this file is justified: the
// two that carry one describe a well-formed request this Host cannot serve,
// which is a client's business and a thing to branch on. A rig that refused to
// launch, or one that produced a session missing capabilities, is a HOST
// DEFECT — a misconfigured target or a broken build — and mapping it to a
// public code would tell a client to retry something only an operator can fix.
// Whoever maps these to the wire should choose a code at that seam.
type RigLaunchError struct {
	AgentID   sessionwire.AgentID
	SessionID sessionwire.SessionID
	Operation string
	Cause     error
}

func (e *RigLaunchError) Error() string {
	// Cause is dereferenced only if it is there. This is an exported struct, so
	// another package may construct one without a cause, and an Error method
	// that panics is the worst possible place for a nil dereference: it fires
	// while something is already reporting a failure.
	message := "department: rig " + e.Operation + " for agent " + strconv.Quote(string(e.AgentID)) + " failed"
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *RigLaunchError) Unwrap() error { return e.Cause }

// ErrNoRigSession is the cause of a RigLaunchError raised when a rig reports
// success and hands back nothing. It is EXPORTED so a test — and a caller — can
// tell that failure apart from a rig that refused, which is the difference
// between a broken rig and a busy one.
var ErrNoRigSession = errors.New("the rig reported success and returned no session")
