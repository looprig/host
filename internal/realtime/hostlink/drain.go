package hostlink

import (
	"encoding/json"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Scope
// ---------------------------------------------------------------------------

// DrainScope is what one drain covers.
//
// Core spells the two scopes as an OPTIONAL tenant/session pair on the request,
// which is a shape a caller can get half right; this is the RESOLVED form, and
// the only thing that can produce one FROM A REQUEST is drainScope below. A
// scope therefore either names the session this Host is configured to hold or
// names nothing at all — "a session this Host does not hold" is not a value a
// state machine can be handed.
//
// THE WHOLE-HOST SCOPE IS NOT ONE drainScope CAN PRODUCE, since R-1. A request
// naming no tenant is refused rather than resolved, because every link is
// authenticated for one tenant and the Drainer behind them is the Host's. The
// zero scope is still a legitimate VALUE — the process lifecycle constructs it
// and hands it to the Drainer directly — so nothing here may read a zero Key as
// impossible; what it may not do is accept one off the wire.
//
// IT CARRIES NO IDEMPOTENCY KEY, deliberately. See drainScope.
type DrainScope struct {
	// Key is the fixed session of a dedicated Host. Its zero value means the
	// whole Host, and a partially populated Key is not reachable: the resolver
	// sets both fields or neither.
	Key registry.Key
}

// WholeHost reports whether this scope covers the Host rather than one session.
func (s DrainScope) WholeHost() bool { return s.Key == registry.Key{} }

// DrainStatus is one drain's stable identity and current state.
//
// GENERATION IS THE FIELD THAT MATTERS AND IT IS THE STATE MACHINE'S, not the
// handler's. Every request is answered by asking the machine again rather than
// from any memory this package keeps, which is why a Host that somehow restarted
// its drain would report a different generation instead of a cached one that
// lies.
type DrainStatus struct {
	// Generation identifies this drain across retries, reconnects and
	// ambiguous replies. Core refuses a zero generation.
	Generation uint64

	// State is Core's bounded drain state. A value outside Core's two is not
	// published; see drainObservation.
	State sessionwire.HostLinkDrainState
}

// DrainStarter begins, or re-reports, the single drain this Host is running.
//
// IT IS THE ONLY MUTATOR THIS PACKAGE HOLDS besides its own routing table, and
// it deliberately takes NO CONTEXT and returns NO COMPLETION: the RPC
// acknowledges initiation plus generation, so a StartDrain that blocked until
// release would make the acknowledgement a statement about the wrong event.
type DrainStarter interface {
	// StartDrain begins the drain covering scope and reports its stable
	// generation and current state. It is IDEMPOTENT: a call for a drain
	// already begun returns that same generation rather than starting a
	// second one, and it returns promptly rather than waiting for release.
	StartDrain(DrainScope) (DrainStatus, error)
}

// DrainObserver reports a drain already begun, and is a pure read.
type DrainObserver interface {
	// ObserveDrain returns the drain covering scope and whether one has begun
	// at all. It never starts one.
	ObserveDrain(DrainScope) (DrainStatus, bool)
}

// DrainScopeReporter reports the scope the call that BEGAN the drain named.
//
// IT IS A SEGREGATED CAPABILITY DISCOVERED BY ASSERTION and deliberately NOT a
// method on DrainObserver: a state machine that cannot answer it is a state
// machine this package still observes, and widening the observer interface
// would break every implementation for one rung's benefit.
//
// WHY IT EXISTS. rung 8 of drainScope refuses a fixed-session drain unless the
// requesting tenant currently HOLDS that session. That is right for BEGINNING a
// Host-wide drain and wrong for OBSERVING one that has finished: release drops
// the residency, so the terminal `drained` answer becomes unreachable to the
// only caller entitled to it at the instant it becomes true. The observation
// path therefore accepts EITHER a current holder OR the caller that began this
// very drain.
//
// A REPORTER THAT IS ABSENT CHANGES NOTHING. Without one the observation path
// is exactly rung 8 as R-1 wrote it.
type DrainScopeReporter interface {
	// BegunScope reports the scope the drain was begun with, and whether a
	// drain has begun that named one at all.
	BegunScope() (registry.Key, bool)
}

// ---------------------------------------------------------------------------
// Transport dispatch
// ---------------------------------------------------------------------------

// MethodDrain and MethodDrainStatus are the two reserved drain RPC names.
//
// They are SEPARATE METHODS rather than one method with a mode flag, because
// the difference between them is exactly the difference a Factory must not get
// wrong: one initiates and the other only looks. A mode flag would put that
// distinction in a body a retry could corrupt.
const (
	MethodDrain       = "hostlink.drain"
	MethodDrainStatus = "hostlink.drain_status"
)

// errUnpublishableDrainObservation reports a state machine whose answer Core's
// own Validate refuses. It is returned to the transport rather than published,
// because an invalid observation is this Host's defect and not a Factory's.
var errUnpublishableDrainObservation = errors.New("hostlink: the drain state machine produced an observation Core refuses")

// dispatchDrain answers one drain or drain-status RPC.
//
// IT IS THE ONE RPC THAT RETURNS A BODY ON BOTH OUTCOMES, which the bind and
// unbind reply contract — empty means accepted — cannot express, because an
// acknowledgement carries a generation. What keeps the two apart is Core's own
// strict decoding: HostLinkDrainObservation and HostLinkError each require a
// member the other refuses as unknown, so neither body decodes as the other.
// TestADrainAcknowledgementCannotBeReadAsARefusal asserts that in both
// directions; it is a property of Core's decoders, so this package must not add
// a member to either record to "help".
func (m *Multiplexer) dispatchDrain(method string, data []byte) ([]byte, error) {
	var request sessionwire.HostLinkDrainRequest
	if decodeErr := json.Unmarshal(data, &request); decodeErr != nil {
		return drainReply(sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalMalformedRequest,
			Reason:  "the drain body is not a valid Core record",
			Cause:   decodeErr,
		})
	}
	if method == MethodDrainStatus {
		return drainReply(m.ObserveDrain(request))
	}
	return drainReply(m.StartDrain(request))
}

// drainReply encodes one drain outcome for the transport.
func drainReply(observation sessionwire.HostLinkDrainObservation, err error) ([]byte, error) {
	if err == nil {
		return json.Marshal(observation)
	}
	var refusal *BindError
	if !errors.As(err, &refusal) {
		return nil, err
	}
	wire, published := refusal.HostLinkError()
	if !published {
		return nil, errUnroutableRPC
	}
	return json.Marshal(wire)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// StartDrain begins this Host's drain for the scope the request names and
// acknowledges the INITIATION.
//
// IT ACKNOWLEDGES INITIATION AND NOT RELEASE. The returned state is whatever
// the state machine reports at this instant — draining while it works, drained
// once it has finished — and the handler neither waits for nor infers the
// second from the first.
func (m *Multiplexer) StartDrain(request sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	scope, err := m.drainScope(request, false)
	if err != nil {
		return sessionwire.HostLinkDrainObservation{}, err
	}
	if m.drainStarter == nil {
		return sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalDrainUnsupported,
			Key:     scope.Key,
			Reason:  "this Host was composed without a drain state machine, so it has no drain to begin",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	status, err := m.drainStarter.StartDrain(scope)
	if err != nil {
		return sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalDrainUnavailable,
			Key:     scope.Key,
			Reason:  "the drain state machine could not begin a drain",
			Cause:   err,
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return m.drainObservation(scope, status)
}

// ObserveDrain reports the drain covering the scope the request names, and
// STARTS NOTHING.
//
// The absent drain is a REFUSAL rather than a drained-looking observation,
// which is the whole point of the separate method: a Factory polling a Host
// that never received the drain request must not read "no drain here" as "the
// drain finished" and delete the workload.
func (m *Multiplexer) ObserveDrain(request sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	scope, err := m.drainScope(request, true)
	if err != nil {
		return sessionwire.HostLinkDrainObservation{}, err
	}
	if m.drainObserver == nil {
		return sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalDrainUnsupported,
			Key:     scope.Key,
			Reason:  "this Host was composed without a drain state machine, so it has no drain to observe",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	status, begun := m.drainObserver.ObserveDrain(scope)
	if !begun {
		return sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalNoDrainInProgress,
			Key:     scope.Key,
			Reason:  "this Host has begun no drain covering that scope",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return m.drainObservation(scope, status)
}

// drainObservation builds the Core record for one status, and refuses an
// invalid one rather than putting it on the wire.
//
// The validation is NOT a defensive spelling of the marshaller's own check: an
// injected seam may hand this package a zero generation or a drain state
// outside Core's two. Marshalling such a value would fail at the transport with
// a bad-request the Factory would read as its own fault; it is refused here as
// this Host's condition instead.
func (m *Multiplexer) drainObservation(scope DrainScope, status DrainStatus) (sessionwire.HostLinkDrainObservation, error) {
	observation := sessionwire.HostLinkDrainObservation{
		HostID:          m.hostID,
		HostGeneration:  m.generation,
		DrainGeneration: status.Generation,
		State:           status.State,
		TenantID:        scope.Key.TenantID,
		SessionID:       scope.Key.SessionID,
	}
	if err := observation.Validate(); err != nil {
		return sessionwire.HostLinkDrainObservation{}, &BindError{
			Refusal: RefusalDrainUnavailable,
			Key:     scope.Key,
			Reason:  "the drain state machine reported a status Core refuses to publish",
			Cause:   errors.Join(errUnpublishableDrainObservation, err),
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return observation, nil
}

// drainScope resolves the request's optional tenant/session pair against this
// Host's identity and configuration.
//
// IT IS AN ORDERED SEQUENCE OF TOTAL PREDICATES, and the order is the
// mechanism, as it is on the bind path. Host identity is decided before scope,
// so a request routed from a stale placement record learns nothing about which
// session this Host holds; the whole-Host scope is refused before the tenant is
// compared, because there is no tenant on it to compare; and the tenant is
// decided before the fixed session, so a foreign tenant cannot probe for it
// either.
// TestTheDrainRefusalLadderAnswersWithItsFirstFailingCheck enumerates the
// predicates and holds each one as the FIRST failure, which is the property the
// ordering buys.
//
// THE IDEMPOTENCY KEY IS NOT CONSULTED, and that is a decision rather than an
// omission. Core calls it "a retry-stable initiation key" and a Host could have
// keyed a cache on it; this one does not, for the reason Binding.IdempotencyKey
// already gives on the bind path — replaying a cached acceptance makes the
// answer a statement about the PAST. A drain's answer is a statement about the
// machine NOW, which is strictly stronger than key-scoped idempotency:
// EVERY request gets the same generation, not merely every request sharing a
// key.
// That is also why the key is not carried onto DrainScope, where a state
// machine could branch on it.
func (m *Multiplexer) drainScope(request sessionwire.HostLinkDrainRequest, observing bool) (DrainScope, error) {
	if err := request.Validate(); err != nil {
		return DrainScope{}, &BindError{
			Refusal: RefusalMalformedRequest,
			Reason:  "the drain request is not a valid Core record",
			Cause:   err,
		}
	}
	if request.HostID != m.hostID {
		return DrainScope{}, &BindError{
			Refusal: RefusalForeignHost,
			Reason:  "the drain is addressed to another Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostGeneration != m.generation {
		return DrainScope{}, &BindError{
			Refusal: RefusalStaleHostGeneration,
			Reason:  "the drain names an earlier incarnation of this Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	// Core's own Validate has already established that the tenant and session
	// are both present or both absent, so one test decides the scope — and the
	// WHOLE-HOST one is REFUSED HERE, before the tenant comparison below ever
	// runs. That refusal is R-1 and it is not a tightening of an existing rule;
	// it closes a hole.
	//
	// THE HOLE. This check used to RETURN the whole-Host scope, above the
	// tenant comparison, so an empty TenantID short-circuited the tenant check
	// entirely. Every tenant's Multiplexer is handed the SAME Host-level
	// Drainer, so a link authenticated as tenant-a could begin a drain that
	// checkpointed and released tenant-b's resident sessions. It is
	// AVAILABILITY and not confidentiality — tenant-a reads nothing of
	// tenant-b's and cannot address its sessions — and it is exactly the shape
	// of defect the ordered ladder exists to prevent, reached by a rung that
	// answered before the ladder got to the tenant.
	//
	// The comment that justified it argued HostLink "admits exactly ONE
	// PRINCIPAL CLASS", and that premise is what H8 falsified: one principal
	// class is not one PRINCIPAL, and a Host serving several tenants runs one
	// Multiplexer per authenticated tenant over one drain.
	//
	// WHAT IS LEFT REACHABLE. A dedicated Host's fixed session, named by its
	// own tenant, AND ONLY WHEN THAT TENANT CURRENTLY HOLDS IT — see the last
	// rung below. It is NOT "scoped by construction": an earlier version of
	// this comment said so, and it was false, because the fixed session is one
	// bare SessionID handed to every tenant's table. Whole-Host drain remains
	// reachable ONLY through the PROCESS-LIFECYCLE path — Service.Stop calls
	// the Drainer directly and never passes through this resolver — which is
	// the path a platform's termination signal already takes. It is not a
	// second spelling of this one and nothing here gates it.
	//
	// NO SEAM IS LEFT FOR A FACTORY CONTROLLER. If one later needs a drain over
	// this link it gets a TENANT-SCOPED or SERVICE-PRINCIPAL path of its own;
	// re-admitting the empty tenant here would reopen the hole exactly.
	if request.TenantID == "" {
		return DrainScope{}, &BindError{
			Refusal: RefusalWrongDrainScope,
			Reason:  "this link is authenticated for one tenant, so it cannot drain the whole Host; a whole-Host drain is the process lifecycle's",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.TenantID != m.tenant {
		return DrainScope{}, &BindError{
			Refusal: RefusalForeignTenant,
			Reason:  "this link is authenticated for another tenant",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}
	if m.fixedSession == "" {
		return DrainScope{}, &BindError{
			Refusal: RefusalWrongDrainScope,
			Key:     key,
			Reason:  "this Host holds no fixed session, so it is drained as a whole and not per session",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.SessionID != m.fixedSession {
		return DrainScope{}, &BindError{
			Refusal: RefusalWrongDrainScope,
			Key:     key,
			Reason:  "this Host's fixed session is not the one the drain names",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	// AND THE REQUESTER MUST HOLD IT. This is the second half of R-1 and it
	// closes the same defect class as the first: a drain whose scope this Host
	// cannot attribute to the caller.
	//
	// THE HOLE. A dedicated Host's fixed session is ONE SessionID, and the
	// composition hands that same value to EVERY tenant's Multiplexer. Without
	// this rung the resolver compares the request's session against the fixed
	// one and the request's tenant against the LINK's, and never compares the
	// fixed session's OWNER against the requester — so a link authenticated as
	// tenant-b, holding nothing at all, could name {tenant-b, the fixed
	// session}, pass every rung above, and begin the HOST-WIDE drain that
	// checkpoints and releases tenant-a's session. Measured before it was
	// closed, not read out of the source.
	//
	// WHAT MAKES THE OWNER KNOWABLE IS CAPACITY, and that is why this is an
	// attribution rather than an invention. host.Options.validatePlacement
	// requires Capacity == 1 for dedicated placement, so a Host with a fixed
	// session holds AT MOST ONE resident session. "the requester holds the
	// fixed session" and "the requester owns everything the Host-wide drain
	// would touch" are therefore the same statement, and Residencies.Get
	// decides it. On a Host that could hold two tenants at once they would NOT
	// be the same statement and this rung would not be sufficient.
	//
	// IT DISCLOSES NOTHING NEW, which is why it may sit at the bottom of an
	// ordered ladder that otherwise refuses before reading anything. The key it
	// reads is the caller's OWN tenant paired with a session the caller has
	// just been told is this Host's fixed one; Bind already answers exactly
	// that question for exactly that key. It is placed LAST for that reason:
	// a foreign tenant and a wrong session are both refused above it, so
	// neither reaches a residency read.
	//
	// IT IS A RESIDENCY READ AND NOT AN OWNERSHIP PROOF. It says this tenant
	// holds this session on this Host NOW; it is not the lease, it grants
	// nothing, and a Host holding nothing yet refuses every drain that reaches
	// here — which is fail-closed and correct, because a Host holding nothing
	// has nothing for a HostLink drain to cover.
	//
	// ONE CALLER IS ATTRIBUTED WITHOUT HOLDING IT, and only on the OBSERVATION
	// path: the caller that BEGAN this very drain. Release drops the residency,
	// so without this the terminal `drained` answer is unreachable to the only
	// caller entitled to it at exactly the instant it becomes true — and a
	// Factory reading completion from the disconnect instead is reading a
	// signal that is identical whether release finished or FinishRelease
	// refused. See DrainScopeReporter for why this discloses nothing new: the
	// match requires naming the tenant AND session that began the drain, and
	// only that caller ever received the acknowledgement. A tenant that held
	// nothing began nothing and is refused here exactly as before.
	if _, held := m.residencies.Get(key); !held && !(observing && m.beganDrainFor(key)) {
		return DrainScope{}, &BindError{
			Refusal: RefusalWrongDrainScope,
			Key:     key,
			Reason:  "this link's tenant does not hold this Host's fixed session, so the Host-wide drain it would begin is not its own to begin",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return DrainScope{Key: key}, nil
}

// beganDrainFor reports whether this Host's drain was begun by a caller naming
// exactly this key.
//
// IT IS FALSE WHEN THE STATE MACHINE CANNOT ANSWER, which is the whole point of
// discovering the capability rather than requiring it: an observer without a
// DrainScopeReporter leaves rung 8 exactly as R-1 wrote it.
func (m *Multiplexer) beganDrainFor(key registry.Key) bool {
	reporter, reports := m.drainObserver.(DrainScopeReporter)
	if !reports {
		return false
	}
	begun, named := reporter.BegunScope()
	return named && begun == key
}
