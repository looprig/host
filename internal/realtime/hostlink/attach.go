package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// The attach seam
// ---------------------------------------------------------------------------

// AttachRequest is the RESOLVED form of a Core attach request: what an Attacher
// is handed once this package has fenced the tenant and the Host incarnation.
//
// It carries the request's session, launch and attribution fields and NOTHING
// about the Host: host_id and host_generation have already been compared here
// and an Attacher has no use for them. The idempotency key rides along because
// the residency manager's per-key serialization is what makes a retry safe and
// a caller correlating a retry storm wants the key on the same record.
type AttachRequest struct {
	Key                    registry.Key
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string
	Mode                   sessionwire.HostLinkAttachMode
	ActorID                string
	TraceID                string
	IdempotencyKey         string
}

// Attacher makes one session resident on this Host, or reports why it did not.
//
// IT IS THE ONE PLACE A HOSTLINK RPC CAN CAUSE A RESIDENCY TO EXIST, and it is
// a SEAM rather than a handle for the reason DrainStarter is: this package
// still holds no mutable registry handle and no residency manager. What it
// holds is an injected capability with exactly one method, and "an attach RPC
// grants nothing itself — the store grants the lease" is the whole of what
// that capability is allowed to mean. Bind is unchanged: it validates current
// ownership and never grants it, and the two guard tests that hold that
// structurally (TestProductionHoldsNoMutableRegistryHandle,
// TestCollaboratorInterfacesExposeOnlyReads) stay true with this seam present.
//
// The observation an accepted attach returns is the ACCEPTED REPLY BODY. Core
// requires it: its host_id, host_generation and lease_epoch are what the
// following bind needs, so attach is, like drain, an exception to the empty
// accepted body. An Attacher that cannot produce one has not attached.
//
// A REFUSAL IS AN *AttachRefusal. Any other error is a failure with no
// placement outcome and is answered without a Core class; see AttachRefusal.
type Attacher interface {
	Attach(context.Context, AttachRequest) (sessionwire.HostLinkRegistryObservation, error)
}

// AttachRefusal is the typed error an Attacher returns.
//
// Code is the HostLink class a Factory branches on and is EMPTY when the
// failure is not a placement outcome — a store that failed, a runtime that
// refused to launch. Core's own contract says such a failure carries no code
// at all, because re-placing a session onto the next Host running the same
// broken build is a stampede and not a recovery; this package therefore
// answers an empty Code at the TRANSPORT rather than with a HostLinkError.
//
// CurrentLeaseEpoch is the OTHER holder's epoch and is read only for
// epoch_mismatch. Core refuses an epoch_mismatch carrying a zero epoch, so an
// Attacher that knows the lease is held elsewhere but not by whom must still
// set the Code: this package substitutes a publishable class rather than put
// an invalid body on the wire. See RefusalHolderEpochUnknown.
type AttachRefusal struct {
	Code              sessionwire.HostLinkErrorCode
	CurrentLeaseEpoch uint64
	Reason            string
	Cause             error
}

func (e *AttachRefusal) Error() string {
	message := "hostlink: attach refused"
	if e.Code != "" {
		message += " (" + string(e.Code) + ")"
	}
	if e.Reason != "" {
		message += ": " + e.Reason
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the cause the Attacher recorded, if any.
func (e *AttachRefusal) Unwrap() error { return e.Cause }

// ---------------------------------------------------------------------------
// Transport dispatch
// ---------------------------------------------------------------------------

// MethodAttach is the reserved RPC name that carries a HostLinkAttachRequest.
// It is Core's, as every framing name here is.
const MethodAttach = sessionwire.HostLinkMethodAttach

// errAttachFailed is returned to the transport when an attach failed with no
// Core class to publish.
//
// IT IS NOT errUnroutableRPC, and the two reach the transport as different
// Centrifuge errors on purpose. A malformed body is the CALLER's fault and is
// answered ErrorBadRequest; a launch that failed, a store that was down, or an
// observation this Host itself could not publish is the HOST's, and is answered
// ErrorInternal. A Factory must treat neither as a placement outcome, but it
// may retry the second — the attach is idempotent per key — where retrying the
// first would repeat the same refusal forever.
var errAttachFailed = errors.New("hostlink: the attach failed with no HostLink class to publish")

// dispatchAttach answers one attach RPC.
//
// IT RETURNS A BODY ON BOTH OUTCOMES, as the drain methods do, and for the same
// structural reason: the accepted body is a HostLinkRegistryObservation and the
// refused body is a HostLinkError, and Core's strict decoders keep the two
// apart because each requires a member the other refuses as unknown.
func (m *Multiplexer) dispatchAttach(ctx context.Context, data []byte) ([]byte, error) {
	var request sessionwire.HostLinkAttachRequest
	if decodeErr := json.Unmarshal(data, &request); decodeErr != nil {
		return attachReply(sessionwire.HostLinkRegistryObservation{}, &BindError{
			Refusal: RefusalMalformedRequest,
			Reason:  "the attach body is not a valid Core record",
			Cause:   decodeErr,
		})
	}
	return attachReply(m.Attach(ctx, request))
}

// attachReply encodes one attach outcome for the transport.
func attachReply(observation sessionwire.HostLinkRegistryObservation, err error) ([]byte, error) {
	if err == nil {
		return json.Marshal(observation)
	}
	var refusal *BindError
	if !errors.As(err, &refusal) {
		return nil, err
	}
	wire, published := refusal.HostLinkError()
	if !published {
		if refusal.Refusal == RefusalMalformedRequest {
			return nil, errUnroutableRPC
		}
		return nil, errAttachFailed
	}
	return json.Marshal(wire)
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

// Attach fences the request against this link's tenant and this Host's
// incarnation, and only then asks the Attacher to make the session resident.
//
// THE ORDER IS THE MECHANISM, as it is for Bind and drainScope. Core's attach
// record says a Host that is not the incarnation the caller placed on "must
// refuse the attach before it acquires any lease": without that fence a
// replacement Host serving the same endpoint would take the session lease and
// return an observation the caller cannot bind to, stranding the session with
// a lease and no route. So the tenant is compared first — a foreign tenant
// learns nothing, not even whether this Host is the one it named — then the
// Host identity and generation, and the Attacher is reached only by a request
// that passed all three. TestTheAttachRefusalLadderAnswersWithItsFirstFailingCheck
// holds each rung as the FIRST failure and asserts the Attacher was not called.
//
// THE IDEMPOTENCY KEY IS CARRIED AND NOT CONSULTED HERE, for the reason
// Binding.IdempotencyKey gives: this package caches no acceptance. The
// residency manager serializes attaches per session and answers a repeat with
// the existing residency, which is a statement about NOW rather than a replay.
func (m *Multiplexer) Attach(ctx context.Context, request sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	resolved, err := m.attachScope(request)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	if m.attacher == nil {
		return sessionwire.HostLinkRegistryObservation{}, &BindError{
			Refusal: RefusalAttachUnsupported,
			Key:     resolved.Key,
			Reason:  "this Host was composed without an attacher, so HostLink cannot make a session resident here",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	observation, err := m.attacher.Attach(ctx, resolved)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, m.attachRefusal(resolved.Key, err)
	}
	// VALIDATED HERE FOR drainObservation'S REASON: an injected seam may hand
	// this package a record Core refuses, and marshalling it would fail at the
	// transport as a bad request the Factory reads as its own fault. It is
	// refused as this Host's condition, with no Core class, because the
	// session may well BE resident now and telling a Factory to re-place it
	// would be wrong; the attach is idempotent, so a retry is the repair.
	if err := observation.Validate(); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, &BindError{
			Refusal: RefusalUnpublishableAttach,
			Key:     resolved.Key,
			Reason:  "the attacher produced an observation Core refuses to publish",
			Cause:   err,
		}
	}
	return observation, nil
}

// attachScope resolves a Core attach request against this link's tenant and
// this Host's incarnation. It is the attach path's refusal ladder up to, and
// not including, the Attacher.
func (m *Multiplexer) attachScope(request sessionwire.HostLinkAttachRequest) (AttachRequest, error) {
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}
	if err := request.Validate(); err != nil {
		return AttachRequest{}, &BindError{Refusal: RefusalMalformedRequest, Key: key, Reason: "the attach request is not a valid Core record", Cause: err}
	}
	if request.TenantID != m.tenant {
		// Returned BEFORE the Host identity is compared, so a foreign tenant
		// cannot learn from the refusal which Host it reached. It is the same
		// rule Bind applies to a tenant-bound link (R-1: every link is
		// authenticated for one tenant).
		return AttachRequest{}, &BindError{
			Refusal: RefusalForeignTenant,
			Key:     key,
			Reason:  "this link is authenticated for another tenant",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostID != m.hostID {
		return AttachRequest{}, &BindError{
			Refusal: RefusalForeignHost,
			Key:     key,
			Reason:  "the attach is addressed to another Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostGeneration != m.generation {
		return AttachRequest{}, &BindError{
			Refusal: RefusalStaleHostGeneration,
			Key:     key,
			Reason:  "the attach names an earlier incarnation of this Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return AttachRequest{
		Key:                    key,
		AgentID:                request.AgentID,
		RuntimeCompatibilityID: request.RuntimeCompatibilityID,
		Mode:                   request.Mode,
		ActorID:                request.ActorID,
		TraceID:                request.TraceID,
		IdempotencyKey:         request.IdempotencyKey,
	}, nil
}

// attachRefusal maps what the Attacher returned onto this package's refusals.
//
// THREE OUTCOMES, and the middle one is the obligation Core put on this Host.
// A refusal with a Core class is published as that class; a refusal with none,
// or an error that is not a refusal at all, is a failure that carries no code
// and leaves at the transport; and an epoch_mismatch whose holder epoch the
// Attacher could not supply is downgraded to runtime_unavailable, because
// Core's Validate refuses the class with a zero epoch and an invalid body would
// lose the signal entirely. The local Refusal names the downgrade so an
// operator can tell it from a genuinely stale route.
func (m *Multiplexer) attachRefusal(key registry.Key, err error) error {
	var refused *AttachRefusal
	if !errors.As(err, &refused) || refused.Code == "" {
		return &BindError{
			Refusal: RefusalAttachFailed,
			Key:     key,
			Reason:  "the attach failed with no placement outcome to report",
			Cause:   err,
		}
	}
	if refused.Code == sessionwire.HostLinkErrorEpochMismatch && refused.CurrentLeaseEpoch == 0 {
		return &BindError{
			Refusal: RefusalHolderEpochUnknown,
			Key:     key,
			Reason:  "the session lease is held elsewhere and the holder's epoch is not available, so the registry the caller placed from is stale",
			Cause:   err,
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	return &BindError{
		Refusal:           RefusalAttachRefused,
		Key:               key,
		Reason:            "the attacher refused the attach: " + strconv.Quote(refused.Reason),
		Cause:             err,
		wire:              refused.Code,
		currentLeaseEpoch: refused.CurrentLeaseEpoch,
	}
}
