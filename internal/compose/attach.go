package compose

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/residency"
)

// linkAttacher is the composition's hostlink.Attacher: the way a
// hostlink.attach RPC reaches Service.Attach, which remains the composition's
// ONE attach entry point.
//
// IT IS A SEAM IMPLEMENTATION AND NOT A SECOND ENTRY POINT. HostLink's
// Multiplexer has already fenced the tenant against the link's authentication
// and the Host identity and generation against this process's before this is
// reached (Core: "refuse the attach before it acquires any lease"), so what is
// left here is translation in both directions — Core's record into the
// residency manager's request, and the manager's outcome into the Core class
// and the registry observation the wire carries — plus nothing that decides.
// The manager decides: per-key serialization, the idempotent warm path, the
// lease, the codes.
type linkAttacher struct {
	service *Service
}

var _ hostlink.Attacher = linkAttacher{}

// Attach translates the resolved wire request, runs the composition's attach,
// and answers with the registry observation a Factory binds with.
func (a linkAttacher) Attach(ctx context.Context, request hostlink.AttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	mode, err := attachMode(request.Mode)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	held, err := a.service.Attach(ctx, residency.Request{
		TenantID:        request.Key.TenantID,
		SessionID:       request.Key.SessionID,
		AgentID:         request.AgentID,
		Mode:            mode,
		CompatibilityID: department.CompatibilityID(request.RuntimeCompatibilityID),
		// THE PRINCIPAL IS THE WIRE'S ACTOR, and the tenant is the request's.
		// Core says actor_id is the requesting SERVICE identity — a Factory or
		// its reconciler, not an end user — and the manager requires one so
		// the attach can be attributed at all. Nothing else from the link is
		// installed: the manager's Principal is a whitelist, and the wire's
		// three attribution fields are the whole of what it admits.
		Principal: residency.Principal{
			TenantID: request.Key.TenantID,
			ActorID:  residency.ActorID(request.ActorID),
			TraceID:  residency.TraceID(request.TraceID),
		},
	})
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, attachRefusal(err)
	}
	observation, resident := a.service.manager.Observe(held.Key)
	if !resident {
		// Attached and gone before the reply could be read. It is not a
		// placement outcome — nothing here says another Host should hold the
		// session — so it carries no class, and the attach's idempotency
		// makes a retry the right repair.
		return sessionwire.HostLinkRegistryObservation{}, &hostlink.AttachRefusal{
			Reason: "the session was released between the attach completing and its observation being read",
		}
	}
	return observation, nil
}

// attachMode maps Core's closed attach mode onto the residency manager's.
//
// IT IS A SWITCH AND NOT A CONVERSION. The two enumerations spell the same two
// words today, and a string conversion would silently carry a third word Core
// added later into a manager that refuses it with no class at all. Mapping
// each member by name means a new Core mode arrives here as a refusal this
// composition wrote, and a test holds the two enumerations to each other.
func attachMode(mode sessionwire.HostLinkAttachMode) (residency.Mode, error) {
	switch mode {
	case sessionwire.HostLinkAttachModeCreate:
		return residency.ModeCreate, nil
	case sessionwire.HostLinkAttachModeRestore:
		return residency.ModeRestore, nil
	default:
		return "", &hostlink.AttachRefusal{Reason: "the attach mode " + string(mode) + " is not one this composition maps"}
	}
}

// attachRefusal maps the residency manager's outcome onto the Attacher seam's.
//
// THE CODE IS THE MANAGER'S AND IS NOT RE-DECIDED. AttachError.HostLinkCode is
// the placement class the manager chose — epoch_mismatch for a lease held
// elsewhere, runtime_mismatch, no_capacity, not_admitting,
// runtime_unavailable — or nothing at all for a failure that is not a
// placement outcome (a store that failed, a launch that failed outright), and
// that "nothing" is preserved: Core says such a failure carries no code, and
// launchCode in the manager is where that ruling lives.
//
// THE HOLDER'S EPOCH IS LIFTED HERE, and this is the obligation Core put on
// this Host when it published the attach record. The manager sets
// epoch_mismatch when the store reports ErrLeaseHeld; the store's adapter
// joins that sentinel with the provider's own *storage.LeaseHeldError, whose
// HolderEpoch is the OTHER holder's epoch — so it survives into
// AttachError.Cause and errors.As reaches it. Core requires the class to
// carry a non-zero epoch. When no provider error is in the chain the epoch
// stays zero and the Multiplexer publishes a different class rather than an
// invalid body; see hostlink.RefusalHolderEpochUnknown.
func attachRefusal(err error) *hostlink.AttachRefusal {
	var refused *residency.AttachError
	if !errors.As(err, &refused) {
		return &hostlink.AttachRefusal{Reason: "the attach failed", Cause: err}
	}
	code, _ := refused.HostLinkCode()
	refusal := &hostlink.AttachRefusal{Code: code, Reason: refused.Reason, Cause: err}
	if code == sessionwire.HostLinkErrorEpochMismatch {
		refusal.CurrentLeaseEpoch, _ = LeaseHolderEpoch(err)
	}
	return refusal
}

// LeaseHolderEpoch lifts the OTHER holder's epoch out of a refused attach.
//
// It is the one mechanism for that lift, shared by the HostLink attach path
// and the exported composition's Attach, so the two cannot read the chain
// differently. The epoch is storage.LeaseHeldError.HolderEpoch, which the
// store adapter joins into residency.ErrLeaseHeld's refusal and the manager
// carries as AttachError.Cause; false means no provider error is in the chain,
// which a SessionLeases other than the released adapter may legitimately
// produce.
func LeaseHolderEpoch(err error) (uint64, bool) {
	var held *storage.LeaseHeldError
	if !errors.As(err, &held) {
		return 0, false
	}
	return held.HolderEpoch, true
}
