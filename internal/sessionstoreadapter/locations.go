package sessionstoreadapter

import (
	"context"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// PublishResidency writes the epoch-fenced durable Host route.
//
// It DELEGATES THE VALIDATION as well as the write, FOR EVERY MEMBER BUT ONE.
// Core defines what a route means — that the endpoint is credential-free, that a
// routed session's residency is attaching, resident or releasing rather than
// cold, that the expiry falls after the observation — and the store applies
// exactly those rules through HostRegistration.Observation, which is the same
// projection a Factory's peer reads. Re-checking any of them here would be a
// second enumeration free to drift from the one that decides.
//
// VERSION IS THE EXCEPTION AND IT IS CHECKED HERE — finding F14.
// PutHostRegistrationRequest has no version member at all, and the store stamps
// CurrentWireVersion when it projects the record back. So an observation
// carrying an unsupported version, which
// HostLinkRegistryObservation.Validate refuses with unsupported_version, would
// be published without refusal and read back as current: this adapter would be
// LOOSER than the seam it satisfies, which is exactly the class-4 defect O3.3
// exists to remove rather than relocate. The check is the smallest thing that
// closes it, and it is stated as a finding rather than as a rule of this
// package's own, because a route's version is Core's business.
func (s *Store) PublishResidency(ctx context.Context, observation sessionwire.HostLinkRegistryObservation) error {
	if observation.Version != sessionwire.CurrentWireVersion {
		return &UnsupportedWireVersionError{Version: observation.Version}
	}
	_, err := s.store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID:   observation.TenantID,
		SessionID:  observation.SessionID,
		LeaseEpoch: observation.LeaseEpoch,
		ObservedAt: observation.ObservedAt,
		ExpiresAt:  observation.ExpiresAt,
		Route: sessionstore.HostRoute{
			HostID:                 observation.HostID,
			HostGeneration:         observation.HostGeneration,
			AgentID:                observation.AgentID,
			RuntimeCompatibilityID: observation.RuntimeCompatibilityID,
			Placement:              observation.Placement,
			InternalEndpoint:       observation.InternalEndpoint,
			Residency:              observation.Residency,
			Accepting:              observation.Accepting,
		},
	})
	return classifyRegistry(err)
}

// TombstoneResidency writes the epoch-fenced tombstone that removes the route
// without erasing the fencing high-water mark.
//
// The store calls it a cleared registration and Host calls it a tombstone; they
// are one record. Route is nil exactly when the registration is a tombstone, so
// the high-water mark survives the removal, which is the property a rollback
// depends on and the reason this is not a delete.
func (s *Store) TombstoneResidency(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	epoch uint64,
) error {
	_, err := s.store.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{
		TenantID:   tenant,
		SessionID:  session,
		LeaseEpoch: epoch,
	})
	return classifyRegistry(err)
}

// UnsupportedWireVersionError reports an observation this adapter refuses to
// write, because the released record cannot carry its version.
//
// It is not sessionwire's own refusal restated. Core refuses an unsupported
// version because the record could not be read; this refuses it because the
// record would be STORED WITHOUT IT and read back as current, which is a
// different failure and deserves a different sentence.
type UnsupportedWireVersionError struct {
	Version sessionwire.WireVersion
}

func (e *UnsupportedWireVersionError) Error() string {
	return "sessionstoreadapter: the durable Host route carries no wire version, so an observation at version " +
		strconv.FormatUint(uint64(e.Version), 10) + " cannot be published; the store would store it as version " +
		strconv.FormatUint(uint64(sessionwire.CurrentWireVersion), 10)
}
