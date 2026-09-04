package sessionstoreadapter

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// PublishResidency writes the epoch-fenced durable Host route.
//
// It DELEGATES THE VALIDATION as well as the write. Core defines what a route
// means — that the endpoint is credential-free, that a routed session's
// residency is attaching, resident or releasing rather than cold, that the
// expiry falls after the observation — and the store applies exactly those rules
// through HostRegistration.Observation, which is the same projection a Factory's
// peer reads. Re-checking any of them here would be a second enumeration free to
// drift from the one that decides.
func (s *Store) PublishResidency(ctx context.Context, observation sessionwire.HostLinkRegistryObservation) error {
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
