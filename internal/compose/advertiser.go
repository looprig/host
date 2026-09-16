package compose

import (
	"context"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

// advertiser is the production lifecycle.Advertiser, and the first thing in
// this module that writes what a Factory's placement pass reads.
//
// It is TWO RECORDS AND NOT ONE, because the seam it satisfies names two: the
// target-directory rows, which say this Host will take new sessions, and the
// per-session registry rows, which say a resident session will take new work.
// A drain that retired only the first would leave a Factory holding a live
// session row advertising Accepting true on a Host that had stopped.
type advertiser struct {
	host       *hostconfig.Host
	capacity   *service.CapacityPublisher
	directory  TargetDirectory
	registry   *registry.Registry
	locations  residency.Locations
	generation uint64
}

// advertiser is a lifecycle.Advertiser.
var _ lifecycle.Advertiser = (*advertiser)(nil)

// publish writes this Host's current derivation to the durable directory.
//
// It is BOTH the initial publication and the heartbeat, for the reason
// CapacityPublisher.Publish is: a heartbeat that wrote a differently derived
// record from the first publication is exactly the drift that makes a stale
// advertisement hard to explain. The Tombstone flag decides which store
// operation a row takes, and it is the derivation's own answer rather than a
// second reading of the drain flag here.
func (a *advertiser) publish(ctx context.Context) error {
	rows, err := a.capacity.Publish()
	if err != nil {
		return err
	}
	var failures []error
	for _, row := range rows {
		if row.Tombstone {
			err = a.directory.WithdrawTarget(ctx, row)
		} else {
			err = a.directory.PublishTarget(ctx, row)
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// PublishNonaccepting returns once every row this Host publishes says so,
// durably.
//
// THE TARGET ROWS ARE WITHDRAWN RATHER THAN REPUBLISHED. A row still carrying an
// advertisement stays ranked and stays due, so a Factory paging candidates would
// go on finding this Host; a withdrawn row has no advertisement at all, which is
// what makes "withdrawn implies not ranked" a property of the record's shape
// instead of a filter every reader has to remember.
//
// THE SESSION ROWS ARE WRITTEN HERE, WHICH IS A SECOND WRITER OF A RECORD THE
// HEARTBEAT OWNS, and the argument for it is narrow enough to state exactly.
// The heartbeat derives Accepting as `entry.Accepting && !Admissions.Draining()`
// and the drain flips that ledger BEFORE this call, so every beat from now on
// publishes false; what it cannot do is publish NOW, and the acknowledgement
// this call gates is a statement about now. Three properties make the overlap
// safe rather than a race to be won: the value is MONOTONE for the rest of this
// process's life, since nothing un-drains a Host; the write goes through the
// SAME Locations seam at the SAME epoch and generation, so it is the same
// record and not a parallel one; and a beat still in flight from a read taken
// before the flip is corrected by the next beat, which is at most one heartbeat
// interval away and well inside the expiry margin host.MinHeartbeatsBeforeExpiry
// enforces.
//
// A ROW THIS HOST CAN NO LONGER WRITE IS NOT A FAILURE. A superseded epoch or a
// lost grant means a successor owns that session and this Host publishes nothing
// for it at all, so there is no row of ours left saying Accepting true. Treating
// that as a drain failure would make a Host that lost one session unable to
// report a drain it had genuinely performed.
func (a *advertiser) PublishNonaccepting(ctx context.Context) error {
	var failures []error
	if err := a.publish(ctx); err != nil {
		failures = append(failures, err)
	}
	for _, entry := range a.registry.Snapshot() {
		if err := a.locations.PublishResidency(ctx, a.nonaccepting(entry)); err != nil {
			if errors.Is(err, residency.ErrEpochSuperseded) || errors.Is(err, residency.ErrLeaseNotHeld) {
				continue
			}
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// nonaccepting derives one session's registry row with admission closed.
//
// The residency state is the entry's own and is deliberately not forced to
// releasing: this Host has stopped accepting, and the session has not yet begun
// its release. Saying otherwise would publish a state change the drain has not
// made, and the drain's own BeginRelease is the write that makes it.
func (a *advertiser) nonaccepting(entry registry.Entry) sessionwire.HostLinkRegistryObservation {
	now := a.host.Clock().Now()
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               entry.Key.TenantID,
		SessionID:              entry.Key.SessionID,
		HostID:                 a.host.ID(),
		HostGeneration:         a.generation,
		AgentID:                entry.AgentID,
		RuntimeCompatibilityID: string(entry.CompatibilityID),
		Placement:              a.host.Placement(),
		InternalEndpoint:       a.host.InternalEndpoint(),
		Residency:              residencyOf(entry.State),
		Accepting:              false,
		LeaseEpoch:             entry.LeaseEpoch,
		ObservedAt:             now,
		ExpiresAt:              now.Add(a.host.RegistryExpiry()),
	}
}

// residencyOf maps a local residency state onto the wire member a router reads.
//
// It is the same mapping residency's heartbeat makes, and it is spelled again
// here rather than exported from there for one reason: this is the composition,
// and a composition that reached into another package for a private derivation
// would be coupling to the derivation instead of to the record. The DEFAULT ARM
// is the half that matters — Core accepts only attaching, resident and
// releasing, so anything that is not still serving is releasing as far as a
// router cares, and a state added later lands on the conservative answer.
func residencyOf(state registry.ResidencyState) sessionwire.SessionResidency {
	switch state {
	case registry.StateResident:
		return sessionwire.SessionResidencyResident
	default:
		return sessionwire.SessionResidencyReleasing
	}
}
