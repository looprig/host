package sessionstoreadapter

import (
	"context"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/service"
)

// MaxAdvertisementTTL is the longest expiry promise the durable target
// directory will store.
//
// IT IS REPORTED RATHER THAN RESTATED. The released store refuses an
// advertisement whose promise exceeds MaxHostTargetTTL, and host.Options puts no
// ceiling on RegistryExpiry at all — so a Host with a one-hour expiry is
// constructible, passes every option rule, and can never publish a single row.
// The composition reads this and refuses at construction, which hands the
// operator the configuration instead of a Host that starts and goes silent.
func (s *Store) MaxAdvertisementTTL() time.Duration { return sessionstore.MaxHostTargetTTL }

// PublishTarget writes one derived advertisement to the durable target
// directory, and is also the heartbeat.
//
// THE ADVERTISEMENT'S OWN PLACEMENT FIELDS ARE NOT WRITTEN, and the omission is
// the released store's design rather than a loss. Namespace, RankingScope,
// StableKey, Rank, Ranked and DueAt describe where an OrderedIndex row goes and
// how it is ordered; the store derives every one of them itself from the key and
// the advertisement, in one compare-and-swap that moves the stored value, the
// rank and the due time together. Writing this module's copies alongside would
// be a second answer to each, free to drift from the one that decides a
// placement page.
//
// VERSION IS CHECKED HERE FOR F14'S REASON. PublishHostTargetRequest carries no
// version member and the store stamps the current one when it projects the row
// back, so an advertisement at a version Core refuses would be published without
// refusal and read back as current.
func (s *Store) PublishTarget(ctx context.Context, advertisement service.Advertisement) error {
	report := advertisement.Report
	if report.Version != sessionwire.CurrentWireVersion {
		return &UnsupportedWireVersionError{Version: report.Version}
	}
	_, err := s.store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
		Key:            hostTargetKey(report),
		HostID:         report.HostID,
		HostGeneration: report.HostGeneration,
		ObservedAt:     report.ObservedAt,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint:  report.InternalEndpoint,
			IsolationClass:    report.IsolationClass,
			Accepting:         report.Accepting,
			AvailableCapacity: report.AvailableCapacity,
			ExpiresAt:         report.ExpiresAt,
		},
	})
	return err
}

// WithdrawTarget removes this Host's offer for one target, gracefully and
// immediately.
//
// IT IS NOT A PUBLICATION WITH Accepting FALSE. A row that still carried an
// advertisement would still be ranked and still be due, so a Factory paging
// candidates would go on finding this Host and then filtering it; the withdrawn
// row has no advertisement at all, which is what makes "withdrawn implies not
// ranked and not due" a property of the record's shape. That is the durability
// the drain's acknowledgement rests on.
//
// The version is checked for the same reason it is checked on a publication,
// and the consequence differs: a withdrawal this adapter refused would leave a
// live offer standing while the Host believed it had retired it.
func (s *Store) WithdrawTarget(ctx context.Context, advertisement service.Advertisement) error {
	report := advertisement.Report
	if report.Version != sessionwire.CurrentWireVersion {
		return &UnsupportedWireVersionError{Version: report.Version}
	}
	_, err := s.store.DrainHostTarget(ctx, sessionstore.DrainHostTargetRequest{
		Key:            hostTargetKey(report),
		HostID:         report.HostID,
		HostGeneration: report.HostGeneration,
	})
	return err
}

// hostTargetKey is the (agent, runtime, placement) triple an advertisement is
// filed under, spelled once so a publication and its withdrawal cannot address
// different rows.
func hostTargetKey(report sessionwire.HostLinkCapacityReport) sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{
		AgentID:                report.AgentID,
		RuntimeCompatibilityID: report.RuntimeCompatibilityID,
		Placement:              report.Placement,
	}
}
