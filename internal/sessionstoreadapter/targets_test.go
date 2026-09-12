package sessionstoreadapter_test

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/service"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// testAdvertisement is one derived target-directory row, in the shape the
// capacity publisher hands out.
func testAdvertisement(accepting bool, capacity uint64) service.Advertisement {
	now := time.Now().UTC()
	return service.Advertisement{
		Namespace:    service.AdvertisementNamespace,
		RankingScope: "launchtarget/" + string(testAgent),
		StableKey:    "stable-key",
		Rank:         int64(capacity),
		Ranked:       accepting,
		DueAt:        now.Add(time.Minute),
		Tombstone:    !accepting,
		Report: sessionwire.HostLinkCapacityReport{
			Version:                sessionwire.CurrentWireVersion,
			HostID:                 "host-a",
			HostGeneration:         4,
			AgentID:                testAgent,
			RuntimeCompatibilityID: testCompat,
			Placement:              sessionwire.HostPlacementPooled,
			InternalEndpoint:       "ws://10.0.0.1:7100/hostlink",
			IsolationClass:         sessionwire.HostIsolationClassCrossTenantIsolated,
			Accepting:              accepting,
			AvailableCapacity:      capacity,
			ObservedAt:             now,
			ExpiresAt:              now.Add(time.Minute),
		},
	}
}

// targetKey is the released store's own reading of the advertisement's identity,
// used by the test to read back what the adapter wrote.
func targetKey() sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{
		AgentID:                testAgent,
		RuntimeCompatibilityID: testCompat,
		Placement:              sessionwire.HostPlacementPooled,
	}
}

// TestPublishedTargetsAreReadableAsPlacementCandidates is the first time
// anything in this module writes the §15 target directory.
//
// internal/service derives an advertisement and its own package documentation
// says nothing there writes; internal/lifecycle's drain declares an Advertiser
// whose whole contract is that a Host's rows say accepting=false DURABLY. The
// two met at nothing until this adapter, so what is asserted here is the round
// trip — the bytes a Factory's placement page would read — rather than that a
// call returned nil.
func TestPublishedTargetsAreReadableAsPlacementCandidates(t *testing.T) {
	released, adapted := openStore(t)

	if err := adapted.PublishTarget(t.Context(), testAdvertisement(true, 6)); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}
	page, err := released.ListCompatibleHosts(t.Context(), sessionstore.ListCompatibleHostsRequest{Key: targetKey()})
	if err != nil {
		t.Fatalf("ListCompatibleHosts: %v", err)
	}
	if len(page.Hosts) != 1 {
		t.Fatalf("placement page carries %d hosts, want 1 (lapsed %d, unreadable %d)", len(page.Hosts), page.LapsedSkipped, page.UnreadableSkipped)
	}
	got := page.Hosts[0]
	if got.HostID != "host-a" || got.HostGeneration != 4 {
		t.Errorf("published row identifies (%q, %d), want (host-a, 4)", got.HostID, got.HostGeneration)
	}
	if !got.Accepting || got.AvailableCapacity != 6 {
		t.Errorf("published row = (accepting %v, capacity %d), want (true, 6)", got.Accepting, got.AvailableCapacity)
	}
	if got.InternalEndpoint != "ws://10.0.0.1:7100/hostlink" {
		t.Errorf("published endpoint = %q, want the advertised one", got.InternalEndpoint)
	}
	if got.IsolationClass != sessionwire.HostIsolationClassCrossTenantIsolated {
		t.Errorf("published isolation class = %q, want the advertised one", got.IsolationClass)
	}
}

// TestAWithdrawnTargetLeavesThePlacementPage is the durability the drain's
// Advertiser promises: after the withdrawal returns, a Factory reading the
// placement page for this target does not find this Host.
//
// It is a WITHDRAWAL and not an expiry. A drained Host that merely stopped
// heartbeating would stay a candidate until its promise lapsed, which is the
// whole registry expiry — so the acknowledgement a drain gives Factory would be
// a statement about an in-process flag and not about what a placement reader
// can see.
func TestAWithdrawnTargetLeavesThePlacementPage(t *testing.T) {
	released, adapted := openStore(t)

	if err := adapted.PublishTarget(t.Context(), testAdvertisement(true, 6)); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}
	if err := adapted.WithdrawTarget(t.Context(), testAdvertisement(false, 0)); err != nil {
		t.Fatalf("WithdrawTarget: %v", err)
	}
	page, err := released.ListCompatibleHosts(t.Context(), sessionstore.ListCompatibleHostsRequest{Key: targetKey()})
	if err != nil {
		t.Fatalf("ListCompatibleHosts: %v", err)
	}
	if len(page.Hosts) != 0 {
		t.Fatalf("placement page after a withdrawal carries %d hosts, want none: %#v", len(page.Hosts), page.Hosts)
	}
}

// TestAnUnsupportedWireVersionIsRefusedBeforeTheTargetIsWritten is F14 applied
// to the second record with no version member of its own.
//
// PublishHostTargetRequest carries no version, exactly as
// PutHostRegistrationRequest does not, and the store projects the row back at
// the current one. An advertisement at a version Core refuses would therefore be
// stored without refusal and read back as current, which makes this adapter
// LOOSER than the seam it satisfies. The refusal is here for that reason and not
// because a version is this package's business.
func TestAnUnsupportedWireVersionIsRefusedBeforeTheTargetIsWritten(t *testing.T) {
	released, adapted := openStore(t)

	advertisement := testAdvertisement(true, 6)
	advertisement.Report.Version = sessionwire.CurrentWireVersion + 1

	var unsupported *sessionstoreadapter.UnsupportedWireVersionError
	if err := adapted.PublishTarget(t.Context(), advertisement); !errors.As(err, &unsupported) {
		t.Fatalf("PublishTarget at an unsupported version = %v, want *UnsupportedWireVersionError", err)
	}
	page, err := released.ListCompatibleHosts(t.Context(), sessionstore.ListCompatibleHostsRequest{Key: targetKey()})
	if err != nil {
		t.Fatalf("ListCompatibleHosts: %v", err)
	}
	if len(page.Hosts) != 0 {
		t.Fatalf("a refused publication still wrote %d rows", len(page.Hosts))
	}

	if err := adapted.WithdrawTarget(t.Context(), advertisement); !errors.As(err, &unsupported) {
		t.Fatalf("WithdrawTarget at an unsupported version = %v, want *UnsupportedWireVersionError", err)
	}
}

// TestTheAdvertisementTTLBoundIsTheStoresOwn keeps the composition's
// construction-time refusal honest. It reports the released store's constant
// rather than a number this module chose, so a released change moves both at
// once.
func TestTheAdvertisementTTLBoundIsTheStoresOwn(t *testing.T) {
	_, adapted := openStore(t)

	if got := adapted.MaxAdvertisementTTL(); got != sessionstore.MaxHostTargetTTL {
		t.Errorf("MaxAdvertisementTTL() = %v, want the released store's %v", got, sessionstore.MaxHostTargetTTL)
	}
}

// TestAnExpiryBeyondTheStoresBoundIsRefused is why that bound has a reader. A
// Host whose registry expiry exceeds it can be constructed by host.New and can
// never publish anything, so the composition refuses it at construction — and
// this is the measurement that the underlying refusal exists.
func TestAnExpiryBeyondTheStoresBoundIsRefused(t *testing.T) {
	_, adapted := openStore(t)

	advertisement := testAdvertisement(true, 6)
	advertisement.Report.ExpiresAt = advertisement.Report.ObservedAt.Add(sessionstore.MaxHostTargetTTL + time.Minute)

	if err := adapted.PublishTarget(t.Context(), advertisement); err == nil {
		t.Fatal("PublishTarget with an expiry beyond the store's bound succeeded")
	}
}
