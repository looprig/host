package sessionstoreadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// createDispositionSession makes the catalog record AcquireResidency will accept.
//
// THE MODE IS ON THE IMMUTABLE BINDING AND IS SET AT CREATION. createSession in
// adapter_test.go writes no binding at all, which the store preserves as the
// legacy v1 record — so it is the LEGACY fixture, and the two are kept apart here
// rather than parameterised, because the difference between them is the whole of
// finding F16(a).
func createDispositionSession(t *testing.T, released *sessionstore.Store) {
	t.Helper()
	now := time.Now().UTC()
	if _, _, err := released.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID:               testTenant,
		SessionID:              testSession,
		AgentID:                testAgent,
		RuntimeCompatibilityID: testCompat,
		CreatedAt:              now,
		LastActiveAt:           now,
		State:                  sessionwire.SessionStateRunning,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "idem-1",
		Binding: sessionstore.SessionBinding{
			StorageBindingID: "binding-1",
			BindingVersion:   "v1",
			RuntimeSessionID: "runtime-session-1",
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
	}); err != nil {
		t.Fatalf("create disposition catalog entry: %v", err)
	}
}

// THE RESIDENCY GRANT IS A DIFFERENT LEASE FROM THE JOURNAL GRANT, MEASURED
// THROUGH THE RELEASED STORE.
//
// This is F15's claim reduced to an experiment rather than a reading. One session,
// one backend, both grants taken: the residency epoch and the journal epoch are
// minted by two different Leaser names in two different namespaces, and the only
// reason they agree here is that memstore starts every name at 1. That coincidence
// is the point — it is exactly what made the fused seam pass for twelve tasks, and
// it is why this test asserts on the NAMESPACES (through the types) rather than on
// the numbers.
//
// THE TYPE ASSERTION IS THE LOAD-BEARING HALF. *Grant wraps a JournalWriter and no
// longer satisfies residency.Lease at all; *ResidencyLease does. A maintainer
// re-fusing them has to defeat the compiler first.
func TestTheResidencyGrantAndTheJournalGrantAreDifferentLeases(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	lease, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
	if lease.Epoch() == 0 {
		t.Fatal("the residency grant reports epoch 0, which Core refuses on every fenced record")
	}

	// THE CONTROL, and it is what makes the claim above measurable rather than
	// asserted: the journal grant over the SAME session is refused, because this
	// session is in disposition mode. So the two grants cannot even be held at
	// once here — there is no state in which one uint64 could be both.
	if _, err := adapted.OpenSession(t.Context(), testTenant, testSession); err == nil {
		t.Fatal("OpenSession granted a journal writer over a disposition-mode session")
	}

	// *Grant is not a residency.Lease, and *ResidencyLease is. The compile-time
	// assertions live in adapter_test.go; this reads the shape at run time so a
	// reader of THIS test sees why the substitution is impossible.
	var _ residency.Lease = lease
	if _, ok := any(lease).(*sessionstoreadapter.Grant); ok {
		t.Fatal("the residency lease is a journal Grant, so the two domains are fused again")
	}
}

// F16(a). AcquireResidency refuses a session whose immutable binding is not
// disposition mode, and Host does not soften that: Host reads catalog records and
// does not write them, so which mode a session carries is its creator's decision.
//
// BOTH DIRECTIONS. The legacy row is the refusal and the disposition row is the
// control that keeps the refusal from being "this fixture never acquires anything".
func TestAcquireSessionLeaseRefusesASessionThatIsNotInDispositionMode(t *testing.T) {
	for _, row := range []struct {
		name    string
		create  func(*testing.T, *sessionstore.Store)
		refused bool
	}{
		{name: "a legacy session", create: createSession, refused: true},
		{name: "control: a disposition session", create: createDispositionSession, refused: false},
	} {
		t.Run(row.name, func(t *testing.T) {
			released, adapted := openStore(t)
			row.create(t, released)

			lease, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
			if !row.refused {
				if err != nil {
					t.Fatalf("AcquireSessionLease on a disposition session: %v", err)
				}
				t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
				return
			}
			if err == nil {
				t.Fatal("AcquireSessionLease granted residency over a legacy-mode session")
			}
			if lease != nil {
				t.Fatalf("the refusal handed back a lease (%T), which a caller would attach under", lease)
			}
			var catalogErr *sessionstore.CatalogError
			if !errors.As(err, &catalogErr) {
				t.Fatalf("the refusal is %T, want the store's *CatalogError", err)
			}
			// THE REFUSAL IS NOT CONTENTION, and a caller that read it as
			// ErrLeaseHeld would tell Factory to re-place a session that has
			// nowhere better to go — §15's stampede.
			if errors.Is(err, residency.ErrLeaseHeld) {
				t.Error("a mode refusal was classified as another holder owning the lease")
			}
		})
	}
}

// CONTENTION IS ErrLeaseHeld, AND IT ARRIVES AS storage.LeaseHeldError.
//
// The residency grant is taken through storage.Leaser directly, so none of the
// journal vocabulary classifyJournal reads appears on this path — an adapter that
// reused that classifier would leave a second holder's refusal unclassified, and
// residency's step 2 would answer Factory with no HostLink code at all.
func TestAcquireSessionLeaseReportsASecondHolderAsLeaseHeld(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	first, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("first AcquireSessionLease: %v", err)
	}
	t.Cleanup(func() { _ = first.Release(context.WithoutCancel(t.Context())) })

	_, err = adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if !errors.Is(err, residency.ErrLeaseHeld) {
		t.Fatalf("the second acquisition = %v, want residency.ErrLeaseHeld", err)
	}
	// The store's own error survives as the cause rather than being replaced.
	var held *storage.LeaseHeldError
	if !errors.As(err, &held) {
		t.Errorf("the refusal does not carry the provider's *storage.LeaseHeldError: %v", err)
	}
}

// F3'S OTHER HALF, DISCHARGED. residency.Lease.Lost is a channel a heartbeat and
// a drain supervisor select on, and over the JOURNAL grant this package had to
// close it itself from the classification of a write Host made — an echo of Host's
// own traffic, silent for an idle session. ResidencyGrant.Lost is Storage's own
// lease channel, forwarded unmodified.
//
// WHAT THIS IS NOT IS A TAKEOVER TEST, and saying so is the honest half.
// ResidencyGrant's doc states memstore provides neither TTL nor crash takeover, so
// the only thing that closes this channel under this module's tests is a release.
// A real takeover needs pgstore behind PGSTORE_TEST_DSN and this package has no
// such lane; what is measured here is that the channel is the PROVIDER's and is
// live, which is the part Host owns.
//
// THE OPEN-BEFORE CHECK IS THE CONTROL. Without it, a Lost() that returned an
// already-closed channel — or one closed at construction — would pass.
func TestTheResidencyGrantsLossChannelIsTheProvidersAndIsLive(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	lease, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	select {
	case <-lease.Lost():
		t.Fatal("a freshly granted residency lease reports itself already lost")
	default:
	}

	if err := lease.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case <-lease.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("the loss channel did not close after the grant was released, so a heartbeat selecting on it would never learn")
	}
}

// F16(b). A REFUSED ACQUISITION CAN STILL OWE A RELEASE, and (Lease, error) cannot
// say so — which is why every fake in this module leaked it.
//
// THIS TEST MEASURES THE ARM THAT IS REACHABLE THROUGH THE STORE.
// AcquireResidency takes the provider grant and only then observes the caller's
// context is done, so a context cancelled at that point is a refusal the released
// store really produces — and when its own rollback SUCCEEDS it owes nothing, which
// is the control. The cleanup arm needs the rollback to fail as well, and memstore
// offers no way to make a release fail on demand; that arm is asserted over the
// classifier in classify_test.go, where the store's own error type can be
// constructed.
func TestARefusedAcquisitionCarriesItsCleanupObligation(t *testing.T) {
	t.Run("a cancelled acquisition whose cleanup succeeded owes nothing", func(t *testing.T) {
		released, adapted := openStore(t)
		createDispositionSession(t, released)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		lease, err := adapted.AcquireSessionLease(ctx, testTenant, testSession)
		if err == nil {
			t.Fatal("a cancelled acquisition was granted")
		}
		if lease != nil {
			t.Fatalf("the refusal handed back a lease (%T)", lease)
		}
		var owed *residency.LeaseCleanupError
		if errors.As(err, &owed) {
			t.Fatalf("a refusal whose rollback succeeded reported a cleanup obligation: %v", err)
		}
		// THE CONTROL FOR THE ROW ABOVE: the session is still acquirable, which
		// it would not be if the refused acquisition had leaked its lease.
		regained, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
		if err != nil {
			t.Fatalf("the cancelled acquisition leaked its grant: %v", err)
		}
		_ = regained.Release(context.WithoutCancel(t.Context()))
	})
}

// TestTheResidencyEpochIsTheProvidersNumberUnmodified is the value assertion the
// rest of this file was missing, and its absence was a real hole: the only thing
// asserted anywhere about ResidencyLease.Epoch was `!= 0`, so returning
// `grant.Epoch() + 1` survived the whole module. That is this lane's own rule —
// mutate the VALUE, not the mechanism — failing on the single value-producing
// method of the file it was added with.
//
// THE ANCHOR IS A DOCUMENTED PROVIDER FACT, not a coincidence, and the difference
// matters enough to write down. memstore's Leaser gives each name an independent,
// strictly increasing counter that advances on every grant, and its own tests pin
// the first grant's epoch at 1. So the two rows below are 1 and 2 by the
// PROVIDER's contract, and this test asserts that the adapter reports those
// numbers UNMODIFIED — an off-by-one reports 2 and 3, a doubling reports 2 and 4,
// and both die here. It does mean this test would have to move if the memory
// provider's base changed; that is the cost of anchoring a value assertion to
// something real instead of to itself.
//
// IT IS NOT A CLAIM THAT 1 IS A RESIDENCY EPOCH ANYWHERE ELSE. residency's fakes
// deliberately mint from 1000 precisely so that the provider's 1 cannot be
// confused with anything; here the 1 is the subject, not the fixture.
func TestTheResidencyEpochIsTheProvidersNumberUnmodified(t *testing.T) {
	released, adapted := openStore(t)
	createDispositionSession(t, released)

	first, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("first AcquireSessionLease: %v", err)
	}
	if got := first.Epoch(); got != 1 {
		t.Errorf("the first grant reports residency epoch %d, want the provider's 1", got)
	}
	if err := first.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// THE SECOND ROW IS WHAT MAKES THE FIRST AN ASSERTION ABOUT THE PROVIDER'S
	// COUNTER rather than about a constant: a method returning a hard-coded 1
	// passes the row above and fails this one.
	second, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("second AcquireSessionLease: %v", err)
	}
	t.Cleanup(func() { _ = second.Release(context.WithoutCancel(t.Context())) })
	if got := second.Epoch(); got != 2 {
		t.Errorf("the regranted lease reports residency epoch %d, want the provider's 2", got)
	}
}
