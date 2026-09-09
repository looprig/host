package harnessadapter_test

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// FINDING H9, MEASURED. This is the one finding in this task that no adapter can
// absorb, so it is measured rather than described, and it is measured in both
// directions because a one-directional result would look like an ordering bug.
//
// Host addresses SessionStore by (TenantID, SessionID) — the identities Factory
// admitted the session under. harness/pkg/sessionstore opens the SAME released
// store with WithLegacySingleTenant("local") and derives its session id from the
// Harness UUID, so every record a live session commits — the opening fence, the
// public events, and the EnvelopeKindApplicationPrefix that
// runtimecommand.Applier writes before each effect — is filed under
// ("local", "<uuid>") in the LEGACY SINGLE-TENANT LAYOUT.
//
// ON THE DEFAULTS the two layouts are mutually exclusive at the backend, and
// that is what these first two tests measure: whichever store initializes a
// backend first, the other refuses it with keyspace layout_mismatch.
//
// THE "AT ALL" IS NO LONGER TRUE, AND THE CORRECTION IS MEASURED BELOW. An
// earlier version of this comment said Host and the runtime it launches "cannot
// share one storage backend AT ALL under the released modules", that "nothing in
// this repository can fix it", and that it was "a release owed". harness v0.32.0
// adds WithTenant, whose own doc recites these failure modes as the thing it
// exists to fix, and the O3.3 rebind measured the result rather than taking the
// doc's word: see TestHostAndHarnessCanShareABackendOnTheLegacyLayout.
//
// WHAT REMAINS TRUE. The two tests below still pass unchanged, because they
// exercise the DEFAULTS and the defaults did not move: harness still files under
// "local" unless told otherwise, and Host's native open is still multi-tenant.
// Sharing is possible, not automatic, and it is not free: the two costs are
// asserted on the sharing test below. The correlation failure described above
// is therefore still reachable; it is now a configuration Host can avoid
// rather than a wall.
func TestHostCannotOpenABackendHarnessInitialized(t *testing.T) {
	backend := memstore.New()
	rigSessionID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	harnessSide, err := harnessstore.Open(backend)
	if err != nil {
		t.Fatalf("open the harness session store: %v", err)
	}
	lease, err := harnessSide.AcquireLease(t.Context(), rigSessionID)
	if err != nil {
		t.Fatalf("acquire the harness lease: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
	if _, err := harnessSide.OpenJournal(t.Context(), rigSessionID, lease); err != nil {
		t.Fatalf("open the harness journal: %v", err)
	}

	_, err = sessionstore.Open(t.Context(), backend)
	var keyspaceErr *sessionstore.KeyspaceError
	if !errors.As(err, &keyspaceErr) {
		t.Fatalf("Host's Open over a harness-initialized backend = %v, want a KeyspaceError", err)
	}
	if keyspaceErr.Code != sessionstore.KeyspaceLayoutMismatch {
		t.Fatalf("keyspace code = %q, want layout_mismatch", keyspaceErr.Code)
	}
}

func TestHarnessCannotOpenABackendHostInitialized(t *testing.T) {
	backend := memstore.New()

	hostSide, err := sessionstore.Open(t.Context(), backend)
	if err != nil {
		t.Fatalf("open the released store as Host does: %v", err)
	}
	t.Cleanup(func() { _ = hostSide.Close(context.WithoutCancel(t.Context())) })
	if _, _, err := hostSide.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID:               sessionwire.TenantID("tenant-a"),
		SessionID:              sessionwire.SessionID("session-a"),
		AgentID:                sessionwire.AgentID("agent-a"),
		RuntimeCompatibilityID: "runtime-a",
		CreatedAt:              testInstant(),
		LastActiveAt:           testInstant(),
		State:                  sessionwire.SessionStateRunning,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "idem-1",
	}); err != nil {
		t.Fatalf("create Host's catalog record: %v", err)
	}

	// THE CODE, NOT MERELY AN ERROR. An assertion on err != nil would pass for
	// an unrelated *InvalidBackendError and would keep passing if the layouts
	// ever became compatible for a different reason, which is the one outcome
	// this test exists to detect a change in.
	_, err = harnessstore.Open(backend)
	var keyspaceErr *sessionstore.KeyspaceError
	if !errors.As(err, &keyspaceErr) {
		t.Fatalf("harness Open over a Host-initialized backend = %v, want a KeyspaceError", err)
	}
	if keyspaceErr.Code != sessionstore.KeyspaceLayoutMismatch {
		t.Fatalf("keyspace code = %q, want layout_mismatch", keyspaceErr.Code)
	}
}

// H9 RE-AUDITED AT harness v0.32.0. This is the measurement behind the
// correction above, and it is deliberately three assertions rather than one,
// because "they can share a backend" is true only with two qualifications and
// reporting it without them would be the overstatement this file exists to
// avoid.
//
// WHAT WithTenant DISCHARGES: the tenant half. harness's Store no longer fixes
// the tenant at "local"; it files under whatever tenant it is opened with, so a
// Host that admitted a session as (TenantID, SessionID) can name the same
// tenant and both stores open the same backend, in either initialization order.
//
// COST 1 — HOST LOSES ITS NATIVE LAYOUT. Sharing requires Host to open with
// WithLegacySingleTenant. Host's ordinary multi-tenant open over a
// harness-initialized backend still fails layout_mismatch, which the first
// subtest asserts rather than assumes. A Host that shares a backend with its
// runtime is a single-tenant Host for that backend.
//
// COST 2 — AND THIS IS THE HALF STILL OWED. On the legacy layout the session is
// addressed by the Harness UUID's canonical rendering, and nothing else
// resolves: a SessionID that is not a UUID is refused with
// KeyspaceError{legacy_session}. Host's SessionID is the identity Factory
// admitted the session under, which CLAUDE.md states is an opaque sessionwire
// string and NOT a UUID by contract. So sharing works exactly when Host's
// session identity happens to be the rig's UUID, which Host does not control
// and cannot in general arrange. THAT is what is still a release owed, and it is
// narrower than the sentence it replaces: not "they cannot share", but "Harness
// still imposes its own identity grammar on the shared keyspace".
func TestHostAndHarnessCanShareABackendOnTheLegacyLayout(t *testing.T) {
	const sharedTenant = sessionwire.TenantID("tenant-shared")
	rigSessionID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	openHarness := func(t *testing.T, backend *storage.Composite) {
		t.Helper()
		harnessSide, err := harnessstore.Open(backend, harnessstore.WithTenant(sharedTenant))
		if err != nil {
			t.Fatalf("harness Open with WithTenant: %v", err)
		}
		lease, err := harnessSide.AcquireLease(t.Context(), rigSessionID)
		if err != nil {
			t.Fatalf("acquire the harness lease: %v", err)
		}
		t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
		if _, err := harnessSide.OpenJournal(t.Context(), rigSessionID, lease); err != nil {
			t.Fatalf("open the harness journal: %v", err)
		}
	}

	// COST 1, ASSERTED. Host's NATIVE open is still refused, so the discharge
	// below is not "the layouts became compatible".
	t.Run("host's multi-tenant layout is still refused", func(t *testing.T) {
		backend := memstore.New()
		openHarness(t, backend)

		_, err := sessionstore.Open(t.Context(), backend)
		var keyspaceErr *sessionstore.KeyspaceError
		if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != sessionstore.KeyspaceLayoutMismatch {
			t.Fatalf("Host's native Open over a harness-initialized backend = %v, want layout_mismatch", err)
		}
	})

	// THE DISCHARGE, IN BOTH INITIALIZATION ORDERS. One order alone would look
	// like an ordering artefact, which is why the two tests above are paired.
	t.Run("both open the same backend on the legacy layout", func(t *testing.T) {
		backend := memstore.New()
		openHarness(t, backend)

		hostSide, err := sessionstore.Open(t.Context(), backend, sessionstore.WithLegacySingleTenant(sharedTenant))
		if err != nil {
			t.Fatalf("Host's legacy-single-tenant Open over a harness-initialized backend: %v", err)
		}
		t.Cleanup(func() { _ = hostSide.Close(context.WithoutCancel(t.Context())) })

		// AND THE ADDRESS RESOLVES, not merely the Open. An Open that succeeded
		// while every read missed would be the same defect one layer down.
		//
		// THE ASSERTION IS ON CapturedTip, NOT ON err == nil, and the difference
		// is the whole finding. ReadPublicJournal answers an EMPTY PAGE without
		// error for a session that does not exist in this scope, so a nil error
		// says only that the request was well formed. An earlier version of this
		// test checked the error alone and therefore passed when pointed at a
		// canonical UUID no harness store had ever created — it asserted exactly
		// nothing about the address resolving, which is the one thing it exists
		// to establish.
		page, err := hostSide.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{
			TenantID:  sharedTenant,
			SessionID: sessionwire.SessionID(rigSessionID.String()),
			Limit:     10,
		})
		if err != nil {
			t.Fatalf("Host's read of harness's session scope: %v", err)
		}
		if page.CapturedTip == 0 {
			t.Fatalf("Host read harness's session scope and found an empty journal (CapturedTip=0); the Open succeeded but the address did not resolve")
		}

		// THE CONTROL, IN THE SAME TEST. A session identity nothing created must
		// come back with a tip of zero. Without it, "CapturedTip > 0" could be
		// satisfied by a store that answered every address alike, and the
		// assertion above would be measuring the wrong thing while passing.
		bogus, err := hostSide.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{
			TenantID:  sharedTenant,
			SessionID: sessionwire.SessionID("99999999-9999-9999-9999-999999999999"),
			Limit:     10,
		})
		if err != nil {
			t.Fatalf("reading a session identity nothing created: %v", err)
		}
		if bogus.CapturedTip != 0 {
			t.Fatalf("a session identity nothing created reported CapturedTip=%d, so the tip does not distinguish scopes and the assertion above proves nothing", bogus.CapturedTip)
		}
	})

	t.Run("host first, then harness", func(t *testing.T) {
		backend := memstore.New()
		hostSide, err := sessionstore.Open(t.Context(), backend, sessionstore.WithLegacySingleTenant(sharedTenant))
		if err != nil {
			t.Fatalf("Host's legacy Open: %v", err)
		}
		t.Cleanup(func() { _ = hostSide.Close(context.WithoutCancel(t.Context())) })
		if _, err := harnessstore.Open(backend, harnessstore.WithTenant(sharedTenant)); err != nil {
			t.Fatalf("harness Open over a Host-initialized legacy backend: %v", err)
		}
	})

	// COST 2, ASSERTED. The half still owed.
	t.Run("a non-uuid session identity is refused", func(t *testing.T) {
		backend := memstore.New()
		openHarness(t, backend)
		hostSide, err := sessionstore.Open(t.Context(), backend, sessionstore.WithLegacySingleTenant(sharedTenant))
		if err != nil {
			t.Fatalf("Host's legacy Open: %v", err)
		}
		t.Cleanup(func() { _ = hostSide.Close(context.WithoutCancel(t.Context())) })

		_, err = hostSide.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{
			TenantID:  sharedTenant,
			SessionID: sessionwire.SessionID("factory-admitted-opaque-id"),
			Limit:     10,
		})
		var keyspaceErr *sessionstore.KeyspaceError
		if !errors.As(err, &keyspaceErr) {
			t.Fatalf("a non-uuid SessionID on the legacy layout = %v, want a KeyspaceError", err)
		}
		if keyspaceErr.Code != sessionstore.KeyspaceLegacySession {
			t.Fatalf("keyspace code = %q, want legacy_session", keyspaceErr.Code)
		}
	})

	// A DIFFERENT TENANT IS STILL REFUSED, so the discharge is bounded and is
	// not "any two stores may now share any backend".
	t.Run("a mismatched tenant is still refused", func(t *testing.T) {
		backend := memstore.New()
		openHarness(t, backend)
		_, err := sessionstore.Open(t.Context(), backend, sessionstore.WithLegacySingleTenant(sessionwire.TenantID("other-tenant")))
		var keyspaceErr *sessionstore.KeyspaceError
		if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != sessionstore.KeyspaceLayoutMismatch {
			t.Fatalf("a mismatched tenant = %v, want layout_mismatch", err)
		}
	})
}
