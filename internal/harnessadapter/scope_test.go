package harnessadapter_test

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
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
// The two layouts are mutually exclusive at the backend, and that is what these
// two tests measure: whichever store initializes a backend first, the other
// refuses it with keyspace layout_mismatch. So Host and the runtime it launches
// cannot share one storage backend AT ALL under the released modules.
//
// The consequence is not a translation gap, it is a correctness one, and it does
// not go away by giving them separate backends — it gets quieter.
// commands.Applier appends its own application prefix under Host's scope, drives
// the runtime, and then asks HOST'S scope what the journal proves. Harness's
// records are somewhere else, so the correlation reports ABSENT for every
// command harness actually applied — and absent is the one outcome that licenses
// Factory's deadline reconciler to settle `rejected`. That is a rejection
// written over a committed effect.
//
// Nothing in this repository can fix it. Harness must accept Host's
// (TenantID, SessionID) and file its records under them, or SessionStore must
// offer a way for two stores to share a keyspace. It is a release owed.
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

	if _, err := harnessstore.Open(backend); err == nil {
		t.Fatal("harness opened a backend Host had already initialized in the multi-tenant layout")
	}
}
