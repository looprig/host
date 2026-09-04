package sessionstoreadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// THE SEAMS ARE ASSERTED STRUCTURALLY AND THE ASSERTIONS ARE THE POINT. Every
// one of these was satisfied by a fake for twelve tasks; each line below is the
// first time the released store is required to satisfy it, and a drift on either
// side is now a compile failure rather than a fake that quietly kept agreeing
// with a shape nothing else has.
var (
	_ host.SessionStore       = (*sessionstoreadapter.Store)(nil)
	_ residency.DurableStore  = (*sessionstoreadapter.Store)(nil)
	_ residency.Locations     = (*sessionstoreadapter.Store)(nil)
	_ commands.CommandRecords = (*sessionstoreadapter.Store)(nil)
	_ commands.Applications   = (*sessionstoreadapter.Store)(nil)
	_ commands.Gates          = (*sessionstoreadapter.Store)(nil)
	_ commands.InboxWrites    = (*sessionstoreadapter.Store)(nil)
	_ residency.Lease         = (*sessionstoreadapter.Grant)(nil)
	_ commands.JournalWrites  = (*sessionstoreadapter.Grant)(nil)
)

const (
	testTenant  = sessionwire.TenantID("tenant-a")
	testSession = sessionwire.SessionID("session-a")
	testAgent   = sessionwire.AgentID("agent-a")
	testCompat  = "runtime-a"
)

// openStore opens the released store over Storage's memory provider and adapts
// it. Both stores in this package's tests share one backend so a test can watch
// the durable state the adapter wrote.
func openStore(t *testing.T, options ...sessionstoreadapter.Option) (*sessionstore.Store, *sessionstoreadapter.Store) {
	t.Helper()
	released, err := sessionstore.Open(t.Context(), memstore.New())
	if err != nil {
		t.Fatalf("open released store: %v", err)
	}
	t.Cleanup(func() { _ = released.Close(context.WithoutCancel(t.Context())) })
	adapted, err := sessionstoreadapter.New(released, options...)
	if err != nil {
		t.Fatalf("adapt store: %v", err)
	}
	return released, adapted
}

// createSession makes the durable catalog record a hydration reads.
func createSession(t *testing.T, released *sessionstore.Store) {
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
	}); err != nil {
		t.Fatalf("create catalog entry: %v", err)
	}
}

func TestNewRefusesNothingToAdapt(t *testing.T) {
	if _, err := sessionstoreadapter.New(nil); !errors.Is(err, sessionstoreadapter.ErrNoStore) {
		t.Fatalf("New(nil) = %v, want ErrNoStore", err)
	}
}

func TestNewRefusesANilOption(t *testing.T) {
	released, err := sessionstore.Open(t.Context(), memstore.New())
	if err != nil {
		t.Fatalf("open released store: %v", err)
	}
	t.Cleanup(func() { _ = released.Close(context.WithoutCancel(t.Context())) })
	if _, err := sessionstoreadapter.New(released, nil); !errors.Is(err, sessionstoreadapter.ErrNilOption) {
		t.Fatalf("New(store, nil) = %v, want ErrNilOption", err)
	}
}

// ---------------------------------------------------------------------------
// F4 and F5: members the released record has no source for
// ---------------------------------------------------------------------------

func TestLoadSessionStateRefusesWithoutANamespaceLayout(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	_, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
	var unavailable *sessionstoreadapter.UnavailableMemberError
	if !errors.As(err, &unavailable) || unavailable.Member != "Namespace" {
		t.Fatalf("LoadSessionState = %v, want an unavailable Namespace", err)
	}
}

// A COLD SESSION IS SERVABLE AND AN EXISTING ONE IS NOT, which is F5 stated as a
// behaviour rather than as prose. The create path needs no Harness identity, so
// it works against the released store; the restore path needs one the store has
// never held.
func TestLoadSessionStateServesTheCreatePathAndRefusesTheRestorePath(t *testing.T) {
	released, adapted := openStore(t, sessionstoreadapter.WithNamespaceLayout(
		func(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
			return string(tenant) + "/" + string(session)
		}))

	state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadSessionState for a cold session: %v", err)
	}
	if state.Exists {
		t.Fatal("a session with no catalog record reported Exists")
	}
	if state.Namespace != "tenant-a/session-a" {
		t.Fatalf("Namespace = %q, want the layout's answer", state.Namespace)
	}

	createSession(t, released)
	_, err = adapted.LoadSessionState(t.Context(), testTenant, testSession)
	var unavailable *sessionstoreadapter.UnavailableMemberError
	if !errors.As(err, &unavailable) || unavailable.Member != "RigSessionID" {
		t.Fatalf("LoadSessionState for an existing session = %v, want an unavailable RigSessionID", err)
	}
}

func TestLoadSessionStateReportsTheDurableRecordWhenBothMembersAreSupplied(t *testing.T) {
	rigSessionID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	released, adapted := openStore(t,
		sessionstoreadapter.WithNamespaceLayout(func(sessionwire.TenantID, sessionwire.SessionID) string {
			return "objects/a"
		}),
		sessionstoreadapter.WithRigSessionIDs(
			func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uuid.UUID, error) {
				return rigSessionID, nil
			}))
	createSession(t, released)

	state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadSessionState: %v", err)
	}
	if !state.Exists {
		t.Fatal("an existing session reported Exists=false")
	}
	if string(state.CompatibilityID) != testCompat {
		t.Fatalf("CompatibilityID = %q, want %q", state.CompatibilityID, testCompat)
	}
	if state.RigSessionID != rigSessionID {
		t.Fatalf("RigSessionID = %v, want %v", state.RigSessionID, rigSessionID)
	}
	if state.HasCheckpoint {
		t.Fatal("a session with no checkpoint reported one")
	}
}
