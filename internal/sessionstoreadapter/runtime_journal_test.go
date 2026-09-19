package sessionstoreadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/harnesstest"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// The runtime identity comes from the BINDING, and the journal decides a create
// ---------------------------------------------------------------------------

const journalBinding = "binding-journal"

// derivedRuntimeID is a runtime identity of the shape Factory derives (a
// UUIDv8 over the session identity). It is a literal, and it is NOT anything
// derivable from testSession: a Host that launched under the Core session id —
// or under a uuid computed from it — would name a different stream.
var derivedRuntimeID = uuid.MustParse("7c0e5b1a-94d2-8f3e-a6b1-3d58e0c2f917")

// createBoundSession makes Factory's disposition catalog record, whose
// immutable binding names runtimeID as the runtime session.
func createBoundSession(t *testing.T, released *sessionstore.Store, runtimeID string) {
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
		IdempotencyKey:         "idem-bound",
		Binding: sessionstore.SessionBinding{
			StorageBindingID: journalBinding,
			BindingVersion:   "v1",
			RuntimeSessionID: runtimeID,
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
	}); err != nil {
		t.Fatalf("create the bound catalog entry: %v", err)
	}
}

func namespaceLayout() sessionstoreadapter.Option {
	return sessionstoreadapter.WithNamespaceLayout(func(sessionwire.TenantID, sessionwire.SessionID) string { return "objects/a" })
}

// journalRouter serves one harness store as the (tenant, binding) reader.
func journalRouter(t *testing.T, reader sessionstore.DispositionEvidenceReader) *sessionstoreadapter.TenantEvidenceRouter {
	t.Helper()
	router, err := sessionstoreadapter.NewTenantEvidenceRouter(map[sessionstoreadapter.EvidenceKey]sessionstore.DispositionEvidenceReader{
		{TenantID: testTenant, StorageBindingID: journalBinding}: reader,
	})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	return router
}

// startConversation launches a real harness session under id, runs no turn,
// and releases its residency NONTERMINALLY — the state a released session is in
// when Factory re-places it.
func startConversation(t *testing.T, rigged interface {
	NewSessionUnder(context.Context, uuid.UUID) (session.SessionController, error)
}, id uuid.UUID) {
	t.Helper()
	controller, err := rigged.NewSessionUnder(t.Context(), id)
	if err != nil {
		t.Fatalf("launch under %s: %v", id, err)
	}
	releaser, ok := controller.(session.Releaser)
	if !ok {
		t.Fatal("the harness session is not a session.Releaser")
	}
	if err := releaser.ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("release residency: %v", err)
	}
}

// TestTheRigSessionIDIsTheBindingsRuntimeSessionID is R5 at the store: the
// Harness identity Host launches and restores under is the runtime session id
// the IMMUTABLE BINDING names — Factory's derived id — and never the Core
// session id. The RigSessionIDs collaborator is not consulted for a bound
// record, even when it answers something else.
func TestTheRigSessionIDIsTheBindingsRuntimeSessionID(t *testing.T) {
	collaboratorAnswer := uuid.MustParse("99999999-8888-4777-8666-555555555555")
	consulted := false
	released, adapted := openStore(t, namespaceLayout(),
		sessionstoreadapter.WithRigSessionIDs(func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uuid.UUID, error) {
			consulted = true
			return collaboratorAnswer, nil
		}))
	createBoundSession(t, released, derivedRuntimeID.String())

	state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadSessionState: %v", err)
	}
	if state.RigSessionID != derivedRuntimeID {
		t.Fatalf("RigSessionID = %v, want the binding's runtime session id %v", state.RigSessionID, derivedRuntimeID)
	}
	if consulted {
		t.Fatal("the RigSessionIDs collaborator was consulted for a record whose binding names the runtime session")
	}
}

// TestABindingThatNamesNoUUIDIsRefused: the store accepts any bounded opaque
// runtime session id, and Harness identifies a session by UUID. A binding Host
// cannot represent is refused before any launch, never read as the zero id.
func TestABindingThatNamesNoUUIDIsRefused(t *testing.T) {
	for _, runtimeID := range []string{"runtime-session-1", "00000000-0000-0000-0000-000000000000"} {
		t.Run(runtimeID, func(t *testing.T) {
			released, adapted := openStore(t, namespaceLayout())
			createBoundSession(t, released, runtimeID)
			_, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
			var refused *sessionstoreadapter.RuntimeSessionIDError
			if !errors.As(err, &refused) {
				t.Fatalf("LoadSessionState over runtime session %q = %v, want *RuntimeSessionIDError", runtimeID, err)
			}
		})
	}
}

// TestTheRuntimeJournalIsReadFromTheHarnessStore holds the three answers, each
// against the released harness store: no conversation under the runtime id is
// Absent; one is Present; and a composition that gives this Host no harness
// store for the session's binding is Unknown — which refuses a create.
func TestTheRuntimeJournalIsReadFromTheHarnessStore(t *testing.T) {
	t.Run("absent, then present", func(t *testing.T) {
		journal := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
		released, adapted := openStore(t, namespaceLayout(), sessionstoreadapter.WithRuntimeJournals(journalRouter(t, journal)))
		createBoundSession(t, released, derivedRuntimeID.String())

		state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
		if err != nil {
			t.Fatalf("LoadSessionState: %v", err)
		}
		if state.RuntimeJournal != residency.RuntimeJournalAbsent {
			t.Fatalf("RuntimeJournal before any launch = %v, want Absent", state.RuntimeJournal)
		}

		startConversation(t, harnesstest.Launcher(harnesstest.Rig(t, journal, &harnesstest.RecordingLLM{})), derivedRuntimeID)

		state, err = adapted.LoadSessionState(t.Context(), testTenant, testSession)
		if err != nil {
			t.Fatalf("LoadSessionState: %v", err)
		}
		if state.RuntimeJournal != residency.RuntimeJournalPresent {
			t.Fatalf("RuntimeJournal after a conversation began = %v, want Present", state.RuntimeJournal)
		}
	})

	t.Run("another runtime id's conversation is not this one", func(t *testing.T) {
		journal := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
		startConversation(t, harnesstest.Launcher(harnesstest.Rig(t, journal, &harnesstest.RecordingLLM{})), uuid.MustParse("12345678-1234-4234-8234-123456789abc"))
		released, adapted := openStore(t, namespaceLayout(), sessionstoreadapter.WithRuntimeJournals(journalRouter(t, journal)))
		createBoundSession(t, released, derivedRuntimeID.String())
		state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
		if err != nil {
			t.Fatalf("LoadSessionState: %v", err)
		}
		if state.RuntimeJournal != residency.RuntimeJournalAbsent {
			t.Fatalf("RuntimeJournal = %v, want Absent: the conversation in the store is another runtime session's", state.RuntimeJournal)
		}
	})

	t.Run("unknown without a harness store for the binding", func(t *testing.T) {
		for name, options := range map[string][]sessionstoreadapter.Option{
			"no journals configured":     {namespaceLayout()},
			"reader is not harness's":    {namespaceLayout(), sessionstoreadapter.WithRuntimeJournals(journalRouter(t, opaqueReader{}))},
			"binding served by no store": {namespaceLayout(), sessionstoreadapter.WithRuntimeJournals(otherBindingRouter(t))},
		} {
			t.Run(name, func(t *testing.T) {
				released, adapted := openStore(t, options...)
				createBoundSession(t, released, derivedRuntimeID.String())
				state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
				if err != nil {
					t.Fatalf("LoadSessionState: %v", err)
				}
				if state.RuntimeJournal != residency.RuntimeJournalUnknown {
					t.Fatalf("RuntimeJournal = %v, want Unknown", state.RuntimeJournal)
				}
				if state.RigSessionID != derivedRuntimeID {
					t.Fatalf("RigSessionID = %v, want %v", state.RigSessionID, derivedRuntimeID)
				}
			})
		}
	})
}

// TestTheJournalIsTheAuthorityAndTheCatalogOnlyAShortcut separates the two
// reads. harness's catalog is a BEST-EFFORT cache folded after each append, so
// its absence proves nothing: a conversation whose catalog entry is missing is
// still Present, read from the ledger. And a catalog entry is taken as Present
// without a ledger read, which is the cheap path the owner ruling names.
func TestTheJournalIsTheAuthorityAndTheCatalogOnlyAShortcut(t *testing.T) {
	written := harnesstest.Backend(t)
	writer := harnesstest.Store(t, written, testTenant)
	startConversation(t, harnesstest.Launcher(harnesstest.Rig(t, writer, &harnesstest.RecordingLLM{})), derivedRuntimeID)

	for name, probe := range map[string]*struct{ ledger, kv bool }{
		"ledger present, catalog lost":   {ledger: true, kv: false},
		"catalog present, ledger unread": {ledger: false, kv: true},
	} {
		t.Run(name, func(t *testing.T) {
			ledger, kv := harnesstest.Backend(t), harnesstest.Backend(t)
			if probe.ledger {
				ledger = written
			}
			if probe.kv {
				kv = written
			}
			journal := harnesstest.Store(t, harnesstest.Compose(t, ledger, kv), testTenant)
			released, adapted := openStore(t, namespaceLayout(), sessionstoreadapter.WithRuntimeJournals(journalRouter(t, journal)))
			createBoundSession(t, released, derivedRuntimeID.String())
			state, err := adapted.LoadSessionState(t.Context(), testTenant, testSession)
			if err != nil {
				t.Fatalf("LoadSessionState: %v", err)
			}
			if state.RuntimeJournal != residency.RuntimeJournalPresent {
				t.Fatalf("RuntimeJournal = %v, want Present", state.RuntimeJournal)
			}
		})
	}
}

// opaqueReader is an evidence reader that is not a harness store.
type opaqueReader struct{}

// ReadDispositionEvidence refuses; it is never reached here.
func (opaqueReader) ReadDispositionEvidence(context.Context, sessionstore.DispositionEvidenceRequest) (sessionstore.DispositionEvidence, error) {
	return sessionstore.DispositionEvidence{}, errors.New("opaque")
}

// otherBindingRouter serves a harness store under a binding no session names.
func otherBindingRouter(t *testing.T) *sessionstoreadapter.TenantEvidenceRouter {
	t.Helper()
	router, err := sessionstoreadapter.NewTenantEvidenceRouter(map[sessionstoreadapter.EvidenceKey]sessionstore.DispositionEvidenceReader{
		{TenantID: testTenant, StorageBindingID: "binding-elsewhere"}: harnesstest.Store(t, harnesstest.Backend(t), testTenant),
	})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	return router
}
