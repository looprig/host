package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnessadapter"
	"github.com/looprig/host/internal/harnesstest"
)

// ---------------------------------------------------------------------------
// The restart bug, end to end over the REAL harness runtime
// ---------------------------------------------------------------------------
//
// Factory sends every attach as create. Before v0.3.0 a composed Host launched
// every create fresh, with no identity, so a session released by one Host and
// re-placed on another started its conversation over — and had the launch
// named the binding's id, harness would have re-opened that stream with a
// second SessionStarted and restored nothing. These tests compose real Hosts
// over the released session store, launch through harnessadapter onto a real
// rig over a real harness journal, and read the conversation back out of that
// journal and out of what the model is sent.

// capturingLauncher is the released rig, recording every controller it
// launches or restores so a test can drive the conversation directly.
type capturingLauncher struct {
	rig *rig.Rig

	mu       sync.Mutex
	creates  int
	restores []uuid.UUID
	last     session.SessionController

	// holdRestore, when set, parks every restore until it is closed, which is
	// how a test holds a successor between its residency grant and its
	// runtime's restore.
	holdRestore chan struct{}
}

// NewSession launches through the real rig.
func (l *capturingLauncher) NewSession(ctx context.Context, options ...rig.SessionOption) (session.SessionController, error) {
	controller, err := l.rig.NewSession(ctx, options...)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.creates++
	if err == nil {
		l.last = controller
	}
	return controller, err
}

// RestoreSession restores through the real rig.
func (l *capturingLauncher) RestoreSession(ctx context.Context, id uuid.UUID) (session.SessionController, error) {
	if l.holdRestore != nil {
		select {
		case <-l.holdRestore:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	controller, err := l.rig.RestoreSession(ctx, id)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.restores = append(l.restores, id)
	if err == nil {
		l.last = controller
	}
	return controller, err
}

// RigForCreate resolves every launch to the one rig.
func (l *capturingLauncher) RigForCreate(context.Context, department.RigCreateRequest) (harnessadapter.Launcher, error) {
	return l, nil
}

// RigForRestore resolves every relaunch to the one rig.
func (l *capturingLauncher) RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (harnessadapter.Launcher, error) {
	return l, nil
}

// counts reports how many creates and which restores the launcher saw.
func (l *capturingLauncher) counts() (int, []uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.creates, append([]uuid.UUID(nil), l.restores...)
}

// controller returns the most recently launched controller.
func (l *capturingLauncher) controller() session.SessionController {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last
}

// realRuntimeWorld is one orchestration backend and one harness journal that
// several Hosts, in turn, run the same session over.
type realRuntimeWorld struct {
	fixture   *composeFixture
	journal   *harnessstore.Store
	llm       *harnesstest.RecordingLLM
	runtimeID uuid.UUID
}

func newRealRuntimeWorld(t *testing.T) *realRuntimeWorld {
	t.Helper()
	fixture := newComposeFixture(t)
	// The runtime id is Factory's DERIVED id — a UUID that is not, and cannot
	// be computed from, the Core session id. The binding is the only record of
	// it, which is the point.
	runtimeID := uuid.MustParse("0f4d2a6c-81b3-8e57-9c20-6a1e7d3b5f48")
	world := &realRuntimeWorld{fixture: fixture, journal: fixture.journal, llm: &harnesstest.RecordingLLM{}, runtimeID: runtimeID}
	other := fixture.otherParty(t)
	now := time.Now().UTC()
	if _, _, err := other.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID:               composeTenant,
		SessionID:              composeSession,
		AgentID:                composeAgent,
		RuntimeCompatibilityID: string(composeCompat),
		CreatedAt:              now,
		LastActiveAt:           now,
		State:                  sessionwire.SessionStateRunning,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "idem-real",
		Binding: sessionstore.SessionBinding{
			StorageBindingID: composeBinding,
			BindingVersion:   "v1",
			RuntimeSessionID: runtimeID.String(),
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
	}); err != nil {
		t.Fatalf("seed the catalog: %v", err)
	}
	return world
}

// host composes and starts one Host over the world, at a generation of its
// own, launching through a real rig over the shared journal. journalStores, if
// non-nil, replaces the composition's journal table.
func (w *realRuntimeWorld) host(t *testing.T, generation uint64, journalStores map[host.EvidenceKey]sessionstore.DispositionEvidenceReader) (*host.Service, *capturingLauncher) {
	t.Helper()
	launcher := &capturingLauncher{rig: harnesstest.Rig(t, w.journal, w.llm)}
	adapter, err := harnessadapter.New(launcher)
	if err != nil {
		t.Fatalf("harnessadapter.New: %v", err)
	}
	blueprint := w.fixture.blueprint(t)
	blueprint.Generation = generation
	blueprint.Collaborators.Registrar = host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		target, err := department.NewRigTarget(adapter, composeCompat, department.Capabilities{
			SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming,
		})
		if err != nil {
			return nil, err
		}
		return []department.Registration{{AgentID: composeAgent, Target: target}}, nil
	})
	if journalStores != nil {
		blueprint.Collaborators.JournalStores = journalStores
	}
	service, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return service, launcher
}

// attachAsFactoryDoes attaches in the only mode Factory ever sends.
func attachAsFactoryDoes(t *testing.T, service *host.Service) (host.Residency, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return service.Attach(ctx, host.AttachRequest{
		TenantID: composeTenant, SessionID: composeSession, AgentID: composeAgent,
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory",
	})
}

// turn runs one user turn on the controller and waits until it is DONE in the
// journal. Submit only enqueues, so an idle wait issued straight after it can
// return before the turn has begun; the journal's TurnDone count is the
// condition that cannot.
func (w *realRuntimeWorld) turn(t *testing.T, controller session.SessionController, text string) {
	t.Helper()
	done := countOf[event.TurnDone](t, w.journal, w.runtimeID)
	if _, err := controller.Submit(t.Context(), []content.Block{&content.TextBlock{Text: text}}); err != nil {
		t.Fatalf("Submit(%q): %v", text, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for countOf[event.TurnDone](t, w.journal, w.runtimeID) == done {
		if time.Now().After(deadline) {
			t.Fatalf("the turn %q did not finish within 10s", text)
		}
		time.Sleep(5 * time.Millisecond)
	}
	idle, ok := controller.(session.IdleWaiter)
	if !ok {
		t.Fatal("the harness session is not a session.IdleWaiter")
	}
	idleCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := idle.WaitIdle(idleCtx); err != nil {
		t.Fatalf("WaitIdle after %q: %v", text, err)
	}
}

const rememberedWord = "the remembered word is PERSIMMON"

// TestAReleasedSessionReattachedAsCreateResumesTheSameConversation is the
// required proof. Host A creates the session under the binding's runtime id
// and a turn is run; A drains, releasing it; Host B is asked to attach it as
// CREATE — as Factory always asks — and restores it instead: one stream, one
// SessionStarted, and the next turn is run over the first one.
func TestAReleasedSessionReattachedAsCreateResumesTheSameConversation(t *testing.T) {
	world := newRealRuntimeWorld(t)

	first, firstLauncher := world.host(t, 4, nil)
	if _, err := attachAsFactoryDoes(t, first); err != nil {
		t.Fatalf("Host A attach(create) of a new session: %v", err)
	}
	creates, restores := firstLauncher.counts()
	if creates != 1 || len(restores) != 0 {
		t.Fatalf("Host A: creates = %d, restores = %v; a genuinely new session must CREATE", creates, restores)
	}
	if got := firstLauncher.controller().SessionID(); got != world.runtimeID {
		t.Fatalf("Host A launched runtime session %v, want the binding's %v (R5)", got, world.runtimeID)
	}
	world.turn(t, firstLauncher.controller(), rememberedWord)
	stopWithin(t, first)
	if got := harnesstest.CountSessionStarted(t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("after Host A: %d SessionStarted under the runtime id, want 1", got)
	}

	second, secondLauncher := world.host(t, 5, nil)
	t.Cleanup(func() { stopBounded(second) })
	if _, err := attachAsFactoryDoes(t, second); err != nil {
		t.Fatalf("Host B attach(create) of the released session: %v", err)
	}
	creates, restores = secondLauncher.counts()
	if creates != 0 {
		t.Fatalf("Host B CREATED %d sessions over an existing conversation; that is the silent restart", creates)
	}
	if len(restores) != 1 || restores[0] != world.runtimeID {
		t.Fatalf("Host B restored %v, want exactly the binding's runtime session %v", restores, world.runtimeID)
	}
	if got := harnesstest.CountSessionStarted(t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("after Host B: %d SessionStarted under the runtime id, want 1 — a second one is a restarted conversation", got)
	}
	if got := countOf[event.RestoreDone](t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("the journal records %d RestoreDone, want exactly 1: Host B restored the conversation once", got)
	}

	before := len(world.llm.Requests())
	world.turn(t, secondLauncher.controller(), "what was the word?")
	requests := world.llm.Requests()
	if len(requests) <= before {
		t.Fatal("the turn on Host B sent the model nothing")
	}
	sent, err := json.Marshal(requests[len(requests)-1].Messages)
	if err != nil {
		t.Fatalf("marshal the request: %v", err)
	}
	if !strings.Contains(string(sent), "PERSIMMON") {
		t.Fatalf("the turn on Host B was run over a conversation that does not contain Host A's turn; the model was sent %s", sent)
	}
}

// TestACreateThatCannotBeRestoredIsRefusedAndStartsNothingOver: the session has
// a conversation, and the Host asked to attach it holds no journal store for
// the session's binding (its table serves another binding), so it cannot tell
// a new session from a re-placed one. It refuses with runtime_unavailable
// rather than launching — and the journal still holds exactly the one
// conversation it held.
func TestACreateThatCannotBeRestoredIsRefusedAndStartsNothingOver(t *testing.T) {
	world := newRealRuntimeWorld(t)
	first, firstLauncher := world.host(t, 4, nil)
	if _, err := attachAsFactoryDoes(t, first); err != nil {
		t.Fatalf("Host A attach: %v", err)
	}
	world.turn(t, firstLauncher.controller(), rememberedWord)
	stopWithin(t, first)

	elsewhere := map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: composeTenant, StorageBindingID: "binding-elsewhere"}: world.journal}
	blind, blindLauncher := world.host(t, 5, elsewhere)
	t.Cleanup(func() { stopBounded(blind) })
	_, err := attachAsFactoryDoes(t, blind)
	var refused *host.AttachError
	if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("attach(create) with no journal store for the binding = %v, want an AttachError with runtime_unavailable", err)
	}
	if creates, restores := blindLauncher.counts(); creates != 0 || len(restores) != 0 {
		t.Fatalf("the refusing Host launched anyway: creates = %d, restores = %v", creates, restores)
	}
	if got := harnesstest.CountSessionStarted(t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("%d SessionStarted after the refusal, want the original 1", got)
	}
}

// TestComposeRefusesAJournalReaderThatIsNotAHarnessStore: a reader that cannot
// answer the create decision would make every create refuse at the first
// placement; Compose refuses it at startup instead. The control is the
// fixture's harness store, accepted.
func TestComposeRefusesAJournalReaderThatIsNotAHarnessStore(t *testing.T) {
	f := newComposeFixture(t)
	blueprint := f.blueprint(t)
	blueprint.Collaborators.JournalStores = map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: composeTenant, StorageBindingID: composeBinding}: stubEvidence{}}
	service, err := host.Compose(t.Context(), blueprint)
	var invalid *host.InvalidCompositionError
	if !errors.As(err, &invalid) || invalid.Field != "Collaborators.JournalStores" {
		if service != nil {
			stopBounded(service)
		}
		t.Fatalf("Compose with a non-harness journal reader = %v, want an InvalidCompositionError on Collaborators.JournalStores", err)
	}
	service, err = host.Compose(t.Context(), f.blueprint(t))
	if err != nil {
		t.Fatalf("control: Compose with a harness journal store = %v", err)
	}
	stopBounded(service)
}

// servesTheBinding is a journal table serving the fixture's binding with reader.
func servesTheBinding(reader sessionstore.DispositionEvidenceReader) map[host.EvidenceKey]sessionstore.DispositionEvidenceReader {
	return map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: composeTenant, StorageBindingID: composeBinding}: reader}
}

// countOf counts the events of type E filed under id.
func countOf[E event.Event](t *testing.T, store *harnessstore.Store, id uuid.UUID) int {
	t.Helper()
	count := 0
	for _, ev := range harnesstest.Events(t, store, id) {
		if _, ok := ev.(E); ok {
			count++
		}
	}
	return count
}

// errLedgerDown is the injected storage fault.
var errLedgerDown = errors.New("injected: the ledger is unreachable")

// unreadableLedger fails every read of the journal it wraps: outright
// (readFails), or at the cursor's first Next.
type unreadableLedger struct {
	storage.Ledger
	readFails bool
}

// Read fails, or returns a cursor whose Next fails.
func (l unreadableLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	if l.readFails {
		return nil, errLedgerDown
	}
	inner, err := l.Ledger.Read(ctx, name, from)
	if err != nil {
		return nil, err
	}
	return unreadableCursor{Cursor: inner}, nil
}

// unreadableCursor fails every Next.
type unreadableCursor struct{ storage.Cursor }

// Next fails with the injected fault.
func (unreadableCursor) Next(context.Context) (storage.Record, error) {
	return storage.Record{}, errLedgerDown
}

// TestAJournalReadFailureRefusesTheAttachAndStartsNothingOver is the fail-closed
// rule end to end, under a storage fault: the session HAS a conversation, the
// Host asked to attach it reads the same ledger through a store whose catalog
// entry is missing (the best-effort cache lost it) and whose ledger read fails.
// The attach is refused — with the EMPTY, unclassified code, the one every other
// durable-read failure gets (runtime_unavailable is reserved for a binding with
// no harness journal store) — nothing is launched, and the conversation still
// has exactly one SessionStarted.
func TestAJournalReadFailureRefusesTheAttachAndStartsNothingOver(t *testing.T) {
	for name, readFails := range map[string]bool{"the ledger read fails": true, "the cursor's Next fails": false} {
		t.Run(name, func(t *testing.T) {
			world := newRealRuntimeWorld(t)
			first, firstLauncher := world.host(t, 4, nil)
			if _, err := attachAsFactoryDoes(t, first); err != nil {
				t.Fatalf("Host A attach: %v", err)
			}
			world.turn(t, firstLauncher.controller(), rememberedWord)
			stopWithin(t, first)

			backend := world.fixture.journalBackend
			unreadable, err := storage.NewCompositeWithOrderedIndex(unreadableLedger{Ledger: backend.Ledger, readFails: readFails},
				backend.Leaser, harnesstest.Backend(t).KV, backend.Blobs, backend.OrderedIndex)
			if err != nil {
				t.Fatal(err)
			}
			reader := harnesstest.Store(t, unreadable, composeTenant)
			second, secondLauncher := world.host(t, 5, servesTheBinding(reader))
			t.Cleanup(func() { stopBounded(second) })

			_, err = attachAsFactoryDoes(t, second)
			var refused *host.AttachError
			if !errors.As(err, &refused) || refused.Code != "" || !errors.Is(err, errLedgerDown) {
				t.Fatalf("attach(create) over an unreadable journal = %v, want an AttachError with the empty code carrying the ledger fault", err)
			}
			if creates, restores := secondLauncher.counts(); creates != 0 || len(restores) != 0 {
				t.Fatalf("the Host launched over an unreadable journal: creates = %d, restores = %v", creates, restores)
			}
			if got := harnesstest.CountSessionStarted(t, world.journal, world.runtimeID); got != 1 {
				t.Fatalf("%d SessionStarted after the refusal, want the original 1", got)
			}
		})
	}
}

// stopWithin drains a Host, failing rather than hanging if the drain wedges.
func stopWithin(t *testing.T, service *host.Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := service.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// stopBounded drains a Host at cleanup, bounded so a wedged drain cannot hang
// the package.
func stopBounded(service *host.Service) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = service.Stop(ctx)
}
