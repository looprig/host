package sessionstoreadapter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/harnesstest"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// permissiveLeaser grants every Acquire a strictly greater epoch and never
// reports a loss, which is how a lease TAKEOVER looks to the store: the
// predecessor's grant is neither released nor refused, it is simply below the
// successor's. memstore refuses a second live holder, so a takeover cannot be
// produced over it.
type permissiveLeaser struct {
	mu     sync.Mutex
	epochs map[string]uint64
}

func (l *permissiveLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.epochs == nil {
		l.epochs = map[string]uint64{}
	}
	l.epochs[name]++
	return permissiveLease{epoch: l.epochs[name], lost: make(chan struct{})}, nil
}

type permissiveLease struct {
	epoch uint64
	lost  chan struct{}
}

func (l permissiveLease) Epoch() uint64                 { return l.epoch }
func (l permissiveLease) Lost() <-chan struct{}         { return l.lost }
func (l permissiveLease) Release(context.Context) error { return nil }

// gateWorld is the released store over a backend whose leaser permits a
// takeover, adapted with a gate journal.
type gateWorld struct {
	released *sessionstore.Store
	adapted  *sessionstoreadapter.Store
}

func newGateWorld(t *testing.T) *gateWorld {
	t.Helper()
	base := memstore.New()
	composite, err := storage.NewCompositeWithOrderedIndex(base.Ledger, &permissiveLeaser{}, base.KV, base.Blobs, base.OrderedIndex)
	if err != nil {
		t.Fatal(err)
	}
	released, err := sessionstore.Open(t.Context(), composite)
	if err != nil {
		t.Fatalf("open the released store: %v", err)
	}
	t.Cleanup(func() { _ = released.Close(context.WithoutCancel(t.Context())) })
	journal := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
	adapted, err := sessionstoreadapter.New(released, namespaceLayout(), sessionstoreadapter.WithGateJournals(journalRouter(t, journal)))
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}
	createBoundSession(t, released, derivedRuntimeID.String())
	return &gateWorld{released: released, adapted: adapted}
}

// session takes a residency grant and binds a gate session to it.
func (w *gateWorld) session(t *testing.T) (gates.Session, residency.Lease) {
	t.Helper()
	lease, err := w.adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	session, err := w.adapted.GateSessionFor(lease, testTenant, testSession)
	if err != nil {
		t.Fatalf("GateSessionFor: %v", err)
	}
	if _, err := session.Scope(t.Context()); err != nil {
		t.Fatalf("Scope: %v", err)
	}
	return session, lease
}

// mark is the projection's residency high-water mark.
func (w *gateWorld) mark(t *testing.T) uint64 {
	t.Helper()
	entry, err := w.released.GetCatalogEntry(t.Context(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry.Record.LeaseEpoch
}

// projectedGate builds the projection of one ask_user gate journaled under the
// binding's runtime session.
func projectedGate(t *testing.T, seed byte, seq uint64) sessionwire.GateProjection {
	t.Helper()
	id := func(b byte) uuid.UUID {
		var u uuid.UUID
		for i := range u {
			u[i] = b
		}
		u[6], u[8] = 0x40|(b&0x0f), 0x80|(b&0x3f)
		return u
	}
	opened := event.GateOpened{
		Header: event.Header{
			Coordinates: identity.Coordinates{SessionID: derivedRuntimeID, LoopID: id(0x62), TurnID: id(0x63), StepID: id(seed + 1)},
			EventID:     id(seed), CreatedAt: time.Now().UTC(),
		},
		Gate: gate.Gate{
			ID: gate.ID(id(seed + 2)), Kind: gate.KindAskUser, Resolver: gate.ResolverLoop,
			Prompt: gate.Prompt{Title: "Question", Body: "answer?", Controls: []gate.Control{{Action: "answer", Label: "Answer"}}},
		},
	}
	projection, err := gates.Project(gates.Scope{TenantID: testTenant, SessionID: testSession, AgentID: testAgent, RuntimeSessionID: derivedRuntimeID}, opened, seq)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	return projection
}

// TestAGateSessionTakesBothIdentitiesFromOneBindingRead: the Core id the
// projection is answered under and the runtime id the events carry come from
// one read of the immutable binding (harness's ReadScope does not check the
// pairing, so this is the only guard).
func TestAGateSessionTakesBothIdentitiesFromOneBindingRead(t *testing.T) {
	w := newGateWorld(t)
	lease, err := w.adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	session, err := w.adapted.GateSessionFor(lease, testTenant, testSession)
	if err != nil {
		t.Fatalf("GateSessionFor: %v", err)
	}
	scope, err := session.Scope(t.Context())
	if err != nil {
		t.Fatalf("Scope: %v", err)
	}
	want := gates.Scope{TenantID: testTenant, SessionID: testSession, AgentID: testAgent, RuntimeSessionID: derivedRuntimeID}
	if scope != want {
		t.Fatalf("Scope = %+v, want %+v", scope, want)
	}
}

// TestGateWritesCarryTheGrantAndASuccessorFencesItsPredecessor is the
// sessionstore v0.12.0 recipe through this adapter: a gate write carries the
// store-issued grant (never a bare epoch) and raises the projection's residency
// mark to it; a successor's first write — even a resolve of a gate the
// projection does not hold — raises it past the predecessor, whose every later
// write is then refused as residency.ErrEpochSuperseded.
func TestGateWritesCarryTheGrantAndASuccessorFencesItsPredecessor(t *testing.T) {
	w := newGateWorld(t)
	predecessor, first := w.session(t)
	opened := projectedGate(t, 0x10, 3)
	if err := predecessor.Open(t.Context(), opened); err != nil {
		t.Fatalf("the predecessor's Open: %v", err)
	}
	if got := w.mark(t); got != uint64(first.Epoch()) {
		t.Fatalf("mark after the predecessor's open = %d, want its grant %d", got, first.Epoch())
	}

	successor, second := w.session(t)
	if err := successor.Resolve(t.Context(), gates.FenceGateID); err != nil {
		t.Fatalf("the successor's fencing write: %v", err)
	}
	if got := w.mark(t); got != uint64(second.Epoch()) || second.Epoch() <= first.Epoch() {
		t.Fatalf("mark after the fencing write = %d, want the successor's grant %d (above %d)", got, second.Epoch(), first.Epoch())
	}
	for name, write := range map[string]func() error{
		"open":    func() error { return predecessor.Open(t.Context(), projectedGate(t, 0x20, 5)) },
		"resolve": func() error { return predecessor.Resolve(t.Context(), opened.GateID) },
	} {
		if err := write(); !errors.Is(err, residency.ErrEpochSuperseded) {
			t.Fatalf("the predecessor's %s after the fence = %v, want ErrEpochSuperseded", name, err)
		}
	}
	projected, err := successor.Projected(t.Context())
	if err != nil {
		t.Fatalf("Projected: %v", err)
	}
	if len(projected) != 1 || projected[0].GateID != opened.GateID {
		t.Fatalf("projection = %+v, want only the gate the predecessor opened before the fence", projected)
	}
}

// TestAGateWhoseIntentIsTombstonedIsUnpublishable: a gate identity whose
// deadline intent was retired can never be projected again — sessionstore's F2
// residue — and the adapter says so with gates.ErrUnpublishable, so the
// publisher stops retrying it.
func TestAGateWhoseIntentIsTombstonedIsUnpublishable(t *testing.T) {
	w := newGateWorld(t)
	session, _ := w.session(t)
	opened := projectedGate(t, 0x10, 3)
	if err := session.Open(t.Context(), opened); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := session.Resolve(t.Context(), opened.GateID); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := session.Open(t.Context(), opened); !errors.Is(err, gates.ErrUnpublishable) {
		t.Fatalf("re-opening a gate whose intent is retired = %v, want ErrUnpublishable", err)
	}
}

// TestGateSessionForRefusesWhatItCannotBind: a lease this store did not issue
// carries no grant, and a store with no gate journals cannot fold anything.
func TestGateSessionForRefusesWhatItCannotBind(t *testing.T) {
	w := newGateWorld(t)
	if session, err := w.adapted.GateSessionFor(foreignLease{}, testTenant, testSession); !errors.Is(err, sessionstoreadapter.ErrForeignLease) || session != nil {
		t.Fatalf("GateSessionFor(a foreign lease) = (%v, %v), want (nil, ErrForeignLease)", session, err)
	}
	released, bare := openStore(t)
	createBoundSession(t, released, derivedRuntimeID.String())
	lease, err := bare.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	if session, err := bare.GateSessionFor(lease, testTenant, testSession); !errors.Is(err, sessionstoreadapter.ErrNoGateJournals) || session != nil {
		t.Fatalf("GateSessionFor with no gate journals = (%v, %v), want (nil, ErrNoGateJournals)", session, err)
	}
}

// foreignLease is a residency.Lease this package did not issue.
type foreignLease struct{}

func (foreignLease) Epoch() residency.ResidencyEpoch { return 1 }
func (foreignLease) Lost() <-chan struct{}           { return nil }
func (foreignLease) Release(context.Context) error   { return nil }

// TestReplayIsTheRuntimesJournalPositionedBySequence: Replay reads the journal
// registered for the binding, under the binding's runtime session, from the
// inclusive sequence it is given — which is what lets the publisher resume a
// fold instead of re-reading a session's whole history on every hint.
func TestReplayIsTheRuntimesJournalPositionedBySequence(t *testing.T) {
	journal := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
	released, adapted := openStore(t, namespaceLayout(), sessionstoreadapter.WithGateJournals(journalRouter(t, journal)))
	createBoundSession(t, released, derivedRuntimeID.String())
	startConversation(t, harnesstest.Launcher(harnesstest.Rig(t, journal, &harnesstest.RecordingLLM{})), derivedRuntimeID)
	lease, err := adapted.AcquireSessionLease(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("AcquireSessionLease: %v", err)
	}
	session, err := adapted.GateSessionFor(lease, testTenant, testSession)
	if err != nil {
		t.Fatalf("GateSessionFor: %v", err)
	}
	if err := session.Replay(t.Context(), 1, func(event.Event, uint64) error { return nil }); err == nil {
		t.Fatal("Replay before Scope succeeded; the runtime identity is read from the binding")
	}
	if _, err := session.Scope(t.Context()); err != nil {
		t.Fatalf("Scope: %v", err)
	}

	var all []uint64
	started := 0
	if err := session.Replay(t.Context(), 1, func(ev event.Event, seq uint64) error {
		if _, ok := ev.(event.SessionStarted); ok {
			started++
		}
		all = append(all, seq)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if started != 1 || len(all) < 2 {
		t.Fatalf("Replay from 1 visited %v with %d SessionStarted, want the conversation", all, started)
	}
	last := all[len(all)-1]
	var tail []uint64
	if err := session.Replay(t.Context(), last, func(_ event.Event, seq uint64) error {
		tail = append(tail, seq)
		return nil
	}); err != nil {
		t.Fatalf("Replay from %d: %v", last, err)
	}
	if len(tail) != 1 || tail[0] != last {
		t.Fatalf("Replay from %d visited %v, want only that sequence", last, tail)
	}
}

// TestLoadGateDecidesADispositionGatesOwnerByResidencyEpoch: on a disposition
// session the projection's residency mark IS the owner the backstop needs — the
// grant of the last Host to write a gate, which v0.12.0 raises on every gate
// write — so an open gate is answered rather than refused. OwnerEpoch is that
// RESIDENCY epoch, never a journal epoch, and OwnerHostID is left empty: the
// store records no Host identity, and the caller compares epochs only.
func TestLoadGateDecidesADispositionGatesOwnerByResidencyEpoch(t *testing.T) {
	w := newGateWorld(t)
	first, firstLease := w.session(t)
	opened := projectedGate(t, 0x10, 3)
	if err := first.Open(t.Context(), opened); err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, held, err := w.adapted.LoadGate(t.Context(), testTenant, testSession, opened.GateID)
	if err != nil || !held {
		t.Fatalf("LoadGate for an open disposition gate = (%+v, %t, %v), want it answered", got, held, err)
	}
	want := commands.Gate{
		GateID:           opened.GateID,
		Open:             true,
		OwnerEpoch:       uint64(firstLease.Epoch()),
		OpenedEventID:    opened.OpenedEventID,
		OpenedJournalSeq: opened.OpenedJournalSeq,
	}
	if got != want {
		t.Fatalf("LoadGate = %+v, want %+v", got, want)
	}

	// A SUCCESSOR'S FENCING WRITE MOVES THE OWNER, and nothing else about the
	// gate: the next read names the successor's residency.
	successor, secondLease := w.session(t)
	if err := successor.Resolve(t.Context(), gates.FenceGateID); err != nil {
		t.Fatalf("fence: %v", err)
	}
	got, held, err = w.adapted.LoadGate(t.Context(), testTenant, testSession, opened.GateID)
	if err != nil || !held || got.OwnerEpoch != uint64(secondLease.Epoch()) {
		t.Fatalf("LoadGate after the successor's fence = (%+v, %t, %v), want owner epoch %d", got, held, err, secondLease.Epoch())
	}

	// And an absent gate is still absent.
	if got, held, err := w.adapted.LoadGate(t.Context(), testTenant, testSession, "gate-never-opened"); err != nil || held || got != (commands.Gate{}) {
		t.Fatalf("LoadGate for an unopened gate = (%+v, %t, %v), want (zero, false, nil)", got, held, err)
	}
}

// TestASeventeenthOpenGateIsProjectionFull: sessionstore holds at most
// MaxCatalogOpenGates open gates per session, and the one that does not fit is
// reported as gates.ErrProjectionFull, which the publisher does not retry on a
// timer (quality gate F7).
func TestASeventeenthOpenGateIsProjectionFull(t *testing.T) {
	w := newGateWorld(t)
	session, _ := w.session(t)
	for index := 0; index < sessionstore.MaxCatalogOpenGates; index++ {
		if err := session.Open(t.Context(), projectedGate(t, byte(0x10+index*4), uint64(3+index))); err != nil {
			t.Fatalf("open gate %d: %v", index, err)
		}
	}
	err := session.Open(t.Context(), projectedGate(t, 0xC0, 99))
	if !errors.Is(err, gates.ErrProjectionFull) {
		t.Fatalf("the %dth open gate = %v, want ErrProjectionFull", sessionstore.MaxCatalogOpenGates+1, err)
	}
	if errors.Is(err, gates.ErrUnpublishable) {
		t.Fatal("a full projection was classified as a permanently unpublishable gate")
	}
}
