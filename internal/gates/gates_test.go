package gates

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	coresessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"

	"github.com/looprig/host/internal/residency"
)

const (
	gatesTenant  coresessionwire.TenantID  = "tenant-gates"
	gatesSession coresessionwire.SessionID = "core/session/opaque"
	gatesAgent   coresessionwire.AgentID   = "agent-gates"
)

var (
	gatesRuntime = uuid.MustParse("0f4d2a6c-81b3-8e57-9c20-6a1e7d3b5f48")
	openedAt     = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
)

func seedUUID(seed byte) uuid.UUID {
	var id uuid.UUID
	for i := range id {
		id[i] = seed
	}
	id[6] = 0x40 | (seed & 0x0f)
	id[8] = 0x80 | (seed & 0x3f)
	return id
}

// opened builds a journaled GateOpened of one kind, with an optional timeout.
func opened(seed byte, kind gate.Kind, timeout time.Duration) event.GateOpened {
	g := gate.Gate{
		ID: gate.ID(seedUUID(seed + 2)), Kind: kind, Resolver: gate.ResolverLoop,
		Prompt: gate.Prompt{Title: "Question", Body: "answer?", Controls: []gate.Control{{Action: "answer", Label: "Answer"}}},
	}
	if kind == gate.KindPermission {
		g.Prompt.Controls = gate.ApprovalControls()
	}
	g.ResponsePolicy.Timeout = timeout
	return event.GateOpened{
		Header: event.Header{
			Coordinates: identity.Coordinates{SessionID: gatesRuntime, LoopID: seedUUID(0x62), TurnID: seedUUID(0x63), StepID: seedUUID(seed + 1)},
			EventID:     seedUUID(seed), CreatedAt: openedAt,
		},
		Gate: g,
	}
}

// resolved builds the GateResolved closing a gate.
func resolved(seed byte, id gate.ID) event.GateResolved {
	return event.GateResolved{
		Header: event.Header{
			Coordinates: identity.Coordinates{SessionID: gatesRuntime, LoopID: seedUUID(0x62), TurnID: seedUUID(0x63), StepID: seedUUID(seed + 1)},
			EventID:     seedUUID(seed), CreatedAt: openedAt.Add(time.Minute),
		},
		GateID: id, Resolver: gate.ResolverLoop, Reason: gate.CloseAnswered, Action: "answer",
	}
}

// journaled is one event at a ledger sequence.
type journaled struct {
	ev  event.Event
	seq uint64
}

// fakeSession is the durable gate surface over an in-memory projection and
// journal, recording every call in order.
type fakeSession struct {
	mu        sync.Mutex
	calls     []string
	journal   []journaled
	projected []coresessionwire.GateProjection
	opened    []coresessionwire.GateProjection
	froms     []uint64

	openErr      map[coresessionwire.GateID]error
	projectedErr error
	resolveErr   error
}

func (s *fakeSession) record(call string) {
	s.calls = append(s.calls, call)
}

func (s *fakeSession) Scope(context.Context) (Scope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("scope")
	return Scope{TenantID: gatesTenant, SessionID: gatesSession, AgentID: gatesAgent, RuntimeSessionID: gatesRuntime}, nil
}

func (s *fakeSession) Projected(context.Context) ([]coresessionwire.GateProjection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("projected")
	if err := s.projectedErr; err != nil {
		s.projectedErr = nil
		return nil, err
	}
	return append([]coresessionwire.GateProjection(nil), s.projected...), nil
}

func (s *fakeSession) Open(_ context.Context, projection coresessionwire.GateProjection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("open:" + string(projection.GateID))
	if err := s.openErr[projection.GateID]; err != nil {
		return err
	}
	s.opened = append(s.opened, projection)
	s.projected = append(s.projected, projection)
	return nil
}

func (s *fakeSession) Resolve(_ context.Context, id coresessionwire.GateID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("resolve:" + string(id))
	if s.resolveErr != nil {
		return s.resolveErr
	}
	s.projected = slices.DeleteFunc(s.projected, func(p coresessionwire.GateProjection) bool { return p.GateID == id })
	return nil
}

func (s *fakeSession) Replay(_ context.Context, from uint64, visit func(event.Event, uint64) error) error {
	s.mu.Lock()
	s.record("replay")
	s.froms = append(s.froms, from)
	entries := append([]journaled(nil), s.journal...)
	s.mu.Unlock()
	for _, entry := range entries {
		if entry.seq < from {
			continue
		}
		if err := visit(entry.ev, entry.seq); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeSession) append(ev event.Event, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.journal = append(s.journal, journaled{ev: ev, seq: seq})
}

func (s *fakeSession) snapshot() (calls []string, projected []coresessionwire.GateProjection, froms []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...), append([]coresessionwire.GateProjection(nil), s.projected...), append([]uint64(nil), s.froms...)
}

// fakeHints is the runtime's committed stream, driven by the test.
type fakeHints struct {
	mu            sync.Mutex
	subscriptions []chan coresessionwire.EnduringPublication
}

func (h *fakeHints) SubscribeCommitted(context.Context, coresessionwire.EventID) (<-chan coresessionwire.EnduringPublication, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	channel := make(chan coresessionwire.EnduringPublication, 8)
	h.subscriptions = append(h.subscriptions, channel)
	return channel, nil
}

func (h *fakeHints) current() chan coresessionwire.EnduringPublication {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.subscriptions[len(h.subscriptions)-1]
}

func (h *fakeHints) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subscriptions)
}

// hint delivers one hint on the current subscription.
func (h *fakeHints) hint() {
	h.current() <- coresessionwire.EnduringPublication{}
}

// harness is one running publisher under test.
type harness struct {
	session   *fakeSession
	hints     *fakeHints
	publisher *Publisher
	converged chan struct{}
}

func start(t *testing.T, session *fakeSession) *harness {
	t.Helper()
	h := &harness{session: session, hints: &fakeHints{}, converged: make(chan struct{}, 64)}
	publisher, err := Start(t.Context(), Options{
		Session:     session,
		Hints:       h.hints,
		After:       time.After,
		Retry:       5 * time.Millisecond,
		OnConverged: func() { h.converged <- struct{}{} },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	h.publisher = publisher
	t.Cleanup(publisher.Stop)
	return h
}

// awaitConverged waits for the next converged pass.
func (h *harness) awaitConverged(t *testing.T) {
	t.Helper()
	select {
	case <-h.converged:
	case <-time.After(5 * time.Second):
		calls, _, _ := h.session.snapshot()
		t.Fatalf("the publisher did not converge within 5s; calls = %v", calls)
	}
}

// eventually polls cond for up to five seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func gateIDOf(ev event.GateOpened) coresessionwire.GateID {
	return coresessionwire.GateID(ev.Gate.ID.String())
}

// TestThePublisherFencesBeforeItWritesAnyGate: the first write is the fencing
// resolve, before the fold and before any open. It is what raises the residency
// mark so a predecessor's late write is refused (sessionstore F1), and what
// narrows the booked F2 window ("one fencing write before opening any gate").
func TestThePublisherFencesBeforeItWritesAnyGate(t *testing.T) {
	session := &fakeSession{}
	ask := opened(0x10, gate.KindAskUser, 0)
	session.append(ask, 4)
	h := start(t, session)
	h.awaitConverged(t)

	calls, _, _ := session.snapshot()
	want := []string{"scope", "resolve:" + string(FenceGateID), "replay", "projected", "open:" + string(gateIDOf(ask))}
	if len(calls) < len(want) || !slices.Equal(calls[:len(want)], want) {
		t.Fatalf("calls = %v, want them to begin %v", calls, want)
	}
	if h.hints.count() != 1 {
		t.Fatalf("%d subscriptions, want the one taken before the fold", h.hints.count())
	}
}

// TestAnOpenGateIsProjectedFromItsJournaledEvent: every member of the
// projection is a function of the GateOpened and its sequence — the deadline is
// the durable creation time plus the gate's own timeout, or plus the kind's
// compiled TTL — so two Hosts folding one journal project one gate (R1).
func TestAnOpenGateIsProjectedFromItsJournaledEvent(t *testing.T) {
	for _, row := range []struct {
		name     string
		kind     gate.Kind
		timeout  time.Duration
		deadline time.Time
	}{
		{"a timeout on the gate", gate.KindAskUser, 90 * time.Second, openedAt.Add(90 * time.Second)},
		{"ask_user with none: a day", gate.KindAskUser, 0, openedAt.Add(24 * time.Hour)},
		{"permission with none: five minutes", gate.KindPermission, 0, openedAt.Add(5 * time.Minute)},
	} {
		t.Run(row.name, func(t *testing.T) {
			session := &fakeSession{}
			ev := opened(0x10, row.kind, row.timeout)
			session.append(ev, 7)
			h := start(t, session)
			h.awaitConverged(t)

			_, projected, _ := session.snapshot()
			if len(projected) != 1 {
				t.Fatalf("%d gates projected, want 1", len(projected))
			}
			got := projected[0]
			if got.GateID != gateIDOf(ev) || got.OpenedJournalSeq != 7 || got.OpenedEventID != coresessionwire.EventID(ev.EventID.String()) {
				t.Errorf("projected identity = (%q, %d, %q), want the event's", got.GateID, got.OpenedJournalSeq, got.OpenedEventID)
			}
			if !got.Deadline.Equal(row.deadline) {
				t.Errorf("deadline = %v, want %v", got.Deadline, row.deadline)
			}
			if got.Answerability != coresessionwire.GateAnswerabilityResident {
				t.Errorf("answerability = %q, want resident", got.Answerability)
			}
			if got.Kind != string(row.kind) {
				t.Errorf("kind = %q, want %q", got.Kind, row.kind)
			}
		})
	}
}

// TestTheProjectionConvergesOnTheFold: a gate the journal resolved is not
// opened; a projected gate the journal has closed (an ask_user gate closed at
// restore, a gate answered while no Host held the session) is resolved; a gate
// both hold open is left exactly as projected, with no re-open.
func TestTheProjectionConvergesOnTheFold(t *testing.T) {
	session := &fakeSession{}
	stillOpen := opened(0x10, gate.KindPermission, 0)
	closed := opened(0x20, gate.KindAskUser, 0)
	closedAtRestore := opened(0x30, gate.KindAskUser, 0)
	session.append(stillOpen, 3)
	session.append(closed, 5)
	session.append(resolved(0x50, closed.Gate.ID), 6)
	session.append(closedAtRestore, 8)
	session.append(resolved(0x60, closedAtRestore.Gate.ID), 9)
	predecessorDeadline := openedAt.Add(time.Hour)
	session.projected = []coresessionwire.GateProjection{
		{GateID: gateIDOf(stillOpen), OpenedJournalSeq: 3, Deadline: predecessorDeadline},
		{GateID: gateIDOf(closedAtRestore), OpenedJournalSeq: 8},
	}
	h := start(t, session)
	h.awaitConverged(t)

	calls, projected, _ := session.snapshot()
	for _, call := range calls {
		if call == "open:"+string(gateIDOf(stillOpen)) || call == "open:"+string(gateIDOf(closed)) {
			t.Errorf("the publisher made %s; a projected gate is left alone and a resolved one never opened", call)
		}
	}
	if !slices.Contains(calls, "resolve:"+string(gateIDOf(closedAtRestore))) {
		t.Errorf("the gate the journal closed was not resolved; calls = %v", calls)
	}
	if len(projected) != 1 || projected[0].GateID != gateIDOf(stillOpen) || !projected[0].Deadline.Equal(predecessorDeadline) {
		t.Fatalf("projection = %+v, want only the gate still open, as its predecessor projected it", projected)
	}
}

// TestAHintFoldsOnlyWhatWasAppendedSince: after the first pass the fold is
// positioned past the last sequence it read, and a hint opens a newly journaled
// gate and resolves one the runtime closed.
func TestAHintFoldsOnlyWhatWasAppendedSince(t *testing.T) {
	session := &fakeSession{}
	first := opened(0x10, gate.KindAskUser, 0)
	session.append(first, 4)
	h := start(t, session)
	h.awaitConverged(t)

	second := opened(0x20, gate.KindAskUser, 0)
	session.append(resolved(0x30, first.Gate.ID), 6)
	session.append(second, 9)
	h.hints.hint()
	h.awaitConverged(t)

	_, projected, froms := session.snapshot()
	if len(projected) != 1 || projected[0].GateID != gateIDOf(second) {
		t.Fatalf("projection = %+v, want only the second gate", projected)
	}
	if len(froms) < 2 || froms[0] != 1 || froms[1] != 5 {
		t.Fatalf("replays started at %v, want 1 and then 5 (the sequence after the last one read)", froms)
	}
}

// TestALostSubscriptionIsAGapInHintsAndNotInState: the committed stream ends
// (the pump's overflow does exactly this), the runtime journals a gate while
// nothing is subscribed, and the publisher resubscribes and projects it.
func TestALostSubscriptionIsAGapInHintsAndNotInState(t *testing.T) {
	session := &fakeSession{}
	h := start(t, session)
	h.awaitConverged(t)

	missed := opened(0x10, gate.KindAskUser, 0)
	session.append(missed, 3)
	close(h.hints.current())
	eventually(t, "a resubscription", func() bool { return h.hints.count() == 2 })
	h.awaitConverged(t)

	_, projected, _ := session.snapshot()
	if len(projected) != 1 || projected[0].GateID != gateIDOf(missed) {
		t.Fatalf("projection = %+v, want the gate journaled while unsubscribed", projected)
	}
}

// TestASupersededGrantStopsThePublisherForGood: the store refusing a write for
// a mark above this Host's grant means a successor holds the session; the
// publisher stops and writes nothing more.
func TestASupersededGrantStopsThePublisherForGood(t *testing.T) {
	session := &fakeSession{}
	ask := opened(0x10, gate.KindAskUser, 0)
	session.append(ask, 3)
	session.openErr = map[coresessionwire.GateID]error{gateIDOf(ask): errors.Join(residency.ErrEpochSuperseded, errors.New("store: epoch"))}
	h := start(t, session)

	eventually(t, "the publisher to report it was superseded", h.publisher.Superseded)
	before, _, _ := session.snapshot()
	session.append(opened(0x20, gate.KindAskUser, 0), 5)
	select {
	case h.hints.current() <- coresessionwire.EnduringPublication{}:
	default:
	}
	time.Sleep(30 * time.Millisecond)
	after, _, _ := session.snapshot()
	if len(after) != len(before) {
		t.Fatalf("a superseded publisher went on calling the store: %v", after[len(before):])
	}
}

// TestAnUnpublishableGateIsNotRetriedAndDoesNotBlockTheOthers: sessionstore's
// F2 residue makes one gate identity unprojectable forever. The publisher
// records it once and still projects every other gate.
func TestAnUnpublishableGateIsNotRetriedAndDoesNotBlockTheOthers(t *testing.T) {
	session := &fakeSession{}
	poisoned := opened(0x10, gate.KindAskUser, 0)
	healthy := opened(0x20, gate.KindAskUser, 0)
	session.append(poisoned, 3)
	session.append(healthy, 5)
	session.openErr = map[coresessionwire.GateID]error{gateIDOf(poisoned): errors.Join(ErrUnpublishable, errors.New("store: deleted gate_intent"))}
	h := start(t, session)
	h.awaitConverged(t)

	session.append(opened(0x30, gate.KindAskUser, 0), 7)
	h.hints.hint()
	h.awaitConverged(t)

	calls, projected, _ := session.snapshot()
	attempts := 0
	for _, call := range calls {
		if call == "open:"+string(gateIDOf(poisoned)) {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatalf("the unpublishable gate was opened %d times, want exactly 1", attempts)
	}
	if len(projected) != 2 {
		t.Fatalf("projection = %+v, want the two healthy gates", projected)
	}
}

// TestAFailedPassIsRetried: a transient read failure is retried after Retry
// and converges without a hint.
func TestAFailedPassIsRetried(t *testing.T) {
	session := &fakeSession{projectedErr: errors.New("injected: the catalog read failed")}
	ask := opened(0x10, gate.KindAskUser, 0)
	session.append(ask, 3)
	h := start(t, session)
	h.awaitConverged(t)

	_, projected, _ := session.snapshot()
	if len(projected) != 1 {
		t.Fatalf("projection = %+v after the retry, want the gate", projected)
	}
}

// TestStartRefusesWhatItCannotRun holds every required option.
func TestStartRefusesWhatItCannotRun(t *testing.T) {
	valid := Options{Session: &fakeSession{}, Hints: &fakeHints{}, After: time.After, Retry: time.Second}
	for field, change := range map[string]func(*Options){
		"Session": func(o *Options) { o.Session = nil },
		"Hints":   func(o *Options) { o.Hints = nil },
		"After":   func(o *Options) { o.After = nil },
		"Retry":   func(o *Options) { o.Retry = 0 },
	} {
		options := valid
		change(&options)
		_, err := Start(t.Context(), options)
		var invalid *InvalidOptionsError
		if !errors.As(err, &invalid) || invalid.Field != field {
			t.Errorf("Start without %s = %v, want InvalidOptionsError naming it", field, err)
		}
	}
}
