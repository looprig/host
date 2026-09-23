package host_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/harnesstest"
)

// errJournalOutage is the injected transient storage failure.
var errJournalOutage = errors.New("injected: the journal store is briefly unreachable")

// outageLedger refuses every append while an outage is on, and serves normally
// otherwise: a PostgreSQL or S3 blip under the runtime's journal, not a lost
// database. Reads are left alone so the fault is exactly one failed write path.
type outageLedger struct {
	storage.Ledger
	down atomic.Bool
	// grace is how many appends the outage still lets through once it is on,
	// which places the first failed write at a chosen point in a command.
	grace atomic.Int32
}

func (l *outageLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if l.down.Load() && l.grace.Add(-1) < 0 {
		return errJournalOutage
	}
	return l.Ledger.Append(ctx, name, expected, payload)
}

// begin starts an outage that lets grace more appends through first.
func (l *outageLedger) begin(grace int32) {
	l.grace.Store(grace)
	l.down.Store(true)
}

// withJournalOutage re-opens the world's harness journal over a ledger the test
// can take down, before any Host exists, and returns the switch.
func (w *realRuntimeWorld) withJournalOutage(t *testing.T) *outageLedger {
	t.Helper()
	backend := w.fixture.journalBackend
	ledger := &outageLedger{Ledger: backend.Ledger}
	composite, err := storage.NewCompositeWithOrderedIndex(ledger, backend.Leaser, backend.KV, backend.Blobs, backend.OrderedIndex)
	if err != nil {
		t.Fatal(err)
	}
	store := harnesstest.Store(t, composite, composeTenant)
	w.fixture.journal = store
	w.journal = store
	return ledger
}

// commandState reads one admitted command's durable state.
func (w *realRuntimeWorld) commandState(t *testing.T, id sessionwire.CommandID) sessionstore.InboxState {
	t.Helper()
	entry, err := w.factory.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{TenantID: composeTenant, SessionID: composeSession, CommandID: id})
	if err != nil {
		t.Fatalf("GetDispositionCommand(%s): %v", id, err)
	}
	return entry.Record.State
}

// awaitState waits until a command reaches one of the given states.
func (w *realRuntimeWorld) awaitState(t *testing.T, id sessionwire.CommandID, bound time.Duration, states ...sessionstore.InboxState) sessionstore.InboxState {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		state := w.commandState(t, id)
		for _, want := range states {
			if state == want {
				return state
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("command %s is %q after %v, want one of %v", id, state, bound, states)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitJournalQuiet waits until the runtime's journal ends in SessionIdle and
// has stopped growing.
func (w *realRuntimeWorld) awaitJournalQuiet(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		events := harnesstest.Events(t, w.journal, w.runtimeID)
		if len(events) == last && len(events) > 0 {
			if _, idle := events[len(events)-1].(event.SessionIdle); idle {
				return
			}
		}
		last = len(events)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the runtime's journal never went quiet at SessionIdle")
}

// turnsCarrying counts the journal's TurnStarted records whose user message is
// exactly text: how many times an input was applied.
func (w *realRuntimeWorld) turnsCarrying(t *testing.T, text string) int {
	t.Helper()
	count := 0
	for _, ev := range harnesstest.Events(t, w.journal, w.runtimeID) {
		started, ok := ev.(event.TurnStarted)
		if !ok || started.Message == nil {
			continue
		}
		for _, block := range started.Message.Blocks {
			if tb, ok := block.(*content.TextBlock); ok && tb.Text == text {
				count++
			}
		}
	}
	return count
}

// TestATransientJournalOutageIsRecoveredByRestore is D3 end to end over a real
// harness runtime: the runtime's journal refuses writes for a moment while a
// command is being applied. Before the fix the command sat `applying` for good
// under the runtime's own grant — nothing may close an attempt its own runtime
// authored — and every later command queued behind it.
//
// Now the Host gives the runtime up, the residency comes free, and a successor
// attach (Factory's pending sweep does this in production) RESTORES the session
// from the journal: the stranded command settles truthfully — applied if its
// effect is durable, not_applied if not, never both — and every command after it
// is applied exactly once.
func TestATransientJournalOutageIsRecoveredByRestore(t *testing.T) {
	// The runtime's appends for one input run: the audit intent record, the
	// application prefix, the `applied` disposition, then the turn's events.
	for _, row := range []struct {
		name  string
		grace int32
	}{
		// The prefix itself fails. The runtime refuses the command WITHOUT
		// faulting, so only the stranded-attempt report can move it: the P3.1
		// PostgreSQL shape.
		{name: "the application prefix fails", grace: 1},
		// The command is durably applied and the TURN's first event fails, which
		// latches the runtime's persistence fault: the P3.1 S3 shape. The input
		// is a durable debt the restore re-runs.
		{name: "the turn fails after the disposition", grace: 3},
	} {
		t.Run(row.name, func(t *testing.T) {
			runTransientJournalOutage(t, row.grace)
		})
	}
}

func runTransientJournalOutage(t *testing.T, grace int32) {
	world := newRealRuntimeWorld(t)
	outage := world.withJournalOutage(t)
	// The consumer reconciles often, standing in for Factory's wake on admission.
	service, _ := world.hostWith(t, 4, func(blueprint *host.Composition) {
		blueprint.Options.ReconcileInterval = 50 * time.Millisecond
	})
	t.Cleanup(func() { stopBounded(service) })

	const before, during, after = "before the outage", "during the outage", "after the outage"
	first := world.admitInput(t, before)
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	world.awaitApplied(t, first, before)
	// `applied` is committed BEFORE the turn runs, so the first turn's own
	// appends can still be arriving. Let the journal settle, so the outage meets
	// the NEXT command and not the tail of this one.
	world.awaitJournalQuiet(t)

	outage.begin(grace)
	stranded := world.admitInput(t, during)
	world.awaitState(t, stranded, 15*time.Second, sessionstore.InboxStateApplying, sessionstore.InboxStateApplied, sessionstore.InboxStateRejected)
	time.Sleep(200 * time.Millisecond) // the runtime meets the outage
	outage.down.Store(false)

	if !world.awaitReleased(t, 15*time.Second) {
		t.Fatalf("the Host kept the session resident after its runtime could make no progress; command %q is %q", during, world.commandState(t, stranded))
	}
	// The successor: Factory places a session with open work and no owner.
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("the successor attach (restore) failed: %v", err)
	}
	settled := world.awaitState(t, stranded, 15*time.Second, sessionstore.InboxStateApplied, sessionstore.InboxStateRejected)
	last := world.admitInput(t, after)
	world.awaitApplied(t, last, after)
	world.awaitJournalQuiet(t) // `applied` precedes the turn it applies
	t.Logf("the command in flight at the outage settled %q", settled)

	for text, want := range map[string]int{before: 1, after: 1} {
		if got := world.turnsCarrying(t, text); got != want {
			t.Errorf("input %q was applied %d times, want %d", text, got, want)
		}
	}
	wantDuring := 0
	if settled == sessionstore.InboxStateApplied {
		wantDuring = 1
	}
	if got := world.turnsCarrying(t, during); got != wantDuring {
		t.Errorf("the stranded input settled %q but its turn ran %d times, want %d", settled, got, wantDuring)
	}
	if got := countOf[event.SessionStopped](t, world.journal, world.runtimeID); got != 0 {
		t.Errorf("the recovery journaled %d SessionStopped; the session must stay restorable", got)
	}
}
