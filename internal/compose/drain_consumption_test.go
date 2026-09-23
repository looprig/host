package compose

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/residency"
)

// holdingCheckpointer parks every checkpoint until the test releases it,
// announcing each entry.
type holdingCheckpointer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newHoldingCheckpointer() *holdingCheckpointer {
	return &holdingCheckpointer{entered: make(chan struct{}, 8), release: make(chan struct{})}
}

// Checkpoint announces itself and waits for open.
func (c *holdingCheckpointer) Checkpoint(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) error {
	c.entered <- struct{}{}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// open releases every held checkpoint, once.
func (c *holdingCheckpointer) open() { c.once.Do(func() { close(c.release) }) }

// TestADrainHaltsTheConsumerBeforeItsCheckpoint is the I2.3 finding 1,
// reproduced in the composition: an input admitted while the drain is parked at
// the release checkpoint was APPLIED by the draining Host under its own epoch,
// because the drain stopped the consumer only inside ReleaseResidency. The
// consumer is now halted before the checkpoint, so the command stays pending
// for the successor — including after a warm release's Resume, which must not
// undo a drain's halt.
func TestADrainHaltsTheConsumerBeforeItsCheckpoint(t *testing.T) {
	hold := newHoldingCheckpointer()
	c := newCommandFixtureWith(t, func(o *Options, _ *hostconfig.Options) { o.Checkpointer = hold })
	t.Cleanup(hold.open)
	consumer := c.consumer()

	stopped := make(chan lifecycle.Report, 1)
	go func() {
		report, _ := c.svc.Stop(context.Background())
		stopped <- report
	}()
	select {
	case <-hold.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain never reached the checkpoint")
	}

	during := c.store.accept(t, "command-during-drain", `{"blocks":[{"text":"late"}]}`)
	// Every way a pass can be asked for: a delivery hint, a wake, a warm
	// release's resume, and a direct reconcile.
	consumer.Hint(during)
	c.svc.Wake(keyA)
	c.svc.Resume(keyA)
	if _, err := consumer.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile during the drain: %v", err)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if applied := c.runtime.appliedCommands(); len(applied) != 0 {
			t.Fatalf("a draining Host applied %q after its consumer should have halted", applied[0].CommandID)
		}
		time.Sleep(5 * time.Millisecond)
	}

	hold.open()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return after the checkpoint was released")
	}
	if applied := c.runtime.appliedCommands(); len(applied) != 0 {
		t.Fatalf("the drained Host applied %v", applied)
	}
	if state := c.dispositions.stateOfCommand(during); state != "" && state != commands.StatePending {
		t.Fatalf("the command admitted during the drain is %q, want it left pending for the successor", state)
	}
}

// TestTheGateWaitGaugeCountsASessionParkedAtAGate is the I2.3 finding 2:
// host_sessions_gate_waiting read 0 while a session was parked at a gate,
// because the composition wired no GateWaits source into the metrics although
// its own gate publisher folds exactly that.
func TestTheGateWaitGaugeCountsASessionParkedAtAGate(t *testing.T) {
	f, journal := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)
	if got := f.svc.Metrics().Snapshot().GateWaiting; got != 0 {
		t.Fatalf("GateWaiting = %d with no gate open, want 0", got)
	}

	ask := askOpened(t)
	journal.append(ask, 3)
	fireFiled(t, f, testGateRetry)
	awaitState(t, f, residency.WorkStateGateWaiting)
	if got := f.svc.Metrics().Snapshot().GateWaiting; got != 1 {
		t.Fatalf("GateWaiting = %d with a session parked at a gate, want 1", got)
	}

	journal.append(event.GateResolved{GateID: ask.Gate.ID}, 4)
	fireFiled(t, f, testGateRetry)
	awaitState(t, f, residency.WorkStateIdle)
	if got := f.svc.Metrics().Snapshot().GateWaiting; got != 0 {
		t.Fatalf("GateWaiting = %d after the gate resolved, want 0", got)
	}
}

// refusingReleaseSession is a runtime that refuses its nonterminal release, as
// harness does for a session parked at a gate.
type refusingReleaseSession struct {
	*controllableSession
}

// ReleaseResidency refuses, as harness does for a session that is not idle.
func (refusingReleaseSession) ReleaseResidency(context.Context) error {
	return errors.New("the runtime is not whole-session idle")
}

// TestAForcedReleaseRecordsAndLogsTheResidencyEpoch is the I2.3 spec-case-4
// gap: a forced (crash-equivalent) release recorded the session, the failed
// step and the Host generation, but no epoch — and it journals no
// SessionResidencyReleased, so the epoch appeared nowhere. The drain report
// and the log now name the residency epoch the session was held under.
func TestAForcedReleaseRecordsAndLogsTheResidencyEpoch(t *testing.T) {
	handler := newCapturingHandler()
	f := newFixture(t, withLogger(handler))
	f.rig.Session = refusingReleaseSession{controllableSession: f.runtime}
	f.start()
	f.attach(tenantA, sessionA)
	held := f.svc.residentFor(keyA)
	if held == nil {
		t.Fatal("the attached session is not resident")
	}
	epoch := uint64(held.lease.Epoch())
	if epoch == 0 {
		t.Fatal("the fixture's lease reports a zero epoch, so this test proves nothing")
	}

	report, err := f.svc.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	var forced *lifecycle.Failure
	for i := range report.Failures {
		if report.Failures[i].Step == lifecycle.StepReleaseResidency {
			forced = &report.Failures[i]
		}
	}
	if forced == nil {
		t.Fatalf("failures = %v, want the refused release", report.Failures)
	}
	if forced.ResidencyEpoch != epoch {
		t.Fatalf("the forced release records residency epoch %d, want %d", forced.ResidencyEpoch, epoch)
	}

	want := map[string]string{
		"tenant_id":       string(tenantA),
		"session_id":      string(sessionA),
		"step":            string(lifecycle.StepReleaseResidency),
		"residency_epoch": strconv.FormatUint(epoch, 10),
		"host_generation": strconv.FormatUint(testGen, 10),
	}
	for _, record := range handler.snapshot() {
		if record.message != "host: drain step failed" || record.attributes["step"] != want["step"] {
			continue
		}
		if record.level != slog.LevelError {
			t.Errorf("a forced release was logged at %v, want ERROR", record.level)
		}
		for key, value := range want {
			if record.attributes[key] != value {
				t.Errorf("the forced-release log has %s=%q, want %q", key, record.attributes[key], value)
			}
		}
		return
	}
	t.Fatalf("no drain failure was logged for the forced release: %+v", handler.snapshot())
}

// holdingApplySession is a publishing runtime whose FIRST dispatch blocks until
// released, so a test can hold a consumer pass in flight while a drain waits
// for it. Later dispatches pass straight through, so a consumer wrongly resumed
// shows up as a second application rather than a hang.
type holdingApplySession struct {
	*publishingSession
	entered chan struct{}
	release chan struct{}
	first   sync.Once
}

// ApplyCommand blocks the first dispatch until release closes.
func (s *holdingApplySession) ApplyCommand(ctx context.Context, command department.RuntimeCommand) error {
	s.first.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.publishingSession.ApplyCommand(ctx, command)
}

// TestAResumeWhileTheDrainWaitsOutAPassCannotUndoTheHalt is gate F2: the drain
// latches its halt BEFORE it waits for the pass in flight, under the lock
// Service.Resume checks it with. A warm release aborting while the drain waits
// is exactly the window a latch set after the wait would leave open: its
// Resume would clear the consumer's halt, and the consumer would go on
// applying commands mid-drain.
func TestAResumeWhileTheDrainWaitsOutAPassCannotUndoTheHalt(t *testing.T) {
	hold := newHoldingCheckpointer()
	t.Cleanup(hold.open)
	store := newDurableCommands(keyA)
	inner := newPublishingSession()
	inner.effects = store
	runtime := &holdingApplySession{publishingSession: inner, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-runtime.release:
		default:
			close(runtime.release)
		}
	})
	f := newFixture(t, func(o *Options, _ *hostconfig.Options) {
		o.Inbox = store
		o.Cursors = store
		dispositions := newFakeDispositions(o.Clock.(*fakeClock), store)
		dispositions.payloads = store
		o.Records = dispositions
		o.Writers = &fakeDispositionWriters{store: dispositions}
		o.Checkpointer = hold
		inner.disposition = func(command sessionwire.CommandID) { dispositions.setEvidence(command, "applied") }
	})
	f.rig.Session = runtime
	f.start()
	f.attach(tenantA, sessionA)

	store.accept(t, "command-in-flight", `{"blocks":[{"text":"first"}]}`)
	f.svc.Wake(keyA)
	select {
	case <-runtime.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the consumer never dispatched the first command")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_, _ = f.svc.Stop(context.Background())
	}()
	// The drain publishes nonaccepting before it touches a session, then halts
	// the consumer, which waits for the pass held above.
	awaitCondition(t, "the drain to publish nonaccepting", func() bool { return len(f.directory.withdrawals()) > 0 })
	time.Sleep(100 * time.Millisecond)

	f.svc.Resume(keyA)
	close(runtime.release)
	select {
	case <-hold.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain never reached the checkpoint")
	}

	late := store.accept(t, "command-after-resume", `{"blocks":[{"text":"late"}]}`)
	f.svc.Wake(keyA)
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, applied := range inner.appliedCommands() {
			if applied.CommandID == late {
				t.Fatal("a Resume during the drain's halt restarted consumption: the draining Host applied a later command")
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	hold.open()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return")
	}
}
