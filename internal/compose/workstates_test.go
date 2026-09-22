package compose

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

	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// The derived work-state source
// ---------------------------------------------------------------------------

// testGateRetry is the gate publisher's retry bound in these tests. It differs
// from WorkPoll because the fake clock files every After under its duration,
// and the two loops must be fired separately.
const testGateRetry = 7 * time.Second

// journalGateSessions is a GateSessions over one in-memory runtime journal the
// test appends to, so the publisher's fold sees what the test put there.
type journalGateSessions struct {
	mu        sync.Mutex
	journal   []journalEntry
	replayErr error
}

type journalEntry struct {
	ev  event.Event
	seq uint64
}

func (g *journalGateSessions) append(ev event.Event, seq uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.journal = append(g.journal, journalEntry{ev: ev, seq: seq})
}

func (g *journalGateSessions) GateSessionFor(residency.Lease, sessionwire.TenantID, sessionwire.SessionID) (gates.Session, error) {
	return journalGateSession{sessions: g}, nil
}

type journalGateSession struct {
	sessions *journalGateSessions
}

func (journalGateSession) Scope(context.Context) (gates.Scope, error) {
	return gates.Scope{TenantID: tenantA, SessionID: sessionA, AgentID: testAgent, RuntimeSessionID: testRigSessionID}, nil
}

func (journalGateSession) Projected(context.Context) ([]sessionwire.GateProjection, error) {
	return nil, nil
}

func (journalGateSession) Open(context.Context, sessionwire.GateProjection) error { return nil }

func (journalGateSession) Resolve(context.Context, sessionwire.GateID) error { return nil }

func (s journalGateSession) Replay(_ context.Context, from uint64, visit func(event.Event, uint64) error) error {
	s.sessions.mu.Lock()
	if err := s.sessions.replayErr; err != nil {
		s.sessions.mu.Unlock()
		return err
	}
	journal := append([]journalEntry(nil), s.sessions.journal...)
	s.sessions.mu.Unlock()
	for _, entry := range journal {
		if entry.seq < from {
			continue
		}
		if err := visit(entry.ev, entry.seq); err != nil {
			return err
		}
	}
	return nil
}

// derivedFixture composes a Host that derives its work states over a journal
// the test controls.
func derivedFixture(t *testing.T, adjust ...func(*Options, *hostconfig.Options)) (*fixture, *journalGateSessions) {
	t.Helper()
	journal := &journalGateSessions{}
	f := newFixture(t, append([]func(*Options, *hostconfig.Options){func(options *Options, _ *hostconfig.Options) {
		options.WorkStates = nil
		options.DeriveWorkStates = true
		options.Gates = journal
		options.GateRetry = testGateRetry
	}}, adjust...)...)
	return f, journal
}

// askOpened is a journaled ask_user gate.
func askOpened(t *testing.T) event.GateOpened {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	return event.GateOpened{Gate: gate.Gate{ID: gate.ID(id), Kind: gate.KindAskUser}}
}

// fireFiled fires every channel filed under d once at least one is. Unlike
// fireWhenWaiting it does not depend on seeing the registration event, which a
// wait for a different bound may already have consumed.
func fireFiled(t *testing.T, f *fixture, d time.Duration) {
	t.Helper()
	f.clock.awaitWaiters(t, d, 1)
	f.clock.fire(d)
}

// awaitState polls the composed source until it answers want.
func awaitState(t *testing.T, f *fixture, want residency.WorkState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, known := f.svc.options.WorkStates.WorkState(keyA)
		if known && state == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("WorkState = (%q, %v), want (%q, true)", state, known, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestADerivedSourceIsIdleOnlyWhenTheRuntimeIsIdleAndNoGateIsOpen is the
// source's truth table over a real publisher fold and a controllable runtime.
func TestADerivedSourceIsIdleOnlyWhenTheRuntimeIsIdleAndNoGateIsOpen(t *testing.T) {
	f, journal := derivedFixture(t)
	f.start()

	if state, known := f.svc.options.WorkStates.WorkState(keyA); known {
		t.Fatalf("a session this Host does not hold reported (%q, true)", state)
	}
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)

	// The runtime is busy: working, whatever the gates say.
	f.runtime.HoldIdle()
	awaitState(t, f, residency.WorkStateWorking)
	f.runtime.GoIdle()
	awaitState(t, f, residency.WorkStateIdle)

	// A gate opens in the journal while the runtime is IDLE — a gate restored
	// open under an interrupted turn looks exactly like this. It is a gate wait.
	ask := askOpened(t)
	journal.append(ask, 3)
	fireFiled(t, f, testGateRetry)
	awaitState(t, f, residency.WorkStateGateWaiting)

	// It closes, and the session is idle again.
	journal.append(event.GateResolved{GateID: ask.Gate.ID}, 4)
	fireFiled(t, f, testGateRetry)
	awaitState(t, f, residency.WorkStateIdle)
}

// TestADerivedSourceThatHasNotFoldedIsNeverIdle: until the gate publisher has
// folded the journal once this Host cannot say whether a gate is open, and an
// idle runtime is not enough.
func TestADerivedSourceThatHasNotFoldedIsNeverIdle(t *testing.T) {
	f, journal := derivedFixture(t)
	journal.replayErr = errors.New("injected: the journal cannot be read")
	f.start()
	f.attach(tenantA, sessionA)
	fireFiled(t, f, testGateRetry)
	if state, known := f.svc.options.WorkStates.WorkState(keyA); !known || state != residency.WorkStateWorking {
		t.Fatalf("an unfolded session reported (%q, %v), want (working, true)", state, known)
	}
}

// TestTheDerivedSourceArmsTheWarmCountdownThroughTheSampler is the wiring: the
// composition's own sampler reads the derived source and arms the Host's TTL.
func TestTheDerivedSourceArmsTheWarmCountdownThroughTheSampler(t *testing.T) {
	f, _ := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)
	fireFiled(t, f, f.svc.options.WorkPoll)
	if armed := f.awaitRearming(t); armed != defaultFixtureWarmTTL {
		t.Fatalf("the warm countdown was armed for %v, want %v", armed, defaultFixtureWarmTTL)
	}
}

// TestAGateOpenUnderAnIdleRuntimeNeverArmsTheCountdown: the sampler is fed the
// gate wait, not the runtime's idle.
func TestAGateOpenUnderAnIdleRuntimeNeverArmsTheCountdown(t *testing.T) {
	f, journal := derivedFixture(t)
	journal.append(askOpened(t), 2)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateGateWaiting)
	fireFiled(t, f, f.svc.options.WorkPoll)
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.clock.awaitWaiters(t, f.svc.options.WorkPoll, 1)
	if got := f.clock.rearmings(); len(got) != 0 {
		t.Fatalf("a session with an open gate armed a warm countdown for %v", got)
	}
}

// TestWorkBetweenTwoIdleSamplesRestartsTheCountdown: the journal moved between
// two samples that both found the runtime idle, so the runtime worked in
// between and the countdown restarts from a whole TTL.
func TestWorkBetweenTwoIdleSamplesRestartsTheCountdown(t *testing.T) {
	f, journal := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)

	fireFiled(t, f, f.svc.options.WorkPoll)
	f.awaitRearming(t)

	// A quiet poll: nothing moved, and the running countdown is left alone.
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.clock.awaitWaiters(t, f.svc.options.WorkPoll, 1)
	if got := f.clock.rearmings(); len(got) != 1 {
		t.Fatalf("an idle poll with nothing journaled re-armed the countdown: %v", got)
	}

	// The runtime journals a turn and is idle again by the next sample.
	journal.append(event.TurnDone{}, 5)
	fireFiled(t, f, testGateRetry)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if position, _ := f.svc.options.WorkStates.(activityReporter).activity(keyA); position == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the publisher never folded the turn")
		}
		time.Sleep(time.Millisecond)
	}
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.clock.awaitWaiters(t, f.svc.options.WorkPoll, 1)
	if got := f.clock.rearmings(); len(got) != 2 || got[1] != defaultFixtureWarmTTL {
		t.Fatalf("after work between two idle samples the countdown was armed %v, want a second whole %v", got, defaultFixtureWarmTTL)
	}
}

// TestADedicatedHostWithADerivedSourceNeverArmsTheCountdown is the
// conservative placement choice: a dedicated Host's session is ended by its
// controller's drain, not by a warm release.
func TestADedicatedHostWithADerivedSourceNeverArmsTheCountdown(t *testing.T) {
	f, _ := derivedFixture(t, func(_ *Options, host *hostconfig.Options) {
		host.Placement = sessionwire.HostPlacementDedicated
		host.FixedSessionID = sessionA
		host.Capacity = 1
	})
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)
	if f.clock.waitForWaiters(f.svc.options.WorkPoll, 1, 200*time.Millisecond) {
		t.Fatal("a dedicated Host with a derived source runs the warm sampler")
	}
	if got := f.clock.rearmings(); len(got) != 0 {
		t.Fatalf("a dedicated Host armed a warm countdown for %v", got)
	}
}

// TestDeriveWorkStatesIsRefusedWithoutGatesOrBesideASource: both are
// constructions whose answer would be wrong.
func TestDeriveWorkStatesIsRefusedWithoutGatesOrBesideASource(t *testing.T) {
	for _, row := range []struct {
		name   string
		adjust func(*Options)
	}{
		{"without gates", func(options *Options) { options.Gates = nil }},
		{"beside a caller source", func(options *Options) { options.WorkStates = &fakeWorkStates{} }},
	} {
		t.Run(row.name, func(t *testing.T) {
			f, _ := derivedFixture(t)
			options := f.svc.options
			options.WorkStates = nil
			row.adjust(&options)
			_, err := New(options)
			var invalid *InvalidOptionsError
			if !errors.As(err, &invalid) || invalid.Field != "DeriveWorkStates" {
				t.Fatalf("New = %v, want an InvalidOptionsError on DeriveWorkStates", err)
			}
		})
	}
}

// expireWarm delivers an expiry to every warm countdown the fixture's clock has
// handed out.
func expireWarm(f *fixture) {
	f.clock.mu.Lock()
	timers := append([]*fakeWarmTimer(nil), f.clock.timers...)
	f.clock.mu.Unlock()
	for _, timer := range timers {
		select {
		case timer.expiry <- time.Now():
		default:
		}
	}
}

// neverReleased fails if the runtime is taken through ReleaseResidency within
// a short window.
func neverReleased(t *testing.T, f *fixture, why string) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if f.runtime.Released() != 0 {
			t.Fatalf("%s: the runtime was released (trace %v)", why, f.trace.trace())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAnExpiryAfterTheRuntimeTurnedBusyReleasesNothing is the reviewer's F1
// demonstration, inverted. The countdown is armed by an idle sample; a turn
// starts before the next sample; the countdown expires. Nothing re-sampled,
// and before the fix the release proceeded against a busy runtime. Now the
// expiry re-confirms and takes nothing, and once the session is idle again a
// fresh countdown releases it.
func TestAnExpiryAfterTheRuntimeTurnedBusyReleasesNothing(t *testing.T) {
	f, _ := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.awaitRearming(t)

	f.runtime.HoldIdle()
	awaitState(t, f, residency.WorkStateWorking)
	expireWarm(f)
	neverReleased(t, f, "a runtime busy at expiry")
	for _, step := range []string{"lease.release", "locations.tombstone"} {
		if f.trace.count(step) != 0 {
			t.Fatalf("a runtime busy at expiry reached %q: %v", step, f.trace.trace())
		}
	}

	// The turn ends; the next idle sample arms a whole TTL, and it releases.
	f.runtime.GoIdle()
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.clock.awaitWaiters(t, f.svc.options.WorkPoll, 1)
	if got := f.clock.rearmings(); len(got) != 2 {
		t.Fatalf("after the turn the countdown was armed %v, want a second arming", got)
	}
	expireWarm(f)
	deadline := time.Now().Add(5 * time.Second)
	for f.runtime.Released() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the idle session was never released after its re-armed countdown (trace %v)", f.trace.trace())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAnExpiryAfterWorkSinceTheLastSampleReleasesNothing: the runtime is idle
// at expiry, but the journal moved since the sample that armed the countdown —
// a whole short turn fell between samples. The expiry takes nothing.
func TestAnExpiryAfterWorkSinceTheLastSampleReleasesNothing(t *testing.T) {
	f, journal := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)
	fireFiled(t, f, f.svc.options.WorkPoll)
	f.awaitRearming(t)

	journal.append(event.TurnDone{}, 5)
	fireFiled(t, f, testGateRetry)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if position, _ := f.svc.options.WorkStates.(activityReporter).activity(keyA); position == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the publisher never folded the turn")
		}
		time.Sleep(time.Millisecond)
	}
	expireWarm(f)
	neverReleased(t, f, "a session that worked since its countdown was armed")
}

// TestAResidentWithNoGatePublisherIsNeverIdle is F4: the derived source's
// "unknown is busy" branch for a session whose gates this Host cannot see.
func TestAResidentWithNoGatePublisherIsNeverIdle(t *testing.T) {
	f, _ := derivedFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	awaitState(t, f, residency.WorkStateIdle)

	f.svc.mu.Lock()
	held := f.svc.sessions[keyA]
	work := held.work
	held.work = &sessionWork{stop: work.stop}
	f.svc.mu.Unlock()
	t.Cleanup(func() {
		f.svc.mu.Lock()
		held.work = work
		f.svc.mu.Unlock()
	})

	if state, known := f.svc.options.WorkStates.WorkState(keyA); !known || state != residency.WorkStateWorking {
		t.Fatalf("a resident with no gate publisher reported (%q, %v), want (working, true)", state, known)
	}
	if f.svc.ConfirmIdle(keyA) {
		t.Fatal("a resident with no gate publisher was confirmed idle")
	}
}
