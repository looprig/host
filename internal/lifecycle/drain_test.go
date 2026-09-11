package lifecycle_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// The drain state machine IS O5.4's production DrainStarter and DrainObserver.
// The assertion lives here rather than in production so that the wiring is
// proved without internal/lifecycle owning an opinion about the transport, and
// because hostlink cannot import this package without a cycle.
var (
	_ hostlink.DrainStarter  = (*lifecycle.Drainer)(nil)
	_ hostlink.DrainObserver = (*lifecycle.Drainer)(nil)
)

// The admission ledger the drain consumes is the SAME OBJECT internal/service
// already owns. internal/service's own comment says O6.3 must consume that
// ledger and not keep a second one; this line is what makes that structural
// instead of remembered.
var _ lifecycle.Admissions = (*service.CapacityPublisher)(nil)

const (
	testTenant     = sessionwire.TenantID("tenant-service")
	testGeneration = uint64(4)
	testGrace      = 30 * time.Second
	testIdleGrace  = 5 * time.Second
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeClock hands out timer channels a test fires by hand. NOTHING SLEEPS.
type fakeClock struct {
	mu       sync.Mutex
	waits    []time.Duration
	channels []chan time.Time
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	channel := make(chan time.Time, 1)
	c.waits = append(c.waits, d)
	c.channels = append(c.channels, channel)
	return channel
}

// fireAll expires every timer handed out so far, which is how a test reaches
// the deadline path without waiting for one.
func (c *fakeClock) fireAll() {
	c.mu.Lock()
	channels := append([]chan time.Time(nil), c.channels...)
	c.channels = nil
	c.mu.Unlock()
	for _, channel := range channels {
		channel <- time.Unix(0, 0)
	}
}

func (c *fakeClock) requested() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
}

// pending reports how many timers are outstanding, so a test can wait for the
// drain to REACH the wait rather than racing it.
func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.channels)
}

type fakeLedger struct {
	mu       sync.Mutex
	draining bool
	begins   int
}

func (l *fakeLedger) BeginDrain() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining = true
	l.begins++
}

func (l *fakeLedger) Draining() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func (l *fakeLedger) counts() (draining bool, begins int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining, l.begins
}

type advertiser struct {
	mu        sync.Mutex
	published int
	err       error

	// observedDraining records what the ledger said at each publication, which
	// is how the "ledger first, publication second" order is asserted without
	// a timestamp.
	observedDraining []bool
	ledger           *fakeLedger

	// during runs inside the publication, so a test can inspect the world at
	// the one instant the transition is half-complete by construction.
	during func()
}

func (a *advertiser) PublishNonaccepting(context.Context) error {
	a.mu.Lock()
	a.published++
	a.observedDraining = append(a.observedDraining, a.ledger.Draining())
	hook, err := a.during, a.err
	a.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}

func (a *advertiser) snapshot() (published int, observed []bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.published, append([]bool(nil), a.observedDraining...)
}

func (a *advertiser) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.published
}

// step names one thing a drain does to one session, and the recorded order of
// these is the whole of the sequence assertion.
type step string

const (
	stepBeginRelease     step = "begin_release"
	stepWaitIdle         step = "wait_idle"
	stepCheckpoint       step = "checkpoint"
	stepReleaseResidency step = "release_residency"
	stepFinishRelease    step = "finish_release"
	stepLinkClose        step = "link_close"
)

// journal is the shared, ordered record of everything the drain did, across
// every session. It is shared on purpose: the cross-session ordering claims —
// "no session is checkpointed before the Host stopped accepting" — are not
// expressible in per-session logs.
type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) record(entry string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) recorded() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

func (j *journal) indexOf(entry string) int { return slices.Index(j.recorded(), entry) }

func (j *journal) len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.entries)
}

type fakeSession struct {
	key     registry.Key
	journal *journal

	// hang makes WaitIdle block until the drain cancels it, which is the one
	// hung session the platform grace exists for.
	hang bool

	errs map[step]error

	mu    sync.Mutex
	steps []step
}

func newSession(session sessionwire.SessionID, shared *journal) *fakeSession {
	return &fakeSession{
		key:     registry.Key{TenantID: testTenant, SessionID: session},
		journal: shared,
		errs:    map[step]error{},
	}
}

func (s *fakeSession) Key() registry.Key { return s.key }

func (s *fakeSession) run(name step) error {
	s.mu.Lock()
	s.steps = append(s.steps, name)
	s.mu.Unlock()
	s.journal.record(string(s.key.SessionID) + ":" + string(name))
	return s.errs[name]
}

func (s *fakeSession) BeginRelease(context.Context) error { return s.run(stepBeginRelease) }

func (s *fakeSession) WaitIdle(ctx context.Context) error {
	if err := s.run(stepWaitIdle); err != nil {
		return err
	}
	if !s.hang {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *fakeSession) Checkpoint(context.Context) error       { return s.run(stepCheckpoint) }
func (s *fakeSession) ReleaseResidency(context.Context) error { return s.run(stepReleaseResidency) }
func (s *fakeSession) FinishRelease(context.Context) error    { return s.run(stepFinishRelease) }

func (s *fakeSession) stepsTaken() []step {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]step(nil), s.steps...)
}

type residents struct {
	mu       sync.Mutex
	sessions []lifecycle.Session
	reads    int
}

func (r *residents) ResidentSessions() []lifecycle.Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads++
	return append([]lifecycle.Session(nil), r.sessions...)
}

func (r *residents) add(session lifecycle.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions = append(r.sessions, session)
}

func (r *residents) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

type fakeLink struct {
	mu      sync.Mutex
	journal *journal
	closes  int
	err     error
}

func (l *fakeLink) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closes++
	l.journal.record(string(stepLinkClose))
	return l.err
}

func (l *fakeLink) closeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

type fixture struct {
	t          *testing.T
	clock      *fakeClock
	ledger     *fakeLedger
	advertiser *advertiser
	residents  *residents
	link       *fakeLink
	journal    *journal
	drainer    *lifecycle.Drainer
}

func newFixture(t *testing.T, sessions ...*fakeSession) *fixture {
	t.Helper()
	shared := &journal{}
	clock := &fakeClock{}
	admissions := &fakeLedger{}
	ads := &advertiser{ledger: admissions}
	index := &residents{}
	for _, session := range sessions {
		session.journal = shared
		index.add(session)
	}
	connection := &fakeLink{journal: shared}
	drainer, err := lifecycle.NewDrainer(lifecycle.Options{
		Generation:   testGeneration,
		Clock:        clock,
		Admissions:   admissions,
		Advertiser:   ads,
		Residents:    index,
		Link:         connection,
		Grace:        testGrace,
		IdleBoundary: testIdleGrace,
	})
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	return &fixture{
		t: t, clock: clock, ledger: admissions, advertiser: ads,
		residents: index, link: connection, journal: shared, drainer: drainer,
	}
}

func mustStart(t *testing.T, drainer *lifecycle.Drainer) hostlink.DrainStatus {
	t.Helper()
	status, err := drainer.StartDrain(hostlink.DrainScope{})
	if err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	return status
}

// awaitDrained waits for the state machine to finish. It is not a sleep: Wait
// returns when the last session's cleanup has run.
func awaitDrained(t *testing.T, drainer *lifecycle.Drainer) lifecycle.Report {
	t.Helper()
	report := drainer.Wait()
	if report.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("state after Wait = %q, want %q", report.State, sessionwire.HostLinkDrainStateDrained)
	}
	return report
}

// waitFor spins on a STATE PREDICATE rather than a duration, so a test
// synchronises on the drain having reached a point instead of on time passing.
func waitFor(t *testing.T, what string, reached func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !reached() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Step 2 of O5.4: the atomic transition
// ---------------------------------------------------------------------------

// partialStates are the half-done transitions O5.4 step 2's "atomically"
// forbids, expressed as predicates over what an observer can actually see.
//
// EACH IS A STATEMENT ABOUT ONE ORDERING, and together they are the whole of
// the claim: the ledger flips, then the publication lands, then the drain
// becomes observable and sessions begin to move. A sample that satisfies none
// of these is a world in which no step has been skipped or reordered.
type partialStates struct {
	ledger  *fakeLedger
	ads     *advertiser
	begun   func() bool
	journal *journal

	mu         sync.Mutex
	samples    int
	violations []string
}

func (p *partialStates) sample() {
	// Read the ledger FIRST and the publication SECOND. Reading them the other
	// way round would make "published while still admitting" reachable by the
	// sampler's own interleaving rather than by the drain's.
	draining := p.ledger.Draining()
	published := p.ads.count()
	begun := p.begun()
	touched := p.journal.len()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples++
	if published > 0 && !draining {
		p.violations = append(p.violations, "nonaccepting was published while the ledger was still admitting")
	}
	if begun && published == 0 {
		p.violations = append(p.violations, "the drain became observable before nonaccepting was published")
	}
	if touched > 0 && published == 0 {
		p.violations = append(p.violations, "a session was touched before nonaccepting was published")
	}
	if touched > 0 && !draining {
		p.violations = append(p.violations, "a session was touched while the ledger was still admitting")
	}
}

func (p *partialStates) findings() (samples int, violations []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.samples, append([]string(nil), p.violations...)
}

// TestTheDrainTransitionIsNotObservableHalfDone is O5.4 step 2's "atomically",
// and it carries its own POSITIVE CONTROL because without one it is a
// "nothing happened" assertion.
//
// The claim is that no observer sees the transition partly applied. An
// assertion that a set of forbidden states was never sampled is satisfied just
// as well by a sampler that cannot see those states at all, so the second
// subtest drives the SAME sampler against a deliberately misordered transition
// over the SAME fakes and requires it to report the violation. Only then does
// the first subtest's silence mean the drain is atomic rather than the probe
// being blind.
//
// The interleaving is not hoped for either. The advertiser's `during` hook runs
// INSIDE PublishNonaccepting, which is the one instant at which the ledger has
// flipped and the publication has not returned, so the most dangerous window is
// sampled by construction rather than by a racing goroutine getting lucky. A
// free-running sampler runs alongside it for the windows a hook cannot name.
func TestTheDrainTransitionIsNotObservableHalfDone(t *testing.T) {
	t.Parallel()

	t.Run("the production transition", func(t *testing.T) {
		t.Parallel()
		alpha := newSession("session-alpha", nil)
		beta := newSession("session-beta", nil)
		f := newFixture(t, alpha, beta)

		probe := &partialStates{
			ledger:  f.ledger,
			ads:     f.advertiser,
			journal: f.journal,
			begun: func() bool {
				_, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
				return begun
			},
		}

		// Sampled from INSIDE the publication, where the transition is
		// half-applied by construction.
		//
		// IT READS ONLY THE LOCK-FREE HALF, and that is itself the strongest
		// statement of atomicity available: ObserveDrain cannot be called from
		// here at all, because the whole transition runs inside the Drainer's
		// one critical section and this hook runs on the goroutine holding it.
		// A drain that published from OUTSIDE that section — the shape this
		// test exists to forbid — would let this call through. The free-running
		// sampler below does call ObserveDrain, from another goroutine, and its
		// "observable before published" predicate is what covers that window.
		var insidePublication struct {
			draining bool
			touched  int
		}
		f.advertiser.during = func() {
			insidePublication.draining = f.ledger.Draining()
			insidePublication.touched = f.journal.len()
		}

		stop := make(chan struct{})
		var sampling sync.WaitGroup
		sampling.Add(1)
		go func() {
			defer sampling.Done()
			for {
				select {
				case <-stop:
					probe.sample()
					return
				default:
					probe.sample()
				}
			}
		}()

		status := mustStart(t, f.drainer)
		awaitDrained(t, f.drainer)
		close(stop)
		sampling.Wait()

		samples, violations := probe.findings()
		if len(violations) != 0 {
			t.Fatalf("the transition was observed half-done: %v", violations)
		}
		if samples == 0 {
			t.Fatal("the probe took no samples, so it observed nothing")
		}

		// What the hook saw. The ledger had ALREADY flipped, and neither the
		// drain nor any session had moved — which is the acknowledgement's
		// price, paid in that order.
		if !insidePublication.draining {
			t.Fatal("the ledger was still admitting while nonaccepting was being published")
		}
		if insidePublication.touched != 0 {
			t.Fatalf("%d session steps had run before nonaccepting was published", insidePublication.touched)
		}
		if status.State != sessionwire.HostLinkDrainStateDraining {
			t.Fatalf("acknowledged state = %q, want %q", status.State, sessionwire.HostLinkDrainStateDraining)
		}
	})

	// THE POSITIVE CONTROL. The same probe, the same fakes, a transition that
	// publishes before it flips the ledger. If this does not report a
	// violation, the subtest above proves nothing.
	t.Run("the control: a misordered transition is detected", func(t *testing.T) {
		t.Parallel()
		ledger := &fakeLedger{}
		ads := &advertiser{ledger: ledger}
		shared := &journal{}
		probe := &partialStates{ledger: ledger, ads: ads, journal: shared, begun: func() bool { return false }}

		stop := make(chan struct{})
		var sampling sync.WaitGroup
		sampling.Add(1)
		go func() {
			defer sampling.Done()
			for {
				select {
				case <-stop:
					probe.sample()
					return
				default:
					probe.sample()
				}
			}
		}()

		// Publication first, ledger second — and the publication BLOCKS until
		// the sampler has seen the half-applied world, so the control is not
		// itself a race.
		seen := make(chan struct{})
		ads.during = func() {
			// A bounded spin, not a t.Fatal: this runs off the test goroutine.
			// If the sampler never sees the violation the assertion below is
			// what reports it.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if _, violations := probe.findings(); len(violations) > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			close(seen)
		}
		go func() {
			_ = ads.PublishNonaccepting(context.Background())
			ledger.BeginDrain()
		}()
		<-seen
		close(stop)
		sampling.Wait()

		_, violations := probe.findings()
		if len(violations) == 0 {
			t.Fatal("the probe did not detect a transition that published nonaccepting before it stopped admitting, " +
				"so its silence on the production path means nothing")
		}
		if !slices.Contains(violations, "nonaccepting was published while the ledger was still admitting") {
			t.Fatalf("the probe reported %v, want the misordered publication", violations)
		}
	})
}

// ---------------------------------------------------------------------------
// Sequence
// ---------------------------------------------------------------------------

// TestDrainStopsAdmissionBeforeItTouchesASession is O6.3's ordering claim and
// O5.4's acknowledgement contract in one: the ledger is flipped and the
// nonaccepting publication is durable BEFORE any session is checkpointed or
// released, and StartDrain returns without waiting for either.
func TestDrainStopsAdmissionBeforeItTouchesASession(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	f := newFixture(t, alpha)

	status := mustStart(t, f.drainer)
	if status.Generation != testGeneration {
		t.Fatalf("acknowledged generation = %d, want %d", status.Generation, testGeneration)
	}

	draining, begins := f.ledger.counts()
	if !draining || begins != 1 {
		t.Fatalf("ledger after StartDrain: draining=%v begins=%d, want true and 1", draining, begins)
	}
	published, observed := f.advertiser.snapshot()
	if published != 1 {
		t.Fatalf("nonaccepting publications = %d, want one", published)
	}
	if len(observed) != 1 || !observed[0] {
		t.Fatalf("the ledger said draining=%v at publication time, want true — the ledger flips first", observed)
	}

	awaitDrained(t, f.drainer)
	want := []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease}
	if got := alpha.stepsTaken(); !slices.Equal(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
}

// TestTheAcknowledgementPrecedesCheckpointAndRelease is O6.3 step 3 stated as
// the two things that must be true at the instant StartDrain returns.
//
// The acknowledgement's PRICE is the nonaccepting publication, and nothing
// more: a Factory that reads the acknowledgement knows this Host has stopped
// admitting, and knows NOTHING about any session having been checkpointed or
// released. The wait is what tells it that.
func TestTheAcknowledgementPrecedesCheckpointAndRelease(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	mustStart(t, f.drainer)
	if published, _ := f.advertiser.snapshot(); published != 1 {
		t.Fatalf("nonaccepting publications at acknowledgement = %d, want one", published)
	}

	// The session hangs in WaitIdle, so the drain is provably still in flight.
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })
	for _, forbidden := range []step{stepCheckpoint, stepReleaseResidency, stepFinishRelease} {
		if slices.Contains(alpha.stepsTaken(), forbidden) {
			t.Fatalf("%s had run while the drain was still acknowledged as draining", forbidden)
		}
	}
	if status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{}); !begun || status.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("observation mid-drain = %+v/%v, want draining", status, begun)
	}

	f.clock.fireAll()
	awaitDrained(t, f.drainer)
}

// TestDrainedMeansEveryConditionFactoryWaitsForHasHappened is O5.4 step 4's
// other half.
//
// Step 4 exists so a Factory can wait for NONACCEPTING, CHECKPOINT, COLD
// REGISTRY and LEASE RELEASE before deleting a dedicated workload. Core's
// bounded state has only two values, so `drained` has to carry all four: the
// registry row going cold and the lease being released are FinishRelease's, and
// a `drained` reported before it is a Factory deleting a workload whose lease
// this Host still holds.
func TestDrainedMeansEveryConditionFactoryWaitsForHasHappened(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	beta := newSession("session-beta", nil)
	f := newFixture(t, alpha, beta)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 0 {
		t.Fatalf("a clean drain reported failures: %v", report.Failures)
	}

	if published, _ := f.advertiser.snapshot(); published != 1 {
		t.Fatalf("nonaccepting publications = %d, want one", published)
	}
	for _, session := range []*fakeSession{alpha, beta} {
		for _, required := range []step{stepCheckpoint, stepReleaseResidency, stepFinishRelease} {
			if !slices.Contains(session.stepsTaken(), required) {
				t.Fatalf("%q reported drained without %s", session.Key().SessionID, required)
			}
		}
	}
	// And the observation a Factory would poll agrees with the report.
	status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
	if !begun || status.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("observation after Wait = %+v/%v, want drained", status, begun)
	}
}

// TestDrainingNoSessionsIsStillADrain is step 1's empty case: a Host holding
// nothing still stops admitting, still publishes, and still reports drained.
func TestDrainingNoSessionsIsStillADrain(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 0 {
		t.Fatalf("failures = %v, want none", report.Failures)
	}
	if published, _ := f.advertiser.snapshot(); published != 1 {
		t.Fatalf("nonaccepting publications = %d, want one", published)
	}
	if f.link.closeCount() != 1 {
		t.Fatalf("link closes = %d, want one", f.link.closeCount())
	}
}

// TestEverySessionIsDrainedAndNoneBlocksAnother is step 1's concurrent case.
//
// The sessions are released CONCURRENTLY, so a Host holding many does not pay
// the sum of their idle waits; the ordering that survives is the per-session
// one, which is asserted on each.
func TestEverySessionIsDrainedAndNoneBlocksAnother(t *testing.T) {
	t.Parallel()
	sessions := []*fakeSession{
		newSession("session-alpha", nil),
		newSession("session-beta", nil),
		newSession("session-gamma", nil),
	}
	f := newFixture(t, sessions[0], sessions[1], sessions[2])

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 0 {
		t.Fatalf("failures = %v, want none", report.Failures)
	}

	want := []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease}
	for _, session := range sessions {
		if got := session.stepsTaken(); !slices.Equal(got, want) {
			t.Fatalf("%q steps = %v, want %v", session.Key().SessionID, got, want)
		}
		// No session's first step precedes the nonaccepting publication, and
		// the shared journal is what makes that a cross-session claim.
		if index := f.journal.indexOf(string(session.Key().SessionID) + ":" + string(stepBeginRelease)); index < 0 {
			t.Fatalf("%q never began release", session.Key().SessionID)
		}
	}
	if f.residents.readCount() != 1 {
		t.Fatalf("the resident set was read %d times, want once", f.residents.readCount())
	}
}

// TestASessionThatFailsAStepIsStillCleanedUp is step 1's checkpoint-error case
// and the runbook's "record explicit failures and continue deterministic
// cleanup".
//
// A drain that abandoned the rest of a session on its first failure would turn
// one unreleased resource into several, and a drain that reported success while
// leaving a live route with no owner would be worse than either. The failing
// session's later steps still run AND the other session is untouched by it.
func TestASessionThatFailsAStepIsStillCleanedUp(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	broken := errors.New("the workspace checkpoint could not be committed")
	alpha.errs[stepCheckpoint] = broken
	beta := newSession("session-beta", nil)
	f := newFixture(t, alpha, beta)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)

	want := []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease}
	if got := alpha.stepsTaken(); !slices.Equal(got, want) {
		t.Fatalf("the failing session's steps = %v, want the whole sequence %v", got, want)
	}
	if got := beta.stepsTaken(); !slices.Equal(got, want) {
		t.Fatalf("the healthy session's steps = %v, want %v", got, want)
	}
	if len(report.Failures) != 1 {
		t.Fatalf("failures = %v, want exactly the checkpoint", report.Failures)
	}
	failure := report.Failures[0]
	if failure.Key != alpha.Key() || failure.Step != lifecycle.StepCheckpoint {
		t.Fatalf("failure = %+v, want the checkpoint of %q", failure, alpha.Key().SessionID)
	}
	if !errors.Is(failure, broken) {
		t.Fatalf("the failure lost its cause: %v", failure)
	}
	if !strings.Contains(failure.Error(), string(alpha.Key().SessionID)) {
		t.Fatalf("the failure does not name its session: %v", failure)
	}
}

// TestARefusedReleaseResidencyStillReleasesTheLease records a DECISION rather
// than merely a behaviour, because the two outcomes are both bad and the choice
// between them belongs in a test that will fail if someone reverses it.
//
// ReleaseResidency closing process-local resources is NONTERMINAL. When it
// refuses, continuing to FinishRelease releases the lease while those resources
// may still be live; stopping leaks the lease AND leaves a published route with
// no owner. This drain continues, because the second is unrecoverable without
// an operator and the first costs a rehydration.
//
// WHAT IS NOT HERE, stated so it is not mistaken for covered: the runbook's
// hold names a Shutdown fallback for the case where graceful release refuses.
// department.Runtime declares no terminal capability — Identity, IdleWaiter,
// Liveness, Releaser, PublicationSubscriber, CommandApplier, LeaseEpochReporter
// — and its own comment on Releaser says residency release "is not Shutdown".
// A Shutdown seam declared here would therefore be one nothing in this module
// can satisfy. The refusal is recorded as its own Failure so a reconciler can
// find it; the fallback is H4.1's and O7.1's to wire.
func TestARefusedReleaseResidencyStillReleasesTheLease(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	refused := errors.New("the runtime refused to release residency")
	alpha.errs[stepReleaseResidency] = refused
	f := newFixture(t, alpha)

	mustStart(t, f.drainer)
	report := awaitDrained(t, f.drainer)

	if !slices.Contains(alpha.stepsTaken(), stepFinishRelease) {
		t.Fatal("a refused ReleaseResidency stopped the drain before the lease was released")
	}
	if len(report.Failures) != 1 || report.Failures[0].Step != lifecycle.StepReleaseResidency {
		t.Fatalf("failures = %v, want exactly the release refusal", report.Failures)
	}
	if !errors.Is(report.Failures[0], refused) {
		t.Fatalf("the refusal lost its cause: %v", report.Failures[0])
	}
}

// TestAFailedNonacceptingPublicationRefusesTheDrain is the ONE failure that is
// not recorded and continued past.
//
// Nothing downstream knows this drain began, so continuing would release
// sessions a Factory is still routing work to. The ledger stays flipped — this
// Host has stopped admitting either way — and the caller may retry, which is
// what makes the refusal a running-Host condition rather than a lost drain.
func TestAFailedNonacceptingPublicationRefusesTheDrain(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	f := newFixture(t, alpha)
	unreachable := errors.New("the orchestration store is unreachable")
	f.advertiser.err = unreachable

	_, err := f.drainer.StartDrain(hostlink.DrainScope{})
	if !errors.Is(err, unreachable) {
		t.Fatalf("StartDrain error = %v, want the publication's own cause", err)
	}
	if draining, _ := f.ledger.counts(); !draining {
		t.Fatal("the ledger was rolled back, so this Host is admitting again after refusing a drain")
	}
	if _, begun := f.drainer.ObserveDrain(hostlink.DrainScope{}); begun {
		t.Fatal("a drain that never published is observable as begun")
	}
	if steps := alpha.stepsTaken(); len(steps) != 0 {
		t.Fatalf("the session was touched by a refused drain: %v", steps)
	}

	// And the retry succeeds once the store comes back, which is what
	// distinguishes a refusal from a Host that can never drain again.
	f.advertiser.mu.Lock()
	f.advertiser.err = nil
	f.advertiser.mu.Unlock()
	mustStart(t, f.drainer)
	awaitDrained(t, f.drainer)
	if published, _ := f.advertiser.snapshot(); published != 2 {
		t.Fatalf("publications = %d, want the failed one and the retry", published)
	}
}

// TestRepeatedDrainIsOneDrain is step 1's repeated case and O5.4 step 3's
// idempotency, at the layer that actually owns it.
//
// A HOST DRAINS ONCE. Idempotency lives here and not in HostLink because the
// transport has no way to know whether the work it acknowledged is still
// running. Concurrent, sequential and post-completion requests are all one
// drain, and the scope a request names does not create a second.
func TestRepeatedDrainIsOneDrain(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	const callers = 16
	statuses := make([]hostlink.DrainStatus, callers)
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(callers)
	for index := range callers {
		go func() {
			defer done.Done()
			start.Wait()
			scope := hostlink.DrainScope{}
			if index%2 == 0 {
				scope = hostlink.DrainScope{Key: alpha.Key()}
			}
			status, err := f.drainer.StartDrain(scope)
			if err != nil {
				t.Errorf("concurrent StartDrain: %v", err)
				return
			}
			statuses[index] = status
		}()
	}
	start.Done()
	done.Wait()

	for index, status := range statuses {
		if status.Generation != testGeneration {
			t.Fatalf("caller %d saw generation %d, want %d", index, status.Generation, testGeneration)
		}
	}
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("admission was stopped %d times, want once", begins)
	}
	if published, _ := f.advertiser.snapshot(); published != 1 {
		t.Fatalf("nonaccepting publications = %d, want one", published)
	}
	if f.residents.readCount() != 1 {
		t.Fatalf("the resident set was read %d times, want once", f.residents.readCount())
	}

	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })
	f.clock.fireAll()
	awaitDrained(t, f.drainer)

	// A request AFTER completion is still the same drain.
	after := mustStart(t, f.drainer)
	if after.Generation != testGeneration || after.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("post-completion request = %+v, want the same generation at drained", after)
	}
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("a post-completion request stopped admission again: begins=%d", begins)
	}
}

// TestObservingReportsProgressAndNeverStartsADrain is the observer half of the
// seams, held against the machine rather than against a fake.
func TestObservingReportsProgressAndNeverStartsADrain(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	if status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{}); begun {
		t.Fatalf("an unbegun Drainer reported %+v, want no drain", status)
	}
	if draining, begins := f.ledger.counts(); draining || begins != 0 {
		t.Fatalf("observation stopped admission: draining=%v begins=%d", draining, begins)
	}
	if published, _ := f.advertiser.snapshot(); published != 0 {
		t.Fatalf("observation published %d times, want none", published)
	}

	mustStart(t, f.drainer)
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })
	status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
	if !begun || status.State != sessionwire.HostLinkDrainStateDraining || status.Generation != testGeneration {
		t.Fatalf("observation mid-drain = %+v/%v, want draining at %d", status, begun, testGeneration)
	}

	f.clock.fireAll()
	awaitDrained(t, f.drainer)
	status, begun = f.drainer.ObserveDrain(hostlink.DrainScope{})
	if !begun || status.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("observation after completion = %+v/%v, want drained", status, begun)
	}
}

// TestANewlyResidentSessionIsRefusedRatherThanDrained is step 1's "newly
// refused attach".
//
// The resident set is snapshotted ONCE, by the call that begins the drain. A
// session that becomes resident afterwards is refused by the ledger that is
// already flipped; chasing it here would be a machine that can be kept working
// by a caller it has already told to stop.
func TestANewlyResidentSessionIsRefusedRatherThanDrained(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	mustStart(t, f.drainer)
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })

	late := newSession("session-late", f.journal)
	f.residents.add(late)

	f.clock.fireAll()
	awaitDrained(t, f.drainer)
	if steps := late.stepsTaken(); len(steps) != 0 {
		t.Fatalf("a session that attached after the drain began was drained: %v", steps)
	}
	if f.residents.readCount() != 1 {
		t.Fatalf("the resident set was read %d times, want once", f.residents.readCount())
	}
	// The control: the ledger is what refuses it, and it is flipped.
	if draining, _ := f.ledger.counts(); !draining {
		t.Fatal("the ledger is not draining, so nothing refuses the late attach")
	}
}

// TestTheIdleBoundaryAndPlatformGraceAreBothBounded is step 1's hung-session and
// platform-grace cases.
//
// The per-session idle wait is bounded by IdleBoundary and the whole drain by
// Grace, and the tighter one wins because the idle wait's context derives from
// the grace's. A session that never settles is recorded and its cleanup still
// runs, which is the point of bounding it rather than of failing.
func TestTheIdleBoundaryAndPlatformGraceAreBothBounded(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	mustStart(t, f.drainer)
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 1 })
	if requested := f.clock.requested(); !slices.Contains(requested, testIdleGrace) || !slices.Contains(requested, testGrace) {
		t.Fatalf("timers requested = %v, want both %v and %v", requested, testIdleGrace, testGrace)
	}

	f.clock.fireAll()
	report := awaitDrained(t, f.drainer)
	if len(report.Failures) != 1 || report.Failures[0].Step != lifecycle.StepWaitIdle {
		t.Fatalf("failures = %v, want exactly the idle wait", report.Failures)
	}
	if !errors.Is(report.Failures[0], lifecycle.ErrIdleBoundary) {
		t.Fatalf("the failure is not attributable to the boundary: %v", report.Failures[0])
	}
	// Cleanup ran to the end regardless, which is what the boundary is for.
	for _, required := range []step{stepCheckpoint, stepReleaseResidency, stepFinishRelease} {
		if !slices.Contains(alpha.stepsTaken(), required) {
			t.Fatalf("a session that missed its boundary skipped %s", required)
		}
	}
}

// TestTheLinkIsOptionalAndClosesLast is step 1's HostLink shutdown ordering.
//
// Factory reaches the bounded drain-status observation through this link, so a
// link closed when the drain began would leave Factory inferring completion
// from a disconnect — which is the inference O5.4's status observation exists to
// replace.
func TestTheLinkIsOptionalAndClosesLast(t *testing.T) {
	t.Parallel()

	t.Run("it closes after the last release", func(t *testing.T) {
		t.Parallel()
		alpha := newSession("session-alpha", nil)
		alpha.hang = true
		f := newFixture(t, alpha)

		mustStart(t, f.drainer)
		// The session hangs, so the drain is PROVABLY still in flight when
		// this runs. Synchronising on the fake clock's state rather than on a
		// duration is what makes the assertion below deterministic.
		waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })
		if f.link.closeCount() != 0 {
			t.Fatalf("the link was closed %d times during initiation, want none", f.link.closeCount())
		}

		f.clock.fireAll()
		awaitDrained(t, f.drainer)
		if f.link.closeCount() != 1 {
			t.Fatalf("link closes = %d, want one", f.link.closeCount())
		}
		// The ORDER, not the count: an index comparison is independent of
		// scheduling altogether.
		closed := f.journal.indexOf(string(stepLinkClose))
		finished := f.journal.indexOf("session-alpha:" + string(stepFinishRelease))
		if closed < 0 || finished < 0 || closed < finished {
			t.Fatalf("journal = %v: the link closed at %d and the release finished at %d", f.journal.recorded(), closed, finished)
		}
	})

	t.Run("a Host composed without one still drains", func(t *testing.T) {
		t.Parallel()
		index := &residents{}
		ledger := &fakeLedger{}
		drainer, err := lifecycle.NewDrainer(lifecycle.Options{
			Generation:   testGeneration,
			Clock:        &fakeClock{},
			Admissions:   ledger,
			Advertiser:   &advertiser{ledger: ledger},
			Residents:    index,
			Grace:        testGrace,
			IdleBoundary: testIdleGrace,
		})
		if err != nil {
			t.Fatalf("NewDrainer without a link: %v", err)
		}
		mustStart(t, drainer)
		awaitDrained(t, drainer)
	})

	t.Run("a failing close is recorded and the Host is still drained", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.link.err = errors.New("the transport would not shut down")
		mustStart(t, f.drainer)
		report := awaitDrained(t, f.drainer)
		if len(report.Failures) != 1 || report.Failures[0].Step != lifecycle.StepCloseLink {
			t.Fatalf("failures = %v, want exactly the link close", report.Failures)
		}
	})
}

// TestOptionsAreValidated refuses a Drainer that could not do its job, at
// construction rather than at the drain.
func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()

	valid := func() lifecycle.Options {
		ledger := &fakeLedger{}
		return lifecycle.Options{
			Generation:   testGeneration,
			Clock:        &fakeClock{},
			Admissions:   ledger,
			Advertiser:   &advertiser{ledger: ledger},
			Residents:    &residents{},
			Grace:        testGrace,
			IdleBoundary: testIdleGrace,
		}
	}

	for _, test := range []struct {
		name    string
		corrupt func(*lifecycle.Options)
		field   string
	}{
		{"a zero generation", func(o *lifecycle.Options) { o.Generation = 0 }, "Generation"},
		{"no clock", func(o *lifecycle.Options) { o.Clock = nil }, "Clock"},
		{"no admission ledger", func(o *lifecycle.Options) { o.Admissions = nil }, "Admissions"},
		{"no advertiser", func(o *lifecycle.Options) { o.Advertiser = nil }, "Advertiser"},
		{"no resident index", func(o *lifecycle.Options) { o.Residents = nil }, "Residents"},
		{"a zero grace", func(o *lifecycle.Options) { o.Grace = 0 }, "Grace"},
		{"a zero idle boundary", func(o *lifecycle.Options) { o.IdleBoundary = 0 }, "IdleBoundary"},
		{
			"an idle boundary longer than the whole drain",
			func(o *lifecycle.Options) { o.IdleBoundary = testGrace + time.Second },
			"IdleBoundary",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := valid()
			test.corrupt(&options)
			drainer, err := lifecycle.NewDrainer(options)
			if err == nil {
				t.Fatalf("NewDrainer accepted %s and returned %#v", test.name, drainer)
			}
			var invalid *lifecycle.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("error %v is not an *InvalidOptionsError", err)
			}
			if invalid.Field != test.field {
				t.Fatalf("field = %q, want %q", invalid.Field, test.field)
			}
			if invalid.Reason == "" {
				t.Fatalf("%s was refused with no reason", test.field)
			}
		})
	}

	if _, err := lifecycle.NewDrainer(valid()); err != nil {
		t.Fatalf("valid options were refused: %v", err)
	}
}

// TestWaitOnAnUnbegunDrainerReturnsRatherThanBlocking holds the one shape that
// would turn a composition mistake into a hung shutdown.
func TestWaitOnAnUnbegunDrainerReturnsRatherThanBlocking(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	returned := make(chan lifecycle.Report, 1)
	go func() { returned <- f.drainer.Wait() }()
	select {
	case report := <-returned:
		if report.State != sessionwire.HostLinkDrainStateDraining {
			t.Fatalf("an unbegun report = %+v, want the unbegun state", report)
		}
		if report.Generation != testGeneration {
			t.Fatalf("generation = %d, want %d", report.Generation, testGeneration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked on a Drainer that had not begun")
	}
}

// ---------------------------------------------------------------------------
// Class 4: the fake this lane was asked to inject, audited in both directions
// ---------------------------------------------------------------------------

// TestTheDrainFakeMatchesTheProductionMachineInBothDirections is the Class 4
// audit of hostlink's stubDrains.
//
// A fake LOOSER than the dependency lets a handler defect through; a fake
// TIGHTER than it lets the handler rely on a promise production does not make.
// The stub cannot be reached from here and the Drainer cannot be reached from
// there, so the audit is over the OBSERVABLE CONTRACT both must satisfy, driven
// against the real Drainer: every property hostlink's tests rely on is asserted
// of production, and any the Drainer does not have would be a promise the stub
// invented.
//
// The properties, and where hostlink depends on each:
//
//  1. StartDrain returns PROMPTLY and does not wait for release — the whole of
//     "acknowledges initiation, not completion".
//  2. A repeat returns the SAME generation and begins nothing — every
//     idempotency assertion in hostlink's drain_test.go.
//  3. ObserveDrain on an unbegun machine returns (zero, false) and STARTS
//     NOTHING — RefusalNoDrainInProgress rests on it.
//  4. StartDrain and ObserveDrain report the SAME status once a drain exists —
//     the stub answers both from one `status()`, which would be a fiction if
//     production could diverge.
//  5. The scope is accepted and not branched on into a second drain — hostlink
//     resolves the scope and production must not re-decide it.
func TestTheDrainFakeMatchesTheProductionMachineInBothDirections(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	alpha.hang = true
	f := newFixture(t, alpha)

	// 3. Unbegun observation, before anything else can have begun a drain.
	if status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{}); begun || status != (hostlink.DrainStatus{}) {
		t.Fatalf("unbegun ObserveDrain = %+v/%v, want the zero status and false", status, begun)
	}
	if draining, _ := f.ledger.counts(); draining {
		t.Fatal("ObserveDrain began a drain")
	}

	// 1. StartDrain returns while the session is still hanging in WaitIdle.
	started := make(chan hostlink.DrainStatus, 1)
	go func() { started <- mustStart(t, f.drainer) }()
	var first hostlink.DrainStatus
	select {
	case first = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("StartDrain did not return while a session was still settling")
	}
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pending() > 0 })
	if slices.Contains(alpha.stepsTaken(), stepFinishRelease) {
		t.Fatal("StartDrain returned only after the release had finished")
	}

	// 2. A repeat: same generation, nothing begun again.
	repeat, err := f.drainer.StartDrain(hostlink.DrainScope{Key: alpha.Key()})
	if err != nil {
		t.Fatalf("repeat StartDrain: %v", err)
	}
	if repeat != first {
		t.Fatalf("repeat = %+v, want %+v", repeat, first)
	}
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("the repeat began %d drains, want none beyond the first", begins)
	}

	// 4. The two methods agree.
	observed, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
	if !begun || observed != first {
		t.Fatalf("ObserveDrain = %+v/%v, want %+v", observed, begun, first)
	}

	// 5. A different scope did not produce a second drain, which subtest 2
	//    already exercised by passing the session scope.
	if f.residents.readCount() != 1 {
		t.Fatalf("the resident set was read %d times, want once", f.residents.readCount())
	}

	f.clock.fireAll()
	awaitDrained(t, f.drainer)

	// And the state moves, which is what makes hostlink's "re-ask, never
	// cache" assertion meaningful against production rather than only against
	// a stub that could be told to change.
	final, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
	if !begun || final.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("final observation = %+v/%v, want drained", final, begun)
	}
	if final.Generation != first.Generation {
		t.Fatalf("the generation moved from %d to %d across the drain", first.Generation, final.Generation)
	}
}

// TestTheDrainSeamsAreNarrowerThanTheTypesBehindThem holds the local interfaces
// at the width step 2 requires.
//
// A widened Session is a drain that can do something to a session the ordered
// sequence does not name, and a widened Admissions is a second answer to "is
// this Host admitting". The method SETS are pinned by name.
func TestTheDrainSeamsAreNarrowerThanTheTypesBehindThem(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		seam    reflect.Type
		methods []string
	}{
		{reflect.TypeOf((*lifecycle.Clock)(nil)).Elem(), []string{"After"}},
		{reflect.TypeOf((*lifecycle.Admissions)(nil)).Elem(), []string{"BeginDrain", "Draining"}},
		{reflect.TypeOf((*lifecycle.Advertiser)(nil)).Elem(), []string{"PublishNonaccepting"}},
		{reflect.TypeOf((*lifecycle.Residents)(nil)).Elem(), []string{"ResidentSessions"}},
		{reflect.TypeOf((*lifecycle.Link)(nil)).Elem(), []string{"Close"}},
		{
			reflect.TypeOf((*lifecycle.Session)(nil)).Elem(),
			[]string{"BeginRelease", "Checkpoint", "FinishRelease", "Key", "ReleaseResidency", "WaitIdle"},
		},
	} {
		got := make([]string, test.seam.NumMethod())
		for index := range test.seam.NumMethod() {
			got[index] = test.seam.Method(index).Name
		}
		if !slices.Equal(got, test.methods) {
			t.Fatalf("%s methods = %v, want %v", test.seam, got, test.methods)
		}
	}
}

// ---------------------------------------------------------------------------
// The registry seams this package must not read
// ---------------------------------------------------------------------------

// TestTheDrainReadsNeitherHalfOfTheRegistryEntrySeam holds two standing
// deferrals this task was told not to disturb, structurally rather than by
// recollection.
//
// registry.Entry.Accepting HAS NO READER, and that is a ruled deferral: inside
// internal/registry, Accepting is a strict function of State — Insert sets
// {resident, true} and MarkReleasing and BeginTeardown are the only writers,
// both setting it false alongside a non-resident State — so {resident,
// accepting:false} is unreachable. This drain publishes accepting=false through
// the injected Advertiser, which is the ORCHESTRATION-STORE row and not a
// registry entry, so it does not make that row reachable. A reader added here
// would be the change that reopens it.
//
// registry.Entry.LeaseEpoch is the RESIDENCY epoch and sits beside
// Entry.Runtime.LeaseEpoch(), which is the JOURNAL one: one identifier over two
// domains, booked as O3.4-registry-entry-seam and currently test-defended
// rather than type-defended. A drain has no business reading either, so this
// package adds no third reader to name a domain for.
//
// The check parses this package's PRODUCTION files and fails at zero of them,
// because a guard that reaches nothing reports the same green as one that finds
// nothing wrong.
func TestTheDrainReadsNeitherHalfOfTheRegistryEntrySeam(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the lifecycle package directory: %v", err)
	}
	forbidden := map[string]string{
		"Accepting":  "registry.Entry.Accepting has no reader by a ruled deferral; publishing nonaccepting goes through the Advertiser",
		"LeaseEpoch": "registry.Entry.LeaseEpoch is one identifier over two domains (O3.4-registry-entry-seam); a drain reads neither",
	}
	fileSet := token.NewFileSet()
	var production int
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		production++
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, isSelector := node.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			if reason, banned := forbidden[selector.Sel.Name]; banned {
				t.Errorf("%s:%d reads %q: %s", name, fileSet.Position(selector.Pos()).Line, selector.Sel.Name, reason)
			}
			return true
		})
	}
	if production == 0 {
		t.Fatal("no production files were parsed, so this check proves nothing")
	}
}
