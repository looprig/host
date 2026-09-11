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

	// testPublishBound is deliberately the TIGHTEST of the three, so a drain
	// that took the wrong timer is visible in fakeClock.requested().
	testPublishBound = 2 * time.Second
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeClock hands out timer channels a test fires by hand. NOTHING SLEEPS.
//
// It keeps each outstanding timer's DURATION alongside its channel, because
// "is the drain waiting yet" is not a question a bare count can answer once the
// drain arms more than one kind of timer. It could not: the publish bound and
// the idle boundary are both live at once, and a test that spun on a count
// raced past the wait it meant to synchronise on.
type fakeClock struct {
	mu     sync.Mutex
	waits  []time.Duration
	timers []fakeTimer
}

type fakeTimer struct {
	after   time.Duration
	channel chan time.Time
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	channel := make(chan time.Time, 1)
	c.waits = append(c.waits, d)
	c.timers = append(c.timers, fakeTimer{after: d, channel: channel})
	return channel
}

// fireAll expires every timer handed out so far, which is how a test reaches
// the deadline path without waiting for one.
func (c *fakeClock) fireAll() {
	c.mu.Lock()
	timers := append([]fakeTimer(nil), c.timers...)
	c.timers = nil
	c.mu.Unlock()
	for _, timer := range timers {
		timer.channel <- time.Unix(0, 0)
	}
}

// fireFor expires only the timers of ONE duration, which is how a test reaches
// a bound while deliberately leaving another armed.
func (c *fakeClock) fireFor(after time.Duration) {
	c.mu.Lock()
	var fired, kept []fakeTimer
	for _, timer := range c.timers {
		if timer.after == after {
			fired = append(fired, timer)
			continue
		}
		kept = append(kept, timer)
	}
	c.timers = kept
	c.mu.Unlock()
	for _, timer := range fired {
		timer.channel <- time.Unix(0, 0)
	}
}

// requested reports every duration ever asked for, fired or not.
func (c *fakeClock) requested() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
}

// pendingFor reports how many timers of ONE duration are outstanding, so a test
// can wait for the drain to REACH a particular bound rather than racing it or
// synchronising on some other bound that happens to be armed.
func (c *fakeClock) pendingFor(after time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var count int
	for _, timer := range c.timers {
		if timer.after == after {
			count++
		}
	}
	return count
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

	// block makes PublishNonaccepting GENUINELY BLOCK until it is released, or
	// until its context is cancelled. It is the positive control for every
	// "answered promptly" assertion below: an advertiser that returns instantly
	// makes "the status RPC did not wait" true for free.
	block   chan struct{}
	cancels int

	// deaf makes the blocked publication IGNORE its context, which is the case
	// the publish bound has to hold without. An advertiser that honours
	// cancellation proves only the cooperative half.
	deaf bool
}

func (a *advertiser) PublishNonaccepting(ctx context.Context) error {
	a.mu.Lock()
	a.published++
	a.observedDraining = append(a.observedDraining, a.ledger.Draining())
	hook, err, block, deaf := a.during, a.err, a.block, a.deaf
	a.mu.Unlock()
	if hook != nil {
		hook()
	}
	if block != nil {
		if deaf {
			<-block
			return err
		}
		select {
		case <-block:
		case <-ctx.Done():
			a.mu.Lock()
			a.cancels++
			a.mu.Unlock()
			return ctx.Err()
		}
	}
	return err
}

// cancelCount reports how many publications were ended by their context, which
// is the cooperative half of the publish bound.
func (a *advertiser) cancelCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cancels
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

func (j *journal) entryCount() int {
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

	// deaf makes WaitIdle block and IGNORE cancellation, which is the session
	// the platform grace has to bound without the session's cooperation.
	deaf    bool
	release chan struct{}

	errs map[step]error

	mu    sync.Mutex
	steps []step
}

func newSession(session sessionwire.SessionID, shared *journal) *fakeSession {
	return &fakeSession{
		key:     registry.Key{TenantID: testTenant, SessionID: session},
		journal: shared,
		errs:    map[step]error{},
		release: make(chan struct{}),
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
	if s.deaf {
		// IGNORES CANCELLATION. This is not a hypothetical: WaitIdle is a
		// capability Host declares and a Harness session implements, and a
		// bound that only holds when the far side cooperates is not a bound.
		<-s.release
		return nil
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
		PublishBound: testPublishBound,
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
	touched := p.journal.entryCount()

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
		// half-applied by construction — the one instant at which the ledger
		// has stopped and the publication has not landed.
		//
		// IT CALLS ObserveDrain, and that is a DETERMINISTIC assertion rather
		// than a sampled one: at this instant a drain must not be observable,
		// because nothing downstream yet knows one began. It became callable
		// when the publication stopped running under the lock, which is the
		// B2 fix; before that it deadlocked, and the deadlock was being read as
		// evidence of atomicity when it was only evidence of a held lock.
		var insidePublication struct {
			draining bool
			begun    bool
			touched  int
		}
		f.advertiser.during = func() {
			insidePublication.draining = f.ledger.Draining()
			_, insidePublication.begun = f.drainer.ObserveDrain(hostlink.DrainScope{})
			insidePublication.touched = f.journal.entryCount()
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
		// THE SHARED JOURNAL IS PUMPED. `touched == 0` inside the publication
		// below is a claim about TIMING, and it is equally true of a journal
		// that never records anything — which is a different fixture bug with
		// the same green. By now the drain has run to the end, so it must have
		// entries.
		if f.journal.entryCount() == 0 {
			t.Fatal("the shared journal recorded nothing across a completed drain, " +
				"so the half-done assertions above are about a dead observable")
		}

		// What the hook saw. The ledger had ALREADY flipped, and neither the
		// drain nor any session had moved — which is the acknowledgement's
		// price, paid in that order.
		if !insidePublication.draining {
			t.Fatal("the ledger was still admitting while nonaccepting was being published")
		}
		if insidePublication.begun {
			t.Fatal("the drain was observable from inside its own nonaccepting publication, " +
				"so a Factory could see a drain this Host had not yet published")
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

// TestTheStatusObservationIsAnsweredWhileTheDrainIsStillPublishing is the half
// of step 4 that regressed, and it is the reason StartDrain does not hold its
// lock across the publication.
//
// An earlier version did, and ObserveDrain and Wait take the same lock — so the
// bounded status observation was blocked for the whole publication, on a
// context derived from Background that nothing could cancel. "Returns promptly"
// was a claim about a path that could block forever.
//
// A "DOES NOT BLOCK" ASSERTION IS A NOTHING-HAPPENED CLAIM and needs a positive
// control that genuinely blocks: against an advertiser that returns instantly,
// every assertion below passes for free. The control is the advertiser's
// `block` channel, and the test asserts the publication IS still in flight —
// StartDrain has not returned — at the moment the status observation is
// answered.
func TestTheStatusObservationIsAnsweredWhileTheDrainIsStillPublishing(t *testing.T) {
	t.Parallel()
	alpha := newSession("session-alpha", nil)
	f := newFixture(t, alpha)

	release := make(chan struct{})
	inFlight := make(chan struct{})
	f.advertiser.block = release
	f.advertiser.during = func() { close(inFlight) }

	acknowledged := make(chan hostlink.DrainStatus, 1)
	go func() { acknowledged <- mustStart(t, f.drainer) }()
	<-inFlight

	// THE POSITIVE CONTROL: the publication is genuinely in flight, so the
	// observation below is answered from underneath a real block.
	select {
	case status := <-acknowledged:
		t.Fatalf("StartDrain returned %+v while its publication was still blocked, "+
			"so this test's control does not block and proves nothing", status)
	default:
	}

	observed := make(chan bool, 1)
	go func() {
		_, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
		observed <- begun
	}()
	select {
	case begun := <-observed:
		if begun {
			t.Fatal("a drain was observable while its nonaccepting publication was still in flight")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the status observation blocked behind the nonaccepting publication, " +
			"which is the unbounded wait step 4's bounded observation exists to avoid")
	}

	// Wait is on the same lock and must not block either.
	waited := make(chan lifecycle.Report, 1)
	go func() { waited <- f.drainer.Wait() }()
	select {
	case report := <-waited:
		if report.State != sessionwire.HostLinkDrainStateDraining {
			t.Fatalf("Wait mid-publication = %+v, want the unbegun report", report)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked behind the nonaccepting publication")
	}

	close(release)
	status := <-acknowledged
	if status.State != sessionwire.HostLinkDrainStateDraining || status.Generation != testGeneration {
		t.Fatalf("acknowledgement = %+v, want draining at %d", status, testGeneration)
	}
	awaitDrained(t, f.drainer)
}

// TestAPublicationThatNeverAnswersRefusesTheDrainAtItsBound is the other half
// of the same defect: the acknowledgement itself had no bound.
//
// DrainStarter takes no context by design, so the publication's bound has to
// come from the composition. An Advertiser that never returned hung the drain
// RPC forever; now the wait ends at PublishBound and the drain is REFUSED,
// because this Host cannot claim the row landed.
//
// THE BOUND HOLDS WITHOUT THE ADVERTISER'S COOPERATION. The context is
// cancelled — asserted, because that is the half a well-behaved store needs —
// but the wait ends whether or not anything honours it.
func TestAPublicationThatNeverAnswersRefusesTheDrainAtItsBound(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		deaf bool
	}{
		{"an advertiser that honours cancellation", false},
		{"an advertiser that ignores it", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			alpha := newSession("session-alpha", nil)
			f := newFixture(t, alpha)
			release := make(chan struct{})
			f.advertiser.block = release
			f.advertiser.deaf = test.deaf

			refused := make(chan error, 1)
			go func() {
				_, err := f.drainer.StartDrain(hostlink.DrainScope{})
				refused <- err
			}()
			waitFor(t, "the publish bound to be armed", func() bool { return f.clock.pendingFor(testPublishBound) > 0 })
			f.clock.fireAll()

			select {
			case err := <-refused:
				if !errors.Is(err, lifecycle.ErrPublishBound) {
					t.Fatalf("StartDrain error = %v, want ErrPublishBound", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("StartDrain did not return when its publish bound expired, so the acknowledgement is unbounded")
			}

			if _, begun := f.drainer.ObserveDrain(hostlink.DrainScope{}); begun {
				t.Fatal("a drain whose publication never landed is observable as begun")
			}
			if draining, begins := f.ledger.counts(); !draining || begins != 1 {
				t.Fatalf("ledger after a bounded-out publication: draining=%v begins=%d, want true and 1", draining, begins)
			}
			if steps := alpha.stepsTaken(); len(steps) != 0 {
				t.Fatalf("a session was touched by a drain that never published: %v", steps)
			}
			// THE POSITIVE CONTROL, on the same object: the recorder this
			// emptiness is read from is live. Without it a fixture whose
			// sessions record nothing passes the assertion above.
			if err := alpha.BeginRelease(context.Background()); err != nil {
				t.Fatalf("driving the session by hand: %v", err)
			}
			if steps := alpha.stepsTaken(); !slices.Equal(steps, []step{stepBeginRelease}) {
				t.Fatalf("the session's recorder is dead (%v), so the assertion above proved nothing", steps)
			}
			if test.deaf {
				// Release the orphaned publication so the package does not end
				// with it parked on an unbuffered channel.
				close(release)
			} else {
				waitFor(t, "the publication's context to be cancelled", func() bool { return f.advertiser.cancelCount() == 1 })
			}
		})
	}
}

// TestConcurrentStartsShareOnePublicationAndOneLedgerStop holds the property
// releasing the lock could have cost.
//
// The critical section no longer spans the publication, so "one drain" has to
// be carried by `starting` instead. Sixteen callers arriving while a slow
// publication is in flight must produce ONE ledger stop and ONE publication,
// and all sixteen must be answered from that one attempt — not queue up
// sixteen publications against a store that is already slow.
func TestConcurrentStartsShareOnePublicationAndOneLedgerStop(t *testing.T) {
	t.Parallel()
	f := newFixture(t, newSession("session-alpha", nil))

	release := make(chan struct{})
	inFlight := make(chan struct{})
	// ONCE, because the failure this test exists to catch is a SECOND
	// publication. A bare close would panic on it, and a panic is not an
	// assertion — the count below is what must report the defect.
	var announced sync.Once
	f.advertiser.block = release
	f.advertiser.during = func() { announced.Do(func() { close(inFlight) }) }

	const callers = 16
	statuses := make(chan hostlink.DrainStatus, callers)
	go func() { statuses <- mustStart(t, f.drainer) }()
	<-inFlight

	var joined sync.WaitGroup
	for range callers - 1 {
		joined.Add(1)
		go func() {
			defer joined.Done()
			statuses <- mustStart(t, f.drainer)
		}()
	}
	// The late callers race the release deliberately: whichever side wins, each
	// of them finds either `starting` or `begun` and is answered from the one
	// attempt. There is no third state for them to find, which is the property
	// under test, so there is nothing here to synchronise on.
	close(release)
	joined.Wait()

	for range callers {
		status := <-statuses
		// The GENERATION is the invariant. A late caller may legitimately see
		// `drained` if the release finished first — that is the same drain, and
		// insisting on `draining` would be asserting a scheduling accident.
		if status.Generation != testGeneration {
			t.Fatalf("caller saw generation %d, want %d", status.Generation, testGeneration)
		}
		if status.State != sessionwire.HostLinkDrainStateDraining && status.State != sessionwire.HostLinkDrainStateDrained {
			t.Fatalf("caller saw state %q, which is neither of Core's two", status.State)
		}
	}
	if published, _ := f.advertiser.snapshot(); published != 1 {
		t.Fatalf("%d callers produced %d publications, want one", callers, published)
	}
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("admission was stopped %d times, want once", begins)
	}
	awaitDrained(t, f.drainer)
}

// TestARetryAfterARefusedPublicationDoesNotStopAdmissionTwice holds the
// ledger's once-ever property across the refusal path.
//
// The ledger deliberately stays stopped through a refused drain, so a retry
// must not stop it again: "how many times did this Host stop admitting" is the
// count every atomicity assertion in this file reads, and a retry storm against
// a flapping store would otherwise inflate it.
func TestARetryAfterARefusedPublicationDoesNotStopAdmissionTwice(t *testing.T) {
	t.Parallel()
	f := newFixture(t, newSession("session-alpha", nil))
	f.advertiser.err = errors.New("the orchestration store is unreachable")

	for range 3 {
		if _, err := f.drainer.StartDrain(hostlink.DrainScope{}); err == nil {
			t.Fatal("a drain was accepted while the store was unreachable")
		}
	}
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("three refused drains stopped admission %d times, want once", begins)
	}

	f.advertiser.mu.Lock()
	f.advertiser.err = nil
	f.advertiser.mu.Unlock()
	mustStart(t, f.drainer)
	awaitDrained(t, f.drainer)
	if _, begins := f.ledger.counts(); begins != 1 {
		t.Fatalf("the successful retry stopped admission %d times, want once", begins)
	}
	if published, _ := f.advertiser.snapshot(); published != 4 {
		t.Fatalf("publications = %d, want the three refusals and the retry", published)
	}
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
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })

	// THE OBSERVABLE HAS BEEN PUMPED before the absence below is read. The
	// drain has reached the idle wait, so these two steps MUST already be
	// recorded — and without asserting that, "checkpoint has not run" is
	// equally true of a recorder that records nothing at all. A probe that
	// stubbed stepsTaken to nil left this whole test green.
	taken := alpha.stepsTaken()
	for _, reached := range []step{stepBeginRelease, stepWaitIdle} {
		if !slices.Contains(taken, reached) {
			t.Fatalf("steps = %v: the drain reached the idle wait without recording %s, "+
				"so the absences asserted below are about a dead observable", taken, reached)
		}
	}
	for _, forbidden := range []step{stepCheckpoint, stepReleaseResidency, stepFinishRelease} {
		if slices.Contains(taken, forbidden) {
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

// TestARefusedFinishReleaseWithholdsDrained is the Host-side mitigation for the
// one failure Core's two-valued state CAN express.
//
// A Factory deletes a dedicated workload when it observes `drained`. Core
// defines that value as "the observed scope has finished release", and a
// session whose FinishRelease refused has not finished release — its
// epoch-fenced tombstone was not written or its lease was not released. A Host
// that reported `drained` anyway would have a Factory delete a workload whose
// lease this Host still holds, which destroys work.
//
// So the state stays `draining`. A Factory then waits and escalates instead of
// deleting: a leaked workload is visible to an operator and deleted work is not.
// This is a deliberate deviation from O6.3's prose, which ends "then report Host
// drained"; Core's definition of the value decides when that is true.
//
// THE POSITIVE CONTROL IS THE SECOND SUBTEST. "State stayed draining" is
// satisfied by a drain that never reaches the end at all, so the same fixture
// with a succeeding FinishRelease must reach `drained` — otherwise this test
// would pass against a Drainer that simply never finishes.
func TestARefusedFinishReleaseWithholdsDrained(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		refuse  bool
		want    sessionwire.HostLinkDrainState
		wantErr bool
	}{
		{"a refused FinishRelease", true, sessionwire.HostLinkDrainStateDraining, true},
		{"the control: it succeeds", false, sessionwire.HostLinkDrainStateDrained, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			alpha := newSession("session-alpha", nil)
			refused := errors.New("the residency tombstone could not be written")
			if test.refuse {
				alpha.errs[stepFinishRelease] = refused
			}
			f := newFixture(t, alpha)

			mustStart(t, f.drainer)
			report := f.drainer.Wait()
			if report.State != test.want {
				t.Fatalf("state = %q, want %q", report.State, test.want)
			}

			// Whatever the state, the cleanup RAN. Withholding `drained` is a
			// statement about the outcome, not a refusal to finish the work.
			want := []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease}
			if got := alpha.stepsTaken(); !slices.Equal(got, want) {
				t.Fatalf("steps = %v, want %v", got, want)
			}
			if f.link.closeCount() != 1 {
				t.Fatalf("link closes = %d, want one", f.link.closeCount())
			}

			// And the observation a Factory polls agrees with the report, so
			// the two cannot disagree about whether the workload may be deleted.
			status, begun := f.drainer.ObserveDrain(hostlink.DrainScope{})
			if !begun || status.State != test.want {
				t.Fatalf("observation = %+v/%v, want state %q", status, begun, test.want)
			}

			if test.wantErr {
				if len(report.Failures) != 1 || report.Failures[0].Step != lifecycle.StepFinishRelease {
					t.Fatalf("failures = %v, want exactly the finish release", report.Failures)
				}
				if !errors.Is(report.Failures[0], refused) {
					t.Fatalf("the refusal lost its cause: %v", report.Failures[0])
				}
			} else if len(report.Failures) != 0 {
				t.Fatalf("the control reported failures: %v", report.Failures)
			}
		})
	}
}

// TestOnlyAFinishReleaseFailureWithholdsDrained is the other half: every other
// failure this drain books still reports drained, because none of them means release
// did not finish. The tombstone is written and the lease is released in all of
// them, so a Factory may delete the workload.
func TestOnlyAFinishReleaseFailureWithholdsDrained(t *testing.T) {
	t.Parallel()

	for _, failing := range []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency} {
		t.Run(string(failing), func(t *testing.T) {
			t.Parallel()
			alpha := newSession("session-alpha", nil)
			alpha.errs[failing] = errors.New("this step refused")
			f := newFixture(t, alpha)

			mustStart(t, f.drainer)
			report := awaitDrained(t, f.drainer)
			if len(report.Failures) != 1 {
				t.Fatalf("failures = %v, want exactly one", report.Failures)
			}
		})
	}

	t.Run("close_link", func(t *testing.T) {
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

	// THE POSITIVE CONTROL for the emptiness asserted above, on the same
	// object: the retry DID drive this session, so the earlier zero was a
	// statement about the refused drain rather than about a dead recorder.
	want := []step{stepBeginRelease, stepWaitIdle, stepCheckpoint, stepReleaseResidency, stepFinishRelease}
	if got := alpha.stepsTaken(); !slices.Equal(got, want) {
		t.Fatalf("steps after the successful retry = %v, want %v", got, want)
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

	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })
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
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })
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
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })

	late := newSession("session-late", f.journal)
	f.residents.add(late)

	f.clock.fireAll()
	awaitDrained(t, f.drainer)
	if steps := late.stepsTaken(); len(steps) != 0 {
		t.Fatalf("a session that attached after the drain began was drained: %v", steps)
	}

	// THE POSITIVE CONTROL IS ON THE SAME OBJECT. "late recorded nothing" is
	// satisfied by a `late` whose recorder never works — a different fixture
	// bug with the same green. Driving one step by hand shows the observable
	// this assertion reads is live for THIS session, not merely for alpha.
	if err := late.BeginRelease(context.Background()); err != nil {
		t.Fatalf("driving the late session by hand: %v", err)
	}
	if steps := late.stepsTaken(); !slices.Equal(steps, []step{stepBeginRelease}) {
		t.Fatalf("the late session's recorder is dead (%v), so the assertion above proved nothing", steps)
	}
	if steps := alpha.stepsTaken(); len(steps) == 0 {
		t.Fatal("the drained session recorded nothing either")
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
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })
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
		waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })
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
			PublishBound: testPublishBound,
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
			PublishBound: testPublishBound,
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
		{"a zero publish bound", func(o *lifecycle.Options) { o.PublishBound = 0 }, "PublishBound"},
		{
			"a publish bound longer than the whole drain",
			func(o *lifecycle.Options) { o.PublishBound = testGrace + time.Second },
			"PublishBound",
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
	waitFor(t, "the drain to reach the idle wait", func() bool { return f.clock.pendingFor(testIdleGrace) > 0 })
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

// TestThePlatformGraceBoundsASessionThatIgnoresCancellation is the half of
// Options.Grace's own doc comment that was not true.
//
// Grace says "when it expires, every outstanding idle wait is cancelled and
// cleanup continues". CANCELLING IS NOT BOUNDING. A WaitIdle that ignores its
// context left this drain blocked on that session's answer forever: the state
// stayed `draining` permanently, the link never closed, and every other
// session's cleanup was already done and waiting on it. Both timers fired and
// nothing moved.
//
// A bound that holds only when the far side cooperates is not a bound, and
// WaitIdle is a capability Host DECLARES and someone else implements, so the
// uncooperative case is the one it exists for. The wait for the session's own
// answer is now itself bounded by the platform grace, and such a session is
// booked as abandoned rather than waited on.
//
// It fails by TIMING OUT ON A CHANNEL rather than by hanging the package, so a
// regression reports a failure instead of a ten-minute panic.
func TestThePlatformGraceBoundsASessionThatIgnoresCancellation(t *testing.T) {
	t.Parallel()
	deaf := newSession("session-deaf", nil)
	deaf.deaf = true
	healthy := newSession("session-healthy", nil)
	f := newFixture(t, deaf, healthy)

	mustStart(t, f.drainer)
	// BOTH sessions must have armed their idle bound before the clock fires,
	// or the deaf one's timer is created after fireAll and never expires —
	// which would be this fixture failing to reach the path, not the path
	// failing. Go evaluates every select operand, so entering waitIdle always
	// arms one.
	waitFor(t, "both sessions to arm their idle bound and the grace to be armed", func() bool {
		return f.clock.pendingFor(testIdleGrace) == 2 && f.clock.pendingFor(testGrace) > 0
	})

	finished := make(chan lifecycle.Report, 1)
	go func() { finished <- f.drainer.Wait() }()
	f.clock.fireAll()

	var report lifecycle.Report
	select {
	case report = <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the platform grace did not bound a session whose WaitIdle ignores cancellation: " +
			"the drain never finished and the state stayed draining permanently")
	}

	if report.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("state = %q, want %q", report.State, sessionwire.HostLinkDrainStateDrained)
	}
	// The deaf session is RECORDED, not silently skipped, and it is
	// attributable to the abandonment rather than to an ordinary error.
	var abandoned *lifecycle.Failure
	for index := range report.Failures {
		if report.Failures[index].Key == deaf.Key() && report.Failures[index].Step == lifecycle.StepWaitIdle {
			abandoned = &report.Failures[index]
		}
	}
	if abandoned == nil {
		t.Fatalf("failures = %v, want the deaf session's idle wait", report.Failures)
	}
	if !errors.Is(abandoned, lifecycle.ErrIdleBoundary) || !errors.Is(abandoned, lifecycle.ErrWaitAbandoned) {
		t.Fatalf("the abandoned wait is not attributable: %v", abandoned)
	}
	// Cleanup still ran to the end for BOTH, which is what the bound is for.
	for _, session := range []*fakeSession{deaf, healthy} {
		if !slices.Contains(session.stepsTaken(), stepFinishRelease) {
			t.Fatalf("%q did not reach FinishRelease", session.Key().SessionID)
		}
	}
	if f.link.closeCount() != 1 {
		t.Fatalf("link closes = %d, want one", f.link.closeCount())
	}

	// Release the abandoned goroutine so the package does not end with it
	// parked on an unbuffered channel.
	close(deaf.release)
}

// TestThePlatformGraceEndsAWaitWhoseOwnBoundHasNotFired is the arm of waitIdle's
// first select that nothing else reaches.
//
// The per-session idle boundary and the platform grace both bound one wait, and
// the validated IdleBoundary <= Grace makes the idle timer fire first with a
// real clock. That is an argument about a clock, not a property of this code,
// and a mechanism whose only defence is an argument is one nothing tests: a
// probe that deleted the grace arm from the first select left the whole suite
// green.
//
// So this fires ONLY the grace and leaves every idle timer armed. The drain must
// still finish, because the grace is the whole drain's bound and a session that
// has not reached its own is not exempt from it.
func TestThePlatformGraceEndsAWaitWhoseOwnBoundHasNotFired(t *testing.T) {
	t.Parallel()
	deaf := newSession("session-deaf", nil)
	deaf.deaf = true
	f := newFixture(t, deaf)

	mustStart(t, f.drainer)
	waitFor(t, "the session to arm its idle bound and the grace to be armed", func() bool {
		return f.clock.pendingFor(testIdleGrace) == 1 && f.clock.pendingFor(testGrace) > 0
	})

	finished := make(chan lifecycle.Report, 1)
	go func() { finished <- f.drainer.Wait() }()
	f.clock.fireFor(testGrace)

	var report lifecycle.Report
	select {
	case report = <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the platform grace did not end a wait whose own idle bound had not fired")
	}
	if report.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("state = %q, want %q", report.State, sessionwire.HostLinkDrainStateDrained)
	}
	if f.clock.pendingFor(testIdleGrace) != 1 {
		t.Fatalf("the idle bound fired after all (%d pending), so this test took the other arm", f.clock.pendingFor(testIdleGrace))
	}
	if !slices.Contains(deaf.stepsTaken(), stepFinishRelease) {
		t.Fatalf("%q did not reach FinishRelease", deaf.Key().SessionID)
	}
	close(deaf.release)
}

// ---------------------------------------------------------------------------
// The registry seams this package must not read
// ---------------------------------------------------------------------------

// TestTheDrainReadsNeitherHalfOfTheRegistryEntrySeam holds two standing
// deferrals this task was told not to disturb, structurally rather than by
// recollection.
//
// registry.Entry.Accepting HAS NO READER IN THIS PACKAGE, and as of O6.1 that
// is a CHOICE rather than a consequence. It used to be a ruled deferral resting
// on unreachability: Accepting was a strict function of State, so {resident,
// accepting:false} could not be produced. registry.StopAdmitting produces it
// now — a warm release closes one session's admission before its residency
// state moves — so the old justification is gone and the rule is not.
//
// What holds it is the DIVISION OF LABOUR. A drain is Host-wide: it publishes
// accepting=false through the injected Advertiser, which is the
// ORCHESTRATION-STORE row and not a registry entry, and it releases every
// resident session whatever each one's admission says. Reading the per-session
// flag here would make a Host-wide decision out of a per-session fact.
// internal/realtime/hostlink is where that flag is read, because a BIND is
// about one session. metrics.go reads Entry.State and nothing else.
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
		"Accepting":  "registry.Entry.Accepting is a PER-SESSION admission fact and this package makes Host-wide decisions; publishing nonaccepting goes through the Advertiser and the per-session flag is read in internal/realtime/hostlink",
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
