package residency

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

// WarmTimer is one session's warm countdown, narrowed to what an arming needs.
//
// IT IS DECLARED HERE RATHER THAN REUSING host.Clock, whose NewTimer returns a
// CONCRETE *time.Timer. A concrete timer cannot be made to expire by a test
// without sleeping, and the runbook's step 1 for this task is "use an injected
// fake clock; no test sleeps". The three methods are time.Timer's own, so a
// production adapter is a two-line wrapper rather than a reimplementation.
type WarmTimer interface {
	// C is the expiry channel.
	C() <-chan time.Time

	// Stop cancels an arming.
	Stop() bool

	// Reset arms the timer for a fresh duration.
	Reset(time.Duration) bool
}

// WarmClock hands out warm timers.
type WarmClock interface {
	NewWarmTimer(time.Duration) WarmTimer
}

// ---------------------------------------------------------------------------
// What a session is doing
// ---------------------------------------------------------------------------

// WorkState is what an observer reports one session to be doing, and it is a
// THREE-VALUED type rather than a boolean on purpose.
//
// 04-host.md: "A waiting resident gate is not whole-session idle and therefore
// is not warm-evicted by this plan." A boolean idle signal makes that a rule
// about every caller, and the one caller that reports a gate wait as idle
// releases a session with a human waiting on it. Naming the gate wait puts the
// rule in the releaser, where it is enforced once.
type WorkState string

const (
	// WorkStateWorking reports work in flight. It cancels any warm arming.
	WorkStateWorking WorkState = "working"

	// WorkStateGateWaiting reports a resident gate waiting on a response. It is
	// NOT idle: §9.4's phase-one implementation pins the session for a bounded
	// TTL and the continuation that would let it be released is a later plan.
	WorkStateGateWaiting WorkState = "gate_waiting"

	// WorkStateIdle reports durable whole-session idle, which is the only
	// observation that arms a warm release.
	WorkStateIdle WorkState = "idle"
)

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------

// WarmRegistry is the local residency index, narrowed to the one write a warm
// release makes to it directly.
//
// Everything else this package does to the registry goes through the session's
// own release halves, which is what keeps ONE writer per durable record. This
// one is separate because it is the per-session admission stop, and it must
// happen BEFORE the residency state moves: see StopAdmitting's own doc, and
// WarmReleaser.release step 2.
type WarmRegistry interface {
	StopAdmitting(registry.Key, uint64) (registry.Entry, bool)
}

// The concrete registry satisfies the seam. A drift is a compile failure.
var _ WarmRegistry = (*registry.Registry)(nil)

// DurableInbox is the durable command inbox, narrowed to the re-read step 1
// makes.
//
// IT IS A DURABLE READ AND NOT A LOCAL ONE. The point of step 1 is that a
// command accepted by ANOTHER component since this session went idle is
// visible; a local queue depth would report only what this process has already
// seen, which is exactly the work a warm release does not need to worry about.
type DurableInbox interface {
	// AcceptedWork reports whether the durable inbox holds work accepted for
	// this session and not yet terminal.
	AcceptedWork(context.Context, registry.Key) (bool, error)
}

// Consumption is the durable command consumer for a session, narrowed to the
// wake an aborted release owes it.
//
// THE WAKE IS NOT OPTIONAL AND IT IS EASY TO OMIT. A consumer that went quiet
// because the session was idle has no reason to look again on its own, so an
// abort that merely returned would leave accepted durable work sitting in the
// inbox of a session that is resident, accepting, and doing nothing. The
// releaser is the only thing that knows the session stopped being a release
// candidate.
type Consumption interface {
	// Wake resumes consumption for a session. It is a wake and not a delivery,
	// so repeating it is harmless.
	Wake(registry.Key)
}

// WarmSession is one resident session, narrowed to the ordered steps a warm
// release takes it through.
//
// THE ORDER IS §9.3'S AND IT IS THE REASON THIS IS SIX METHODS RATHER THAN ONE.
// A single Release(ctx) would hide the properties O6.1 exists to establish —
// that the durable `releasing` mark precedes the checkpoint, that the tombstone
// precedes the lease release, and that local state is dropped last — behind an
// implementation this package could not test. residency.Heartbeat already
// offers BeginRelease and FinishRelease, split for exactly this caller.
//
// THE PRODUCTION ADAPTER IS NOT THIS TASK'S, and the seam is stated rather than
// invented: no production lifecycle.Session or Advertiser adapter exists in
// this module — both are O7.1's — and department.Runtime identifies a runtime
// with sessionwire identities while Harness identifies a session with a UUID.
// O7.1 composes a Heartbeat, a Lease and a department.Runtime into this shape.
type WarmSession interface {
	// Key identifies the session.
	Key() registry.Key

	// Generation is the registry generation this residency was installed under.
	// Every step is fenced by it, so a warm timer armed for one residency
	// cannot act on the residency that REPLACED it under the same key.
	Generation() uint64

	// Lost closes when this Host's residency grant is gone. The warm path then
	// abandons the session: releasing under a lost grant would write fenced
	// records under an epoch a successor has superseded, and Heartbeat.surrender
	// already owns the handoff for that case.
	Lost() <-chan struct{}

	// BeginRelease marks the residency releasing and publishes that state
	// durably, without terminating it.
	BeginRelease(context.Context) error

	// Checkpoint commits the runtime and workspace checkpoints the release
	// requires.
	Checkpoint(context.Context) error

	// ReleaseResidency closes process-local resources. It is NONTERMINAL: no
	// terminal event is appended and the session stays durable and resumable.
	ReleaseResidency(context.Context) error

	// FinishRelease writes the epoch-fenced tombstone and removes the visible
	// Host registry entry.
	FinishRelease(context.Context) error

	// ReleaseLease drops the residency grant.
	ReleaseLease(context.Context) error

	// DropState drops in-memory state and disposable local materializations.
	DropState(context.Context) error
}

// WarmObserver is handed the outcome of every warm release attempt.
//
// It is OPTIONAL. A Host composed without one still releases; what it loses is
// the only record of a recorded failure, which is why internal/lifecycle's
// metrics consume this.
type WarmObserver interface {
	WarmRelease(WarmOutcome)
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

// WarmStep names one thing a warm release does, so a failure says which.
type WarmStep string

const (
	WarmStepInbox            WarmStep = "inbox_reread"
	WarmStepStopAdmitting    WarmStep = "stop_admitting"
	WarmStepBeginRelease     WarmStep = "begin_release"
	WarmStepCheckpoint       WarmStep = "checkpoint"
	WarmStepReleaseResidency WarmStep = "release_residency"
	WarmStepFinishRelease    WarmStep = "finish_release"
	WarmStepReleaseLease     WarmStep = "release_lease"
	WarmStepDropState        WarmStep = "drop_state"
)

// WarmFailure is one step that did not succeed.
type WarmFailure struct {
	Step WarmStep
	Err  error
}

func (f WarmFailure) Error() string {
	return "residency: warm release failed at " + string(f.Step) + ": " + f.Err.Error()
}

func (f WarmFailure) Unwrap() error { return f.Err }

// WarmOutcomeKind is how one warm release attempt ended.
type WarmOutcomeKind string

const (
	// WarmOutcomeReleased reports a release that ran to the end. It may still
	// carry Failures: see WarmReleaser.release for why a failure after step 2
	// is recorded rather than returned.
	WarmOutcomeReleased WarmOutcomeKind = "released"

	// WarmOutcomeAborted reports an attempt that stopped at step 1, before
	// anything was taken. The session stays resident and accepting and is
	// released on a later idle observation.
	WarmOutcomeAborted WarmOutcomeKind = "aborted"

	// WarmOutcomeAbandoned reports a session this releaser has stopped
	// watching without releasing it, because it no longer owns it: the grant
	// is gone, or the residency has been replaced under the same key. Somebody
	// else owns what happens next — Heartbeat's teardown handoff, or the
	// replacement's own releaser.
	WarmOutcomeAbandoned WarmOutcomeKind = "abandoned"
)

// WarmOutcome is one warm release attempt's result.
type WarmOutcome struct {
	Key        registry.Key
	Generation uint64
	Kind       WarmOutcomeKind

	// Reason is free text for an operator. A caller branches on Kind and on
	// the Failures' Steps, never on this.
	Reason string

	// Failures are every step that did not succeed, in the order they ran.
	Failures []WarmFailure
}

// ---------------------------------------------------------------------------
// Options and construction
// ---------------------------------------------------------------------------

// WarmOptions configures this Host's warm release.
type WarmOptions struct {
	Clock WarmClock

	// TTL is how long a session stays warm after durable whole-session idle.
	//
	// IT MUST BE POSITIVE. host.Options already refuses a non-positive WarmTTL,
	// so accepting zero here would define a behaviour for a configuration no
	// Host can be constructed with. A deployment that wants no warm window
	// does not watch the session.
	TTL time.Duration

	Registry    WarmRegistry
	Inbox       DurableInbox
	Consumption Consumption

	// Admissions is the Host's ONE admission ledger. The warm release credits a
	// released session's weight back through it; keeping a second count here
	// would be the over-admission bug internal/service's own documentation
	// names this task as a required consumer to avoid.
	Admissions Admissions

	// Observer receives every attempt's outcome. It is optional.
	Observer WarmObserver
}

// WarmReleaser releases idle sessions after their warm TTL, and is safe for
// concurrent use.
//
// ONE WATCH PER KEY, ONE GOROUTINE PER WATCH. Two timers for one session is a
// release racing itself, so a second Watch is refused rather than replacing the
// first silently.
type WarmReleaser struct {
	options WarmOptions

	stopOnce sync.Once
	stopped  chan struct{}

	mu      sync.Mutex
	watches map[registry.Key]*warmWatch
	wait    sync.WaitGroup
}

// warmWatch is one session's arming state.
//
// armed AND releasing are read and written under this mutex by BOTH Observe and
// the watch goroutine, which is what makes an arming cancelled by Observe
// unable to be acted on by a tick already in flight. The goroutine re-reads
// armed after taking a tick and discards it if the arming was cancelled in the
// meantime, so correctness does not depend on Stop winning a race with an
// expiry that has already been delivered.
type warmWatch struct {
	session WarmSession
	timer   WarmTimer

	// done ends this watch's goroutine. Forget and Stop both close it, and
	// they must: disarming a timer stops the countdown and leaves the
	// goroutine parked on a channel nobody will ever write to, which is a
	// leak per forgotten session for the life of the process. A stopped
	// releaser waits for these, so "Stop returned" means the goroutines ended.
	stopOnce sync.Once
	done     chan struct{}

	mu        sync.Mutex
	armed     bool
	releasing bool
}

// ErrWarmSessionWatched reports a second Watch for a key already watched.
var ErrWarmSessionWatched = errors.New("residency: this session is already watched for warm release")

// NewWarmReleaser validates the options and returns a releaser watching
// nothing.
func NewWarmReleaser(options WarmOptions) (*WarmReleaser, error) {
	for _, required := range []struct {
		field   string
		present bool
		reason  string
	}{
		{"Clock", options.Clock != nil, "is required; a warm TTL measured against the wall clock is untestable and a release nothing can trigger is not a release"},
		{"Registry", options.Registry != nil, "is required; stopping admission for one session is the step a racing command is refused by"},
		{"Inbox", options.Inbox != nil, "is required; step 1 is a durable re-read and a release that skipped it would release a session holding accepted work"},
		{"Consumption", options.Consumption != nil, "is required; an aborted release owes a wake, and nothing else knows the session stopped being a release candidate"},
		{"Admissions", options.Admissions != nil, "is required; a release that does not credit the ledger leaks this Host's capacity one session at a time"},
	} {
		if !required.present {
			return nil, &InvalidManagerOptionsError{Field: required.field, Reason: required.reason}
		}
	}
	if options.TTL <= 0 {
		return nil, &InvalidManagerOptionsError{
			Field:  "TTL",
			Reason: "must be positive; host.Options already refuses a non-positive WarmTTL, so a zero here would define a behaviour no Host can be constructed with. A deployment that wants no warm window does not watch the session",
		}
	}
	return &WarmReleaser{
		options: options,
		stopped: make(chan struct{}),
		watches: map[registry.Key]*warmWatch{},
	}, nil
}

// Watch begins watching a resident session. It arms NOTHING: only a durable
// whole-session idle observation starts a countdown.
func (w *WarmReleaser) Watch(session WarmSession) error {
	key := session.Key()
	timer := w.options.Clock.NewWarmTimer(w.options.TTL)
	// A freshly created timer is running. Stop it: Watch is not an idle
	// observation, and a session that has never been observed idle must not be
	// counting down to release.
	timer.Stop()
	watch := &warmWatch{session: session, timer: timer, done: make(chan struct{})}

	w.mu.Lock()
	if _, held := w.watches[key]; held {
		w.mu.Unlock()
		return ErrWarmSessionWatched
	}
	select {
	case <-w.stopped:
		w.mu.Unlock()
		return errors.New("residency: this warm releaser has been stopped")
	default:
	}
	w.watches[key] = watch
	w.wait.Add(1)
	w.mu.Unlock()

	go w.run(watch)
	return nil
}

// Forget stops watching a session WITHOUT releasing it. It is what a caller
// uses when the residency has gone by another path — a teardown, a drain, an
// attach that replaced it — and repeating it is safe.
func (w *WarmReleaser) Forget(key registry.Key) {
	w.mu.Lock()
	watch, held := w.watches[key]
	delete(w.watches, key)
	w.mu.Unlock()
	if held {
		watch.stop()
	}
}

// Observe records what a session is doing, arming or cancelling its warm
// countdown.
//
// IT DOES THE WORK SYNCHRONOUSLY rather than posting to the watch goroutine.
// An asynchronous signal would make "the timer was cancelled" a fact a caller
// cannot rely on having taken effect when Observe returns, which is exactly
// the guarantee an activity reset needs: work has arrived, and the release must
// not fire.
func (w *WarmReleaser) Observe(key registry.Key, state WorkState) {
	w.mu.Lock()
	watch, held := w.watches[key]
	w.mu.Unlock()
	if !held {
		return
	}

	watch.mu.Lock()
	defer watch.mu.Unlock()
	if watch.releasing {
		// The release is already past step 1 and cannot be taken back. See
		// release: there is no ResumeAdmitting and no MarkResident.
		return
	}
	if state == WorkStateIdle {
		watch.armed = true
		watch.timer.Reset(w.options.TTL)
		return
	}
	// EVERY OTHER STATE CANCELS, including one this type does not yet name.
	// Listing the idle case and defaulting the rest to "not idle" is what makes
	// a future WorkState fail safe rather than warm-evict a session nobody
	// meant to release.
	watch.armed = false
	watch.timer.Stop()
}

// Stop ends every watch and waits for the goroutines. A release already past
// step 1 is allowed to finish: stopping halfway is the one thing worse than not
// starting.
func (w *WarmReleaser) Stop() {
	w.stopOnce.Do(func() { close(w.stopped) })
	w.mu.Lock()
	for key, watch := range w.watches {
		delete(w.watches, key)
		watch.stop()
	}
	w.mu.Unlock()
	w.wait.Wait()
}

// stop disarms a watch and ends its goroutine. It releases nothing: see
// WarmReleaser.Forget.
//
// BOTH HALVES MATTER AND ARE SEPARATELY OBSERVABLE. Disarming without ending
// the goroutine leaks one goroutine per forgotten session; ending it without
// disarming leaves an armed watch whose tick could still be taken in the window
// before the goroutine notices, which is a release of a session the caller has
// said this releaser no longer owns.
func (watch *warmWatch) stop() {
	watch.mu.Lock()
	watch.armed = false
	watch.timer.Stop()
	watch.mu.Unlock()
	watch.stopOnce.Do(func() { close(watch.done) })
}

// run is one session's watch.
func (w *WarmReleaser) run(watch *warmWatch) {
	defer w.wait.Done()
	session := watch.session
	for {
		select {
		case <-w.stopped:
			return
		case <-watch.done:
			// Forget. The caller owns this residency by another path now, and
			// nothing here has written to it.
			return
		case <-session.Lost():
			w.finish(watch, WarmOutcome{
				Key:        session.Key(),
				Generation: session.Generation(),
				Kind:       WarmOutcomeAbandoned,
				Reason:     "this Host's residency grant for the session is gone, so the warm path wrote nothing and the teardown owner releases it",
			})
			return
		case <-watch.timer.C():
			// THE ARMING IS RE-READ UNDER THE LOCK. A tick already in flight
			// when Observe cancelled the arming is discarded here, so the
			// cancellation does not have to win a race with a delivery that
			// has already happened.
			watch.mu.Lock()
			armed := watch.armed
			if armed {
				watch.armed = false
				watch.releasing = true
			}
			watch.mu.Unlock()
			if !armed {
				continue
			}

			outcome := w.release(session)

			if outcome.Kind == WarmOutcomeAborted {
				// The session stays resident and is released on a later idle
				// observation. "Abort THIS release attempt" is the runbook's
				// wording; a releaser that stopped watching would leave the
				// session resident forever, which is the leak the warm TTL
				// exists to prevent.
				watch.mu.Lock()
				watch.releasing = false
				watch.mu.Unlock()
				w.report(outcome)
				continue
			}
			w.finish(watch, outcome)
			return
		}
	}
}

// finish drops the watch and reports the outcome.
func (w *WarmReleaser) finish(watch *warmWatch, outcome WarmOutcome) {
	w.mu.Lock()
	if held, watching := w.watches[outcome.Key]; watching && held == watch {
		// KEYED ON IDENTITY, not on the key alone. A Forget followed by a
		// Watch for the same session installs a NEW watch, and a release
		// finishing after that would otherwise delete its successor's entry
		// and leave a live session unwatched.
		delete(w.watches, outcome.Key)
	}
	w.mu.Unlock()
	watch.stopOnce.Do(func() { close(watch.done) })
	w.report(outcome)
}

// report hands one attempt's outcome to the observer, if there is one.
func (w *WarmReleaser) report(outcome WarmOutcome) {
	if w.options.Observer != nil {
		w.options.Observer.WarmRelease(outcome)
	}
}

// release runs 04-host.md's O6.1 algorithm for one session.
//
// THE CONTEXT IS Background AND NOT THE RELEASER'S LIFETIME. Cancelling a
// release halfway leaves a live route with no owner, a lease nobody will renew,
// and a Host that believes it still holds a session it has stopped serving —
// which is strictly worse than any of the individual steps failing.
// internal/lifecycle's drain makes the same choice for the same reason.
//
// THE FAILURE POLICY CHANGES AT STEP 2, deliberately:
//
//   - BEFORE it, a failure ABORTS. Nothing has been taken, the session is going
//     nowhere, and the cost of not releasing is a warm runtime that stays
//     resident until the next idle observation.
//   - AFTER it, a failure is RECORDED and the release CONTINUES. The residency
//     has been published `releasing` and the registry offers no way to take
//     that back — there is no ResumeAdmitting and no MarkResident, by design,
//     because un-releasing a session a Factory has stopped routing to is a
//     second protocol with its own races. Stopping halfway turns one unreleased
//     resource into several. A failed required checkpoint costs a rehydration,
//     because the durable session and its journal remain authoritative; it does
//     not cost work.
func (w *WarmReleaser) release(session WarmSession) WarmOutcome {
	ctx := context.Background()
	key, generation := session.Key(), session.Generation()
	outcome := WarmOutcome{Key: key, Generation: generation}

	// STEP 1. Re-read the durable inbox. An unreadable inbox FAILS CLOSED: a
	// store that will not answer has said nothing about whether work was
	// accepted, and releasing on that silence releases a session that may hold
	// accepted durable work.
	accepted, err := w.options.Inbox.AcceptedWork(ctx, key)
	switch {
	case err != nil:
		outcome.Kind = WarmOutcomeAborted
		outcome.Reason = "the durable inbox could not be re-read, so this Host cannot say the session is free of accepted work"
		outcome.Failures = append(outcome.Failures, WarmFailure{Step: WarmStepInbox, Err: err})
		w.options.Consumption.Wake(key)
		return outcome
	case accepted:
		outcome.Kind = WarmOutcomeAborted
		outcome.Reason = "the durable inbox holds work accepted since this session went idle"
		w.options.Consumption.Wake(key)
		return outcome
	}

	// STEP 2. Stop admission for this session, then mark it releasing.
	//
	// THE ORDER WITHIN THE STEP IS THE MECHANISM. §9.3 calls the pair atomic,
	// and two objects cannot share a critical section; what a racing command
	// actually requires is that admission is never open while the residency is
	// advertised releasing. Closing admission first gives that in the only
	// direction that matters — the reverse order has a window in which a
	// Factory reads `releasing` from a Host still accepting commands into the
	// runtime that record says is going away. The intermediate row is
	// {resident, accepting:false}, and a command arriving in it is refused
	// not_admitting by internal/realtime/hostlink.
	if _, current := w.options.Registry.StopAdmitting(key, generation); !current {
		outcome.Kind = WarmOutcomeAbandoned
		outcome.Reason = "the residency was removed or replaced before admission could be stopped, so this warm timer owns nothing"
		return outcome
	}

	run := func(step WarmStep, action func(context.Context) error) {
		if err := action(ctx); err != nil {
			outcome.Failures = append(outcome.Failures, WarmFailure{Step: step, Err: err})
		}
	}

	run(WarmStepBeginRelease, session.BeginRelease)
	// STEP 3.
	run(WarmStepCheckpoint, session.Checkpoint)
	// STEP 4. Nonterminal: no terminal event is appended.
	run(WarmStepReleaseResidency, session.ReleaseResidency)
	// STEP 5. The epoch-fenced tombstone and the visible registry entry.
	run(WarmStepFinishRelease, session.FinishRelease)
	// STEP 6. The lease FIRST, then in-memory state.
	run(WarmStepReleaseLease, session.ReleaseLease)
	// The admission credit is in-memory Host state and belongs here with the
	// rest of it, AFTER the lease release: a Host that credited its capacity
	// while still holding the grant would admit a replacement it has no room
	// for.
	w.options.Admissions.Release(key)
	run(WarmStepDropState, session.DropState)

	outcome.Kind = WarmOutcomeReleased
	outcome.Reason = "the session was released and remains durable and resumable"
	return outcome
}
