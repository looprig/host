// Package lifecycle owns this Host's drain: the single-owner state machine
// that stops admission, releases every resident session in order, and reports
// when the Host is drained.
//
// IT DEPENDS ON NO UNPUBLISHED CODE, which is the whole of runbook 04's O6.3
// title. The top-level `drain` repository is a tier-0 leaf with no configured
// origin, so this package names narrow local interfaces and nothing else; if
// `github.com/looprig/drain` is ever released, an adapter satisfying those
// interfaces is a separate, additive commit and NOT a rewrite of anything here.
//
// IT DOES NOT SELF-EXIT. A drain is something a placement controller or a
// platform asks this Host to do; a Host that decided on its own that it had
// been idle long enough and terminated would, in a pooled Deployment, be
// restarted by its controller and do it again — a restart loop that reads as
// instability rather than as the idleness that caused it.
//
// WHAT IT DOES NOT OWN. This package holds no admission ledger and no registry
// of its own. internal/service's CapacityPublisher is the Host's ONE ledger and
// its ONE drain flag, and its own comment says so; a second "draining" would be
// a Host that stops accepting in one place while advertising Accepting from the
// other. Everything here is either injected or derived from what is injected.
// It reads neither half of registry.Entry's epoch/accepting seam either — see
// TestTheDrainReadsNeitherHalfOfTheRegistryEntrySeam for why both are
// deliberate abstentions rather than omissions.
//
// WHAT IT CANNOT DO, stated so it is not mistaken for done. The runbook's
// 2026-09-05 hold names a Shutdown fallback for the case where graceful release
// REFUSES. department.Runtime declares no terminal capability — Identity,
// IdleWaiter, Liveness, Releaser, PublicationSubscriber, CommandApplier,
// LeaseEpochReporter — and department's own comment on Releaser records that
// residency release "is NONTERMINAL and is not Shutdown". A Shutdown method on
// Session here would therefore be a seam nothing in this module can satisfy.
// The refusal is RECORDED as its own Failure so a reconciler can find it; the
// fallback belongs to H4.1 and to O7.1's composition.
package lifecycle

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------

// Clock is the time source, so every bound in this package is testable without
// a sleep.
type Clock interface {
	// After returns a channel that receives once the duration has elapsed.
	After(time.Duration) <-chan time.Time
}

// Admissions is the Host's admission ledger and its drain flag.
//
// It is the SAME OBJECT internal/service owns, narrowed to two methods.
// service.CapacityPublisher satisfies it and the conformance assertion lives in
// this package's test, so production here does not name internal/service.
type Admissions interface {
	// BeginDrain stops admission and unranks every advertisement. It does not
	// evict: sessions already admitted stay charged until their own release.
	BeginDrain()

	// Draining reports whether graceful drain has begun.
	Draining() bool
}

// Advertiser publishes this Host's stopped admission where a Factory reads it.
//
// IT IS THE DURABILITY BOUNDARY OF THE ACKNOWLEDGEMENT. Admissions.BeginDrain
// flips an in-process flag; this is what makes the consequence visible outside
// this process, and StartDrain does not acknowledge until it has returned. A
// Host that acknowledged first would have told a placement controller it had
// stopped accepting while a Factory reading the orchestration store still saw
// an accepting, ranked target.
//
// IT IS NOT registry.Entry.Accepting. The rows this publishes are the
// orchestration store's; the local registry index is an optimization whose
// accepting flag is a strict function of its own state.
type Advertiser interface {
	// PublishNonaccepting returns once every target advertisement and registry
	// row this Host publishes says accepting=false, durably.
	PublishNonaccepting(context.Context) error
}

// Session is one resident session, narrowed to the ordered steps a drain takes
// it through.
//
// THE ORDER IS §9.3'S AND IT IS THE REASON THIS IS FIVE METHODS RATHER THAN
// ONE. A single Release(ctx) would hide exactly the property O6.3 exists to
// establish — that the durable `releasing` mark precedes the checkpoint, and
// that the tombstone and lease release follow it — behind an implementation
// this package could not test. residency.Heartbeat already offers the two
// halves this shape needs, BeginRelease and FinishRelease, and its own doc
// records that it was split for this caller rather than fused.
//
// BeginRelease PRECEDES WaitIdle, which the runbook's prose does not order. A
// session that is still admitting work has no reason to become idle, so waiting
// first is waiting on a condition the drain has not yet made reachable; marking
// it releasing first is what bounds the wait.
type Session interface {
	// BeginRelease marks the residency releasing and stops admission for this
	// session, durably, without terminating it.
	BeginRelease(context.Context) error

	// Checkpoint commits the runtime and workspace checkpoints the release
	// requires.
	Checkpoint(context.Context) error

	// FinishRelease writes the epoch-fenced tombstone and releases the lease.
	FinishRelease(context.Context) error

	// Key identifies the session, for the failure record.
	Key() registry.Key

	// ReleaseResidency closes process-local resources. It is NONTERMINAL: the
	// session stays durable and resumable and no terminal event is appended.
	ReleaseResidency(context.Context) error

	// WaitIdle blocks until the session has no work in flight. The drain
	// bounds it with the configured idle boundary and cancels the context when
	// that expires, so an implementation must honour cancellation.
	WaitIdle(context.Context) error
}

// Residents enumerates the sessions this Host holds.
type Residents interface {
	// ResidentSessions returns the sessions held at the moment of the call.
	ResidentSessions() []Session
}

// Link is the HostLink server, narrowed to its shutdown.
//
// It is OPTIONAL and it closes LAST. Factory reaches the bounded drain-status
// observation through this link, so a link closed when the drain began would
// leave Factory inferring completion from a disconnect — which is the inference
// O5.4's status observation exists to replace.
type Link interface {
	Close(context.Context) error
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Options configures one Host's drain.
type Options struct {
	// Generation is this drain's stable identity, supplied by the composition
	// so it can carry the Host's incarnation. Core refuses a zero generation.
	Generation uint64

	Clock      Clock
	Admissions Admissions
	Advertiser Advertiser
	Residents  Residents

	// Link is the HostLink server to close once the Host is drained. It is
	// optional; a Host composed without one still drains.
	Link Link

	// Grace is the whole drain's bound — the platform's termination grace, less
	// whatever margin the composition keeps for its own shutdown. When it
	// expires, every outstanding idle wait is cancelled and cleanup continues.
	Grace time.Duration

	// IdleBoundary is one session's bound on reaching idle. It must not exceed
	// Grace: a per-session wait longer than the whole drain's is a bound that
	// can never be the one that fires, which reads as a policy and is not one.
	IdleBoundary time.Duration
}

// InvalidOptionsError reports an option a Drainer may not run with.
type InvalidOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidOptionsError) Error() string {
	return "lifecycle: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// ---------------------------------------------------------------------------
// Failures and report
// ---------------------------------------------------------------------------

// Step names one thing the drain does to one session, so a failure says which.
type Step string

const (
	StepBeginRelease     Step = "begin_release"
	StepWaitIdle         Step = "wait_idle"
	StepCheckpoint       Step = "checkpoint"
	StepReleaseResidency Step = "release_residency"
	StepFinishRelease    Step = "finish_release"
	StepCloseLink        Step = "close_link"
)

// Failure is one step that did not succeed.
//
// IT IS RECORDED RATHER THAN RETURNED, and that is the runbook's "record
// explicit failures and continue deterministic cleanup". A drain that abandoned
// the rest of a session on its first failure would turn one unreleased resource
// into several, and a drain that reported success while leaving a live route
// with no owner would be worse than either.
type Failure struct {
	Key  registry.Key
	Step Step
	Err  error
}

func (f Failure) Error() string {
	return "lifecycle: session " + strconv.Quote(string(f.Key.SessionID)) + " failed at " + string(f.Step) + ": " + f.Err.Error()
}

func (f Failure) Unwrap() error { return f.Err }

// Report is one drain's outcome.
type Report struct {
	Generation uint64
	State      sessionwire.HostLinkDrainState

	// Failures are every step that did not succeed, in completion order. A
	// drained Host with failures is drained: the cleanup ran to the end and
	// these are what an operator must reconcile.
	//
	// CORE'S BOUNDED STATE CANNOT EXPRESS THEM. HostLinkDrainState has exactly
	// two values, so a Factory polling the drain-status RPC sees `drained`
	// whether or not a step refused. That is a real limit of the published
	// wire contract and not a decision taken here; the Failures are how a
	// process-local caller and an operator tell the two apart.
	Failures []Failure
}

// ---------------------------------------------------------------------------
// Drainer
// ---------------------------------------------------------------------------

// ErrIdleBoundary marks a wait ended by the configured boundary rather than by
// the session, so a caller tells "this session would not settle" from "this
// session reported an error while settling" with errors.Is.
var ErrIdleBoundary = errors.New("lifecycle: the session did not reach its safe boundary in time")

// Drainer is this Host's single drain, and is safe for concurrent use.
//
// A HOST DRAINS ONCE. Every repeat — a retry, a reconnect after an ambiguous
// reply, a second placement controller, a request that arrives after the Host
// is already drained — is answered with the same generation and the current
// state rather than starting a second machine. That is why StartDrain's
// idempotency lives here and not in HostLink: the transport has no way to know
// whether the work it acknowledged is still running.
type Drainer struct {
	options Options

	mu         sync.Mutex
	begun      bool
	drainState sessionwire.HostLinkDrainState
	failures   []Failure

	// done is closed once cleanup has finished. It is created by the call that
	// begins the drain, so Wait on an unbegun Drainer cannot block on a channel
	// nothing will ever close.
	done chan struct{}
}

// NewDrainer validates the options and returns a Drainer that has not begun.
func NewDrainer(options Options) (*Drainer, error) {
	if options.Generation == 0 {
		return nil, &InvalidOptionsError{Field: "Generation", Reason: "must be non-zero; Core refuses a drain observation carrying a zero generation"}
	}
	for _, required := range []struct {
		field   string
		present bool
		reason  string
	}{
		{"Clock", options.Clock != nil, "is required; every bound here is a timer and an unbounded drain is one that can hang a termination"},
		{"Admissions", options.Admissions != nil, "is required; a drain that cannot stop admission is not a drain"},
		{"Advertiser", options.Advertiser != nil, "is required; a drain nobody outside this process can see has not begun"},
		{"Residents", options.Residents != nil, "is required; a drain with nothing to enumerate cannot know it is finished"},
	} {
		if !required.present {
			return nil, &InvalidOptionsError{Field: required.field, Reason: required.reason}
		}
	}
	if options.Grace <= 0 {
		return nil, &InvalidOptionsError{Field: "Grace", Reason: "must be positive; a zero platform grace is an unbounded drain and not an immediate one"}
	}
	if options.IdleBoundary <= 0 {
		return nil, &InvalidOptionsError{Field: "IdleBoundary", Reason: "must be positive; a zero idle boundary is an unbounded wait"}
	}
	if options.IdleBoundary > options.Grace {
		return nil, &InvalidOptionsError{Field: "IdleBoundary", Reason: "must not exceed Grace; a per-session bound longer than the whole drain's can never be the one that fires"}
	}
	return &Drainer{options: options, drainState: sessionwire.HostLinkDrainStateDraining}, nil
}

// StartDrain begins this Host's drain and acknowledges the initiation.
//
// EVERYTHING BEFORE THE GOROUTINE IS THE ACKNOWLEDGEMENT'S PRICE, and the split
// is the contract O5.4 step 2 states: admission is stopped and nonaccepting is
// durably published before this returns, and no checkpoint, no ReleaseResidency
// and no tombstone has run when it does.
//
// THE TRANSITION IS NOT OBSERVABLE HALF-DONE, and the mutex is what makes that
// true rather than the ordering alone. The ledger flip, the publication, the
// `begun` flag and the resident snapshot all happen inside ONE critical
// section, so a concurrent ObserveDrain cannot land between the publication and
// the drain becoming observable, and a concurrent StartDrain cannot begin a
// second machine. TestTheDrainTransitionIsNotObservableHalfDone probes the
// interleavings and carries the positive control that makes its silence mean
// something.
//
// The scope is recorded and not branched on. A dedicated Host holds one
// session, so draining that session and draining the Host are the same work;
// HostLink has already refused a scope this Host does not answer for.
func (d *Drainer) StartDrain(hostlink.DrainScope) (hostlink.DrainStatus, error) {
	d.mu.Lock()
	if d.begun {
		status := hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}
		d.mu.Unlock()
		return status, nil
	}

	// The ledger flips FIRST and the publication follows, so a Factory that
	// reaches the store between the two finds an accepting Host at worst and
	// never a Host that has published nonaccepting while still admitting.
	d.options.Admissions.BeginDrain()
	if err := d.options.Advertiser.PublishNonaccepting(context.Background()); err != nil {
		// NOT recorded and continued past, unlike every other failure here.
		// Nothing downstream knows this drain began, so continuing would
		// release sessions a Factory is still routing work to. The ledger stays
		// flipped — this Host has stopped admitting either way — and the caller
		// may retry.
		d.mu.Unlock()
		return hostlink.DrainStatus{}, err
	}

	d.begun = true
	d.done = make(chan struct{})
	sessions := d.options.Residents.ResidentSessions()
	status := hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}
	d.mu.Unlock()

	go d.run(sessions)
	return status, nil
}

// ObserveDrain reports this Host's drain and starts nothing.
func (d *Drainer) ObserveDrain(hostlink.DrainScope) (hostlink.DrainStatus, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.begun {
		return hostlink.DrainStatus{}, false
	}
	return hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}, true
}

// Wait blocks until the drain has finished and returns its report. On a
// Drainer that has not begun it returns the unbegun report immediately.
func (d *Drainer) Wait() Report {
	d.mu.Lock()
	done := d.done
	d.mu.Unlock()
	if done != nil {
		<-done
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return Report{
		Generation: d.options.Generation,
		State:      d.drainState,
		Failures:   append([]Failure(nil), d.failures...),
	}
}

// run releases every session that was resident when the drain began, then
// closes the link and reports drained.
//
// THE SESSION SET IS TAKEN ONCE, by StartDrain, and is not re-read. A session
// that becomes resident after the drain began is refused by the ledger that is
// already flipped; chasing it here would be a machine that can be kept working
// by a caller it has already told to stop.
func (d *Drainer) run(sessions []Session) {
	// The platform grace bounds the whole drain. Cancelling this context is
	// what ends an idle wait that will not end on its own; the cleanup that
	// follows does not consult it, because cleanup that stopped at the grace
	// would leave exactly the orphaned lease the grace exists to avoid.
	graceCtx, endGrace := context.WithCancel(context.Background())
	defer endGrace()
	go func() {
		select {
		case <-d.options.Clock.After(d.options.Grace):
			endGrace()
		case <-graceCtx.Done():
		}
	}()

	var wait sync.WaitGroup
	for _, session := range sessions {
		wait.Add(1)
		go func() {
			defer wait.Done()
			d.release(graceCtx, session)
		}()
	}
	wait.Wait()
	endGrace()

	if d.options.Link != nil {
		if err := d.options.Link.Close(context.Background()); err != nil {
			d.record(Failure{Step: StepCloseLink, Err: err})
		}
	}

	d.mu.Lock()
	d.drainState = sessionwire.HostLinkDrainStateDrained
	done := d.done
	d.mu.Unlock()
	close(done)
}

// release takes one session through §9.3's order, recording what failed and
// continuing to the end.
//
// A REFUSED ReleaseResidency STILL REACHES FinishRelease. Both outcomes are
// bad: continuing releases the lease while process-local resources may still be
// live, and stopping leaks the lease AND leaves a published route with no
// owner. The second is unrecoverable without an operator and the first costs a
// rehydration, so this continues. See the package comment for the Shutdown
// fallback this layer cannot express.
func (d *Drainer) release(graceCtx context.Context, session Session) {
	key := session.Key()
	run := func(step Step, action func(context.Context) error) {
		if err := action(context.Background()); err != nil {
			d.record(Failure{Key: key, Step: step, Err: err})
		}
	}

	run(StepBeginRelease, session.BeginRelease)
	d.waitIdle(graceCtx, session, key)
	run(StepCheckpoint, session.Checkpoint)
	run(StepReleaseResidency, session.ReleaseResidency)
	run(StepFinishRelease, session.FinishRelease)
}

// waitIdle bounds one session's wait by the idle boundary and by the platform
// grace, whichever ends first, and cancels the wait it hands the session.
func (d *Drainer) waitIdle(graceCtx context.Context, session Session, key registry.Key) {
	idleCtx, stopWaiting := context.WithCancel(graceCtx)
	defer stopWaiting()

	waited := make(chan error, 1)
	go func() { waited <- session.WaitIdle(idleCtx) }()

	select {
	case err := <-waited:
		if err != nil {
			d.record(Failure{Key: key, Step: StepWaitIdle, Err: err})
		}
	case <-d.options.Clock.After(d.options.IdleBoundary):
		// The boundary expired. Cancel the wait and take the session's answer
		// rather than abandoning the goroutine holding it: a WaitIdle that
		// honours cancellation returns promptly, and one that does not would
		// otherwise have this drain report a session released while its own
		// wait was still running.
		stopWaiting()
		err := <-waited
		if err == nil {
			err = context.DeadlineExceeded
		}
		d.record(Failure{Key: key, Step: StepWaitIdle, Err: errors.Join(ErrIdleBoundary, err)})
	}
}

func (d *Drainer) record(failure Failure) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures = append(d.failures, failure)
}
