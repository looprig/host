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
// The refusal is RECORDED as its own Failure so a reconciler can find it.
//
// THE FALLBACK IT DOES TAKE (v0.16.1) IS NOT SHUTDOWN. A Session offering
// Abandoner is given up crash-equivalently after the refusal: nothing
// terminal is written, the runtime stops and hands back its journal lease,
// and an open gate survives for a successor. See Abandoner.
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

// ConsumptionHalter is the optional step a Session offers to stop applying
// durable commands for the rest of its drain.
//
// IT IS THE WARM RELEASE'S STEP 0, AND THE DRAIN USED TO HAVE NO EQUIVALENT.
// The drain stopped a session's command consumer only inside ReleaseResidency,
// AFTER the idle wait and the checkpoint, so a command admitted while the drain
// waited or checkpointed was applied by the draining Host under its own epoch
// while its registration said `releasing` (measured by the tests lane, I2.3).
// Halting first means what the drain checkpoints is the session's last state
// under this Host, and anything admitted later stays pending for the successor.
//
// The halt waits for a pass already in flight and must honour cancellation; the
// drain bounds it by the idle boundary and the platform grace. A halt that is
// cut short leaves consumption halted — only the in-flight pass may still land.
// The drain never takes a halt back: no drain path returns a session to
// resident, so there is nothing to resume.
type ConsumptionHalter interface {
	HaltConsumption(context.Context) error
}

// Abandoner is the optional Session method the drain falls back to when a
// runtime REFUSES its nonterminal release — harness refuses one for a session
// that is not whole-session idle, and a session parked at an open gate never
// is.
//
// IT IS CRASH-EQUIVALENT AND WRITES NOTHING DURABLE: the runtime seals its
// durable writes, stops, and hands its journal lease back, exactly as a
// process crash would except that the lease is released instead of left to
// lapse. So an open gate stays open in the journal and in the projection —
// cancelling the runtime's context instead would resolve it (abandoned) — and
// a successor restores the session as it restores a crashed Host's, in this
// process or another.
//
// BEFORE v0.16.1 THE REFUSED RUNTIME WAS PARKED instead: left running,
// uncancelled, holding its journal lease until the process exited. That was
// safe only for a process that exits after Stop (cmd/host); an embedder that
// stays alive could never restore the session again in-process, and its
// storage was closed beneath a live runtime.
//
// An implementation returns ErrNotAbandonable when the runtime offers no
// crash-equivalent release; the drain then reports the session Parked. It
// must return only once the runtime has torn down, and must honour
// cancellation; the drain bounds it by the idle boundary.
type Abandoner interface {
	AbandonResidency(context.Context) error
}

// ErrNotAbandonable is what an Abandoner returns for a runtime that offers no
// crash-equivalent release. It is not a failure: the session is reported
// Parked, as every refused runtime was before v0.16.1.
var ErrNotAbandonable = errors.New("lifecycle: the runtime offers no crash-equivalent release")

// EpochReporter is the optional Session method that names the RESIDENCY epoch
// the session is held under, so each Failure records the epoch it happened at.
//
// It is spelled ResidencyEpoch and never LeaseEpoch on purpose: this package
// reads neither half of registry.Entry's two-domain LeaseEpoch (see
// TestTheDrainReadsNeitherHalfOfTheRegistryEntrySeam), and the value here is
// the residency grant's — never the runtime's journal epoch.
type EpochReporter interface {
	ResidencyEpoch() uint64
}

// Residents enumerates the sessions this Host holds.
type Residents interface {
	// ResidentSessions returns the sessions held at the moment of the call.
	ResidentSessions() []Session
}

// Attaches is the optional Residents capability that accounts for the attaches
// IN FLIGHT when the drain began.
//
// THE RESIDENT SNAPSHOT DOES NOT COVER THEM, and that gap was a real wedge (the
// tests lane's C1). An attach that passed admission before Admissions.BeginDrain
// and was still hydrating when ResidentSessions was read is in neither set the
// drain acts on: the ledger admitted it, and the snapshot missed it. If it then
// became resident it was never released, the drain reported `drained`, and its
// runtime kept its journal lease for the life of the process — so every
// successor's hydrate was refused.
//
// THE CONTRACT THAT CLOSES IT IS SPLIT between this seam and its implementer.
// The implementer refuses, atomically with the snapshot, any attach that would
// become resident once the drain has begun, so an attach in flight can only roll
// back; the drain waits for those rollbacks before it reports `drained`, because
// until one has finished its lease and runtime are still held.
type Attaches interface {
	// AwaitAttaches blocks until every attach in flight at the moment of the
	// call has finished — resident or rolled back — or the context ends, and
	// then reports the context's error, naming what is still in flight. How a
	// rollback went is the ReleaseLedger's to report.
	AwaitAttaches(context.Context) error

	// AwaitSessionAttaches is AwaitAttaches for ONE session. The drain calls it
	// before it releases a session from its snapshot, because an attach that
	// had already COMMITTED when the snapshot was read is in the snapshot and
	// still running its last steps: released beneath it, its final
	// `resident` publication landed AFTER the drain's tombstone, and it
	// reported the session attached on a Host that had just released it.
	AwaitSessionAttaches(context.Context, registry.Key) error

	// CancelAttaches cancels every attach that has not yet become resident, so
	// it stops waiting on its collaborators and rolls back. It must never
	// cancel a residency that is already in the resident set: that one is the
	// drain's to release.
	CancelAttaches()
}

// Ender is the optional Session method that reports a residency whose ATTACH
// ROLLED BACK after it had committed into the drain's snapshot — while the
// drain waited for it. That rollback went through the residency's one release
// authority, so there is nothing left for the drain to release; what it could
// not give back is on the Host's ReleaseLedger, not here.
//
// IT IS NOT "the residency is gone". A residency another path removed — a warm
// release, a teardown — is not ended in this sense and answers false, so the
// drain's own release steps run and share that path's outcome.
type Ender interface {
	Ended() bool
}

// ReleaseLedger is the optional Residents capability that reports every release on
// this Host — an attach rollback, a warm release, a give-up — that ended with
// resources still held, whether or not the drain's snapshot ever named its
// session. The drain reads it after its own releases and withholds `drained`
// while it reports anything: a grant this Host still holds is a release that did
// not finish, whichever path left it.
type ReleaseLedger interface {
	Unreleased() error
}

// THIS PACKAGE OWNS NO TRANSPORT SHUTDOWN, and the absence is load-bearing
// rather than an omission. It used to hold a Link seam and close it as the
// drain's last step. That was wrong for a HostLink-INITIATED drain, and wrong
// in a way no test caught, because the close and the answer raced nothing — the
// close simply came FIRST. `run` closed the link BEFORE it assigned
// `settledState()`, so the terminal answer a Factory polls for came into
// existence only after the transport carrying it was gone.
//
// WHY THAT WAS NOT MERELY WEAK. The disconnect a Factory was left with is
// Centrifuge `DisconnectShutdown` (3001) in EVERY outcome, including the one
// `settledState` deliberately withholds `drained` for. So the one signal
// reaching a Factory was AMBIGUOUS between "release finished, delete the
// workload" and "FinishRelease refused, this Host still holds the lease". On
// that signal a Factory either deletes work whose lease is still held or waits
// forever. Core says so directly: HostLinkDrainObservation exists "for a
// Factory to observe without inferring release completion from a transport
// close" (core/sessionwire/v1/hostlink.go).
//
// SO THE TRANSPORT CLOSES WHEN THE PROCESS STOPS, NEVER AS A DRAIN STEP. The
// composition's Stop closes it after Wait returns, which keeps "the transport
// closes LAST" — it is still after every release step — while leaving a
// link-drained Host alive and answering. StepCloseLink survives as the step
// name that caller records under; see its own comment.
// TestTheDrainOwnsNoTransportShutdown is the trip-wire over this paragraph.

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

	// Grace is the whole drain's bound — the platform's termination grace, less
	// whatever margin the composition keeps for its own shutdown. When it
	// expires, every outstanding idle wait is cancelled, any wait whose session
	// has not answered that cancellation is ABANDONED with ErrWaitAbandoned,
	// and cleanup continues.
	//
	// The abandonment is the part that makes this a bound. Cancelling a context
	// only asks; a session that ignores it would otherwise hold this drain past
	// any grace at all.
	//
	// An attach still in flight when the drain began (see Attaches) is waited
	// for until one IdleBoundary before the grace — floored at half the grace —
	// then cancelled, and abandoned at the grace itself. So the whole drain,
	// attaches included, stays inside Grace.
	Grace time.Duration

	// IdleBoundary is one session's bound on reaching idle. It must not exceed
	// Grace: a per-session wait longer than the whole drain's is a bound that
	// can never be the one that fires, which reads as a policy and is not one.
	IdleBoundary time.Duration

	// PublishBound is how long StartDrain will wait for the nonaccepting
	// publication before refusing the drain.
	//
	// IT BOUNDS THE ACKNOWLEDGEMENT, which is a synchronous RPC. DrainStarter
	// takes no context by design — the RPC acknowledges initiation, so a
	// caller-supplied deadline would be a deadline on the wrong event — and
	// that left the publication with no bound at all: an Advertiser that never
	// returned hung the drain RPC forever. The context handed to
	// PublishNonaccepting is cancelled when this expires AND the wait for its
	// answer ends, so the bound holds whether or not the Advertiser honours
	// cancellation.
	//
	// EXPIRY REFUSES THE DRAIN rather than proceeding. This Host cannot say
	// nonaccepting was published, so it must not act as though it were; the
	// ledger stays stopped and a retry may find the write landed after all,
	// which is safe because the publication is idempotent at the store.
	//
	// It must not exceed Grace: an acknowledgement that could outlive the whole
	// drain's bound is not an acknowledgement of anything.
	PublishBound time.Duration
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
	StepHaltConsumption  Step = "halt_consumption"
	StepBeginRelease     Step = "begin_release"
	StepWaitIdle         Step = "wait_idle"
	StepCheckpoint       Step = "checkpoint"
	StepReleaseResidency Step = "release_residency"
	StepFinishRelease    Step = "finish_release"

	// StepAbandonResidency is the crash-equivalent fallback after a refused
	// ReleaseResidency (see Abandoner). Its failure leaves the session Parked
	// and, like the refusal before it, does not withhold `drained`: the
	// residency tombstone and lease release still run.
	StepAbandonResidency Step = "abandon_residency"

	// StepSettleAttaches is the wait for the attaches in flight when the drain
	// began (see Attaches). The Host-wide wait's failure carries no session key
	// — it names the attaches in its error — while a wait for one session's own
	// attach carries that session's. Either WITHHOLDS `drained`: an attach that
	// has not finished may still hold a lease and a runtime, or still publish
	// over the drain's tombstone.
	StepSettleAttaches Step = "settle_attaches"

	// StepUnreleased is the Host's ReleaseLedger reporting a release — by any path —
	// that ended with resources still held. It carries no session key (the
	// error names them) and it WITHHOLDS `drained`.
	StepUnreleased Step = "unreleased"

	// StepCloseLink is the transport shutdown, and THIS PACKAGE NEVER RECORDS
	// IT. The drain does not close a transport at all; the process-lifecycle
	// caller closes one after Wait returns and books its failure under this
	// name, so an operator reading a Report sees the same vocabulary wherever
	// the step ran. It stays here because Report is this package's type.
	StepCloseLink Step = "close_link"
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

	// ResidencyEpoch is the residency epoch the session was held under when
	// the step failed, or zero when the session does not report one
	// (EpochReporter). It is what lets an operator tie a forced,
	// crash-equivalent release — which journals no SessionResidencyReleased —
	// to the grant its successor fences. It is never a journal epoch.
	ResidencyEpoch uint64
}

func (f Failure) Error() string {
	epoch := ""
	if f.ResidencyEpoch != 0 {
		epoch = " (residency epoch " + strconv.FormatUint(f.ResidencyEpoch, 10) + ")"
	}
	return "lifecycle: session " + strconv.Quote(string(f.Key.SessionID)) + epoch + " failed at " + string(f.Step) + ": " + f.Err.Error()
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
	// CORE'S BOUNDED STATE CANNOT EXPRESS MOST OF THEM. HostLinkDrainState has
	// exactly two values, so a Factory polling the drain-status RPC cannot see
	// a failed checkpoint, a missed idle boundary or a refused ReleaseResidency
	// at all; these Failures are how a process-local caller and an operator
	// tell them apart. That is a limit of the published wire contract.
	//
	// THE ONE IT CAN EXPRESS IS THE ONE THAT DESTROYS WORK. A refused
	// FinishRelease means release did not finish, so State stays `draining` and
	// a Factory does not delete the workload. See settledState.
	Failures []Failure

	// Abandoned are the sessions whose runtime refused its nonterminal release
	// and was then given up crash-equivalently (see Abandoner): torn down,
	// journal lease released, nothing durable written, so an open gate is
	// preserved for a successor.
	Abandoned []registry.Key

	// Parked are the sessions whose runtime refused its release and could NOT
	// be abandoned (no Abandoner, ErrNotAbandonable, or a failed or unbounded
	// abandon). Such a runtime is left running and keeps its journal lease
	// until the process exits, so no successor — in this process or another —
	// can restore the session before then. A caller that keeps the process
	// alive must treat a non-empty Parked as a leak.
	Parked []registry.Key
}

// ---------------------------------------------------------------------------
// Drainer
// ---------------------------------------------------------------------------

// ErrIdleBoundary marks a wait ended by the configured boundary rather than by
// the session, so a caller tells "this session would not settle" from "this
// session reported an error while settling" with errors.Is.
var ErrIdleBoundary = errors.New("lifecycle: the session did not reach its safe boundary in time")

// ErrWaitAbandoned marks an idle wait this drain STOPPED WAITING ON because the
// platform grace expired before the session answered its cancellation.
//
// It is a strictly worse condition than ErrIdleBoundary alone and is reported
// alongside it, never instead of it. The session's WaitIdle goroutine may still
// be running: this Host has given up on hearing from it, which is what makes
// Grace a bound rather than a request.
var ErrWaitAbandoned = errors.New("lifecycle: the session did not answer cancellation before the platform grace expired")

// ErrPublishBound reports a nonaccepting publication that did not answer within
// Options.PublishBound. The drain is REFUSED: this Host cannot say the write
// landed, and a drain that proceeded on that assumption would release sessions
// a Factory is still routing work to.
var ErrPublishBound = errors.New("lifecycle: the nonaccepting publication did not complete within its bound")

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
	abandoned  []registry.Key
	parked     []registry.Key

	// begunScope is the scope the call that BEGAN this drain named, kept so the
	// caller that began it can still be attributed after its session has been
	// released. It is the zero Key for the process-lifecycle drain, which names
	// no scope, and the zero Key must never be treated as a match.
	begunScope registry.Key

	// ledgerStopped records that Admissions.BeginDrain has been called, so a
	// retry after a refused publication does not stop admission twice. It is
	// separate from begun because the ledger deliberately stays stopped through
	// a refusal: this Host has stopped admitting either way.
	ledgerStopped bool

	// starting and settled are the in-flight publication. A concurrent caller
	// waits on settled and is then answered from the state that attempt left
	// behind, rather than mounting a second publication of its own.
	starting bool
	settled  chan struct{}
	startErr error

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
	if options.PublishBound <= 0 {
		return nil, &InvalidOptionsError{Field: "PublishBound", Reason: "must be positive; an unbounded nonaccepting publication is a drain RPC that can hang forever"}
	}
	if options.PublishBound > options.Grace {
		return nil, &InvalidOptionsError{Field: "PublishBound", Reason: "must not exceed Grace; an acknowledgement that can outlive the whole drain's bound acknowledges nothing"}
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
// THE TRANSITION IS NOT OBSERVABLE HALF-DONE, and what makes that true is the
// ORDER OF THE STATE WRITES under the lock, not the lock being held for the
// whole thing. The ledger flip happens under the lock before the publication
// starts; `begun`, `done` and the resident snapshot are written under the lock
// only after it has returned. So no observer can see nonaccepting published by
// a Host still admitting, and none can see a drain that has not been published.
// A concurrent StartDrain cannot begin a second machine because `starting` is
// set in the same critical section as the ledger flip.
//
// The publication itself runs WITHOUT the lock, deliberately: holding it there
// blocked ObserveDrain and Wait for the whole publication, which made the
// bounded status observation unbounded.
// TestTheDrainTransitionIsNotObservableHalfDone probes the interleavings and
// carries the positive control that makes its silence mean something, and
// TestTheStatusObservationIsAnsweredWhileTheDrainIsStillPublishing holds the
// half that regressed.
//
// THE SCOPE IS NEITHER RECORDED NOR BRANCHED ON, and the parameter is UNNAMED
// so that neither is expressible. An earlier sentence here said "recorded and
// not branched on"; the first half was never true. Every drain this type runs
// is the whole Host's, whatever scope it is handed.
//
// That is sound only because HostLink refuses, above this seam, every scope
// this Host cannot attribute to the caller — a whole-Host drain on a
// tenant-authenticated link, and a fixed session the requesting tenant does not
// hold. A dedicated Host holds one session (its placement pins Capacity to 1),
// so draining that session and draining the Host are then the same work.
func (d *Drainer) StartDrain(scope hostlink.DrainScope) (hostlink.DrainStatus, error) {
	d.mu.Lock()
	if d.begun {
		status := hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}
		d.mu.Unlock()
		return status, nil
	}
	if d.starting {
		// Another caller is publishing. Wait for THAT attempt and answer from
		// what it left behind, rather than mounting a second publication: two
		// concurrent requests are one drain, and a thundering herd of retries
		// against a slow store is the opposite of stopping admission.
		settled := d.settled
		d.mu.Unlock()
		<-settled
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.begun {
			return hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}, nil
		}
		return hostlink.DrainStatus{}, d.startErr
	}

	// The ledger flips FIRST, and inside the lock, so a Factory that reaches
	// the store between the two finds an accepting Host at worst and never a
	// Host that has published nonaccepting while still admitting. It flips ONCE
	// EVER — a retry after a refused publication does not stop admission again.
	if !d.ledgerStopped {
		d.options.Admissions.BeginDrain()
		d.ledgerStopped = true
	}
	d.starting = true
	settled := make(chan struct{})
	d.settled = settled
	d.mu.Unlock()

	// THE LOCK IS RELEASED ACROSS THE PUBLICATION, and that is a correction a
	// gate forced rather than the original design. Holding it made ObserveDrain
	// and Wait block for the whole publication — an unbounded, uncancellable
	// wait on the one RPC whose entire purpose is to answer promptly. Releasing
	// it costs nothing the atomicity claim depends on: `begun` is still set
	// only after the publication returns, so no observer can see a drain that
	// has not been published, and `starting` is what keeps the transition
	// single-owner.
	err := d.publish()

	d.mu.Lock()
	d.starting = false
	d.startErr = err
	if err != nil {
		// NOT recorded and continued past, unlike every other failure here.
		// Nothing downstream knows this drain began, so continuing would
		// release sessions a Factory is still routing work to. The ledger stays
		// stopped — this Host has stopped admitting either way — and the caller
		// may retry.
		d.mu.Unlock()
		close(settled)
		return hostlink.DrainStatus{}, err
	}

	d.begun = true
	d.begunScope = scope.Key
	d.done = make(chan struct{})
	sessions := d.options.Residents.ResidentSessions()
	status := hostlink.DrainStatus{Generation: d.options.Generation, State: d.drainState}
	d.mu.Unlock()
	close(settled)

	go d.run(sessions)
	return status, nil
}

// publish makes this Host's stopped admission durable, bounded by
// Options.PublishBound.
//
// THE BOUND HOLDS WHETHER OR NOT THE ADVERTISER HONOURS CANCELLATION. The
// context is cancelled, which is the cooperative half, and the wait for the
// answer ends regardless, which is the half that makes it a bound. An expiry is
// a REFUSAL: this Host cannot claim the row landed.
func (d *Drainer) publish() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	published := make(chan error, 1)
	go func() { published <- d.options.Advertiser.PublishNonaccepting(ctx) }()

	select {
	case err := <-published:
		return err
	case <-d.options.Clock.After(d.options.PublishBound):
		cancel()
		return ErrPublishBound
	}
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

// BegunScope reports the scope the call that BEGAN this drain named.
//
// IT EXISTS FOR ONE ATTRIBUTION AND NOT AS A GENERAL ACCESSOR. hostlink's drain
// resolver refuses a fixed-session drain unless the requesting tenant currently
// HOLDS that session — R-1's rung 8 — and that read is correct for BEGINNING a
// Host-wide drain. Applied unchanged to the read-only observation it makes the
// TERMINAL answer unreachable: the drain releases the session, so the tenant
// that began the drain stops holding it at exactly the moment `drained` becomes
// true, and is refused the one answer it is waiting for.
//
// WHAT IT LETS THROUGH IS A STRICT SUBSET OF WHO ALREADY KNOWS. A caller
// matches only by naming the tenant AND session that BEGAN this drain, and only
// that caller ever received the acknowledgement. A tenant that never held the
// fixed session began nothing, matches nothing, and is refused exactly as
// before and indistinguishably. So this discloses nothing to anyone who did not
// already have it, which is why it may relax a rung whose whole purpose is
// non-disclosure.
//
// THE SECOND RETURN IS NOT "a drain has begun". It is "a drain has begun AND it
// named a scope". The process-lifecycle drain passes the zero DrainScope, so a
// zero Key would otherwise match a caller that named nothing — and rungs above
// guarantee a real caller's key is never zero, so a zero match could only ever
// be an accident.
func (d *Drainer) BegunScope() (registry.Key, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.begun || d.begunScope == (registry.Key{}) {
		return registry.Key{}, false
	}
	return d.begunScope, true
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
		Abandoned:  append([]registry.Key(nil), d.abandoned...),
		Parked:     append([]registry.Key(nil), d.parked...),
	}
}

// run releases every session that was resident when the drain began, then
// closes the link and reports drained.
//
// THE SESSION SET IS TAKEN ONCE, by StartDrain, and is not re-read. A session
// that begins its attach after the drain began is refused by the ledger that is
// already flipped; chasing it here would be a machine that can be kept working
// by a caller it has already told to stop.
//
// AN ATTACH THE LEDGER ADMITTED BEFORE THE FLIP IS NOT IN THAT SET, and "the
// ledger refuses it" was once the whole of this comment. It is not refused by
// the ledger — it passed the ledger — so when Residents offers Attaches the
// drain also waits for those, concurrently with the releases. See Attaches and
// settleAttaches.
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
	if attaches, ok := d.options.Residents.(Attaches); ok {
		wait.Add(1)
		go func() {
			defer wait.Done()
			d.settleAttaches(graceCtx, attaches)
		}()
	}
	for _, session := range sessions {
		wait.Add(1)
		go func() {
			defer wait.Done()
			d.release(graceCtx, session)
		}()
	}
	wait.Wait()
	endGrace()

	// THE LEDGER IS READ LAST, after every release this drain ran or waited
	// for, so a rollback or warm release that finished while the drain ran is
	// in it. See ReleaseLedger.
	if ledger, ok := d.options.Residents.(ReleaseLedger); ok {
		if err := ledger.Unreleased(); err != nil {
			d.record(Failure{Step: StepUnreleased, Err: err})
		}
	}

	d.mu.Lock()
	d.drainState = d.settledState()
	done := d.done
	d.mu.Unlock()
	close(done)
}

// settledState is the Core state this drain finishes in, and it is NOT
// unconditionally drained.
//
// Core defines HostLinkDrainStateDrained as "the observed scope has FINISHED
// RELEASE". A session whose FinishRelease refused has not: its epoch-fenced
// tombstone was not written or its lease was not released. Reporting `drained`
// there is not merely imprecise — it is the signal a Factory deletes a
// dedicated workload on, so it destroys work whose lease this Host still holds.
//
// THIS IS THE ONE PLACE THIS PACKAGE DOES NOT DO WHAT O6.3's PROSE SAYS. The
// runbook's sequence ends "then report Host drained"; Core's own definition of
// the value says when that is true, and the two disagree exactly here. The
// deliberate deviation is recorded rather than taken quietly.
//
// The cost is real and is the right way round: a drain that cannot finish stays
// `draining`, so a Factory waits and eventually escalates instead of deleting.
// A leaked workload is visible to an operator; deleted work is not.
//
// ONLY FinishRelease, AN UNSETTLED ATTACH AND THE RELEASE LEDGER WITHHOLD IT. An attach
// that did not finish within its bound may still hold a residency grant and a
// runtime's journal lease, and the ReleaseLedger reports a release by any other path
// that left one held; each is the same "release did not finish".
//
// Nothing else does. A failed checkpoint, a missed idle boundary
// and a refused ReleaseResidency are all recorded and none of them means
// release did not finish — the tombstone is written and the lease is released
// in every one of those. A transport that would not close is not in this list
// at all any more: the drain closes none, and the caller that does books its
// failure after this state was already settled.
func (d *Drainer) settledState() sessionwire.HostLinkDrainState {
	for _, failure := range d.failures {
		if failure.Step == StepFinishRelease || failure.Step == StepSettleAttaches || failure.Step == StepUnreleased {
			return sessionwire.HostLinkDrainStateDraining
		}
	}
	return sessionwire.HostLinkDrainStateDrained
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
	var epoch uint64
	if reporter, ok := session.(EpochReporter); ok {
		epoch = reporter.ResidencyEpoch()
	}
	record := func(step Step, err error) {
		d.record(Failure{Key: key, Step: step, Err: err, ResidencyEpoch: epoch})
	}
	run := func(step Step, action func(context.Context) error) {
		if err := action(context.Background()); err != nil {
			record(step, err)
		}
	}

	// FIRST, THE SESSION'S OWN ATTACH, if one is still finishing. See
	// Attaches.AwaitSessionAttaches: a session committed into the snapshot may
	// still be in its attach's last steps, and releasing it concurrently let
	// that attach's `resident` publication land after the tombstone. Bounded by
	// the grace; if it expires the release goes ahead and the failure withholds
	// `drained`, because the two may still interleave.
	//
	// A WAIT CUT SHORT BY THE GRACE LEAVES THE RESIDENCY HELD, deliberately.
	// The attach may yet fail and unwind its own runtime and grant, and a drain
	// release run concurrently would release them a second time through guards
	// neither shares. So nothing is released, and the recorded failure keeps the
	// drain `draining`: the attach's own rollback, or the process exit, ends it.
	if attaches, ok := d.options.Residents.(Attaches); ok {
		if err := attaches.AwaitSessionAttaches(graceCtx, key); err != nil {
			record(StepSettleAttaches, errors.Join(ErrWaitAbandoned, err))
			return
		}
	}
	if ender, ok := session.(Ender); ok && ender.Ended() {
		return
	}

	// STEP 0, as the warm release's: stop applying commands before anything
	// is marked, waited on or checkpointed. See ConsumptionHalter.
	if halter, ok := session.(ConsumptionHalter); ok {
		if err := d.bounded(graceCtx, halter.HaltConsumption); err != nil {
			record(StepHaltConsumption, err)
		}
	}
	run(StepBeginRelease, session.BeginRelease)
	d.waitIdle(graceCtx, session, record)
	run(StepCheckpoint, session.Checkpoint)
	// THE RUNTIME'S RELEASE IS BOUNDED BY THE PLATFORM GRACE, and it is the one
	// step that is. harness refuses a nonterminal release of a session that is
	// not whole-session idle and WAITS for idle first — so a session parked at
	// an open gate, which is never idle, held this call, the drain and Stop
	// forever (measured on a composed Host with a permission gate open). A
	// refusal here is already a path this sequence takes: it is recorded, the
	// runtime is abandoned where it can be (else parked, holding its journal
	// lease until the process exits), and FinishRelease still runs. The
	// session's successor restores it the way it restores a crashed Host's.
	if err := session.ReleaseResidency(graceCtx); err != nil {
		record(StepReleaseResidency, err)
		// THE REFUSED RUNTIME IS ABANDONED BEFORE THE RESIDENCY IS HANDED BACK,
		// so by the time FinishRelease lets a successor in, the journal lease it
		// needs is free. See Abandoner.
		d.abandon(session, record)
	}
	run(StepFinishRelease, session.FinishRelease)
}

// abandon gives up a runtime whose release was refused, crash-equivalently,
// and books the session Abandoned or Parked.
//
// ITS BOUND IS THE IDLE BOUNDARY, ON A FRESH CONTEXT. The grace is normally
// spent by now — a gated runtime's release waits for it — and an abandon cut
// short by an expired context would leave exactly the lease this exists to
// free. An abandon that ignores its cancellation is waited one more boundary
// and then left, recorded, and the session reported Parked.
func (d *Drainer) abandon(session Session, record func(Step, error)) {
	key := session.Key()
	abandoner, ok := session.(Abandoner)
	if !ok {
		d.book(nil, &key)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	answered := make(chan error, 1)
	go func() { answered <- abandoner.AbandonResidency(ctx) }()
	var err error
	select {
	case err = <-answered:
	case <-d.options.Clock.After(d.options.IdleBoundary):
		cancel()
		select {
		case err = <-answered:
			if err == nil {
				err = context.Canceled
			}
			err = errors.Join(ErrIdleBoundary, err)
		case <-d.options.Clock.After(d.options.IdleBoundary):
			err = errors.Join(ErrIdleBoundary, ErrWaitAbandoned)
		}
	}
	switch {
	case err == nil:
		d.book(&key, nil)
	case errors.Is(err, ErrNotAbandonable):
		d.book(nil, &key)
	default:
		record(StepAbandonResidency, err)
		d.book(nil, &key)
	}
}

// book records one session as abandoned or parked.
func (d *Drainer) book(abandoned, parked *registry.Key) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if abandoned != nil {
		d.abandoned = append(d.abandoned, *abandoned)
	}
	if parked != nil {
		d.parked = append(d.parked, *parked)
	}
}

// settleAttaches waits for the attaches in flight when this drain began,
// cancels what is left before the platform grace, and waits for that to finish
// within the grace.
//
// THE WAIT COMES FIRST AND THE CANCELLATION LATER, deliberately. An in-flight
// attach cannot become resident once the drain has begun (the implementer
// refuses it at its commit point), so its rollback through the ordinary path —
// a live runtime's nonterminal release, then the lease — is the well-trodden way
// to give everything back. Cancelling at once would instead cancel a runtime
// launched moments ago on the session context, which is the crash-equivalent
// path.
//
// THE CANCELLATION IS ONE IDLE BOUNDARY BEFORE THE GRACE, so the attach has
// that long to roll back and the whole settle stays INSIDE the grace: the grace
// is the platform's termination bound, and a drain that overran it would be
// killed with the lease still held. When IdleBoundary is most of the grace the
// cancellation is floored at half the grace, so an attach still gets a real
// chance to finish on its own. What is still in flight when the grace expires
// is recorded, and it withholds `drained` (settledState).
func (d *Drainer) settleAttaches(graceCtx context.Context, attaches Attaches) {
	cancelAt := d.options.Grace - d.options.IdleBoundary
	if floor := d.options.Grace / 2; cancelAt < floor {
		cancelAt = floor
	}
	patience, endPatience := context.WithCancel(graceCtx)
	defer endPatience()
	go func() {
		select {
		case <-d.options.Clock.After(cancelAt):
			endPatience()
		case <-patience.Done():
		}
	}()
	if err := attaches.AwaitAttaches(patience); err == nil {
		return
	}
	attaches.CancelAttaches()
	if err := attaches.AwaitAttaches(graceCtx); err != nil {
		d.record(Failure{Step: StepSettleAttaches, Err: errors.Join(ErrWaitAbandoned, err)})
	}
}

// bounded runs one cancellable step under the idle boundary and the platform
// grace, whichever ends first, and reports ErrIdleBoundary joined with the
// step's own answer when a bound ended it. Like waitIdle it takes the step's
// answer if it arrives, and abandons it with ErrWaitAbandoned only once the
// grace is gone.
func (d *Drainer) bounded(graceCtx context.Context, action func(context.Context) error) error {
	ctx, cancel := context.WithCancel(graceCtx)
	defer cancel()
	answered := make(chan error, 1)
	go func() { answered <- action(ctx) }()
	select {
	case err := <-answered:
		return err
	case <-d.options.Clock.After(d.options.IdleBoundary):
	case <-graceCtx.Done():
	}
	cancel()
	var err error
	select {
	case err = <-answered:
	case <-graceCtx.Done():
		select {
		case err = <-answered:
		default:
			err = ErrWaitAbandoned
		}
	}
	if err == nil {
		err = context.DeadlineExceeded
	}
	return errors.Join(ErrIdleBoundary, err)
}

// waitIdle bounds one session's wait by the idle boundary and by the platform
// grace, whichever ends first, and cancels the wait it hands the session.
func (d *Drainer) waitIdle(graceCtx context.Context, session Session, record func(Step, error)) {
	idleCtx, stopWaiting := context.WithCancel(graceCtx)
	defer stopWaiting()

	waited := make(chan error, 1)
	go func() { waited <- session.WaitIdle(idleCtx) }()

	// BOTH BOUNDS ARM THE SAME EXIT. The idle boundary is the per-session one
	// and the platform grace is the whole drain's, and the tighter one wins —
	// but the grace has to be watched HERE as well as below, or a session whose
	// idle timer has not yet fired outlives the grace that is supposed to bound
	// it. IdleBoundary <= Grace makes that unreachable with a real clock; it
	// must not depend on that.
	select {
	case err := <-waited:
		if err != nil {
			record(StepWaitIdle, err)
		}
		return
	case <-d.options.Clock.After(d.options.IdleBoundary):
	case <-graceCtx.Done():
	}

	{
		// A bound expired. Cancel the wait and prefer the session's own
		// answer: a WaitIdle that honours cancellation returns promptly, and
		// taking its answer is what stops this drain reporting a session
		// released while its own wait was still running.
		stopWaiting()

		// THE WAIT FOR THAT ANSWER IS ITSELF BOUNDED, by the platform grace,
		// and that is the difference between cancelling and bounding. An
		// earlier version blocked here unconditionally, so a WaitIdle that
		// IGNORED its context left this drain parked forever: the state stayed
		// `draining`, the link never closed, and every other session's cleanup
		// sat finished behind it — with both timers already fired. WaitIdle is
		// a capability Host declares and someone else implements, so the
		// uncooperative case is the one Grace exists for, and a bound that
		// holds only when the far side cooperates is not a bound.
		var err error
		select {
		case err = <-waited:
		case <-graceCtx.Done():
			// The grace is gone. Take the answer if it has landed since, and
			// otherwise ABANDON the wait rather than outlive the termination
			// this drain is racing. `waited` is buffered, so the orphaned
			// goroutine can still finish and exit.
			select {
			case err = <-waited:
			default:
				err = ErrWaitAbandoned
			}
		}
		if err == nil {
			err = context.DeadlineExceeded
		}
		record(StepWaitIdle, errors.Join(ErrIdleBoundary, err))
	}
}

func (d *Drainer) record(failure Failure) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures = append(d.failures, failure)
}
