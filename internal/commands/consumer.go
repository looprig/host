// Package commands consumes a session's durable command inbox in the immutable
// order the inbox itself recorded, and advances one durable consumption cursor
// behind it.
//
// THE ORDER IS THE STORE'S, NOT THE TRANSPORT'S. §10.4 accepts a command with
// CreateOrdered under ordering_scope=(TenantID, SessionID), which fixes an
// immutable per-session acceptance order at the moment of acceptance; a
// consumer reads a bounded ListOrdered page from its cursor and consumes that.
// HostLink delivery is a LOW-LATENCY HINT and nothing else: it carries a
// CommandID that has already been admitted, it may arrive out of order, twice,
// or not at all, and none of those change what this package does. Hint's
// parameter is unnamed for that reason — see Consumer.Hint.
//
// THE CURSOR IS NOT A QUEUE POSITION, it is an attestation. It advances past a
// command only once that command is TERMINAL or its recoverable application
// prefix is DURABLY OWNED, which are §10.4's two ways a command stops needing
// to be re-driven from the inbox. A command that is neither stops the pass
// where it stands: its successors are not applied, and the pass NAMES it. Both
// halves matter. Skipping the predecessor would apply commands in an order the
// session never accepted; skipping it quietly would leave a session wedged with
// nothing to read but a cursor that stopped moving.
//
// WHAT THIS PACKAGE DOES NOT DO is claim, apply, recover or finalize a command.
// That is O4.2's, behind the Processor seam, and the split is deliberate: the
// consumer decides WHICH command is next and whether the cursor may pass it,
// and it decides that from durable state alone.
package commands

import (
	"context"
	"strconv"
	"strings"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// The durable command
// ---------------------------------------------------------------------------

// State is a command's DURABLE processing state, from §10.4's one state
// machine.
//
// It is NOT sessionwire.CommandState, and the difference is the reason this
// type exists rather than a reuse. Core's type is the PUBLIC projection a
// caller polls — accepted, pending, applied, rejected — and it cannot express
// claimed or applying at all, which are exactly the two states this consumer
// has to distinguish: a claimed command is somebody's in-flight work and an
// applying one may have a recoverable prefix. Projecting one onto the other
// would collapse the distinction the cursor rule is written over.
type State string

const (
	// StatePending is an accepted command nobody has claimed.
	StatePending State = "pending"

	// StateClaimed is a command claimed under a lease epoch, not yet applying.
	StateClaimed State = "claimed"

	// StateApplying is a command whose application has begun.
	StateApplying State = "applying"

	// StateApplied is one of the two mutually exclusive terminal states.
	StateApplied State = "applied"

	// StateRejected is the other, carrying a typed reason.
	StateRejected State = "rejected"
)

// Terminal reports whether a command has reached one of §10.4's two mutually
// exclusive terminal CAS states.
//
// A terminal command is never re-opened, which is what makes it safe for the
// cursor to pass and pointless to hand to a Processor.
func (s State) Terminal() bool { return s == StateApplied || s == StateRejected }

// known reports whether a state is one this package can reason about. An
// unknown one is refused rather than defaulted: defaulting it to pending would
// re-drive somebody else's terminal command, and defaulting it to terminal
// would let the cursor walk past work nobody has done.
func (s State) known() bool {
	switch s {
	case StatePending, StateClaimed, StateApplying, StateApplied, StateRejected:
		return true
	default:
		return false
	}
}

// Command is the part of a SessionInbox record this consumer reads.
//
// IT IS DELIBERATELY NOT THE WHOLE AGGREGATE. §10.4's record also holds the
// private payload or object reference, the allocated RuntimeCommandID, the
// claim epoch and expiry and the terminal result; none of those decide what is
// next or whether the cursor may pass, and the payload in particular is loaded
// from SessionStore AFTER a claim rather than carried around before one. What
// is here is the identity, the immutable acceptance order, and the durable
// state — the three things an ordering decision is made from.
type Command struct {
	// TenantID and SessionID are the scope the record claims to belong to.
	// They are carried so a page can be checked against the session this
	// consumer owns, rather than trusted.
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	// CommandID is the public, retry-stable identity, and the inbox's stable
	// key.
	CommandID sessionwire.CommandID

	// AcceptedOrder is the immutable per-session acceptance order CreateOrdered
	// fixed. It is the ONLY ordering authority: not due_at, not the CommandID,
	// not the order a Factory replica forwarded a hint in.
	AcceptedOrder uint64

	// State is the durable processing state.
	State State
}

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------
//
// Each is a NARROW LOCAL interface, for the reason department.Rig and
// host.SessionStore are: naming a looprig module in go.mod is the same decision
// as depending on it, and publishedLooprigVersions is where that decision is
// recorded. sessionstore v0.1.0 implements neither OrderedIndex's ListOrdered
// nor a per-session consumption cursor. These describe what Host requires; the
// concrete edges are later tasks.

// Inbox is the durable per-session command inbox, read in acceptance order.
type Inbox interface {
	// ListOrdered returns at most limit records STRICTLY AFTER afterOrder, in
	// ascending immutable acceptance order.
	//
	// The bound is the caller's and the ordering is the store's; a consumer
	// that sorted the page itself would be inferring an order rather than
	// reading one, which is the thing §10.4 forbids.
	ListOrdered(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, afterOrder uint64, limit int) ([]Command, error)
}

// CursorWrites is the durable cursor's one write.
//
// IT IS ITS OWN INTERFACE BECAUSE THE FENCING GUARD READS THESE DECLARATIONS.
// TestEveryDurableWriteGoesThroughTheFence collects the methods of every
// interface in this package whose name ends in "Writes" and requires each call
// to one of them to be inside a Fence.Write, so the set of durable writes is
// derived from the seams rather than from a list somebody has to remember to
// widen. Cursors below still has both methods, so nothing that implements it
// changed.
type CursorWrites interface {
	// SaveCursor records the cursor under the writer's lease epoch. The
	// implementation rejects an epoch lower than the greatest already
	// committed for the record.
	//
	// THE REJECTION MUST CARRY A SENTINEL THE FENCE CLASSIFIES, and that is a
	// contract rather than a courtesy. Every cursor write goes through
	// Fence.Write, and the fence records ownership as gone only for the errors
	// it recognises — residency.epochFence classifies exactly
	// residency.ErrEpochSuperseded and residency.ErrFenceConflict and treats
	// anything else as an ambiguous store failure. So an implementation that
	// reports a superseded epoch as an untyped error leaves this Host believing
	// it still owns a session a successor has taken, retrying once per
	// ReconcileInterval and counting the refusals as store trouble. Wrap or
	// return residency.ErrEpochSuperseded.
	SaveCursor(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, epoch uint64, order uint64) error
}

// Cursors is the durable command-consumption cursor of §10.4.
//
// It is a SEPARATE interface from Inbox because it is a different durable
// object with a different writer discipline: the inbox is written by whoever
// accepts and applies commands, and the cursor is written only by the current
// lease holder, through the fence.
type Cursors interface {
	// LoadCursor returns the greatest acceptance order durably consumed for a
	// session, or zero when none has been recorded.
	LoadCursor(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (uint64, error)

	CursorWrites
}

// Fence is the lease-epoch guard every durable write goes through.
//
// It is the SHAPE residency.epochFence already has, declared here because that
// type's methods are unexported and this is a different package. An adapter is
// owed at the composition root; it is mechanical, and the shape is stated here
// so it stays mechanical. THE THREE METHODS ARE NOT INTERCHANGEABLE: Lost is
// the channel a loop selects on, Held is the point check a write makes
// immediately before acting, and Write is the only one that can record the
// OTHER way ownership ends — a store refusing a write because a later epoch has
// committed, which closes no channel.
type Fence interface {
	// Held reports the typed reason ownership is gone, or nil.
	Held() error

	// Lost closes when ownership is gone.
	Lost() <-chan struct{}

	// Write performs one fenced write: refuse before it when ownership is
	// already gone, and record the loss when the store reports a later owner.
	Write(func() error) error
}

// Outcome is what a Processor reports about one command, reduced to the two
// facts the cursor rule is made of.
type Outcome struct {
	// State is the durable state the Processor last observed for the command.
	State State

	// PrefixOwned reports that the command's recoverable application prefix is
	// durably committed and correlated with a lease epoch.
	//
	// IT IS THE SECOND HALF OF THE CURSOR RULE, and it is a separate field
	// rather than an inference from State because §10.4 gives it a separate
	// meaning: an `applying` record MAY have a committed application prefix and
	// may equally have been claimed a moment ago with nothing written. The
	// first is recoverable from the durable correlation by whoever holds the
	// lease next; the second is not, and only the Processor knows which it is.
	PrefixOwned bool
}

// Processor claims, applies, recovers and finalizes ONE command. O4.2
// implements it; this package only decides which command it is handed and
// whether the cursor may pass afterwards.
//
// It is not department.CommandApplier. That interface is Host's control path
// INTO a runtime and takes one admitted command; this one is the whole durable
// protocol around such a call, including the claim, the payload load, the
// journal correlation and the terminal settlement.
//
// WHAT O4.2 DID WITH THE HAND-OFFS THIS COMMENT USED TO CARRY. Two of the four
// were closed, and the other two are open and are O5's; they are stated in the
// present tense because the next implementer reads them here.
//
//  1. CLOSED. A Processor is still handed no Fence — an Applier holds its own,
//     supplied at its own construction — and the structural guard no longer
//     covers SaveCursor alone: TestEveryDurableWriteGoesThroughTheFence derives
//     its subject from every `…Writes` interface in this package and covers
//     every production file, so the applier's transitions are inside it. Both
//     halves were done rather than either.
//  2. OPEN. A PANICKING Processor TAKES THE HOST DOWN. Run calls this on its
//     own goroutine and there is no recover anywhere in the path; releasePass
//     is deferred, so the pass slot is freed and the process is not.
//  3. OPEN. acquirePass HAS NO CONTEXT OR STOP ESCAPE, so Stop cannot bound
//     Run's exit while another caller holds the slot. Harmless while every pass
//     is context-bounded; it becomes a drain question at O5.
//  4. CLOSED, as predicted. Inbox and Cursors survived O4.2 without widening:
//     the applier's transitions write the SAME durable SessionInbox record this
//     Inbox reads, under the same one-writer-per-lease discipline, through a
//     separate InboxWrites seam. Two seams over one object is fine. Drifting
//     into a second writer discipline without noticing is not.
type Processor interface {
	// Process handles one command and reports what the cursor may conclude.
	Process(context.Context, Command) (Outcome, error)
}

// ---------------------------------------------------------------------------
// What a pass reports
// ---------------------------------------------------------------------------

// BlockedCommand names the command a pass stopped at.
//
// IT IS THE "NOT SILENTLY" IN STEP 4. A pass that declines to skip a pending
// predecessor and reports nothing is indistinguishable from a pass that found
// an empty inbox, and the difference is a session that is stuck. This value is
// carried on the result and is readable from the loop through
// Consumer.LastPass.
type BlockedCommand struct {
	CommandID     sessionwire.CommandID
	AcceptedOrder uint64

	// State is the state the pass LAST OBSERVED for this command: the
	// Processor's reported state when it answered, and the record's own durable
	// state when it did not.
	State State

	// Cause is the Processor's error, or nil when the command simply is not yet
	// terminal and owns no prefix. Those are different situations and a caller
	// diagnosing a wedged session needs to tell them apart.
	Cause error
}

// PassResult is what one reconciliation pass did.
type PassResult struct {
	// Cursor is the greatest durable consumption cursor this consumer knows of
	// at the moment the pass ended.
	//
	// IT IS ONE RULE ON EVERY EXIT, which it was not: three exits reported the
	// cursor the pass had loaded and two reported a bare zero, so a pass
	// refused before it could read anything told an operator reading LastPass
	// that the session had consumed nothing — the exact misdiagnosis the
	// Consumed/Cursor split below exists to prevent. Every exit now reports the
	// held cursor.
	//
	// ZERO MEANS ONE THING: this consumer has never had a cursor acknowledged,
	// either because the session has consumed nothing or because no pass has
	// yet managed to read one.
	Cursor uint64

	// Examined is how many records the page held.
	Examined int

	// Consumed is how many of them this pass consumed.
	//
	// It may exceed what Cursor reflects, and exactly when: a pass aborted by
	// cancellation or by a lost grant writes no cursor at all, so the work it
	// did is re-derived on the next pass. Naming the two separately is what
	// makes that visible rather than a discrepancy a reader has to derive.
	Consumed int

	// More reports that the page came back FULL and was fully consumed, so
	// there is more durable work available immediately.
	More bool

	// Blocked names the command the pass stopped at, if any.
	Blocked *BlockedCommand
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// InvalidConsumerOptionsError reports an option a Consumer may not run with.
type InvalidConsumerOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidConsumerOptionsError) Error() string {
	return "commands: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// UnusableHostError reports a *host.Host this package cannot run over.
//
// IT EXISTS BECAUSE A NON-NIL HOST IS NOT A VALIDATED ONE. Every field of
// host.Host is unexported, which restricts NAMING a field from another package
// and nothing else: the empty composite literal &host.Host{} is legal
// everywhere, compiles outside package host, and yields a nil Clock, a zero
// ReconcileInterval and a zero ReconcileBatch. NewConsumer checked only for
// nil, so such a value was accepted, and a zero batch is not cosmetic — see
// Reconcile's note on what "full" means.
//
// Accessors names every accessor whose value is unusable rather than the first,
// and that is what makes each check individually killable. The two ways to
// obtain a Host are host.New, which produces all three valid, and an empty
// literal, which produces all three invalid; a first-violation-wins error would
// therefore exercise exactly one of the three checks no matter what a test did.
type UnusableHostError struct {
	// Accessors are the offending accessor names, in a fixed order.
	Accessors []string
}

func (e *UnusableHostError) Error() string {
	return "commands: the Host cannot be consumed over, because " + strings.Join(e.Accessors, ", ") +
		" " + plural(len(e.Accessors), "is", "are") + " unusable; a *host.Host that did not come from host.New has none of them set"
}

// plural picks the verb form for a count. It exists so the message above reads
// correctly for one accessor and for three, which is the whole of its job.
func plural(count int, one, many string) string {
	if count == 1 {
		return one
	}
	return many
}

// PageProblem is a stable, machine-readable way a ListOrdered page broke the
// ordering contract.
//
// It is a code rather than a sentence for the reason host.OptionErrorCode is:
// Reason is free text a caller must not branch on, and two of these rules
// already read similarly enough that a substring match would confuse them.
type PageProblem string

const (
	// PageProblemOverLimit reports a page wider than the limit asked for.
	PageProblemOverLimit PageProblem = "over_limit"

	// PageProblemNotAfterCursor reports a record at or below the cursor the
	// page was requested after. A store answering with one is replaying
	// history the cursor exists to skip.
	PageProblemNotAfterCursor PageProblem = "not_after_cursor"

	// PageProblemNotMonotonic reports a record whose acceptance order does not
	// strictly increase. §10.4 makes the order immutable and monotone, so this
	// is the store contradicting its own contract.
	PageProblemNotMonotonic PageProblem = "not_monotonic"

	// PageProblemForeignSession reports a record scoped to another tenant or
	// session.
	PageProblemForeignSession PageProblem = "foreign_session"

	// PageProblemInvalidCommandID reports a record whose public identity Core
	// refuses.
	PageProblemInvalidCommandID PageProblem = "invalid_command_id"

	// PageProblemUnknownState reports a durable state this consumer cannot
	// reason about.
	PageProblemUnknownState PageProblem = "unknown_state"

	// PageProblemDuplicateCommandID reports the same public command identity
	// twice in one page.
	//
	// §10.4 makes CommandID the inbox's STABLE KEY, so two records carrying one
	// is the store contradicting itself in the same way a repeated acceptance
	// order does — and it is not caught by the order checks, because the two
	// copies may sit at strictly increasing orders. Left unchecked it hands one
	// command to the Processor twice inside a single pass, which is what the
	// serialization prevents ACROSS passes.
	PageProblemDuplicateCommandID PageProblem = "duplicate_command_id"
)

// PageError reports a ListOrdered page this consumer refused.
//
// NOTHING FROM SUCH A PAGE IS APPLIED, including the records before the bad
// one. A page whose order is already known to be wrong cannot be trusted for
// its prefix either: applying it would be applying commands in an order the
// session never accepted, which is the one thing the immutable order exists to
// prevent.
type PageError struct {
	Problem PageProblem

	// Index is the position in the page, or -1 when the problem is the page's
	// as a whole.
	Index int

	CommandID     sessionwire.CommandID
	AcceptedOrder uint64
	Reason        string
	Cause         error
}

func (e *PageError) Error() string {
	message := "commands: the inbox page is not conforming (" + string(e.Problem) + "): " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the typed cause a lower layer produced, if any.
func (e *PageError) Unwrap() error { return e.Cause }

// CursorRegressionError reports a durable cursor lower than one this consumer
// has already had acknowledged.
//
// It is REFUSED rather than obeyed. This Host is the only writer of the cursor
// while it holds the fence, so a lower value is the store contradicting itself;
// obeying it would re-drive every command in between.
//
// THE HIGH-WATER COMES FROM READS AS WELL AS WRITES, which is wider than an
// earlier version of this comment said. Held is set by every successful load,
// not only by an acknowledged write, so a store that answers 10 and then 5 is
// refused even though this consumer wrote neither. That is deliberate: the
// contradiction is the store's either way, and the consequence of obeying it is
// the same.
type CursorRegressionError struct {
	Key    registry.Key
	Loaded uint64
	Held   uint64
}

func (e *CursorRegressionError) Error() string {
	return "commands: the durable consumption cursor for session " + strconv.Quote(string(e.Key.SessionID)) +
		" went backwards from " + strconv.FormatUint(e.Held, 10) + " to " + strconv.FormatUint(e.Loaded, 10)
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

// Options configures a Consumer.
type Options struct {
	// Host supplies the clock, the reconcile interval and the reconcile batch.
	// Nothing here restates a value host.New already checked.
	Host *host.Host

	// Key is the session this consumer owns the inbox of.
	Key registry.Key

	// LeaseEpoch is the grant every cursor write is stamped with.
	LeaseEpoch uint64

	Inbox     Inbox
	Cursors   Cursors
	Processor Processor
	Fence     Fence
}

// Consumer consumes one session's durable command inbox.
type Consumer struct {
	host      *host.Host
	key       registry.Key
	epoch     uint64
	inbox     Inbox
	cursors   Cursors
	processor Processor
	fence     Fence

	// passes is a ONE-SLOT SEMAPHORE holding the right to run a pass.
	//
	// ONE PASS AT A TIME is not a convenience: two concurrent passes read the
	// same cursor, list the same page and hand the same command to the
	// Processor twice, which moves exactly-once application from a durable
	// property into a scheduling accident. Reconcile is exported and the loop
	// calls it too, so the two-callers case is the ordinary one, not a corner.
	//
	// PARKING IS OBSERVABLE, which is what turns "a second pass must not run
	// while the first is inside the Processor" from a race into a decidable
	// question: exactly one of parked-or-returned happens, so a select between
	// the two is deterministic. That the seam EXISTS is the load-bearing part —
	// the property went unchecked while there was none, and a mutation deleting
	// the whole lock left the package green.
	//
	// THE CHANNEL IS A PREFERENCE AND NOT A NECESSITY, and an earlier version
	// of this comment claimed otherwise: sync.Mutex.TryLock gives the same
	// try-then-hook-then-block shape and this module builds at go 1.26.6, so a
	// mutex would serve. Do not read the choice as an argument that it would
	// not.
	passes chan struct{}

	// parked, when set, is called immediately before a pass blocks on a busy
	// slot. It is unexported, set only from inside this package, and read under
	// mu.
	parked func()

	// hint is a ONE-SLOT WAKE, not a queue. The queue is the durable inbox; a
	// second pending hint would only ask for a pass that is already about to
	// happen, so a full channel drops the send.
	hint chan struct{}

	stopOnce sync.Once
	stopped  chan struct{}

	mu       sync.Mutex
	cursor   uint64
	loaded   bool
	failures int
	last     PassResult
}

// NewConsumer validates options and returns a Consumer.
func NewConsumer(options Options) (*Consumer, error) {
	if options.Host == nil {
		return nil, &InvalidConsumerOptionsError{Field: "Host", Reason: "must be set; the reconcile interval, batch and clock all come from it"}
	}
	// THE THREE ACCESSORS THIS PACKAGE READS, AND ONLY THOSE THREE. This is not
	// a second host.New and restates none of its other rules; it is the
	// boundary check for the values this package consumes, placed here because
	// a nil check alone accepts &host.Host{} and there is no other point at
	// which this package can refuse one. See UnusableHostError.
	var unusable []string
	if options.Host.Clock() == nil {
		unusable = append(unusable, "Clock")
	}
	if options.Host.ReconcileInterval() <= 0 {
		unusable = append(unusable, "ReconcileInterval")
	}
	if options.Host.ReconcileBatch() <= 0 {
		unusable = append(unusable, "ReconcileBatch")
	}
	if len(unusable) > 0 {
		return nil, &UnusableHostError{Accessors: unusable}
	}
	if options.Key.TenantID == "" || options.Key.SessionID == "" {
		return nil, &InvalidConsumerOptionsError{Field: "Key", Reason: "must name both a tenant and a session; every SessionStore operation is scoped by the pair"}
	}
	if options.LeaseEpoch == 0 {
		return nil, &InvalidConsumerOptionsError{Field: "LeaseEpoch", Reason: "must be non-zero; a cursor write stamped with a zero epoch is unfenced"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Inbox", options.Inbox != nil},
		{"Cursors", options.Cursors != nil},
		{"Processor", options.Processor != nil},
		{"Fence", options.Fence != nil},
	} {
		if !required.present {
			return nil, &InvalidConsumerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	return &Consumer{
		host:      options.Host,
		key:       options.Key,
		epoch:     options.LeaseEpoch,
		inbox:     options.Inbox,
		cursors:   options.Cursors,
		processor: options.Processor,
		fence:     options.Fence,
		passes:    make(chan struct{}, 1),
		hint:      make(chan struct{}, 1),
		stopped:   make(chan struct{}),
	}, nil
}

// Hint wakes the consumer because a command was admitted.
//
// THE PARAMETER IS UNNAMED, AND THAT IS THE MECHANISM. §10.5 makes HostLink
// delivery a low-latency path and not a source of truth: the CommandID it
// carries has already been admitted and is already in the durable order, so
// using it to select or prioritise work would be consuming by arrival order —
// the exact thing step 3 forbids. A convention saying "do not read it" is a
// convention someone will forget; an unnamed parameter cannot be read at all,
// and TestHintCarriesNoAuthority keeps it unnamed.
//
// It never blocks and it may drop: a hint arriving while one is already pending
// asks for a pass that is already about to happen.
func (c *Consumer) Hint(sessionwire.CommandID) {
	select {
	case c.hint <- struct{}{}:
	default:
	}
}

// Stop ends the loop. It is idempotent, because an attach rollback and a
// release may both reach it.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() { close(c.stopped) })
}

// Failures reports how many passes have returned an error.
func (c *Consumer) Failures() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failures
}

// LastPass reports what the most recent pass did.
//
// IT IS THE LOOP'S ONLY READER. Run discards each PassResult, so without this
// a blocked predecessor is reported to nobody once the consumer is running by
// itself — which would make BlockedCommand's whole argument vacuous.
func (c *Consumer) LastPass() PassResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// Run reconciles the inbox until the session context ends, the grant is lost,
// or Stop is called.
//
// THE FIRST PASS HAPPENS BEFORE ANY TIMER. §10.5 says an owning Host
// reconciles its inbox ON ATTACH and again on a bounded interval while
// resident; a loop that armed its timer first would leave a command accepted
// before the attach unapplied for a whole interval, on the one path that exists
// to make a cold session recoverable without a wake signal.
//
// A FULL PAGE COSTS NO INTERVAL. When a pass consumed its entire bounded page
// there is more durable work available now, and the loop takes it. This cannot
// spin: More is reported only when the whole page was consumed, and consuming a
// whole page advances the cursor by the batch, so every immediate repetition
// makes strict progress through a finite order.
//
// EVERY REASON TO STOP IS CONSULTED BEFORE A PASS, which is precedence rather
// than a race. Go's select chooses uniformly among ready cases, so a hint that
// arrived at the same moment the grant was lost would otherwise drive a pass
// under a lease this Host no longer holds, half the time.
//
// THE TIMER IS NEVER STOPPED, and that is a cost rather than an oversight.
// host.Clock hands back a *time.Timer, and the only test double that can hand
// back a timer a test fires is &time.Timer{C: ch}, which PANICS on Stop. So an
// unfired timer outlives its round by at most one interval. The remedy when it
// matters is host.Clock gaining a channel-returning After.
func (c *Consumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopped:
			return
		case <-c.fence.Lost():
			return
		default:
		}
		// BOTH WAYS OWNERSHIP ENDS, and this is the second one. The select
		// above can only see the channel; a fence that ended because a store
		// refused a write under a superseded epoch CLOSES NO CHANNEL, which is
		// what Fence's own documentation says four hundred lines above and what
		// this loop forgot. residency.epochFence is exactly that shape: end()
		// sets a flag, held() starts refusing, and lost() goes on returning the
		// lease's channel, which nothing closed.
		//
		// Without this the loop does not exit, it SPINS: every pass is refused
		// at Reconcile's own top-of-pass check, returns an error, arms a timer
		// and repeats, one failed pass per ReconcileInterval forever, with
		// Failures() climbing unbounded — and Failures() is the signal an
		// operator reads. Nothing wires a real fence yet, so this was unowned
		// rather than broken in production; it would have arrived with O7.1.
		if err := c.fence.Held(); err != nil {
			return
		}

		result, err := c.Reconcile(ctx)
		c.record(result, err)
		if err == nil && result.More {
			continue
		}

		timer := c.host.Clock().NewTimer(c.host.ReconcileInterval())
		select {
		case <-ctx.Done():
			return
		case <-c.stopped:
			return
		case <-c.fence.Lost():
			return
		case <-c.hint:
		case <-timer.C:
		}
	}
}

// acquirePass takes the right to run a pass, reporting that it had to wait.
//
// The non-blocking attempt first, then the hook, then the blocking send: a slot
// freed between the two sends makes this report a park that did not cost
// anything, which over-reports waiting and never under-reports it. The
// direction matters because the hook exists to prove a second pass DID wait.
func (c *Consumer) acquirePass() {
	select {
	case c.passes <- struct{}{}:
		return
	default:
	}
	c.mu.Lock()
	parked := c.parked
	c.mu.Unlock()
	if parked != nil {
		parked()
	}
	c.passes <- struct{}{}
}

// releasePass hands the right to run a pass back.
func (c *Consumer) releasePass() { <-c.passes }

// record stores what the loop would otherwise discard.
func (c *Consumer) record(result PassResult, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = result
	if err != nil {
		c.failures++
	}
}

// Reconcile performs ONE bounded pass over the durable order.
//
// It is exported because attach-time reconciliation is a caller's decision:
// O3.1's sequence begins inbox ownership at step 8 and reports attached at step
// 9, and a composition that wants the first pass to have completed before it
// answers a Factory bind calls this directly rather than racing the loop.
//
// IT IS SAFE TO CALL WHILE Run IS RUNNING, and that is stated here because
// here is where a caller reads it. Passes are serialized: a second one waits
// for the first rather than reading the same cursor and listing the same page.
// The sentence lived only on the unexported field that implements it, which no
// caller can see.
func (c *Consumer) Reconcile(ctx context.Context) (PassResult, error) {
	c.acquirePass()
	defer c.releasePass()

	// REFUSED BEFORE THE READ, not only before the write. A Host that has lost
	// the session lease has no business claiming commands out of an inbox a
	// successor now owns, and the read is what leads to the claim.
	if err := c.fence.Held(); err != nil {
		return PassResult{Cursor: c.heldCursor()}, err
	}

	cursor, err := c.loadCursor(ctx)
	if err != nil {
		return PassResult{Cursor: c.heldCursor()}, err
	}

	limit := c.host.ReconcileBatch()
	page, err := c.inbox.ListOrdered(ctx, c.key.TenantID, c.key.SessionID, cursor, limit)
	if err != nil {
		return PassResult{Cursor: cursor}, err
	}
	if err := c.validatePage(page, cursor, limit); err != nil {
		return PassResult{Cursor: cursor}, err
	}

	result := PassResult{Cursor: cursor, Examined: len(page)}
	consumed := cursor
	for _, record := range page {
		// A TERMINAL RECORD IS NOT HANDED ON. §10.4 makes applied and rejected
		// mutually exclusive terminal CAS states, so there is no work left and
		// no state a Processor could move it to. Passing it anyway would ask
		// O4.2 to re-derive that conclusion once per pass for every command a
		// long-lived session ever accepted.
		if record.State.Terminal() {
			consumed = record.AcceptedOrder
			result.Consumed++
			continue
		}
		// ABORTED PASSES WRITE NOTHING. Both of these mean the pass must not
		// continue, and neither may be followed by a cursor write: a cancelled
		// context is not a durable-write context, and a lost grant must not
		// touch a fenced record at all.
		//
		// WHAT RE-DERIVING COSTS DEPENDS ON THE RECORD, and an earlier version
		// of this comment claimed it cost "a terminal-state check per record
		// and no application", which is true of only half of them. A record the
		// Processor drove to applied or rejected is skipped by the terminal
		// short-circuit above and costs that check. A record consumed because
		// its application prefix was OWNED is still `applying`, is not
		// terminal, and IS handed to the Processor again. That is correct and
		// §10.4 requires it — recovering an expired applying prefix is
		// continuation of an existing application, not a new claim — but it is
		// an application call, not a state check, and the sentence said
		// otherwise.
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := c.fence.Held(); err != nil {
			return result, err
		}
		outcome, err := c.processor.Process(ctx, record)
		if err != nil {
			result.Blocked = &BlockedCommand{
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				State:         record.State,
				Cause:         err,
			}
			break
		}
		// THE CURSOR RULE, both halves, in one place. Anything else stops the
		// pass HERE — the successors of a pending predecessor are not applied,
		// because the acceptance order is the order.
		if !outcome.State.Terminal() && !outcome.PrefixOwned {
			result.Blocked = &BlockedCommand{
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				State:         outcome.State,
			}
			break
		}
		consumed = record.AcceptedOrder
		result.Consumed++
	}

	if consumed > cursor {
		if err := c.fence.Write(func() error {
			return c.cursors.SaveCursor(ctx, c.key.TenantID, c.key.SessionID, c.epoch, consumed)
		}); err != nil {
			// THE IN-MEMORY CURSOR DOES NOT MOVE. A cursor advanced on a write
			// nobody acknowledged would make the next pass list from a position
			// no store agreed to, and every command in the gap would be
			// consumed by nobody.
			return result, err
		}
		c.setCursor(consumed)
		result.Cursor = consumed
	}
	// "FULL" IS len(page) == limit AND NOTHING ELSE, which is a well-defined
	// test only because the limit is positive. At zero an EMPTY page satisfies
	// it, Reconcile reports More, Run takes the immediate continuation, no
	// timer is ever armed and the loop spins hot on an empty inbox forever.
	//
	// This carried a `limit > 0` conjunct, and an earlier version of this
	// comment deleted it as unreachable-false on the ground that host.New
	// refuses a non-positive ReconcileBatch and "there is no other way to
	// obtain a *host.Host". THAT WAS WRONG, and wrong in the way that matters:
	// unexported fields restrict naming a field, not the empty composite
	// literal, so &host.Host{} compiles from any package and returns a zero
	// batch. The equivalence had been established over the INTENDED
	// construction path rather than over every construction path, which is
	// exactly the error this program keeps paying for.
	//
	// The invariant now lives at the boundary that consumes it: NewConsumer
	// refuses a Host whose ReconcileBatch is not positive, so no Consumer can
	// reach this line with a zero limit. That check is inside this package,
	// a test reaches it, and a mutation deleting it dies — which is what the
	// conjunct never managed.
	result.More = result.Blocked == nil && len(page) == limit
	return result, nil
}

// loadCursor reads the durable cursor and refuses one that went backwards.
func (c *Consumer) loadCursor(ctx context.Context) (uint64, error) {
	stored, err := c.cursors.LoadCursor(ctx, c.key.TenantID, c.key.SessionID)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	held, loaded := c.cursor, c.loaded
	c.mu.Unlock()
	if loaded && stored < held {
		return 0, &CursorRegressionError{Key: c.key, Loaded: stored, Held: held}
	}
	c.mu.Lock()
	c.cursor, c.loaded = stored, true
	c.mu.Unlock()
	return stored, nil
}

// heldCursor is the greatest cursor this consumer has had acknowledged, or zero
// when it has never had one. See PassResult.Cursor.
func (c *Consumer) heldCursor() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

// setCursor records an acknowledged cursor.
func (c *Consumer) setCursor(order uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor, c.loaded = order, true
}

// validatePage checks the whole page BEFORE any of it is applied.
//
// The checks are over the page as the store returned it, in the order a reader
// would ask them: is it the right size, is each record this session's, is each
// identity one Core accepts, is each state one this package can reason about,
// and does the acceptance order strictly increase from the cursor. The last is
// split in two because "at or below the cursor" and "does not increase" are
// different store defects with different repairs.
func (c *Consumer) validatePage(page []Command, cursor uint64, limit int) error {
	if len(page) > limit {
		return &PageError{
			Problem: PageProblemOverLimit,
			Index:   -1,
			Reason:  "the page holds " + strconv.Itoa(len(page)) + " records for a limit of " + strconv.Itoa(limit),
		}
	}
	previous := cursor
	seen := make(map[sessionwire.CommandID]int, len(page))
	for index, record := range page {
		if record.TenantID != c.key.TenantID || record.SessionID != c.key.SessionID {
			return &PageError{
				Problem:       PageProblemForeignSession,
				Index:         index,
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				Reason: "the record is scoped to (" + strconv.Quote(string(record.TenantID)) + ", " + strconv.Quote(string(record.SessionID)) +
					") and this consumer owns (" + strconv.Quote(string(c.key.TenantID)) + ", " + strconv.Quote(string(c.key.SessionID)) + ")",
			}
		}
		if err := record.CommandID.Validate(); err != nil {
			return &PageError{
				Problem:       PageProblemInvalidCommandID,
				Index:         index,
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				Reason:        "Core refuses the record's public command identity",
				Cause:         err,
			}
		}
		if !record.State.known() {
			return &PageError{
				Problem:       PageProblemUnknownState,
				Index:         index,
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				Reason:        "the durable state " + strconv.Quote(string(record.State)) + " is not one of this package's five",
			}
		}
		if index == 0 {
			if record.AcceptedOrder <= cursor {
				return &PageError{
					Problem:       PageProblemNotAfterCursor,
					Index:         index,
					CommandID:     record.CommandID,
					AcceptedOrder: record.AcceptedOrder,
					Reason: "the record's acceptance order " + strconv.FormatUint(record.AcceptedOrder, 10) +
						" is not after the cursor " + strconv.FormatUint(cursor, 10) + " the page was requested from",
				}
			}
		} else if record.AcceptedOrder <= previous {
			return &PageError{
				Problem:       PageProblemNotMonotonic,
				Index:         index,
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				Reason: "the record's acceptance order " + strconv.FormatUint(record.AcceptedOrder, 10) +
					" does not increase on its predecessor's " + strconv.FormatUint(previous, 10),
			}
		}
		// LAST, DELIBERATELY. A page carrying one record twice breaks the
		// order rule as well — the copy cannot strictly increase on itself —
		// and the order violation is the more specific finding, so it is
		// reported first. This check exists for the case the order rule cannot
		// see: the same CommandID at two DIFFERENT, strictly increasing orders.
		if first, duplicate := seen[record.CommandID]; duplicate {
			return &PageError{
				Problem:       PageProblemDuplicateCommandID,
				Index:         index,
				CommandID:     record.CommandID,
				AcceptedOrder: record.AcceptedOrder,
				Reason: "the page already carries this command at index " + strconv.Itoa(first) +
					"; CommandID is the inbox's stable key and cannot name two records",
			}
		}
		seen[record.CommandID] = index
		previous = record.AcceptedOrder
	}
	return nil
}
