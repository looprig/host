// Applying an admitted command is §10.4's durable protocol around ONE call
// into a runtime, and this file is that protocol. The consumer decided which
// command is next; everything from here is about doing it exactly once.
//
// THE SHAPE IS FIXED BY THE STATE MACHINE, not chosen: a command is claimed
// under the current lease epoch, its private payload is loaded from
// SessionStore only after that claim, it is revalidated, the application prefix
// that correlates CommandID and RuntimeCommandID with the epoch is committed
// BEFORE the runtime-visible effect begins, and the record is then terminally
// CASed to one of two mutually exclusive states. Every one of those steps
// exists because the process can stop between any two of them.
//
// WHAT CROSSES INTO THE RUNTIME IS A sessionwire.CommandEnvelope AND NOTHING
// ELSE. That is step 4's "never send raw private payload" as a type rather than
// as a rule: the envelope carries a wire version and a public CommandID, so
// there is no field a payload could travel in even by accident.
package commands

import (
	"context"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// The durable record, in full
// ---------------------------------------------------------------------------

// Kind is the command kind §10.4's SessionInbox record carries.
//
// It is Host's own enumeration rather than a projection of a Core request type,
// because what Host branches on is the CONSEQUENCE — whether a command is
// driven into the runtime, satisfied by residency itself, or revalidated
// against durable gate state first — and Core's records are shaped for
// admission, not for that.
type Kind string

const (
	// KindCreate and KindRestore are the two commands whose consequence is
	// RESIDENCY. By the time one reaches this Host the runtime it asks for
	// exists, because a Host holds an admitted session's runtime before it
	// consumes that session's inbox at all.
	KindCreate  Kind = "create"
	KindRestore Kind = "restore"

	// KindInput carries private input blocks for a resident runtime.
	KindInput Kind = "input"

	// KindInterrupt asks the resident runtime to interrupt.
	KindInterrupt Kind = "interrupt"

	// KindGateResponse answers one open durable gate.
	KindGateResponse Kind = "gate_response"
)

// known reports whether a kind is one this Host can apply.
//
// AN UNKNOWN KIND IS NOT REJECTED, and that asymmetry with State.known is
// deliberate. A newer Factory may admit a kind a newer Host applies and this
// one does not; rejecting it here would make an older Host in a rolling
// deployment durably destroy work a newer one would have done. Refusing leaves
// the record for a Host that understands it, and §10.4 already bounds how long
// that can last: the apply deadline turns an unapplied command into
// rejected/runtime_unavailable through Factory's deadline reconciler.
func (k Kind) known() bool {
	switch k {
	case KindCreate, KindRestore, KindInput, KindInterrupt, KindGateResponse:
		return true
	default:
		return false
	}
}

// drivesRuntime reports whether applying this kind means calling into the
// runtime. Create and restore do not: see KindCreate.
func (k Kind) drivesRuntime() bool {
	switch k {
	case KindCreate, KindRestore:
		return false
	default:
		return true
	}
}

// Record is the SessionInbox record §10.4 describes, as an applier reads it.
//
// IT IS THE AGGREGATE Command IS NOT. The consumer's Command is the ordering
// decision's three fields; this is what a claim, a deadline check, a
// correlation and a terminal CAS are made from — and it is read FRESH inside
// Process rather than taken from the page, because a page is a snapshot and a
// claim is a compare-and-swap against now.
//
// It carries no payload. That is not an omission: see Payload.
type Record struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	// RuntimeCommandID is the UUID the WINNING CreateOrdered record allocated.
	// §10.4 allows two Factory replicas to propose different UUIDs while racing
	// one public ID and makes only the winner's usable, so this is read from
	// the durable record and never minted here.
	RuntimeCommandID uuid.UUID

	Kind  Kind
	State State

	// AcceptedOrder is the immutable acceptance order, carried so a record read
	// fresh can be checked against the page record it came from.
	AcceptedOrder uint64

	// ApplyDeadline is the outer bound §10.4 puts on applying this command. A
	// NEW claim may not start at or after it; continuing an owned application
	// prefix may.
	ApplyDeadline time.Time

	// ClaimEpoch and ClaimExpiresAt are the current claim, or the zero values
	// when there is none.
	ClaimEpoch     uint64
	ClaimExpiresAt time.Time
}

// Payload is the PRIVATE part of the record, loaded from SessionStore after a
// claim and never sent anywhere.
//
// It is a separate load rather than a field of Record for one reason: step 4
// says the payload is loaded AFTER the claim, and a field on the record a claim
// is decided from would be loaded before it. Two calls make the ordering a
// thing a test can see.
type Payload struct {
	// GateID is the gate a gate response answers, and is what the durable
	// gate/owner recheck of §9.4 is run against.
	GateID sessionwire.GateID

	// Body is the opaque private request body. Host does not decode it —
	// Harness is the semantic validator — and it does not cross HostLink or the
	// runtime seam.
	Body []byte
}

// Prefix is §10.4's private application-prefix record, which correlates a
// command with its runtime allocation BEFORE the runtime-visible effect begins.
//
// ITS EXISTENCE IS THE WHOLE RECOVERY SIGNAL. A crash before it means no effect
// began and the command may be applied afresh; a crash after it means an effect
// may have begun and the next lease holder finishes the durable prefix rather
// than driving the runtime again. Both identities are carried because §10.4
// correlates both, and a prefix that names one of them but not the other
// correlates nothing.
type Prefix struct {
	CommandID        sessionwire.CommandID
	RuntimeCommandID uuid.UUID
	LeaseEpoch       uint64
}

// Gate is the durable gate state §9.4's release-race backstop is decided from.
type Gate struct {
	GateID sessionwire.GateID

	// Open reports an unresolved, unexpired durable gate.
	Open bool

	// OwnerHostID and OwnerEpoch are the resident owner the gate was opened
	// under. A gate response is resumable only while both still name this
	// Host's current residency, because continuation is out of scope: a runtime
	// that was released and relaunched no longer holds what the gate suspended.
	OwnerHostID sessionwire.HostID
	OwnerEpoch  uint64
}

// Claim is one compare-and-swap into StateClaimed.
//
// THE EXPECTED FIELDS ARE THE EXACTLY-ONCE MECHANISM, not bookkeeping. Two
// Hosts that both read a pending record and both decide to apply it are made
// safe by the store refusing the second CAS, and a claim that did not say what
// it expected could not be refused.
type Claim struct {
	// ExpectedState and ExpectedClaimEpoch are the record as it was read.
	ExpectedState      State
	ExpectedClaimEpoch uint64

	// Epoch is the lease epoch the claim is taken under, and ExpiresAt is its
	// bounded TTL. §10.4: a claim never extends the command's apply deadline.
	Epoch     uint64
	ExpiresAt time.Time
}

// ---------------------------------------------------------------------------
// The terminal result
// ---------------------------------------------------------------------------

// Result is one of §10.4's two MUTUALLY EXCLUSIVE terminal CAS states.
//
// MUTUAL EXCLUSION IS A MECHANISM HERE AND NOT A RULE: the state is ONE
// unexported field holding one of two values, so no Result can carry both, and
// no caller outside this package can build one that does — the constructors are
// the only way to set it. What a caller CAN build is the zero value, because
// Result{} compiles from any package however unexported its fields are; that is
// what Validate is for, and finalization refuses an invalid one before it
// writes.
type Result struct {
	state   State
	reason  sessionwire.ErrorCode
	message string
}

// Applied returns the applied terminal result.
func Applied() Result { return Result{state: StateApplied} }

// Rejected returns the rejected terminal result, carrying the typed public
// reason a caller polling CommandStatus will read.
func Rejected(reason sessionwire.ErrorCode, message string) Result {
	return Result{state: StateRejected, reason: reason, message: message}
}

// State reports which of the two terminal states this result commits.
func (r Result) State() State { return r.state }

// Rejection reports the typed reason and message, and whether this result is a
// rejection at all.
func (r Result) Rejection() (sessionwire.ErrorCode, string, bool) {
	if r.state != StateRejected {
		return "", "", false
	}
	return r.reason, r.message, true
}

// Validate refuses a Result that is not exactly one of the two terminal states
// with the fields that state requires.
func (r Result) Validate() error {
	switch r.state {
	case StateApplied:
		if r.reason != "" || r.message != "" {
			return &ApplyError{Refusal: RefusalInvalidResult, Reason: "an applied result carries a rejection reason"}
		}
		return nil
	case StateRejected:
		if r.reason == "" {
			return &ApplyError{Refusal: RefusalInvalidResult, Reason: "a rejected result carries no typed reason, and §10.4 makes the reason part of the terminal state"}
		}
		return nil
	default:
		return &ApplyError{
			Refusal: RefusalInvalidResult,
			Reason:  "the state " + strconv.Quote(string(r.state)) + " is not one of the two terminal states; a Result must come from Applied or Rejected",
		}
	}
}

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------
//
// The reads and the writes are SEPARATE interfaces over the same durable
// object, which is the discipline O4.1 recorded: the claim and the terminal CAS
// write the same SessionInbox record Inbox lists, under the same
// one-writer-per-lease rule, and the split is about fencing rather than about
// ownership. Every method of a `…Writes` interface is a durable write and goes
// through Fence.Write; TestEveryDurableWriteGoesThroughTheFence holds that
// structurally, and it derives the method set from these declarations rather
// than from a list, so a method added here is covered without anyone
// remembering to widen a guard.

// CommandRecords reads the private durable command state.
type CommandRecords interface {
	// LoadCommand returns the full inbox record for one command.
	LoadCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Record, error)

	// LoadPayload returns the private payload or dereferenced object body. It
	// is called only after a claim has been committed.
	LoadPayload(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Payload, error)

	// LoadApplicationPrefix reports the durable application-prefix correlation
	// for a command, and whether one exists at all.
	LoadApplicationPrefix(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Prefix, bool, error)
}

// Gates reads durable gate state for the §9.4 release-race backstop.
type Gates interface {
	// LoadGate returns the durable gate and whether it exists.
	LoadGate(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, gate sessionwire.GateID) (Gate, bool, error)
}

// InboxWrites is every durable write an applier makes to the command aggregate.
//
// EVERY METHOD HERE IS FENCED. The name is load-bearing: the structural guard
// collects the methods of each interface in this package whose name ends in
// "Writes" and requires every call to one of them to be lexically inside a
// Fence.Write. A durable write added to this interface is guarded the moment it
// is declared; a durable write declared on a seam named otherwise is not, which
// is the guard's one stated limit.
type InboxWrites interface {
	// ClaimCommand CASes a record into StateClaimed under a lease epoch. The
	// implementation refuses the CAS when the record no longer matches the
	// claim's expected state and claim epoch, and returns
	// residency.ErrEpochSuperseded when a later epoch has committed.
	ClaimCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, claim Claim) error

	// BeginApplying CASes a claimed record into StateApplying. §10.4 permits
	// this only for the current lease holder, with the runtime ready and
	// application beginning immediately; it is not a placement reservation.
	BeginApplying(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, epoch uint64, expiresAt time.Time) error

	// RecordApplicationPrefix commits the private correlation record before the
	// runtime-visible effect begins.
	RecordApplicationPrefix(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, prefix Prefix) error

	// FinalizeCommand performs the terminal CAS. THERE IS NO due_at PARAMETER,
	// and that is how §10.4's "terminal records become not_due" is held on this
	// side of the seam: the same revision that commits the terminal state sets
	// due_at=not_due, and no caller can ask for anything else.
	FinalizeCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, epoch uint64, result Result) error
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

// ApplyRefusal is a stable, machine-readable reason an applier declined to
// finish a command. It is a code rather than a sentence for the reason
// PageProblem is.
//
// A REFUSAL IS NOT A REJECTION. Every value here leaves the durable record
// where it was and blocks the pass at that command, which the consumer names in
// PassResult.Blocked. The one durable rejection this package writes is
// gate_not_resumable, and it is a Result rather than a refusal.
type ApplyRefusal string

const (
	// RefusalStaleEpoch reports a record already claimed under an epoch later
	// than this applier's, which means this Host no longer owns the session.
	RefusalStaleEpoch ApplyRefusal = "stale_epoch"

	// RefusalClaimHeld reports another epoch's unexpired claim. §10.4 lets an
	// unexpired claim win.
	RefusalClaimHeld ApplyRefusal = "claim_held"

	// RefusalDeadlinePassed reports a new claim refused at or after the apply
	// deadline. Factory's deadline reconciler is what makes such a record
	// terminal; a Host never rejects for a deadline it merely observed.
	RefusalDeadlinePassed ApplyRefusal = "apply_deadline_passed"

	// RefusalUnsupportedKind reports a command kind this Host cannot apply.
	RefusalUnsupportedKind ApplyRefusal = "unsupported_kind"

	// RefusalUnknownState reports a durable state this package cannot reason
	// about.
	RefusalUnknownState ApplyRefusal = "unknown_state"

	// RefusalForeignRecord reports a record that is not the one asked for, or
	// not this session's.
	RefusalForeignRecord ApplyRefusal = "foreign_record"

	// RefusalCorrelation reports a record or prefix whose identities do not
	// correlate: an unallocated RuntimeCommandID, or a prefix naming a
	// different command or runtime command.
	RefusalCorrelation ApplyRefusal = "correlation_mismatch"

	// RefusalMissingPayload reports a kind whose private payload is required
	// and absent.
	RefusalMissingPayload ApplyRefusal = "missing_payload"

	// RefusalInvalidResult reports a terminal result that is not exactly one of
	// the two terminal states.
	RefusalInvalidResult ApplyRefusal = "invalid_result"

	// RefusalStore reports an ambiguous durable-store failure.
	RefusalStore ApplyRefusal = "store_failure"

	// RefusalRuntime reports that the runtime refused or failed the command
	// after its application prefix was committed.
	RefusalRuntime ApplyRefusal = "runtime_failure"
)

// ApplyError reports why an applier declined to finish a command.
type ApplyError struct {
	Refusal   ApplyRefusal
	CommandID sessionwire.CommandID
	Reason    string
	Cause     error
}

func (e *ApplyError) Error() string {
	message := "commands: the command " + strconv.Quote(string(e.CommandID)) + " was not applied (" + string(e.Refusal) + "): " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the typed cause a lower layer produced, if any.
func (e *ApplyError) Unwrap() error { return e.Cause }

// ---------------------------------------------------------------------------
// Applier
// ---------------------------------------------------------------------------

// ApplierOptions configures an Applier.
type ApplierOptions struct {
	// Host supplies the clock, the claim TTL and this Host's identity. Nothing
	// here restates a value host.New already checked.
	Host *host.Host

	// Key is the session whose commands this applier applies.
	Key registry.Key

	// LeaseEpoch is the grant every durable write is stamped with.
	LeaseEpoch uint64

	Records CommandRecords
	Writes  InboxWrites
	Gates   Gates

	// Runtime is Host's control path into the resident runtime. Its argument
	// type is the mechanism behind step 4: a CommandEnvelope has nowhere to put
	// a private payload.
	Runtime department.CommandApplier

	// Fence is the lease-epoch guard. AN APPLIER HAS ITS OWN, and that is the
	// answer to the hand-off O4.1 left on Processor: the applier's claim,
	// prefix and terminal CAS are fenced writes, and they are fenced by the
	// same one mechanism the cursor write goes through rather than by a second
	// discipline.
	Fence Fence
}

// Applier claims, applies, recovers and finalizes one command at a time. It is
// the Processor the consumer hands non-terminal records to.
type Applier struct {
	host    *host.Host
	key     registry.Key
	epoch   uint64
	records CommandRecords
	writes  InboxWrites
	gates   Gates
	runtime department.CommandApplier
	fence   Fence
}

// The applier is the seam O4.1 declared.
var _ Processor = (*Applier)(nil)

// NewApplier validates options and returns an Applier.
func NewApplier(options ApplierOptions) (*Applier, error) {
	if options.Host == nil {
		return nil, &InvalidConsumerOptionsError{Field: "Host", Reason: "must be set; the clock, the claim TTL and this Host's identity all come from it"}
	}
	// THE ACCESSORS THIS TYPE READS, AND ONLY THOSE. Same reason NewConsumer
	// has its own list: &host.Host{} compiles from any package and has none of
	// them set, so a nil check alone accepts a Host with a nil clock and a zero
	// claim TTL — and a zero TTL claims a command that has already expired.
	var unusable []string
	if options.Host.Clock() == nil {
		unusable = append(unusable, "Clock")
	}
	if options.Host.ClaimTTL() <= 0 {
		unusable = append(unusable, "ClaimTTL")
	}
	if options.Host.ID() == "" {
		unusable = append(unusable, "ID")
	}
	if len(unusable) > 0 {
		return nil, &UnusableHostError{Accessors: unusable}
	}
	if options.Key.TenantID == "" || options.Key.SessionID == "" {
		return nil, &InvalidConsumerOptionsError{Field: "Key", Reason: "must name both a tenant and a session; every SessionStore operation is scoped by the pair"}
	}
	if options.LeaseEpoch == 0 {
		return nil, &InvalidConsumerOptionsError{Field: "LeaseEpoch", Reason: "must be non-zero; a claim stamped with a zero epoch is unfenced"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Records", options.Records != nil},
		{"Writes", options.Writes != nil},
		{"Gates", options.Gates != nil},
		{"Runtime", options.Runtime != nil},
		{"Fence", options.Fence != nil},
	} {
		if !required.present {
			return nil, &InvalidConsumerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	return &Applier{
		host:    options.Host,
		key:     options.Key,
		epoch:   options.LeaseEpoch,
		records: options.Records,
		writes:  options.Writes,
		gates:   options.Gates,
		runtime: options.Runtime,
		fence:   options.Fence,
	}, nil
}

// Process claims, applies, recovers or finalizes one command.
//
// IT READS THE RECORD FRESH. The Command it is handed is the ordering
// decision's three fields, taken from a page that may be a whole pass old; a
// claim is a compare-and-swap against the record as it is now, and a deadline
// is compared against the clock as it is now.
func (a *Applier) Process(ctx context.Context, command Command) (Outcome, error) {
	record, err := a.records.LoadCommand(ctx, a.key.TenantID, a.key.SessionID, command.CommandID)
	if err != nil {
		return Outcome{State: command.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: command.CommandID,
			Reason:    "the durable record could not be read",
			Cause:     err,
		}
	}
	if problem := a.checkRecord(record, command); problem != nil {
		return Outcome{State: command.State}, problem
	}
	// A RECORD THAT IS ALREADY TERMINAL IS REPORTED, NOT WRITTEN. The page said
	// it was not, which means somebody finished it in between — a predecessor
	// Host, or Factory's deadline reconciler. §10.4 never re-opens a terminal
	// record, so the only thing left is to tell the consumer what it became.
	if record.State.Terminal() {
		return Outcome{State: record.State}, nil
	}

	prefix, owned, err := a.records.LoadApplicationPrefix(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the application prefix could not be read, so it is unknown whether an effect already began",
			Cause:     err,
		}
	}

	step, err := a.plan(record, prefix, owned, a.now())
	if err != nil {
		return Outcome{State: record.State}, err
	}
	switch step {
	case stepFinishPrefix:
		return a.finishPrefix(ctx, record)
	default:
		return a.applyFresh(ctx, record)
	}
}

// checkRecord refuses a record that is not the one this pass asked for.
//
// The store answering with someone else's record is the same class of defect
// validatePage refuses a foreign page for, and it arrives on a different path:
// there, a page; here, a keyed read. Both end with this Host writing a fenced
// CAS onto a record it never decided anything about.
func (a *Applier) checkRecord(record Record, command Command) error {
	switch {
	case record.TenantID != a.key.TenantID || record.SessionID != a.key.SessionID:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason: "the record is scoped to (" + strconv.Quote(string(record.TenantID)) + ", " + strconv.Quote(string(record.SessionID)) +
				") and this applier owns (" + strconv.Quote(string(a.key.TenantID)) + ", " + strconv.Quote(string(a.key.SessionID)) + ")",
		}
	case record.CommandID != command.CommandID:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason:    "the store answered with the record for " + strconv.Quote(string(record.CommandID)),
		}
	case record.AcceptedOrder != command.AcceptedOrder:
		return &ApplyError{
			Refusal:   RefusalForeignRecord,
			CommandID: command.CommandID,
			Reason: "the record's acceptance order " + strconv.FormatUint(record.AcceptedOrder, 10) +
				" is not the order " + strconv.FormatUint(command.AcceptedOrder, 10) + " this pass consumed it at, and the order is immutable",
		}
	case !record.State.known():
		return &ApplyError{
			Refusal:   RefusalUnknownState,
			CommandID: command.CommandID,
			Reason:    "the durable state " + strconv.Quote(string(record.State)) + " is not one of this package's five",
		}
	case record.RuntimeCommandID.IsZero():
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: command.CommandID,
			Reason:    "the record allocated no RuntimeCommandID, so no application prefix could correlate an effect with it",
		}
	default:
		return nil
	}
}

// applyFresh runs the whole protocol for a command no effect has begun for.
func (a *Applier) applyFresh(ctx context.Context, record Record) (Outcome, error) {
	claim := Claim{
		ExpectedState:      record.State,
		ExpectedClaimEpoch: record.ClaimEpoch,
		Epoch:              a.epoch,
		ExpiresAt:          a.now().Add(a.host.ClaimTTL()),
	}
	if err := a.fence.Write(func() error {
		return a.writes.ClaimCommand(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, claim)
	}); err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the claim was refused",
			Cause:     err,
		}
	}

	// AFTER THE CLAIM, AND ONLY AFTER IT. Step 4 in one line: the private
	// payload is loaded from SessionStore once this applier owns the command,
	// and it is never carried to it by whatever asked for the work.
	payload, err := a.records.LoadPayload(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the private payload could not be loaded",
			Cause:     err,
		}
	}
	if err := requiresPayload(record.Kind, payload); err != nil {
		return Outcome{State: StateClaimed}, err
	}

	// REVALIDATION BEFORE APPLYING, which §10.4 requires and §9.4 gives the one
	// concrete case for: a gate response that lost the release race is durably
	// rejected here rather than driven into a runtime that cannot resume it.
	if record.Kind == KindGateResponse {
		gate, found, err := a.gates.LoadGate(ctx, a.key.TenantID, a.key.SessionID, payload.GateID)
		if err != nil {
			return Outcome{State: StateClaimed}, &ApplyError{
				Refusal:   RefusalStore,
				CommandID: record.CommandID,
				Reason:    "the durable gate could not be read, so the release race could not be decided",
				Cause:     err,
			}
		}
		if reason, resumable := a.gateResumable(gate, found); !resumable {
			return a.finalize(ctx, record, Rejected(sessionwire.ErrorCodeGateNotResumable, reason), false)
		}
	}

	// A CREATE OR RESTORE IS SATISFIED BY RESIDENCY. Nothing is driven into the
	// runtime, so no runtime-visible effect begins, so there is no prefix to
	// correlate one with and no `applying` state to enter: the record goes
	// straight to the terminal CAS from `claimed`, which is the same transition
	// the rejection arm above takes.
	if !record.Kind.drivesRuntime() {
		return a.finalize(ctx, record, Applied(), false)
	}

	if err := a.fence.Write(func() error {
		return a.writes.BeginApplying(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, a.epoch, claim.ExpiresAt)
	}); err != nil {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the record could not be moved into applying",
			Cause:     err,
		}
	}
	// BEFORE THE EFFECT, NOT AFTER IT. A prefix written after the runtime call
	// would be exactly the correlation §10.4 needs and would be missing in the
	// only case it exists for: a crash during the call.
	if err := a.fence.Write(func() error {
		return a.writes.RecordApplicationPrefix(ctx, a.key.TenantID, a.key.SessionID, Prefix{
			CommandID:        record.CommandID,
			RuntimeCommandID: record.RuntimeCommandID,
			LeaseEpoch:       a.epoch,
		})
	}); err != nil {
		return Outcome{State: StateApplying}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the application prefix could not be committed, so the runtime was not driven",
			Cause:     err,
		}
	}

	if err := a.runtime.ApplyCommand(ctx, sessionwire.CommandEnvelope{
		Version:   sessionwire.CurrentWireVersion,
		CommandID: record.CommandID,
	}); err != nil {
		// THE PREFIX IS COMMITTED, so this is reported as an owned prefix and
		// as a failure at the same time, and both halves are true. Past this
		// point a failure is indistinguishable from a crash — that is what the
		// prefix exists to say — so §10.4's answer applies to both: the next
		// pass finishes the durable prefix rather than driving the runtime
		// again. What is NOT the same is that this one has a cause, and the
		// consumer carries it into PassResult.Blocked where an operator reads
		// it.
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalRuntime,
			CommandID: record.CommandID,
			Reason:    "the runtime failed the command after its application prefix was committed",
			Cause:     err,
		}
	}
	return a.finalize(ctx, record, Applied(), true)
}

// gateResumable applies §9.4's backstop and reports why it did not.
func (a *Applier) gateResumable(gate Gate, found bool) (string, bool) {
	switch {
	case !found:
		return "no durable gate exists for this response", false
	case !gate.Open:
		return "the durable gate is no longer open", false
	case gate.OwnerHostID != a.host.ID():
		return "the durable gate is owned by another Host", false
	case gate.OwnerEpoch != a.epoch:
		return "the durable gate was opened under an earlier residency of this session, and continuation is not implemented", false
	default:
		return "", true
	}
}

// requiresPayload refuses a kind whose private payload is required and absent.
func requiresPayload(kind Kind, payload Payload) error {
	switch kind {
	case KindInput:
		if len(payload.Body) == 0 {
			return &ApplyError{Refusal: RefusalMissingPayload, Reason: "an input command carries no private payload"}
		}
	case KindGateResponse:
		if payload.GateID == "" {
			return &ApplyError{Refusal: RefusalMissingPayload, Reason: "a gate response names no gate, so the release race cannot be decided"}
		}
		if len(payload.Body) == 0 {
			return &ApplyError{Refusal: RefusalMissingPayload, Reason: "a gate response carries no private payload"}
		}
	}
	return nil
}

// finalize performs the terminal CAS and reports what the consumer may
// conclude.
//
// prefixOwned decides what an AMBIGUOUS store failure means, and the two
// answers are different in kind. With a prefix committed, the command is
// recoverable by whoever holds the lease next whether or not this CAS landed,
// which is exactly Outcome.PrefixOwned's meaning, so the pass may pass it. With
// no prefix — a create, a restore, a gate rejection — nothing correlates
// anything, so the pass stops and the next one re-derives the same conclusion
// from the same durable state.
//
// A REFUSED FENCED WRITE LANDS HERE TOO, and reporting an owned prefix for it is
// still true: the prefix is committed whatever the reason the terminal CAS did
// not. It does not let a pass carry on under a lease it has lost, because that
// is not this function's to enforce and is enforced twice over — the consumer
// consults the fence between records, and its own cursor write goes through the
// same fence, so nothing durable moves.
func (a *Applier) finalize(ctx context.Context, record Record, result Result, prefixOwned bool) (Outcome, error) {
	if err := result.Validate(); err != nil {
		return Outcome{State: record.State}, err
	}
	if err := a.fence.Write(func() error {
		return a.writes.FinalizeCommand(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, a.epoch, result)
	}); err != nil {
		if prefixOwned {
			return Outcome{State: StateApplying, PrefixOwned: true}, nil
		}
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the terminal compare-and-swap was refused",
			Cause:     err,
		}
	}
	return Outcome{State: result.State()}, nil
}

// now is the clock the deadline and the claim TTL are read against.
func (a *Applier) now() time.Time { return a.host.Clock().Now() }
