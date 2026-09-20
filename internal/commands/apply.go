// Applying an admitted command is §10.4's durable protocol around ONE call into
// a runtime, and this file is that protocol. The consumer decided which command
// is next; everything from here is about doing it exactly once.
//
// THE SHAPE IS FIXED BY THE STATE MACHINE, not chosen: a command is claimed
// under the current lease epoch, its private payload is loaded from SessionStore
// only after that claim, it is revalidated, it enters applying, the application
// prefix that correlates CommandID and RuntimeCommandID with the epoch is
// appended to the journal BEFORE the runtime-visible effect, the runtime is
// driven, and the record is then terminally settled. Every one of those steps
// exists because the process can stop between any two of them.
//
// THE SEAMS ARE NARROW LOCAL INTERFACES OVER METHODS THAT EXIST IN THE LEGACY
// FAMILY, AND ONLY THERE. That qualification is the whole of this paragraph and
// an earlier version of it was missing — it said flatly that "sessionstore
// v0.6.0 HAS all of them", which is true of one family and misleads exactly the
// reader trying to work out why apply does not work in disposition mode.
//
// sessionstore v0.7.0 has all of them for a LEGACY session — GetCommand, the
// inbox record's private Payload/PayloadRef with GetObject behind a reference,
// FindCommandApplication, ReadGates, ClaimCommand, BeginApplyingCommand,
// CompleteCommand, RejectCommand, and the journal's application-prefix envelope
// — and the shapes below are modelled on those rather than invented beside them.
// They are declared locally for the reason O4.1 declared Inbox and Cursors
// locally: naming a looprig module in go.mod is the same decision as depending
// on it, and that decision is a release-ordering commitment recorded elsewhere.
// What O4.1's declaration could also say, and this one cannot, is that the
// module lacks the operation; consulting the module is what shows a shape is
// wrong, and not consulting it is what let a terminal transition the store
// refuses sit behind a green test.
//
// A HOST CANNOT REACH THAT FAMILY. A Host takes residency through
// AcquireResidency, which pins ProtocolModeDisposition, and a disposition
// session refuses the journal grant this protocol appends its prefix under. So
// every seam below is bound to calls that exist and that this Host cannot use on
// a session it can actually hold. The disposition family has no counterpart to
// bind to yet either: it has NO CLAIM EDGE — pending -> claimed has no entry
// point and there is no in-package writer of a DispositionClaim at all — and it
// settles from durable evidence the caller does not supply, which harness
// v0.33.0 cannot produce because runtimecommand.Admitted carries no attempt
// identity. Closing this is a three-repository sequence and not a Host change;
// the README's limitations section is where that is stated for a release reader.
//
// ONE CONSEQUENCE BELONGS HERE RATHER THAN ONLY THERE, because this file is
// where "exactly once" is claimed. In disposition mode THE STORE DECIDES THE
// TERMINAL ARM, so there is no Host-authored rejection once an attempt exists,
// and MISSING EVIDENCE IS NOT PROOF THAT NOTHING WAS APPLIED — a Host must never
// re-dispatch on its absence. The honest claim for a disposition Host is "at
// most one authorized attempt, settled only from the runtime's durable
// disposition", which is narrower than what this file's legacy shapes express.
//
// WHAT SETTLES A COMMAND AS APPLIED IS DURABLE EVIDENCE, not this Host's
// confidence. §10.4 ends an application at a correlated journal effect and the
// store requires the terminal write to NAME that effect, so the applier reads
// the correlation back after driving the runtime and completes with what the
// journal says. A command whose runtime call returned while committing nothing
// is not applied, and saying so is the difference between exactly-once and a
// durable no-op recorded as success.
package commands

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// The durable record, in full
// ---------------------------------------------------------------------------

// Kind is the command kind §10.4's SessionInbox record carries.
//
// It is Host's own enumeration rather than a projection of a Core request type,
// because what Host branches on is what it must REVALIDATE before applying, and
// Core's records are shaped for admission rather than for that. It travels to
// the runtime opaque: see department.RuntimeCommand.
type Kind string

const (
	// KindCreate and KindRestore ask for a session that, by the time the
	// command reaches this Host, exists. They are still DRIVEN INTO THE RUNTIME
	// like every other kind — see the note on Process — because what settles a
	// command is a correlated durable effect, and residency is not one.
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
// deliberate. A newer Factory may admit a kind a newer Host applies and this one
// does not; rejecting it here would make an older Host in a rolling deployment
// durably destroy work a newer one would have done. Refusing leaves the record
// for a Host that understands it, and §10.4 bounds how long that can last: the
// apply deadline turns the command into rejected/runtime_unavailable through
// Factory's deadline reconciler.
//
// THE DEADLINE BOUNDS AN UNATTEMPTED COMMAND AND NOTHING ELSE, and the sentence
// above used to say "an unapplied command", which is wider than the sweep and
// was wrong in the one case that mattered. A deadline sweep SKIPS a record that
// carries a durably authorized attempt, because such a record may have a
// committed effect and only the runtime's own evidence -- or a successor's
// recovery closure -- may settle it. So a kind refused HERE, before any write,
// really is bounded by the deadline; a kind refused at the RUNTIME, after the
// attempt is durable, is not bounded by anything. That is precisely what
// harness v0.36.0 was released to end: host v0.4.0's adapter refused create and
// restore after this applier's disposition sibling had already begun the
// attempt, so every session Factory created sat `applying` for good and the
// consumer never advanced past its FIRST command. Nothing about the tolerance
// here saved it, and nothing here would save the next such kind either.
func (k Kind) known() bool {
	switch k {
	case KindCreate, KindRestore, KindInput, KindInterrupt, KindGateResponse:
		return true
	default:
		return false
	}
}

// Record is the SessionInbox record §10.4 describes, as an applier reads it.
//
// IT IS THE AGGREGATE Command IS NOT. The consumer's Command is the ordering
// decision's three fields; this is what a claim, a deadline check, a correlation
// and a terminal settlement are made from — and it is read FRESH inside Process
// rather than taken from the page, because a page is a snapshot and every
// transition below is a compare-and-swap against the record as it is now.
//
// It carries no payload. That is not an omission: see Payload.
type Record struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	// RuntimeCommandID is the UUID the WINNING acceptance record allocated.
	// §10.4 allows two Factory replicas to propose different UUIDs while racing
	// one public ID and makes only the winner's usable, so this is read from the
	// durable record, never minted here, and §16 forwards it to the runtime.
	RuntimeCommandID uuid.UUID

	Kind  Kind
	State State

	// AcceptedOrder is the immutable acceptance order, carried so a record read
	// fresh can be checked against the page record it came from.
	AcceptedOrder uint64

	// Revision is the provider revision every compare-and-swap names.
	//
	// IT IS REQUIRED, NOT AN OPTIMISATION. A transition is a decision about a
	// record the caller has read; one that named no revision would be a blind
	// write dressed as a compare-and-swap, and two writers that both read a
	// pending record would both succeed.
	Revision uint64

	// ApplyDeadline is the outer bound §10.4 puts on applying this command. A
	// NEW claim may not start at or after it; continuing an application may.
	ApplyDeadline time.Time

	// ClaimEpoch and ClaimExpiresAt are the current claim, or the zero values
	// when there is none. A terminal record keeps the claim that settled it.
	ClaimEpoch     uint64
	ClaimExpiresAt time.Time
}

// Payload is the PRIVATE part of the record, loaded from SessionStore after a
// claim and handed to the runtime opaque.
//
// It is a separate load rather than a field of Record for one reason: step 4
// says the payload is loaded AFTER the claim, and a field on the record a claim
// is decided from would be loaded before it. Two calls make the ordering a thing
// a test can see.
//
// AT MOST ONE OF THE TWO IS SET. §10.1 gives a private body an independent
// immutable object reference once it exceeds its inline threshold; Host passes
// the reference on rather than dereferencing it, so a large body never travels
// through Host's memory.
type Payload struct {
	// Body is the inline private request body.
	Body []byte

	// Ref is the object reference a body too large to inline is stored behind.
	Ref sessionwire.ObjectReference
}

// present reports whether the command has a private body at all, in either
// form.
func (p Payload) present() bool { return len(p.Body) > 0 || p.Ref != (sessionwire.ObjectReference{}) }

// wellFormed reports whether at most one of the two forms is set, which is the
// store's own invariant restated where this package consumes it.
func (p Payload) wellFormed() bool {
	return len(p.Body) == 0 || p.Ref == (sessionwire.ObjectReference{})
}

// Prefix is §10.4's private application-prefix record, which correlates a
// command with its runtime allocation before the runtime-visible effect begins.
//
// ITS EXISTENCE IS THE WHOLE RECOVERY SIGNAL. A crash before it means no effect
// began and the command may be applied afresh; a crash after it means an effect
// may have begun, and what happened is then a question for the journal rather
// than for the record's state.
//
// IT CARRIES NO LEASE EPOCH, and that is stronger than carrying one: the journal
// writer stamps its own grant onto the record and refuses one a caller chose, so
// the epoch on a prefix is the epoch that actually held the stream rather than
// the epoch a caller believed it held.
type Prefix struct {
	CommandID        sessionwire.CommandID
	RuntimeCommandID uuid.UUID
	Kind             Kind
}

// Effect is the durable journal event that carried a command's effect, which is
// what a terminal application RECORDS.
//
// "Applied" with no event is a claim that something happened with nothing to
// point at, so this is required rather than optional, and it is read out of the
// journal correlation rather than composed by the caller.
type Effect struct {
	CompletedAt time.Time
	EventID     sessionwire.EventID
	JournalSeq  uint64
}

// Validate refuses an effect that names no durable event.
func (e Effect) Validate() error {
	switch {
	case e.CompletedAt.IsZero():
		return &ApplyError{Refusal: RefusalInvalidEffect, Reason: "the effect records no completion instant"}
	case e.EventID.Validate() != nil:
		return &ApplyError{Refusal: RefusalInvalidEffect, Reason: "the effect names no valid public event"}
	case e.JournalSeq == 0:
		return &ApplyError{Refusal: RefusalInvalidEffect, Reason: "the effect names no journal sequence"}
	default:
		return nil
	}
}

// Gate is the durable gate state §9.4's release-race backstop is decided from.
type Gate struct {
	GateID sessionwire.GateID

	// Open reports an unresolved, unexpired durable gate.
	Open bool

	// OwnerHostID and OwnerEpoch are the resident owner the gate was opened
	// under. A gate response is resumable only while both still name this Host's
	// current residency, because continuation is out of scope: a runtime that was
	// released and relaunched no longer holds what the gate suspended.
	//
	// ON A DISPOSITION SESSION THE OWNER IS AN EPOCH ALONE. sessionstore
	// v0.12.0 records no Host identity on a gate; it records the RESIDENCY
	// epoch of the last Host to write any gate as the projection's mark, and
	// raises it on every gate write. OwnerEpoch is that residency epoch — never
	// a journal epoch — and OwnerHostID is empty. The owning Host is this one
	// exactly when OwnerEpoch equals the grant it holds. It is reported for a
	// gate the projection does NOT hold too (Open false, found false), because
	// the mark belongs to the projection, not to one gate.
	OwnerHostID sessionwire.HostID
	OwnerEpoch  uint64

	// OpenedEventID and OpenedJournalSeq identify the event that opened the
	// gate, as projected. A gate response names one of them as the version it
	// answers (Core's ExpectedOpen*), and a mismatch is a stale or malformed
	// answer.
	OpenedEventID    sessionwire.EventID
	OpenedJournalSeq uint64
}

// ---------------------------------------------------------------------------
// What the journal proves
// ---------------------------------------------------------------------------

// ApplicationOutcome is what a session's journal proves about one command's
// application.
//
// IT IS FIVE-VALUED AND NOT A BOOLEAN, and the earlier two-valued shape
// (`(Prefix, bool, error)`) is why this is stated at length: each value unlocks
// a DIFFERENT settlement, and a seam that could only say "a prefix exists"
// forced a caller to guess between finishing a command and refusing it. The
// vocabulary is sessionstore's, because the answers are the store's to give.
type ApplicationOutcome string

const (
	// ApplicationAbsent means no record in the journal names this command.
	// Nothing has been applied under it.
	ApplicationAbsent ApplicationOutcome = "absent"

	// ApplicationCommitted means a correlated prefix is immediately followed by
	// the public event that carried its effect. The command has been applied,
	// whichever lease did it, and that event is what a completion names.
	ApplicationCommitted ApplicationOutcome = "committed"

	// ApplicationAbandoned means a correlated prefix is immediately followed by
	// an opening fence above its own epoch: the application started and its
	// writer lost the stream before committing anything more.
	ApplicationAbandoned ApplicationOutcome = "abandoned"

	// ApplicationUnresolved means a correlated prefix exists whose outcome
	// cannot be read yet. Both settlements refuse; the answer may become
	// readable later.
	ApplicationUnresolved ApplicationOutcome = "unresolved"

	// ApplicationConflicted means a prefix names this command's public identity
	// with a different runtime identity or kind. The durable mapping is broken,
	// so no settlement is safe and an operator has to look.
	ApplicationConflicted ApplicationOutcome = "conflicted"
)

// known reports whether an outcome is one this package can act on. An unknown
// one is refused rather than defaulted, for State.known's reason.
func (o ApplicationOutcome) known() bool {
	switch o {
	case ApplicationAbsent, ApplicationCommitted, ApplicationAbandoned, ApplicationUnresolved, ApplicationConflicted:
		return true
	default:
		return false
	}
}

// Application is what one session's journal proves about one command.
type Application struct {
	CommandID        sessionwire.CommandID
	RuntimeCommandID uuid.UUID

	Outcome ApplicationOutcome

	// PrefixEpoch is the lease epoch the correlated prefix was appended under,
	// and is zero when the outcome is Absent.
	PrefixEpoch uint64

	// EffectEventID and EffectSeq name the public event that carried the effect,
	// and are zero unless the outcome is Committed.
	EffectEventID sessionwire.EventID
	EffectSeq     uint64

	// SupersedingEpoch is the highest opening-fence epoch the walk observed. A
	// writer at or below it is provably fenced out of the stream, which is the
	// only thing that makes a negative answer about an in-flight applier safe.
	SupersedingEpoch uint64
}

// hasPrefix reports whether a correlated application prefix exists at all. It
// is Outcome.PrefixOwned's durable meaning: the command is recoverable from the
// journal rather than re-drivable from the inbox.
func (a Application) hasPrefix() bool { return a.Outcome != ApplicationAbsent }

// provesNoEffect reports whether the journal establishes that nothing was ever
// applied under this command. Only two outcomes do.
func (a Application) provesNoEffect() bool {
	return a.Outcome == ApplicationAbsent || a.Outcome == ApplicationAbandoned
}

// fences reports whether the journal proves a writer holding the given lease
// epoch can no longer append to this session.
func (a Application) fences(epoch uint64) bool { return a.SupersedingEpoch > epoch }

// effect is the terminal result a committed application is settled with, taken
// from the journal rather than from the caller.
func (a Application) effect(at time.Time) Effect {
	return Effect{CompletedAt: at, EventID: a.EffectEventID, JournalSeq: a.EffectSeq}
}

// ---------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------
//
// Each is one read and one revision compare-and-swap of the same authoritative
// record Inbox lists. THE EXPECTED REVISION IS THE EXACTLY-ONCE MECHANISM: two
// Hosts that both read a pending record and both decide to apply it are made
// safe by the store refusing the second CAS, and a transition that did not say
// which revision it decided on could not be refused.

// Claim takes a short-lived claim so one writer works on a command at a time.
type Claim struct {
	ExpectedRevision uint64
	Epoch            uint64
	ExpiresAt        time.Time
}

// Applying moves a claimed command into applying: the statement that
// application is starting now, not that capacity has been reserved.
type Applying struct {
	ExpectedRevision uint64
	Epoch            uint64
	ExpiresAt        time.Time
}

// Completion records that an applying command's effect committed.
type Completion struct {
	ExpectedRevision uint64
	Epoch            uint64
	Effect           Effect
}

// Rejection settles a command with a durable typed reason.
type Rejection struct {
	ExpectedRevision uint64
	Epoch            uint64
	Detail           sessionwire.ErrorDetail
}

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------
//
// The reads and the writes are SEPARATE interfaces over the same durable
// objects, which is the discipline O4.1 recorded: the transitions below write
// the same SessionInbox record Inbox lists, under the same one-writer-per-lease
// rule, and the split is about fencing rather than about ownership. Every method
// of a `…Writes` interface is a durable write and goes through Fence.Write;
// TestEveryDurableWriteGoesThroughTheFence holds that structurally and derives
// the method set from these declarations, so a method added here is covered
// without anyone remembering to widen a guard.

// CommandRecords reads the private durable command state.
type CommandRecords interface {
	// LoadCommand returns the full inbox record for one command, including the
	// revision a later compare-and-swap names.
	LoadCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Record, error)

	// LoadPayload returns the private body or its object reference. It is called
	// only after a claim has been committed.
	LoadPayload(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Payload, error)
}

// Applications reads what a session's journal proves about a command.
type Applications interface {
	// FindApplication correlates the journal against the command's OWN durable
	// identities. A caller does not supply them: one that could name the runtime
	// identity to correlate on could ask about a mapping that was never
	// accepted, and the answer would be evidence about nothing.
	FindApplication(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Application, error)
}

// Gates reads durable gate state for the §9.4 release-race backstop.
type Gates interface {
	// LoadGate returns the durable gate and whether it exists.
	LoadGate(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, gate sessionwire.GateID) (Gate, bool, error)
}

// InboxWrites is every durable transition an applier makes to the command
// record.
//
// EVERY METHOD HERE IS FENCED. The name is load-bearing: the structural guard
// collects the methods of each interface in this package whose name ends in
// "Writes" and requires every call to one of them to be lexically inside a
// Fence.Write. A durable write added to this interface is guarded the moment it
// is declared; a durable write declared on a seam named otherwise is not, which
// is the guard's one stated limit — DO NOT RENAME THIS TYPE OUT OF THE
// CONVENTION. Each write also has a behavioural test that loses the grant
// immediately before it, so the structural guard is the forward claim rather
// than the only one.
//
// APPLIED AND REJECTED ARE MUTUALLY EXCLUSIVE BY CONSTRUCTION HERE: they are
// two methods with different required evidence — a journal effect for one, a
// typed public reason for the other — so no single call can commit both, and the
// store refuses either against a record that is already terminal.
type InboxWrites interface {
	// ClaimCommand CASes a record into StateClaimed and returns its new
	// revision. It is refused for a terminal record, for an applying record at
	// any epoch, for an epoch below the record's high-water mark, at or after
	// the apply deadline, and against a live claim at the same epoch — A CLAIM
	// CANNOT BE RENEWED.
	ClaimCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, claim Claim) (uint64, error)

	// BeginApplying CASes a claimed record into StateApplying and returns its
	// new revision. Only the holder of a LIVE claim at the claim's own epoch may
	// make it; there is no deadline check, which is what stops a reconciler's
	// clock from cancelling work that is about to commit.
	BeginApplying(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, applying Applying) (uint64, error)

	// CompleteCommand settles an applying command as applied. It is admitted
	// only from StateApplying, and a caller whose epoch is not the claim's must
	// name the effect the journal correlation found.
	CompleteCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, completion Completion) error

	// RejectCommand settles a command with a durable typed reason. It is
	// admitted only where the journal proves no effect committed, and for an
	// applying record only to a strictly later epoch that the journal has fenced
	// the applier out under.
	RejectCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, rejection Rejection) error
}

// JournalWrites is the one journal append an applier makes.
//
// It is a SEPARATE seam from InboxWrites because it is a different durable
// object with a different writer: the inbox record is a keyed CAS, and this is
// an append to the session's own stream under the journal fence. Both are
// fenced, and both are covered by the same structural guard, which is what the
// naming convention buys — a second write seam needed no edit to the guard.
//
// SO THE SAME PROHIBITION APPLIES HERE: DO NOT RENAME THIS TYPE OUT OF THE
// CONVENTION. The guard's one measured survivor is exactly that rename, and it
// spans both write seams; a warning on only one of them would leave the newer
// seam looking unclaimed.
type JournalWrites interface {
	// AppendApplicationPrefix commits the private correlation record before the
	// runtime-visible effect begins. The lease epoch is the writer's own and is
	// not a parameter; see Prefix.
	AppendApplicationPrefix(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, prefix Prefix) error
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

// ApplyRefusal is a stable, machine-readable reason an applier declined to
// finish a command. It is a code rather than a sentence for the reason
// PageProblem is.
//
// A REFUSAL IS NOT A REJECTION. Every value here leaves the durable record where
// it was and blocks the pass at that command, which the consumer names in
// PassResult.Blocked. The durable rejections this package writes are Rejection
// values, not refusals.
type ApplyRefusal string

const (
	// RefusalStaleEpoch reports a record or a prefix under an epoch later than
	// this applier's, which means this Host no longer owns the session.
	RefusalStaleEpoch ApplyRefusal = "stale_epoch"

	// RefusalClaimHeld reports another epoch's unexpired claim.
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

	// RefusalUnrevisionedRecord reports a record carrying no provider revision.
	//
	// Zero is not a revision any provider assigns, so it cannot be a revision a
	// caller read, and a compare-and-swap naming it is unconditional. The store
	// refuses it as an invalid request; this Host refuses to ISSUE it, which is
	// the half a fake cannot supply.
	RefusalUnrevisionedRecord ApplyRefusal = "unrevisioned_record"

	// RefusalForeignRecord reports a record that is not the one asked for, or
	// not this session's.
	RefusalForeignRecord ApplyRefusal = "foreign_record"

	// RefusalCorrelation reports identities that do not correlate: an
	// unallocated RuntimeCommandID, a journal prefix naming another runtime
	// command, or a committed effect against a record that never entered
	// applying.
	RefusalCorrelation ApplyRefusal = "correlation_mismatch"

	// RefusalUnresolvedApplication reports an application whose outcome the
	// journal cannot yet decide. Neither settlement is safe and the answer may
	// become readable later, so the pass stops rather than guessing.
	RefusalUnresolvedApplication ApplyRefusal = "unresolved_application"

	// RefusalUnfencedApplier reports an abandoned-looking application whose
	// writer the journal has not yet fenced out. Only the fence proves the
	// applier can no longer commit the effect a rejection would orphan.
	RefusalUnfencedApplier ApplyRefusal = "unfenced_applier"

	// RefusalMissingPayload reports a kind whose private payload is required and
	// absent, or a payload the store's own one-of-two invariant refuses.
	RefusalMissingPayload ApplyRefusal = "missing_payload"

	// RefusalUnreadableCreate leaves a claimed create unattempted because this
	// Host cannot read its body, while a newer Host may be able to.
	RefusalUnreadableCreate ApplyRefusal = "unreadable_create"

	// RefusalUnreadableGateResponse reports a gate response whose private body
	// this Host cannot read the gate identity out of, so §9.4's recheck cannot
	// be run.
	RefusalUnreadableGateResponse ApplyRefusal = "unreadable_gate_response"

	// RefusalReferencedGateResponse reports a gate response whose body is
	// stored behind an object reference this Host does not dereference.
	//
	// IT IS NOT THE SAME FINDING as an unreadable body, though both stop the
	// same check, and giving them one code made the difference unobservable:
	// deleting the reference branch left the JSON decode to fail on empty bytes
	// with the same code, so the whole branch was an equivalent mutant. The
	// repairs differ — a malformed body is corruption or a Factory bug, and a
	// referenced one is a body over its inline threshold plus a Host that
	// deliberately cannot fetch it — and an operator reading "not a Core
	// gate-response request" about a perfectly well-formed spilled body is
	// being misdirected.
	RefusalReferencedGateResponse ApplyRefusal = "referenced_gate_response"

	// RefusalGateNotOwned reports a gate response whose gate the durable
	// projection holds under a residency mark that is not this Host's grant:
	// below it, this Host's fencing write has not landed yet; above it, a
	// successor has written. Neither is a statement about the answer, so the
	// command is left claimed and the pass retried, never rejected.
	RefusalGateNotOwned ApplyRefusal = "gate_not_owned"

	// RefusalNoGateReader reports a gate response reaching a composition that
	// supplied no durable gate reader, so the gate could not be checked before
	// the attempt.
	RefusalNoGateReader ApplyRefusal = "no_gate_reader"

	// RefusalInvalidEffect reports a terminal application that names no durable
	// event.
	RefusalInvalidEffect ApplyRefusal = "invalid_effect"

	// RefusalNoCommittedEffect reports a runtime call that returned while the
	// journal shows no committed effect for the command. Nothing was applied, so
	// nothing may be recorded as applied.
	RefusalNoCommittedEffect ApplyRefusal = "no_committed_effect"

	// RefusalStore reports an ambiguous durable-store failure.
	RefusalStore ApplyRefusal = "store_failure"

	// RefusalRuntime reports that the runtime refused or failed the command
	// after its application prefix was committed.
	RefusalRuntime ApplyRefusal = "runtime_failure"

	// ------------------------------------------------------------------
	// Disposition-family refusals. See disposition.go.
	// ------------------------------------------------------------------

	// RefusalInvalidAttempt reports an attempt identity neither downstream
	// module would accept. It is refused before any durable write, because an
	// identity the store wrote and the runtime then refused would authorize a
	// dispatch nothing could ever be evidence about.
	RefusalInvalidAttempt ApplyRefusal = "invalid_attempt"

	// RefusalNoJournalGrant reports a runtime holding no single-writer journal
	// lease, so there is no grant an attempt could name.
	//
	// IT IS NOT A STALE EPOCH. The runtime's grant and this Host's residency are
	// different authorities over different stores, and a Host that substituted
	// its own would author an attempt no evidence could ever match.
	RefusalNoJournalGrant ApplyRefusal = "no_journal_grant"

	// RefusalDispositionUnsupported reports a dispatch the runtime refused
	// because its durable log cannot record a disposition.
	//
	// IT IS DELIBERATELY NOT RefusalRuntime, and the distinction is the one
	// harness exported the error type to make expressible: NOTHING DURABLE WAS
	// WRITTEN, so the command may be re-offered elsewhere. A transport failure
	// says the opposite — something may have happened — so collapsing the two
	// would either strand a re-offerable command or re-drive a real effect.
	RefusalDispositionUnsupported ApplyRefusal = "disposition_unsupported"

	// RefusalEvidenceUnavailable reports a settlement that could not read the
	// runtime's durable disposition.
	//
	// THE COMMAND STAYS APPLYING AT AN UNMOVED REVISION, and that is the safe
	// failure rather than a missing feature. Absence is not a disposition: a
	// Host that concluded "nothing happened" from an unreadable journal would
	// re-drive an effect that may already have committed.
	RefusalEvidenceUnavailable ApplyRefusal = "evidence_unavailable"

	// RefusalEvidenceUnroutable reports a settlement whose evidence could not be
	// OBTAINED because of this deployment's WIRING, rather than because the
	// runtime recorded nothing.
	//
	// IT IS A DIFFERENT REFUSAL FROM RefusalEvidenceUnavailable BECAUSE IT NAMES A
	// DIFFERENT PERSON. Both leave the command applying at an unmoved revision and
	// both are safe; what differs is who has to act. `evidence_unavailable` points
	// an operator at the runtime, which is right when the runtime wrote nothing
	// and WRONG — expensively, since a real effect has already been committed —
	// when the cause is a binding no reader is registered for, or a reader that
	// refuses to resolve the session at all.
	//
	// IT COVERS ONLY THE CAUSES HOST CAN ACTUALLY NAME. A router pointed at the
	// WRONG journal store is not one of them: that store is healthy, at the
	// correct tenant, and truthfully reports it holds no such record, which is
	// indistinguishable from the benign case at settlement time and correctly so.
	// Closing that needs a construction-time answer no released API offers, and it
	// is booked rather than papered over here.
	RefusalEvidenceUnroutable ApplyRefusal = "evidence_unroutable"

	// RefusalEnduringEffect reports a recovery closure refused because the
	// runtime's journal holds a durable effect for the command.
	//
	// IT IS TERMINAL AND MUST NOT BE RETRIED INTO A TOMBSTONE. The predecessor's
	// effect committed and only its evidence is missing; an operator has to
	// look, and the pass blocks until one does.
	RefusalEnduringEffect ApplyRefusal = "enduring_effect"

	// RefusalNoAttemptCloser reports a predecessor's stranded attempt this
	// composition has no closer for. The pass blocks; nothing is concluded.
	RefusalNoAttemptCloser ApplyRefusal = "no_attempt_closer"
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
	Host *hostconfig.Host

	// Key is the session whose commands this applier applies.
	Key registry.Key

	// LeaseEpoch is the grant every durable write is stamped with.
	LeaseEpoch uint64

	Records      CommandRecords
	Applications Applications
	Gates        Gates
	Writes       InboxWrites
	Journal      JournalWrites

	// Runtime is Host's control path into the resident runtime.
	Runtime department.CommandApplier

	// Fence is the lease-epoch guard. AN APPLIER HAS ITS OWN, and that is the
	// answer to the hand-off O4.1 left on Processor: the applier's transitions
	// are fenced writes, and they are fenced by the same one mechanism the
	// cursor write goes through rather than by a second discipline.
	Fence Fence
}

// Applier claims, applies, recovers and settles one command at a time. It is the
// Processor the consumer hands non-terminal records to.
type Applier struct {
	host         *hostconfig.Host
	key          registry.Key
	epoch        uint64
	records      CommandRecords
	applications Applications
	gates        Gates
	writes       InboxWrites
	journal      JournalWrites
	runtime      department.CommandApplier
	fence        Fence
}

// The applier is the seam O4.1 declared.
var _ Processor = (*Applier)(nil)

// NewApplier validates options and returns an Applier.
func NewApplier(options ApplierOptions) (*Applier, error) {
	if options.Host == nil {
		return nil, &InvalidConsumerOptionsError{Field: "Host", Reason: "must be set; the clock, the claim TTL and this Host's identity all come from it"}
	}
	// THE ACCESSORS THIS TYPE READS, AND ONLY THOSE. Same reason NewConsumer has
	// its own list: &host.Host{} compiles from any package and has none of them
	// set, so a nil check alone accepts a Host with a nil clock and a zero claim
	// TTL — and a zero TTL claims a command that has already expired.
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
		{"Applications", options.Applications != nil},
		{"Gates", options.Gates != nil},
		{"Writes", options.Writes != nil},
		{"Journal", options.Journal != nil},
		{"Runtime", options.Runtime != nil},
		{"Fence", options.Fence != nil},
	} {
		if !required.present {
			return nil, &InvalidConsumerOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	return &Applier{
		host:         options.Host,
		key:          options.Key,
		epoch:        options.LeaseEpoch,
		records:      options.Records,
		applications: options.Applications,
		gates:        options.Gates,
		writes:       options.Writes,
		journal:      options.Journal,
		runtime:      options.Runtime,
		fence:        options.Fence,
	}, nil
}

// Process claims, applies, recovers or settles one command.
//
// IT READS THE RECORD FRESH, AND THEN THE JOURNAL. The Command it is handed is
// the ordering decision's three fields, taken from a page that may be a whole
// pass old; every transition below is a compare-and-swap against the record as
// it is now, and every settlement is decided against what the journal proves
// rather than against what the record's state suggests.
//
// EVERY KIND IS DRIVEN INTO THE RUNTIME, including create and restore. An
// earlier version settled those two as applied on the ground that residency is
// their consequence and had already happened — which was sound about the
// consequence and skipped the question of what the durable record may legally
// say. It may say applied only from applying, and only naming a committed
// journal effect; a Host's knowledge that a runtime exists is neither. There is
// no shortcut to write.
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
	// it was not, which means somebody settled it in between — a predecessor
	// Host, or Factory's deadline reconciler. §10.4 never re-opens a terminal
	// record, so the only thing left is to tell the consumer what it became.
	if record.State.Terminal() {
		return Outcome{State: record.State}, nil
	}

	application, err := a.applications.FindApplication(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the journal correlation could not be read, so it is unknown whether an effect already committed",
			Cause:     err,
		}
	}

	step, err := a.plan(record, application, a.now())
	if err != nil {
		return Outcome{State: record.State, PrefixOwned: application.hasPrefix()}, err
	}
	switch step {
	case stepComplete:
		return a.complete(ctx, record, record.Revision, application)
	case stepReject:
		return a.reject(ctx, record, record.Revision, sessionwire.ErrorDetail{
			Code:      sessionwire.ErrorCodeCommandRejected,
			Message:   "the application was abandoned by a superseded lease and no effect committed",
			Retryable: true,
		})
	case stepApplyUnderHeldClaim:
		return a.apply(ctx, record, record.Revision)
	case stepDriveApplyingRecord:
		return a.drive(ctx, record, record.Revision)
	default:
		return a.claimThenApply(ctx, record)
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
	case record.Revision == 0:
		return &ApplyError{
			Refusal:   RefusalUnrevisionedRecord,
			CommandID: command.CommandID,
			Reason:    "the record carries no provider revision, so every transition this applier made would be a blind write dressed as a compare-and-swap",
		}
	case record.RuntimeCommandID.IsZero():
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: command.CommandID,
			Reason:    "the record allocated no RuntimeCommandID, so nothing could correlate an effect with it and the runtime would be handed no mapping",
		}
	default:
		return nil
	}
}

// claimThenApply takes a claim and runs the rest of the protocol.
func (a *Applier) claimThenApply(ctx context.Context, record Record) (Outcome, error) {
	claim := Claim{
		ExpectedRevision: record.Revision,
		Epoch:            a.epoch,
		ExpiresAt:        a.now().Add(a.host.ClaimTTL()),
	}
	var revision uint64
	if err := a.fence.Write(func() error {
		claimed, err := a.writes.ClaimCommand(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, claim)
		revision = claimed
		return err
	}); err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the claim was refused",
			Cause:     err,
		}
	}
	return a.apply(ctx, record, revision)
}

// apply revalidates a claimed command, enters applying, and drives it.
//
// It runs with a LIVE CLAIM AT THIS APPLIER'S EPOCH and nothing here re-takes
// one: a claim cannot be renewed, and a caller that needs more time has exactly
// one move, which is to enter applying before its claim lapses.
func (a *Applier) apply(ctx context.Context, record Record, revision uint64) (Outcome, error) {
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
	if err := checkPayload(record, payload); err != nil {
		return Outcome{State: StateClaimed}, err
	}

	// REVALIDATION BEFORE APPLYING, which §10.4 requires and §9.4 gives the one
	// concrete case for: a gate response that lost the release race is durably
	// rejected here rather than driven into a runtime that cannot resume it.
	if record.Kind == KindGateResponse {
		detail, err := a.gateRejection(ctx, record, payload)
		if err != nil {
			return Outcome{State: StateClaimed}, err
		}
		if detail != nil {
			return a.reject(ctx, record, revision, *detail)
		}
	}

	applying := Applying{
		ExpectedRevision: revision,
		Epoch:            a.epoch,
		ExpiresAt:        a.now().Add(a.host.ClaimTTL()),
	}
	var applyingRevision uint64
	if err := a.fence.Write(func() error {
		next, err := a.writes.BeginApplying(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, applying)
		applyingRevision = next
		return err
	}); err != nil {
		return Outcome{State: StateClaimed}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the record could not be moved into applying",
			Cause:     err,
		}
	}
	return a.driveWithPayload(ctx, record, applyingRevision, payload)
}

// drive continues an APPLYING record this applier already owns: it reloads the
// private payload and drives the runtime without re-entering applying, which is
// a transition the store admits only from claimed.
func (a *Applier) drive(ctx context.Context, record Record, revision uint64) (Outcome, error) {
	payload, err := a.records.LoadPayload(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: StateApplying}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the private payload could not be loaded",
			Cause:     err,
		}
	}
	if err := checkPayload(record, payload); err != nil {
		return Outcome{State: StateApplying}, err
	}
	return a.driveWithPayload(ctx, record, revision, payload)
}

// driveWithPayload appends the correlation, drives the runtime, and settles the
// command on what the journal then proves.
func (a *Applier) driveWithPayload(ctx context.Context, record Record, revision uint64, payload Payload) (Outcome, error) {
	// BEFORE THE EFFECT, NOT AFTER IT. A prefix written after the runtime call
	// would be exactly the correlation §10.4 needs and would be missing in the
	// only case it exists for: a crash during the call.
	if err := a.fence.Write(func() error {
		return a.journal.AppendApplicationPrefix(ctx, a.key.TenantID, a.key.SessionID, Prefix{
			CommandID:        record.CommandID,
			RuntimeCommandID: record.RuntimeCommandID,
			Kind:             record.Kind,
		})
	}); err != nil {
		return Outcome{State: StateApplying}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the application prefix could not be committed, so the runtime was not driven",
			Cause:     err,
		}
	}

	// THE LAST CHECK BEFORE AN IRREVERSIBLE EFFECT, and the only one here that
	// is not a write. Every step above this line is a durable write the fence
	// can refuse; the runtime call is not, and it is the one step no successor
	// can undo. A grant that ended between the prefix and here means this Host
	// no longer owns the session, so it stops rather than driving a runtime it
	// is about to lose — and the prefix it already committed is exactly what lets
	// whoever owns the session next finish the command.
	if err := a.fence.Held(); err != nil {
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalStaleEpoch,
			CommandID: record.CommandID,
			Reason:    "the grant ended after the application prefix was committed, so the runtime was not driven",
			Cause:     err,
		}
	}

	// THE MAPPING AND THE BODY BOTH CROSS. §16 forwards the RuntimeCommandID
	// into the Harness API, and the body travels with it because SessionStore's
	// inbox payload is private to Factory and Host: a runtime handed a bare
	// public identity has no way to obtain what it is being asked to apply.
	if err := a.runtime.ApplyCommand(ctx, department.RuntimeCommand{
		CommandID:        record.CommandID,
		RuntimeCommandID: record.RuntimeCommandID,
		Kind:             string(record.Kind),
		Payload:          payload.Body,
		PayloadRef:       payload.Ref,
	}); err != nil {
		// THE PREFIX IS COMMITTED, so this is reported as an owned prefix and as
		// a failure at the same time, and both halves are true. Past this point
		// what happened is a question for the journal rather than for this
		// call's return value, and the next pass asks it.
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalRuntime,
			CommandID: record.CommandID,
			Reason:    "the runtime failed the command after its application prefix was committed",
			Cause:     err,
		}
	}

	// THE EVIDENCE, NOT THE RETURN VALUE. A terminal application records the
	// journal event that carried the effect, and this applier does not have one
	// until it reads the correlation back. A runtime call that returned while
	// committing nothing leaves the command unapplied, and saying so is what
	// keeps a durable no-op from being recorded as success.
	settled, err := a.applications.FindApplication(ctx, a.key.TenantID, a.key.SessionID, record.CommandID)
	if err != nil {
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the journal correlation could not be read back, so the effect this command committed cannot be named",
			Cause:     err,
		}
	}
	if settled.Outcome != ApplicationCommitted {
		return Outcome{State: StateApplying, PrefixOwned: settled.hasPrefix()}, &ApplyError{
			Refusal:   RefusalNoCommittedEffect,
			CommandID: record.CommandID,
			Reason: "the runtime returned but the journal reports the application as " + strconv.Quote(string(settled.Outcome)) +
				", so no effect exists to record",
		}
	}
	return a.complete(ctx, record, revision, settled)
}

// checkPayload refuses a payload the command cannot be applied without, and one
// whose two forms disagree.
func checkPayload(record Record, payload Payload) error {
	if !payload.wellFormed() {
		return &ApplyError{
			Refusal:   RefusalMissingPayload,
			CommandID: record.CommandID,
			Reason:    "the payload carries both an inline body and an object reference, and at most one of the two is ever set",
		}
	}
	switch record.Kind {
	case KindInput, KindGateResponse:
		if !payload.present() {
			return &ApplyError{
				Refusal:   RefusalMissingPayload,
				CommandID: record.CommandID,
				Reason:    "a " + string(record.Kind) + " command carries no private payload, so the runtime would be asked to apply nothing",
			}
		}
	}
	return nil
}

// gateRejection runs §9.4's release-race backstop and returns the durable
// rejection to write, or nil when the gate is still resumable here.
func (a *Applier) gateRejection(ctx context.Context, record Record, payload Payload) (*sessionwire.ErrorDetail, error) {
	gate, err := gateResponseTarget(record, payload)
	if err != nil {
		return nil, err
	}
	durable, found, err := a.gates.LoadGate(ctx, a.key.TenantID, a.key.SessionID, gate)
	if err != nil {
		return nil, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the durable gate could not be read, so the release race could not be decided",
			Cause:     err,
		}
	}
	reason, resumable := a.gateResumable(durable, found)
	if resumable {
		return nil, nil
	}
	return &sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeGateNotResumable, Message: reason}, nil
}

// gateResponseTarget reads the gate a response answers out of its private body.
//
// HOST DECODES ONE RECORD AND NO MORE. The body of a gate response is Core's own
// GateResponseRequest — a public wire record this module already depends on —
// and §9.4 requires Host to repeat Factory's gate/owner check, which is not
// possible without the gate's identity. Nothing else about the body is read:
// Harness remains the semantic validator, and every other kind's body travels
// opaque.
//
// A REFERENCED BODY IS REFUSED RATHER THAN FETCHED. Host does not dereference a
// private object — that is the runtime's read — so a gate response large enough
// to have been spilled is one this backstop cannot decide, and it says so
// instead of guessing that the gate is fine.
func gateResponseTarget(record Record, payload Payload) (sessionwire.GateID, error) {
	if len(payload.Body) == 0 {
		return "", &ApplyError{
			Refusal:   RefusalReferencedGateResponse,
			CommandID: record.CommandID,
			Reason:    "the gate response's private body is stored behind an object reference, which this Host does not dereference",
		}
	}
	var request sessionwire.GateResponseRequest
	if err := json.Unmarshal(payload.Body, &request); err != nil {
		return "", &ApplyError{
			Refusal:   RefusalUnreadableGateResponse,
			CommandID: record.CommandID,
			Reason:    "the gate response's private body is not a Core gate-response request",
			Cause:     err,
		}
	}
	if request.CommandID != record.CommandID || request.SessionID != record.SessionID {
		return "", &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason: "the gate response's body names (" + strconv.Quote(string(request.SessionID)) + ", " + strconv.Quote(string(request.CommandID)) +
				"), which is not the record it is stored under",
		}
	}
	return request.GateID, nil
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

// complete settles an applying command as applied, naming the correlated effect.
//
// THE EFFECT IS VALIDATED BEFORE IT IS WRITTEN, and the case that reaches it is
// a store contradicting itself: an outcome of committed carries an event and a
// sequence by construction, so a committed correlation with neither is the same
// class of defect CursorRegressionError refuses. Writing it would spend a fenced
// round-trip to be told the result is malformed, and reading it back would leave
// an applied command pointing at nothing.
//
// A FAILED SETTLEMENT IS SILENT ONLY WHEN THE GRANT IS GONE, and the two arms
// are not symmetric. This function used to report EVERY refusal as an owned
// prefix, justified by "a later pass records the same effect from the same
// evidence" — WHICH NAMED A READER THAT DOES NOT EXIST. PrefixOwned is
// consumable: Consumer.Reconcile counts the record consumed, advances its
// cursor and commits it, and every later page is listed strictly after that
// cursor. Nothing else reads the inbox — there is no ListDue sweep in this lane
// — so one timed-out CAS left a record durably `applying` over a committed
// effect, which §10.4 forbids the reconciler from touching and which
// RejectCommand refuses for want of provesNoEffect. The command never became
// terminal, and the pass reported no Blocked, so an operator reading LastPass
// saw a healthy session.
//
// A LOST GRANT IS THE ONE CASE WHERE THE SILENCE IS SAFE, and it is safe because
// of a mechanism rather than a hope: the consumer's own cursor write goes
// through the same fence, so a pass that got here with the grant gone writes no
// cursor at all and re-derives the record next time. That arm keeps reporting an
// owned prefix — the effect IS durable in the journal, and this Host has no
// business writing anything more.
func (a *Applier) complete(ctx context.Context, record Record, revision uint64, application Application) (Outcome, error) {
	effect := application.effect(a.now())
	if err := effect.Validate(); err != nil {
		return Outcome{State: StateApplying, PrefixOwned: true}, err
	}
	if err := a.fence.Write(func() error {
		return a.writes.CompleteCommand(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, Completion{
			ExpectedRevision: revision,
			Epoch:            a.epoch,
			Effect:           effect,
		})
	}); err != nil {
		if lost := a.fence.Held(); lost != nil {
			return Outcome{State: StateApplying, PrefixOwned: true}, nil
		}
		return Outcome{State: StateApplying, PrefixOwned: true}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the terminal completion was refused while this Host still holds the session, and no later reader would revisit a command the cursor had passed",
			Cause:     err,
		}
	}
	return Outcome{State: StateApplied}, nil
}

// reject settles a command with a durable typed reason.
//
// UNLIKE A COMPLETION, AN AMBIGUOUS FAILURE HERE IS AN ERROR. A rejection rests
// on the journal proving that NO effect committed, so there is no durable
// correlation for a later pass to recover from — nothing would make the command
// recoverable, and reporting an owned prefix would advance the consumer's cursor
// past a command no evidence can settle. The pass stops instead, and the next
// one re-derives the same conclusion from the same durable state.
func (a *Applier) reject(ctx context.Context, record Record, revision uint64, detail sessionwire.ErrorDetail) (Outcome, error) {
	if err := detail.Validate(); err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the rejection carries no stable typed code, and §10.4 makes the reason part of the terminal state",
			Cause:     err,
		}
	}
	if err := a.fence.Write(func() error {
		return a.writes.RejectCommand(ctx, a.key.TenantID, a.key.SessionID, record.CommandID, Rejection{
			ExpectedRevision: revision,
			Epoch:            a.epoch,
			Detail:           detail,
		})
	}); err != nil {
		return Outcome{State: record.State}, &ApplyError{
			Refusal:   RefusalStore,
			CommandID: record.CommandID,
			Reason:    "the terminal rejection was refused",
			Cause:     err,
		}
	}
	return Outcome{State: StateRejected}, nil
}

// now is the clock the deadline, the claim TTL and a completion instant are read
// against.
func (a *Applier) now() time.Time { return a.host.Clock().Now() }
