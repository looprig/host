// Applying a command in the DISPOSITION family is a different durable protocol
// from apply.go's, and this file is that protocol.
//
// THE DIFFERENCE IS WHO AUTHORS THE TERMINAL ARM. In the legacy family a Host
// reads the journal correlation itself and then writes `applied` or `rejected`
// from what it concluded. Here it cannot: SettleDispositionCommand takes a
// command, a revision and a residency and NOTHING ELSE — no outcome, no
// absence, no proof structure — and the store derives what it expects from its
// own immutable record and obtains the evidence through its configured reader.
// A Host's whole authority is "at most one authorized attempt"; what that
// attempt produced is the runtime's durable statement, not this Host's opinion.
//
// SO THE SHAPE IS CLAIM, AUTHORIZE, DISPATCH, SETTLE, and each edge exists
// because the process can stop between any two of them:
//
//  1. CLAIM under the residency GRANT. The claim edge takes a *ResidencyGrant
//     rather than an epoch, so a caller cannot name one; DispositionClaim below
//     therefore carries no epoch member at all, and the binding of a grant to a
//     session is the concrete edge's, not this package's.
//  2. AUTHORIZE — mint an attempt identity, read the RUNTIME's own journal
//     grant, and write both immutably into the record. After this the command
//     is `applying` and is closed by EVIDENCE rather than by any timer.
//  3. DISPATCH, carrying the attempt identity to the runtime, which is what
//     makes the runtime's durable disposition be about THIS attempt.
//  4. SETTLE from that evidence.
//
// WHAT "APPLIED" MEANS, and it is narrower than the word: under the attempt's
// journal grant, the runtime durably recorded that it accepted this command
// into its execution path. It does NOT mean a turn started, does NOT mean a
// turn folded, does NOT mean no later rejection can follow, and does NOT mean a
// queued input survives a crash — the runtime's loop mailbox is in memory. Do
// not widen this sentence anywhere, including in a user-facing vocabulary.
//
// MISSING EVIDENCE IS NOT PROOF THAT NOTHING HAPPENED. A settlement that cannot
// read a disposition leaves the command `applying` at an unmoved revision and
// blocks the pass. That is the safe failure and the only honest one: a Host
// that re-dispatched on an absent record would be re-driving an effect that may
// already have committed.
package commands

import (
	"context"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
)

// ---------------------------------------------------------------------------
// The attempt identity
// ---------------------------------------------------------------------------

// MaxAttemptIDBytes bounds a dispatch-attempt identity.
//
// IT IS BOTH DOWNSTREAM BOUNDS AT ONCE, which is why it is a constant here
// rather than a deferral to either. sessionstore bounds
// DispositionAttemptID and harness bounds runtimecommand.AttemptID, both at
// Core's MaxIDBytes, and an identity travels to BOTH: the store writes it into
// the immutable attempt and the runtime writes it into its durable disposition.
// An identity either of them refused would be refused after a durable write on
// one side, so it is refused before either.
const MaxAttemptIDBytes = 256

// AttemptID is the immutable identity of ONE authorized dispatch.
//
// IT IS MINTED BEFORE THE ATTEMPT IS WRITTEN AND NEVER AFTERWARDS. The store
// writes it into the record immutably at BeginDispositionAttempt and no
// transition may rewrite one, which is what lets a successor tell the attempt
// it is recovering from the attempt it would have made.
type AttemptID string

// Validate refuses an identity either downstream module would refuse, which is
// the same rule stated once.
func (id AttemptID) Validate() error {
	switch {
	case id == "":
		return &ApplyError{Refusal: RefusalInvalidAttempt, Reason: "the attempt identity is empty"}
	case len(id) > MaxAttemptIDBytes:
		return &ApplyError{
			Refusal: RefusalInvalidAttempt,
			Reason:  "the attempt identity is longer than " + strconv.Itoa(MaxAttemptIDBytes) + " bytes",
		}
	case !utf8.ValidString(string(id)):
		return &ApplyError{Refusal: RefusalInvalidAttempt, Reason: "the attempt identity is not valid UTF-8"}
	default:
		return nil
	}
}

// AttemptIDs mints one identity per authorized dispatch.
//
// IT IS A SEAM RATHER THAN A FUNCTION CALL because the identity must be UNIQUE
// across every dispatch this deployment ever makes, and uniqueness is a property
// of the source rather than of this package. A test needs a source it can name;
// production needs one that cannot collide. Both are the same shape.
//
// A MINTER THAT REPEATS ITSELF IS A CORRECTNESS FAILURE AND NOT A COSMETIC ONE:
// two attempts sharing an identity would let one attempt's evidence settle the
// other's command, which is precisely the correlation the identity exists to
// make impossible.
type AttemptIDs interface {
	NewAttemptID() (AttemptID, error)
}

// ---------------------------------------------------------------------------
// The durable record
// ---------------------------------------------------------------------------

// DispositionRecord is the disposition-family inbox record as an applier reads
// it. It is Record's counterpart and deliberately not Record itself: the two
// families carry different members, and one type spanning both would let a
// legacy field be read on a disposition record and answer a plausible zero.
//
// THE TWO EPOCHS ARE TWO TYPES OF NUMBER AND ARE NEVER COMPARED. ClaimResidency
// is Host's own grant over the session; AttemptJournalEpoch is the RUNTIME's
// grant over its journal, returned by the runtime itself. sessionstore says in
// terms that a residency epoch "must never be compared with, or used as, a
// journal epoch", and a Host that copied one into the other would author an
// attempt no evidence could ever match.
type DispositionRecord struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	// RuntimeCommandID is the UUID the winning acceptance record allocated. It
	// is read from the durable record and never minted here.
	RuntimeCommandID uuid.UUID

	Kind  Kind
	State State

	AcceptedOrder uint64
	Revision      uint64
	ApplyDeadline time.Time

	// ClaimResidencyEpoch and ClaimExpiresAt are the current claim, or the zero
	// values when there is none. A terminal record keeps the claim that settled
	// it.
	ClaimResidencyEpoch uint64
	ClaimExpiresAt      time.Time

	// AttemptID, AttemptJournalEpoch and AttemptResidencyEpoch are the ONE
	// authorized dispatch, or the zero values when none was authorized. They are
	// immutable once written.
	AttemptID             AttemptID
	AttemptJournalEpoch   uint64
	AttemptResidencyEpoch uint64
}

// hasAttempt reports whether a dispatch was durably authorized for this record.
// It is the whole recovery signal on this side: the store refuses a claim of a
// record with an attempt at any residency, because among non-terminal records an
// attempt exists exactly when the state is applying.
func (r DispositionRecord) hasAttempt() bool { return r.AttemptID != "" }

// ---------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------

// DispositionClaim takes a short-lived claim so one writer works on a command at
// a time.
//
// IT NAMES NO EPOCH, AND THE ABSENCE IS STRUCTURAL RATHER THAN TIDY.
// ClaimDispositionCommand takes a *ResidencyGrant and derives the epoch from it,
// refusing anything that is not a live-looking grant THAT STORE issued for THAT
// session — so no number a caller chose can reach the record's high-water mark.
// A member here would be a number this package could get wrong, for a value the
// store would not read.
type DispositionClaim struct {
	ExpectedRevision uint64
	ExpiresAt        time.Time
}

// DispositionAttempt authorizes exactly one dispatch and records it immutably.
type DispositionAttempt struct {
	ExpectedRevision uint64

	// AttemptID is the minted identity this dispatch will carry to the runtime.
	AttemptID AttemptID

	// JournalEpoch is the RUNTIME's own grant over its journal, read from the
	// runtime and never from this Host's residency.
	JournalEpoch uint64

	// ResidencyEpoch is the Host grant the dispatch is authorized under. The
	// store fences it to EQUAL the claim's own, so it cannot raise the mark.
	ResidencyEpoch uint64

	StartedAt time.Time
}

// DispositionSettlement asks the store to settle from evidence.
//
// IT CARRIES NO OUTCOME, and that is the protocol rather than a narrow request
// shape. The store derives the expected descriptor from its own record, obtains
// the evidence through its configured reader and verifies it before the write; a
// caller that could supply an outcome could talk a settlement into a state the
// journal does not support.
type DispositionSettlement struct {
	ExpectedRevision uint64

	// ResidencyEpoch is settlement CONTEXT and not authority. It is recorded in
	// the outcome, fenced against the claim's high-water mark, and compared with
	// no journal epoch. The evidence is what authorizes the settlement.
	ResidencyEpoch uint64
}

// DispositionRejection refuses a command BEFORE any dispatch of it was durably
// authorized, which is the only rejection this protocol lets a Host make.
//
// IT EXISTS FOR ONE KIND AND ONE REASON, and it used to be deliberately absent.
// Once an attempt exists the store decides the terminal arm from evidence, and
// the apply deadline is FACTORY's reconciler's to act on, not a Host's that
// merely observed a clock — so until gate_response no path in this Host had a
// rejection to make, and a Host that rejected on its own reading would durably
// destroy work a successor would have done. A gate_response body no Host could
// ever apply is different: it is immutable, every Host reads the same bytes,
// and harness refuses it only AFTER the attempt, when nothing would then settle
// the command. So it is rejected here, while the command is still claimed. A
// gate this Host does not yet own is NOT a reason: that blocks and is retried.
//
// It carries no reason, because the store records none (sessionstore v0.8.0's
// stated cost), and no epoch a caller could raise: the residency it names is
// this Host's own, which the released edge requires to be the live claim's.
type DispositionRejection struct {
	ExpectedRevision uint64

	// ResidencyEpoch is the Host grant holding the claim.
	ResidencyEpoch uint64
}

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------

// DispositionRecords reads the private durable command state of one
// disposition-family command.
type DispositionRecords interface {
	// LoadDispositionCommand returns the full record for one command, including
	// the revision a later compare-and-swap names.
	LoadDispositionCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (DispositionRecord, error)

	// LoadDispositionPayload returns the private body or its object reference.
	// It is called only after a claim has been committed.
	LoadDispositionPayload(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) (Payload, error)
}

// DispositionWrites is every durable transition a disposition applier makes.
//
// EVERY METHOD HERE IS FENCED, by the same structural guard that covers
// InboxWrites and CursorWrites: TestEveryDurableWriteGoesThroughTheFence
// collects the methods of every interface in this package whose name ends in
// "Writes" and requires each call to one of them to be lexically inside a
// Fence.Write. DO NOT RENAME THIS TYPE OUT OF THE CONVENTION.
type DispositionWrites interface {
	// ClaimDisposition CASes a pending record into claimed under the caller's
	// residency GRANT and returns its new revision. The grant is the
	// implementation's, bound to this session; see DispositionClaim.
	ClaimDisposition(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, claim DispositionClaim) (uint64, error)

	// BeginAttempt CASes a claimed record into applying and records the attempt
	// immutably. Only the holder of a LIVE claim at the claim's own residency may
	// make it, and there is no deadline check: an applying record is closed by
	// evidence rather than by a timer.
	BeginAttempt(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, attempt DispositionAttempt) (uint64, error)

	// SettleDisposition settles an applying command from durable evidence and
	// reports the terminal state the STORE chose. A caller does not choose it.
	SettleDisposition(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, settlement DispositionSettlement) (State, error)

	// RejectDisposition rejects a claimed command before any attempt. See
	// DispositionRejection for the one case that uses it.
	RejectDisposition(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, rejection DispositionRejection) error
}

// AttemptClosers drives a successor's recovery closure for a PREDECESSOR's
// stranded attempt. It is department.AttemptCloser, restated in this package's
// own vocabulary — a commands.Kind and a commands.AttemptID rather than two
// strings — so a mis-ordered pair of same-typed parameters is a compile failure
// here rather than a silently wrong closure.
//
// IT IS DISCOVERED BY ASSERTION AND IS OPTIONAL, exactly as harness's
// runtimecommand.AttemptCloser is: folding it into the ordinary dispatch seam
// would make every implementer of that seam carry a capability only a recovery
// path uses. A composition without one simply never closes a predecessor's
// attempt, and the consequence is a blocked pass rather than a wrong settlement.
type AttemptClosers interface {
	// CloseAttempt durably records that the named attempt was NOT applied, under
	// a grant strictly later than the attempt's own.
	//
	// IT IS REFUSED RATHER THAN FORCED when the runtime's journal holds an
	// enduring event caused by that command: a tombstone over a committed effect
	// is the one error this protocol cannot recover from. Such a refusal is NOT
	// retryable and must never be retried into a tombstone.
	CloseAttempt(ctx context.Context, command sessionwire.CommandID, runtimeCommand uuid.UUID, kind Kind, attempt AttemptID, attemptJournalEpoch uint64) error
}

// ErrEnduringEffect and ErrDispositionUnsupported are department's sentinels,
// aliased here so this package's own callers name one vocabulary.
//
// THEY ARE ALIASES AND NOT COPIES. An independently-declared sentinel with the
// same name would compare unequal to the one the adapter actually raises, so
// every errors.Is in this package would silently answer false and both refusals
// would collapse into RefusalRuntime — the exact conflation harness exported its
// error type to prevent.
var (
	// ErrEnduringEffect reports a closure refused because the runtime's journal
	// holds a durable effect caused by the command being closed. IT IS TERMINAL
	// and must never be retried into a tombstone.
	ErrEnduringEffect = department.ErrEnduringEffect

	// ErrDispositionUnsupported reports a dispatch refused because the runtime's
	// durable log cannot record a disposition. NOTHING DURABLE WAS WRITTEN, so
	// the command MAY be re-offered, which is not what a transport failure means.
	ErrDispositionUnsupported = department.ErrDispositionUnsupported

	// ErrNoAttemptCloser reports a runtime that offers no recovery closure. It is
	// a sentinel rather than an absent capability because a wrapper that forwards
	// an optional capability satisfies the interface for every runtime it wraps,
	// so an assertion can no longer answer the question.
	ErrNoAttemptCloser = department.ErrNoAttemptCloser

	// ErrPrefixCommitted marks a dispatch that failed after the runtime durably
	// recorded the command's application prefix.
	ErrPrefixCommitted = department.ErrPrefixCommitted
)

// ErrEvidenceUnroutable reports a settlement whose evidence could not be OBTAINED
// because of this deployment's wiring.
//
// IT IS DECLARED HERE AND RAISED BY THE CONCRETE EDGE, which is the direction the
// other sentinels in this file do NOT go — they are department's, because a
// runtime raises them. This one is a STORE fact, and internal/sessionstoreadapter
// imports this package rather than the other way round, so here is the one place
// both the raiser and the reader can name it.
//
// WHY IT IS A SENTINEL AT ALL. Both classifiable causes — an unregistered binding
// and a reader that refuses to resolve the session — already arrive at the
// applier as typed errors with their cause intact; the information was being
// discarded by one unconditional mapping, not missing. Recovering it costs no
// upstream API and leaks nothing: a refusal code names no tenant, session,
// binding or body.
var ErrEvidenceUnroutable = errors.New("commands: this session's settlement evidence could not be obtained from any configured reader, which is a wiring failure rather than a runtime that recorded nothing")

// ---------------------------------------------------------------------------
// The production minter
// ---------------------------------------------------------------------------

// UUIDAttemptIDs mints an attempt identity per dispatch from a fresh random
// UUID.
//
// THE ONE PROPERTY THAT MATTERS IS THAT IT NEVER REPEATS, across processes and
// across Hosts, and a random v4 UUID is what this workspace already uses for
// exactly that. A counter would be wrong rather than merely weaker: two Host
// replicas racing one session would mint the same names, and an attempt identity
// is what decides whether a piece of evidence is about THIS attempt — so a
// collision would let one attempt's disposition settle another's command.
//
// IT IS NOT DERIVED FROM THE COMMAND, and that is also a correctness point. A
// successor recovering a stranded attempt must be able to tell the attempt it is
// closing from the attempt it would itself have made, and a deterministic
// function of the command identity would give both the same name.
type UUIDAttemptIDs struct{}

// UUIDAttemptIDs is the minter seam.
var _ AttemptIDs = UUIDAttemptIDs{}

// NewAttemptID returns a fresh identity, or the entropy failure that stopped it.
//
// A FAILURE IS REPORTED RATHER THAN WORKED AROUND. There is no fallback to a
// counter or to a clock: an identity that might repeat is worse than no
// dispatch, because the dispatch it would authorize is one whose evidence could
// settle somebody else's command.
func (UUIDAttemptIDs) NewAttemptID() (AttemptID, error) {
	id, err := uuid.New()
	if err != nil {
		return "", err
	}
	return AttemptID(id.String()), nil
}
