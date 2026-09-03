// Recovery is the half of §10.4 that exists because a Host can stop between any
// two durable writes, and this file is the decision it makes when it starts
// again.
//
// THERE IS ONE QUESTION AND THE RECORD CANNOT ANSWER IT. An `applying` record
// MAY have a committed effect and may equally have been moved into applying a
// moment before the process died, with nothing appended and nothing begun. What
// separates them is the JOURNAL: a correlated prefix followed by the public
// event that carried the effect means the command has been applied, whichever
// lease did it; a prefix followed by a fence above its own epoch means the
// application started and its writer lost the stream; no prefix at all means
// nothing happened. Those are different settlements, and a seam that could only
// say "a prefix exists" could not tell them apart.
//
// THE APPLY DEADLINE ASYMMETRY FOLLOWS FROM THE SAME LINE. A new claim may not
// start at or after the deadline, and finishing an application may, because
// §10.4 says recovering an expired `applying` prefix is CONTINUATION of an
// existing application rather than creation of a new claim. A rule that refused
// both would strand exactly the commands whose effects already reached a
// runtime.
//
// WHAT THIS FILE NEVER DOES IS RE-CLAIM AN APPLYING RECORD. Applying is a
// fortress: it cannot be re-claimed at any epoch and it cannot be renewed, so
// the only moves out of it are completing it against a committed effect and
// rejecting it against proof that none committed. An earlier version of this
// file claimed an expired applying record and applied it afresh, which the store
// this runs on refuses outright — and refuses for the reason the rule exists: a
// second application of a command whose first may still commit.
package commands

import (
	"strconv"
	"time"
)

// step is what plan decided to do with a record.
type step int

const (
	// stepClaim takes a claim and runs the whole protocol.
	stepClaim step = iota

	// stepApplyUnderHeldClaim continues under a live claim this applier already
	// holds, WITHOUT re-taking it: a claim cannot be renewed, and re-claiming
	// one that is live is refused even at the same epoch.
	stepApplyUnderHeldClaim

	// stepDriveApplyingRecord re-drives an applying record this applier owns
	// whose journal shows no application began. It does not re-enter applying,
	// which the store admits only from claimed.
	stepDriveApplyingRecord

	// stepComplete settles a command whose effect the journal proves committed,
	// WITHOUT driving the runtime again.
	stepComplete

	// stepReject settles an application a superseded lease abandoned, on proof
	// that no effect committed and that its writer is fenced out.
	stepReject
)

// plan decides what to do with one non-terminal record.
//
// IT WRITES NOTHING AND READS NO CLOCK OF ITS OWN. Every input is a parameter, so
// the whole decision is a pure function of the durable record, the journal
// correlation, and one instant — which is what lets a table cover the cases that
// only differ by a nanosecond either side of a deadline.
//
// THE ORDER OF THE CHECKS IS PART OF THE RULE. Ownership is decided before
// anything else, because a Host that no longer owns the session must not reason
// about the record at all; the correlation is checked before it is acted on,
// because a prefix naming another runtime command is evidence about another
// application; and a committed effect outranks every remaining question, because
// the command HAS been applied and recording that is correct however late.
func (a *Applier) plan(record Record, application Application, now time.Time) (step, error) {
	// A LATER EPOCH HAS COMMITTED. The fence would refuse this applier's next
	// write anyway, once it saw the store say so; refusing here means no write
	// is attempted at all, and the difference matters because the first write
	// this applier would attempt is a CLAIM — the one write whose success would
	// take work away from the Host that legitimately owns the session.
	if record.ClaimEpoch > a.epoch {
		return stepClaim, &ApplyError{
			Refusal:   RefusalStaleEpoch,
			CommandID: record.CommandID,
			Reason: "the record is claimed under epoch " + strconv.FormatUint(record.ClaimEpoch, 10) +
				" and this applier holds " + strconv.FormatUint(a.epoch, 10) + ", so this Host no longer owns the session",
		}
	}
	if err := a.checkApplication(record, application); err != nil {
		return stepClaim, err
	}

	// A COMMITTED EFFECT IS A FACT, and it outranks the claim, the deadline and
	// the state's suggestion. The one thing it does not outrank is the store's
	// own precondition: an applied record is reached from applying, so a
	// committed effect beside a record that never entered applying is a
	// contradiction rather than a shortcut.
	if application.Outcome == ApplicationCommitted {
		if record.State != StateApplying {
			return stepClaim, &ApplyError{
				Refusal:   RefusalCorrelation,
				CommandID: record.CommandID,
				Reason: "the journal reports a committed effect for a record in state " + strconv.Quote(string(record.State)) +
					", and an application is completed only from applying",
			}
		}
		return stepComplete, nil
	}

	// AN UNEXPIRED CLAIM WINS, §10.4's words, and this is deliberately STRICTER
	// than the store: a strictly greater epoch is permitted there to take a live
	// claim, on the ground that its holder's lease is gone and making a successor
	// wait out a TTL stalls every claimed command on every failover. The spec
	// sentence is the one this task is held to, and the cost of the narrower rule
	// is bounded by one claim TTL per command; the cost of the wider one is a
	// second writer on a command whose first may still be inside a runtime call
	// this Host cannot see. The case where the epoch IS this applier's is not
	// somebody else's claim and is handled below.
	//
	// THIS CARRIED A `ClaimEpoch != 0` CONJUNCT AND NO LONGER DOES. It was
	// measured unkillable, and it was unkillable because it restated what the
	// expiry already says: every write that sets a claim epoch sets an expiry in
	// the same revision, so an unclaimed record's expiry is the zero instant and
	// no `now` is before it.
	claimLive := now.Before(record.ClaimExpiresAt)
	if claimLive && record.ClaimEpoch != a.epoch {
		return stepClaim, &ApplyError{
			Refusal:   RefusalClaimHeld,
			CommandID: record.CommandID,
			Reason: "epoch " + strconv.FormatUint(record.ClaimEpoch, 10) + " holds an unexpired claim on this command until " +
				record.ClaimExpiresAt.String(),
		}
	}

	if record.State == StateApplying {
		return a.planApplying(record, application)
	}

	// A LIVE CLAIM AT THIS APPLIER'S EPOCH IS NOT RE-TAKEN. A claim cannot be
	// renewed: re-claiming one that is live is refused for as long as it is
	// live, even at the same epoch, and the one move that remains is to enter
	// applying before it lapses. An earlier version re-claimed here and would
	// have been refused by the store on every resumed pass.
	if claimLive && record.State == StateClaimed {
		return stepApplyUnderHeldClaim, nil
	}

	// NOTHING BEGAN, so from here the record needs a NEW claim and every rule
	// about new claims applies.
	if !record.Kind.known() {
		return stepClaim, &ApplyError{
			Refusal:   RefusalUnsupportedKind,
			CommandID: record.CommandID,
			Reason: "the kind " + strconv.Quote(string(record.Kind)) +
				" is not one this Host applies; the apply deadline is what makes such a record terminal, and it is Factory's reconciler that writes it",
		}
	}
	if !now.Before(record.ApplyDeadline) {
		return stepClaim, &ApplyError{
			Refusal:   RefusalDeadlinePassed,
			CommandID: record.CommandID,
			Reason: "a new claim cannot start at or after the apply deadline " + record.ApplyDeadline.String() +
				"; this command is Factory's deadline reconciler's to reject",
		}
	}
	return stepClaim, nil
}

// planApplying decides an applying record with no committed effect.
//
// THE TWO ARMS ARE THE TWO WRITERS. Under this applier's OWN epoch the record is
// its own unfinished work: the journal shows nothing committed, so the effect
// never began and driving it again is the continuation of one application rather
// than a second one. Under a PREDECESSOR's epoch it is somebody else's
// unfinished work, and this applier may settle it only on the two proofs §10.4
// requires — that no effect committed, and that the writer which might still
// commit one has been fenced out of the stream.
func (a *Applier) planApplying(record Record, application Application) (step, error) {
	if application.Outcome == ApplicationUnresolved {
		return stepClaim, &ApplyError{
			Refusal:   RefusalUnresolvedApplication,
			CommandID: record.CommandID,
			Reason:    "a correlated prefix exists whose outcome the journal cannot yet decide, so neither completing nor rejecting is safe",
		}
	}
	if record.ClaimEpoch == a.epoch {
		return stepDriveApplyingRecord, nil
	}
	if !application.provesNoEffect() {
		return stepClaim, &ApplyError{
			Refusal:   RefusalUnresolvedApplication,
			CommandID: record.CommandID,
			Reason:    "the journal reports the application as " + strconv.Quote(string(application.Outcome)) + ", which proves neither an effect nor its absence",
		}
	}
	// THE FENCE, NOT THE EPOCH. Holding a later lease epoch says this applier
	// may act; it does not say the predecessor cannot. Only a committed opening
	// fence above the applier's epoch proves the effect a rejection would orphan
	// can never be appended — and that fence is written by this Host's own
	// attach, so the case where it is missing is a session that has not been
	// re-opened yet rather than a permanent state.
	if !application.fences(record.ClaimEpoch) {
		return stepClaim, &ApplyError{
			Refusal:   RefusalUnfencedApplier,
			CommandID: record.CommandID,
			Reason: "the journal has observed no opening fence above epoch " + strconv.FormatUint(record.ClaimEpoch, 10) +
				", so the applier holding that lease could still commit the effect this rejection would orphan",
		}
	}
	return stepReject, nil
}

// checkApplication refuses a correlation that is about something other than this
// record.
//
// THE RUNTIME IDENTITY IS THE CORRELATION. §10.4 correlates CommandID and
// RuntimeCommandID with the lease epoch, and a prefix matching the public
// identity while naming another runtime command is the durable mapping being
// broken rather than evidence about this command: completing on it would record
// a different application's effect as this one's.
func (a *Applier) checkApplication(record Record, application Application) error {
	if !application.Outcome.known() {
		return &ApplyError{
			Refusal:   RefusalUnresolvedApplication,
			CommandID: record.CommandID,
			Reason:    "the journal reported the outcome " + strconv.Quote(string(application.Outcome)) + ", which is not one this package can act on",
		}
	}
	if application.Outcome == ApplicationConflicted {
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason:    "the journal carries a prefix naming this command with a different runtime identity or kind, so no settlement is safe and an operator has to look",
		}
	}
	if !application.hasPrefix() {
		return nil
	}
	switch {
	case application.CommandID != record.CommandID:
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason:    "the journal correlation is about the command " + strconv.Quote(string(application.CommandID)),
		}
	case application.RuntimeCommandID != record.RuntimeCommandID:
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason: "the correlated prefix names the runtime command " + application.RuntimeCommandID.String() +
				" and the record allocated " + record.RuntimeCommandID.String(),
		}
	case application.PrefixEpoch > a.epoch:
		return &ApplyError{
			Refusal:   RefusalStaleEpoch,
			CommandID: record.CommandID,
			Reason: "the correlated prefix was appended under epoch " + strconv.FormatUint(application.PrefixEpoch, 10) +
				", which is later than this applier's " + strconv.FormatUint(a.epoch, 10),
		}
	default:
		return nil
	}
}
