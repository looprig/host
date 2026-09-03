// Recovery is the half of §10.4 that exists because a Host can stop between any
// two durable writes, and this file is the decision it makes when it starts
// again.
//
// THERE IS ONE QUESTION AND IT IS NOT "how far did I get". A record's own state
// cannot answer it: an `applying` record MAY have a committed application
// prefix and may equally have been moved into applying a moment before the
// process died, with nothing written and no effect begun. The question is
// whether a PREFIX EXISTS, because that is the only durable fact that separates
// "an effect may have begun" from "nothing happened", and §10.4 gives the two
// cases opposite answers: finish the prefix without driving the runtime again,
// or claim and apply from the start.
//
// THE APPLY DEADLINE ASYMMETRY FOLLOWS FROM THE SAME LINE. A new claim may not
// start at or after the deadline, and finishing an owned prefix may, because
// §10.4 says recovering an expired `applying` prefix is CONTINUATION of an
// existing application rather than creation of a new claim. A rule that refused
// both would strand exactly the commands whose effects already reached a
// runtime.
package commands

import (
	"context"
	"strconv"
	"time"
)

// step is what plan decided to do with a record.
type step int

const (
	// stepApplyFresh claims the record and runs the whole protocol.
	stepApplyFresh step = iota

	// stepFinishPrefix finalizes an application whose prefix is already
	// committed, WITHOUT driving the runtime again.
	stepFinishPrefix
)

// plan decides what to do with one non-terminal record.
//
// IT WRITES NOTHING AND READS NO CLOCK OF ITS OWN. Every input is a parameter,
// so the whole decision is a pure function of the durable record, the prefix,
// and one instant — which is what lets a table cover the cases that only differ
// by a nanosecond either side of a deadline.
//
// THE ORDER OF THE CHECKS IS PART OF THE RULE. Ownership is decided before
// anything else, because a Host that no longer owns the session must not reason
// about the record at all; a live claim by another epoch is respected before
// the prefix is acted on, because §10.4 lets an unexpired claim win and the
// holder of that claim may be committing a prefix of its own this instant.
func (a *Applier) plan(record Record, prefix Prefix, owned bool, now time.Time) (step, error) {
	// A LATER EPOCH HAS COMMITTED. The fence would refuse this applier's next
	// write anyway, once it saw the store say so; refusing here means no write
	// is attempted at all, and the difference matters because the first write
	// this applier would attempt is a CLAIM — the one write whose success would
	// take work away from the Host that legitimately owns the session.
	if record.ClaimEpoch > a.epoch {
		return stepApplyFresh, &ApplyError{
			Refusal:   RefusalStaleEpoch,
			CommandID: record.CommandID,
			Reason: "the record is claimed under epoch " + strconv.FormatUint(record.ClaimEpoch, 10) +
				" and this applier holds " + strconv.FormatUint(a.epoch, 10) + ", so this Host no longer owns the session",
		}
	}
	if owned {
		if err := a.checkPrefix(record, prefix); err != nil {
			return stepApplyFresh, err
		}
	}
	// AN UNEXPIRED CLAIM WINS, §10.4's words. The claim epoch is not this
	// applier's — the case where it is, is this applier resuming its own work
	// after a failed pass, and it continues rather than waiting for a claim it
	// holds itself to expire.
	//
	// THIS CARRIED A `ClaimEpoch != 0` CONJUNCT AND NO LONGER DOES. It was
	// measured unkillable, and it was unkillable because it restated what the
	// expiry already says: every write that sets a claim epoch sets an expiry in
	// the same revision, so an unclaimed record's expiry is the zero instant and
	// no `now` is before it. The conjunct only changed the outcome for a record
	// carrying epoch 0 with a future expiry, which is a store contradicting
	// itself, and it changed it in the direction of CLAIMING that record rather
	// than leaving it alone.
	if record.ClaimEpoch != a.epoch && now.Before(record.ClaimExpiresAt) {
		return stepApplyFresh, &ApplyError{
			Refusal:   RefusalClaimHeld,
			CommandID: record.CommandID,
			Reason: "epoch " + strconv.FormatUint(record.ClaimEpoch, 10) + " holds an unexpired claim on this command until " +
				record.ClaimExpiresAt.String(),
		}
	}
	if owned {
		return stepFinishPrefix, nil
	}
	// NOTHING BEGAN, so from here the record needs a NEW claim and every rule
	// about new claims applies.
	if !record.Kind.known() {
		return stepApplyFresh, &ApplyError{
			Refusal:   RefusalUnsupportedKind,
			CommandID: record.CommandID,
			Reason: "the kind " + strconv.Quote(string(record.Kind)) +
				" is not one this Host applies; the apply deadline is what makes such a record terminal, and it is Factory's reconciler that writes it",
		}
	}
	if !now.Before(record.ApplyDeadline) {
		return stepApplyFresh, &ApplyError{
			Refusal:   RefusalDeadlinePassed,
			CommandID: record.CommandID,
			Reason: "a new claim cannot start at or after the apply deadline " + record.ApplyDeadline.String() +
				"; this command is Factory's deadline reconciler's to reject",
		}
	}
	return stepApplyFresh, nil
}

// checkPrefix refuses a prefix that correlates something other than this
// record.
//
// BOTH IDENTITIES, AND THE EPOCH. §10.4 correlates CommandID and
// RuntimeCommandID with the lease epoch, and a prefix matching one of the three
// is not evidence about this command's effect: acting on it would finalize a
// command as applied on the strength of a different command's application. A
// prefix from a LATER epoch is the same fact ClaimEpoch reports one check
// above, arriving on the other record.
func (a *Applier) checkPrefix(record Record, prefix Prefix) error {
	switch {
	case prefix.CommandID != record.CommandID:
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason:    "the application prefix names the command " + strconv.Quote(string(prefix.CommandID)),
		}
	case prefix.RuntimeCommandID != record.RuntimeCommandID:
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason: "the application prefix names the runtime command " + prefix.RuntimeCommandID.String() +
				" and the record allocated " + record.RuntimeCommandID.String(),
		}
	case prefix.LeaseEpoch > a.epoch:
		return &ApplyError{
			Refusal:   RefusalStaleEpoch,
			CommandID: record.CommandID,
			Reason: "the application prefix was committed under epoch " + strconv.FormatUint(prefix.LeaseEpoch, 10) +
				", which is later than this applier's " + strconv.FormatUint(a.epoch, 10),
		}
	case record.State == StatePending:
		return &ApplyError{
			Refusal:   RefusalCorrelation,
			CommandID: record.CommandID,
			Reason:    "an application prefix exists for a record that was never claimed, which no ordering of §10.4's writes produces",
		}
	default:
		return nil
	}
}

// finishPrefix completes an application whose prefix is durably committed.
//
// IT DRIVES NO RUNTIME, and that is the whole of exactly-once on this path.
// §10.4: after an `applying` claim expires the next lease holder uses the
// journal correlation to finish the durable prefix and marks `applied`, EVEN
// AFTER THE DEADLINE. The terminal CAS is stamped with THIS applier's epoch,
// not the prefix's: the write is fenced by the grant the writer holds, and a
// successor finishing a predecessor's prefix is the case that exists for.
func (a *Applier) finishPrefix(ctx context.Context, record Record) (Outcome, error) {
	return a.finalize(ctx, record, Applied(), true)
}
