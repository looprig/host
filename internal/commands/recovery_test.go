package commands

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// advance moves the fixture clock, so a deadline and a claim expiry can be
// crossed by the instant rather than by rewriting the record.
func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// expired is an instant before the fixture clock: a claim held until then is
// over.
var expired = testClockAt.Add(-time.Second)

// unexpired is an instant after the fixture clock and inside the apply
// deadline.
var unexpired = testClockAt.Add(5 * time.Second)

// committedApplication is what the journal proves once a command's effect has
// landed: a correlated prefix followed by the public event that carried it.
func committedApplication(epoch uint64) Application {
	return Application{
		CommandID:        commandID(1),
		RuntimeCommandID: testRuntimeCommandID,
		Outcome:          ApplicationCommitted,
		PrefixEpoch:      epoch,
		EffectEventID:    testEffectEventID,
		EffectSeq:        testEffectSeq,
	}
}

// abandonedApplication is what the journal proves when an application started
// and its writer lost the stream: a correlated prefix followed by an opening
// fence above its own epoch.
func abandonedApplication(epoch, fencedBy uint64) Application {
	return Application{
		CommandID:        commandID(1),
		RuntimeCommandID: testRuntimeCommandID,
		Outcome:          ApplicationAbandoned,
		PrefixEpoch:      epoch,
		SupersedingEpoch: fencedBy,
	}
}

// ---------------------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------------------

// TestAStaleEpochRefusesBeforeAnythingIsWritten asserts a record claimed under
// an epoch LATER than this applier's stops the work rather than racing it.
//
// The fence would refuse the write anyway once the store said so, and that is
// not the same thing: the first write this applier makes is a CLAIM, and a claim
// that succeeded would take a command away from the Host that legitimately owns
// the session.
func TestAStaleEpochRefusesBeforeAnythingIsWritten(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateClaimed, testLaterEpoch, unexpired
	})
	_, err := f.process()
	if refusalOf(err) != RefusalStaleEpoch {
		t.Fatalf("got %v, want a %q refusal", err, RefusalStaleEpoch)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d durable writes were made under an epoch a successor has superseded", writes)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v", driven)
	}
}

// TestAnUnexpiredClaimByAnotherEpochIsRespected holds §10.4's "an unexpired
// claim or applying wins", and its other half: once the claim has expired, the
// current lease holder takes the work over rather than leaving it forever.
//
// The two rows differ ONLY in the claim's expiry, which is what makes the expiry
// the thing under test rather than the state or the epoch.
//
// IT IS STRICTER THAN THE STORE, DELIBERATELY. sessionstore admits a strictly
// greater epoch against a live claim, on the ground that the holder's lease is
// gone and waiting out a TTL stalls every claimed command on every failover.
// §10.4's sentence is the one this task is held to, and the cost of the narrower
// rule is bounded by one claim TTL per command.
func TestAnUnexpiredClaimByAnotherEpochIsRespected(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name      string
		expiresAt time.Time
		refusal   ApplyRefusal
		claims    int
	}{
		{name: "unexpired", expiresAt: unexpired, refusal: RefusalClaimHeld, claims: 0},
		{name: "expired", expiresAt: expired, refusal: "", claims: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				record := &f.stored().record
				record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateClaimed, testOtherEpoch, testCase.expiresAt
			})
			_, err := f.process()
			if got := refusalOf(err); got != testCase.refusal {
				t.Fatalf("got %v (refusal %q), want refusal %q", err, got, testCase.refusal)
			}
			if len(f.store.claims) != testCase.claims {
				t.Errorf("the record was claimed %d times, want %d", len(f.store.claims), testCase.claims)
			}
		})
	}
}

// TestThisAppliersOwnLiveClaimIsNotReTaken is a correction rather than a new
// property, and the correction is the store's.
//
// A CLAIM CANNOT BE RENEWED. Re-claiming one that is live is refused for as long
// as it is live, at the SAME epoch as at any other, because the record carries no
// claimant identity and an equal epoch cannot prove "this is my own claim". The
// one move that remains to a writer holding a live claim is to enter applying
// before it lapses. An earlier version of this applier re-claimed here, which
// read as correct against a fake that did not enforce the rule and would have
// been refused by the store on every resumed pass.
func TestThisAppliersOwnLiveClaimIsNotReTaken(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateClaimed, testEpoch, unexpired
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if claims := f.store.claims; len(claims) != 0 {
		t.Errorf("the applier re-took its own live claim %+v, which the store refuses", claims)
	}
	if applyings := f.store.applyings; len(applyings) != 1 {
		t.Fatalf("the record entered applying %d times, want once", len(applyings))
	}
	if got := f.store.applyings[0].ExpectedRevision; got != testRevision {
		t.Errorf("BeginApplying named revision %d, want the revision the record was read at %d", got, testRevision)
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Errorf("the runtime was driven %d times, want once", len(driven))
	}
}

// ---------------------------------------------------------------------------
// The apply deadline
// ---------------------------------------------------------------------------

// TestANewClaimCannotStartAtOrAfterTheApplyDeadline holds §10.4's sentence
// exactly, including the boundary: AT the deadline is already too late.
//
// A HOST NEVER REJECTS FOR THE DEADLINE. The refusal leaves the record where it
// is and blocks the pass, which the consumer names; §10.4 gives the terminal CAS
// to the deadline reconciler, after it has checked that no correlated application
// evidence exists.
func TestANewClaimCannotStartAtOrAfterTheApplyDeadline(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		advance time.Duration
		refusal ApplyRefusal
	}{
		{"a nanosecond before", testApplyDeadline - time.Nanosecond, ""},
		{"exactly at", testApplyDeadline, RefusalDeadlinePassed},
		{"after", testApplyDeadline + time.Second, RefusalDeadlinePassed},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t)
			f.clock.advance(testCase.advance)
			_, err := f.process()
			if got := refusalOf(err); got != testCase.refusal {
				t.Fatalf("got %v (refusal %q), want refusal %q", err, got, testCase.refusal)
			}
			if testCase.refusal == "" {
				return
			}
			if len(f.store.claims) != 0 {
				t.Errorf("the command was claimed %+v at or after its apply deadline", f.store.claims)
			}
			if rejections := f.store.settledRejections(); len(rejections) != 0 {
				t.Errorf("the Host rejected the command as %+v; the deadline reconciler is what rejects a command whose deadline passed", rejections)
			}
		})
	}
}

// TestAPersistentlyRefusedCommandStopsBeingClaimedAtItsDeadline bounds the
// wedge a post-claim refusal could otherwise create.
//
// A command this Host claims and then cannot apply — a payload it cannot read, a
// kind it does not know — leaves the record CLAIMED under this applier's own
// epoch. While that claim is live this applier goes on retrying, which is
// correct: an unexpired claim wins the deadline race. What must not happen is a
// FRESH claim after the deadline, because the deadline reconciler may settle
// only a pending record or one whose claim has lapsed — so an applier that
// re-claimed every pass would hold the command out of the reconciler's reach
// forever and §16's typed terminal rejection would never be written.
//
// The bound is therefore one claim TTL: the claim lapses, the deadline refuses a
// new one, and the record is the reconciler's.
func TestAPersistentlyRefusedCommandStopsBeingClaimedAtItsDeadline(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		stored := f.put(KindInput, StatePending)
		stored.payload = Payload{}
		stored.record.State, stored.record.ClaimEpoch, stored.record.ClaimExpiresAt = StateClaimed, testEpoch, expired
	})
	f.clock.advance(testApplyDeadline)
	_, err := f.process()
	if refusalOf(err) != RefusalDeadlinePassed {
		t.Fatalf("got %v, want a %q refusal", err, RefusalDeadlinePassed)
	}
	if claims := f.store.claims; len(claims) != 0 {
		t.Errorf("the command was claimed %+v past its deadline, which holds it out of the deadline reconciler's reach", claims)
	}
	if state, due := f.store.stateOf(commandID(1)); state != StateClaimed || !due {
		t.Errorf("the record is %q (due %v), want a lapsed claimed record the reconciler can settle", state, due)
	}
}

// TestACommittedEffectIsRecordedAfterTheDeadline is the asymmetry the deadline
// rule is written with: finishing an application is CONTINUATION of work that
// started before the deadline, not a new claim, so it happens even after it. A
// rule that refused both would leave a command whose effect is visible to a
// client sitting unfinished forever.
func TestACommittedEffectIsRecordedAfterTheDeadline(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		f.stored().application = committedApplication(testOtherEpoch)
	})
	f.clock.advance(testApplyDeadline + time.Hour)
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v for an application whose effect had already committed", driven)
	}
}

// TestAnUnsupportedKindIsRefusedRatherThanRejected asserts an older Host does not
// durably destroy work a newer one would have done. The record is left for a Host
// that understands the kind, and the apply deadline is what bounds how long that
// can last.
func TestAnUnsupportedKindIsRefusedRatherThanRejected(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) { f.put(Kind("checkpoint"), StatePending) })
	_, err := f.process()
	if refusalOf(err) != RefusalUnsupportedKind {
		t.Fatalf("got %v, want a %q refusal", err, RefusalUnsupportedKind)
	}
	if rejections := f.store.settledRejections(); len(rejections) != 0 {
		t.Errorf("the command was rejected as %+v by a Host that does not know what it asks for", rejections)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d durable writes were made for a kind this Host cannot apply", writes)
	}
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

// TestACrashAfterTheEffectCommittedIsCompletedWithoutReapplying is the case the
// correlation exists for.
//
// The record says applying and the journal carries the prefix followed by the
// public event that carried the effect, so the command HAS been applied and
// nothing durable would be gained by driving it again. What is left is to record
// it — naming the event the journal found, because that is what the terminal
// write is.
func TestACrashAfterTheEffectCommittedIsCompletedWithoutReapplying(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testEpoch, expired
		f.stored().application = committedApplication(testEpoch)
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Fatalf("the runtime was driven with %+v for an effect that had already committed; it would happen twice", driven)
	}
	for _, operation := range f.store.operations() {
		if operation == "ClaimCommand" || operation == "BeginApplying" || operation == "AppendApplicationPrefix" {
			t.Errorf("%s ran while recovering an application; recovering is continuation, not a new claim", operation)
		}
	}
	completions := f.store.completions()
	if len(completions) != 1 {
		t.Fatalf("the command was completed %d times, want once", len(completions))
	}
	if want := (Effect{CompletedAt: testClockAt, EventID: testEffectEventID, JournalSeq: testEffectSeq}); completions[0].Effect != want {
		t.Errorf("the completion recorded %+v, want the correlated effect %+v", completions[0].Effect, want)
	}
}

// TestASuccessorCompletesAPredecessorsCommittedApplication asserts §10.4's
// successor case: the next lease holder finishes an application it did not
// start.
//
// THE STORE HOLDS A SUCCESSOR TO THE JOURNAL and takes nothing on its word: a
// completion whose epoch is not the claim's is admitted only when the result
// NAMES the correlated effect. So this passes only because the applier records
// the event the journal found rather than one it composed, which is the property
// the same-epoch path cannot demonstrate.
func TestASuccessorCompletesAPredecessorsCommittedApplication(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		f.stored().application = committedApplication(testOtherEpoch)
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	completions := f.store.completions()
	if len(completions) != 1 {
		t.Fatalf("the command was completed %d times, want once", len(completions))
	}
	if completions[0].Epoch != testEpoch {
		t.Errorf("the completion was stamped with epoch %d, want this applier's %d", completions[0].Epoch, testEpoch)
	}
	if state, _ := f.store.stateOf(commandID(1)); state != StateApplied {
		t.Errorf("the record is %q, want %q; the store refused the successor's completion", state, StateApplied)
	}
}

// TestAnAbandonedApplicationIsRejectedBySuccessorWithEvidence asserts the other
// recovery, and the two proofs it rests on.
//
// A prefix followed by an opening fence above its own epoch means the
// application started and its writer lost the stream before committing anything.
// No effect exists, and the writer that might still commit one is fenced out — so
// a strictly later lease may settle the command rather than leaving it applying
// forever, which is the head-of-line hazard this closes.
func TestAnAbandonedApplicationIsRejectedBySuccessorWithEvidence(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		f.stored().application = abandonedApplication(testOtherEpoch, testEpoch)
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateRejected {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateRejected)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v for a command being settled", driven)
	}
	rejections := f.store.settledRejections()
	if len(rejections) != 1 {
		t.Fatalf("the command was rejected %d times, want once", len(rejections))
	}
	if rejections[0].Detail.Code == "" {
		t.Error("the rejection carries no stable typed code, and §10.4 makes the reason part of the terminal state")
	}
	if state, _ := f.store.stateOf(commandID(1)); state != StateRejected {
		t.Errorf("the record is %q, want %q; the store refused the successor's rejection", state, StateRejected)
	}
}

// TestAnAbandonedApplicationWhoseApplierIsNotFencedIsRefused is the half of the
// rule that a later epoch alone does not buy.
//
// Holding a later lease says this applier MAY act; it does not say the
// predecessor cannot. Only a committed opening fence above the applier's epoch
// proves the effect a rejection would orphan can never be appended. The fence
// arrives with this Host's own attach, so the missing case is a session that has
// not been re-opened rather than a permanent state — which is why it refuses
// rather than settles.
func TestAnAbandonedApplicationWhoseApplierIsNotFencedIsRefused(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		// The walk observed no fence above the applier's epoch.
		f.stored().application = abandonedApplication(testOtherEpoch, testOtherEpoch)
	})
	_, err := f.process()
	if refusalOf(err) != RefusalUnfencedApplier {
		t.Fatalf("got %v, want a %q refusal", err, RefusalUnfencedApplier)
	}
	if rejections := f.store.settledRejections(); len(rejections) != 0 {
		t.Errorf("the command was rejected as %+v while its applier could still commit the effect that would orphan", rejections)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d durable writes were made", writes)
	}
}

// TestAnUnresolvedApplicationIsRefused asserts the outcome on which EVERY move
// is unsafe, under both writers.
//
// A correlated prefix whose outcome cannot be read is either an application
// still in flight or one whose next record has not landed. Completing it would
// record an effect that may not exist; rejecting it would orphan one that may;
// and DRIVING IT AGAIN would apply a command whose first application may still
// commit. The answer may become readable later, so the pass stops rather than
// guessing — and a two-valued "a prefix exists" seam could not have expressed
// this case at all.
//
// THE OWN-EPOCH ROW IS THE ONE THAT MATTERS MOST, and it was missing until a
// mutation went looking for it: deleting the unresolved check survived the whole
// package, because for a PREDECESSOR's record the next check refuses with the
// same code for a different reason. Under this applier's own epoch nothing else
// refuses at all — the record would have been driven into the runtime a second
// time, which is the one hazard the whole correlation exists to prevent.
func TestAnUnresolvedApplicationIsRefused(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name  string
		epoch uint64
	}{
		{"under a predecessor's claim", testOtherEpoch},
		{"under this applier's own claim", testEpoch},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				record := &f.stored().record
				record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testCase.epoch, expired
				f.stored().application = Application{
					CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID,
					Outcome: ApplicationUnresolved, PrefixEpoch: testCase.epoch,
				}
			})
			outcome, err := f.process()
			if refusalOf(err) != RefusalUnresolvedApplication {
				t.Fatalf("got %v, want a %q refusal", err, RefusalUnresolvedApplication)
			}
			if !outcome.PrefixOwned {
				t.Error("PrefixOwned = false, though a correlated prefix exists")
			}
			if driven := f.runtime.commands(); len(driven) != 0 {
				t.Errorf("the runtime was driven with %+v while its previous application may still commit", driven)
			}
			if completions, rejections := f.store.completions(), f.store.settledRejections(); len(completions)+len(rejections) != 0 {
				t.Errorf("the command was settled as %+v / %+v on evidence that proves neither", completions, rejections)
			}
		})
	}
}

// TestAnApplyingRecordThisApplierOwnsIsDrivenWithoutReclaiming is the direct
// regression for the transition the store refuses.
//
// An applying record whose journal shows nothing is this applier's own
// unfinished work: no effect began, so driving it is the continuation of one
// application rather than a second. What must NOT happen is a re-claim — applying
// is not claimable at any epoch — and an earlier version of this file did exactly
// that and called it applying afresh.
func TestAnApplyingRecordThisApplierOwnsIsDrivenWithoutReclaiming(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testEpoch, expired
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	operations := f.store.operations()
	for _, forbidden := range []string{"ClaimCommand", "BeginApplying"} {
		if indexOf(operations, forbidden) >= 0 {
			t.Errorf("the operations ran as %v; %s against an applying record is refused at every epoch", operations, forbidden)
		}
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Errorf("the runtime was driven %d times, want once: no effect had committed", len(driven))
	}
	if completions := f.store.completions(); len(completions) != 1 || completions[0].Revision != testRevision {
		t.Errorf("the completion is %+v, want one naming the revision the record was read at %d", completions, testRevision)
	}
}

// TestAnApplyingRecordThisApplierIsFencedOutOfIsRefused is the check the drive
// arm was missing, and the case that made it reachable.
//
// An ABANDONED outcome means a correlated prefix exists and a fence above its
// epoch was observed — that is what abandonment IS — so an abandoned application
// under this applier's OWN claim epoch is the journal saying this applier has
// been fenced out of the stream. The drive arm ran before that was read: it
// appended a SECOND prefix and drove the runtime under an epoch already proven
// superseded. The real journal writer refuses the fenced append, so the damage
// surfaced as a store failure rather than as a double application — but the
// posture of this package is to be stricter where the store cannot help, and
// this was looser than evidence it had already read.
func TestAnApplyingRecordThisApplierIsFencedOutOfIsRefused(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testEpoch, expired
		f.stored().application = abandonedApplication(testEpoch, testEpoch+5)
	})
	outcome, err := f.process()
	if refusalOf(err) != RefusalStaleEpoch {
		t.Fatalf("got %v, want a %q refusal", err, RefusalStaleEpoch)
	}
	if prefixes := f.store.prefixes; len(prefixes) != 0 {
		t.Errorf("a second application prefix %+v was appended under an epoch the journal has fenced out", prefixes)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v under an epoch the journal has fenced out", driven)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d durable writes were attempted", writes)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though a correlated prefix exists")
	}
}

// TestAResumedApplicationRevalidatesItsPayload covers the payload check on the
// path that reaches it WITHOUT a claim.
//
// The drive arm reloads the private body and checks it exactly as the claim arm
// does, and until this existed only the claim arm was tested: deleting the check
// from the resumed path survived the package. The two are separate calls because
// the two paths reach the runtime by different routes, and a guard on one of them
// is not a guard on the other.
func TestAResumedApplicationRevalidatesItsPayload(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		stored := f.put(KindInput, StatePending)
		stored.payload = Payload{}
		stored.record.State, stored.record.ClaimEpoch, stored.record.ClaimExpiresAt = StateApplying, testEpoch, expired
	})
	_, err := f.process()
	if refusalOf(err) != RefusalMissingPayload {
		t.Fatalf("got %v, want a %q refusal", err, RefusalMissingPayload)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v for a command whose body is gone", driven)
	}
	if prefixes := f.store.prefixes; len(prefixes) != 0 {
		t.Errorf("an application prefix %+v was appended for an application that cannot begin", prefixes)
	}
}

// TestACorrelationAboutAnotherApplicationIsRefused asserts a prefix that names
// something else is not evidence about this command.
//
// Acting on one would record a DIFFERENT application's effect as this command's,
// which is the one way this design can report an effect that never happened. The
// conflicted row is the store's own finding for a broken durable mapping, and the
// non-applying row is the store's precondition: an application is completed only
// from applying, so a committed effect beside a claimed record is a contradiction
// rather than a shortcut.
func TestACorrelationAboutAnotherApplicationIsRefused(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		state       State
		claimEpoch  uint64
		application Application
		refusal     ApplyRefusal
	}{
		{
			name: "a prefix naming another runtime allocation", state: StateApplying, claimEpoch: testEpoch,
			application: Application{
				CommandID: commandID(1), RuntimeCommandID: testOtherRuntimeCommandID,
				Outcome: ApplicationCommitted, EffectEventID: testEffectEventID, EffectSeq: testEffectSeq,
			},
			refusal: RefusalCorrelation,
		},
		{
			name: "a correlation about another command", state: StateApplying, claimEpoch: testEpoch,
			application: Application{
				CommandID: commandID(9), RuntimeCommandID: testRuntimeCommandID,
				Outcome: ApplicationCommitted, EffectEventID: testEffectEventID, EffectSeq: testEffectSeq,
			},
			refusal: RefusalCorrelation,
		},
		{
			name: "a broken durable mapping", state: StateApplying, claimEpoch: testEpoch,
			application: Application{
				CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, Outcome: ApplicationConflicted,
			},
			refusal: RefusalCorrelation,
		},
		{
			name: "a prefix from a later epoch", state: StateApplying, claimEpoch: testEpoch,
			application: Application{
				CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID,
				Outcome: ApplicationCommitted, PrefixEpoch: testLaterEpoch,
				EffectEventID: testEffectEventID, EffectSeq: testEffectSeq,
			},
			refusal: RefusalStaleEpoch,
		},
		{
			name: "a committed effect beside a record that never entered applying", state: StateClaimed, claimEpoch: testEpoch,
			application: committedApplication(testEpoch),
			refusal:     RefusalCorrelation,
		},
		{
			name: "an outcome this package cannot act on", state: StateApplying, claimEpoch: testEpoch,
			application: Application{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, Outcome: ApplicationOutcome("partly")},
			refusal:     RefusalUnresolvedApplication,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				record := &f.stored().record
				record.State, record.ClaimEpoch, record.ClaimExpiresAt = testCase.state, testCase.claimEpoch, expired
				f.stored().application = testCase.application
			})
			_, err := f.process()
			if got := refusalOf(err); got != testCase.refusal {
				t.Fatalf("got %v (refusal %q), want refusal %q", err, got, testCase.refusal)
			}
			if completions := f.store.completions(); len(completions) != 0 {
				t.Errorf("the command was completed as %+v on a correlation about something else", completions)
			}
			if writes := f.fence.fencedWrites(); writes != 0 {
				t.Errorf("%d durable writes were made", writes)
			}
		})
	}
}

// TestACommittedCorrelationNamingNoEventIsRefused covers the store contradicting
// itself: an outcome of committed carries an event and a sequence by
// construction, so one with neither is a defect rather than a settlement.
//
// It exists because deleting the validation survived the whole package: both
// call sites reach complete only for a committed outcome, so nothing else could
// produce an effect that names nothing, and the guard had no reader. Writing it
// would spend a fenced round-trip to be told the result is malformed, and a
// store that accepted it would leave an applied command pointing at nothing.
func TestACommittedCorrelationNamingNoEventIsRefused(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testEpoch, expired
		f.stored().application = Application{
			CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID,
			Outcome: ApplicationCommitted, PrefixEpoch: testEpoch,
		}
	})
	outcome, err := f.process()
	if refusalOf(err) != RefusalInvalidEffect {
		t.Fatalf("got %v, want a %q refusal", err, RefusalInvalidEffect)
	}
	if completions := f.store.completions(); len(completions) != 0 {
		t.Errorf("the store was asked to record %+v, which names no durable event", completions)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though a correlated prefix exists")
	}
}

// TestACompletionRefusedWhileTheGrantIsHeldStopsTheCursor is the correction to
// a test that proved the wrong thing.
//
// Its predecessor called Process twice ON THE BARE APPLIER, where there is no
// cursor, and concluded from the second pass that a refused completion was
// recoverable. AT THE COMPOSITION THERE IS A CURSOR: Reconcile treats
// PrefixOwned as consumable, advances, and commits — and every later page is
// listed strictly after that cursor. Nothing in this lane re-reads a command
// below it; there is no due sweep. So the old behaviour left a record durably
// applying over a committed effect, unsettleable by the reconciler (§10.4
// forbids it touching applying) and by rejection (provesNoEffect is false), with
// a pass that reported no Blocked at all.
//
// THE TEST THEREFORE RUNS THROUGH THE REAL CONSUMER, because that is the level
// the defect existed at. The exactly-once half is kept: the retry completes the
// command WITHOUT driving the runtime again.
func TestACompletionRefusedWhileTheGrantIsHeldStopsTheCursor(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.store.errs["CompleteCommand"] = errTestStore
	cursors := &fakeCursors{}
	inbox := &fakeInbox{pages: [][]Command{{command(1, StatePending)}, {command(1, StateApplying)}}}
	consumer, err := NewConsumer(Options{
		Host: f.host, Key: registry.Key{TenantID: testTenant, SessionID: testSession},
		LeaseEpoch: testEpoch, Inbox: inbox, Cursors: cursors, Processor: f.applier, Fence: f.fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// A PROCESSOR ERROR IS REPORTED THROUGH Blocked, NOT THROUGH Reconcile's
	// return: O4.1 made a failed command stop the pass and NAME itself, which is
	// the whole of "not silently". So the assertion is the pass result, and the
	// thing that changed is that there is one at all — before the fix this pass
	// reported Blocked nil, consumed one, and a committed cursor.
	result, err := consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile reported %v", err)
	}
	if result.Blocked == nil || result.Blocked.CommandID != commandID(1) {
		t.Fatalf("the pass reports Blocked %+v, want the command it could not settle; LastPass is the only place an operator sees this", result.Blocked)
	}
	if !errors.Is(result.Blocked.Cause, errTestStore) {
		t.Errorf("the blocked command carries %v, want the store failure that stopped it", result.Blocked.Cause)
	}
	if result.Consumed != 0 {
		t.Errorf("consumed = %d, want 0; a command that was not settled was counted as consumed", result.Consumed)
	}
	if writes := cursors.written(); len(writes) != 0 {
		t.Errorf("the cursor was written %+v past a command no later reader would revisit", writes)
	}
	if state, due := f.store.stateOf(commandID(1)); state != StateApplying || !due {
		t.Errorf("the record is %q (due %v), want an applying record still due", state, due)
	}

	// The retry settles it from the same evidence, and drives nothing.
	f.store.mu.Lock()
	delete(f.store.errs, "CompleteCommand")
	f.store.mu.Unlock()
	if _, err := consumer.Reconcile(context.Background()); err != nil {
		t.Fatalf("the recovery pass reported %v", err)
	}
	if state, _ := f.store.stateOf(commandID(1)); state != StateApplied {
		t.Errorf("the record is %q, want %q", state, StateApplied)
	}
	if driven := f.runtime.commands(); len(driven) != 1 {
		t.Errorf("the runtime was driven %d times in total, want once; the recovery pass re-applied a committed effect", len(driven))
	}
	if writes := cursors.written(); len(writes) != 1 || writes[0].Order != 1 {
		t.Errorf("the cursor was written %+v, want one write past the settled command", writes)
	}
}

// TestAnAmbiguousRejectionIsAFailure is the other half of the rule above, and
// the two are not the same case.
//
// A rejection rests on the journal proving that NO effect committed, so an
// ambiguous failure leaves nothing correlating anything: reporting it as an owned
// prefix would advance the consumer's cursor past a command no evidence can
// settle. The pass stops instead, and the next one re-derives the same conclusion
// from the same durable state.
func TestAnAmbiguousRejectionIsAFailure(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		f.put(KindGateResponse, StatePending)
		delete(f.store.gates, testGate)
	})
	f.store.errs["RejectCommand"] = errTestStore
	outcome, err := f.process()
	if !errors.Is(err, errTestStore) {
		t.Fatalf("got %v, want the ambiguous store failure", err)
	}
	if outcome.PrefixOwned {
		t.Error("PrefixOwned = true for a command that committed no application prefix, so the cursor would pass a command nothing can recover")
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v", driven)
	}
}

// TestARuntimeFailureAfterThePrefixIsReportedAndRecovered asserts both halves of
// the one case where a failure and an owned prefix are true at once.
//
// Past the prefix a failed call is INDISTINGUISHABLE from a crash — that is what
// the prefix exists to say — so what happened is a question for the journal
// rather than for the call's return value, and the next pass asks it. What the
// failure adds is a CAUSE, which the consumer carries into PassResult.Blocked
// where an operator reads it.
func TestARuntimeFailureAfterThePrefixIsReportedAndRecovered(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) { f.runtime.err = errTestRuntime })
	outcome, err := f.process()
	if !errors.Is(err, errTestRuntime) {
		t.Fatalf("got %v, want the runtime failure to be carried", err)
	}
	if refusalOf(err) != RefusalRuntime {
		t.Errorf("the refusal is %q, want %q", refusalOf(err), RefusalRuntime)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, though the correlation was appended before the call")
	}
	if completions := f.store.completions(); len(completions) != 0 {
		t.Errorf("the command was completed as %+v by the pass whose runtime call failed", completions)
	}

	// The next pass finds the record applying with the journal still unresolved
	// under this applier's own epoch, and drives it again — which is correct
	// precisely because no effect committed.
	f.runtime.mu.Lock()
	f.runtime.err = nil
	f.runtime.mu.Unlock()
	f.stored().application = Application{Outcome: ApplicationAbsent}
	second, err := f.process()
	if err != nil {
		t.Fatalf("the recovery pass reported %v", err)
	}
	if second.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", second.State, StateApplied)
	}
}

// TestPlanIsAPureDecisionOverTheDurableFacts covers the decision table directly,
// at the boundaries a store fixture cannot reach as precisely.
//
// It is not a duplicate of the tests above: those establish what the applier
// DOES for a case, this establishes that the decision is a function of the
// record, the journal correlation and one instant — nothing read from a clock
// inside, and nothing carried from a previous call.
func TestPlanIsAPureDecisionOverTheDurableFacts(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	base := Record{
		TenantID: testTenant, SessionID: testSession, CommandID: commandID(1),
		RuntimeCommandID: testRuntimeCommandID, Kind: KindInput, State: StatePending,
		AcceptedOrder: 1, Revision: testRevision, ApplyDeadline: testClockAt.Add(testApplyDeadline),
	}
	absent := Application{Outcome: ApplicationAbsent}

	for _, testCase := range []struct {
		name        string
		record      func(Record) Record
		application Application
		now         time.Time
		want        step
		refusal     ApplyRefusal
	}{
		{
			name:        "a pending record inside its deadline",
			record:      func(r Record) Record { return r },
			application: absent,
			now:         testClockAt,
			want:        stepClaim,
		},
		{
			name: "a live claim this applier holds",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateClaimed, testEpoch, unexpired
				return r
			},
			application: absent,
			now:         testClockAt,
			want:        stepApplyUnderHeldClaim,
		},
		{
			name: "an applying record this applier owns with nothing in the journal",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testEpoch, expired
				return r
			},
			application: absent,
			now:         testClockAt,
			want:        stepDriveApplyingRecord,
		},
		{
			name: "a committed effect under a predecessor",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
				return r
			},
			application: committedApplication(testOtherEpoch),
			now:         testClockAt,
			want:        stepComplete,
		},
		{
			name: "an abandoned application whose writer is fenced",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
				return r
			},
			application: abandonedApplication(testOtherEpoch, testEpoch),
			now:         testClockAt,
			want:        stepReject,
		},
		{
			name: "a committed effect under another epoch's LIVE claim",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testOtherEpoch, unexpired
				return r
			},
			// A committed effect is a fact and outranks the claim: the command
			// HAS been applied, and recording that is correct however many
			// writers are looking at it.
			application: committedApplication(testOtherEpoch),
			now:         testClockAt,
			want:        stepComplete,
		},
		{
			name: "an unfinished application under another epoch's LIVE claim",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testOtherEpoch, unexpired
				return r
			},
			application: absent,
			now:         testClockAt,
			refusal:     RefusalClaimHeld,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got, err := f.applier.plan(testCase.record(base), testCase.application, testCase.now)
			if refusal := refusalOf(err); refusal != testCase.refusal {
				t.Fatalf("plan reported %v (refusal %q), want refusal %q", err, refusal, testCase.refusal)
			}
			if testCase.refusal != "" {
				return
			}
			if got != testCase.want {
				t.Errorf("plan chose step %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestAnApplierAndAConsumerShareOneFence records the answer to O4.1's first
// hand-off as a composition rather than as a sentence: the applier's durable
// writes are fenced by the SAME mechanism the cursor write is, so a grant that
// ends stops both at once rather than stopping one and leaving the other to find
// out from the store.
func TestAnApplierAndAConsumerShareOneFence(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	key := registry.Key{TenantID: testTenant, SessionID: testSession}
	consumer, err := NewConsumer(Options{
		Host: f.host, Key: key, LeaseEpoch: testEpoch,
		Inbox: &fakeInbox{pages: [][]Command{{command(1, StatePending)}}}, Cursors: &fakeCursors{},
		Processor: f.applier, Fence: f.fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// THE OTHER WAY OWNERSHIP ENDS: a store refused a fenced write under a
	// superseded epoch, which closes no channel.
	f.fence.end()
	result, err := consumer.Reconcile(context.Background())
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("Reconcile reported %v, want the grant-gone cause", err)
	}
	if result.Consumed != 0 {
		t.Errorf("consumed = %d, want 0", result.Consumed)
	}
	if claims := f.store.claims; len(claims) != 0 {
		t.Errorf("the applier claimed %+v under a grant that had ended", claims)
	}
	if driven := f.runtime.commands(); len(driven) != 0 {
		t.Errorf("the runtime was driven with %+v", driven)
	}
}

// TestTheGateRecheckNamesThisHostAndThisResidency pins what "the durable
// gate/owner" means, because the two halves fail differently and a check that
// compared only one would pass every test that varied the other.
func TestTheGateRecheckNamesThisHostAndThisResidency(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	for _, testCase := range []struct {
		name      string
		gate      Gate
		found     bool
		resumable bool
	}{
		{"open here, this residency", Gate{Open: true, OwnerHostID: testHostID, OwnerEpoch: testEpoch}, true, true},
		{"not found", Gate{}, false, false},
		{"resolved", Gate{Open: false, OwnerHostID: testHostID, OwnerEpoch: testEpoch}, true, false},
		{"another Host", Gate{Open: true, OwnerHostID: sessionwire.HostID("host-elsewhere"), OwnerEpoch: testEpoch}, true, false},
		{"an earlier residency", Gate{Open: true, OwnerHostID: testHostID, OwnerEpoch: testOtherEpoch}, true, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			reason, resumable := f.applier.gateResumable(testCase.gate, testCase.found)
			if resumable != testCase.resumable {
				t.Fatalf("resumable = %v, want %v", resumable, testCase.resumable)
			}
			if resumable == (reason != "") {
				t.Errorf("the reason %q does not match the verdict %v; a refusal with no reason tells an operator nothing and a resumable gate with one reads as a refusal", reason, resumable)
			}
		})
	}
}
