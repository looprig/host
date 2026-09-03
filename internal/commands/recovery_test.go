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

// ---------------------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------------------

// TestAStaleEpochRefusesBeforeAnythingIsWritten asserts a record claimed under
// an epoch LATER than this applier's stops the work rather than racing it.
//
// The fence would refuse the write anyway once the store said so, and that is
// not the same thing: the first write this applier makes is a CLAIM, and a
// claim that succeeded would take a command away from the Host that
// legitimately owns the session. The check is the one that runs first.
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
	if applied := f.runtime.applied(); len(applied) != 0 {
		t.Errorf("the runtime was driven with %v", applied)
	}
}

// TestAnUnexpiredClaimByAnotherEpochIsRespected holds §10.4's "an unexpired
// claim or applying wins", and its other half: once the claim has expired, the
// current lease holder takes the work over rather than leaving it forever.
//
// The two rows differ ONLY in the claim's expiry, which is what makes the
// expiry the thing under test rather than the state or the epoch.
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

// TestThisAppliersOwnClaimIsResumedRatherThanWaitedOut asserts a claim under
// THIS applier's epoch is not somebody else's: a pass that failed after
// claiming and runs again must not sit out its own claim's TTL, which would
// stall the session for a claim nobody else can take.
func TestThisAppliersOwnClaimIsResumedRatherThanWaitedOut(t *testing.T) {
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
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Errorf("the runtime was driven %d times, want once", len(applied))
	}
}

// ---------------------------------------------------------------------------
// The apply deadline
// ---------------------------------------------------------------------------

// TestANewClaimCannotStartAtOrAfterTheApplyDeadline holds §10.4's sentence
// exactly, including the boundary: AT the deadline is already too late.
//
// A HOST NEVER REJECTS FOR THE DEADLINE. The refusal leaves the record where it
// is and blocks the pass, which the consumer names; §10.4 gives the terminal
// CAS to the deadline reconciler, after it has checked that no correlated
// application evidence exists — a check a Host that has already decided to
// apply is the wrong party to make.
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
				t.Errorf("the command was claimed %v at or after its apply deadline", f.store.claims)
			}
			if results := f.store.terminalResults(); len(results) != 0 {
				t.Errorf("the Host made the command terminal as %v; the deadline reconciler is what rejects a command whose deadline passed, after checking for application evidence this Host did not look for", results)
			}
		})
	}
}

// TestAnOwnedPrefixIsFinishedAfterTheDeadline is the asymmetry the deadline
// rule is written with: recovering an expired `applying` prefix is CONTINUATION
// of an existing application, not a new claim, so it happens even after the
// deadline. A rule that refused both would strand exactly the commands whose
// effects already reached a runtime.
func TestAnOwnedPrefixIsFinishedAfterTheDeadline(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		f.store.prefixes[commandID(1)] = Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testOtherEpoch}
	})
	f.clock.advance(testApplyDeadline + time.Hour)
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 0 {
		t.Errorf("the runtime was driven with %v for an application that had already begun", applied)
	}
}

// TestAnUnsupportedKindIsRefusedRatherThanRejected asserts an older Host does
// not durably destroy work a newer one would have done. The record is left for
// a Host that understands the kind, and the apply deadline is what bounds how
// long that can last.
func TestAnUnsupportedKindIsRefusedRatherThanRejected(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) { f.put(Kind("checkpoint"), StatePending) })
	_, err := f.process()
	if refusalOf(err) != RefusalUnsupportedKind {
		t.Fatalf("got %v, want a %q refusal", err, RefusalUnsupportedKind)
	}
	if results := f.store.terminalResults(); len(results) != 0 {
		t.Errorf("the command was made terminal as %v by a Host that does not know what it asks for", results)
	}
	if writes := f.fence.fencedWrites(); writes != 0 {
		t.Errorf("%d durable writes were made for a kind this Host cannot apply", writes)
	}
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

// TestACrashAfterTheApplyingPrefixIsFinishedWithoutReapplying is the case the
// prefix exists for.
//
// The durable record says `applying` and a correlated prefix is committed, so
// an effect may already have reached the runtime and nothing durable can say
// whether it completed. §10.4's answer is to finish the prefix, and the
// assertion that matters is the negative one: the runtime is not driven a
// second time.
func TestACrashAfterTheApplyingPrefixIsFinishedWithoutReapplying(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testEpoch, expired
		f.store.prefixes[commandID(1)] = Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testEpoch}
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 0 {
		t.Fatalf("the runtime was driven with %v for an application that had already begun; the effect would happen twice", applied)
	}
	for _, operation := range f.store.operations() {
		if operation == "ClaimCommand" {
			t.Error("the record was claimed again; recovering an owned prefix is continuation of an existing application, not a new claim")
		}
	}
	if results := f.store.terminalResults(); len(results) != 1 || results[0].Result.State() != StateApplied {
		t.Errorf("the terminal results are %v, want one applied", results)
	}
}

// TestASuccessorFinishesAPredecessorsPrefixUnderItsOwnEpoch asserts the
// successor case §10.4 names: the next lease holder finishes the durable
// prefix. The terminal CAS carries the epoch of the applier that WRITES it, not
// the prefix's, because the write is fenced by the grant its writer holds.
func TestASuccessorFinishesAPredecessorsPrefixUnderItsOwnEpoch(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
		f.store.prefixes[commandID(1)] = Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testOtherEpoch}
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 0 {
		t.Errorf("the runtime was driven with %v for a predecessor's application", applied)
	}
	results := f.store.terminalResults()
	if len(results) != 1 {
		t.Fatalf("the terminal CAS ran %d times, want once", len(results))
	}
	if results[0].Epoch != testEpoch {
		t.Errorf("the terminal CAS was stamped with epoch %d, want this applier's %d", results[0].Epoch, testEpoch)
	}
}

// TestAnExpiredApplyingClaimWithNoPrefixIsAppliedAfresh is the other half of
// the same read. `applying` alone means nothing: the prefix is written BEFORE
// the runtime-visible effect begins, so its absence establishes that no effect
// did, and the command is claimed and applied from the start rather than
// finished on the strength of a state nobody acted on.
func TestAnExpiredApplyingClaimWithNoPrefixIsAppliedAfresh(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		record := &f.stored().record
		record.State, record.ClaimEpoch, record.ClaimExpiresAt = StateApplying, testOtherEpoch, expired
	})
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if outcome.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", outcome.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Fatalf("the runtime was driven %d times, want once: no prefix was committed, so no effect had begun", len(applied))
	}
	if claims := f.store.claims; len(claims) != 1 || claims[0].ExpectedState != StateApplying || claims[0].ExpectedClaimEpoch != testOtherEpoch {
		t.Errorf("the claim was %v, want one expecting the record as it was read", claims)
	}
}

// TestAPrefixMustCorrelateThisCommandAndThisAllocation asserts a prefix that
// names something else is not evidence about this command.
//
// Acting on one would finalize a command as applied on the strength of a
// DIFFERENT application, which is the one way this design can report an effect
// that never happened. The `applying`-record row is the same defect from the
// other side: a prefix beside a record nobody ever claimed is an ordering
// §10.4's writes cannot produce.
func TestAPrefixMustCorrelateThisCommandAndThisAllocation(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		prefix  Prefix
		state   State
		refusal ApplyRefusal
	}{
		{
			name:    "another command",
			prefix:  Prefix{CommandID: commandID(9), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testEpoch},
			state:   StateApplying,
			refusal: RefusalCorrelation,
		},
		{
			name:    "another runtime allocation",
			prefix:  Prefix{CommandID: commandID(1), RuntimeCommandID: testOtherRuntimeCommandID, LeaseEpoch: testEpoch},
			state:   StateApplying,
			refusal: RefusalCorrelation,
		},
		{
			name:    "a later epoch",
			prefix:  Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testLaterEpoch},
			state:   StateApplying,
			refusal: RefusalStaleEpoch,
		},
		{
			name:    "a record that was never claimed",
			prefix:  Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testEpoch},
			state:   StatePending,
			refusal: RefusalCorrelation,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := newApplierFixture(t, func(f *applierFixture) {
				record := &f.stored().record
				record.State = testCase.state
				if testCase.state != StatePending {
					record.ClaimEpoch, record.ClaimExpiresAt = testEpoch, expired
				}
				f.store.prefixes[commandID(1)] = testCase.prefix
			})
			_, err := f.process()
			if got := refusalOf(err); got != testCase.refusal {
				t.Fatalf("got %v (refusal %q), want refusal %q", err, got, testCase.refusal)
			}
			if results := f.store.terminalResults(); len(results) != 0 {
				t.Errorf("the command was finalized as %v on the strength of a prefix that correlates something else", results)
			}
			if writes := f.fence.fencedWrites(); writes != 0 {
				t.Errorf("%d durable writes were made", writes)
			}
		})
	}
}

// TestATerminalCASAmbiguityLeavesTheCommandRecoverable is the case
// Outcome.PrefixOwned exists for.
//
// The prefix is committed and the terminal CAS came back with an ambiguous
// store failure, so nothing durable says whether the command is terminal. That
// is EXACTLY §10.4's recoverable condition, so the pass reports it as owned
// rather than as a failure: the record stays due, and a later pass — this
// Host's or a successor's — finishes it. The half that makes it exactly-once is
// the second pass, which finalizes WITHOUT driving the runtime again.
func TestATerminalCASAmbiguityLeavesTheCommandRecoverable(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	f.store.errs["FinalizeCommand"] = errTestStore
	outcome, err := f.process()
	if err != nil {
		t.Fatalf("the pass reported %v; an owned prefix is recoverable and is not a failure", err)
	}
	if !outcome.PrefixOwned {
		t.Error("PrefixOwned = false, so the consumer's cursor stops on a command nothing can re-drive from the inbox")
	}
	if outcome.State.Terminal() {
		t.Errorf("outcome state = %q, which reports a terminal state no store acknowledged", outcome.State)
	}
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Fatalf("the runtime was driven %d times, want once", len(applied))
	}

	f.store.mu.Lock()
	delete(f.store.errs, "FinalizeCommand")
	f.store.mu.Unlock()
	second, err := f.process()
	if err != nil {
		t.Fatalf("the recovery pass reported %v", err)
	}
	if second.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", second.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Errorf("the runtime was driven %d times in total, want once; the recovery pass re-applied a committed effect", len(applied))
	}
}

// TestAnAmbiguousTerminalCASWithNoPrefixIsAFailure is the other half of the
// rule above, and the two are not the same case.
//
// A gate rejection commits no application prefix, so an ambiguous CAS leaves
// NOTHING correlating anything: reporting it as an owned prefix would advance
// the consumer's cursor past a command no durable fact makes recoverable. The
// pass stops instead, and the next one re-derives the same rejection from the
// same durable state.
func TestAnAmbiguousTerminalCASWithNoPrefixIsAFailure(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t, func(f *applierFixture) {
		f.put(KindGateResponse, StatePending)
		delete(f.store.gates, testGate)
	})
	f.store.errs["FinalizeCommand"] = errTestStore
	outcome, err := f.process()
	if !errors.Is(err, errTestStore) {
		t.Fatalf("got %v, want the ambiguous store failure", err)
	}
	if outcome.PrefixOwned {
		t.Error("PrefixOwned = true for a command that committed no application prefix, so the cursor would pass a command nothing can recover")
	}
	if applied := f.runtime.applied(); len(applied) != 0 {
		t.Errorf("the runtime was driven with %v", applied)
	}
}

// TestARuntimeFailureAfterThePrefixIsReportedAndRecovered asserts both halves
// of the one case where a failure and an owned prefix are true at once.
//
// Past the prefix a failed call is INDISTINGUISHABLE from a crash — that is
// what the prefix exists to say — so the durable answer is §10.4's for both:
// the next pass finishes the prefix rather than driving the runtime again. What
// the failure adds is a CAUSE, which the consumer carries into
// PassResult.Blocked where an operator reads it.
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
		t.Error("PrefixOwned = false, though the correlation was committed before the call")
	}
	if results := f.store.terminalResults(); len(results) != 0 {
		t.Errorf("the command was finalized as %v by the pass whose runtime call failed", results)
	}

	f.runtime.mu.Lock()
	f.runtime.err = nil
	f.runtime.mu.Unlock()
	second, err := f.process()
	if err != nil {
		t.Fatalf("the recovery pass reported %v", err)
	}
	if second.State != StateApplied {
		t.Errorf("outcome state = %q, want %q", second.State, StateApplied)
	}
	if applied := f.runtime.applied(); len(applied) != 1 {
		t.Errorf("the runtime was driven %d times in total, want once", len(applied))
	}
}

// TestPlanIsAPureDecisionOverTheDurableFacts covers the decision table
// directly, at the boundaries a store fixture cannot reach as precisely.
//
// It is not a duplicate of the tests above: those establish what the applier
// DOES for a case, this establishes that the decision is a function of the
// record, the prefix and one instant — nothing read from a clock inside, and
// nothing carried from a previous call.
func TestPlanIsAPureDecisionOverTheDurableFacts(t *testing.T) {
	t.Parallel()

	f := newApplierFixture(t)
	base := Record{
		TenantID: testTenant, SessionID: testSession, CommandID: commandID(1),
		RuntimeCommandID: testRuntimeCommandID, Kind: KindInput, State: StatePending,
		AcceptedOrder: 1, ApplyDeadline: testClockAt.Add(testApplyDeadline),
	}
	prefix := Prefix{CommandID: commandID(1), RuntimeCommandID: testRuntimeCommandID, LeaseEpoch: testEpoch}

	for _, testCase := range []struct {
		name    string
		record  func(Record) Record
		owned   bool
		now     time.Time
		want    step
		refusal ApplyRefusal
	}{
		{
			name:   "a pending record inside its deadline",
			record: func(r Record) Record { return r },
			now:    testClockAt,
			want:   stepApplyFresh,
		},
		{
			name: "an applying record with an owned prefix",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testEpoch, expired
				return r
			},
			owned: true,
			now:   testClockAt,
			want:  stepFinishPrefix,
		},
		{
			name: "an owned prefix under another epoch's LIVE claim",
			record: func(r Record) Record {
				r.State, r.ClaimEpoch, r.ClaimExpiresAt = StateApplying, testOtherEpoch, unexpired
				return r
			},
			owned:   true,
			now:     testClockAt,
			refusal: RefusalClaimHeld,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got, err := f.applier.plan(testCase.record(base), prefix, testCase.owned, testCase.now)
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
// ends stops both at once rather than stopping one and leaving the other to
// find out from the store.
func TestAnApplierAndAConsumerShareOneFence(t *testing.T) {
	t.Parallel()

	clock := newManualClock(testClockAt)
	built := newTestHost(t, clock)
	store := newFakeStore()
	store.commands[commandID(1)] = &storedCommand{
		record: Record{
			TenantID: testTenant, SessionID: testSession, CommandID: commandID(1),
			RuntimeCommandID: testRuntimeCommandID, Kind: KindInput, State: StatePending,
			AcceptedOrder: 1, ApplyDeadline: testClockAt.Add(testApplyDeadline),
		},
		payload: Payload{Body: []byte(`{"blocks":[]}`)},
		due:     true,
	}
	fence := newFakeFence()
	runtime := &fakeRuntime{}
	key := registry.Key{TenantID: testTenant, SessionID: testSession}
	applier, err := NewApplier(ApplierOptions{
		Host: built, Key: key, LeaseEpoch: testEpoch,
		Records: store, Writes: store, Gates: store, Runtime: runtime, Fence: fence,
	})
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	consumer, err := NewConsumer(Options{
		Host: built, Key: key, LeaseEpoch: testEpoch,
		Inbox: &fakeInbox{pages: [][]Command{{command(1, StatePending)}}}, Cursors: &fakeCursors{},
		Processor: applier, Fence: fence,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// THE OTHER WAY OWNERSHIP ENDS: a store refused a fenced write under a
	// superseded epoch, which closes no channel.
	fence.end()
	result, err := consumer.Reconcile(context.Background())
	if !errors.Is(err, errTestGrantGone) {
		t.Fatalf("Reconcile reported %v, want the grant-gone cause", err)
	}
	if result.Consumed != 0 {
		t.Errorf("consumed = %d, want 0", result.Consumed)
	}
	if claims := store.claims; len(claims) != 0 {
		t.Errorf("the applier claimed %v under a grant that had ended", claims)
	}
	if applied := runtime.applied(); len(applied) != 0 {
		t.Errorf("the runtime was driven with %v", applied)
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
