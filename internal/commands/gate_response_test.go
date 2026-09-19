package commands

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// ---------------------------------------------------------------------------
// The gate response's pre-attempt checks
// ---------------------------------------------------------------------------
//
// ONCE AN ATTEMPT EXISTS ONLY THE RUNTIME'S EVIDENCE SETTLES A COMMAND, so every
// question Host can answer about a gate response is answered BEFORE the attempt,
// while the command is still claimed and a rejection is still possible. A body
// no Host could ever apply is rejected; a gate this Host does not yet own blocks
// without writing; everything else is the runtime's to decide.

// testGateUUID is the harness gate identity the fixture's answers name.
var testGateUUID = uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f")

const testGateSeq uint64 = 7

// fakeGates is the durable gate projection as LoadGate answers it.
type fakeGates struct {
	mu    sync.Mutex
	gates map[sessionwire.GateID]Gate
	err   error
	loads int
}

func (g *fakeGates) LoadGate(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, gate sessionwire.GateID) (Gate, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.loads++
	if g.err != nil {
		return Gate{}, false, g.err
	}
	held, ok := g.gates[gate]
	return held, ok, nil
}

// ownedGate is the fixture's gate, projected and owned at residency owner.
func ownedGate(owner uint64) *fakeGates {
	id := sessionwire.GateID(testGateUUID.String())
	return &fakeGates{gates: map[sessionwire.GateID]Gate{id: {
		GateID: id, Open: true, OwnerEpoch: owner,
		OpenedEventID: "event-opened", OpenedJournalSeq: testGateSeq,
	}}}
}

// gateAnswer is the body Factory stores for the fixture's command.
func gateAnswer() sessionwire.GateResponseRequest {
	return sessionwire.GateResponseRequest{
		CommandEnvelope:        sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(1)},
		SessionID:              testSession,
		GateID:                 sessionwire.GateID(testGateUUID.String()),
		Action:                 "answer",
		Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"blue"`)},
		ExpectedOpenJournalSeq: testGateSeq,
	}
}

// gateFixture is a disposition fixture holding one pending gate_response.
func gateFixture(t *testing.T, gates *fakeGates, body func(*sessionwire.GateResponseRequest)) *dispositionFixture {
	t.Helper()
	return newDispositionFixture(t, func(f *dispositionFixture) {
		f.gates = gates
		f.put(KindGateResponse, StatePending)
		answer := gateAnswer()
		if body != nil {
			body(&answer)
		}
		encoded, err := json.Marshal(answer)
		if err != nil {
			t.Fatal(err)
		}
		f.store.payloads[commandID(1)] = Payload{Body: encoded}
	})
}

// TestAGateResponseToAGateThisHostOwnsIsDispatchedUnderItsAttempt is the
// acceptance path: the gate is projected, its owner is this Host's residency,
// the answer names the version the projection holds, and the command is claimed,
// checked, authorized, dispatched and settled — in that order, the check before
// the attempt.
func TestAGateResponseToAGateThisHostOwnsIsDispatchedUnderItsAttempt(t *testing.T) {
	f := gateFixture(t, ownedGate(testEpoch), nil)
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v, %v), want applied", outcome, err)
	}
	want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "BeginAttempt", "SettleDisposition"}
	if got := f.store.operations(); !equalStrings(got, want) {
		t.Fatalf("the store saw %v, want %v", got, want)
	}
	if f.gates.loads != 1 {
		t.Fatalf("the projection was read %d times, want once, before the attempt", f.gates.loads)
	}
	driven := f.runtime.commands()
	if len(driven) != 1 || driven[0].Kind != string(KindGateResponse) || driven[0].AttemptID == "" {
		t.Fatalf("the runtime was driven with %+v, want one gate_response under an attempt", driven)
	}
}

// TestAGateResponseNoHostCouldApplyIsRejectedBeforeAnyAttempt: each row is
// permanent — the body is immutable and every Host reads the same bytes — so the
// claimed command is rejected, and never reaches an attempt no evidence would
// settle.
func TestAGateResponseNoHostCouldApplyIsRejectedBeforeAnyAttempt(t *testing.T) {
	for _, row := range []struct {
		name string
		body func(*sessionwire.GateResponseRequest)
	}{
		{"it names another session", func(r *sessionwire.GateResponseRequest) { r.SessionID = "session-other" }},
		{"it names another command", func(r *sessionwire.GateResponseRequest) { r.CommandID = commandID(2) }},
		{"its gate id is not a harness gate id", func(r *sessionwire.GateResponseRequest) { r.GateID = "gate-a" }},
		{"it names no expected-open version", func(r *sessionwire.GateResponseRequest) { r.ExpectedOpenJournalSeq = 0 }},
		{"it answers a stale open sequence", func(r *sessionwire.GateResponseRequest) { r.ExpectedOpenJournalSeq = testGateSeq + 1 }},
		{"it answers a stale open event", func(r *sessionwire.GateResponseRequest) {
			r.ExpectedOpenJournalSeq = 0
			r.ExpectedOpenEventID = "event-other"
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := gateFixture(t, ownedGate(testEpoch), row.body)
			outcome, err := f.process()
			if err != nil || outcome.State != StateRejected || outcome.PrefixOwned {
				t.Fatalf("Process = (%+v, %v), want rejected with no attempt", outcome, err)
			}
			want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "RejectDisposition"}
			if got := f.store.operations(); !equalStrings(got, want) {
				t.Fatalf("the store saw %v, want %v", got, want)
			}
			if len(f.runtime.commands()) != 0 {
				t.Fatal("a rejected gate response reached the runtime")
			}
			rejects := f.store.rejects
			if len(rejects) != 1 || rejects[0].ExpectedRevision != testRevision+1 || rejects[0].ResidencyEpoch != testEpoch {
				t.Fatalf("rejections = %+v, want one at the claim's revision under this Host's residency", rejects)
			}
			if got := f.stored().record.State; got != StateRejected {
				t.Fatalf("the durable record is %q, want rejected", got)
			}
		})
	}
}

// TestAGateResponseIsNotRejectedForAGateThisHostDoesNotOwn: a mark BELOW this
// Host's grant means its fencing write has not landed yet; ABOVE it means a
// successor has written. Neither is a statement about the answer, so neither is
// a rejection: the pass blocks with the claim held and nothing written, and a
// later pass — prompted by the publisher converging — proceeds once the mark is
// this Host's.
func TestAGateResponseIsNotRejectedForAGateThisHostDoesNotOwn(t *testing.T) {
	for name, owner := range map[string]uint64{"not yet fenced": testEpoch - 1, "superseded": testEpoch + 1} {
		t.Run(name, func(t *testing.T) {
			gates := ownedGate(owner)
			f := gateFixture(t, gates, nil)
			_, err := f.process()
			var refused *ApplyError
			if !errors.As(err, &refused) || refused.Refusal != RefusalGateNotOwned {
				t.Fatalf("Process = %v, want RefusalGateNotOwned", err)
			}
			want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload"}
			if got := f.store.operations(); !equalStrings(got, want) {
				t.Fatalf("the store saw %v, want %v", got, want)
			}
			if got := f.stored().record.State; got != StateClaimed {
				t.Fatalf("the durable record is %q, want still claimed", got)
			}
		})
	}

	// And once the mark is this Host's, the same claimed command proceeds.
	gates := ownedGate(testEpoch - 1)
	f := gateFixture(t, gates, nil)
	if _, err := f.process(); err == nil {
		t.Fatal("the first pass did not block")
	}
	id := sessionwire.GateID(testGateUUID.String())
	gates.mu.Lock()
	held := gates.gates[id]
	held.OwnerEpoch = testEpoch
	gates.gates[id] = held
	gates.mu.Unlock()
	if outcome, err := f.process(); err != nil || outcome.State != StateApplied {
		t.Fatalf("after the fencing write, Process = (%+v, %v), want applied", outcome, err)
	}
}

// TestAGateResponseToAGateNotProjectedIsTheRuntimesToDecide: a gate the
// projection no longer holds — answered, timed out, or closed at restore — is
// not rejected by Host. The runtime is the authority on whether it is open, and
// harness settles an answer to a closed gate as no_op; rejecting on Host's own
// reading would race the projection and could destroy a valid answer.
func TestAGateResponseToAGateNotProjectedIsTheRuntimesToDecide(t *testing.T) {
	f := gateFixture(t, &fakeGates{}, nil)
	f.runtime.onApply = func(command sessionwire.CommandID) { f.setEvidence(command, "no_op") }
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v, %v), want the runtime's no_op settled", outcome, err)
	}
	if len(f.runtime.commands()) != 1 {
		t.Fatal("the runtime was not asked")
	}
}

// TestAGateResponseHostCannotReadYetBlocks: a projection that cannot be read
// and a composition with no gate reader are this Host's limits and not the
// answer's, so each blocks with no write
// past the claim and no rejection.
func TestAGateResponseHostCannotReadYetBlocks(t *testing.T) {
	for _, row := range []struct {
		name      string
		configure func(*dispositionFixture)
		refusal   ApplyRefusal
	}{
		{"an unreadable projection", func(f *dispositionFixture) {
			f.gates.err = errors.New("injected: the catalog read failed")
		}, RefusalStore},
		{"no gate reader", func(f *dispositionFixture) { f.gates = nil }, RefusalNoGateReader},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := gateFixture(t, ownedGate(testEpoch), nil)
			row.configure(f)
			f.rebuild()
			_, err := f.process()
			var refused *ApplyError
			if !errors.As(err, &refused) || refused.Refusal != row.refusal {
				t.Fatalf("Process = %v, want %q", err, row.refusal)
			}
			for _, op := range f.store.operations() {
				if op == "BeginAttempt" || op == "RejectDisposition" {
					t.Fatalf("the store saw %s; a limit of this Host is neither an attempt nor a rejection", op)
				}
			}
		})
	}
}

// TestOnlyAGateResponseReadsTheProjection: input and interrupt carry no gate.
func TestOnlyAGateResponseReadsTheProjection(t *testing.T) {
	gates := ownedGate(testEpoch)
	f := newDispositionFixture(t, func(f *dispositionFixture) { f.gates = gates })
	if outcome, err := f.process(); err != nil || outcome.State != StateApplied {
		t.Fatalf("Process(input) = (%+v, %v)", outcome, err)
	}
	if gates.loads != 0 {
		t.Fatalf("an input read the gate projection %d times", gates.loads)
	}
}

// TestAFailedRejectionIsAStoreRefusal: the rejection is a durable write like
// any other, and one that fails leaves the command claimed and the pass blocked.
func TestAFailedRejectionIsAStoreRefusal(t *testing.T) {
	f := gateFixture(t, ownedGate(testEpoch), func(r *sessionwire.GateResponseRequest) { r.GateID = "gate-a" })
	f.store.errs["RejectDisposition"] = errors.New("injected: the reject write failed")
	_, err := f.process()
	var refused *ApplyError
	if !errors.As(err, &refused) || refused.Refusal != RefusalStore {
		t.Fatalf("Process = %v, want a store refusal", err)
	}
	if got := f.stored().record.State; got != StateClaimed {
		t.Fatalf("the durable record is %q, want still claimed", got)
	}
}

// TestAHostRetakesItsOwnLapsedClaim is the spec gate's M2 construction: this
// Host claims a gate response and blocks (its fencing write has not landed)
// for longer than the claim TTL. The claim is still THIS Host's residency but
// has lapsed, and BeginAttempt refuses a lapsed claim, so resuming straight
// at the attempt was refused on every pass and the user's answer sat claimed
// until Factory's deadline sweep rejected it. The claim is re-taken first.
func TestAHostRetakesItsOwnLapsedClaim(t *testing.T) {
	gates := ownedGate(testEpoch - 1)
	f := gateFixture(t, gates, nil)
	if _, err := f.process(); err == nil {
		t.Fatal("the first pass did not block")
	}
	if got := f.stored().record.State; got != StateClaimed {
		t.Fatalf("after the block the record is %q, want claimed", got)
	}
	// The block outlives the claim: the TTL is 11s in this fixture, the
	// spec gate's was 5s against a 6s block.
	f.clock.advance(12 * time.Second)
	id := sessionwire.GateID(testGateUUID.String())
	gates.mu.Lock()
	held := gates.gates[id]
	held.OwnerEpoch = testEpoch
	gates.gates[id] = held
	gates.mu.Unlock()

	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("after the fence landed, Process = (%+v, %v), want the lapsed claim re-taken and the answer applied", outcome, err)
	}
	claims := 0
	for _, op := range f.store.operations() {
		if op == "ClaimDisposition" {
			claims++
		}
	}
	if claims != 2 {
		t.Fatalf("the store saw %d claims, want the original and the re-take", claims)
	}
}

// TestAReferencedGateResponseIsRejectedNotBlocked (spec gate C1, quality gate
// F5): no released Host can apply a body stored by reference, and blocking on
// it held the session's whole stream — interrupts included — until Factory's
// deadline sweep rejected it anyway. It is rejected before any attempt, even by
// a composition with no gate reader.
func TestAReferencedGateResponseIsRejectedNotBlocked(t *testing.T) {
	for name, gates := range map[string]*fakeGates{"with a gate reader": ownedGate(testEpoch), "with none": nil} {
		t.Run(name, func(t *testing.T) {
			f := gateFixture(t, gates, nil)
			f.store.payloads[commandID(1)] = Payload{Ref: sessionwire.ObjectReference{ObjectID: "object-1"}}
			outcome, err := f.process()
			if err != nil || outcome.State != StateRejected {
				t.Fatalf("Process = (%+v, %v), want rejected", outcome, err)
			}
			want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "RejectDisposition"}
			if got := f.store.operations(); !equalStrings(got, want) {
				t.Fatalf("the store saw %v, want %v", got, want)
			}
		})
	}
}
