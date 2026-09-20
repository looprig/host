package commands

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func createPayload(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal(sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(1)},
		SessionID:       testSession, AgentID: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func createFixture(t *testing.T, payload Payload) *dispositionFixture {
	t.Helper()
	return newDispositionFixture(t, func(f *dispositionFixture) {
		f.put(KindCreate, StatePending)
		f.store.payloads[commandID(1)] = payload
	})
}

func TestPermanentCreateBodyIsRejectedBeforeAttempt(t *testing.T) {
	for _, row := range []struct {
		name string
		body []byte
	}{
		{"absent", nil},
		{"invalid JSON", []byte("not-json")},
		{"not object", []byte(`"hello"`)},
		{"wrong session", []byte(`{"version":1,"command_id":"v1:command-1","session_id":"other","agent_id":"agent-a"}`)},
		{"wrong command", []byte(`{"version":1,"command_id":"v1:command-2","session_id":"session-inbox","agent_id":"agent-a"}`)},
		{"blocks not array", []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a","blocks":{}}`)},
		{"null blocks", []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a","blocks":null}`)},
		{"empty blocks", []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a","blocks":[]}`)},
		{"duplicate field", []byte(`{"version":1,"version":1,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a"}`)},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := createFixture(t, Payload{Body: row.body})
			outcome, err := f.process()
			if err != nil || outcome.State != StateRejected || outcome.PrefixOwned {
				t.Fatalf("Process = (%+v,%v)", outcome, err)
			}
			want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload", "RejectDisposition"}
			if got := f.store.operations(); !equalStrings(got, want) {
				t.Fatalf("operations = %v", got)
			}
			if len(f.store.begins) != 0 || f.attempts.n != 0 || len(f.runtime.commands()) != 0 {
				t.Fatal("rejected create reached attempt or runtime")
			}
		})
	}
}

func TestCreateBodyThisHostCannotReadBlocksWithoutAttempt(t *testing.T) {
	for _, row := range []struct {
		name    string
		payload Payload
	}{
		{"newer version", Payload{Body: []byte(`{"version":99,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a"}`)}},
		{"future member", Payload{Body: []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","agent_id":"agent-a","future":true}`)}},
		{"reference only", Payload{Ref: sessionwire.ObjectReference{ObjectID: "object-1"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := createFixture(t, row.payload)
			outcome, err := f.process()
			var refusal *ApplyError
			if !errors.As(err, &refusal) || refusal.Refusal != RefusalUnreadableCreate || outcome.State != StateClaimed || outcome.PrefixOwned {
				t.Fatalf("Process = (%+v,%v)", outcome, err)
			}
			want := []string{"LoadDispositionCommand", "ClaimDisposition", "LoadDispositionPayload"}
			if got := f.store.operations(); !equalStrings(got, want) {
				t.Fatalf("operations = %v", got)
			}
			if len(f.store.begins) != 0 || len(f.store.rejects) != 0 || f.attempts.n != 0 || len(f.runtime.commands()) != 0 {
				t.Fatal("unsupported create was attempted, rejected or dispatched")
			}
		})
	}
}

func TestBareCreateStillDispatches(t *testing.T) {
	f := createFixture(t, Payload{Body: createPayload(t)})
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v,%v)", outcome, err)
	}
}

func TestCreateRejectionDoesNotWriteAfterFenceLoss(t *testing.T) {
	f := createFixture(t, Payload{Body: []byte("not-json")})
	f.stored().record.State = StateClaimed
	f.stored().record.ClaimResidencyEpoch = testEpoch
	f.stored().record.ClaimExpiresAt = f.clock.Now().Add(testApplyDeadline)
	f.fence.end()
	outcome, err := f.process()
	if err == nil || outcome.State != StateClaimed {
		t.Fatalf("Process = (%+v,%v)", outcome, err)
	}
	if len(f.store.rejects) != 0 || len(f.store.begins) != 0 {
		t.Fatal("wrote after fence loss")
	}
}

func TestApplyingCreateBypassesBodyPrecheckAndSettlesEvidence(t *testing.T) {
	f := createFixture(t, Payload{Body: []byte("not-json")})
	stored := f.stored()
	stored.record.State = StateApplying
	stored.record.ClaimResidencyEpoch = testEpoch
	stored.record.AttemptID = "earlier-attempt"
	stored.record.AttemptJournalEpoch = testJournalEpoch
	stored.record.AttemptResidencyEpoch = testEpoch
	stored.evidence = "applied"
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied || !outcome.PrefixOwned {
		t.Fatalf("Process = (%+v,%v)", outcome, err)
	}
	want := []string{"LoadDispositionCommand", "SettleDisposition"}
	if got := f.store.operations(); !equalStrings(got, want) {
		t.Fatalf("operations = %v", got)
	}
}
