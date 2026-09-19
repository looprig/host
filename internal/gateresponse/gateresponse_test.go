package gateresponse

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

const (
	testSession sessionwire.SessionID = "core/session/opaque"
	testCommand sessionwire.CommandID = "command-1"
)

var testGate = uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f")

// request is a valid gate response for (testSession, testCommand), in the shape
// Factory stores it: json.Marshal of Core's record.
func request() sessionwire.GateResponseRequest {
	return sessionwire.GateResponseRequest{
		CommandEnvelope:        sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: testCommand},
		SessionID:              testSession,
		GateID:                 sessionwire.GateID(testGate.String()),
		Action:                 "answer",
		Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"blue"`)},
		ExpectedOpenJournalSeq: 7,
	}
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// TestDecodeReadsFactorysStoredBody: the body Factory admits decodes, and the
// gate identity is harness's.
func TestDecodeReadsFactorysStoredBody(t *testing.T) {
	got, err := Decode(encode(t, request()), testSession, testCommand)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.GateID != testGate || got.Request.Action != "answer" || string(got.Request.Values["answer"]) != `"blue"` || got.Request.ExpectedOpenJournalSeq != 7 {
		t.Fatalf("Decode = %+v", got)
	}
}

// TestDecodeRefusesEveryBodyNoHostCouldApply: each row is permanent, so each is
// a MalformedError.
func TestDecodeRefusesEveryBodyNoHostCouldApply(t *testing.T) {
	for _, row := range []struct {
		name    string
		body    func(t *testing.T) []byte
		session sessionwire.SessionID
		command sessionwire.CommandID
	}{
		{"empty", func(*testing.T) []byte { return nil }, testSession, testCommand},
		{"not JSON", func(*testing.T) []byte { return []byte("not json") }, testSession, testCommand},
		{"an unknown member", func(*testing.T) []byte {
			return []byte(`{"version":1,"command_id":"command-1","session_id":"core/session/opaque","gate_id":"` + testGate.String() + `","action":"a","values":{},"expected_open_journal_seq":7,"extra":1}`)
		}, testSession, testCommand},
		{"no expected-open version", func(t *testing.T) []byte {
			r := request()
			r.ExpectedOpenJournalSeq = 0
			return encode(t, r)
		}, testSession, testCommand},
		{"another session", func(t *testing.T) []byte { return encode(t, request()) }, "core/session/other", testCommand},
		{"another command", func(t *testing.T) []byte { return encode(t, request()) }, testSession, "command-2"},
		{"a gate id that is not a UUID", func(t *testing.T) []byte {
			r := request()
			r.GateID = "gate-a"
			return encode(t, r)
		}, testSession, testCommand},
		{"the zero gate id", func(t *testing.T) []byte {
			r := request()
			r.GateID = sessionwire.GateID(uuid.UUID{}.String())
			return encode(t, r)
		}, testSession, testCommand},
	} {
		t.Run(row.name, func(t *testing.T) {
			_, err := Decode(row.body(t), row.session, row.command)
			var malformed *MalformedError
			if !errors.As(err, &malformed) {
				t.Fatalf("Decode = %v, want a MalformedError", err)
			}
		})
	}
}
