package commands

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

var stampedPrincipal = &sessionwire.Principal{Tenant: testTenant, Subject: "user-1", Kind: sessionwire.PrincipalKindActor}

func stampedBody(t *testing.T, kind Kind) []byte {
	t.Helper()
	envelope := sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(1)}
	var request any
	switch kind {
	case KindInput:
		request = sessionwire.InputRequest{CommandEnvelope: envelope, SessionID: testSession,
			Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`), Metadata: sessionwire.MessageMetadata{"space": "family"}, Principal: stampedPrincipal}
	case KindCreate:
		request = sessionwire.CreateRequest{CommandEnvelope: envelope, SessionID: testSession, AgentID: "agent-a",
			Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`), Metadata: sessionwire.MessageMetadata{"space": "family"}, Principal: stampedPrincipal}
	case KindInterrupt:
		request = sessionwire.InterruptRequest{CommandEnvelope: envelope, SessionID: testSession, Principal: stampedPrincipal}
	case KindRestore:
		request = sessionwire.RestoreRequest{CommandEnvelope: envelope, SessionID: testSession, Principal: stampedPrincipal}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestTheDecodedMembersReachTheRuntime(t *testing.T) {
	for _, kind := range []Kind{KindInput, KindCreate, KindInterrupt, KindRestore} {
		t.Run(string(kind), func(t *testing.T) {
			body := stampedBody(t, kind)
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				f.put(kind, StatePending)
				f.store.payloads[commandID(1)] = Payload{Body: body}
			})
			outcome, err := f.process()
			if err != nil || outcome.State != StateApplied {
				t.Fatalf("Process = (%+v,%v)", outcome, err)
			}
			commands := f.runtime.commands()
			if len(commands) != 1 {
				t.Fatalf("dispatched %d commands", len(commands))
			}
			got := commands[0]
			if got.Principal == nil || *got.Principal != *stampedPrincipal {
				t.Fatalf("Principal = %+v", got.Principal)
			}
			if want := kind == KindInput || kind == KindCreate; (got.Metadata["space"] == "family") != want {
				t.Fatalf("Metadata = %v", got.Metadata)
			}
			if string(got.Payload) != string(body) {
				t.Fatalf("payload rewritten: %s", got.Payload)
			}
		})
	}
}

func TestPermanentlyUnreadableInputIsRejectedBeforeAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		f.store.payloads[commandID(1)] = Payload{Body: []byte("not-json")}
	})
	outcome, err := f.process()
	if err != nil || outcome.State != StateRejected || len(f.store.begins) != 0 || len(f.runtime.commands()) != 0 {
		t.Fatalf("Process = (%+v,%v), attempts=%d dispatched=%d", outcome, err, len(f.store.begins), len(f.runtime.commands()))
	}
}

func TestAnInputBodyANewerHostMayReadBlocksBeforeAttempt(t *testing.T) {
	f := newDispositionFixture(t, func(f *dispositionFixture) {
		f.store.payloads[commandID(1)] = Payload{Body: []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","blocks":[{"type":"text","text":"x"}],"future":1}`)}
	})
	outcome, err := f.process()
	var refusal *ApplyError
	if !errors.As(err, &refusal) || refusal.Refusal != RefusalUnreadableCommand || outcome.State != StateClaimed || len(f.store.begins) != 0 || len(f.store.rejects) != 0 {
		t.Fatalf("Process = (%+v,%v)", outcome, err)
	}
}
