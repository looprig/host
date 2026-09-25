package commands

import (
	"encoding/json"
	"errors"
	"strings"
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

func TestPreAttemptBodyClassificationAcrossKinds(t *testing.T) {
	largeBody, err := json.Marshal(sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID(1)},
		SessionID:       testSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"` + strings.Repeat("x", 65537) + `"}]`),
	})
	if err != nil || len(largeBody) <= 64*1024 {
		t.Fatalf("oversized input fixture has %d bytes: %v", len(largeBody), err)
	}
	// Factory stores this oversized body by object reference. The applier has
	// no object reader, so it must block before an attempt, not strand one.
	ref := Payload{Ref: sessionwire.ObjectReference{ObjectID: "object-1"}}
	for _, row := range []struct {
		name    string
		kind    Kind
		payload Payload
		reject  bool
	}{
		{"over-64-KiB input by reference", KindInput, ref, false},
		{"interrupt by reference", KindInterrupt, ref, false},
		{"restore by reference", KindRestore, ref, false},
		{"malformed interrupt", KindInterrupt, Payload{Body: []byte("not-json")}, true},
		{"malformed restore", KindRestore, Payload{Body: []byte(`{"version":1,"command_id":"v1:command-1","session_id":"session-inbox","principal":null}`)}, true},
		{"future interrupt version", KindInterrupt, Payload{Body: []byte(`{"version":99,"command_id":"v1:command-1","session_id":"session-inbox"}`)}, false},
		{"future restore version", KindRestore, Payload{Body: []byte(`{"version":99,"command_id":"v1:command-1","session_id":"session-inbox"}`)}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t, func(f *dispositionFixture) {
				f.put(row.kind, StatePending)
				f.store.payloads[commandID(1)] = row.payload
			})
			outcome, err := f.process()
			if len(f.store.begins) != 0 || f.attempts.n != 0 || len(f.runtime.commands()) != 0 {
				t.Fatalf("a pre-attempt body reached the attempt or runtime: %+v, %v", outcome, err)
			}
			if row.reject {
				if err != nil || outcome.State != StateRejected || outcome.PrefixOwned || len(f.store.rejects) != 1 {
					t.Fatalf("Process = (%+v,%v), rejects=%d", outcome, err, len(f.store.rejects))
				}
				return
			}
			var refusal *ApplyError
			if !errors.As(err, &refusal) || refusal.Refusal != RefusalUnreadableCommand || outcome.State != StateClaimed || outcome.PrefixOwned || len(f.store.rejects) != 0 {
				t.Fatalf("Process = (%+v,%v), rejects=%d", outcome, err, len(f.store.rejects))
			}
		})
	}
}

func TestUnstampedCommandDispatchesWithoutInventedMembers(t *testing.T) {
	f := newDispositionFixture(t)
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v,%v)", outcome, err)
	}
	commands := f.runtime.commands()
	if len(commands) != 1 || commands[0].Principal != nil || commands[0].Metadata != nil {
		t.Fatalf("unstamped command gained members: %+v", commands)
	}
}
