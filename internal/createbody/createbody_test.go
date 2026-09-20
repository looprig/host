package createbody_test

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/createbody"
)

const (
	testSession = sessionwire.SessionID("session-a")
	testCommand = sessionwire.CommandID("command-a")
)

// stored is the body Factory canonically encodes for a create.
func stored(t *testing.T, blocks string) []byte {
	t.Helper()
	request := sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: testCommand},
		SessionID:       testSession,
		AgentID:         "agent-a",
	}
	if blocks != "" {
		request.Blocks = json.RawMessage(blocks)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("the fixture is not a create Core admits: %v", err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// THE RE-PRESENTED BODY IS BYTE-IDENTICAL TO THE INPUT FACTORY WOULD HAVE
// ADMITTED for the same message, which is the property the whole design rests
// on: one composition-chosen encoding serves both kinds because the decoder
// cannot tell the two bodies apart.
func TestTheFirstMessageIsTheInputFactoryWouldHaveAdmitted(t *testing.T) {
	const blocks = `[{"type":"text","text":"the first thing the user said"}]`

	presented, err := createbody.FirstMessage(stored(t, blocks))
	if err != nil {
		t.Fatalf("FirstMessage: %v", err)
	}
	equivalent, err := json.Marshal(sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: testCommand},
		SessionID:       testSession,
		Blocks:          json.RawMessage(blocks),
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(presented) != string(equivalent) {
		t.Fatalf("the re-presented body is\n%s\nwant the input body\n%s", presented, equivalent)
	}
	// AND CORE ITSELF ACCEPTS IT, so a decoder that uses Core's strict decoder
	// -- the only decoder an input's body could have been written for -- reads
	// it rather than refusing it.
	var decoded sessionwire.InputRequest
	if err := json.Unmarshal(presented, &decoded); err != nil {
		t.Fatalf("Core's strict input decoder refused the re-presented body: %v", err)
	}
	if string(decoded.Blocks) != blocks {
		t.Fatalf("the decoded blocks are %s, want %s", decoded.Blocks, blocks)
	}
}

// A BARE CREATE IS NOT AN ERROR AND CARRIES NOTHING. Core's Blocks member is
// omitempty and an idle create is legitimate; refusing one would make every
// idle session's first command unsettleable.
func TestABareCreateCarriesNothingAndIsNotAnError(t *testing.T) {
	presented, err := createbody.FirstMessage(stored(t, ""))
	if err != nil {
		t.Fatalf("FirstMessage of a bare create: %v", err)
	}
	if presented != nil {
		t.Fatalf("a bare create produced %s, want no body at all", presented)
	}
}

// A BODY CORE DOES NOT ADMIT IS REFUSED, PERMANENTLY. Every row is a body no
// Host could ever read, so a retry or another Host reaches the same answer.
func TestABodyCoreDoesNotAdmitIsRefused(t *testing.T) {
	for _, row := range []struct {
		name string
		body []byte
	}{
		{"no body at all", nil},
		{"an empty body", []byte{}},
		{"not JSON", []byte("hello")},
		{"a JSON value that is not an object", []byte(`"hello"`)},
		{"an input request, not a create", []byte(`{"version":1,"command_id":"command-a","session_id":"session-a","blocks":[{"type":"text","text":"hi"}]}`)},
		{"a create with no agent", []byte(`{"version":1,"command_id":"command-a","session_id":"session-a"}`)},
		{"a create whose blocks are not a block array", []byte(`{"version":1,"command_id":"command-a","session_id":"session-a","agent_id":"agent-a","blocks":{"text":"hi"}}`)},
	} {
		t.Run(row.name, func(t *testing.T) {
			presented, err := createbody.FirstMessage(row.body)
			var malformed *createbody.MalformedError
			if !errors.As(err, &malformed) {
				t.Fatalf("FirstMessage = (%s, %v), want a MalformedError", presented, err)
			}
			if presented != nil {
				t.Fatalf("a refused body still produced %s", presented)
			}
		})
	}
}

func TestANewerCreateBodyIsUnsupportedHere(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"version":99,"command_id":"command-a","session_id":"session-a","agent_id":"agent-a"}`),
		[]byte(`{"version":1,"command_id":"command-a","session_id":"session-a","agent_id":"agent-a","future_member":true}`),
	} {
		_, err := createbody.FirstMessage(body)
		var unsupported *createbody.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("FirstMessage(%s) = %v, want UnsupportedError", body, err)
		}
	}
}

func TestCreateBodyIdentityMismatchIsPermanent(t *testing.T) {
	for _, row := range []struct {
		session sessionwire.SessionID
		command sessionwire.CommandID
	}{
		{"session-other", testCommand}, {testSession, "command-other"},
	} {
		err := createbody.Check(stored(t, ""), row.session, row.command)
		var malformed *createbody.MalformedError
		if !errors.As(err, &malformed) {
			t.Fatalf("Check identity = %v, want MalformedError", err)
		}
	}
}

// AN ABSENT BODY IS DISTINGUISHABLE, because a command whose body was never
// inline is a different diagnosis from one whose bytes are wrong.
func TestAnAbsentBodyNamesItsOwnCause(t *testing.T) {
	_, err := createbody.FirstMessage(nil)
	if !errors.Is(err, createbody.ErrEmptyBody) {
		t.Fatalf("FirstMessage(nil) = %v, want ErrEmptyBody", err)
	}
}
