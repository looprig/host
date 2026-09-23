package publicbody

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

const (
	runtimeSession = "11111111-1111-4111-8111-111111111111"
	runtimeCommand = "ffffffff-ffff-4fff-8fff-ffffffffffff"
	machineCommand = "eeeeeeee-eeee-4eee-8eee-000000000001"
	publicSession  = sessionwire.SessionID("session-public-1")
	publicCommand  = sessionwire.CommandID("command-public-1")
)

// esc is an HTML-escaped fragment as Core's canonical encoding writes it.
const esc = `\u003cb\u003e \u0026`

type mapCommands map[string]sessionwire.CommandID

func (m mapCommands) PublicCommand(_ context.Context, runtime uuid.UUID, _ uint64) (sessionwire.CommandID, bool, error) {
	public, ok := m[runtime.String()]
	return public, ok, nil
}

func ids(commands Commands) Identities {
	return Identities{RuntimeSessionID: uuid.MustParse(runtimeSession), SessionID: publicSession, Commands: commands}
}

func projectBody(t *testing.T, body string, commands Commands) string {
	t.Helper()
	out, err := Project(context.Background(), json.RawMessage(body), ids(commands), 7)
	if err != nil {
		t.Fatalf("Project(%s): %v", body, err)
	}
	return string(out)
}

// TestTheRuntimeIdentitiesBecomeThePublicOnesAndNothingElseMoves is W1: the
// real harness shape of a command-caused event, with its member order kept.
func TestTheRuntimeIdentitiesBecomeThePublicOnesAndNothingElseMoves(t *testing.T) {
	body := `{"cause":{"command_id":"` + runtimeCommand + `","agency":1},"created_at":"2026-08-27T10:00:00Z","event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee","loop_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","message":{"role":"user","blocks":[{"Text":"a ` + esc + ` c","type":"text"}]},"session_id":"` + runtimeSession + `","turn_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","turn_index":1,"type":"TurnStarted","v":1}`
	want := `{"cause":{"command_id":"command-public-1","agency":1},"created_at":"2026-08-27T10:00:00Z","event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee","loop_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","message":{"role":"user","blocks":[{"Text":"a ` + esc + ` c","type":"text"}]},"session_id":"session-public-1","turn_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","turn_index":1,"type":"TurnStarted","v":1}`
	if got := projectBody(t, body, mapCommands{runtimeCommand: publicCommand}); got != want {
		t.Fatalf("projected\n got %s\nwant %s", got, want)
	}
}

// TestHarnessStructOrderIsKept: harness marshals most events from structs, so
// members are NOT sorted; the projection must not sort them either.
func TestHarnessStructOrderIsKept(t *testing.T) {
	body := `{"type":"SessionIdle","v":1,"session_id":"` + runtimeSession + `","event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}`
	want := `{"type":"SessionIdle","v":1,"session_id":"session-public-1","event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}`
	if got := projectBody(t, body, nil); got != want {
		t.Fatalf("projected\n got %s\nwant %s", got, want)
	}
}

// TestAMachineCauseLosesItsCommandID: a runtime command with no public
// identity is removed, and a cause left empty goes with it.
func TestAMachineCauseLosesItsCommandID(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{
			name: "cause holds only the command",
			body: `{"cause":{"command_id":"` + machineCommand + `"},"session_id":"` + runtimeSession + `","type":"TurnStarted"}`,
			want: `{"session_id":"session-public-1","type":"TurnStarted"}`,
		},
		{
			name: "cause is the last member",
			body: `{"type":"TurnStarted","cause":{"command_id":"` + machineCommand + `"}}`,
			want: `{"type":"TurnStarted"}`,
		},
		{
			name: "cause keeps its other members",
			body: `{"cause":{"loop_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","command_id":"` + machineCommand + `","session_id":"` + runtimeSession + `"},"type":"TurnStarted"}`,
			want: `{"cause":{"loop_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","session_id":"session-public-1"},"type":"TurnStarted"}`,
		},
		{
			name: "only command_id is a command",
			body: `{"cause":{"event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee","agency":1,"command_id":"` + machineCommand + `"},"type":"X"}`,
			want: `{"cause":{"event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee","agency":1},"type":"X"}`,
		},
		{
			name: "command id that is no uuid",
			body: `{"cause":{"command_id":"not-a-uuid","event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},"type":"X"}`,
			want: `{"cause":{"event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},"type":"X"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := projectBody(t, tc.body, mapCommands{runtimeCommand: publicCommand}); got != tc.want {
				t.Fatalf("projected\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// TestANestedRuntimeSessionIsReplacedWhereverItIsNamed: coordinates carried
// inside a payload (a gate's review context, a subject) name the session too.
func TestANestedRuntimeSessionIsReplacedWhereverItIsNamed(t *testing.T) {
	body := `{"gate":{"subject":{"coordinates":[{"session_id":"` + runtimeSession + `","loop_id":"l"},{"session_id":"22222222-2222-4222-8222-222222222222"}]}},"type":"GateOpened"}`
	want := `{"gate":{"subject":{"coordinates":[{"session_id":"session-public-1","loop_id":"l"},{"session_id":"22222222-2222-4222-8222-222222222222"}]}},"type":"GateOpened"}`
	if got := projectBody(t, body, nil); got != want {
		t.Fatalf("projected\n got %s\nwant %s", got, want)
	}
}

// TestOnlyTheTopLevelCauseIsACommandCause: `cause` is the event header's; a
// member of the same name inside the payload is content.
func TestOnlyTheTopLevelCauseIsACommandCause(t *testing.T) {
	body := `{"payload":{"cause":{"command_id":"` + runtimeCommand + `"}},"type":"X"}`
	if got := projectBody(t, body, mapCommands{runtimeCommand: publicCommand}); got != body {
		t.Fatalf("a payload cause was projected: %s", got)
	}
}

// TestABodyNamingNothingPrivateIsTheSameBytes: nothing is re-encoded when
// nothing changed.
func TestABodyNamingNothingPrivateIsTheSameBytes(t *testing.T) {
	body := `{"z":1,"a":[1,{"b":"` + esc + `"}],"session_id":"33333333-3333-4333-8333-333333333333","s":"é"}`
	if got := projectBody(t, body, nil); got != body {
		t.Fatalf("projected\n got %s\nwant %s", got, body)
	}
}

// TestANonCanonicalBodyIsRefused: the projection neither repairs nor passes on
// a body Core would refuse.
func TestANonCanonicalBodyIsRefused(t *testing.T) {
	for _, body := range []string{`{"a": 1}`, `{"a":"<"}`, `not json`, ``} {
		if _, err := Project(context.Background(), json.RawMessage(body), ids(nil), 1); !errors.Is(err, ErrNonCanonical) {
			t.Errorf("Project(%q) = %v, want ErrNonCanonical", body, err)
		}
	}
}

type failingCommands struct{ err error }

func (f failingCommands) PublicCommand(context.Context, uuid.UUID, uint64) (sessionwire.CommandID, bool, error) {
	return "", false, f.err
}

// TestAMappingThatCannotBeReadFailsTheProjection: an unreadable mapping is not
// an absent one — removing the id would publish a body that differs from the
// one a later read produces.
func TestAMappingThatCannotBeReadFailsTheProjection(t *testing.T) {
	cause := errors.New("store down")
	body := `{"cause":{"command_id":"` + runtimeCommand + `"},"type":"X"}`
	_, err := Project(context.Background(), json.RawMessage(body), ids(failingCommands{cause}), 1)
	var mapping *MappingError
	if !errors.Is(err, cause) || !errors.As(err, &mapping) {
		t.Fatalf("Project = %v, want the mapping's error as a *MappingError", err)
	}
}

// TestTheProjectionRequiresBothSessionIdentities: an unbound projector would
// silently carry the runtime id.
func TestTheProjectionRequiresBothSessionIdentities(t *testing.T) {
	body := json.RawMessage(`{"type":"X"}`)
	if _, err := Project(context.Background(), body, Identities{SessionID: publicSession}, 1); err == nil {
		t.Error("a projection with no runtime session id was accepted")
	}
	if _, err := Project(context.Background(), body, Identities{RuntimeSessionID: uuid.MustParse(runtimeSession)}, 1); err == nil {
		t.Error("a projection with no public session id was accepted")
	}
}

// TestFreeTextNamingARuntimeIdentityIsProjectedToo is review S2's probe:
// harness formats the runtime session id into error text (`journal: session
// <id> ...`, a capture-spill path under it), and a TurnFailed projects that
// text as its message. A mapped command's runtime id is as private and goes
// the same way; a machine command's is not private and is left in the text.
func TestFreeTextNamingARuntimeIdentityIsProjectedToo(t *testing.T) {
	body := `{"cause":{"command_id":"` + runtimeCommand + `"},"error":{"kind":"journal","message":"journal: session ` + runtimeSession + ` lease lost; spill /var/capture/` + runtimeSession + `/tool-1; command ` + runtimeCommand + `; machine ` + machineCommand + `"},"other":"22222222-2222-4222-8222-222222222222","type":"TurnFailed"}`
	want := `{"cause":{"command_id":"command-public-1"},"error":{"kind":"journal","message":"journal: session session-public-1 lease lost; spill /var/capture/session-public-1/tool-1; command command-public-1; machine ` + machineCommand + `"},"other":"22222222-2222-4222-8222-222222222222","type":"TurnFailed"}`
	if got := projectBody(t, body, mapCommands{runtimeCommand: publicCommand}); got != want {
		t.Fatalf("projected\n got %s\nwant %s", got, want)
	}
}

// TestAPublicIdIsWrittenAsJSONStringContent: a public id carrying a character
// JSON escapes is written escaped, so the body stays canonical.
func TestAPublicIdIsWrittenAsJSONStringContent(t *testing.T) {
	body := `{"message":"at ` + runtimeSession + `"}`
	out, err := Project(context.Background(), json.RawMessage(body), Identities{
		RuntimeSessionID: uuid.MustParse(runtimeSession), SessionID: "a<b&c",
	}, 1)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if want := `{"message":"at a\u003cb\u0026c"}`; string(out) != want {
		t.Fatalf("projected %s, want %s", out, want)
	}
}

type scriptedSource struct {
	pages [][]Pair
	reads int
	err   error
}

func (s *scriptedSource) Read(_ context.Context, after, bound uint64) ([]Pair, uint64, bool, error) {
	s.reads++
	if s.err != nil {
		return nil, after, false, s.err
	}
	if int(after) >= len(s.pages) {
		return nil, after, true, nil
	}
	return s.pages[after], after + 1, after+1 >= bound, nil
}

// TestTheIndexReadsOnlyUntilItCanAnswerAndRemembersBothAnswers.
func TestTheIndexReadsOnlyUntilItCanAnswerAndRemembersBothAnswers(t *testing.T) {
	a, b := uuid.MustParse(runtimeCommand), uuid.MustParse(machineCommand)
	source := &scriptedSource{pages: [][]Pair{{}, {{Runtime: a, Public: publicCommand}}, {}, {}}}
	index := NewIndex(source)

	got, ok, err := index.PublicCommand(context.Background(), a, 10)
	if err != nil || !ok || got != publicCommand {
		t.Fatalf("PublicCommand(a) = (%q, %v, %v)", got, ok, err)
	}
	if source.reads != 2 {
		t.Fatalf("read %d pages to find a mapping on the second, want 2", source.reads)
	}
	if _, _, _ = index.PublicCommand(context.Background(), a, 10); source.reads != 2 {
		t.Fatal("a known mapping was read again")
	}
	// b is absent; the bound stops the read at page 3.
	if _, ok, _ := index.PublicCommand(context.Background(), b, 3); ok {
		t.Fatal("an unmapped command was answered")
	}
	if source.reads != 3 {
		t.Fatalf("reads = %d, want 3: the index read past the bound", source.reads)
	}
	if _, _, _ = index.PublicCommand(context.Background(), b, 3); source.reads != 3 {
		t.Fatal("a remembered miss was read again")
	}
}

// TestTheIndexStopsAtTheEndOfItsSource.
func TestTheIndexStopsAtTheEndOfItsSource(t *testing.T) {
	source := &scriptedSource{pages: [][]Pair{{}}}
	index := NewIndex(source)
	if _, ok, err := index.PublicCommand(context.Background(), uuid.MustParse(runtimeCommand), 1<<40); ok || err != nil {
		t.Fatalf("= (%v, %v), want a miss", ok, err)
	}
	if source.reads != 2 {
		t.Fatalf("reads = %d, want 2", source.reads)
	}
}

// TestTheIndexPropagatesASourceFailureAndRemembersNothing.
func TestTheIndexPropagatesASourceFailureAndRemembersNothing(t *testing.T) {
	cause := errors.New("down")
	source := &scriptedSource{err: cause}
	index := NewIndex(source)
	id := uuid.MustParse(runtimeCommand)
	if _, _, err := index.PublicCommand(context.Background(), id, 1); !errors.Is(err, cause) {
		t.Fatalf("err = %v", err)
	}
	source.err = nil
	source.pages = [][]Pair{{{Runtime: id, Public: publicCommand}}}
	if got, ok, _ := index.PublicCommand(context.Background(), id, 1); !ok || got != publicCommand {
		t.Fatal("a failed read was remembered as a miss")
	}
}
