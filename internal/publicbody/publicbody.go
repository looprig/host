// Package publicbody projects a runtime's committed public event body onto the
// identities a client may hold (finding W1).
//
// A harness public body names the RUNTIME session on every event
// (`session_id`, and again under `cause`) and, on an event a command caused,
// the RUNTIME command id (`cause.command_id`). Both are private: the runtime
// session id is the durable binding's, which Factory never shows a client, and
// the runtime command id is the once-allocated mapping Factory's store keeps
// beside the command a client admitted. Host relays these bodies to Factory,
// and Factory serves them to browsers on the live tail and from /journal, so a
// client that watched a session held both.
//
// THE PROJECTION IS ONE FUNCTION APPLIED ON BOTH PATHS. Host applies it to the
// live tail, and a composition applies it to /journal through the reader host
// exports; a consumer joining its durable read to the live stream dedupes on
// (sequence, event id) and may compare bodies, so the two must produce the same
// bytes for the same event. They do because they are this code, fed the same
// identities: the public session id, the runtime session id, and the immutable
// public-to-runtime command mapping, which the live path reads from the
// session's disposition inbox and the journal path from the runtime journal's
// own application prefixes — two copies of one admission record.
//
// WHAT IT CHANGES, and nothing else:
//
//   - every occurrence of the runtime session id — any `session_id` value at any
//     depth, and any mention inside a string (an error message, a path) —
//     becomes the public session id;
//   - the top-level `cause.command_id` becomes the public command id the
//     client admitted, and so does every other occurrence of that runtime
//     command id; or, when the runtime command has no public identity (a
//     machine-originated cause: a subagent hand-back, a compaction waiter),
//     it is REMOVED from `cause`, and a `cause` left empty by that removal is
//     removed too, which is how the runtime encodes a zero cause.
//
// Every other byte is carried: members keep their order, and a body naming
// neither identity is returned as the same bytes. Loop, turn and step ids are
// the runtime's own content identities with no private record behind them,
// and they stay.
//
// PROJECTED BODIES ARE NOT HARNESS EVENTS ANY MORE, and no consumer should
// decode them with harness: a public command id need not be a UUID, and a
// Reply whose cause was a machine command loses the cause harness requires.
package publicbody

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// Commands resolves a runtime command id to the public command id a client
// admitted. ok is false when the runtime command has no public identity.
//
// bound is the journal sequence of the event naming the command: a mapping
// that exists is durable at or below it, so a resolver that reads the journal
// need read no further.
type Commands interface {
	PublicCommand(ctx context.Context, runtime uuid.UUID, bound uint64) (sessionwire.CommandID, bool, error)
}

// Identities are what one session's bodies are projected with.
type Identities struct {
	// RuntimeSessionID is the runtime's own session id, the value to replace.
	RuntimeSessionID uuid.UUID

	// SessionID is the public (Core) session id, the value it becomes.
	SessionID sessionwire.SessionID

	// Commands maps runtime command ids. Nil maps none: every
	// cause.command_id is then removed.
	Commands Commands
}

// MappingError reports a command mapping that could not be READ — a store or
// journal failure, not a finding about the body. It is the one projection
// failure a caller may retry: the same body projects once the read succeeds.
type MappingError struct{ Cause error }

func (e *MappingError) Error() string {
	return "publicbody: read the command mapping: " + e.Cause.Error()
}
func (e *MappingError) Unwrap() error { return e.Cause }

// ErrNonCanonical reports a body this package would not hand on: one that is
// not canonical JSON on the way in, or whose projection would not be.
var ErrNonCanonical = errors.New("publicbody: the public body is not canonical JSON")

// Project returns body with its private identities replaced. seq is the
// event's journal sequence (see Commands).
//
// IT IS LINEAR IN THE BODY, WHATEVER ITS NESTING (review S1). A body can carry
// model-controlled JSON — a tool call's input is embedded raw — so a projection
// that re-tokenised every subtree at every level cost depth × size (7.4 s for
// depth 1000 around a 1 MiB leaf). It now makes a fixed number of passes:
//
//   - THE RUNTIME SESSION ID IS REPLACED AS BYTES, everywhere. A UUID is ASCII
//     hex and hyphens, which JSON never escapes, so its encoding occurs only
//     inside string tokens, literally; replacing it there rewrites every
//     `session_id` value at any depth AND every free-text mention (review S2:
//     a TurnFailed or RestoreErrored message, a capture-spill path) with one
//     rule, and cannot produce invalid JSON because the replacement is the
//     public id's own string encoding;
//   - the TOP-LEVEL object alone is split into members, once, to find the
//     header's `cause`. A mapped runtime command id is then replaced as bytes
//     too, everywhere (it is as private as the session id); an unmapped one
//     is a machine id, is removed from `cause` only, and an emptied `cause`
//     goes with it.
func Project(ctx context.Context, body json.RawMessage, ids Identities, seq uint64) (json.RawMessage, error) {
	if ids.RuntimeSessionID.IsZero() {
		return nil, errors.New("publicbody: no runtime session id to project")
	}
	if err := ids.SessionID.Validate(); err != nil {
		return nil, fmt.Errorf("publicbody: the public session id is invalid: %w", err)
	}
	out := replaceAll(body, ids.RuntimeSessionID.String(), string(ids.SessionID))
	out, err := projectCause(ctx, out, ids, seq)
	if err != nil {
		return nil, err
	}
	// ONE CHECK, ON THE WAY OUT. Unchanged bytes are the input's, and changed
	// ones are the input's fragments with string contents replaced, so a
	// non-canonical input is refused here either way.
	if !canonical(out) {
		return nil, ErrNonCanonical
	}
	return out, nil
}

// Projection binds Project to one session's identities, in the shape the live
// tail's projector takes.
func Projection(ids Identities) func(context.Context, json.RawMessage, uint64) (json.RawMessage, error) {
	return func(ctx context.Context, body json.RawMessage, seq uint64) (json.RawMessage, error) {
		return Project(ctx, body, ids, seq)
	}
}

// replaceAll replaces every occurrence of a UUID's text with a string's JSON
// content encoding. The input is returned as the same bytes when it does not
// name the UUID.
//
// A MATCH THAT STARTS INSIDE A `\uXXXX` ESCAPE'S FOUR HEX DIGITS IS SKIPPED
// (review R-S1). Canonical JSON never escapes a UUID's own hex digits or
// hyphens, so an occurrence of the id's text is ordinarily always a literal
// run of bytes safe to replace whole. But when the id happens to start with
// the same four hex digits Go chose to `\u`-escape some other character
// (`<`, `>`, `&`, U+2028/2029, or a control character), those four bytes are
// the escape's payload, not literal text: replacing them would consume part
// of the escape and leave `\u` followed by literal characters, which is not
// valid JSON. Skipping that one match leaves the escape, and the bytes after
// it, exactly as they were; an id occurring anywhere else in the body -
// including right after a complete, non-overlapping escape - is replaced as
// usual.
func replaceAll(body json.RawMessage, uuidText, replacement string) json.RawMessage {
	old := []byte(uuidText)
	if !bytes.Contains(body, old) {
		return body
	}
	quoted := mustString(replacement)
	newBytes := quoted[1 : len(quoted)-1]

	var out bytes.Buffer
	rest := body
	base := 0
	for {
		idx := bytes.Index(rest, old)
		if idx < 0 {
			out.Write(rest)
			break
		}
		matchStart := base + idx
		if overlapsUnicodeEscape(body, matchStart) {
			// Keep this one byte literally and resume the search right after
			// it: the match starting here is not a real occurrence of the id.
			out.Write(rest[:idx+1])
			rest = rest[idx+1:]
			base = matchStart + 1
			continue
		}
		out.Write(rest[:idx])
		out.Write(newBytes)
		rest = rest[idx+len(old):]
		base = matchStart + len(old)
	}
	return out.Bytes()
}

// overlapsUnicodeEscape reports whether a match starting at matchStart begins
// inside a `\uXXXX` escape's four hex digits: within the 4 bytes before
// matchStart there is a `u` that is itself an unescaped escape lead (an odd
// run of backslashes immediately before it).
func overlapsUnicodeEscape(body []byte, matchStart int) bool {
	for offset := 1; offset <= 4; offset++ {
		u := matchStart - offset
		if u < 0 {
			break
		}
		if body[u] == 'u' && isEscapeLead(body, u) {
			return true
		}
	}
	return false
}

// isEscapeLead reports whether the byte at u (a 'u') is the escape character
// of a `\u` sequence rather than a literal 'u' following an escaped literal
// backslash (`\\u`, two content bytes: a backslash, then 'u').
func isEscapeLead(body []byte, u int) bool {
	if u == 0 || body[u-1] != '\\' {
		return false
	}
	count := 0
	for j := u - 1; j >= 0 && body[j] == '\\'; j-- {
		count++
	}
	return count%2 == 1
}

// projectCause maps or removes the header cause's command id.
func projectCause(ctx context.Context, body json.RawMessage, ids Identities, seq uint64) (json.RawMessage, error) {
	if firstByte(body) != '{' || !bytes.Contains(body, []byte(`"command_id"`)) {
		return body, nil
	}
	members, err := objectMembers(body)
	if err != nil {
		return nil, err
	}
	at := -1
	for i, m := range members {
		if m.key == "cause" && firstByte(m.value) == '{' {
			at = i
		}
	}
	if at < 0 {
		return body, nil
	}
	cause, err := objectMembers(members[at].value)
	if err != nil {
		return nil, err
	}
	for i, m := range cause {
		if m.key != "command_id" {
			continue
		}
		runtime, public, mapped, err := resolveCommand(ctx, m.value, ids, seq)
		if err != nil {
			return nil, err
		}
		if mapped {
			return replaceAll(body, runtime.String(), string(public)), nil
		}
		cause = append(cause[:i:i], cause[i+1:]...)
		if len(cause) == 0 {
			members = append(members[:at:at], members[at+1:]...)
		} else {
			members[at].value = encodeObject(cause)
		}
		return encodeObject(members), nil
	}
	return body, nil
}

// resolveCommand resolves one cause.command_id. A value that is not a runtime
// command id at all has no public identity either.
func resolveCommand(ctx context.Context, raw json.RawMessage, ids Identities, seq uint64) (uuid.UUID, sessionwire.CommandID, bool, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return uuid.UUID{}, "", false, nil
	}
	runtime, err := uuid.Parse(text)
	if err != nil || runtime.IsZero() || ids.Commands == nil {
		return uuid.UUID{}, "", false, nil
	}
	public, ok, err := ids.Commands.PublicCommand(ctx, runtime, seq)
	if err != nil {
		return uuid.UUID{}, "", false, &MappingError{Cause: err}
	}
	return runtime, public, ok, nil
}

// member is one object member with its key's ORIGINAL bytes, so re-encoding an
// object reproduces every key exactly as it was written.
type member struct {
	key    string
	rawKey []byte
	value  json.RawMessage
}

// objectMembers splits a compact object into its members, in order.
func objectMembers(raw json.RawMessage) ([]member, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, ErrNonCanonical
	}
	var members []member
	for decoder.More() {
		start := decoder.InputOffset()
		token, err := decoder.Token()
		if err != nil {
			return nil, ErrNonCanonical
		}
		key, ok := token.(string)
		if !ok {
			return nil, ErrNonCanonical
		}
		rawKey := bytes.TrimLeft(raw[start:decoder.InputOffset()], ", \t\r\n")
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, ErrNonCanonical
		}
		members = append(members, member{key: key, rawKey: rawKey, value: value})
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrNonCanonical
	}
	return members, nil
}

func encodeObject(members []member) json.RawMessage {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(m.rawKey)
		out.WriteByte(':')
		out.Write(m.value)
	}
	out.WriteByte('}')
	return out.Bytes()
}

func mustString(text string) json.RawMessage {
	encoded, _ := json.Marshal(text)
	return encoded
}

func firstByte(raw json.RawMessage) byte {
	if len(raw) == 0 {
		return 0
	}
	return raw[0]
}

// canonical is Core's transport-canonical rule for a public body: valid JSON
// that json.Marshal reproduces byte for byte (compact, HTML-escaped).
func canonical(body json.RawMessage) bool {
	if len(body) == 0 || !json.Valid(body) {
		return false
	}
	encoded, err := json.Marshal(body)
	return err == nil && bytes.Equal(encoded, body)
}
