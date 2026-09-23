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
//   - every object member named `session_id`, at any depth, whose value is the
//     runtime session id becomes the public session id;
//   - the top-level `cause.command_id` becomes the public command id the
//     client admitted, or is REMOVED when the runtime command has no public
//     identity (a machine-originated cause: a subagent hand-back, a compaction
//     waiter); a `cause` left empty by that removal is removed too, which is
//     how the runtime encodes a zero cause.
//
// Every other byte is carried: members keep their order, untouched values keep
// their exact encoding, and an object nothing changed in is not re-encoded at
// all. Loop, turn and step ids are the runtime's own content identities with no
// private record behind them, and they stay.
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

func (e *MappingError) Error() string { return "publicbody: read the command mapping: " + e.Cause.Error() }
func (e *MappingError) Unwrap() error { return e.Cause }

// ErrNonCanonical reports a body this package would not hand on: one that is
// not canonical JSON on the way in, or whose projection would not be.
var ErrNonCanonical = errors.New("publicbody: the public body is not canonical JSON")

// Project returns body with its private identities replaced. seq is the
// event's journal sequence (see Commands).
func Project(ctx context.Context, body json.RawMessage, ids Identities, seq uint64) (json.RawMessage, error) {
	if ids.RuntimeSessionID.IsZero() {
		return nil, errors.New("publicbody: no runtime session id to project")
	}
	if err := ids.SessionID.Validate(); err != nil {
		return nil, fmt.Errorf("publicbody: the public session id is invalid: %w", err)
	}
	// ONE CHECK, ON THE WAY OUT. A body nothing changed is returned as the
	// same bytes, and a changed one is rebuilt from the original fragments, so
	// a non-canonical input is refused here either way.
	p := projector{ctx: ctx, ids: ids, runtime: ids.RuntimeSessionID.String(), seq: seq}
	out, _, err := p.value(body, true)
	if err != nil {
		return nil, err
	}
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

// projector is one projection in progress.
type projector struct {
	ctx     context.Context
	ids     Identities
	runtime string
	seq     uint64
}

// value projects one JSON value, reporting whether it changed. An unchanged
// value is returned as the SAME bytes it arrived as.
func (p projector) value(raw json.RawMessage, top bool) (json.RawMessage, bool, error) {
	switch firstByte(raw) {
	case '{':
		return p.projectObject(raw, top)
	case '[':
		return p.projectArray(raw)
	default:
		return raw, false, nil
	}
}

// projectObject projects one object; top marks the event's own header level,
// the only level whose `cause` is the command cause.
func (p projector) projectObject(raw json.RawMessage, top bool) (json.RawMessage, bool, error) {
	members, err := objectMembers(raw)
	if err != nil {
		return nil, false, err
	}
	changed := false
	kept := members[:0:0]
	for _, m := range members {
		switch {
		case m.key == "session_id" && isString(m.value, p.runtime):
			m.value = mustString(string(p.ids.SessionID))
			changed = true
		case top && m.key == "cause" && firstByte(m.value) == '{':
			cause, drop, causeChanged, err := p.cause(m.value)
			if err != nil {
				return nil, false, err
			}
			if drop {
				changed = true
				continue
			}
			if causeChanged {
				m.value, changed = cause, true
			}
		default:
			projected, valueChanged, err := p.value(m.value, false)
			if err != nil {
				return nil, false, err
			}
			if valueChanged {
				m.value, changed = projected, true
			}
		}
		kept = append(kept, m)
	}
	if !changed {
		return raw, false, nil
	}
	return encodeObject(kept), true, nil
}

// cause projects the top-level cause, reporting whether it is now empty and
// must be dropped.
func (p projector) cause(raw json.RawMessage) (json.RawMessage, bool, bool, error) {
	members, err := objectMembers(raw)
	if err != nil {
		return nil, false, false, err
	}
	changed := false
	kept := members[:0:0]
	for _, m := range members {
		switch {
		case m.key == "session_id" && isString(m.value, p.runtime):
			m.value, changed = mustString(string(p.ids.SessionID)), true
		case m.key == "command_id":
			public, ok, err := p.command(m.value)
			if err != nil {
				return nil, false, false, err
			}
			changed = true
			if !ok {
				continue
			}
			m.value = mustString(string(public))
		default:
			projected, valueChanged, err := p.value(m.value, false)
			if err != nil {
				return nil, false, false, err
			}
			if valueChanged {
				m.value, changed = projected, true
			}
		}
		kept = append(kept, m)
	}
	if !changed {
		return raw, false, false, nil
	}
	if len(kept) == 0 {
		return nil, true, true, nil
	}
	return encodeObject(kept), false, true, nil
}

// command resolves one cause.command_id. A value that is not a runtime command
// id at all has no public identity either, and is removed rather than carried.
func (p projector) command(raw json.RawMessage) (sessionwire.CommandID, bool, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", false, nil
	}
	runtime, err := uuid.Parse(text)
	if err != nil || runtime.IsZero() || p.ids.Commands == nil {
		return "", false, nil
	}
	public, ok, err := p.ids.Commands.PublicCommand(p.ctx, runtime, p.seq)
	if err != nil {
		return "", false, &MappingError{Cause: err}
	}
	return public, ok, nil
}

// projectArray projects each element of one array.
func (p projector) projectArray(raw json.RawMessage) (json.RawMessage, bool, error) {
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return nil, false, ErrNonCanonical
	}
	changed := false
	for i, element := range elements {
		projected, elementChanged, err := p.value(element, false)
		if err != nil {
			return nil, false, err
		}
		if elementChanged {
			elements[i], changed = projected, true
		}
	}
	if !changed {
		return raw, false, nil
	}
	var out bytes.Buffer
	out.WriteByte('[')
	for i, element := range elements {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(element)
	}
	out.WriteByte(']')
	return out.Bytes(), true, nil
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

func isString(raw json.RawMessage, want string) bool {
	if firstByte(raw) != '"' {
		return false
	}
	var text string
	return json.Unmarshal(raw, &text) == nil && text == want
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
