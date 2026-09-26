// Package livetext reads and joins bounded transient text and reasoning bodies.
package livetext

import (
	"encoding/json"
	"errors"
)

// MaxBytes caps one preview frame before transport encoding.
const MaxBytes = 2048

// Delta is the correlation and visible content of one public TokenDelta body.
type Delta struct {
	Version   int    `json:"v"`
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	LoopID    string `json:"loop_id"`
	TurnID    string `json:"turn_id"`
	Chunk     struct {
		Type     string `json:"chunk_type"`
		Text     string `json:"text"`
		Thinking string `json:"thinking"`
	} `json:"chunk"`
}

// Parse accepts only bounded text or reasoning deltas with loop and turn identities.
func Parse(body json.RawMessage) (Delta, bool) {
	var decoded Delta
	if json.Unmarshal(body, &decoded) != nil || decoded.Version != 1 || decoded.Type != "TokenDelta" || decoded.LoopID == "" || decoded.TurnID == "" || decoded.Chunk.Type != "text" && decoded.Chunk.Type != "thinking" {
		return Delta{}, false
	}
	if decoded.Chunk.Type == "thinking" {
		decoded.Chunk.Text = decoded.Chunk.Thinking
	}
	if decoded.Chunk.Text == "" || len(decoded.Chunk.Text) > MaxBytes {
		return Delta{}, false
	}
	return decoded, true
}

// Join replaces the first body's text with the concatenation the caller
// checked against MaxBytes. All other public fields stay present.
func Join(first json.RawMessage, text string) (json.RawMessage, error) {
	decoded, ok := Parse(first)
	if !ok {
		return nil, errors.New("livetext: unsupported live delta")
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(first, &body); err != nil {
		return nil, err
	}
	var chunk []byte
	var err error
	if decoded.Chunk.Type == "thinking" {
		chunk, err = json.Marshal(struct {
			ChunkType string `json:"chunk_type"`
			Thinking  string `json:"thinking"`
		}{ChunkType: "thinking", Thinking: text})
	} else {
		chunk, err = json.Marshal(struct {
			ChunkType string `json:"chunk_type"`
			Text      string `json:"text"`
		}{ChunkType: "text", Text: text})
	}
	if err != nil {
		return nil, err
	}
	body["chunk"] = chunk
	return json.Marshal(body)
}

// Merge joins only adjacent chunks of the same kind, loop and turn within the raw cap.
func Merge(first, next json.RawMessage) (json.RawMessage, bool) {
	a, ok := Parse(first)
	if !ok {
		return nil, false
	}
	b, ok := Parse(next)
	if !ok || a.SessionID != b.SessionID || a.LoopID != b.LoopID || a.TurnID != b.TurnID || a.Chunk.Type != b.Chunk.Type || len(a.Chunk.Text)+len(b.Chunk.Text) > MaxBytes {
		return nil, false
	}
	joined, err := Join(first, a.Chunk.Text+b.Chunk.Text)
	return joined, err == nil
}

// BoundaryLoop names the loop whose gapped previews a committed boundary clears.
// A boundary without a loop identifier clears all preview gaps.
func BoundaryLoop(body json.RawMessage) (string, bool) {
	var boundary struct {
		Type   string `json:"type"`
		LoopID string `json:"loop_id"`
	}
	if json.Unmarshal(body, &boundary) != nil {
		return "", false
	}
	switch boundary.Type {
	case "StepDone", "TurnDone", "TurnFailed", "TurnInterrupted":
		return boundary.LoopID, true
	}
	return "", false
}
