// Package livetext reads and joins the narrow transient text body Host emits.
package livetext

import "encoding/json"

// MaxBytes caps one text preview frame before transport encoding.
const MaxBytes = 2048

// Delta is the correlation and text content of one public TokenDelta body.
type Delta struct {
	Version   int    `json:"v"`
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	LoopID    string `json:"loop_id"`
	TurnID    string `json:"turn_id"`
	Chunk     struct {
		Type string `json:"chunk_type"`
		Text string `json:"text"`
	} `json:"chunk"`
}

// Parse accepts only bounded text deltas with loop and turn identities.
func Parse(body json.RawMessage) (Delta, bool) {
	var decoded Delta
	if json.Unmarshal(body, &decoded) != nil || decoded.Version != 1 || decoded.Type != "TokenDelta" || decoded.LoopID == "" || decoded.TurnID == "" || decoded.Chunk.Type != "text" || decoded.Chunk.Text == "" || len(decoded.Chunk.Text) > MaxBytes {
		return Delta{}, false
	}
	return decoded, true
}

// Join replaces the first body's text with the concatenation the caller
// checked against MaxBytes. All other public fields stay present.
func Join(first json.RawMessage, text string) (json.RawMessage, error) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(first, &body); err != nil {
		return nil, err
	}
	chunk, err := json.Marshal(struct {
		ChunkType string `json:"chunk_type"`
		Text      string `json:"text"`
	}{ChunkType: "text", Text: text})
	if err != nil {
		return nil, err
	}
	body["chunk"] = chunk
	return json.Marshal(body)
}

// Merge joins only adjacent text for the same loop and turn within the raw cap.
func Merge(first, next json.RawMessage) (json.RawMessage, bool) {
	a, ok := Parse(first)
	if !ok {
		return nil, false
	}
	b, ok := Parse(next)
	if !ok || a.SessionID != b.SessionID || a.LoopID != b.LoopID || a.TurnID != b.TurnID || len(a.Chunk.Text)+len(b.Chunk.Text) > MaxBytes {
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
