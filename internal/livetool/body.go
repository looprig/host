// Package livetool reads and bounds transient tool-step bodies: harness's
// public projection of ToolCallStarted and ToolCallCompleted.
//
// The bodies carry the tool's redacted audit summary and a capped result
// preview, never raw tool arguments. This package neither widens nor derives
// either; it only shortens them so one frame fits the transport cap.
package livetool

import (
	"encoding/json"
	"unicode/utf8"
)

const (
	// KindStarted is the body type of a tool call that began executing.
	KindStarted = "ToolCallStarted"
	// KindCompleted is the body type of a tool call that finished.
	KindCompleted = "ToolCallCompleted"
)

// ellipsis marks a member Fit shortened.
const ellipsis = "…"

// Step is the correlation and visible content of one public tool-step body.
type Step struct {
	Version         int    `json:"v"`
	Type            string `json:"type"`
	SessionID       string `json:"session_id"`
	LoopID          string `json:"loop_id"`
	TurnID          string `json:"turn_id"`
	StepID          string `json:"step_id"`
	ToolExecutionID string `json:"tool_execution_id"`
	ToolUseID       string `json:"tool_use_id"`
	ToolName        string `json:"tool_name"`
	Summary         string `json:"summary"`
	IsError         bool   `json:"is_error"`
	ElapsedMillis   uint64 `json:"elapsed_ms"`
	ResultPreview   string `json:"result_preview"`
}

// Parse accepts only version-1 ToolCallStarted and ToolCallCompleted bodies
// carrying loop, turn and tool-execution identities. tool_use_id is optional:
// a runtime on harness before v0.42.0 does not send it.
func Parse(body json.RawMessage) (Step, bool) {
	var decoded Step
	if json.Unmarshal(body, &decoded) != nil || decoded.Version != 1 ||
		decoded.Type != KindStarted && decoded.Type != KindCompleted ||
		decoded.LoopID == "" || decoded.TurnID == "" || decoded.ToolExecutionID == "" {
		return Step{}, false
	}
	return decoded, true
}

// Kind reports which tool-step kind body is, when Parse accepts it.
func Kind(body json.RawMessage) (string, bool) {
	decoded, ok := Parse(body)
	return decoded.Type, ok
}

// Fit returns body shortened until its encoding is at most limit bytes. It
// truncates result_preview first and summary second, each at a rune boundary
// with a trailing ellipsis, and leaves every other member intact. A body that
// already fits is returned byte-identical. It refuses a body Parse refuses and
// one that cannot fit even with both members reduced to the ellipsis.
func Fit(body json.RawMessage, limit int) (json.RawMessage, bool) {
	if _, ok := Parse(body); !ok {
		return nil, false
	}
	if len(body) <= limit {
		return body, true
	}
	var members map[string]json.RawMessage
	if json.Unmarshal(body, &members) != nil {
		return nil, false
	}
	for _, name := range []string{"result_preview", "summary"} {
		raw, present := members[name]
		if !present {
			continue
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return nil, false
		}
		cut := len(text)
		for {
			encoded, err := json.Marshal(members)
			if err != nil {
				return nil, false
			}
			if len(encoded) <= limit {
				return encoded, true
			}
			if cut == 0 {
				break
			}
			// Every raw byte removed shortens the encoding by at least one byte,
			// so cutting the overshoot plus the ellipsis converges; escaping only
			// makes each step remove more.
			cut -= len(encoded) - limit + len(ellipsis)
			if cut < 0 {
				cut = 0
			}
			for cut > 0 && !utf8.RuneStart(text[cut]) {
				cut--
			}
			shortened, err := json.Marshal(text[:cut] + ellipsis)
			if err != nil {
				return nil, false
			}
			members[name] = shortened
		}
	}
	return nil, false
}
