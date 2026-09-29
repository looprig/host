package livetool

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func step(kind string, members map[string]any) json.RawMessage {
	body := map[string]any{
		"v": 1, "type": kind, "session_id": "session-a",
		"loop_id": "loop-a", "turn_id": "turn-a", "step_id": "step-a",
		"event_id": "event-a", "created_at": "2026-09-29T00:00:00Z",
		"tool_execution_id": "exec-a", "tool_use_id": "toolu_1", "tool_name": "Bash",
	}
	for key, value := range members {
		if value == nil {
			delete(body, key)
			continue
		}
		body[key] = value
	}
	encoded, _ := json.Marshal(body)
	return encoded
}

func TestParseAcceptsBothToolStepKinds(t *testing.T) {
	started, ok := Parse(step("ToolCallStarted", map[string]any{"summary": "ls -la"}))
	if !ok || started.Type != "ToolCallStarted" || started.ToolUseID != "toolu_1" || started.ToolName != "Bash" || started.Summary != "ls -la" || started.ToolExecutionID != "exec-a" || started.StepID != "step-a" {
		t.Fatalf("Parse(started) = %+v, %v", started, ok)
	}
	completed, ok := Parse(step("ToolCallCompleted", map[string]any{"is_error": true, "elapsed_ms": 12, "result_preview": "boom"}))
	if !ok || !completed.IsError || completed.ElapsedMillis != 12 || completed.ResultPreview != "boom" {
		t.Fatalf("Parse(completed) = %+v, %v", completed, ok)
	}
	if kind, ok := Kind(step("ToolCallCompleted", nil)); !ok || kind != "ToolCallCompleted" {
		t.Fatalf("Kind = %q, %v", kind, ok)
	}
}

func TestParseAcceptsABodyFromAnOlderRuntimeWithoutJoinKey(t *testing.T) {
	if _, ok := Parse(step("ToolCallStarted", map[string]any{"tool_use_id": nil})); !ok {
		t.Fatal("refused a harness <= v0.41 body that carries no tool_use_id")
	}
}

func TestParseRefusesOtherBodies(t *testing.T) {
	textDelta, _ := json.Marshal(map[string]any{"v": 1, "type": "TokenDelta", "session_id": "session-a", "loop_id": "loop-a", "turn_id": "turn-a", "chunk": map[string]any{"chunk_type": "text", "text": "hi"}})
	for name, body := range map[string]json.RawMessage{
		"token delta":            textDelta,
		"other type":             step("PermissionRequested", nil),
		"version 2":              step("ToolCallStarted", map[string]any{"v": 2}),
		"missing loop":           step("ToolCallStarted", map[string]any{"loop_id": nil}),
		"missing turn":           step("ToolCallStarted", map[string]any{"turn_id": nil}),
		"missing tool execution": step("ToolCallCompleted", map[string]any{"tool_execution_id": nil}),
		"not json":               json.RawMessage(`{`),
	} {
		t.Run(name, func(t *testing.T) {
			if got, ok := Parse(body); ok {
				t.Fatalf("Parse accepted %s as %+v", body, got)
			}
			if _, ok := Kind(body); ok {
				t.Fatalf("Kind accepted %s", body)
			}
		})
	}
}

func TestFitLeavesAFittingBodyByteIdentical(t *testing.T) {
	body := step("ToolCallCompleted", map[string]any{"result_preview": "ok"})
	got, ok := Fit(body, 4096)
	if !ok || string(got) != string(body) {
		t.Fatalf("Fit(fitting) = %s, %v; want the input unchanged", got, ok)
	}
}

func TestFitTruncatesResultPreviewBeforeSummary(t *testing.T) {
	body := step("ToolCallCompleted", map[string]any{"summary": "short summary", "result_preview": strings.Repeat("é", 3000)})
	got, ok := Fit(body, 1024)
	if !ok || len(got) > 1024 {
		t.Fatalf("Fit = %d bytes, %v; want <= 1024", len(got), ok)
	}
	parsed, ok := Parse(got)
	if !ok {
		t.Fatalf("Fit produced an unparsable body: %s", got)
	}
	if parsed.Summary != "short summary" {
		t.Fatalf("summary = %q, want untouched while result_preview could absorb the cut", parsed.Summary)
	}
	if !strings.HasSuffix(parsed.ResultPreview, "…") || !utf8.ValidString(parsed.ResultPreview) || parsed.ResultPreview == "…" {
		t.Fatalf("result_preview = %q, want a rune-safe prefix ending in an ellipsis", parsed.ResultPreview)
	}
	if parsed.ToolUseID != "toolu_1" || parsed.ToolExecutionID != "exec-a" || parsed.ToolName != "Bash" {
		t.Fatalf("identities lost by truncation: %+v", parsed)
	}
}

func TestFitTruncatesSummaryWhenResultPreviewIsNotEnough(t *testing.T) {
	body := step("ToolCallStarted", map[string]any{"summary": strings.Repeat("s", 2000)})
	got, ok := Fit(body, 700)
	if !ok || len(got) > 700 {
		t.Fatalf("Fit = %d bytes, %v", len(got), ok)
	}
	parsed, _ := Parse(got)
	if !strings.HasSuffix(parsed.Summary, "…") {
		t.Fatalf("summary = %q, want truncated", parsed.Summary)
	}
}

func TestFitBoundsEscapeHeavyContent(t *testing.T) {
	// Every byte below encodes as a six-byte \u escape, the worst case.
	heavy := strings.Repeat("\x01<>&", 700)
	body := step("ToolCallCompleted", map[string]any{"summary": heavy, "result_preview": heavy})
	for _, limit := range []int{4096, 2048, 600} {
		got, ok := Fit(body, limit)
		if !ok || len(got) > limit {
			t.Fatalf("Fit(limit %d) = %d bytes, %v", limit, len(got), ok)
		}
		if _, ok := Parse(got); !ok {
			t.Fatalf("Fit(limit %d) produced an unparsable body", limit)
		}
	}
}

func TestFitRefusesWhatCannotFit(t *testing.T) {
	if got, ok := Fit(step("ToolCallStarted", map[string]any{"summary": "x"}), 50); ok {
		t.Fatalf("Fit fitted %s into 50 bytes", got)
	}
	if _, ok := Fit(json.RawMessage(`{"v":1,"type":"TokenDelta"}`), 4096); ok {
		t.Fatal("Fit accepted a body that is not a tool step")
	}
}
