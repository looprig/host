package livetext

import (
	"encoding/json"
	"strings"
	"testing"
)

func delta(loop, turn, text string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{"v": 1, "type": "TokenDelta", "session_id": "", "loop_id": loop, "turn_id": turn, "chunk": map[string]any{"chunk_type": "text", "text": text}})
	return body
}

func thinkingDelta(loop, turn, thinking string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{"v": 1, "type": "TokenDelta", "session_id": "session-a", "loop_id": loop, "turn_id": turn, "chunk": map[string]any{"chunk_type": "thinking", "thinking": thinking}})
	return body
}

func TestThinkingDeltasParseAndMergeWithinTheirOwnKind(t *testing.T) {
	first := thinkingDelta("loop-a", "turn-a", "reason ")
	second := thinkingDelta("loop-a", "turn-a", "continues")
	if got, ok := Parse(first); !ok || got.Chunk.Type != "thinking" {
		t.Fatalf("Parse(thinking) = %+v, %v", got, ok)
	}
	merged, ok := Merge(first, second)
	if !ok || !strings.Contains(string(merged), `"thinking":"reason continues"`) {
		t.Fatalf("Merge(thinking) = %s, %v", merged, ok)
	}
	if _, ok := Merge(first, delta("loop-a", "turn-a", "answer")); ok {
		t.Fatal("merged reasoning and text")
	}
}

func TestThinkingDeltasKeepTheTwoKiBCap(t *testing.T) {
	if _, ok := Parse(thinkingDelta("loop-a", "turn-a", strings.Repeat("x", 2049))); ok {
		t.Fatal("accepted oversized reasoning")
	}
	if _, ok := Merge(thinkingDelta("loop-a", "turn-a", strings.Repeat("x", 2048)), thinkingDelta("loop-a", "turn-a", "y")); ok {
		t.Fatal("merged reasoning beyond cap")
	}
}

func TestTextMergeKeepsV012Bytes(t *testing.T) {
	merged, ok := Merge(delta("loop-a", "turn-a", "hello "), delta("loop-a", "turn-a", "world"))
	const released = `{"chunk":{"chunk_type":"text","text":"hello world"},"loop_id":"loop-a","session_id":"","turn_id":"turn-a","type":"TokenDelta","v":1}`
	if !ok || string(merged) != released {
		t.Fatalf("text merge = %s, %v; want v0.12.0 bytes %s", merged, ok, released)
	}
}

func TestMergeRejectsAnotherLoopOrTurn(t *testing.T) {
	first := delta("loop-a", "turn-a", "one")
	for _, second := range []json.RawMessage{delta("loop-b", "turn-a", "two"), delta("loop-a", "turn-b", "two")} {
		if _, ok := Merge(first, second); ok {
			t.Fatalf("merged distinct key: %s", second)
		}
	}
}

func TestMergeCapsRawTextAtTwoKiB(t *testing.T) {
	if _, ok := Parse(delta("loop-a", "turn-a", strings.Repeat("x", 2049))); ok {
		t.Fatal("accepted oversized text")
	}
	if _, ok := Merge(delta("loop-a", "turn-a", strings.Repeat("x", 2048)), delta("loop-a", "turn-a", "y")); ok {
		t.Fatal("merged beyond cap")
	}
}
