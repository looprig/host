package livetext

import (
	"encoding/json"
	"strings"
	"testing"
)

func delta(loop, turn, text string) json.RawMessage {
	body, _ := json.Marshal(Delta{Version: 1, Type: "TokenDelta", LoopID: loop, TurnID: turn, Chunk: struct {
		Type string `json:"chunk_type"`
		Text string `json:"text"`
	}{Type: "text", Text: text}})
	return body
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
