package publicbody

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// nestedBody is review S1's shape: model-controlled tool input nested depth
// deep around a large leaf, inside an event naming the runtime session.
func nestedBody(depth, leaf int) json.RawMessage {
	var b strings.Builder
	b.WriteString(`{"session_id":"` + runtimeSession + `","input":`)
	for range depth {
		b.WriteString(`{"a":[`)
	}
	b.WriteString(`"` + strings.Repeat("x", leaf) + `"`)
	for range depth {
		b.WriteString(`]}`)
	}
	b.WriteString(`,"type":"StepDone"}`)
	return json.RawMessage(b.String())
}

func benchmarkNested(b *testing.B, depth int) {
	body := nestedBody(depth, 1<<20)
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		if _, err := Project(context.Background(), body, ids(nil), 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProjectDepth10(b *testing.B)   { benchmarkNested(b, 10) }
func BenchmarkProjectDepth100(b *testing.B)  { benchmarkNested(b, 100) }
func BenchmarkProjectDepth1000(b *testing.B) { benchmarkNested(b, 1000) }

// BenchmarkProjectCausedDepth4000 takes the cause path, which splits the top
// level, near Go's nesting limit (each level is an object and an array).
func BenchmarkProjectCausedDepth4000(b *testing.B) {
	body := json.RawMessage(`{"cause":{"command_id":"` + machineCommand + `"},` + string(nestedBody(4000, 1<<20))[1:])
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		if _, err := Project(context.Background(), body, ids(mapCommands{}), 1); err != nil {
			b.Fatal(err)
		}
	}
}
