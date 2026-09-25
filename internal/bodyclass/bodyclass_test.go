package bodyclass_test

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/bodyclass"
)

func TestPermanentSplitsNeverReadableFromNotYetReadable(t *testing.T) {
	for _, row := range []struct {
		name      string
		body      string
		permanent bool
	}{
		{"future member", `{"version":1,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}],"future":1}`, false},
		{"newer version", `{"version":99,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}]}`, false},
		{"invalid json", `not-json`, true},
		{"not an object", `"x"`, true},
		{"duplicate member", `{"version":1,"version":1,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}]}`, true},
		{"missing member", `{"version":1,"session_id":"s","blocks":[{"type":"text","text":"x"}]}`, true},
		{"null principal", `{"version":1,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}],"principal":null}`, true},
		{"reserved metadata key", `{"version":1,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}],"metadata":{"looprig_x":"v"}}`, true},
		{"future principal member", `{"version":1,"command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}],"principal":{"tenant":"t","subject":"u","kind":"actor","x":1}}`, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			var request sessionwire.InputRequest
			err := json.Unmarshal([]byte(row.body), &request)
			if err == nil {
				t.Fatalf("%s decoded", row.body)
			}
			if got := bodyclass.Permanent(err); got != row.permanent {
				t.Fatalf("Permanent(%v) = %v, want %v", err, got, row.permanent)
			}
		})
	}
}

func TestAnUnclassifiedFailureIsNotPermanent(t *testing.T) {
	if bodyclass.Permanent(errors.New("future failure")) {
		t.Fatal("an unclassified failure must block")
	}
}
