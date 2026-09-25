package inputbody_test

import (
	"errors"
	"testing"

	"github.com/looprig/host/internal/inputbody"
)

func TestCheckReadsStampedInput(t *testing.T) {
	body := []byte(`{"version":1,"command_id":"c-1","session_id":"s-1","blocks":[{"type":"text","text":"x"}],"metadata":{"space":"family"},"principal":{"tenant":"acme","subject":"u","kind":"actor"}}`)
	members, err := inputbody.Check(body, "s-1", "c-1")
	if err != nil || members.Principal == nil || members.Principal.Subject != "u" || members.Metadata["space"] != "family" {
		t.Fatalf("Check = (%+v,%v)", members, err)
	}
}

func TestCheckClassifiesInputFailures(t *testing.T) {
	for _, body := range []string{
		`not-json`,
		`{"version":1,"command_id":"c-1","session_id":"other","blocks":[{"type":"text","text":"x"}]}`,
		`{"version":1,"command_id":"c-1","session_id":"s-1","blocks":[{"type":"text","text":"x"}],"principal":null}`,
	} {
		_, err := inputbody.Check([]byte(body), "s-1", "c-1")
		var malformed *inputbody.MalformedError
		if !errors.As(err, &malformed) {
			t.Fatalf("%s: %v", body, err)
		}
	}
	for _, body := range []string{
		`{"version":99,"command_id":"c-1","session_id":"s-1","blocks":[{"type":"text","text":"x"}]}`,
		`{"version":1,"command_id":"c-1","session_id":"s-1","blocks":[{"type":"text","text":"x"}],"future":true}`,
	} {
		_, err := inputbody.Check([]byte(body), "s-1", "c-1")
		var unsupported *inputbody.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%s: %v", body, err)
		}
	}
}
