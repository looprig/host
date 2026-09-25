package controlbody_test

import (
	"errors"
	"testing"

	"github.com/looprig/host/internal/controlbody"
)

func TestInterruptAndRestoreReadPrincipal(t *testing.T) {
	body := []byte(`{"version":1,"command_id":"c-1","session_id":"s-1","principal":{"tenant":"acme","subject":"u","kind":"actor"}}`)
	for _, check := range []func([]byte) (bool, error){
		func(b []byte) (bool, error) {
			m, err := controlbody.CheckInterrupt(b, "s-1", "c-1")
			return m.Principal != nil, err
		},
		func(b []byte) (bool, error) {
			m, err := controlbody.CheckRestore(b, "s-1", "c-1")
			return m.Principal != nil, err
		},
	} {
		ok, err := check(body)
		if err != nil || !ok {
			t.Fatalf("Check = (%v,%v)", ok, err)
		}
	}
}

func TestAbsentControlBodyHasNoMembers(t *testing.T) {
	m, err := controlbody.CheckInterrupt(nil, "s-1", "c-1")
	if err != nil || m.Principal != nil {
		t.Fatalf("CheckInterrupt(nil) = (%+v,%v)", m, err)
	}
}

func TestControlBodyWithUnknownMetadataBlocksForNewerHost(t *testing.T) {
	_, err := controlbody.CheckInterrupt([]byte(`{"version":1,"command_id":"c-1","session_id":"s-1","metadata":{"k":"v"}}`), "s-1", "c-1")
	var unsupported *controlbody.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("err = %v", err)
	}
}
