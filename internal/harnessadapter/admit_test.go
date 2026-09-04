package harnessadapter

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/runtimecommand"

	"github.com/looprig/host/department"
)

// The adapter is the department seam it claims to be.
var _ department.Rig = (*Adapter)(nil)

func inputCommand() department.RuntimeCommand {
	return department.RuntimeCommand{
		CommandID:        sessionwire.CommandID("command-a"),
		RuntimeCommandID: uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		Kind:             "input",
		Payload:          []byte("hello"),
	}
}

// H5. runtimecommand.Admitted refuses a zero lease epoch and
// department.RuntimeCommand carries none, so a session bound without one cannot
// apply anything. The fake applier accepts every command and asserts on the
// fields it was given; it has no epoch to be missing.
func TestAdmitRefusesWithoutABoundLeaseEpoch(t *testing.T) {
	bound := &boundSession{decode: func([]byte) ([]content.Block, error) { return nil, nil }}
	_, err := bound.admit(inputCommand())
	var unsupported *UnsupportedCommandError
	if !errors.As(err, &unsupported) {
		t.Fatalf("admit with no epoch = %v, want UnsupportedCommandError", err)
	}
	if !strings.Contains(unsupported.Reason, "lease epoch") {
		t.Fatalf("reason = %q, want the missing lease epoch", unsupported.Reason)
	}
}

// H6. commands.Kind has five members and runtimecommand.Kind has two. The three
// with no counterpart are refused HERE, before the durable prefix is written,
// rather than by Admitted.Validate after it.
func TestAdmitRefusesEveryKindHarnessDoesNotApply(t *testing.T) {
	// THE SPACE IS DERIVED FROM HOST'S OWN VOCABULARY, not picked: every kind
	// commands declares is admitted here and classified by whether
	// runtimecommand.Kind names it. A kind added to either side lands in the
	// right arm without an edit.
	for _, kind := range []string{"create", "restore", "input", "interrupt", "gate_response"} {
		t.Run(kind, func(t *testing.T) {
			bound := &boundSession{
				leaseEpoch: 3,
				decode: func([]byte) ([]content.Block, error) {
					return []content.Block{&content.TextBlock{Text: "hello"}}, nil
				},
			}
			command := inputCommand()
			command.Kind = kind
			admitted, err := bound.admit(command)

			if runtimecommand.Kind(kind).Valid() {
				if err != nil {
					t.Fatalf("admit(%q) = %v, want it accepted", kind, err)
				}
				if string(admitted.Kind) != kind {
					t.Fatalf("kind = %q, want %q", admitted.Kind, kind)
				}
				return
			}
			var unsupported *UnsupportedCommandError
			if !errors.As(err, &unsupported) {
				t.Fatalf("admit(%q) = %v, want UnsupportedCommandError", kind, err)
			}
			if !strings.Contains(unsupported.Reason, "input and interrupt") {
				t.Fatalf("reason = %q, want the two-kind refusal", unsupported.Reason)
			}
		})
	}
}

// H7. An input command must carry decoded content.Blocks and Host's payload is
// opaque bytes, so a binding with no decoder refuses rather than applying an
// empty turn.
func TestAdmitRefusesAnInputWithNoDecoder(t *testing.T) {
	bound := &boundSession{leaseEpoch: 3}
	_, err := bound.admit(inputCommand())
	var unsupported *UnsupportedCommandError
	if !errors.As(err, &unsupported) {
		t.Fatalf("admit with no decoder = %v, want UnsupportedCommandError", err)
	}
	if !strings.Contains(unsupported.Reason, "block decoder") {
		t.Fatalf("reason = %q, want the missing decoder", unsupported.Reason)
	}
}

// AN EMPTY DECODE IS REFUSED BY HARNESS ITSELF, which is why the adapter hands
// the result to Admitted.Validate rather than deciding for it. A decoder that
// returns nothing produces an input command with no content, and that is the one
// case runtimecommand names in its own validator.
func TestAdmitRefusesAnInputThatDecodesToNothing(t *testing.T) {
	bound := &boundSession{
		leaseEpoch: 3,
		decode:     func([]byte) ([]content.Block, error) { return nil, nil },
	}
	_, err := bound.admit(inputCommand())
	var invalid *runtimecommand.ValidationError
	if !errors.As(err, &invalid) || invalid.Field != "Blocks" {
		t.Fatalf("admit of an empty input = %v, want a Blocks validation error", err)
	}
}

// H8. RuntimeCommand.PayloadRef exists so a large private body is dereferenced
// by the runtime instead of travelling through Host's memory. Admitted has no
// reference member and no object reader, so a referenced payload cannot cross.
// The fake accepts one and records it.
func TestAdmitRefusesAnObjectReferencedPayload(t *testing.T) {
	bound := &boundSession{
		leaseEpoch: 3,
		decode: func([]byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: "hello"}}, nil
		},
	}
	command := inputCommand()
	command.Payload = nil
	command.PayloadRef = sessionwire.ObjectReference{ObjectID: "01hzzzzzzzzzzzzzzzzzzzzzzz"}
	_, err := bound.admit(command)
	var unsupported *UnsupportedCommandError
	if !errors.As(err, &unsupported) {
		t.Fatalf("admit of a referenced payload = %v, want UnsupportedCommandError", err)
	}
	if !strings.Contains(unsupported.Reason, "object-reference") {
		t.Fatalf("reason = %q, want the object-reference refusal", unsupported.Reason)
	}
}

// An interrupt carries no payload and needs no decoder, so it is the one Host
// kind that crosses unchanged.
func TestAdmitCarriesAnInterruptUnchanged(t *testing.T) {
	bound := &boundSession{leaseEpoch: 9}
	command := inputCommand()
	command.Kind = "interrupt"
	command.Payload = nil

	admitted, err := bound.admit(command)
	if err != nil {
		t.Fatalf("admit an interrupt: %v", err)
	}
	if admitted.LeaseEpoch != 9 {
		t.Fatalf("lease epoch = %d, want the bound 9", admitted.LeaseEpoch)
	}
	if admitted.RuntimeCommandID != command.RuntimeCommandID {
		t.Fatalf("runtime command id = %v, want %v", admitted.RuntimeCommandID, command.RuntimeCommandID)
	}
	if len(admitted.Blocks) != 0 {
		t.Fatalf("an interrupt carried %d blocks", len(admitted.Blocks))
	}
}

func TestNewRefusesNoResolver(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrNoRigs) {
		t.Fatalf("New(nil) = %v, want ErrNoRigs", err)
	}
}
