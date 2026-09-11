package harnessadapter

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"

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

// stubLeaseEpochReporter is a session.LeaseEpochReporter whose answer a test
// chooses, including the legitimate "this session holds no lease that reports an
// epoch" answer harness's two-result form exists to express.
type stubLeaseEpochReporter struct {
	epoch uint64
	held  bool
}

func (r stubLeaseEpochReporter) LeaseEpoch() (uint64, bool) { return r.epoch, r.held }

// heldEpoch is a reporter that holds the given grant.
func heldEpoch(epoch uint64) session.LeaseEpochReporter {
	return stubLeaseEpochReporter{epoch: epoch, held: true}
}

// H5 AS REWRITTEN BY O3.4. The refusal is no longer "nothing was BOUND" — there is
// nothing to bind. runtimecommand.Admitted refuses a zero lease epoch and
// department.RuntimeCommand carries none, so the epoch is read from the RUNTIME's
// own grant at admission time, and a runtime that holds no such grant has no epoch
// an admitted command could name.
//
// held IS WHAT IS BRANCHED ON AND NOT THE NUMBER. The second row is the control
// that makes that a mechanism rather than a spelling: a reporter holding a grant
// whose epoch is 0 would be admitted by a check reading only the number, and
// harness would then refuse it at Validate — so the two rows must part company.
func TestAdmitRefusesARuntimeThatHoldsNoJournalGrant(t *testing.T) {
	for _, row := range []struct {
		name     string
		reporter session.LeaseEpochReporter
		refused  bool
	}{
		{name: "no grant at all", reporter: stubLeaseEpochReporter{}, refused: true},
		{name: "a released grant still reporting its old number", reporter: stubLeaseEpochReporter{epoch: 7}, refused: true},
		{name: "control: a held grant", reporter: heldEpoch(7), refused: false},
	} {
		t.Run(row.name, func(t *testing.T) {
			bound := &boundSession{
				leaseEpoch: row.reporter,
				decode: func([]byte) ([]content.Block, error) {
					return []content.Block{&content.TextBlock{Text: "hello"}}, nil
				},
			}
			admitted, err := bound.admit(inputCommand())
			if !row.refused {
				if err != nil {
					t.Fatalf("admit under a held grant: %v", err)
				}
				if admitted.LeaseEpoch != 7 {
					t.Fatalf("lease epoch = %d, want the runtime's 7", admitted.LeaseEpoch)
				}
				return
			}
			var unsupported *UnsupportedCommandError
			if !errors.As(err, &unsupported) {
				t.Fatalf("admit with no held grant = %v, want UnsupportedCommandError", err)
			}
			if !strings.Contains(unsupported.Reason, "journal lease") {
				t.Fatalf("reason = %q, want the missing journal lease", unsupported.Reason)
			}
		})
	}
}

// THE EPOCH IS THE RUNTIME'S AND IS RE-READ PER COMMAND, which is the whole of
// O3.4's first live consequence. harness compares an admitted command's LeaseEpoch
// for EQUALITY against the lease the session itself holds
// (internal/sessionruntime/runtime_command.go:140), so a number Host chose — and
// the only lease Host holds is its residency grant — is right exactly when two
// independent counters happen to agree.
//
// THE SECOND ADMISSION IS THE ASSERTION, not the first. A binding that cached the
// epoch it was constructed with passes the first row and fails the second, which is
// the difference between reading the capability and remembering it.
func TestAdmitReadsTheEpochFromTheRuntimeEveryTime(t *testing.T) {
	reporter := &movingReporter{epoch: 4, held: true}
	bound := &boundSession{
		leaseEpoch: reporter,
		decode: func([]byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: "hello"}}, nil
		},
	}
	first, err := bound.admit(inputCommand())
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if first.LeaseEpoch != 4 {
		t.Fatalf("first lease epoch = %d, want the runtime's 4", first.LeaseEpoch)
	}
	reporter.moveTo(11, true)
	second, err := bound.admit(inputCommand())
	if err != nil {
		t.Fatalf("second admit: %v", err)
	}
	if second.LeaseEpoch != 11 {
		t.Fatalf("second lease epoch = %d, want the runtime's new 11", second.LeaseEpoch)
	}
}

// movingReporter is a grant whose epoch a test moves under a live binding.
type movingReporter struct {
	mu    sync.Mutex
	epoch uint64
	held  bool
}

func (r *movingReporter) LeaseEpoch() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch, r.held
}

func (r *movingReporter) moveTo(epoch uint64, held bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch, r.held = epoch, held
}

// H6. commands.Kind has five members and runtimecommand.Kind has two. The three
// with no counterpart are refused HERE, before the durable prefix is written,
// rather than by Admitted.Validate after it.
func TestAdmitRefusesEveryKindHarnessDoesNotApply(t *testing.T) {
	// THE LIST IS A COPY OF commands.Kind's five constants AND THE DUPLICATION IS
	// FORCED, which is worth saying rather than dressing up as derivation. This
	// package cannot import internal/commands — that is Host's inbox vocabulary
	// and this is the harness edge — so there is nothing to enumerate from, and
	// these are string literals that must be re-checked by hand when either
	// vocabulary moves. What IS derived is the classification: each kind is sent
	// down the accept or refuse arm by runtimecommand.Kind.Valid rather than by a
	// second list here, so a kind harness starts applying changes arm without an
	// edit. Only the SPACE is hand-written; the expectation is not.
	for _, kind := range []string{"create", "restore", "input", "interrupt", "gate_response"} {
		t.Run(kind, func(t *testing.T) {
			bound := &boundSession{
				leaseEpoch: heldEpoch(3),
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
	bound := &boundSession{leaseEpoch: heldEpoch(3)}
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
		leaseEpoch: heldEpoch(3),
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
		leaseEpoch: heldEpoch(3),
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
	bound := &boundSession{leaseEpoch: heldEpoch(9)}
	command := inputCommand()
	command.Kind = "interrupt"
	command.Payload = nil

	admitted, err := bound.admit(command)
	if err != nil {
		t.Fatalf("admit an interrupt: %v", err)
	}
	if admitted.LeaseEpoch != 9 {
		t.Fatalf("lease epoch = %d, want the runtime's 9", admitted.LeaseEpoch)
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

// ---------------------------------------------------------------------------
// ApplyCommand
// ---------------------------------------------------------------------------

func newApplyingController(applier applierPart) *applyingController {
	return &applyingController{
		fullController: *newFullController(newFakeSubscription(nil), nil, nil),
		applierPart:    applier,
	}
}

// The happy path: the translated command reaches the released applier unchanged.
func TestApplyCommandReachesTheReleasedApplier(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller)

	command := inputCommand()
	command.Kind = "interrupt"
	command.Payload = nil
	if err := runtime.ApplyCommand(t.Context(), command); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("the applier saw %d commands, want one", len(seen))
	}
	if seen[0].CommandID != runtimecommand.CommandID(command.CommandID) ||
		seen[0].RuntimeCommandID != command.RuntimeCommandID ||
		seen[0].Kind != runtimecommand.KindInterrupt ||
		seen[0].LeaseEpoch != 7 {
		t.Fatalf("the applier saw %+v, want the translated command at epoch 7", seen[0])
	}
}

// The blocks a decoder produced cross intact, which is the only path H7's
// decode has to a runtime.
func TestApplyCommandCarriesDecodedBlocks(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller,
		WithBlockDecoder(func(body []byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: string(body)}}, nil
		}))

	if err := runtime.ApplyCommand(t.Context(), inputCommand()); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	if len(seen) != 1 || len(seen[0].Blocks) != 1 {
		t.Fatalf("the applier saw %+v, want one decoded block", seen)
	}
	text, ok := seen[0].Blocks[0].(*content.TextBlock)
	if !ok || text.Text != "hello" {
		t.Fatalf("the block is %#v, want the decoded payload", seen[0].Blocks[0])
	}
}

// A SESSION WITH NO RUNTIME-COMMAND CAPABILITY IS REFUSED, in both of the two
// ways the released two-result form expresses it, and in the third way a
// controller can lack the provider entirely. None of them may reach the applier.
func TestApplyCommandRefusesASessionThatCannotApply(t *testing.T) {
	var seen []runtimecommand.Admitted
	for _, tt := range []struct {
		name       string
		controller session.SessionController
		reason     string
	}{
		{
			name:       "no provider at all",
			controller: newFullController(newFakeSubscription(nil), nil, nil),
			reason:     "declares no runtime-command capability",
		},
		{
			name:       "provider reports unavailable",
			controller: newApplyingController(applierPart{available: false}),
			reason:     "no durable application-prefix log",
		},
		{
			name:       "provider reports available with a nil applier",
			controller: newApplyingController(applierPart{available: true, nilApplier: true}),
			reason:     "no durable application-prefix log",
		},
		{
			// THE BOOLEAN IS THE AUTHORITY, NOT THE NIL. This provider hands back
			// a usable applier alongside ok=false, and the command must still be
			// refused: an adapter that read only the nil would drive a runtime
			// the session has just said cannot durably record the application,
			// which is the failure the two-result form exists to prevent.
			name:       "provider reports unavailable with a usable applier",
			controller: newApplyingController(applierPart{available: false, liveApplierWhenUnavailable: true, admitted: &seen}),
			reason:     "no durable application-prefix log",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime := boundFor(t, tt.controller)
			command := inputCommand()
			command.Kind = "interrupt"
			command.Payload = nil

			err := runtime.ApplyCommand(t.Context(), command)
			if len(seen) != 0 {
				t.Fatalf("a session that cannot apply was driven %d times", len(seen))
			}
			var unsupported *UnsupportedCommandError
			if !errors.As(err, &unsupported) {
				t.Fatalf("ApplyCommand = %v, want UnsupportedCommandError", err)
			}
			if !strings.Contains(unsupported.Reason, tt.reason) {
				t.Fatalf("reason = %q, want it to say %q", unsupported.Reason, tt.reason)
			}
		})
	}
}

// A translation refusal must stop BEFORE the applier, because the applier's own
// contract says a non-nil error does not license a retry: the prefix is written
// before the effect, so reaching it with an untranslatable command would leave a
// durable prefix behind for a command that was never applied.
func TestApplyCommandRefusesBeforeReachingTheApplier(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller)

	command := inputCommand()
	command.Kind = "gate_response"
	if err := runtime.ApplyCommand(t.Context(), command); err == nil {
		t.Fatal("a gate response was accepted")
	}
	if len(seen) != 0 {
		t.Fatalf("the applier was reached %d times for a command it cannot express", len(seen))
	}
}

// A failure the applier reports is propagated rather than swallowed.
func TestApplyCommandReportsAnApplierFailure(t *testing.T) {
	sentinel := errors.New("the runtime refused the command")
	controller := newApplyingController(applierPart{available: true, err: sentinel})
	runtime := boundFor(t, controller)

	command := inputCommand()
	command.Kind = "interrupt"
	command.Payload = nil
	if err := runtime.ApplyCommand(t.Context(), command); !errors.Is(err, sentinel) {
		t.Fatalf("ApplyCommand = %v, want the applier's error", err)
	}
}

// A decoder failure is reported as itself rather than wrapped into a refusal
// that would read as a Host policy.
func TestApplyCommandReportsADecodeFailure(t *testing.T) {
	sentinel := errors.New("the payload is not canonical blocks")
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller,
		WithBlockDecoder(func([]byte) ([]content.Block, error) { return nil, sentinel }))

	if err := runtime.ApplyCommand(t.Context(), inputCommand()); !errors.Is(err, sentinel) {
		t.Fatalf("ApplyCommand = %v, want the decoder's error", err)
	}
	if len(seen) != 0 {
		t.Fatalf("the applier was reached %d times after a decode failure", len(seen))
	}
}

// Both refusals this package raises name their subject. IncapableSessionError is
// what an operator sees when a Host will not bind a launched session, and
// UnsupportedCommandError is what it sees when a command cannot be applied;
// neither is useful without the capability, the command and the reason.
func TestEveryRefusalNamesItsSubject(t *testing.T) {
	incapable := (&IncapableSessionError{Missing: []string{"session.IdleWaiter", "session.Liveness"}}).Error()
	for _, want := range []string{"session.IdleWaiter", "session.Liveness"} {
		if !strings.Contains(incapable, want) {
			t.Fatalf("%q does not name %q", incapable, want)
		}
	}

	unsupported := (&UnsupportedCommandError{
		CommandID: "command-a",
		Kind:      "gate_response",
		Reason:    "harness applies only input and interrupt commands",
	}).Error()
	for _, want := range []string{"command-a", "gate_response", "input and interrupt"} {
		if !strings.Contains(unsupported, want) {
			t.Fatalf("%q does not name %q", unsupported, want)
		}
	}
}
