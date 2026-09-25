package harnessadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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

func TestAdmitCarriesDecodedMembersWithoutRewritingInputBytes(t *testing.T) {
	principal := &sessionwire.Principal{Tenant: testTenant, Subject: "user-1", Kind: sessionwire.PrincipalKindActor}
	metadata := sessionwire.MessageMetadata{"space": "family"}
	var decoded []byte
	bound := &boundSession{leaseEpoch: heldEpoch(3), tenant: testTenant, session: testSession,
		decode: func(body []byte) ([]content.Block, error) {
			decoded = append([]byte(nil), body...)
			return []content.Block{&content.TextBlock{Text: "hello"}}, nil
		}}
	for _, kind := range []string{"input", "create", "interrupt", "restore"} {
		t.Run(kind, func(t *testing.T) {
			command := inputCommand()
			command.Kind = kind
			command.Principal = principal
			if kind == "input" || kind == "create" {
				command.Metadata = metadata
			}
			var request any
			envelope := sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: command.CommandID}
			switch kind {
			case "input":
				request = sessionwire.InputRequest{CommandEnvelope: envelope, SessionID: testSession, Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`), Principal: principal, Metadata: metadata}
			case "create":
				request = sessionwire.CreateRequest{CommandEnvelope: envelope, SessionID: testSession, AgentID: "agent-a", Blocks: json.RawMessage(`[{"type":"text","text":"hello"}]`), Principal: principal, Metadata: metadata}
			case "interrupt":
				request = sessionwire.InterruptRequest{CommandEnvelope: envelope, SessionID: testSession, Principal: principal}
			case "restore":
				request = sessionwire.RestoreRequest{CommandEnvelope: envelope, SessionID: testSession, Principal: principal}
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			command.Payload = body
			admitted, err := bound.admit(command)
			if err != nil {
				t.Fatal(err)
			}
			if admitted.Principal == nil || *admitted.Principal != *principal {
				t.Fatalf("Principal = %+v", admitted.Principal)
			}
			if want := kind == "input" || kind == "create"; (admitted.Metadata["space"] == "family") != want {
				t.Fatalf("Metadata = %v", admitted.Metadata)
			}
			if kind == "input" && string(decoded) != string(body) {
				t.Fatalf("decoder received rewritten input: %s", decoded)
			}
		})
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

// H6. commands.Kind has five members and, since harness v0.36.0,
// runtimecommand.Kind has the same five. Every kind Factory admits is now
// applied, and a kind NEITHER vocabulary holds is refused HERE, before the
// durable prefix is written, rather than by Admitted.Validate after it.
//
// THE TEST ASSERTS WHAT CROSSED, NOT WHICH ARM THE KIND WENT DOWN, and that is
// the whole reason it is shaped this way. Its previous form branched on
// runtimecommand.Kind(kind).Valid() and checked only that an accepted kind
// crossed with its own name on it. When harness widened Kind.Valid() to five it
// therefore went green for create and restore BY ITSELF, while the payload
// decode was still reached only by KindInput -- so a create crossed with no
// Blocks, the applier sent nothing, and the command settled `applied` with the
// user's first message dropped in silence. A test that passes vacuously through
// the defect it is nearest to is worse than no test, so each row now names the
// content its kind must carry, and a missing decode arm fails the create row by
// assertion.
func TestAdmitRefusesEveryKindHarnessDoesNotApply(t *testing.T) {
	// THE LIST IS A COPY OF commands.Kind's five constants AND THE DUPLICATION
	// IS FORCED, which is worth saying rather than dressing up as derivation.
	// This package cannot import internal/commands — that is Host's inbox
	// vocabulary and this is the harness edge — so there is nothing to
	// enumerate from, and these are string literals that must be re-checked by
	// hand when either vocabulary moves. The last row is in neither vocabulary
	// and is what keeps the refusal arm reachable at all now that all five of
	// Factory's kinds are applied.
	//
	// gate_response IS NOT A ROW: its body must name a gate harness could have
	// minted and the session it is stored under, which is a fixture of its own;
	// see TestAdmitBuildsAGateResponseFromTheStoredBody and
	// TestAdmitRefusesAGateResponseItCannotDecode.
	for _, row := range []struct {
		kind string
		body []byte
		// carries is the text the admitted command's single block must hold,
		// or "" when the kind must cross carrying nothing.
		carries string
	}{
		{kind: "create", body: createBody(t, "the first thing the user said"), carries: "the first thing the user said"},
		{kind: "create", body: createBody(t, ""), carries: ""},
		{kind: "input", body: inputBody(t, "a later message"), carries: "a later message"},
		{kind: "interrupt", body: []byte(`{"version":1,"command_id":"command-a","session_id":"session-a"}`), carries: ""},
		{kind: "restore", body: []byte(`{"version":1,"command_id":"command-a","session_id":"session-a"}`), carries: ""},
		{kind: "no_such_kind", body: nil, carries: ""},
	} {
		name := row.kind
		if row.kind == "create" && row.carries == "" {
			name = "create with no first message"
		}
		t.Run(name, func(t *testing.T) {
			bound := &boundSession{leaseEpoch: heldEpoch(3), decode: inputShapedDecoder(t)}
			command := inputCommand()
			command.Kind = row.kind
			command.Payload = row.body
			admitted, err := bound.admit(command)

			if runtimecommand.Kind(row.kind).Valid() {
				if err != nil {
					t.Fatalf("admit(%q) = %v, want it accepted", row.kind, err)
				}
				if string(admitted.Kind) != row.kind {
					t.Fatalf("kind = %q, want %q", admitted.Kind, row.kind)
				}
				if row.carries == "" {
					if len(admitted.Blocks) != 0 {
						t.Fatalf("a %q crossed with %d blocks, want none", row.kind, len(admitted.Blocks))
					}
					return
				}
				if len(admitted.Blocks) != 1 {
					t.Fatalf("a %q crossed with %d blocks, want the one its body carries", row.kind, len(admitted.Blocks))
				}
				text, ok := admitted.Blocks[0].(*content.TextBlock)
				if !ok || text.Text != row.carries {
					t.Fatalf("a %q crossed carrying %#v, want %q", row.kind, admitted.Blocks[0], row.carries)
				}
				return
			}
			var unsupported *UnsupportedCommandError
			if !errors.As(err, &unsupported) {
				t.Fatalf("admit(%q) = %v, want UnsupportedCommandError", row.kind, err)
			}
			if !strings.Contains(unsupported.Reason, "create, input, interrupt, restore and gate_response") {
				t.Fatalf("reason = %q, want the five-kind refusal", unsupported.Reason)
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

// A CREATE WITH BLOCKS MUST PROPAGATE THE SHARED DECODER'S ERROR. Swallowing
// it would send an empty create to harness, settle applied, and lose the first
// message while every durable status looks healthy.
func TestApplyCommandReportsACreateDecodeFailure(t *testing.T) {
	sentinel := errors.New("the create's blocks cannot be decoded")
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller,
		WithBlockDecoder(func([]byte) ([]content.Block, error) { return nil, sentinel }))
	if err := runtime.ApplyCommand(t.Context(), createCommand(t, "first words")); !errors.Is(err, sentinel) {
		t.Fatalf("create ApplyCommand = %v, want decoder error", err)
	}
	if len(seen) != 0 {
		t.Fatalf("the applier saw %d creates after the decoder refused", len(seen))
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

// ---------------------------------------------------------------------------
// The attempt identity
// ---------------------------------------------------------------------------

// THE ATTEMPT IDENTITY IS WHAT MAKES A DISPATCH SETTLEABLE, and it crosses this
// seam or nothing is ever written. harness v0.34.0 writes the kind-5 disposition
// frame ONLY when Admitted.AttemptID is non-empty — the field is optional by
// design, and an empty one leaves a legacy journal's bytes unchanged — so a Host
// that authorized an attempt and then dropped its name produces a command that
// sits applying forever with a real effect behind it. That is the exact failure
// the whole boundary in internal/commands/dispatch.go existed to prevent, so the
// carry is asserted on the value harness receives rather than on the one Host
// sent.
func TestAdmitCarriesTheAttemptIdentityToHarness(t *testing.T) {
	var seen []runtimecommand.Admitted
	runtime := boundFor(t, newApplyingController(applierPart{available: true, admitted: &seen}),
		WithBlockDecoder(func(body []byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: string(body)}}, nil
		}))

	command := inputCommand()
	command.AttemptID = "attempt-9"
	if err := runtime.ApplyCommand(t.Context(), command); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("the released applier saw %d commands, want 1", len(seen))
	}
	if seen[0].AttemptID != runtimecommand.AttemptID("attempt-9") {
		t.Errorf("AttemptID = %q, want %q", seen[0].AttemptID, "attempt-9")
	}
}

// AN ABSENT ATTEMPT IS THE LEGACY SHAPE AND IS NOT INVENTED HERE. The field is
// optional in the released type and this adapter has no authority to mint one:
// the identity is the STORE's, written immutably by BeginDispositionAttempt
// before the dispatch, so an adapter that substituted a value would name an
// attempt no evidence could ever be about. The row is the negative half of the
// one above — without it, a mapping that always wrote some non-empty identity
// would pass the positive.
func TestAdmitDoesNotInventAnAttemptIdentity(t *testing.T) {
	var seen []runtimecommand.Admitted
	runtime := boundFor(t, newApplyingController(applierPart{available: true, admitted: &seen}),
		WithBlockDecoder(func(body []byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: string(body)}}, nil
		}))

	if err := runtime.ApplyCommand(t.Context(), inputCommand()); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("the released applier saw %d commands, want 1", len(seen))
	}
	if seen[0].AttemptID != "" {
		t.Errorf("AttemptID = %q, want it empty for a command that names no attempt", seen[0].AttemptID)
	}
}

// A MALFORMED ATTEMPT IDENTITY IS REFUSED BEFORE THE APPLIER, on the same terms
// as every other refusal in admit: the released Admitted.Validate bounds an
// AttemptID at MaxAttemptIDBytes and requires valid UTF-8, and a command that
// cannot be admitted must not reach a durable writer. The positive control is
// that the applier saw nothing at all.
func TestAdmitRefusesAnOversizedAttemptIdentity(t *testing.T) {
	var seen []runtimecommand.Admitted
	runtime := boundFor(t, newApplyingController(applierPart{available: true, admitted: &seen}),
		WithBlockDecoder(func(body []byte) ([]content.Block, error) {
			return []content.Block{&content.TextBlock{Text: string(body)}}, nil
		}))

	command := inputCommand()
	command.AttemptID = strings.Repeat("a", runtimecommand.MaxAttemptIDBytes+1)
	if err := runtime.ApplyCommand(t.Context(), command); err == nil {
		t.Fatal("ApplyCommand admitted an attempt identity the released type refuses")
	}
	if len(seen) != 0 {
		t.Errorf("the released applier saw %d commands, want 0", len(seen))
	}
}

// ---------------------------------------------------------------------------
// The two refusals Host is told to act on
// ---------------------------------------------------------------------------

// A DISPOSITION THE RUNTIME CANNOT RECORD IS A DIFFERENT REFUSAL FROM A
// TRANSPORT FAILURE, and this adapter is the only place that can tell them
// apart: harness raises *runtimecommand.DispositionUnsupportedError BEFORE any
// durable write, so nothing happened and the command may be re-offered — while a
// transport failure says the opposite. The control row is an ordinary failure at
// the same seam; without it a mapping that reported EVERY dispatch failure as
// re-offerable would pass the positive.
func TestApplyCommandDistinguishesAnUnrecordableDisposition(t *testing.T) {
	for _, row := range []struct {
		name        string
		err         error
		reofferable bool
	}{
		{
			"the session cannot record a disposition",
			&runtimecommand.DispositionUnsupportedError{CommandID: "command-a", AttemptID: "attempt-9"},
			true,
		},
		{"an ordinary transport failure", errors.New("harnessadapter_test: connection reset"), false},
	} {
		t.Run(row.name, func(t *testing.T) {
			controller := newApplyingController(applierPart{available: true, err: row.err})
			runtime := boundFor(t, controller, WithBlockDecoder(func(body []byte) ([]content.Block, error) {
				return []content.Block{&content.TextBlock{Text: string(body)}}, nil
			}))
			err := runtime.ApplyCommand(t.Context(), inputCommand())
			if err == nil {
				t.Fatal("ApplyCommand reported success for a failed dispatch")
			}
			if got := errors.Is(err, department.ErrDispositionUnsupported); got != row.reofferable {
				t.Errorf("errors.Is(err, ErrDispositionUnsupported) = %v, want %v (err = %v)", got, row.reofferable, err)
			}
			// The original always survives: a Host that needed the identities
			// harness named must still be able to reach them.
			if !errors.Is(err, row.err) {
				t.Errorf("the released error did not survive the mapping: %v", err)
			}
		})
	}
}

// D3 GATE F1. harness reports whether the application prefix committed before a
// failure through the returned Disposition; the adapter must surface it as
// department.ErrPrefixCommitted, joined, and only when it is true.
func TestApplyCommandSurfacesAPrefixThatCommittedBeforeTheFailure(t *testing.T) {
	failure := errors.New("harnessadapter_test: the disposition append failed")
	for _, row := range []struct {
		name      string
		prefix    uint64
		committed bool
	}{
		{"the prefix committed", 17, true},
		{"nothing durable was written", 0, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			controller := newApplyingController(applierPart{available: true, err: failure, prefixOnErr: row.prefix})
			runtime := boundFor(t, controller, WithBlockDecoder(func(body []byte) ([]content.Block, error) {
				return []content.Block{&content.TextBlock{Text: string(body)}}, nil
			}))
			err := runtime.ApplyCommand(t.Context(), inputCommand())
			if !errors.Is(err, failure) {
				t.Fatalf("ApplyCommand = %v, want the runtime's failure to survive", err)
			}
			if got := errors.Is(err, department.ErrPrefixCommitted); got != row.committed {
				t.Errorf("errors.Is(err, ErrPrefixCommitted) = %v, want %v", got, row.committed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The recovery closure
// ---------------------------------------------------------------------------

// THE CLOSER IS DISCOVERED BY ASSERTION ON THE RELEASED APPLIER, not advertised
// unconditionally. A session whose applier is not a runtimecommand.AttemptCloser
// has no closure to offer, and a Host that was told otherwise would block on a
// capability that answers nothing.
func TestCloseAttemptReachesTheReleasedCloser(t *testing.T) {
	closer := &recordingAttemptCloser{}
	controller := newApplyingController(applierPart{available: true, closer: closer})
	runtime := boundFor(t, controller)

	capability, ok := runtime.(department.AttemptCloser)
	if !ok {
		t.Fatal("the bound session is not a department.AttemptCloser")
	}
	err := capability.CloseAttempt(t.Context(), "command-a",
		uuid.MustParse("11111111-2222-3333-4444-555555555555"), "input", "attempt-9", 40)
	if err != nil {
		t.Fatalf("CloseAttempt: %v", err)
	}
	if len(closer.seen) != 1 {
		t.Fatalf("the released closer saw %d closures, want one", len(closer.seen))
	}
	got := closer.seen[0]
	if got.AttemptID != "attempt-9" || got.AttemptJournalEpoch != 40 || got.Kind != runtimecommand.KindInput {
		t.Errorf("the released closer saw %+v, want the attempt's own identity, kind and grant", got)
	}
}

// AN ENDURING EFFECT IS A DIFFERENT REFUSAL FROM EVERY OTHER CLOSURE REFUSAL,
// and it is the one a caller must NEVER retry: the predecessor's effect
// committed and only its evidence is missing, so a retry that became a tombstone
// would destroy it. The control rows are the two authorization refusals, which
// are ordinary failures and must not carry the sentinel.
func TestCloseAttemptDistinguishesAnEnduringEffect(t *testing.T) {
	for _, row := range []struct {
		name     string
		err      error
		enduring bool
	}{
		{"the journal holds a committed effect", &runtimecommand.EnduringEffectError{AttemptID: "attempt-9"}, true},
		{"no live grant", &runtimecommand.ClosureNotAuthorizedError{AttemptID: "attempt-9", Held: false}, false},
		{"a grant that is not later", &runtimecommand.ClosureNotAuthorizedError{AttemptID: "attempt-9", Held: true}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			closer := &recordingAttemptCloser{err: row.err}
			controller := newApplyingController(applierPart{available: true, closer: closer})
			runtime := boundFor(t, controller)
			capability, ok := runtime.(department.AttemptCloser)
			if !ok {
				t.Fatal("the bound session is not a department.AttemptCloser")
			}
			err := capability.CloseAttempt(t.Context(), "command-a",
				uuid.MustParse("11111111-2222-3333-4444-555555555555"), "input", "attempt-9", 40)
			if err == nil {
				t.Fatal("CloseAttempt reported success for a refused closure")
			}
			if got := errors.Is(err, department.ErrEnduringEffect); got != row.enduring {
				t.Errorf("errors.Is(err, ErrEnduringEffect) = %v, want %v (err = %v)", got, row.enduring, err)
			}
			if !errors.Is(err, row.err) {
				t.Errorf("the released error did not survive the mapping: %v", err)
			}
		})
	}
}

// A SESSION WHOSE APPLIER CANNOT CLOSE REFUSES AT THE CALL rather than
// advertising a closure it cannot write, which is the released interface's own
// stated rule.
func TestCloseAttemptRefusesASessionWithNoReleasedCloser(t *testing.T) {
	controller := newApplyingController(applierPart{available: true})
	runtime := boundFor(t, controller)
	capability, ok := runtime.(department.AttemptCloser)
	if !ok {
		t.Fatal("the bound session is not a department.AttemptCloser")
	}
	err := capability.CloseAttempt(t.Context(), "command-a",
		uuid.MustParse("11111111-2222-3333-4444-555555555555"), "input", "attempt-9", 40)
	if err == nil {
		t.Fatal("CloseAttempt reported success for a session with no released closer")
	}
	if errors.Is(err, department.ErrEnduringEffect) {
		t.Error("an unavailable capability was reported as an enduring effect, which a caller must never retry")
	}
}

// A MALFORMED CLOSURE IS REFUSED BEFORE THE RELEASED CLOSER, on the released
// type's own rule: a closure that cannot name an attempt is one that could
// tombstone the wrong thing.
func TestCloseAttemptRefusesAMalformedClosure(t *testing.T) {
	for _, row := range []struct {
		name    string
		attempt string
		kind    string
		epoch   uint64
	}{
		{"no attempt identity", "", "input", 40},
		// A KIND NO VOCABULARY HOLDS, and it has to be invented rather than
		// borrowed. runtimecommand.Kind named three kinds at v0.34.0 and five
		// at v0.36.0, so gate_response, then create and restore in turn stopped
		// being examples of a kind the released closer refuses. commands.Kind
		// has the same five, so there is no Host kind left to borrow either:
		// the row names a string neither vocabulary has ever held.
		{"an unknown kind", "attempt-9", "no_such_kind", 40},
		{"no attempt grant", "attempt-9", "input", 0},
	} {
		t.Run(row.name, func(t *testing.T) {
			closer := &recordingAttemptCloser{}
			controller := newApplyingController(applierPart{available: true, closer: closer})
			runtime := boundFor(t, controller)
			capability := runtime.(department.AttemptCloser)
			err := capability.CloseAttempt(t.Context(), "command-a",
				uuid.MustParse("11111111-2222-3333-4444-555555555555"), row.kind, row.attempt, row.epoch)
			if err == nil {
				t.Fatal("CloseAttempt admitted a closure the released type refuses")
			}
			if len(closer.seen) != 0 {
				t.Errorf("the released closer saw %d closures, want none", len(closer.seen))
			}
		})
	}
}

// recordingAttemptCloser is runtimecommand.AttemptCloser.
type recordingAttemptCloser struct {
	seen []runtimecommand.Closure
	err  error
}

func (c *recordingAttemptCloser) CloseAttempt(_ context.Context, closure runtimecommand.Closure) (runtimecommand.ClosureResult, error) {
	if c.err != nil {
		return runtimecommand.ClosureResult{}, c.err
	}
	c.seen = append(c.seen, closure)
	return runtimecommand.ClosureResult{Sequence: 1, Appended: true}, nil
}

// ---------------------------------------------------------------------------
// H6's create and restore arms
// ---------------------------------------------------------------------------
//
// THE SILENT DROP THESE COVER IS WHAT MAKES THE VOCABULARY TEST ABOVE
// INSUFFICIENT ON ITS OWN. The kind gate is Kind.Valid(), so harness v0.36.0
// opened it for create and restore by itself; the payload decode was reached
// only by KindInput. A create therefore crossed with no Blocks, the applier
// sent nothing, and the command settled `applied` -- the user's first message
// dropped in silence with the record looking perfectly settled. Every row below
// asserts on what CROSSED, never on the arm the kind was sent down.

// createBody is the Core CreateRequest Factory stores as a create's private
// body, carrying blocks when text is non-empty.
func createBody(t *testing.T, text string) []byte {
	t.Helper()
	request := sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-a"},
		SessionID:       testSession,
		AgentID:         "agent-a",
	}
	if text != "" {
		request.Blocks = json.RawMessage(`[{"type":"text","text":` + strconv.Quote(text) + `}]`)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("the fixture's create request is not one Core admits: %v", err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// createCommand is one create as the applier dispatches it.
func createCommand(t *testing.T, text string) department.RuntimeCommand {
	t.Helper()
	command := inputCommand()
	command.Kind = "create"
	command.Payload = createBody(t, text)
	return command
}

// inputShapedDecoder is the decoder a composition binds: it reads the
// INPUT-SHAPED body H7 names, and refuses anything else, which is what makes a
// create's own CreateRequest body unreadable to it.
func inputShapedDecoder(t *testing.T) BlockDecoder {
	t.Helper()
	return func(body []byte) ([]content.Block, error) {
		var request sessionwire.InputRequest
		if err := json.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(request.Blocks, &blocks); err != nil {
			return nil, err
		}
		decoded := make([]content.Block, 0, len(blocks))
		for _, block := range blocks {
			decoded = append(decoded, &content.TextBlock{Text: block.Text})
		}
		return decoded, nil
	}
}

// A CREATE'S FIRST MESSAGE CROSSES, and it crosses through the SAME decoder an
// input's body goes through: the create's blocks are re-presented in the
// input-shaped body that decoder reads, so a composition binds one encoding
// rather than two.
func TestAdmitDecodesACreatesFirstMessage(t *testing.T) {
	bound := &boundSession{leaseEpoch: heldEpoch(3), decode: inputShapedDecoder(t)}

	admitted, err := bound.admit(createCommand(t, "the first thing the user said"))
	if err != nil {
		t.Fatalf("admit of a create carrying a first message: %v", err)
	}
	if len(admitted.Blocks) != 1 {
		t.Fatalf("the create crossed with %d blocks, want its first message", len(admitted.Blocks))
	}
	text, ok := admitted.Blocks[0].(*content.TextBlock)
	if !ok || text.Text != "the first thing the user said" {
		t.Fatalf("the block is %#v, want the create's first message", admitted.Blocks[0])
	}
}

// The create arm preserves every block, its type and its order. A one-block
// text fixture cannot catch a truncated or entirely dropped first message.
func TestAdmitCarriesMultiblockImageCreateInOrder(t *testing.T) {
	blocks, err := content.MarshalBlocks([]content.Block{
		&content.TextBlock{Text: "first"},
		&content.ImageBlock{MediaType: "image/png", Source: content.ImageSource{URL: "https://example.test/first.png"}},
		&content.TextBlock{Text: "last"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-a"},
		SessionID:       testSession, AgentID: "agent-a", Blocks: blocks,
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	command := createCommand(t, "")
	command.Payload = body
	bound := &boundSession{leaseEpoch: heldEpoch(3), decode: func(body []byte) ([]content.Block, error) {
		var input sessionwire.InputRequest
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
		return content.UnmarshalBlocks(input.Blocks)
	}}
	admitted, err := bound.admit(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.Blocks) != 3 {
		t.Fatalf("create crossed with %d blocks, want 3", len(admitted.Blocks))
	}
	first, ok := admitted.Blocks[0].(*content.TextBlock)
	if !ok || first.Text != "first" {
		t.Fatalf("first block = %#v", admitted.Blocks[0])
	}
	image, ok := admitted.Blocks[1].(*content.ImageBlock)
	if !ok || image.MediaType != "image/png" || image.Source.URL != "https://example.test/first.png" {
		t.Fatalf("image block = %#v", admitted.Blocks[1])
	}
	last, ok := admitted.Blocks[2].(*content.TextBlock)
	if !ok || last.Text != "last" {
		t.Fatalf("last block = %#v", admitted.Blocks[2])
	}
}

// A BARE CREATE CARRIES NOTHING AND IS STILL ADMITTED. Core's
// CreateRequest.Blocks is omitempty and an idle create is legitimate; refusing
// one would make every idle session's first command unsettleable, which is the
// wedge harness v0.36.0 exists to end.
func TestAdmitCarriesABareCreateWithNoBlocks(t *testing.T) {
	consulted := false
	bound := &boundSession{
		leaseEpoch: heldEpoch(3),
		decode: func([]byte) ([]content.Block, error) {
			consulted = true
			return []content.Block{&content.TextBlock{Text: "invented"}}, nil
		},
	}

	admitted, err := bound.admit(createCommand(t, ""))
	if err != nil {
		t.Fatalf("admit of a bare create: %v", err)
	}
	if len(admitted.Blocks) != 0 {
		t.Fatalf("a bare create crossed with %d blocks, want none", len(admitted.Blocks))
	}
	if consulted {
		t.Fatal("the decoder was consulted for a create that carries no blocks")
	}
}

// H7 EXTENDS TO A CREATE THAT CARRIES ONE. A composition with no decoder
// refuses rather than applying an empty first turn, which is exactly the
// silent drop this release exists to prevent.
func TestAdmitRefusesACreateWithAFirstMessageAndNoDecoder(t *testing.T) {
	bound := &boundSession{leaseEpoch: heldEpoch(3)}

	_, err := bound.admit(createCommand(t, "the first thing the user said"))
	var unsupported *UnsupportedCommandError
	if !errors.As(err, &unsupported) {
		t.Fatalf("admit of a create with no decoder = %v, want UnsupportedCommandError", err)
	}
	if !strings.Contains(unsupported.Reason, "block decoder") {
		t.Fatalf("reason = %q, want the missing decoder", unsupported.Reason)
	}
}

// A BODY THAT IS NOT A CORE CreateRequest IS REFUSED. The adapter reads the
// stored record itself rather than handing opaque bytes to the input decoder,
// so a body no Host could read is refused here rather than silently producing
// an empty turn.
func TestAdmitRefusesACreateBodyCoreDoesNotAdmit(t *testing.T) {
	for _, row := range []struct {
		name string
		body []byte
	}{
		{"no body at all", nil},
		{"not JSON", []byte("hello")},
		{"an input request, not a create", []byte(`{"version":1,"command_id":"command-a","session_id":"session-a","blocks":[{"type":"text","text":"hi"}]}`)},
	} {
		t.Run(row.name, func(t *testing.T) {
			bound := &boundSession{leaseEpoch: heldEpoch(3), decode: inputShapedDecoder(t)}
			command := createCommand(t, "")
			command.Payload = row.body

			if _, err := bound.admit(command); err == nil {
				t.Fatal("admit accepted a create body Core does not admit")
			}
		})
	}
}

// A RESTORE CARRIES NOTHING AND CONSULTS NOTHING. Core's RestoreRequest has no
// blocks member and Admitted.Validate refuses a restore that carries any, so
// the only correct arm is a pass-through.
func TestAdmitPassesARestoreThroughCarryingNothing(t *testing.T) {
	consulted := false
	bound := &boundSession{
		leaseEpoch: heldEpoch(3),
		decode: func([]byte) ([]content.Block, error) {
			consulted = true
			return []content.Block{&content.TextBlock{Text: "invented"}}, nil
		},
	}
	command := inputCommand()
	command.Kind = "restore"
	command.Payload = []byte(`{"version":1,"command_id":"command-a","session_id":"session-a"}`)

	admitted, err := bound.admit(command)
	if err != nil {
		t.Fatalf("admit of a restore: %v", err)
	}
	if len(admitted.Blocks) != 0 {
		t.Fatalf("a restore crossed with %d blocks, want none", len(admitted.Blocks))
	}
	if admitted.GateResponse != nil {
		t.Fatal("a restore crossed carrying a gate response")
	}
	if consulted {
		t.Fatal("the decoder was consulted for a restore")
	}
}

// inputBody is the Core InputRequest Factory stores as an input's private body.
func inputBody(t *testing.T, text string) []byte {
	t.Helper()
	request := sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-a"},
		SessionID:       testSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":` + strconv.Quote(text) + `}]`),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("the fixture's input request is not one Core admits: %v", err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
