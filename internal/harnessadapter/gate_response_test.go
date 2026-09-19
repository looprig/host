package harnessadapter

import (
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/runtimecommand"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/gateresponse"
)

var adapterGate = uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f")

// gateResponseCommand is a gate_response for the bound session, carrying the
// body Factory stores: json.Marshal of Core's record.
func gateResponseCommand(t *testing.T, change func(*sessionwire.GateResponseRequest)) department.RuntimeCommand {
	t.Helper()
	request := sessionwire.GateResponseRequest{
		CommandEnvelope:        sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-gate"},
		SessionID:              testSession,
		GateID:                 sessionwire.GateID(adapterGate.String()),
		Action:                 "answer",
		Values:                 map[string]json.RawMessage{"answer": json.RawMessage(`"blue"`)},
		ExpectedOpenJournalSeq: 7,
	}
	if change != nil {
		change(&request)
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return department.RuntimeCommand{
		CommandID:        "command-gate",
		RuntimeCommandID: uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		Kind:             string(runtimecommand.KindGateResponse),
		Payload:          body,
		AttemptID:        "attempt-gate",
	}
}

// TestAdmitBuildsAGateResponseFromTheStoredBody: the answer crosses as
// harness's decoded GateResponse — the gate as harness's own identity, the
// action and raw values verbatim, and the USER as its source (a classifier
// source would be refused by the session, and Host never asserts one) — under
// the attempt the durable record authorized.
func TestAdmitBuildsAGateResponseFromTheStoredBody(t *testing.T) {
	bound := &boundSession{leaseEpoch: heldEpoch(3), tenant: testTenant, session: testSession}
	admitted, err := bound.admit(gateResponseCommand(t, nil))
	if err != nil {
		t.Fatalf("admit(gate_response) = %v, want it admitted", err)
	}
	if admitted.Kind != runtimecommand.KindGateResponse || admitted.AttemptID != "attempt-gate" || admitted.LeaseEpoch != 3 {
		t.Fatalf("admitted = %+v", admitted)
	}
	response := admitted.GateResponse
	if response == nil {
		t.Fatal("the admitted gate_response carries no answer")
	}
	if response.GateID != gate.ID(adapterGate) || response.Action != "answer" || string(response.Values["answer"]) != `"blue"` {
		t.Fatalf("the answer = %+v, want the stored body's", response)
	}
	if response.Source.Kind != gate.ResponseFromUser {
		t.Fatalf("the answer's source = %q, want %q", response.Source.Kind, gate.ResponseFromUser)
	}
	if len(admitted.Blocks) != 0 {
		t.Fatal("a gate_response carried input blocks")
	}
}

// TestAdmitRefusesAGateResponseItCannotDecode: the same rule the applier
// applies before the attempt, so a body refused here was already rejected
// there. It is refused BEFORE the applier and therefore before the prefix.
func TestAdmitRefusesAGateResponseItCannotDecode(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller)
	err := runtime.ApplyCommand(t.Context(), gateResponseCommand(t, func(r *sessionwire.GateResponseRequest) { r.GateID = "gate-a" }))
	var malformed *gateresponse.MalformedError
	if !errors.As(err, &malformed) {
		t.Fatalf("ApplyCommand(a gate_response naming no harness gate) = %v, want a MalformedError", err)
	}
	if len(seen) != 0 {
		t.Fatalf("the released applier saw %d commands, want none", len(seen))
	}
}

// TestAGateResponseReachesTheReleasedApplier is the happy path through the
// bound session.
func TestAGateResponseReachesTheReleasedApplier(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller)
	if err := runtime.ApplyCommand(t.Context(), gateResponseCommand(t, nil)); err != nil {
		t.Fatalf("ApplyCommand(gate_response) = %v", err)
	}
	if len(seen) != 1 || seen[0].GateResponse == nil || seen[0].GateResponse.GateID != gate.ID(adapterGate) {
		t.Fatalf("the released applier saw %+v, want the one answer", seen)
	}
}
