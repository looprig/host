package host_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host"
)

// TestAComposedHostAppliesAFactoryShapedReferencedInput crosses Factory's
// upload/inbox API, the composed Host, and a real Harness runtime journal.
func TestAComposedHostAppliesAFactoryShapedReferencedInput(t *testing.T) {
	w := newRealRuntimeWorld(t)
	id := sessionwire.CommandID("referenced-composed-input")
	words := "REFERENCE-REACHED-MODEL-" + strings.Repeat("x", sessionstore.MaxInboxPayloadBytes)
	body, err := json.Marshal(sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id},
		SessionID:       composeSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":` + string(mustJSON(t, words)) + `}]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= sessionstore.MaxInboxPayloadBytes {
		t.Fatalf("body is only %d bytes", len(body))
	}
	metadata, err := w.factory.PutCommandPayload(t.Context(), sessionstore.PutCommandPayloadRequest{
		TenantID: composeTenant, SessionID: composeSession, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutCommandPayload: %v", err)
	}
	runtimeID, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := w.factory.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: composeTenant, SessionID: composeSession, CommandID: id, Binding: w.binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeID.String()),
		Kind:                     "input", PayloadObject: &metadata, AcceptedAt: now, ApplyDeadline: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AdmitDispositionCommand: %v", err)
	}
	service, _ := w.hostWith(t, 4, func(blueprint *host.Composition) {
		blueprint.Options.ReconcileInterval = 20 * time.Millisecond
	})
	t.Cleanup(func() { stopBounded(service) })
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	w.awaitApplied(t, id, "referenced input")
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, request := range w.llm.Requests() {
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte("REFERENCE-REACHED-MODEL-")) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the model did not receive the referenced input")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustJSON(t *testing.T, value string) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
