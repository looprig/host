package sessionstoreadapter

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/sessionstore"
)

// A deployment with two journal stores needs something above them, and harness
// says so in as many words: a Store "does NOT route on StorageBindingID — a
// Store holds no registry of bindings it serves", pinned on that side by a test
// in harness named for exactly that consequence. SELECTING which reader serves which
// binding is the composition root's job, and this is that selection.

// stubEvidenceReader answers for one binding and records what it was asked.
type stubEvidenceReader struct {
	name string
	seen []sessionstore.DispositionEvidenceRequest
	err  error
}

func (r *stubEvidenceReader) ReadDispositionEvidence(
	_ context.Context, req sessionstore.DispositionEvidenceRequest,
) (sessionstore.DispositionEvidence, error) {
	r.seen = append(r.seen, req)
	if r.err != nil {
		return sessionstore.DispositionEvidence{}, r.err
	}
	return sessionstore.DispositionEvidence{
		AttemptID: req.Attempt.AttemptID,
		Kind:      sessionstore.DispositionApplied,
	}, nil
}

func requestFor(binding string) sessionstore.DispositionEvidenceRequest {
	return sessionstore.DispositionEvidenceRequest{
		TenantID:  "tenant-a",
		SessionID: "session-a",
		CommandID: "command-a",
		Binding: sessionstore.SessionBinding{
			StorageBindingID: binding,
			RuntimeSessionID: "11111111-2222-3333-4444-555555555555",
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
		Attempt: sessionstore.DispositionAttempt{AttemptID: "attempt-9"},
	}
}

// THE ROUTER SENDS EACH BINDING TO ITS OWN READER, and the second binding is
// what makes that a routing decision rather than a pass-through. With one
// reader registered, a router that ignored the field entirely would be
// indistinguishable from one that read it.
func TestEvidenceRouterRoutesOnTheStorageBindingID(t *testing.T) {
	first := &stubEvidenceReader{name: "first"}
	second := &stubEvidenceReader{name: "second"}
	router, err := NewEvidenceRouter(map[string]sessionstore.DispositionEvidenceReader{
		"binding-1": first,
		"binding-2": second,
	})
	if err != nil {
		t.Fatalf("NewEvidenceRouter: %v", err)
	}

	if _, err := router.ReadDispositionEvidence(t.Context(), requestFor("binding-2")); err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	if len(first.seen) != 0 {
		t.Errorf("the reader for binding-1 was asked about binding-2 %d times", len(first.seen))
	}
	if len(second.seen) != 1 {
		t.Fatalf("the reader for binding-2 was asked %d times, want once", len(second.seen))
	}
	// THE REQUEST CROSSES UNCHANGED. Every member was derived by the
	// orchestration store from its own immutable record, and a router that
	// reinterpreted one would be substituting its own answer for the store's.
	if got := second.seen[0]; got != requestFor("binding-2") {
		t.Errorf("the reader saw %+v, want the request unchanged", got)
	}
}

// AN UNREGISTERED BINDING IS REFUSED AND NEVER ANSWERED EMPTY. The released
// reader contract is explicit that absence is not a disposition and the zero
// value settles nothing — but a router that returned the zero value AND a nil
// error would be handing the verifier a silence, so the refusal is asserted on
// the error rather than on the value.
func TestEvidenceRouterRefusesAnUnregisteredBinding(t *testing.T) {
	only := &stubEvidenceReader{name: "only"}
	router, err := NewEvidenceRouter(map[string]sessionstore.DispositionEvidenceReader{"binding-1": only})
	if err != nil {
		t.Fatalf("NewEvidenceRouter: %v", err)
	}
	evidence, err := router.ReadDispositionEvidence(t.Context(), requestFor("binding-unknown"))
	if err == nil {
		t.Fatal("the router answered for a binding it serves no reader for")
	}
	if !errors.Is(err, ErrUnroutableBinding) {
		t.Errorf("ReadDispositionEvidence = %v, want it to carry ErrUnroutableBinding", err)
	}
	if evidence != (sessionstore.DispositionEvidence{}) {
		t.Errorf("the router returned %+v beside its refusal", evidence)
	}
	if len(only.seen) != 0 {
		t.Error("an unregistered binding reached a reader")
	}
}

// A READER'S OWN REFUSAL IS PROPAGATED AND NOT SWALLOWED. The released contract
// says an error is the CORRECT answer for an unavailable, cancelled, incomplete
// or unreadable journal, and that a reader must never report one as an empty
// disposition — a router that converted a downstream refusal into a zero value
// would defeat that one layer up.
func TestEvidenceRouterPropagatesAReadersRefusal(t *testing.T) {
	sentinel := errors.New("sessionstoreadapter_test: the journal is unreadable")
	router, err := NewEvidenceRouter(map[string]sessionstore.DispositionEvidenceReader{
		"binding-1": &stubEvidenceReader{name: "only", err: sentinel},
	})
	if err != nil {
		t.Fatalf("NewEvidenceRouter: %v", err)
	}
	evidence, err := router.ReadDispositionEvidence(t.Context(), requestFor("binding-1"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("ReadDispositionEvidence = %v, want the reader's own error", err)
	}
	if evidence != (sessionstore.DispositionEvidence{}) {
		t.Errorf("the router returned %+v beside a refusal", evidence)
	}
}

// A ROUTER WITH NO READERS IS REFUSED AT CONSTRUCTION. It could answer nothing,
// so every settlement in the deployment would fail at the evidence read — which
// is a configuration mistake an operator should be told about at startup rather
// than discover one command at a time.
func TestNewEvidenceRouterRefusesAnEmptyOrMalformedTable(t *testing.T) {
	for _, row := range []struct {
		name    string
		readers map[string]sessionstore.DispositionEvidenceReader
	}{
		{"no readers at all", map[string]sessionstore.DispositionEvidenceReader{}},
		{"a nil table", nil},
		{"an unnamed binding", map[string]sessionstore.DispositionEvidenceReader{"": &stubEvidenceReader{}}},
		{"a nil reader", map[string]sessionstore.DispositionEvidenceReader{"binding-1": nil}},
	} {
		t.Run(row.name, func(t *testing.T) {
			if _, err := NewEvidenceRouter(row.readers); err == nil {
				t.Fatal("NewEvidenceRouter accepted a table that can answer nothing")
			}
		})
	}
}

// THE TABLE IS COPIED, so a composition root that kept its map and mutated it
// afterwards cannot re-point a live router at another store. A settlement's
// evidence source is fixed for the process's life.
func TestEvidenceRouterCopiesItsTable(t *testing.T) {
	only := &stubEvidenceReader{name: "only"}
	table := map[string]sessionstore.DispositionEvidenceReader{"binding-1": only}
	router, err := NewEvidenceRouter(table)
	if err != nil {
		t.Fatalf("NewEvidenceRouter: %v", err)
	}
	impostor := &stubEvidenceReader{name: "impostor"}
	table["binding-1"] = impostor
	if _, err := router.ReadDispositionEvidence(t.Context(), requestFor("binding-1")); err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	if len(impostor.seen) != 0 {
		t.Error("a mutation of the caller's map re-pointed a live router")
	}
	if len(only.seen) != 1 {
		t.Errorf("the registered reader was asked %d times, want once", len(only.seen))
	}
}

// The router is the released reader seam.
var _ sessionstore.DispositionEvidenceReader = (*EvidenceRouter)(nil)
