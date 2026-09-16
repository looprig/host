package sessionstoreadapter

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"

	"github.com/looprig/sessionstore"
)

// This file is the SELECTION a deployment with more than one journal store
// needs, and it exists here because neither module below it can do it.
//
// sessionstore's settlement hands its configured reader the session's IMMUTABLE
// BINDING and asks; harness's Store answers for the one keyspace it owns and
// says plainly that it "does NOT route on StorageBindingID — a Store holds no
// registry of bindings it serves", a limitation pinned by a test on that side so
// nobody assumes otherwise. The binding's StorageBindingID is therefore a field
// that identifies a store and that no store can check. Routing on it is the
// composition root's job, and this is the object that does it.
//
// IT IS NOT A FALLBACK AND HAS NO DEFAULT READER. A router that sent an
// unrecognised binding to "the only store we have" would answer a question about
// one keyspace out of another, confidently — which is exactly the failure
// harness's own tenant refusal exists to prevent one layer down.

// EvidenceRouter selects the settlement evidence reader that serves one
// session's storage binding.
type EvidenceRouter struct {
	readers map[string]sessionstore.DispositionEvidenceReader
}

// EvidenceRouter is the released reader seam, so a store is configured with one
// through sessionstore.WithDispositionEvidence.
var _ sessionstore.DispositionEvidenceReader = (*EvidenceRouter)(nil)

// NewEvidenceRouter builds a router over one reader per storage binding.
//
// IT COPIES THE TABLE. A composition root that kept its map and mutated it later
// could otherwise re-point a live router at another store, and a settlement's
// evidence source must be fixed for the process's life: a command settled from
// one journal and a command settled from another are two different durable
// statements, and which one a record got would depend on when it was settled.
func NewEvidenceRouter(readers map[string]sessionstore.DispositionEvidenceReader) (*EvidenceRouter, error) {
	if len(readers) == 0 {
		return nil, &InvalidEvidenceTableError{Reason: "the table is empty, so every settlement in this deployment would fail at the evidence read"}
	}
	for binding, reader := range readers {
		if binding == "" {
			return nil, &InvalidEvidenceTableError{Reason: "a reader is registered under no storage binding, which no session's binding can ever name"}
		}
		if reader == nil {
			return nil, &InvalidEvidenceTableError{Binding: binding, Reason: "the reader is nil, and a nil reader would panic at the first settlement rather than refuse at startup"}
		}
	}
	return &EvidenceRouter{readers: maps.Clone(readers)}, nil
}

// ReadDispositionEvidence forwards the request to the reader that serves its
// binding, unchanged.
//
// NOTHING IS REINTERPRETED ON THE WAY. Every member of the request was derived
// by the orchestration store from its own immutable record — the pinned binding,
// the durable runtime mapping and the immutable attempt — and a router that
// rewrote one would be substituting its own answer for the store's. Only the
// routing binding is read, and nothing else is touched.
//
// An unregistered binding is refused and NEVER answered empty. The released
// contract says a reader must NEVER report a missing record as an empty
// DispositionEvidence, because absence is not a disposition and the zero value
// is refused precisely so a reader which did so settles nothing. A router is
// bound by the same rule: the command is left unsettled and an operator is told
// which binding has no reader.
func (r *EvidenceRouter) ReadDispositionEvidence(
	ctx context.Context, req sessionstore.DispositionEvidenceRequest,
) (sessionstore.DispositionEvidence, error) {
	reader, served := r.readers[req.Binding.StorageBindingID]
	if !served {
		return sessionstore.DispositionEvidence{}, &UnroutableBindingError{
			Binding:   req.Binding.StorageBindingID,
			CommandID: string(req.CommandID),
		}
	}
	return reader.ReadDispositionEvidence(ctx, req)
}

// Bindings reports the storage bindings this router serves, sorted, so a
// composition can log what it can settle for at startup rather than at the first
// command that cannot be.
func (r *EvidenceRouter) Bindings() []string {
	bindings := make([]string, 0, len(r.readers))
	for binding := range r.readers {
		bindings = append(bindings, binding)
	}
	slices.Sort(bindings)
	return bindings
}

// ErrUnroutableBinding is the sentinel an unroutable binding carries.
//
// IT IS A SENTINEL AS WELL AS A TYPE because the thing a caller usually wants to
// know is "was this a configuration gap or a real journal failure", and that is
// a yes-or-no question. The type carries which binding, for the operator who has
// to fix it.
var ErrUnroutableBinding = errors.New("sessionstoreadapter: no settlement evidence reader is registered for this session's storage binding")

// UnroutableBindingError names the binding no reader serves. TenantID is set
// when the router that refused was tenant-keyed (TenantEvidenceRouter), and
// empty otherwise.
type UnroutableBindingError struct {
	TenantID  string
	Binding   string
	CommandID string
}

func (e *UnroutableBindingError) Error() string {
	return ErrUnroutableBinding.Error() + " (binding " + strconv.Quote(e.Binding) +
		tenantClause(e.TenantID) + ", command " + strconv.Quote(e.CommandID) + ")"
}

func (e *UnroutableBindingError) Unwrap() error { return ErrUnroutableBinding }

// InvalidEvidenceTableError reports a routing table that could not answer.
type InvalidEvidenceTableError struct {
	Binding string
	Reason  string
}

func (e *InvalidEvidenceTableError) Error() string {
	message := "sessionstoreadapter: the settlement evidence routing table is unusable"
	if e.Binding != "" {
		message += " at binding " + strconv.Quote(e.Binding)
	}
	return message + ": " + e.Reason
}
