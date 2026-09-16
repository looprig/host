package sessionstoreadapter

import (
	"context"
	"maps"
	"slices"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// EvidenceKey names the journal store that serves one tenant's sessions under
// one storage binding.
//
// THE TENANT IS HALF OF THE KEY AND THAT IS WHAT CLOSES CONDITION (b). A
// harness Store files ONE tenant's sessions for its whole life and refuses an
// evidence request naming another, and the orchestration tenant and the
// journal store's tenant are configured independently with no type relating
// them. A router keyed on the binding alone could hand tenant-b's settlement to
// tenant-a's journal store, which would refuse it — correctly, and too late,
// after the dispatch. Keying on both turns that misconfiguration into an
// UnroutableBindingError at settlement naming the tenant, and lets a
// composition refuse a table that names no reader for a tenant it serves.
type EvidenceKey struct {
	TenantID         sessionwire.TenantID
	StorageBindingID string
}

// TenantEvidenceRouter forwards each settlement evidence request to the reader
// registered for its tenant and storage binding.
type TenantEvidenceRouter struct {
	readers map[EvidenceKey]sessionstore.DispositionEvidenceReader
}

// TenantEvidenceRouter is the released reader seam.
var _ sessionstore.DispositionEvidenceReader = (*TenantEvidenceRouter)(nil)

// NewTenantEvidenceRouter builds a router over one reader per (tenant,
// binding). It copies the table, for the reason NewEvidenceRouter gives.
func NewTenantEvidenceRouter(readers map[EvidenceKey]sessionstore.DispositionEvidenceReader) (*TenantEvidenceRouter, error) {
	if len(readers) == 0 {
		return nil, &InvalidEvidenceTableError{Reason: "the table is empty, so every settlement in this deployment would fail at the evidence read"}
	}
	for key, reader := range readers {
		if err := key.TenantID.Validate(); err != nil {
			return nil, &InvalidEvidenceTableError{Binding: key.StorageBindingID, Reason: "a reader is registered under a tenant Core refuses (" + err.Error() + "), which no session can ever name"}
		}
		if key.StorageBindingID == "" {
			return nil, &InvalidEvidenceTableError{Reason: "a reader is registered under no storage binding, which no session's binding can ever name"}
		}
		if reader == nil {
			return nil, &InvalidEvidenceTableError{Binding: key.StorageBindingID, Reason: "the reader is nil, and a nil reader would panic at the first settlement rather than refuse at startup"}
		}
	}
	return &TenantEvidenceRouter{readers: maps.Clone(readers)}, nil
}

// ReadDispositionEvidence forwards the request, unchanged, to the reader that
// serves its tenant and binding; an unregistered pair is refused and never
// answered empty, for the reason EvidenceRouter.ReadDispositionEvidence gives.
func (r *TenantEvidenceRouter) ReadDispositionEvidence(
	ctx context.Context, req sessionstore.DispositionEvidenceRequest,
) (sessionstore.DispositionEvidence, error) {
	key := EvidenceKey{TenantID: req.TenantID, StorageBindingID: req.Binding.StorageBindingID}
	reader, served := r.readers[key]
	if !served {
		return sessionstore.DispositionEvidence{}, &UnroutableBindingError{
			TenantID:  string(req.TenantID),
			Binding:   req.Binding.StorageBindingID,
			CommandID: string(req.CommandID),
		}
	}
	return reader.ReadDispositionEvidence(ctx, req)
}

// Keys reports the (tenant, binding) pairs this router serves, sorted.
func (r *TenantEvidenceRouter) Keys() []EvidenceKey {
	keys := make([]EvidenceKey, 0, len(r.readers))
	for key := range r.readers {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b EvidenceKey) int {
		if a.TenantID != b.TenantID {
			if a.TenantID < b.TenantID {
				return -1
			}
			return 1
		}
		if a.StorageBindingID < b.StorageBindingID {
			return -1
		}
		if a.StorageBindingID > b.StorageBindingID {
			return 1
		}
		return 0
	})
	return keys
}

// tenantClause spells the tenant into an UnroutableBindingError's message when
// the router that produced it was tenant-keyed.
func tenantClause(tenant string) string {
	if tenant == "" {
		return ""
	}
	return ", tenant " + strconv.Quote(tenant)
}
