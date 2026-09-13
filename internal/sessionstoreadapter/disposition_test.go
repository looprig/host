package sessionstoreadapter

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/host/internal/residency"
)

// THE WRITER FACTORY REFUSES A LEASE IT DID NOT ISSUE, and the refusal is the
// whole of what keeps the residency grant from being bypassed.
//
// sessionstore's ClaimDispositionCommand takes a *ResidencyGrant and derives the
// residency epoch from it — "the epoch comes off the grant and from nowhere
// else, so no number a caller chose can reach the record's mark" — and the grant
// is PRIVATE to this package's ResidencyLease. A foreign residency.Lease
// therefore carries no grant at all, and a factory that accepted one would
// produce a writer whose every claim is refused, at the first command of a
// session this Host had already taken residency of.
//
// THREE ROWS, because there are three separable ways to arrive with no grant and
// the comparison is a disjunction: a lease of another type, a nil interface, and
// a lease of the right type holding no grant. A mutant disabling the guard
// SURVIVED before these existed.
func TestDispositionWriterForRefusesALeaseThisStoreDidNotIssue(t *testing.T) {
	adapted := &Store{}
	for _, row := range []struct {
		name  string
		lease residency.Lease
	}{
		{"a lease of another implementation", foreignLease{}},
		{"no lease at all", nil},
		{"this package's lease holding no grant", &ResidencyLease{}},
	} {
		t.Run(row.name, func(t *testing.T) {
			writer, err := adapted.DispositionWriterFor(row.lease)
			if !errors.Is(err, ErrForeignLease) {
				t.Fatalf("DispositionWriterFor = %v, want ErrForeignLease", err)
			}
			// AND IT RETURNS A REAL NIL. A typed nil here would be a non-nil
			// interface holding nothing, and the composition's own check is
			// `writer == nil` — so the refusal would be read as a writer and
			// dereferenced at the first claim.
			if writer != nil {
				t.Errorf("DispositionWriterFor returned %T beside its refusal", writer)
			}
		})
	}
}

// foreignLease is a residency.Lease this package did not issue.
type foreignLease struct{}

func (foreignLease) Epoch() residency.ResidencyEpoch { return 1 }
func (foreignLease) Lost() <-chan struct{}           { return nil }
func (foreignLease) Release(context.Context) error   { return nil }
