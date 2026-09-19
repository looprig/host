package sessionstoreadapter

import (
	"errors"
	"testing"

	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/residency"
)

// TestClassifyGateWriteNamesOnlyItsOwnThreeAnswers (spec gate S11): only a
// tombstoned GATE INTENT is unpublishable and only the open-gate cap is a full
// projection; the same codes on another field, and every other code, pass
// through unclassified.
func TestClassifyGateWriteNamesOnlyItsOwnThreeAnswers(t *testing.T) {
	for _, row := range []struct {
		name string
		err  error
		want error
	}{
		{"epoch", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorEpoch}, residency.ErrEpochSuperseded},
		{"deleted gate intent", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted, Field: "gate_intent"}, gates.ErrUnpublishable},
		{"too many open gates", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorTooLarge, Field: "open_gates"}, gates.ErrProjectionFull},
		{"deleted catalog record", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted, Field: "catalog"}, nil},
		{"too large elsewhere", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorTooLarge, Field: "gate"}, nil},
		{"conflict", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorConflict, Field: "gate_id"}, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			got := classifyGateWrite(row.err)
			for _, sentinel := range []error{residency.ErrEpochSuperseded, gates.ErrUnpublishable, gates.ErrProjectionFull} {
				if errors.Is(got, sentinel) != (sentinel == row.want) {
					t.Errorf("classifyGateWrite(%v) is %v: %v, want only %v", row.err, sentinel, errors.Is(got, sentinel), row.want)
				}
			}
			if !errors.Is(got, row.err) {
				t.Errorf("the store's own error did not survive")
			}
		})
	}
}
