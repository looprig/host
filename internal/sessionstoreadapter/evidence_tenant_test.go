package sessionstoreadapter

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/sessionstore"
)

// THE TENANT IS HALF OF THE ROUTE, and the two readers below differ ONLY in the
// tenant, so a router that read the binding alone could not tell them apart.
func TestTenantEvidenceRouterRoutesOnTenantAndBinding(t *testing.T) {
	tenantA := &stubEvidenceReader{name: "a"}
	tenantB := &stubEvidenceReader{name: "b"}
	router, err := NewTenantEvidenceRouter(map[EvidenceKey]sessionstore.DispositionEvidenceReader{
		{TenantID: "tenant-a", StorageBindingID: "binding-1"}: tenantA,
		{TenantID: "tenant-b", StorageBindingID: "binding-1"}: tenantB,
	})
	if err != nil {
		t.Fatalf("NewTenantEvidenceRouter: %v", err)
	}
	request := requestFor("binding-1")
	request.TenantID = "tenant-b"
	if _, err := router.ReadDispositionEvidence(t.Context(), request); err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(tenantB.seen) != 1 || len(tenantA.seen) != 0 {
		t.Fatalf("tenant-b's request reached a=%d b=%d readers, want only b", len(tenantA.seen), len(tenantB.seen))
	}
	if tenantB.seen[0].TenantID != "tenant-b" || tenantB.seen[0].Binding.StorageBindingID != "binding-1" {
		t.Fatalf("the request was rewritten on the way: %+v", tenantB.seen[0])
	}

	// A tenant with no reader under a binding another tenant DOES have is
	// refused, naming the tenant: that is condition (b) diagnosed at the
	// router instead of at the journal store's own refusal.
	request.TenantID = "tenant-c"
	_, err = router.ReadDispositionEvidence(t.Context(), request)
	var unroutable *UnroutableBindingError
	if !errors.As(err, &unroutable) || !errors.Is(err, ErrUnroutableBinding) {
		t.Fatalf("an unregistered tenant was answered %v, want *UnroutableBindingError", err)
	}
	if unroutable.TenantID != "tenant-c" || unroutable.Binding != "binding-1" || !strings.Contains(err.Error(), `"tenant-c"`) {
		t.Fatalf("the refusal names %+v (%v), want tenant-c under binding-1", unroutable, err)
	}
	if keys := router.Keys(); len(keys) != 2 || keys[0].TenantID != "tenant-a" || keys[1].TenantID != "tenant-b" {
		t.Fatalf("Keys = %v, want the two registered pairs sorted by tenant", keys)
	}
}

func TestNewTenantEvidenceRouterRefusesAnEmptyOrMalformedTable(t *testing.T) {
	reader := &stubEvidenceReader{}
	for name, table := range map[string]map[EvidenceKey]sessionstore.DispositionEvidenceReader{
		"empty":         {},
		"empty tenant":  {{TenantID: "", StorageBindingID: "binding-1"}: reader},
		"bad tenant":    {{TenantID: "\xff", StorageBindingID: "binding-1"}: reader},
		"empty binding": {{TenantID: "tenant-a", StorageBindingID: ""}: reader},
		"nil reader":    {{TenantID: "tenant-a", StorageBindingID: "binding-1"}: nil},
	} {
		router, err := NewTenantEvidenceRouter(table)
		var invalid *InvalidEvidenceTableError
		if !errors.As(err, &invalid) || router != nil {
			t.Errorf("%s: NewTenantEvidenceRouter = (%v, %v), want *InvalidEvidenceTableError and no router", name, router, err)
		}
	}
}

// The table is copied: a caller mutating its map afterwards re-points nothing.
func TestTenantEvidenceRouterCopiesItsTable(t *testing.T) {
	reader := &stubEvidenceReader{}
	table := map[EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: "tenant-a", StorageBindingID: "binding-1"}: reader}
	router, err := NewTenantEvidenceRouter(table)
	if err != nil {
		t.Fatalf("NewTenantEvidenceRouter: %v", err)
	}
	delete(table, EvidenceKey{TenantID: "tenant-a", StorageBindingID: "binding-1"})
	if _, err := router.ReadDispositionEvidence(t.Context(), requestFor("binding-1")); err != nil {
		t.Fatalf("the router lost its reader when the caller's map changed: %v", err)
	}
}
