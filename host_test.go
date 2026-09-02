package host_test

import (
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
)

func TestNewRejectsMissingIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options host.Options
		field   string
	}{
		{name: "no host ID", options: host.Options{TenantID: "tenant"}, field: "HostID"},
		{name: "no tenant ID", options: host.Options{HostID: "host-1"}, field: "TenantID"},
		{name: "neither", options: host.Options{}, field: "HostID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := host.New(tt.options)
			if got != nil {
				t.Fatalf("New(%+v) returned a Host as well as %v", tt.options, err)
			}
			var invalid *host.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New(%+v) error = %v, want *host.InvalidOptionsError", tt.options, err)
			}
			if invalid.Field != tt.field {
				t.Errorf("error field = %q, want %q", invalid.Field, tt.field)
			}
		})
	}
}

func TestNewRetainsIdentity(t *testing.T) {
	t.Parallel()

	const (
		id     sessionwire.HostID   = "host-1"
		tenant sessionwire.TenantID = "tenant-1"
	)
	created, err := host.New(host.Options{HostID: id, TenantID: tenant})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if created.ID() != id {
		t.Errorf("ID() = %q, want %q", created.ID(), id)
	}
	if created.TenantID() != tenant {
		t.Errorf("TenantID() = %q, want %q", created.TenantID(), tenant)
	}
}
