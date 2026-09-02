package host

import (
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Host is the tenant-scoped Department runtime host.
//
// It is a placeholder: it holds its identity and nothing else. Department
// registration, residency, command consumption and HostLink are separate
// tasks, and this type exists so the module compiles, ships its checks, and
// fixes the constructor's validation contract before any of them land.
type Host struct {
	id     sessionwire.HostID
	tenant sessionwire.TenantID
}

// New validates options and returns a Host. It returns *InvalidOptionsError
// for a missing identity; a Host with an unusable identity is never returned.
func New(options Options) (*Host, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	return &Host{id: options.HostID, tenant: options.TenantID}, nil
}

// ID returns the Host's identity.
func (h *Host) ID() sessionwire.HostID { return h.id }

// TenantID returns the tenant every session on this Host belongs to.
func (h *Host) TenantID() sessionwire.TenantID { return h.tenant }
