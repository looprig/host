package host

import (
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	hostconfig "github.com/looprig/host/internal/hostconfig"
)

// Host is the tenant-scoped Department runtime host.
//
// It holds a RESOLVED configuration: every option was validated once at
// construction and nothing here changes afterwards. Residency, command
// consumption, HostLink and drain are separate concerns; what this type fixes
// is that a Host which exists is a Host whose configuration was coherent.
//
// The fields are unexported and the accessors return values. Nothing mutable
// is handed out, and nothing the caller still holds can reach in: Options is
// taken BY VALUE, so a caller mutating its own copy afterwards changes nothing
// here. Department and the collaborator interfaces are shared by design —
// Department is immutable by construction, and a copied SessionStore would be
// a different SessionStore.
//
// SINCE v0.2.0 THE RESOLVED CONFIGURATION LIVES IN internal/hostconfig AND
// THIS TYPE CARRIES IT. Every package under internal/ consumes the
// configuration, and this package now also exports the composition that runs
// it (Compose), which imports those packages; a leaf both sides import cannot
// live in either, so it moved down and this type became the exported carrier.
// Nothing a v0.1.0 caller could see has changed: the method set, the Options
// fields, the error codes and the constants are the same names with the same
// values, held to the internal ones by test rather than by aliasing — an alias
// would have changed the exported type's identity, which is a break.
type Host struct {
	resolved *hostconfig.Host
}

// New validates options and returns a Host.
//
// It returns *InvalidOptionsError, carrying a stable OptionErrorCode, for every
// refusal. A Host with an incoherent configuration is never returned: there is
// no partially valid Host and no package-global default to fall back on, so a
// caller that ignores the error has nothing to use. The rules themselves are
// internal/hostconfig's, and every one of them reaches this constructor;
// TestNewDelegatesEveryRefusal holds that.
func New(options Options) (*Host, error) {
	resolved, err := hostconfig.New(options.resolved())
	if err != nil {
		return nil, exportOptionsError(err)
	}
	return &Host{resolved: resolved}, nil
}

// ID returns the Host's identity.
func (h *Host) ID() sessionwire.HostID { return h.resolved.ID() }

// InternalEndpoint returns the address Factory dials for HostLink.
func (h *Host) InternalEndpoint() sessionwire.InternalEndpoint { return h.resolved.InternalEndpoint() }

// IsolationClass returns the advertised cross-tenant boundary, which is also
// this Host's pooled admission rule. There is no TenantID accessor beside it:
// see Options.IsolationClass for H8.
func (h *Host) IsolationClass() sessionwire.HostIsolationClass { return h.resolved.IsolationClass() }

// Department returns the immutable set of launch targets this Host serves.
func (h *Host) Department() *department.Department { return h.resolved.Department() }

// Placement returns the admission model this Host runs under.
func (h *Host) Placement() sessionwire.HostPlacement { return h.resolved.Placement() }

// Capacity returns how many sessions this Host may hold resident.
func (h *Host) Capacity() uint64 { return h.resolved.Capacity() }

// FixedSessionID returns the session a dedicated Host is bound to, or empty for
// a pooled Host.
func (h *Host) FixedSessionID() sessionwire.SessionID { return h.resolved.FixedSessionID() }

// WarmTTL returns how long a released session stays warm before drain.
func (h *Host) WarmTTL() time.Duration { return h.resolved.WarmTTL() }

// RegistryHeartbeat returns the advertisement refresh interval.
func (h *Host) RegistryHeartbeat() time.Duration { return h.resolved.RegistryHeartbeat() }

// RegistryExpiry returns how long an advertisement survives without a refresh.
func (h *Host) RegistryExpiry() time.Duration { return h.resolved.RegistryExpiry() }

// ClaimTTL returns how long a command claim is held.
func (h *Host) ClaimTTL() time.Duration { return h.resolved.ClaimTTL() }

// ApplyDeadline returns the outer bound for applying an admitted command.
func (h *Host) ApplyDeadline() time.Duration { return h.resolved.ApplyDeadline() }

// CommandQueueSize returns the inbound command buffer size.
func (h *Host) CommandQueueSize() int { return h.resolved.CommandQueueSize() }

// ReconcileInterval returns how often reconciliation runs.
func (h *Host) ReconcileInterval() time.Duration { return h.resolved.ReconcileInterval() }

// ReconcileBatch returns the bound on one reconciliation pass.
func (h *Host) ReconcileBatch() int { return h.resolved.ReconcileBatch() }

// SessionStore returns the durable session state collaborator.
func (h *Host) SessionStore() SessionStore { return h.resolved.SessionStore() }

// Workspaces returns the workspace collaborator. It does not checkpoint; see
// WorkspaceProvider.
func (h *Host) Workspaces() WorkspaceProvider { return h.resolved.Workspaces() }

// Clock returns the time source.
func (h *Host) Clock() Clock { return h.resolved.Clock() }

// Auth returns the credential verifier.
func (h *Host) Auth() AuthVerifier { return h.resolved.Auth() }
