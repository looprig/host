package hostconfig

import (
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
)

// Host is the tenant-scoped Department runtime host.
//
// It holds a RESOLVED configuration: every option was validated once at
// construction and nothing here changes afterwards. Residency, command
// consumption, HostLink and drain are separate tasks; what this type fixes is
// that a Host which exists is a Host whose configuration was coherent.
//
// The fields are unexported and the accessors return values. Nothing mutable
// is handed out, and nothing the caller still holds can reach in: Options is
// taken BY VALUE, so a caller mutating its own copy afterwards changes nothing
// here. The two reference-typed fields are deliberate exceptions with reasons
// rather than oversights — Department is immutable by construction, so copying
// it would buy nothing, and the collaborator interfaces must be shared to be
// useful at all. Copying a SessionStore is not a defence; it is a different
// SessionStore.
type Host struct {
	id               sessionwire.HostID
	internalEndpoint sessionwire.InternalEndpoint
	isolationClass   sessionwire.HostIsolationClass

	department   *department.Department
	sessionStore SessionStore
	workspaces   WorkspaceProvider
	clock        Clock
	auth         AuthVerifier

	placement      sessionwire.HostPlacement
	capacity       uint64
	fixedSessionID sessionwire.SessionID

	warmTTL           time.Duration
	registryHeartbeat time.Duration
	registryExpiry    time.Duration
	claimTTL          time.Duration
	applyDeadline     time.Duration
	commandQueueSize  int
	reconcileInterval time.Duration
	reconcileBatch    int
}

// New validates options and returns a Host.
//
// It returns *InvalidOptionsError, carrying a stable OptionErrorCode, for every
// refusal. A Host with an incoherent configuration is never returned: there is
// no partially valid Host and no package-global default to fall back on, so a
// caller that ignores the error has nothing to use.
func New(options Options) (*Host, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	return &Host{
		id:                options.HostID,
		internalEndpoint:  options.InternalEndpoint,
		isolationClass:    options.IsolationClass,
		department:        options.Department,
		sessionStore:      options.SessionStore,
		workspaces:        options.Workspaces,
		clock:             options.Clock,
		auth:              options.Auth,
		placement:         options.Placement,
		capacity:          options.Capacity,
		fixedSessionID:    options.FixedSessionID,
		warmTTL:           options.WarmTTL,
		registryHeartbeat: options.RegistryHeartbeat,
		registryExpiry:    options.RegistryExpiry,
		claimTTL:          options.ClaimTTL,
		applyDeadline:     options.ApplyDeadline,
		commandQueueSize:  options.CommandQueueSize,
		reconcileInterval: options.ReconcileInterval,
		reconcileBatch:    options.ReconcileBatch,
	}, nil
}

// ID returns the Host's identity.
func (h *Host) ID() sessionwire.HostID { return h.id }

// InternalEndpoint returns the HostLink base address this Host advertises.
func (h *Host) InternalEndpoint() sessionwire.InternalEndpoint { return h.internalEndpoint }

// IsolationClass returns the advertised cross-tenant boundary, which is also
// this Host's pooled admission rule. There is no TenantID accessor beside it:
// see Options.IsolationClass for H8.
func (h *Host) IsolationClass() sessionwire.HostIsolationClass { return h.isolationClass }

// Department returns the immutable set of launch targets this Host serves.
func (h *Host) Department() *department.Department { return h.department }

// Placement returns the admission model this Host runs under.
func (h *Host) Placement() sessionwire.HostPlacement { return h.placement }

// Capacity returns how many sessions this Host may hold resident.
func (h *Host) Capacity() uint64 { return h.capacity }

// FixedSessionID returns the session a dedicated Host is bound to, or empty for
// a pooled Host.
func (h *Host) FixedSessionID() sessionwire.SessionID { return h.fixedSessionID }

// WarmTTL returns how long a released session stays warm before drain.
func (h *Host) WarmTTL() time.Duration { return h.warmTTL }

// RegistryHeartbeat returns the advertisement refresh interval.
func (h *Host) RegistryHeartbeat() time.Duration { return h.registryHeartbeat }

// RegistryExpiry returns how long an advertisement survives without a refresh.
func (h *Host) RegistryExpiry() time.Duration { return h.registryExpiry }

// ClaimTTL returns how long a command claim is held.
func (h *Host) ClaimTTL() time.Duration { return h.claimTTL }

// ApplyDeadline returns the outer bound for applying an admitted command.
func (h *Host) ApplyDeadline() time.Duration { return h.applyDeadline }

// CommandQueueSize returns the inbound command buffer size.
func (h *Host) CommandQueueSize() int { return h.commandQueueSize }

// ReconcileInterval returns how often reconciliation runs.
func (h *Host) ReconcileInterval() time.Duration { return h.reconcileInterval }

// ReconcileBatch returns the bound on one reconciliation pass.
func (h *Host) ReconcileBatch() int { return h.reconcileBatch }

// SessionStore returns the durable session state collaborator.
func (h *Host) SessionStore() SessionStore { return h.sessionStore }

// Workspaces returns the workspace collaborator. It does not checkpoint; see
// WorkspaceProvider.
func (h *Host) Workspaces() WorkspaceProvider { return h.workspaces }

// Clock returns the time source.
func (h *Host) Clock() Clock { return h.clock }

// Auth returns the credential verifier.
func (h *Host) Auth() AuthVerifier { return h.auth }
