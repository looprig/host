package host

import (
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Options configures a Host. Both identities are required: a Host is always a
// named runtime acting for exactly one tenant, and neither is discoverable.
type Options struct {
	// HostID identifies this Host to Factory and in HostLink bindings.
	HostID sessionwire.HostID

	// TenantID scopes every session this Host may hold resident.
	TenantID sessionwire.TenantID
}

// InvalidOptionsError reports a required Host option that was absent or
// malformed.
type InvalidOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidOptionsError) Error() string {
	return "host: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

func (o Options) validate() error {
	if o.HostID == "" {
		return &InvalidOptionsError{Field: "HostID", Reason: "must not be empty"}
	}
	if o.TenantID == "" {
		return &InvalidOptionsError{Field: "TenantID", Reason: "must not be empty"}
	}
	return nil
}
