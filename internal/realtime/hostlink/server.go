// Package hostlink owns the authenticated realtime server used by Factory to
// reach one Host. Its boundary is expressed in Looprig and standard-library
// types; the transport engine is private to this package.
package hostlink

import (
	"context"
	"net/http"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Authenticator verifies the service credential presented by a Factory when
// it opens a HostLink. Authentication happens for every physical connection,
// including reconnects.
type Authenticator interface {
	VerifyTenant(context.Context, sessionwire.TenantID, string) error
}

// Config contains the Looprig-owned inputs to the embedded server.
type Config struct {
	TenantID      sessionwire.TenantID
	Authenticator Authenticator
	PingInterval  time.Duration
	PongTimeout   time.Duration
}

// Server is an embedded HostLink transport. Handler is mounted at the Host's
// private service endpoint; Close gracefully releases active connections.
type Server interface {
	Handler() http.Handler
	Close(context.Context) error
}
