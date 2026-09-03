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

	// PingInterval and PongTimeout are optional but must be supplied together:
	// setting one without the other is rejected. When both are zero HostLink
	// sends no PingPongConfig at all, so the transport inherits Centrifuge's
	// own defaults of 25s ping and 10s pong (centrifuge@v0.38.0 config.go:218
	// and config.go:224) — zero does not mean "no heartbeat". When supplied,
	// PingInterval must be at least one second and PongTimeout must be
	// strictly shorter than PingInterval.
	PingInterval time.Duration
	PongTimeout  time.Duration
}

// Server is an embedded HostLink transport, mounted at the Host's private
// service endpoint; Close gracefully releases active connections.
type Server interface {
	// Handler serves only clients that select the JSON protocol explicitly,
	// by a sole Sec-WebSocket-Protocol field of centrifuge-json tokens or by a
	// format/cf_protocol query value of json. Anything else, including a
	// request that names no protocol at all, is answered with HTTP 400 before
	// the upgrade; see selectsJSONProtocol for the rule and its provenance.
	Handler() http.Handler

	Close(context.Context) error
}
