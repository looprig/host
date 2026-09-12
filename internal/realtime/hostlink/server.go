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
	// sends no PingPongConfig at all and the heartbeat falls to Centrifuge's
	// own application-level defaults of 25s ping and 10s pong
	// (centrifuge@v0.38.0 config.go:218 and config.go:224) — zero does not mean
	// "no heartbeat". That route held one exception until O5.2 removed it:
	// cf_ws_frame_ping_pong would have moved a zero-configured link onto
	// WebSocket frame ping instead, and rejectedTransportQueryKeys now refuses
	// the key before the upgrade. When supplied, PingInterval must be at least
	// one second and PongTimeout must be strictly shorter than PingInterval.
	PingInterval time.Duration
	PongTimeout  time.Duration

	// Multiplexer is the routing table this server's links bind through. It is
	// optional: a server built without one still authenticates, negotiates and
	// heartbeats, and answers every RPC and subscribe with Centrifuge's own
	// unhandled-command refusal.
	Multiplexer *Multiplexer
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

	// Publish puts one payload on one channel.
	//
	// IT REPORTS TRANSPORT ACCEPTANCE AND NOT DELIVERY. A channel nobody has
	// subscribed is not an error: a Factory link that has dropped and not yet
	// reconnected is an ordinary state, and a transport that refused it would
	// make an idle route indistinguishable from a broken one to the caller that
	// decides whether to invalidate it.
	//
	// It is the production side of the seam the live event relay declares. That
	// seam takes a channel and a payload and nothing else, so this signature
	// carries no link, no replica set and no context — the fan-out belongs to
	// the transport, which owns a queue per physical connection, and Host owns
	// the decision to publish once.
	Publish(channel string, payload []byte) error

	Close(context.Context) error
}
