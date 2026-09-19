package hostconfig_test

import (
	"errors"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
)

// TestInternalEndpointIsABase holds host v0.3.0's reading of InternalEndpoint:
// it is the BASE every tenant's HostLink address is derived from by Core's
// sessionwire.HostLinkEndpoint, and a value that function would refuse — or one
// it accepts but no client could ever dial — is refused at construction, where
// the operator is still holding the configuration, rather than surfacing as a
// Factory dial failure for every tenant.
//
// EVERY REFUSAL ROW NAMES ITS OWN REASON, and the accepted rows are the
// controls: each is a legal base of a different shape (scheme, host form, port,
// trailing slash), so a rule that refuses more than it names fails here too.
func TestInternalEndpointIsABase(t *testing.T) {
	t.Parallel()

	// A hostname long enough that the base still validates (it is under
	// MaxIDBytes) but even a one-byte tenant cannot be appended to it.
	tooLongForAnyTenant := sessionwire.InternalEndpoint("ws://" + strings.Repeat("h", sessionwire.MaxIDBytes-len("ws://")-len(sessionwire.HostLinkPathPrefix)))
	if err := tooLongForAnyTenant.Validate(); err != nil {
		t.Fatalf("control: the too-long base must itself validate, got %v", err)
	}

	type refusal struct {
		code        sessionwire.HostLinkEndpointCode // set for Core's base refusals
		undiallable bool                             // set for Host's dialability refusals
	}
	for _, tt := range []struct {
		name     string
		endpoint sessionwire.InternalEndpoint
		refused  *refusal
	}{
		// Accepted: bare bases of every shape.
		{name: "ws name, no port", endpoint: "ws://host"},
		{name: "trailing slash", endpoint: "ws://host/"},
		{name: "wss name and port", endpoint: "wss://host-7c1.internal.example:8443"},
		{name: "ipv4 and port", endpoint: "ws://10.0.4.7:9000"},
		{name: "ipv6 and port, trailing slash", endpoint: "ws://[2001:db8::1]:9000/"},
		{name: "ipv6, no port", endpoint: "ws://[::1]"},
		{name: "lowest port", endpoint: "ws://host:1"},
		{name: "highest port", endpoint: "ws://host:65535"},
		{name: "leading-zero port", endpoint: "ws://host:080"},

		// Core's three base refusals.
		{name: "invalid_base: wrong scheme", endpoint: "http://host", refused: &refusal{code: sessionwire.HostLinkEndpointCodeInvalidBase}},
		{name: "invalid_base: credential", endpoint: "ws://user:secret@host", refused: &refusal{code: sessionwire.HostLinkEndpointCodeInvalidBase}},
		{name: "invalid_base: query", endpoint: "ws://host?x=1", refused: &refusal{code: sessionwire.HostLinkEndpointCodeInvalidBase}},
		{name: "base_names_tenant: v0.2.1 prefix", endpoint: "ws://10.0.0.1:7100/hostlink", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNamesTenant}},
		{name: "base_names_tenant: prefix with slash", endpoint: "ws://host/hostlink/", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNamesTenant}},
		{name: "base_names_tenant: per-tenant endpoint", endpoint: "ws://host/hostlink/tenant-a", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNamesTenant}},
		{name: "base_not_bare: other path", endpoint: "ws://10.0.4.7:9000/link", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNotBare}},
		{name: "base_not_bare: ingress prefix", endpoint: "wss://gw.example/pods/host-7/", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNotBare}},
		{name: "base_not_bare: empty fragment", endpoint: "ws://host#", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNotBare}},
		{name: "base_not_bare: case-folded prefix", endpoint: "ws://host/HOSTLINK/x", refused: &refusal{code: sessionwire.HostLinkEndpointCodeBaseNotBare}},
		{name: "too_long: no tenant fits", endpoint: tooLongForAnyTenant, refused: &refusal{code: sessionwire.HostLinkEndpointCodeTooLong}},

		// Host's own: authorities Validate and HostLinkEndpoint accept and no
		// client can dial (core spec F4).
		{name: "undiallable: empty port", endpoint: "ws://host:", refused: &refusal{undiallable: true}},
		{name: "undiallable: port above range", endpoint: "ws://host:99999", refused: &refusal{undiallable: true}},
		{name: "undiallable: port one above range", endpoint: "ws://host:65536", refused: &refusal{undiallable: true}},
		{name: "undiallable: port zero", endpoint: "ws://host:0", refused: &refusal{undiallable: true}},
		{name: "undiallable: two ports", endpoint: "ws://host:80:90", refused: &refusal{undiallable: true}},
		{name: "undiallable: ipv6 empty port", endpoint: "ws://[::1]:", refused: &refusal{undiallable: true}},
		{name: "undiallable: ipv6 port above range", endpoint: "ws://[::1]:70000", refused: &refusal{undiallable: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := pooledOptions(t)
			options.InternalEndpoint = tt.endpoint
			built, err := hostconfig.New(options)
			if tt.refused == nil {
				if err != nil {
					t.Fatalf("New(%q) = %v, want a legal base accepted", tt.endpoint, err)
				}
				if got := built.InternalEndpoint(); got != tt.endpoint {
					t.Fatalf("InternalEndpoint() = %q, want the configured base %q advertised unchanged", got, tt.endpoint)
				}
				return
			}
			var invalid *hostconfig.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("New(%q) = %v, want *InvalidOptionsError", tt.endpoint, err)
			}
			if invalid.Field != "InternalEndpoint" || invalid.Code != hostconfig.OptionErrorCodeInvalid {
				t.Fatalf("refusal = (%q, %q), want (InternalEndpoint, invalid)", invalid.Field, invalid.Code)
			}
			var derive *sessionwire.HostLinkEndpointError
			derived := errors.As(err, &derive)
			undiallable := errors.Is(err, hostconfig.ErrUndiallableEndpoint)
			if tt.refused.undiallable {
				if !undiallable || derived {
					t.Fatalf("New(%q) = %v, want ErrUndiallableEndpoint and no HostLinkEndpointError", tt.endpoint, err)
				}
				return
			}
			if !derived || undiallable {
				t.Fatalf("New(%q) = %v, want a *HostLinkEndpointError and not ErrUndiallableEndpoint", tt.endpoint, err)
			}
			if derive.Code != tt.refused.code {
				t.Fatalf("HostLinkEndpointError.Code = %q, want %q", derive.Code, tt.refused.code)
			}
		})
	}
}
