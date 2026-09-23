package compose

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/hostconfig"
)

// ---------------------------------------------------------------------------
// Every runtime_mismatch an attach can earn is a record Core can encode
// ---------------------------------------------------------------------------
//
// Core requires runtime_mismatch to carry runtime_compatibility_id, and its
// encoder refuses the record without one. Up to host v0.7.0 no attach refusal
// carried it, so every runtime_mismatch the residency manager decided — the
// dedicated Host's "not my fixed session" (D3.1 F1) among them — failed to
// encode and left at the transport as Centrifuge 107 bad request, which a
// Factory reads as its own malformed request. These drive the whole composed
// path over a real link and decode the reply with Core's own decoder.

// requireRuntimeMismatchWithHostBuild asserts the refusal is a Core-decodable
// runtime_mismatch naming the build THIS Host launches.
func requireRuntimeMismatchWithHostBuild(t *testing.T, refusal sessionwire.HostLinkError) {
	t.Helper()
	if refusal.Code != sessionwire.HostLinkErrorRuntimeMismatch {
		t.Fatalf("refusal class = %q, want runtime_mismatch", refusal.Code)
	}
	if refusal.RuntimeCompatibilityID != string(testCompat) {
		t.Fatalf("runtime_compatibility_id = %q, want this Host's build %q", refusal.RuntimeCompatibilityID, testCompat)
	}
}

func TestADedicatedHostRefusesAnotherSessionWithAnEncodableRuntimeMismatch(t *testing.T) {
	f := newFixture(t, func(_ *Options, options *hostconfig.Options) {
		options.Placement = sessionwire.HostPlacementDedicated
		options.FixedSessionID = sessionA
		options.Capacity = 1
	})
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	refusal := refusedAttachOverLink(t, link, 2, f.attachRequest(tenantA, "session-not-mine"))
	requireRuntimeMismatchWithHostBuild(t, refusal)
	if got := f.trace.indexOf("lease.acquire"); got != -1 {
		t.Fatalf("the refused foreign session reached the lease store (trace %v)", f.trace.trace())
	}

	// THE CONTROL: the fixed session itself attaches on the same link.
	attachedObservation(t, link, 3, f.attachRequest(tenantA, sessionA))
}

func TestAColdAttachPlacedOnAnotherBuildIsRefusedWithAnEncodableRuntimeMismatch(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	request := f.attachRequest(tenantA, sessionA)
	request.RuntimeCompatibilityID = "runtime-other"
	requireRuntimeMismatchWithHostBuild(t, refusedAttachOverLink(t, link, 2, request))
}

func TestAWarmAttachPlacedOnAnotherBuildIsRefusedWithAnEncodableRuntimeMismatch(t *testing.T) {
	f := newFixture(t)
	f.start()
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	attachedObservation(t, link, 2, f.attachRequest(tenantA, sessionA))
	request := f.attachRequest(tenantA, sessionA)
	request.RuntimeCompatibilityID = "runtime-other"
	requireRuntimeMismatchWithHostBuild(t, refusedAttachOverLink(t, link, 3, request))
}
