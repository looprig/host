package hostlink_test

import (
	"errors"
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// O6.2 step 2 — HostLink is the same in both modes
// ---------------------------------------------------------------------------
//
// 04-host.md step 2 requires dedicated mode to "use the same lease, registry,
// inbox, HostLink, checkpoint, and release behavior as pooled mode". The other
// five are asserted in internal/residency/dedicated_test.go; HostLink is here
// because the multiplexer is the only place FixedSessionID reaches this package.
//
// WHAT FixedSessionID ACTUALLY DOES HERE, audited rather than assumed. It has
// exactly two production readers: NewMultiplexer validates its SHAPE
// (bindings.go:436), and drain.go:292-300 scopes a session-scoped drain request
// to it. Bind, Unbind, Deliver, MaySubscribe, CloseLink and InvalidateSession do
// not consult it at all. So a dedicated multiplexer is not a second routing
// regime; it is the pooled one plus a drain scope, and THAT is the property
// worth locking in.
//
// It follows that this file asserts nothing about binding a session OTHER than
// the fixed one over a dedicated multiplexer. A non-fixed session cannot be
// resident on a dedicated Host — internal/residency refuses the attach — and
// Bind refuses anything that is not resident, so the input is unreachable and an
// assertion about it would be a claim about a state no deployment can produce.
// It is defended transitively by residency, not by the multiplexer. That is
// recorded here rather than tested, because testing it would fix an arbitrary
// answer to a question the design has not been asked.

// hostLinkObservation is everything one scenario made observable through the
// multiplexer, gathered so the POOLED and DEDICATED runs can be compared to each
// other rather than each to a literal.
//
// The comparison is the assertion. Two per-mode assertions would both keep
// passing if either mode drifted; equality between the runs breaks on a
// divergence in either direction, including a field nobody thought to check —
// adding a field to this struct puts it under the sameness claim automatically.
type hostLinkObservation struct {
	Binding        hostlink.Binding
	Rebound        hostlink.Binding
	RebindRefusal  string
	DeliverRefusal string
	Hints          []sessionwire.CommandID
	MaySubscribe   bool
	MayOther       bool
	LenAfterBind   int
	Replicas       []hostlink.LinkID
	LinkBindings   []hostlink.Binding
	Closed         []hostlink.Binding
	LenAfterClose  int
	UnbindRefusal  string
}

// observeHostLink drives one full bind/deliver/close cycle for the session a
// dedicated Host would be bound to, over a multiplexer built with or without a
// FixedSessionID.
func observeHostLink(t *testing.T, fixed sessionwire.SessionID) hostLinkObservation {
	t.Helper()
	f := newFixture(t, func(o *hostlink.MultiplexerOptions) { o.FixedSessionID = fixed })

	const link = hostlink.LinkID("link-a")
	observation := hostLinkObservation{}

	binding, err := f.mux.Bind(link, bindRequest(testSession))
	if err != nil {
		t.Fatalf("bind %q: %v", testSession, err)
	}
	observation.Binding = binding
	observation.LenAfterBind = f.mux.Len()
	observation.MaySubscribe = f.mux.MaySubscribe(link, binding.Channel)
	observation.MayOther = f.mux.MaySubscribe(link, "some/other/channel")
	observation.Replicas = f.mux.Replicas(registry.Key{TenantID: testTenant, SessionID: testSession})
	observation.LinkBindings = f.mux.LinkBindings(link)

	// A REBIND, because a retry is the ordinary case and it re-validates rather
	// than replaying: if a dedicated multiplexer ever started short-circuiting
	// "my one session is already bound", this is where it would show.
	rebound, err := f.mux.Bind(link, bindRequest(testSession))
	if err != nil {
		observation.RebindRefusal = refusalText(err)
	} else {
		observation.Rebound = rebound
	}

	if err := f.mux.Deliver(link, binding.Channel, sessionwire.HostLinkCommandDelivery{CommandID: "command-one"}); err != nil {
		observation.DeliverRefusal = refusalText(err)
	}
	observation.Hints = f.consumers.consumers[residencyKey(testSession)].recorded()

	observation.Closed = f.mux.CloseLink(link)
	observation.LenAfterClose = f.mux.Len()
	// The unbind AFTER the close is deliberate: it exercises the refusal arm,
	// so the comparison covers a rejection and not only acceptances.
	if err := f.mux.Unbind(link, unbindRequest(testSession)); err != nil {
		observation.UnbindRefusal = refusalText(err)
	}
	return observation
}

// refusalText reduces a BindError to the two fields a caller branches on, so the
// comparison is over the classification rather than over a sentence that will be
// reworded.
func refusalText(err error) string {
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		return "non-BindError: " + err.Error()
	}
	return string(refusal.Refusal)
}

// TestHostLinkBehavesIdenticallyAtEitherPlacement is step 2's HostLink clause.
//
// The dedicated arm's FixedSessionID names the very session being bound, which
// is the only configuration a dedicated Host can be in. The two runs must be
// indistinguishable through every exported accessor the cycle touches.
func TestHostLinkBehavesIdenticallyAtEitherPlacement(t *testing.T) {
	pooled := observeHostLink(t, "")
	dedicated := observeHostLink(t, testSession)

	// The pooled arm must have actually done something. Without this, a
	// multiplexer that refused every bind would make the two runs trivially
	// equal and the comparison would assert nothing.
	if pooled.Binding.Channel == "" || pooled.LenAfterBind != 1 || len(pooled.Hints) != 1 {
		t.Fatalf("the pooled arm did not complete a bind/deliver cycle: %+v", pooled)
	}
	if pooled.DeliverRefusal != "" || pooled.RebindRefusal != "" {
		t.Fatalf("the pooled arm was refused: deliver %q, rebind %q", pooled.DeliverRefusal, pooled.RebindRefusal)
	}
	if !reflect.DeepEqual(pooled, dedicated) {
		t.Errorf("HostLink diverged between the placements:\n pooled    %+v\n dedicated %+v", pooled, dedicated)
	}
}

// TestOnlyDrainReadsTheFixedSessionOnTheMultiplexer is the AUDIT of the
// paragraph at the top of this file, kept honest by the compiler rather than by
// a reader's memory.
//
// It is a behavioural probe, not a source scan: a dedicated multiplexer whose
// fixed session is a session that is NOT the one being bound must still bind,
// then deliver, then close exactly as a pooled one does. If someone later
// teaches Bind or Deliver to consult FixedSessionID, this test fails and the
// comment above has to be rewritten in the same edit — which is the only
// mechanism that keeps a comment from outliving the code it describes.
//
// It does NOT claim the input is reachable. It claims that TODAY nothing on the
// routing path reads the field, which is exactly what makes the sameness above a
// statement about the code and not about one lucky configuration.
func TestOnlyDrainReadsTheFixedSessionOnTheMultiplexer(t *testing.T) {
	bound := observeHostLink(t, testSession)
	unrelated := observeHostLink(t, otherSession)

	// Neither arm may be vacuous. Two multiplexers that both refused everything
	// are equal, and the comparison below would then be satisfied by exactly the
	// regression it is looking for.
	if bound.LenAfterBind != 1 || len(bound.Hints) != 1 || bound.DeliverRefusal != "" {
		t.Fatalf("the fixed-session arm did not complete a bind/deliver cycle: %+v", bound)
	}
	if !reflect.DeepEqual(bound, unrelated) {
		t.Errorf("the routing path read FixedSessionID:\n fixed=%q %+v\n fixed=%q %+v", testSession, bound, otherSession, unrelated)
	}
}
