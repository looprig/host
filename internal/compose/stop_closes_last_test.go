package compose

import (
	"context"
	"errors"
	"net/http"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
)

// This file holds the half of the transport change that is NOT about the drain:
// where the close went, and that it is still last.
//
// The drain used to close every tenant transport as its final cleanup step.
// That destroyed the link carrying the terminal `drained` answer before
// settledState assigned it, and the Centrifuge 3001 disconnect a Factory was
// left with is identical whether release finished or FinishRelease refused —
// ambiguous between "delete the workload" and "this Host still holds the
// lease". Service.Stop closes them now, after Wait returns.
//
// TestStopReleasesTheSessionAndClosesEveryTransportLast in service_test.go is
// unchanged and still passes: "last" is still after every release step. This
// file adds the two things that test cannot see — that a drain alone closes
// NOTHING, and that a failure to close is still booked under StepCloseLink.

// TestALinkInitiatedDrainClosesNoTransportAndStopClosesThemAll is the
// before-and-after in one test, so the two halves cannot drift.
func TestALinkInitiatedDrainClosesNoTransportAndStopClosesThemAll(t *testing.T) {
	f := dedicatedDrainFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	if live := len(f.svc.links.all()); live != 1 {
		t.Fatalf("%d tenant links are live before the drain, want 1: there is no transport for the drain to leave alone", live)
	}

	_ = drainObservationOf(t, link.rpc(2, hostlink.MethodDrain, fixedSessionDrainRequest("close-none")))
	f.svc.drainer.Wait()

	// (a) THE DRAIN CLOSED NOTHING.
	if live := len(f.svc.links.all()); live != 1 {
		t.Errorf("%d tenant links survived a link-initiated drain, want 1: the drain closed a transport, which destroys the link carrying the terminal `drained` answer", live)
	}
	// And the endpoint still upgrades, which is the property a reconnecting
	// Factory actually depends on — a live map entry whose server had been shut
	// down would satisfy the count above.
	if !hostLinkHandshakeAccepted(t, server.URL, tenantA) {
		t.Error("the HostLink endpoint refused a handshake after a link-initiated drain, so the transport is shut down even though its map entry survives")
	}

	// AND THE PROCESS IS STILL ALIVE AND NOT READY, which is the state a
	// platform must see for the answer above to be deliverable at all. Ready
	// false stops new placements; Live true stops the platform killing the
	// process before a controller has read `drained` and deleted the workload.
	// Live()'s doc used to claim liveness turns false once release finished —
	// the code never did, and the code is the correct one.
	if f.svc.Ready() {
		t.Error("a drained Host still reports ready, so a placement controller may still target it")
	}
	if !f.svc.Live() {
		t.Error("a drained Host reports not live, so a platform may kill the process before a controller reads `drained` — reintroducing the disconnect-instead-of-an-answer as a SIGKILL")
	}

	// (b) STOP CLOSES THEM.
	report, err := f.svc.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if live := len(f.svc.links.all()); live != 0 {
		t.Errorf("%d tenant links survived Stop, want 0", live)
	}
	// AND THE REFUSAL NAMES THIS HOST'S CONDITION, not the caller's. A stopped
	// Host answered 400 "HostLink requires a valid tenant" until errHostClosed
	// was given a type: a Factory reading a 4xx looks at its own credential and
	// stops retrying, for a condition that is entirely this Host's.
	accepted, status := hostLinkHandshake(t, server.URL, tenantA)
	if accepted {
		t.Error("the HostLink endpoint still upgrades after Stop")
	} else if status != http.StatusServiceUnavailable {
		t.Errorf("a stopped Host refused the handshake with %d, want %d: a 4xx tells a Factory its own request was at fault",
			status, http.StatusServiceUnavailable)
	}
	for _, failure := range report.Failures {
		if failure.Step == lifecycle.StepCloseLink {
			t.Errorf("Stop booked a close failure against a transport that closed: %v", failure)
		}
	}
	// AND THE STOP DID NOT RE-RUN THE DRAIN. StartDrain is idempotent, so the
	// second one Stop issues must find the first.
	if released := f.runtime.Released(); released != 1 {
		t.Errorf("the runtime was released %d times across a link drain and a Stop, want 1", released)
	}
}

// TestATransportThatWillNotCloseIsBookedUnderStepCloseLink is the failure path,
// which moved out of internal/lifecycle with the close itself.
//
// IT IS A RECORDED FAILURE AND NOT A RETURNED ERROR. Every release succeeded; a
// socket that will not shut down is an operator's reconciliation, and a Stop
// that returned an error for it would tell a caller its drain failed when it
// did not.
func TestATransportThatWillNotCloseIsBookedUnderStepCloseLink(t *testing.T) {
	f := newFixture(t)
	f.start()

	refused := errors.New("the transport would not shut down")
	f.svc.links = &links{
		clock: f.clock,
		max:   1,
		build: func(sessionwire.TenantID) (*tenantLink, error) {
			return nil, errors.New("this fixture builds no links")
		},
		byTenant: map[sessionwire.TenantID]*tenantLink{
			tenantA: {tenant: tenantA, server: &refusingServer{err: refused}},
		},
	}

	report, err := f.svc.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop returned an error for a transport that would not close, want the failure recorded: %v", err)
	}
	var booked []lifecycle.Failure
	for _, failure := range report.Failures {
		if failure.Step == lifecycle.StepCloseLink {
			booked = append(booked, failure)
		}
	}
	if len(booked) != 1 {
		t.Fatalf("close failures booked = %v, want exactly one under %q", report.Failures, lifecycle.StepCloseLink)
	}
	if !errors.Is(booked[0], refused) {
		t.Errorf("the booked failure lost its cause: %v", booked[0])
	}
}

// refusingServer is a transport whose shutdown fails.
type refusingServer struct{ err error }

// Handler is never reached by the test that uses this.
func (s *refusingServer) Handler() http.Handler { return http.NotFoundHandler() }

// Publish is never reached by the test that uses this.
func (s *refusingServer) Publish(string, []byte) error { return nil }

// Close refuses.
func (s *refusingServer) Close(context.Context) error { return s.err }
