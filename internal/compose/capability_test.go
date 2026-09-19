package compose

import (
	"context"
	"errors"
	"encoding/json"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/gates"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
)

// inertGateSessions is a GateSessions that binds nothing; the capability is a
// property of the wiring, not of what a session later does.
type inertGateSessions struct{}

func (inertGateSessions) GateSessionFor(residency.Lease, sessionwire.TenantID, sessionwire.SessionID) (gates.Session, error) {
	return nil, context.Canceled
}

// inertGateReads is a commands.Gates that holds no gate.
type inertGateReads struct{}

func (inertGateReads) LoadGate(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.GateID) (commands.Gate, bool, error) {
	return commands.Gate{}, false, nil
}

// advertisesGateResponse connects as tenant-a and reports whether the reply
// Supports Core's gate-response token.
func advertisesGateResponse(t *testing.T, f *fixture) bool {
	t.Helper()
	f.start()
	link := dialFactoryLink(t, f.serve().URL, tenantA)
	var connect struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(link.connected, &connect); err != nil {
		t.Fatalf("decode the connect result %s: %v", link.connected, err)
	}
	reply, err := sessionwire.DecodeHostLinkConnectReply(connect.Data)
	if err != nil {
		t.Fatalf("Core refuses the connect reply %s: %v", connect.Data, err)
	}
	if !reply.Supports(sessionwire.HostLinkMethodAttach) {
		t.Fatal("the reply does not advertise the methods, so the absence below proves nothing")
	}
	return reply.Supports(sessionwire.HostLinkCapabilityGateResponse)
}

// TestTheGateResponseTokenIsAdvertisedOnlyWhenBothGateSeamsAreWired: a Host
// advertises hostlink.command.gate_response exactly when it both publishes
// gates (Gates) and checks answers against them (GateReads). Either half alone
// cannot apply a gate response, and a Factory reading the token would send it
// one.
func TestTheGateResponseTokenIsAdvertisedOnlyWhenBothGateSeamsAreWired(t *testing.T) {
	for _, row := range []struct {
		name       string
		gates      GateSessions
		reads      commands.Gates
		advertised bool
	}{
		{"both wired", inertGateSessions{}, inertGateReads{}, true},
		{"neither wired", nil, nil, false},
		{"publication only", inertGateSessions{}, nil, false},
		{"answer checks only", nil, inertGateReads{}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t, func(options *Options, _ *hostconfig.Options) {
				options.Gates = row.gates
				options.GateReads = row.reads
			})
			if got := advertisesGateResponse(t, f); got != row.advertised {
				t.Fatalf("Supports(gate_response) = %v, want %v", got, row.advertised)
			}
		})
	}
}

// fencingGateSessions records the fence writes and can refuse them.
type fencingGateSessions struct {
	resolves []sessionwire.GateID
	err      error
}

func (g *fencingGateSessions) GateSessionFor(residency.Lease, sessionwire.TenantID, sessionwire.SessionID) (gates.Session, error) {
	return fencingSession{sessions: g}, nil
}

type fencingSession struct {
	gates.Session
	sessions *fencingGateSessions
}

func (s fencingSession) Resolve(_ context.Context, id sessionwire.GateID) error {
	s.sessions.resolves = append(s.sessions.resolves, id)
	return s.sessions.err
}

// TestTheGateFenceIsWrittenAtAttachBeforeTheRuntimeLaunches (quality gate F2):
// the fencing write is made under the fresh grant before hydration launches the
// runtime; a fence that fails refuses the attach, launches nothing and hands
// the grant back.
func TestTheGateFenceIsWrittenAtAttachBeforeTheRuntimeLaunches(t *testing.T) {
	refused := &fencingGateSessions{err: errors.New("injected: the fence write failed")}
	f := newFixture(t, func(options *Options, _ *hostconfig.Options) { options.Gates = refused })
	f.start()
	f.attachRefused(tenantA, sessionA)
	if len(refused.resolves) != 1 || refused.resolves[0] != gates.FenceGateID {
		t.Fatalf("fence writes = %v, want the one fence", refused.resolves)
	}
	if f.rig.Launches() != 0 {
		t.Fatal("a refused fence still launched the runtime")
	}
	if released := f.trace.count("lease.release"); released != 1 {
		t.Fatalf("the grant was released %d times after a refused fence, want 1", released)
	}
}
