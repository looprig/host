package harnessadapter

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnesstest"
)

// TestNewSessionLaunchesUnderTheRequestedHarnessIdentity is R5 against the
// RELEASED rig: a create naming a Harness identity launches under exactly that
// identity (rig.WithSessionID), so the journal the runtime writes is the one
// the durable binding names. It used to launch with no options at all, and the
// rig minted an id nothing recorded.
func TestNewSessionLaunchesUnderTheRequestedHarnessIdentity(t *testing.T) {
	store := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
	adapter := newAdapter(t, stubRigs{launcher: harnesstest.Rig(t, store, &harnesstest.RecordingLLM{})})
	named := uuid.MustParse("7c0e5b1a-94d2-8f3e-a6b1-3d58e0c2f917")

	launched, err := adapter.NewSession(t.Context(), department.RigCreateRequest{
		TenantID:     testTenant,
		SessionID:    testSession,
		AgentID:      sessionwire.AgentID("agent-a"),
		Placement:    sessionwire.HostPlacementPooled,
		Storage:      department.StorageContext{Namespace: "objects/a"},
		RigSessionID: named,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = launched.(*boundSession).controller.Shutdown(t.Context()) })
	if launched.ID() != named {
		t.Fatalf("the session launched under %v, want the requested %v", launched.ID(), named)
	}
	if got := harnesstest.CountSessionStarted(t, store, named); got != 1 {
		t.Fatalf("the journal under the requested id holds %d SessionStarted, want 1", got)
	}
}

// TestNewSessionWithNoIdentityLetsTheRigMint is the control: a request with no
// durable binding names no identity, and the rig mints one rather than the
// adapter passing rig.WithSessionID the zero UUID (which harness refuses).
func TestNewSessionWithNoIdentityLetsTheRigMint(t *testing.T) {
	store := harnesstest.Store(t, harnesstest.Backend(t), testTenant)
	adapter := newAdapter(t, stubRigs{launcher: harnesstest.Rig(t, store, &harnesstest.RecordingLLM{})})

	launched, err := adapter.NewSession(t.Context(), department.RigCreateRequest{
		TenantID:  testTenant,
		SessionID: testSession,
		AgentID:   sessionwire.AgentID("agent-a"),
		Placement: sessionwire.HostPlacementPooled,
		Storage:   department.StorageContext{Namespace: "objects/a"},
	})
	if err != nil {
		t.Fatalf("NewSession with no identity: %v", err)
	}
	t.Cleanup(func() { _ = launched.(*boundSession).controller.Shutdown(t.Context()) })
	if launched.ID().IsZero() {
		t.Fatal("the rig launched a session with the zero identity")
	}
}
