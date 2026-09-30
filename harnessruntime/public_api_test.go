package harnessruntime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/runtimecommand"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnesstest"
)

// A PRODUCT THAT NAMES NO DECODER GETS CORE'S InputRequest, not a refusal:
// the body Factory admitted crosses as the blocks it carries.
func TestNewDecodesCoreInputRequestsByDefault(t *testing.T) {
	var seen []runtimecommand.Admitted
	controller := newApplyingController(applierPart{available: true, admitted: &seen})
	runtime := boundFor(t, controller)

	command := inputCommand()
	command.Payload = inputBody(t, "hello from Factory")
	if err := runtime.ApplyCommand(t.Context(), command); err != nil {
		t.Fatalf("ApplyCommand with no decoder named: %v", err)
	}
	if len(seen) != 1 || len(seen[0].Blocks) != 1 {
		t.Fatalf("the applier saw %+v, want one decoded block", seen)
	}
	if text, ok := seen[0].Blocks[0].(*content.TextBlock); !ok || text.Text != "hello from Factory" {
		t.Fatalf("the block is %#v, want the InputRequest's text", seen[0].Blocks[0])
	}

	// And a create's first message, re-presented input-shaped, crosses too.
	seen = nil
	if err := runtime.ApplyCommand(t.Context(), createCommand(t, "the first message")); err != nil {
		t.Fatalf("ApplyCommand(create) with no decoder named: %v", err)
	}
	if len(seen) != 1 || len(seen[0].Blocks) != 1 {
		t.Fatalf("the applier saw %+v for the create, want its first message", seen)
	}
}

func TestDecodeInputBlocksIsCoresStrictDecoder(t *testing.T) {
	multi := sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "command-a"},
		SessionID:       testSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"one"},{"type":"text","text":"two"}]`),
	}
	body, err := json.Marshal(multi)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := DecodeInputBlocks(body)
	if err != nil {
		t.Fatalf("DecodeInputBlocks: %v", err)
	}
	if len(blocks) != 2 || blocks[1].(*content.TextBlock).Text != "two" {
		t.Fatalf("decoded %#v, want both blocks in order", blocks)
	}

	for name, refused := range map[string][]byte{
		"an unknown member":  []byte(`{"version":"1","command_id":"c","session_id":"s","blocks":[{"type":"text","text":"x"}],"extra":1}`),
		"a create's body":    createBody(t, "not input-shaped"),
		"no blocks":          []byte(`{"version":"1","command_id":"c","session_id":"s"}`),
		"not JSON":           []byte("hello"),
		"an unknown block":   []byte(`{"version":"1","command_id":"c","session_id":"s","blocks":[{"type":"hologram"}]}`),
		"an empty body":      nil,
		"a JSON null":        []byte("null"),
		"a JSON array":       []byte(`[{"type":"text","text":"x"}]`),
		"a trailing value":   append(append([]byte{}, body...), []byte(` {}`)...),
		"a duplicate member": []byte(`{"version":"1","command_id":"c","command_id":"d","session_id":"s","blocks":[{"type":"text","text":"x"}]}`),
	} {
		if blocks, err := DecodeInputBlocks(refused); err == nil {
			t.Errorf("DecodeInputBlocks(%s) = %#v, want a refusal", name, blocks)
		}
	}
}

func TestOptionsAndResolversRefuseNothingToWorkWith(t *testing.T) {
	rigs := stubRigs{launcher: &fakeLauncher{}}
	if _, err := New(rigs, WithBlockDecoder(nil)); !errors.Is(err, ErrNilOption) {
		t.Errorf("New(WithBlockDecoder(nil)) = %v, want ErrNilOption", err)
	}
	if _, err := New(rigs, nil); !errors.Is(err, ErrNilOption) {
		t.Errorf("New(nil option) = %v, want ErrNilOption", err)
	}
	if _, err := New(nil); !errors.Is(err, ErrNoRigs) {
		t.Errorf("New(nil) = %v, want ErrNoRigs", err)
	}
	if _, err := New((*stubRigsPointer)(nil)); !errors.Is(err, ErrNoRigs) {
		t.Errorf("New(typed nil) = %v, want ErrNoRigs", err)
	}
	create := func(context.Context, department.RigCreateRequest) (Launcher, error) { return nil, nil }
	restore := func(context.Context, uuid.UUID, department.RigRestoreRequest) (Launcher, error) { return nil, nil }
	if _, err := New(RigsFunc(nil, restore)); !errors.Is(err, ErrNoRigs) {
		t.Errorf("New(RigsFunc(nil, restore)) = %v, want ErrNoRigs", err)
	}
	if _, err := New(RigsFunc(create, nil)); !errors.Is(err, ErrNoRigs) {
		t.Errorf("New(RigsFunc(create, nil)) = %v, want ErrNoRigs", err)
	}
	if _, err := Target(nil, "build", conformanceCapabilities()); !errors.Is(err, ErrNoRigs) {
		t.Errorf("Target(nil) = %v, want New's ErrNoRigs", err)
	}
}

type stubRigsPointer struct{ stubRigs }

// SharedRig hands the same rig to every launch and refuses a nil one at the
// launch rather than handing the adapter a typed nil.
func TestSharedRigResolvesEveryLaunchToOneRig(t *testing.T) {
	store := harnesstest.Store(t, harnesstest.Backend(t), "tenant-a")
	shared := harnesstest.Rig(t, store, &harnesstest.RecordingLLM{})
	rigs := SharedRig(shared)
	for name, resolve := range map[string]func() (Launcher, error){
		"create": func() (Launcher, error) {
			return rigs.RigForCreate(t.Context(), department.RigCreateRequest{TenantID: "x"})
		},
		"restore": func() (Launcher, error) {
			return rigs.RigForRestore(t.Context(), uuid.UUID{}, department.RigRestoreRequest{TenantID: "y"})
		},
	} {
		got, err := resolve()
		if err != nil || got != Launcher(shared) {
			t.Errorf("%s = (%v, %v), want the shared rig", name, got, err)
		}
	}

	adapter := newAdapter(t, SharedRig(nil))
	if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, ErrNoRig) {
		t.Errorf("a launch over SharedRig(nil) = %v, want ErrNoRig", err)
	}
}

// RigPerTenant resolves by the REQUEST's tenant, refuses one it does not know,
// and does not see a later write to the caller's map.
func TestRigPerTenantRoutesByTenantAndRefusesAStranger(t *testing.T) {
	storeA := harnesstest.Store(t, harnesstest.Backend(t), "tenant-a")
	storeB := harnesstest.Store(t, harnesstest.Backend(t), "tenant-b")
	rigA := harnesstest.Rig(t, storeA, &harnesstest.RecordingLLM{})
	rigB := harnesstest.Rig(t, storeB, &harnesstest.RecordingLLM{})
	table := map[sessionwire.TenantID]*rig.Rig{"tenant-a": rigA, "tenant-b": rigB}
	rigs := RigPerTenant(table)
	table["tenant-c"] = rigA
	delete(table, "tenant-b")

	if got, err := rigs.RigForCreate(t.Context(), department.RigCreateRequest{TenantID: "tenant-b"}); err != nil || got != Launcher(rigB) {
		t.Errorf("create for tenant-b = (%v, %v), want tenant-b's rig", got, err)
	}
	if got, err := rigs.RigForRestore(t.Context(), uuid.UUID{}, department.RigRestoreRequest{TenantID: "tenant-a"}); err != nil || got != Launcher(rigA) {
		t.Errorf("restore for tenant-a = (%v, %v), want tenant-a's rig", got, err)
	}
	for _, tenant := range []sessionwire.TenantID{"tenant-c", ""} {
		_, err := rigs.RigForCreate(t.Context(), department.RigCreateRequest{TenantID: tenant})
		var unknown *UnknownTenantError
		if !errors.Is(err, ErrUnknownTenant) || !errors.As(err, &unknown) || unknown.TenantID != tenant {
			t.Errorf("create for %q = %v, want an *UnknownTenantError naming it", tenant, err)
		}
		if _, err := rigs.RigForRestore(t.Context(), uuid.UUID{}, department.RigRestoreRequest{TenantID: tenant}); !errors.Is(err, ErrUnknownTenant) {
			t.Errorf("restore for %q = %v, want ErrUnknownTenant", tenant, err)
		}
	}
}

// Target forces both recovery declarations on, over whatever the caller
// declared, and otherwise keeps the caller's capabilities and build.
func TestTargetDeclaresRecoveryAndKeepsTheRest(t *testing.T) {
	declared := conformanceCapabilities()
	declared.RequiresWorkspace = true
	declared.AdmissionWeight = 3
	declared.Recovery = department.Recovery{AttemptCloser: false, PersistenceFaults: false}
	target, err := Target(stubRigs{launcher: &fakeLauncher{}}, "build-7", declared)
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	want := declared
	want.Recovery = department.Recovery{AttemptCloser: true, PersistenceFaults: true}
	if got := target.Capabilities(); got != want {
		t.Errorf("Capabilities() = %+v, want %+v", got, want)
	}
	if target.CompatibilityID() != "build-7" {
		t.Errorf("CompatibilityID() = %q, want build-7", target.CompatibilityID())
	}
	if _, err := Target(stubRigs{launcher: &fakeLauncher{}}, "", declared); err == nil {
		t.Error("Target accepted an empty compatibility id")
	}

	registration := Registration("assistant", target)
	if registration.AgentID != "assistant" || registration.Target != target {
		t.Errorf("Registration = %+v, want the agent and the target", registration)
	}
	if _, err := department.New([]department.Registration{registration}); err != nil {
		t.Errorf("a Registration does not form a Department: %v", err)
	}
}
