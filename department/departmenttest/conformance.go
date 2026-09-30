// Package departmenttest is test support for products that register a runtime
// with Host: an exported conformance run that launches one real session through
// a department.LaunchTarget and checks the contract Host relies on.
//
// It imports no harness package, so it can check any runtime; it is run in
// Host against github.com/looprig/host/harnessruntime, and a product runs it
// against its own target (Carbon's, the tests lanes' rigs) so a capability
// that compiles but is not wired fails a unit test rather than a failover.
package departmenttest

import (
	"context"
	"encoding/json"
	"maps"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
)

// Applied is what a runtime's own command applier received for one command,
// observed beneath the department seam.
type Applied struct {
	Principal *sessionwire.Principal
	Metadata  sessionwire.MessageMetadata
}

// Recorder reports what the launched runtime's own applier received. It is
// the product's probe below the seam Host hands commands across — for a
// harness runtime, a recording runtimecommand.Applier or a read of the
// session's journal — because a runtime that drops Principal or Metadata in
// translation looks identical from above.
type Recorder interface {
	// Applied reports what the runtime applied for command, and whether it
	// applied that command at all. The whole command is passed so a recorder
	// can correlate on either identity: harness, for one, stamps the
	// RuntimeCommandID as the cause of the turn an input starts. It is called
	// after ApplyCommand returns; a recorder over an asynchronous effect may
	// wait for it, bounded by its own deadline.
	Applied(command department.RuntimeCommand) (Applied, bool)
}

// Launch launches one session through target for request, the way the
// product's Host would (normally target.Create), and returns the runtime with
// a Recorder over it. It may fill in request fields its rigs need — a tenant
// RigPerTenant knows, a workspace root — but must not change RigSessionID;
// honouring that is one of the checks.
type Launch func(ctx context.Context, target department.LaunchTarget, request department.CreateRequest) (department.Runtime, Recorder, error)

// Option adjusts a conformance run.
type Option func(*run)

// WithInputEncoder replaces the default encoding of the probe input's body,
// json.Marshal of Core's sessionwire.InputRequest, for a product whose
// runtime reads another body shape (harnessruntime.WithBlockDecoder).
func WithInputEncoder(encode func(sessionwire.InputRequest) ([]byte, error)) Option {
	return func(r *run) { r.encode = encode }
}

type run struct {
	encode func(sessionwire.InputRequest) ([]byte, error)
}

// Probe identities. They are fixed so a failure names the same values every
// run, and distinct from any zero value so a propagation defect cannot pass.
const (
	probeTenant  = sessionwire.TenantID("conformance-tenant")
	probeSession = sessionwire.SessionID("conformance-session")
	probeAgent   = sessionwire.AgentID("conformance-agent")
)

// RunRuntimeConformance launches one session through target via launch and
// checks, each as its own subtest:
//
//   - the target declares both recovery capabilities (department.Recovery),
//     which host.Compose requires under the default durable profile;
//   - the launched session runs under the RigSessionID Host asked for;
//   - the runtime has the six required capabilities, holds a journal lease
//     epoch, and offers both recovery capabilities — AttemptCloser and
//     PersistenceFaults — for real (AttemptCloserAvailable and
//     PersistenceFaultsAvailable report true where the runtime reports them);
//   - it offers department.LiveOptionsSubscriber;
//   - an input's Principal and Metadata reach the runtime's own applier
//     through ApplyCommand;
//   - an unresolved PayloadRef is refused and never reaches the applier.
//
// The session is released (nonterminally) when the test ends. The probe input
// is applied to a real runtime, so launch must supply one whose model answers.
func RunRuntimeConformance(t *testing.T, target department.LaunchTarget, launch Launch, options ...Option) {
	t.Helper()
	settings := run{encode: func(request sessionwire.InputRequest) ([]byte, error) { return json.Marshal(request) }}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if target == nil {
		t.Fatal("departmenttest: no target to check")
	}
	if launch == nil {
		t.Fatal("departmenttest: no launch function")
	}

	t.Run("target declares recovery", func(t *testing.T) {
		recovery := target.Capabilities().Recovery
		if !recovery.AttemptCloser {
			t.Error("Capabilities().Recovery.AttemptCloser is false; host.Compose refuses this target under RuntimeProfileDurable")
		}
		if !recovery.PersistenceFaults {
			t.Error("Capabilities().Recovery.PersistenceFaults is false; host.Compose refuses this target under RuntimeProfileDurable")
		}
	})

	rigSessionID, err := uuid.New()
	if err != nil {
		t.Fatalf("departmenttest: mint a rig session id: %v", err)
	}
	placement := sessionwire.HostPlacementDedicated
	if target.Capabilities().PoolingPermitted() {
		placement = sessionwire.HostPlacementPooled
	}
	request := department.CreateRequest{
		TenantID:      probeTenant,
		SessionID:     probeSession,
		AgentID:       probeAgent,
		Placement:     placement,
		WorkspaceRoot: t.TempDir(),
		Storage:       department.StorageContext{Namespace: "conformance/" + string(probeSession)},
		RigSessionID:  rigSessionID,
	}
	runtime, recorder, err := launch(t.Context(), target, request)
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if runtime == nil {
		t.Fatal("launch returned no runtime and no error")
	}
	t.Cleanup(func() { _ = runtime.ReleaseResidency(context.WithoutCancel(t.Context())) })

	t.Run("RigSessionID honoured", func(t *testing.T) {
		got, ok := department.RigSessionID(runtime)
		if !ok {
			t.Fatal("department.RigSessionID reports no Harness identity; launch through department.NewRigTarget (or forward RigSessionID from a wrapper)")
		}
		if got != rigSessionID {
			t.Errorf("the session runs under %s, want the requested %s", got, rigSessionID)
		}
	})

	t.Run("required capabilities", func(t *testing.T) {
		if runtime.SessionID() == "" || runtime.AgentID() == "" {
			t.Errorf("the runtime reports identities (%q, %q), want Host's", runtime.SessionID(), runtime.AgentID())
		}
		if runtime.Done() == nil {
			t.Error("Liveness.Done returns a nil channel, which never closes")
		}
		if _, held := runtime.LeaseEpoch(); !held {
			t.Error("the runtime holds no journal lease epoch, so no command could be applied to it")
		}
	})

	t.Run("recovery capabilities", func(t *testing.T) {
		if _, ok := runtime.(department.AttemptCloser); !ok {
			t.Error("the runtime is not a department.AttemptCloser")
		} else if reporter, ok := runtime.(interface{ AttemptCloserAvailable() bool }); ok && !reporter.AttemptCloserAvailable() {
			t.Error("AttemptCloserAvailable() is false: the wrapper offers the method but the runtime beneath has no closer")
		}
		faults, ok := runtime.(department.PersistenceFaults)
		if !ok {
			t.Error("the runtime is not a department.PersistenceFaults")
		} else {
			if reporter, ok := runtime.(interface{ PersistenceFaultsAvailable() bool }); ok && !reporter.PersistenceFaultsAvailable() {
				t.Error("PersistenceFaultsAvailable() is false: the wrapper offers the methods but the runtime beneath reports no faults")
			}
			if faults.PersistenceFaulted() == nil {
				t.Error("PersistenceFaulted returns a nil channel, so a fault would never be noticed")
			}
		}
	})

	t.Run("live options subscription", func(t *testing.T) {
		if _, ok := runtime.(department.LiveOptionsSubscriber); !ok {
			t.Error("the runtime is not a department.LiveOptionsSubscriber, so Host cannot relay tool-step previews")
		}
	})

	t.Run("principal and metadata survive ApplyCommand", func(t *testing.T) {
		if recorder == nil {
			t.Fatal("launch returned no Recorder, so what reached the runtime cannot be observed")
		}
		principal := &sessionwire.Principal{Tenant: probeTenant, Subject: "conformance-subject", Kind: sessionwire.PrincipalKindActor}
		metadata := sessionwire.MessageMetadata{"conformance": "probe", "channel": "departmenttest"}
		const commandID = sessionwire.CommandID("conformance-input-1")
		blocks, err := content.MarshalBlocks([]content.Block{&content.TextBlock{Text: "departmenttest conformance probe"}})
		if err != nil {
			t.Fatalf("encode the probe blocks: %v", err)
		}
		body, err := settings.encode(sessionwire.InputRequest{
			CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: commandID},
			SessionID:       probeSession,
			Blocks:          blocks,
			Metadata:        metadata,
			Principal:       principal,
		})
		if err != nil {
			t.Fatalf("encode the probe body: %v", err)
		}
		command := department.RuntimeCommand{
			CommandID:        commandID,
			RuntimeCommandID: mustUUID(t),
			Kind:             "input",
			Payload:          body,
			AttemptID:        "conformance-attempt-1",
			Principal:        principal,
			Metadata:         metadata,
		}
		if err := runtime.ApplyCommand(t.Context(), command); err != nil {
			t.Fatalf("ApplyCommand: %v", err)
		}
		applied, ok := recorder.Applied(command)
		if !ok {
			t.Fatal("ApplyCommand succeeded and the runtime's applier recorded nothing")
		}
		if applied.Principal == nil || *applied.Principal != *principal {
			t.Errorf("the runtime received principal %+v, want %+v", applied.Principal, *principal)
		}
		if !maps.Equal(applied.Metadata, metadata) {
			t.Errorf("the runtime received metadata %v, want %v", applied.Metadata, metadata)
		}
	})

	t.Run("unresolved PayloadRef refused", func(t *testing.T) {
		command := department.RuntimeCommand{
			CommandID:        "conformance-input-ref",
			RuntimeCommandID: mustUUID(t),
			Kind:             "input",
			PayloadRef:       sessionwire.ObjectReference{ObjectID: "conformance-object"},
			AttemptID:        "conformance-attempt-ref",
		}
		err := runtime.ApplyCommand(t.Context(), command)
		if err == nil {
			t.Error("ApplyCommand accepted a command whose body is an unresolved object reference; a runtime must refuse it, never dereference it")
		}
		if recorder != nil {
			if _, ok := recorder.Applied(command); ok {
				t.Error("the unresolved reference reached the runtime's applier")
			}
		}
	})
}

func mustUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("departmenttest: mint a uuid: %v", err)
	}
	return id
}
