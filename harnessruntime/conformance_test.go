package harnessruntime

import (
	"context"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/runtimecommand"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/department/departmenttest"
	"github.com/looprig/host/internal/harnesstest"
)

// conformanceCapabilities declares NO recovery, so the conformance's "target
// declares recovery" row can pass only if Target forces the declaration on.
func conformanceCapabilities() department.Capabilities {
	return department.Capabilities{
		SupportsPooled:    true,
		SupportsDedicated: true,
		AdmissionWeight:   1,
		CaptureSafety:     department.CaptureSafetyStreaming,
	}
}

// admittedRecorder is the recording applier's view, as a departmenttest.Recorder.
type admittedRecorder struct {
	mu       sync.Mutex
	admitted *[]runtimecommand.Admitted
}

func (r *admittedRecorder) Applied(command department.RuntimeCommand) (departmenttest.Applied, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, admitted := range *r.admitted {
		if string(admitted.CommandID) == string(command.CommandID) {
			return departmenttest.Applied{Principal: admitted.Principal, Metadata: admitted.Metadata}, true
		}
	}
	return departmenttest.Applied{}, false
}

// TestTheAdapterPassesRuntimeConformanceOverARecordingApplier runs the exported
// conformance against Target over a harness session whose applier records the
// released Admitted value, so principal and metadata are checked at the exact
// type harness receives.
func TestTheAdapterPassesRuntimeConformanceOverARecordingApplier(t *testing.T) {
	var admitted []runtimecommand.Admitted
	controller := &applyingController{
		fullController: *newFullController(newFakeSubscription(nil), nil, nil),
		applierPart:    applierPart{available: true, admitted: &admitted, closer: &recordingAttemptCloser{}},
	}
	target, err := Target(stubRigs{launcher: &fakeLauncher{controller: controller}}, "conformance-build", conformanceCapabilities())
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	departmenttest.RunRuntimeConformance(t, target, func(ctx context.Context, target department.LaunchTarget, request department.CreateRequest) (department.Runtime, departmenttest.Recorder, error) {
		// The fake cannot read rig.WithSessionID, so it is launched under
		// the id the request names, as the released rig is.
		controller.id = request.RigSessionID
		runtime, err := target.Create(ctx, request)
		return runtime, &admittedRecorder{admitted: &admitted}, err
	})
}

// journalRecorder reads a real harness session's journal for the turn an
// input started: harness stamps the admitted RuntimeCommandID as its cause.
type journalRecorder struct {
	t       *testing.T
	store   *harnessstore.Store
	runtime department.Runtime
}

func (r journalRecorder) Applied(command department.RuntimeCommand) (departmenttest.Applied, bool) {
	id, ok := department.RigSessionID(r.runtime)
	if !ok {
		return departmenttest.Applied{}, false
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, recorded := range harnesstest.Events(r.t, r.store, id) {
			var header event.Header
			var input *event.MessageInput
			switch value := recorded.(type) {
			case event.TurnStarted:
				header, input = value.Header, value.Input
			case event.TurnFoldedInto:
				header, input = value.Header, value.Input
			default:
				continue
			}
			if header.Cause.CommandID == command.RuntimeCommandID && input != nil {
				return departmenttest.Applied{Principal: input.Principal, Metadata: input.Metadata}, true
			}
		}
		if time.Now().After(deadline) {
			return departmenttest.Applied{}, false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTheAdapterPassesRuntimeConformanceOverARealRig runs the same conformance
// against Target over SharedRig and a real harness rig on an in-memory journal,
// observing the probe through the journal the session wrote.
func TestTheAdapterPassesRuntimeConformanceOverARealRig(t *testing.T) {
	store := harnesstest.Store(t, harnesstest.Backend(t), sessionwire.TenantID("conformance-tenant"))
	target, err := Target(SharedRig(harnesstest.Rig(t, store, &harnesstest.RecordingLLM{})), "conformance-build", conformanceCapabilities())
	if err != nil {
		t.Fatalf("Target: %v", err)
	}
	departmenttest.RunRuntimeConformance(t, target, func(ctx context.Context, target department.LaunchTarget, request department.CreateRequest) (department.Runtime, departmenttest.Recorder, error) {
		runtime, err := target.Create(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		return runtime, journalRecorder{t: t, store: store, runtime: runtime}, nil
	})
}
