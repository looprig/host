package host_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
)

// syncBuffer is a bytes.Buffer safe for a logger another goroutine may write.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// profileRegistrar registers the fixture's fake rig under recovery.
func profileRegistrar(f *composeFixture, recovery department.Recovery) host.Registrar {
	return host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		target, err := department.NewRigTarget(f.rig, composeCompat, department.Capabilities{
			SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1,
			CaptureSafety: department.CaptureSafetyStreaming, Recovery: recovery,
		})
		if err != nil {
			return nil, err
		}
		return []department.Registration{{AgentID: composeAgent, Target: target}}, nil
	})
}

// THE DEFAULT PROFILE IS DURABLE, and it refuses at composition a target that
// does not declare both recovery capabilities — the zero Composition, not an
// opt-in, is what refuses.
func TestTheDefaultProfileRefusesAnUndeclaredTargetAtCompose(t *testing.T) {
	var zero host.Composition
	if zero.RuntimeProfile != host.RuntimeProfileDurable {
		t.Fatalf("the zero RuntimeProfile is %v, want durable", zero.RuntimeProfile)
	}

	for _, row := range []struct {
		name     string
		recovery department.Recovery
		lacks    []string
	}{
		{"nothing declared", department.Recovery{}, []string{"AttemptCloser", "PersistenceFaults"}},
		{"closer only", department.Recovery{AttemptCloser: true}, []string{"PersistenceFaults"}},
		{"faults only", department.Recovery{PersistenceFaults: true}, []string{"AttemptCloser"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newComposeFixture(t)
			blueprint := f.blueprint(t)
			blueprint.RuntimeProfile = host.RuntimeProfileDurable
			blueprint.Collaborators.Registrar = profileRegistrar(f, row.recovery)
			service, err := host.Compose(t.Context(), blueprint)
			if err == nil {
				_, _ = service.Stop(t.Context())
				t.Fatal("a durable composition admitted a target without full recovery")
			}
			var invalid *host.InvalidCompositionError
			if !errors.As(err, &invalid) || invalid.Field != "Collaborators.Registrar" {
				t.Fatalf("Compose = %v, want *InvalidCompositionError on Collaborators.Registrar", err)
			}
			if !strings.Contains(invalid.Reason, string(composeAgent)) {
				t.Errorf("the refusal %q does not name the agent", invalid.Reason)
			}
			for _, lack := range row.lacks {
				if !strings.Contains(invalid.Reason, lack) {
					t.Errorf("the refusal %q does not name the missing %s", invalid.Reason, lack)
				}
			}
			if !strings.Contains(invalid.Reason, "harnessruntime.Target") || !strings.Contains(invalid.Reason, "RuntimeProfileBestEffort") {
				t.Errorf("the refusal %q names no remedy", invalid.Reason)
			}
		})
	}

	t.Run("control: a declared target composes", func(t *testing.T) {
		f := newComposeFixture(t)
		blueprint := f.blueprint(t)
		blueprint.RuntimeProfile = host.RuntimeProfileDurable
		blueprint.Collaborators.Registrar = profileRegistrar(f, department.Recovery{AttemptCloser: true, PersistenceFaults: true})
		service, err := host.Compose(t.Context(), blueprint)
		if err != nil {
			t.Fatalf("Compose: %v", err)
		}
		if err := service.CloseUnstarted(t.Context()); err != nil {
			t.Fatalf("CloseUnstarted: %v", err)
		}
	})
}

// BEST-EFFORT IS THE EXPLICIT OPT-OUT: it admits the undeclared target and
// says so once, at WARN, on Start — never at Compose, and never twice.
func TestBestEffortAdmitsAnUndeclaredTargetAndWarnsOnce(t *testing.T) {
	f := newComposeFixture(t)
	logs := &syncBuffer{}
	blueprint := f.blueprint(t)
	blueprint.RuntimeProfile = host.RuntimeProfileBestEffort
	blueprint.Collaborators.Registrar = profileRegistrar(f, department.Recovery{})
	blueprint.Collaborators.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	service, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatalf("Compose under best effort: %v", err)
	}
	if strings.Contains(logs.String(), "RuntimeProfileBestEffort") {
		t.Fatal("the warning was logged at Compose, before anything runs")
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _, _ = service.Stop(context.WithoutCancel(t.Context())) }()
	_ = service.Start(t.Context()) // a second Start is refused by the lifecycle and must not warn again

	warnings := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "RuntimeProfileBestEffort") {
			warnings++
			if !strings.Contains(line, "level=WARN") || !strings.Contains(line, string(composeAgent)) {
				t.Errorf("the warning %q is not a WARN naming the undeclared agent", line)
			}
		}
	}
	if warnings != 1 {
		t.Errorf("logged %d best-effort warnings, want exactly one:\n%s", warnings, logs.String())
	}
}

func TestComposeRefusesAnUnknownRuntimeProfile(t *testing.T) {
	blueprint := newComposeFixture(t).blueprint(t)
	blueprint.RuntimeProfile = host.RuntimeProfileBestEffort + 1
	_, err := host.Compose(t.Context(), blueprint)
	var invalid *host.InvalidCompositionError
	if !errors.As(err, &invalid) || invalid.Field != "RuntimeProfile" {
		t.Fatalf("Compose = %v, want *InvalidCompositionError on RuntimeProfile", err)
	}
	if got := (host.RuntimeProfileBestEffort + 1).String(); got != "RuntimeProfile(2)" {
		t.Errorf("String() = %q", got)
	}
	if host.RuntimeProfileDurable.String() != "durable" || host.RuntimeProfileBestEffort.String() != "best-effort" {
		t.Error("the profiles do not name themselves")
	}
}
