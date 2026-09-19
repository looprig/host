package compose

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// capturedRecord is one log record, flattened.
type capturedRecord struct {
	level      slog.Level
	message    string
	attributes map[string]string
}

// capturingHandler records every log record it is handed.
type capturingHandler struct {
	mu      *sync.Mutex
	records *[]capturedRecord
}

func newCapturingHandler() capturingHandler {
	return capturingHandler{mu: &sync.Mutex{}, records: &[]capturedRecord{}}
}

func (capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h capturingHandler) Handle(_ context.Context, record slog.Record) error {
	flat := capturedRecord{level: record.Level, message: record.Message, attributes: map[string]string{}}
	record.Attrs(func(attribute slog.Attr) bool {
		flat.attributes[attribute.Key] = attribute.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, flat)
	return nil
}

func (h capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h capturingHandler) snapshot() []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]capturedRecord(nil), *h.records...)
}

// withLogger composes the fixture with a capturing logger.
func withLogger(handler capturingHandler) func(*Options, *hostconfig.Options) {
	return func(options *Options, _ *hostconfig.Options) { options.Logger = slog.New(handler) }
}

// attachRefused attaches and requires a refusal.
func (f *fixture) attachRefused(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	f.t.Helper()
	_, err := f.svc.Attach(f.t.Context(), residency.Request{
		TenantID: tenant, SessionID: session, AgentID: testAgent, Mode: residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenant, ActorID: "actor-a"},
	})
	if err == nil {
		f.t.Fatal("the attach succeeded, want a refusal")
	}
}

// TestAHydrationRefusalIsLoggedAtWarn is the v0.3.0 booking (N3 and F12): a
// refusal about the session — here the permanently unrunnable binding — is a
// WARN naming the session, the step and the code, because a Factory counts it
// without logging it.
func TestAHydrationRefusalIsLoggedAtWarn(t *testing.T) {
	handler := newCapturingHandler()
	f := newFixture(t, withLogger(handler))
	f.start()
	f.store.mu.Lock()
	f.store.stateErr = fmt.Errorf("binding: %w", residency.ErrInvalidRuntimeIdentity)
	f.store.mu.Unlock()

	f.attachRefused(tenantA, sessionA)

	var warned []capturedRecord
	for _, record := range handler.snapshot() {
		if record.level == slog.LevelWarn && record.message == "host: attach refused" {
			warned = append(warned, record)
		}
	}
	if len(warned) != 1 {
		t.Fatalf("%d WARN attach refusals were logged, want 1: %+v", len(warned), handler.snapshot())
	}
	got := warned[0].attributes
	for key, want := range map[string]string{
		"tenant_id":  string(tenantA),
		"session_id": string(sessionA),
		"step":       string(residency.StepHydrate),
		"code":       string(sessionwire.HostLinkErrorRuntimeUnavailable),
	} {
		if got[key] != want {
			t.Errorf("attribute %s = %q, want %q", key, got[key], want)
		}
	}
	if got["error"] == "" || got["reason"] == "" {
		t.Errorf("the record carries no error or reason: %+v", got)
	}
}

// TestAPlacementRaceRefusalIsNotAWarning: epoch_mismatch is the ordinary
// traffic of a placement race, so it is DEBUG and never WARN.
func TestAPlacementRaceRefusalIsNotAWarning(t *testing.T) {
	handler := newCapturingHandler()
	f := newFixture(t, withLogger(handler))
	f.start()
	f.store.holdElsewhere(registry.Key{TenantID: tenantA, SessionID: sessionA}, 7)

	f.attachRefused(tenantA, sessionA)

	var debug int
	for _, record := range handler.snapshot() {
		if record.message != "host: attach refused" {
			continue
		}
		if record.level == slog.LevelWarn {
			t.Fatalf("a lease-contention refusal was logged at WARN: %+v", record)
		}
		if record.level == slog.LevelDebug && record.attributes["code"] == string(sessionwire.HostLinkErrorEpochMismatch) {
			debug++
		}
	}
	if debug != 1 {
		t.Fatalf("%d DEBUG epoch_mismatch records, want 1: %+v", debug, handler.snapshot())
	}
}

// TestAHostWithNoLoggerStillRefusesQuietly: the logger is optional and a nil
// one discards; nothing about the refusal changes.
func TestAHostWithNoLoggerStillRefusesQuietly(t *testing.T) {
	f := newFixture(t)
	f.start()
	sentinel := errors.New("injected: the durable read failed")
	f.store.mu.Lock()
	f.store.stateErr = sentinel
	f.store.mu.Unlock()
	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID: tenantA, SessionID: sessionA, AgentID: testAgent, Mode: residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Attach with no logger = %v, want the durable failure", err)
	}
}
