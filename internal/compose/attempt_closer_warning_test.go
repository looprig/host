package compose

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	hostconfig "github.com/looprig/host/internal/hostconfig"
)

func TestSessionCompositionWarnsWhenRuntimeCannotClosePredecessorAttempt(t *testing.T) {
	var logs bytes.Buffer
	f := newFixture(t, func(options *Options, _ *hostconfig.Options) {
		options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	f.start()
	f.attach(tenantA, sessionA)
	got := logs.String()
	if !strings.Contains(got, "attempt_closer_unavailable") || !strings.Contains(got, string(sessionA)) {
		t.Fatalf("missing per-session recovery warning: %s", got)
	}
}

func TestSessionCompositionDoesNotWarnWhenRuntimeHasAttemptCloser(t *testing.T) {
	var logs bytes.Buffer
	f := newFixture(t, func(options *Options, _ *hostconfig.Options) {
		options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	f.rig.Session = newPublishingSession()
	f.start()
	f.attach(tenantA, sessionA)
	if strings.Contains(logs.String(), "attempt_closer_unavailable") {
		t.Fatalf("warned for a runtime with a closer: %s", logs.String())
	}
}

type optimisticCapabilityReporter struct{}

func (optimisticCapabilityReporter) AttemptCloserAvailable() bool { return true }

func TestAvailabilityReportCannotInventAttemptCloserMethod(t *testing.T) {
	closer, available := attemptCloserFor(optimisticCapabilityReporter{})
	if available || closer != nil {
		t.Fatalf("reporter with no closer method: %v, %v", available, closer)
	}
}
