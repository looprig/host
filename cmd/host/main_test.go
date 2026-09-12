package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http/httptest"

	"github.com/looprig/sessionstore"
	"os"
	"strings"
	"testing"
	"time"
)

// environment is a Config source a test supplies without mutating the process.
type environment map[string]string

func (e environment) lookup(name string) (string, bool) {
	value, supplied := e[name]
	return value, supplied
}

// completeEnvironment is one coherent deployment's variables.
func completeEnvironment() environment {
	return environment{
		"HOST_ID":                    "host-a",
		"HOST_GENERATION":            "4",
		"HOST_INTERNAL_ENDPOINT":     "ws://10.0.0.1:7100/hostlink",
		"HOST_ISOLATION_CLASS":       "cross_tenant_isolated",
		"HOST_PLACEMENT":             "pooled",
		"HOST_CAPACITY":              "4",
		"HOST_WARM_TTL":              "90s",
		"HOST_REGISTRY_HEARTBEAT":    "10s",
		"HOST_REGISTRY_EXPIRY":       "60s",
		"HOST_CLAIM_TTL":             "5s",
		"HOST_APPLY_DEADLINE":        "30s",
		"HOST_RECONCILE_INTERVAL":    "1m",
		"HOST_COMMAND_QUEUE_SIZE":    "16",
		"HOST_RECONCILE_BATCH":       "32",
		"HOST_LISTEN_ADDRESS":        "127.0.0.1:0",
		"HOST_MAX_BINDINGS_PER_LINK": "4",
		"HOST_MAX_BINDINGS":          "8",
		"HOST_MAX_TENANT_LINKS":      "3",
		"HOST_DRAIN_GRACE":           "30s",
		"HOST_DRAIN_IDLE_BOUNDARY":   "10s",
		"HOST_DRAIN_PUBLISH_BOUND":   "5s",
		"HOST_COMPATIBILITY_TIMEOUT": "20s",
		"HOST_WORK_POLL":             "1s",
	}
}

// TestACompleteEnvironmentParses is the positive control for every refusal
// below: without it, a test asserting that a missing variable is reported would
// also pass against a parser that reported one for every input.
func TestACompleteEnvironmentParses(t *testing.T) {
	t.Parallel()

	config, err := LoadConfig(completeEnvironment().lookup)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.HostID != "host-a" || config.HostGeneration != 4 || config.Capacity != 4 {
		t.Errorf("identities parsed as (%q, %d, %d)", config.HostID, config.HostGeneration, config.Capacity)
	}
	if config.RegistryExpiry != time.Minute || config.WarmTTL != 90*time.Second {
		t.Errorf("durations parsed as (%v, %v)", config.RegistryExpiry, config.WarmTTL)
	}
	if config.FixedSessionID != "" {
		t.Errorf("an unset fixed session parsed as %q, want empty", config.FixedSessionID)
	}
	if config.PingInterval != 0 || config.PongTimeout != 0 {
		t.Errorf("an unset heartbeat parsed as (%v, %v), want both zero", config.PingInterval, config.PongTimeout)
	}
}

// TestEveryRequiredVariableIsReportedByName derives its subject from the
// complete environment rather than listing the variables again.
//
// A TABLE WOULD BE DEFEATED BY THE OBVIOUS MISTAKE — adding a variable to the
// parser and not to the table — which is exactly how a required value becomes
// silently optional. Removing each key in turn means a new variable is covered
// the moment it is read.
func TestEveryRequiredVariableIsReportedByName(t *testing.T) {
	t.Parallel()

	complete := completeEnvironment()
	if len(complete) == 0 {
		t.Fatal("the complete environment is empty; this check would be vacuous")
	}
	for name := range complete {
		t.Run(name, func(t *testing.T) {
			partial := completeEnvironment()
			delete(partial, name)

			var refusal *ConfigError
			if _, err := LoadConfig(partial.lookup); !errors.As(err, &refusal) {
				t.Fatalf("LoadConfig without %s = %v, want *ConfigError", name, err)
			}
			if refusal.Variable != name {
				t.Errorf("LoadConfig without %s names %q; an operator is sent to the wrong variable", name, refusal.Variable)
			}
		})
	}
}

// TestAMalformedValueNamesItsOwnVariable covers the other refusal class.
func TestAMalformedValueNamesItsOwnVariable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ name, value string }{
		{name: "HOST_WARM_TTL", value: "ninety"},
		{name: "HOST_CAPACITY", value: "-1"},
		{name: "HOST_MAX_BINDINGS", value: "many"},
	} {
		t.Run(test.name, func(t *testing.T) {
			broken := completeEnvironment()
			broken[test.name] = test.value

			var refusal *ConfigError
			if _, err := LoadConfig(broken.lookup); !errors.As(err, &refusal) {
				t.Fatalf("LoadConfig with %s=%q = %v, want *ConfigError", test.name, test.value, err)
			}
			if refusal.Variable != test.name {
				t.Errorf("the refusal names %q, want %q", refusal.Variable, test.name)
			}
			if refusal.Unwrap() == nil {
				t.Error("the refusal carries no parse failure to unwrap")
			}
		})
	}
}

// TestAPingWithoutAPongIsRefusedByTheServerAndNotDefaultedHere pins where that
// rule lives. Supplying the interval alone must reach a refusal, and it must not
// be this file that invents a pong.
func TestAPingWithoutAPongIsRefusedByTheServerAndNotDefaultedHere(t *testing.T) {
	t.Parallel()

	partial := completeEnvironment()
	partial["HOST_PING_INTERVAL"] = "25s"

	var refusal *ConfigError
	if _, err := LoadConfig(partial.lookup); !errors.As(err, &refusal) {
		t.Fatalf("LoadConfig with a ping and no pong = %v, want *ConfigError", err)
	}
	if refusal.Variable != "HOST_PONG_TIMEOUT" {
		t.Errorf("the refusal names %q, want HOST_PONG_TIMEOUT", refusal.Variable)
	}
}

// ---------------------------------------------------------------------------
// The binary is generic
// ---------------------------------------------------------------------------

// TestTheGenericBinaryRefusesRatherThanRunningWithNoProduct is 04-host.md's
// step 4 read as a behaviour.
//
// A binary that quietly opened an in-memory store and registered nothing would
// start, advertise zero targets, and answer every probe as healthy while being
// able to serve nothing — the failure that is hardest to notice. The refusal
// names the missing piece.
func TestTheGenericBinaryRefusesRatherThanRunningWithNoProduct(t *testing.T) {
	t.Parallel()

	err := Run(t.Context(), completeEnvironment().lookup, unconfiguredBootstrap{})
	if !errors.Is(err, errNoBootstrap) {
		t.Fatalf("Run with no product = %v, want the bootstrap refusal", err)
	}
}

// TestConfigurationIsRefusedBeforeAnythingIsOpened orders the two refusals.
//
// A Deployment with a malformed duration must never reach the store, because
// opening one is the expensive, side-effecting step and because the message an
// operator wants is about the variable they got wrong.
func TestConfigurationIsRefusedBeforeAnythingIsOpened(t *testing.T) {
	t.Parallel()

	broken := completeEnvironment()
	delete(broken, "HOST_ID")
	opener := &countingBootstrap{}

	var refusal *ConfigError
	if err := Run(t.Context(), broken.lookup, opener); !errors.As(err, &refusal) {
		t.Fatalf("Run with a broken configuration = %v, want *ConfigError", err)
	}
	if opener.opened != 0 {
		t.Errorf("a broken configuration opened the store %d times, want 0", opener.opened)
	}
}

// countingBootstrap records whether the store was opened.
type countingBootstrap struct {
	unconfiguredBootstrap
	opened int
}

// Store counts the call and then refuses like its embedded default.
func (b *countingBootstrap) Store(ctx context.Context) (*sessionstore.Store, error) {
	b.opened++
	return b.unconfiguredBootstrap.Store(ctx)
}

// TestTheBinaryNamesNoProductAndRegistersNoAgent is the structural half of step
// 4, and it is derived from the SOURCE rather than from a list.
//
// The import half is already held module-wide by import_boundary_test.go. What
// is left, and what only this package can check, is that the binary contains no
// department.Registration literal of its own: a product's agents could be
// hardcoded here without importing anything a boundary guard would notice,
// because a Registration is an agent id and an interface value.
func TestTheBinaryNamesNoProductAndRegistersNoAgent(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	inspected := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		inspected++
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "Registration" {
				t.Errorf("%s constructs a %s literal; this binary is generic and a product supplies its registrations",
					name, selector.Sel.Name)
			}
			return true
		})
	}
	if inspected == 0 {
		t.Fatal("no production file was inspected; this guard reached nothing")
	}
}

// TestAProbeAnswers200WhenTrueAnd503WhenFalse pins what each probe route puts
// on the wire.
//
// 503 IS THE REFUSAL AND NOT 500. Kubernetes treats any non-2xx as a probe
// failure, so the code is for the human reading the logs, and "deliberately not
// taking traffic" is what 503 means. The body is asserted too, because the
// difference between the two probes is invisible in the status alone and an
// operator reading a log needs to know which question was answered.
func TestAProbeAnswers200WhenTrueAnd503WhenFalse(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		ok     bool
		status int
		body   string
	}{
		{name: "accepting", ok: true, status: 200, body: "accepting"},
		{name: "not accepting", ok: false, status: 503, body: "not accepting"},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			probe(recorder, test.ok, "accepting", "not accepting")
			if recorder.Code != test.status {
				t.Errorf("probe(%v) = %d, want %d", test.ok, recorder.Code, test.status)
			}
			if got := strings.TrimSpace(recorder.Body.String()); got != test.body {
				t.Errorf("probe(%v) body = %q, want %q", test.ok, got, test.body)
			}
		})
	}
}
