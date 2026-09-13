package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http/httptest"
	"reflect"

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
func (b *countingBootstrap) Store(ctx context.Context, evidence sessionstore.DispositionEvidenceReader) (*sessionstore.Store, error) {
	b.opened++
	return b.unconfiguredBootstrap.Store(ctx, evidence)
}

// TestTheBinaryNamesNoProductAndRegistersNoAgent is the structural half of "this
// binary is generic".
//
// It parses every production file in the package and fails on any composite
// literal whose TYPE EXPRESSION names department.Registration, at any depth: a
// bare literal, a slice of them written []department.Registration{{…}} — whose
// elements carry no type of their own at all — a variadic ...department.Registration,
// a pointer, a map value. Classification is by parsed structure and never by a
// source-text search, which a newline or an alias defeats.
//
// THIS LIST IS A RESIDUE AND NOT A CLOSURE. What it does NOT catch, stated so
// that nobody reads the guard as a proof:
//
//   - a registration built field-by-field into a var and returned, with no
//     composite literal anywhere;
//   - a dot-import or a local type alias, which removes the selector this
//     matches on;
//   - a registration constructed in another package this binary imports, since
//     the walk is over this package's own files;
//   - a product named in DATA — an agent id in a string constant, a config
//     file, a build tag — which is not a registration at compile time at all;
//   - department.New called with a slice assembled at run time.
//
// The import boundary is what actually stops a product dependency; this stops
// the one spelling that would otherwise look ordinary in review.
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
			if named := namesRegistration(literal.Type); named != "" {
				t.Errorf("%s constructs a %s literal; this binary is generic and a product supplies its registrations",
					name, named)
			}
			return true
		})
	}
	if inspected == 0 {
		t.Fatal("no production file was inspected; this guard reached nothing")
	}
}

// namesRegistration reports the selector a type expression resolves to if that
// selector is Registration, unwrapping the element-type spellings a composite
// literal can hide one behind.
//
// A []department.Registration{{…}} literal is the reason this exists: the outer
// literal's type is an *ast.ArrayType, and its ELEMENTS have no Type field at
// all, so a check that looked only at literal.Type for a *ast.SelectorExpr saw
// nothing. Measured: without this, that spelling compiled and the guard stayed
// green.
func namesRegistration(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.SelectorExpr:
		if typed.Sel != nil && typed.Sel.Name == "Registration" {
			return typed.Sel.Name
		}
	case *ast.Ident:
		if typed.Name == "Registration" {
			return typed.Name
		}
	case *ast.ArrayType:
		return namesRegistration(typed.Elt)
	case *ast.Ellipsis:
		return namesRegistration(typed.Elt)
	case *ast.StarExpr:
		return namesRegistration(typed.X)
	case *ast.MapType:
		if named := namesRegistration(typed.Key); named != "" {
			return named
		}
		return namesRegistration(typed.Value)
	}
	return ""
}

// TestTheRegistrationGuardSeesTheSpellingsItClaims is the guard's own control.
//
// A structural guard that matched nothing would pass the test above for the
// wrong reason — the package legitimately contains no registration — so the
// matcher is exercised directly against each spelling it claims to catch and
// against one it must not.
func TestTheRegistrationGuardSeesTheSpellingsItClaims(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		caught bool
	}{
		{source: "department.Registration{}", caught: true},
		{source: "[]department.Registration{{}}", caught: true},
		{source: "[2]department.Registration{}", caught: true},
		{source: "[]*department.Registration{}", caught: true},
		{source: "map[string]department.Registration{}", caught: true},
		{source: "Registration{}", caught: true},
		{source: "department.Capabilities{}", caught: false},
		{source: "[]string{}", caught: false},
	} {
		expression, err := parser.ParseExpr(test.source)
		if err != nil {
			t.Fatalf("parse %q: %v", test.source, err)
		}
		literal, ok := expression.(*ast.CompositeLit)
		if !ok {
			t.Fatalf("%q did not parse as a composite literal", test.source)
		}
		if caught := namesRegistration(literal.Type) != ""; caught != test.caught {
			t.Errorf("the guard catching %q = %v, want %v", test.source, caught, test.caught)
		}
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

// ---------------------------------------------------------------------------
// The Bootstrap seam narrows as the store grows
// ---------------------------------------------------------------------------

// TestBootstrapAsksAProductForNothingTheReleasedStoreCanAnswer holds the seam to
// the things Host genuinely cannot know.
//
// THREE OF THE SEVEN METHODS WERE THERE BECAUSE OF A MISSING RELEASE, not
// because of a design: Inbox and Cursors were injected because sessionstore
// v0.6.0 had no per-session ordered listing and no durable consumption cursor,
// and Bootstrap.Store's own doc said so. v0.7.0 publishes both, so the
// composition binds the adapted store and the two seams leave the product's
// surface. Workspaces stays, because workspace materialization is still not
// that store's business.
//
// THE ASSERTION IS ON THE METHOD SET AND IS EXACT IN BOTH DIRECTIONS. A method
// this list does not name is a new thing being asked of every product and must
// be reviewed as one; a method it names that is gone is a stale obligation. The
// count alone would not do it — two methods could be swapped — so the names are
// compared as a set.
func TestBootstrapAsksAProductForNothingTheReleasedStoreCanAnswer(t *testing.T) {
	t.Parallel()

	seam := reflect.TypeOf((*Bootstrap)(nil)).Elem()
	if seam.NumMethod() == 0 {
		t.Fatal("Bootstrap declares no methods, so this guard read nothing")
	}
	got := map[string]bool{}
	for index := range seam.NumMethod() {
		got[seam.Method(index).Name] = true
	}
	want := map[string]bool{
		"Store":        true,
		"Registrar":    true,
		"Checkpointer": true,
		"Auth":         true,
		"Workspaces":   true,

		// JournalStores is the SIXTH, and it is here for the same reason the
		// first three are: a disposition session's journal is not in the
		// orchestration store, the immutable binding says where it is, and no
		// released reader can route on that binding — a harness Store holds no
		// registry of the bindings it serves and answers only for the keyspace
		// it owns. Which journal store serves which binding is a deployment
		// fact, and there is no release that could make it Host's to know.
		"JournalStores": true,
	}
	for name := range want {
		if !got[name] {
			t.Errorf("Bootstrap no longer declares %s, which a product still has to supply", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("Bootstrap declares %s, which this file does not account for: either the released store cannot answer it and this list must say so, or it can and the composition should bind it", name)
		}
	}
}
