package host_test

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"

	"github.com/looprig/host/internal/modfiles"
)

// centrifugeExemptDir is the ONE directory in which a Centrifuge import is
// legal, spelled once. Every exemption decision is derived from this constant;
// there is no second place to widen.
const centrifugeExemptDir = "internal/realtime/hostlink"

// forbiddenLooprigModules maps a looprig module name to why Host may not import
// it. The key is a MODULE NAME, matched as a whole path segment, never as a
// substring: "github.com/looprig/factorylike" is a different module and is not
// covered by the "factory" entry.
var forbiddenLooprigModules = map[string]string{
	"factory":  "Host is consumed by Factory and must not depend on it",
	"wui":      "Host serves no web UI bundle",
	"tui":      "Host serves no terminal UI",
	"carbon":   "product repository",
	"client":   "product repository",
	"kosa":     "product repository",
	"policy53": "product repository",
	"capstan":  "product repository",
	"tests":    "integration repository",
}

// publishedLooprigVersions is the exact set of looprig module versions Host may
// name. A module absent from this map is unreleased as far as Host is
// concerned, which is the condition that produces a pseudo-version pin.
var publishedLooprigVersions = map[string]string{
	"core":         "v0.7.0",
	"storage":      "v0.6.0",
	"sessionstore": "v0.1.0",
	"fsstore":      "v0.5.1",
	"natsstore":    "v0.5.1",
}

// ---------------------------------------------------------------------------
// Decision functions
// ---------------------------------------------------------------------------

// importAllowed reports whether a module-owned file at fileRelative (a
// slash-separated path relative to the module root) may import importPath.
//
// Every comparison below is over PATH SEGMENTS. Nothing here searches text: a
// sibling module in this workspace banned a SQL keyword with a substring match
// and a newline walked straight through it, and banned in-memory sorting by
// looking for "sort." while slices.SortFunc sailed past. An import path is a
// structured value and is treated as one.
func importAllowed(fileRelative, importPath string) bool {
	allowed, _ := importVerdict(fileRelative, importPath)
	return allowed
}

// importVerdict is importAllowed plus the REASON, which is the thing a person
// reading a failure needs and the thing the guard already had.
//
// forbiddenLooprigModules is a map of reasons. The go.mod rule consumed those
// values from the day it was written; this one discarded them and reported
// "imports forbidden package X", leaving the reader to guess whether X is
// banned by layering, by product boundary, or by mistake. Two guards served by
// one map, one of them throwing the map's payload away.
func importVerdict(fileRelative, importPath string) (bool, string) {
	if importPath == "C" {
		return false, "cgo is invisible to module-graph checks and Host has no business acquiring it"
	}
	segments := strings.Split(importPath, "/")
	if module, ok := looprigModule(segments); ok {
		if reason, forbidden := forbiddenLooprigModules[module]; forbidden {
			return false, reason
		}
		return true, ""
	}
	if isCentrifugal(segments) {
		if underCentrifugeExemption(fileRelative) {
			return true, ""
		}
		return false, "Centrifuge is permitted only under " + centrifugeExemptDir + "/ at the host module root, and this file is outside it"
	}
	return true, ""
}

// looprigModule returns the looprig module name a segmented import path names.
func looprigModule(segments []string) (string, bool) {
	if len(segments) < 3 || segments[0] != "github.com" || segments[1] != "looprig" {
		return "", false
	}
	return segments[2], true
}

// isCentrifugal reports whether a segmented import path names a Centrifugo
// project module. The exemption is granted to the realtime transport as a
// whole, so centrifuge and its protocol package are one class.
func isCentrifugal(segments []string) bool {
	return len(segments) >= 2 && segments[0] == "github.com" && segments[1] == "centrifugal"
}

// underCentrifugeExemption reports whether fileRelative lies inside the single
// exempt directory. fileRelative is relative to the OUTERMOST module root, which
// is what makes the exemption a property of one directory in this repository
// rather than of a directory NAME: see qualify.
//
// The scope is DERIVED from centrifugeExemptDir and compared segment by
// segment, which answers the question this exemption exists to survive: what
// happens when someone adds a second package under internal/realtime. A sibling
// gets nothing, and — because the comparison is over segments and not a string
// prefix — neither does a directory whose name merely starts with "hostlink".
// The exemption cannot be widened by naming a file; only by editing the one
// constant, which is a visible diff.
func underCentrifugeExemption(fileRelative string) bool {
	exempt := strings.Split(centrifugeExemptDir, "/")
	segments := strings.Split(path.Clean(fileRelative), "/")
	if len(segments) <= len(exempt) {
		// Equal length would be the directory itself, which is not a file.
		return false
	}
	return slices.Equal(segments[:len(exempt)], exempt)
}

// replaceViolations reports every local filesystem replace directive.
//
// The classification is modfile's own: a module-to-module replacement always
// carries a version, and a replacement by a directory never does. That is a
// property of the parsed directive, not of how the path happens to be spelled,
// so "../core", "./core" and "/tmp/core" are one case rather than three
// patterns to keep in step.
func replaceViolations(parsed *modfile.File) []string {
	var violations []string
	for _, replacement := range parsed.Replace {
		if replacement.New.Version == "" {
			violations = append(violations, "go.mod replaces "+replacement.Old.Path+
				" with the local filesystem path "+strconv.Quote(replacement.New.Path)+
				"; a published module must not carry one")
			continue
		}
		// A versioned replacement is exempt from the LOCAL-PATH rule and from
		// nothing else. Its target is a module Host actually builds against, so
		// it goes through the same classification a requirement does.
		//
		// This is the standing hazard of this repository, and it was live here:
		// an exclusion added to make a guard pass must itself be tested, and a
		// too-wide one produces exactly the same green. `continue` exempted the
		// target from EVERY rule, so `replace …/core => …/core v0.9.9` — an
		// unpublished Core, which is precisely what publishedLooprigVersions
		// exists to prevent — passed, and so did a replacement onto Factory.
		if violation, bad := looprigDependencyViolation("replaces "+replacement.Old.Path+" with", replacement.New.Path, replacement.New.Version); bad {
			violations = append(violations, violation)
		}
	}
	return violations
}

// goModViolations is EVERY rule that applies to a parsed go.mod, in one place.
//
// It exists because "two callers that must each remember to list the same
// mechanisms" is the shape this file has produced five escapes with, and it
// had quietly grown a sixth: the root guard listed replaceViolations and
// requireViolations, and the nested extension point listed the same two,
// separately, in the other order. Adding a SEVENTH go.mod rule — an
// excludeViolations over the exclude directive, say, whose target
// looprigDependencyViolation already classifies verbatim — and wiring it into
// the root guard alone would have left every declared nested module without
// it, silently, with make check green.
//
// So: a new go.mod rule is added HERE, and both callers inherit it. Adding one
// to a caller instead is the bug, and TestGoModRulesHaveOneSite says so.
func goModViolations(parsed *modfile.File) []string {
	return append(replaceViolations(parsed), requireViolations(parsed)...)
}

// requireViolations reports every looprig requirement Host may not name: a
// forbidden module at any version, a module with no release Host is allowed to
// depend on, and a version other than the published one. Pseudo-versions fail
// as a consequence rather than as a pattern — no pseudo-version is in
// publishedLooprigVersions.
//
// The forbidden check comes FIRST, and its position is the point rather than a
// detail. Consulting only publishedLooprigVersions would reject Factory for
// being ABSENT from it, which is an accident: Factory ships in this same
// program, and extending that map to "every published looprig module" is the
// natural next edit to it. A forbidden module also arrives INDIRECTLY, pulled
// into go.mod by a released dependency with no import anywhere in this
// repository — so the import guard is silent by construction and this is the
// only guard standing. The failure mode of getting this wrong is a green
// `make check` over a Host that depends on Factory.
// See TestForbiddenModulesAreRejectedEvenWhenPublished.
func requireViolations(parsed *modfile.File) []string {
	var violations []string
	for _, requirement := range parsed.Require {
		if violation, bad := looprigDependencyViolation("requires", requirement.Mod.Path, requirement.Mod.Version); bad {
			violations = append(violations, violation)
		}
	}
	return violations
}

// looprigDependencyViolation classifies ONE looprig module Host would build
// against, however go.mod names it. action names the directive so the message
// says which one, because a require and a replace target read very differently
// to whoever has to fix it.
//
// It is shared rather than duplicated: a rule that applies to a requirement and
// not to a replacement target is not a rule, and the two had already drifted
// once — replaceViolations classified nothing at all.
func looprigDependencyViolation(action, modulePath, version string) (string, bool) {
	module, ok := looprigModule(strings.Split(modulePath, "/"))
	if !ok {
		return "", false
	}
	prefix := "go.mod " + action + " " + modulePath + " at " + version
	if reason, forbidden := forbiddenLooprigModules[module]; forbidden {
		return prefix + "; " + reason, true
	}
	published, ok := publishedLooprigVersions[module]
	if !ok {
		return prefix + "; Host has no released version of that module to name", true
	}
	if version != published {
		return prefix + ", which is not the published version " + published, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Import classification
// ---------------------------------------------------------------------------

func TestImportClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		file  string
		imp   string
		allow bool
	}{
		// Legal shapes.
		{name: "standard library", file: "host.go", imp: "context", allow: true},
		{name: "standard library nested", file: "host.go", imp: "path/filepath", allow: true},
		{name: "core root", file: "host.go", imp: "github.com/looprig/core", allow: true},
		{name: "core sessionwire", file: "host.go", imp: "github.com/looprig/core/sessionwire/v1", allow: true},
		{name: "storage", file: "host.go", imp: "github.com/looprig/storage", allow: true},
		{name: "sessionstore", file: "host.go", imp: "github.com/looprig/sessionstore", allow: true},
		{name: "harness", file: "department/runtime.go", imp: "github.com/looprig/harness", allow: true},
		{name: "own module", file: "host.go", imp: "github.com/looprig/host/department", allow: true},
		{name: "drain", file: "internal/lifecycle/drain.go", imp: "github.com/looprig/drain", allow: true},
		{name: "ordinary third party", file: "internal/service/service.go", imp: "golang.org/x/sync/errgroup", allow: true},
		{name: "nats client", file: "internal/commands/consumer.go", imp: "github.com/nats-io/nats.go", allow: true},
		// A module whose name merely BEGINS with a forbidden one is a
		// different module. Substring bans have failed in this program before.
		{name: "factory-prefixed sibling", file: "host.go", imp: "github.com/looprig/factorylike", allow: true},
		{name: "wui-prefixed sibling", file: "host.go", imp: "github.com/looprig/wuikit", allow: true},
		{name: "non-looprig factory", file: "host.go", imp: "github.com/example/factory", allow: true},

		// Forbidden shapes.
		{name: "factory root", file: "host.go", imp: "github.com/looprig/factory", allow: false},
		{name: "factory package", file: "internal/service/service.go", imp: "github.com/looprig/factory/placement", allow: false},
		{name: "factory in a test file", file: "host_test.go", imp: "github.com/looprig/factory", allow: false},
		{name: "factory under the centrifuge exemption", file: centrifugeExemptDir + "/server.go", imp: "github.com/looprig/factory", allow: false},
		{name: "wui", file: "host.go", imp: "github.com/looprig/wui", allow: false},
		{name: "wui package", file: "host.go", imp: "github.com/looprig/wui/contract", allow: false},
		{name: "tui", file: "host.go", imp: "github.com/looprig/tui", allow: false},
		{name: "carbon", file: "host.go", imp: "github.com/looprig/carbon", allow: false},
		{name: "client", file: "host.go", imp: "github.com/looprig/client", allow: false},
		{name: "kosa", file: "host.go", imp: "github.com/looprig/kosa", allow: false},
		{name: "policy53", file: "host.go", imp: "github.com/looprig/policy53", allow: false},
		{name: "capstan", file: "host.go", imp: "github.com/looprig/capstan", allow: false},
		{name: "tests", file: "host.go", imp: "github.com/looprig/tests/harnessfake", allow: false},
		{name: "cgo pseudo-package", file: "host.go", imp: "C", allow: false},

		// Centrifuge: scoped by path, structurally.
		{name: "centrifuge in hostlink", file: centrifugeExemptDir + "/centrifuge.go", imp: "github.com/centrifugal/centrifuge", allow: true},
		{name: "centrifuge in a hostlink subpackage", file: centrifugeExemptDir + "/bindings/bind.go", imp: "github.com/centrifugal/centrifuge", allow: true},
		{name: "centrifuge in a hostlink test", file: centrifugeExemptDir + "/server_test.go", imp: "github.com/centrifugal/centrifuge", allow: true},
		{name: "centrifuge protocol in hostlink", file: centrifugeExemptDir + "/server.go", imp: "github.com/centrifugal/protocol", allow: true},
		{name: "centrifuge at the module root", file: "host.go", imp: "github.com/centrifugal/centrifuge", allow: false},
		{name: "centrifuge in a realtime sibling", file: "internal/realtime/other/x.go", imp: "github.com/centrifugal/centrifuge", allow: false},
		// The second package under internal/realtime is the question this
		// exemption has to survive, and a prefix-suffixed directory name is
		// how a scope grows inside itself.
		{name: "centrifuge in a hostlink-prefixed sibling", file: "internal/realtime/hostlink2/x.go", imp: "github.com/centrifugal/centrifuge", allow: false},
		{name: "centrifuge in realtime itself", file: "internal/realtime/realtime.go", imp: "github.com/centrifugal/centrifuge", allow: false},
		{name: "centrifuge in a differently rooted hostlink", file: "hostlink/server.go", imp: "github.com/centrifugal/centrifuge", allow: false},
		{name: "centrifuge in service", file: "internal/service/service.go", imp: "github.com/centrifugal/centrifuge", allow: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := importAllowed(tt.file, tt.imp); got != tt.allow {
				t.Errorf("importAllowed(%q, %q) = %v, want %v", tt.file, tt.imp, got, tt.allow)
			}
		})
	}
}

// TestModuleImportsStayWithinBoundary is the live guard. It fails loudly if the
// walk reached nothing, and it asserts the walk reached ITS SUBJECT: this very
// file, which contains "github.com/looprig/factory" as a string literal and
// must nonetheless be clean, because the guard parses imports rather than
// searching source text.
//
// The subject it asserts reaching is ONE FILE, and the floors are module-wide.
// That was enough while the module was one package and is not enough now: a
// predicate bug that skipped an entire subdirectory would leave files > 0,
// productionFiles > 0, imports > 0 and this file present, all satisfied by the
// root package alone, and the guard would stay green over an unscanned
// package. department/ is in fact scanned today — checked by hand, not by
// assertion — and a commit message in this repository claimed that as
// "verified rather than assumed", which overstated what the tree holds.
//
// The fix is not an allowlist of package directories, which is the enumeration
// this file has spent six rounds removing. It is an independent oracle: `go
// list ./...` knows the package set without consulting modfiles' predicates, so
// asserting every listed package directory appears in scan.scanned catches a
// widened predicate that hides a whole package. Deliberately not done here —
// it is O0.1's design rather than this task's, and it needs its own RED.
func TestModuleImportsStayWithinBoundary(t *testing.T) {
	t.Parallel()

	scan, err := scanImports(".", "")
	if err != nil {
		t.Fatalf("scan module imports: %v", err)
	}
	for _, violation := range scan.violations {
		t.Error(violation)
	}
	if scan.files == 0 {
		t.Fatal("no Go files were scanned; the boundary guard would be vacuous")
	}
	if scan.productionFiles == 0 {
		t.Fatal("no production Go files were scanned; the boundary guard would be vacuous")
	}
	if !slices.Contains(scan.scanned, "import_boundary_test.go") {
		t.Fatalf("the walk did not reach its own file; scanned = %q", scan.scanned)
	}
	if scan.imports == 0 {
		t.Fatal("no import was classified; the boundary guard would be vacuous")
	}
}

func TestImportScanReportsForbiddenImportsInAFixtureTree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
	writeFixture(t, root, "host.go", "package host\n\nimport _ \"github.com/looprig/factory\"\n")
	writeFixture(t, root, "legal.go", "package host\n\nimport (\n\t_ \"context\"\n\t_ \"github.com/looprig/core/sessionwire/v1\"\n\t_ \"github.com/looprig/sessionstore\"\n\t_ \"github.com/looprig/harness\"\n\t_ \"golang.org/x/sync/errgroup\"\n)\n")
	writeFixture(t, root, "internal/realtime/hostlink/centrifuge.go", "package hostlink\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")
	writeFixture(t, root, "internal/realtime/other/x.go", "package other\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")
	writeFixture(t, root, "internal/service/service_test.go", "package service\n\nimport _ \"github.com/looprig/wui\"\n")
	// Not owned by this module, so not this module's boundary.
	writeFixture(t, root, "testdata/ignored.go", "package ignored\n\nimport _ \"github.com/looprig/factory\"\n")
	writeFixture(t, root, "nested/go.mod", "module example.com/nested\n")
	writeFixture(t, root, "nested/x.go", "package nested\n\nimport _ \"github.com/looprig/factory\"\n")

	scan, err := scanImports(root, "")
	if err != nil {
		t.Fatalf("scan fixture imports: %v", err)
	}

	wantViolations := []string{"host.go", "internal/realtime/other/x.go", "internal/service/service_test.go"}
	for _, want := range wantViolations {
		if !slices.ContainsFunc(scan.violations, func(v string) bool { return strings.Contains(v, want) }) {
			t.Errorf("violations = %q, want one naming %s", scan.violations, want)
		}
	}
	if len(scan.violations) != len(wantViolations) {
		t.Errorf("violations = %q, want exactly %d", scan.violations, len(wantViolations))
	}
	for _, clean := range []string{"legal.go", "hostlink/centrifuge.go", "testdata", "nested"} {
		if slices.ContainsFunc(scan.violations, func(v string) bool { return strings.Contains(v, clean) }) {
			t.Errorf("violations = %q, want nothing naming %s", scan.violations, clean)
		}
	}
}

// TestImportViolationMessagesCarryTheReason holds the payload the message must
// carry. A count-only or path-only message has already cost this program a
// mis-triage; "forbidden package" states that a rule exists, not which one.
func TestImportViolationMessagesCarryTheReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		imp  string
		want string
	}{
		{name: "forbidden module", file: "host.go", imp: "github.com/looprig/factory", want: "Host is consumed by Factory and must not depend on it"},
		{name: "product repository", file: "host.go", imp: "github.com/looprig/carbon", want: "product repository"},
		{name: "web UI", file: "host.go", imp: "github.com/looprig/wui/contract", want: "Host serves no web UI bundle"},
		{name: "centrifuge outside the exemption", file: "internal/service/service.go", imp: "github.com/centrifugal/centrifuge", want: "permitted only under " + centrifugeExemptDir + "/"},
		{name: "cgo", file: "host.go", imp: "C", want: "invisible to module-graph checks"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			allowed, reason := importVerdict(tt.file, tt.imp)
			if allowed {
				t.Fatalf("importVerdict(%q, %q) allowed it", tt.file, tt.imp)
			}
			if !strings.Contains(reason, tt.want) {
				t.Errorf("reason = %q, want it to say %q", reason, tt.want)
			}
		})
	}

	t.Run("an allowed import carries no reason", func(t *testing.T) {
		t.Parallel()
		if allowed, reason := importVerdict("host.go", "github.com/looprig/core"); !allowed || reason != "" {
			t.Errorf("importVerdict for a legal import = (%v, %q), want (true, \"\")", allowed, reason)
		}
	})
}

// TestImportScanCanReportZero exists because a scan that cannot report zero
// cannot be trusted when it reports zero.
func TestImportScanCanReportZero(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
	writeFixture(t, root, "testdata/ignored.go", "package ignored\n")

	scan, err := scanImports(root, "")
	if err != nil {
		t.Fatalf("scan empty fixture: %v", err)
	}
	if scan.files != 0 || scan.productionFiles != 0 || scan.imports != 0 {
		t.Fatalf("scan of a module with no owned Go files = %+v, want zeroes", scan)
	}
}

func TestImportScanRejectsGoSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := t.TempDir()
	writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
	writeFixture(t, target, "target.go", "package target\n")
	if err := os.Symlink(filepath.Join(target, "target.go"), filepath.Join(root, "linked.go")); err != nil {
		t.Fatalf("create Go file symlink: %v", err)
	}

	_, err := scanImports(root, "")
	var symlinkErr *modfiles.SymlinkError
	if !errors.As(err, &symlinkErr) {
		t.Fatalf("scanImports() error = %v, want *modfiles.SymlinkError", err)
	}
}

// TestCentrifugeExemptionHoldsAtTheLiveRootSpelling drives the classifier with
// the path shape the LIVE enumerator produces for the root the live guard
// actually passes ("."), not with a hand-written one. A sibling module lost a
// whole classification to exactly this gap: its fixtures rooted at an already
// absolute t.TempDir(), so filepath.Rel never saw the relative root the real
// call uses, and the classifier silently answered false for every real file.
func TestCentrifugeExemptionHoldsAtTheLiveRootSpelling(t *testing.T) {
	t.Parallel()

	const liveRoot = "."

	files, err := modfiles.Files(liveRoot)
	if err != nil {
		t.Fatalf("modfiles.Files(%q): %v", liveRoot, err)
	}
	if len(files) == 0 {
		t.Fatal("the live enumerator returned no files, so this test would be vacuous")
	}
	for _, path := range files {
		if !filepath.IsAbs(path) {
			t.Fatalf("modfiles.Files(%q) returned relative path %q; the classification below rests on absolute ones", liveRoot, path)
		}
	}

	absRoot, err := filepath.Abs(liveRoot)
	if err != nil {
		t.Fatalf("absolute live root: %v", err)
	}
	exempt := filepath.Join(absRoot, filepath.FromSlash(centrifugeExemptDir), "centrifuge.go")
	relative, err := moduleRelative(liveRoot, exempt)
	if err != nil {
		t.Fatalf("moduleRelative(%q, %q): %v", liveRoot, exempt, err)
	}
	if relative != centrifugeExemptDir+"/centrifuge.go" {
		t.Fatalf("moduleRelative(%q, %q) = %q, want %q", liveRoot, exempt, relative, centrifugeExemptDir+"/centrifuge.go")
	}
	if !importAllowed(relative, "github.com/centrifugal/centrifuge") {
		t.Fatalf("a real hostlink file would be reported as a Centrifuge violation")
	}

	sibling := filepath.Join(absRoot, "internal", "realtime", "other", "x.go")
	siblingRelative, err := moduleRelative(liveRoot, sibling)
	if err != nil {
		t.Fatalf("moduleRelative(%q, %q): %v", liveRoot, sibling, err)
	}
	if importAllowed(siblingRelative, "github.com/centrifugal/centrifuge") {
		t.Fatalf("a second package under internal/realtime inherited the hostlink exemption")
	}
}

// ---------------------------------------------------------------------------
// go.mod
// ---------------------------------------------------------------------------

func TestGoModHasNoLocalReplaceAndNamesOnlyPublishedVersions(t *testing.T) {
	t.Parallel()

	parsed := parseGoMod(t, "go.mod")
	if parsed.Module == nil || parsed.Module.Mod.Path != "github.com/looprig/host" {
		t.Fatalf("go.mod module = %+v, want github.com/looprig/host; the guard is reading the wrong file", parsed.Module)
	}
	if len(parsed.Require) == 0 {
		t.Fatal("go.mod requires nothing, so the go.mod rules below would be vacuous")
	}
	for _, violation := range goModViolations(parsed) {
		t.Error(violation)
	}
}

func TestReplaceViolations(t *testing.T) {
	t.Parallel()

	// Reasons, not counts — the same lesson the require table learned. A
	// replacement has two distinct failure modes and a count cannot tell them
	// apart.
	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{name: "no replace", source: "module m\n\ngo 1.26.6\n"},
		{
			name:   "relative directory",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => ../core\n",
			want:   []string{`replaces github.com/looprig/core with the local filesystem path "../core"`},
		},
		{
			name:   "same directory",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => ./core\n",
			want:   []string{`the local filesystem path "./core"`},
		},
		{
			name:   "absolute directory",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => /tmp/core\n",
			want:   []string{`the local filesystem path "/tmp/core"`},
		},
		{
			name:   "block form",
			source: "module m\n\ngo 1.26.6\n\nreplace (\n\tgithub.com/looprig/core => ../core\n\tgithub.com/looprig/storage => ../storage\n)\n",
			want:   []string{`"../core"`, `"../storage"`},
		},
		{
			name:   "versioned replacement is not a filesystem replace",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => github.com/fork/core v0.7.0\n",
		},
		// A versioned replacement is exempt from the LOCAL-PATH rule only. Its
		// target is a module Host builds against, and every other rule applies
		// to it. The exemption used to exempt it from all of them, so each of
		// the next three passed.
		{
			name:   "versioned replacement onto a forbidden module",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/storage => github.com/looprig/factory v0.9.9\n",
			want:   []string{`replaces github.com/looprig/storage with github.com/looprig/factory at v0.9.9; Host is consumed by Factory and must not depend on it`},
		},
		{
			name:   "versioned replacement onto an unpublished version",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => github.com/looprig/core v0.9.9\n",
			want:   []string{`replaces github.com/looprig/core with github.com/looprig/core at v0.9.9, which is not the published version v0.7.0`},
		},
		{
			name:   "versioned replacement onto an unreleased module",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => github.com/looprig/harness v0.30.2\n",
			want:   []string{`with github.com/looprig/harness at v0.30.2; Host has no released version of that module to name`},
		},
		{
			name:   "versioned replacement onto a non-looprig fork is unconstrained",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => github.com/fork/core v9.9.9\n",
		},
		{
			name:   "a local replace of a forbidden module reports the local path, which is the fix",
			source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/factory => ../factory\n",
			want:   []string{`the local filesystem path "../factory"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := modfile.Parse("go.mod", []byte(tt.source), nil)
			if err != nil {
				t.Fatalf("parse fixture go.mod: %v", err)
			}
			got := replaceViolations(parsed)
			if len(got) != len(tt.want) {
				t.Fatalf("replaceViolations() = %q (%d), want %d", got, len(got), len(tt.want))
			}
			for i, want := range tt.want {
				if !strings.Contains(got[i], want) {
					t.Errorf("replaceViolations()[%d] = %q, want it to say %q", i, got[i], want)
				}
			}
		})
	}
}

func TestRequireViolations(t *testing.T) {
	t.Parallel()

	// Each row asserts the REASON, not just the count. A count-only assertion
	// let a mutant through the whole suite here once: the message degraded to
	// "not the published version \"\"" for an unreleased module, which sends a
	// maintainer hunting version skew that does not exist. A kill whose message
	// does not say what broke is a kill that gets mis-triaged later.
	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{
			name:   "published pins",
			source: "module m\n\ngo 1.26.6\n\nrequire (\n\tgithub.com/looprig/core v0.7.0\n\tgithub.com/looprig/sessionstore v0.1.0\n\tgithub.com/looprig/storage v0.6.0\n)\n",
		},
		{
			name:   "non-looprig dependency is unconstrained",
			source: "module m\n\ngo 1.26.6\n\nrequire golang.org/x/mod v0.40.0\n",
		},
		{
			name:   "unreleased module",
			source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/harness v0.30.2\n",
			want:   []string{`requires github.com/looprig/harness at v0.30.2; Host has no released version of that module to name`},
		},
		{
			name:   "pseudo-version",
			source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/sessionstore v0.0.0-20260901060329-a34464c893e6\n",
			want:   []string{`requires github.com/looprig/sessionstore at v0.0.0-20260901060329-a34464c893e6, which is not the published version v0.1.0`},
		},
		{
			name:   "unpublished version of a released module",
			source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/core v0.8.0\n",
			want:   []string{`requires github.com/looprig/core at v0.8.0, which is not the published version v0.7.0`},
		},
		{
			// A forbidden module must be rejected FOR BEING FORBIDDEN. Today it
			// is also unreleased, and a rejection that rests on that is an
			// accident of absence: Factory ships in this same program.
			name:   "factory",
			source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/factory v0.1.0\n",
			want:   []string{`requires github.com/looprig/factory at v0.1.0; Host is consumed by Factory and must not depend on it`},
		},
		{
			// The realistic arrival is INDIRECT, through a released looprig
			// dependency, where there is no import for the import guard to see.
			name:   "forbidden module arriving indirectly",
			source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/wui v0.1.1 // indirect\n",
			want:   []string{`requires github.com/looprig/wui at v0.1.1; Host serves no web UI bundle`},
		},
		{
			name:   "several at once, in file order",
			source: "module m\n\ngo 1.26.6\n\nrequire (\n\tgithub.com/looprig/core v0.7.0\n\tgithub.com/looprig/factory v0.1.0\n\tgithub.com/looprig/harness v0.30.2\n)\n",
			want: []string{
				`github.com/looprig/factory at v0.1.0; Host is consumed by Factory`,
				`github.com/looprig/harness at v0.30.2; Host has no released version`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := modfile.Parse("go.mod", []byte(tt.source), nil)
			if err != nil {
				t.Fatalf("parse fixture go.mod: %v", err)
			}
			got := requireViolations(parsed)
			if len(got) != len(tt.want) {
				t.Fatalf("requireViolations() = %q (%d), want %d", got, len(got), len(tt.want))
			}
			for i, want := range tt.want {
				if !strings.Contains(got[i], want) {
					t.Errorf("requireViolations()[%d] = %q, want it to say %q", i, got[i], want)
				}
			}
		})
	}
}

// TestForbiddenModulesAreRejectedEvenWhenPublished pins the ORDER of the two
// rules in requireViolations, which is the whole of finding 1.
//
// The ban on Factory must not rest on Factory being absent from
// publishedLooprigVersions, because Factory ships in this same program and
// "make the version map list every published looprig module" is the natural
// next edit to that map. If the ban were an accident of absence, that edit
// would silently let Host depend on Factory — and an INDIRECT requirement has
// no import for the import guard to catch, so the go.mod guard is the only one
// standing.
//
// It is not parallel: it substitutes the package-level map, and every reader of
// that map is a parallel test that is paused for the duration of a sequential
// one.
func TestForbiddenModulesAreRejectedEvenWhenPublished(t *testing.T) {
	original := publishedLooprigVersions
	t.Cleanup(func() { publishedLooprigVersions = original })

	extended := make(map[string]string, len(original)+len(forbiddenLooprigModules))
	for module, version := range original {
		extended[module] = version
	}
	for module := range forbiddenLooprigModules {
		extended[module] = "v1.0.0"
	}
	publishedLooprigVersions = extended

	for module, reason := range forbiddenLooprigModules {
		source := "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/" + module + " v1.0.0\n"
		parsed, err := modfile.Parse("go.mod", []byte(source), nil)
		if err != nil {
			t.Fatalf("parse fixture go.mod: %v", err)
		}
		got := requireViolations(parsed)
		if len(got) != 1 {
			t.Errorf("requireViolations() for a published %s = %q, want exactly one violation", module, got)
			continue
		}
		if !strings.Contains(got[0], reason) {
			t.Errorf("requireViolations() for %s = %q, want it to say %q", module, got[0], reason)
		}
	}
}

// ---------------------------------------------------------------------------
// Nested modules
// ---------------------------------------------------------------------------

// nestedModuleAllowlist names every directory under this module that is
// deliberately its own module or repository, relative to the module root and
// slash-separated. It is EMPTY, and adding to it is a decision, not an
// accident.
var nestedModuleAllowlist []string

// TestModuleContainsNoUndeclaredNestedModule closes the one hole the file
// enumerator has by construction.
//
// modfiles.Files skips any directory holding go.mod or .git, which is correct —
// a nested repository is not this module's content — but the consequence is
// that the boundary is a property of the MODULE rather than of the TREE, and
// silently so. A nested module beside a file importing Factory is not scanned,
// and the guard reports a clean walk.
//
// Exploitability is low: a nested module is not in `go build ./...`, and the
// parent cannot reach it without a require plus a replace, which
// replaceViolations already bans. What this assertion buys is that the day
// someone legitimately publishes a nested module from inside this repository —
// the established workspace pattern, see flow/store and pluto/cmd/pluto — they
// are stopped and made to decide.
//
// A declared entry BUYS AN EXTRA SCAN rooted at it; it does not suspend the
// rule. A suspension would mean the first real entry lands as untested code
// written under pressure to turn a red guard green, which is the worst moment
// to be writing a boundary. The scan and the entry format are exercised today
// by TestNestedModuleScanCoversADeclaredModule, with no entry in the live list.
func TestModuleContainsNoUndeclaredNestedModule(t *testing.T) {
	t.Parallel()

	scan, err := scanNestedModules(".", nestedModuleAllowlist)
	if err != nil {
		t.Fatalf("walk for nested modules: %v", err)
	}
	if scan.directories == 0 {
		t.Fatal("no directory was visited; this assertion would be vacuous")
	}
	for _, path := range scan.undeclared {
		t.Errorf("%s is a nested module or repository, so nothing inside it is covered by the import guard. Either remove it, or add it to nestedModuleAllowlist, which does not suspend the boundary — it extends the guard into that module", path)
	}
	for _, violation := range scan.violations {
		t.Error(violation)
	}
	// A declared entry that matches nothing is a stale entry silently doing
	// nothing, which is how an allowlist rots into a comment.
	for _, declared := range nestedModuleAllowlist {
		if !slices.Contains(scan.declared, declared) {
			t.Errorf("nestedModuleAllowlist names %q, which is not a nested module of this repository. Remove it, or correct it to the module-root-relative slash-separated path nestedModuleDirectories reports", declared)
		}
	}
}

func TestNestedModuleScanCoversADeclaredModule(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
	writeFixture(t, root, "host.go", "package host\n")
	writeFixture(t, root, "internal/httpapi/go.mod", "module github.com/looprig/host/internal/httpapi\n")
	writeFixture(t, root, "internal/httpapi/api.go", "package httpapi\n\nimport _ \"github.com/looprig/factory\"\n")
	writeFixture(t, root, "internal/httpapi/legal.go", "package httpapi\n\nimport _ \"github.com/looprig/core\"\n")

	t.Run("undeclared: reported, and its contents unscanned", func(t *testing.T) {
		t.Parallel()
		scan, err := scanNestedModules(root, nil)
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if !slices.Equal(scan.undeclared, []string{"internal/httpapi"}) {
			t.Errorf("undeclared = %q, want [internal/httpapi]", scan.undeclared)
		}
		if len(scan.violations) != 0 {
			t.Errorf("violations = %q, want none: an undeclared module is reported as a module, not scanned", scan.violations)
		}
	})

	t.Run("declared: scanned, and the forbidden import inside it reported", func(t *testing.T) {
		t.Parallel()
		scan, err := scanNestedModules(root, []string{"internal/httpapi"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.undeclared) != 0 {
			t.Errorf("undeclared = %q, want none", scan.undeclared)
		}
		if !slices.Equal(scan.declared, []string{"internal/httpapi"}) {
			t.Fatalf("declared = %q, want [internal/httpapi]. The allowlist entry format must be exactly what nestedModuleDirectories reports", scan.declared)
		}
		if len(scan.violations) != 1 || !strings.HasPrefix(scan.violations[0], "internal/httpapi/api.go imports ") ||
			!strings.Contains(scan.violations[0], "Host is consumed by Factory") {
			t.Errorf("violations = %q, want one naming internal/httpapi/api.go and its reason", scan.violations)
		}
	})

	// B: the extension has to buy BOTH halves of the guard, not one level of
	// one half. A nested module inside a declared one was neither reported nor
	// scanned, because scanImports walks with modfiles.Files, which stops at a
	// boundary — so declaring a module DISABLED, for that path, the very
	// mechanism that reports this situation at the top level.
	t.Run("a nested module inside a declared one is reported", func(t *testing.T) {
		t.Parallel()
		deep := t.TempDir()
		writeFixture(t, deep, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, deep, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, deep, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		writeFixture(t, deep, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")
		writeFixture(t, deep, "tools/checkout/inner/go.mod", "module example.com/inner\n")
		writeFixture(t, deep, "tools/checkout/inner/bad.go", "package inner\n\nimport _ \"github.com/looprig/factory\"\n")

		scan, err := scanNestedModules(deep, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if !slices.Equal(scan.undeclared, []string{"tools/checkout/inner"}) {
			t.Errorf("undeclared = %q, want [tools/checkout/inner]. A declared module inherits the whole guard, including the part that finds nested modules", scan.undeclared)
		}
	})

	t.Run("a module nested two deep is scanned when both levels are declared", func(t *testing.T) {
		t.Parallel()
		deep := t.TempDir()
		writeFixture(t, deep, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, deep, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, deep, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		writeFixture(t, deep, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")
		writeFixture(t, deep, "tools/checkout/inner/go.mod", "module example.com/inner\n")
		writeFixture(t, deep, "tools/checkout/inner/bad.go", "package inner\n\nimport _ \"github.com/looprig/factory\"\n")

		scan, err := scanNestedModules(deep, []string{"tools/checkout", "tools/checkout/inner"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.undeclared) != 0 {
			t.Errorf("undeclared = %q, want none", scan.undeclared)
		}
		if !slices.Equal(scan.declared, []string{"tools/checkout", "tools/checkout/inner"}) {
			t.Fatalf("declared = %q, want both levels named by their full root-relative paths", scan.declared)
		}
		if len(scan.violations) != 1 || !strings.HasPrefix(scan.violations[0], "tools/checkout/inner/bad.go imports ") ||
			!strings.Contains(scan.violations[0], "Host is consumed by Factory") {
			t.Errorf("violations = %q, want one naming tools/checkout/inner/bad.go and its reason", scan.violations)
		}
	})

	// C: the arm has to floor the quantity the CALLER CONSUMES. Violations come
	// from classified imports; flooring only the file count leaves a declared
	// module of import-free files reporting a confident zero.
	t.Run("a declared module whose files carry no import classifies nothing", func(t *testing.T) {
		t.Parallel()
		bare := t.TempDir()
		writeFixture(t, bare, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, bare, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, bare, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		writeFixture(t, bare, "tools/checkout/ok.go", "package checkout\n")

		scan, err := scanNestedModules(bare, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.violations) != 1 || !strings.Contains(scan.violations[0], "no import") {
			t.Fatalf("violations = %q, want one saying the extra scan classified no import", scan.violations)
		}
	})

	t.Run("a declared module with no Go file is a vacuous scan", func(t *testing.T) {
		t.Parallel()
		empty := t.TempDir()
		writeFixture(t, empty, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, empty, "host.go", "package host\n")
		writeFixture(t, empty, "tools/checkout/.git/HEAD", "ref: refs/heads/main\n")

		scan, err := scanNestedModules(empty, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.violations) != 1 || !strings.Contains(scan.violations[0], "no Go file") {
			t.Fatalf("violations = %q, want one saying the extra scan found no Go file", scan.violations)
		}
	})
}

// TestDeclaredNestedModuleInheritsTheWholeGuard is the terminating assertion of
// a sequence: five rounds of "extend mechanism X into the nested scan", each of
// which left the OTHER mechanisms behind. The extension point applies the whole
// guard now, and the subtests below are one per mechanism rather than one per
// bug, so the next mechanism added has a slot it visibly does not fill.
func TestDeclaredNestedModuleInheritsTheWholeGuard(t *testing.T) {
	t.Parallel()

	// Mechanism 1, the part that is PATH-SCOPED. Classification used to happen
	// on a path relative to the NESTED root, because qualification was applied
	// at report time and not at classification time, so every path-scoped rule
	// silently re-anchored. Declaring tools/checkout granted it its own
	// Centrifuge exemption, and the exemption whose whole point is that it can
	// only be widened by editing one constant was widened by CREATING A
	// DIRECTORY.
	t.Run("a path-scoped rule stays anchored to the outermost module root", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		// The outer module's own exemption still holds.
		writeFixture(t, root, centrifugeExemptDir+"/server.go", "package hostlink\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")
		writeFixture(t, root, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		// The escape: a directory inside the nested module spelled exactly like
		// the exempt one.
		writeFixture(t, root, "tools/checkout/"+centrifugeExemptDir+"/x.go", "package hostlink\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")
		// The control, one position over, proving the payload is catchable.
		writeFixture(t, root, "tools/checkout/other/y.go", "package other\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		// HasPrefix, not Contains: a violation qualified TWICE — once at
		// classification and again at report time — still CONTAINS the right
		// path, and a Contains assertion cannot see the difference.
		for _, want := range []string{
			"tools/checkout/" + centrifugeExemptDir + "/x.go imports ",
			"tools/checkout/other/y.go imports ",
		} {
			if !slices.ContainsFunc(scan.violations, func(v string) bool { return strings.HasPrefix(v, want) }) {
				t.Errorf("violations = %q, want one naming %s. Declaring a nested module must not grant it its own copy of a path-scoped exemption", scan.violations, want)
			}
		}
		if len(scan.violations) != 2 {
			t.Errorf("violations = %q, want exactly 2", scan.violations)
		}
		// The message has to say WHOSE directory it means, because it now names
		// a file in a different module than the directory it cites.
		for _, violation := range scan.violations {
			if !strings.Contains(violation, "host module root") {
				t.Errorf("violation %q does not say which module's %s is meant", violation, centrifugeExemptDir)
			}
		}
	})

	// The exemption still works where it is granted: the outer module root.
	t.Run("the outermost module keeps its own exemption", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, centrifugeExemptDir+"/server.go", "package hostlink\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")

		scan, err := scanImports(root, "")
		if err != nil {
			t.Fatalf("scanImports: %v", err)
		}
		if len(scan.violations) != 0 {
			t.Fatalf("violations = %q, want none", scan.violations)
		}
	})

	// Mechanisms 2 and 3. The declared module's OWN go.mod was never parsed, so
	// neither the looprig-dependency rule nor the local-path rule applied to
	// it. Both payloads are proven catchable in the root go.mod by
	// TestRequireViolations and TestReplaceViolations; the escape is position,
	// not payload. This matters more than it reads: requireViolations' own
	// comment calls itself "the only guard standing" for a forbidden module
	// arriving indirectly, because the import guard is silent by construction
	// there.
	t.Run("the declared module's own go.mod goes through both dependency mechanisms", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n\ngo 1.26.6\n\nrequire github.com/looprig/factory v0.1.0 // indirect\n\nreplace github.com/looprig/core => ../../../core\n")
		writeFixture(t, root, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if scan.goModFiles != 1 {
			t.Fatalf("goModFiles = %d, want 1; the dependency mechanisms never saw a go.mod, so their zero is vacuous", scan.goModFiles)
		}
		for _, want := range []string{
			"Host is consumed by Factory and must not depend on it",
			"local filesystem path",
		} {
			if !slices.ContainsFunc(scan.violations, func(v string) bool { return strings.Contains(v, want) }) {
				t.Errorf("violations = %q, want one saying %q", scan.violations, want)
			}
		}
		for _, violation := range scan.violations {
			if !strings.HasPrefix(violation, "tools/checkout/go.mod ") {
				t.Errorf("violation %q does not name which module's go.mod is at fault", violation)
			}
		}
		if len(scan.violations) != 2 {
			t.Errorf("violations = %q, want exactly 2", scan.violations)
		}
	})

	// The control for the pair above: a clean nested go.mod is silent, so the
	// two violations are the directives and not the parsing.
	t.Run("a clean nested go.mod reports nothing", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n\ngo 1.26.6\n\nrequire github.com/looprig/core v0.7.0\n")
		writeFixture(t, root, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.violations) != 0 {
			t.Fatalf("violations = %q, want none", scan.violations)
		}
		if scan.goModFiles != 1 {
			t.Fatalf("goModFiles = %d, want 1", scan.goModFiles)
		}
	})

	// A declared boundary is not always a MODULE: a vendored git checkout is a
	// boundary with a .git and no go.mod. The dependency mechanisms have
	// nothing to read and must not invent a violation.
	t.Run("a declared repository with no go.mod is not a dependency violation", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/.git/HEAD", "ref: refs/heads/main\n")
		writeFixture(t, root, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.violations) != 0 {
			t.Fatalf("violations = %q, want none", scan.violations)
		}
		if scan.goModFiles != 0 {
			t.Fatalf("goModFiles = %d, want 0", scan.goModFiles)
		}
	})

	// The cross product, which is the shape every escape in this file has had:
	// a mechanism applied at one depth and not the next. The recursion carries
	// the qualified prefix into BOTH the import scan and the go.mod check, so a
	// module two levels down is checked by everything.
	t.Run("a go.mod two levels deep goes through the dependency mechanisms", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		writeFixture(t, root, "tools/checkout/ok.go", "package checkout\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/inner/go.mod", "module example.com/inner\n\ngo 1.26.6\n\nrequire github.com/looprig/wui v1.2.3\n")
		writeFixture(t, root, "tools/checkout/inner/ok.go", "package inner\n\nimport _ \"context\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout", "tools/checkout/inner"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if scan.goModFiles != 2 {
			t.Errorf("goModFiles = %d, want 2; the recursion did not carry the go.mod check down", scan.goModFiles)
		}
		if len(scan.violations) != 1 ||
			!strings.HasPrefix(scan.violations[0], "tools/checkout/inner/go.mod ") ||
			!strings.Contains(scan.violations[0], "Host serves no web UI bundle") {
			t.Fatalf("violations = %q, want one naming tools/checkout/inner/go.mod and its reason", scan.violations)
		}
	})

	// Mechanism 4 flooring. productionFiles is deliberately NOT floored here,
	// and that asymmetry with the root scan was reasoning in a comment with no
	// test under it. A declared module that is genuinely test support is a
	// legitimate shape; flooring it would force a false violation, and a later
	// reviewer applying "floor what the caller consumes" mechanically would add
	// the floor and break it. This subtest is the evidence.
	t.Run("a declared module holding only test files is not a vacuous scan", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n\nimport _ \"context\"\n")
		writeFixture(t, root, "tools/checkout/go.mod", "module github.com/looprig/host/tools/checkout\n")
		writeFixture(t, root, "tools/checkout/helper_test.go", "package checkout_test\n\nimport _ \"testing\"\n")

		scan, err := scanNestedModules(root, []string{"tools/checkout"})
		if err != nil {
			t.Fatalf("scanNestedModules: %v", err)
		}
		if len(scan.violations) != 0 {
			t.Fatalf("violations = %q, want none: a declared nested module may legitimately be test support", scan.violations)
		}
	})
}

// goModRuleConsumers names the two functions that run the go.mod rules over a
// parsed file: the root guard and the nested extension point. It enumerates
// CONSUMERS, deliberately, and not rules — the consumers are fixed at two
// because there are two go.mod files a run can see, while the rules are the
// thing that grows, and enumerating the growing side is what this file has
// spent six rounds paying for.
var goModRuleConsumers = []string{
	"TestGoModHasNoLocalReplaceAndNamesOnlyPublishedVersions",
	"nestedGoModViolations",
}

// TestGoModRulesHaveOneSite is what makes "a new go.mod rule is added in one
// place" a checkable claim rather than a note in CLAUDE.md.
//
// Extracting goModViolations makes the two consumers agree TODAY. It does not
// stop the next maintainer adding an excludeViolations to one of them directly,
// which is precisely the edit that produced this round's finding — the root
// guard and the nested arm each listed the same two rules, separately, and a
// seventh wired into the root alone left every declared nested module without
// it. So the shape is asserted: a consumer calls goModViolations and no other
// rule function.
//
// Its limits, largest first, because a guard that discloses its second-largest
// limit and not its largest is prose asserting more than the code holds — which
// is this repository's most persistent defect and the thing six rounds were
// spent on.
//
//   - A rule written INLINE in a consumer — a for over parsed.Exclude calling
//     looprigDependencyViolation, three lines, no function at all — escapes
//     entirely, and that is a likelier edit than writing a whole rule function.
//     Measured: with those three lines in nestedGoModViolations, the same
//     go.mod yields [] from the root consumer's path and a violation from the
//     nested one, with this test PASSING. Widening the hook to catch
//     parsed.<Field> access inside a consumer is not the answer: the root guard
//     legitimately reads parsed.Module and parsed.Require for its own vacuity
//     floors, so it would false-positive, and enumerating modfile's fields
//     reintroduces exactly the enumeration this test exists to avoid.
//   - A rule function named off-convention — checkExcludes rather than
//     excludeViolations — escapes the …Violations hook. Smaller, because it
//     takes deliberate deviation from a convention the file follows throughout.
//   - The check is FILE-SCOPED: it parses import_boundary_test.go alone, while
//     TestDocCommentsNameTheirOwnDeclaration twelve lines below enumerates the
//     whole module. The consequence is not only narrowness — a CORRECT consumer
//     in a sibling file cannot be listed at all, because the staleness arm
//     below would report it as naming no function in this file. Latent while
//     the guard is one file; fix the scope before it is two.
//   - The consumer list is policed for STALENESS, not for OMISSION. An entry
//     naming nothing fails; a third consumer nobody listed is simply not
//     checked. Half-policed, and accepted: the alternative is deciding
//     structurally what counts as a consumer, which is the enumeration problem
//     again one level up.
//
// What it does hold is the shape that actually failed here: a rule function
// that exists and is called from a consumer instead of from goModViolations.
func TestGoModRulesHaveOneSite(t *testing.T) {
	t.Parallel()

	parsed, err := parser.ParseFile(token.NewFileSet(), "import_boundary_test.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse import_boundary_test.go: %v", err)
	}

	found := map[string]bool{}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !slices.Contains(goModRuleConsumers, function.Name.Name) {
			continue
		}
		found[function.Name.Name] = true

		calls := map[string]bool{}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if callee, ok := call.Fun.(*ast.Ident); ok && strings.HasSuffix(callee.Name, "Violations") {
				calls[callee.Name] = true
			}
			return true
		})

		if !calls["goModViolations"] {
			t.Errorf("%s does not call goModViolations, so the single site is not the site it uses", function.Name.Name)
		}
		for callee := range calls {
			if callee != "goModViolations" {
				t.Errorf("%s calls %s directly. Every go.mod rule goes through goModViolations, so that both this consumer and the other one inherit a new rule from one edit; listing a rule here is how the other consumer silently misses it", function.Name.Name, callee)
			}
		}
	}
	for _, consumer := range goModRuleConsumers {
		if !found[consumer] {
			t.Errorf("goModRuleConsumers names %q, which is not a function in this file. Correct it or remove it; a stale entry asserts nothing", consumer)
		}
	}
}

// TestDocCommentsNameTheirOwnDeclaration is a structural check for a mechanism
// that has now produced three defects in this repository by itself: a comment
// written for one declaration attaches to the declaration that happens to sit
// under it, because a blank line was missing. gofmt, go vet and staticcheck all
// pass it, and the result is a documented thing with someone else's
// documentation and an undocumented thing beside it. Round 3 lost
// moduleRelative's doc to nestedModuleDirectories; round 5 lost
// FuzzBoundaryMarkerName's to boundaryMarkers. Three by one mechanism is a
// missing check, not a slip.
//
// FIVE firings, zero false positives by the guard's own definition, and the
// distribution is the useful part: ONE was a stolen doc comment (the shape the
// check was built for) and FOUR were prose in one declaration's comment opening
// with the name of a declaration that had no documentation anywhere. Both are
// real — an undocumented declaration whose name reads like a doc heading
// somewhere else is exactly how the first kind starts — but the message
// described only the first repair, so four correct findings read as false
// positives. It names both repairs now.
//
// ONE LIMIT WORTH KNOWING, and it arrived without a firing: a MISSPELLED
// self-reference is invisible here. The check only inspects a line whose first
// word is a known declaration name, so a doc comment opening
// "TestDurationsHaveNoUndOCUMENTEDMinimum" against the real
// TestDurationsHaveNoUndocumentedMinimum is classified as prose and skipped.
// The stated reason for not fixing it matters, because a wrong one makes the
// problem look harder than it is. It is NOT that near-miss matching needs a
// distance heuristic that would fire on ordinary words: the edit distance from
// TestDurationsHaveNoUndocumentedMinimum to any English word is enormous. The
// real false-positive risk is SHORT declaration names — Now, New, ID, Host,
// Clock all sit within a couple of edits of ordinary prose — which sinks the
// naive version and nothing more.
//
// A cheaper rule needs no distance metric at all: flag a CamelCase token at the
// start of a doc-comment line, over some length, matching no declaration in the
// package. Detection by exclusion rather than by proximity, and prose contains
// no long CamelCase tokens. Not built, because it guards a comment typo that a
// reader caught — but it is available, and the next person should know the
// obstacle is short names rather than the idea.
//
// The rule is stated over the SYMPTOM rather than over Go's naming convention,
// because the convention alone gives both false negatives and false positives.
// A line inside a doc comment that opens with the name of a declaration in this
// module is a violation only when that declaration has no doc comment of its
// own. That is exactly what a missing blank line produces, in either ordering —
// the merged comment can name the attached declaration first and bury the other
// one further down, which the first version of this check did not see — while a
// comment that merely DISCUSSES a documented declaration, common and correct in
// this file, is left alone.
func TestDocCommentsNameTheirOwnDeclaration(t *testing.T) {
	t.Parallel()

	files, err := modfiles.Files(".")
	if err != nil {
		t.Fatalf("enumerate module files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no Go files were enumerated; this check would be vacuous")
	}

	// documented is one LINE of one declaration's doc comment, not one comment.
	// Checking only the first line catches the comment written for the
	// declaration below and left attached to the one above, but not the mirror
	// image — two doc comments merged into one group, whose first line names
	// the declaration it is attached to correctly while a second declaration's
	// documentation is buried inside it and that declaration has none. Both
	// have occurred here; the mirror survived the first version of this check.
	type documented struct {
		file  string
		names []string
		word  string
	}
	var (
		declarationNames = map[string]bool{}
		documentedNames  = map[string]bool{}
		docs             []documented
	)
	for _, file := range files {
		relative, err := moduleRelative(".", file)
		if err != nil {
			t.Fatalf("relativize %s: %v", file, err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", relative, err)
		}
		for _, declaration := range parsed.Decls {
			names, doc := declaredNames(declaration)
			for _, name := range names {
				declarationNames[name] = true
				if doc != nil {
					documentedNames[name] = true
				}
			}
			if doc == nil || len(names) == 0 {
				continue
			}
			for _, line := range doc.List {
				fields := strings.Fields(line.Text)
				if len(fields) < 3 || fields[0] != "//" {
					continue
				}
				docs = append(docs, documented{file: relative, names: names, word: fields[1]})
			}
		}
	}
	if len(docs) == 0 {
		t.Fatal("no documented declaration was found; this check would be vacuous")
	}

	checked := 0
	for _, doc := range docs {
		if !declarationNames[doc.word] {
			// The line opens with prose rather than a name. Go's convention is
			// not universally followed in this repository and enforcing it is
			// not this test's business; attachment is.
			continue
		}
		checked++
		if slices.Contains(doc.names, doc.word) {
			continue
		}
		// documentedNames is keyed by BARE name across every package in the
		// module, so a name defined in two packages and documented in only one
		// suppresses a real finding in the other. That is a false NEGATIVE and
		// never a false positive, which is the right way round for a check
		// whose value depends on never firing on prose.
		if documentedNames[doc.word] {
			// A comment that DISCUSSES another declaration is ordinary and
			// frequent in this file — "forbiddenLooprigModules is a map of
			// reasons" appears in importVerdict's doc and belongs there. What
			// is not ordinary is a declaration with no documentation of its own
			// whose name opens a line inside someone else's, which is exactly
			// what a missing blank line produces, in both orderings.
			continue
		}
		t.Errorf("%s: %s has NO DOC COMMENT OF ITS OWN, and a line opening with its name sits inside %q's. Two edits fix this and they are different repairs: if the line was written FOR %s, separate the comments with a blank line so it attaches to the right declaration; if it merely MENTIONS %s in prose, give %s a doc comment of its own and this stops firing. The check cannot tell those apart, which is why it names both",
			doc.file, doc.word, strings.Join(doc.names, ", "), doc.word, doc.word, doc.word)
	}
	if checked == 0 {
		t.Fatal("no doc comment line opened with a declaration name, so nothing was checked and this guard is vacuous")
	}
}

// declaredNames returns every name a declaration binds, and its doc comment.
func declaredNames(declaration ast.Decl) ([]string, *ast.CommentGroup) {
	switch declaration := declaration.(type) {
	case *ast.FuncDecl:
		return []string{declaration.Name.Name}, declaration.Doc
	case *ast.GenDecl:
		var names []string
		for _, spec := range declaration.Specs {
			switch spec := spec.(type) {
			case *ast.TypeSpec:
				names = append(names, spec.Name.Name)
			case *ast.ValueSpec:
				for _, name := range spec.Names {
					names = append(names, name.Name)
				}
			}
		}
		return names, declaration.Doc
	default:
		return nil, nil
	}
}

// The three Example tables below are named for what they are. They document
// intent and they are NOT the guard: a table is a sample, and a sample is
// defeated by picking a literal it does not contain. Naming one
// "AreExactlyTheStructuralOnes" over twelve equality checks was a claim the
// body did not support, and the mutant that proved it differed from a killed
// one only in spelling "codegen" instead of "generated".
//
// The claim of exactness is carried by the three fuzz ORACLES further down,
// which compare each predicate against an independent statement of the rule
// over arbitrary input. Those are what make "exactly" true.
func TestModfilesIgnoredDirectoryNameExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ignored bool
	}{
		{name: "vendor", ignored: true},
		{name: "testdata", ignored: true},
		{name: ".git", ignored: true},
		{name: ".worktrees", ignored: true},
		{name: "_scratch", ignored: true},
		// Widening the set past the structural names is the mutation this
		// exists to catch: an ignored directory is scanned by neither walk.
		{name: "generated", ignored: false},
		{name: "internal", ignored: false},
		{name: "cmd", ignored: false},
		{name: "department", ignored: false},
		{name: "vendored", ignored: false},
		{name: "testdata2", ignored: false},
		{name: "realtime", ignored: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := modfiles.IsIgnoredDirectoryName(tt.name); got != tt.ignored {
				t.Errorf("modfiles.IsIgnoredDirectoryName(%q) = %v, want %v. A directory this walk skips is scanned by NEITHER the import guard nor the nested-module guard, so widening this set opens a hole in both at once", tt.name, got, tt.ignored)
			}
		})
	}
}

func TestModfilesBoundaryMarkerExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		marker   string
		boundary bool
	}{
		{name: "go.mod", marker: "go.mod", boundary: true},
		{name: "git checkout", marker: ".git", boundary: true},
		// go.work is a WORKSPACE file, not a module boundary, and adding it
		// here is the plausible edit that produced a surviving mutant: the
		// enumerator would skip the directory and the nested-module guard would
		// not report it. If Host ever needs that, this row changes first and
		// the consequence is visible in the diff.
		{name: "go.work", marker: "go.work", boundary: false},
		{name: "go.sum alone", marker: "go.sum", boundary: false},
		{name: "vendor manifest", marker: "vendor.json", boundary: false},
		{name: "ordinary source", marker: "x.go", boundary: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeFixture(t, dir, tt.marker, "")
			got, err := modfiles.IsBoundaryDirectory(dir)
			if err != nil {
				t.Fatalf("modfiles.IsBoundaryDirectory: %v", err)
			}
			if got != tt.boundary {
				t.Errorf("a directory holding %q: modfiles.IsBoundaryDirectory = %v, want %v", tt.marker, got, tt.boundary)
			}
		})
	}
}

func TestNestedModuleDetection(t *testing.T) {
	t.Parallel()

	t.Run("finds a nested module and a nested repository", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "host.go", "package host\n")
		writeFixture(t, root, "internal/httpapi/go.mod", "module github.com/looprig/host/internal/httpapi\n")
		writeFixture(t, root, "internal/httpapi/api.go", "package httpapi\n\nimport _ \"github.com/looprig/factory\"\n")
		writeFixture(t, root, "tools/checkout/.git/HEAD", "ref: refs/heads/main\n")
		writeFixture(t, root, "internal/plain/plain.go", "package plain\n")

		// The premise: the import guard really is blind to both of them.
		scan, err := scanImports(root, "")
		if err != nil {
			t.Fatalf("scanImports: %v", err)
		}
		if len(scan.violations) != 0 {
			t.Fatalf("violations = %q; this fixture exists because the import scan does NOT see inside a nested module", scan.violations)
		}

		nested, directories, err := nestedModuleDirectories(root)
		if err != nil {
			t.Fatalf("nestedModuleDirectories: %v", err)
		}
		if directories == 0 {
			t.Fatal("visited no directories")
		}
		want := []string{"internal/httpapi", "tools/checkout"}
		if !slices.Equal(nested, want) {
			t.Errorf("nested = %q, want %q", nested, want)
		}
	})

	t.Run("reports none for an ordinary tree", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
		writeFixture(t, root, "internal/plain/plain.go", "package plain\n")
		// Structural and excluded directories are not nested modules, and a
		// go.mod under testdata is fixture data.
		writeFixture(t, root, "testdata/fixture/go.mod", "module example.com/fixture\n")
		writeFixture(t, root, ".worktrees/branch/go.mod", "module example.com/worktree\n")

		nested, directories, err := nestedModuleDirectories(root)
		if err != nil {
			t.Fatalf("nestedModuleDirectories: %v", err)
		}
		if directories == 0 {
			t.Fatal("visited no directories, so the empty result below means nothing")
		}
		if len(nested) != 0 {
			t.Errorf("nested = %q, want none", nested)
		}
	})
}

// ---------------------------------------------------------------------------
// Fuzz
// ---------------------------------------------------------------------------

// The three oracles below are the reason the sample tables are only samples.
//
// Each states the rule INDEPENDENTLY of the implementation and asserts equality
// over arbitrary input, so a widening is caught whatever literal it is spelled
// with. Two mutants motivated them and neither was exotic: `|| name ==
// "codegen"` in the directory predicate, and `|| strings.HasSuffix(name,
// "_generated.go")` in the file one. Both survived the entire suite, and the
// second survived `make check` as well — a file the enumerator skips is also a
// file `make fmt-check` never pipes into gofmt, so it was unformatted,
// unchecked and unguarded at once.
//
// A widening of any of these three is a hole in every walk over this tree at
// the same time. That is why the oracle, and not the table, is the guard.

func FuzzIgnoredDirectoryName(f *testing.F) {
	for _, seed := range []string{"", ".", "..", "vendor", "vendored", "testdata", "testdata2", "generated", "codegen", "gen", "dist", "build", "proto", "node_modules", "internal", "cmd", ".git", "_scratch", "Vendor", "©"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		// The rule, stated without reference to the implementation.
		want := name == "vendor" || name == "testdata" ||
			strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
		if got := modfiles.IsIgnoredDirectoryName(name); got != want {
			t.Fatalf("modfiles.IsIgnoredDirectoryName(%q) = %v, want %v. A directory skipped here is scanned by NEITHER the import guard nor the nested-module guard", name, got, want)
		}
	})
}

func FuzzIgnoredFileName(f *testing.F) {
	for _, seed := range []string{"", ".", "host.go", "zz_generated.go", "x_generated.go", "_x.go", ".hidden.go", "generated.go", "host_test.go", "go.mod", "©.go"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		want := strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
		if got := modfiles.IsIgnoredFileName(name); got != want {
			t.Fatalf("modfiles.IsIgnoredFileName(%q) = %v, want %v. A file skipped here is invisible to the import guard AND to make fmt-check, which pipes this enumerator into gofmt", name, got, want)
		}
	})
}

// boundaryMarkers is the oracle's independent statement of the rule. It is a
// list rather than an expression because FuzzBoundaryMarkerName's skip has to
// iterate the same set its assertion uses; two spellings of it would be the
// copy this file has already paid for twice.
var boundaryMarkers = []string{"go.mod", ".git"}

// FuzzBoundaryMarkerName drives the predicate through the filesystem, because
// that is how it decides.
//
// The skip below is scoped to LOOKUP, and the first version scoped it to
// STORAGE, which is the wrong half of the same sentence. It compared the name
// the volume stored against the name written — right for a normalizing volume
// such as HFS+, which stores "café" decomposed — and blind to the common case:
// case-insensitive APFS, the macOS default, is case-PRESERVING, so ".Git" is
// stored verbatim, the storage check passes, and os.Lstat(dir + "/.git") then
// finds it anyway. The oracle states a rule over NAMES while the predicate
// answers a question about the FILESYSTEM, and the bridge between them has to
// check the half the predicate uses.
//
// The predicate is not wrong there — on a volume where git itself would open
// ".Git", calling that directory a boundary is defensible — so the input is
// discarded rather than asserted either way. What is discarded is exactly the
// set of names the volume conflates with a marker, which is why the skip is
// written as the lookup the predicate performs rather than as a list of
// spellings.
//
// This also cost a false green: the failure is derived rather than seeded, so a
// 30s run finds ".Git" sometimes and not others, and one clean run was reported
// as evidence. A target whose failure is probabilistic has not been shown to be
// able to fail until it has failed. See TestDocCommentsNameTheirOwnDeclaration
// for why this comment is separated from the var above by a blank line.
func FuzzBoundaryMarkerName(f *testing.F) {
	for _, seed := range []string{"go.mod", ".git", "go.work", "go.sum", "BUILD.bazel", "vendor.json", "x.go", "go.mod.bak", "Makefile", "GO.MOD", ".Git", ".GIT", "Go.mod", "go.MOD"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if name == "" || name == "." || name == ".." || len(name) > 64 ||
			strings.ContainsAny(name, "/\x00") {
			t.Skip("not a single legal filename")
		}
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Skip("filesystem refused the name")
		}
		for _, marker := range boundaryMarkers {
			if name == marker {
				continue
			}
			if _, err := os.Lstat(filepath.Join(dir, marker)); err == nil {
				t.Skip("this volume resolves " + strconv.Quote(name) + " to " + strconv.Quote(marker) +
					"; that is the volume deciding, not the predicate")
			}
		}

		want := slices.Contains(boundaryMarkers, name)
		got, err := modfiles.IsBoundaryDirectory(dir)
		if err != nil {
			t.Fatalf("modfiles.IsBoundaryDirectory: %v", err)
		}
		if got != want {
			t.Fatalf("a directory holding %q: modfiles.IsBoundaryDirectory = %v, want %v. Widening this hides the directory from the enumerator, and only the nested-module guard would see it — and only if some directory actually carries the marker", name, got, want)
		}
	})
}

// FuzzImportClassification asserts the property a substring ban cannot hold:
// appending anything but a path separator to a forbidden module's path names a
// DIFFERENT module, and must be allowed.
func FuzzImportClassification(f *testing.F) {
	for _, seed := range []string{"", "/", "/placement", "x", "like", " ", "\n", "\n/x", "-fork", "/../wui", "//x", "©"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, suffix string) {
		const forbidden = "github.com/looprig/factory"
		path := forbidden + suffix

		got := importAllowed("host.go", path)
		wantForbidden := suffix == "" || strings.HasPrefix(suffix, "/")
		if got == wantForbidden {
			t.Fatalf("importAllowed(%q, %q) = %v; %q names %s the forbidden module",
				"host.go", path, got, path, map[bool]string{true: "", false: "a different module than"}[wantForbidden])
		}
		// The classifier must never panic on an arbitrary import path, and the
		// exempt directory must not launder a forbidden looprig import.
		if importAllowed(centrifugeExemptDir+"/server.go", forbidden+suffix) != got {
			t.Fatalf("the Centrifuge exemption changed the verdict for %q", path)
		}
		importAllowed(suffix, suffix)
	})
}

// ---------------------------------------------------------------------------
// Plumbing
// ---------------------------------------------------------------------------

type importScan struct {
	files           int
	productionFiles int
	imports         int
	scanned         []string
	violations      []string
}

// qualify turns a path relative to root into one relative to the OUTERMOST
// module root. It is the whole of the fix for a defect that had five rounds to
// hide in: qualification used to happen at REPORT time, so a scan rooted at a
// declared nested module classified every file by its nested-module-relative
// path and every path-scoped rule silently re-anchored. Declaring
// tools/checkout granted it its own Centrifuge exemption, and an exemption that
// could only be widened by editing one constant was widened by CREATING A
// DIRECTORY named internal/realtime/hostlink inside it.
//
// Anything path-scoped added to importVerdict inherits this by construction,
// which is the point: the previous four fixes each extended one mechanism into
// the nested scan and left the others behind, and extension is precisely the
// operation that does that.
func qualify(prefix, relative string) string {
	if prefix == "" {
		return relative
	}
	return prefix + "/" + relative
}

// scanImports enumerates the Go files owned by the module at root, parses their
// import declarations, and classifies each one by its path relative to the
// OUTERMOST module root — prefix, which is "" for that root itself. It parses;
// it never searches source text. A ban expressed as a substring search has
// twice been defeated in this program by whitespace and by an equivalent
// spelling.
//
// prefix is a required parameter rather than a second entry point, so a caller
// scanning a nested root cannot forget it.
func scanImports(root, prefix string) (importScan, error) {
	var scan importScan
	files, err := modfiles.Files(root)
	if err != nil {
		return importScan{}, err
	}
	for _, path := range files {
		relative, err := moduleRelative(root, path)
		if err != nil {
			return importScan{}, err
		}
		relative = qualify(prefix, relative)
		scan.files++
		if !strings.HasSuffix(relative, "_test.go") {
			scan.productionFiles++
		}
		scan.scanned = append(scan.scanned, relative)
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return importScan{}, err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return importScan{}, err
			}
			scan.imports++
			if allowed, reason := importVerdict(relative, importPath); !allowed {
				scan.violations = append(scan.violations, relative+" imports "+strconv.Quote(importPath)+": "+reason)
			}
		}
	}
	slices.Sort(scan.violations)
	return scan, nil
}

// nestedScan is the result of walking for nested modules and then scanning the
// declared ones.
type nestedScan struct {
	directories int
	// goModFiles counts the declared modules whose own go.mod was parsed, so a
	// caller can tell "no dependency violation" from "no go.mod was read".
	goModFiles int
	declared   []string
	undeclared []string
	violations []string
}

// scanNestedModules finds every nested module below root and, for each one the
// allowlist declares, applies the WHOLE guard to it: mechanism 1 with paths
// still anchored to the OUTERMOST root, mechanisms 2 and 3 over that module's
// own go.mod, and this walk again. Declaring a nested module extends the
// boundary into it; it does not suspend the boundary around it, and it does not
// buy one level or one mechanism.
//
// Read the last sentence as a rule about FUTURE mechanisms, because the history
// of this function is five rounds of one mechanism at a time. Every fix was
// "extend mechanism X to the nested scan", and extension is exactly the
// operation that leaves the other mechanisms behind: the recursion added to
// close round 4 inherited mechanism 1's PATH SCOPE (re-anchored, so a declared
// module got its own Centrifuge exemption) and did not inherit 2 or 3 at all. A
// sixth mechanism must be applied HERE, not enumerated as the sixth escape.
//
// The recursion is the correction of a real gap. Scanning a declared module
// with scanImports alone walks with modfiles.Files, which stops at a boundary,
// so a module nested INSIDE a declared one was neither scanned nor reported —
// declaring a module disabled, for that path, the very mechanism that reports
// this situation at the top level. The realistic arrival is the one the doc
// above cites: Host publishes a nested module per the flow/store pattern,
// declares it, and someone later vendors a checkout inside it.
func scanNestedModules(root string, allowlist []string) (nestedScan, error) {
	return scanNestedModulesUnder(root, "", allowlist)
}

// scanNestedModulesUnder is scanNestedModules with the path prefix that makes
// every reported path relative to the OUTERMOST module root, so an allowlist
// entry is spelled the same way at every depth.
func scanNestedModulesUnder(root, prefix string, allowlist []string) (nestedScan, error) {
	nested, directories, err := nestedModuleDirectories(root)
	if err != nil {
		return nestedScan{}, err
	}
	scan := nestedScan{directories: directories}
	for _, path := range nested {
		qualified := qualify(prefix, path)
		if !slices.Contains(allowlist, qualified) {
			scan.undeclared = append(scan.undeclared, qualified)
			continue
		}
		scan.declared = append(scan.declared, qualified)

		directory := filepath.Join(root, filepath.FromSlash(path))

		// Mechanisms 2 and 3: the declared module's OWN go.mod. Neither ran
		// here for five rounds, and requireViolations' comment calls itself
		// "the only guard standing" for a forbidden module arriving
		// INDIRECTLY, because the import guard is silent by construction
		// there. For a declared nested module the only guard standing was not
		// standing: `require github.com/looprig/factory v0.1.0 // indirect`
		// and `replace github.com/looprig/core => ../../../core` both passed,
		// while the identical strings are caught in the root go.mod.
		modViolations, parsedMod, err := nestedGoModViolations(directory, qualified)
		if err != nil {
			return nestedScan{}, err
		}
		if parsedMod {
			scan.goModFiles++
		}
		scan.violations = append(scan.violations, modViolations...)

		// Mechanism 1, classified on paths relative to the OUTERMOST root.
		inner, err := scanImports(directory, qualified)
		if err != nil {
			return nestedScan{}, err
		}
		// Both floors, because both quantities are consumed. files == 0 says
		// the scan saw nothing; imports == 0 says it CLASSIFIED nothing, and
		// the violations below come from classified imports — flooring only the
		// first left a declared module of import-free files reporting a
		// confident zero. productionFiles is deliberately NOT floored: a
		// declared nested module may legitimately be test support, and
		// flooring it would force a false violation on a legitimate shape. That
		// asymmetry with the root scan is asserted rather than merely reasoned
		// about — see the "only test files" subtest of
		// TestDeclaredNestedModuleInheritsTheWholeGuard — because otherwise the
		// next reviewer applying "floor what the caller consumes" uniformly
		// adds the floor and breaks it, with nothing red to say so.
		if inner.files == 0 {
			scan.violations = append(scan.violations, qualified+" is declared in nestedModuleAllowlist but holds no Go file, so the extra scan its declaration buys is vacuous")
		} else if inner.imports == 0 {
			scan.violations = append(scan.violations, qualified+" is declared in nestedModuleAllowlist but no import was classified in it, so the extra scan its declaration buys is vacuous")
		}
		// No re-prefixing: inner.violations already name outermost-relative
		// paths, because qualification happens at classification time now.
		scan.violations = append(scan.violations, inner.violations...)

		deeper, err := scanNestedModulesUnder(directory, qualified, allowlist)
		if err != nil {
			return nestedScan{}, err
		}
		scan.directories += deeper.directories
		scan.goModFiles += deeper.goModFiles
		scan.declared = append(scan.declared, deeper.declared...)
		scan.undeclared = append(scan.undeclared, deeper.undeclared...)
		scan.violations = append(scan.violations, deeper.violations...)
	}
	return scan, nil
}

// nestedGoModViolations runs the go.mod rules over a declared nested module's
// own go.mod and reports whether there was one to read. Its job is FINDING and
// PARSING that file and qualifying what comes back; which rules apply is
// goModViolations' job, and listing them here again would be the second site
// this whole round exists to remove.
//
// A declared BOUNDARY is not always a module: a vendored git checkout carries
// .git and no go.mod, and having nothing to read is not a violation. A go.mod
// that exists and does not parse is, because it is this module's tree.
func nestedGoModViolations(directory, qualified string) ([]string, bool, error) {
	name := qualified + "/go.mod"
	source, err := os.ReadFile(filepath.Join(directory, "go.mod")) //nolint:gosec // a walked module-owned path
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	parsed, err := modfile.Parse(name, source, nil)
	if err != nil {
		return nil, false, err
	}
	var violations []string
	for _, violation := range goModViolations(parsed) {
		violations = append(violations, qualified+"/"+violation)
	}
	return violations, true, nil
}

// nestedModuleDirectories returns the module-root-relative paths of every
// nested module or repository below root, and the number of directories
// visited so the caller can tell "none" from "walked nothing".
//
// It CALLS modfiles.IsIgnoredDirectoryName and modfiles.IsBoundaryDirectory
// rather than restating them. An earlier version retyped both literals and
// claimed in a comment that the two "cannot disagree about what a boundary is",
// which was exactly backwards: the guard was enforceable and its agreement with
// the enumerator was prose. Two mutants survived the entire suite on that gap —
// adding "go.work" to the enumerator's boundary markers, and "generated" to its
// ignored names, both plausible one-line edits to a function whose whole job is
// skipping structural directories — each silently reopening the hole this test
// exists to close, with make check green.
//
// Sharing the predicates makes the two walks agree by construction. It does not
// stop the shared answer from being WIDENED, which is a hole in both walks at
// once, so the sets themselves are pinned by oracle.
//
// The two predicates are NOT symmetric, and an earlier round of this file
// claimed the opposite in both halves. Measured:
//
//   - BOUNDARY MARKERS: sharing kills exactly the EXPLOITABLE widenings. A
//     widened marker only hides something if some directory actually holds the
//     new marker — and if one does, this walk inherits the widening and reports
//     that directory. A marker no directory carries (go.work, today) hides
//     nothing and is caught by the oracle instead. Sharing and pinning cover
//     the live case and the dormant case respectively.
//   - IGNORED NAMES: sharing kills NOTHING. Both walks skip the widened name in
//     perfect agreement, and both pass. Only the oracle catches it.
func nestedModuleDirectories(root string) ([]string, int, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, 0, err
	}
	var nested []string
	directories := 0
	err = filepath.WalkDir(absoluteRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || path == absoluteRoot {
			return nil
		}
		if modfiles.IsIgnoredDirectoryName(entry.Name()) {
			return filepath.SkipDir
		}
		directories++
		boundary, err := modfiles.IsBoundaryDirectory(path)
		if err != nil {
			return err
		}
		if !boundary {
			return nil
		}
		relative, err := filepath.Rel(absoluteRoot, path)
		if err != nil {
			return err
		}
		nested = append(nested, filepath.ToSlash(relative))
		return filepath.SkipDir
	})
	if err != nil {
		return nil, 0, err
	}
	slices.Sort(nested)
	return nested, directories, nil
}

// moduleRelative returns path as a slash-separated path relative to root. It
// resolves root first: modfiles.Files returns absolute paths and the live guard
// passes ".", so comparing the two unresolved yields an error, not a decision.
func moduleRelative(root, path string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(absoluteRoot, path)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(relative), nil
}

func parseGoMod(t *testing.T, path string) *modfile.File {
	t.Helper()
	source, err := os.ReadFile(path) //nolint:gosec // fixed repository-relative path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	parsed, err := modfile.Parse(path, source, nil)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return parsed
}

func writeFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture %q: %v", relative, err)
	}
}
