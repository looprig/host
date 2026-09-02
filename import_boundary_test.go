package host_test

import (
	"errors"
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
	if importPath == "C" {
		// cgo is not a dependency Host has any business acquiring, and it is
		// invisible to module-graph checks.
		return false
	}
	segments := strings.Split(importPath, "/")
	if module, ok := looprigModule(segments); ok {
		_, forbidden := forbiddenLooprigModules[module]
		return !forbidden
	}
	if isCentrifugal(segments) {
		return underCentrifugeExemption(fileRelative)
	}
	return true
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
// exempt directory.
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
		if replacement.New.Version != "" {
			continue
		}
		violations = append(violations, "go.mod replaces "+replacement.Old.Path+
			" with the local filesystem path "+strconv.Quote(replacement.New.Path)+
			"; a published module must not carry one")
	}
	return violations
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
		module, ok := looprigModule(strings.Split(requirement.Mod.Path, "/"))
		if !ok {
			continue
		}
		if reason, forbidden := forbiddenLooprigModules[module]; forbidden {
			violations = append(violations, "go.mod requires "+requirement.Mod.Path+
				" at "+requirement.Mod.Version+"; "+reason)
			continue
		}
		published, ok := publishedLooprigVersions[module]
		if !ok {
			violations = append(violations, "go.mod requires "+requirement.Mod.Path+
				" at "+requirement.Mod.Version+"; Host has no released version of that module to name")
			continue
		}
		if requirement.Mod.Version != published {
			violations = append(violations, "go.mod requires "+requirement.Mod.Path+
				" at "+requirement.Mod.Version+", which is not the published version "+published)
		}
	}
	return violations
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
func TestModuleImportsStayWithinBoundary(t *testing.T) {
	t.Parallel()

	scan, err := scanImports(".")
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

	scan, err := scanImports(root)
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

// TestImportScanCanReportZero exists because a scan that cannot report zero
// cannot be trusted when it reports zero.
func TestImportScanCanReportZero(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module github.com/looprig/host\n")
	writeFixture(t, root, "testdata/ignored.go", "package ignored\n")

	scan, err := scanImports(root)
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

	_, err := scanImports(root)
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
		t.Fatal("go.mod requires nothing, so both go.mod assertions below would be vacuous")
	}
	for _, violation := range replaceViolations(parsed) {
		t.Error(violation)
	}
	for _, violation := range requireViolations(parsed) {
		t.Error(violation)
	}
}

func TestReplaceViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   int
	}{
		{name: "no replace", source: "module m\n\ngo 1.26.6\n", want: 0},
		{name: "relative directory", source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => ../core\n", want: 1},
		{name: "same directory", source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => ./core\n", want: 1},
		{name: "absolute directory", source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => /tmp/core\n", want: 1},
		{name: "block form", source: "module m\n\ngo 1.26.6\n\nreplace (\n\tgithub.com/looprig/core => ../core\n\tgithub.com/looprig/storage => ../storage\n)\n", want: 2},
		{name: "versioned replacement is not a filesystem replace", source: "module m\n\ngo 1.26.6\n\nreplace github.com/looprig/core => github.com/fork/core v0.7.0\n", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := modfile.Parse("go.mod", []byte(tt.source), nil)
			if err != nil {
				t.Fatalf("parse fixture go.mod: %v", err)
			}
			if got := replaceViolations(parsed); len(got) != tt.want {
				t.Errorf("replaceViolations() = %q (%d), want %d", got, len(got), tt.want)
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
// and the guard reports a clean walk. This was driven in the sibling Factory
// scaffold and it survives there.
//
// Exploitability is low: a nested module is not in `go build ./...`, and the
// parent cannot reach it without a require plus a replace, which
// replaceViolations already bans. What this assertion buys is that the day
// someone legitimately publishes a nested module from inside this repository —
// which is the established workspace pattern, see flow/store and
// pluto/cmd/pluto — they are stopped and made to decide whether the boundary
// should reach into it, instead of silently acquiring an unguarded subtree.
func TestModuleContainsNoUndeclaredNestedModule(t *testing.T) {
	t.Parallel()

	nested, directories, err := nestedModuleDirectories(".")
	if err != nil {
		t.Fatalf("walk for nested modules: %v", err)
	}
	if directories == 0 {
		t.Fatal("no directory was visited; this assertion would be vacuous")
	}
	for _, path := range nested {
		if slices.Contains(nestedModuleAllowlist, path) {
			continue
		}
		t.Errorf("%s is a nested module or repository, so nothing inside it is covered by the import guard. Either remove it, or add it to nestedModuleAllowlist and extend the guard to reach into it deliberately", path)
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
		scan, err := scanImports(root)
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

// scanImports enumerates the Go files this module owns, parses their import
// declarations, and classifies each one. It parses; it never searches source
// text. A ban expressed as a substring search has twice been defeated in this
// program by whitespace and by an equivalent spelling.
func scanImports(root string) (importScan, error) {
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
			if !importAllowed(relative, importPath) {
				scan.violations = append(scan.violations, relative+" imports forbidden package "+strconv.Quote(importPath))
			}
		}
	}
	slices.Sort(scan.violations)
	return scan, nil
}

// moduleRelative returns path as a slash-separated path relative to root. It
// resolves root first: modfiles.Files returns absolute paths and the live guard
// passes ".", so comparing the two unresolved yields an error, not a decision.
// nestedModuleDirectories returns the module-root-relative paths of every
// nested module or repository below root, and the number of directories
// visited so the caller can tell "none" from "walked nothing".
//
// It applies the same exclusions modfiles does — vendor, testdata, and dot- or
// underscore-prefixed names — because a directory the Go tool never builds is
// not a nested module of this one, and because the module root's own .git and
// .worktrees are structural rather than content. A directory is a boundary if
// it holds go.mod or .git, which is modfiles' own rule, restated here against
// the same markers so the two cannot disagree about what a boundary is.
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
		name := entry.Name()
		if name == "vendor" || name == "testdata" || name[0] == '.' || name[0] == '_' {
			return filepath.SkipDir
		}
		directories++
		for _, marker := range []string{"go.mod", ".git"} {
			_, statErr := os.Lstat(filepath.Join(path, marker))
			switch {
			case statErr == nil:
				relative, relErr := filepath.Rel(absoluteRoot, path)
				if relErr != nil {
					return relErr
				}
				nested = append(nested, filepath.ToSlash(relative))
				return filepath.SkipDir
			case os.IsNotExist(statErr):
				continue
			default:
				return statErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	slices.Sort(nested)
	return nested, directories, nil
}

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
