package host_test

import (
	"errors"
	"go/parser"
	"go/token"
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
// module with no release Host is allowed to depend on, and a version other than
// the published one. Pseudo-versions fail as a consequence rather than as a
// pattern — no pseudo-version is in publishedLooprigVersions.
func requireViolations(parsed *modfile.File) []string {
	var violations []string
	for _, requirement := range parsed.Require {
		module, ok := looprigModule(strings.Split(requirement.Mod.Path, "/"))
		if !ok {
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

	tests := []struct {
		name   string
		source string
		want   int
	}{
		{name: "published pins", source: "module m\n\ngo 1.26.6\n\nrequire (\n\tgithub.com/looprig/core v0.7.0\n\tgithub.com/looprig/sessionstore v0.1.0\n\tgithub.com/looprig/storage v0.6.0\n)\n", want: 0},
		{name: "non-looprig dependency is unconstrained", source: "module m\n\ngo 1.26.6\n\nrequire golang.org/x/mod v0.40.0\n", want: 0},
		{name: "unreleased module", source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/harness v0.30.2\n", want: 1},
		{name: "pseudo-version", source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/sessionstore v0.0.0-20260901060329-a34464c893e6\n", want: 1},
		{name: "unpublished version of a released module", source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/core v0.8.0\n", want: 1},
		{name: "factory", source: "module m\n\ngo 1.26.6\n\nrequire github.com/looprig/factory v0.1.0\n", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := modfile.Parse("go.mod", []byte(tt.source), nil)
			if err != nil {
				t.Fatalf("parse fixture go.mod: %v", err)
			}
			if got := requireViolations(parsed); len(got) != tt.want {
				t.Errorf("requireViolations() = %q (%d), want %d", got, len(got), tt.want)
			}
		})
	}
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
