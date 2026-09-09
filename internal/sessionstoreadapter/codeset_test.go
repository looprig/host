package sessionstoreadapter

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// The derived error-code set
// ---------------------------------------------------------------------------
//
// WHY THIS FILE EXISTS. The tables in classify_test.go are literal slices, and
// that file says so: "a code added upstream is not defaulted, not reported and
// not classified — it is simply ABSENT AND UNTESTED until somebody extends the
// table", with an instruction to re-count on every sessionstore bump. That is a
// fixed fixture standing in for a "for all codes" claim, and the obligation it
// creates is discharged by a human remembering. The O3.3 rebind onto
// sessionstore v0.6.0 is the second time that re-count came due.
//
// So the sets are DERIVED HERE instead, from the pinned module's own source.
// The pinned version comes from the test binary's build info, the module
// directory from GOMODCACHE, and the constants from go/parser over the
// package's non-test files — structural parsing, not a text search, because
// this repository bans classifying source by substring. A code added upstream
// now appears in the derived set on the next bump and is asserted whether or
// not anybody extended a table.
//
// WHAT THIS DOES NOT DO. It does not check that Host's classification of a NEW
// code is correct — nothing can, because correctness is a judgement about what
// the code means. It checks that every code the pinned module declares gets a
// classification Host has DECIDED, and that a code Host has not considered is
// visible rather than silently defaulted. That is the property the literal
// tables were reaching for.
//
// The derivation fails rather than skips when it cannot reach the source: a
// guard whose subject is absent must not report success, and an empty derived
// set is exactly the vacuous pass this repository floors everywhere else.

// pinnedSessionstoreDir locates the source of the sessionstore version this test
// binary was built against.
//
// It reads the version from build info rather than from go.mod, so it describes
// the module that is actually linked in. A mismatch between the two is the state
// a stale vendor tree used to produce, and it is not this test's job to conceal.
func pinnedSessionstoreDir(t *testing.T) string {
	t.Helper()

	const modulePath = "github.com/looprig/sessionstore"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("no build info: the derived code set cannot establish which sessionstore version is linked in")
	}
	version := ""
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			version = dep.Version
			if dep.Replace != nil {
				t.Fatalf("%s is replaced (%s => %s); this module forbids replacements and the derivation would read the wrong source", modulePath, dep.Path, dep.Replace.Path)
			}
			break
		}
	}
	if version == "" {
		t.Fatalf("build info names no version of %s", modulePath)
	}

	cache := os.Getenv("GOMODCACHE")
	if cache == "" {
		out, err := exec.Command("go", "env", "GOMODCACHE").Output()
		if err != nil {
			t.Fatalf("locating GOMODCACHE: %v", err)
		}
		cache = strings.TrimSpace(string(out))
	}
	if cache == "" {
		t.Fatal("GOMODCACHE is empty, so the pinned module source cannot be located")
	}

	// The module path is entirely lower case, so the cache's case encoding is
	// the identity here. Asserting that keeps the shortcut honest if the path
	// ever changes.
	if strings.ToLower(modulePath) != modulePath {
		t.Fatalf("%s needs the module cache case encoding, which this helper does not apply", modulePath)
	}

	dir := filepath.Join(cache, filepath.FromSlash(modulePath)+"@"+version)
	if entry, err := os.Stat(dir); err != nil || !entry.IsDir() {
		t.Fatalf("pinned %s@%s is not readable at %s: %v", modulePath, version, dir, err)
	}
	return dir
}

// declaredCodes returns the VALUES of every package-level constant in dir whose
// declared type is typeName.
//
// It parses declarations rather than matching text. A constant is collected only
// when its ValueSpec carries the type explicitly, which is how sessionstore
// declares all three code vocabularies; a spec that inherited its type from a
// previous line would be missed, so the caller floors the result and the
// known-member control below proves the parse reached its subject.
//
// The files are enumerated here and parsed one at a time rather than through
// go/parser's ParseDir, which is deprecated and which staticcheck refuses. That
// is not merely a workaround: ParseDir groups files into packages by build tag,
// and this guard wants every constant the module DECLARES regardless of which
// platform builds it. A completeness check that lost a constant to a build tag
// would report a clean sweep it had not earned.
func declaredCodes(t *testing.T, dir, typeName string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}

	fileSet := token.NewFileSet()
	var codes []string
	parsed := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		parsed++
		{
			for _, decl := range file.Decls {
				general, ok := decl.(*ast.GenDecl)
				if !ok || general.Tok != token.CONST {
					continue
				}
				for _, spec := range general.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					ident, ok := value.Type.(*ast.Ident)
					if !ok || ident.Name != typeName {
						continue
					}
					for _, expression := range value.Values {
						literal, ok := expression.(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							continue
						}
						unquoted, err := strconv.Unquote(literal.Value)
						if err != nil {
							t.Fatalf("unquoting a %s constant: %v", typeName, err)
						}
						codes = append(codes, unquoted)
					}
				}
			}
		}
	}
	if parsed == 0 {
		t.Fatalf("parsed no production files in %s; the derivation had nothing to read", dir)
	}
	return codes
}

// requireDerivedSet floors the derivation and proves it reached its subject.
//
// The floor is the vacuous-pass guard: zero codes would make every assertion
// below pass without examining anything. The known member is the positive
// control over the PARSE — a derivation that silently matched nothing, or
// matched a different type, fails here rather than reporting a clean sweep.
func requireDerivedSet(t *testing.T, typeName string, codes []string, knownMember string) {
	t.Helper()

	if len(codes) == 0 {
		t.Fatalf("derived no %s constants at all; the derivation did not reach its subject", typeName)
	}
	for _, code := range codes {
		if code == knownMember {
			return
		}
	}
	t.Fatalf("derived %d %s constants but not the known member %q, so the parse matched the wrong declarations: %v", len(codes), typeName, knownMember, codes)
}

// undecidedCodes reports every code whose classification is not the one the
// expectation names.
//
// It is a pure function of a classifier so that the assertion can be run
// against a DELIBERATELY WRONG one; see the control tests below. A check whose
// only observed outcome is "nothing was found" is not evidence that it can find
// anything.
func undecidedCodes(classify func(error) error, codes []string, construct func(string) error, sentinelFor map[string]error, sentinels []error) []string {
	var findings []string
	for _, code := range codes {
		original := construct(code)
		classified := classify(original)
		want, mapped := sentinelFor[code]

		if mapped {
			if !errors.Is(classified, want) {
				findings = append(findings, fmt.Sprintf("%s: expected the ownership sentinel, got %v", code, classified))
			}
			for _, other := range sentinels {
				if other != want && errors.Is(classified, other) {
					findings = append(findings, fmt.Sprintf("%s: also unwraps to a second sentinel", code))
				}
			}
			continue
		}
		if classified != original {
			findings = append(findings, fmt.Sprintf("%s: the store's error was rewritten to %v", code, classified))
		}
		for _, sentinel := range sentinels {
			if errors.Is(classified, sentinel) {
				findings = append(findings, fmt.Sprintf("%s: surrendered ownership for a failure that says nothing about it", code))
			}
		}
	}
	return findings
}

var residencySentinels = []error{residency.ErrLeaseHeld, residency.ErrFenceConflict, residency.ErrEpochSuperseded}

// TestEveryDeclaredJournalCodeIsClassifiedDeliberately asserts the mapping over
// the codes the PINNED module declares, not over a list restated here.
func TestEveryDeclaredJournalCodeIsClassifiedDeliberately(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "JournalErrorCode")
	requireDerivedSet(t, "JournalErrorCode", codes, string(sessionstore.JournalErrorFenced))

	findings := undecidedCodes(
		classifyJournal,
		codes,
		func(code string) error {
			return &sessionstore.JournalError{Code: sessionstore.JournalErrorCode(code), Field: "lease"}
		},
		map[string]error{
			string(sessionstore.JournalErrorLeaseHeld): residency.ErrLeaseHeld,
			string(sessionstore.JournalErrorFenced):    residency.ErrFenceConflict,
			string(sessionstore.JournalErrorLeaseLost): residency.ErrEpochSuperseded,
		},
		residencySentinels,
	)
	if len(findings) != 0 {
		t.Fatalf("classifyJournal over the %d declared codes: %s", len(codes), strings.Join(findings, "; "))
	}
}

// TestEveryDeclaredInboxCodeIsClassifiedDeliberately is F12's for-all arm,
// derived rather than enumerated.
func TestEveryDeclaredInboxCodeIsClassifiedDeliberately(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "InboxErrorCode")
	requireDerivedSet(t, "InboxErrorCode", codes, string(sessionstore.InboxErrorEpoch))

	findings := undecidedCodes(
		classifyInbox,
		codes,
		func(code string) error {
			return &sessionstore.InboxError{Code: sessionstore.InboxErrorCode(code), Field: "lease_epoch"}
		},
		map[string]error{string(sessionstore.InboxErrorEpoch): residency.ErrEpochSuperseded},
		residencySentinels,
	)
	if len(findings) != 0 {
		t.Fatalf("classifyInbox over the %d declared codes: %s", len(codes), strings.Join(findings, "; "))
	}
}

// TestEveryDeclaredRegistryCodeIsClassifiedDeliberately covers the third
// vocabulary on the same derivation.
func TestEveryDeclaredRegistryCodeIsClassifiedDeliberately(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "RegistryErrorCode")
	requireDerivedSet(t, "RegistryErrorCode", codes, string(sessionstore.RegistryErrorEpoch))

	findings := undecidedCodes(
		classifyRegistry,
		codes,
		func(code string) error {
			return &sessionstore.RegistryError{Code: sessionstore.RegistryErrorCode(code), Field: "lease_epoch"}
		},
		map[string]error{string(sessionstore.RegistryErrorEpoch): residency.ErrEpochSuperseded},
		residencySentinels,
	)
	if len(findings) != 0 {
		t.Fatalf("classifyRegistry over the %d declared codes: %s", len(codes), strings.Join(findings, "; "))
	}
}

// THE POSITIVE CONTROL. Each assertion above passes by finding nothing, which
// is the shape that passes just as well when the check is inert. These run the
// same undecidedCodes over classifiers that are known to be wrong in each of
// the two directions a classifier can be wrong, and require it to say so.
func TestUndecidedCodesReportsAClassifierThatIsWrongInEitherDirection(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "JournalErrorCode")
	requireDerivedSet(t, "JournalErrorCode", codes, string(sessionstore.JournalErrorFenced))

	construct := func(code string) error {
		return &sessionstore.JournalError{Code: sessionstore.JournalErrorCode(code)}
	}
	expectation := map[string]error{
		string(sessionstore.JournalErrorLeaseHeld): residency.ErrLeaseHeld,
		string(sessionstore.JournalErrorFenced):    residency.ErrFenceConflict,
		string(sessionstore.JournalErrorLeaseLost): residency.ErrEpochSuperseded,
	}

	// TOO LOOSE: a classifier that surrenders ownership for every failure. This
	// is the direction that makes a Host stop writing to a session it holds.
	tooLoose := func(err error) error { return errors.Join(residency.ErrEpochSuperseded, err) }
	if findings := undecidedCodes(tooLoose, codes, construct, expectation, residencySentinels); len(findings) == 0 {
		t.Fatal("undecidedCodes accepted a classifier that reports every journal failure as a lost epoch")
	}

	// TOO TIGHT: a classifier that maps nothing, so a real takeover reads as an
	// ordinary fault and this Host goes on writing to a session it has lost.
	tooTight := func(err error) error { return err }
	if findings := undecidedCodes(tooTight, codes, construct, expectation, residencySentinels); len(findings) == 0 {
		t.Fatal("undecidedCodes accepted a classifier that maps no ownership code at all")
	}
}

// The derivation itself gets a known-answer control: a type name that is not
// declared must produce the empty set, or requireDerivedSet's floor is standing
// on a parser that matches everything it is asked for.
func TestDeclaredCodesFindsNothingForATypeThatDoesNotExist(t *testing.T) {
	if codes := declaredCodes(t, pinnedSessionstoreDir(t), "NoSuchErrorCode"); len(codes) != 0 {
		t.Fatalf("declaredCodes matched %d constants for a type sessionstore does not declare: %v", len(codes), codes)
	}
}
