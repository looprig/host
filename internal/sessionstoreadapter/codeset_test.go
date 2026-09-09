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
	"sort"
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
// HOW IT FAILS CLOSED, AND WHY THAT TOOK TWO ATTEMPTS. The first version of
// this file derived the code set and then iterated it against a LITERAL
// expectation map. That does not deliver the property above: an upstream code
// nobody had considered was absent from the map, took the "not mapped" branch,
// was asserted to pass through unchanged — which it does — and reported clean.
// The derivation moved one axis and the expectation stayed a fixture, so a new
// code was still silently defaulted. A mutation adding a code to a copy of the
// module source passed all five tests.
//
// So the expectation is now CLOSED rather than defaulted. Each vocabulary names
// its mapped codes AND its passthrough codes, and unconsideredCodes requires the
// derived set and that union to be EQUAL. A code upstream adds is in neither
// list and fails; a code upstream removes is in a list and no longer derived,
// and fails too. THE TWO LISTS ARE STILL LITERALS — that is said plainly rather
// than implied away — but a literal that must account for every derived member
// is a different object from one that is consulted only when it happens to
// match.
//
// UNIT OF ANALYSIS: a package-level `const` whose ValueSpec carries an explicit
// type name and a string BasicLit value, in a non-test .go file in the module
// ROOT directory. WHAT IT CANNOT SEE — FIVE THINGS, and the count is stated
// because an earlier version of this comment said four and was missing the one a
// gate found: constants in subdirectories or in internal/ (none of these five
// vocabularies live there, checked); a constant whose type is inherited from an
// earlier spec in the same block rather than restated; A CONSTANT WHOSE VALUE IS
// NOT A PLAIN STRING LITERAL — a concatenation or a conversion is dropped
// SILENTLY, because the walk requires an *ast.BasicLit; a code the module
// produces without declaring a constant for it; and — most importantly —
// whether Host's classification of any code is SEMANTICALLY right. It
// establishes that every declared code has a decision, never that the decision
// is correct.
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
// The KNOWN MEMBER is what does the work: it is the positive control over the
// parse, and a derivation that silently matched nothing, or matched a different
// type, fails on it rather than reporting a clean sweep.
//
// THE ZERO FLOOR BELOW IT IS STRICTLY REDUNDANT, and that is recorded because an
// earlier version of this file claimed otherwise. Removing the floor AND
// emptying the derivation still fails, on the known-member check, with "derived
// 0 JournalErrorCode constants but not the known member". A mutation that
// deleted only the floor therefore survives — not because the floor is dormant
// and needed, which is what was first reported, but because nothing depends on
// it. It is kept for its clearer message on the commonest failure, not for
// coverage it does not add.
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

// unconsideredCodes compares the DERIVED set against the codes Host has
// actually considered, in both directions.
//
// unknown is every derived code named by neither list: upstream declares it and
// Host has never made a decision about it. stale is every listed code that is no
// longer derived: Host holds an opinion about something upstream has dropped.
// Returning both is what makes this a closed expectation rather than a default,
// and it is a pure function of its inputs so that the controls below can hand it
// a set with a known answer.
func unconsideredCodes(derived []string, mapped map[string]error, passthrough []string) (unknown, stale []string) {
	considered := make(map[string]bool, len(mapped)+len(passthrough))
	for code := range mapped {
		considered[code] = true
	}
	for _, code := range passthrough {
		considered[code] = true
	}
	seen := make(map[string]bool, len(derived))
	for _, code := range derived {
		seen[code] = true
		if !considered[code] {
			unknown = append(unknown, code)
		}
	}
	for code := range considered {
		if !seen[code] {
			stale = append(stale, code)
		}
	}
	sort.Strings(unknown)
	sort.Strings(stale)
	return unknown, stale
}

// requireConsidered fails with the decision the maintainer has to make, rather
// than with a diff.
func requireConsidered(t *testing.T, typeName string, derived []string, mapped map[string]error, passthrough []string) {
	t.Helper()
	unknown, stale := unconsideredCodes(derived, mapped, passthrough)
	if len(unknown) != 0 {
		t.Fatalf("%s: the pinned module declares %v, which Host has never classified. Decide whether each is an ownership statement and add it to the mapped or the passthrough list in this file.", typeName, unknown)
	}
	if len(stale) != 0 {
		t.Fatalf("%s: this file still names %v, which the pinned module no longer declares. Remove them.", typeName, stale)
	}
}

// THE LITERAL LISTS. These are literals, said plainly. Their job is not to be
// the set — declaredCodes derives that — but to force a human decision for every
// member of it. Splitting them by vocabulary keeps each one next to the switch
// it describes in store.go.
var (
	journalMapped = map[string]error{
		string(sessionstore.JournalErrorLeaseHeld): residency.ErrLeaseHeld,
		string(sessionstore.JournalErrorFenced):    residency.ErrFenceConflict,
		string(sessionstore.JournalErrorLeaseLost): residency.ErrEpochSuperseded,
	}
	journalPassthrough = []string{
		string(sessionstore.JournalErrorInvalid),
		string(sessionstore.JournalErrorUnknown),
		string(sessionstore.JournalErrorClosed),
		string(sessionstore.JournalErrorBackend),
		string(sessionstore.JournalErrorIntegrity),
		string(sessionstore.JournalErrorTooLarge),
		string(sessionstore.JournalErrorCursor),
	}

	inboxMapped      = map[string]error{string(sessionstore.InboxErrorEpoch): residency.ErrEpochSuperseded}
	inboxPassthrough = []string{
		string(sessionstore.InboxErrorInvalid),
		string(sessionstore.InboxErrorCursor),
		string(sessionstore.InboxErrorCommandMismatch),
		string(sessionstore.InboxErrorNotFound),
		string(sessionstore.InboxErrorDeleted),
		string(sessionstore.InboxErrorIdentity),
		string(sessionstore.InboxErrorConflict),
		string(sessionstore.InboxErrorClaimHeld),
		string(sessionstore.InboxErrorClaimLost),
		string(sessionstore.InboxErrorDeadline),
		string(sessionstore.InboxErrorState),
		string(sessionstore.InboxErrorEvidence),
		string(sessionstore.InboxErrorTerminal),
		string(sessionstore.InboxErrorUnknown),
		string(sessionstore.InboxErrorBackend),
		string(sessionstore.InboxErrorMalformed),
		string(sessionstore.InboxErrorVersion),
		string(sessionstore.InboxErrorTooLarge),
	}

	registryMapped      = map[string]error{string(sessionstore.RegistryErrorEpoch): residency.ErrEpochSuperseded}
	registryPassthrough = []string{
		string(sessionstore.RegistryErrorInvalid),
		string(sessionstore.RegistryErrorNotFound),
		string(sessionstore.RegistryErrorExpired),
		string(sessionstore.RegistryErrorReleased),
		string(sessionstore.RegistryErrorDeleted),
		string(sessionstore.RegistryErrorIdentity),
		string(sessionstore.RegistryErrorConflict),
		string(sessionstore.RegistryErrorUnknown),
		string(sessionstore.RegistryErrorBackend),
		string(sessionstore.RegistryErrorMalformed),
		string(sessionstore.RegistryErrorVersion),
		string(sessionstore.RegistryErrorTooLarge),
	}
)

var residencySentinels = []error{residency.ErrLeaseHeld, residency.ErrFenceConflict, residency.ErrEpochSuperseded}

// TestEveryDeclaredJournalCodeIsClassifiedDeliberately asserts the mapping over
// the codes the PINNED module declares, not over a list restated here.
func TestEveryDeclaredJournalCodeIsClassifiedDeliberately(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "JournalErrorCode")
	requireDerivedSet(t, "JournalErrorCode", codes, string(sessionstore.JournalErrorFenced))

	requireConsidered(t, "JournalErrorCode", codes, journalMapped, journalPassthrough)

	findings := undecidedCodes(
		classifyJournal,
		codes,
		func(code string) error {
			return &sessionstore.JournalError{Code: sessionstore.JournalErrorCode(code), Field: "lease"}
		},
		journalMapped,
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

	requireConsidered(t, "InboxErrorCode", codes, inboxMapped, inboxPassthrough)

	findings := undecidedCodes(
		classifyInbox,
		codes,
		func(code string) error {
			return &sessionstore.InboxError{Code: sessionstore.InboxErrorCode(code), Field: "lease_epoch"}
		},
		inboxMapped,
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

	requireConsidered(t, "RegistryErrorCode", codes, registryMapped, registryPassthrough)

	findings := undecidedCodes(
		classifyRegistry,
		codes,
		func(code string) error {
			return &sessionstore.RegistryError{Code: sessionstore.RegistryErrorCode(code), Field: "lease_epoch"}
		},
		registryMapped,
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
	expectation := journalMapped

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

// M-A, THE MUTATION THAT DEFEATED THE FIRST VERSION OF THIS FILE, KEPT AS A
// TEST. It copies the pinned module's root .go files to a temporary directory,
// appends a JournalErrorCode nobody has considered, and runs the REAL derivation
// over it. The old expectation reported that set clean because an unnamed code
// takes the passthrough branch; the closed one must report it.
//
// It exercises declaredCodes itself rather than hand-building a slice, so a
// derivation that stopped seeing added constants would fail here too.
func TestAnUpstreamCodeNobodyConsideredIsReported(t *testing.T) {
	source := pinnedSessionstoreDir(t)
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatalf("reading %s: %v", source, err)
	}
	staged := t.TempDir()
	copied := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(staged, name), body, 0o600); err != nil {
			t.Fatalf("staging %s: %v", name, err)
		}
		copied++
	}
	if copied == 0 {
		t.Fatal("staged no files, so the mutation would be applied to nothing")
	}

	const invented = "quarantined_by_a_future_release"
	addition := "\n\nconst JournalErrorQuarantined JournalErrorCode = \"" + invented + "\"\n"
	if err := os.WriteFile(filepath.Join(staged, "zz_added_code.go"), []byte("package sessionstore"+addition), 0o600); err != nil {
		t.Fatalf("writing the added code: %v", err)
	}

	codes := declaredCodes(t, staged, "JournalErrorCode")

	// The derivation must SEE it, or the control is testing nothing.
	found := false
	for _, code := range codes {
		if code == invented {
			found = true
		}
	}
	if !found {
		t.Fatalf("the derivation did not pick up the added code; it saw %v", codes)
	}

	// The closed expectation must REPORT it. This is the assertion that fails
	// on the first version of this file.
	unknown, _ := unconsideredCodes(codes, journalMapped, journalPassthrough)
	if len(unknown) != 1 || unknown[0] != invented {
		t.Fatalf("unconsideredCodes reported %v, want exactly [%s]", unknown, invented)
	}

	// And for contrast: the behaviour check alone passes it, which is precisely
	// why the behaviour check is not sufficient on its own.
	findings := undecidedCodes(
		classifyJournal,
		codes,
		func(code string) error {
			return &sessionstore.JournalError{Code: sessionstore.JournalErrorCode(code)}
		},
		journalMapped,
		residencySentinels,
	)
	if len(findings) != 0 {
		t.Fatalf("undecidedCodes unexpectedly reported the added code: %v; the contrast this control draws no longer holds", findings)
	}
}

// The other direction of the closed expectation: a code this file names that
// upstream has dropped must be reported as stale, or the lists would silently
// accumulate opinions about codes that no longer exist.
func TestACodeThisFileNamesThatUpstreamDroppedIsReported(t *testing.T) {
	derived := []string{string(sessionstore.JournalErrorLeaseHeld)}
	unknown, stale := unconsideredCodes(derived, journalMapped, journalPassthrough)
	if len(unknown) != 0 {
		t.Fatalf("unknown = %v, want none", unknown)
	}
	if len(stale) != len(journalMapped)+len(journalPassthrough)-1 {
		t.Fatalf("stale = %v, want every listed code but the one derived", stale)
	}
}

// The known-answer control on the comparison itself, in the direction that
// matters most: an exactly-matching set reports nothing, so the two findings
// above are not an artefact of a function that always reports something.
func TestUnconsideredCodesIsSilentOnAnExactMatch(t *testing.T) {
	var derived []string
	for code := range journalMapped {
		derived = append(derived, code)
	}
	derived = append(derived, journalPassthrough...)
	if unknown, stale := unconsideredCodes(derived, journalMapped, journalPassthrough); len(unknown) != 0 || len(stale) != 0 {
		t.Fatalf("an exact match reported unknown=%v stale=%v", unknown, stale)
	}
}

// ---------------------------------------------------------------------------
// The other two vocabularies Host reads
// ---------------------------------------------------------------------------
//
// THE FIRST VERSION OF THIS FILE COVERED THREE OF FIVE. classifyJournal,
// classifyInbox and classifyRegistry were closed over their derived sets while
// isCatalogAbsent in durable.go matched Catalog and Keyspace codes by literal
// with no guard at all — the identical gap, left open in the same edit that
// closed the others. These two close it.
//
// THE SHAPE IS DIFFERENT AND SO IS THE CHECK. isCatalogAbsent is a PREDICATE,
// not a mapping onto a sentinel, so there is no classifier to run
// undecidedCodes over. What is asserted is the same property in the same two
// directions: every declared code is accounted for, and the predicate answers
// true for exactly the codes named as absence.

var (
	catalogAbsent = map[string]error{
		string(sessionstore.CatalogErrorNotFound): nil,
		string(sessionstore.CatalogErrorDeleted):  nil,
	}
	catalogNotAbsent = []string{
		string(sessionstore.CatalogErrorInvalid),
		string(sessionstore.CatalogErrorCursor),
		string(sessionstore.CatalogErrorIdentity),
		string(sessionstore.CatalogErrorEpoch),
		string(sessionstore.CatalogErrorSequence),
		string(sessionstore.CatalogErrorTooSoon),
		string(sessionstore.CatalogErrorConflict),
		string(sessionstore.CatalogErrorUnknown),
		string(sessionstore.CatalogErrorBackend),
		string(sessionstore.CatalogErrorMalformed),
		string(sessionstore.CatalogErrorVersion),
		string(sessionstore.CatalogErrorTooLarge),
	}

	keyspaceAbsent = map[string]error{
		string(sessionstore.KeyspaceBindingNotFound): nil,
	}
	keyspaceNotAbsent = []string{
		string(sessionstore.KeyspaceBackend),
		string(sessionstore.KeyspaceMarkerMalformed),
		string(sessionstore.KeyspaceLayoutMismatch),
		string(sessionstore.KeyspaceMarkerAmbiguous),
		string(sessionstore.KeyspaceBindingAmbiguous),
		string(sessionstore.KeyspaceScopeInvalid),
		string(sessionstore.KeyspaceHashCollision),
		string(sessionstore.KeyspaceLegacyTenant),
		string(sessionstore.KeyspaceLegacySession),
	}
)

// TestEveryDeclaredCatalogCodeIsConsidered closes isCatalogAbsent's catalog arm
// over the derived set, in both directions.
func TestEveryDeclaredCatalogCodeIsConsidered(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "CatalogErrorCode")
	requireDerivedSet(t, "CatalogErrorCode", codes, string(sessionstore.CatalogErrorNotFound))
	requireConsidered(t, "CatalogErrorCode", codes, catalogAbsent, catalogNotAbsent)

	for _, code := range codes {
		_, wantAbsent := catalogAbsent[code]
		got := isCatalogAbsent(&sessionstore.CatalogError{Code: sessionstore.CatalogErrorCode(code)})
		if got != wantAbsent {
			t.Fatalf("isCatalogAbsent(catalog %q) = %v, want %v", code, got, wantAbsent)
		}
	}
}

// TestEveryDeclaredKeyspaceCodeIsConsidered closes the keyspace arm. F13 is the
// reason this arm exists at all: a session that never existed fails at the
// KEYSPACE, before the catalog is consulted.
func TestEveryDeclaredKeyspaceCodeIsConsidered(t *testing.T) {
	codes := declaredCodes(t, pinnedSessionstoreDir(t), "KeyspaceErrorCode")
	requireDerivedSet(t, "KeyspaceErrorCode", codes, string(sessionstore.KeyspaceBindingNotFound))
	requireConsidered(t, "KeyspaceErrorCode", codes, keyspaceAbsent, keyspaceNotAbsent)

	for _, code := range codes {
		_, wantAbsent := keyspaceAbsent[code]
		got := isCatalogAbsent(&sessionstore.KeyspaceError{Code: sessionstore.KeyspaceErrorCode(code)})
		if got != wantAbsent {
			t.Fatalf("isCatalogAbsent(keyspace %q) = %v, want %v", code, got, wantAbsent)
		}
	}
}
