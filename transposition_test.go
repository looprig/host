package host_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/looprig/host/internal/modfiles"
)

// ---------------------------------------------------------------------------
// Adjacent same-typed parameters
// ---------------------------------------------------------------------------
//
// WHY THIS FILE EXISTS. Three times in this repository an implementation has
// received two adjacent parameters OF THE SAME TYPE in the wrong order:
// fixture_test.go's fakeCursors.SaveCursor, the inbox double that inherited the
// transposition from it, and cmd/host/main.go's unconfiguredBootstrap.SaveCursor.
// All three compiled. None of them was caught by a test, because a test that
// never reads the value back cannot tell `epoch, order` from `order, epoch`, and
// the one that DID read it back had the same transposition on both sides.
//
// THE DEFECT CLASS IS EXACTLY: two adjacent parameters of the same type that
// MEAN DIFFERENT THINGS. The compiler is blind to it by construction, `go vet`
// has no rule for it, and the only written record of the intended order is the
// interface's parameter NAMES — which Go does not require an implementation to
// repeat, and which is precisely why an implementation is free to disagree.
//
// SO THE INTERFACE IS MADE THE AUTHORITY. Every interface method in this module
// whose parameter list contains an adjacent same-typed run is a shape; every
// function or method with THE SAME NAME AND THE IDENTICAL PARAMETER TYPE LIST is
// held to that shape's parameter names, within the run, in the interface's
// order. A blank is permitted anywhere, because `_` is a deliberate statement
// that the value is unused and cannot be silently misread.
//
// ITS LIMITS ARE A RESIDUE AND NOT A BOUNDARY. What this walk is known NOT to
// see, stated so that a spelling absent from the list is understood as
// unexamined rather than as excluded:
//
//   - a transposition at a CALL SITE rather than in a declaration: passing
//     (order, epoch) to a correctly-declared SaveCursor is the same defect and
//     this guard cannot see it;
//   - two adjacent parameters whose types are spelled differently but are
//     identical after resolution (an alias, or a named type and its underlying
//     one) — the comparison is on the printed type expression, not on types
//     resolved by the type checker;
//   - an implementation whose parameter list differs from the interface's in
//     any way at all, including a differently spelled but equivalent type,
//     because the type-list equality gate drops it rather than risk holding an
//     unrelated function to a shape it never promised;
//   - a same-typed pair that is NOT adjacent, separated by a parameter of
//     another type;
//   - an interface method declared in a DEPENDENCY rather than in this module:
//     the walk reads this module's own files only;
//   - whether the interface's own parameter ORDER is the right one. This
//     establishes that every implementation agrees with the declaration, never
//     that the declaration is correct.
//
// It fails rather than skips when it cannot reach its subject: a walk that found
// no shapes at all is the vacuous pass this repository floors everywhere else.

// paramShape is one interface method's order-sensitive parameter names.
type paramShape struct {
	method string
	// types is the full parameter type list, printed. It is the gate: only a
	// function whose types match exactly is held to this shape.
	types []string
	// names is the parameter name at each position, "" for a blank or unnamed.
	names []string
	// runs are the [start, end) index ranges of adjacent same-typed parameters.
	runs [][2]int
	// where is the declaration position, for the failure message.
	where string
}

// printType renders a type expression the way the source spells it.
func printType(fileSet *token.FileSet, expression ast.Expr) string {
	var builder strings.Builder
	if err := printer.Fprint(&builder, fileSet, expression); err != nil {
		return "<unprintable>"
	}
	return builder.String()
}

// flattenParams expands a parameter list into positional (name, type) pairs.
//
// The expansion is what makes the grouped spelling `epoch, order uint64` and the
// repeated spelling `epoch uint64, order uint64` the same object here. They are
// the same declaration to the compiler and they are the same defect surface, so
// a guard that saw only one of them would have missed two of the three known
// instances.
func flattenParams(fileSet *token.FileSet, list *ast.FieldList) (names, types []string) {
	if list == nil {
		return nil, nil
	}
	for _, field := range list.List {
		rendered := printType(fileSet, field.Type)
		if len(field.Names) == 0 {
			names = append(names, "")
			types = append(types, rendered)
			continue
		}
		for _, name := range field.Names {
			names = append(names, name.Name)
			types = append(types, rendered)
		}
	}
	return names, types
}

// sameTypedRuns reports the index ranges of two or more ADJACENT parameters of
// one type.
func sameTypedRuns(types []string) [][2]int {
	var runs [][2]int
	for start := 0; start < len(types); {
		end := start + 1
		for end < len(types) && types[end] == types[start] {
			end++
		}
		if end-start >= 2 {
			runs = append(runs, [2]int{start, end})
		}
		start = end
	}
	return runs
}

// orderSensitive reports whether a run names at least two DISTINCT non-blank
// parameters, which is the only case in which an order can be got wrong.
//
// A run of blanks, or a run whose names repeat, carries no order to transpose.
func orderSensitive(names []string, run [2]int) bool {
	seen := map[string]bool{}
	for index := run[0]; index < run[1]; index++ {
		name := names[index]
		if name == "" || name == "_" {
			continue
		}
		seen[name] = true
	}
	return len(seen) >= 2
}

// transposedRuns compares one implementation's names against a shape's, and
// returns a description of every run that disagrees.
//
// IT IS A PURE FUNCTION SO THAT IT CAN BE DRIVEN WITH A KNOWN-WRONG INPUT. A
// check whose only observed outcome is "nothing was found" is not evidence that
// it can find anything; the controls below hand it a transposition and require
// it to say so.
func transposedRuns(shape paramShape, names []string) []string {
	var findings []string
	for _, run := range shape.runs {
		for index := run[0]; index < run[1]; index++ {
			got, want := names[index], shape.names[index]
			if got == "" || got == "_" {
				// A blank is a deliberate "this value is unused". It cannot be
				// read in the wrong slot because it is not read at all.
				continue
			}
			if got == want {
				continue
			}
			findings = append(findings, fmt.Sprintf(
				"parameter %d is named %q where %s declares %q (the run is %s)",
				index, got, shape.where, want, strings.Join(shape.names[run[0]:run[1]], ", ")))
		}
	}
	return findings
}

// collectShapes reads every interface method in the module that has an
// order-sensitive run.
func collectShapes(t *testing.T, files []string) map[string][]paramShape {
	t.Helper()

	fileSet := token.NewFileSet()
	shapes := map[string][]paramShape{}
	for _, path := range files {
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			declared, ok := node.(*ast.InterfaceType)
			if !ok || declared.Methods == nil {
				return true
			}
			for _, method := range declared.Methods.List {
				signature, ok := method.Type.(*ast.FuncType)
				if !ok || len(method.Names) == 0 {
					continue
				}
				names, types := flattenParams(fileSet, signature.Params)
				var runs [][2]int
				for _, run := range sameTypedRuns(types) {
					if orderSensitive(names, run) {
						runs = append(runs, run)
					}
				}
				if len(runs) == 0 {
					continue
				}
				name := method.Names[0].Name
				shapes[name] = append(shapes[name], paramShape{
					method: name,
					types:  types,
					names:  names,
					runs:   runs,
					where:  fileSet.Position(method.Pos()).String(),
				})
			}
			return true
		})
	}
	return shapes
}

// TestNoImplementationTransposesAdjacentSameTypedParameters holds every
// implementation in this module to the parameter order its interface declares.
func TestNoImplementationTransposesAdjacentSameTypedParameters(t *testing.T) {
	t.Parallel()

	files, err := modfiles.Files(".")
	if err != nil {
		t.Fatalf("enumerate module files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("the enumerator returned no files, so this guard would be vacuous")
	}

	shapes := collectShapes(t, files)
	if len(shapes) == 0 {
		t.Fatal("no interface method in this module has an order-sensitive parameter run, so this guard read nothing; commands.CursorWrites.SaveCursor is one, and its absence means the walk did not reach its subject")
	}
	// THE KNOWN MEMBER is the positive control over the collection: a walk that
	// matched nothing, or that matched the wrong node kind, fails here rather
	// than reporting a clean sweep over an empty set.
	if _, held := shapes["SaveCursor"]; !held {
		names := make([]string, 0, len(shapes))
		for name := range shapes {
			names = append(names, name)
		}
		sort.Strings(names)
		t.Fatalf("the walk collected %v but not SaveCursor, whose declaration is the reason this guard exists", names)
	}

	fileSet := token.NewFileSet()
	checked := 0
	for _, path := range files {
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			candidates, held := shapes[function.Name.Name]
			if !held {
				continue
			}
			names, types := flattenParams(fileSet, function.Type.Params)
			for _, shape := range candidates {
				if !equalStrings(shape.types, types) {
					continue
				}
				checked++
				for _, finding := range transposedRuns(shape, names) {
					t.Errorf("%s: %s.%s: %s",
						fileSet.Position(function.Pos()), parsed.Name.Name, function.Name.Name, finding)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no function in this module matched any collected shape, so every assertion above was skipped")
	}
}

// equalStrings reports element-wise equality.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

// TestTransposedRunsReportsATranspositionAndIsSilentOnAgreement drives the
// comparison with a known answer in BOTH directions, so the sweep above is not
// passing because it cannot fail.
//
// The shape is the real one: `SaveCursor(ctx, tenant, session, epoch, order)`,
// with the run at positions 3 and 4.
func TestTransposedRunsReportsATranspositionAndIsSilentOnAgreement(t *testing.T) {
	t.Parallel()

	shape := paramShape{
		method: "SaveCursor",
		types:  []string{"context.Context", "sessionwire.TenantID", "sessionwire.SessionID", "uint64", "uint64"},
		names:  []string{"ctx", "tenant", "session", "epoch", "order"},
		runs:   [][2]int{{3, 5}},
		where:  "internal/commands/consumer.go:169",
	}

	// THE TRANSPOSITION. This is the exact spelling cmd/host/main.go carried.
	transposed := []string{"ctx", "tenant", "session", "order", "epoch"}
	findings := transposedRuns(shape, transposed)
	if len(findings) != 2 {
		t.Fatalf("a fully transposed run reported %d findings (%v), want one per position", len(findings), findings)
	}

	// AGREEMENT. The same comparison over the declared order reports nothing.
	if findings := transposedRuns(shape, shape.names); len(findings) != 0 {
		t.Fatalf("the declared order was reported as transposed: %v", findings)
	}

	// BLANKS ARE PERMITTED, which is the compose doubles' spelling and is a
	// deliberate statement that the value is unused.
	blanked := []string{"_", "_", "_", "_", "order"}
	if findings := transposedRuns(shape, blanked); len(findings) != 0 {
		t.Fatalf("a blanked epoch was reported as transposed: %v", findings)
	}

	// AND A HALF-TRANSPOSITION IS STILL REPORTED: naming the epoch slot "order"
	// while leaving the order slot blank is the shape that reads a position out
	// of an epoch.
	half := []string{"_", "_", "_", "order", "_"}
	if findings := transposedRuns(shape, half); len(findings) != 1 {
		t.Fatalf("a half transposition reported %v, want exactly one finding", findings)
	}
}

// TestSameTypedRunsFindsOnlyAdjacentRunsOfTwoOrMore is the known-answer control
// on the run detector, which is what decides whether a declaration is examined
// at all.
func TestSameTypedRunsFindsOnlyAdjacentRunsOfTwoOrMore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		types []string
		want  [][2]int
	}{
		{name: "none", types: []string{"context.Context", "int", "string"}},
		{name: "trailing pair", types: []string{"context.Context", "uint64", "uint64"}, want: [][2]int{{1, 3}}},
		{name: "leading triple", types: []string{"string", "string", "string", "int"}, want: [][2]int{{0, 3}}},
		{name: "two runs", types: []string{"string", "string", "int", "uint64", "uint64"}, want: [][2]int{{0, 2}, {3, 5}}},
		{name: "separated pair is not a run", types: []string{"uint64", "string", "uint64"}},
		{name: "empty", types: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sameTypedRuns(test.types)
			if len(got) != len(test.want) {
				t.Fatalf("sameTypedRuns(%v) = %v, want %v", test.types, got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("sameTypedRuns(%v) = %v, want %v", test.types, got, test.want)
				}
			}
		})
	}
}

// TestOrderSensitiveIgnoresRunsWithNothingToTranspose is the control on the
// filter: a run of blanks, or one whose names repeat, carries no order and must
// not be collected as a shape. Without this the guard would hold every
// two-string helper in the module to a name it never promised.
func TestOrderSensitiveIgnoresRunsWithNothingToTranspose(t *testing.T) {
	t.Parallel()

	if orderSensitive([]string{"_", "_"}, [2]int{0, 2}) {
		t.Error("a run of blanks was reported as order sensitive")
	}
	if orderSensitive([]string{"", ""}, [2]int{0, 2}) {
		t.Error("a run of unnamed parameters was reported as order sensitive")
	}
	if orderSensitive([]string{"epoch", "_"}, [2]int{0, 2}) {
		t.Error("a run with one named parameter was reported as order sensitive")
	}
	if !orderSensitive([]string{"epoch", "order"}, [2]int{0, 2}) {
		t.Error("a run with two distinct names was NOT reported as order sensitive, so the subject of this guard would never be collected")
	}
}
