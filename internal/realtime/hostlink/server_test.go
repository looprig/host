package hostlink_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
)

func TestExportedPackageBoundaryDoesNotExposeCentrifuge(t *testing.T) {
	t.Parallel()

	packageDir := packageDirectory(t)
	entries, err := os.ReadDir(packageDir)
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	set := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(packageDir, entry.Name())
		file, err := parser.ParseFile(set, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatal("boundary guard found zero production Go files")
	}

	exports := dependencyExports(t, packageDir)
	lookup := func(path string) (io.ReadCloser, error) {
		export, ok := exports[path]
		if !ok || export == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(export)
	}
	information := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{},
		Defs:  map[*ast.Ident]types.Object{},
	}
	checked, err := (&types.Config{Importer: importer.ForCompiler(set, "gc", lookup)}).Check("github.com/looprig/host/internal/realtime/hostlink", set, files, information)
	if err != nil {
		t.Fatalf("type-check production package: %v", err)
	}

	walker := boundaryTypeWalker{t: t, observedPackages: map[string]bool{}, seen: map[types.Type]bool{}}
	exported := 0
	for _, name := range checked.Scope().Names() {
		if !ast.IsExported(name) {
			continue
		}
		exported++
		object := checked.Scope().Lookup(name)
		walker.inspect(object.String(), object.Type())
	}
	if exported == 0 {
		t.Fatal("boundary guard found zero exported package-scope objects")
	}

	exportedDeclarations := 0
	for _, file := range files {
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if !declaration.Name.IsExported() {
					continue
				}
				exportedDeclarations++
				walker.inspect(declaration.Name.Name, information.Defs[declaration.Name].Type())
			case *ast.GenDecl:
				for _, specification := range declaration.Specs {
					switch specification := specification.(type) {
					case *ast.TypeSpec:
						if !specification.Name.IsExported() {
							continue
						}
						exportedDeclarations++
						walker.inspect(specification.Name.Name, information.Defs[specification.Name].Type())
						walker.inspect(specification.Name.Name+" definition", information.TypeOf(specification.Type))
					case *ast.ValueSpec:
						for _, name := range specification.Names {
							if !name.IsExported() {
								continue
							}
							exportedDeclarations++
							walker.inspect(name.Name, information.Defs[name].Type())
						}
					}
				}
			}
		}
	}
	if exportedDeclarations == 0 {
		t.Fatal("boundary guard found zero exported source declarations")
	}
	for _, legal := range []string{"context", "net/http", "time", "github.com/looprig/core/sessionwire/v1"} {
		if !walker.observedPackages[legal] {
			t.Errorf("boundary guard did not traverse legal package %q", legal)
		}
	}
}

type listedPackage struct {
	ImportPath string
	Export     string
}

func dependencyExports(t *testing.T, packageDir string) map[string]string {
	t.Helper()
	command := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	command.Dir = packageDir
	command.Env = appendWithoutGoWork(os.Environ(), "GOWORK=off")
	output, err := command.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list dependencies: %v\n%s", err, exitError.Stderr)
		}
		t.Fatalf("go list dependencies: %v", err)
	}

	exports := map[string]string{}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode go list output: %v", err)
		}
		if listed.Export != "" {
			exports[listed.ImportPath] = listed.Export
		}
	}
	if len(exports) == 0 {
		t.Fatal("go list returned zero dependency exports")
	}
	return exports
}

func appendWithoutGoWork(environment []string, goWork string) []string {
	filtered := make([]string, 0, len(environment)+1)
	for _, variable := range environment {
		if !strings.HasPrefix(variable, "GOWORK=") {
			filtered = append(filtered, variable)
		}
	}
	return append(filtered, goWork)
}

type boundaryTypeWalker struct {
	t                *testing.T
	observedPackages map[string]bool
	seen             map[types.Type]bool
}

func (walker *boundaryTypeWalker) inspect(subject string, typ types.Type) {
	walker.t.Helper()
	if typ == nil || walker.seen[typ] {
		return
	}
	walker.seen[typ] = true

	switch typ := typ.(type) {
	case *types.Alias:
		walker.inspectPackage(subject, typ.Obj())
		for index := 0; index < typ.TypeArgs().Len(); index++ {
			walker.inspect(subject, typ.TypeArgs().At(index))
		}
		walker.inspect(subject, types.Unalias(typ))
	case *types.Named:
		walker.inspectPackage(subject, typ.Obj())
		for index := 0; index < typ.TypeArgs().Len(); index++ {
			walker.inspect(subject, typ.TypeArgs().At(index))
		}
		walker.inspect(subject, typ.Underlying())
		for index := 0; index < typ.NumMethods(); index++ {
			method := typ.Method(index)
			if method.Exported() {
				walker.inspect(subject+" method "+method.Name(), method.Type())
			}
		}
	case *types.Pointer:
		walker.inspect(subject, typ.Elem())
	case *types.Array:
		walker.inspect(subject, typ.Elem())
	case *types.Slice:
		walker.inspect(subject, typ.Elem())
	case *types.Map:
		walker.inspect(subject, typ.Key())
		walker.inspect(subject, typ.Elem())
	case *types.Chan:
		walker.inspect(subject, typ.Elem())
	case *types.Struct:
		for index := 0; index < typ.NumFields(); index++ {
			field := typ.Field(index)
			if field.Exported() || field.Anonymous() {
				walker.inspect(subject+" field "+field.Name(), field.Type())
			}
		}
	case *types.Signature:
		walker.inspectTuple(subject, typ.Params())
		walker.inspectTuple(subject, typ.Results())
		if typeParameters := typ.TypeParams(); typeParameters != nil {
			for index := 0; index < typeParameters.Len(); index++ {
				walker.inspect(subject, typeParameters.At(index).Constraint())
			}
		}
	case *types.Interface:
		typ.Complete()
		for index := 0; index < typ.NumExplicitMethods(); index++ {
			method := typ.ExplicitMethod(index)
			if method.Exported() {
				walker.inspect(subject+" method "+method.Name(), method.Type())
			}
		}
		for index := 0; index < typ.NumEmbeddeds(); index++ {
			walker.inspect(subject, typ.EmbeddedType(index))
		}
	case *types.TypeParam:
		walker.inspect(subject, typ.Constraint())
	case *types.Union:
		for index := 0; index < typ.Len(); index++ {
			walker.inspect(subject, typ.Term(index).Type())
		}
	}
}

func (walker *boundaryTypeWalker) inspectTuple(subject string, tuple *types.Tuple) {
	for index := 0; index < tuple.Len(); index++ {
		walker.inspect(subject, tuple.At(index).Type())
	}
}

func (walker *boundaryTypeWalker) inspectPackage(subject string, object *types.TypeName) {
	if object.Pkg() == nil {
		return
	}
	path := object.Pkg().Path()
	walker.observedPackages[path] = true
	if path == "github.com/centrifugal/centrifuge" || strings.HasPrefix(path, "github.com/centrifugal/centrifuge/") {
		walker.t.Fatalf("%s exposes forbidden type %s from %s", subject, object.Name(), path)
	}
}

func TestHostPinsTheCentrifugeServer(t *testing.T) {
	t.Parallel()

	moduleRoot := filepath.Clean(filepath.Join(packageDirectory(t), "..", "..", ".."))
	data, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	parsed, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		t.Fatalf("parse go.mod: %v", err)
	}
	for _, requirement := range parsed.Require {
		if requirement.Mod.Path == "github.com/centrifugal/centrifuge" {
			if requirement.Mod.Version != "v0.38.0" {
				t.Fatalf("centrifuge version = %q, want exact v0.38.0", requirement.Mod.Version)
			}
			if requirement.Indirect {
				t.Fatal("centrifuge is indirect; the HostLink server must own the dependency")
			}
			return
		}
	}
	t.Fatal("go.mod does not require github.com/centrifugal/centrifuge")
}

func packageDirectory(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate this package")
	}
	return filepath.Dir(source)
}
