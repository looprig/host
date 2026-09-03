package hostlink_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
	var files []sourceFile
	types := map[string]typeDeclaration{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(packageDir, entry.Name())
		file, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		imports := importPaths(t, file)
		files = append(files, sourceFile{name: entry.Name(), syntax: file, imports: imports})
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.TYPE {
				continue
			}
			for _, specification := range general.Specs {
				typeSpec := specification.(*ast.TypeSpec)
				types[typeSpec.Name.Name] = typeDeclaration{expression: typeSpec.Type, imports: imports}
			}
		}
	}
	if len(files) == 0 {
		t.Fatal("boundary guard found zero production Go files")
	}

	observedPackages := map[string]bool{}
	exported := 0
	for _, file := range files {
		for _, declaration := range file.syntax.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if !ast.IsExported(declaration.Name.Name) {
					continue
				}
				exported++
				assertBoundaryExpression(t, file.name+":"+declaration.Name.Name, declaration.Type, file.imports, types, observedPackages, map[string]bool{})
			case *ast.GenDecl:
				for _, specification := range declaration.Specs {
					switch specification := specification.(type) {
					case *ast.TypeSpec:
						if !ast.IsExported(specification.Name.Name) {
							continue
						}
						exported++
						assertBoundaryExpression(t, file.name+":"+specification.Name.Name, specification.Type, file.imports, types, observedPackages, map[string]bool{})
					case *ast.ValueSpec:
						for index, name := range specification.Names {
							if !ast.IsExported(name.Name) {
								continue
							}
							exported++
							if specification.Type != nil {
								assertBoundaryExpression(t, file.name+":"+name.Name, specification.Type, file.imports, types, observedPackages, map[string]bool{})
							} else if index < len(specification.Values) {
								assertBoundaryExpression(t, file.name+":"+name.Name, specification.Values[index], file.imports, types, observedPackages, map[string]bool{})
							} else {
								t.Fatalf("%s:%s has an inferred exported type the guard cannot resolve", file.name, name.Name)
							}
						}
					}
				}
			}
		}
	}
	if exported == 0 {
		t.Fatal("boundary guard found zero exported declarations")
	}
	for _, legal := range []string{"context", "net/http", "time", "github.com/looprig/core/sessionwire/v1"} {
		if !observedPackages[legal] {
			t.Errorf("boundary guard did not traverse legal package %q", legal)
		}
	}
}

type sourceFile struct {
	name    string
	syntax  *ast.File
	imports map[string]string
}

type typeDeclaration struct {
	expression ast.Expr
	imports    map[string]string
}

func importPaths(t *testing.T, file *ast.File) map[string]string {
	t.Helper()
	imports := map[string]string{}
	for _, specification := range file.Imports {
		path, err := strconv.Unquote(specification.Path.Value)
		if err != nil {
			t.Fatalf("unquote import %s: %v", specification.Path.Value, err)
		}
		name := filepath.Base(path)
		if specification.Name != nil {
			name = specification.Name.Name
		}
		if name == "." && isCentrifugeImport(path) {
			t.Fatalf("dot import could expose forbidden Centrifuge declarations: %s", path)
		}
		imports[name] = path
	}
	return imports
}

func assertBoundaryExpression(t *testing.T, subject string, expression ast.Expr, imports map[string]string, types map[string]typeDeclaration, observedPackages map[string]bool, visiting map[string]bool) {
	t.Helper()
	ast.Inspect(expression, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.SelectorExpr:
			identifier, ok := node.X.(*ast.Ident)
			if !ok {
				return true
			}
			path := imports[identifier.Name]
			if path == "" {
				return true
			}
			observedPackages[path] = true
			if isCentrifugeImport(path) {
				t.Fatalf("%s exposes forbidden type %s.%s from %s", subject, identifier.Name, node.Sel.Name, path)
			}
		case *ast.Ident:
			declaration, ok := types[node.Name]
			if !ok || visiting[node.Name] {
				return true
			}
			visiting[node.Name] = true
			assertBoundaryExpression(t, subject+"->"+node.Name, declaration.expression, declaration.imports, types, observedPackages, visiting)
			delete(visiting, node.Name)
		}
		return true
	})
}

func isCentrifugeImport(path string) bool {
	const module = "github.com/centrifugal/centrifuge"
	return path == module || strings.HasPrefix(path, module+"/")
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
