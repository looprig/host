package harnesstest_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// TestNoProductionPackageImportsTestSupport holds this package's promise that
// nothing in a production path imports it — and so that inference, which it
// alone names, never enters a production build of Host. It reads `go list`'s
// structured import lists (non-test imports only), not source text.
func TestNoProductionPackageImportsTestSupport(t *testing.T) {
	const self = "github.com/looprig/host/internal/harnesstest"
	cmd := exec.Command("go", "list", "-json", "github.com/looprig/host/...")
	cmd.Dir = "../.."
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(out)))
	packages := 0
	for decoder.More() {
		var pkg struct {
			ImportPath string
			Imports    []string
		}
		if err := decoder.Decode(&pkg); err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		packages++
		if pkg.ImportPath == self {
			continue
		}
		for _, imported := range pkg.Imports {
			if imported == self || imported == "github.com/looprig/inference" || strings.HasPrefix(imported, "github.com/looprig/inference/") {
				t.Errorf("production package %s imports %s", pkg.ImportPath, imported)
			}
		}
	}
	if packages < 10 {
		t.Fatalf("go list reported %d packages; the guard read nothing", packages)
	}
}
