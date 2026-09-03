package hostlink_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/realtime/hostlink"
	"golang.org/x/mod/modfile"
)

func TestServerBoundaryUsesLooprigOwnedTypes(t *testing.T) {
	t.Parallel()

	var authenticator hostlink.Authenticator = acceptingAuthenticator{}
	if err := authenticator.VerifyTenant(context.Background(), "tenant-a", "service-secret"); err != nil {
		t.Fatalf("VerifyTenant: %v", err)
	}

	for _, boundary := range []reflect.Type{
		reflect.TypeOf((*hostlink.Authenticator)(nil)).Elem(),
		reflect.TypeOf((*hostlink.Server)(nil)).Elem(),
		reflect.TypeOf(hostlink.Config{}),
	} {
		assertNoCentrifugeType(t, boundary, map[reflect.Type]bool{})
	}
}

type acceptingAuthenticator struct{}

func (acceptingAuthenticator) VerifyTenant(context.Context, sessionwire.TenantID, string) error {
	return nil
}

func assertNoCentrifugeType(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	if typ == nil || seen[typ] {
		return
	}
	seen[typ] = true
	if typ.PkgPath() == "github.com/centrifugal/centrifuge" {
		t.Fatalf("public boundary exposes Centrifuge type %s", typ)
	}
	switch typ.Kind() {
	case reflect.Interface:
		for index := 0; index < typ.NumMethod(); index++ {
			assertNoCentrifugeType(t, typ.Method(index).Type, seen)
		}
	case reflect.Func:
		for index := 0; index < typ.NumIn(); index++ {
			assertNoCentrifugeType(t, typ.In(index), seen)
		}
		for index := 0; index < typ.NumOut(); index++ {
			assertNoCentrifugeType(t, typ.Out(index), seen)
		}
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
		assertNoCentrifugeType(t, typ.Elem(), seen)
	case reflect.Map:
		assertNoCentrifugeType(t, typ.Key(), seen)
		assertNoCentrifugeType(t, typ.Elem(), seen)
	case reflect.Struct:
		for index := 0; index < typ.NumField(); index++ {
			assertNoCentrifugeType(t, typ.Field(index).Type, seen)
		}
	}
}

func TestHostPinsTheCentrifugeServer(t *testing.T) {
	t.Parallel()

	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate this package")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", ".."))
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
