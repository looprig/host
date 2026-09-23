package hostlink_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
)

// ---------------------------------------------------------------------------
// Every HostLinkError this Host can emit is one Core can encode
// ---------------------------------------------------------------------------
//
// Core's HostLinkError encoder VALIDATES: epoch_mismatch must carry a non-zero
// current_lease_epoch, runtime_mismatch a runtime_compatibility_id, and every
// other class neither. A refusal that breaks the rule does not reach the wire
// as a refusal at all — json.Marshal fails, and until host v0.7.1 that failure
// left at the transport as Centrifuge 107 bad request, which a Factory reads as
// ITS OWN malformed request (D3.1 F1: every runtime_mismatch an attach earned,
// the dedicated Host's "not my fixed session" among them).
//
// Two tests hold it shut. The table drives every shape an Attacher can hand
// back — the one refusal source this package does not construct itself —
// through the real link, and requires either a body Core's own decoder accepts
// or an INTERNAL transport error, never 107. The structural guard then covers
// every refusal this package constructs itself: each BindError literal that
// names a constant class carries exactly the members Core requires for it.

// hostCompat is a compatibility id a Host could advertise; badCompat is one
// Core's opaque-value rule refuses.
const (
	hostCompat = "runtime-build-2"
	badCompat  = "has a space\x00"
)

// attachOutcome is the ONE outcome a table row may produce: either the
// transport answers 100 with no body, or a specific, fully-named Core class.
//
// A ROW HAS EXACTLY ONE RIGHT ANSWER, never "this code or that one". The
// earlier version of this table accepted 100 for EVERY row and accepted
// either the Attacher's code or the runtime_unavailable downgrade for a
// published one — which is how mutant MA (reverting the per-class member
// switch in Multiplexer.attachRefusal to copy both members unconditionally)
// survived: a regression that turns, say, no_capacity carrying an epoch into
// an unpublishable record and hence a 100 still passed, because 100 was
// always an acceptable answer. Asserting the EXACT outcome closes that.
type attachOutcome struct {
	transportInternal bool // true: reply.Error.Code == 100, no body
	code              sessionwire.HostLinkErrorCode
	epoch             uint64
	compat            string
}

// expectedAttachOutcome is attachRefusal's contract, restated as data. It
// mirrors internal/realtime/hostlink/attach.go's attachRefusal exactly:
//
//   - code == "" or an unknown code: this Host cannot encode a class, so the
//     transport answers 100 regardless of anything else the Attacher named.
//   - epoch_mismatch: an epoch of 0 is Core-unencodable for the class, so it
//     downgrades to runtime_unavailable; a non-zero epoch is published AS
//     epoch_mismatch, but attachRefusal's per-class switch never copies a
//     compat id onto this class, so compat is dropped regardless of what the
//     Attacher named.
//   - runtime_mismatch: an empty or Core-invalid compat id cannot be
//     encoded, so it downgrades to runtime_unavailable; a valid one is
//     published AS runtime_mismatch, with epoch dropped (the switch never
//     copies it onto this class).
//   - every other valid code (not_admitting, releasing, no_capacity,
//     runtime_unavailable): published exactly as that code, with BOTH members
//     dropped — neither rides with a class that carries neither.
func expectedAttachOutcome(code sessionwire.HostLinkErrorCode, epoch uint64, compat string) attachOutcome {
	switch code {
	case "", "not_a_core_code":
		return attachOutcome{transportInternal: true}
	case sessionwire.HostLinkErrorEpochMismatch:
		if epoch == 0 {
			return attachOutcome{code: sessionwire.HostLinkErrorRuntimeUnavailable}
		}
		return attachOutcome{code: sessionwire.HostLinkErrorEpochMismatch, epoch: epoch}
	case sessionwire.HostLinkErrorRuntimeMismatch:
		valid := (sessionwire.HostLinkError{Code: code, RuntimeCompatibilityID: compat}).Validate() == nil
		if !valid {
			return attachOutcome{code: sessionwire.HostLinkErrorRuntimeUnavailable}
		}
		return attachOutcome{code: sessionwire.HostLinkErrorRuntimeMismatch, compat: compat}
	default:
		return attachOutcome{code: code}
	}
}

func TestEveryAttacherRefusalShapeIsCoreEncodableOrAnInternalFailure(t *testing.T) {
	attacher := &recordingAttacher{answer: acceptedObservation(testSession)}
	f := newFixture(t, withAttacher(attacher))
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}

	codes := []sessionwire.HostLinkErrorCode{
		"", // no placement outcome
		sessionwire.HostLinkErrorNotAdmitting,
		sessionwire.HostLinkErrorReleasing,
		sessionwire.HostLinkErrorNoCapacity,
		sessionwire.HostLinkErrorEpochMismatch,
		sessionwire.HostLinkErrorRuntimeUnavailable,
		sessionwire.HostLinkErrorRuntimeMismatch,
		"not_a_core_code",
	}
	id := uint32(2)
	for _, code := range codes {
		for _, epoch := range []uint64{0, 42} {
			for _, compat := range []string{"", hostCompat, badCompat} {
				attacher.refuseWith = &hostlink.AttachRefusal{
					Code:                   code,
					CurrentLeaseEpoch:      epoch,
					RuntimeCompatibilityID: compat,
					Reason:                 "table row",
				}
				reply := rpc(t, connection, id, hostlink.MethodAttach, attachRequest(testSession))
				id++
				row := fmt.Sprintf("code=%q epoch=%d compat=%q", code, epoch, compat)
				want := expectedAttachOutcome(code, epoch, compat)
				if want.transportInternal {
					if reply.Error == nil || reply.Error.Code != 100 {
						t.Errorf("%s: reply = %#v, want transport 100 (internal): this Host can encode no class for this row", row, reply)
					}
					continue
				}
				if reply.Error != nil {
					t.Errorf("%s: answered transport code %d, want the published class %q", row, reply.Error.Code, want.code)
					continue
				}
				wire := refusedRPC(t, reply)
				if wire.Code != want.code {
					t.Errorf("%s: published %q, want exactly %q", row, wire.Code, want.code)
				}
				if wire.CurrentLeaseEpoch != want.epoch {
					t.Errorf("%s: epoch_mismatch names epoch %d, want %d", row, wire.CurrentLeaseEpoch, want.epoch)
				}
				if wire.RuntimeCompatibilityID != want.compat {
					t.Errorf("%s: runtime_mismatch names build %q, want %q", row, wire.RuntimeCompatibilityID, want.compat)
				}
			}
		}
	}

	// THE SPECIFIC FIX, stated positively so the table cannot pass by
	// downgrading everything: a runtime_mismatch with a usable build id is
	// published AS runtime_mismatch.
	attacher.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorRuntimeMismatch, RuntimeCompatibilityID: hostCompat}
	wire := refusedRPC(t, rpc(t, connection, id, hostlink.MethodAttach, attachRequest(testSession)))
	if wire.Code != sessionwire.HostLinkErrorRuntimeMismatch || wire.RuntimeCompatibilityID != hostCompat {
		t.Fatalf("refusal = %#v, want runtime_mismatch naming %q", wire, hostCompat)
	}
}

// TestEveryConstantClassBindErrorCarriesTheMembersCoreRequires is the
// structural half: it parses this package's production files and checks every
// BindError composite literal whose wire class is a Core constant.
//
// A literal whose class is not a constant (the attach path's
// `wire: refused.Code`) is the dynamic case the table above drives. The guard
// fails as vacuous if it finds no constant-class literal, so a refactor that
// renames the field cannot turn it into a test of nothing.
func TestEveryConstantClassBindErrorCarriesTheMembersCoreRequires(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := literal.Type.(*ast.Ident); !ok || ident.Name != "BindError" {
				return true
			}
			fields := map[string]ast.Expr{}
			for _, element := range literal.Elts {
				if kv, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok {
						fields[key.Name] = kv.Value
					}
				}
			}
			wire, ok := fields["wire"].(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := wire.X.(*ast.Ident); !ok || pkg.Name != "sessionwire" {
				return true
			}
			checked++
			at := fset.Position(literal.Pos())
			_, hasEpoch := fields["currentLeaseEpoch"]
			_, hasCompat := fields["runtimeCompatibilityID"]
			switch wire.Sel.Name {
			case "HostLinkErrorEpochMismatch":
				if !hasEpoch || hasCompat {
					t.Errorf("%s: an epoch_mismatch BindError must carry currentLeaseEpoch and no runtimeCompatibilityID", at)
				}
			case "HostLinkErrorRuntimeMismatch":
				if !hasCompat || hasEpoch {
					t.Errorf("%s: a runtime_mismatch BindError must carry runtimeCompatibilityID and no currentLeaseEpoch", at)
				}
			default:
				if hasEpoch || hasCompat {
					t.Errorf("%s: a %s BindError carries a member Core refuses for that class", at, wire.Sel.Name)
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("found no BindError literal with a constant wire class; the guard is vacuous")
	}
}
