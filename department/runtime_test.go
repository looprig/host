package department_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/testkit"
)

// rigSessionUUID is the Harness identity the fake hands back. It is a literal
// with no zero bytes so an assertion cannot pass against uuid.UUID{}.
var rigSessionUUID = uuid.MustParse("6f1c9e2a-3b47-4d58-9a10-2c7e5f8b4d63")

// launchRequest is the create request every test starts from. EVERY field is
// distinct from its zero value, because the standing trap in this program is a
// fixture whose default already equals the value being asserted: such a test
// passes against an adapter that propagates nothing.
func launchRequest() department.CreateRequest {
	return department.CreateRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
		RigSessionID:  rigSessionUUID,
	}
}

// restoreRequest is launchRequest as a restore, for the given durable build.
func restoreRequest(compatibility department.CompatibilityID) department.RestoreRequest {
	return department.RestoreRequest{
		TenantID:        "tenant-9f3",
		SessionID:       "session-71c",
		AgentID:         "reviewer",
		Placement:       sessionwire.HostPlacementDedicated,
		WorkspaceRoot:   "/srv/workspaces/71c",
		Storage:         department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
		CompatibilityID: compatibility,
		RigSessionID:    rigSessionUUID,
	}
}

// rigTarget builds a rig-backed launch target at the file's standard build.
func rigTarget(t *testing.T, rig department.Rig) department.LaunchTarget {
	t.Helper()
	target, err := department.NewRigTarget(rig, "rig-2026-09", pooledCapabilities())
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	return target
}

// ---------------------------------------------------------------------------
// Propagation, asserted against the consumer
// ---------------------------------------------------------------------------

// TestCreatePropagatesTheLaunchContextToTheRig asserts against the RIG, which
// is what consumes these values, and not against the adapter's own getters.
//
// A getter assertion proves an adapter can remember a value. It does not prove
// the value reached the thing that needs it, and the Harness lane has just paid
// for that distinction: a record written into the wrong envelope kind was
// invisible to the released reader that exists to read it, and round-tripping
// it through the writer's own decode proved nothing at all.
func TestCreatePropagatesTheLaunchContextToTheRig(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)
	request := launchRequest()

	if _, err := target.Create(t.Context(), request); err != nil {
		t.Fatalf("Create: %v", err)
	}

	creates := rig.Creates()
	if len(creates) != 1 {
		t.Fatalf("the rig was asked for %d sessions, want exactly 1", len(creates))
	}
	got := creates[0]
	// The expectation is spelled out in LITERALS, not built by converting the
	// request the adapter was given. Deriving the oracle from the same
	// operation the implementation performs makes the two agree by
	// construction: if the adapter converts and the test converts, a dropped
	// field is dropped identically on both sides and the assertion cannot see
	// it. An oracle restates the answer; it does not recompute it.
	want := department.RigCreateRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
		RigSessionID:  uuid.MustParse("6f1c9e2a-3b47-4d58-9a10-2c7e5f8b4d63"),
	}
	if got != want {
		t.Errorf("the rig received %+v, want %+v", got, want)
	}
	assertEveryFieldPropagated(t, got)
}

func TestRestorePropagatesTheLaunchContextAndTheHarnessIdentity(t *testing.T) {
	t.Parallel()

	session := testkit.NewFullSession(rigSessionUUID)
	rig := &testkit.FakeRig{Session: session}
	target := rigTarget(t, rig)

	created, err := target.Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	harnessID, ok := department.RigSessionID(created)
	if !ok {
		t.Fatal("RigSessionID did not recognise a runtime this package adapted")
	}
	if harnessID != rigSessionUUID {
		t.Fatalf("RigSessionID = %v, want %v", harnessID, rigSessionUUID)
	}

	if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restores := rig.Restores()
	if len(restores) != 1 {
		t.Fatalf("the rig was asked to restore %d sessions, want exactly 1", len(restores))
	}
	// A WHOLE-VALUE oracle, spelled in literals, exactly as the create test
	// does — and it is needed MORE here, not less. Create hands the rig a
	// conversion, so a forgotten field stops the build; Restore hands it a
	// field-by-field literal, because RestoreRequest carries two fields a rig
	// never sees and the two types are therefore not convertible. Nothing but
	// this comparison stands between a deleted line and a silently dropped
	// field: probing Storage and TenantID alone left deleting Placement and
	// WorkspaceRoot green, which is "propagate workspace context" gone.
	wantRestore := department.RigRestoreRequest{
		TenantID:      "tenant-9f3",
		SessionID:     "session-71c",
		AgentID:       "reviewer",
		Placement:     sessionwire.HostPlacementDedicated,
		WorkspaceRoot: "/srv/workspaces/71c",
		Storage:       department.StorageContext{Namespace: "tenants/9f3/sessions/71c"},
	}
	if restores[0] != wantRestore {
		t.Errorf("the rig received %+v, want %+v", restores[0], wantRestore)
	}
	assertEveryFieldPropagated(t, restores[0])
	// Harness's identity for the session must reach the rig, because a restore
	// has nothing to restore FROM without it. A zero UUID here is the defect
	// this assertion exists for: the adapter passed uuid.UUID{} and every other
	// test in this file still passed, because none of them looked.
	ids := rig.RestoredIDs()
	if len(ids) != 1 || ids[0] != rigSessionUUID {
		t.Errorf("the rig was asked to restore %v, want %v", ids, rigSessionUUID)
	}
	if rigSessionUUID.IsZero() {
		t.Fatal("the fixture UUID is the zero value, so the assertion above cannot tell a propagated identity from an unset one")
	}
}

// TestAdaptedRuntimeCarriesHostIdentitiesNotHarnessOnes is the adapter O1.1
// said was mandatory, asserted rather than promised. Host's identities come
// from the REQUEST and Harness's UUID is kept beside them: nothing is parsed
// from one into the other, which is what makes the two identity spaces
// independent rather than merely differently spelled.
func TestAdaptedRuntimeCarriesHostIdentitiesNotHarnessOnes(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := runtime.SessionID(); got != "session-71c" {
		t.Errorf("SessionID() = %q, want the request's sessionwire identity", got)
	}
	if got := runtime.AgentID(); got != "reviewer" {
		t.Errorf("AgentID() = %q, want the request's agent identity", got)
	}
	if got := string(runtime.SessionID()); got == rigSessionUUID.String() {
		t.Error("SessionID() returned Harness's UUID; the two identity spaces are not the same one spelled differently")
	}
}

// maxWalkDepth bounds the recursion. A self-referential pointer is not a shape
// any request struct has today, and an unbounded walk on one does not fail —
// it exhausts the stack and kills the process, which is the one failure mode a
// guard must never have. A bounded walk complains instead.
const maxWalkDepth = 16

// assertEveryFieldPropagated requires every field of the value the rig received
// to carry data, so the assertion fails when the struct GROWS and not only when
// a value changes.
//
// It exists because a whole-value comparison covers every field BY VALUE and
// nothing about the struct's SHAPE. Add a field to both RigRestoreRequest and
// RestoreRequest, forget to wire it into the adapter's literal, and got and
// want both sit at the zero value, agree, and pass while the field is silently
// dropped. A hand-enumerated list of "is this fixture field non-zero" checks
// goes stale in the very same edit by the very same mechanism — it is the
// struct's field list written a second time, and the second copy is the one
// nobody updates.
//
// It applies to Create as well as Restore, because the hole is shared. Create's
// conversion protects against DIVERGENT growth — a field added to one type
// alone stops the build — and not against COORDINATED growth, where the field
// is added to both and propagated by neither.
//
// allowedZero names fields, by their full dotted path, whose correct propagated
// value IS the zero — the class for which "require non-zero" is the wrong
// question. It exists because without it the only way to pass is to lie in the
// fixture, and THE LIE DOES NOT STAY LOCAL: these same fixtures feed the
// whole-value literal oracles beside this one, so forcing a future ReadOnly or
// DisableTelemetry bool to true would silently make the non-default the only
// case every other assertion in the file exercises. Name the field here and add
// a separate test for the non-zero case instead. Nothing uses it today; it is
// the documented alternative to the fixture lie, not a live exemption.
//
// Its limit of SCOPE is that the walk sees only the RIG-SIDE struct, the value
// the rig actually received. A field added to RestoreRequest alone and never
// forwarded is invisible to it. That is correct — this assertion is about what
// crossed the seam — but it is not a completeness check on Host's own request
// types.
//
// See unpropagatedFields for what it can and CANNOT judge. It does not claim to
// cover every field class, and the classes it cannot are named there.
func assertEveryFieldPropagated(t *testing.T, received any, allowedZero ...string) {
	t.Helper()
	complaints, err := unpropagatedFields(received, allowedZero...)
	if err != nil {
		t.Fatalf("assertEveryFieldPropagated: %v", err)
	}
	for _, complaint := range complaints {
		t.Error(complaint)
	}
}

// unpropagatedFields reports every field of received that carries no data. It
// is separated from the assertion so it can be tested directly, which is the
// only way to prove a walk does not silently skip a field class.
//
// WHAT IT JUDGES, and why each needed deciding rather than assuming:
//
//   - TYPES THAT JUDGE THEMSELVES. If a value has an IsZero() bool method, that
//     answer wins. reflect.Value.IsZero is NOT the type's notion of empty and
//     time.Time proves it in both directions: walking its fields reports a UTC
//     timestamp as empty, because a nil location IS UTC; refusing to walk it and
//     using reflect's answer reports the ZERO INSTANT IN A NON-UTC ZONE as
//     populated, because loc is then non-nil. Both extremes are wrong for the
//     same type. Asking the type is right for it, for netip.Addr, for uuid.UUID,
//     and for anything else that follows the convention.
//   - SLICES, MAPS and ARRAYS: length AND elements. Length alone was half the
//     question, and the half it left was arbitrary rather than principled — a
//     pointer to "" was unwrapped and reported while a []string{""} was not,
//     which is the same emptiness with opposite verdicts. A Labels or Env field
//     propagated but blanked is exactly the defect this walk exists to catch.
//   - INTERFACES and POINTERS: nil, then the payload. any("") is a non-nil
//     interface holding a zero value.
//   - STRUCTS are asked LAST and in that order: an empty one is reported,
//     because it can never carry data and no walk can judge it; one whose fields
//     are all exported is WALKED PER-FIELD even if it has an IsZero, since the
//     per-field answer is strictly better and a self-judging struct that gains a
//     field would otherwise hide it; only one with unexported fields consults
//     IsZero, then reflect.
//   - UNEXPORTED FIELDS are reported as unverifiable rather than skipped. A test
//     outside this package cannot set one, so it would sit permanently zero.
//
// WHAT IT CANNOT JUDGE, stated because a guard that silently skips a class is
// worse than the enumeration it replaced, and a doc claiming completeness is
// worse than either:
//
//   - A struct with unexported fields and no IsZero method is judged only as a
//     whole. Its individual fields are not checked, so a partially populated one
//     passes.
//   - Recursion is bounded at maxWalkDepth. Beyond it the walk complains rather
//     than descending, so a legitimately deeper shape would be reported as a
//     defect it is not.
//   - A POINTER-RECEIVER IsZero splits one type in two: declared as a value
//     field an all-exported struct is walked per-field, declared as a pointer it
//     satisfies the interface and is judged whole. Whether the walk checks
//     fields therefore depends on how the field was DECLARED, not on the type.
//   - MAP KEYS are never judged, only values, so map[string]string{"": "v"}
//     passes.
//   - allowedZero naming a STRUCT field exempts its entire subtree, and keeps
//     exempting it as the struct grows. An exemption written for one field
//     silently widens to every field added under it later.
//   - PARTIAL emptiness inside a collection. A slice or map passes when ANY
//     element carries data, so []string{"a", ""} and a partially filled array
//     are accepted. Requiring every element would false-positive on a []byte or
//     json.RawMessage holding a zero byte, which is ordinary data; see
//     elementsCarryData for the reasoning behind the line.
//   - A field whose correct value is the zero is a defect to this walk unless
//     named in allowedZero. That is the trade, not an oversight.
func unpropagatedFields(received any, allowedZero ...string) ([]string, error) {
	value := reflect.ValueOf(received)
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("need a struct, got %s", value.Kind())
	}
	if value.NumField() == 0 {
		return nil, fmt.Errorf("%s has no fields, so this assertion would be vacuous", value.Type())
	}
	return walkForData(value, value.Type().Name(), allowedZero, 0), nil
}

func walkForData(value reflect.Value, path string, allowedZero []string, depth int) []string {
	var complaints []string
	for i := range value.NumField() {
		field := value.Type().Field(i)
		name := path + "." + field.Name
		if slices.Contains(allowedZero, name) {
			continue
		}
		if !field.IsExported() {
			complaints = append(complaints, name+" is unexported, so this walk cannot verify it was propagated and a test outside this package cannot set it. Export it, or move the assertion into the package")
			continue
		}
		complaints = append(complaints, valueCarriesData(value.Field(i), name, allowedZero, depth+1)...)
	}
	return complaints
}

func valueCarriesData(value reflect.Value, name string, allowedZero []string, depth int) []string {
	if depth > maxWalkDepth {
		return []string{name + " exceeded the walk's depth limit of " + strconv.Itoa(maxWalkDepth) + "; a cyclic value would otherwise exhaust the stack, and a guard that crashes reports nothing"}
	}
	// A STRUCT IS ASKED LAST, and the ordering is the whole of a defect this
	// walk had. Consulting IsZero before the kind switch let a self-judging
	// all-exported struct short-circuit the per-field walk — and IsZero methods
	// are written when a type has one field and almost never updated when it
	// grows, so the hand-maintained staleness the walk exists to escape came
	// back through the new rule's front door:
	//
	//	type T struct{ A, B string }
	//	func (t T) IsZero() bool { return t.A == "" }
	//
	// T{A: "x"} then reported nothing while B was added and never propagated.
	// IsZero is now consulted only where the walk CANNOT do better: non-struct
	// kinds, and structs with unexported fields. An all-exported struct keeps
	// its per-field answer, which is strictly more informative than any
	// whole-value verdict it could give about itself.
	if value.Kind() == reflect.Struct {
		if value.NumField() == 0 {
			return []string{name + " is an empty struct; it can never carry data and this walk cannot judge it"}
		}
		if allFieldsExported(value.Type()) {
			return walkForData(value, name, allowedZero, depth)
		}
		if complaints, judged := selfJudged(value, name); judged {
			return complaints
		}
		// A foreign struct with no IsZero: judged whole, which is an admitted
		// approximation rather than a per-field answer.
		if value.IsZero() {
			return []string{name + " is the zero " + value.Type().String() + " in what the rig received"}
		}
		return nil
	}

	// Every other kind: the type's own answer wins where it has one, because
	// here the walk has nothing better to offer.
	if complaints, judged := selfJudged(value, name); judged {
		return complaints
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		return elementsCarryData(value, name, allowedZero, depth)
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return []string{name + " is nil in what the rig received"}
		}
		return valueCarriesData(value.Elem(), name, allowedZero, depth+1)
	default:
		if value.IsZero() {
			return []string{name + " is the zero value in what the rig received. Either the adapter does not propagate it or the fixture does not set it — for this assertion those are one defect, because a field nobody wired is invisible to a whole-value comparison: want and got agree at zero"}
		}
		return nil
	}
}

// selfJudged asks a value whether it is zero, if its type says how.
func selfJudged(value reflect.Value, name string) ([]string, bool) {
	if !value.CanInterface() {
		return nil, false
	}
	zeroer, ok := value.Interface().(interface{ IsZero() bool })
	if !ok {
		return nil, false
	}
	if zeroer.IsZero() {
		return []string{name + " reports itself zero via " + value.Type().String() + ".IsZero in what the rig received"}, true
	}
	return nil, true
}

// elementsCarryData requires a collection to be non-empty AND at least one
// element to carry data.
//
// "At least one", not "every one", and the line is a judgement rather than an
// oversight. The escape this closes is a collection PROPAGATED BUT BLANKED —
// []string{"", ""} or map[string]string{"k": ""} — which length alone passed,
// and which made the old rule arbitrary rather than principled: a pointer to ""
// was unwrapped and reported while a slice of "" was not, the same emptiness
// with opposite verdicts.
//
// Requiring EVERY element would go too far in the other direction and produce
// false positives on legitimate data. A []byte or json.RawMessage containing a
// zero byte is ordinary, and so is a []string with one empty entry. Partial
// emptiness INSIDE a collection is therefore out of scope: this walk asks
// whether a field crossed the seam carrying something, not whether every
// element of it is meaningful. A partially populated array or slice passes, and
// that is stated in unpropagatedFields' limits.
func elementsCarryData(value reflect.Value, name string, allowedZero []string, depth int) []string {
	if value.Len() == 0 {
		if value.Kind() == reflect.Array {
			return []string{name + " is a zero-length array; it can never carry data"}
		}
		return []string{name + " is empty in what the rig received. A non-nil empty slice or map is NOT the zero value, so length is the question here and IsZero would have passed it"}
	}

	elements := make([]reflect.Value, 0, value.Len())
	labels := make([]string, 0, value.Len())
	if value.Kind() == reflect.Map {
		for _, key := range value.MapKeys() {
			elements = append(elements, value.MapIndex(key))
			labels = append(labels, name+"["+fmt.Sprint(key.Interface())+"]")
		}
	} else {
		for i := range value.Len() {
			elements = append(elements, value.Index(i))
			labels = append(labels, name+"["+strconv.Itoa(i)+"]")
		}
	}

	var first []string
	for i, element := range elements {
		complaints := valueCarriesData(element, labels[i], allowedZero, depth+1)
		if len(complaints) == 0 {
			return nil
		}
		if first == nil {
			first = complaints
		}
	}
	return append([]string{name + " has " + strconv.Itoa(len(elements)) + " element(s) and NONE of them carries data, so the field was propagated blank"}, first...)
}

func allFieldsExported(typ reflect.Type) bool {
	for i := range typ.NumField() {
		if !typ.Field(i).IsExported() {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// The walk itself
// ---------------------------------------------------------------------------

type walkNested struct{ Name string }

type walkForeign struct{ hidden string }

type walkEmpty struct{}

type walkCyclic struct {
	Name string
	Next *walkCyclic
}

type walkCollections struct {
	Strings []string
	Labels  map[string]string
	Fixed   [2]walkNested
	Raw     []byte
}

// walkVersion is a NON-STRUCT type that judges itself, which is the case the
// element walk cannot answer: it reports emptiness by its leading component,
// while "at least one element carries data" would call {0, 1, 0} populated.
// uuid.UUID is the real instance of this shape in this program.
type walkVersion [3]int

// IsZero reports whether the version is unset, which it decides by its major.
func (v walkVersion) IsZero() bool { return v[0] == 0 }

// walkSelfJudging is the fifth class, and it is the shape an IsZero method
// actually has in the wild: written when the type had ONE field, never revisited
// when it grew. B is invisible to it.
type walkSelfJudging struct{ A, B string }

// IsZero reports whether the value is empty, judging only its first field.
func (w walkSelfJudging) IsZero() bool { return w.A == "" }

type walkOptional struct {
	Str  string
	Flag bool
}

type walkWithEmptyNested struct {
	Str   string
	Empty walkEmpty
}

type walkProbe struct {
	Str     string
	Num     int
	Flag    bool
	Slice   []string
	Map     map[string]string
	Iface   any
	Pointer *string
	When    time.Time
	Nested  walkNested
	Foreign walkForeign
	Bytes   [4]byte
}

type walkUnexported struct {
	Str    string
	hidden string
}

// TestTheWalkJudgesEveryFieldClass is the test that makes the walk trustworthy,
// and it exists because a guard that SILENTLY SKIPS a field class is worse than
// the enumeration it replaced: the enumeration failed visibly when it went
// stale, and a skipped class fails never.
//
// Every row is a field class reflection treats differently, and each is asserted
// in BOTH directions — a populated value must be accepted and an empty one must
// be reported. A row with only the negative half would pass against a walk that
// rejects everything; a row with only the positive half would pass against one
// that checks nothing.
func TestTheWalkJudgesEveryFieldClass(t *testing.T) {
	t.Parallel()

	text := "carried"
	full := walkProbe{
		Str:     "carried",
		Num:     7,
		Flag:    true,
		Slice:   []string{"one"},
		Map:     map[string]string{"k": "v"},
		Iface:   any("carried"),
		Pointer: &text,
		When:    time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Nested:  walkNested{Name: "carried"},
		Foreign: walkForeign{hidden: "carried"},
		Bytes:   [4]byte{1, 2, 3, 4},
	}

	t.Run("a fully populated value is accepted", func(t *testing.T) {
		t.Parallel()
		complaints, err := unpropagatedFields(full)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if len(complaints) != 0 {
			t.Errorf("complaints = %q, want none. A walk that rejects a populated value cannot distinguish a dropped field from a supported one", complaints)
		}
	})

	// The UTC row is the one that matters most and the one a naive walk gets
	// wrong: time.Time stores wall == 0 and loc == nil for a UTC timestamp —
	// nil loc IS UTC — so recursing into its fields reports two of three as
	// zero for a perfectly good time. sessionwire already carries ObservedAt
	// and ExpiresAt, so this is a field class Host will meet.
	t.Run("a UTC time.Time is not reported", func(t *testing.T) {
		t.Parallel()
		probe := full
		probe.When = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
		complaints, err := unpropagatedFields(probe)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		for _, complaint := range complaints {
			if strings.Contains(complaint, "When") {
				t.Errorf("a UTC timestamp was reported as unpropagated: %s", complaint)
			}
		}
	})

	emptied := map[string]func(*walkProbe){
		"Str":     func(p *walkProbe) { p.Str = "" },
		"Num":     func(p *walkProbe) { p.Num = 0 },
		"Flag":    func(p *walkProbe) { p.Flag = false },
		"Bytes":   func(p *walkProbe) { p.Bytes = [4]byte{} },
		"When":    func(p *walkProbe) { p.When = time.Time{} },
		"Foreign": func(p *walkProbe) { p.Foreign = walkForeign{} },
		"Nested":  func(p *walkProbe) { p.Nested = walkNested{} },
		// IsZero cannot see these four. A nil slice is the zero value and an
		// EMPTY NON-NIL one is not, so length is the question; a non-nil
		// interface or pointer holding a zero payload is likewise not zero.
		"Slice":       func(p *walkProbe) { p.Slice = []string{} },
		"Map":         func(p *walkProbe) { p.Map = map[string]string{} },
		"Iface":       func(p *walkProbe) { p.Iface = any("") },
		"Pointer":     func(p *walkProbe) { empty := ""; p.Pointer = &empty },
		"nil slice":   func(p *walkProbe) { p.Slice = nil },
		"nil pointer": func(p *walkProbe) { p.Pointer = nil },
		"nil iface":   func(p *walkProbe) { p.Iface = nil },
	}
	fields := map[string]string{"nil slice": "Slice", "nil pointer": "Pointer", "nil iface": "Iface"}

	// FLOORED, and for the reason this whole file exists: deleting rows reduces
	// coverage silently, which is the same staleness the walk was built to
	// escape, reintroduced in the test that certifies the walk. Raise this
	// deliberately when adding a class; do not lower it to match a deletion.
	if len(emptied) != 14 {
		t.Fatalf("the emptied table has %d rows, want 14. Each is a field class reflection treats differently; a deleted row is a class nobody checks", len(emptied))
	}

	for name, empty := range emptied {
		t.Run("an empty "+name+" is reported", func(t *testing.T) {
			t.Parallel()
			probe := full
			empty(&probe)
			complaints, err := unpropagatedFields(probe)
			if err != nil {
				t.Fatalf("unpropagatedFields: %v", err)
			}
			field := name
			if mapped, ok := fields[name]; ok {
				field = mapped
			}
			if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "."+field) }) {
				t.Errorf("emptying %s produced %q, want a complaint naming it. A field class the walk cannot see is a field the guard silently skips", name, complaints)
			}
		})
	}

	t.Run("an unexported field is reported rather than skipped", func(t *testing.T) {
		t.Parallel()
		complaints, err := unpropagatedFields(walkUnexported{Str: "carried", hidden: "carried"})
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "hidden") }) {
			t.Errorf("complaints = %q, want one naming the unexported field. Skipping it silently is the hole this whole test exists to deny", complaints)
		}
	})

	// The NESTED empty struct, distinct from the top-level one below. The
	// top-level case is refused outright; a nested one is reachable by the walk
	// and was silently skipped, which is the failure mode this whole test denies.
	// W1. The type's own IsZero wins, and time.Time is why: reflect gets it
	// wrong in BOTH directions. Walking its fields calls a UTC timestamp empty,
	// because a nil location IS UTC. Refusing to walk it and trusting
	// reflect.Value.IsZero calls the ZERO INSTANT IN A NON-UTC ZONE populated,
	// because loc is then non-nil — which is a live escape on a type sessionwire
	// already carries.
	t.Run("a type that judges itself is asked", func(t *testing.T) {
		t.Parallel()
		zoned := time.Time{}.In(time.FixedZone("Z", 3600))
		if !zoned.IsZero() {
			t.Fatal("the fixture is not the zero instant, so this row proves nothing")
		}
		if reflect.ValueOf(zoned).IsZero() {
			t.Fatal("reflect already agrees with the type here, so this row cannot distinguish the two")
		}
		probe := full
		probe.When = zoned
		complaints, err := unpropagatedFields(probe)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "When") }) {
			t.Errorf("complaints = %q, want one naming When. A zero instant carrying a location is still the zero instant, and only the type knows that", complaints)
		}
	})

	// W2 and W4. A collection propagated but BLANKED passed on length alone,
	// and the half-question was arbitrary: a pointer to "" was reported while a
	// slice of "" was not.
	t.Run("a collection whose elements carry nothing is reported", func(t *testing.T) {
		t.Parallel()
		populated := walkCollections{
			Strings: []string{"carried"},
			Labels:  map[string]string{"k": "carried"},
			Fixed:   [2]walkNested{{Name: "carried"}, {}},
			Raw:     []byte{0x00, 0x01},
		}
		complaints, err := unpropagatedFields(populated)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if len(complaints) != 0 {
			t.Errorf("complaints = %q, want none. A partially filled collection is propagated, and a []byte holding a zero byte is ordinary data", complaints)
		}

		for name, blank := range map[string]walkCollections{
			"Strings": {Strings: []string{"", ""}, Labels: map[string]string{"k": "v"}, Fixed: [2]walkNested{{Name: "x"}}, Raw: []byte{1}},
			"Labels":  {Strings: []string{"x"}, Labels: map[string]string{"k": ""}, Fixed: [2]walkNested{{Name: "x"}}, Raw: []byte{1}},
			"Fixed":   {Strings: []string{"x"}, Labels: map[string]string{"k": "v"}, Fixed: [2]walkNested{}, Raw: []byte{1}},
			"Raw":     {Strings: []string{"x"}, Labels: map[string]string{"k": "v"}, Fixed: [2]walkNested{{Name: "x"}}, Raw: []byte{0, 0}},
		} {
			complaints, err := unpropagatedFields(blank)
			if err != nil {
				t.Fatalf("unpropagatedFields: %v", err)
			}
			if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "."+name) }) {
				t.Errorf("a blanked %s produced %q, want a complaint naming it", name, complaints)
			}
		}
	})

	// The NON-STRUCT half of "ask the type". It needs its own row because the
	// time.Time row exercises the STRUCT branch — time.Time has unexported
	// fields — so removing IsZero for every other kind was a survivor until
	// this existed. An array whose type reports emptiness by one component
	// disagrees with "at least one element carries data", which is exactly the
	// case where the walk cannot do better than the type.
	t.Run("a non-struct type that judges itself is asked", func(t *testing.T) {
		t.Parallel()
		probe := struct{ V walkVersion }{V: walkVersion{0, 1, 0}}
		if !probe.V.IsZero() {
			t.Fatal("the fixture does not report itself zero, so this row proves nothing")
		}
		if reflect.ValueOf(probe.V).IsZero() {
			t.Fatal("reflect already agrees, so this row cannot distinguish the two")
		}
		complaints, err := unpropagatedFields(probe)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, ".V") }) {
			t.Errorf("complaints = %q, want one naming V. An element walk would call {0,1,0} populated; only the type knows it is unset", complaints)
		}
		// And a populated one passes, so the row is not satisfied by a walk
		// that rejects every value of this type.
		complaints, err = unpropagatedFields(struct{ V walkVersion }{V: walkVersion{1, 0, 0}})
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if len(complaints) != 0 {
			t.Errorf("complaints = %q, want none for a set version", complaints)
		}
	})

	// The fifth class, and a direct consequence of the fix for the first: asking
	// the type won for time.Time and then won too often. A self-judging struct
	// whose fields are all exported must still be WALKED, because IsZero
	// methods are written for one field and almost never updated when the
	// struct grows — which is the hand-maintained staleness this whole walk
	// exists to escape, re-entering through the new rule's front door.
	t.Run("a self-judging all-exported struct is still walked per-field", func(t *testing.T) {
		t.Parallel()
		probe := struct{ S walkSelfJudging }{S: walkSelfJudging{A: "carried"}}
		if probe.S.IsZero() {
			t.Fatal("the fixture reports itself zero, so this row cannot show the walk overriding it")
		}
		complaints, err := unpropagatedFields(probe)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, ".S.B") }) {
			t.Errorf("complaints = %q, want one naming S.B. The type says it is populated and it is missing a field; the per-field answer is the better one and must win", complaints)
		}
		// And the walk does not simply ignore the type: fully populated passes.
		full := struct{ S walkSelfJudging }{S: walkSelfJudging{A: "carried", B: "carried"}}
		complaints, err = unpropagatedFields(full)
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if len(complaints) != 0 {
			t.Errorf("complaints = %q, want none for a fully populated self-judging struct", complaints)
		}
	})

	// W5. A cyclic value must COMPLAIN, not exhaust the stack. A guard whose
	// failure mode on an input class is a crash reports nothing at all.
	t.Run("a cyclic value is bounded rather than fatal", func(t *testing.T) {
		t.Parallel()
		cycle := &walkCyclic{Name: "carried"}
		cycle.Next = cycle
		complaints, err := unpropagatedFields(struct{ Cycle *walkCyclic }{Cycle: cycle})
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "depth limit") }) {
			t.Errorf("complaints = %q, want one naming the depth limit", complaints)
		}
	})

	// W6. The documented alternative to lying in a shared fixture.
	t.Run("allowedZero exempts exactly the field it names", func(t *testing.T) {
		t.Parallel()
		probe := walkOptional{Str: "carried"}
		complaints, err := unpropagatedFields(probe, "walkOptional.Flag")
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if len(complaints) != 0 {
			t.Errorf("complaints = %q, want none: the zero field was named as permitted", complaints)
		}
		// And it exempts nothing else: blanking a second field still reports.
		complaints, err = unpropagatedFields(walkOptional{}, "walkOptional.Flag")
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "Str") }) {
			t.Errorf("complaints = %q, want one naming Str. An opt-out that exempts more than it names is worse than none", complaints)
		}
	})

	t.Run("a pointer to an empty struct is reported", func(t *testing.T) {
		t.Parallel()
		complaints, err := unpropagatedFields(struct {
			Str   string
			Empty *walkEmpty
		}{Str: "carried", Empty: &walkEmpty{}})
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "Empty") }) {
			t.Errorf("complaints = %q, want one naming the pointer to an empty struct", complaints)
		}
	})

	t.Run("a nested empty struct is reported rather than skipped", func(t *testing.T) {
		t.Parallel()
		complaints, err := unpropagatedFields(walkWithEmptyNested{Str: "carried"})
		if err != nil {
			t.Fatalf("unpropagatedFields: %v", err)
		}
		if !slices.ContainsFunc(complaints, func(c string) bool { return strings.Contains(c, "Empty") }) {
			t.Errorf("complaints = %q, want one naming the nested empty struct", complaints)
		}
	})

	t.Run("a non-struct and an empty struct are refused", func(t *testing.T) {
		t.Parallel()
		if _, err := unpropagatedFields("not a struct"); err == nil {
			t.Error("unpropagatedFields accepted a non-struct")
		}
		if _, err := unpropagatedFields(struct{}{}); err == nil {
			t.Error("unpropagatedFields accepted a struct with no fields, which would be a vacuous assertion")
		}
	})
}

// ---------------------------------------------------------------------------
// Fail closed on compatibility mismatch
// ---------------------------------------------------------------------------

// TestRestoreFailsClosedOnCompatibilityMismatch is an ORDERING claim as much as
// an error claim: the refusal must happen with nothing launched. An adapter
// that starts a session and then notices the mismatch has already spent the
// resource it was supposed to protect.
func TestRestoreFailsClosedOnCompatibilityMismatch(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)

	runtime, err := target.Restore(t.Context(), restoreRequest("rig-2026-08"))
	var mismatch *department.CompatibilityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Restore onto a different runtime build error = %v, want *CompatibilityMismatchError", err)
	}
	if runtime != nil {
		t.Error("Restore returned a runtime alongside its error")
	}
	if rig.Launches() != 0 {
		t.Errorf("the rig was asked to launch %d times; a fail-closed mismatch must be decided before anything is launched", rig.Launches())
	}
	if mismatch.Durable != "rig-2026-08" || mismatch.Target != "rig-2026-09" {
		t.Errorf("mismatch = %+v, want it to name both the durable build and this target's", mismatch)
	}
	if got := mismatch.ErrorCode(); got != sessionwire.ErrorCodeRuntimeUnavailable {
		t.Errorf("ErrorCode() = %q, want %q", got, sessionwire.ErrorCodeRuntimeUnavailable)
	}

	// The control: the SAME request with the matching build launches, so the
	// refusal is the compatibility id and not the request being malformed.
	if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); err != nil {
		t.Fatalf("Restore with the matching build: %v", err)
	}
	if rig.Launches() != 1 {
		t.Errorf("the matching restore launched %d times, want 1", rig.Launches())
	}
}

// TestRestoreComparesAgainstTHISTargetsBuild varies the TARGET side, which
// nothing did.
//
// Every other restore test builds its target through rigTarget, which hardcodes
// "rig-2026-09", so only the request side ever varied — and replacing
// `request.CompatibilityID != t.compatibility` with a comparison against the
// literal "rig-2026-09" left the whole module green. This is the accessor /
// comparison divergence that CompatibilityID's own doc gives as the reason it
// has a test, uncovered on the comparison half: the accessor got its second
// target two rounds ago and the comparison did not.
//
// The failure it admits is not subtle. A Host registering two agents at
// different builds would have a target declared rig-2025-01 that ACCEPTS
// durable state written by rig-2026-09 and REFUSES its own, reporting itself
// contradictory on the way — "written by rig-2025-01 and this target launches
// rig-2025-01" — because Target is read off t.compatibility while the decision
// no longer is.
func TestRestoreComparesAgainstTHISTargetsBuild(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target, err := department.NewRigTarget(rig, "rig-2025-01", pooledCapabilities())
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}

	// Its own build is accepted.
	if _, err := target.Restore(t.Context(), restoreRequest("rig-2025-01")); err != nil {
		t.Fatalf("Restore onto this target's own build: %v", err)
	}
	if rig.Launches() != 1 {
		t.Fatalf("the matching restore launched %d times, want 1", rig.Launches())
	}

	// The build every OTHER test in this file uses is refused, which is the
	// half a hardcoded comparison inverts.
	runtime, err := target.Restore(t.Context(), restoreRequest("rig-2026-09"))
	var mismatch *department.CompatibilityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Restore onto a foreign build error = %v, want *CompatibilityMismatchError", err)
	}
	if runtime != nil {
		t.Error("Restore returned a runtime alongside its error")
	}
	if rig.Launches() != 1 {
		t.Errorf("the rig launched %d times; the refusal must be decided before anything is launched", rig.Launches())
	}
	if mismatch.Durable != "rig-2026-09" || mismatch.Target != "rig-2025-01" {
		t.Errorf("mismatch = %+v, want Durable rig-2026-09 and Target rig-2025-01. An error naming the same build twice means the comparison and the accessor disagree", mismatch)
	}

	// The comparison is EXACT BYTE IDENTITY, and the two rows below are the
	// leniencies a maintainer reaches for. Both FAIL OPEN, which is why they are
	// pinned rather than left to taste: Core validates this field with
	// validateHostLinkOpaque, an opaque-identity rule, so "tolerate a compatible
	// family" or "ignore casing" is a contract change wearing the costume of a
	// kindness. A prefix rule restores rig-2026-09-hotfix2 state onto a
	// rig-2026-09 runtime; a case-insensitive one restores RIG-2026-09 state
	// onto anything.
	for _, durable := range []department.CompatibilityID{
		"rig-2025-01-hotfix2", // extends the target's id
		"RIG-2025-01",         // differs only in case
		"rig-2025",            // a prefix OF the target's id
	} {
		runtime, err := target.Restore(t.Context(), restoreRequest(durable))
		if !errors.As(err, &mismatch) {
			t.Errorf("Restore of state written by %q error = %v, want *CompatibilityMismatchError. Compatibility is opaque byte identity, and every loosening of it fails open", durable, err)
		}
		if runtime != nil {
			t.Errorf("Restore of state written by %q returned a runtime", durable)
		}
	}
	if rig.Launches() != 1 {
		t.Errorf("the rig launched %d times, want 1: none of the near-miss builds may launch", rig.Launches())
	}
}

// TestRestoreWithNoDeclaredCompatibilityFailsClosed covers the ZERO VALUE of
// the field, which is the forgotten-field case rather than a deliberate one: a
// RestoreRequest nobody filled in must not restore onto an arbitrary runtime.
func TestRestoreWithNoDeclaredCompatibilityFailsClosed(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	_, err := rigTarget(t, rig).Restore(t.Context(), restoreRequest(""))
	var mismatch *department.CompatibilityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Restore with an empty compatibility id error = %v, want *CompatibilityMismatchError", err)
	}
	if rig.Launches() != 0 {
		t.Error("the rig was launched for a restore with no declared runtime build")
	}
}

// ---------------------------------------------------------------------------
// Capability rejection
// ---------------------------------------------------------------------------

// TestLaunchRejectsASessionMissingACapability walks every capability Host
// requires and proves each one is individually load-bearing.
//
// The rejection is BEFORE publication in the sense this task can actually
// prove: no Runtime value is returned, so there is nothing for a caller to
// register. Asserting that also requires the control below — the same rig with
// a complete session succeeds — or "returns an error" would be satisfied by an
// adapter that rejects everything.
func TestLaunchRejectsASessionMissingACapability(t *testing.T) {
	t.Parallel()

	if len(testkit.AllCapabilities) != 6 {
		t.Fatalf("testkit lists %d capabilities, want the 6 Host requires", len(testkit.AllCapabilities))
	}

	for _, capability := range testkit.AllCapabilities {
		t.Run(string(capability), func(t *testing.T) {
			t.Parallel()
			rig := &testkit.FakeRig{Session: testkit.NewSessionWithout(rigSessionUUID, capability)}
			target := rigTarget(t, rig)

			runtime, err := target.Create(t.Context(), launchRequest())
			var incapable *department.IncapableRuntimeError
			if !errors.As(err, &incapable) {
				t.Fatalf("Create with a session missing %s error = %v, want *IncapableRuntimeError", capability, err)
			}
			if runtime != nil {
				t.Error("Create returned a runtime alongside its error; there must be nothing to publish")
			}
			if !slices.Contains(incapable.Missing, string(capability)) {
				t.Errorf("Missing = %q, want it to name %s", incapable.Missing, capability)
			}
			if len(incapable.Missing) != 1 {
				t.Errorf("Missing = %q, want exactly the one capability withheld", incapable.Missing)
			}
			if incapable.AgentID != "reviewer" || incapable.SessionID != "session-71c" {
				t.Errorf("error = %+v, want it to name the agent and session", incapable)
			}

			// Restore rejects the same session for the same reason, so the
			// check is not on one path only.
			if _, err := target.Restore(t.Context(), restoreRequest("rig-2026-09")); !errors.As(err, &incapable) {
				t.Errorf("Restore with a session missing %s error = %v, want *IncapableRuntimeError", capability, err)
			}
		})
	}

	t.Run("control: a complete session is accepted", func(t *testing.T) {
		t.Parallel()
		rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
		runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
		if err != nil {
			t.Fatalf("Create with a complete session: %v", err)
		}
		if runtime == nil {
			t.Fatal("Create returned no runtime and no error")
		}
	})
}

// TestIncapableRuntimeErrorNamesEveryMissingCapability holds that a session
// short of several capabilities costs one round trip to diagnose rather than
// five. An error naming only the first is a guard that makes you run it again.
func TestIncapableRuntimeErrorNamesEveryMissingCapability(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewBareSession(rigSessionUUID)}
	_, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	var incapable *department.IncapableRuntimeError
	if !errors.As(err, &incapable) {
		t.Fatalf("Create error = %v, want *IncapableRuntimeError", err)
	}
	want := make([]string, 0, len(testkit.AllCapabilities))
	for _, capability := range testkit.AllCapabilities {
		want = append(want, string(capability))
	}
	slices.Sort(want)
	got := slices.Clone(incapable.Missing)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("Missing = %q, want all five: %q", got, want)
	}
	for _, capability := range want {
		if !strings.Contains(incapable.Error(), capability) {
			t.Errorf("Error() = %q, want it to name %s", incapable.Error(), capability)
		}
	}
}

// TestRigSessionIDSurvivesAWrapperAndRefusesAStranger covers the branch nothing
// covered, and the reason it is an interface assertion.
//
// runtime.(*rigRuntime) would break the day anything wraps a Runtime — a
// residency decorator, a metrics wrapper, a spy — and it would break by
// returning false, which reads as "not one of ours" when it means "your wrapper
// ate it". The interface assertion survives a wrapper that forwards the method.
func TestRigSessionIDSurvivesAWrapperAndRefusesAStranger(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	adapted, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if got, ok := department.RigSessionID(forwardingWrapper{Runtime: adapted}); !ok || got != rigSessionUUID {
		t.Errorf("RigSessionID through a forwarding wrapper = (%v, %v), want (%v, true). A concrete type assertion would report false here and read as \"not one of ours\"", got, ok, rigSessionUUID)
	}

	// A Runtime this package did not adapt reports false, which is the branch
	// that had no test at all.
	if got, ok := department.RigSessionID(fakeRuntime{}); ok {
		t.Errorf("RigSessionID of a foreign Runtime = (%v, %v), want (zero, false)", got, ok)
	}
	if _, ok := department.RigSessionID(opaqueWrapper{Runtime: adapted}); ok {
		t.Error("RigSessionID reported true through a wrapper that does not forward it")
	}
}

// forwardingWrapper is the decorator shape O2 will actually build.
type forwardingWrapper struct{ department.Runtime }

// RigSessionID forwards to the wrapped runtime.
func (w forwardingWrapper) RigSessionID() uuid.UUID {
	id, _ := department.RigSessionID(w.Runtime)
	return id
}

// opaqueWrapper wraps a Runtime without forwarding the correlation method.
type opaqueWrapper struct{ department.Runtime }

// ---------------------------------------------------------------------------
// Construction and rig failure
// ---------------------------------------------------------------------------

func TestNewRigTargetRejectsAnUnusableTarget(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnknown

	tests := []struct {
		name          string
		rig           department.Rig
		compatibility department.CompatibilityID
		capabilities  department.Capabilities
	}{
		{name: "nil rig", rig: nil, compatibility: "rig-2026-09", capabilities: pooledCapabilities()},
		{name: "empty compatibility id", rig: &testkit.FakeRig{}, compatibility: "", capabilities: pooledCapabilities()},
		{name: "pooled with unsafe capture", rig: &testkit.FakeRig{}, compatibility: "rig-2026-09", capabilities: unsafe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			target, err := department.NewRigTarget(tt.rig, tt.compatibility, tt.capabilities)
			if err == nil {
				t.Fatal("NewRigTarget accepted an unusable target")
			}
			if target != nil {
				t.Error("NewRigTarget returned a target alongside its error; an unusable target must not reach a Registration, let alone a Department")
			}
		})
	}
}

// TestNewRigTargetSharesTheDepartmentsCapabilityRules is the one-site check.
// NewRigTarget and department.New both refuse the same Capabilities, because
// the rules live in Capabilities.Validate and neither restates them. Two sites
// applying the same rule separately is the shape this package has already spent
// several rounds removing.
func TestNewRigTargetSharesTheDepartmentsCapabilityRules(t *testing.T) {
	t.Parallel()

	unsafe := pooledCapabilities()
	unsafe.CaptureSafety = department.CaptureSafetyUnboundedMaterialized
	noPlacement := pooledCapabilities()
	noPlacement.SupportsPooled, noPlacement.SupportsDedicated = false, false
	zeroWeight := pooledCapabilities()
	zeroWeight.AdmissionWeight = 0

	for _, capabilities := range []department.Capabilities{unsafe, noPlacement, zeroWeight} {
		_, targetErr := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", capabilities)
		_, registryErr := department.New([]department.Registration{{
			AgentID: "reviewer",
			Target:  stubTarget{compatibility: "rig-2026-09", capabilities: capabilities},
		}})
		if (targetErr == nil) != (registryErr == nil) {
			t.Errorf("for %+v NewRigTarget error = %v but department.New error = %v; the two disagree about the same rule", capabilities, targetErr, registryErr)
		}
		// BOTH halves. The extraction that created *InvalidCapabilitiesError
		// also moved O1.1's "unwraps to nil" assertion off the capability rules
		// to make room for it — so it created a guarantee, relocated the
		// assertion that used to constrain that code, and then pinned only
		// NewRigTarget's side. Deleting `Cause: err` from validateRegistration's
		// capability branch left the suite green.
		var invalid *department.InvalidCapabilitiesError
		if !errors.As(targetErr, &invalid) {
			t.Errorf("NewRigTarget error = %v; errors.As did not reach *InvalidCapabilitiesError", targetErr)
		}
		if !errors.As(registryErr, &invalid) {
			t.Errorf("department.New error = %v; errors.As did not reach *InvalidCapabilitiesError. The registry applies the same rules through the same function, so it must carry the same typed cause", registryErr)
		}
	}

	// The control: a legal Capabilities is accepted by both.
	if _, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", pooledCapabilities()); err != nil {
		t.Errorf("NewRigTarget with legal capabilities: %v", err)
	}
}

// TestLaunchWrapsARigFailureWithoutLosingTheCause keeps the repository rule
// that a package-level API returns a typed error preserving its cause.
func TestLaunchWrapsARigFailureWithoutLosingTheCause(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{NewErr: testkit.ErrRigRefused, RestoreErr: testkit.ErrRigRefused}
	target := rigTarget(t, rig)

	for _, launch := range []struct {
		name string
		run  func() (department.Runtime, error)
	}{
		{name: "create", run: func() (department.Runtime, error) { return target.Create(t.Context(), launchRequest()) }},
		{name: "restore", run: func() (department.Runtime, error) {
			return target.Restore(t.Context(), restoreRequest("rig-2026-09"))
		}},
	} {
		t.Run(launch.name, func(t *testing.T) {
			runtime, err := launch.run()
			var launchErr *department.RigLaunchError
			if !errors.As(err, &launchErr) {
				t.Fatalf("%s error = %v, want *RigLaunchError", launch.name, err)
			}
			if !errors.Is(err, testkit.ErrRigRefused) {
				t.Errorf("%s error does not unwrap to the rig's own cause", launch.name)
			}
			if runtime != nil {
				t.Error("a failed launch returned a runtime")
			}
			// The exact label, not merely a non-empty one: swapping "create"
			// and "restore" left the suite green, which is a message that
			// sends the reader to the wrong half of the adapter.
			if launchErr.Operation != launch.name {
				t.Errorf("Operation = %q, want %q", launchErr.Operation, launch.name)
			}
		})
	}

	// The Error method must not panic on a nil Cause. This is an EXPORTED
	// struct, so another package may construct one without a cause, and an
	// Error method that dereferences unconditionally fires its nil panic while
	// something is already reporting a failure — the worst possible moment.
	// Nothing tested this: deleting the nil check left the suite green because
	// every RigLaunchError in this file is built by the adapter, which always
	// supplies one.
	t.Run("a launch error with no cause still formats", func(t *testing.T) {
		t.Parallel()
		bare := &department.RigLaunchError{AgentID: "reviewer", SessionID: "session-71c", Operation: "create"}
		message := bare.Error()
		if message == "" {
			t.Fatal("Error() returned nothing")
		}
		if !strings.Contains(message, "reviewer") || !strings.Contains(message, "create") {
			t.Errorf("Error() = %q, want it to name the agent and the operation", message)
		}
		if errors.Unwrap(bare) != nil {
			t.Error("Unwrap() invented a cause")
		}
	})
}

// TestLaunchRejectsASessionTheRigDidNotReturn covers the rig that reports
// success and hands back nothing, which is a contract violation this package
// must not turn into a nil dereference three layers away.
//
// It covers BOTH nils. A nil interface is what a fake produces; a TYPED nil —
// a non-nil interface holding a nil pointer — is what a real rig produces, from
// `var s *Session; ...; return s, nil`, and that one panicked. The guard that
// claimed to prevent a dereference three layers away covered only the nil this
// package's own test double happens to make.
func TestLaunchRejectsASessionTheRigDidNotReturn(t *testing.T) {
	t.Parallel()

	t.Run("a typed nil is not a session", func(t *testing.T) {
		t.Parallel()
		var absent *testkit.FullSession
		rig := &testkit.FakeRig{Session: absent}
		runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
		if runtime != nil {
			t.Error("Create returned a runtime for a typed-nil session")
		}
		var launchErr *department.RigLaunchError
		if !errors.As(err, &launchErr) {
			t.Fatalf("Create with a typed-nil session error = %v, want *RigLaunchError", err)
		}
		if !errors.Is(err, department.ErrNoRigSession) {
			t.Error("a typed-nil session was not reported as an absent one")
		}
	})

	rig := &testkit.FakeRig{}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err == nil {
		t.Fatal("Create accepted a rig that returned no session and no error")
	}
	if runtime != nil {
		t.Error("Create returned a runtime for a session that does not exist")
	}

	// The TYPE and the CAUSE, not merely that something failed. Asserting only
	// "an error came back" is what let a real mutant live: delete the nil check
	// AND move session.ID() below the capability discovery, and a nil session
	// becomes IncapableRuntimeError{Missing: all five} — because a type
	// assertion on a nil interface returns ok=false. No panic, suite green, and
	// a diagnosis that sends the reader to look for five missing methods on a
	// session that was never returned.
	//
	// This is why "removing the guard only panics" was the wrong reading here.
	// It holds only when nothing can be moved above the dereference, and here
	// the dereference is reorderable.
	var launchErr *department.RigLaunchError
	if !errors.As(err, &launchErr) {
		t.Fatalf("Create error = %v, want *RigLaunchError", err)
	}
	if !errors.Is(err, department.ErrNoRigSession) {
		t.Errorf("Create error does not unwrap to ErrNoRigSession, so it cannot be told from a rig that refused")
	}
	var incapable *department.IncapableRuntimeError
	if errors.As(err, &incapable) {
		t.Errorf("a rig that returned no session was diagnosed as an incapable one: %v", incapable)
	}
}

// TestRigTargetExposesItsDeclaredCompatibilityAndCapabilities asserts the
// ACCESSORS, which nothing did.
//
// "Expose the declared compatibility id" is a named property of this task and
// it had no test: a mutant returning a constant from CompatibilityID left the
// whole suite green, because every other test reached t.compatibility through
// Restore's comparison, which is a different code path. The two uses are what
// make the gap matter rather than merely exist — HostLink advertises this value
// as RuntimeCompatibilityID and Restore refuses a durable state that disagrees
// with it, so an accessor out of step with the comparison is a Host advertising
// a build it will then refuse to restore.
func TestRigTargetExposesItsDeclaredCompatibilityAndCapabilities(t *testing.T) {
	t.Parallel()

	declared := department.Capabilities{
		SupportsPooled:     false,
		SupportsDedicated:  true,
		RequiresWorkspace:  true,
		RequiresCheckpoint: true,
		AdmissionWeight:    7,
		CaptureSafety:      department.CaptureSafetyUnboundedMaterialized,
	}
	target, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2026-09", declared)
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	if got := target.CompatibilityID(); got != "rig-2026-09" {
		t.Errorf("CompatibilityID() = %q, want the declared build", got)
	}
	if got := target.Capabilities(); got != declared {
		t.Errorf("Capabilities() = %+v, want %+v", got, declared)
	}
	// The control, and it is LOCAL on purpose. A second target declared with a
	// different build AND different capabilities: no hardcoded return can
	// satisfy two distinct declarations, so this kills the exploit form of the
	// mutation — the one that returns exactly the constant the first assertion
	// expects — without depending on any other test.
	//
	// Both halves were measured, and the two accessors were not in the same
	// state. CompatibilityID's control was already local. Capabilities' was
	// CROSS-TEST: this test asserted `declared` and
	// TestARigTargetSurvivesRegistration asserted pooledCapabilities(), which
	// is a real guard but one that deleting the other test would silently
	// weaken with nothing turning red. It is local now.
	//
	// The dependency also ran the other way, which is the sharper half. Running
	// TestARigTargetSurvivesRegistration ALONE against CompatibilityID
	// hardcoded to "rig-2026-09" PASSES, because that is the value it expects:
	// a test with one expected value cannot catch a constant equal to it. That
	// test kills the naive mutant ("rig-0000-00") on its own and the exploit
	// mutant not at all, so the second target below is what actually closes the
	// class.
	otherCapabilities := department.Capabilities{
		SupportsPooled:     true,
		SupportsDedicated:  false,
		RequiresWorkspace:  false,
		RequiresCheckpoint: false,
		AdmissionWeight:    1,
		CaptureSafety:      department.CaptureSafetyStreaming,
	}
	if otherCapabilities == declared {
		t.Fatal("the two capability fixtures are equal, so neither is a control for the other")
	}
	other, err := department.NewRigTarget(&testkit.FakeRig{}, "rig-2025-01", otherCapabilities)
	if err != nil {
		t.Fatalf("NewRigTarget: %v", err)
	}
	if got := other.CompatibilityID(); got != "rig-2025-01" {
		t.Errorf("CompatibilityID() = %q, want the second target's declared build", got)
	}
	if got := other.Capabilities(); got != otherCapabilities {
		t.Errorf("Capabilities() = %+v, want the second target's declaration %+v", got, otherCapabilities)
	}
}

// TestARigTargetSurvivesRegistration is the closest this task gets to step 2's
// "before registry publication": a real rig target — not the stub every other
// registry test uses — goes into a Department and is read back out.
//
// It matters beyond coverage. O2.2's registry entry stores the compatibility id
// READ OFF THE TARGET, so the value that reaches the registry is the accessor's
// and not Restore's. Registering a stub proves the registry works; registering
// this proves the thing the registry will actually hold.
func TestARigTargetSurvivesRegistration(t *testing.T) {
	t.Parallel()

	rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
	target := rigTarget(t, rig)

	registry, err := department.New([]department.Registration{{AgentID: "reviewer", Target: target}})
	if err != nil {
		t.Fatalf("New with a rig target: %v", err)
	}
	registered, err := registry.Target("reviewer")
	if err != nil {
		t.Fatalf("Target(reviewer): %v", err)
	}
	if got := registered.CompatibilityID(); got != "rig-2026-09" {
		t.Errorf("the registered target reports build %q, want the one it was constructed with. This is the value O2.2 stores and HostLink advertises", got)
	}
	if got := registered.Capabilities(); got != pooledCapabilities() {
		t.Errorf("the registered target reports %+v, want %+v", got, pooledCapabilities())
	}

	// And it still launches once registered, so registration does not hand back
	// something merely shaped like the target.
	runtime, err := registered.Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create through the registry: %v", err)
	}
	if runtime.SessionID() != "session-71c" {
		t.Errorf("SessionID() = %q through the registry", runtime.SessionID())
	}
}

// ---------------------------------------------------------------------------
// The capabilities reach through
// ---------------------------------------------------------------------------

// TestAdaptedRuntimeForwardsToTheSession asserts against the SESSION for each
// capability, for the same reason the propagation tests do: a Runtime that
// answers WaitIdle itself, or subscribes to its own empty channel, satisfies
// the interface and does nothing.
func TestAdaptedRuntimeForwardsToTheSession(t *testing.T) {
	t.Parallel()

	session := testkit.NewFullSession(rigSessionUUID)
	rig := &testkit.FakeRig{Session: session}
	runtime, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := runtime.ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("ReleaseResidency: %v", err)
	}
	if session.Released() != 1 {
		t.Errorf("the session saw %d releases, want 1", session.Released())
	}

	command := department.RuntimeCommand{
		CommandID:        "command-4b2",
		RuntimeCommandID: uuid.MustParse("3f2a1c40-5d6e-4a7b-8c9d-0e1f2a3b4c5d"),
		Kind:             "input",
		Payload:          []byte(`{"blocks":[]}`),
	}
	if err := runtime.ApplyCommand(t.Context(), command); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	// THE WHOLE VALUE, not the identity. The seam exists to carry the runtime
	// mapping and the private body as well as the public id, and comparing only
	// the id is what let a version of it that carried neither look correct.
	if applied := session.Applied(); len(applied) != 1 || !reflect.DeepEqual(applied[0], command) {
		t.Errorf("the session applied %+v, want the whole command the runtime was given %+v", applied, command)
	}

	if _, err := runtime.SubscribeCommitted(t.Context(), "event-88"); err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}
	if after := session.SubscribedAfter(); !slices.Equal(after, []sessionwire.EventID{"event-88"}) {
		t.Errorf("the session was subscribed from %q, want [event-88]", after)
	}

	// Liveness is the session's channel, not one the adapter made: closing the
	// session's must be observable through the runtime.
	select {
	case <-runtime.Done():
		t.Fatal("Done() reported a stopped runtime before the session stopped")
	default:
	}
	session.Stop()
	select {
	case <-runtime.Done():
	default:
		t.Error("the session stopped and Done() did not report it; the adapter is not forwarding the session's channel")
	}

	// A cancelled context must reach the session rather than be swallowed.
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.WaitIdle(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitIdle with a cancelled context = %v, want context.Canceled", err)
	}
}

// otherRigSessionUUID is a Harness identity no request in this file names.
var otherRigSessionUUID = uuid.MustParse("0b8d4e21-7c3a-4f96-a5e2-91d3c7f60a18")

// TestALaunchUnderAnotherHarnessIdentityIsRefused holds R5 at the product
// seam. department.Rig is implemented OUTSIDE this module, so nothing but this
// check stops a Rig that ignores RigSessionID — launching under a minted id —
// from writing a journal the durable binding does not name: the conversation
// then exists nowhere Host can find it again, and the next placement starts
// over. The refusal releases what the rig launched rather than leaking it.
func TestALaunchUnderAnotherHarnessIdentityIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()
		session := testkit.NewFullSession(otherRigSessionUUID)
		rig := &testkit.FakeRig{Session: session}
		_, err := rigTarget(t, rig).Create(t.Context(), launchRequest())
		var launch *department.RigLaunchError
		if !errors.As(err, &launch) || !errors.Is(err, department.ErrRigSessionIdentity) {
			t.Fatalf("Create under another identity = %v, want a RigLaunchError wrapping ErrRigSessionIdentity", err)
		}
		if session.Released() != 1 {
			t.Errorf("the mis-launched session was released %d times, want 1", session.Released())
		}
	})
	t.Run("restore", func(t *testing.T) {
		t.Parallel()
		session := testkit.NewFullSession(otherRigSessionUUID)
		rig := &testkit.FakeRig{Session: session}
		_, err := rigTarget(t, rig).Restore(t.Context(), restoreRequest("rig-2026-09"))
		if !errors.Is(err, department.ErrRigSessionIdentity) {
			t.Fatalf("Restore answered by another identity = %v, want ErrRigSessionIdentity", err)
		}
		if session.Released() != 1 {
			t.Errorf("the mis-restored session was released %d times, want 1", session.Released())
		}
	})
	t.Run("create with no identity lets the rig mint one", func(t *testing.T) {
		t.Parallel()
		rig := &testkit.FakeRig{Session: testkit.NewFullSession(otherRigSessionUUID)}
		request := launchRequest()
		request.RigSessionID = uuid.UUID{}
		if _, err := rigTarget(t, rig).Create(t.Context(), request); err != nil {
			t.Fatalf("Create with no named identity = %v, want the rig's own id accepted", err)
		}
	})
	t.Run("control: the named identity is accepted", func(t *testing.T) {
		t.Parallel()
		rig := &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}
		if _, err := rigTarget(t, rig).Create(t.Context(), launchRequest()); err != nil {
			t.Fatalf("Create under the named identity = %v", err)
		}
	})
}

// faultingSession is a full session that also offers PersistenceFaults.
type faultingSession struct {
	*testkit.FullSession
	faulted   chan struct{}
	fault     error
	abandoned int
}

func (s *faultingSession) PersistenceFaulted() <-chan struct{} { return s.faulted }
func (s *faultingSession) PersistenceFault() error             { return s.fault }
func (s *faultingSession) AbandonResidency(context.Context) error {
	s.abandoned++
	return nil
}

// TestTheRuntimeForwardsPersistenceFaultsOnlyWhenOffered holds D3's department
// half both ways: offered, the wrapper forwards all three methods; absent, the
// wrapper reports unavailable, never fires, and refuses the abandon — rather
// than advertising a capability that panics.
func TestTheRuntimeForwardsPersistenceFaultsOnlyWhenOffered(t *testing.T) {
	t.Parallel()
	type availability interface{ PersistenceFaultsAvailable() bool }

	t.Run("offered", func(t *testing.T) {
		t.Parallel()
		session := &faultingSession{FullSession: testkit.NewFullSession(rigSessionUUID), faulted: make(chan struct{}), fault: errors.New("append failed")}
		runtime, err := rigTarget(t, &testkit.FakeRig{Session: session}).Create(t.Context(), launchRequest())
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		faults, ok := runtime.(department.PersistenceFaults)
		if !ok || !runtime.(availability).PersistenceFaultsAvailable() {
			t.Fatal("the wrapper does not report the offered capability")
		}
		if faults.PersistenceFaulted() != (<-chan struct{})(session.faulted) || !errors.Is(faults.PersistenceFault(), session.fault) {
			t.Error("the wrapper does not forward the runtime's fault")
		}
		if err := faults.AbandonResidency(t.Context()); err != nil || session.abandoned != 1 {
			t.Errorf("AbandonResidency = %v after %d calls, want nil after 1", err, session.abandoned)
		}
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		runtime, err := rigTarget(t, &testkit.FakeRig{Session: testkit.NewFullSession(rigSessionUUID)}).Create(t.Context(), launchRequest())
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if runtime.(availability).PersistenceFaultsAvailable() {
			t.Fatal("the wrapper reports a capability the runtime does not offer")
		}
		faults := runtime.(department.PersistenceFaults)
		if faults.PersistenceFaulted() != nil || faults.PersistenceFault() != nil {
			t.Error("an unsupervisable runtime reports a fault channel or a fault")
		}
		if err := faults.AbandonResidency(t.Context()); !errors.Is(err, department.ErrNoPersistenceFaults) {
			t.Errorf("AbandonResidency = %v, want ErrNoPersistenceFaults", err)
		}
	})
}
