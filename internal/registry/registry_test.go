package registry_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

// stubTarget is a LaunchTarget that launches nothing. The id makes two
// instances distinguishable, so an identity assertion cannot pass merely
// because two zero-size values compare equal.
type stubTarget struct{ id string }

func (stubTarget) CompatibilityID() department.CompatibilityID { return "rig-2026-09" }

func (stubTarget) Capabilities() department.Capabilities {
	return department.Capabilities{SupportsPooled: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming}
}

func (stubTarget) Create(context.Context, department.CreateRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

func (stubTarget) Restore(context.Context, department.RestoreRequest) (department.Runtime, error) {
	return nil, errors.New("stub")
}

// stubRuntime is a Runtime handle that does nothing. The id distinguishes
// instances for the same reason stubTarget's does.
type stubRuntime struct{ id string }

func (r stubRuntime) SessionID() sessionwire.SessionID { return sessionwire.SessionID(r.id) }
func (stubRuntime) AgentID() sessionwire.AgentID       { return "reviewer" }
func (stubRuntime) WaitIdle(context.Context) error     { return nil }
func (stubRuntime) Done() <-chan struct{}              { return nil }
func (stubRuntime) ReleaseResidency(context.Context) error {
	return nil
}

// LeaseEpoch reports the RUNTIME's journal grant. The base is 1 while
// residency's lease fake mints residency epochs from 1000, so a registry entry
// that confused the two would not merely be wrong, it would be visibly wrong.
func (stubRuntime) LeaseEpoch() (uint64, bool) { return 1, true }

func (stubRuntime) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, nil
}

func (stubRuntime) ApplyCommand(context.Context, department.RuntimeCommand) error { return nil }

// testClock is a manually advanced time source.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the current fake instant.
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// advance moves the fake clock forward.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
}

func admission(id string) registry.Admission {
	return registry.Admission{
		AgentID:         "reviewer",
		Target:          stubTarget{id: "target-" + id},
		CompatibilityID: department.CompatibilityID("rig-" + id),
		Runtime:         stubRuntime{id: "runtime-" + id},
		LeaseEpoch:      7,
	}
}

// ---------------------------------------------------------------------------
// The security property
// ---------------------------------------------------------------------------

// TestTwoTenantsMaySharaASessionID is the reason the key is composite, and it
// CONSTRUCTS THE COLLISION rather than asserting the struct's shape.
//
// SessionIDs are client-supplied opaque strings and Core mints none of them, so
// two tenants naming a session identically is an ordinary event rather than an
// attack. A bare-SessionID index would hand one tenant the other's live Runtime
// handle — which is the O2.1 Auth finding in data-structure form, and asserting
// that Key has two fields would not catch a lookup that ignored one of them.
func TestTwoTenantsMayShareASessionID(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	alice := registry.Key{TenantID: "tenant-alice", SessionID: "session-shared"}
	bob := registry.Key{TenantID: "tenant-bob", SessionID: "session-shared"}

	aliceEntry, wonAlice := index.Insert(alice, admission("alice"))
	bobEntry, wonBob := index.Insert(bob, admission("bob"))
	if !wonAlice || !wonBob {
		t.Fatalf("both tenants must claim their own residency; won = (%v, %v)", wonAlice, wonBob)
	}
	if index.Len() != 2 {
		t.Fatalf("Len() = %d, want 2: one residency per tenant", index.Len())
	}
	if aliceEntry.Generation == bobEntry.Generation {
		t.Error("the two residencies share a generation; they are distinct residencies")
	}

	got, ok := index.Get(alice)
	if !ok {
		t.Fatal("Get(alice) found nothing")
	}
	if got.Runtime != (stubRuntime{id: "runtime-alice"}) {
		t.Errorf("Get(alice).Runtime = %#v, want alice's. A bare-SessionID index hands one tenant the other's live handle", got.Runtime)
	}
	if got.Key.TenantID != "tenant-alice" {
		t.Errorf("Get(alice).Key.TenantID = %q", got.Key.TenantID)
	}

	// Removing one tenant's residency leaves the other's untouched.
	if !index.RemoveByGeneration(alice, aliceEntry.Generation) {
		t.Fatal("RemoveByGeneration(alice) reported no removal")
	}
	if _, ok := index.Get(bob); !ok {
		t.Error("removing alice's residency removed bob's; the key is not separating tenants")
	}
	if _, ok := index.Get(alice); ok {
		t.Error("alice's residency survived its own removal")
	}
}

// productionFiles lists the package's non-test Go sources, so a guard over the
// package cannot be escaped by adding a second file.
func productionFiles(dir string) ([]string, error) {
	listing, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var sources []string
	for _, item := range listing {
		name := item.Name()
		if item.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources = append(sources, filepath.Join(dir, name))
	}
	slices.Sort(sources)
	return sources, nil
}

// TestEveryFieldIsCarried asserts the WHOLE entry, because asserting a couple
// of fields is the same defect as asserting a struct's shape.
//
// Only Runtime and LeaseEpoch round-tripped under assertion before this;
// deleting AgentID, Target and CompatibilityID from Insert's literal left the
// suite green. Three of the ten required fields were carried by nothing but
// hope — in the file whose own commit message disparages asserting a struct's
// shape rather than its behaviour.
func TestEveryFieldIsCarried(t *testing.T) {
	t.Parallel()

	clock := newClock()
	index := registry.New(clock)
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	admitted := admission("a")

	got, won := index.Insert(key, admitted)
	if !won {
		t.Fatal("Insert did not establish the residency")
	}
	if got.Generation == 0 {
		t.Error("Generation is zero; the registry assigns it")
	}

	want := registry.Entry{
		Key:             key,
		AgentID:         admitted.AgentID,
		Target:          admitted.Target,
		CompatibilityID: admitted.CompatibilityID,
		Runtime:         admitted.Runtime,
		LeaseEpoch:      admitted.LeaseEpoch,
		Generation:      got.Generation,
		State:           registry.StateResident,
		Accepting:       true,
		LastActivity:    clock.Now(),
		TeardownOwned:   false,
	}
	if got != want {
		t.Errorf("Insert returned %+v, want %+v", got, want)
	}
	if held, _ := index.Get(key); held != want {
		t.Errorf("Get returned %+v, want %+v", held, want)
	}

	// The fixture-default control: a field whose expected value is the zero
	// value proves nothing, so every carried field must be non-zero here.
	for name, zero := range map[string]bool{
		"AgentID":         want.AgentID == "",
		"Target":          want.Target == nil,
		"CompatibilityID": want.CompatibilityID == "",
		"Runtime":         want.Runtime == nil,
		"LeaseEpoch":      want.LeaseEpoch == 0,
		"TenantID":        want.Key.TenantID == "",
		"SessionID":       want.Key.SessionID == "",
		"LastActivity":    want.LastActivity.IsZero(),
	} {
		if zero {
			t.Errorf("fixture field %s is zero-valued; an assertion on it proves nothing", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Step 2, as an assertion rather than a sentence
// ---------------------------------------------------------------------------

// TestRegistryNeverGatesOnTheLeaseEpoch holds the negative half of the claim
// the package comment makes: this index never DECIDES anything from a lease
// epoch.
//
// The rule is a WHITELIST OF POSITIONS, not a blacklist of branch shapes, and
// that is the correction of a real defect. The first version flagged
// BinaryExpr, IfStmt.Cond and SwitchStmt.Tag — which caught
// `if epochRejected(entry.LeaseEpoch)`, and was defeated by the most ordinary
// refactor in Go:
//
//	rejected := epochRejected(entry.LeaseEpoch)
//	if rejected { ... }
//
// An assignment is none of those three, so the guard was a speed bump wearing a
// rule's name. Enumerating the shapes a decision can take is unbounded;
// enumerating the places the field may legitimately APPEAR is not. It may be
// declared, and it may be carried across in a composite literal. Any third
// reference is reported without this test needing to understand what it
// computes.
//
// I previously disclosed the opposite limit — that a helper taking the field
// would escape — and that was wrong in both directions: a helper call inside a
// condition WAS caught, and the hoisted assignment was not. A stated limit is
// read as the boundary of what is known, so getting it backwards was worse than
// saying nothing.
//
// IT PARSES THE WHOLE PACKAGE, not registry.go. A file-scoped rule ends the
// first time this package gets a second file: an epoch.go holding
// `func epochRejected(e *Entry) bool { return e.LeaseEpoch == 999 }`, called
// from registry.go, passed. That is not an awkward edit, it is the next
// ordinary one — and it was inconsistent with the return-type guard in the same
// commit, which moved from file-scoped parsing to package-scoped reflection for
// exactly this reason.
//
// WHAT IT DOES NOT SEE, stated no wider than the probes that demonstrate it.
// The rule is over syntactic occurrences of the identifier in this package's
// production files, so two things are outside it. A WHOLE-STRUCT COMPARISON of
// a type containing the field is not seen — build an Entry in a whitelisted
// carry position and then compare it with `candidate != *existing`, and the
// outcome depends on the lease epoch while the identifier appears only where
// the whitelist blesses it. And a decision made in a DIFFERENT PACKAGE, from a
// value this one handed out, is outside it by construction; that half is the
// caller-side rule the package comment says explicitly is not enforced here.
//
// Both are more awkward than the hoisted assignment that defeated the previous
// version, and neither is impossible. This is the third time this disclosure
// has been written and the second time it was too wide: the failure mode is not
// a claim with no probe — each of these had one — it is a claim BROADER than
// the probe that demonstrates it.
func TestRegistryNeverGatesOnTheLeaseEpoch(t *testing.T) {
	t.Parallel()

	fileSet := token.NewFileSet()
	sources, err := productionFiles(".")
	if err != nil {
		t.Fatalf("enumerate package files: %v", err)
	}
	if len(sources) == 0 {
		t.Fatal("no production file was found; this guard would be vacuous")
	}
	files := make([]*ast.File, 0, len(sources))
	for _, source := range sources {
		parsed, err := parser.ParseFile(fileSet, source, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}
		files = append(files, parsed)
	}

	// Pass one: every position where naming the field is legitimate.
	permitted := map[token.Pos]bool{}
	for _, parsed := range files {
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.StructType:
				for _, field := range node.Fields.List {
					for _, name := range field.Names {
						if name.Name == "LeaseEpoch" {
							permitted[name.Pos()] = true
						}
					}
				}
			case *ast.KeyValueExpr:
				key, ok := node.Key.(*ast.Ident)
				if !ok || key.Name != "LeaseEpoch" {
					return true
				}
				// The key and the whole carried value: `LeaseEpoch: x.LeaseEpoch`.
				ast.Inspect(node, func(inner ast.Node) bool {
					switch inner := inner.(type) {
					case *ast.Ident:
						permitted[inner.Pos()] = true
					case *ast.SelectorExpr:
						permitted[inner.Sel.Pos()] = true
					}
					return true
				})
			}
			return true
		})
	}

	// Pass two: every occurrence must be one of them.
	occurrences := 0
	for _, parsed := range files {
		ast.Inspect(parsed, func(node ast.Node) bool {
			var pos token.Pos
			switch node := node.(type) {
			case *ast.SelectorExpr:
				if node.Sel.Name != "LeaseEpoch" {
					return true
				}
				pos = node.Sel.Pos()
			case *ast.Ident:
				if node.Name != "LeaseEpoch" {
					return true
				}
				pos = node.Pos()
			default:
				return true
			}
			occurrences++
			if !permitted[pos] {
				t.Errorf("%s names LeaseEpoch outside a declaration or a carry. The registry may CARRY a lease epoch so a caller can compare it, and may never decide anything from one: a caller that trusted this index would be getting an authorization answer from a cache", fileSet.Position(pos))
			}
			return true
		})
	}

	// Floored twice: the field must be seen at all, and at least one position
	// must have been whitelisted. Either being zero means a rename has made
	// this guard vacuous while it still passes.
	if occurrences == 0 {
		t.Fatal("the package never mentions LeaseEpoch; this guard is asserting nothing. If the field was renamed, rename it here too")
	}
	if len(permitted) == 0 {
		t.Fatal("no legitimate LeaseEpoch position was recognised; the whitelist matched nothing and every occurrence would be reported")
	}
}

// TestAStaleLeaseEpochChangesNothing is the behavioural half: an entry whose
// carried epoch disagrees with anything is still stored, still found and still
// removable, because the registry does not adjudicate leases.
func TestAStaleLeaseEpochChangesNothing(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}

	stale := admission("stale")
	stale.LeaseEpoch = 0
	entry, won := index.Insert(key, stale)
	if !won {
		t.Fatal("an entry with a zero lease epoch was refused; the registry is adjudicating a lease")
	}
	if entry.LeaseEpoch != 0 {
		t.Errorf("LeaseEpoch = %d, want the value it was given", entry.LeaseEpoch)
	}
	if _, ok := index.Get(key); !ok {
		t.Error("an entry with a zero lease epoch is not retrievable")
	}
	if !index.RemoveByGeneration(key, entry.Generation) {
		t.Error("an entry with a zero lease epoch could not be removed")
	}

	// A very large epoch is equally uninteresting to the registry.
	future := admission("future")
	future.LeaseEpoch = ^uint64(0)
	if _, won := index.Insert(key, future); !won {
		t.Error("an entry with a maximal lease epoch was refused")
	}
}

// ---------------------------------------------------------------------------
// One winner
// ---------------------------------------------------------------------------

// TestInsertHasExactlyOneWinner is a race by construction, so it is written as
// one rather than as a sequence.
func TestInsertHasExactlyOneWinner(t *testing.T) {
	t.Parallel()

	const racers = 32
	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}

	var (
		start   = make(chan struct{})
		wins    atomicCounter
		wg      sync.WaitGroup
		entries = make([]registry.Entry, racers)
		won     = make([]bool, racers)
	)
	wg.Add(racers)
	for i := range racers {
		go func() {
			defer wg.Done()
			<-start
			entries[i], won[i] = index.Insert(key, admission(strconv.Itoa(i)))
			if won[i] {
				wins.add()
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := wins.get(); got != 1 {
		t.Fatalf("%d callers claimed residency, want exactly 1", got)
	}
	if index.Len() != 1 {
		t.Errorf("Len() = %d, want 1", index.Len())
	}

	// Every loser saw the SAME residency the winner established, so a loser can
	// act on it rather than having to re-read.
	held, ok := index.Get(key)
	if !ok {
		t.Fatal("Get found nothing after a contested insert")
	}
	for i := range racers {
		if entries[i].Generation != held.Generation {
			t.Fatalf("racer %d saw generation %d, want the held %d: every caller must be told the residency that actually exists", i, entries[i].Generation, held.Generation)
		}
	}
}

// TestRemoveByGenerationHasExactlyOneWinner is the mirror race.
func TestRemoveByGenerationHasExactlyOneWinner(t *testing.T) {
	t.Parallel()

	const racers = 32
	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))

	var (
		start = make(chan struct{})
		wins  atomicCounter
		wg    sync.WaitGroup
	)
	wg.Add(racers)
	for range racers {
		go func() {
			defer wg.Done()
			<-start
			if index.RemoveByGeneration(key, entry.Generation) {
				wins.add()
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := wins.get(); got != 1 {
		t.Fatalf("%d callers removed the residency, want exactly 1", got)
	}
	if index.Len() != 0 {
		t.Errorf("Len() = %d, want 0", index.Len())
	}
}

// TestBeginTeardownHasExactlyOneOwner holds the single-owner teardown state.
func TestBeginTeardownHasExactlyOneOwner(t *testing.T) {
	t.Parallel()

	const racers = 32
	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))

	var (
		start = make(chan struct{})
		wins  atomicCounter
		wg    sync.WaitGroup
	)
	wg.Add(racers)
	for range racers {
		go func() {
			defer wg.Done()
			<-start
			if _, owned := index.BeginTeardown(key, entry.Generation); owned {
				wins.add()
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := wins.get(); got != 1 {
		t.Fatalf("%d callers claimed teardown, want exactly 1", got)
	}
	held, ok := index.Get(key)
	if !ok {
		t.Fatal("the residency vanished during teardown claiming")
	}
	if !held.TeardownOwned {
		t.Error("TeardownOwned is false after a caller claimed it")
	}
	if held.State != registry.StateDraining {
		t.Errorf("State = %q, want %q", held.State, registry.StateDraining)
	}
}

// TestTeardownOwnershipSurvivesAStateChange is what makes ownership a recorded
// fact rather than an inference from the state.
//
// The doc claims BeginTeardown must not read ownership off StateDraining
// because state is re-enterable and ownership is once-only — and nothing
// constructed the sequence that shows it. Replacing `if entry.TeardownOwned`
// with `if entry.State == StateDraining` passed everything, one MarkReleasing
// call away from a second caller being handed an ownership it does not have.
//
// The property is not about that particular ordering being sensible. It is that
// ownership must survive ANY subsequent state change, because a fact recorded
// once cannot be recovered from a field that keeps moving.
func TestTeardownOwnershipSurvivesAStateChange(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))

	if _, owned := index.BeginTeardown(key, entry.Generation); !owned {
		t.Fatal("the first caller did not win teardown")
	}

	// Any subsequent state change. The registry does not order these, and it
	// must not lose the ownership fact when one moves the state away from
	// draining.
	released, ok := index.MarkReleasing(key, entry.Generation)
	if !ok {
		t.Fatal("MarkReleasing failed after teardown began")
	}
	if released.State != registry.StateReleasing {
		t.Fatalf("State = %q, want %q: this test needs the state to have moved", released.State, registry.StateReleasing)
	}
	if !released.TeardownOwned {
		t.Error("TeardownOwned was cleared by a state change; ownership is once-only and must not be recoverable from the state")
	}

	if _, owned := index.BeginTeardown(key, entry.Generation); owned {
		t.Error("a second caller was handed teardown ownership after the state moved away from draining. Ownership is a fact, not an inference")
	}
}

// atomicCounter is a mutex counter, so the race detector has something real to
// check rather than a plain int.
type atomicCounter struct {
	mu sync.Mutex
	n  int
}

// add increments the counter.
func (c *atomicCounter) add() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

// get reads the counter.
func (c *atomicCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

// TestGenerationSeparatesSuccessiveResidencies is why removal is keyed by it: a
// late caller holding an old generation must not remove the residency that
// replaced it.
func TestGenerationSeparatesSuccessiveResidencies(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}

	first, _ := index.Insert(key, admission("first"))
	if !index.RemoveByGeneration(key, first.Generation) {
		t.Fatal("the first residency could not be removed")
	}
	second, won := index.Insert(key, admission("second"))
	if !won {
		t.Fatal("the second residency was refused after the first was removed")
	}
	if second.Generation == first.Generation {
		t.Fatalf("both residencies have generation %d; a stale caller could not tell them apart", first.Generation)
	}

	// The stale caller acts, and must change nothing.
	if index.RemoveByGeneration(key, first.Generation) {
		t.Error("a stale generation removed the residency that replaced it")
	}
	if _, ok := index.Get(key); !ok {
		t.Fatal("the second residency was removed by a stale caller")
	}
	if _, ok := index.MarkReleasing(key, first.Generation); ok {
		t.Error("a stale generation marked the replacement residency releasing")
	}
	if _, ok := index.BeginTeardown(key, first.Generation); ok {
		t.Error("a stale generation claimed teardown of the replacement residency")
	}
	if index.Touch(key, first.Generation) {
		t.Error("a stale generation touched the replacement residency")
	}

	// A FABRICATED FUTURE generation must be refused too, and the check must be
	// an inequality rather than a comparison. `entry.Generation > generation`
	// in place of `!=` passes every row above — the tests only ever present a
	// LOWER generation — while accepting any number a caller invents.
	future := second.Generation + 1000
	if index.RemoveByGeneration(key, future) {
		t.Error("a fabricated future generation removed the residency")
	}
	if _, ok := index.MarkReleasing(key, future); ok {
		t.Error("a fabricated future generation marked the residency releasing")
	}
	if _, ok := index.BeginTeardown(key, future); ok {
		t.Error("a fabricated future generation claimed teardown")
	}
	if index.Touch(key, future) {
		t.Error("a fabricated future generation touched the residency")
	}

	// The control: the CURRENT generation still works, so the rejections above
	// are about staleness rather than about everything being refused.
	if _, ok := index.MarkReleasing(key, second.Generation); !ok {
		t.Error("the current generation could not mark releasing")
	}
	if !index.Touch(key, second.Generation) {
		t.Error("the current generation could not touch")
	}
	if !index.RemoveByGeneration(key, second.Generation) {
		t.Error("the current generation could not remove")
	}
}

// ---------------------------------------------------------------------------
// Releasing and staleness
// ---------------------------------------------------------------------------

func TestMarkReleasingStopsAcceptance(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))
	if !entry.Accepting || entry.State != registry.StateResident {
		t.Fatalf("a fresh residency is (%v, %q), want accepting and resident", entry.Accepting, entry.State)
	}

	released, ok := index.MarkReleasing(key, entry.Generation)
	if !ok {
		t.Fatal("MarkReleasing reported no residency")
	}
	if released.Accepting {
		t.Error("a releasing residency is still accepting work")
	}
	if released.State != registry.StateReleasing {
		t.Errorf("State = %q, want %q", released.State, registry.StateReleasing)
	}
	// The stored row changed, not just the returned copy.
	if held, _ := index.Get(key); held.Accepting || held.State != registry.StateReleasing {
		t.Errorf("the stored entry is (%v, %q); MarkReleasing changed only its return value", held.Accepting, held.State)
	}
}

// TestRemoveStaleLeavesResidentSessionsAlone is the cleanup rule, and its
// control is the half that matters: a stale RESIDENT session is still doing
// work, and reaping it would be the registry deciding something it has no
// standing to decide.
func TestRemoveStaleLeavesResidentSessionsAlone(t *testing.T) {
	t.Parallel()

	clock := newClock()
	index := registry.New(clock)
	resident := registry.Key{TenantID: "tenant-9f3", SessionID: "session-resident"}
	releasing := registry.Key{TenantID: "tenant-9f3", SessionID: "session-releasing"}
	draining := registry.Key{TenantID: "tenant-9f3", SessionID: "session-draining"}
	fresh := registry.Key{TenantID: "tenant-9f3", SessionID: "session-fresh"}

	residentEntry, _ := index.Insert(resident, admission("resident"))
	releasingEntry, _ := index.Insert(releasing, admission("releasing"))
	if _, ok := index.MarkReleasing(releasing, releasingEntry.Generation); !ok {
		t.Fatal("MarkReleasing failed")
	}
	drainingEntry, _ := index.Insert(draining, admission("draining"))
	if _, ok := index.BeginTeardown(draining, drainingEntry.Generation); !ok {
		t.Fatal("BeginTeardown failed")
	}

	clock.advance(time.Hour)
	index.Insert(fresh, admission("fresh"))
	cutoff := clock.Now().Add(-time.Minute)

	removed := index.RemoveStale(cutoff)
	if !slices.Contains(removed, draining) {
		t.Errorf("RemoveStale removed %v, want the stale DRAINING residency too. Testing one non-resident state leaves the other's sweep unheld: `State != StateReleasing` in place of `State == StateResident` passes with only a releasing row", removed)
	}
	if !slices.Contains(removed, releasing) {
		t.Errorf("RemoveStale removed %v, want the stale releasing residency", removed)
	}
	if slices.Contains(removed, resident) {
		t.Error("RemoveStale reaped a RESIDENT session; a session still holding work is not the registry's to reap")
	}
	if slices.Contains(removed, fresh) {
		t.Error("RemoveStale reaped a session active after the cutoff")
	}
	if _, ok := index.Get(resident); !ok {
		t.Error("the resident session was removed")
	}
	_ = residentEntry
}

// TestRemoveStaleTreatsTheCutoffAsExclusive pins the tie-break, which nothing
// did: an entry last active EXACTLY at the cutoff is not stale.
//
// The boundary is arbitrary in the sense that either answer could have been
// chosen, and load-bearing in the sense that a caller passing clock.Now() as
// the cutoff must not sweep a residency touched in the same instant. Unpinned,
// `Before` could become `!After` and nothing would notice.
func TestRemoveStaleTreatsTheCutoffAsExclusive(t *testing.T) {
	t.Parallel()

	clock := newClock()
	index := registry.New(clock)
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))
	index.MarkReleasing(key, entry.Generation)
	at := clock.Now()

	if removed := index.RemoveStale(at); len(removed) != 0 {
		t.Errorf("RemoveStale(lastActivity) removed %v; an entry active exactly at the cutoff is not yet stale", removed)
	}
	if removed := index.RemoveStale(at.Add(time.Nanosecond)); !slices.Contains(removed, key) {
		t.Errorf("RemoveStale(lastActivity+1ns) removed %v, want the residency", removed)
	}
}

// TestTouchDefersStaleness pins that activity is what staleness measures.
func TestTouchDefersStaleness(t *testing.T) {
	t.Parallel()

	clock := newClock()
	index := registry.New(clock)
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))
	index.MarkReleasing(key, entry.Generation)

	clock.advance(time.Hour)
	if !index.Touch(key, entry.Generation) {
		t.Fatal("Touch reported no residency")
	}
	if removed := index.RemoveStale(clock.Now().Add(-time.Minute)); len(removed) != 0 {
		t.Errorf("RemoveStale removed %v after a Touch at the current instant", removed)
	}

	clock.advance(time.Hour)
	if removed := index.RemoveStale(clock.Now().Add(-time.Minute)); !slices.Contains(removed, key) {
		t.Errorf("RemoveStale removed %v, want the residency untouched for an hour", removed)
	}
}

// ---------------------------------------------------------------------------
// No mutable map
// ---------------------------------------------------------------------------

// TestSnapshotIsACopy is O1.1's Department problem again: a caller must not be
// handed anything it can write back into the index.
func TestSnapshotIsACopy(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	for i := range 4 {
		index.Insert(registry.Key{TenantID: "tenant-9f3", SessionID: sessionwire.SessionID("session-" + strconv.Itoa(i))}, admission(strconv.Itoa(i)))
	}

	first := index.Snapshot()
	if len(first) != 4 {
		t.Fatalf("Snapshot() returned %d entries, want 4", len(first))
	}
	for i := range first {
		first[i].State = "clobbered"
		first[i].Accepting = false
		first[i].Generation = 999
	}
	first = append(first, registry.Entry{Key: registry.Key{SessionID: "appended"}})
	_ = first

	second := index.Snapshot()
	if len(second) != 4 {
		t.Fatalf("Snapshot() returned %d entries after mutating an earlier one, want 4", len(second))
	}
	for _, entry := range second {
		if entry.State != registry.StateResident || !entry.Accepting || entry.Generation == 999 {
			t.Fatalf("a mutated snapshot reached the registry: %+v", entry)
		}
	}

	// Mutating a Get result likewise changes nothing.
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-0"}
	got, _ := index.Get(key)
	got.State = "clobbered"
	if held, _ := index.Get(key); held.State != registry.StateResident {
		t.Errorf("mutating a Get result changed the stored entry to %q", held.State)
	}
}

// permittedSignatureTypes is the FINITE set of types an exported Registry
// method may mention, in either position.
//
// It is a whitelist because the previous version was a blacklist, and a
// blacklist of the ways a leak can be spelled is unbounded. Four spellings
// walked through it, none exotic:
//
//   - All() func(func(Key, *Entry) bool) — a range-over-func iterator yielding
//     live rows, which is the most likely way this package actually grows and a
//     WORSE leak than returning a slice, because it holds the mutex open across
//     caller code. Kind Func fell to the default arm.
//   - Each(fn func(*Entry)) — live rows through a PARAMETER. The guard walked
//     results only, so the one construct step 3 explicitly blesses, a callback,
//     was the one position it never looked at.
//   - Debug() any — Kind Interface, default arm.
//   - View() Live, where Live has an unexported map field and an exported
//     accessor returning it.
//
// "Reflection answers what the type IS, so there is no spelling to miss" was
// false as written. Reflection removed the SPELLING axis and left the POSITION
// axis and the KIND axis untouched. Enumerating the legitimate set removes all
// three at once — which is the rule I wrote for the epoch guard in the same
// commit and did not apply here.
//
// Adding a legitimate callback means adding its exact type to this list. That
// is the point: a reviewer then sees whether it takes Entry or *Entry.
func permittedSignatureTypes() map[reflect.Type]bool {
	return map[reflect.Type]bool{
		reflect.TypeFor[registry.Entry]():     true,
		reflect.TypeFor[[]registry.Entry]():   true,
		reflect.TypeFor[[]registry.Key]():     true,
		reflect.TypeFor[registry.Key]():       true,
		reflect.TypeFor[registry.Admission](): true,
		reflect.TypeFor[uint64]():             true,
		reflect.TypeFor[bool]():               true,
		reflect.TypeFor[int]():                true,
		reflect.TypeFor[time.Time]():          true,
	}
}

// TestExportedMethodsMentionOnlyPermittedTypes is step 3 as a rule over the
// whole signature: "return leases and handles through narrow callbacks or
// snapshots; do not expose the mutable map."
func TestExportedMethodsMentionOnlyPermittedTypes(t *testing.T) {
	t.Parallel()

	permitted := permittedSignatureTypes()
	registryType := reflect.TypeFor[*registry.Registry]()

	checked := 0
	for i := range registryType.NumMethod() {
		method := registryType.Method(i)
		checked++
		// Parameter 0 is the receiver on a method obtained from the type.
		for p := 1; p < method.Type.NumIn(); p++ {
			if !permitted[method.Type.In(p)] {
				t.Errorf("Registry.%s takes %s, which is not in the permitted set. A live row reaches a caller through a parameter as easily as through a return — a callback is the construct step 3 blesses and the one a results-only rule never inspects", method.Name, method.Type.In(p))
			}
		}
		for r := range method.Type.NumOut() {
			if !permitted[method.Type.Out(r)] {
				t.Errorf("Registry.%s returns %s, which is not in the permitted set. Add it here deliberately if it is legitimate, so a reviewer sees whether it carries Entry or *Entry", method.Name, method.Type.Out(r))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no exported Registry method was inspected; this guard is asserting nothing")
	}

	// The guard must REPORT every shape that defeated its predecessor, or it
	// would pass for want of understanding rather than for want of a defect.
	for _, banned := range []struct {
		name string
		typ  reflect.Type
	}{
		{"map[Key]*Entry", reflect.TypeFor[map[registry.Key]*registry.Entry]()},
		{"[]*Entry", reflect.TypeFor[[]*registry.Entry]()},
		{"chan Entry", reflect.TypeFor[chan registry.Entry]()},
		{"*Entry", reflect.TypeFor[*registry.Entry]()},
		{"named map type", reflect.TypeFor[namedView]()},
		{"struct with an accessor to the live map", reflect.TypeFor[accessorView]()},
		{"range-over-func iterator", reflect.TypeFor[func(func(registry.Key, *registry.Entry) bool)]()},
		{"callback taking a live row", reflect.TypeFor[func(*registry.Entry)]()},
		{"any", reflect.TypeFor[any]()},
	} {
		if permitted[banned.typ] {
			t.Errorf("the permitted set contains %s; that is a leak spelled as a permission", banned.name)
		}
	}
	// And it must permit what the registry legitimately uses, or it would be
	// satisfied by permitting nothing.
	for _, allowed := range []reflect.Type{
		reflect.TypeFor[registry.Entry](),
		reflect.TypeFor[[]registry.Entry](),
		reflect.TypeFor[[]registry.Key](),
		reflect.TypeFor[bool](),
	} {
		if !permitted[allowed] {
			t.Errorf("the permitted set omits %s, which the registry legitimately uses", allowed)
		}
	}
}

// namedView and accessorView are two of the spellings that defeated the
// blacklist, kept so the whitelist is tested against them.
type namedView map[registry.Key]*registry.Entry

type accessorView struct {
	rows map[registry.Key]*registry.Entry
}

// Rows hands out the live map through a method rather than a field.
func (v accessorView) Rows() map[registry.Key]*registry.Entry { return v.rows }

// TestPermittedTypesDoNotThemselvesExposeState is the second half, and it is
// needed because the whitelist above permits Entry BY NAME: if Entry later
// gained a map field, every method returning one would still pass.
//
// SCOPE LIMIT, stated as a limit rather than as a principle. This walks
// EXPORTED FIELDS. An unexported field is unreachable BY NAME from outside the
// package, which is why walking it would only reproduce time.Time's unexported
// *Location as a false positive — but it is NOT unreachable in general, because
// any exported method of the type can return it, and the author of such a leak
// is also the author of the accessor. A leak behind an accessor method on a
// permitted type is therefore OUT OF SCOPE here; the whitelist above is what
// stops such a type reaching a signature at all.
func TestPermittedTypesDoNotThemselvesExposeState(t *testing.T) {
	t.Parallel()

	for typ := range permittedSignatureTypes() {
		for _, complaint := range exposesInternalState(typ, typ.String(), map[reflect.Type]bool{}) {
			t.Error(complaint)
		}
	}

	// THE REGISTRY'S OWN EXPORTED SURFACE. A method is not the only way out: an
	// exported FIELD on Registry aliasing r.entries, assigned in New, is never
	// inspected by a rule over signatures, and that shape survived all three
	// versions of this guard. Registry has no exported fields today, so this
	// loop reports nothing until someone adds one — which is the point, and why
	// it is not floored on a count that would have to be zero. The walk it uses
	// is exercised against positive cases below instead.
	registryStruct := reflect.TypeFor[registry.Registry]()
	for i := range registryStruct.NumField() {
		field := registryStruct.Field(i)
		if !field.IsExported() {
			continue
		}
		for _, complaint := range exposesInternalState(field.Type, "Registry."+field.Name, map[reflect.Type]bool{}) {
			t.Error(complaint)
		}
	}

	// Bidirectional, as before: the walk must see the shapes it bans and must
	// not flag the shapes the registry uses.
	for _, banned := range []struct {
		name string
		typ  reflect.Type
	}{
		{"map[Key]*Entry", reflect.TypeFor[map[registry.Key]*registry.Entry]()},
		{"[]*Entry", reflect.TypeFor[[]*registry.Entry]()},
		{"chan Entry", reflect.TypeFor[chan registry.Entry]()},
		{"struct with an exported map field", reflect.TypeFor[wrappedView]()},
	} {
		if complaints := exposesInternalState(banned.typ, "probe", map[reflect.Type]bool{}); len(complaints) == 0 {
			t.Errorf("the walk does not recognise %s as exposing internal state", banned.name)
		}
	}
	for _, allowed := range []reflect.Type{
		reflect.TypeFor[registry.Entry](),
		reflect.TypeFor[[]registry.Entry](),
		reflect.TypeFor[time.Time](),
	} {
		if complaints := exposesInternalState(allowed, "probe", map[reflect.Type]bool{}); len(complaints) != 0 {
			t.Errorf("the walk flags %s, which the registry legitimately carries: %v", allowed, complaints)
		}
	}
}

type wrappedView struct {
	Rows map[registry.Key]*registry.Entry
}

// A package-level FUNCTION taking a *Registry and returning the live map —
// `func Entries(r *Registry) map[Key]*Entry` — is out of scope for every guard
// here, and deliberately so. Nothing inspects package-level functions, and this
// package sits under internal/, so such a function is reachable only from this
// module and only by whoever wrote it. Recorded as an edge rather than closed:
// enumerating every package-level function's signature costs more than it buys
// behind that boundary. If this package ever leaves internal/, close it.

// exposesInternalState reports every way typ hands out mutable shared state
// through its EXPORTED surface. See the scope limit above.
func exposesInternalState(typ reflect.Type, method string, seen map[reflect.Type]bool) []string {
	if seen[typ] {
		return nil
	}
	seen[typ] = true

	switch typ.Kind() {
	case reflect.Map:
		return []string{method + " exposes " + typ.String() + ", a map. A caller handed the live index can write to it"}
	case reflect.Chan:
		return []string{method + " exposes " + typ.String() + ", a channel, which is another way to hand out mutable shared state"}
	case reflect.Pointer:
		return []string{method + " exposes " + typ.String() + ", a pointer that aliases a registry row; carry a copy"}
	case reflect.Slice, reflect.Array:
		return exposesInternalState(typ.Elem(), method, seen)
	case reflect.Struct:
		var complaints []string
		for i := range typ.NumField() {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			complaints = append(complaints, exposesInternalState(field.Type, method, seen)...)
		}
		return complaints
	default:
		return nil
	}
}

// TestSnapshotIsDeterministicallyOrdered pins that callers see a stable
// sequence rather than map iteration order, with enough entries that agreement
// by luck is not the explanation.
func TestSnapshotIsDeterministicallyOrdered(t *testing.T) {
	t.Parallel()

	const count = 32
	index := registry.New(newClock())
	for i := count - 1; i >= 0; i-- {
		tenant := sessionwire.TenantID("tenant-" + strconv.Itoa(100+i%4))
		session := sessionwire.SessionID("session-" + strconv.Itoa(100+i))
		index.Insert(registry.Key{TenantID: tenant, SessionID: session}, admission(strconv.Itoa(i)))
	}

	want := index.Snapshot()
	if len(want) != count {
		t.Fatalf("Snapshot() returned %d entries, want %d", len(want), count)
	}
	if !slices.IsSortedFunc(want, func(a, b registry.Entry) int {
		if a.Key.TenantID != b.Key.TenantID {
			if a.Key.TenantID < b.Key.TenantID {
				return -1
			}
			return 1
		}
		if a.Key.SessionID < b.Key.SessionID {
			return -1
		}
		if a.Key.SessionID > b.Key.SessionID {
			return 1
		}
		return 0
	}) {
		t.Errorf("Snapshot() is not ordered by (TenantID, SessionID): %v", keysOf(want))
	}
	for attempt := range 32 {
		if got := index.Snapshot(); !slices.Equal(keysOf(got), keysOf(want)) {
			t.Fatalf("Snapshot() differed on attempt %d: %v then %v", attempt, keysOf(want), keysOf(got))
		}
	}
}

func keysOf(entries []registry.Entry) []registry.Key {
	keys := make([]registry.Key, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	return keys
}

// ---------------------------------------------------------------------------
// Absent keys
// ---------------------------------------------------------------------------

func TestOperationsOnAnAbsentKeyReportNothing(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-absent"}

	if _, ok := index.Get(key); ok {
		t.Error("Get found an absent key")
	}
	if _, ok := index.MarkReleasing(key, 1); ok {
		t.Error("MarkReleasing succeeded on an absent key")
	}
	if _, ok := index.BeginTeardown(key, 1); ok {
		t.Error("BeginTeardown succeeded on an absent key")
	}
	if index.Touch(key, 1) {
		t.Error("Touch succeeded on an absent key")
	}
	if index.RemoveByGeneration(key, 1) {
		t.Error("RemoveByGeneration succeeded on an absent key")
	}
	if index.Len() != 0 {
		t.Errorf("Len() = %d, want 0", index.Len())
	}
	if snapshot := index.Snapshot(); len(snapshot) != 0 {
		t.Errorf("Snapshot() = %v, want empty", snapshot)
	}
}

// TestStopAdmittingLeavesTheResidencyResident is the row O6.1 makes reachable,
// and the reason it is a separate writer from MarkReleasing.
//
// Until this method existed, Accepting was a STRICT FUNCTION of State — Insert
// set {resident, true}, MarkReleasing and BeginTeardown both set false — so
// {resident, accepting:false} could not be produced through the public API and
// a reader consulting both fields would have one decide every reachable case
// and the other decide none. Warm release stops admission for a session BEFORE
// it marks it releasing, precisely so no command can be accepted into a
// residency whose durable `releasing` observation has not been written yet, and
// that intermediate state is this one. The assertion is therefore on the pair,
// not on Accepting alone: a StopAdmitting that also moved State would collapse
// back into MarkReleasing and buy nothing.
func TestStopAdmittingLeavesTheResidencyResident(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	entry, _ := index.Insert(key, admission("a"))
	if !entry.Accepting || entry.State != registry.StateResident {
		t.Fatalf("a fresh residency is (%v, %q), want accepting and resident", entry.Accepting, entry.State)
	}

	stopped, ok := index.StopAdmitting(key, entry.Generation)
	if !ok {
		t.Fatal("StopAdmitting reported no residency")
	}
	if stopped.Accepting {
		t.Error("the residency is still accepting after StopAdmitting")
	}
	if stopped.State != registry.StateResident {
		t.Errorf("State = %q, want %q: stopping admission is not a residency transition", stopped.State, registry.StateResident)
	}
	// The stored row changed, not just the returned copy.
	held, _ := index.Get(key)
	if held.Accepting || held.State != registry.StateResident {
		t.Errorf("the stored entry is (%v, %q); StopAdmitting changed only its return value or moved the state", held.Accepting, held.State)
	}
	// Idempotent, and a later MarkReleasing still moves the state.
	if again, ok := index.StopAdmitting(key, entry.Generation); !ok || again.Accepting {
		t.Errorf("a repeated StopAdmitting reported (%v, %v), want (accepting=false, ok=true)", again.Accepting, ok)
	}
	released, ok := index.MarkReleasing(key, entry.Generation)
	if !ok || released.State != registry.StateReleasing || released.Accepting {
		t.Errorf("MarkReleasing after StopAdmitting = (%q, %v, %v), want (releasing, false, true)", released.State, released.Accepting, ok)
	}
}

// TestStopAdmittingRefusesAReplacedResidency holds the generation rule every
// other mutating method takes, because a late warm timer holding a generation
// that has been replaced must not close admission on its REPLACEMENT.
func TestStopAdmittingRefusesAReplacedResidency(t *testing.T) {
	t.Parallel()

	index := registry.New(newClock())
	key := registry.Key{TenantID: "tenant-9f3", SessionID: "session-71c"}
	first, _ := index.Insert(key, admission("a"))
	if !index.RemoveByGeneration(key, first.Generation) {
		t.Fatal("the first residency could not be removed")
	}
	second, _ := index.Insert(key, admission("b"))

	if _, ok := index.StopAdmitting(key, first.Generation); ok {
		t.Error("StopAdmitting acted under a generation that has been replaced")
	}
	if held, _ := index.Get(key); !held.Accepting {
		t.Error("the replacement residency stopped accepting on the previous generation's timer")
	}
	if _, ok := index.StopAdmitting(key, second.Generation); !ok {
		t.Error("StopAdmitting refused the current generation")
	}
	if _, ok := index.StopAdmitting(registry.Key{TenantID: "tenant-9f3", SessionID: "absent"}, 1); ok {
		t.Error("StopAdmitting succeeded on an absent key")
	}
}
