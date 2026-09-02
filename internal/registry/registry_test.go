package registry_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
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

func (stubRuntime) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, nil
}

func (stubRuntime) ApplyCommand(context.Context, sessionwire.CommandEnvelope) error { return nil }

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

// ---------------------------------------------------------------------------
// Step 2, as an assertion rather than a sentence
// ---------------------------------------------------------------------------

// TestRegistryNeverGatesOnTheLeaseEpoch holds the claim the package comment
// makes: the registry is an OPTIMIZATION ONLY, and every mutating caller
// validates the durable lease epoch separately.
//
// That sentence is exactly the shape this repository treats as a defect when
// nothing checks it — a claim in prose a future reader will quote. What it
// means concretely is that the registry may CARRY a lease epoch so a caller can
// compare it, and may never BRANCH on one: the moment it does, a caller that
// trusted the index has been handed an authorization answer by a cache.
//
// The structural half is the load-bearing one, because the behavioural half
// below can only show that today's code does not gate; it cannot stop tomorrow's
// from starting to.
func TestRegistryNeverGatesOnTheLeaseEpoch(t *testing.T) {
	t.Parallel()

	parsed, err := parser.ParseFile(token.NewFileSet(), "registry.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse registry.go: %v", err)
	}

	mentions := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.BinaryExpr:
			for _, operand := range []ast.Expr{node.X, node.Y} {
				if namesLeaseEpoch(operand) {
					t.Errorf("registry.go compares LeaseEpoch (%s). The registry may carry a lease epoch and may never branch on one: a caller that trusted this index would be getting an authorization answer from a cache", node.Op)
				}
			}
		case *ast.SwitchStmt:
			if node.Tag != nil && namesLeaseEpoch(node.Tag) {
				t.Error("registry.go switches on LeaseEpoch")
			}
		case *ast.IfStmt:
			if node.Cond != nil && namesLeaseEpoch(node.Cond) {
				t.Error("registry.go branches on LeaseEpoch")
			}
		case *ast.SelectorExpr:
			if node.Sel.Name == "LeaseEpoch" {
				mentions++
			}
		}
		return true
	})

	// Floored: the walk must have SEEN the field, or a rename would make this
	// guard silently vacuous while still passing.
	if mentions == 0 {
		t.Fatal("registry.go never mentions LeaseEpoch; this guard is asserting nothing. If the field was renamed, rename it here too")
	}
}

func namesLeaseEpoch(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "LeaseEpoch" {
			found = true
		}
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "LeaseEpoch" {
			found = true
		}
		return true
	})
	return found
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
	fresh := registry.Key{TenantID: "tenant-9f3", SessionID: "session-fresh"}

	residentEntry, _ := index.Insert(resident, admission("resident"))
	releasingEntry, _ := index.Insert(releasing, admission("releasing"))
	if _, ok := index.MarkReleasing(releasing, releasingEntry.Generation); !ok {
		t.Fatal("MarkReleasing failed")
	}

	clock.advance(time.Hour)
	index.Insert(fresh, admission("fresh"))
	cutoff := clock.Now().Add(-time.Minute)

	removed := index.RemoveStale(cutoff)
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

// TestNoExportedMethodHandsOutInternalState is step 3 as a structural rule
// rather than a sentence: "return leases and handles through narrow callbacks
// or snapshots; do not expose the mutable map."
//
// TestSnapshotIsACopy closes REPLACEMENT — Snapshot returning the live rows
// dies there. It does not close ADDITION, and a probe confirmed it: bolting an
// Entries() method returning map[Key]*Entry onto the Registry passes every
// behavioural test in this file, because no test calls a method that does not
// exist yet. That is the same addition-shaped residual O2.1 left open on its
// delegation guard, and here it is cheap to close.
//
// The rule is over RETURN TYPES, which is where the leak can only be: an
// exported method may not return a map, a channel, or a pointer into the
// registry's own rows. Copies and narrow values are what remain.
func TestNoExportedMethodHandsOutInternalState(t *testing.T) {
	t.Parallel()

	parsed, err := parser.ParseFile(token.NewFileSet(), "registry.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse registry.go: %v", err)
	}

	checked := 0
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || !function.Name.IsExported() {
			continue
		}
		if !receiverIsRegistry(function.Recv) {
			continue
		}
		checked++
		if function.Type.Results == nil {
			continue
		}
		for _, result := range function.Type.Results.List {
			switch result.Type.(type) {
			case *ast.MapType:
				t.Errorf("Registry.%s returns a map. A caller handed the live index can write to it; return a snapshot or take a callback", function.Name.Name)
			case *ast.ChanType:
				t.Errorf("Registry.%s returns a channel, which is another way to hand out mutable shared state", function.Name.Name)
			case *ast.StarExpr:
				t.Errorf("Registry.%s returns a pointer, which aliases a registry row; return a copy", function.Name.Name)
			}
		}
	}

	// Floored: with no methods found the loop above asserts nothing, which is
	// how this guard would go quiet after a receiver rename.
	if checked == 0 {
		t.Fatal("no exported Registry method was inspected; this guard is asserting nothing")
	}
}

func receiverIsRegistry(fields *ast.FieldList) bool {
	if len(fields.List) != 1 {
		return false
	}
	switch receiver := fields.List[0].Type.(type) {
	case *ast.StarExpr:
		ident, ok := receiver.X.(*ast.Ident)
		return ok && ident.Name == "Registry"
	case *ast.Ident:
		return receiver.Name == "Registry"
	}
	return false
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
