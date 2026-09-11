package hostlink_test

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Drain fixture
// ---------------------------------------------------------------------------

const (
	testDrainGeneration = uint64(3)
	testIdempotencyKey  = "factory-drain-attempt-1"
)

// stubDrains is one Host's whole drain state machine, narrowed to the two
// methods HostLink may reach.
//
// IT COUNTS BEGINS SEPARATELY FROM CALLS, which is the distinction every
// idempotency assertion below rests on: a state machine that answers a retry
// with the same generation while quietly restarting itself would satisfy a
// call count and would still be the bug.
//
// IT IS AUDITED IN BOTH DIRECTIONS, not only for being loose. Looser than the
// real machine would let a handler defect through; TIGHTER would let this
// package rely on a promise production does not make. The audit is
// TestTheDrainFakeMatchesTheProductionMachineInBothDirections in
// internal/lifecycle, which is the only package that may see both — this one
// cannot import lifecycle without a cycle, and the interface assertions that
// pin the production type to these seams live there for the same reason.
type stubDrains struct {
	mu sync.Mutex

	begins     int
	starts     int
	observes   int
	scopes     []hostlink.DrainScope
	begun      bool
	drainState sessionwire.HostLinkDrainState
	generation uint64
	startErr   error

	// entered counts entries to StartDrain WITHOUT taking the mutex, so a test
	// can synchronise on the call having arrived even when the call is holding
	// something. A predicate that needed this fake's own lock deadlocked
	// against a blocked StartDrain and turned an assertion into a ten-minute
	// hang, which is not a kill.
	entered atomic.Int64

	// block makes StartDrain GENUINELY BLOCK until it is released.
	//
	// IT EXISTS BECAUSE A FAKE THAT CANNOT BE SLOW CANNOT EXPOSE A HANDLER THAT
	// SERIALIZES. The six-property audit in internal/lifecycle compares
	// BEHAVIOUR, not blocking, so the timing axis was invisible to it — and the
	// production machine did once block its own status observation for the
	// whole of a publication. The stub is the only place this package can put
	// that axis, and the lock is released before blocking on purpose: a fake
	// that blocked while holding its own mutex would fail the test below for
	// the FAKE's reason rather than the handler's.
	block chan struct{}
}

func newStubDrains() *stubDrains {
	return &stubDrains{drainState: sessionwire.HostLinkDrainStateDraining, generation: testDrainGeneration}
}

func (d *stubDrains) StartDrain(scope hostlink.DrainScope) (hostlink.DrainStatus, error) {
	d.entered.Add(1)
	d.mu.Lock()
	d.starts++
	d.scopes = append(d.scopes, scope)
	if d.startErr != nil {
		err := d.startErr
		d.mu.Unlock()
		return hostlink.DrainStatus{}, err
	}
	block := d.block
	d.mu.Unlock()
	if block != nil {
		<-block
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.begun {
		d.begun = true
		d.begins++
	}
	return d.status(), nil
}

func (d *stubDrains) ObserveDrain(scope hostlink.DrainScope) (hostlink.DrainStatus, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.observes++
	d.scopes = append(d.scopes, scope)
	if !d.begun {
		return hostlink.DrainStatus{}, false
	}
	return d.status(), true
}

// status is the answer both methods give, so a divergence between the two is
// not representable in the fake and cannot be mistaken for one in production.
func (d *stubDrains) status() hostlink.DrainStatus {
	return hostlink.DrainStatus{Generation: d.generation, State: d.drainState}
}

func (d *stubDrains) reachDrained() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.drainState = sessionwire.HostLinkDrainStateDrained
}

func (d *stubDrains) counts() (begins, starts, observes int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.begins, d.starts, d.observes
}

func (d *stubDrains) scopesSeen() []hostlink.DrainScope {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]hostlink.DrainScope(nil), d.scopes...)
}

type drainFixture struct {
	drains *stubDrains
	mux    *hostlink.Multiplexer
}

// newDrainFixture builds a POOLED Host with both drain seams supplied.
func newDrainFixture(t *testing.T) *drainFixture { return newDrainFixtureFor(t, "") }

// newDrainFixtureFor builds a Host holding the given fixed session; the empty
// session is a pooled Host.
func newDrainFixtureFor(t *testing.T, fixed sessionwire.SessionID) *drainFixture {
	t.Helper()
	drains := newStubDrains()
	mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID:           testTenant,
		HostID:             testHostID,
		HostGeneration:     testGeneration,
		Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
		Admission:          &stubAdmission{},
		Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
		DrainStarter:       drains,
		DrainObserver:      drains,
		FixedSessionID:     fixed,
		MaxBindingsPerLink: 2,
		MaxBindings:        3,
	})
	if err != nil {
		t.Fatalf("NewMultiplexer: %v", err)
	}
	return &drainFixture{drains: drains, mux: mux}
}

// drainRequest is the whole-Host request a placement controller sends.
func drainRequest() sessionwire.HostLinkDrainRequest {
	return sessionwire.HostLinkDrainRequest{
		Version:        sessionwire.CurrentWireVersion,
		HostID:         testHostID,
		HostGeneration: testGeneration,
		IdempotencyKey: testIdempotencyKey,
	}
}

// sessionDrainRequest is the dedicated-Host request naming one fixed session.
func sessionDrainRequest(session sessionwire.SessionID) sessionwire.HostLinkDrainRequest {
	request := drainRequest()
	request.TenantID = testTenant
	request.SessionID = session
	return request
}

func mustStartDrain(t *testing.T, mux *hostlink.Multiplexer, request sessionwire.HostLinkDrainRequest) sessionwire.HostLinkDrainObservation {
	t.Helper()
	observation, err := mux.StartDrain(request)
	if err != nil {
		t.Fatalf("StartDrain(%+v): %v", request, err)
	}
	return observation
}

func refusedDrain(t *testing.T, err error) *hostlink.BindError {
	t.Helper()
	if err == nil {
		t.Fatal("the drain was accepted, want a refusal")
	}
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("error %v is not a *hostlink.BindError", err)
	}
	return refusal
}

// ---------------------------------------------------------------------------
// Step 2: the acknowledgement is initiation, not completion
// ---------------------------------------------------------------------------

// TestDrainAcknowledgesInitiationAndNotCompletion is O5.4's central claim.
//
// The acknowledgement carries the drain GENERATION and whatever state the
// machine reports at this instant, and the handler neither waits for release
// nor infers completion from having started one. The two halves are asserted
// against the same machine so that a handler which cached "drained" after the
// first call would fail the first half and one which reported "drained" on
// initiation would fail the second.
func TestDrainAcknowledgesInitiationAndNotCompletion(t *testing.T) {
	t.Parallel()
	f := newDrainFixture(t)

	acknowledged := mustStartDrain(t, f.mux, drainRequest())
	if acknowledged.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("acknowledged state = %q, want %q", acknowledged.State, sessionwire.HostLinkDrainStateDraining)
	}
	if acknowledged.DrainGeneration != testDrainGeneration {
		t.Fatalf("acknowledged drain generation = %d, want %d", acknowledged.DrainGeneration, testDrainGeneration)
	}
	if acknowledged.HostID != testHostID || acknowledged.HostGeneration != testGeneration {
		t.Fatalf("acknowledgement names %q/%d, want %q/%d", acknowledged.HostID, acknowledged.HostGeneration, testHostID, testGeneration)
	}
	if acknowledged.TenantID != "" || acknowledged.SessionID != "" {
		t.Fatalf("a whole-Host acknowledgement carried a scope: %+v", acknowledged)
	}

	// The SAME machine, now finished. The handler reports the new state
	// because it re-asks, which is the property a cache would break.
	f.drains.reachDrained()
	later := mustStartDrain(t, f.mux, drainRequest())
	if later.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("state after the machine finished = %q, want %q", later.State, sessionwire.HostLinkDrainStateDrained)
	}
	if later.DrainGeneration != acknowledged.DrainGeneration {
		t.Fatalf("generation moved from %d to %d across the drain", acknowledged.DrainGeneration, later.DrainGeneration)
	}
}

// ---------------------------------------------------------------------------
// Step 3: the retry space, derived from the mechanism
// ---------------------------------------------------------------------------

// TestEveryDrainRequestIsAnsweredWithTheSameGenerationAndBeginsOnce is step 3's
// for-all, and the space it quantifies over is DERIVED rather than taken from
// the seven cases the runbook names.
//
// The mechanism is this. An accepted drain is a function of exactly three
// things: the resolved scope, the state machine's (begun, state) pair, and
// nothing else — this package keeps NO per-link, per-request and per-key
// memory, so there is no fourth axis for a retry to differ on. That collapses
// the space to a product of two:
//
//   - the MACHINE axis, which is total in `begun`: not begun, begun and
//     draining, begun and drained. There is no fourth value, and
//     StartDrain is a total function of it.
//   - the REQUEST axis, which is everything a caller may legitimately vary
//     while still addressing this Host: the idempotency key, the scope
//     (whole-Host or this Host's fixed session), the link the request arrives
//     on, and the number of requests in flight at once.
//
// The runbook's seven cases are INSTANCES of that product — repeated is
// (begun, same request), already drained is (drained, same request), reconnect
// after an ambiguous reply is (begun, new link), simultaneous is the concurrent
// entry — and the subtests below cover their neighbours too, which is where
// this program keeps finding the defect: a different idempotency key, a
// different scope on the same Host, and a first request that arrives after the
// machine already finished.
//
// The two cases NOT in this product are refusals and are elsewhere: a stale
// Host generation and a wrong fixed SessionID never reach the machine at all,
// so they belong to the refusal ladder, and a machine that cannot start is
// TestADrainTheStateMachineRefusesIsNotAcknowledged.
func TestEveryDrainRequestIsAnsweredWithTheSameGenerationAndBeginsOnce(t *testing.T) {
	t.Parallel()

	// vary is the REQUEST axis: each mutates a request in a way a caller may
	// legitimately produce, and none of them may move the generation.
	vary := []struct {
		name    string
		request func(sessionwire.HostLinkDrainRequest) sessionwire.HostLinkDrainRequest
	}{
		{"the identical retry", func(r sessionwire.HostLinkDrainRequest) sessionwire.HostLinkDrainRequest { return r }},
		{"a different idempotency key", func(r sessionwire.HostLinkDrainRequest) sessionwire.HostLinkDrainRequest {
			r.IdempotencyKey = "a-second-placement-controller"
			return r
		}},
		{"the fixed session's own scope", func(r sessionwire.HostLinkDrainRequest) sessionwire.HostLinkDrainRequest {
			return sessionDrainRequest(testSession)
		}},
	}

	// at is the MACHINE axis. Each positions the machine before the retries.
	at := []struct {
		name  string
		reach func(*drainFixture)
		state sessionwire.HostLinkDrainState
	}{
		{"while it is still draining", func(*drainFixture) {}, sessionwire.HostLinkDrainStateDraining},
		{"after it has drained", func(f *drainFixture) { f.drains.reachDrained() }, sessionwire.HostLinkDrainStateDrained},
	}

	for _, machine := range at {
		for _, request := range vary {
			t.Run(machine.name+", "+request.name, func(t *testing.T) {
				t.Parallel()
				// A DEDICATED Host, so that the fixed-session scope is a
				// legitimate variation rather than a refusal.
				f := newDrainFixtureFor(t, testSession)
				first := mustStartDrain(t, f.mux, drainRequest())
				machine.reach(f)

				retried := mustStartDrain(t, f.mux, request.request(drainRequest()))
				if retried.DrainGeneration != first.DrainGeneration {
					t.Fatalf("retry generation = %d, want %d", retried.DrainGeneration, first.DrainGeneration)
				}
				if retried.State != machine.state {
					t.Fatalf("retry state = %q, want %q", retried.State, machine.state)
				}

				// And the OBSERVATION agrees with the acknowledgement, which
				// is what lets a Factory poll one and act on the other.
				observed, err := f.mux.ObserveDrain(request.request(drainRequest()))
				if err != nil {
					t.Fatalf("ObserveDrain: %v", err)
				}
				if observed.DrainGeneration != first.DrainGeneration || observed.State != machine.state {
					t.Fatalf("observation = %+v, want generation %d state %q", observed, first.DrainGeneration, machine.state)
				}

				if begins, _, _ := f.drains.counts(); begins != 1 {
					t.Fatalf("the machine began %d drains, want exactly one", begins)
				}
			})
		}
	}

	// The FIRST request arriving after the machine already finished — the
	// neighbour of "already drained" that is not a retry at all, because this
	// link never sent the first one.
	t.Run("a first request after the machine already drained", func(t *testing.T) {
		t.Parallel()
		f := newDrainFixtureFor(t, testSession)
		mustStartDrain(t, f.mux, drainRequest())
		f.drains.reachDrained()

		// A different Multiplexer over the SAME machine is the reconnect that
		// inherits nothing: a new link, a new routing table, one Host.
		reconnected, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
			TenantID:           testTenant,
			HostID:             testHostID,
			HostGeneration:     testGeneration,
			Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
			Admission:          &stubAdmission{},
			Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
			DrainStarter:       f.drains,
			DrainObserver:      f.drains,
			FixedSessionID:     testSession,
			MaxBindingsPerLink: 2,
			MaxBindings:        3,
		})
		if err != nil {
			t.Fatalf("NewMultiplexer: %v", err)
		}
		observation := mustStartDrain(t, reconnected, drainRequest())
		if observation.DrainGeneration != testDrainGeneration {
			t.Fatalf("generation after reconnect = %d, want %d", observation.DrainGeneration, testDrainGeneration)
		}
		if observation.State != sessionwire.HostLinkDrainStateDrained {
			t.Fatalf("state after reconnect = %q, want %q", observation.State, sessionwire.HostLinkDrainStateDrained)
		}
		if begins, _, _ := f.drains.counts(); begins != 1 {
			t.Fatalf("the machine began %d drains across the reconnect, want one", begins)
		}
	})

	// SIMULTANEOUS requests. The handler holds no lock of its own, so this is
	// an assertion about the seam being consulted concurrently and every
	// answer agreeing — the axis a sequential retry cannot reach.
	t.Run("many at once", func(t *testing.T) {
		t.Parallel()
		f := newDrainFixtureFor(t, testSession)
		const callers = 24
		generations := make([]uint64, callers)
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(callers)
		for index := range callers {
			go func() {
				defer done.Done()
				start.Wait()
				request := drainRequest()
				if index%2 == 0 {
					request = sessionDrainRequest(testSession)
				}
				observation, err := f.mux.StartDrain(request)
				if err != nil {
					t.Errorf("concurrent StartDrain: %v", err)
					return
				}
				generations[index] = observation.DrainGeneration
			}()
		}
		start.Done()
		done.Wait()
		for index, generation := range generations {
			if generation != testDrainGeneration {
				t.Fatalf("caller %d saw generation %d, want %d", index, generation, testDrainGeneration)
			}
		}
		if begins, starts, _ := f.drains.counts(); begins != 1 || starts != callers {
			t.Fatalf("%d concurrent requests produced begins=%d starts=%d, want 1 and %d", callers, begins, starts, callers)
		}
	})
}

// TestTheIdempotencyKeyIsNeverDeduplicatedOn states the mechanism the retry
// space above rests on, so that it is a property of the code rather than a
// reading of it.
//
// Core calls the member "a retry-stable initiation key", and a Host COULD have
// keyed a cache on it. This one does not, for the reason Binding.IdempotencyKey
// already gives on the bind path: replaying a cached acceptance would make the
// answer a statement about the past. A drain's answer is a statement about the
// machine NOW, which is strictly stronger than key-scoped idempotency: EVERY
// call gets the same generation, not merely every call sharing a key —
// and the assertion is that the key never reaches the state machine at all.
func TestTheIdempotencyKeyIsNeverDeduplicatedOn(t *testing.T) {
	t.Parallel()
	f := newDrainFixtureFor(t, testSession)

	first := mustStartDrain(t, f.mux, drainRequest())
	differing := drainRequest()
	differing.IdempotencyKey = "an-entirely-different-attempt"
	second := mustStartDrain(t, f.mux, differing)
	if second.DrainGeneration != first.DrainGeneration {
		t.Fatalf("a differing idempotency key moved the generation from %d to %d", first.DrainGeneration, second.DrainGeneration)
	}

	// The key is not on the resolved scope, so the machine cannot branch on it
	// even if it wanted to. DrainScope's whole field set is asserted by
	// TestDrainSeamsExposeExactlyOneMethodEach's sibling below.
	for _, scope := range f.drains.scopesSeen() {
		value := reflect.ValueOf(scope)
		for field := range value.NumField() {
			if strings.Contains(strings.ToLower(value.Type().Field(field).Name), "idempotency") {
				t.Fatalf("DrainScope carries %q, so the state machine can key on a retry", value.Type().Field(field).Name)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Steps 1 and 3: the refusal ladder
// ---------------------------------------------------------------------------

// TestTheDrainRefusalLadderAnswersWithItsFirstFailingCheck is the refusal half
// of the derived space, and it is derived the same way.
//
// drainScope is an ORDERED SEQUENCE OF TOTAL PREDICATES, so the outcome space
// is exactly: for each predicate, the requests whose FIRST failure is that one,
// plus the requests that pass them all. Enumerating the predicates therefore
// enumerates the space, and each row below fails its own predicate AND every
// predicate after it — which is what makes the assertion "this check fired
// first" rather than "this check fired".
//
// The ordering is not cosmetic. Host identity is decided before scope, so any
// call routed from a stale placement record learns nothing about which
// session this Host holds; the tenant is decided before the fixed session, so a
// foreign tenant cannot probe for it either. A row that fails several
// predicates and is answered by the LAST one is that disclosure.
func TestTheDrainRefusalLadderAnswersWithItsFirstFailingCheck(t *testing.T) {
	t.Parallel()

	// Every row is invalid at its own rung and at every rung below it.
	for _, test := range []struct {
		name    string
		fixed   sessionwire.SessionID
		request func() sessionwire.HostLinkDrainRequest
		refusal hostlink.Refusal
		reason  string
		wire    sessionwire.HostLinkErrorCode
	}{
		{
			name:  "1. a record Core refuses",
			fixed: testSession,
			request: func() sessionwire.HostLinkDrainRequest {
				request := sessionDrainRequest(otherSession)
				request.HostID = "host-beta"
				request.HostGeneration = testGeneration + 1
				request.TenantID = "tenant-other"
				request.Version = 0
				return request
			},
			refusal: hostlink.RefusalMalformedRequest,
			reason:  "not a valid Core record",
		},
		{
			name:  "2. another Host",
			fixed: testSession,
			request: func() sessionwire.HostLinkDrainRequest {
				request := sessionDrainRequest(otherSession)
				request.HostID = "host-beta"
				request.HostGeneration = testGeneration + 1
				request.TenantID = "tenant-other"
				return request
			},
			refusal: hostlink.RefusalForeignHost,
			reason:  "addressed to another Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:  "3. an earlier incarnation of this Host",
			fixed: testSession,
			request: func() sessionwire.HostLinkDrainRequest {
				request := sessionDrainRequest(otherSession)
				request.HostGeneration = testGeneration - 1
				request.TenantID = "tenant-other"
				return request
			},
			refusal: hostlink.RefusalStaleHostGeneration,
			reason:  "earlier incarnation",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			// The NEIGHBOUR of "stale". A generation ahead of this process's
			// is the same repair and must not be mistaken for a match.
			name:  "3b. a later incarnation of this Host",
			fixed: testSession,
			request: func() sessionwire.HostLinkDrainRequest {
				request := sessionDrainRequest(otherSession)
				request.HostGeneration = testGeneration + 1
				request.TenantID = "tenant-other"
				return request
			},
			refusal: hostlink.RefusalStaleHostGeneration,
			reason:  "earlier incarnation",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:  "5. another tenant",
			fixed: testSession,
			request: func() sessionwire.HostLinkDrainRequest {
				request := sessionDrainRequest(otherSession)
				request.TenantID = "tenant-other"
				return request
			},
			refusal: hostlink.RefusalForeignTenant,
			reason:  "authenticated for another tenant",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "6. a session scope on a pooled Host",
			fixed:   "",
			request: func() sessionwire.HostLinkDrainRequest { return sessionDrainRequest(testSession) },
			refusal: hostlink.RefusalWrongDrainScope,
			reason:  "holds no fixed session",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
		{
			name:    "7. the wrong fixed session",
			fixed:   testSession,
			request: func() sessionwire.HostLinkDrainRequest { return sessionDrainRequest(otherSession) },
			refusal: hostlink.RefusalWrongDrainScope,
			reason:  "fixed session is not the one",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newDrainFixtureFor(t, test.fixed)

			// Both methods take the same ladder. A refusal reachable through
			// one and not the other is a status RPC that discloses what the
			// initiating one refuses to.
			for method, call := range map[string]func(sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error){
				hostlink.MethodDrain:       f.mux.StartDrain,
				hostlink.MethodDrainStatus: f.mux.ObserveDrain,
			} {
				_, err := call(test.request())
				refusal := refusedDrain(t, err)
				if refusal.Refusal != test.refusal {
					t.Fatalf("%s: refusal = %q, want %q (%s)", method, refusal.Refusal, test.refusal, refusal.Reason)
				}
				if !strings.Contains(refusal.Reason, test.reason) {
					t.Fatalf("%s: reason %q does not name %q", method, refusal.Reason, test.reason)
				}
				wire, published := refusal.HostLinkError()
				switch {
				case test.wire == "" && published:
					t.Fatalf("%s: a malformed request published the Core class %q", method, wire.Code)
				case test.wire != "" && !published:
					t.Fatalf("%s: refusal %q published no Core class, want %q", method, refusal.Refusal, test.wire)
				case test.wire != "" && wire.Code != test.wire:
					t.Fatalf("%s: Core class = %q, want %q", method, wire.Code, test.wire)
				}
			}

			// The other half of every row: a refused request is refused
			// BEFORE the state machine, so a probe costs a caller nothing and
			// tells it nothing.
			if begins, starts, observes := f.drains.counts(); begins != 0 || starts != 0 || observes != 0 {
				t.Fatalf("a refused request reached the machine: begins=%d starts=%d observes=%d", begins, starts, observes)
			}
		})
	}
}

// TestDrainScopeReachesTheStateMachineExactlyAsRequested proves the two scopes
// are distinguishable where they must be: a dedicated Host's fixed session
// resolves to a populated Key and a whole-Host drain to the zero one.
//
// Both are legitimate on a dedicated Host — draining that Host and draining its
// one session are the same work — which is why the whole-Host row is run
// against a dedicated fixture rather than only a pooled one.
func TestDrainScopeReachesTheStateMachineExactlyAsRequested(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		fixed   sessionwire.SessionID
		request sessionwire.HostLinkDrainRequest
		want    hostlink.DrainScope
	}{
		{"a pooled Host drains whole", "", drainRequest(), hostlink.DrainScope{}},
		{"a dedicated Host drains whole", testSession, drainRequest(), hostlink.DrainScope{}},
		{
			"a dedicated Host drains its session",
			testSession,
			sessionDrainRequest(testSession),
			hostlink.DrainScope{Key: registry.Key{TenantID: testTenant, SessionID: testSession}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newDrainFixtureFor(t, test.fixed)
			observation := mustStartDrain(t, f.mux, test.request)

			scopes := f.drains.scopesSeen()
			if len(scopes) != 1 {
				t.Fatalf("the machine saw %d scopes, want one", len(scopes))
			}
			if scopes[0] != test.want {
				t.Fatalf("scope = %+v, want %+v", scopes[0], test.want)
			}
			if scopes[0].WholeHost() != (test.want.Key == registry.Key{}) {
				t.Fatalf("WholeHost() = %v for scope %+v", scopes[0].WholeHost(), scopes[0])
			}

			// The scope round-trips onto the acknowledgement, which is how a
			// Factory correlates the answer with the request it sent.
			if observation.TenantID != test.want.Key.TenantID || observation.SessionID != test.want.Key.SessionID {
				t.Fatalf("acknowledgement scope = %q/%q, want %q/%q",
					observation.TenantID, observation.SessionID, test.want.Key.TenantID, test.want.Key.SessionID)
			}
		})
	}
}

// TestADrainTheStateMachineRefusesIsNotAcknowledged is the process-shutdown
// case from step 3, and the two refusals it distinguishes.
//
// A Host composed WITHOUT a state machine can never drain gracefully and needs
// a redeploy; a machine that could not start is an ordinary running-Host
// condition — a process already shutting down is the canonical one — and needs
// a retry or a different Host. One value for both would make each an equally
// good explanation of the other.
func TestADrainTheStateMachineRefusesIsNotAcknowledged(t *testing.T) {
	t.Parallel()

	t.Run("the machine refuses to start", func(t *testing.T) {
		t.Parallel()
		f := newDrainFixture(t)
		shuttingDown := errors.New("the host process is already terminating")
		f.drains.startErr = shuttingDown

		_, err := f.mux.StartDrain(drainRequest())
		refusal := refusedDrain(t, err)
		if refusal.Refusal != hostlink.RefusalDrainUnavailable {
			t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalDrainUnavailable)
		}
		if !errors.Is(err, shuttingDown) {
			t.Fatalf("the machine's own cause was lost: %v", err)
		}
		if wire, published := refusal.HostLinkError(); !published || wire.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
			t.Fatalf("Core class = %+v/%v, want runtime_unavailable", wire, published)
		}
	})

	t.Run("the Host was composed without one", func(t *testing.T) {
		t.Parallel()
		mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
			TenantID:           testTenant,
			HostID:             testHostID,
			HostGeneration:     testGeneration,
			Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
			Admission:          &stubAdmission{},
			Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
			MaxBindingsPerLink: 2,
			MaxBindings:        3,
		})
		if err != nil {
			t.Fatalf("NewMultiplexer: %v", err)
		}
		for name, call := range map[string]func(sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error){
			hostlink.MethodDrain:       mux.StartDrain,
			hostlink.MethodDrainStatus: mux.ObserveDrain,
		} {
			_, err := call(drainRequest())
			refusal := refusedDrain(t, err)
			if refusal.Refusal != hostlink.RefusalDrainUnsupported {
				t.Fatalf("%s: refusal = %q, want %q", name, refusal.Refusal, hostlink.RefusalDrainUnsupported)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Step 4: the bounded status observation
// ---------------------------------------------------------------------------

// TestObservingADrainNeverStartsOne is step 4's property and the reason the two
// methods are separate.
//
// A Factory polling a Host whose drain request never arrived must not read
// "no drain here" as "the drain finished" and delete the workload, so the
// absent drain is a REFUSAL and not a drained-looking observation.
func TestObservingADrainNeverStartsOne(t *testing.T) {
	t.Parallel()
	f := newDrainFixture(t)

	_, err := f.mux.ObserveDrain(drainRequest())
	refusal := refusedDrain(t, err)
	if refusal.Refusal != hostlink.RefusalNoDrainInProgress {
		t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalNoDrainInProgress)
	}
	if begins, starts, observes := f.drains.counts(); begins != 0 || starts != 0 || observes != 1 {
		t.Fatalf("observation counts: begins=%d starts=%d observes=%d, want 0/0/1", begins, starts, observes)
	}

	// And once a drain exists the same call reports it, so the refusal above
	// is about the drain's absence and not about the method being inert.
	mustStartDrain(t, f.mux, drainRequest())
	observed, err := f.mux.ObserveDrain(drainRequest())
	if err != nil {
		t.Fatalf("ObserveDrain after the drain began: %v", err)
	}
	if observed.State != sessionwire.HostLinkDrainStateDraining || observed.DrainGeneration != testDrainGeneration {
		t.Fatalf("observation = %+v, want draining at %d", observed, testDrainGeneration)
	}
	if begins, _, _ := f.drains.counts(); begins != 1 {
		t.Fatalf("observation began %d drains, want the one StartDrain began", begins)
	}
}

// TestAnUnpublishableDrainObservationIsRefusedRatherThanSent guards the seam
// this package cannot control.
//
// It is NOT a defensive spelling of the marshaller's own check. An injected
// state machine may hand this package a zero generation or a state outside
// Core's two; marshalling such a value fails at the transport with a
// bad-request a Factory would read as ITS OWN fault. It is refused here as this
// Host's condition instead.
func TestAnUnpublishableDrainObservationIsRefusedRatherThanSent(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		corrupt func(*stubDrains)
	}{
		{"a zero drain generation", func(d *stubDrains) { d.generation = 0 }},
		{"a state outside Core's two", func(d *stubDrains) { d.drainState = "half-drained" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newDrainFixture(t)
			test.corrupt(f.drains)

			_, err := f.mux.StartDrain(drainRequest())
			refusal := refusedDrain(t, err)
			if refusal.Refusal != hostlink.RefusalDrainUnavailable {
				t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalDrainUnavailable)
			}
			if !strings.Contains(refusal.Error(), "Core refuses") {
				t.Fatalf("the refusal does not name the unpublishable observation: %v", refusal)
			}
		})
	}
}

// TestADrainAcknowledgementCannotBeReadAsARefusal is the reply contract for the
// one RPC that returns a body on BOTH outcomes.
//
// Bind and unbind rely on "empty means accepted", which drain cannot use
// because an acknowledgement carries a generation. What keeps the two apart is
// Core's own strict decoding: HostLinkDrainObservation and HostLinkError each
// require a member the other refuses as unknown. That is a property of Core's
// decoders, so this package must not add a member to either record to "help" —
// and the assertion runs in BOTH directions for that reason.
func TestADrainAcknowledgementCannotBeReadAsARefusal(t *testing.T) {
	t.Parallel()
	f := newDrainFixture(t)

	acknowledged, err := json.Marshal(mustStartDrain(t, f.mux, drainRequest()))
	if err != nil {
		t.Fatalf("marshal acknowledgement: %v", err)
	}
	var asRefusal sessionwire.HostLinkError
	if err := json.Unmarshal(acknowledged, &asRefusal); err == nil {
		t.Fatalf("the acknowledgement %s decoded as the refusal %#v", acknowledged, asRefusal)
	}

	_, refusalErr := f.mux.ObserveDrain(func() sessionwire.HostLinkDrainRequest {
		request := drainRequest()
		request.HostID = "host-beta"
		return request
	}())
	wire, published := refusedDrain(t, refusalErr).HostLinkError()
	if !published {
		t.Fatal("the refusal published no Core class")
	}
	refused, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal refusal: %v", err)
	}
	var asObservation sessionwire.HostLinkDrainObservation
	if err := json.Unmarshal(refused, &asObservation); err == nil {
		t.Fatalf("the refusal %s decoded as the acknowledgement %#v", refused, asObservation)
	}
}

// ---------------------------------------------------------------------------
// Over the wire
// ---------------------------------------------------------------------------

// TestDrainRPCsSpeakCoreRecordsOverTheLink drives the whole path a placement
// controller takes, over a real authenticated connection.
func TestDrainRPCsSpeakCoreRecordsOverTheLink(t *testing.T) {
	f := newDrainFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}

	// Status BEFORE any drain, and it is first ON PURPOSE: a drain_status
	// wired to the initiating handler would begin one here, and every later
	// assertion in this test would still pass.
	early := refusedRPC(t, rpc(t, connection, 2, hostlink.MethodDrainStatus, drainRequest()))
	if early.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("status before any drain = %#v, want a refusal", early)
	}
	if begins, starts, _ := f.drains.counts(); begins != 0 || starts != 0 {
		t.Fatalf("a status RPC began a drain: begins=%d starts=%d", begins, starts)
	}

	acknowledged := drainObservationOverTheLink(t, rpc(t, connection, 3, hostlink.MethodDrain, drainRequest()))
	if acknowledged.State != sessionwire.HostLinkDrainStateDraining || acknowledged.DrainGeneration != testDrainGeneration {
		t.Fatalf("acknowledgement = %+v, want draining at generation %d", acknowledged, testDrainGeneration)
	}

	retried := drainObservationOverTheLink(t, rpc(t, connection, 4, hostlink.MethodDrain, drainRequest()))
	if retried.DrainGeneration != acknowledged.DrainGeneration {
		t.Fatalf("retry generation = %d, want %d", retried.DrainGeneration, acknowledged.DrainGeneration)
	}

	f.drains.reachDrained()
	observed := drainObservationOverTheLink(t, rpc(t, connection, 5, hostlink.MethodDrainStatus, drainRequest()))
	if observed.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("observed state = %q, want %q", observed.State, sessionwire.HostLinkDrainStateDrained)
	}

	foreign := drainRequest()
	foreign.HostID = "host-beta"
	refusal := refusedRPC(t, rpc(t, connection, 6, hostlink.MethodDrain, foreign))
	if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Fatalf("refusal = %#v, want runtime_unavailable", refusal)
	}

	if begins, _, _ := f.drains.counts(); begins != 1 {
		t.Fatalf("the link began %d drains, want one", begins)
	}
}

func drainObservationOverTheLink(t *testing.T, reply rpcReply) sessionwire.HostLinkDrainObservation {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("drain rpc failed at the transport: %#v", *reply.Error)
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 {
		t.Fatalf("drain rpc returned no body: %#v", reply)
	}
	var observation sessionwire.HostLinkDrainObservation
	if err := json.Unmarshal(reply.RPC.Data, &observation); err != nil {
		t.Fatalf("drain body %s is not a Core HostLinkDrainObservation: %v", reply.RPC.Data, err)
	}
	return observation
}

// ---------------------------------------------------------------------------
// Step 1: only a service principal reaches the RPC
// ---------------------------------------------------------------------------

// TestOnlyAnAuthenticatedServicePrincipalReachesTheDrainRPC is step 1's "never
// a tenant or browser principal", made through the only mechanism this package
// has — and it is spelled out because the guarantee is NOT in the drain path.
//
// core v0.7.0's HostLink connect record carries NO ROLE, so there is no role
// check to write. What makes the criterion true is structural: HostLink admits
// exactly ONE PRINCIPAL CLASS, because every physical connection presents a
// service credential to Authenticator.VerifyTenant and a connection that fails
// it is disconnected before any RPC handler is installed. If that ever stopped
// being true — a second connect gate, or a gate that authenticated after
// installing handlers — whole-Host drain would become reachable by whatever the
// new class is, and no assertion in the drain path would notice.
//
// So the property asserted here is the one that carries the weight: there is
// exactly one connect gate, VerifyTenant is the first thing it does, and a
// connection that fails it reaches neither the drain RPC nor the state machine.
// The behavioural half runs first and the structural half pins the mechanism it
// depends on.
func TestOnlyAnAuthenticatedServicePrincipalReachesTheDrainRPC(t *testing.T) {
	f := newDrainFixture(t)
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	disconnect := rejectedConnect(t, connection, "not-the-service-credential", sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if disconnect.Code != 4500 {
		t.Fatalf("disconnect code = %d, want the authentication code 4500", disconnect.Code)
	}

	// The RPC the link never got to send. Writing it AFTER the close proves
	// the handler is unreachable rather than merely unused.
	if err := connection.WriteJSON(map[string]any{
		"id":  2,
		"rpc": map[string]any{"method": hostlink.MethodDrain, "data": drainRequest()},
	}); err == nil {
		if _, _, readErr := connection.ReadMessage(); readErr == nil {
			t.Fatal("a drain RPC was answered on a link that failed authentication")
		}
	}
	if begins, starts, observes := f.drains.counts(); begins != 0 || starts != 0 || observes != 0 {
		t.Fatalf("an unauthenticated link reached the drain state machine: begins=%d starts=%d observes=%d", begins, starts, observes)
	}

	// The POSITIVE CONTROL for the count above: the same request on a link
	// that DOES authenticate reaches the machine. Without it, zero calls is
	// satisfied by a drain RPC nothing can ever reach.
	admitted := dial(t, httpServer.URL, "")
	defer admitted.Close()
	if reply := connect(t, admitted, testCredential, sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	}); reply.Connect == nil {
		t.Fatalf("the service credential was refused: %#v", reply)
	}
	drainObservationOverTheLink(t, rpc(t, admitted, 3, hostlink.MethodDrain, drainRequest()))
	if begins, _, _ := f.drains.counts(); begins != 1 {
		t.Fatalf("an authenticated link began %d drains, want one", begins)
	}

	// And the structural half: ONE connect gate, authenticating first.
	assertOneConnectGateAuthenticatingFirst(t)
}

// assertOneConnectGateAuthenticatingFirst holds the mechanism the test above
// depends on: this package installs exactly ONE connect gate, and it
// authenticates before it does anything else.
//
// It is a STRUCTURAL check because the property is about what EXISTS, not about
// what one connection did: a gate that skipped VerifyTenant would leave every
// behavioural test in this file green, since they all dial the gate that does
// authenticate.
//
// IT WALKS THE WHOLE PACKAGE, and that is the correction a probe forced. An
// earlier version parsed a hard-coded "centrifuge.go". centrifuge@v0.38.0's
// Node.OnConnecting is a SILENT SETTER — the last registration wins and none of
// them is refused — so a second gate declared in ANY OTHER FILE of this package
// installed the exact defect this guard exists to catch and the whole suite
// stayed green. The two sibling guards in this module already walked their
// directories; this one did not, and the inconsistency was the tell.
//
// It fails at zero production files and at zero registrations, so a renamed
// file or a rewritten transport is a failure rather than a silent pass.
func assertOneConnectGateAuthenticatingFirst(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the hostlink package directory: %v", err)
	}
	fileSet := token.NewFileSet()

	type connectGate struct {
		file string
		line int
		body *ast.FuncLit
	}
	var gates []connectGate
	var production int
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		production++
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector || selector.Sel.Name != "OnConnecting" || len(call.Args) != 1 {
				return true
			}
			literal, isLiteral := call.Args[0].(*ast.FuncLit)
			if !isLiteral {
				t.Fatalf("%s: OnConnecting is registered with a non-literal handler, which this check cannot read", name)
			}
			gates = append(gates, connectGate{file: name, line: fileSet.Position(call.Pos()).Line, body: literal})
			return true
		})
	}
	if production == 0 {
		t.Fatal("no production files were parsed, so this check proves nothing")
	}
	if len(gates) != 1 {
		where := make([]string, len(gates))
		for index, gate := range gates {
			where[index] = gate.file + ":" + strconv.Itoa(gate.line)
		}
		t.Fatalf("this package registers %d connect gates (%v), want exactly one; "+
			"Node.OnConnecting is a silent setter, so a second gate is a second principal class", len(gates), where)
	}

	gate := gates[0]
	if len(gate.body.Body.List) == 0 {
		t.Fatalf("%s:%d: the connect gate is empty", gate.file, gate.line)
	}
	var verified bool
	ast.Inspect(gate.body.Body.List[0], func(node ast.Node) bool {
		selector, isSelector := node.(*ast.SelectorExpr)
		if isSelector && selector.Sel.Name == "VerifyTenant" {
			verified = true
		}
		return !verified
	})
	if !verified {
		t.Fatalf("%s:%d: the connect gate does not call Authenticator.VerifyTenant as its first statement, "+
			"so a connection can reach a handler before presenting a service credential", gate.file, gate.line)
	}
}

// TestAStatusRPCIsAnsweredWhileADrainRPCIsStillStarting is the timing axis of
// the Class 4 audit, which the behavioural audit could not see.
//
// The handler holds no lock of its own, so `drain_status` must be answerable
// while `drain` is still inside the state machine. That was not a hypothetical:
// the production machine held one mutex across its nonaccepting publication, so
// the status observation — the RPC whose whole purpose is answering a Factory
// promptly — blocked for the entire write. The fix is in internal/lifecycle;
// what belongs HERE is the property that the transport adds no serialization of
// its own, and a fake able to express the delay so that the property is
// testable at all.
//
// "IS ANSWERED PROMPTLY" IS A NOTHING-HAPPENED CLAIM, so the blocked start is
// the positive control: the test asserts the drain RPC has NOT returned at the
// moment the status RPC is answered.
func TestAStatusRPCIsAnsweredWhileADrainRPCIsStillStarting(t *testing.T) {
	t.Parallel()
	f := newDrainFixture(t)
	release := make(chan struct{})
	f.drains.block = release

	acknowledged := make(chan sessionwire.HostLinkDrainObservation, 1)
	go func() { acknowledged <- mustStartDrain(t, f.mux, drainRequest()) }()
	waitFor(t, "the drain to reach the state machine", func() bool { return f.drains.entered.Load() > 0 })

	// THE POSITIVE CONTROL: the starter is genuinely blocked.
	select {
	case observation := <-acknowledged:
		t.Fatalf("the drain RPC returned %+v while its state machine was still blocked, "+
			"so this test's control does not block and proves nothing", observation)
	default:
	}

	answered := make(chan error, 1)
	go func() {
		_, err := f.mux.ObserveDrain(drainRequest())
		answered <- err
	}()
	select {
	case err := <-answered:
		// No drain has begun yet — the starter has not returned — so the
		// bounded observation correctly refuses rather than inventing one.
		refusal := refusedDrain(t, err)
		if refusal.Refusal != hostlink.RefusalNoDrainInProgress {
			t.Fatalf("refusal = %q, want %q", refusal.Refusal, hostlink.RefusalNoDrainInProgress)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the status RPC blocked behind an in-flight drain RPC, so the handler serializes the two")
	}

	close(release)
	if observation := <-acknowledged; observation.DrainGeneration != testDrainGeneration {
		t.Fatalf("acknowledgement = %+v, want generation %d", observation, testDrainGeneration)
	}
}

// waitFor spins on a STATE PREDICATE rather than a duration.
func waitFor(t *testing.T, what string, reached func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !reached() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Composition
// ---------------------------------------------------------------------------

// TestDrainSeamsAreSuppliedTogetherOrNotAtAll refuses the half-composed Host.
//
// One seam without the other is a Host that can be told to drain and cannot be
// asked how it is going, or the reverse — and either would be discovered by a
// Factory in production rather than at construction.
func TestDrainSeamsAreSuppliedTogetherOrNotAtAll(t *testing.T) {
	t.Parallel()

	base := func(drains *stubDrains) hostlink.MultiplexerOptions {
		return hostlink.MultiplexerOptions{
			TenantID:           testTenant,
			HostID:             testHostID,
			HostGeneration:     testGeneration,
			Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
			Admission:          &stubAdmission{},
			Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
			DrainStarter:       drains,
			DrainObserver:      drains,
			MaxBindingsPerLink: 2,
			MaxBindings:        3,
		}
	}

	for _, test := range []struct {
		name  string
		drop  func(*hostlink.MultiplexerOptions)
		field string
	}{
		{"a starter nobody can observe", func(o *hostlink.MultiplexerOptions) { o.DrainObserver = nil }, "DrainObserver"},
		{"an observer nobody can begin", func(o *hostlink.MultiplexerOptions) { o.DrainStarter = nil }, "DrainStarter"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := base(newStubDrains())
			test.drop(&options)
			_, err := hostlink.NewMultiplexer(options)
			if err == nil {
				t.Fatal("a half-composed Host was accepted")
			}
			var invalid *hostlink.InvalidMultiplexerOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("error %v is not an *InvalidMultiplexerOptionsError", err)
			}
			if invalid.Field != test.field {
				t.Fatalf("field = %q, want %q", invalid.Field, test.field)
			}
		})
	}

	t.Run("neither is a Host that refuses drain", func(t *testing.T) {
		t.Parallel()
		options := base(nil)
		options.DrainStarter = nil
		options.DrainObserver = nil
		if _, err := hostlink.NewMultiplexer(options); err != nil {
			t.Fatalf("a Host composed without drain was refused: %v", err)
		}
	})

	t.Run("both is a Host that drains", func(t *testing.T) {
		t.Parallel()
		if _, err := hostlink.NewMultiplexer(base(newStubDrains())); err != nil {
			t.Fatalf("a fully composed Host was refused: %v", err)
		}
	})
}

// TestAFixedSessionMustBeOneThisHostCouldHold rejects a dedicated Host
// configured with a session Core would refuse, at construction rather than at
// the first drain.
func TestAFixedSessionMustBeOneThisHostCouldHold(t *testing.T) {
	t.Parallel()
	drains := newStubDrains()
	_, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID:           testTenant,
		HostID:             testHostID,
		HostGeneration:     testGeneration,
		Residencies:        &recordingResidencies{inner: registry.New(frozenClock{})},
		Admission:          &stubAdmission{},
		Consumers:          &stubConsumers{consumers: map[registry.Key]*stubConsumer{}},
		DrainStarter:       drains,
		DrainObserver:      drains,
		FixedSessionID:     sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes+1)),
		MaxBindingsPerLink: 2,
		MaxBindings:        3,
	})
	if err == nil {
		t.Fatal("a fixed session Core refuses was accepted")
	}
	var invalid *hostlink.InvalidMultiplexerOptionsError
	if !errors.As(err, &invalid) || invalid.Field != "FixedSessionID" {
		t.Fatalf("error = %v, want an InvalidMultiplexerOptionsError on FixedSessionID", err)
	}
}

// TestDrainSeamsExposeExactlyOneMethodEach holds the two collaborator
// interfaces at their narrowest.
//
// Step 2 requires them NARROW, and narrowness is not a comment: a second method
// on DrainStarter is a second thing the transport may make the Host do, and a
// second method on DrainObserver is a read that could mutate. The method SET is
// asserted by name, so a widened seam fails here rather than in review.
func TestDrainSeamsExposeExactlyOneMethodEach(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		seam   reflect.Type
		method string
	}{
		{reflect.TypeOf((*hostlink.DrainStarter)(nil)).Elem(), "StartDrain"},
		{reflect.TypeOf((*hostlink.DrainObserver)(nil)).Elem(), "ObserveDrain"},
	} {
		if test.seam.NumMethod() != 1 {
			t.Fatalf("%s has %d methods, want one", test.seam, test.seam.NumMethod())
		}
		if name := test.seam.Method(0).Name; name != test.method {
			t.Fatalf("%s's method is %q, want %q", test.seam, name, test.method)
		}
	}

	// DrainScope is the resolved form and carries exactly the Key. A field
	// added here is a new thing a state machine may branch on, which is why
	// the whole set is pinned rather than the one field being probed.
	scope := reflect.TypeOf(hostlink.DrainScope{})
	if scope.NumField() != 1 || scope.Field(0).Name != "Key" {
		names := make([]string, scope.NumField())
		for index := range scope.NumField() {
			names[index] = scope.Field(index).Name
		}
		t.Fatalf("DrainScope fields = %v, want exactly [Key]", names)
	}
}
