package compose

import (
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
)

// TestASuccessorAdmittedBeforeTheOldCreditStaysCharged is booked finding B2 on
// the lost and fault paths. The ledger is keyed by session and Admit is
// idempotent, so a successor attach that passed its step 1 AFTER the old
// residency's registry entry was removed and BEFORE the old path credited found
// the old charge, charged nothing, and was then uncharged by that credit: a
// resident session holding no capacity, and a Host that over-admits by one.
//
// The interleaving is forced, not hoped for: when the old path releases its
// lease (after FinishRelease removed the registry entry, before the credit),
// a real attach of the same session is started and parked in its lease
// acquisition — past Admit — until the old path has finished.
func TestASuccessorAdmittedBeforeTheOldCreditStaysCharged(t *testing.T) {
	for _, path := range []struct {
		name string
		end  func(f *fixture)
	}{
		{"lost", func(f *fixture) { f.store.loseLease(keyA) }},
		{"fault", func(f *fixture) { f.runtime.Fault(errors.New("injected journal append failure")) }},
	} {
		t.Run(path.name, func(t *testing.T) {
			f := newFixture(t, func(_ *Options, host *hostconfig.Options) { host.Capacity = 1 })
			f.start()
			first := f.attach(tenantA, sessionA)

			f.store.acquireEntered = make(chan struct{})
			f.store.acquireRelease = make(chan struct{})
			f.rig.Session = newControllableSession(testRigSessionID)
			type result struct {
				held residency.Residency
				err  error
			}
			successor := make(chan result, 1)
			var once sync.Once
			f.trace.watch(func(step string) {
				if step != "lease.release" {
					return
				}
				once.Do(func() {
					go func() {
						held, err := f.svc.Attach(t.Context(), residency.Request{
							TenantID: tenantA, SessionID: sessionA, AgentID: testAgent, Mode: residency.ModeCreate,
							Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
						})
						successor <- result{held, err}
					}()
					select {
					case <-f.store.acquireEntered:
					case <-time.After(5 * time.Second):
						t.Error("the successor attach never reached its lease acquisition")
					}
				})
			})

			path.end(f)
			awaitCondition(t, "the old residency to be forgotten", func() bool {
				f.svc.mu.Lock()
				defer f.svc.mu.Unlock()
				held, present := f.svc.sessions[keyA]
				return !present || held.generation != first.Generation
			})
			close(f.store.acquireRelease)
			var got result
			select {
			case got = <-successor:
			case <-time.After(5 * time.Second):
				t.Fatal("the successor attach did not finish")
			}
			if got.err != nil || !got.held.Attached || got.held.Generation == first.Generation {
				t.Fatalf("successor attach = (%+v, %v), want a fresh residency", got.held, got.err)
			}
			// Let any late credit of the old path land before measuring.
			time.Sleep(50 * time.Millisecond)
			if consumed := f.svc.capacity.ConsumedWeight(); consumed != 1 {
				t.Fatalf("ConsumedWeight = %d with the successor resident, want 1: the old residency's credit uncharged its successor", consumed)
			}
			f.rig.Session = newControllableSession(testRigSessionID)
			_, err := f.svc.Attach(t.Context(), residency.Request{
				TenantID: tenantA, SessionID: sessionB, AgentID: testAgent, Mode: residency.ModeCreate,
				Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
			})
			var refused *residency.AttachError
			if !errors.As(err, &refused) || refused.Code != sessionwire.HostLinkErrorNoCapacity {
				t.Fatalf("attach into a one-slot Host the successor fills = %v, want no_capacity", err)
			}
		})
	}
}
