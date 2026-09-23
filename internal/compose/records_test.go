package compose

import (
	"context"
	"fmt"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// TestSessionsPassingThroughAPooledHostLeaveNoRecordBehind is booked finding
// B1 on the composed Host: every residency that ends — here, lost — prunes the
// Manager's record and cancels the session context its abandoned runtime ran
// on, so a long-lived pooled Host's memory returns to its baseline however
// many distinct sessions it has held.
func TestSessionsPassingThroughAPooledHostLeaveNoRecordBehind(t *testing.T) {
	f := newFixture(t)
	f.start()
	baseline := f.svc.manager.Records()
	var contexts []context.Context
	for i := range 20 {
		session := sessionwire.SessionID(fmt.Sprintf("session-pass-%02d", i))
		key := registry.Key{TenantID: tenantA, SessionID: session}
		f.rig.Session = newControllableSession(testRigSessionID)
		f.attach(tenantA, session)
		ctx, ok := f.svc.manager.SessionContext(key)
		if !ok {
			t.Fatalf("no session context for %s", session)
		}
		contexts = append(contexts, ctx)
		f.store.loseLease(key)
		awaitCondition(t, "the lost residency to be forgotten", func() bool { return f.svc.residentFor(key) == nil })
	}
	awaitCondition(t, "the Manager's records to return to baseline", func() bool { return f.svc.manager.Records() == baseline })
	for i, ctx := range contexts {
		if ctx.Err() == nil {
			t.Fatalf("session %d's context was not cancelled when its abandoned residency ended", i)
		}
	}
	if consumed := f.svc.capacity.ConsumedWeight(); consumed != 0 {
		t.Fatalf("ConsumedWeight = %d after every session left, want 0", consumed)
	}
}

// TestADrainedResidencyLeavesNoRecordBehind: the drain's release ends every
// residency it releases, and prunes its record.
func TestADrainedResidencyLeavesNoRecordBehind(t *testing.T) {
	f := newFixture(t)
	f.start()
	for i := range 3 {
		f.rig.Session = newControllableSession(testRigSessionID)
		f.attach(tenantA, sessionwire.SessionID(fmt.Sprintf("session-drain-%d", i)))
	}
	if got := f.svc.manager.Records(); got != 3 {
		t.Fatalf("%d records with three sessions resident, want 3", got)
	}
	report, err := f.svc.Stop(t.Context())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(report.Failures) != 0 {
		t.Fatalf("drain failures: %+v", report.Failures)
	}
	if got := f.svc.manager.Records(); got != 0 {
		t.Fatalf("%d records after the drain released every session, want 0", got)
	}
}
