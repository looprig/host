package sessionstoreadapter_test

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

const testRuntimeCommandID = "44444444-4444-4444-4444-444444444444"

// admitCommand makes one command durable so the transitions have a record.
func admitCommand(t *testing.T, released *sessionstore.Store, id sessionwire.CommandID, kind string) sessionstore.InboxEntry {
	t.Helper()
	now := time.Now().UTC()
	entry, _, err := released.AdmitCommand(t.Context(), sessionstore.AdmitCommandRequest{
		TenantID:                 testTenant,
		SessionID:                testSession,
		CommandID:                id,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(testRuntimeCommandID),
		Kind:                     sessionstore.CommandKind(kind),
		Payload:                  []byte(`{"blocks":[]}`),
		AcceptedAt:               now,
		ApplyDeadline:            now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("admit command: %v", err)
	}
	return entry
}

// F2, MEASURED. residency models AcquireSessionLease and CommitOpeningFence as
// two seams, so an attach can be written against "the lease was granted and the
// fence was refused". The released store has no such state: OpenJournal takes
// the lease and fences at the tip it read, and while a grant is live a second
// open is refused AT THE LEASE with nothing taken. The direction that fails is
// fake→store — residency's fakeLeases can grant a lease whose fencer then
// refuses, and no released composition can.
func TestOpenSessionRefusesASecondGrantAtTheLease(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	first, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("first OpenSession: %v", err)
	}
	if first.Epoch() == 0 {
		t.Fatal("a granted lease reported epoch zero, which Core refuses on a registry observation")
	}

	second, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if !errors.Is(err, residency.ErrLeaseHeld) {
		t.Fatalf("second OpenSession = (%v, %v), want ErrLeaseHeld", second, err)
	}
}

// A RELEASED GRANT LETS A SUCCESSOR IN AT A STRICTLY HIGHER EPOCH, which is what
// makes the epoch usable as Core's lease_epoch. It also ends the predecessor:
// its next append is refused as a closed writer rather than committing under an
// epoch somebody else now holds.
func TestASuccessorTakesAStrictlyHigherEpochAfterARelease(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	first, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("first OpenSession: %v", err)
	}
	if err := first.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("second OpenSession: %v", err)
	}
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("second epoch %d did not exceed the first %d", second.Epoch(), first.Epoch())
	}

	if err := first.AppendApplicationPrefix(t.Context(), testTenant, testSession, commands.Prefix{
		CommandID:        "command-a",
		RuntimeCommandID: uuid.MustParse(testRuntimeCommandID),
		Kind:             commands.KindInput,
	}); err == nil {
		t.Fatal("a released grant appended a prefix")
	}
}

// F3, MEASURED IN BOTH DIRECTIONS. Lost is open while the grant is live, stays
// open across a session another writer could take, and closes on Release. What
// it never does is close because SOMETHING ELSE happened: the released store
// publishes no lease-loss signal to observe.
//
// THE TAKEOVER ARM IS NOT REACHABLE FROM THIS PACKAGE, and saying so is part of
// the finding rather than a gap in it. A takeover requires the live grant's
// lease to lapse, and neither sessionstore nor memstore offers a way to expire
// one; while the grant is live every rival open is refused at the lease (above).
// So the state residency's heartbeat is written against — this Host still
// believing it owns a session a successor has taken — cannot be constructed
// against the released store in one process, and the fake's expiry timer is the
// only thing that has ever produced it.
func TestLostReportsOnlyWhatThisGrantDid(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	select {
	case <-grant.Lost():
		t.Fatal("Lost was closed for a live grant")
	default:
	}

	if _, err := adapted.OpenSession(t.Context(), testTenant, testSession); err == nil {
		t.Fatal("a rival open succeeded against a live grant")
	}
	select {
	case <-grant.Lost():
		t.Fatal("Lost closed for a rival the released store never told this grant about")
	default:
	}

	if err := grant.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case <-grant.Lost():
	default:
		t.Fatal("Lost stayed open after the grant was released")
	}
}

// The prefix is committed under the WRITER's epoch and not one a caller chose,
// which is what commands.Prefix says it wants. Appending one and reading the
// correlation back proves the two agree.
func TestAppendApplicationPrefixIsStampedWithTheGrantsOwnEpoch(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	admitCommand(t, released, "command-a", "input")

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	runtimeCommandID := uuid.MustParse(testRuntimeCommandID)
	if err := grant.AppendApplicationPrefix(t.Context(), testTenant, testSession, commands.Prefix{
		CommandID:        "command-a",
		RuntimeCommandID: runtimeCommandID,
		Kind:             commands.KindInput,
	}); err != nil {
		t.Fatalf("AppendApplicationPrefix: %v", err)
	}

	application, err := adapted.FindApplication(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("FindApplication: %v", err)
	}
	if application.PrefixEpoch != grant.Epoch() {
		t.Fatalf("PrefixEpoch = %d, want the grant's own epoch %d", application.PrefixEpoch, grant.Epoch())
	}
	if application.RuntimeCommandID != runtimeCommandID {
		t.Fatalf("RuntimeCommandID = %v, want %v", application.RuntimeCommandID, runtimeCommandID)
	}
	if application.Outcome == commands.ApplicationAbsent {
		t.Fatal("a committed prefix read back as absent")
	}
}

// A write routed to a grant that owns a different session is refused rather than
// appended to the wrong stream. The released Append takes no identities, so
// nothing below this adapter could catch it.
func TestAppendApplicationPrefixRefusesAForeignSession(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	err = grant.AppendApplicationPrefix(t.Context(), testTenant, sessionwire.SessionID("session-b"), commands.Prefix{
		CommandID:        "command-a",
		RuntimeCommandID: uuid.MustParse(testRuntimeCommandID),
		Kind:             commands.KindInput,
	})
	var foreign *sessionstoreadapter.ForeignGrantError
	if !errors.As(err, &foreign) {
		t.Fatalf("AppendApplicationPrefix for a foreign session = %v, want ForeignGrantError", err)
	}
}
