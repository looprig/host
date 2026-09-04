package sessionstoreadapter_test

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// Locations
// ---------------------------------------------------------------------------

func observation(epoch uint64, at time.Time) sessionwire.HostLinkRegistryObservation {
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               testTenant,
		SessionID:              testSession,
		HostID:                 sessionwire.HostID("host-adapter"),
		HostGeneration:         7,
		AgentID:                testAgent,
		RuntimeCompatibilityID: testCompat,
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       sessionwire.InternalEndpoint("ws://10.0.0.7:9443/hostlink"),
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             epoch,
		ObservedAt:             at,
		ExpiresAt:              at.Add(31 * time.Second),
	}
}

// The route round-trips through the released record: what Host published is what
// a Factory's peer reads back, member for member.
func TestPublishResidencyRoundTripsThroughTheReleasedRecord(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	now := time.Now().UTC()
	published := observation(4, now)
	if err := adapted.PublishResidency(t.Context(), published); err != nil {
		t.Fatalf("PublishResidency: %v", err)
	}

	entry, err := released.GetHostRegistration(t.Context(), sessionstore.GetHostRegistrationRequest{
		TenantID:  testTenant,
		SessionID: testSession,
	})
	if err != nil {
		t.Fatalf("GetHostRegistration: %v", err)
	}
	read, err := entry.Registration.Observation()
	if err != nil {
		t.Fatalf("Observation: %v", err)
	}
	if read.HostID != published.HostID || read.HostGeneration != published.HostGeneration ||
		read.AgentID != published.AgentID || read.RuntimeCompatibilityID != published.RuntimeCompatibilityID ||
		read.Placement != published.Placement || read.InternalEndpoint != published.InternalEndpoint ||
		read.Residency != published.Residency || read.Accepting != published.Accepting ||
		read.LeaseEpoch != published.LeaseEpoch {
		t.Fatalf("the route read back as %+v, want %+v", read, published)
	}
}

// A LOWER EPOCH IS REFUSED AS ErrEpochSuperseded, which is the sentinel
// residency.epochFence classifies. An adapter that let the store's own typed
// error through would leave a superseded Host retrying a session it has lost.
func TestPublishResidencyReportsASupersededEpochAsTheSentinel(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	now := time.Now().UTC()
	if err := adapted.PublishResidency(t.Context(), observation(9, now)); err != nil {
		t.Fatalf("PublishResidency at epoch 9: %v", err)
	}
	err := adapted.PublishResidency(t.Context(), observation(8, now))
	if !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("PublishResidency at a lower epoch = %v, want ErrEpochSuperseded", err)
	}
}

// The tombstone removes the route and keeps the high-water mark, which is what
// makes a rollback safe.
func TestTombstoneResidencyRemovesTheRouteAndKeepsTheMark(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	now := time.Now().UTC()
	if err := adapted.PublishResidency(t.Context(), observation(5, now)); err != nil {
		t.Fatalf("PublishResidency: %v", err)
	}
	if err := adapted.TombstoneResidency(t.Context(), testTenant, testSession, 5); err != nil {
		t.Fatalf("TombstoneResidency: %v", err)
	}
	if err := adapted.PublishResidency(t.Context(), observation(4, now)); !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("a republish below the tombstoned epoch = %v, want ErrEpochSuperseded", err)
	}
}

// ---------------------------------------------------------------------------
// F9: the payload load the store cannot order
// ---------------------------------------------------------------------------

// THE PAYLOAD IS READABLE BEFORE ANY CLAIM. commands splits LoadCommand and
// LoadPayload so that step 4's "the payload is loaded AFTER the claim" is a
// thing a test can see; the released record carries the body inline and hands it
// to any reader. So the ordering is a Host discipline and not a store-enforced
// one, and a fake that refuses an unclaimed payload read is asserting a
// guarantee nothing behind it makes.
func TestLoadPayloadAnswersBeforeAnyClaim(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	admitCommand(t, released, "command-a", string(commands.KindInput))

	record, err := adapted.LoadCommand(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("LoadCommand: %v", err)
	}
	if record.State != commands.StatePending {
		t.Fatalf("state = %q, want pending; nothing has claimed this command", record.State)
	}
	payload, err := adapted.LoadPayload(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("LoadPayload before a claim: %v", err)
	}
	if len(payload.Body) == 0 {
		t.Fatal("the released store withheld an unclaimed payload; it does not")
	}
}

// ---------------------------------------------------------------------------
// F10: the gate the projection cannot decide
// ---------------------------------------------------------------------------

// A session with no open gates answers soundly, because absence is the whole
// answer and the owner members describe a gate that exists.
func TestLoadGateReportsAnAbsentGate(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	gate, held, err := adapted.LoadGate(t.Context(), testTenant, testSession, sessionwire.GateID("gate-a"))
	if err != nil {
		t.Fatalf("LoadGate for an absent gate: %v", err)
	}
	if held {
		t.Fatal("a session with no gates reported one held")
	}
	if gate != (commands.Gate{}) {
		t.Fatalf("an absent gate answered %+v", gate)
	}
}

// ---------------------------------------------------------------------------
// The runtime identity the store does not constrain
// ---------------------------------------------------------------------------

// F11, MEASURED. sessionstore validates RuntimeCommandID as bounded opaque UTF-8
// and imposes no UUID grammar; Host's Record declares a uuid.UUID. A record
// written by an allocator that is not Harness therefore cannot be represented at
// all, and it is refused rather than zeroed — a zero runtime identity is the one
// value runtimecommand.Admitted.Validate rejects outright, so zeroing would move
// the failure past a durable claim.
func TestLoadCommandRefusesARuntimeIdentityThatIsNotAUUID(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	now := time.Now().UTC()
	if _, _, err := released.AdmitCommand(t.Context(), sessionstore.AdmitCommandRequest{
		TenantID:                 testTenant,
		SessionID:                testSession,
		CommandID:                "command-opaque",
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("not-a-uuid"),
		Kind:                     sessionstore.CommandKind(commands.KindInput),
		Payload:                  []byte(`{}`),
		AcceptedAt:               now,
		ApplyDeadline:            now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("the store refused a non-UUID runtime identity, so F11 is not what this test says: %v", err)
	}

	_, err := adapted.LoadCommand(t.Context(), testTenant, testSession, "command-opaque")
	var unmapped *sessionstoreadapter.UnmappedValueError
	if !errors.As(err, &unmapped) {
		t.Fatalf("LoadCommand = %v, want an UnmappedValueError", err)
	}
	if unmapped.Vocabulary != "sessionstore.RuntimeCommandID" {
		t.Fatalf("vocabulary = %q, want the runtime command id", unmapped.Vocabulary)
	}
}
