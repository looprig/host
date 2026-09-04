package sessionstoreadapter_test

import (
	"encoding/json"
	"errors"
	"strconv"
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

// F10's REFUSAL PATH, which is the branch the finding is about. A gate that
// EXISTS cannot be decided: the projection names no owning Host and no lease
// epoch, and §9.4's release-race backstop is decided from exactly those. The
// absent case above is the trivially-right branch and, alone, would leave a
// LoadGate that answered "absent" for an open gate entirely green.
func TestLoadGateRefusesAGateItCannotDecide(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	// The store refuses a gate naming an event the journal has not committed, so
	// the record's tip is advanced first. That refusal is the write-side half of
	// "a reader validates its matching durable open event"; it is not what this
	// test is about, and stepping over it is what lets the test reach the branch
	// that is.
	if _, err := released.UpdateCatalogHostState(t.Context(), sessionstore.UpdateCatalogHostStateRequest{
		TenantID:       testTenant,
		SessionID:      testSession,
		LeaseEpoch:     grant.Epoch(),
		State:          sessionwire.SessionStateRunning,
		Residency:      sessionwire.SessionResidencyResident,
		LastActiveAt:   time.Now().UTC(),
		LastJournalSeq: 4,
		LastEventID:    sessionwire.EventID("event-1"),
	}); err != nil {
		t.Fatalf("advance the catalog tip: %v", err)
	}
	if _, err := released.OpenGate(t.Context(), sessionstore.OpenGateRequest{
		TenantID:   testTenant,
		SessionID:  testSession,
		LeaseEpoch: grant.Epoch(),
		Gate: sessionwire.GateProjection{
			GateID:           sessionwire.GateID("gate-a"),
			Kind:             "permission",
			Prompt:           sessionwire.GatePrompt{Title: "may I"},
			OpenedEventID:    sessionwire.EventID("event-1"),
			OpenedJournalSeq: 1,
			Deadline:         time.Now().UTC().Add(time.Hour),
			Answerability:    sessionwire.GateAnswerabilityResident,
		},
	}); err != nil {
		t.Fatalf("open a durable gate: %v", err)
	}

	// THE PREMISE: the store really does report the gate, so the refusal below
	// is about the members it cannot report rather than about the read failing.
	page, err := released.ReadGates(t.Context(), sessionstore.ReadGatesRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("ReadGates: %v", err)
	}
	if len(page.Gates) != 1 {
		t.Fatalf("the store reports %d open gates, want one", len(page.Gates))
	}

	gate, held, err := adapted.LoadGate(t.Context(), testTenant, testSession, sessionwire.GateID("gate-a"))
	var unavailable *sessionstoreadapter.GateOwnerUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("LoadGate for an open gate = (%+v, %t, %v), want a GateOwnerUnavailableError", gate, held, err)
	}
	if !errors.Is(err, sessionstoreadapter.ErrGateOwnerUnavailable) {
		t.Fatalf("the refusal %v does not unwrap to ErrGateOwnerUnavailable", err)
	}
	if unavailable.GateID != sessionwire.GateID("gate-a") {
		t.Fatalf("the refusal names gate %q, want gate-a", unavailable.GateID)
	}
	// A REFUSAL AND NOT A PARTIAL ANSWER. Answering with a zero owner would read
	// as "owned by no Host", which is what would make every gate response
	// resumable.
	if held || gate != (commands.Gate{}) {
		t.Fatalf("the refusal carried a gate %+v held=%t", gate, held)
	}

	// AND A DIFFERENT GATE ON THE SAME SESSION IS STILL ABSENT, so the refusal
	// is keyed on the gate rather than on the session having any gate at all.
	if _, held, err := adapted.LoadGate(t.Context(), testTenant, testSession, sessionwire.GateID("gate-b")); err != nil || held {
		t.Fatalf("LoadGate for an unopened gate = (%t, %v), want (false, nil)", held, err)
	}
}

// ---------------------------------------------------------------------------
// The terminal transitions
// ---------------------------------------------------------------------------

// A COMPLETION NAMES THE EVENT THAT CARRIED THE EFFECT, and the store keeps it.
// "Applied" with no event is a claim that something happened with nothing to
// point at, so the members are asserted rather than the absence of an error.
func TestCompleteCommandSettlesAnApplyingRecord(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	entry := admitCommand(t, released, "command-a", string(commands.KindInterrupt))

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	now := time.Now().UTC()
	claimed, err := adapted.ClaimCommand(t.Context(), testTenant, testSession, "command-a", commands.Claim{
		ExpectedRevision: entry.Revision,
		Epoch:            grant.Epoch(),
		ExpiresAt:        now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimCommand: %v", err)
	}
	applying, err := adapted.BeginApplying(t.Context(), testTenant, testSession, "command-a", commands.Applying{
		ExpectedRevision: claimed,
		Epoch:            grant.Epoch(),
		ExpiresAt:        now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("BeginApplying: %v", err)
	}
	if err := adapted.CompleteCommand(t.Context(), testTenant, testSession, "command-a", commands.Completion{
		ExpectedRevision: applying,
		Epoch:            grant.Epoch(),
		Effect:           commands.Effect{CompletedAt: now, EventID: sessionwire.EventID("event-9"), JournalSeq: 9},
	}); err != nil {
		t.Fatalf("CompleteCommand: %v", err)
	}

	record, err := adapted.LoadCommand(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("LoadCommand: %v", err)
	}
	if record.State != commands.StateApplied {
		t.Fatalf("durable state = %q, want applied", record.State)
	}
	status, err := released.GetCommand(t.Context(), sessionstore.GetCommandRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: "command-a",
	})
	if err != nil {
		t.Fatalf("GetCommand: %v", err)
	}
	if status.Record.Result.EventID != sessionwire.EventID("event-9") || status.Record.Result.JournalSeq != 9 {
		t.Fatalf("the durable result is %+v, want the effect this completion named", status.Record.Result)
	}
}

// A REJECTION CARRIES A TYPED PUBLIC REASON, and the store keeps that too. A
// rejection whose detail were dropped would settle a command with nothing a
// client could branch on.
func TestRejectCommandSettlesWithItsTypedReason(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	entry := admitCommand(t, released, "command-a", string(commands.KindInterrupt))

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	detail := sessionwire.ErrorDetail{
		Code:    sessionwire.ErrorCodeRuntimeUnavailable,
		Message: "this Host has no runtime for the agent",
	}
	if err := adapted.RejectCommand(t.Context(), testTenant, testSession, "command-a", commands.Rejection{
		ExpectedRevision: entry.Revision,
		Epoch:            grant.Epoch(),
		Detail:           detail,
	}); err != nil {
		t.Fatalf("RejectCommand: %v", err)
	}

	record, err := adapted.LoadCommand(t.Context(), testTenant, testSession, "command-a")
	if err != nil {
		t.Fatalf("LoadCommand: %v", err)
	}
	if record.State != commands.StateRejected {
		t.Fatalf("durable state = %q, want rejected", record.State)
	}
	status, err := released.GetCommand(t.Context(), sessionstore.GetCommandRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: "command-a",
	})
	if err != nil {
		t.Fatalf("GetCommand: %v", err)
	}
	if status.Record.Rejection == nil || status.Record.Rejection.Code != detail.Code {
		t.Fatalf("the durable rejection is %+v, want the detail this call named", status.Record.Rejection)
	}
}

// A TRANSITION AT AN EPOCH BELOW THE RECORD'S HIGH-WATER MARK ARRIVES AS THE
// SENTINEL, through the store rather than through a constructed error. This is
// the integration half of TestClassifyInboxReportsASupersededEpochAsTheSentinel.
func TestATransitionBelowTheRecordsEpochReportsErrEpochSuperseded(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)
	entry := admitCommand(t, released, "command-a", string(commands.KindInterrupt))

	grant, err := adapted.OpenSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	now := time.Now().UTC()
	claimed, err := adapted.ClaimCommand(t.Context(), testTenant, testSession, "command-a", commands.Claim{
		ExpectedRevision: entry.Revision,
		Epoch:            grant.Epoch() + 5,
		ExpiresAt:        now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimCommand at a high epoch: %v", err)
	}

	_, err = adapted.ClaimCommand(t.Context(), testTenant, testSession, "command-a", commands.Claim{
		ExpectedRevision: claimed,
		Epoch:            grant.Epoch(),
		ExpiresAt:        now.Add(time.Minute),
	})
	if !errors.Is(err, residency.ErrEpochSuperseded) {
		t.Fatalf("a claim below the record's high-water mark = %v, want ErrEpochSuperseded", err)
	}
}

// ---------------------------------------------------------------------------
// LoadSession
// ---------------------------------------------------------------------------

// F1. The bytes are this adapter's choice and nothing may parse them, but they
// must be the record: an encoding that answered with an empty document would
// make every existence check pass while telling a reader nothing.
func TestLoadSessionAnswersWithTheDurableRecordAndRefusesAnAbsentOne(t *testing.T) {
	released, adapted := openStore(t)

	if _, err := adapted.LoadSession(t.Context(), testTenant, testSession); err == nil {
		t.Fatal("LoadSession answered for a session with no durable record")
	}

	createSession(t, released)
	encoded, err := adapted.LoadSession(t.Context(), testTenant, testSession)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	var record map[string]any
	if err := json.Unmarshal(encoded, &record); err != nil {
		t.Fatalf("the answer is not the record's canonical JSON: %v", err)
	}
	if record["SessionID"] != string(testSession) || record["AgentID"] != string(testAgent) {
		t.Fatalf("the answer is %v, want the durable catalog record", record)
	}
}

// F14, IN BOTH DIRECTIONS. The seam's own validator refuses an observation at a
// version Core does not support; the released request has no version member, so
// without this refusal the adapter would publish it and the store would hand it
// back stamped current — an adapter looser than the interface it satisfies.
func TestPublishResidencyRefusesAVersionTheDurableRouteCannotCarry(t *testing.T) {
	released, adapted := openStore(t)
	createSession(t, released)

	now := time.Now().UTC()
	for _, version := range []sessionwire.WireVersion{0, sessionwire.CurrentWireVersion + 1} {
		t.Run(strconv.FormatUint(uint64(version), 10), func(t *testing.T) {
			unsupported := observation(3, now)
			unsupported.Version = version

			// THE PREMISE: the seam's own validator refuses this observation, so
			// the adapter accepting it would be strictly looser than the type it
			// takes.
			if err := unsupported.Validate(); err == nil {
				t.Fatalf("sessionwire accepts version %d, so this row proves nothing", version)
			}

			err := adapted.PublishResidency(t.Context(), unsupported)
			var refused *sessionstoreadapter.UnsupportedWireVersionError
			if !errors.As(err, &refused) {
				t.Fatalf("PublishResidency at version %d = %v, want an UnsupportedWireVersionError", version, err)
			}
			if refused.Version != version {
				t.Fatalf("the refusal names version %d, want %d", refused.Version, version)
			}
			// NOTHING WAS WRITTEN. A refusal after the write would leave the
			// route durable and the caller believing it had not published.
			if _, err := released.GetHostRegistration(t.Context(), sessionstore.GetHostRegistrationRequest{
				TenantID: testTenant, SessionID: testSession,
			}); err == nil {
				t.Fatal("a refused observation was published anyway")
			}
		})
	}

	// THE OTHER DIRECTION: the current version publishes.
	if err := adapted.PublishResidency(t.Context(), observation(3, now)); err != nil {
		t.Fatalf("PublishResidency at the current version: %v", err)
	}
}
