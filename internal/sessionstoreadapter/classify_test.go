package sessionstoreadapter

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------
//
// THESE ARE UNIT TESTS OVER CONSTRUCTED STORE ERRORS, and that is deliberate
// rather than second-best. Two of the three ownership codes — journal
// lease_lost and journal fenced — are produced by the released store only on a
// lease takeover, which memstore cannot stage (see the note beside Grant.end).
// The integration tests reach lease_held and registry epoch; nothing in this
// module can reach the other two through the store, so the arms that decide
// whether this Host keeps writing to a session it has lost would otherwise be
// held by prose alone. The mapping is a pure function of an exported error
// type, so it is tested as one.
//
// EVERY ARM IS ENUMERATED FROM THE RELEASED VOCABULARY, not sampled: the tables
// below walk the store's own code constants, so a code added upstream lands in
// the default row and is reported as unmapped rather than silently classified.

func TestClassifyJournalMapsEveryOwnershipCode(t *testing.T) {
	for _, tt := range []struct {
		code     sessionstore.JournalErrorCode
		sentinel error
	}{
		{code: sessionstore.JournalErrorLeaseHeld, sentinel: residency.ErrLeaseHeld},
		{code: sessionstore.JournalErrorFenced, sentinel: residency.ErrFenceConflict},
		{code: sessionstore.JournalErrorLeaseLost, sentinel: residency.ErrEpochSuperseded},
	} {
		t.Run(string(tt.code), func(t *testing.T) {
			original := &sessionstore.JournalError{Code: tt.code, Field: "lease"}
			classified := classifyJournal(original)

			if !errors.Is(classified, tt.sentinel) {
				t.Fatalf("classifyJournal(%q) = %v, which does not unwrap to the sentinel residency.epochFence classifies", tt.code, classified)
			}
			// THE STORE'S ERROR SURVIVES. An operator diagnosing this needs the
			// field and the code, and a classification that replaced them would
			// buy the sentinel by discarding the diagnosis.
			var preserved *sessionstore.JournalError
			if !errors.As(classified, &preserved) || preserved.Code != tt.code {
				t.Fatalf("classifyJournal(%q) lost the store's error: %v", tt.code, classified)
			}
			// THE THREE ARE NOT INTERCHANGEABLE. residency.LossReason renders
			// them differently and an operator looks in a different place for
			// each, so a mapping that collapsed two would be undetectable by a
			// test asserting only "some sentinel".
			for _, other := range []error{residency.ErrLeaseHeld, residency.ErrFenceConflict, residency.ErrEpochSuperseded} {
				if other != tt.sentinel && errors.Is(classified, other) {
					t.Fatalf("classifyJournal(%q) also unwraps to a second sentinel", tt.code)
				}
			}
		})
	}
}

// A code that is not an ownership statement is passed through unchanged. It must
// NOT unwrap to a residency sentinel: epochFence records ownership as gone for
// exactly those, so classifying a backend fault as one would surrender a session
// this Host still holds.
func TestClassifyJournalLeavesEveryOtherCodeAlone(t *testing.T) {
	for _, code := range []sessionstore.JournalErrorCode{
		sessionstore.JournalErrorInvalid,
		sessionstore.JournalErrorUnknown,
		sessionstore.JournalErrorClosed,
		sessionstore.JournalErrorBackend,
		sessionstore.JournalErrorIntegrity,
		sessionstore.JournalErrorTooLarge,
		sessionstore.JournalErrorCursor,
	} {
		t.Run(string(code), func(t *testing.T) {
			original := &sessionstore.JournalError{Code: code}
			classified := classifyJournal(original)
			if classified != error(original) {
				t.Fatalf("classifyJournal(%q) = %v, want the store's error unchanged", code, classified)
			}
			for _, sentinel := range []error{residency.ErrLeaseHeld, residency.ErrFenceConflict, residency.ErrEpochSuperseded} {
				if errors.Is(classified, sentinel) {
					t.Fatalf("classifyJournal(%q) surrendered ownership for a failure that says nothing about it", code)
				}
			}
		})
	}
}

// A WRAPPED error is classified, because the store wraps its own and a caller
// may wrap again. Matching only the top-level value is the commonest way a
// classification silently stops firing.
func TestClassifyJournalSeesAWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("open the journal: %w", &sessionstore.JournalError{Code: sessionstore.JournalErrorFenced})
	if !errors.Is(classifyJournal(wrapped), residency.ErrFenceConflict) {
		t.Fatal("a wrapped fenced error was not classified")
	}
}

func TestClassifyJournalPassesNilThrough(t *testing.T) {
	if err := classifyJournal(nil); err != nil {
		t.Fatalf("classifyJournal(nil) = %v, want nil", err)
	}
	if err := classifyRegistry(nil); err != nil {
		t.Fatalf("classifyRegistry(nil) = %v, want nil", err)
	}
	if err := classifyInbox(nil); err != nil {
		t.Fatalf("classifyInbox(nil) = %v, want nil", err)
	}
}

// M3's SUBJECT. F12's whole paragraph rests on this arm — a superseded epoch
// must arrive at commands.Fence.Write as ErrEpochSuperseded or the fence never
// records the loss and this Host applies commands against a session a successor
// owns, indefinitely, with a green suite.
func TestClassifyInboxReportsASupersededEpochAsTheSentinel(t *testing.T) {
	original := &sessionstore.InboxError{Code: sessionstore.InboxErrorEpoch, Field: "lease_epoch", Epoch: 4}
	classified := classifyInbox(original)

	if !errors.Is(classified, residency.ErrEpochSuperseded) {
		t.Fatalf("classifyInbox(epoch) = %v, want ErrEpochSuperseded", classified)
	}
	var preserved *sessionstore.InboxError
	if !errors.As(classified, &preserved) || preserved.Epoch != 4 {
		t.Fatalf("classifyInbox(epoch) lost the store's error: %v", classified)
	}
}

// THE OTHER DIRECTION, AND IT IS F12 ITSELF. Three inbox codes have a Host
// refusal with the same name, and none of them is promoted: the applier reaches
// RefusalClaimHeld, RefusalDeadlinePassed and its terminal path from its OWN
// precondition checks against a record it read, and only LOSES the race to
// these. Promoting one would make a lost compare-and-swap indistinguishable
// from a precondition the applier evaluated. This asserts the deferral to O5/O6
// rather than leaving it as a comment.
func TestClassifyInboxLeavesEveryOtherCodeAlone(t *testing.T) {
	for _, code := range []sessionstore.InboxErrorCode{
		sessionstore.InboxErrorInvalid,
		sessionstore.InboxErrorCursor,
		sessionstore.InboxErrorCommandMismatch,
		sessionstore.InboxErrorNotFound,
		sessionstore.InboxErrorDeleted,
		sessionstore.InboxErrorIdentity,
		sessionstore.InboxErrorConflict,
		sessionstore.InboxErrorClaimHeld,
		sessionstore.InboxErrorClaimLost,
		sessionstore.InboxErrorDeadline,
		sessionstore.InboxErrorState,
		sessionstore.InboxErrorEvidence,
		sessionstore.InboxErrorTerminal,
		sessionstore.InboxErrorUnknown,
		sessionstore.InboxErrorBackend,
		sessionstore.InboxErrorMalformed,
		sessionstore.InboxErrorVersion,
		sessionstore.InboxErrorTooLarge,
	} {
		t.Run(string(code), func(t *testing.T) {
			original := &sessionstore.InboxError{Code: code}
			classified := classifyInbox(original)
			if classified != error(original) {
				t.Fatalf("classifyInbox(%q) = %v, want the store's error unchanged", code, classified)
			}
			if errors.Is(classified, residency.ErrEpochSuperseded) {
				t.Fatalf("classifyInbox(%q) surrendered ownership for a failure that says nothing about it", code)
			}
		})
	}
}

func TestClassifyRegistryLeavesEveryNonEpochCodeAlone(t *testing.T) {
	for _, code := range []sessionstore.RegistryErrorCode{
		sessionstore.RegistryErrorInvalid,
		sessionstore.RegistryErrorNotFound,
		sessionstore.RegistryErrorExpired,
		sessionstore.RegistryErrorReleased,
		sessionstore.RegistryErrorDeleted,
		sessionstore.RegistryErrorIdentity,
		sessionstore.RegistryErrorConflict,
		sessionstore.RegistryErrorUnknown,
		sessionstore.RegistryErrorBackend,
		sessionstore.RegistryErrorMalformed,
		sessionstore.RegistryErrorVersion,
		sessionstore.RegistryErrorTooLarge,
	} {
		t.Run(string(code), func(t *testing.T) {
			original := &sessionstore.RegistryError{Code: code}
			if classified := classifyRegistry(original); classified != error(original) {
				t.Fatalf("classifyRegistry(%q) = %v, want the store's error unchanged", code, classified)
			}
		})
	}
	// A failure that is not a registry error at all passes through untouched.
	other := errors.New("the backend is unreachable")
	if classifyRegistry(other) != other {
		t.Fatal("classifyRegistry rewrote a failure that is not the store's")
	}
}

// ---------------------------------------------------------------------------
// M4: isCatalogAbsent, all three arms
// ---------------------------------------------------------------------------

// F13's WHOLE CLAIM, ARM BY ARM. The keyspace arm is the one that was wrong on
// first run; the two catalog arms are the ones the comment asserts about and
// nothing measured. Each is now the sole difference between "this session has no
// durable state" and "the store failed", which is the difference between the
// create path and a refused attach.
func TestIsCatalogAbsentAdmitsEveryWayTheStoreSaysThereIsNoRecord(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "never created", err: &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound}},
		{name: "record gone", err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}},
		{name: "record deleted", err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted}},
		{name: "wrapped", err: fmt.Errorf("read the catalog: %w", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if !isCatalogAbsent(tt.err) {
				t.Fatalf("isCatalogAbsent(%v) = false; a hydration would report a store failure for a session that simply has no durable state", tt.err)
			}
		})
	}
}

// THE OTHER DIRECTION. Everything else is a failure, and treating one as absence
// would send a session with durable state down the CREATE path — a second
// runtime over a session that already has one.
func TestIsCatalogAbsentRefusesEveryFailureThatIsNotAbsence(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "other keyspace code", err: &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceLayoutMismatch}},
		{name: "keyspace hash collision", err: &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceHashCollision}},
		{name: "catalog backend", err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend}},
		{name: "catalog malformed", err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorMalformed}},
		{name: "catalog conflict", err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorConflict}},
		{name: "not the store's error", err: errors.New("the backend is unreachable")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if isCatalogAbsent(tt.err) {
				t.Fatalf("isCatalogAbsent(%v) = true; a session with durable state would be hydrated as new", tt.err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Vocabulary translation
// ---------------------------------------------------------------------------

// EVERY RELEASED STATE MAPS TO EXACTLY ONE HOST STATE, and no two map to the
// same one. A collapse here would let the cursor pass a command nobody applied
// or re-drive a terminal one, and a table asserting only "no error" could not
// see it.
func TestAdaptStateIsATotalInjection(t *testing.T) {
	want := map[sessionstore.InboxState]commands.State{
		sessionstore.InboxStatePending:  commands.StatePending,
		sessionstore.InboxStateClaimed:  commands.StateClaimed,
		sessionstore.InboxStateApplying: commands.StateApplying,
		sessionstore.InboxStateApplied:  commands.StateApplied,
		sessionstore.InboxStateRejected: commands.StateRejected,
	}
	seen := map[commands.State]sessionstore.InboxState{}
	for state, expected := range want {
		got, err := adaptState(state)
		if err != nil {
			t.Fatalf("adaptState(%q): %v", state, err)
		}
		if got != expected {
			t.Fatalf("adaptState(%q) = %q, want %q", state, got, expected)
		}
		if previous, collision := seen[got]; collision {
			t.Fatalf("adaptState maps both %q and %q onto %q", previous, state, got)
		}
		seen[got] = state
	}
}

// AN UNKNOWN STATE IS REFUSED, NOT DEFAULTED. Defaulting it to pending would
// re-drive somebody else's terminal command and defaulting it to terminal would
// let the cursor walk past work nobody has done.
func TestAdaptStateRefusesAStateHostCannotReasonAbout(t *testing.T) {
	got, err := adaptState(sessionstore.InboxState("quiesced"))
	var unmapped *UnmappedValueError
	if !errors.As(err, &unmapped) {
		t.Fatalf("adaptState of an unknown state = (%q, %v), want an UnmappedValueError", got, err)
	}
	if got != "" {
		t.Fatalf("adaptState of an unknown state returned %q, which commands.State.known would accept if it were a member", got)
	}
	if unmapped.Vocabulary != "sessionstore.InboxState" || unmapped.Value != "quiesced" {
		t.Fatalf("the refusal is %+v, want it to name the vocabulary and the value", unmapped)
	}
}

func TestAdaptOutcomeIsATotalInjection(t *testing.T) {
	want := map[sessionstore.CommandApplicationOutcome]commands.ApplicationOutcome{
		sessionstore.CommandApplicationAbsent:     commands.ApplicationAbsent,
		sessionstore.CommandApplicationCommitted:  commands.ApplicationCommitted,
		sessionstore.CommandApplicationAbandoned:  commands.ApplicationAbandoned,
		sessionstore.CommandApplicationUnresolved: commands.ApplicationUnresolved,
		sessionstore.CommandApplicationConflicted: commands.ApplicationConflicted,
	}
	seen := map[commands.ApplicationOutcome]sessionstore.CommandApplicationOutcome{}
	for outcome, expected := range want {
		got, err := adaptOutcome(outcome)
		if err != nil {
			t.Fatalf("adaptOutcome(%q): %v", outcome, err)
		}
		if got != expected {
			t.Fatalf("adaptOutcome(%q) = %q, want %q", outcome, got, expected)
		}
		if previous, collision := seen[got]; collision {
			t.Fatalf("adaptOutcome maps both %q and %q onto %q", previous, outcome, got)
		}
		seen[got] = outcome
	}
}

// EACH OUTCOME UNLOCKS A DIFFERENT SETTLEMENT, so an unknown one has no safe
// default: absent licenses a rejection and committed licenses a completion.
func TestAdaptOutcomeRefusesAnOutcomeHostCannotActOn(t *testing.T) {
	got, err := adaptOutcome(sessionstore.CommandApplicationOutcome("partial"))
	var unmapped *UnmappedValueError
	if !errors.As(err, &unmapped) {
		t.Fatalf("adaptOutcome of an unknown outcome = (%q, %v), want an UnmappedValueError", got, err)
	}
	if got != "" {
		t.Fatalf("adaptOutcome of an unknown outcome returned %q, which the applier would act on", got)
	}
}

func TestUnmappedValueErrorReportsItsCause(t *testing.T) {
	cause := errors.New("not a uuid")
	err := &UnmappedValueError{Vocabulary: "sessionstore.RuntimeCommandID", Value: "x", Cause: cause}
	if !errors.Is(err, cause) {
		t.Fatal("UnmappedValueError does not unwrap to its cause")
	}
	if (&UnmappedValueError{Vocabulary: "v", Value: "x"}).Unwrap() != nil {
		t.Fatal("an UnmappedValueError with no cause unwraps to something")
	}
}

// ---------------------------------------------------------------------------
// What a failure says
// ---------------------------------------------------------------------------

// EVERY REFUSAL THIS PACKAGE RAISES NAMES ITS SUBJECT. These messages are what an
// operator has when a Host refuses an attach or a command, and a message that
// dropped the member, the value or the session is the difference between one
// diagnosis and a bisect. Asserting the substrings is what keeps a later
// rewording from quietly removing them.
func TestEveryRefusalNamesItsSubject(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "unavailable member",
			err:  &UnavailableMemberError{Member: "Namespace", Reason: "no layout was supplied"},
			want: []string{"Namespace", "no layout was supplied"},
		},
		{
			name: "unmapped value with a cause",
			err:  &UnmappedValueError{Vocabulary: "sessionstore.InboxState", Value: "quiesced", Cause: errors.New("unknown")},
			want: []string{"sessionstore.InboxState", "quiesced", "unknown"},
		},
		{
			name: "unmapped value with no cause",
			err:  &UnmappedValueError{Vocabulary: "sessionstore.InboxState", Value: "quiesced"},
			want: []string{"sessionstore.InboxState", "quiesced"},
		},
		{
			name: "foreign grant",
			err: &ForeignGrantError{
				GrantTenant: "tenant-a", GrantSession: "session-a",
				CalledTenant: "tenant-a", CalledSession: "session-b",
			},
			want: []string{"session-a", "session-b"},
		},
		{
			name: "gate owner unavailable",
			err:  &GateOwnerUnavailableError{GateID: "gate-a"},
			want: []string{"gate-a", "no owning Host"},
		},
		{
			name: "unsupported wire version",
			err:  &UnsupportedWireVersionError{Version: 9},
			want: []string{"9", "1"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			message := tt.err.Error()
			for _, want := range tt.want {
				if !strings.Contains(message, want) {
					t.Fatalf("%q does not name %q", message, want)
				}
			}
		})
	}
}
