package commands

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ---------------------------------------------------------------------------
// The refusal, and the control that makes it falsifiable
// ---------------------------------------------------------------------------

// TestNoDispatchRefusesEveryCommandItIsHanded holds the refusal at the unit the
// composition wires.
//
// IT IS A FOR-ALL AND IS DISCHARGED OVER THE READER'S DOMAIN rather than over
// one convenient record. Process branches on nothing, so every non-terminal
// state the consumer can hand it is one row here; a build that refused only
// `pending` would leave a Host that dispatches a command it recovered mid-flight,
// which is the case with an effect already behind it.
func TestNoDispatchRefusesEveryCommandItIsHanded(t *testing.T) {
	t.Parallel()

	// THE STATES THE CONSUMER ACTUALLY HANDS ON. Reconcile short-circuits the
	// two terminal ones, so these three are the domain — and the unknown state is
	// here because a newer Factory's vocabulary must not become a dispatch.
	for _, state := range []State{StatePending, StateClaimed, StateApplying, State("state-from-a-newer-factory")} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			command := Command{
				TenantID:      "tenant-a",
				SessionID:     "session-a",
				CommandID:     sessionwire.CommandID("v1:command-" + string(state)),
				AcceptedOrder: 7,
				State:         state,
			}
			outcome, err := NoDispatch{}.Process(context.Background(), command)
			if err == nil {
				t.Fatalf("a %q command was accepted for dispatch", state)
			}
			var refusal *ApplyError
			if !errors.As(err, &refusal) {
				t.Fatalf("the refusal is %T, want a *ApplyError", err)
			}
			if refusal.Refusal != RefusalDispatchUnavailable {
				t.Errorf("the refusal code is %q, want %q", refusal.Refusal, RefusalDispatchUnavailable)
			}
			if refusal.CommandID != command.CommandID {
				t.Errorf("the refusal names command %q, want %q", refusal.CommandID, command.CommandID)
			}
			// THE STATE IS REPORTED BACK, NOT ZEROED. A caller diagnosing a
			// wedged session reads it off the blocked command.
			if outcome.State != state {
				t.Errorf("the outcome reports state %q, want the record's own %q", outcome.State, state)
			}
			// AND NO PREFIX IS CLAIMED. PrefixOwned true would tell the consumer
			// it may advance its cursor past a command nothing applied.
			if outcome.PrefixOwned {
				t.Errorf("the outcome claims an owned application prefix for a command that was never dispatched")
			}
		})
	}
}

// TestADispatchRefusalBlocksThePassAndWritesNothing is the refusal AS THE
// CONSUMER SEES IT, which is the only form that constrains a Host.
//
// A REFUSAL THAT THE CONSUMER TREATED AS PROGRESS WOULD BE WORSE THAN NO
// REFUSAL: the cursor would pass a command nobody applied, and no later Host
// would ever list it again. So the claim is about the durable cursor and not
// only about the error value.
func TestADispatchRefusalBlocksThePassAndWritesNothing(t *testing.T) {
	t.Parallel()

	f := newConsumerFixture(t, func(f *consumerFixture) {
		f.inbox.pages = [][]Command{{command(1, StatePending)}}
	})
	f.consumer.processor = NoDispatch{}

	result, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Blocked == nil {
		t.Fatalf("the pass did not block; it reported %+v", result)
	}
	if result.Blocked.AcceptedOrder != 1 {
		t.Errorf("the pass blocked at order %d, want 1", result.Blocked.AcceptedOrder)
	}
	var refusal *ApplyError
	if !errors.As(result.Blocked.Cause, &refusal) || refusal.Refusal != RefusalDispatchUnavailable {
		t.Errorf("the pass blocked with %v, want a dispatch refusal", result.Blocked.Cause)
	}
	if result.Consumed != 0 {
		t.Errorf("the pass consumed %d records, want 0", result.Consumed)
	}
	if saved := f.cursors.written(); len(saved) != 0 {
		t.Errorf("the pass wrote the durable cursor %v; a refused dispatch advances nothing", saved)
	}

	// THE POSITIVE CONTROL, through the SAME consumer, the SAME inbox and the
	// SAME cursor. A terminal record is consumed and the cursor IS written, so
	// "nothing was written" above is a statement about the refusal rather than
	// about a consumer that cannot write, an inbox that lists nothing or a
	// cursor double that records nothing.
	f.inbox.pages = [][]Command{{command(1, StateApplied)}}
	control, err := f.consumer.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("the control pass: %v", err)
	}
	if control.Blocked != nil {
		t.Fatalf("the control pass blocked at %+v; it must consume a terminal record", control.Blocked)
	}
	if control.Consumed != 1 {
		t.Errorf("the control pass consumed %d records, want 1", control.Consumed)
	}
	if saved := f.cursors.written(); len(saved) != 1 || saved[0].Order != 1 {
		t.Fatalf("the control pass wrote %v, want exactly [1]; the cursor probe above cannot distinguish no write from a broken probe", saved)
	}
}
