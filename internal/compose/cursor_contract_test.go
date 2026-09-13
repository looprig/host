package compose

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// FINDING C-1: the cursor read that could not be answered
// ---------------------------------------------------------------------------
//
// sessionstore v0.7.0 REFUSES a session with no disposition catalog — absent, or
// bound to another protocol — rather than answering "zero" about it, and Host's
// commands.Cursors documented the opposite. The seam's doc has moved to meet the
// store; the two call sites had to be reconciled with it and they were reconciled
// in OPPOSITE directions, which is the whole of the finding:
//
//   - Consumer.loadCursor PROPAGATES the error, and that was already right. A
//     consumer that read zero from a refusal would list from the head of a stream
//     it has no authority over and re-drive every command in it.
//
//   - Service.cursorFor SWALLOWED it and answered zero, and that was wrong. Its
//     reasoning — "zero is conservative, because reporting work makes the session
//     NOT be released, which is the safe side" — holds for a TRANSIENT read
//     failure and does not hold for a PERMANENT refusal. A session whose
//     disposition catalog this store will never vouch for would report accepted
//     work on every warm-release attempt, forever, and be held resident by a
//     Host that cannot consume a single command in it. That is not the safe
//     side; it is a resident session leak with nothing naming the cause.
//
// The warm releaser already has the right behaviour for an unreadable inbox: it
// ABORTS AND NAMES THE STEP, with the failure attached, so the reason reaches an
// operator. Propagating simply lets it do its job.

// refusingCursors is a cursor reader that refuses, as the released store does
// for a session with no disposition catalog.
//
// IT IS A DOUBLE THAT CAN REFUSE, which fixture_test.go's fakeCursors could not.
// A fake that is unable to produce its dependency's refusal is looser than the
// dependency, and a test written against it establishes nothing about the case
// the dependency actually has.
type refusingCursors struct {
	err error
}

func (c refusingCursors) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	return 0, c.err
}

func (c refusingCursors) SaveCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64, uint64) error {
	return c.err
}

var _ commands.Cursors = refusingCursors{}

// countingInbox records whether it was asked, and from what bound.
type countingInbox struct {
	bounds  []uint64
	records []commands.Command
}

func (i *countingInbox) ListOrdered(
	_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, afterOrder uint64, _ int,
) ([]commands.Command, error) {
	i.bounds = append(i.bounds, afterOrder)
	return append([]commands.Command(nil), i.records...), nil
}

var _ commands.Inbox = (*countingInbox)(nil)

var errCursorRefused = errors.New("this session has no disposition catalog this store will vouch for")

// TestAcceptedWorkPropagatesACursorRefusalRatherThanReadingItAsZero is C-1's
// ruling, made executable.
func TestAcceptedWorkPropagatesACursorRefusalRatherThanReadingItAsZero(t *testing.T) {
	t.Parallel()

	inbox := &countingInbox{}
	service := &Service{options: Options{
		Inbox:   inbox,
		Cursors: refusingCursors{err: errCursorRefused},
	}}
	key := registry.Key{TenantID: "tenant-a", SessionID: "session-a"}

	accepted, err := service.AcceptedWork(context.Background(), key)
	if !errors.Is(err, errCursorRefused) {
		t.Fatalf("AcceptedWork = (%v, %v), want the cursor's refusal", accepted, err)
	}
	if accepted {
		t.Error("AcceptedWork reported accepted work alongside a refusal; a failed read must report nothing")
	}

	// THE INBOX IS NOT ASKED AT ALL. This is the half that distinguishes
	// propagating from swallowing-then-failing-later: a cursor that cannot be
	// read leaves no bound to list from, and listing from zero is exactly the
	// wrong answer this test exists to prevent.
	if len(inbox.bounds) != 0 {
		t.Errorf("the inbox was listed from %v after an unreadable cursor; zero is not a position", inbox.bounds)
	}
}

// TestAcceptedWorkStillReadsTheCursorAsTheBoundWhenItCanBeRead is the CONTROL.
//
// Without it the assertion above is satisfied by an AcceptedWork that fails for
// every session, and the cursor's whole job — being the bound the re-read starts
// from — would be untested.
func TestAcceptedWorkStillReadsTheCursorAsTheBoundWhenItCanBeRead(t *testing.T) {
	t.Parallel()

	inbox := &countingInbox{records: []commands.Command{{CommandID: "command-a", AcceptedOrder: 42}}}
	cursors := &fakeCursors{cursor: 41}
	service := &Service{options: Options{Inbox: inbox, Cursors: cursors}}
	key := registry.Key{TenantID: "tenant-a", SessionID: "session-a"}

	accepted, err := service.AcceptedWork(context.Background(), key)
	if err != nil {
		t.Fatalf("AcceptedWork: %v", err)
	}
	if !accepted {
		t.Error("AcceptedWork reported no work for an inbox holding a record above the cursor")
	}
	if len(inbox.bounds) != 1 || inbox.bounds[0] != 41 {
		t.Fatalf("the inbox was listed from %v, want exactly [41] — the cursor is the bound", inbox.bounds)
	}
}
