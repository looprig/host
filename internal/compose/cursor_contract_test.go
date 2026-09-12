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

// ---------------------------------------------------------------------------
// The pending journal grant, and the guard that lives in this package
// ---------------------------------------------------------------------------
//
// O7.2 left this open and flagged it: stashGrant keyed pending journal grants by
// registry.Key and a SECOND opening for one key would OVERWRITE the first,
// stranding a grant nobody could release. It is unreachable today because
// residency.Manager.Attach's per-key slot guarantees one opening per key — but
// THE ONLY GUARD LIVED IN A DIFFERENT PACKAGE, which is exactly the shape that
// let O7.1's cross-tenant bypass survive its own guard: a caller's discipline
// asserted somewhere else, with the object that depends on it asserting nothing.
//
// It is closed rather than restated. stashGrant refuses the second grant, and
// CommitOpeningFence releases what it has just taken rather than holding a grant
// it cannot hand on. Both halves matter: refusing without releasing would swap a
// stranded grant for a leaked one.

// recordingGrant is a journal grant that counts its own release.
type recordingGrant struct {
	name     string
	released int
}

func (g *recordingGrant) AppendApplicationPrefix(
	context.Context, sessionwire.TenantID, sessionwire.SessionID, commands.Prefix,
) error {
	return nil
}

func (g *recordingGrant) Release(context.Context) error {
	g.released++
	return nil
}

var _ JournalGrant = (*recordingGrant)(nil)

// TestASecondOpeningForOneSessionIsRefusedAndItsGrantReleased is the close.
func TestASecondOpeningForOneSessionIsRefusedAndItsGrantReleased(t *testing.T) {
	t.Parallel()

	first := &recordingGrant{name: "first"}
	second := &recordingGrant{name: "second"}
	grants := []*recordingGrant{first, second}
	opened := 0

	service := &Service{options: Options{
		OpenSession: func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (JournalGrant, error) {
			grant := grants[opened]
			opened++
			return grant, nil
		},
	}}
	fencer := service.journal()

	if err := fencer.CommitOpeningFence(context.Background(), "tenant-a", "session-a"); err != nil {
		t.Fatalf("the first opening fence: %v", err)
	}

	// THE SECOND OPENING. It is refused, by name.
	err := fencer.CommitOpeningFence(context.Background(), "tenant-a", "session-a")
	var refusal *PendingGrantHeldError
	if !errors.As(err, &refusal) {
		t.Fatalf("the second opening fence = %v, want a PendingGrantHeldError", err)
	}
	if refusal.Key.SessionID != "session-a" {
		t.Errorf("the refusal names session %q, want session-a", refusal.Key.SessionID)
	}

	// THE FIRST GRANT SURVIVES, which is the property the overwrite destroyed:
	// the ownership that follows the FIRST opening must still find its grant.
	stashed, held := service.takeGrant(registry.Key{TenantID: "tenant-a", SessionID: "session-a"})
	if !held {
		t.Fatal("the stash holds no grant at all after a refused second opening")
	}
	if stashed != JournalGrant(first) {
		t.Fatal("the stash holds the SECOND grant; the overwrite this test exists to prevent happened anyway")
	}

	// AND THE SECOND GRANT IS NOT LEAKED. Refusing without releasing would trade
	// a stranded grant for one nobody ever hands back, which is the same defect
	// with a different name.
	if second.released != 1 {
		t.Errorf("the refused grant was released %d times, want once", second.released)
	}
	if first.released != 0 {
		t.Errorf("the surviving grant was released %d times, want none", first.released)
	}
}

// TestOneOpeningPerSessionStillStashesItsGrant is the CONTROL.
//
// Without it the refusal above is satisfied by a stashGrant that refuses
// everything, and the ordinary attach — one opening, one grant, taken by the
// ownership that follows — would be untested here.
func TestOneOpeningPerSessionStillStashesItsGrant(t *testing.T) {
	t.Parallel()

	grant := &recordingGrant{name: "only"}
	service := &Service{options: Options{
		OpenSession: func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (JournalGrant, error) {
			return grant, nil
		},
	}}

	if err := service.journal().CommitOpeningFence(context.Background(), "tenant-a", "session-a"); err != nil {
		t.Fatalf("CommitOpeningFence: %v", err)
	}
	stashed, held := service.takeGrant(registry.Key{TenantID: "tenant-a", SessionID: "session-a"})
	if !held || stashed != JournalGrant(grant) {
		t.Fatalf("takeGrant = (%v, %v), want the grant the opening produced", stashed, held)
	}
	if grant.released != 0 {
		t.Errorf("a legitimate opening released its own grant %d times", grant.released)
	}

	// A DIFFERENT SESSION IS NOT THE SAME KEY, so the refusal is keyed on the
	// session and not on "any grant is pending".
	other := &recordingGrant{name: "other"}
	service.options.OpenSession = func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (JournalGrant, error) {
		return other, nil
	}
	if err := service.journal().CommitOpeningFence(context.Background(), "tenant-a", "session-b"); err != nil {
		t.Fatalf("opening a second SESSION was refused: %v", err)
	}
	if other.released != 0 {
		t.Errorf("a second session's grant was released %d times, want none", other.released)
	}
}
