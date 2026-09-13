package department

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// This file is INTERNAL because adaptRigSession is, and the property under test
// is exactly what that function does with an OPTIONAL capability. Reaching it
// through the exported launch path would drag a rig, a target and a department
// into a test about one type assertion.

var closerRigSessionUUID = uuid.MustParse("3f2b7c11-1111-4222-8333-444455556666")

// A RUNTIME THAT OFFERS NO RECOVERY CLOSURE REFUSES AT THE CALL, and says so
// with the sentinel rather than by not satisfying the interface.
//
// THE ASSERTION CANNOT ANSWER THIS QUESTION AND THAT IS THE WHOLE POINT. A Go
// wrapper that declares CloseAttempt satisfies AttemptCloser for every runtime
// it wraps, so `runtime.(AttemptCloser)` succeeds even for a session that offers
// nothing — and embedding the failed assertion's nil interface instead would
// satisfy it too and then PANIC at the first call. Refusing here is the only
// shape that lets a caller tell the two apart, and the sentinel is what it
// reads.
//
// IT IS NOT AN IncapableRuntimeError. That type is what an ATTACH refuses with,
// and an absent closer refuses no attach: the runtime is fully usable and one
// recovery path is unavailable.
func TestAnAdaptedRuntimeWithNoCloserRefusesTheClosure(t *testing.T) {
	for _, row := range []struct {
		name    string
		session RigSession
		refused bool
	}{
		{"a session offering no closure", capableSessionWithoutCloser{}, true},
		{"a session offering one", capableSessionWithCloser{}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			runtime, err := adaptRigSession("session-a", "agent-a", row.session)
			if err != nil {
				t.Fatalf("adaptRigSession: %v", err)
			}
			// THE ASSERTION SUCCEEDS IN BOTH ROWS. Asserting otherwise is the
			// mistake this test exists to document.
			closer, ok := any(runtime).(AttemptCloser)
			if !ok {
				t.Fatal("the adapted runtime is not a department.AttemptCloser; a caller can no longer reach a closure at all")
			}
			err = closer.CloseAttempt(t.Context(), "command-a",
				uuid.MustParse("11111111-2222-3333-4444-555555555555"), "input", "attempt-1", 1)
			if row.refused {
				if !errors.Is(err, ErrNoAttemptCloser) {
					t.Fatalf("CloseAttempt = %v, want ErrNoAttemptCloser", err)
				}
				return
			}
			if errors.Is(err, ErrNoAttemptCloser) {
				t.Fatalf("CloseAttempt refused a runtime that offers a closure: %v", err)
			}
			if !errors.Is(err, errClosureForwarded) {
				t.Fatalf("CloseAttempt = %v, want the runtime's own closer to have been reached", err)
			}
		})
	}
}

var errClosureForwarded = errors.New("department_test: the runtime's own closer was reached")

// capableSession satisfies every REQUIRED capability and nothing optional. It is
// declared here rather than reused from internal/testkit because that package
// imports this one, so an internal test cannot name it without a cycle.
type capableSession struct{}

func (capableSession) ID() uuid.UUID                          { return closerRigSessionUUID }
func (capableSession) WaitIdle(context.Context) error         { return nil }
func (capableSession) Done() <-chan struct{}                  { return nil }
func (capableSession) ReleaseResidency(context.Context) error { return nil }
func (capableSession) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return nil, nil
}
func (capableSession) ApplyCommand(context.Context, RuntimeCommand) error { return nil }
func (capableSession) LeaseEpoch() (uint64, bool)                         { return 1, true }

// capableSessionWithoutCloser offers no optional closure capability.
type capableSessionWithoutCloser struct{ capableSession }

// capableSessionWithCloser adds the segregated closure capability.
type capableSessionWithCloser struct{ capableSession }

func (capableSessionWithCloser) CloseAttempt(
	context.Context, sessionwire.CommandID, uuid.UUID, string, string, uint64,
) error {
	return errClosureForwarded
}
