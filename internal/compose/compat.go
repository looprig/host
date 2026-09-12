package compose

import (
	"context"
	"errors"

	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// CompatibilityOutcome is why a synchronous compatibility wait returned.
//
// IT IS FIVE VALUES BECAUSE THE WAIT HAS FIVE ENDINGS and a caller acts
// differently on each. A boolean, or an error-or-nil, would fold "the session
// finished its work" together with "the session is waiting for a human at a
// gate" — and a legacy caller that resubmitted on the second would be answering
// a question the session is still asking.
type CompatibilityOutcome string

const (
	// CompatibilityIdle is durable whole-session idle: no work in flight.
	CompatibilityIdle CompatibilityOutcome = "idle"

	// CompatibilityGateBoundary is a resident gate wait. The session has not
	// finished; it is blocked on an answer this Host is not the source of.
	CompatibilityGateBoundary CompatibilityOutcome = "gate_boundary"

	// CompatibilityFailed is the runtime stopping answering, or this Host's
	// residency grant being lost.
	CompatibilityFailed CompatibilityOutcome = "failed"

	// CompatibilityStopped is this Host being stopped underneath the wait.
	CompatibilityStopped CompatibilityOutcome = "stopped"

	// CompatibilityTimedOut is the configured bound expiring.
	CompatibilityTimedOut CompatibilityOutcome = "timed_out"
)

// ErrNotResident is what a compatibility wait reports for a session this Host
// does not hold.
var ErrNotResident = errors.New("compose: this Host holds no residency for that session")

// AwaitSessionIdle is the synchronous compatibility wait of 04-host.md's step 3.
//
// IT IS NOT ON THE HOSTLINK COMMAND PATH AND MUST NOT REACH IT. A HostLink
// command delivery acknowledges PICKUP AND STATUS — the RPC's whole contract is
// that the command was accepted for application — and a delivery that blocked
// until the session went idle would make the acknowledgement a statement about
// a different event, hold a transport RPC open for the length of a model turn,
// and reintroduce the request-scoped session lifetime §9 exists to remove. This
// exists for a caller that has not yet been ported off a synchronous API, and
// TestTheHostLinkCommandPathDoesNotWaitForIdle is what keeps the two apart.
//
// THE FIVE ENDINGS ARE RACED, NOT ORDERED. Whichever lands first wins, and no
// ending is preferred: preferring one would mean a session that went idle and
// was then lost could report either, depending on how a select was written
// rather than on what happened.
//
// THE GATE BOUNDARY NEEDS A WorkStates SOURCE AND NOTHING IN THIS MODULE HAS
// ONE. A resident gate wait lives inside the runtime and department.Runtime
// exposes no reader for it, so a composition built without that optional seam
// can never return CompatibilityGateBoundary. That is asserted rather than
// left as a sentence.
func (s *Service) AwaitSessionIdle(ctx context.Context, key registry.Key) (CompatibilityOutcome, error) {
	s.mu.Lock()
	held, present := s.sessions[key]
	stopping := s.stopping
	s.mu.Unlock()
	if !present {
		return "", ErrNotResident
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	idle := make(chan error, 1)
	go func() { idle <- held.runtime.WaitIdle(waitCtx) }()

	deadline := s.options.Clock.After(s.options.CompatibilityTimeout)
	for {
		poll := s.options.Clock.After(s.options.WorkPoll)
		select {
		case err := <-idle:
			if err != nil {
				return CompatibilityFailed, err
			}
			return CompatibilityIdle, nil
		case <-held.runtime.Done():
			return CompatibilityFailed, nil
		case <-held.Lost():
			return CompatibilityFailed, nil
		case <-stopping:
			return CompatibilityStopped, nil
		case <-ctx.Done():
			return CompatibilityStopped, ctx.Err()
		case <-deadline:
			return CompatibilityTimedOut, nil
		case <-poll:
			if s.atGate(key) {
				return CompatibilityGateBoundary, nil
			}
		}
	}
}

// atGate reports whether the optional work-state source says this session is
// waiting at a gate.
//
// A COMPOSITION WITHOUT THE SOURCE ANSWERS FALSE, always, which is why the gate
// boundary is unreachable there. That is the correct absence rather than a
// default: inventing "not at a gate" from no information would be the same
// mistake as reporting a memory budget of zero for an unknown one.
func (s *Service) atGate(key registry.Key) bool {
	if s.options.WorkStates == nil {
		return false
	}
	state, known := s.options.WorkStates.WorkState(key)
	return known && state == residency.WorkStateGateWaiting
}
