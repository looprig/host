package compose

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// derivedWorkStates is the work-state source a composition derives from what
// it already holds for each resident session: the runtime's own idle probe and
// the gate publisher's fold of the runtime journal.
//
// IT EXISTS BECAUSE NOTHING ELSE COULD SUPPLY ONE. WorkStates was an optional
// seam no composition wired, so the warm releaser was never told a session was
// idle and a composed Host never warm-released anything: an idle pooled session
// held its capacity until the Host stopped. What the seam said was missing — "a
// reader of the runtime's gate state that department.Runtime does not expose" —
// has existed since host v0.4.0 in the gate publisher, which folds the same
// journal the projection Factory reads is converged on.
//
// A SESSION IS IDLE ONLY WHEN EVERY SOURCE SAYS SO, and each answer it cannot
// give is reported as work, never as idle:
//
//   - no gate publisher, or one that has not folded yet: working. A session
//     whose gates this Host cannot see may have a human waiting at one.
//   - the fold holds an open gate: gate_waiting. 04-host.md: "A waiting
//     resident gate is not whole-session idle and therefore is not
//     warm-evicted." That includes a gate restored open under an interrupted
//     turn, where the runtime itself is idle.
//   - the runtime is not idle NOW: working.
//
// Pending and in-flight commands are not this source's question. The warm
// releaser's step 1 re-reads the durable inbox from the consumption cursor
// before it takes anything, and a command that is accepted, claimed or applying
// is above the cursor, so a release with work outstanding aborts there.
type derivedWorkStates struct {
	service *Service
}

var _ WorkStates = derivedWorkStates{}

// WorkState reports what one resident session is doing.
func (d derivedWorkStates) WorkState(key registry.Key) (residency.WorkState, bool) {
	held := d.service.residentFor(key)
	if held == nil {
		return "", false
	}
	if held.work == nil || held.work.gates == nil {
		return residency.WorkStateWorking, true
	}
	open, _, folded := held.work.gates.Activity()
	switch {
	case !folded:
		return residency.WorkStateWorking, true
	case open > 0:
		return residency.WorkStateGateWaiting, true
	}
	if !idleNow(held.runtime) {
		return residency.WorkStateWorking, true
	}
	return residency.WorkStateIdle, true
}

// activity reports the journal position the session's gate fold has reached.
// The sampler compares two readings: a position that moved between two idle
// samples means the runtime worked in between, and the countdown restarts.
func (d derivedWorkStates) activity(key registry.Key) (uint64, bool) {
	held := d.service.residentFor(key)
	if held == nil || held.work == nil || held.work.gates == nil {
		return 0, false
	}
	_, position, folded := held.work.gates.Activity()
	return position, folded
}

// activityReporter is the derived source's second question, which only the
// sampler asks.
type activityReporter interface {
	activity(registry.Key) (uint64, bool)
}

// idleNow asks the runtime whether it is idle at this instant, without waiting.
//
// IT CALLS WaitIdle WITH A CONTEXT THAT IS ALREADY DONE. department.IdleWaiter
// requires an idle runtime to answer nil without waiting, which harness's hub
// does on its fast path before it looks at the context; a busy one registers a
// waiter and returns the context's error at once. Any error — cancellation, a
// stopped or faulted session — is "not idle", so a runtime that consults its
// context first is simply never warm-released rather than released wrongly.
func idleNow(runtime interface{ WaitIdle(context.Context) error }) bool {
	if runtime == nil {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return runtime.WaitIdle(ctx) == nil
}

// residentFor returns the composition's handle on a resident session, or nil.
func (s *Service) residentFor(key registry.Key) *resident {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[key]
}

// warmSamples reports whether the sampler feeds the warm releaser at all.
//
// A DEDICATED HOST WITH A DERIVED SOURCE DOES NOT WARM-RELEASE. A dedicated
// Host's one session is ended by its controller's drain-before-delete, and a
// warm release would leave its workload running with nothing resident while
// placement, the controller and the durable desire all still describe the
// session as living there — a cross-module state no released Factory or
// controller has been proven against. The derived source still answers the
// compatibility wait's gate boundary there. A caller-supplied source keeps its
// pre-existing behaviour on both placements.
func (s *Service) warmSamples() bool {
	return !(s.options.DeriveWorkStates && s.options.Host.Placement() == sessionwire.HostPlacementDedicated)
}
