package compose

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/lifecycle"
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
// Pending and in-flight commands are not this source's question. At expiry the
// warm releaser halts the session's consumer (waiting out a pass in flight),
// re-confirms through ConfirmIdle, and re-reads the durable inbox from the
// consumption cursor; a command that is accepted, claimed or applying is above
// the cursor, so a release with work outstanding aborts there, and one admitted
// after the re-read is never claimed by this Host.
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

// activityMark is one sampled journal position, fenced by the residency
// generation it was read under.
type activityMark struct {
	generation uint64
	position   uint64
}

// markActivity records a sampled position and reports whether it moved since
// the previous sample of the same residency.
func (s *Service) markActivity(key registry.Key, generation, position uint64) bool {
	s.activityMu.Lock()
	defer s.activityMu.Unlock()
	if s.activity == nil {
		s.activity = map[registry.Key]activityMark{}
	}
	last, had := s.activity[key]
	s.activity[key] = activityMark{generation: generation, position: position}
	return had && last.generation == generation && last.position != position
}

// pruneActivity forgets the marks of sessions the last sample did not see.
func (s *Service) pruneActivity(seen map[registry.Key]bool) {
	s.activityMu.Lock()
	defer s.activityMu.Unlock()
	for key := range s.activity {
		if !seen[key] {
			delete(s.activity, key)
		}
	}
}

// ConfirmIdle is the warm releaser's re-confirmation at expiry
// (residency.WarmConfirmer).
//
// A COUNTDOWN IS ARMED BY A SAMPLE AND FIRES BETWEEN SAMPLES, so the sample
// that armed it says nothing about the instant it fires: a user's reply that
// arrived after the last sample would otherwise be taken into a release. The
// session must be idle NOW by the same source that armed it, and — where the
// source reports a journal position — the position must be the one recorded by
// the last sample. The countdown survives a sample only if that sample saw no
// movement, so "unchanged since the last sample" is "unchanged since the
// countdown was armed". Anything this cannot establish is false.
func (s *Service) ConfirmIdle(key registry.Key) bool {
	if s.options.WorkStates == nil {
		return false
	}
	state, known := s.options.WorkStates.WorkState(key)
	if !known || state != residency.WorkStateIdle {
		return false
	}
	reporter, reports := s.options.WorkStates.(activityReporter)
	if !reports {
		return true
	}
	position, ok := reporter.activity(key)
	if !ok {
		return false
	}
	held := s.residentFor(key)
	if held == nil {
		return false
	}
	s.activityMu.Lock()
	mark, had := s.activity[key]
	s.activityMu.Unlock()
	// FENCED BY GENERATION, so a mark sampled under a residency this one
	// REPLACED under the same key can never confirm it.
	return had && mark.generation == held.generation && mark.position == position
}

var _ residency.WarmConfirmer = (*Service)(nil)

// Halt stops a resident session's command consumer and waits, bounded by ctx,
// for its pass in flight (residency.ConsumptionHalter). A session with no
// consumer has nothing to halt.
func (s *Service) Halt(ctx context.Context, key registry.Key) error {
	s.mu.Lock()
	consumer, held := s.consumers[key]
	s.mu.Unlock()
	if !held {
		return nil
	}
	return consumer.Halt(ctx)
}

// Resume undoes Halt for a warm release that aborted.
//
// IT REFUSES, SILENTLY, TO UNDO A DRAIN'S HALT. A warm release runs
// concurrently with a drain until Stop stops the releaser, and every warm
// abort path resumes the consumer it halted. The halt is one flag, not a
// count, so without this a warm abort racing the drain would restart
// consumption on a session the drain is releasing — the defect the drain's
// halt exists to close.
func (s *Service) Resume(key registry.Key) {
	s.mu.Lock()
	consumer, held := s.consumers[key]
	session := s.sessions[key]
	s.mu.Unlock()
	if session != nil && session.drainHalted.Load() {
		return
	}
	if held {
		consumer.Resume()
	}
}

// GateWaiting counts the resident sessions parked at a gate, from each
// session's gate publisher fold (lifecycle.GateWaits). A session with no
// publisher, or one that has not folded yet, is not counted: this gauge
// reports gates this Host can see, and the derived work-state source already
// treats the unknown case as work.
func (s *Service) GateWaiting() uint64 {
	s.mu.Lock()
	held := make([]*resident, 0, len(s.sessions))
	for _, session := range s.sessions {
		held = append(held, session)
	}
	s.mu.Unlock()
	var waiting uint64
	for _, session := range held {
		if session.work == nil || session.work.gates == nil {
			continue
		}
		if open, _, folded := session.work.gates.Activity(); folded && open > 0 {
			waiting++
		}
	}
	return waiting
}

var _ lifecycle.GateWaits = (*Service)(nil)

var _ residency.ConsumptionHalter = (*Service)(nil)
