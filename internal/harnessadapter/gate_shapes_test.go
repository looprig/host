package harnessadapter

import (
	"testing"

	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/session"
)

// ---------------------------------------------------------------------------
// Which controller shapes survive bind's fourth gate
// ---------------------------------------------------------------------------
//
// THIS FILE EXISTS BECAUSE A SENTENCE IN bind WAS WRONG FOUR TIMES RUNNING, and
// each time the repair was reasoning where a construction was available. bind's
// first three gates are type assertions, which any typed nil passes without
// running code; the fourth CALLS CommittedPublicEvents. So whether a typed-nil
// controller reaches a published route turns on ONE property of that method:
// whether answering it touches the receiver.
//
// The shapes below enumerate the ways a real controller can answer, and the
// table records what each one does. It is a measurement rather than a claim,
// which is the whole point: an earlier comment asserted that "any forwarding
// wrapper" is the shape that survives, and a forwarding wrapper is precisely one
// that CANNOT — it must read its own field to forward.
//
// THE CAPABILITY IS CALLED DIRECTLY, NOT THROUGH bind, and that is deliberate.
// bind refuses every typed nil at its first line, so a probe routed through it
// reports the guard for all five shapes and discriminates nothing. Calling the
// method is what isolates the property the guard exists to protect against; a
// first version of this probe went through bind, reported five identical
// refusals, and would have supported any sentence at all.

// realSessionShape mirrors sessionruntime.Session: its answer reads a struct
// field before deciding. This is the released type's shape.
type realSessionShape struct {
	hub *struct{ supported bool }
	nilSessionController
}

func (s *realSessionShape) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	if !s.hub.supported {
		return nil, false
	}
	return s, true
}

func (s *realSessionShape) SubscribeCommittedPublicEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errNotImplemented
}

// constAnswerShape answers from a constant and touches nothing.
type constAnswerShape struct{ nilSessionController }

func (*constAnswerShape) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return committedPart{}, true
}

// forwardingWrapperShape delegates to a wrapped session, which is what a
// decorator, a metrics wrapper or a test spy does.
type forwardingWrapperShape struct {
	inner session.CommittedPublicEventProvider
	nilSessionController
}

func (w *forwardingWrapperShape) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return w.inner.CommittedPublicEvents()
}

// embeddingWrapperShape promotes the capability from an embedded interface,
// which is the other way a wrapper is written.
type embeddingWrapperShape struct {
	session.CommittedPublicEventProvider
	controllerBase
	idlePart
	livenessPart
	releaserPart
}

// selfSourceShape returns itself unconditionally. It is sessionruntime.Session's
// own shape MINUS the field read, and it is the one that survives.
type selfSourceShape struct{ nilSessionController }

func (w *selfSourceShape) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return w, true
}

func (w *selfSourceShape) SubscribeCommittedPublicEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errNotImplemented
}

// TestOnlyAReceiverFreeAnswerSurvivesTheFourthGate records which typed-nil
// controller shapes can answer bind's fourth gate and which die in it.
//
// THE SPLIT IS THE PROPERTY, not the taxonomy: three of these five read the
// receiver on the way to an answer and all three panic; the two that do not
// answer. Nothing about "wrapper" or "real session" predicts it — a forwarding
// wrapper and the released session fail together, and a wrapper that returns
// itself succeeds alongside a constant.
func TestOnlyAReceiverFreeAnswerSurvivesTheFourthGate(t *testing.T) {
	for _, tt := range []struct {
		name        string
		controller  session.CommittedPublicEventProvider
		touchesSelf bool
	}{
		{name: "the released session's shape, which reads s.hub", controller: (*realSessionShape)(nil), touchesSelf: true},
		{name: "a forwarding wrapper, which reads w.inner", controller: (*forwardingWrapperShape)(nil), touchesSelf: true},
		{name: "an embedding wrapper, which reads the promoted field", controller: (*embeddingWrapperShape)(nil), touchesSelf: true},
		{name: "a constant answer", controller: (*constAnswerShape)(nil), touchesSelf: false},
		{name: "an answer returning the receiver unconditionally", controller: (*selfSourceShape)(nil), touchesSelf: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, available, panicked := callCapability(tt.controller)

			if tt.touchesSelf {
				if !panicked {
					t.Fatalf("a shape that reads the receiver answered (%v, %t) on a typed nil instead of panicking", source, available)
				}
				return
			}
			if panicked {
				t.Fatal("a shape that touches nothing panicked on a typed nil")
			}
			// IT MUST ALSO PASS THE GATE, not merely survive the call. bind
			// refuses on either result, so a shape answering (nil, false) would
			// be stopped there and could never reach a published route — which is
			// the distinction the previous fixture got wrong.
			if !available || source == nil {
				t.Fatalf("the answer is (%v, %t), which bind refuses; this shape cannot reach a route", source, available)
			}
		})
	}
}

// callCapability runs the fourth gate's call the way bind runs it, reporting a
// nil-receiver panic instead of letting it abort the package.
func callCapability(provider session.CommittedPublicEventProvider) (source session.CommittedPublicEventSource, available, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	source, available = provider.CommittedPublicEvents()
	return source, available, false
}
