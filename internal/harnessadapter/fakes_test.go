package harnessadapter

import (
	"context"
	"errors"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/runtimecommand"
	"github.com/looprig/harness/pkg/session"
	"github.com/looprig/harness/pkg/workspacestore"

	"github.com/looprig/host/department"
)

// ---------------------------------------------------------------------------
// A harness session controller, assembled from the capabilities under test
// ---------------------------------------------------------------------------
//
// THE BASE IS EVERY SessionController METHOD AND NOTHING ELSE, which is what
// makes the capability tests mean something: a fake that carried WaitIdle
// unconditionally could not express the session bind is supposed to refuse.
// Each capability is added by embedding a part, exactly as harness adds them to
// its own concrete session, so the assertions in bind are asked the same
// question they will be asked in production.

type controllerBase struct {
	id uuid.UUID
}

func (c controllerBase) SessionID() uuid.UUID             { return c.id }
func (controllerBase) ActiveLoop() loop.Handle            { return nil }
func (controllerBase) Loop(uuid.UUID) (loop.Handle, bool) { return nil, false }
func (controllerBase) Submit(context.Context, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (controllerBase) SubmitToLoop(context.Context, uuid.UUID, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (controllerBase) Compact(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (controllerBase) CompactToLoop(context.Context, uuid.UUID) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (controllerBase) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errNotImplemented
}
func (controllerBase) RespondGate(context.Context, gate.GateResponse) error { return errNotImplemented }
func (controllerBase) Interrupt(context.Context) (bool, error)              { return false, errNotImplemented }
func (controllerBase) SetActiveLoop(context.Context, uuid.UUID) error       { return errNotImplemented }
func (controllerBase) LoopController(uuid.UUID) (loop.Controller, bool)     { return nil, false }
func (controllerBase) CheckpointWorkspace(context.Context) (workspacestore.Ref, error) {
	return "", errNotImplemented
}
func (controllerBase) RestoreWorkspace(context.Context, workspacestore.Ref) error {
	return errNotImplemented
}
func (controllerBase) Shutdown(context.Context) error { return errNotImplemented }

var errNotImplemented = errors.New("harnessadapter_test: the fake controller implements only the capabilities under test")

// The bare base is a SessionController and nothing more, which is the state
// harness's own doc describes: none of the four capabilities is on the base
// contract.
var _ session.SessionController = controllerBase{}

// idlePart, livenessPart and releaserPart are the three assertion-discovered
// capabilities.

type idlePart struct{ err error }

func (p idlePart) WaitIdle(context.Context) error { return p.err }

type livenessPart struct{ done chan struct{} }

func (p livenessPart) Done() <-chan struct{} { return p.done }

type releaserPart struct {
	err      error
	released *int
}

func (p releaserPart) ReleaseResidency(context.Context) error {
	if p.released != nil {
		*p.released++
	}
	return p.err
}

// committedPart is the PROVIDER, not the source, so a fake can answer the
// question bind is supposed to ask. `available` false is the headless case
// harness names.
type committedPart struct {
	available    bool
	subscription event.Subscription
	subscribeErr error

	filters *[]event.EventFilter
}

func (p committedPart) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	if !p.available {
		return nil, false
	}
	return p, true
}

func (p committedPart) SubscribeCommittedPublicEvents(filter event.EventFilter) (event.Subscription, error) {
	if p.filters != nil {
		*p.filters = append(*p.filters, filter)
	}
	if p.subscribeErr != nil {
		return nil, p.subscribeErr
	}
	return p.subscription, nil
}

// fullController has every capability Host requires.
type fullController struct {
	controllerBase
	idlePart
	livenessPart
	releaserPart
	committedPart
}

func newFullController(subscription event.Subscription, filters *[]event.EventFilter, released *int) *fullController {
	return &fullController{
		controllerBase: controllerBase{id: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")},
		livenessPart:   livenessPart{done: make(chan struct{})},
		releaserPart:   releaserPart{released: released},
		committedPart:  committedPart{available: true, subscription: subscription, filters: filters},
	}
}

// ---------------------------------------------------------------------------
// A runtime-command applier
// ---------------------------------------------------------------------------

type applierPart struct {
	available  bool
	nilApplier bool
	err        error
	admitted   *[]runtimecommand.Admitted

	// liveApplierWhenUnavailable makes RuntimeCommands report the capability
	// UNAVAILABLE while still handing back a usable applier. A conforming
	// provider does not do that — runtimecommand.Provider documents ok=false as
	// coming with a nil Applier — and that is exactly why the fixture exists: it
	// is the only construction that can tell whether this adapter obeys the
	// BOOLEAN or merely the nil. Trusting the nil would make the two-result form
	// decorative, and the failure it prevents (an applier that fails only after
	// Host has acknowledged the command) is the one harness says the second
	// result is for.
	liveApplierWhenUnavailable bool
}

func (p applierPart) RuntimeCommands() (runtimecommand.Applier, bool) {
	if !p.available {
		if p.liveApplierWhenUnavailable {
			return p, false
		}
		return nil, false
	}
	if p.nilApplier {
		return nil, true
	}
	return p, true
}

func (p applierPart) ApplyRuntimeCommand(_ context.Context, admitted runtimecommand.Admitted) (runtimecommand.Disposition, error) {
	if p.admitted != nil {
		*p.admitted = append(*p.admitted, admitted)
	}
	if p.err != nil {
		return runtimecommand.Disposition{}, p.err
	}
	return runtimecommand.Disposition{CommandID: admitted.CommandID, RuntimeCommandID: admitted.RuntimeCommandID}, nil
}

type applyingController struct {
	fullController
	applierPart
}

// ---------------------------------------------------------------------------
// An event subscription
// ---------------------------------------------------------------------------

type fakeSubscription struct {
	deliveries chan event.Delivery
	closed     *int
	err        error
}

func newFakeSubscription(closed *int) *fakeSubscription {
	return &fakeSubscription{deliveries: make(chan event.Delivery, 8), closed: closed}
}

func (s *fakeSubscription) Events() <-chan event.Delivery { return s.deliveries }
func (s *fakeSubscription) Close() error {
	if s.closed != nil {
		*s.closed++
	}
	return nil
}
func (s *fakeSubscription) Err() error { return s.err }

// ---------------------------------------------------------------------------
// A launcher
// ---------------------------------------------------------------------------

type fakeLauncher struct {
	controller session.SessionController
	err        error

	created  *int
	restored *[]uuid.UUID
}

func (l *fakeLauncher) NewSession(context.Context, ...rig.SessionOption) (session.SessionController, error) {
	if l.created != nil {
		*l.created++
	}
	if l.err != nil {
		return nil, l.err
	}
	return l.controller, nil
}

func (l *fakeLauncher) RestoreSession(_ context.Context, id uuid.UUID) (session.SessionController, error) {
	if l.restored != nil {
		*l.restored = append(*l.restored, id)
	}
	if l.err != nil {
		return nil, l.err
	}
	return l.controller, nil
}

// The fake is the same seam the released rig satisfies.
var _ Launcher = (*fakeLauncher)(nil)

// stubRigs hands back one launcher and records what it was asked for.
type stubRigs struct {
	launcher Launcher
	err      error

	creates  *[]department.RigCreateRequest
	restores *[]department.RigRestoreRequest
}

func (r stubRigs) RigForCreate(_ context.Context, request department.RigCreateRequest) (Launcher, error) {
	if r.creates != nil {
		*r.creates = append(*r.creates, request)
	}
	return r.launcher, r.err
}

func (r stubRigs) RigForRestore(_ context.Context, _ uuid.UUID, request department.RigRestoreRequest) (Launcher, error) {
	if r.restores != nil {
		*r.restores = append(*r.restores, request)
	}
	return r.launcher, r.err
}

// nilSourceProvider reports the capability available and hands back nothing.
// The two results of the released two-result form are independent, and a caller
// that trusted only the boolean would carry a nil source to its first subscribe.
type nilSourceProvider struct{}

func (nilSourceProvider) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return nil, true
}
