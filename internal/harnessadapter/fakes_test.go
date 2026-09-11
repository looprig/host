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

// leaseEpochPart is the RUNTIME's journal grant, the fourth assertion-discovered
// capability. Its base is 7 and residency's residency-lease fake mints from 1000,
// so a value that crossed the domains is a visibly wrong number rather than a
// coincidence.
type leaseEpochPart struct {
	epoch uint64
	held  bool
}

func (p leaseEpochPart) LeaseEpoch() (uint64, bool) { return p.epoch, p.held }

// fullController has every capability Host requires.
type fullController struct {
	controllerBase
	idlePart
	livenessPart
	releaserPart
	committedPart
	leaseEpochPart
}

func newFullController(subscription event.Subscription, filters *[]event.EventFilter, released *int) *fullController {
	return &fullController{
		controllerBase: controllerBase{id: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")},
		livenessPart:   livenessPart{done: make(chan struct{})},
		releaserPart:   releaserPart{released: released},
		committedPart:  committedPart{available: true, subscription: subscription, filters: filters},
		leaseEpochPart: leaseEpochPart{epoch: 7, held: true},
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

// BOTH METHODS ARE NIL-RECEIVER-SAFE, and that is a property of the FIXTURE
// rather than of the subject. The typed-nil row hands a (*fakeLauncher)(nil)
// to the adapter; if isNil ever stopped catching it the call would land here,
// and a receiver that dereferenced a field would die by SIGSEGV — a kill by
// panic, which is not an assertion kill, and which aborts the binary so every
// remaining test in the package goes unrun. Returning an error instead lets the
// same mutation fail on `errors.Is(err, ErrNoRig)` and leaves the suite intact.
func (l *fakeLauncher) NewSession(context.Context, ...rig.SessionOption) (session.SessionController, error) {
	if l == nil {
		return nil, errNilLauncher
	}
	if l.created != nil {
		*l.created++
	}
	if l.err != nil {
		return nil, l.err
	}
	return l.controller, nil
}

func (l *fakeLauncher) RestoreSession(_ context.Context, id uuid.UUID) (session.SessionController, error) {
	if l == nil {
		return nil, errNilLauncher
	}
	if l.restored != nil {
		*l.restored = append(*l.restored, id)
	}
	if l.err != nil {
		return nil, l.err
	}
	return l.controller, nil
}

// errNilLauncher is what a typed-nil launcher answers instead of panicking. It
// is deliberately NOT ErrNoRig: the test asserts the adapter's own refusal, so a
// fixture answering with the same sentinel would make the assertion pass for the
// wrong reason.
var errNilLauncher = errors.New("harnessadapter_test: this launcher is a typed nil and should never have been called")

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

// nilSessionController is a controller whose only instance is a typed nil.
//
// Every method has a POINTER receiver and none of them dereferences it, so a
// (*nilSessionController)(nil) satisfies session.SessionController and all four
// capability interfaces while being nothing at all. That is exactly the value
// rig.newSession produces if its lifecycle ever reports success with no session,
// and it is the only construction that can tell a nil-interface check apart from
// a real one.
type nilSessionController struct{}

func (*nilSessionController) SessionID() uuid.UUID    { return uuid.UUID{} }
func (*nilSessionController) ActiveLoop() loop.Handle { return nil }
func (*nilSessionController) Loop(uuid.UUID) (loop.Handle, bool) {
	return nil, false
}
func (*nilSessionController) Submit(context.Context, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (*nilSessionController) SubmitToLoop(context.Context, uuid.UUID, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (*nilSessionController) Compact(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (*nilSessionController) CompactToLoop(context.Context, uuid.UUID) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}
func (*nilSessionController) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errNotImplemented
}
func (*nilSessionController) RespondGate(context.Context, gate.GateResponse) error {
	return errNotImplemented
}
func (*nilSessionController) Interrupt(context.Context) (bool, error) {
	return false, errNotImplemented
}
func (*nilSessionController) SetActiveLoop(context.Context, uuid.UUID) error {
	return errNotImplemented
}
func (*nilSessionController) LoopController(uuid.UUID) (loop.Controller, bool) { return nil, false }
func (*nilSessionController) CheckpointWorkspace(context.Context) (workspacestore.Ref, error) {
	return "", errNotImplemented
}
func (*nilSessionController) RestoreWorkspace(context.Context, workspacestore.Ref) error {
	return errNotImplemented
}
func (*nilSessionController) Shutdown(context.Context) error { return errNotImplemented }

// The four capabilities, so a typed nil passes every gate in bind.
//
// THE FOURTH ONE ANSWERS, and that is the difference between demonstrating the
// mechanism and tripping an earlier check. bind's first three gates are type
// assertions, which a typed nil satisfies without any method running; the fourth
// one CALLS the provider and reads both of its results. A fixture answering
// (nil, false) there is refused as INCAPABLE, so a bind missing its typed-nil
// guard would still fail — but naming a missing capability rather than an absent
// session, and the row would pass while proving nothing about the guard it
// exists for.
//
// It answers because that is the REACHABLE half of the real hazard. A
// *sessionruntime.Session typed nil panics at this gate, because the released
// method reads the hub struct field; a controller whose
// capability answers without dereferencing — which is any forwarding wrapper —
// passes all four gates and is bound. See the comment on bind.
func (*nilSessionController) WaitIdle(context.Context) error         { return errNotImplemented }
func (*nilSessionController) Done() <-chan struct{}                  { return nil }
func (*nilSessionController) ReleaseResidency(context.Context) error { return errNotImplemented }
func (*nilSessionController) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	return committedPart{}, true
}

var (
	_ session.SessionController            = (*nilSessionController)(nil)
	_ session.IdleWaiter                   = (*nilSessionController)(nil)
	_ session.Liveness                     = (*nilSessionController)(nil)
	_ session.Releaser                     = (*nilSessionController)(nil)
	_ session.CommittedPublicEventProvider = (*nilSessionController)(nil)
)
