package harnessadapter

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
)

// Rigs resolves the assembled Rig one launch runs on.
//
// IT IS PER-REQUEST BECAUSE FINDING H1 LEAVES NO ALTERNATIVE. A rig's workspace
// root and resource storage are fixed by rig.Define, and rig.NewSession accepts
// neither; department.CreateRequest carries a WorkspaceRoot that Host has just
// materialized and a storage namespace the session's objects must live under.
// The only place both can be honoured is the assembly, so the composition root
// assembles per launch. A shared rig is still expressible — return the same
// pointer — and the difference is then visible rather than assumed.
type Rigs interface {
	// RigForCreate assembles the launcher a new session launches on.
	RigForCreate(context.Context, department.RigCreateRequest) (Launcher, error)

	// RigForRestore assembles the launcher a restored session relaunches on. The
	// UUID is Harness's identity for the session and is passed so an assembly can
	// refuse a session it has no state for.
	RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (Launcher, error)
}

// Launcher is the pair of released rig methods this adapter calls.
//
// It is an interface over *rig.Rig rather than the concrete type, and the
// assertion below is what makes that safe rather than merely convenient: the
// released type must satisfy it or this package does not build, so the seam
// cannot drift away from the API it stands for. What it buys is that every path
// through NewSession, RestoreSession and bind is reachable from a test, which
// *rig.Rig is not — assembling one needs loops, primers and a model adapter that
// no unit test can supply, and 460 lines went untested behind exactly that.
type Launcher interface {
	NewSession(context.Context, ...rig.SessionOption) (session.SessionController, error)
	RestoreSession(context.Context, uuid.UUID) (session.SessionController, error)
}

// The released rig is the launcher. A drift here is a build failure rather than
// a seam that quietly stops describing anything.
var _ Launcher = (*rig.Rig)(nil)

// Adapter is a department.Rig over the released rig API.
type Adapter struct {
	rigs Rigs

	leaseEpoch uint64
	decode     BlockDecoder
}

// BlockDecoder turns a command's private body into the content blocks Harness
// requires.
//
// IT IS A PARAMETER FOR FINDING H7'S REASON. Host carries the body opaque on
// purpose — department.RuntimeCommand says so in as many words — and
// runtimecommand.Admitted.Validate refuses an input command carrying no
// content.Block. Those two cannot both hold, so an encoding has to be named
// somewhere; naming it here rather than inside the adapter keeps it a
// composition decision, and leaving it unset makes an input command refuse
// rather than apply an empty turn.
type BlockDecoder func([]byte) ([]content.Block, error)

// Option configures an Adapter.
type Option func(*Adapter)

// WithLeaseEpoch supplies the session lease epoch every admitted command is
// applied under.
//
// FINDING H5: runtimecommand.Admitted refuses a zero epoch and
// department.RuntimeCommand has no member to carry one, so it arrives here
// instead of on the command. That makes the epoch a property of the BINDING
// rather than of the command, which is weaker than the durable record: an
// adapter bound at attach applies every later command under the epoch it was
// bound with, and only the store's own compare-and-swap notices if that has
// moved on.
func WithLeaseEpoch(epoch uint64) Option {
	return func(a *Adapter) { a.leaseEpoch = epoch }
}

// WithBlockDecoder supplies the decoder an input command's body is read with.
func WithBlockDecoder(decode BlockDecoder) Option {
	return func(a *Adapter) { a.decode = decode }
}

// New adapts a rig resolver into the department.Rig Host launches through.
func New(rigs Rigs, options ...Option) (*Adapter, error) {
	if rigs == nil {
		return nil, ErrNoRigs
	}
	adapted := &Adapter{rigs: rigs}
	for _, option := range options {
		if option == nil {
			return nil, ErrNilOption
		}
		option(adapted)
	}
	return adapted, nil
}

// ErrNoRigs is the refusal New returns when it is handed no resolver.
var ErrNoRigs = errors.New("harnessadapter: no rig resolver to adapt")

// ErrNilOption is the refusal New returns for a nil option.
var ErrNilOption = errors.New("harnessadapter: a nil option was supplied")

// NewSession launches a session for a create request.
func (a *Adapter) NewSession(ctx context.Context, request department.RigCreateRequest) (department.RigSession, error) {
	assembled, err := a.rigs.RigForCreate(ctx, request)
	if err != nil {
		return nil, err
	}
	if isNil(assembled) {
		return nil, ErrNoRig
	}
	controller, err := assembled.NewSession(ctx)
	if err != nil {
		return nil, err
	}
	return a.bind(controller, request.TenantID, request.SessionID)
}

// RestoreSession relaunches a session over existing durable state.
//
// FINDING H2: every member of the request is dropped, because
// rig.Rig.RestoreSession takes the UUID and nothing else. The request is still
// taken — it is the only place an assembly can see what is being restored — and
// is forwarded to RigForRestore, so the drop happens at a seam a composition can
// act on rather than silently inside this call.
func (a *Adapter) RestoreSession(
	ctx context.Context,
	id uuid.UUID,
	request department.RigRestoreRequest,
) (department.RigSession, error) {
	assembled, err := a.rigs.RigForRestore(ctx, id, request)
	if err != nil {
		return nil, err
	}
	if isNil(assembled) {
		return nil, ErrNoRig
	}
	controller, err := assembled.RestoreSession(ctx, id)
	if err != nil {
		return nil, err
	}
	return a.bind(controller, request.TenantID, request.SessionID)
}

// ErrNoRig is the refusal returned when a resolver reports success and hands
// back nothing. It is department.ErrNoRigSession one layer up, and it exists for
// the same reason: a nil rig would otherwise be dereferenced by NewSession.
var ErrNoRig = errors.New("harnessadapter: the rig resolver reported success and returned no rig")

// isNil reports whether an interface value holds nothing, in either of the two
// ways Go allows.
//
// BOTH WAYS, for department.isNilSession's measured reason. A nil INTERFACE is
// what a `return nil, nil` produces; a TYPED NIL — a non-nil Launcher holding a
// nil *rig.Rig — is what the commonest bug in the language produces, and a bare
// `== nil` walks straight past it into the dereference the guard exists to
// prevent. That defect was found by test in department and would have been
// reintroduced here the moment Rigs stopped returning a concrete pointer.
func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflected.IsNil()
	default:
		return false
	}
}

// bind discovers the capabilities Host requires on a launched controller and
// refuses one short of any of them.
//
// IT REPORTS EVERY MISSING CAPABILITY rather than the first, for
// department.adaptRigSession's reason, and it discovers them by assertion
// because that is what harness prescribes: session.IdleWaiter, Liveness and
// Releaser each document the assertion in their own doc comment, and each warns
// that a wrapper failing to forward the method silently opts its session out.
// The names asserted on are HARNESS'S, so a released signature change is a build
// failure here rather than a run-time IncapableSessionError.
func (a *Adapter) bind(
	controller session.SessionController,
	tenant sessionwire.TenantID,
	sessionID sessionwire.SessionID,
) (department.RigSession, error) {
	// BOTH WAYS, AND THIS IS THE MORE REACHABLE OF THE TWO SITES. A Launcher is
	// something the composition root constructs; a CONTROLLER arrives through
	// released harness code, and rig.newSession is the canonical typed-nil
	// producer — it assigns a concrete *sessionruntime.Session and widens it to
	// the interface on return, so a lifecycle that reported success with no
	// session hands back a non-nil interface holding a nil pointer. A bare
	// `== nil` walks straight past that.
	//
	// WHAT HAPPENS NEXT DEPENDS ON THE CONTROLLER, and the two cases are worth
	// separating because an earlier version of this comment described only the
	// second and attributed it to the first.
	//
	//   - A *sessionruntime.Session typed nil PANICS INSIDE bind, at the fourth
	//     gate below. The first three gates are type assertions, which a typed
	//     nil passes without running anything; the fourth CALLS
	//     CommittedPublicEvents, and that method reads s.hub — a struct field —
	//     so the nil receiver is dereferenced there. Nothing is published: the
	//     crash is inside Host's launch path, before any route exists.
	//   - A controller whose capability ANSWERS WITHOUT DEREFERENCING passes all
	//     four gates, and that is not a hypothetical shape — it is any forwarding
	//     wrapper, which harness's own capability docs warn callers to expect
	//     around a live session. bind then returns a usable-looking boundSession,
	//     Host publishes the residency route, and the panic arrives at the first
	//     ID() call with a resident session already advertised.
	//
	// So the guard is load-bearing in both cases and for different reasons: it
	// turns a nil dereference inside the launch path into a typed refusal, and it
	// is the only thing standing between a wrapped typed nil and a published
	// route. TestBindRefusesEveryWayALaunchHandsBackNoSession exercises the
	// second case, because it is the one a test can construct and the one whose
	// consequence outlives the call.
	if isNil(controller) {
		return nil, ErrNoSession
	}
	bound := &boundSession{
		controller: controller,
		tenant:     tenant,
		session:    sessionID,
		leaseEpoch: a.leaseEpoch,
		decode:     a.decode,
	}
	var missing []string
	if capability, ok := controller.(session.IdleWaiter); ok {
		bound.idle = capability
	} else {
		missing = append(missing, "session.IdleWaiter")
	}
	if capability, ok := controller.(session.Liveness); ok {
		bound.live = capability
	} else {
		missing = append(missing, "session.Liveness")
	}
	if capability, ok := controller.(session.Releaser); ok {
		bound.releaser = capability
	} else {
		missing = append(missing, "session.Releaser")
	}

	// THE TWO-RESULT FORM, AND NOT A BARE ASSERTION ON THE SOURCE. Harness
	// publishes both session.CommittedPublicEventSource and the
	// session.CommittedPublicEventProvider that reports whether the source will
	// work, and its doc says why: the capability is a property of the session's
	// PERSISTENCE and not of its Go type, so ok is false for a headless session
	// and for one over a journal predating the committed-bytes seam. The live
	// runtime declares SubscribeCommittedPublicEvents unconditionally, so an
	// assertion on the SOURCE succeeds for every real session and never asks the
	// question — Host would publish the residency route, a client would attach
	// and start advancing a cursor, and only then would the subscription fail.
	// That is verbatim the shape of failure harness says this capability exists
	// to prevent, and it is why ApplyCommand consults runtimecommand.Provider the
	// same way rather than asserting on the Applier.
	provider, ok := controller.(session.CommittedPublicEventProvider)
	if !ok {
		missing = append(missing, "session.CommittedPublicEventProvider")
	} else if source, available := provider.CommittedPublicEvents(); !available || source == nil {
		missing = append(missing, "committed public events (the session reports no committed-bytes persistence)")
	} else {
		bound.committed = source
	}

	if len(missing) > 0 {
		return nil, &IncapableSessionError{Missing: missing}
	}
	return bound, nil
}

// ErrNoSession is the refusal returned when a rig reports success and hands back
// a nil controller.
var ErrNoSession = errors.New("harnessadapter: the rig reported success and returned no session")

// IncapableSessionError reports a launched harness session missing capabilities
// Host requires.
type IncapableSessionError struct {
	Missing []string
}

func (e *IncapableSessionError) Error() string {
	return "harnessadapter: the harness session is missing capabilities Host requires: " +
		strings.Join(e.Missing, ", ")
}

// UnsupportedCommandError reports an admitted command the released runtime
// command API cannot express.
type UnsupportedCommandError struct {
	CommandID sessionwire.CommandID
	Kind      string
	Reason    string
}

func (e *UnsupportedCommandError) Error() string {
	return "harnessadapter: command " + strconv.Quote(string(e.CommandID)) +
		" of kind " + strconv.Quote(e.Kind) + " cannot be applied: " + e.Reason
}
