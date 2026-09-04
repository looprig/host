package harnessadapter

import (
	"context"
	"errors"
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
	// RigForCreate assembles the rig a new session launches on.
	RigForCreate(context.Context, department.RigCreateRequest) (*rig.Rig, error)

	// RigForRestore assembles the rig a restored session relaunches on. The UUID
	// is Harness's identity for the session and is passed so an assembly can
	// refuse a session it has no state for.
	RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (*rig.Rig, error)
}

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
	if assembled == nil {
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
	if assembled == nil {
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

// bind discovers the capabilities Host requires on a launched controller and
// refuses one short of any of them.
//
// IT REPORTS EVERY MISSING CAPABILITY rather than the first, for
// department.adaptRigSession's reason, and it discovers them the same way
// department does — by assertion — because finding H0 is that none of the four
// is on session.SessionController.
func (a *Adapter) bind(
	controller session.SessionController,
	tenant sessionwire.TenantID,
	sessionID sessionwire.SessionID,
) (department.RigSession, error) {
	if controller == nil {
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
	if capability, ok := controller.(idleWaiter); ok {
		bound.idle = capability
	} else {
		missing = append(missing, "WaitIdle")
	}
	if capability, ok := controller.(liveness); ok {
		bound.live = capability
	} else {
		missing = append(missing, "Done")
	}
	if capability, ok := controller.(releaser); ok {
		bound.releaser = capability
	} else {
		missing = append(missing, "ReleaseResidency")
	}
	if capability, ok := controller.(committedSubscriber); ok {
		bound.committed = capability
	} else {
		missing = append(missing, "SubscribeCommittedPublicEvents")
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
