// Package testkit holds test doubles shared by this module's tests. It is
// production code by build tag and test support by intent: it lives under
// internal/ so nothing outside this module can depend on it.
package testkit

import (
	"context"
	"errors"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/department"
)

// FakeRig is a department.Rig that records what it was asked for.
//
// It is the CONSUMER of everything the adapter claims to propagate, which is
// the only counterparty an assertion about propagation may use. Asserting that
// the adapter's own getters return what was passed in proves the adapter can
// remember a value; it does not prove the value reached the thing that needs
// it. A sibling lane lost that distinction and shipped a record written into an
// envelope its reader could not see, having round-tripped it through the
// writer's own decoder.
//
// Every recorded field's ZERO VALUE is distinct from what tests assert, so an
// assertion cannot pass because the fixture already held the answer.
type FakeRig struct {
	mu sync.Mutex

	// Session is handed back by both operations. Nil means "return NewErr".
	Session department.RigSession

	// NewErr and RestoreErr, when set, are returned instead of a session.
	NewErr     error
	RestoreErr error

	creates  []department.RigCreateRequest
	restores []department.RigRestoreRequest
	restored []uuid.UUID
}

// NewSession records the request and returns the configured session.
func (f *FakeRig) NewSession(_ context.Context, request department.RigCreateRequest) (department.RigSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, request)
	if f.NewErr != nil {
		return nil, f.NewErr
	}
	return f.Session, nil
}

// RestoreSession records the request and returns the configured session.
func (f *FakeRig) RestoreSession(_ context.Context, id uuid.UUID, request department.RigRestoreRequest) (department.RigSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, request)
	f.restored = append(f.restored, id)
	if f.RestoreErr != nil {
		return nil, f.RestoreErr
	}
	return f.Session, nil
}

// Creates returns every create request this rig received, in order.
func (f *FakeRig) Creates() []department.RigCreateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]department.RigCreateRequest(nil), f.creates...)
}

// Restores returns every restore request this rig received, in order.
func (f *FakeRig) Restores() []department.RigRestoreRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]department.RigRestoreRequest(nil), f.restores...)
}

// RestoredIDs returns the Harness identity each restore was asked for.
func (f *FakeRig) RestoredIDs() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.restored...)
}

// Launches reports how many times this rig was asked to produce a session by
// either operation. It is the ordering assertion's instrument: a rejection that
// must happen BEFORE a launch is proved by this staying at zero, not by an
// error being returned.
func (f *FakeRig) Launches() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates) + len(f.restores)
}

// ErrRigRefused is a stable cause for a rig that declines to launch.
var ErrRigRefused = errors.New("testkit: the rig refused to launch")

// Capability names a capability FakeSession can be built with or without.
type Capability string

const (
	CapabilityIdleWaiter            Capability = "IdleWaiter"
	CapabilityLiveness              Capability = "Liveness"
	CapabilityReleaser              Capability = "Releaser"
	CapabilityPublicationSubscriber Capability = "PublicationSubscriber"
	CapabilityCommandApplier        Capability = "CommandApplier"
)

// AllCapabilities is every capability Host requires of a launched session.
var AllCapabilities = []Capability{
	CapabilityIdleWaiter,
	CapabilityLiveness,
	CapabilityReleaser,
	CapabilityPublicationSubscriber,
	CapabilityCommandApplier,
}

// A session missing a capability is a distinct TYPE, not a flag: a Go method
// set cannot be conditional at runtime, and the adapter discovers capabilities
// by type assertion, so the only honest way to model "missing" is to build a
// value whose method set genuinely lacks the method. The capabilities are
// therefore parts, and each variant embeds the parts it has.

type idPart struct{ id uuid.UUID }

// ID is Harness's identity for this session.
func (p idPart) ID() uuid.UUID { return p.id }

type idlePart struct{}

// WaitIdle returns nil unless the context is already done, so a test can prove
// the adapter forwards a cancelled context rather than swallowing it.
func (idlePart) WaitIdle(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type livenessPart struct{ stopped chan struct{} }

// Done reports when the session has stopped.
func (p livenessPart) Done() <-chan struct{} { return p.stopped }

type releaserPart struct {
	mu       sync.Mutex
	released int
}

// ReleaseResidency records a nonterminal residency release.
func (p *releaserPart) ReleaseResidency(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released++
	return nil
}

// Released reports how many times residency was released.
func (p *releaserPart) Released() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.released
}

type subscriberPart struct {
	mu    sync.Mutex
	after []sessionwire.EventID
}

// SubscribeCommitted records the resume point and returns a closed channel.
func (p *subscriberPart) SubscribeCommitted(_ context.Context, after sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.after = append(p.after, after)
	published := make(chan sessionwire.EnduringPublication)
	close(published)
	return published, nil
}

// SubscribedAfter reports the resume point of every subscription.
func (p *subscriberPart) SubscribedAfter() []sessionwire.EventID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sessionwire.EventID(nil), p.after...)
}

type applierPart struct {
	mu      sync.Mutex
	applied []sessionwire.CommandEnvelope
}

// ApplyCommand records the command it was asked to apply.
func (p *applierPart) ApplyCommand(_ context.Context, command sessionwire.CommandEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied = append(p.applied, command)
	return nil
}

// Applied reports every command this session was asked to apply.
func (p *applierPart) Applied() []sessionwire.CommandEnvelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sessionwire.CommandEnvelope(nil), p.applied...)
}

// FullSession has every capability Host requires.
type FullSession struct {
	idPart
	idlePart
	livenessPart
	*releaserPart
	*subscriberPart
	*applierPart
}

// NewFullSession returns a session with every capability.
func NewFullSession(id uuid.UUID) *FullSession {
	return &FullSession{
		idPart:         idPart{id: id},
		livenessPart:   livenessPart{stopped: make(chan struct{})},
		releaserPart:   &releaserPart{},
		subscriberPart: &subscriberPart{},
		applierPart:    &applierPart{},
	}
}

// Stop closes the liveness channel, so Done reports a stopped runtime.
func (s *FullSession) Stop() { close(s.stopped) }

type sessionWithoutIdleWaiter struct {
	idPart
	livenessPart
	*releaserPart
	*subscriberPart
	*applierPart
}

type sessionWithoutLiveness struct {
	idPart
	idlePart
	*releaserPart
	*subscriberPart
	*applierPart
}

type sessionWithoutReleaser struct {
	idPart
	idlePart
	livenessPart
	*subscriberPart
	*applierPart
}

type sessionWithoutSubscriber struct {
	idPart
	idlePart
	livenessPart
	*releaserPart
	*applierPart
}

type sessionWithoutApplier struct {
	idPart
	idlePart
	livenessPart
	*releaserPart
	*subscriberPart
}

// bareSession has the identity and nothing else, so an error naming missing
// capabilities has more than one to name.
type bareSession struct{ idPart }

// NewSessionWithout returns a session with every capability EXCEPT the one
// named. It panics on an unknown capability rather than returning a session
// that quietly has everything, because a typo'd capability name in a test
// would otherwise produce a passing assertion about a rejection that never
// happened.
func NewSessionWithout(id uuid.UUID, missing Capability) department.RigSession {
	identity := idPart{id: id}
	stopped := livenessPart{stopped: make(chan struct{})}
	switch missing {
	case CapabilityIdleWaiter:
		return &sessionWithoutIdleWaiter{idPart: identity, livenessPart: stopped, releaserPart: &releaserPart{}, subscriberPart: &subscriberPart{}, applierPart: &applierPart{}}
	case CapabilityLiveness:
		return &sessionWithoutLiveness{idPart: identity, releaserPart: &releaserPart{}, subscriberPart: &subscriberPart{}, applierPart: &applierPart{}}
	case CapabilityReleaser:
		return &sessionWithoutReleaser{idPart: identity, livenessPart: stopped, subscriberPart: &subscriberPart{}, applierPart: &applierPart{}}
	case CapabilityPublicationSubscriber:
		return &sessionWithoutSubscriber{idPart: identity, livenessPart: stopped, releaserPart: &releaserPart{}, applierPart: &applierPart{}}
	case CapabilityCommandApplier:
		return &sessionWithoutApplier{idPart: identity, livenessPart: stopped, releaserPart: &releaserPart{}, subscriberPart: &subscriberPart{}}
	default:
		panic("testkit: unknown capability " + string(missing))
	}
}

// NewBareSession returns a session with the identity and no capability at all.
func NewBareSession(id uuid.UUID) department.RigSession { return &bareSession{idPart{id: id}} }
