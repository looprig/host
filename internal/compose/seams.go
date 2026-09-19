package compose

import (
	"context"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"time"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/gates"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

// TargetDirectory is the durable §15 target directory this Host advertises into.
//
// internal/service derives an Advertisement and its own package documentation
// says nothing there writes; this is the write. It is declared here rather than
// there because a writer taking an Advertisement is a second surface through
// which one reaches a caller, and that package's enumeration exists to keep
// there being exactly one.
type TargetDirectory interface {
	// MaxAdvertisementTTL is the longest expiry promise the directory stores.
	// The composition refuses a Host whose registry expiry exceeds it, because
	// such a Host is constructible and could never publish a single row.
	MaxAdvertisementTTL() time.Duration

	// PublishTarget writes one derived advertisement. It is also the heartbeat.
	PublishTarget(context.Context, service.Advertisement) error

	// WithdrawTarget removes this Host's offer for one target. It is NOT a
	// publication carrying accepting=false: a row that still carried an
	// advertisement would still be ranked and still be due.
	WithdrawTarget(context.Context, service.Advertisement) error
}

// WorkStates reports what one resident session is currently doing.
//
// IT IS OPTIONAL AND NOTHING IN THIS MODULE IMPLEMENTS IT. A resident gate wait
// is §9.4 state that lives inside the runtime, and department.Runtime exposes
// no reader for it; internal/lifecycle's GateWaits records the same absence
// from the metrics side. The seam is here because two consumers need the same
// answer and must not develop two — the warm releaser, which must not evict a
// session with a human waiting at a gate, and the compatibility wait, whose
// gate boundary is one of its five outcomes.
//
// A COMPOSITION WITHOUT ONE IS EXPLICIT ABOUT THE CONSEQUENCE. The warm
// releaser receives no work observations, so it never arms a countdown and
// never evicts; the compatibility wait can never return at a gate boundary. Both
// are absences a test asserts rather than behaviours a comment claims.
type WorkStates interface {
	// WorkState returns the session's current state and whether it is known.
	WorkState(registry.Key) (residency.WorkState, bool)
}

// DispositionWriters binds one session's disposition transitions to the
// residency GRANT this Host holds for it.
//
// IT IS DECLARED HERE RATHER THAN IN internal/commands BECAUSE IT IS A
// COMPOSITION CONCERN. commands consumes one session's writer and knows nothing
// about how a Host got it; what this expresses is the step between holding a
// residency.Lease and having something that may write under it, and that step
// exists only because sessionstore's claim edge takes a grant rather than an
// epoch — "the epoch comes off the grant and from nowhere else, so no number a
// caller chose can reach the record's mark".
//
// A REFUSAL HERE FAILS THE ATTACH, and that is the right place for it. A Host
// that took residency of a session and then discovered it had no writer for it
// would be holding a session it can consume from and never apply in, which is
// the shape the whole dispatch boundary existed to prevent.
type DispositionWriters interface {
	// DispositionWriterFor returns the writer bound to this lease's grant.
	//
	// The lease is the one this composition recorded when the grant was taken;
	// an implementation that cannot recognise it must REFUSE rather than return
	// a writer with no grant, because such a writer would fail at the first
	// command of a session this Host had already taken residency of.
	DispositionWriterFor(residency.Lease) (commands.DispositionWrites, error)
}

// GateSessions binds one resident session's durable gate surface to the
// residency grant this Host holds for it, for the gate publisher.
//
// IT IS A FACTORY FOR DispositionWriters' REASON: sessionstore v0.12.0's gate
// writes take the store-issued *ResidencyGrant and refuse a bare epoch, so the
// grant is bound once per session rather than passed per call. An
// implementation that cannot recognise the lease must refuse.
type GateSessions interface {
	GateSessionFor(residency.Lease, sessionwire.TenantID, sessionwire.SessionID) (gates.Session, error)
}
