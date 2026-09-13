package compose

import (
	"context"
	"time"

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
