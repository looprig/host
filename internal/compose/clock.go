// Package compose is Host's composition root: it builds the Department
// runtime, the residency manager, the command consumers, the live event relay,
// the per-tenant HostLink servers, the metrics and the drain into one running
// service, with an explicit start and stop order.
//
// WHY IT IS NOT internal/service, WHICH 04-host.md NAMES. That package's own
// guard, TestPublishIsTheOnlyPublishingSurface, asserts its production file set
// is exactly [capacity.go events.go] and enumerates every exported declaration
// in it, because the property it holds is "Host publishes one advertisement per
// LaunchTarget and nothing else". Putting a composition root there would force
// that enumeration open for every type a composition exports, which trades a
// guard for a filename. The composition consumes internal/service instead, and
// the two halves of §15 stay on opposite sides of a package boundary.
//
// WHAT IT OWNS AND WHAT IT DOES NOT. It owns wiring and lifetime: which object
// is handed to which, in what order they start, and in what order they stop. It
// decides no policy — admission is internal/service's ledger, residency is
// internal/residency's manager, the drain sequence is internal/lifecycle's
// state machine — and the adapters here are mechanical joins between seams that
// were declared to fit. Where two seams do NOT fit, the divergence is named at
// the adapter rather than smoothed over; see releaseSession for the one case.
package compose

import (
	"time"

	"github.com/looprig/host/internal/residency"
)

// Clock is the composition's single time source.
//
// IT IS ONE INTERFACE OVER THREE SEAMS THAT ALREADY EXIST, and merging them is
// the point rather than a convenience. host.Clock, lifecycle.Clock and
// residency.WarmClock each describe the time a different package needs, and a
// composition free to satisfy them independently could hand the drain a real
// clock and the warm releaser a fake one — so a test could pass while the two
// bounds it was comparing came from different timelines. Naming one type here
// is what makes them the same clock by construction.
type Clock interface {
	// Now is the instant every derived record is stamped with.
	Now() time.Time

	// After is the bound every wait in the composition and the drain is held
	// to.
	After(time.Duration) <-chan time.Time

	// NewWarmTimer is one session's warm countdown.
	NewWarmTimer(time.Duration) residency.WarmTimer
}

// SystemClock is the production Clock: the real one.
//
// IT IS THE FIRST PRODUCTION residency.WarmClock IN THE MODULE. Until this task
// the warm releaser had a fake and no real timer at all, and residency.WarmTimer
// documents why it could not simply reuse host.Clock: NewTimer there returns a
// CONCRETE *time.Timer, which a test cannot expire without sleeping. The three
// methods below are time.Timer's own, so this is a forwarding wrapper and not a
// reimplementation — which is what keeps the fake and the real timer honest
// about each other.
type SystemClock struct{}

// Now is the wall clock, in UTC.
//
// UTC IS NOT COSMETIC HERE. Every record this Host publishes carries an instant
// Core validates and a Factory compares against its own, and a Location-carrying
// time round-trips through JSON as an offset. Fixing the zone at the source is
// one decision instead of one per derivation.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// After is time.After.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewWarmTimer arms a real timer.
func (SystemClock) NewWarmTimer(d time.Duration) residency.WarmTimer {
	return systemWarmTimer{timer: time.NewTimer(d)}
}

// NewTimer satisfies host.Clock, so one value is the Host's clock and the
// composition's.
func (SystemClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// systemWarmTimer is one armed *time.Timer behind residency.WarmTimer.
type systemWarmTimer struct {
	timer *time.Timer
}

// C is the expiry channel.
func (t systemWarmTimer) C() <-chan time.Time { return t.timer.C }

// Stop cancels an arming, and reports whether this call was the one that
// stopped it. The result is time.Timer's own and is not reinterpreted: the warm
// releaser branches on it to tell "I disarmed a live countdown" from "it had
// already fired", and a wrapper that returned true unconditionally would make
// the second look like the first.
func (t systemWarmTimer) Stop() bool { return t.timer.Stop() }

// Reset arms the timer for a fresh duration.
func (t systemWarmTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
