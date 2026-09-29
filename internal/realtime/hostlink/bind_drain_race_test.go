package hostlink_test

import (
	"testing"
	"time"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// racingResidencies runs a hook the first time a bind reads ownership, so a
// test can start a release at the one instant that matters: after the bind has
// validated, before it records its route.
type racingResidencies struct {
	inner  *recordingResidencies
	onRead func()
}

func (r *racingResidencies) Get(key registry.Key) (registry.Entry, bool) {
	entry, held := r.inner.Get(key)
	if hook := r.onRead; hook != nil {
		r.onRead = nil
		hook()
	}
	return entry, held
}

// TestABindRacingADrainDoesNotOutliveTheDrainsInvalidation is the bind half of
// the drain/attach race. A bind that validated a resident, admitting session
// just before the drain began must not record its route AFTER the drain's
// release has invalidated the session's routes: that route points at a session
// this Host no longer holds, and a link-drained Host stays up answering, so
// nothing would ever drop it.
func TestABindRacingADrainDoesNotOutliveTheDrainsInvalidation(t *testing.T) {
	var residencies *racingResidencies
	f := newFixture(t, func(options *hostlink.MultiplexerOptions) {
		residencies = &racingResidencies{inner: options.Residencies.(*recordingResidencies)}
		options.Residencies = residencies
	})
	invalidated := make(chan struct{})
	residencies.onRead = func() {
		// The drain begins and releases the session. Under the fix the
		// invalidation waits for the bind's critical section, so this gives it
		// a moment and then lets the bind go on regardless.
		go func() {
			f.admission.beginDrain()
			f.mux.InvalidateSession(residencyKey(testSession))
			close(invalidated)
		}()
		select {
		case <-invalidated:
		case <-time.After(100 * time.Millisecond):
		}
	}

	_, _ = f.mux.Bind(linkA, bindRequest(testSession))
	select {
	case <-invalidated:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain's invalidation never ran")
	}
	if bound := f.mux.Replicas(residencyKey(testSession)); len(bound) != 0 {
		t.Fatalf("links %v still route to a session the drain released: the bind recorded its route after the invalidation", bound)
	}
}
