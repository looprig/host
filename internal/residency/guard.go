package residency

// Guard is the attach's own epoch fence, exported so the composition root can
// hand the SAME fence to the command consumer.
//
// It exists because commands.Fence asks for it by name: that interface is "the
// SHAPE residency.epochFence already has, declared there because that type's
// methods are unexported and this is a different package", and it records that
// an adapter is owed at the composition root. An adapter outside this package
// cannot reach the unexported methods, so the hand-off has to start here.
//
// IT IS A VIEW AND NOT A SECOND FENCE. The three methods delegate to the one
// epochFence an attach built over its one grant, which is the property
// OwnershipRequest.Fence's own documentation calls "one grant, one fence": a
// supersession recorded through this view is visible to the heartbeat, and the
// reverse, because there is one piece of state and not two.
//
// THE ZERO VALUE FAILS CLOSED ON THE TWO METHODS THAT DECIDE, AND OPEN ON THE
// ONE THAT NOTIFIES. Held and Write are the authorizing pair: a Guard holding
// no fence reports ownership gone and runs no write, rather than dereferencing
// nil at the first write. Lost cannot answer that way — it returns a channel,
// and the only "already lost" channel is a closed one, which would make every
// select watching it spin. It returns nil instead, which blocks forever and
// therefore reads as "not lost". A caller that watched Lost ALONE would
// therefore learn nothing from a zero Guard; it must ask Held, or let Write
// refuse. This is stated in the heading because a heading that said only "fails
// closed" would be the wider-than-true sentence. That
// state is not reachable through BeginOwnership, which refuses a nil fence
// before any of this; it is reachable because Guard is a method on an exported
// struct, and the safe answer for a guard with nothing to guard is the refusing
// one.
type Guard struct {
	fence *epochFence
}

// Guard returns the view of this request's fence.
func (r OwnershipRequest) Guard() Guard { return Guard{fence: r.Fence} }

// Held reports the typed reason ownership is gone, or nil.
func (g Guard) Held() error {
	if g.fence == nil {
		return ErrLeaseNotHeld
	}
	return g.fence.held()
}

// Lost closes when ownership is gone. With no fence it is a nil channel, which
// blocks forever in a select and is therefore "not lost" rather than "lost".
func (g Guard) Lost() <-chan struct{} {
	if g.fence == nil {
		return nil
	}
	return g.fence.lost()
}

// Write performs one fenced write through the attach's own fence.
func (g Guard) Write(run func() error) error {
	if g.fence == nil {
		return ErrLeaseNotHeld
	}
	return g.fence.write(run)
}
