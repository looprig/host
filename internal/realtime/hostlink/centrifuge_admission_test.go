package hostlink

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
)

func TestAcceptedSubscriptionIsChargedBeforeCentrifugeMarksItReady(t *testing.T) {
	now := time.Unix(100, 0)
	server := &centrifugeServer{budget: newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })}
	active := map[string]*centrifuge.Client{"client-a": nil}
	server.noteSubscribed("client-a", "session-channel")
	if !server.admitSubscribed("session-channel", active, ephemeralBurstBytes) {
		t.Fatal("the first subscriber burst was refused")
	}
	if server.admitSubscribed("session-channel", active, 1) {
		t.Fatal("a pending subscription received an uncharged frame")
	}
}

func TestRejectedSubscriptionStopsSpendingClientBudget(t *testing.T) {
	now := time.Unix(100, 0)
	server := &centrifugeServer{budget: newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })}
	active := map[string]*centrifuge.Client{"client-a": nil}
	attempt := server.noteSubscribed("client-a", "session-channel")
	server.subscriptionFinished("client-a", "session-channel", attempt, false)
	first := server.admitSubscribed("session-channel", active, ephemeralBurstBytes)
	second := server.admitSubscribed("session-channel", active, ephemeralBurstBytes)
	if !first || !second {
		t.Fatal("a rejected subscription spent tokens on frames it cannot receive")
	}
}

func TestLateRejectedAttemptCannotRemoveNewerSubscription(t *testing.T) {
	now := time.Unix(100, 0)
	server := &centrifugeServer{budget: newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })}
	active := map[string]*centrifuge.Client{"client-a": nil}
	old := server.noteSubscribed("client-a", "session-channel")
	server.noteSubscribed("client-a", "session-channel")
	server.subscriptionFinished("client-a", "session-channel", old, false)
	if !server.admitSubscribed("session-channel", active, ephemeralBurstBytes) || server.admitSubscribed("session-channel", active, 1) {
		t.Fatal("an older rejection removed the current subscriber's budget")
	}
}

func TestDisconnectedSubscriberIsPrunedEvenIfRecordedLate(t *testing.T) {
	server := &centrifugeServer{budget: newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, time.Now)}
	server.noteSubscribed("gone", "session-channel")
	server.admitSubscribed("session-channel", map[string]*centrifuge.Client{}, 1)
	if len(server.subscribed["session-channel"]) != 0 {
		t.Fatal("a client absent from Centrifuge's Hub remained in admission state")
	}
}
