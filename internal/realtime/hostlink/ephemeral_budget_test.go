package hostlink

import (
	"testing"
	"time"
)

func TestEphemeralAdmissionRefillsAfterBurst(t *testing.T) {
	now := time.Unix(100, 0)
	budget := newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })
	for i := 0; i < 2; i++ {
		if !budget.admit([]string{"client-a"}, 4*1024) {
			t.Fatalf("frame %d was refused inside the burst", i)
		}
	}
	if budget.admit([]string{"client-a"}, 1) {
		t.Fatal("frame beyond the burst was admitted")
	}
	now = now.Add(125 * time.Millisecond)
	if !budget.admit([]string{"client-a"}, 4*1024) {
		t.Fatal("one 4 KiB frame was refused after 125 ms refill")
	}
	if budget.admit([]string{"client-a"}, 1) {
		t.Fatal("refill admitted more than 4 KiB")
	}
}

func TestEphemeralFloodAcrossChannelsStaysBelowClientQueueLimit(t *testing.T) {
	now := time.Unix(100, 0)
	budget := newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })
	admitted := 0
	for millisecond := 0; millisecond < 30_000; millisecond++ {
		// Alternating channels on the same connection must spend one budget.
		for range 4 {
			if budget.admit([]string{"client-a"}, 4*1024) {
				admitted += 4 * 1024
			}
		}
		now = now.Add(time.Millisecond)
	}
	if limit := ephemeralBurstBytes + ephemeralRateBytesPerSecond*30; admitted > limit {
		t.Fatalf("admitted %d transient bytes during a 30 s stall, bound %d", admitted, limit)
	}
	if admitted >= clientQueueMaxBytes {
		t.Fatalf("transient bytes %d reached the 1 MiB client queue limit", admitted)
	}
	if ephemeralBurstBytes+ephemeralRateBytesPerSecond*30 >= clientQueueMaxBytes {
		t.Fatal("a 30 s stall now exceeds the configured Centrifuge client queue")
	}
}

func TestEphemeralAdmissionChargesEverySubscriberAtomically(t *testing.T) {
	now := time.Unix(100, 0)
	budget := newEphemeralBudget(ephemeralRateBytesPerSecond, ephemeralBurstBytes, func() time.Time { return now })
	if !budget.admit([]string{"slow"}, 8*1024) {
		t.Fatal("slow client's initial burst was refused")
	}
	if budget.admit([]string{"slow", "fast"}, 1) {
		t.Fatal("publication was admitted with one exhausted subscriber")
	}
	if !budget.admit([]string{"fast"}, 8*1024) {
		t.Fatal("rejected broadcast spent the fast client's tokens")
	}
}
