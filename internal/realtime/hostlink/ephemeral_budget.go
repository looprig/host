package hostlink

import (
	"sync"
	"time"
)

// ephemeralBudget accounts for the transient bytes admitted to each physical
// Centrifuge client. A publish broadcasts one frame, so all subscribed clients
// must have enough tokens before any of them is charged.
type ephemeralBudget struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	now    func() time.Time
	client map[string]ephemeralTokens
}

type ephemeralTokens struct {
	remaining float64
	at        time.Time
}

func newEphemeralBudget(rate, burst int, now func() time.Time) *ephemeralBudget {
	return &ephemeralBudget{rate: float64(rate), burst: float64(burst), now: now, client: make(map[string]ephemeralTokens)}
}

func (b *ephemeralBudget) admit(clients []string, size int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size <= 0 || float64(size) > b.burst {
		return false
	}
	now := b.now()
	updated := make(map[string]ephemeralTokens, len(clients))
	for _, id := range clients {
		state, found := b.client[id]
		if !found {
			state.remaining = b.burst
			state.at = now
		} else if elapsed := now.Sub(state.at).Seconds(); elapsed > 0 {
			state.remaining = min(b.burst, state.remaining+elapsed*b.rate)
			state.at = now
		}
		if state.remaining < float64(size) {
			return false
		}
		updated[id] = state
	}
	for id, state := range updated {
		state.remaining -= float64(size)
		b.client[id] = state
	}
	return true
}

func (b *ephemeralBudget) forget(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.client, clientID)
}
