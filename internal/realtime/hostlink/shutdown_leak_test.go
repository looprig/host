package hostlink_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/realtime/hostlink"
)

// eagleAggregateFrame is the frame of Centrifuge's per-Node metrics aggregator
// goroutine. The open parenthesis keeps it from also matching aggregateOnce,
// which that same goroutine calls on every tick.
const eagleAggregateFrame = "github.com/FZambia/eagle.(*Eagle).aggregate("

func eagleAggregators(t *testing.T) int {
	t.Helper()
	var dump bytes.Buffer
	// debug=2 prints every goroutine on its own, so each contributes exactly
	// one occurrence of the frame.
	if err := pprof.Lookup("goroutine").WriteTo(&dump, 2); err != nil {
		t.Fatalf("goroutine profile: %v", err)
	}
	return strings.Count(dump.String(), eagleAggregateFrame)
}

// TestClosingAServerLeavesNoTransportGoroutineBehind holds the Centrifuge
// Shutdown fix in place for HostLink's own server.
//
// A Host builds one Centrifuge Node per tenant HostLink server, and Close is
// Node.Shutdown. centrifuge v0.38.0 started an eagle metrics aggregator for
// every Node and Shutdown never closed it, so each closed server leaked one
// goroutine for the life of the process -- which the tests lane had to count
// rather than fail on. v0.39.0 closes it (centrifuge@v0.39.3/node.go:438-442).
// This case fails at v0.38.0 and passes at v0.39.3.
//
// It is NOT parallel: every parallel case in this package builds servers of
// its own, and their aggregators would move the count. The control comes
// first, so a probe that could not see an aggregator cannot pass for the wrong
// reason.
func TestClosingAServerLeavesNoTransportGoroutineBehind(t *testing.T) {
	const servers = 3
	before := eagleAggregators(t)

	auth := authenticatorFunc(func(context.Context, sessionwire.TenantID, string) error { return nil })
	built := make([]hostlink.Server, 0, servers)
	for i := 0; i < servers; i++ {
		server, err := hostlink.NewCentrifugeServer(hostlink.Config{TenantID: testTenant, Authenticator: auth})
		if err != nil {
			t.Fatalf("NewCentrifugeServer: %v", err)
		}
		built = append(built, server)
	}

	if running := eagleAggregators(t); running < before+servers {
		t.Fatalf("with %d servers running the probe sees %d aggregators (baseline %d); it cannot see what it is meant to count", servers, running, before)
	}

	for _, server := range built {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Close(ctx); err != nil {
			cancel()
			t.Fatalf("Close: %v", err)
		}
		cancel()
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		after := eagleAggregators(t)
		if after <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d servers were closed and %d metrics aggregators outlive them (baseline %d, now %d): Close leaks the Node's eagle exporter",
				servers, after-before, before, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
