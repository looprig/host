package compose

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// persistenceFaultsFor asks a runtime for its durable-health capability, and
// asks the wrapper whether it actually forwards one, for attemptCloserFor's
// reason: department's wrapper has the methods for every runtime.
func persistenceFaultsFor(runtime any) (department.PersistenceFaults, bool) {
	faults, ok := runtime.(department.PersistenceFaults)
	if !ok {
		return nil, false
	}
	if reporter, ok := runtime.(interface{ PersistenceFaultsAvailable() bool }); ok && !reporter.PersistenceFaultsAvailable() {
		return nil, false
	}
	return faults, true
}

// superviseFaults watches one resident runtime for a latched persistence fault
// and, when one latches, gives the session up so a successor restores it (D3).
//
// A FAULTED RUNTIME IS TREATED AS LOST RESIDENCY, NOT AS A BUSY ONE. One failed
// journal append — a brief database or object-store outage — faults a harness
// runtime for good, and it then refuses every command: before this, Host kept
// such a session resident at the same epoch and kept claiming commands into it,
// the next attempt sat `applying` with nothing to close it, and everything behind
// it went `pending` until the apply deadline rejected it. A successor RESTORING
// from the journal is the recovery the runtime's own design names; it closes the
// in-flight attempt through the ordinary recovery path (settled from evidence if
// the runtime's frame landed, closed not_applied under a later grant if not).
//
// It runs under the SESSION context and also ends when the runtime stops by any
// other path, so a session released normally leaves no watcher behind.
func (s *Service) superviseFaults(ctx context.Context, held *resident) {
	faults, ok := persistenceFaultsFor(held.runtime)
	if !ok {
		return
	}
	faulted := faults.PersistenceFaulted()
	if faulted == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-held.runtime.Done():
			return
		case <-faulted:
		}
		s.giveUp(held, "persistence_fault", faults.PersistenceFault())
	}()
}

// strandedAttempt is the disposition applier's report that this runtime failed
// a command after its attempt became durable and recorded nothing to settle
// from. The attempt names the runtime's own grant, so only a successor can
// close it; a journal write that failed WITHOUT faulting the runtime (the
// application prefix itself, during an outage) reaches Host only this way.
//
// It is called on the consumer's own goroutine, in the middle of a pass, and the
// release HALTS that consumer — waiting for the pass in flight to finish — before
// it abandons the runtime. Waiting on its own goroutine would deadlock, so the
// release runs on another.
func (s *Service) strandedAttempt(key registry.Key, generation uint64, command sessionwire.CommandID, cause error) {
	held := s.residentFor(key)
	if held == nil || held.generation != generation {
		return
	}
	go s.giveUp(held, "stranded_attempt", fmt.Errorf("command %s: %w", command, cause))
}

// giveUp releases a runtime that can no longer make progress, once per
// residency however many reports arrive.
func (s *Service) giveUp(held *resident, reason string, cause error) {
	faults, ok := persistenceFaultsFor(held.runtime)
	if !ok {
		// A runtime that cannot be abandoned stays resident: a graceful release
		// would anchor to a log it cannot trust, and a shutdown would end the
		// session. Its composition owns this case.
		s.options.logger().LogAttrs(context.Background(), slog.LevelError,
			"host: the runtime can make no further progress and offers no crash-equivalent release; the session stays resident and blocked",
			slog.String("tenant_id", string(held.key.TenantID)),
			slog.String("session_id", string(held.key.SessionID)),
			slog.String("reason", reason),
			slog.String("cause", errorText(cause)))
		return
	}
	held.giveUpOnce.Do(func() {
		s.releaseUnusable(context.Background(), held, faults, reason, cause)
	})
}

// releaseUnusable is the crash-equivalent release of a runtime that can make no
// further progress: faulted, or holding an attempt only a successor can close.
//
// THE ORDER IS THE DRAIN'S, WITH TWO STEPS CHANGED. Admission is closed and the
// residency marked releasing first, so no Factory routes a new command here;
// then the session's consumer, tail and gate publisher stop. The CHECKPOINT IS
// SKIPPED — there is nothing trustworthy to anchor — and the runtime is ABANDONED
// rather than released: harness's ReleaseResidency refuses a faulted session and
// its Shutdown would make the session terminal, while AbandonResidency writes
// nothing and hands the journal lease back. The tombstone, heartbeat stop,
// residency-lease release and local-state drop then run exactly as a drain's.
//
// EVERY STEP RUNS WHETHER OR NOT AN EARLIER ONE FAILED. The outage that faulted
// the runtime may still be on, so the durable writes can fail; a lease whose
// release fails stops renewing and lapses, the registry row expires, and
// Factory's pending sweep then re-places the session. What cannot be written is
// logged, because a faulted session that silently stays resident is the defect.
func (s *Service) releaseUnusable(ctx context.Context, held *resident, faults department.PersistenceFaults, reason string, cause error) {
	logger := s.options.logger()
	attrs := []slog.Attr{
		slog.String("tenant_id", string(held.key.TenantID)),
		slog.String("session_id", string(held.key.SessionID)),
		slog.Uint64("generation", held.generation),
	}
	logger.LogAttrs(ctx, slog.LevelError,
		"host: the runtime can make no further progress; releasing the session so a successor restores it from the journal",
		append(attrs, slog.String("reason", reason), slog.String("cause", errorText(cause)))...)

	var failures []error
	if err := held.BeginRelease(ctx); err != nil {
		failures = append(failures, err)
	}
	// THE CONSUMER IS HALTED, AND WAITED FOR, BEFORE THE RUNTIME IS ABANDONED (D3
	// gate F2). Consumer.Stop does not wait for a pass in flight, so a pass could
	// otherwise still be dispatching into a runtime mid-abandon. Halt waits,
	// bounded; a pass that outlives the bound is still harmless, because the
	// abandon seals the runtime-command log and refuses its writes.
	if s.residentFor(held.key) == held {
		haltCtx, cancel := context.WithTimeout(ctx, giveUpHaltBound)
		if err := s.Halt(haltCtx, held.key); err != nil {
			failures = append(failures, fmt.Errorf("halt the consumer: %w", err))
		}
		cancel()
	}
	held.stopWork()
	if err := held.residencyOnce.run(func() error { return faults.AbandonResidency(ctx) }); err != nil {
		failures = append(failures, err)
	}
	if err := (releaseSession{resident: held}).FinishRelease(ctx); err != nil {
		failures = append(failures, err)
	}
	// The admission credit AFTER the lease release, for the warm release's
	// reason: crediting capacity while still holding the grant would admit a
	// replacement this Host has no room for. BOTH ARE KEYED BY KEY ONLY, so they
	// run only while this residency is still the one held (D3 gate F3): a late
	// give-up for a replaced residency must not uncharge its successor.
	if s.residentFor(held.key) == held {
		s.capacity.Release(held.key)
		s.warm.Forget(held.key)
	}
	s.forget(held.key, held.generation)

	if err := errors.Join(failures...); err != nil {
		logger.LogAttrs(ctx, slog.LevelWarn,
			"host: the unusable session's release did not complete durably; its lease and registry row lapse on expiry",
			append(attrs, slog.String("error", err.Error()))...)
		return
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "host: the unusable session was released", attrs...)
}

// giveUpHaltBound bounds how long a give-up waits for the session's in-flight
// consumer pass before abandoning the runtime anyway.
const giveUpHaltBound = 30 * time.Second

// errorText renders an error for a log attribute, including nil.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
