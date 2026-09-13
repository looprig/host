package compose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/testkit"
)

// ---------------------------------------------------------------------------
// The attach a composed Host could not perform
// ---------------------------------------------------------------------------
//
// EVERY TEST HERE IS IN COMPOSITION AND NONE OF THEM IS AT AN ADAPTER, and the
// distinction is a correction rather than a preference. The claim "a composed
// Host can consume" was previously discharged by a differential over three
// adapter methods, and a gate accepted a sentence that was wider than its probe.
// The subject below is *Service: the object a binary builds, holding the
// residency manager, the ownership seam, the consumer and the live tail.

// TestAComposedHostAttachesADispositionSessionAndStartsItsConsumer is the
// property the attach-time journal fence made unreachable.
//
// THE FENCE WAS LEGACY RESIDUE. Step 3 committed an opening in-stream ownership
// record BEFORE hydration, through a call whose bindSessionScope pins
// ProtocolModeLegacy — so a session whose catalog binding is disposition, which
// is the only binding AcquireResidency will grant residency over, conflicted on
// binding.protocol_mode and the attach died at StepFence. BeginOwnership — the
// last step but one, now step 7 of 8 — never ran, and the durable command
// consumer was therefore never started on any session this Host could hold.
//
// THE PROBE IS TWO INDEPENDENT THINGS AND THREE READINGS, and the difference is
// worth stating because an earlier version of this comment claimed three things.
// An attach that merely returns nil has proved nothing about ownership, so (a)
// the composition must be tracking the session and (b) the consumer must be
// registered and reachable through the seam HostLink delivers commands over.
// Those two are both gated on BeginOwnership having completed — they are two
// readings of ONE event, not two events — and a build that returned success and
// started nothing fails both. (c), the runtime's journal epoch, is genuinely
// independent of them.
func TestAComposedHostAttachesADispositionSessionAndStartsItsConsumer(t *testing.T) {
	f := newFixture(t)
	f.start()

	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	if mode := f.store.mode(key); mode != modeDisposition {
		t.Fatalf("the session under test is bound to %q, want %q; this test is about the mode a Host can hold", mode, modeDisposition)
	}

	held, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID:  tenantA,
		SessionID: sessionA,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if err != nil {
		var refusal *residency.AttachError
		if errors.As(err, &refusal) {
			t.Fatalf("the attach failed at step %q: %v", refusal.Step, err)
		}
		t.Fatalf("the attach failed: %v", err)
	}
	if !held.Attached {
		t.Fatalf("Attach reported Attached false; this call established the residency")
	}

	// (a) OWNERSHIP RAN. trackResident is called only from the composition's
	// BeginOwnership, after the inner heartbeat ownership succeeded, so a
	// tracked session is step 7 having completed rather than step 6.
	f.svc.mu.Lock()
	_, tracked := f.svc.sessions[key]
	f.svc.mu.Unlock()
	if !tracked {
		t.Errorf("the composition tracks no resident session, so BeginOwnership did not complete")
	}

	// (b) THE CONSUMER IS RUNNING AND REACHABLE. ConsumerFor is the seam the
	// HostLink command path resolves a delivery through; a consumer started and
	// not registered is one no command can ever reach.
	if _, running := f.svc.ConsumerFor(key); !running {
		t.Errorf("no durable command consumer is registered for the attached session")
	}

	// THERE IS NO "NO JOURNAL WRITER WAS OPENED" ASSERTION HERE, deliberately.
	// compose.Options declares no opener at all, so the claim is held by the
	// compiler, and a trace counter over a call nothing can make is a negative
	// assertion that cannot fail. The structural form of the rule lives where a
	// reinstatement would be written:
	// internal/residency.TestNoProductionFileOpensAJournalWriter.

	// (c) THE RUNTIME'S GRANT IS WHAT THE RESIDENCY REPORTS.
	if !held.JournalEpochHeld {
		t.Errorf("the residency reports no journal grant; the fixture's runtime holds one")
	}
	if want := residency.JournalEpoch(testkit.FirstJournalEpoch); held.JournalEpoch != want {
		t.Errorf("the residency reports journal epoch %d, want the runtime's %d", held.JournalEpoch, want)
	}
}

// TestAComposedHostRefusesALegacySessionBeforeItTakesAnything is the OTHER
// protocol mode, and it exists because a suite that only ever drives one mode
// cannot see a mode defect at all.
//
// IT IS NOT A SECOND SPELLING OF THE TEST ABOVE. Removing the fence does not
// make a Host mode-blind: a legacy-bound session has no residency grant to take,
// because AcquireResidency refuses it at the catalog binding, so the attach
// fails at StepLease with nothing acquired. That is the branch the mode still
// governs after the fence is gone, and it is Host's own branch rather than the
// store's — step 2's refusal path.
func TestAComposedHostRefusesALegacySessionBeforeItTakesAnything(t *testing.T) {
	f := newFixture(t)
	f.start()

	legacy := sessionwire.SessionID("session-legacy")
	key := registry.Key{TenantID: tenantA, SessionID: legacy}
	f.store.bind(key, modeLegacy)

	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID:  tenantA,
		SessionID: legacy,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if err == nil {
		t.Fatalf("the attach of a legacy-bound session succeeded; the store grants residency only in disposition mode")
	}
	var refusal *residency.AttachError
	if !errors.As(err, &refusal) {
		t.Fatalf("the refusal is %T, want a *residency.AttachError naming the step", err)
	}
	if refusal.Step != residency.StepLease {
		t.Errorf("the attach failed at step %q, want %q; a legacy session is refused the residency grant itself", refusal.Step, residency.StepLease)
	}
	if !strings.Contains(err.Error(), "binding.protocol_mode") {
		t.Errorf("the refusal does not name the catalog binding: %v", err)
	}

	// NOTHING WAS TAKEN AND NOTHING WAS STARTED.
	f.svc.mu.Lock()
	_, tracked := f.svc.sessions[key]
	f.svc.mu.Unlock()
	if tracked {
		t.Errorf("the composition tracks a session whose lease was refused")
	}
	if _, running := f.svc.ConsumerFor(key); running {
		t.Errorf("a consumer was started for a session whose lease was refused")
	}

	// THE CONTROL. The same fixture, the same Host, a disposition session: the
	// refusal above is a decision about the binding rather than a Host that
	// cannot attach anything.
	if held := f.attach(tenantA, sessionA); !held.Attached {
		t.Fatalf("the control attach of a disposition session did not establish a residency")
	}
	if _, running := f.svc.ConsumerFor(registry.Key{TenantID: tenantA, SessionID: sessionA}); !running {
		t.Fatalf("the control session started no consumer, so the probe above cannot distinguish a refusal from a broken fixture")
	}
}

// ---------------------------------------------------------------------------
// The two axes the composed suite could not see
// ---------------------------------------------------------------------------
//
// Both tests below exist because a mutant survived the whole compose package,
// and both mutants were survivors of the SAME shape as the defect this change
// was justified by fixing: a double corrected on the axis it was caught on and
// left loose on every other. The properties are covered in internal/commands;
// what was missing is that the COMPOSITION could not see them, and a composition
// is where "the consumer this Host built is pointed at the session it attached"
// is decided.

// TestEachAttachedSessionConsumesItsOwnStream is the key axis.
//
// A composition that built every session's consumer with one hard-coded key
// compiled, attached two sessions, started two consumers and passed every
// composed assertion in this package — because one inbox answered every ask with
// one slice. It is not an equivalent mutation: three distinct keys genuinely
// reach beginWork in this suite. What was missing was an assertion that could
// tell them apart, which is this one.
func TestEachAttachedSessionConsumesItsOwnStream(t *testing.T) {
	first := registry.Key{TenantID: tenantA, SessionID: sessionA}
	second := registry.Key{TenantID: tenantA, SessionID: sessionwire.SessionID("session-second")}

	inbox := &fakeInbox{perSession: map[registry.Key][]commands.Command{
		first:  {{TenantID: first.TenantID, SessionID: first.SessionID, CommandID: "command-for-first", AcceptedOrder: 1, State: commands.StatePending}},
		second: {{TenantID: second.TenantID, SessionID: second.SessionID, CommandID: "command-for-second", AcceptedOrder: 1, State: commands.StatePending}},
	}}
	cursors := &fakeCursors{perSession: map[registry.Key]uint64{}}

	f := newFixture(t, func(o *Options, _ *host.Options) {
		o.Inbox = inbox
		o.Cursors = cursors
	})
	f.start()
	f.attach(first.TenantID, first.SessionID)
	f.attach(second.TenantID, second.SessionID)

	// EACH CONSUMER BLOCKS ON ITS OWN SESSION'S COMMAND. Under a hard-coded key
	// both block on the first session's, and both consumers are otherwise
	// indistinguishable from correct ones.
	for _, want := range []struct {
		key     registry.Key
		command sessionwire.CommandID
	}{
		{first, "command-for-first"},
		{second, "command-for-second"},
	} {
		blocked := awaitBlockedFor(t, f, want.key)
		if blocked.CommandID != want.command {
			t.Errorf("the consumer for %s/%s blocked on %q, want %q; it is reading another session's stream",
				want.key.TenantID, want.key.SessionID, blocked.CommandID, want.command)
		}
	}
}

// TestAComposedConsumerListsStrictlyAfterItsDurableCursor is the cursor axis.
//
// A Reconcile that listed from 0 rather than from its durable cursor survived
// the whole compose package, because the inbox double returned everything it
// held whatever it was asked. The discriminator here is a record BELOW the
// cursor that is still non-terminal: a Host honouring its cursor never sees it
// and blocks on the record above; a Host listing from zero blocks on it.
func TestAComposedConsumerListsStrictlyAfterItsDurableCursor(t *testing.T) {
	key := registry.Key{TenantID: tenantA, SessionID: sessionA}
	inbox := &fakeInbox{perSession: map[registry.Key][]commands.Command{key: {
		{TenantID: key.TenantID, SessionID: key.SessionID, CommandID: "command-below-the-cursor", AcceptedOrder: 1, State: commands.StatePending},
		{TenantID: key.TenantID, SessionID: key.SessionID, CommandID: "command-above-the-cursor", AcceptedOrder: 2, State: commands.StatePending},
	}}}
	// THE CURSOR IS ALREADY PAST THE FIRST RECORD, as it would be for a session
	// a predecessor Host consumed part of.
	cursors := &fakeCursors{perSession: map[registry.Key]uint64{key: 1}}

	f := newFixture(t, func(o *Options, _ *host.Options) {
		o.Inbox = inbox
		o.Cursors = cursors
	})
	f.start()
	f.attach(key.TenantID, key.SessionID)

	// THE BOUND IT ASKED WITH, FIRST. The block below reads the CONSEQUENCE of
	// the bound, and a consumer whose own validatePage refuses a not-after-cursor
	// page never blocks at all — so asserting the consequence first makes a real
	// defect fail by TIMEOUT and name the wrong thing. This asserts the cause,
	// immediately, and was reordered after a mutant died here in ten seconds
	// instead of none.
	asks := awaitAsked(t, inbox)
	for _, ask := range asks {
		if ask.Key != key {
			t.Errorf("the inbox was listed for %s/%s, want %s/%s", ask.Key.TenantID, ask.Key.SessionID, key.TenantID, key.SessionID)
		}
		if ask.After < 1 {
			t.Errorf("the inbox was listed from %d with a durable cursor at 1; a consumer lists STRICTLY AFTER its cursor", ask.After)
		}
		if ask.Limit <= 0 {
			t.Errorf("the inbox was listed with limit %d; an unbounded page is not what the seam declares", ask.Limit)
		}
	}

	blocked := awaitBlockedFor(t, f, key)
	if blocked.CommandID == "command-below-the-cursor" {
		t.Fatalf("the consumer reached a command at order 1 with its durable cursor at 1; it is listing from zero rather than strictly after its cursor")
	}
	if blocked.CommandID != "command-above-the-cursor" || blocked.AcceptedOrder != 2 {
		t.Errorf("the consumer blocked on %q at order %d, want %q at 2", blocked.CommandID, blocked.AcceptedOrder, "command-above-the-cursor")
	}
}

// awaitAsked blocks until an inbox has been listed at least once and returns
// every call it received.
func awaitAsked(t *testing.T, inbox *fakeInbox) []inboxAsk {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if asks := inbox.asked(); len(asks) > 0 {
			return asks
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatalf("the inbox was never listed, so nothing here is a statement about the bound")
			return nil
		}
	}
}

// awaitBlockedFor reports what one attached session's consumer stopped at.
//
// IT READS THE CONSUMER'S OWN LAST PASS, so a session that never started a
// consumer, never listed, or listed an empty page is distinguishable from one
// that blocked — three outcomes an absence assertion would fuse.
func awaitBlockedFor(t *testing.T, f *fixture, key registry.Key) commands.BlockedCommand {
	t.Helper()
	f.svc.mu.Lock()
	consumer := f.svc.consumers[key]
	f.svc.mu.Unlock()
	if consumer == nil {
		t.Fatalf("%s/%s has no consumer at all", key.TenantID, key.SessionID)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if blocked := consumer.LastPass().Blocked; blocked != nil {
			return *blocked
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatalf("%s/%s never blocked; its last pass was %+v", key.TenantID, key.SessionID, consumer.LastPass())
			return commands.BlockedCommand{}
		}
	}
}

// TestAWedgedSessionIsVisibleOnTheMetricsSurface is the wedge's operational
// signature, and it is here rather than in internal/lifecycle because the claim
// is about a COMPOSED Host: the gauge is only worth anything if the composition
// satisfies the seam.
//
// A DELIBERATE WEDGE NOBODY CAN SEE IS INDISTINGUISHABLE FROM A HANG. A blocked
// pass returns a nil error, so Consumer.Failures() stays at zero; nothing in
// internal/commands logs; and the HostLink delivery is answered `accepted`. This
// gauge is the only exported signal that separates "refusing to dispatch, as
// designed" from "idle".
//
// THE CONTROL IS THE SAME GAUGE ON THE SAME HOST. A session with nothing to
// consume must read zero through the identical path, so a non-zero reading is a
// statement about the wedge rather than about a counter that always answers one.
func TestAWedgedSessionIsVisibleOnTheMetricsSurface(t *testing.T) {
	wedged := registry.Key{TenantID: tenantA, SessionID: sessionA}
	idle := registry.Key{TenantID: tenantA, SessionID: sessionwire.SessionID("session-idle")}

	inbox := &fakeInbox{perSession: map[registry.Key][]commands.Command{
		wedged: {{TenantID: wedged.TenantID, SessionID: wedged.SessionID, CommandID: "command-wedged", AcceptedOrder: 1, State: commands.StatePending}},
		idle:   nil,
	}}
	f := newFixture(t, func(o *Options, _ *host.Options) {
		o.Inbox = inbox
		o.Cursors = &fakeCursors{perSession: map[registry.Key]uint64{}}
	})
	f.start()

	// THE CONTROL FIRST, so that "zero" is observed on a live Host rather than
	// inferred from the absence of an attach.
	f.attach(idle.TenantID, idle.SessionID)
	if blocked := f.svc.Metrics().Snapshot().BlockedSessions; blocked != 0 {
		t.Fatalf("a Host holding one session with an empty inbox reports %d blocked sessions, want 0", blocked)
	}

	f.attach(wedged.TenantID, wedged.SessionID)
	awaitBlockedFor(t, f, wedged)

	if blocked := f.svc.Metrics().Snapshot().BlockedSessions; blocked != 1 {
		t.Errorf("a Host with one wedged session and one idle one reports %d blocked sessions, want 1", blocked)
	}

	// AND IT IS ON THE COLLECTOR, not only on the snapshot. A gauge computed and
	// never published is the same invisibility in a different place.
	if !strings.Contains(collect(t, f.svc.Metrics()), "host_sessions_command_blocked") {
		t.Errorf("the blocked-sessions gauge is not published by the collector")
	}
}

// collect renders every metric the collector publishes, as descriptor text.
func collect(t *testing.T, collector prometheus.Collector) string {
	t.Helper()
	out := make(chan prometheus.Metric, 64)
	collector.Collect(out)
	close(out)
	var rendered strings.Builder
	for metric := range out {
		rendered.WriteString(metric.Desc().String())
		rendered.WriteString("\n")
	}
	return rendered.String()
}

// A HOST THAT CANNOT BIND A WRITER TO ITS RESIDENCY GRANT DOES NOT ATTACH, and
// this test exists because two mutants proving otherwise SURVIVED the composed
// suite: one that ignored the writer factory's refusal, and one that accepted a
// session with no recorded grant at all.
//
// THE FAILURE IT PREVENTS IS THE ONE THE WHOLE DISPATCH BOUNDARY EXISTED FOR. A
// Host that attached anyway would hold residency of a session it can list and
// consume but never apply in: every command would reach the claim edge, be
// refused for want of a grant, and block the pass — durably, silently, for as
// long as this Host held the session, while a successor that could have applied
// them is told the session is taken.
//
// THE REFUSAL IS AT ATTACH AND NOT AT THE FIRST COMMAND, which is the whole
// point: the attach is the last moment at which nothing has been taken.
func TestAnAttachIsRefusedWhenNoWriterCanBeBoundToTheGrant(t *testing.T) {
	refusal := errors.New("compose_test: this store did not issue that residency lease")
	f := newFixture(t, func(o *Options, _ *host.Options) {
		o.Writers = &fakeDispositionWriters{refuse: refusal}
	})
	f.start()

	_, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID: tenantA, SessionID: sessionA, AgentID: testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	})
	if err == nil {
		t.Fatal("the attach succeeded with no disposition writer bound to its grant")
	}
	if !errors.Is(err, refusal) {
		t.Errorf("Attach = %v, want it to carry the writer factory's own refusal", err)
	}
	// AND NOTHING IS LEFT HOLDING THE SESSION. An attach that failed after
	// recording residency would be the same wedge by another route.
	if _, held := f.svc.ConsumerFor(keyA); held {
		t.Error("a consumer was started for a session whose writer could not be bound")
	}

	// THE CONTROL. The same fixture with a working factory attaches, so the
	// refusal above is about the writer and not about a fixture that cannot
	// attach at all.
	g := newFixture(t)
	g.start()
	g.attach(tenantA, sessionA)
	if _, held := g.svc.ConsumerFor(keyA); !held {
		t.Error("the control attach started no consumer")
	}
}

// THE COMPOSED HOST DISCOVERS ITS RUNTIME'S RECOVERY CLOSER, AND CONVERTS AN
// ABSENT ONE INTO A REAL NIL. Three mutants covering this region all SURVIVED
// before this test existed, and the reason was a STRUCTURAL blind spot rather
// than a value one: no composed runtime implemented department.AttemptCloser, so
// "discover it by assertion" and "never discover it" produced the same nil in
// every fixture, and "wrap nil in an adapter" produced a typed nil nothing
// called.
//
// ONE OF THOSE THREE ALSO SCORED A FALSE KILL. The typed-nil mutant was reported
// KILLED by TestNewBuildsAndDoesNotStart — a known goroutine-count flake — and
// SURVIVED on a second run. A kill attributed to a flaky test is not a kill.
//
// THE TWO ROWS ARE THE TWO STRUCTURES. A runtime that IS a closer must reach the
// applier as one; a runtime that is not must reach it as a REAL nil, because the
// applier's "no closer" arm tests for nil and a typed nil would panic at the one
// command that needs it instead of blocking the pass.
func TestTheComposedApplierGetsTheRuntimesCloserOrARealNil(t *testing.T) {
	for _, row := range []struct {
		name      string
		closer    department.AttemptCloser
		wantFound bool
	}{
		{"a runtime that offers a recovery closure", stubComposedCloser{}, true},
		{"a runtime that does not", nil, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			got := closerOrNil(row.closer)
			if row.wantFound {
				if got == nil {
					t.Fatal("closerOrNil discarded a closer the runtime offers")
				}
				if err := got.CloseAttempt(t.Context(), "command-a", uuid.UUID{}, commands.KindInput, "attempt-1", 1); err == nil {
					t.Error("the adapter did not forward to the runtime's own closer")
				}
				return
			}
			// A REAL NIL AND NOT A TYPED ONE. `got == nil` is the applier's own
			// check, so this comparison is the exact thing under test.
			if got != nil {
				t.Fatalf("closerOrNil returned %T for a runtime with no closer; the applier's nil check would miss it", got)
			}
		})
	}
}

// stubComposedCloser is a runtime capability that refuses, which is enough to
// show the forwarding happened.
type stubComposedCloser struct{}

func (stubComposedCloser) CloseAttempt(context.Context, sessionwire.CommandID, uuid.UUID, string, string, uint64) error {
	return errors.New("compose_test: this runtime's closer refuses")
}

// THE `lease == nil` GUARD IN beginWork IS AN EQUIVALENT MUTANT HERE, MEASURED
// RATHER THAN ARGUED, and it is recorded so the next reader does not spend the
// same hour on it.
//
// Deleting it SURVIVES this suite, and a search over the whole domain says why
// rather than merely that no fixture reaches it. There are exactly two routes to
// a nil recorded lease. leaseRecorder writes the map on EVERY successful
// acquisition and returns early on every failure, so the map cannot be missing
// an entry for a session whose acquisition succeeded; and the other route — an
// inner SessionLeases that reports success and grants nothing — is refused by
// internal/residency's own step 2, which tests `lease == nil` before the runtime
// exists and long before ownership begins. So no input reaches beginWork with a
// nil lease.
//
// IT IS KEPT ANYWAY, and the reason is that the argument above is about the
// CURRENT wiring rather than about the guard. The alternative to refusing is a
// nil reaching the writer factory and being refused there as "foreign", which
// names the wrong fault for a maintainer who changed the recorder.
