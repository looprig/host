package compose

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// This file is task O7.3's completion-gate line, measured in the one shape the
// module can actually reach it in.
//
// THE GATE LINE. "authenticated HostLink drain marks capacity/session
// nonaccepting before reporting initiation and is idempotently observable
// through release."
//
// WHY IT IS HERE AND NOT IN internal/compose/service_test.go's pooled
// watermark test, and not in internal/realtime/hostlink's drain tests. The two
// existing measurements each hold one half and neither holds the gate's shape:
//
//   - TestTheDrainAcknowledgesAfterNonacceptingIsDurableAndBeforeAnyCheckpoint
//     has the watermark and the ordering, but it is a POOLED fixture and it
//     calls f.svc.drainer.StartDrain directly — no Multiplexer, no resolver, no
//     connection. After R-1 a pooled Host answers NO HostLink drain at all, so
//     that fixture cannot be promoted; it measures the process-lifecycle path.
//   - internal/realtime/hostlink's drain tests go through the resolver but over
//     a STUB Drainer, so there is no advertiser, no directory and no durable
//     row for a watermark to be taken over.
//   - TestADedicatedHostRefusesADrainFromATenantThatDoesNotHoldItsSession is
//     the dedicated composed fixture this file reuses wholesale — the same
//     three option overrides, the same attach, the same resident-session
//     non-vacuity check — but it asserts REFUSAL and drives mux.StartDrain
//     in-process rather than over a connection.
//
// WHAT IS REACHABLE, which is what this file measures. After R-1 there is
// exactly one authenticated HostLink drain in existence: a DEDICATED Host's
// fixed session, named by the tenant that CURRENTLY HOLDS it, over that
// tenant's own authenticated link, after its attach has completed. Every other
// spelling is refused by drainScope's ladder, and those refusals are R-1's and
// are asserted elsewhere. This file does not re-derive them and must not be
// edited to "also cover" them — a fixture that can tell the too-early refusal
// apart from the wrong-tenant refusal would be disclosing cross-tenant
// occupancy on a cross_tenant_isolated Host, which is the thing R-1 made those
// two deliberately indistinguishable to prevent.

// drainObservationOf decodes one drain RPC reply as an ACKNOWLEDGEMENT.
//
// It fails on a refusal rather than returning a zero observation, because the
// zero observation has State "" and DrainGeneration 0, and every assertion
// below would then be comparing two zeroes and passing. Core's strict decoders
// keep HostLinkError and HostLinkDrainObservation apart in both directions —
// each requires a member the other refuses — so a refusal arrives here as a
// decode failure and not as an empty acknowledgement.
func drainObservationOf(t *testing.T, reply wireReply) sessionwire.HostLinkDrainObservation {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("the drain rpc was refused at the transport: %#v", *reply.Error)
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 || string(reply.RPC.Data) == "null" {
		t.Fatalf("the drain rpc returned no body: %s", mustJSON(t, reply))
	}
	if refusal, isRefusal := refusalOf(t, reply); isRefusal && refusal.Code != "" {
		t.Fatalf("the drain was refused with %q, want an acknowledgement: %s", refusal.Code, reply.RPC.Data)
	}
	var observation sessionwire.HostLinkDrainObservation
	if err := json.Unmarshal(reply.RPC.Data, &observation); err != nil {
		t.Fatalf("the drain reply %s does not decode as a Core drain observation: %v", reply.RPC.Data, err)
	}
	if err := observation.Validate(); err != nil {
		t.Fatalf("the drain reply %s is not a valid Core drain observation: %v", reply.RPC.Data, err)
	}
	return observation
}

// dedicatedDrainFixture is the R-1 dedicated composed Host, held once so the
// two tests below cannot drift apart in their configuration.
//
// THE THREE OVERRIDES ARE A UNIT. Placement without FixedSessionID is refused
// at construction, and Capacity is what makes rung 8's attribution sound; see
// TestRungEightsAttributionDependsOnTheDedicatedCapacityBound, which is the
// trip-wire that fires if the capacity bound is ever relaxed.
func dedicatedDrainFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixture(t, func(_ *Options, options *hostconfig.Options) {
		options.Placement = sessionwire.HostPlacementDedicated
		options.FixedSessionID = sessionA
		options.Capacity = 1
	})
}

// hostLinkHandshakeAccepted reports whether this Host will still UPGRADE a
// HostLink WebSocket for a tenant.
//
// IT IS NOT fixture_test.go's hostLinkConnect, which fails the test on a
// refused handshake because every caller there expects one to succeed. The
// question here is the opposite one — is the endpoint gone? — and a refused
// handshake is the expected answer, so it is reported rather than fatal.
//
// IT ASKS ONLY ABOUT THE UPGRADE and deliberately does not send a connect
// frame. An accepted upgrade whose connect is then refused would make this
// return true, which over-reports reachability; that direction is safe, because
// true is what makes the assertion FIRE and demand a re-measurement.
func hostLinkHandshakeAccepted(t *testing.T, serverURL string, tenant sessionwire.TenantID) bool {
	t.Helper()
	accepted, _ := hostLinkHandshake(t, serverURL, tenant)
	return accepted
}

// hostLinkHandshake reports whether the upgrade was accepted and, when it was
// not, the HTTP status it was refused with.
//
// THE STATUS IS RETURNED BECAUSE IT IS A CONTRACT AND NOT A DETAIL. A Host that
// has stopped is UNAVAILABLE; it used to answer 400 "HostLink requires a valid
// tenant", because resolve returned an anonymous error and the handler's
// fall-through claimed the caller's credential was the problem. A Factory
// reading a 4xx looks at itself and stops retrying.
func hostLinkHandshake(t *testing.T, serverURL string, tenant sessionwire.TenantID) (bool, int) {
	t.Helper()
	endpoint := "ws" + strings.TrimPrefix(serverURL, "http") + HostLinkPathPrefix + string(tenant)
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		t.Logf("the HostLink handshake for %s was refused with %d: %v", tenant, status, err)
		return false, status
	}
	t.Cleanup(func() { _ = connection.Close() })
	return true, http.StatusSwitchingProtocols
}

// fixedSessionDrainRequest is the one drain request this Host answers.
func fixedSessionDrainRequest(key string) sessionwire.HostLinkDrainRequest {
	return sessionwire.HostLinkDrainRequest{
		Version:        sessionwire.CurrentWireVersion,
		HostID:         testHostID,
		HostGeneration: testGen,
		IdempotencyKey: key,
		TenantID:       tenantA,
		SessionID:      sessionA,
	}
}

// TestAnAuthenticatedHostLinkDrainIsNonacceptingBeforeItReportsInitiation is
// the FIRST half of O7.3's gate line, over a real authenticated WebSocket on a
// dedicated composed Host.
//
// IT IS AN ORDERING AND NOT A PAIR OF STATES, and the distinction is the whole
// difference between this test and a green one that proves nothing. Two devices
// carry it.
//
// FIRST, THE WATERMARK. An attach publishes an `attaching` observation that
// ALREADY carries Accepting=false, so an assertion over every row ever written
// is satisfied before the drain does anything at all — measured on the pooled
// sibling, where deleting the drain's publication entirely left the suite green.
// Every row assertion below is taken over f.store.published()[beforeDrain:],
// and beforeDrain is sampled after the attach AND after the link's connect, so
// no row either of them wrote can satisfy it.
//
// SECOND, THE HOLD MAKES "BEFORE" A FACT RATHER THAN A SAMPLE. The drain's
// per-session work runs on its own goroutine and its first durable write is the
// `releasing` observation; the store holds that write until this test lets it
// go. So at the point the RPC reply lands, the ONLY writes that can have
// happened are the ones Drainer.StartDrain made BEFORE it returned its status
// to the Multiplexer. A publication moved to after the acknowledgement — onto
// the run goroutine, or after the return — is therefore not merely unlikely to
// be observed here, it is BLOCKED, and the assertion fails deterministically.
// That reversal is the positive control and it was run: see
// CODEX_RESULT_O7.3_WATERMARK.md's mutation table, row PC1.
//
// THE CHECKPOINT ASSERTION IS THE OTHER DIRECTION. An acknowledgement that
// waited for release would make the RPC whose contract is "initiation plus
// generation" block for the length of every resident session's release.
func TestAnAuthenticatedHostLinkDrainIsNonacceptingBeforeItReportsInitiation(t *testing.T) {
	f := dedicatedDrainFixture(t)
	f.start()
	f.store.holdReleasing()
	f.attach(tenantA, sessionA)

	// NON-VACUITY 1: the link's tenant holds the fixed session. Without this a
	// dedicated Host refuses every drain that reaches rung 8 — the refusal R-1
	// made deliberately indistinguishable from "another tenant holds it" — and
	// every assertion below would be about a refusal rather than a drain.
	resident := map[registry.Key]bool{}
	for _, held := range f.svc.ResidentSessions() {
		resident[held.Key()] = true
	}
	if !resident[keyA] {
		t.Fatal("the fixed session is not resident, so this Host answers no HostLink drain and this test measures nothing")
	}

	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	// NON-VACUITY 2: the connection is AUTHENTICATED, not merely open.
	// dialFactoryLink fails a refused connect, but a Host whose auth seam was
	// never consulted would accept the same connect and the word
	// "authenticated" in the gate line would be describing nothing.
	if verifications := f.auth.verifications(); len(verifications) == 0 {
		t.Fatal("the link connected without the auth seam being consulted once, so this drain is not an AUTHENTICATED HostLink drain")
	}

	beforeDrain := len(f.store.published())
	withdrawnBefore := len(f.directory.withdrawals())

	initiation := drainObservationOf(t, link.rpc(2, hostlink.MethodDrain, fixedSessionDrainRequest("o73-initiation")))
	if initiation.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the drain reported state %q at initiation, want %q: the per-session work is held, so a drained answer here means the hold is not holding and the ordering below is a sample rather than a fact",
			initiation.State, sessionwire.HostLinkDrainStateDraining)
	}
	if initiation.DrainGeneration == 0 {
		t.Fatal("the acknowledgement carries generation 0, so the idempotency half below would be comparing zeroes")
	}

	// THE CAPACITY HALF: the Host's own target advertisement is durably
	// withdrawn. A row still carrying an advertisement stays ranked and stays
	// due, so a Factory paging candidates would go on finding a Host that has
	// already told a placement controller it stopped accepting.
	if withdrawnNow := len(f.directory.withdrawals()); withdrawnNow == withdrawnBefore {
		t.Errorf("the drain reported initiation over the link with %d target withdrawals, unchanged from before it: capacity was still advertised when the initiation was reported", withdrawnNow)
	}

	// THE SESSION HALF: the resident session's own registry row is durably
	// nonaccepting, written during the drain and not by the attach.
	duringDrain := f.store.published()[beforeDrain:]
	if !slices.ContainsFunc(duringDrain, func(row sessionwire.HostLinkRegistryObservation) bool {
		return row.TenantID == tenantA && row.SessionID == sessionA && !row.Accepting && row.Residency == sessionwire.SessionResidencyResident
	}) {
		t.Errorf("the drain reported initiation over the link before it published a nonaccepting row for the RESIDENT fixed session; rows written during the drain: %#v", duringDrain)
	}

	// AND NOT PAST INITIATION: no session has been checkpointed yet.
	if position := f.trace.indexOf("checkpoint"); position != -1 {
		t.Errorf("a checkpoint ran at trace position %d, before the drain reported initiation over the link: %v", position, f.trace.trace())
	}

	f.store.releaseHold()
	report := f.svc.drainer.Wait()
	if len(report.Failures) != 0 {
		t.Fatalf("the drain reported failures: %v", report.Failures)
	}
	// THE CLOSER. A drain that checkpointed nothing at all would satisfy the
	// absence asserted above without the ordering being true of anything.
	if f.trace.indexOf("checkpoint") == -1 {
		t.Fatalf("no checkpoint ran at all, so the ordering asserted above was vacuous: %v", f.trace.trace())
	}
}

// TestAnAuthenticatedHostLinkDrainIsIdempotentlyObservableThroughRelease is the
// SECOND half of O7.3's gate line.
//
// IT USED TO BE NAMED "UpToRelease" AND TO ASSERT THE OPPOSITE, which is worth
// knowing before trusting it. The drain closed every transport as its last
// cleanup step, BEFORE settledState assigned the terminal state, so the
// `drained` answer was unreachable over HostLink by construction — and the
// Centrifuge 3001 disconnect that replaced it is IDENTICAL whether release
// finished or FinishRelease refused, which is the one case Core's contract and
// settledState both exist to keep apart. This test recorded that as a boundary;
// the drain now closes nothing and the boundary is gone.
//
// FOUR CLAIMS, in the order the sequence makes them:
//
//  1. IDEMPOTENT. A repeat drain begins nothing new: same generation, same
//     state, no second target withdrawal, and the runtime released exactly ONCE
//     across the whole sequence.
//  2. OBSERVABLE. hostlink.drain_status answers the same generation and runs no
//     checkpoint. The separate-method design exists so "no drain here" can never
//     be read as "the drain finished".
//  3. THROUGH RELEASE, over THE SAME LINK, including the TERMINAL answer. This
//     is the half the gate line is really about: a Factory deletes a dedicated
//     workload on `drained` and must READ it.
//  4. AND THE LINK IS STILL OPEN AFTERWARDS, because three assertions about
//     answers could all be satisfied by a Host that answered once and then
//     dropped its Factory.
//
// NOT KEY-SCOPED. Every request carries a DIFFERENT idempotency key on purpose.
// drainScope does not consult the key at all, and a Host answering from a
// key-scoped cache would be making a statement about the PAST. Reusing one key
// would let such a cache pass.
//
// THE MUTATOR PATH IS NOT PART OF THIS. hostlink.drain after release is refused
// and that is asserted inline where it happens; see the comment there and
// TestRelaxingRungEightForObservationDisclosesNothingToAnyoneElse.
func TestAnAuthenticatedHostLinkDrainIsIdempotentlyObservableThroughRelease(t *testing.T) {
	f := dedicatedDrainFixture(t)
	f.start()
	f.store.holdReleasing()
	f.attach(tenantA, sessionA)

	resident := map[registry.Key]bool{}
	for _, held := range f.svc.ResidentSessions() {
		resident[held.Key()] = true
	}
	if !resident[keyA] {
		t.Fatal("the fixed session is not resident, so this Host answers no HostLink drain and this test measures nothing")
	}

	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	initiation := drainObservationOf(t, link.rpc(2, hostlink.MethodDrain, fixedSessionDrainRequest("o73-idempotent-first")))
	if initiation.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("the first drain reported %q, want %q", initiation.State, sessionwire.HostLinkDrainStateDraining)
	}
	withdrawnAfterFirst := len(f.directory.withdrawals())
	if withdrawnAfterFirst == 0 {
		t.Fatal("the first drain withdrew nothing, so 'no second withdrawal' below is vacuous")
	}

	// (1) IDEMPOTENT, over the same authenticated link.
	repeat := drainObservationOf(t, link.rpc(3, hostlink.MethodDrain, fixedSessionDrainRequest("o73-idempotent-repeat")))
	if repeat.DrainGeneration != initiation.DrainGeneration {
		t.Errorf("a repeat drain answered with generation %d, want %d: a Factory retrying its own request would read a second drain",
			repeat.DrainGeneration, initiation.DrainGeneration)
	}
	if repeat.State != sessionwire.HostLinkDrainStateDraining {
		t.Errorf("a repeat drain answered with state %q, want %q", repeat.State, sessionwire.HostLinkDrainStateDraining)
	}
	if withdrawnNow := len(f.directory.withdrawals()); withdrawnNow != withdrawnAfterFirst {
		t.Errorf("a repeat drain withdrew %d targets in total, want the first drain's %d: it re-ran the publication", withdrawnNow, withdrawnAfterFirst)
	}

	// (2) OBSERVABLE, and starting nothing.
	observed := drainObservationOf(t, link.rpc(4, hostlink.MethodDrainStatus, fixedSessionDrainRequest("o73-idempotent-observe")))
	if observed.DrainGeneration != initiation.DrainGeneration {
		t.Errorf("drain_status answered with generation %d, want %d", observed.DrainGeneration, initiation.DrainGeneration)
	}
	if observed.State != sessionwire.HostLinkDrainStateDraining {
		t.Errorf("drain_status answered with state %q while the release is held, want %q", observed.State, sessionwire.HostLinkDrainStateDraining)
	}
	if position := f.trace.indexOf("checkpoint"); position != -1 {
		t.Errorf("observing a drain ran a checkpoint at trace position %d: %v", position, f.trace.trace())
	}

	// RELEASE HAPPENS HERE.
	f.store.releaseHold()
	report := f.svc.drainer.Wait()
	if len(report.Failures) != 0 {
		t.Fatalf("the drain reported failures: %v", report.Failures)
	}
	// The closer for every "nothing re-ran" assertion above: a drain that
	// released nothing satisfies all of them without the idempotency being
	// true of anything.
	if f.trace.indexOf("checkpoint") == -1 {
		t.Fatalf("no checkpoint ran at all, so the idempotency asserted above was vacuous: %v", f.trace.trace())
	}
	if released := f.runtime.Released(); released != 1 {
		t.Errorf("the runtime was released %d times across two drain requests and one observation, want 1", released)
	}

	// (2) AND (1) THROUGH RELEASE, over the SAME link, which is the half of the
	// gate line that was unreachable until the drain stopped closing the
	// transport. The terminal answer is the point: a Factory deletes a
	// dedicated workload on `drained` and must read it, not infer it from a
	// disconnect that is ambiguous between success and a refused FinishRelease.
	afterRelease := drainObservationOf(t, link.rpc(5, hostlink.MethodDrainStatus, fixedSessionDrainRequest("o73-idempotent-after")))
	if afterRelease.DrainGeneration != initiation.DrainGeneration {
		t.Errorf("drain_status after release answered with generation %d, want the initiation's %d: the generation is not stable through release",
			afterRelease.DrainGeneration, initiation.DrainGeneration)
	}
	if afterRelease.State != sessionwire.HostLinkDrainStateDrained {
		t.Errorf("drain_status after release answered with state %q, want the terminal %q", afterRelease.State, sessionwire.HostLinkDrainStateDrained)
	}
	// AND BEGINNING IS STILL NOT OBSERVING. `hostlink.drain` after release is
	// REFUSED, and the asymmetry is deliberate rather than an oversight: R-1's
	// rung 8 attributes the right to BEGIN a Host-wide drain to the tenant that
	// HOLDS the fixed session, and release ends that. The observation path is
	// relaxed because it begins nothing and tells only the caller that already
	// received the acknowledgement; the mutator path is not relaxed at all.
	// A Factory whose retry spans completion must ask drain_status, which is
	// the method whose entire purpose is to look without initiating.
	postRelease := link.rpc(6, hostlink.MethodDrain, fixedSessionDrainRequest("o73-idempotent-post"))
	if refusal, isRefusal := refusalOf(t, postRelease); !isRefusal {
		t.Errorf("hostlink.drain after release was ACCEPTED (%s); beginning a Host-wide drain is attributed to the tenant that HOLDS the fixed session and this one no longer does", mustJSON(t, postRelease))
	} else if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Errorf("hostlink.drain after release was refused with %q, want %q", refusal.Code, sessionwire.HostLinkErrorRuntimeUnavailable)
	}

	if released := f.runtime.Released(); released != 1 {
		t.Errorf("the runtime was released %d times across the whole sequence, want 1: a drain after release re-ran it or the refusal above ran one", released)
	}
	if withdrawnNow := len(f.directory.withdrawals()); withdrawnNow != withdrawnAfterFirst {
		t.Errorf("%d targets were withdrawn in total, want the first drain's %d: a drain after release re-published", withdrawnNow, withdrawnAfterFirst)
	}

	// AND THE TRANSPORT IS STILL OPEN, because it is not the drain's to close.
	// Without this the four assertions above could be satisfied by a Host that
	// answered once and then dropped the link, which is the disconnect the
	// observation exists to replace.
	if live := len(f.svc.links.all()); live == 0 {
		t.Error("no tenant link survived the drain, so the answers above came from a Host that has already dropped its Factory")
	}
}

// TestRelaxingRungEightForObservationDisclosesNothingToAnyoneElse is the
// security row for the one rung this work relaxed, and it is the row that must
// not be deleted.
//
// WHAT WAS RELAXED. drainScope's rung 8 refuses a fixed-session drain unless
// the requesting tenant currently HOLDS that session. Release drops the
// residency, so under that rule the terminal `drained` answer was unreachable
// to the only caller entitled to it at exactly the instant it became true. The
// OBSERVATION path — and only it — now also admits the caller that BEGAN this
// drain, matched on the tenant AND session the drain was begun with.
//
// WHY THAT DISCLOSES NOTHING NEW, asserted rather than argued. The relaxation's
// match set is a subset of "callers that already received the acknowledgement".
// A tenant that never held the fixed session began nothing, so it matches
// nothing — and this test is the measurement, over the hardest case available:
// the drain has ALREADY completed, so the holder check fails for BOTH tenants
// and the relaxation is the only thing that can separate them. tenant-b must
// still be refused, with the SAME refusal class it got before the relaxation,
// because R-1 made that refusal deliberately indistinguishable from "another
// tenant holds it" on a cross_tenant_isolated Host.
//
// THE MUTATOR PATH IS NOT RELAXED AT ALL, and the last assertion holds that:
// tenant-a itself, which DID begin the drain, is still refused hostlink.drain
// after release. Beginning a Host-wide drain is attributed to a holder; only
// looking is attributed to the beginner.
func TestRelaxingRungEightForObservationDisclosesNothingToAnyoneElse(t *testing.T) {
	f := dedicatedDrainFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	server := f.serve()

	owner := dialFactoryLink(t, server.URL, tenantA)
	initiation := drainObservationOf(t, owner.rpc(2, hostlink.MethodDrain, fixedSessionDrainRequest("disclosure-begin")))
	f.svc.drainer.Wait()

	// THE BEGINNER READS THE TERMINAL ANSWER. Without this the refusals below
	// would be satisfied by a Host that refuses everyone, which is the old
	// behaviour under a new test's name.
	settled := drainObservationOf(t, owner.rpc(3, hostlink.MethodDrainStatus, fixedSessionDrainRequest("disclosure-own")))
	if settled.State != sessionwire.HostLinkDrainStateDrained || settled.DrainGeneration != initiation.DrainGeneration {
		t.Fatalf("the tenant that began the drain observed %q at generation %d, want %q at %d",
			settled.State, settled.DrainGeneration, sessionwire.HostLinkDrainStateDrained, initiation.DrainGeneration)
	}

	// AND NOBODY ELSE DOES. tenant-b holds nothing, began nothing, and names
	// this Host's fixed session under its own tenant — every rung above 8
	// passes.
	stranger := dialFactoryLink(t, server.URL, tenantB)
	attack := fixedSessionDrainRequest("disclosure-attack")
	attack.TenantID = tenantB
	refusal, isRefusal := refusalOf(t, stranger.rpc(4, hostlink.MethodDrainStatus, attack))
	if !isRefusal {
		t.Fatal("A TENANT THAT NEVER HELD THE FIXED SESSION OBSERVED THIS HOST'S DRAIN. The observation " +
			"relaxation must match the tenant AND session the drain was BEGUN with; a match on the session " +
			"alone, or on 'a drain has begun', hands cross-tenant occupancy to any link that can name the " +
			"fixed session on a cross_tenant_isolated Host.")
	}
	if refusal.Code != sessionwire.HostLinkErrorRuntimeUnavailable {
		t.Errorf("the stranger's observation was refused with %q, want %q: R-1 keeps this refusal indistinguishable from the one a too-early caller gets",
			refusal.Code, sessionwire.HostLinkErrorRuntimeUnavailable)
	}

	// AND THE MUTATOR IS UNCHANGED even for the beginner.
	if _, isRefusal := refusalOf(t, owner.rpc(5, hostlink.MethodDrain, fixedSessionDrainRequest("disclosure-restart"))); !isRefusal {
		t.Error("hostlink.drain after release was accepted for the tenant that began it; only the OBSERVATION path is relaxed")
	}
}

// TestADrainThatCouldNotFinishReleaseAnswersDrainingOverTheSameLink is the
// gate line's parenthesis — "including the terminal `drained` (or WITHHELD
// `draining`) answer" — and it is the case the whole transport change exists
// for.
//
// THE TWO OUTCOMES USED TO BE THE SAME BYTES ON THE WIRE. The drain closed
// every transport as its last step, and `links.Close` is `node.Shutdown`, which
// sends Centrifuge `DisconnectShutdown` 3001. It sent 3001 whether release
// finished or FinishRelease refused — so the ONE signal reaching a Factory was
// ambiguous between "release finished, delete the dedicated workload" and "this
// Host still holds the lease". settledState goes to considerable trouble to
// keep those apart, in its own words because `drained` "is the signal a Factory
// deletes a dedicated workload on, so it destroys work whose lease this Host
// still holds" — and then the transport threw the distinction away.
//
// SO THIS TEST IS THE OTHER HALF OF THE SIBLING ABOVE, and neither is worth
// much alone: one shows the terminal `drained` crosses the link, this one shows
// that when release did NOT finish, the SAME link carries `draining` instead.
// A Factory can tell them apart because there are two answers, not because it
// guesses from a socket.
//
// THE FAILURE IS INJECTED AT THE TOMBSTONE because that is what FinishRelease
// does. A checkpoint failure or a refused ReleaseResidency would be recorded
// and STILL report drained, which is settledState's rule and not this test's.
func TestADrainThatCouldNotFinishReleaseAnswersDrainingOverTheSameLink(t *testing.T) {
	f := dedicatedDrainFixture(t)
	f.start()
	f.attach(tenantA, sessionA)
	server := f.serve()
	link := dialFactoryLink(t, server.URL, tenantA)

	refused := errors.New("the durable store would not accept the epoch-fenced tombstone")
	f.store.refuseTombstones(refused)

	initiation := drainObservationOf(t, link.rpc(2, hostlink.MethodDrain, fixedSessionDrainRequest("withheld-begin")))
	report := f.svc.drainer.Wait()

	// NON-VACUITY: the drain really did fail at FinishRelease, and at nothing
	// else that would have withheld `drained` for a different reason.
	var finishFailures int
	for _, failure := range report.Failures {
		if failure.Step == lifecycle.StepFinishRelease {
			finishFailures++
			if !errors.Is(failure, refused) {
				t.Errorf("the finish-release failure lost its cause: %v", failure)
			}
		}
	}
	if finishFailures != 1 {
		t.Fatalf("failures = %v, want exactly one at %q: the injection did not take, so the answer below is not the WITHHELD one", report.Failures, lifecycle.StepFinishRelease)
	}

	// THE WITHHELD ANSWER, over the same authenticated link, after the drain
	// has finished doing everything it is going to do.
	settled := drainObservationOf(t, link.rpc(3, hostlink.MethodDrainStatus, fixedSessionDrainRequest("withheld-observe")))
	if settled.State != sessionwire.HostLinkDrainStateDraining {
		t.Errorf("a drain whose FinishRelease refused answered %q over the link, want %q. A FACTORY READING %q HERE DELETES A "+
			"DEDICATED WORKLOAD WHOSE LEASE THIS HOST STILL HOLDS.",
			settled.State, sessionwire.HostLinkDrainStateDraining, sessionwire.HostLinkDrainStateDrained)
	}
	if settled.DrainGeneration != initiation.DrainGeneration {
		t.Errorf("the withheld answer carries generation %d, want the initiation's %d", settled.DrainGeneration, initiation.DrainGeneration)
	}
	// AND THE LINK IS STILL THERE TO CARRY IT, which is the whole point: this
	// is exactly the outcome whose signal used to be a 3001 disconnect
	// indistinguishable from success.
	if live := len(f.svc.links.all()); live == 0 {
		t.Error("no link survived a drain that could not finish release, so a Factory is back to inferring this outcome from a disconnect")
	}
}
