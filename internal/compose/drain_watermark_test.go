package compose

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
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
	return newFixture(t, func(_ *Options, options *host.Options) {
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
	endpoint := "ws" + strings.TrimPrefix(serverURL, "http") + HostLinkPathPrefix + string(tenant)
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if err != nil {
		status := "no response"
		if response != nil {
			status = response.Status
		}
		t.Logf("the HostLink handshake for %s was refused after the drain (%s): %v", tenant, status, err)
		return false
	}
	t.Cleanup(func() { _ = connection.Close() })
	return true
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

// TestAnAuthenticatedHostLinkDrainIsIdempotentlyObservableUpToRelease is the
// SECOND half of O7.3's gate line, measured — and it does NOT reach the gate's
// wording. Its name says "up to release" and not "through release" because
// that is what the module does, and the difference is the finding this file
// exists to record rather than paper over. The last subtest below asserts the
// boundary itself, so the day it moves this test fails and says so.
//
// WHAT IS TRUE, AND IS ASSERTED HERE. While the drain runs, an authenticated
// HostLink drain is idempotent and observable in all three senses the gate
// wants:
//
//  1. IDEMPOTENT. A repeat drain begins nothing new: the generation is
//     unchanged, no second target withdrawal is written, and — the assertion
//     that would catch a repeat which actually re-ran the drain — the runtime
//     is released exactly ONCE across the whole sequence.
//  2. OBSERVABLE. hostlink.drain_status answers with the same generation and
//     starts nothing. The separate-method design exists so that "no drain
//     here" can never be read as "the drain finished".
//  3. NOT KEY-SCOPED. Every request below carries a DIFFERENT idempotency key
//     on purpose. drainScope does not consult the key at all, and a Host that
//     answered from a key-scoped cache would be making a statement about the
//     PAST. Reusing one key here would let such a cache pass.
//
// WHAT IS NOT TRUE, AND IS ALSO ASSERTED HERE. The drained state is NOT
// observable over HostLink at all — not late, not racily, NEVER — and it is a
// structural impossibility rather than a timing one. Drainer.run closes
// options.Link BEFORE it assigns d.drainState = d.settledState() and before it
// closes d.done, so the ONLY transport a Factory could ask on is gone strictly
// before the answer it would ask for exists. Measured, not read: after Wait
// returns, links.all() is empty and a fresh WebSocket handshake to the same
// HostLink path is refused with 400. In-process ObserveDrain reports
// state "drained" at the same instant, so the state is real and it is the
// TRANSPORT that is missing, which is what makes this a gap and not an absence
// of behaviour.
//
// WHY THAT MATTERS AND IS NOT TIDIED AWAY. The stop-order test's own comment
// gives the reason the link is held open across the drain: "Factory reaches the
// bounded drain-status observation through the link, so a link closed when the
// drain began would leave it inferring completion from a disconnect — the
// inference the status observation exists to replace." At COMPLETION that is
// exactly what a Factory is left with. For the process-lifecycle drain it is
// arguably moot, since the process is leaving; for a HostLink-INITIATED drain
// of a dedicated Host it is not, because the Host is still running. Whether to
// keep HostLink serving after a link-initiated drain is a DESIGN decision about
// Host's lifecycle, not a test fix, and it is root's to make. Until it is made,
// O7.3's gate line is not satisfied in its second half. See
// CODEX_RESULT_O7.3_WATERMARK.md.
func TestAnAuthenticatedHostLinkDrainIsIdempotentlyObservableUpToRelease(t *testing.T) {
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

	// THE BOUNDARY. This is the half of the gate line the module does not
	// reach, asserted as the fact it is so that the day it changes this test
	// fails loudly rather than a gate line being ticked on a stale reading.
	t.Run("the drained state is unreachable over HostLink because the drain closed the transport first", func(t *testing.T) {
		// The state EXISTS. Without this the two absences below would be
		// satisfied by a drain that simply never finished.
		settled, begun := f.svc.drainer.ObserveDrain(hostlink.DrainScope{})
		if !begun || settled.State != sessionwire.HostLinkDrainStateDrained {
			t.Fatalf("in process the drain reports (begun %v, state %q), want a begun drain in state %q: "+
				"the transport absences below are only a GAP if the answer a Factory wants exists",
				begun, settled.State, sessionwire.HostLinkDrainStateDrained)
		}
		if settled.Generation != initiation.DrainGeneration {
			t.Errorf("the settled drain carries generation %d, want the initiation's %d", settled.Generation, initiation.DrainGeneration)
		}

		// AND NO LINK SURVIVES TO CARRY IT. Drainer.run closes options.Link
		// before it assigns the settled state, so this is an ordering the drain
		// guarantees and not a race this test won.
		if live := len(f.svc.links.all()); live != 0 {
			t.Fatalf("%d tenant links survived the drain. IF THIS FIRES, THE DRAINED STATE MAY NOW BE OBSERVABLE OVER HOSTLINK "+
				"AND O7.3's GATE LINE MAY BE SATISFIABLE THROUGH RELEASE. Re-measure it: ask hostlink.drain_status over the "+
				"surviving link and assert the generation is the initiation's and the state is %q.", live, sessionwire.HostLinkDrainStateDrained)
		}
		if hostLinkHandshakeAccepted(t, server.URL, tenantA) {
			t.Fatal("a fresh authenticated HostLink connection was accepted after the drain completed. IF THIS FIRES, A FACTORY " +
				"CAN RECONNECT AND ASK FOR THE DRAINED STATE, so re-measure O7.3's second half over the reconnected link " +
				"instead of accepting this boundary.")
		}
	})
}
