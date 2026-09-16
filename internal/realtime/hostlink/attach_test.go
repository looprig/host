package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

// recordingAttacher is an Attacher whose answer a test chooses and whose calls
// a test can count. The count is the load-bearing instrument: every rung of
// the attach ladder below the Attacher is asserted as a refusal AND as zero
// calls, which is how "refuse before any lease" is measured rather than read.
type recordingAttacher struct {
	mu         sync.Mutex
	calls      []hostlink.AttachRequest
	answer     sessionwire.HostLinkRegistryObservation
	refuseWith error
}

func (a *recordingAttacher) Attach(_ context.Context, request hostlink.AttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	a.mu.Lock()
	a.calls = append(a.calls, request)
	a.mu.Unlock()
	if a.refuseWith != nil {
		return sessionwire.HostLinkRegistryObservation{}, a.refuseWith
	}
	return a.answer, nil
}

func (a *recordingAttacher) recorded() []hostlink.AttachRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]hostlink.AttachRequest(nil), a.calls...)
}

// attachRequest is a fully valid Core attach request for one session on this
// fixture's Host.
func attachRequest(session sessionwire.SessionID) sessionwire.HostLinkAttachRequest {
	return sessionwire.HostLinkAttachRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               testTenant,
		SessionID:              session,
		HostID:                 testHostID,
		HostGeneration:         testGeneration,
		AgentID:                "agent-alpha",
		RuntimeCompatibilityID: testRuntime,
		Mode:                   sessionwire.HostLinkAttachModeCreate,
		ActorID:                "factory-service",
		TraceID:                "trace-1",
		IdempotencyKey:         "attach-attempt-1",
	}
}

// acceptedObservation is a valid observation an Attacher would return for the
// fixture's session.
func acceptedObservation(session sessionwire.SessionID) sessionwire.HostLinkRegistryObservation {
	now := time.Unix(1_700_000_000, 0).UTC()
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               testTenant,
		SessionID:              session,
		HostID:                 testHostID,
		HostGeneration:         testGeneration,
		AgentID:                "agent-alpha",
		RuntimeCompatibilityID: testRuntime,
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       "ws://10.0.0.1:7100/hostlink",
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             testEpoch,
		ObservedAt:             now,
		ExpiresAt:              now.Add(time.Minute),
	}
}

func withAttacher(attacher hostlink.Attacher) func(*hostlink.MultiplexerOptions) {
	return func(o *hostlink.MultiplexerOptions) { o.Attacher = attacher }
}

func refusedAttach(t *testing.T, err error) *hostlink.BindError {
	t.Helper()
	if err == nil {
		t.Fatal("the attach was accepted, want a refusal")
	}
	var refusal *hostlink.BindError
	if !errors.As(err, &refusal) {
		t.Fatalf("error = %T %v, want *hostlink.BindError", err, err)
	}
	return refusal
}

// ---------------------------------------------------------------------------
// The ladder
// ---------------------------------------------------------------------------

// TestTheAttachRefusalLadderAnswersWithItsFirstFailingCheck is a SINGLE-FAULT
// matrix over the attach path, in the shape TestEachBindRefusalHasASoleCause
// has for bind. Every row starts from a request the same fixture is proved to
// accept, changes one thing, and asserts one refusal, one wire class, and how
// many times the Attacher was reached.
//
// THE CALL COUNT IS THE OBLIGATION Core put on this Host: a request that names
// another Host or an earlier incarnation, or a tenant this link did not
// authenticate as, must be refused BEFORE any lease is acquired — and the only
// thing on this path that can acquire one is the Attacher. Zero calls is that
// property measured. The rows AT the Attacher then hold the mapping of what it
// returned onto the wire, including the exact holder epoch.
func TestTheAttachRefusalLadderAnswersWithItsFirstFailingCheck(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		perturb func(*recordingAttacher, *hostlink.MultiplexerOptions, *sessionwire.HostLinkAttachRequest)
		refusal hostlink.Refusal
		code    sessionwire.HostLinkErrorCode
		calls   int
		check   func(*testing.T, *hostlink.BindError)
	}{
		{
			name: "malformed request",
			perturb: func(_ *recordingAttacher, _ *hostlink.MultiplexerOptions, r *sessionwire.HostLinkAttachRequest) {
				r.ActorID = ""
			},
			refusal: hostlink.RefusalMalformedRequest,
			calls:   0,
		},
		{
			name: "foreign tenant",
			perturb: func(_ *recordingAttacher, _ *hostlink.MultiplexerOptions, r *sessionwire.HostLinkAttachRequest) {
				r.TenantID = "tenant-other"
			},
			refusal: hostlink.RefusalForeignTenant,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			calls:   0,
		},
		{
			name: "foreign host",
			perturb: func(_ *recordingAttacher, _ *hostlink.MultiplexerOptions, r *sessionwire.HostLinkAttachRequest) {
				r.HostID = "host-beta"
			},
			refusal: hostlink.RefusalForeignHost,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			calls:   0,
		},
		{
			name: "stale host generation",
			perturb: func(_ *recordingAttacher, _ *hostlink.MultiplexerOptions, r *sessionwire.HostLinkAttachRequest) {
				r.HostGeneration = testGeneration + 1
			},
			refusal: hostlink.RefusalStaleHostGeneration,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			calls:   0,
		},
		{
			name: "no attacher composed",
			perturb: func(_ *recordingAttacher, o *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				o.Attacher = nil
			},
			refusal: hostlink.RefusalAttachUnsupported,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			calls:   0,
		},
		{
			name: "attacher refuses with a class",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorRuntimeMismatch, Reason: "wrong build"}
			},
			refusal: hostlink.RefusalAttachRefused,
			code:    sessionwire.HostLinkErrorRuntimeMismatch,
			calls:   1,
		},
		{
			name: "attacher refuses no_capacity",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorNoCapacity}
			},
			refusal: hostlink.RefusalAttachRefused,
			code:    sessionwire.HostLinkErrorNoCapacity,
			calls:   1,
		},
		{
			name: "lease held elsewhere, holder epoch known",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 42}
			},
			refusal: hostlink.RefusalAttachRefused,
			code:    sessionwire.HostLinkErrorEpochMismatch,
			calls:   1,
			check: func(t *testing.T, refusal *hostlink.BindError) {
				wire, _ := refusal.HostLinkError()
				// EXACTLY the holder's epoch: "not zero" would pass a build
				// that published this Host's own epoch or a constant.
				if wire.CurrentLeaseEpoch != 42 {
					t.Fatalf("current_lease_epoch = %d, want the holder's 42", wire.CurrentLeaseEpoch)
				}
				if err := wire.Validate(); err != nil {
					t.Fatalf("the published refusal is a record Core refuses: %v", err)
				}
			},
		},
		{
			name: "lease held elsewhere, holder epoch unknown",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorEpochMismatch}
			},
			refusal: hostlink.RefusalHolderEpochUnknown,
			code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			calls:   1,
			check: func(t *testing.T, refusal *hostlink.BindError) {
				wire, _ := refusal.HostLinkError()
				if err := wire.Validate(); err != nil {
					t.Fatalf("the downgraded refusal is still a record Core refuses: %v", err)
				}
				if wire.CurrentLeaseEpoch != 0 {
					t.Fatalf("the downgraded refusal carries epoch %d; it has none to carry", wire.CurrentLeaseEpoch)
				}
			},
		},
		{
			name: "attacher fails with no class",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = &hostlink.AttachRefusal{Reason: "the store was unreachable"}
			},
			refusal: hostlink.RefusalAttachFailed,
			calls:   1,
		},
		{
			name: "attacher fails with an untyped error",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.refuseWith = errors.New("boom")
			},
			refusal: hostlink.RefusalAttachFailed,
			calls:   1,
		},
		{
			name: "attacher returns an observation Core refuses",
			perturb: func(a *recordingAttacher, _ *hostlink.MultiplexerOptions, _ *sessionwire.HostLinkAttachRequest) {
				a.answer.LeaseEpoch = 0
			},
			refusal: hostlink.RefusalUnpublishableAttach,
			calls:   1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			attacher := &recordingAttacher{answer: acceptedObservation(testSession)}
			var options hostlink.MultiplexerOptions
			f := newFixture(t, withAttacher(attacher), func(o *hostlink.MultiplexerOptions) { options = *o })
			request := attachRequest(testSession)

			// THE CONTROL: the same fixture and request are accepted before the
			// perturbation, so the row below differs in exactly one respect.
			observation, err := f.mux.Attach(t.Context(), request)
			if err != nil {
				t.Fatalf("the control attach was refused: %v", err)
			}
			if observation != acceptedObservation(testSession) {
				t.Fatalf("the control returned %+v, want the attacher's observation", observation)
			}
			if got := attacher.recorded(); len(got) != 1 {
				t.Fatalf("the control reached the attacher %d times, want 1", len(got))
			}
			attacher.calls = nil

			mux := f.mux
			test.perturb(attacher, &options, &request)
			if options.Attacher == nil {
				// Only the "no attacher" row rebuilds; every other row keeps the
				// very Multiplexer the control ran on.
				rebuilt, err := hostlink.NewMultiplexer(options)
				if err != nil {
					t.Fatalf("rebuild without an attacher: %v", err)
				}
				mux = rebuilt
			}

			refusal := refusedAttach(t, mustFailAttach(t, mux, request))
			if refusal.Refusal != test.refusal {
				t.Fatalf("refusal = %q, want %q", refusal.Refusal, test.refusal)
			}
			wire, published := refusal.HostLinkError()
			if test.code == "" {
				if published {
					t.Fatalf("refusal %q published Core class %q, want none", refusal.Refusal, wire.Code)
				}
			} else {
				if !published {
					t.Fatalf("refusal %q published no Core class, want %q", refusal.Refusal, test.code)
				}
				if wire.Code != test.code {
					t.Fatalf("wire class = %q, want %q", wire.Code, test.code)
				}
			}
			if got := len(attacher.recorded()); got != test.calls {
				t.Fatalf("the attacher was reached %d times, want %d; a rung below it must refuse before any lease can be taken", got, test.calls)
			}
			if test.check != nil {
				test.check(t, refusal)
			}
		})
	}
}

func mustFailAttach(t *testing.T, mux *hostlink.Multiplexer, request sessionwire.HostLinkAttachRequest) error {
	t.Helper()
	observation, err := mux.Attach(t.Context(), request)
	if err == nil {
		t.Fatalf("Attach(%+v) succeeded with %+v, want a refusal", request, observation)
	}
	return err
}

// TestAnAcceptedAttachHandsTheAttacherEveryWireFieldExactly is the positive
// half: the resolved request the Attacher receives carries each member of the
// Core record, unchanged, and nothing about the Host.
func TestAnAcceptedAttachHandsTheAttacherEveryWireFieldExactly(t *testing.T) {
	t.Parallel()
	attacher := &recordingAttacher{answer: acceptedObservation(otherSession)}
	f := newFixture(t, withAttacher(attacher))

	request := attachRequest(otherSession)
	request.Mode = sessionwire.HostLinkAttachModeRestore
	request.TraceID = "trace-xyz"
	request.ActorID = "reconciler-7"
	request.IdempotencyKey = "attempt-9"
	if _, err := f.mux.Attach(t.Context(), request); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	want := hostlink.AttachRequest{
		Key:                    registry.Key{TenantID: testTenant, SessionID: otherSession},
		AgentID:                "agent-alpha",
		RuntimeCompatibilityID: testRuntime,
		Mode:                   sessionwire.HostLinkAttachModeRestore,
		ActorID:                "reconciler-7",
		TraceID:                "trace-xyz",
		IdempotencyKey:         "attempt-9",
	}
	got := attacher.recorded()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("the attacher received %+v, want %+v", got, want)
	}
}

// TestTheAttacherSeamHasExactlyOneMethod holds the seam's shape the way
// TestCollaboratorInterfacesExposeOnlyReads holds the read-only collaborators':
// derived from the type, not from a list. A second method on Attacher would be
// a second thing a HostLink RPC can do to residency, and must be reviewed as
// one.
func TestTheAttacherSeamHasExactlyOneMethod(t *testing.T) {
	t.Parallel()
	seam := reflect.TypeOf((*hostlink.Attacher)(nil)).Elem()
	if got := seam.NumMethod(); got != 1 {
		t.Fatalf("Attacher has %d methods, want exactly one", got)
	}
	method := seam.Method(0)
	if method.Name != "Attach" {
		t.Fatalf("Attacher's method = %q, want Attach", method.Name)
	}
	if method.Type.NumIn() != 2 || method.Type.In(1) != reflect.TypeOf(hostlink.AttachRequest{}) {
		t.Fatalf("Attach takes %v, want (context.Context, hostlink.AttachRequest)", method.Type)
	}
	if method.Type.NumOut() != 2 || method.Type.Out(0) != reflect.TypeOf(sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("Attach returns %v, want (sessionwire.HostLinkRegistryObservation, error)", method.Type)
	}
}

// ---------------------------------------------------------------------------
// Over the wire
// ---------------------------------------------------------------------------

// TestAttachRPCSpeaksCoreRecordsOverTheLink drives the attach RPC through the
// real transport and pins the three reply shapes a Factory can receive: an
// accepted attach is a HostLinkRegistryObservation body, a refused one is a
// HostLinkError body carrying the holder's epoch, and a failure with no class
// is a TRANSPORT error whose code is Centrifuge's internal (100) — not the
// bad-request 107 a malformed body earns, because the two are different faults
// with different repairs.
func TestAttachRPCSpeaksCoreRecordsOverTheLink(t *testing.T) {
	attacher := &recordingAttacher{answer: acceptedObservation(testSession)}
	f := newFixture(t, withAttacher(attacher))
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}

	// ACCEPTED: the body is the observation, decoded by Core's strict decoder.
	reply := rpc(t, connection, 2, hostlink.MethodAttach, attachRequest(testSession))
	if reply.Error != nil {
		t.Fatalf("attach failed at the transport: %#v", *reply.Error)
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 {
		t.Fatalf("accepted attach returned no body: %#v", reply)
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(reply.RPC.Data, &observation); err != nil {
		t.Fatalf("accepted attach body %s is not a Core registry observation: %v", reply.RPC.Data, err)
	}
	if observation.LeaseEpoch != testEpoch || observation.HostID != testHostID || observation.HostGeneration != testGeneration {
		t.Fatalf("observation = %+v, want the attacher's tuple", observation)
	}
	// AND IT IS NOT READABLE AS A REFUSAL: the two strict decoders keep the
	// bodies apart, which is what lets a Factory branch on decode success.
	var asRefusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply.RPC.Data, &asRefusal); err == nil {
		t.Fatalf("the accepted body also decodes as a HostLinkError, so a Factory cannot tell the two apart")
	}

	// REFUSED: a HostLinkError body carrying the OTHER holder's epoch.
	attacher.refuseWith = &hostlink.AttachRefusal{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 42}
	refusal := refusedRPC(t, rpc(t, connection, 3, hostlink.MethodAttach, attachRequest(testSession)))
	if refusal.Code != sessionwire.HostLinkErrorEpochMismatch || refusal.CurrentLeaseEpoch != 42 {
		t.Fatalf("refusal = %#v, want epoch_mismatch naming the holder's epoch 42", refusal)
	}

	// FAILED WITH NO CLASS: a transport error, internal, not bad request.
	attacher.refuseWith = &hostlink.AttachRefusal{Reason: "the launch failed"}
	failed := rpc(t, connection, 4, hostlink.MethodAttach, attachRequest(testSession))
	if failed.Error == nil {
		t.Fatalf("a code-less failure was answered with a body %s, want a transport error", failed.RPC.Data)
	}
	if failed.Error.Code != 100 {
		t.Fatalf("code-less failure answered transport code %d, want 100 (internal)", failed.Error.Code)
	}

	// MALFORMED: bad request, so the two transport faults stay apart.
	malformed := rpc(t, connection, 5, hostlink.MethodAttach, map[string]any{"version": 1})
	if malformed.Error == nil || malformed.Error.Code != 107 {
		t.Fatalf("malformed attach answered %#v, want transport code 107 (bad request)", malformed.Error)
	}
}

// ---------------------------------------------------------------------------
// Order, and the connection loop
// ---------------------------------------------------------------------------

// TestTheAttachLadderDecidesTheTenantBeforeTheHost is the two-fault probe the
// single-fault matrix cannot make. A request that is wrong in two respects is
// answered by the EARLIER rung, and the order is the disclosure rule: a
// foreign tenant is refused before the Host identity is compared, so it cannot
// learn from the refusal which Host it reached; a malformed body is refused
// before anything is compared at all.
func TestTheAttachLadderDecidesTheTenantBeforeTheHost(t *testing.T) {
	t.Parallel()
	attacher := &recordingAttacher{answer: acceptedObservation(testSession)}
	f := newFixture(t, withAttacher(attacher))

	for _, test := range []struct {
		name    string
		perturb func(*sessionwire.HostLinkAttachRequest)
		want    hostlink.Refusal
	}{
		{
			name: "foreign tenant and foreign host",
			perturb: func(r *sessionwire.HostLinkAttachRequest) {
				r.TenantID = "tenant-other"
				r.HostID = "host-beta"
			},
			want: hostlink.RefusalForeignTenant,
		},
		{
			name: "foreign tenant and stale generation",
			perturb: func(r *sessionwire.HostLinkAttachRequest) {
				r.TenantID = "tenant-other"
				r.HostGeneration = testGeneration + 1
			},
			want: hostlink.RefusalForeignTenant,
		},
		{
			name: "foreign host and stale generation",
			perturb: func(r *sessionwire.HostLinkAttachRequest) {
				r.HostID = "host-beta"
				r.HostGeneration = testGeneration + 1
			},
			want: hostlink.RefusalForeignHost,
		},
		{
			name: "malformed and foreign tenant",
			perturb: func(r *sessionwire.HostLinkAttachRequest) {
				r.ActorID = ""
				r.TenantID = "tenant-other"
			},
			want: hostlink.RefusalMalformedRequest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := attachRequest(testSession)
			test.perturb(&request)
			refusal := refusedAttach(t, mustFailAttach(t, f.mux, request))
			if refusal.Refusal != test.want {
				t.Fatalf("refusal = %q, want %q (the earlier rung)", refusal.Refusal, test.want)
			}
		})
	}
	if got := attacher.recorded(); len(got) != 0 {
		t.Fatalf("two-fault requests reached the attacher %d times, want 0", len(got))
	}
}

// parkingAttacher blocks inside Attach until released, so a test can hold an
// attach in flight while it drives other RPCs on the same link.
type parkingAttacher struct {
	release chan struct{}
	entered chan struct{}
	answer  sessionwire.HostLinkRegistryObservation
}

func (a *parkingAttacher) Attach(ctx context.Context, _ hostlink.AttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	close(a.entered)
	select {
	case <-a.release:
		return a.answer, nil
	case <-ctx.Done():
		return sessionwire.HostLinkRegistryObservation{}, ctx.Err()
	}
}

// TestAnAttachInFlightDoesNotStallTheLink holds the reason attach is answered
// off the connection's command loop: an attach launches a runtime, and a bind
// issued behind it on the same link must be answered while the attach is still
// parked in the Attacher. The attach's own reply then arrives once the
// Attacher returns, on the same connection, with the id it was sent under.
func TestAnAttachInFlightDoesNotStallTheLink(t *testing.T) {
	attacher := &parkingAttacher{
		release: make(chan struct{}),
		entered: make(chan struct{}),
		answer:  acceptedObservation(testSession),
	}
	f := newFixture(t, withAttacher(attacher))
	auth := &recordingAuthenticator{wantToken: testCredential}
	server, httpServer := startServer(t, auth, hostlink.Config{Multiplexer: f.mux})
	defer closeServers(t, server, httpServer)

	connection := dial(t, httpServer.URL, "")
	defer connection.Close()
	if reply := connect(t, connection, testCredential, sessionwire.VersionNegotiationRequest{SupportedVersions: []sessionwire.WireVersion{1}}); reply.Connect == nil {
		t.Fatalf("connect reply = %#v", reply)
	}

	// Write the attach WITHOUT reading its reply, and wait until the Attacher
	// has it — so the bind below is provably issued while it is in flight.
	writeRPC(t, connection, 2, hostlink.MethodAttach, attachRequest(testSession))
	select {
	case <-attacher.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the attacher was never entered")
	}

	// The next frame on the wire must be THE BIND'S reply, not the attach's.
	bound := rpc(t, connection, 3, hostlink.MethodBind, bindRequest(otherSession))
	acceptedRPC(t, bound)

	// Release the attach; its reply arrives under its own id.
	close(attacher.release)
	connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, frame, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read the attach reply: %v", err)
	}
	var reply rpcReply
	if err := json.Unmarshal(frame, &reply); err != nil {
		t.Fatalf("decode %s: %v", frame, err)
	}
	if reply.ID != 2 || reply.Error != nil || reply.RPC == nil {
		t.Fatalf("the released attach answered %#v, want an accepted reply under id 2", reply)
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(reply.RPC.Data, &observation); err != nil {
		t.Fatalf("the attach reply %s is not a registry observation: %v", reply.RPC.Data, err)
	}
}

// writeRPC writes one RPC command and does NOT read a reply.
func writeRPC(t *testing.T, connection *websocket.Conn, id uint32, method string, body any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal rpc body: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"id":  id,
		"rpc": map[string]any{"method": method, "data": json.RawMessage(data)},
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write command: %v", err)
	}
}
