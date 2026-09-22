package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host"
	"github.com/looprig/host/internal/harnesstest"
)

// ---------------------------------------------------------------------------
// Warm release through the EXPORTED composition, over the real harness runtime
// ---------------------------------------------------------------------------
//
// host v0.6.0's Compose wired no work-state source, so the warm releaser it
// builds was never told a session was idle and never armed a countdown: an
// idle pooled session stayed resident until the Host stopped. These tests
// compose real Hosts with host.Compose, launch through harnessadapter onto a
// real rig over a real harness journal, and observe the release from OUTSIDE
// the Host — through the durable residency lease another party can or cannot
// take — rather than through any internal state.

// warmWorld is realRuntimeWorld with a short warm TTL and poll.
const (
	warmTTL  = 200 * time.Millisecond
	warmPoll = 20 * time.Millisecond
)

// warmHost composes and starts one Host with a short warm window.
func (w *realRuntimeWorld) warmHost(t *testing.T, generation uint64) (*host.Service, *capturingLauncher) {
	t.Helper()
	return w.hostWith(t, generation, func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
	})
}

// residencyHeld reports whether some Host still holds the session's residency
// lease, by trying to take it as another party would. A successful take is
// released at once.
func (w *realRuntimeWorld) residencyHeld(t *testing.T) bool {
	t.Helper()
	other := w.fixture.otherParty(t)
	lease, err := other.AcquireResidency(t.Context(), sessionstore.AcquireResidencyRequest{TenantID: composeTenant, SessionID: composeSession})
	if err != nil {
		return true
	}
	if err := lease.Release(t.Context()); err != nil {
		t.Fatalf("release the probe lease: %v", err)
	}
	return false
}

// awaitReleased waits up to bound for the residency to become free.
func (w *realRuntimeWorld) awaitReleased(t *testing.T, bound time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if !w.residencyHeld(t) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// TestAnIdlePooledSessionIsWarmReleasedByAComposedHost is the measurement. A
// session is made resident on a composed Host, runs one turn, and goes idle.
// After its warm TTL (with generous slack) the residency must be free: the Host
// released it without stopping, and the journal records no SessionStopped.
func TestAnIdlePooledSessionIsWarmReleasedByAComposedHost(t *testing.T) {
	world := newRealRuntimeWorld(t)
	service, launcher := world.warmHost(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	world.turn(t, launcher.controller(), rememberedWord)

	if !world.awaitReleased(t, 20*warmTTL+5*time.Second) {
		t.Fatalf("an idle pooled session was still resident %v after going idle with a %v warm TTL: the composed Host never warm-released it", 20*warmTTL+5*time.Second, warmTTL)
	}
	if got := countOf[event.SessionStopped](t, world.journal, world.runtimeID); got != 0 {
		t.Fatalf("the warm release appended %d SessionStopped; a warm release is nonterminal", got)
	}
}

// TestAWarmReleasedSessionIsRestoredByTheNextHost: after the warm release a
// second Host is asked to attach the session as create, as Factory does on the
// next command, and restores the same conversation rather than restarting it.
func TestAWarmReleasedSessionIsRestoredByTheNextHost(t *testing.T) {
	world := newRealRuntimeWorld(t)
	first, firstLauncher := world.warmHost(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	if _, err := attachAsFactoryDoes(t, first); err != nil {
		t.Fatalf("Host A attach: %v", err)
	}
	world.turn(t, firstLauncher.controller(), rememberedWord)
	if !world.awaitReleased(t, 20*warmTTL+5*time.Second) {
		t.Fatal("Host A never warm-released the idle session")
	}

	second, secondLauncher := world.host(t, 5, nil)
	t.Cleanup(func() { stopBounded(second) })
	if _, err := attachAsFactoryDoes(t, second); err != nil {
		var refused *host.AttachError
		if errors.As(err, &refused) {
			code, _ := refused.HostLinkCode()
			t.Fatalf("Host B attach after the warm release refused %q: %v", code, err)
		}
		t.Fatalf("Host B attach after the warm release: %v", err)
	}
	creates, restores := secondLauncher.counts()
	if creates != 0 || len(restores) != 1 || restores[0] != world.runtimeID {
		t.Fatalf("Host B: creates = %d, restores = %v; a warm-released session must be RESTORED", creates, restores)
	}
	if got := harnesstest.CountSessionStarted(t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("%d SessionStarted after the restore, want 1", got)
	}

	// THE CONVERSATION CARRIED OVER: the next turn on Host B is run over Host
	// A's, so the model is sent the word it was told before the release.
	before := len(world.llm.Requests())
	world.turn(t, secondLauncher.controller(), "what was the word?")
	requests := world.llm.Requests()
	if len(requests) <= before {
		t.Fatal("the turn on Host B sent the model nothing")
	}
	sent, err := json.Marshal(requests[len(requests)-1].Messages)
	if err != nil {
		t.Fatalf("marshal the request: %v", err)
	}
	if !strings.Contains(string(sent), "PERSIMMON") {
		t.Fatalf("the restored session's next turn does not carry the pre-release conversation; the model was sent %s", sent)
	}
}

// ---------------------------------------------------------------------------
// What must NOT be released
// ---------------------------------------------------------------------------

// quietWindow is how long a test watches a session it expects to stay
// resident: ten warm TTLs, so a release that was going to happen has had ten
// chances to.
const quietWindow = 10 * warmTTL

// residentSessions reads host_sessions{state="resident"} off the Host's own
// metrics endpoint, which is what an operator (or an HPA) would read.
func residentSessions(t *testing.T, service *host.Service) int {
	t.Helper()
	return sessionsIn(t, service, "resident")
}

// sessionsIn reads host_sessions for one local residency state.
func sessionsIn(t *testing.T, service *host.Service, state string) int {
	t.Helper()
	recorder := httptest.NewRecorder()
	service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if value, found := strings.CutPrefix(line, `host_sessions{state="`+state+`"} `); found {
			count, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				t.Fatalf("host_sessions{state=%q} = %q: %v", state, value, err)
			}
			return count
		}
	}
	t.Fatalf("the metrics carry no host_sessions{state=%q}:\n%s", state, recorder.Body.String())
	return 0
}

// awaitResidentSessions waits up to bound for the Host to hold want sessions.
func awaitResidentSessions(t *testing.T, service *host.Service, want int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for residentSessions(t, service) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the Host holds %d resident sessions after %v, want %d", residentSessions(t, service), bound, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stayResident fails if the Host lets go of its one session within the quiet
// window.
func stayResident(t *testing.T, service *host.Service, why string) {
	t.Helper()
	deadline := time.Now().Add(quietWindow)
	for time.Now().Before(deadline) {
		if got := residentSessions(t, service); got != 1 {
			t.Fatalf("%s: the Host holds %d resident sessions %v into a %v warm TTL, want it to keep the one", why, got, quietWindow, warmTTL)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// blockingLLM holds every model call until it is opened, so a turn stays in
// flight for as long as a test needs it to.
type blockingLLM struct {
	inner   *harnesstest.RecordingLLM
	calls   atomic.Int32
	opened  chan struct{}
	release sync.Once
}

func (l *blockingLLM) Invoke(ctx context.Context, request inference.Request) (*inference.Response, error) {
	return l.inner.Invoke(ctx, request)
}

func (l *blockingLLM) Stream(ctx context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	l.calls.Add(1)
	select {
	case <-l.opened:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return l.inner.Stream(ctx, request)
}

func (l *blockingLLM) unblock() { l.release.Do(func() { close(l.opened) }) }

// TestABusySessionIsNotWarmReleased: a turn is in flight — the model call
// has not returned — for ten warm TTLs, and the session stays resident. Once
// the turn finishes the session is released: the control that shows the Host
// was able to release it all along.
func TestABusySessionIsNotWarmReleased(t *testing.T) {
	world := newRealRuntimeWorld(t)
	model := &blockingLLM{inner: world.llm, opened: make(chan struct{})}
	t.Cleanup(model.unblock)
	world.model = model
	service, launcher := world.warmHost(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := launcher.controller().Submit(t.Context(), []content.Block{&content.TextBlock{Text: "take your time"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for model.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the turn never reached the model")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stayResident(t, service, "a turn in flight")
	if !world.residencyHeld(t) {
		t.Fatal("the residency lease was released while a turn was in flight")
	}

	model.unblock()
	if !world.awaitReleased(t, 20*warmTTL+5*time.Second) {
		t.Fatal("after the turn finished the idle session was never warm-released")
	}
	if got := countOf[event.TurnDone](t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("%d TurnDone before the release, want the one turn finished", got)
	}
}

// warmGateWorld is a gate world whose Hosts carry the short warm window.
func warmGateWorld(t *testing.T, options gateE2EOptions) *gateE2EWorld {
	t.Helper()
	world := newGateE2EWorld(t, options)
	world.adjust = func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
	}
	return world
}

// TestASessionWaitingAtAGateIsNotWarmReleased: an agent asks the user a
// question and a human is waiting to answer it. The session stays resident for
// ten warm TTLs; when the answer settles and the turn finishes, it is released.
func TestASessionWaitingAtAGateIsNotWarmReleased(t *testing.T) {
	world := warmGateWorld(t, gateE2EOptions{})
	service, launcher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })

	world.submit(t, launcher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]
	stayResident(t, service, "an open ask_user gate")

	id := world.answer(t, opened, "answer", answerValue())
	if entry := world.settled(t, id); outcomeOf(t, entry) != "applied" {
		t.Fatalf("the answer settled %q, want applied: the session held its gate and must still apply the answer", outcomeOf(t, entry))
	}
	gateE2EEventually(t, "the agent to continue with the answer", func() bool { return world.llm.sawToolResult(gateE2EAnswer) })
	awaitResidentSessions(t, service, 0, 20*warmTTL+5*time.Second)
}

// TestARestoredSessionWithAnOpenGateIsNotWarmReleased is the case the runtime's
// own idle cannot see. Host A's agent raises a permission gate and A crashes;
// Host B restores the session, and harness restores the gate OPEN under an
// interrupted turn — so B's runtime is idle while a human still has a question
// to answer. B keeps the session; once the approval settles, B releases it.
func TestARestoredSessionWithAnOpenGateIsNotWarmReleased(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	world.submit(t, firstLauncher.controller(), "PLEASE-RUN-GATED")
	opened := world.gates(t, 1)[0]

	world.adjust = func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
	}
	second, secondLauncher, _ := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	if _, restores := secondLauncher.counts(); len(restores) != 1 {
		t.Fatalf("Host B restored %v, want the one session", restores)
	}
	idle, ok := secondLauncher.controller().(session.IdleWaiter)
	if !ok {
		t.Fatal("the restored harness session is not a session.IdleWaiter")
	}
	idleCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := idle.WaitIdle(idleCtx); err != nil {
		t.Fatalf("the restored runtime never went idle: %v", err)
	}
	stayResident(t, second, "a restored open permission gate under an idle runtime")

	id := world.answer(t, opened, string(gate.ApprovalApprove), map[string]json.RawMessage{})
	if entry := world.settled(t, id); outcomeOf(t, entry) != "applied" {
		t.Fatalf("the approval settled %q, want applied", outcomeOf(t, entry))
	}
	world.gates(t, 0)
	awaitResidentSessions(t, second, 0, 20*warmTTL+5*time.Second)
}

// TestAPendingCommandAbortsTheWarmReleaseAndIsApplied: a command is durably
// admitted while the session is idle and the Host has not consumed it yet —
// there is no HostLink hint and the reconcile interval is a minute. When the
// warm countdown expires the release re-reads the inbox, finds the command,
// aborts and wakes the consumer; the command is applied, and only after the
// turn it started is the session released.
func TestAPendingCommandAbortsTheWarmReleaseAndIsApplied(t *testing.T) {
	const ttl = time.Second
	world := newGateE2EWorld(t, gateE2EOptions{})
	world.decoder = func(body []byte) ([]content.Block, error) {
		var request sessionwire.InputRequest
		if err := json.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		return content.UnmarshalBlocks(request.Blocks)
	}
	world.adjust = func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = ttl
		blueprint.WorkPoll = warmPoll
		blueprint.Options.ReconcileInterval = time.Minute
	}
	service, _, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	// Let the consumer's first pass finish over an empty inbox.
	time.Sleep(100 * time.Millisecond)

	id := sessionwire.CommandID("pending-input")
	request := sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id},
		SessionID:       composeSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":"a command nobody hinted"}]`),
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	runtimeCommand, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := world.factory.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: composeTenant, SessionID: composeSession, CommandID: id, Binding: world.binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeCommand.String()),
		Kind:                     "input", Payload: payload,
		AcceptedAt: now, ApplyDeadline: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("admit the input: %v", err)
	}

	awaitResidentSessions(t, service, 0, 20*ttl+5*time.Second)
	entry := world.command(t, id)
	if entry.Record.State != sessionstore.InboxStateApplied {
		t.Fatalf("the session was released with its admitted command %q; the release must abort on durable work and apply it first", entry.Record.State)
	}
	if got := countOf[event.TurnDone](t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("%d TurnDone, want the pending input's turn", got)
	}
}

// ---------------------------------------------------------------------------
// The release window: after the inbox re-read, before the runtime release
// ---------------------------------------------------------------------------

// pausingCheckpointer holds the FIRST checkpoint — a warm release's step 3 —
// until the test lets it go, so a test can act inside the release window.
type pausingCheckpointer struct {
	once    sync.Once
	entered chan struct{}
	proceed chan struct{}
}

func newPausingCheckpointer() *pausingCheckpointer {
	return &pausingCheckpointer{entered: make(chan struct{}), proceed: make(chan struct{})}
}

func (c *pausingCheckpointer) Checkpoint(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID) error {
	first := false
	c.once.Do(func() { first = true })
	if !first {
		return nil
	}
	close(c.entered)
	select {
	case <-c.proceed:
	case <-ctx.Done():
	}
	return nil
}

// awaitEntered waits for the release to reach its checkpoint.
func (c *pausingCheckpointer) awaitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the warm release never reached its checkpoint")
	}
}

// inputDecoder is the input-shaped block decoder a composition binds.
func inputDecoder(body []byte) ([]content.Block, error) {
	var request sessionwire.InputRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	return content.UnmarshalBlocks(request.Blocks)
}

// admitInput admits an input exactly as Factory does and returns its id.
func (w *gateE2EWorld) admitInput(t *testing.T, id sessionwire.CommandID, text string) {
	t.Helper()
	request := sessionwire.InputRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id},
		SessionID:       composeSession,
		Blocks:          json.RawMessage(`[{"type":"text","text":` + strconv.Quote(text) + `}]`),
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	runtimeCommand, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := w.factory.AdmitDispositionCommand(t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: composeTenant, SessionID: composeSession, CommandID: id, Binding: w.binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeCommand.String()),
		Kind:                     "input", Payload: payload,
		AcceptedAt: now, ApplyDeadline: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("admit the input: %v", err)
	}
}

// TestACommandAdmittedDuringTheReleaseIsLeftForTheSuccessor is F2. The warm
// release has re-read an empty inbox and is paused at its checkpoint; an input
// is admitted durably, and the Host's consumer — reconciling every 50ms — would
// claim it at once if it were running. It must not be claimed or applied under
// the releasing lease. It stays pending; the release completes; the next Host
// restores the session and applies it.
func TestACommandAdmittedDuringTheReleaseIsLeftForTheSuccessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	world.decoder = inputDecoder
	checkpointer := newPausingCheckpointer()
	world.adjust = func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
		blueprint.Collaborators.Checkpointer = checkpointer
	}
	first, _, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	checkpointer.awaitEntered(t)

	const id = sessionwire.CommandID("admitted-mid-release")
	world.admitInput(t, id, "arrived while the Host was releasing")
	time.Sleep(10 * 50 * time.Millisecond) // ten reconcile intervals
	if state := world.command(t, id).Record.State; state != sessionstore.InboxStatePending {
		t.Fatalf("a command admitted after the release's inbox re-read went %q on the releasing Host; it must stay pending for the successor", state)
	}
	close(checkpointer.proceed)
	awaitResidentSessions(t, first, 0, 20*time.Second)
	if state := world.command(t, id).Record.State; state != sessionstore.InboxStatePending {
		t.Fatalf("after the release the command is %q, want pending", state)
	}

	world.adjust = nil
	second, secondLauncher, _ := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	if _, restores := secondLauncher.counts(); len(restores) != 1 {
		t.Fatalf("the successor restored %v, want the one session", restores)
	}
	if entry := world.settled(t, id); entry.Record.State != sessionstore.InboxStateApplied {
		t.Fatalf("the successor settled the command %q, want applied", entry.Record.State)
	}
}

// TestARuntimeThatTurnsBusyInsideTheReleaseLeavesTheSessionHeld is F3 over the
// real harness runtime. A turn starts inside the release window (while the
// release is paused at its checkpoint), so at step 4 the runtime is busy and
// harness would wait for it. The runtime release is bounded by the drain's
// grace; when it expires the session is HELD — still resident under its grant,
// its live output still relayed — rather than torn down under a running turn
// or wedging the releaser. The turn finishes, and the Host then stops cleanly.
func TestARuntimeThatTurnsBusyInsideTheReleaseLeavesTheSessionHeld(t *testing.T) {
	world := newRealRuntimeWorld(t)
	model := &blockingLLM{inner: world.llm, opened: make(chan struct{})}
	t.Cleanup(model.unblock)
	world.model = model
	checkpointer := newPausingCheckpointer()
	service, launcher := world.hostWith(t, 4, func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
		blueprint.Drain = host.DrainOptions{Grace: time.Second, IdleBoundary: 500 * time.Millisecond, PublishBound: 500 * time.Millisecond}
		blueprint.Collaborators.Checkpointer = checkpointer
	})
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	checkpointer.awaitEntered(t)

	if _, err := launcher.controller().Submit(t.Context(), []content.Block{&content.TextBlock{Text: "started inside the release"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for model.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the turn never reached the model")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(checkpointer.proceed)

	// Well past the grace: the release stopped at step 4, and the grant is
	// still held by this Host.
	time.Sleep(3 * time.Second)
	if !world.residencyHeld(t) {
		t.Fatal("the grant was handed back while the runtime was running a turn")
	}
	if got := sessionsIn(t, service, "releasing"); got != 1 {
		t.Fatalf("host_sessions{state=\"releasing\"} = %d, want the one held session", got)
	}
	if got := releaseFailures(t, service); got != 1 {
		t.Fatalf("host_release_failures_total = %d, want 1: the release must have STOPPED at the bounded runtime release, not still be waiting on it", got)
	}
	if got := countOf[event.SessionStopped](t, world.journal, world.runtimeID); got != 0 {
		t.Fatalf("%d SessionStopped; a held session is not terminated", got)
	}

	model.unblock()
	gateDeadline := time.Now().Add(10 * time.Second)
	for countOf[event.TurnDone](t, world.journal, world.runtimeID) == 0 {
		if time.Now().After(gateDeadline) {
			t.Fatal("the held session's turn never finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopWithin(t, service)
	if world.residencyHeld(t) {
		t.Fatal("after the drain the held session's grant is still held")
	}
	// THE DRAIN RELEASED THE RUNTIME: a held session is still offered to it.
	live, ok := launcher.controller().(session.Liveness)
	if !ok {
		t.Fatal("the harness session is not a session.Liveness")
	}
	select {
	case <-live.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("after the drain the held session's runtime was never released")
	}
}

// ---------------------------------------------------------------------------
// A backgrounded delegate in flight
// ---------------------------------------------------------------------------

// delegatingLLM makes the primer start a BACKGROUND child and finish its own
// turn at once, and holds the child's model call until it is unblocked.
type delegatingLLM struct {
	inner      *harnesstest.RecordingLLM
	childCalls atomic.Int32
	opened     chan struct{}
	release    sync.Once
}

func (l *delegatingLLM) Invoke(ctx context.Context, request inference.Request) (*inference.Response, error) {
	return l.inner.Invoke(ctx, request)
}

func (l *delegatingLLM) Stream(ctx context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	var chunk content.Chunk = &content.TextChunk{Text: "ok"}
	if len(request.Messages) > 0 {
		last := request.Messages[len(request.Messages)-1]
		if _, isResult := last.(*content.ToolResultMessage); !isResult {
			text, _ := json.Marshal(last)
			switch {
			case strings.Contains(string(text), "CHILD-WORK"):
				l.childCalls.Add(1)
				select {
				case <-l.opened:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				chunk = &content.TextChunk{Text: "child done"}
			case strings.Contains(string(text), "PLEASE-DELEGATE"):
				chunk = &content.ToolUseChunk{Index: 0, ID: "use-start-agent", Name: "StartAgent",
					InputJSON: `{"agent_type":"worker","instructions":"CHILD-WORK","wait_for_response":false}`}
			}
		}
	}
	sent := false
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if sent {
			return nil, io.EOF
		}
		sent = true
		return chunk, nil
	}, nil), nil
}

func (l *delegatingLLM) unblock() { l.release.Do(func() { close(l.opened) }) }

// delegatingRig is a primer that may delegate, in the background, to a worker.
func delegatingRig(t testing.TB, store *harnessstore.Store, llm inference.Client) *rig.Rig {
	t.Helper()
	testModel := model.Model{Provider: "test", APIFormat: model.APIFormatOpenAI, BaseURL: "http://localhost", Name: "model"}
	evaluator, err := gate.NewInteractiveEvaluator(
		[]gate.AccessBinding{{Kind: "tool.invoke", Source: gateE2EAccess{}}}, nil, loop.GateApprover(), gateE2ERules{}, nil)
	if err != nil {
		t.Fatalf("NewInteractiveEvaluator: %v", err)
	}
	primer, err := loop.Define(loop.WithName("agent"), loop.WithInference(llm, testModel),
		loop.WithDelegates("worker"), loop.WithDelegation(loop.Delegation{Style: loop.DelegationManaged}),
		loop.WithAccessGate(evaluator), loop.WithPolicyRevision("warm-delegate"))
	if err != nil {
		t.Fatalf("define the primer: %v", err)
	}
	worker, err := loop.Define(loop.WithName("worker"), loop.WithInference(llm, testModel))
	if err != nil {
		t.Fatalf("define the worker: %v", err)
	}
	defined, err := rig.Define(rig.WithLoops(primer, worker), rig.WithPrimers("agent"), rig.WithSessionStore(store))
	if err != nil {
		t.Fatalf("define the rig: %v", err)
	}
	return defined
}

// TestASessionWithABackgroundDelegateInFlightIsNotWarmReleased is the case
// the Host cannot see directly: the primer's own turn has ENDED, but a child
// it started in the background is still running and will hand its result
// back. harness holds a quiescence token for that hand-back, so the session
// is not idle; the Host must keep it. When the child finishes and its result
// is handed back, the session goes idle and is released.
func TestASessionWithABackgroundDelegateInFlightIsNotWarmReleased(t *testing.T) {
	world := newRealRuntimeWorld(t)
	model := &delegatingLLM{inner: world.llm, opened: make(chan struct{})}
	t.Cleanup(model.unblock)
	world.model = model
	world.define = delegatingRig
	service, launcher := world.warmHost(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	if _, err := attachAsFactoryDoes(t, service); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := launcher.controller().Submit(t.Context(), []content.Block{&content.TextBlock{Text: "PLEASE-DELEGATE"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for model.childCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the background child never reached the model")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The primer's own turn has ENDED: the session is quiet but for the child.
	for countOf[event.TurnDone](t, world.journal, world.runtimeID) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the primer's turn never finished while its child ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stayResident(t, service, "a background delegate in flight")

	model.unblock()
	awaitResidentSessions(t, service, 0, 20*warmTTL+10*time.Second)
}

// releaseFailures reads host_release_failures_total off the Host's metrics.
func releaseFailures(t *testing.T, service *host.Service) int {
	t.Helper()
	recorder := httptest.NewRecorder()
	service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if value, found := strings.CutPrefix(line, "host_release_failures_total "); found {
			count, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				t.Fatalf("host_release_failures_total = %q: %v", value, err)
			}
			return count
		}
	}
	t.Fatalf("the metrics carry no host_release_failures_total:\n%s", recorder.Body.String())
	return 0
}

// TestAHeldSessionStillPublishesAndAppliesTheAnswerToAGateItRaised is F3's
// recoverability, over the real harness runtime. Inside the release window the
// agent asks the user a question, so at step 4 the runtime is parked at a gate
// and cannot release. After the bounded wait the session is HELD, and it is
// still a working session: the gate is projected for Factory, the answer is
// claimed and applied under this Host's live grant, and the agent continues.
// Tearing its work down before the wait (or not bounding the wait) would leave
// the question unanswerable.
func TestAHeldSessionStillPublishesAndAppliesTheAnswerToAGateItRaised(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	checkpointer := newPausingCheckpointer()
	world.adjust = func(blueprint *host.Composition) {
		blueprint.Options.WarmTTL = warmTTL
		blueprint.WorkPoll = warmPoll
		blueprint.Drain = host.DrainOptions{Grace: time.Second, IdleBoundary: 500 * time.Millisecond, PublishBound: 500 * time.Millisecond}
		blueprint.Collaborators.Checkpointer = checkpointer
	}
	service, launcher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	checkpointer.awaitEntered(t)

	world.submit(t, launcher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]
	close(checkpointer.proceed)

	deadline := time.Now().Add(10 * time.Second)
	for releaseFailures(t, service) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the warm release never stopped at the runtime release; it is still waiting on a runtime parked at a gate")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := sessionsIn(t, service, "releasing"); got != 1 {
		t.Fatalf("host_sessions{state=\"releasing\"} = %d, want the one held session", got)
	}

	id := world.answer(t, opened, "answer", answerValue())
	if entry := world.settled(t, id); outcomeOf(t, entry) != "applied" {
		t.Fatalf("the held session settled the answer %q, want applied", outcomeOf(t, entry))
	}
	gateE2EEventually(t, "the agent to continue with the answer", func() bool { return world.llm.sawToolResult(gateE2EAnswer) })
}
