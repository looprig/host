package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnessadapter"
	"github.com/looprig/host/internal/harnesstest"
)

// ---------------------------------------------------------------------------
// Gap 4 end to end: every gate an agent raises reaches the durable projection,
// and the user's answer settles through the disposition path.
// ---------------------------------------------------------------------------
//
// Everything below is real and released: the composed Host (host.Compose), the
// orchestration store (sessionstore v0.12.0) over one backend, the runtime's
// journal store (harness v0.36.0) over another, and a real rig whose loop raises
// real gates — an ask_user gate from a tool that asks, and a permission gate
// from a tool the access gate holds for approval. The test plays Factory: it
// reads the projection with ReadGates and admits the answer with
// AdmitDispositionCommand, exactly the records Factory writes.

// gateE2EAnswer is what the user answers every ask_user gate with.
const gateE2EAnswer = "ULTRAMARINE"

// gateE2ELLM asks, requests a gated tool, or answers, by what the turn says, and
// records every request so a test can read what the model was sent.
type gateE2ELLM struct {
	mu       sync.Mutex
	uses     int
	requests []inference.Request
}

func (*gateE2ELLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("gates e2e: Invoke is unused")
}

func (l *gateE2ELLM) Stream(_ context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	l.mu.Lock()
	l.requests = append(l.requests, request)
	l.uses++
	use := l.uses
	l.mu.Unlock()

	chunk := content.Chunk(&content.TextChunk{Text: "ok"})
	if len(request.Messages) > 0 {
		last := request.Messages[len(request.Messages)-1]
		switch last.(type) {
		case *content.ToolResultMessage:
			chunk = &content.TextChunk{Text: "done"}
		default:
			text, _ := json.Marshal(last)
			switch {
			case strings.Contains(string(text), "PLEASE-ASK"):
				chunk = &content.ToolUseChunk{Index: 0, ID: "use-ask-" + string(rune('a'+use%26)), Name: "Ask", InputJSON: `{}`}
			case strings.Contains(string(text), "PLEASE-RUN-GATED"):
				chunk = &content.ToolUseChunk{Index: 0, ID: "use-gated-" + string(rune('a'+use%26)), Name: "Gated", InputJSON: `{}`}
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

// sawToolResult reports whether any request sent the model a tool result
// containing text.
func (l *gateE2ELLM) sawToolResult(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, request := range l.requests {
		for _, message := range request.Messages {
			switch message.(type) {
			case *content.ToolResultMessage:
				encoded, _ := json.Marshal(message)
				if strings.Contains(string(encoded), text) {
					return true
				}
			}
		}
	}
	return false
}

// gateE2EAccess holds the Gated tool for approval and allows everything else.
type gateE2EAccess struct{}

func (gateE2EAccess) AccessVersion() uint16 { return gate.CurrentAccessVersion }
func (gateE2EAccess) AccessFor(_, scope string) (uint8, error) {
	if scope == "Gated" {
		return gate.AccessGated, nil
	}
	return gate.AccessAllow, nil
}

type gateE2ERules struct{}

func (gateE2ERules) WriteRules(context.Context, []tool.RuleCandidate) error { return nil }

// gateE2EAsk asks the user through a real ask_user gate and returns the answer.
type gateE2EAsk struct{}

func (gateE2EAsk) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{Name: "Ask", Schema: json.RawMessage(`{"type":"object"}`)}, nil
}

func (gateE2EAsk) PrepareCall(context.Context, uuid.UUID, string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{ToolName: "Ask", Summary: "ask the user", Requirements: []tool.Requirement{{
		Kind: "tool.invoke", Scope: "Ask", Match: "Ask", Description: "ask the user",
	}}}, nil, nil
}

func (gateE2EAsk) InvokableRun(ctx context.Context, _ string) (*tool.ToolResult, error) {
	answer, err := loop.RequestUserInput(ctx, "What is your favourite colour?", nil)
	if err != nil {
		return nil, err
	}
	return tool.TextResult("the user answered " + answer), nil
}

// gateE2EGated runs only once its permission gate is approved.
type gateE2EGated struct{ runs *atomic.Int32 }

func (gateE2EGated) Info(context.Context) (*tool.ToolInfo, error) {
	return &tool.ToolInfo{Name: "Gated", Schema: json.RawMessage(`{"type":"object"}`)}, nil
}

func (gateE2EGated) PrepareCall(context.Context, uuid.UUID, string) (tool.Request, tool.PreparedArtifact, error) {
	return tool.Request{ToolName: "Gated", Summary: "run the gated tool", Requirements: []tool.Requirement{{
		Kind: "tool.invoke", Scope: "Gated", Match: "Gated", Description: "run Gated",
	}}}, nil, nil
}

func (g gateE2EGated) InvokableRun(context.Context, string) (*tool.ToolResult, error) {
	g.runs.Add(1)
	return tool.TextResult("GATED-TOOL-RAN"), nil
}

// gateE2ERig defines a real rig whose one loop can raise both gate kinds.
func gateE2ERig(t *testing.T, store *harnessstore.Store, llm inference.Client, runs *atomic.Int32) *rig.Rig {
	t.Helper()
	evaluator, err := gate.NewInteractiveEvaluator(
		[]gate.AccessBinding{{Kind: "tool.invoke", Source: gateE2EAccess{}}}, nil, loop.GateApprover(), gateE2ERules{}, nil)
	if err != nil {
		t.Fatalf("NewInteractiveEvaluator: %v", err)
	}
	definition, err := loop.Define(
		loop.WithName("agent"),
		loop.WithInference(llm, model.Model{Provider: "test", APIFormat: model.APIFormatOpenAI, BaseURL: "http://localhost", Name: "model"}),
		loop.WithTools(
			tool.NewDefinition("Ask", 0, func(context.Context, tool.Bindings) ([]tool.InvokableTool, error) {
				return []tool.InvokableTool{gateE2EAsk{}}, nil
			}),
			tool.NewDefinition("Gated", 0, func(context.Context, tool.Bindings) ([]tool.InvokableTool, error) {
				return []tool.InvokableTool{gateE2EGated{runs: runs}}, nil
			}),
		),
		loop.WithAccessGate(evaluator),
		loop.WithPolicyRevision("gates-e2e"),
	)
	if err != nil {
		t.Fatalf("loop.Define: %v", err)
	}
	defined, err := rig.Define(rig.WithLoops(definition), rig.WithPrimers("agent"), rig.WithSessionStore(store))
	if err != nil {
		t.Fatalf("rig.Define: %v", err)
	}
	return defined
}

// gateE2EWorld is one orchestration store and one runtime journal that one or
// more Hosts, in turn, run a gating session over.
type gateE2EWorld struct {
	fixture   *composeFixture
	factory   *sessionstore.Store
	journal   *harnessstore.Store
	llm       *gateE2ELLM
	runs      *atomic.Int32
	runtimeID uuid.UUID
	binding   sessionstore.SessionBinding
	nextID    atomic.Int32

	// adjust, when set, changes every blueprint this world composes.
	adjust func(*host.Composition)

	// decoder, when set, is the block decoder input commands are read with.
	decoder harnessadapter.BlockDecoder
}

// gateE2ETakeover is a journal leaser under which a later Acquire takes a held
// lease over at a strictly greater epoch, which is what a successor sees once a
// crashed Host's lease has lapsed. memstore never lapses a lease, so without it
// a Host that stopped with its runtime parked at a gate — a runtime that cannot
// release its journal lease until its process exits — would hold the journal
// for the rest of the test.
type gateE2ETakeover struct {
	mu     sync.Mutex
	epochs map[string]uint64
}

func (l *gateE2ETakeover) Acquire(_ context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.epochs == nil {
		l.epochs = map[string]uint64{}
	}
	l.epochs[name]++
	return gateE2ELease{epoch: l.epochs[name], lost: make(chan struct{})}, nil
}

type gateE2ELease struct {
	epoch uint64
	lost  chan struct{}
}

func (l gateE2ELease) Epoch() uint64                 { return l.epoch }
func (l gateE2ELease) Lost() <-chan struct{}         { return l.lost }
func (l gateE2ELease) Release(context.Context) error { return nil }

// gateE2EOptions shape the runtime journal's backend.
type gateE2EOptions struct {
	// ledger, when set, wraps the journal's ledger (the fault seam).
	ledger func(storage.Ledger) storage.Ledger
	// takeover lets a successor take a crashed Host's leases over — the
	// residency lease and the runtime's journal lease — at a strictly greater
	// epoch, which is how a lease that lapsed with its process looks.
	takeover bool
}

// newGateE2EWorld seeds the session over a runtime journal shaped by options.
func newGateE2EWorld(t *testing.T, options gateE2EOptions) *gateE2EWorld {
	t.Helper()
	fixture := newComposeFixture(t)
	if options.takeover {
		// The ORCHESTRATION backend too: a crashed Host's residency lease
		// lapses as its journal lease does, and a successor takes both over.
		base := fixture.backend
		wrapped, err := storage.NewCompositeWithOrderedIndex(base.Ledger, &gateE2ETakeover{}, base.KV, base.Blobs, base.OrderedIndex)
		if err != nil {
			t.Fatal(err)
		}
		fixture.backend = wrapped
	}
	if options.ledger != nil || options.takeover {
		base := fixture.journalBackend
		ledger, leaser := base.Ledger, base.Leaser
		if options.ledger != nil {
			ledger = options.ledger(ledger)
		}
		if options.takeover {
			leaser = &gateE2ETakeover{}
		}
		wrapped, err := storage.NewCompositeWithOrderedIndex(ledger, leaser, base.KV, base.Blobs, base.OrderedIndex)
		if err != nil {
			t.Fatal(err)
		}
		fixture.journal = harnesstest.Store(t, wrapped, composeTenant)
	}
	world := &gateE2EWorld{
		fixture:   fixture,
		factory:   fixture.otherParty(t),
		journal:   fixture.journal,
		llm:       &gateE2ELLM{},
		runs:      &atomic.Int32{},
		runtimeID: uuid.MustParse("5b0e8c1d-2f3a-8b4c-9d5e-6f7a8b9c0d1e"),
	}
	world.binding = sessionstore.SessionBinding{
		StorageBindingID: composeBinding,
		BindingVersion:   "v1",
		RuntimeSessionID: world.runtimeID.String(),
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
	now := time.Now().UTC()
	if _, _, err := world.factory.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID: composeTenant, SessionID: composeSession, AgentID: composeAgent,
		RuntimeCompatibilityID: string(composeCompat), CreatedAt: now, LastActiveAt: now,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled, IdempotencyKey: "idem-gates",
		Binding: world.binding,
	}); err != nil {
		t.Fatalf("seed the catalog: %v", err)
	}
	return world
}

// host composes, starts and attaches one Host at a generation, and returns it
// with the launcher recording its runtime and the residency epoch it holds.
func (w *gateE2EWorld) host(t *testing.T, generation uint64) (*host.Service, *capturingLauncher, uint64) {
	t.Helper()
	service, launcher := w.compose(t, generation, nil)
	resident, err := attachAsFactoryDoes(t, service)
	if err != nil {
		t.Fatalf("attach at generation %d: %v", generation, err)
	}
	return service, launcher, resident.LeaseEpoch
}

// heldAttach is an attach running in the background while its restore is held.
type heldAttach struct {
	service  *host.Service
	launcher *capturingLauncher
	release  chan struct{}
	done     chan error
}

// hostHeld composes and starts a Host whose attach runs in the background and
// parks at its runtime restore until release is closed.
func (w *gateE2EWorld) hostHeld(t *testing.T, generation uint64) *heldAttach {
	t.Helper()
	release := make(chan struct{})
	service, launcher := w.compose(t, generation, release)
	held := &heldAttach{service: service, launcher: launcher, release: release, done: make(chan error, 1)}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 60*time.Second)
		defer cancel()
		_, err := service.Attach(ctx, host.AttachRequest{
			TenantID: composeTenant, SessionID: composeSession, AgentID: composeAgent,
			Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory",
		})
		held.done <- err
	}()
	return held
}

// finish releases the held restore and waits for the attach.
func (h *heldAttach) finish(t *testing.T) {
	t.Helper()
	close(h.release)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("the held attach: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the held attach did not finish")
	}
}

// compose composes and starts one Host; hold, when non-nil, parks its
// runtime restores until closed.
func (w *gateE2EWorld) compose(t *testing.T, generation uint64, hold chan struct{}) (*host.Service, *capturingLauncher) {
	t.Helper()
	launcher := &capturingLauncher{rig: gateE2ERig(t, w.journal, w.llm, w.runs), holdRestore: hold}
	var options []harnessadapter.Option
	if w.decoder != nil {
		options = append(options, harnessadapter.WithBlockDecoder(w.decoder))
	}
	adapter, err := harnessadapter.New(launcher, options...)
	if err != nil {
		t.Fatalf("harnessadapter.New: %v", err)
	}
	blueprint := w.fixture.blueprint(t)
	blueprint.Generation = generation
	// A command is found by the periodic reconcile, which is what a HostLink
	// hint only hurries; a short interval keeps the test fast.
	blueprint.Options.ReconcileInterval = 50 * time.Millisecond
	// An open gate keeps a session from idling, so a drain waits out the idle
	// boundary; a short one keeps a re-placement fast.
	blueprint.Drain.IdleBoundary = time.Second
	// A runtime parked at a gate cannot release nonterminally (harness waits
	// for whole-session idle), so its release is bounded by the grace.
	blueprint.Drain.Grace = 2 * time.Second
	blueprint.Drain.PublishBound = time.Second
	blueprint.Collaborators.Registrar = host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		target, err := department.NewRigTarget(adapter, composeCompat, department.Capabilities{
			SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming,
		})
		if err != nil {
			return nil, err
		}
		return []department.Registration{{AgentID: composeAgent, Target: target}}, nil
	})
	if w.adjust != nil {
		w.adjust(&blueprint)
	}
	service, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return service, launcher
}

// submit runs a user turn on the runtime.
func (w *gateE2EWorld) submit(t *testing.T, controller session.SessionController, text string) {
	t.Helper()
	if _, err := controller.Submit(t.Context(), []content.Block{&content.TextBlock{Text: text}}); err != nil {
		t.Fatalf("Submit(%q): %v", text, err)
	}
}

// gates waits until the projection holds exactly count open gates, and returns
// them as Factory reads them.
func (w *gateE2EWorld) gates(t *testing.T, count int) []sessionwire.GateProjection {
	t.Helper()
	var page sessionwire.GatePage
	gateE2EEventually(t, "the projection to hold the expected open gates", func() bool {
		var err error
		page, err = w.factory.ReadGates(t.Context(), sessionstore.ReadGatesRequest{TenantID: composeTenant, SessionID: composeSession})
		return err == nil && len(page.Gates) == count
	})
	return page.Gates
}

// mark is the projection's residency mark: the grant of the last gate writer.
func (w *gateE2EWorld) mark(t *testing.T) uint64 {
	t.Helper()
	entry, err := w.factory.GetCatalogEntry(t.Context(), sessionstore.GetCatalogEntryRequest{TenantID: composeTenant, SessionID: composeSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry.Record.LeaseEpoch
}

// answer admits a gate_response exactly as Factory does and returns its id.
func (w *gateE2EWorld) answer(t *testing.T, projection sessionwire.GateProjection, action string, values map[string]json.RawMessage) sessionwire.CommandID {
	t.Helper()
	id := sessionwire.CommandID("gate-answer-" + string(rune('a'+w.nextID.Add(1))))
	request := sessionwire.GateResponseRequest{
		CommandEnvelope:        sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id},
		SessionID:              composeSession,
		GateID:                 projection.GateID,
		Action:                 action,
		Values:                 values,
		ExpectedOpenJournalSeq: projection.OpenedJournalSeq,
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
		Kind:                     "gate_response", Payload: payload,
		AcceptedAt: now, ApplyDeadline: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("admit the gate response: %v", err)
	}
	return id
}

// command reads one command's durable record.
func (w *gateE2EWorld) command(t *testing.T, id sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	t.Helper()
	entry, err := w.factory.GetDispositionCommand(t.Context(), sessionstore.GetDispositionCommandRequest{TenantID: composeTenant, SessionID: composeSession, CommandID: id})
	if err != nil {
		t.Fatalf("GetDispositionCommand(%s): %v", id, err)
	}
	return entry
}

// settled waits for a command to become terminal and returns it.
func (w *gateE2EWorld) settled(t *testing.T, id sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	t.Helper()
	var entry sessionstore.DispositionInboxEntry
	deadline := time.Now().Add(15 * time.Second)
	for {
		entry = w.command(t, id)
		if entry.Record.State == sessionstore.InboxStateApplied || entry.Record.State == sessionstore.InboxStateRejected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("command %s did not settle within 15s: state %q, claim %+v, attempt %+v", id, entry.Record.State, entry.Record.Claim, entry.Record.Attempt)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return entry
}

// resolutions returns every GateResolved the journal holds for one gate.
func (w *gateE2EWorld) resolutions(t *testing.T, gateID sessionwire.GateID) []event.GateResolved {
	t.Helper()
	var found []event.GateResolved
	for _, ev := range harnesstest.Events(t, w.journal, w.runtimeID) {
		if resolved, ok := ev.(event.GateResolved); ok && resolved.GateID.String() == string(gateID) {
			found = append(found, resolved)
		}
	}
	return found
}

// answerValue is the ask_user answer as Core carries it.
func answerValue() map[string]json.RawMessage {
	return map[string]json.RawMessage{"answer": json.RawMessage(`"` + gateE2EAnswer + `"`)}
}

// gateE2EEventually polls cond for up to fifteen seconds.
func gateE2EEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// outcomeOf is a settled command's disposition kind.
func outcomeOf(t *testing.T, entry sessionstore.DispositionInboxEntry) string {
	t.Helper()
	if entry.Record.Outcome == nil {
		return ""
	}
	return string(entry.Record.Outcome.Kind)
}

// TestAnAskUserGateReachesFactoryAndItsAnswerSettlesApplied is the required
// proof: the agent asks, the gate is visible to Factory's ReadGates under the
// Core session id, the answer is admitted, applied under an authorized attempt,
// settles `applied` from the runtime's own disposition, the projection is
// cleared, and the agent continues with the user's answer.
func TestAnAskUserGateReachesFactoryAndItsAnswerSettlesApplied(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	service, launcher, epoch := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	// THE CAPABILITY SIGNAL, before any gate exists: a gate-publishing Host
	// fences on attach, so the projection's residency mark equals the live
	// owner's registered residency epoch. No Host before v0.4.0 writes the mark
	// on a disposition session at all, so a Factory comparing the two admits a
	// gate_response only to a Host that can apply it.
	gateE2EEventually(t, "the attach-time fencing write", func() bool { return world.mark(t) == epoch })

	world.submit(t, launcher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]
	if got := world.mark(t); got != epoch {
		t.Fatalf("the projection's residency mark = %d, want the Host's residency %d", got, epoch)
	}
	if opened.Kind != string(gate.KindAskUser) || opened.Answerability != sessionwire.GateAnswerabilityResident {
		t.Fatalf("projected gate = %+v, want a resident ask_user gate", opened)
	}
	if opened.Prompt.Body != "What is your favourite colour?" {
		t.Fatalf("projected prompt = %+v, want the agent's question", opened.Prompt)
	}
	if _, err := uuid.Parse(string(opened.GateID)); err != nil {
		t.Fatalf("projected gate id %q is not harness's", opened.GateID)
	}

	id := world.answer(t, opened, "answer", answerValue())
	entry := world.settled(t, id)
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "applied" {
		t.Fatalf("the answer settled %q with disposition %q, want applied/applied", entry.Record.State, outcomeOf(t, entry))
	}
	if entry.Record.Attempt == nil {
		t.Fatal("the answer settled with no authorized attempt")
	}
	world.gates(t, 0)
	gateE2EEventually(t, "the agent to continue with the answer", func() bool { return world.llm.sawToolResult(gateE2EAnswer) })
	resolved := world.resolutions(t, opened.GateID)
	if len(resolved) != 1 || resolved[0].Source.Kind != gate.ResponseFromUser {
		t.Fatalf("the journal resolved the gate %d times (%+v), want once, by the user", len(resolved), resolved)
	}
}

// TestTwoAnswersToOneGateApplyOnceAndTheSecondIsANoOp: Factory may admit two
// answers while the gate is still projected. The first is applied; the second
// finds the gate closed and harness settles it no_op — a successful application
// with no effect, never a second answer.
func TestTwoAnswersToOneGateApplyOnceAndTheSecondIsANoOp(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	service, launcher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })

	world.submit(t, launcher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]
	first := world.answer(t, opened, "answer", answerValue())
	second := world.answer(t, opened, "answer", map[string]json.RawMessage{"answer": json.RawMessage(`"CRIMSON"`)})

	firstEntry, secondEntry := world.settled(t, first), world.settled(t, second)
	if outcomeOf(t, firstEntry) != "applied" || outcomeOf(t, secondEntry) != "no_op" {
		t.Fatalf("dispositions = (%q, %q), want (applied, no_op)", outcomeOf(t, firstEntry), outcomeOf(t, secondEntry))
	}
	if secondEntry.Record.State != sessionstore.InboxStateApplied {
		t.Fatalf("the second answer settled %q, want applied (no_op is a successful application)", secondEntry.Record.State)
	}
	if resolved := world.resolutions(t, opened.GateID); len(resolved) != 1 {
		t.Fatalf("the journal resolved the gate %d times, want once", len(resolved))
	}
	gateE2EEventually(t, "the agent to continue with the first answer", func() bool { return world.llm.sawToolResult(gateE2EAnswer) })
	if world.llm.sawToolResult("CRIMSON") {
		t.Fatal("the second answer reached the agent")
	}
}

// TestAPermissionGateOpenAcrossAReplacementIsAnsweredOnTheSuccessor: Host A's
// agent raises a permission gate and A publishes it; A crashes with the gate
// open; Host B restores the session (harness restores a permission gate with
// its ORIGINAL GateOpened), makes its fencing write — the mark moves to B's
// residency, so A could no longer write — and the answer Factory admits against
// the projection A published is applied on B, and B's agent runs the tool.
func TestAPermissionGateOpenAcrossAReplacementIsAnsweredOnTheSuccessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, firstEpoch := world.host(t, 4)
	world.submit(t, firstLauncher.controller(), "PLEASE-RUN-GATED")
	opened := world.gates(t, 1)[0]
	if opened.Kind != string(gate.KindPermission) {
		t.Fatalf("projected gate kind = %q, want a permission gate", opened.Kind)
	}
	if got := world.mark(t); got != firstEpoch {
		t.Fatalf("mark on Host A = %d, want A's residency %d", got, firstEpoch)
	}
	// HOST A CRASHES with the gate open: it is not drained (a drain would
	// abandon the gate; see TestADrainWithAGateOpenEndsAndLeavesTheGateToASuccessor), its leases
	// lapse, and its in-memory runtime is a zombie a successor fences out.
	t.Cleanup(func() { stopBounded(first) })
	if world.runs.Load() != 0 {
		t.Fatal("the gated tool ran before anyone approved it")
	}

	second, secondLauncher, secondEpoch := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	if _, restores := secondLauncher.counts(); len(restores) != 1 {
		t.Fatalf("Host B restored %v, want the one session", restores)
	}
	gateE2EEventually(t, "Host B's fencing write", func() bool { return world.mark(t) == secondEpoch })
	if secondEpoch <= firstEpoch {
		t.Fatalf("Host B's residency %d is not above A's %d", secondEpoch, firstEpoch)
	}
	still := world.gates(t, 1)[0]
	if still.GateID != opened.GateID || still.OpenedJournalSeq != opened.OpenedJournalSeq {
		t.Fatalf("after the re-placement the projection holds %+v, want the same gate %+v", still, opened)
	}

	id := world.answer(t, opened, string(gate.ApprovalApprove), map[string]json.RawMessage{})
	entry := world.settled(t, id)
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "applied" {
		t.Fatalf("the approval settled %q/%q, want applied/applied", entry.Record.State, outcomeOf(t, entry))
	}
	if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != secondEpoch {
		t.Fatalf("the approval's attempt = %+v, want it authorized under Host B's residency %d", entry.Record.Attempt, secondEpoch)
	}
	world.gates(t, 0)
	resolved := world.resolutions(t, opened.GateID)
	if len(resolved) != 1 || resolved[0].Source.Kind != gate.ResponseFromUser || resolved[0].Action != string(gate.ApprovalApprove) {
		t.Fatalf("the journal resolved the gate %+v, want once, approved by the user", resolved)
	}
	runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
	if err != nil || resolved[0].Cause.CommandID != runtimeCommand {
		t.Fatalf("the GateResolved's cause = %v, want the admitted runtime command %v", resolved[0].Cause.CommandID, runtimeCommand)
	}

	// HARNESS v0.35.0'S LIMIT, PINNED SO A CHANGE IS NOTICED: restore marks
	// the turn that was parked at the gate TurnInterrupted, so the approval is
	// applied to the restored gate and nothing is waiting to run the tool.
	// Resuming that turn is harness's to do (booked for v0.36.0).
	if got := countOf[event.TurnInterrupted](t, world.journal, world.runtimeID); got != 1 {
		t.Fatalf("%d TurnInterrupted, want the restore's one", got)
	}
	if world.runs.Load() != 0 {
		t.Fatal("the gated tool ran after a restore; harness now resumes the interrupted turn, so update this test and the README")
	}
	// The session itself goes on: the next turn runs on Host B.
	done := countOf[event.TurnDone](t, world.journal, world.runtimeID)
	world.submit(t, secondLauncher.controller(), "carry on")
	gateE2EEventually(t, "the next turn to finish on Host B", func() bool {
		return countOf[event.TurnDone](t, world.journal, world.runtimeID) > done
	})
}

// TestAnAskUserGateClosedAtRestoreSettlesNoOp documents harness v0.35.0's
// limit: restore closes an ask_user gate (CloseRestoreUnavailable). The
// successor's fold sees that GateResolved and clears the projection, and an
// answer admitted before it could see so settles no_op — never a rejection and
// never an answer to a gate that no longer exists.
func TestAnAskUserGateClosedAtRestoreSettlesNoOp(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, firstEpoch := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	world.submit(t, firstLauncher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]
	// Host A crashes with the gate open, but its consumer is still running —
	// the zombie a successor must fence out.

	// THE SUCCESSOR FENCES BEFORE ITS RESTORE. Host B is held between its
	// residency grant and its runtime restore; its fencing write has already
	// raised the mark, so the answer admitted now cannot be taken by A, whose
	// ownership check fails. Deterministic: before the fence moved to attach,
	// A applied it in the window (spec gate C2, quality gate F2).
	second := world.hostHeld(t, 5)
	t.Cleanup(func() { stopBounded(second.service) })
	gateE2EEventually(t, "Host B's attach-time fencing write", func() bool { return world.mark(t) > firstEpoch })
	id := world.answer(t, opened, "answer", answerValue())
	time.Sleep(300 * time.Millisecond)
	if entry := world.command(t, id); entry.Record.Attempt != nil {
		t.Fatalf("a fenced-out predecessor began an attempt on the answer: %+v", entry.Record.Attempt)
	}
	second.finish(t)

	entry := world.settled(t, id)
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "no_op" {
		t.Fatalf("the answer to a gate closed at restore settled %q/%q, want applied/no_op", entry.Record.State, outcomeOf(t, entry))
	}
	world.gates(t, 0)
	resolved := world.resolutions(t, opened.GateID)
	if len(resolved) != 1 || resolved[0].Reason != gate.CloseRestoreUnavailable {
		t.Fatalf("the gate's resolutions = %+v, want exactly the restore's close", resolved)
	}
}

// TestADrainWithAGateOpenEndsAndLeavesTheGateToASuccessor: a graceful drain of
// a session parked at a gate is CRASH-EQUIVALENT. harness releases a session
// nonterminally only when it is whole-session idle, and a session at a gate
// never is, so the release is refused within the drain's grace. The refused
// runtime is then left PARKED — its context is not cancelled — so it writes
// nothing: no TurnInterrupted, no GateResolved{abandoned}. The gate stays open
// and projected, exactly as after a crash. The runtime keeps its journal lease
// until its process exits, so a successor in the SAME process is refused.
// (spec gate M1, quality gate F1: before, Stop cancelled the runtime and it
// abandoned the gate after the publisher had stopped.)
func TestADrainWithAGateOpenEndsAndLeavesTheGateToASuccessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	first, firstLauncher, _ := world.host(t, 4)
	world.submit(t, firstLauncher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]

	report, err := stopReport(t, first)
	if err != nil {
		t.Fatalf("Stop with a gate open = %v, want it to end within its grace", err)
	}
	refused := false
	for _, failure := range report.Failures {
		if failure.SessionID == composeSession && failure.Step == "release_residency" {
			refused = true
		}
		// The drain halts the consumer first; a parked gate holds no pass in
		// flight, so that halt must not be what costs this drain its bound.
		if failure.Step == "halt_consumption" {
			t.Errorf("the drain could not halt a gated session's consumer: %v", failure)
		}
	}
	if !refused {
		t.Fatalf("drain failures = %+v, want the runtime's refused release recorded", report.Failures)
	}
	// Abandonment used to land up to ~300ms after Stop returned; a parked
	// runtime writes nothing at all, so a second of silence is the assertion.
	time.Sleep(time.Second)
	for _, ev := range harnesstest.Events(t, world.journal, world.runtimeID) {
		switch ev := ev.(type) {
		case event.GateResolved:
			t.Fatalf("the drain resolved the gate (%q); a parked runtime writes nothing", ev.Reason)
		case event.TurnInterrupted:
			t.Fatal("the drain interrupted the parked turn; the runtime was cancelled")
		}
	}
	if still := world.gates(t, 1)[0]; still.GateID != opened.GateID {
		t.Fatalf("the projection after the drain holds %+v, want the open gate", still)
	}

	blueprint := world.fixture.blueprint(t)
	blueprint.Generation = 5
	launcher := &capturingLauncher{rig: gateE2ERig(t, world.journal, world.llm, world.runs)}
	adapter, err := harnessadapter.New(launcher)
	if err != nil {
		t.Fatal(err)
	}
	blueprint.Collaborators.Registrar = host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		target, err := department.NewRigTarget(adapter, composeCompat, department.Capabilities{
			SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1, CaptureSafety: department.CaptureSafetyStreaming,
		})
		if err != nil {
			return nil, err
		}
		return []department.Registration{{AgentID: composeAgent, Target: target}}, nil
	})
	second, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	t.Cleanup(func() { stopBounded(second) })
	if err := second.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := attachAsFactoryDoes(t, second); err == nil || !strings.Contains(err.Error(), "lease held") {
		t.Fatalf("a successor's attach while the drained runtime still holds its journal = %v, want the journal lease refusal", err)
	}
}

// TestAnAnswerRacingTheGatesTimeoutSettlesOnceByWhicheverWon: a host-owned form
// gate with a short response timeout (policy: decline) is answered at about the
// moment the timeout fires, over and over. Whichever lands first resolves the
// gate, exactly once; the answer settles `applied` when it won (the GateResolved
// is the user's and names the admitted runtime command) and `no_op` when the
// timeout won (the policy's). Never two resolutions, never a rejection, never a
// command left applying.
func TestAnAnswerRacingTheGatesTimeoutSettlesOnceByWhicheverWon(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{})
	service, launcher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(service) })
	controller := launcher.controller()
	opener, ok := controller.(session.GateHost)
	if !ok {
		t.Fatal("the harness session is not a session.GateHost")
	}

	won := map[string]int{}
	for round, timeout := range []time.Duration{
		40 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond, 160 * time.Millisecond, 250 * time.Millisecond,
		40 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond, 160 * time.Millisecond, 250 * time.Millisecond,
		60 * time.Millisecond, 100 * time.Millisecond, 140 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
	} {
		schema := gate.PromptSchema{Fields: []gate.Field{{Name: "colour", Label: "Colour", Kind: gate.FieldText, Required: true}}}
		id, err := opener.OpenHostGate(t.Context(), controller.ActiveLoop().ID(), gate.Gate{
			Kind: gate.KindForm, Resolver: gate.ResolverSession,
			Prompt: gate.Prompt{Title: "Pick a colour", Controls: []gate.Control{
				{Action: gate.FormActionAccept, Label: "Accept"}, {Action: gate.FormActionDecline, Label: "Decline"},
			}},
			ResponsePolicy: gate.ResponsePolicy{Timeout: timeout, OnTimeout: gate.PolicyRespond,
				Response: gate.ResponseTemplate{Action: gate.FormActionDecline}},
		}, gate.FormPayload{Title: "Pick a colour", Schema: schema})
		if err != nil {
			t.Fatalf("round %d: OpenHostGate: %v", round, err)
		}
		go func() { _, _ = opener.AwaitGateAnswer(context.WithoutCancel(t.Context()), id) }()
		gateID := sessionwire.GateID(id.String())

		// Factory can only answer a gate it has seen. If the timeout closed it
		// before it was ever projected, there was nothing to answer.
		projection, seen := world.projectedOrClosed(t, gateID)
		if !seen {
			won["closed before it was visible"]++
			continue
		}
		command := world.answer(t, projection, gate.FormActionAccept, map[string]json.RawMessage{"colour": json.RawMessage(`"teal"`)})
		entry := world.settled(t, command)
		resolved := world.resolutions(t, gateID)
		if len(resolved) != 1 {
			t.Fatalf("round %d (timeout %v): the gate was resolved %d times: %+v", round, timeout, len(resolved), resolved)
		}
		runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
		if err != nil {
			t.Fatal(err)
		}
		answerWon := resolved[0].Cause.CommandID == runtimeCommand
		switch {
		case entry.Record.State != sessionstore.InboxStateApplied:
			t.Fatalf("round %d: the answer settled %q, want applied", round, entry.Record.State)
		case answerWon && (outcomeOf(t, entry) != "applied" || resolved[0].Source.Kind != gate.ResponseFromUser):
			t.Fatalf("round %d: the answer resolved the gate but settled %q (source %q)", round, outcomeOf(t, entry), resolved[0].Source.Kind)
		case !answerWon && (outcomeOf(t, entry) != "no_op" || resolved[0].Source.Kind != gate.ResponseFromPolicy):
			t.Fatalf("round %d: the timeout resolved the gate (%q, source %+v, cause %v) but the answer settled %q", round, resolved[0].Reason, resolved[0].Source, resolved[0].Cause.CommandID, outcomeOf(t, entry))
		}
		if answerWon {
			won["answer"]++
		} else {
			won["timeout"]++
		}
	}
	world.gates(t, 0)
	t.Logf("race outcomes over the rounds: %v", won)
}

// projectedOrClosed waits until the projection holds gateID (returned) or the
// journal has already closed it (false).
func (w *gateE2EWorld) projectedOrClosed(t *testing.T, gateID sessionwire.GateID) (sessionwire.GateProjection, bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		page, err := w.factory.ReadGates(t.Context(), sessionstore.ReadGatesRequest{TenantID: composeTenant, SessionID: composeSession})
		if err == nil {
			for _, projection := range page.Gates {
				if projection.GateID == gateID {
					return projection, true
				}
			}
		}
		if len(w.resolutions(t, gateID)) > 0 {
			return sessionwire.GateProjection{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("gate %s was neither projected nor closed within 15s", gateID)
	return sessionwire.GateProjection{}, false
}

// gateE2ECrashLedger fails the append of a gate_response's disposition frame
// while armed — the durable state a crash leaves between the runtime's
// GateResolved (the answer, durable) and its disposition (the evidence, lost).
type gateE2ECrashLedger struct {
	storage.Ledger
	armed *atomic.Bool
	hits  *atomic.Int32
}

func (l gateE2ECrashLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if l.armed.Load() {
		if envelope, err := sessionstore.DecodeEnvelope(payload); err == nil &&
			envelope.DispositionKind != "" && envelope.CommandKind == "gate_response" {
			l.hits.Add(1)
			return errors.New("injected: the process died before the disposition was written")
		}
	}
	return l.Ledger.Append(ctx, name, expected, payload)
}

// TestACrashBetweenTheAnswerAndItsDispositionIsNeverATombstone: the answer is
// durable (GateResolved, caused by the admitted runtime command) and its
// disposition frame never landed. Host A cannot settle it — there is no
// evidence — and Host B, taking over, must not close the attempt as
// not_applied over an answer that was applied: harness's closer finds the
// GateResolved by its cause and refuses. The command stays applying (the known
// liveness gap, the same one input has); the gate is not re-opened, and the
// projection is cleared by the successor's fold.
func TestACrashBetweenTheAnswerAndItsDispositionIsNeverATombstone(t *testing.T) {
	armed, hits := &atomic.Bool{}, &atomic.Int32{}
	world := newGateE2EWorld(t, gateE2EOptions{
		takeover: true,
		ledger: func(inner storage.Ledger) storage.Ledger {
			return gateE2ECrashLedger{Ledger: inner, armed: armed, hits: hits}
		},
	})
	first, firstLauncher, _ := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	world.submit(t, firstLauncher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]

	armed.Store(true)
	id := world.answer(t, opened, "answer", answerValue())
	gateE2EEventually(t, "the disposition append to be lost", func() bool { return hits.Load() > 0 })
	gateE2EEventually(t, "the answer to be durable", func() bool { return len(world.resolutions(t, opened.GateID)) == 1 })
	// The answer reached A's agent in memory before the crash: waited for
	// HERE, on the event itself, because once B restores it holds the journal
	// and A's loop can no longer reach the model (quality gate F4: asserted
	// after B's attach, this raced B's fence 9/20 at -race -cpu 1).
	gateE2EEventually(t, "the agent to receive the answer before the crash", func() bool { return world.llm.sawToolResult(gateE2EAnswer) })
	// AND HOST A KEPT ITS RUNTIME (D3 gate F1). The lost disposition leaves the
	// attempt applying under A's own grant, which D3 otherwise reports stranded
	// and abandons; a gate answer whose prefix committed is exempt, because
	// abandoning would throw the in-memory answer away and settle nothing. The
	// agent seeing the answer above is only deterministic because of that: the
	// gate measured the loss 2 runs in 6 when A abandoned.
	liveness, ok := firstLauncher.controller().(session.Liveness)
	if !ok {
		t.Fatal("the harness session is not a session.Liveness")
	}
	select {
	case <-liveness.Done():
		t.Fatal("Host A abandoned its runtime over a gate answer whose prefix committed")
	default:
	}
	// Host A crashes here: it is abandoned, not drained, and its leases lapse.

	second, _, secondEpoch := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	gateE2EEventually(t, "Host B's fencing write", func() bool { return world.mark(t) == secondEpoch })
	world.gates(t, 0)

	// Give Host B's consumer many passes at the stranded attempt.
	time.Sleep(time.Second)
	entry := world.command(t, id)
	if entry.Record.State != sessionstore.InboxStateApplying || entry.Record.Outcome != nil {
		t.Fatalf("the command is %q with outcome %+v, want it still applying: a not_applied tombstone over an applied answer is the one unrecoverable error",
			entry.Record.State, entry.Record.Outcome)
	}
	resolved := world.resolutions(t, opened.GateID)
	runtimeCommand, err := uuid.Parse(string(entry.Record.Descriptor.RuntimeCommandID))
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 1 || resolved[0].Cause.CommandID != runtimeCommand {
		t.Fatalf("resolutions = %+v, want exactly the answer, caused by the admitted command", resolved)
	}
}

// TestAPermissionGateSurvivesADrainAndIsAnsweredOnTheSuccessor is the
// crash-equivalence the drain now keeps: Host A drains with a permission gate
// open, its parked runtime's journal lease lapses (as it does when the drained
// process exits), and Host B restores the session with the gate still open and
// applies the approval under its own residency.
func TestAPermissionGateSurvivesADrainAndIsAnsweredOnTheSuccessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, _ := world.host(t, 4)
	world.submit(t, firstLauncher.controller(), "PLEASE-RUN-GATED")
	opened := world.gates(t, 1)[0]
	stopWithin(t, first)

	second, _, secondEpoch := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	gateE2EEventually(t, "Host B's fencing write", func() bool { return world.mark(t) == secondEpoch })
	if still := world.gates(t, 1)[0]; still.GateID != opened.GateID {
		t.Fatalf("after the drain and the restore the projection holds %+v, want the same gate", still)
	}
	entry := world.settled(t, world.answer(t, opened, string(gate.ApprovalApprove), map[string]json.RawMessage{}))
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "applied" {
		t.Fatalf("the approval after a drain settled %q/%q, want applied/applied", entry.Record.State, outcomeOf(t, entry))
	}
	if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != secondEpoch {
		t.Fatalf("the approval's attempt = %+v, want Host B's residency %d", entry.Record.Attempt, secondEpoch)
	}
	for _, resolved := range world.resolutions(t, opened.GateID) {
		if resolved.Reason == gate.CloseAbandoned {
			t.Fatal("the gate was abandoned across the drain")
		}
	}
}

// TestAnAnswerInTheFenceWindowIsNeverTakenByAFencedPredecessor is the quality
// gate's F2 construction, held open deterministically: Host A holds a
// permission gate and is not dead; Host B has taken residency and is held
// before its runtime restore. The answer is admitted in that window. Before
// v0.4.0's fix round the projection fence came after the restore, so A's
// ownership check still passed, A began the attempt, lost the journal to B,
// and the user's answer settled rejected/not_applied. B now fences at attach,
// before the restore: A's check fails, A never begins an attempt, and B applies
// the answer.
func TestAnAnswerInTheFenceWindowIsNeverTakenByAFencedPredecessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, firstEpoch := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	world.submit(t, firstLauncher.controller(), "PLEASE-RUN-GATED")
	opened := world.gates(t, 1)[0]

	second := world.hostHeld(t, 5)
	t.Cleanup(func() { stopBounded(second.service) })
	gateE2EEventually(t, "Host B's attach-time fencing write", func() bool { return world.mark(t) > firstEpoch })
	secondEpoch := world.mark(t)
	id := world.answer(t, opened, string(gate.ApprovalApprove), map[string]json.RawMessage{})
	time.Sleep(500 * time.Millisecond)
	if entry := world.command(t, id); entry.Record.Attempt != nil || entry.Record.State == sessionstore.InboxStateRejected {
		t.Fatalf("in the fence window the predecessor took the answer: state %q, attempt %+v", entry.Record.State, entry.Record.Attempt)
	}
	second.finish(t)

	entry := world.settled(t, id)
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "applied" {
		t.Fatalf("the answer settled %q/%q, want applied/applied on the successor", entry.Record.State, outcomeOf(t, entry))
	}
	if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != secondEpoch {
		t.Fatalf("the attempt = %+v, want Host B's residency %d", entry.Record.Attempt, secondEpoch)
	}
	if resolved := world.resolutions(t, opened.GateID); len(resolved) != 1 {
		t.Fatalf("the gate was resolved %d times, want once", len(resolved))
	}
}

// TestAnAnswerToAGateTheSuccessorAlreadyResolvedIsNotTakenByThePredecessor is
// the UNPROJECTED arm of the ownership check, end to end (the regate's X11e):
// Host A is fenced out and still live; Host B has taken the session and its
// publisher has already resolved the gate, so the projection no longer holds
// it. A must not take the not-projected arm and dispatch to its zombie runtime
// — the successor's recovery would settle the user's answer
// rejected/not_applied — and B must settle it no_op.
func TestAnAnswerToAGateTheSuccessorAlreadyResolvedIsNotTakenByThePredecessor(t *testing.T) {
	world := newGateE2EWorld(t, gateE2EOptions{takeover: true})
	first, firstLauncher, firstEpoch := world.host(t, 4)
	t.Cleanup(func() { stopBounded(first) })
	world.submit(t, firstLauncher.controller(), "PLEASE-ASK")
	opened := world.gates(t, 1)[0]

	// Host A crashes; Host B takes the session, restores it (which closes the
	// ask_user gate) and its publisher clears the projection. A is still live.
	second, _, secondEpoch := world.host(t, 5)
	t.Cleanup(func() { stopBounded(second) })
	gateE2EEventually(t, "Host B's fence and its publisher clearing the gate", func() bool {
		page, err := world.factory.ReadGates(t.Context(), sessionstore.ReadGatesRequest{TenantID: composeTenant, SessionID: composeSession})
		return err == nil && len(page.Gates) == 0 && world.mark(t) == secondEpoch && secondEpoch > firstEpoch
	})

	// The answer Factory admitted against the projection it last saw.
	entry := world.settled(t, world.answer(t, opened, "answer", answerValue()))
	if entry.Record.State != sessionstore.InboxStateApplied || outcomeOf(t, entry) != "no_op" {
		t.Fatalf("the answer settled %q/%q, want applied/no_op from the successor", entry.Record.State, outcomeOf(t, entry))
	}
	if entry.Record.Attempt == nil || uint64(entry.Record.Attempt.ResidencyEpoch) != secondEpoch {
		t.Fatalf("the attempt = %+v, want the successor's residency %d and never the fenced-out predecessor's %d",
			entry.Record.Attempt, secondEpoch, firstEpoch)
	}
}
