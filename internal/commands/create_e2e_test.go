// The create and restore arms, end to end over released modules.
//
// EVERY SEAM BELOW IS REAL AND NONE OF IT VOUCHES FOR ITSELF. The applier is
// commands.DispositionApplier; the records, the claims, the attempts and the
// settlement are sessionstore v0.12.0's over one backend; the evidence the
// store settles from is a REAL harness journal over another, reached through
// the same DispositionEvidenceReader a composed Host registers; and the runtime
// is a real rig behind the production harnessadapter. The only doubles are the
// inference client, which records what the model was sent, and the ownership
// fence, which is composition rather than durability.
//
// THAT IS THE POINT OF THE FILE RATHER THAN THOROUGHNESS. The defect this
// release exists to close -- create and restore could never settle -- was
// invisible for one reason: every harness that ever drove it had the product
// runtime VOUCH for its own dispatches. A recorder-backed evidence reader would
// make every case here pass with the arms removed. So nothing here records an
// outcome on the runtime's behalf, and the create case asserts the first
// message reached the MODEL REQUEST, because a create that settles `applied`
// with an empty turn is exactly what the payload defect looks like.
package commands_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/inference"
	"github.com/looprig/inference/stream"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/harnessadapter"
	"github.com/looprig/host/internal/harnesstest"
	"github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

const (
	liveTenant  = sessionwire.TenantID("tenant-live")
	liveSession = sessionwire.SessionID("session-live")
	liveAgent   = sessionwire.AgentID("agent-live")
	liveCompat  = "compat-live"
	liveBinding = "binding-live"
)

// liveRuntimeID is the runtime identity Factory's immutable binding names. It
// is a literal of the shape Factory derives and is NOT anything computable
// from the Core session id.
var liveRuntimeID = uuid.MustParse("2c7a91f4-3b5e-8d6c-a1f0-4e93b7d20c58")

// ---------------------------------------------------------------------------
// The doubles, and there are only two
// ---------------------------------------------------------------------------

// recordingLLM answers every turn with one text chunk and keeps every request,
// so a test can read what the model was actually sent.
type recordingLLM struct {
	mu       sync.Mutex
	requests []inference.Request
}

func (*recordingLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("commands_test: Invoke is unused")
}

func (l *recordingLLM) Stream(_ context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	l.mu.Lock()
	l.requests = append(l.requests, request)
	l.mu.Unlock()
	sent := false
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if sent {
			return nil, io.EOF
		}
		sent = true
		return content.Chunk(&content.TextChunk{Text: "ok"}), nil
	}, nil), nil
}

// sawUserText reports whether any model request carried text as a user
// message. It reads the REQUEST rather than the journal deliberately: a
// journal record proves harness wrote something down, and what this release's
// defect destroyed was the message the MODEL sees.
func (l *recordingLLM) sawUserText(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, request := range l.requests {
		for _, message := range request.Messages {
			if _, user := message.(*content.UserMessage); !user {
				continue
			}
			encoded, err := json.Marshal(message)
			if err != nil {
				continue
			}
			if strings.Contains(string(encoded), text) {
				return true
			}
		}
	}
	return false
}

func (l *recordingLLM) modelRequests() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.requests)
}

// openFence is the ownership guard, and it is the one seam that is a double
// rather than a released edge. Losing a residency grant is a composition
// concern with its own tests in internal/residency; nothing here is about it,
// and a fence that refused would only hide the durable answers this file reads.
type openFence struct{}

func (openFence) Held() error                    { return nil }
func (openFence) Lost() <-chan struct{}          { return nil }
func (openFence) Write(write func() error) error { return write() }

// ---------------------------------------------------------------------------
// The world
// ---------------------------------------------------------------------------

// liveWorld is one session held by one Host, over two real stores.
type liveWorld struct {
	t *testing.T

	// released is the orchestration store, as Factory and Host both see it.
	released *sessionstore.Store
	adapted  *sessionstoreadapter.Store

	// journal is the runtime's OWN store, on a different backend. It is the
	// evidence reader the released store settles from.
	journal *harnessstore.Store

	llm     *recordingLLM
	adapter *harnessadapter.Adapter
	binding sessionstore.SessionBinding
	host    *hostconfig.Host

	nextCommand int
}

func newLiveWorld(t *testing.T) *liveWorld {
	t.Helper()
	llm := &recordingLLM{}
	journal := harnesstest.Store(t, harnesstest.Backend(t), liveTenant)
	adapter, err := harnessadapter.New(&rigLaunchers{rig: harnesstest.Rig(t, journal, llm)},
		harnessadapter.WithBlockDecoder(liveDecoder()))
	if err != nil {
		t.Fatalf("harnessadapter.New: %v", err)
	}

	router, err := sessionstoreadapter.NewTenantEvidenceRouter(
		map[sessionstoreadapter.EvidenceKey]sessionstore.DispositionEvidenceReader{
			{TenantID: liveTenant, StorageBindingID: liveBinding}: journal,
		})
	if err != nil {
		t.Fatalf("evidence router: %v", err)
	}
	released, err := sessionstore.Open(t.Context(), memstore.New(), sessionstore.WithDispositionEvidence(router))
	if err != nil {
		t.Fatalf("open the released store: %v", err)
	}
	t.Cleanup(func() { _ = released.Close(context.WithoutCancel(t.Context())) })
	adapted, err := sessionstoreadapter.New(released,
		sessionstoreadapter.WithNamespaceLayout(func(sessionwire.TenantID, sessionwire.SessionID) string { return "objects/live" }))
	if err != nil {
		t.Fatalf("adapt the released store: %v", err)
	}

	world := &liveWorld{
		t:        t,
		released: released,
		adapted:  adapted,
		journal:  journal,
		llm:      llm,
		adapter:  adapter,
		host:     newLiveHost(t, adapter),
		binding: sessionstore.SessionBinding{
			StorageBindingID: liveBinding,
			BindingVersion:   "v1",
			RuntimeSessionID: liveRuntimeID.String(),
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
	}
	now := time.Now().UTC()
	if _, _, err := released.CreateCatalogEntry(t.Context(), sessionstore.CreateCatalogEntryRequest{
		TenantID: liveTenant, SessionID: liveSession, AgentID: liveAgent,
		RuntimeCompatibilityID: liveCompat, CreatedAt: now, LastActiveAt: now,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled, IdempotencyKey: "idem-live",
		Binding: world.binding,
	}); err != nil {
		t.Fatalf("seed the catalog: %v", err)
	}
	return world
}

// newLiveHost is the Host configuration the applier and the consumer read their
// clock, claim TTL and identity from. The department holds the SAME adapter the
// runtime launches through, so nothing here describes a Host that could not
// have launched the session it is applying commands to.
func newLiveHost(t *testing.T, adapter *harnessadapter.Adapter) *hostconfig.Host {
	t.Helper()
	target, err := department.NewRigTarget(adapter, liveCompat, department.Capabilities{
		SupportsPooled: true, SupportsDedicated: true, AdmissionWeight: 1,
		CaptureSafety: department.CaptureSafetyStreaming,
	})
	if err != nil {
		t.Fatalf("department.NewRigTarget: %v", err)
	}
	dept, err := department.New([]department.Registration{{AgentID: liveAgent, Target: target}})
	if err != nil {
		t.Fatalf("department.New: %v", err)
	}
	built, err := hostconfig.New(hostconfig.Options{
		HostID:            sessionwire.HostID("host-live"),
		InternalEndpoint:  sessionwire.InternalEndpoint("ws://10.0.0.9:9443"),
		IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
		Department:        dept,
		SessionStore:      unusedSessionStore{},
		Workspaces:        unusedWorkspaces{},
		Clock:             wallClock{},
		Auth:              openAuth{},
		Placement:         sessionwire.HostPlacementPooled,
		Capacity:          4,
		WarmTTL:           time.Minute,
		RegistryHeartbeat: 5 * time.Second,
		RegistryExpiry:    31 * time.Second,
		ClaimTTL:          30 * time.Second,
		ApplyDeadline:     2 * time.Minute,
		CommandQueueSize:  64,
		ReconcileInterval: 50 * time.Millisecond,
		ReconcileBatch:    16,
	})
	if err != nil {
		t.Fatalf("hostconfig.New: %v", err)
	}
	return built
}

// wallClock is the real clock, which is what this world wants: every deadline
// here is minutes wide and nothing is measured by moving time by hand.
type wallClock struct{}

func (wallClock) Now() time.Time                       { return time.Now().UTC() }
func (wallClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

type unusedSessionStore struct{}

func (unusedSessionStore) LoadSession(context.Context, sessionwire.TenantID, sessionwire.SessionID) ([]byte, error) {
	return nil, errors.New("commands_test: this world loads no session through hostconfig")
}

type unusedWorkspaces struct{}

func (unusedWorkspaces) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", errors.New("commands_test: this world materializes no workspace")
}

type openAuth struct{}

func (openAuth) VerifyTenant(context.Context, sessionwire.TenantID, string) error { return nil }

// ---------------------------------------------------------------------------
// Launching the runtime through the production adapter
// ---------------------------------------------------------------------------

// rigLaunchers is harnessadapter.Rigs over one real rig.
type rigLaunchers struct{ rig *rig.Rig }

func (r *rigLaunchers) RigForCreate(context.Context, department.RigCreateRequest) (harnessadapter.Launcher, error) {
	return r.rig, nil
}

func (r *rigLaunchers) RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (harnessadapter.Launcher, error) {
	return r.rig, nil
}

// liveDecoder is the block decoder a composition binds: it reads the
// INPUT-SHAPED body finding H7 names, and a create's first message reaches it
// through internal/createbody's re-presentation rather than through a second
// encoding.
func liveDecoder() harnessadapter.BlockDecoder {
	return func(body []byte) ([]content.Block, error) {
		var request sessionwire.InputRequest
		if err := json.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(request.Blocks, &blocks); err != nil {
			return nil, err
		}
		decoded := make([]content.Block, 0, len(blocks))
		for _, block := range blocks {
			decoded = append(decoded, &content.TextBlock{Text: block.Text})
		}
		return decoded, nil
	}
}

// runtime is one launched session behind the production adapter, plus the
// capabilities the applier discovers by assertion.
type liveRuntime struct {
	session department.RigSession
	applier department.CommandApplier
	epochs  department.LeaseEpochReporter
	closer  department.AttemptCloser
	release session.Releaser
}

// launch takes the session resident, through harnessadapter's own NewSession or
// RestoreSession, as a composed Host does.
func (w *liveWorld) launch(restore bool) *liveRuntime {
	w.t.Helper()
	adapter := w.adapter
	var launched department.RigSession
	var err error
	if restore {
		launched, err = adapter.RestoreSession(w.t.Context(), liveRuntimeID, department.RigRestoreRequest{
			TenantID: liveTenant, SessionID: liveSession, AgentID: liveAgent,
			Storage: department.StorageContext{Namespace: "objects/live"},
		})
	} else {
		launched, err = adapter.NewSession(w.t.Context(), department.RigCreateRequest{
			TenantID: liveTenant, SessionID: liveSession, AgentID: liveAgent,
			Placement: sessionwire.HostPlacementPooled, RigSessionID: liveRuntimeID,
			Storage: department.StorageContext{Namespace: "objects/live"},
		})
	}
	if err != nil {
		w.t.Fatalf("launch the runtime (restore=%v): %v", restore, err)
	}
	built := &liveRuntime{session: launched}
	var ok bool
	if built.applier, ok = launched.(department.CommandApplier); !ok {
		w.t.Fatal("the launched session cannot apply commands")
	}
	if built.epochs, ok = launched.(department.LeaseEpochReporter); !ok {
		w.t.Fatal("the launched session reports no journal lease epoch")
	}
	if built.closer, ok = launched.(department.AttemptCloser); !ok {
		w.t.Fatal("the launched session offers no recovery closure")
	}
	if built.release, ok = launched.(session.Releaser); !ok {
		w.t.Fatal("the launched session cannot release residency")
	}
	return built
}

// closerAdapter restates department's two-string closure in commands' own
// vocabulary, exactly as internal/compose does. It is a translation and not a
// double: the closure it forwards is the released runtime's own.
type closerAdapter struct{ closer department.AttemptCloser }

func (a *closerAdapter) CloseAttempt(
	ctx context.Context,
	command sessionwire.CommandID,
	runtimeCommand uuid.UUID,
	kind commands.Kind,
	attempt commands.AttemptID,
	attemptJournalEpoch uint64,
) error {
	return a.closer.CloseAttempt(ctx, command, runtimeCommand, string(kind), string(attempt), attemptJournalEpoch)
}

// hold takes the session's residency grant.
func (w *liveWorld) hold() residency.Lease {
	w.t.Helper()
	lease, err := w.adapted.AcquireSessionLease(w.t.Context(), liveTenant, liveSession)
	if err != nil {
		w.t.Fatalf("acquire the residency grant: %v", err)
	}
	return lease
}

// consumer builds the real consumer over the real applier for one residency.
func (w *liveWorld) consumer(lease residency.Lease, runtime *liveRuntime, dispatch department.CommandApplier) *commands.Consumer {
	w.t.Helper()
	writer, err := w.adapted.DispositionWriterFor(lease)
	if err != nil {
		w.t.Fatalf("bind the disposition writer: %v", err)
	}
	if dispatch == nil {
		dispatch = runtime.applier
	}
	key := registry.Key{TenantID: liveTenant, SessionID: liveSession}
	applier, err := commands.NewDispositionApplier(commands.DispositionApplierOptions{
		Host:           w.host,
		Key:            key,
		ResidencyEpoch: uint64(lease.Epoch()),
		Records:        w.adapted,
		Writes:         writer,
		Runtime:        dispatch,
		JournalEpochs:  runtime.epochs,
		Attempts:       commands.UUIDAttemptIDs{},
		Closer:         &closerAdapter{closer: runtime.closer},
		Fence:          openFence{},
	})
	if err != nil {
		w.t.Fatalf("NewDispositionApplier: %v", err)
	}
	consumer, err := commands.NewConsumer(commands.Options{
		Host:       w.host,
		Key:        key,
		LeaseEpoch: uint64(lease.Epoch()),
		Inbox:      w.adapted,
		Cursors:    w.adapted,
		Processor:  applier,
		Fence:      openFence{},
	})
	if err != nil {
		w.t.Fatalf("NewConsumer: %v", err)
	}
	return consumer
}

// ---------------------------------------------------------------------------
// Playing Factory
// ---------------------------------------------------------------------------

// admit writes one command into the session's disposition inbox exactly as
// Factory does: the canonical Core record as the private body, the immutable
// binding, and an accepted order the store assigns.
func (w *liveWorld) admit(id sessionwire.CommandID, kind sessionstore.CommandKind, request any) sessionwire.CommandID {
	w.t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		w.t.Fatal(err)
	}
	runtimeCommand, err := uuid.New()
	if err != nil {
		w.t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := w.released.AdmitDispositionCommand(w.t.Context(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: liveTenant, SessionID: liveSession, CommandID: id, Binding: w.binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeCommand.String()),
		Kind:                     kind, Payload: payload,
		AcceptedAt: now, ApplyDeadline: now.Add(2 * time.Minute),
	}); err != nil {
		w.t.Fatalf("admit a %s: %v", kind, err)
	}
	return id
}

// envelope is the Core command envelope for one admitted id.
func envelope(id sessionwire.CommandID) sessionwire.CommandEnvelope {
	return sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id}
}

// blockArray is the canonical JSON block array Core carries.
func blockArray(text string) json.RawMessage {
	return json.RawMessage(`[{"type":"text","text":` + strconv.Quote(text) + `}]`)
}

// nextID is the client-generated, retry-stable command identity Factory mints.
func (w *liveWorld) nextID() sessionwire.CommandID {
	w.nextCommand++
	return sessionwire.CommandID("command-" + strconv.Itoa(w.nextCommand))
}

// admitCreate admits a create, with a first message when text is non-empty.
func (w *liveWorld) admitCreate(text string) sessionwire.CommandID {
	w.t.Helper()
	id := w.nextID()
	request := sessionwire.CreateRequest{
		CommandEnvelope: envelope(id), SessionID: liveSession, AgentID: liveAgent,
	}
	if text != "" {
		request.Blocks = blockArray(text)
	}
	if err := request.Validate(); err != nil {
		w.t.Fatalf("the fixture's create is not one Core admits: %v", err)
	}
	return w.admit(id, "create", request)
}

// admitInput admits an input carrying text.
func (w *liveWorld) admitInput(text string) sessionwire.CommandID {
	w.t.Helper()
	id := w.nextID()
	request := sessionwire.InputRequest{CommandEnvelope: envelope(id), SessionID: liveSession, Blocks: blockArray(text)}
	if err := request.Validate(); err != nil {
		w.t.Fatalf("the fixture's input is not one Core admits: %v", err)
	}
	return w.admit(id, "input", request)
}

// admitRestore admits a restore.
func (w *liveWorld) admitRestore() sessionwire.CommandID {
	w.t.Helper()
	id := w.nextID()
	request := sessionwire.RestoreRequest{CommandEnvelope: envelope(id), SessionID: liveSession}
	if err := request.Validate(); err != nil {
		w.t.Fatalf("the fixture's restore is not one Core admits: %v", err)
	}
	return w.admit(id, "restore", request)
}

// record reads one command's durable record.
func (w *liveWorld) record(id sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	w.t.Helper()
	entry, err := w.released.GetDispositionCommand(w.t.Context(), sessionstore.GetDispositionCommandRequest{
		TenantID: liveTenant, SessionID: liveSession, CommandID: id,
	})
	if err != nil {
		w.t.Fatalf("GetDispositionCommand(%s): %v", id, err)
	}
	return entry
}

// cursor reads the durable consumption cursor.
func (w *liveWorld) cursor() uint64 {
	w.t.Helper()
	at, err := w.adapted.LoadCursor(w.t.Context(), liveTenant, liveSession)
	if err != nil {
		w.t.Fatalf("LoadCursor: %v", err)
	}
	return at
}

// eventually polls cond for up to fifteen seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// drain runs reconcile passes until one consumes nothing and reports no more
// durable work.
//
// A SINGLE PASS IS NOT THE CONTRACT, which the tests lane measured against
// released host v0.4.0: an attach runs two passes, the second following the
// first within milliseconds because Reconcile reported More. A fixture that
// asserted after one pass would be asserting about a moment the Host does not
// promise anything at.
func drain(t *testing.T, consumer *commands.Consumer) {
	t.Helper()
	for i := 0; i < 16; i++ {
		pass, err := consumer.Reconcile(t.Context())
		if err != nil {
			t.Fatalf("Reconcile: %v (pass %+v)", err, pass)
		}
		if !pass.More && pass.Consumed == 0 {
			return
		}
	}
	t.Fatal("the consumer never ran out of durable work")
}

// ---------------------------------------------------------------------------
// The cases
// ---------------------------------------------------------------------------

// A CREATE'S FIRST MESSAGE REACHES THE MODEL, AND THE COMMAND SETTLES APPLIED.
//
// BOTH HALVES ARE THE ASSERTION AND NEITHER ALONE WOULD DO. The settlement
// alone is what the payload defect produces: harness accepts a create with no
// Blocks, sends nothing, and records `applied` -- a perfectly settled record
// over a turn that never happened. The model request alone would not prove the
// durable record ever left `applying`, which is the wedge this release closes.
func TestACreatesFirstMessageReachesTheModelAndSettlesApplied(t *testing.T) {
	const firstWords = "REACHED-THE-MODEL"
	world := newLiveWorld(t)
	runtime := world.launch(false)
	lease := world.hold()
	consumer := world.consumer(lease, runtime, nil)

	id := world.admitCreate(firstWords)
	drain(t, consumer)

	entry := world.record(id)
	if entry.Record.State != sessionstore.InboxStateApplied {
		t.Fatalf("the create settled %q, want applied", entry.Record.State)
	}
	eventually(t, "the create's first message to reach a model request", func() bool {
		return world.llm.sawUserText(firstWords)
	})
}

// A CREATE WITH NO FIRST MESSAGE STILL SETTLES, and nothing is invented for it.
// Core's CreateRequest.Blocks is omitempty and an idle create is legitimate; a
// Host that refused one would reinstate the wedge for every idle session.
func TestABareCreateSettlesAppliedAndSendsNothing(t *testing.T) {
	world := newLiveWorld(t)
	runtime := world.launch(false)
	lease := world.hold()
	consumer := world.consumer(lease, runtime, nil)

	id := world.admitCreate("")
	drain(t, consumer)

	if state := world.record(id).Record.State; state != sessionstore.InboxStateApplied {
		t.Fatalf("a bare create settled %q, want applied", state)
	}
	if requests := world.llm.modelRequests(); requests != 0 {
		t.Fatalf("a bare create drove %d model requests, want none", requests)
	}
}

// A RESTORE SETTLES APPLIED AND DRIVES NO TURN. Its effect is residency, which
// Host has already performed by the time the command is dispatched, so there is
// nothing for the runtime to send and nothing to invent.
func TestARestoreSettlesAppliedAndDrivesNoTurn(t *testing.T) {
	world := newLiveWorld(t)
	runtime := world.launch(false)
	lease := world.hold()
	consumer := world.consumer(lease, runtime, nil)

	id := world.admitRestore()
	drain(t, consumer)

	if state := world.record(id).Record.State; state != sessionstore.InboxStateApplied {
		t.Fatalf("a restore settled %q, want applied", state)
	}
	if requests := world.llm.modelRequests(); requests != 0 {
		t.Fatalf("a restore drove %d model requests, want none", requests)
	}
}

// THE STREAM MOVES PAST A CREATE. Before this release the consumer blocked at
// the session's FIRST command and every later input, interrupt and gate
// response sat behind it forever. This is that claim as a durable measurement:
// two commands, both terminal, and a cursor past both.
func TestACreateThenAnInputBothSettleAndTheCursorPassesBoth(t *testing.T) {
	const firstWords = "FIRST-WORDS"
	const laterWords = "LATER-WORDS"
	world := newLiveWorld(t)
	runtime := world.launch(false)
	lease := world.hold()
	consumer := world.consumer(lease, runtime, nil)

	create := world.admitCreate(firstWords)
	input := world.admitInput(laterWords)
	drain(t, consumer)

	for _, row := range []struct {
		what string
		id   sessionwire.CommandID
	}{{"the create", create}, {"the input", input}} {
		if state := world.record(row.id).Record.State; state != sessionstore.InboxStateApplied {
			t.Fatalf("%s settled %q, want applied", row.what, state)
		}
	}
	last := world.record(input).AcceptedOrder
	if at := world.cursor(); at < last {
		t.Fatalf("the cursor is at %d, want at least the input's acceptance order %d", at, last)
	}
	for _, text := range []string{firstWords, laterWords} {
		eventually(t, "the model to be sent "+text, func() bool { return world.llm.sawUserText(text) })
	}
}

// ---------------------------------------------------------------------------
// Migration: the creates host v0.4.0 stranded
// ---------------------------------------------------------------------------

// refusingRuntime is a v0.4.0 Host's runtime edge, reproduced exactly: it
// reports the journal grant it really holds, and refuses every command AFTER
// the attempt is durable, which is what H6 did to a create.
type refusingRuntime struct{}

func (refusingRuntime) ApplyCommand(context.Context, department.RuntimeCommand) error {
	return errors.New("harnessadapter: harness applies only input, interrupt and gate_response commands")
}

// A STRANDED v0.4.0 CREATE IS CLOSED BY A SUCCESSOR AND THE STREAM UNBLOCKS.
//
// THIS IS THE UPGRADE PATH AND IT IS NOT HYPOTHETICAL. Every session a released
// host v0.4.0 held has a create sitting `applying` with a durable attempt and
// no disposition frame: the deadline sweeps skip an attempt-bearing record, so
// it never expires, and the consumer never advances past it. Only a SUCCESSOR
// holding a strictly later journal grant can conclude anything about it, and
// only because harness v0.36.0's Closure.Validate now accepts the kind -- at
// v0.35.0 it refused the same kinds the adapter did, which is why nothing could
// rescue the record.
func TestASuccessorClosesAStrandedCreateAndTheStreamUnblocks(t *testing.T) {
	world := newLiveWorld(t)

	// The predecessor: a real runtime, a real claim, a real durable attempt,
	// and a refusal that arrives after it.
	predecessor := world.launch(false)
	predecessorLease := world.hold()
	stranding := world.consumer(predecessorLease, predecessor, refusingRuntime{})

	create := world.admitCreate("THE-LOST-FIRST-WORDS")
	pass, _ := stranding.Reconcile(t.Context())
	if pass.Blocked == nil {
		t.Fatalf("the v0.4.0-shaped refusal blocked nothing: %+v", pass)
	}
	stranded := world.record(create)
	if stranded.Record.State != sessionstore.InboxStateApplying {
		t.Fatalf("the stranded create is %q, want applying", stranded.Record.State)
	}
	if stranded.Record.Attempt == nil || stranded.Record.Attempt.AttemptID == "" {
		t.Fatalf("the stranded create carries no attempt: %+v", stranded.Record.Attempt)
	}
	if at := world.cursor(); at >= stranded.AcceptedOrder {
		t.Fatalf("the cursor advanced to %d past the stranded create at %d", at, stranded.AcceptedOrder)
	}
	predecessorEpoch, held := predecessor.epochs.LeaseEpoch()
	if !held {
		t.Fatal("the predecessor holds no journal grant")
	}

	// The predecessor goes away, grant and all.
	if err := predecessor.release.ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("release the predecessor's residency: %v", err)
	}
	if err := predecessorLease.Release(t.Context()); err != nil {
		t.Fatalf("release the predecessor's residency grant: %v", err)
	}

	// The successor: a RESTORED runtime under a strictly later journal grant,
	// which is the only thing that may close a predecessor's attempt.
	successor := world.launch(true)
	successorEpoch, held := successor.epochs.LeaseEpoch()
	if !held {
		t.Fatal("the successor holds no journal grant")
	}
	if successorEpoch <= predecessorEpoch {
		t.Fatalf("the successor's journal grant is %d, want strictly above the predecessor's %d", successorEpoch, predecessorEpoch)
	}
	successorLease := world.hold()
	consumer := world.consumer(successorLease, successor, nil)

	input := world.admitInput("THE-WORDS-THAT-FOLLOW")
	drain(t, consumer)

	// THE OUTCOME IS ASSERTED, NOT JUST THE STATE. `rejected` is reachable
	// several ways -- a pre-attempt rejection, a deadline sweep -- and only one
	// of them is this release's claim: a recovery CLOSURE, written by the
	// successor under its own strictly later journal grant, naming the
	// predecessor's attempt and its dead grant.
	closed := world.record(create).Record
	if closed.State != sessionstore.InboxStateRejected {
		t.Fatalf("the stranded create settled %q, want rejected", closed.State)
	}
	if closed.Outcome == nil {
		t.Fatal("the stranded create settled with no outcome, so nothing closed it")
	}
	if closed.Outcome.Kind != "not_applied" {
		t.Fatalf("the stranded create's outcome is %q, want not_applied", closed.Outcome.Kind)
	}
	if closed.Outcome.AttemptID != stranded.Record.Attempt.AttemptID {
		t.Fatalf("the closure names attempt %q, want the stranded one %q", closed.Outcome.AttemptID, stranded.Record.Attempt.AttemptID)
	}
	if uint64(closed.Outcome.AttemptJournalEpoch) != predecessorEpoch || uint64(closed.Outcome.AuthorJournalEpoch) != successorEpoch {
		t.Fatalf("the closure is attempt-epoch %d author-epoch %d, want %d closed by %d",
			closed.Outcome.AttemptJournalEpoch, closed.Outcome.AuthorJournalEpoch, predecessorEpoch, successorEpoch)
	}
	if state := world.record(input).Record.State; state != sessionstore.InboxStateApplied {
		t.Fatalf("the input behind the stranded create settled %q, want applied", state)
	}
	if at := world.cursor(); at < world.record(input).AcceptedOrder {
		t.Fatalf("the cursor is at %d, want past the input the stranded create was blocking", at)
	}
	if world.llm.sawUserText("THE-LOST-FIRST-WORDS") {
		t.Fatal("a create closed as not applied still reached the model")
	}
	eventually(t, "the input behind the stranded create to reach the model", func() bool {
		return world.llm.sawUserText("THE-WORDS-THAT-FOLLOW")
	})
}
