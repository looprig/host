package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// This file is task O7.2: the live-plane behaviour Harness's pkg/serve holds,
// re-established as HOST behaviour at the COMPOSED level.
//
// WHAT IT IS NOT. It is not a migration of Harness's HTTP handlers and it does
// not import pkg/serve. The Harness originals stay green in their own
// repository — they are the compatibility baseline, not a thing being moved —
// and each test below names the original whose BEHAVIOUR it re-establishes
// here. Host's live plane is a Centrifuge RPC/publication transport, not a REST
// API, so a ported behaviour is asserted through the mechanism Host actually
// has: an authenticated per-tenant WebSocket, a bind, a subscription, a command
// delivery, a publication push.
//
// WHY IT LIVES IN internal/compose. Every one of these properties is a claim
// about more than one collaborator — the registry AND the journal grant, the
// residency AND the relay, the transport AND the applier — and the unit tests
// under internal/residency, internal/commands, internal/service and
// internal/realtime/hostlink each hold one side of it. A property that is true
// of two packages separately and false of their composition is exactly the
// class O7.1 shipped (a tenant set on two objects, guarded on one), so these
// assertions are made against the composed Service and its real transport.
//
// NO TEST HERE SLEEPS. Where a bound is Host's, it comes from the fixture's
// fake clock. The only real deadlines are WebSocket read/write deadlines, which
// bound a real network read against a real embedded transport and are failure
// bounds nothing waits for.

// ---------------------------------------------------------------------------
// A Factory link: the real client this file asserts through
// ---------------------------------------------------------------------------

// factoryLink is one real WebSocket connection to one tenant's HostLink
// endpoint, with a single reader goroutine demultiplexing replies from pushes.
//
// IT KEEPS EVERY FRAME IT EVER RECEIVED, verbatim, and that is the whole reason
// it exists rather than the read-one-frame helpers the hostlink package's own
// tests use. The privacy claim below is a claim about BYTES — "this secret
// never crossed the wire" — and a helper that decodes a frame and hands back a
// struct has already thrown away the evidence. See linkPrivacy.
type factoryLink struct {
	t          *testing.T
	connection *websocket.Conn

	mu      sync.Mutex
	frames  [][]byte
	replies map[uint32]json.RawMessage
	pushes  []pushFrame
	readErr error

	waiting chan struct{}
	closed  bool
}

// pushFrame is one asynchronous publication.
type pushFrame struct {
	Channel string
	Data    json.RawMessage
}

// wireReply is one command reply, in the shape the transport writes it.
type wireReply struct {
	ID  uint32 `json:"id"`
	RPC *struct {
		Data json.RawMessage `json:"data"`
	} `json:"rpc,omitempty"`
	Connect   *json.RawMessage `json:"connect,omitempty"`
	Subscribe *json.RawMessage `json:"subscribe,omitempty"`
	Error     *struct {
		Code    uint32 `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// pushEnvelope is the asynchronous frame shape. A push carries no id, so a
// reader that matched it against a pending id would answer the wrong question.
type pushEnvelope struct {
	Push *struct {
		Channel string `json:"channel"`
		Pub     *struct {
			Data json.RawMessage `json:"data"`
		} `json:"pub"`
	} `json:"push,omitempty"`
}

// dialFactoryLink opens and authenticates one link for a tenant.
func dialFactoryLink(t *testing.T, serverURL string, tenant sessionwire.TenantID) *factoryLink {
	t.Helper()
	endpoint := "ws" + strings.TrimPrefix(serverURL, "http") + HostLinkPathPrefix + string(tenant)
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}})
	if err != nil {
		t.Fatalf("dial %s (response %#v): %v", endpoint, response, err)
	}
	link := &factoryLink{
		t:          t,
		connection: connection,
		replies:    map[uint32]json.RawMessage{},
		waiting:    make(chan struct{}, 1),
	}
	go link.read()
	t.Cleanup(link.close)

	negotiation, err := json.Marshal(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		t.Fatalf("marshal negotiation request: %v", err)
	}
	reply := link.send(1, map[string]any{
		"id": 1,
		"connect": map[string]any{
			"token": credentialFor(tenant),
			"data":  json.RawMessage(negotiation),
		},
	})
	if reply.Connect == nil || reply.Error != nil {
		t.Fatalf("connect as %s was refused: %s", tenant, mustJSON(t, reply))
	}
	return link
}

// read is the one reader.
//
// CENTRIFUGE'S JSON PROTOCOL IS NEWLINE-DELIMITED AND BATCHES, which is why
// this splits before it decodes. One WebSocket message may carry several
// replies and pushes separated by "\n", and json.Unmarshal over the whole
// message fails on the trailing documents rather than returning the first: an
// earlier version of this reader decoded the message whole and lost every
// publication after the first in a batch, which showed up as an ordering test
// that passed on a quiet run and reported "0 of 5 arrived" on a busy one. The
// transport's own ping is an empty object and is answered in kind, so a link
// configured with a pong timeout is not dropped mid-test.
func (l *factoryLink) read() {
	for {
		_, message, err := l.connection.ReadMessage()
		if err != nil {
			l.mu.Lock()
			l.readErr = err
			l.mu.Unlock()
			l.wake()
			return
		}
		l.mu.Lock()
		l.frames = append(l.frames, append([]byte(nil), message...))
		l.mu.Unlock()

		for _, frame := range strings.Split(string(message), "\n") {
			frame = strings.TrimSpace(frame)
			if frame == "" {
				continue
			}
			if frame == "{}" {
				_ = l.connection.WriteMessage(websocket.TextMessage, []byte("{}"))
				continue
			}
			l.absorb([]byte(frame))
		}
		l.wake()
	}
}

// absorb files one decoded document as a push or as a reply.
//
// A PUSH CARRIES NO id, so it is tried first: a reader that matched a push
// against a pending id would answer the wrong question, and one that filed a
// reply as a push would lose it.
func (l *factoryLink) absorb(frame []byte) {
	var envelope pushEnvelope
	if err := json.Unmarshal(frame, &envelope); err == nil && envelope.Push != nil && envelope.Push.Pub != nil {
		l.mu.Lock()
		l.pushes = append(l.pushes, pushFrame{
			Channel: envelope.Push.Channel,
			Data:    append(json.RawMessage(nil), envelope.Push.Pub.Data...),
		})
		l.mu.Unlock()
		return
	}
	var reply wireReply
	if err := json.Unmarshal(frame, &reply); err == nil && reply.ID != 0 {
		l.mu.Lock()
		l.replies[reply.ID] = append(json.RawMessage(nil), frame...)
		l.mu.Unlock()
	}
}

// wake releases anything blocked on new state without ever blocking the reader.
func (l *factoryLink) wake() {
	select {
	case l.waiting <- struct{}{}:
	default:
	}
}

// send writes one command and returns its reply.
//
// The wait is bounded by a REAL deadline because the counterparty is a real
// transport on a real socket; it is a failure bound and a passing test never
// reaches it.
func (l *factoryLink) send(id uint32, command any) wireReply {
	l.t.Helper()
	payload, err := json.Marshal(command)
	if err != nil {
		l.t.Fatalf("marshal command: %v", err)
	}
	if err := l.connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		l.t.Fatalf("set write deadline: %v", err)
	}
	if err := l.connection.WriteMessage(websocket.TextMessage, payload); err != nil {
		l.t.Fatalf("write command %d: %v", id, err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		l.mu.Lock()
		frame, held := l.replies[id]
		readErr := l.readErr
		l.mu.Unlock()
		if held {
			var reply wireReply
			if err := json.Unmarshal(frame, &reply); err != nil {
				l.t.Fatalf("decode reply %s: %v", frame, err)
			}
			return reply
		}
		if readErr != nil {
			l.t.Fatalf("the link ended before reply %d arrived: %v", id, readErr)
		}
		select {
		case <-l.waiting:
		case <-deadline.C:
			l.t.Fatalf("no reply to command %d within the failure bound", id)
		}
	}
}

// rpc issues one HostLink RPC.
func (l *factoryLink) rpc(id uint32, method string, body any) wireReply {
	l.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		l.t.Fatalf("marshal rpc body: %v", err)
	}
	return l.send(id, map[string]any{
		"id":  id,
		"rpc": map[string]any{"method": method, "data": json.RawMessage(data)},
	})
}

// subscribe joins one channel.
func (l *factoryLink) subscribe(id uint32, channel string) wireReply {
	l.t.Helper()
	return l.send(id, map[string]any{"id": id, "subscribe": map[string]any{"channel": channel}})
}

// awaitPushes blocks until at least want publications have arrived and returns
// them. The bound is a real failure deadline, not a wait this test schedules.
func (l *factoryLink) awaitPushes(want int) []pushFrame {
	l.t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		l.mu.Lock()
		got := append([]pushFrame(nil), l.pushes...)
		l.mu.Unlock()
		if len(got) >= want {
			return got
		}
		select {
		case <-l.waiting:
		case <-deadline.C:
			l.t.Fatalf("%d publications arrived, want at least %d", len(got), want)
			return nil
		}
	}
}

// received is every byte this link was ever sent, concatenated. It is the
// privacy claim's instrument.
func (l *factoryLink) received() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	var all []byte
	for _, frame := range l.frames {
		all = append(all, frame...)
	}
	return all
}

func (l *factoryLink) close() {
	l.mu.Lock()
	already := l.closed
	l.closed = true
	l.mu.Unlock()
	if !already {
		_ = l.connection.Close()
	}
}

// accepted asserts the reply is an ACCEPTED RPC: no transport error and an
// empty body. The bind reply contract is "empty means accepted", so this is the
// whole of it.
func accepted(t *testing.T, reply wireReply) {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("rpc was refused at the transport: %#v", *reply.Error)
	}
	if reply.RPC != nil && len(reply.RPC.Data) != 0 && string(reply.RPC.Data) != "null" {
		t.Fatalf("accepted rpc returned a body %s, want none", reply.RPC.Data)
	}
}

// refusalOf returns the Core class an RPC was refused with, or reports that the
// reply was not a refusal at all.
func refusalOf(t *testing.T, reply wireReply) (sessionwire.HostLinkError, bool) {
	t.Helper()
	if reply.Error != nil {
		// A transport-level error is a refusal too, but it carries no Core
		// class. The caller decides whether that is acceptable.
		return sessionwire.HostLinkError{}, false
	}
	if reply.RPC == nil || len(reply.RPC.Data) == 0 || string(reply.RPC.Data) == "null" {
		return sessionwire.HostLinkError{}, false
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply.RPC.Data, &refusal); err != nil {
		return sessionwire.HostLinkError{}, false
	}
	return refusal, true
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(encoded)
}

// serve mounts the composition's handler on a real HTTP server.
func (f *fixture) serve() *httptest.Server {
	f.t.Helper()
	server := httptest.NewServer(f.svc.Handler())
	f.t.Cleanup(server.Close)
	return server
}

// ---------------------------------------------------------------------------
// A runtime that actually publishes
// ---------------------------------------------------------------------------

// publishingSession is a runtime whose committed tail a test feeds.
//
// The fixture's controllableSession returns a CLOSED publication channel, which
// makes every relay in that file end at once — correct for the lifecycle tests
// it was written for, and useless for an event-ordering claim, since a relay
// that published nothing satisfies "published in order" vacuously. This one
// hands back a live channel and records every command the applier drove into
// it.
type publishingSession struct {
	id uuid.UUID

	mu         sync.Mutex
	published  chan sessionwire.EnduringPublication
	applied    []department.RuntimeCommand
	released   int
	epoch      uint64
	held       bool
	stopped    chan struct{}
	subscribes []sessionwire.EventID

	// effects, when set, is the durable store this runtime commits its public
	// effects into.
	effects *durableCommands

	// drove announces each application. It is the rendezvous awaitApplied waits
	// on, so a test blocks on the EVENT it is asserting about rather than
	// polling a counter on a timer.
	drove chan struct{}
}

func newPublishingSession() *publishingSession {
	id, err := uuid.New()
	if err != nil {
		panic(err)
	}
	return &publishingSession{
		id:        id,
		published: make(chan sessionwire.EnduringPublication),
		stopped:   make(chan struct{}),
		drove:     make(chan struct{}, 1),
		epoch:     1,
		held:      true,
	}
}

func (s *publishingSession) ID() uuid.UUID                      { return s.id }
func (s *publishingSession) WaitIdle(ctx context.Context) error { return ctx.Err() }
func (s *publishingSession) Done() <-chan struct{}              { return s.stopped }

func (s *publishingSession) ReleaseResidency(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released++
	return nil
}

func (s *publishingSession) Released() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released
}

// SubscribeCommitted hands back the live tail and records the resume point it
// was asked for. The recorded point is the evidence for the exact-join claim:
// Host never positions a live subscription.
func (s *publishingSession) SubscribeCommitted(_ context.Context, from sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	s.mu.Lock()
	s.subscribes = append(s.subscribes, from)
	s.mu.Unlock()
	return s.published, nil
}

func (s *publishingSession) resumePoints() []sessionwire.EventID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sessionwire.EventID(nil), s.subscribes...)
}

// ApplyCommand records the command and, when this session was given a durable
// store, commits the public effect a terminal application is settled on. A
// runtime that returned without committing anything leaves the command
// unapplied, which is the applier's own rule and not a shortcut taken here.
func (s *publishingSession) ApplyCommand(_ context.Context, command department.RuntimeCommand) error {
	s.mu.Lock()
	s.applied = append(s.applied, command)
	count := uint64(len(s.applied))
	effects := s.effects
	s.mu.Unlock()
	if effects != nil {
		effects.commitEffect(command.CommandID, count)
	}
	select {
	case s.drove <- struct{}{}:
	default:
	}
	return nil
}

func (s *publishingSession) appliedCommands() []department.RuntimeCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]department.RuntimeCommand(nil), s.applied...)
}

func (s *publishingSession) LeaseEpoch() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.held
}

// commit hands one committed event to the relay. It blocks until the relay has
// taken it, which is what makes "in order" an assertion about the relay rather
// than about a buffer.
func (s *publishingSession) commit(t *testing.T, publication sessionwire.EnduringPublication) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	select {
	case s.published <- publication:
	case <-deadline.C:
		t.Fatalf("the relay never took the publication at seq %d", publication.JournalSeq)
	}
}

// enduring builds one valid committed publication. CoveredThrough equals
// JournalSeq because Core refuses anything else on a live public event.
func enduring(key registry.Key, seq uint64, body string) sessionwire.EnduringPublication {
	return sessionwire.EnduringPublication{
		TenantID:       key.TenantID,
		SessionID:      key.SessionID,
		EventID:        sessionwire.EventID("event-" + strconv.FormatUint(seq, 10)),
		JournalSeq:     seq,
		CoveredThrough: seq,
		Body:           json.RawMessage(body),
	}
}

// ---------------------------------------------------------------------------
// A durable command inbox, faithful to the seams it satisfies
// ---------------------------------------------------------------------------

// durableCommands is the SessionInbox half of §10.4, as a test double: the
// ordered inbox, the private record, the journal correlation, and the four
// compare-and-swap transitions, over one map.
//
// IT IS AT LEAST AS STRICT AS THE SEAMS IT STANDS IN FOR, which is the rule
// O7.1 broke in the other direction with a fake authenticator that discarded
// the tenant it was asked about. ListOrdered honours afterOrder and limit
// rather than returning everything; every transition refuses a revision that is
// not the record's current one; and FindApplication answers from what has
// actually been written — absent before a prefix, unresolved after a prefix
// with no effect, committed only once the runtime committed one.
//
// THE IDEMPOTENCE UNDER TEST IS NOT THIS TYPE'S. Nothing here deduplicates by
// CommandID. A command is consumed once because Host advances a durable cursor
// past its immutable acceptance order and asks for records STRICTLY AFTER it;
// a double-application would show up here as a second ApplyCommand on the
// runtime, which is what the test asserts on.
type durableCommands struct {
	key registry.Key

	mu      sync.Mutex
	records map[sessionwire.CommandID]*durableRecord
	order   []sessionwire.CommandID
	// cursor and cursorEpoch are the two high-water marks
	// SaveDispositionCommandCursor keeps, and NEITHER EVER FALLS. Modelling
	// only the order — which this double did — makes the epoch fence
	// unrepresentable and every test over it vacuous.
	cursor      uint64
	cursorEpoch uint64
	lists       int
	// payloadLoads counts private-body reads. See payloadLoadCount.
	payloadLoads int
}

// durableRecord is one inbox record and everything the journal proves about it.
type durableRecord struct {
	command commands.Command
	runtime uuid.UUID
	kind    commands.Kind
	payload []byte

	revision  uint64
	state     commands.State
	deadline  time.Time
	prefixed  bool
	effectID  sessionwire.EventID
	effectSeq uint64
}

func newDurableCommands(key registry.Key) *durableCommands {
	return &durableCommands{key: key, records: map[sessionwire.CommandID]*durableRecord{}}
}

// accept admits one command at the next acceptance order and returns its ID.
func (d *durableCommands) accept(t *testing.T, id sessionwire.CommandID, payload string) sessionwire.CommandID {
	t.Helper()
	runtimeID, err := uuid.New()
	if err != nil {
		t.Fatalf("mint a runtime command id: %v", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	order := uint64(len(d.order) + 1)
	d.order = append(d.order, id)
	d.records[id] = &durableRecord{
		command: commands.Command{
			TenantID:      d.key.TenantID,
			SessionID:     d.key.SessionID,
			CommandID:     id,
			AcceptedOrder: order,
			State:         commands.StatePending,
		},
		runtime:  runtimeID,
		kind:     commands.KindInput,
		payload:  []byte(payload),
		revision: 1,
		state:    commands.StatePending,
		// The deadline is far beyond the fixture's frozen instant, so no claim
		// here is refused for being late.
		deadline: time.Unix(1_800_000_000, 0).UTC(),
	}
	return id
}

// settle drives one record to a terminal state OUT OF BAND, as a predecessor
// Host or Factory's deadline reconciler would leave it.
//
// It is the fixture's way of producing the one record a Host that does not
// dispatch can still consume, which is what makes "the consumer is alive and
// refuses only the dispatch" a falsifiable claim rather than a sentence.
func (d *durableCommands) settle(id sessionwire.CommandID, state commands.State) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record := d.records[id]
	record.state = state
	record.revision++
}

// consumedCursor is the durable cursor's current value.
func (d *durableCommands) consumedCursor() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cursor
}

// ListOrdered answers STRICTLY AFTER afterOrder, bounded by limit.
func (d *durableCommands) ListOrdered(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, afterOrder uint64, limit int) ([]commands.Command, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lists++
	var page []commands.Command
	for _, id := range d.order {
		record := d.records[id]
		if record.command.TenantID != tenant || record.command.SessionID != session {
			continue
		}
		if record.command.AcceptedOrder <= afterOrder {
			continue
		}
		next := record.command
		next.State = record.state
		page = append(page, next)
		if len(page) == limit {
			break
		}
	}
	return page, nil
}

func (d *durableCommands) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cursor, nil
}

// SaveCursor takes the EPOCH FIRST AND THE ORDER SECOND, which is the seam's
// order and not the obvious one: commands.CursorWrites declares
// SaveCursor(ctx, tenant, session, epoch, order) and both are uint64, so the
// compiler cannot catch a transposition. This double had them the other way
// round and recorded the residency epoch (9) as the consumed order, which
// silently skipped every command below order 9 — see the finding recorded
// against fixture_test.go's fakeCursors, which still had the same transposition
// when this was written.
//
// IT NOW APPLIES BOTH FENCES, IN THE STORE'S ORDER, and the two arms were added
// because the differential in inbox_differential_test.go found them missing.
// Before that this method accepted everything: a position behind the committed
// one was a silent no-op where sessionstore refuses with InboxErrorOrder, and
// there was NO EPOCH FENCE AT ALL, so a save from a superseded lease was
// accepted and walked the committed mark FORWARD to the epoch's own value. A
// double that cannot produce its dependency's refusals is looser than the
// dependency, and the composed test that ran against it established nothing
// about either property.
//
// THE EPOCH IS FENCED BEFORE THE ORDER, which is the store's order and is
// load-bearing for exactly one caller: the one below BOTH marks. Epoch-first
// tells it "you have lost the session", which is terminal and correct;
// order-first would tell it "your position is stale", which invites it to fetch
// newer data and retry forever against a session it no longer owns.
func (d *durableCommands) SaveCursor(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, epoch uint64, order uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if epoch < d.cursorEpoch {
		return fmt.Errorf("%w: epoch %d is below the committed %d", errStaleDoubleSave, epoch, d.cursorEpoch)
	}
	if order < d.cursor {
		return fmt.Errorf("%w: order %d is below the committed %d", errStaleDoubleSave, order, d.cursor)
	}
	d.cursorEpoch, d.cursor = epoch, order
	return nil
}

func (d *durableCommands) LoadCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID) (commands.Record, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, held := d.records[id]
	if !held {
		return commands.Record{}, errors.New("durableCommands: no such command")
	}
	return commands.Record{
		TenantID:         record.command.TenantID,
		SessionID:        record.command.SessionID,
		CommandID:        id,
		RuntimeCommandID: record.runtime,
		Kind:             record.kind,
		State:            record.state,
		AcceptedOrder:    record.command.AcceptedOrder,
		Revision:         record.revision,
		ApplyDeadline:    record.deadline,
	}, nil
}

func (d *durableCommands) LoadPayload(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID) (commands.Payload, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.payloadLoads++
	record, held := d.records[id]
	if !held {
		return commands.Payload{}, errors.New("durableCommands: no such command")
	}
	return commands.Payload{Body: append([]byte(nil), record.payload...)}, nil
}

// payloadLoadCount reports how many times Host asked this store for a private
// command body.
//
// IT IS THE MEASUREMENT BEHIND A CLAIM THAT WOULD OTHERWISE BE STRUCTURAL. A
// composed Host runs commands.NoDispatch, and the only production reader of a
// payload is commands.Applier, which has no production call site — so "the
// private body did not cross HostLink" is true because the body never enters the
// process. That is a strong property and a weak assertion: it cannot fail for
// any implementation of the code under test. This counter turns the premise into
// something a run observes, and it fires the moment an applier is wired back in.
func (d *durableCommands) payloadLoadCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.payloadLoads
}

// storedPayload reads a record's private body WITHOUT counting the read, so a
// test can establish that the store really holds the secret without spending the
// measurement above.
func (d *durableCommands) storedPayload(id sessionwire.CommandID) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, held := d.records[id]
	if !held {
		return nil
	}
	return append([]byte(nil), record.payload...)
}

func (d *durableCommands) FindApplication(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID) (commands.Application, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, held := d.records[id]
	if !held {
		return commands.Application{}, errors.New("durableCommands: no such command")
	}
	application := commands.Application{CommandID: id, RuntimeCommandID: record.runtime, Outcome: commands.ApplicationAbsent}
	switch {
	case record.effectSeq != 0:
		application.Outcome = commands.ApplicationCommitted
		application.PrefixEpoch = 1
		application.EffectEventID = record.effectID
		application.EffectSeq = record.effectSeq
	case record.prefixed:
		application.Outcome = commands.ApplicationUnresolved
		application.PrefixEpoch = 1
	}
	return application, nil
}

func (d *durableCommands) LoadGate(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.GateID) (commands.Gate, bool, error) {
	return commands.Gate{}, false, nil
}

// cas advances one record's state under a revision check.
func (d *durableCommands) cas(id sessionwire.CommandID, expected uint64, from []commands.State, to commands.State) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, held := d.records[id]
	if !held {
		return 0, errors.New("durableCommands: no such command")
	}
	if record.revision != expected {
		return 0, errors.New("durableCommands: revision conflict")
	}
	if !slices.Contains(from, record.state) {
		return 0, errors.New("durableCommands: " + string(record.state) + " is not a legal predecessor of " + string(to))
	}
	record.state = to
	record.revision++
	return record.revision, nil
}

func (d *durableCommands) ClaimCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID, claim commands.Claim) (uint64, error) {
	return d.cas(id, claim.ExpectedRevision, []commands.State{commands.StatePending, commands.StateClaimed}, commands.StateClaimed)
}

func (d *durableCommands) BeginApplying(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID, applying commands.Applying) (uint64, error) {
	return d.cas(id, applying.ExpectedRevision, []commands.State{commands.StateClaimed}, commands.StateApplying)
}

func (d *durableCommands) CompleteCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID, completion commands.Completion) error {
	_, err := d.cas(id, completion.ExpectedRevision, []commands.State{commands.StateApplying}, commands.StateApplied)
	return err
}

func (d *durableCommands) RejectCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, id sessionwire.CommandID, rejection commands.Rejection) error {
	_, err := d.cas(id, rejection.ExpectedRevision, []commands.State{commands.StatePending, commands.StateClaimed, commands.StateApplying}, commands.StateRejected)
	return err
}

// commitEffect is the runtime's side: the public event that carried the effect.
func (d *durableCommands) commitEffect(id sessionwire.CommandID, seq uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if record, held := d.records[id]; held {
		record.effectID = sessionwire.EventID("effect-" + string(id))
		record.effectSeq = seq
	}
}

func (d *durableCommands) stateOf(id sessionwire.CommandID) commands.State {
	d.mu.Lock()
	defer d.mu.Unlock()
	if record, held := d.records[id]; held {
		return record.state
	}
	return ""
}

// ---------------------------------------------------------------------------
// 1. Concurrent create: one launch, one grant, one consumer, no corpse
// ---------------------------------------------------------------------------

// TestConcurrentCreateLaunchesOneSessionAndStartsOneConsumer is the composed
// form of Harness's TestServerHandleRestoreConcurrentColdRestoreYieldsToTheWinner
// and TestServerHandleRestoreRaceLoserStartsNoWatcher (pkg/serve,
// handlers_lifecycle_test.go).
//
// WHAT THE COMPOSITION ADDS TO internal/residency'S OWN RACE TESTS is the object
// that package cannot see. An attach starts a COMMAND CONSUMER at its last step,
// and that is the composition's: the residency manager is handed an ownership
// seam and has no idea a consumer exists. A second consumer for one session is a
// second reader of one inbox advancing one durable cursor, and it does not show
// up in a registry count.
//
// IT USED TO COUNT JOURNAL GRANTS TOO, and that half is gone with the attach's
// journal fence rather than merely unasserted: a composed Host opens no journal
// writer, so there is no second grant to race for.
//
// THE MEASURED SHAPE, WHICH IS NOT THE SHAPE HARNESS HAS. Harness lets N cold
// restores race and cleans up the N-1 losers. Host does not produce losers on
// this path at all: Manager.Attach takes a per-key slot and RE-READS the
// attached predicate inside it, so concurrent cold attaches become one launch
// and N-1 idempotent hits. This test was first written to assert "every loser
// released its grant" and the probe measured ONE opening on every run of five,
// with no barrier, which is why it now asserts the stronger property the code
// actually holds. An assertion about cleaning up losers would have been a
// sentence wider than its probe, and would have passed forever against a build
// that had losers and leaked them.
//
// THE COUNTING INSTRUMENT HAS A POSITIVE CONTROL. "Exactly one opening" is a
// claim that can pass because the counter is broken, so a second session is
// attached concurrently in the same fixture and the same counter reports its
// opening separately. A counter that always answered one would fail there.
func TestConcurrentCreateLaunchesOneSessionAndStartsOneConsumer(t *testing.T) {
	const racers = 8
	f := newFixture(t)
	f.start()

	type outcome struct {
		held residency.Residency
		err  error
	}
	// A CLOSED CHANNEL, not a sleep. Every racer parks on the same receive and
	// one close releases all of them, so the race is real on every run rather
	// than on the runs where a sleep happened to be long enough.
	start := make(chan struct{})
	var parked, done sync.WaitGroup
	parked.Add(racers)
	done.Add(racers)
	results := make([]outcome, racers)
	for index := range racers {
		go func() {
			defer done.Done()
			parked.Done()
			<-start
			held, err := f.svc.Attach(context.Background(), residency.Request{
				TenantID:  tenantA,
				SessionID: sessionA,
				AgentID:   testAgent,
				Mode:      residency.ModeCreate,
				Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
			})
			results[index] = outcome{held: held, err: err}
		}()
	}
	parked.Wait()
	close(start)
	done.Wait()

	// (a) EVERY CALLER GOT THE SAME RESIDENCY. A racer handed a different
	// generation or a different runtime has been handed a corpse: an object
	// whose ReleaseResidency some other racer already called.
	generations := map[uint64]int{}
	attached := 0
	for index, answer := range results {
		if answer.err != nil {
			t.Fatalf("racer %d failed; concurrent legal creates settle on a winner rather than erroring: %v", index, answer.err)
		}
		generations[answer.held.Generation]++
		if answer.held.Attached {
			attached++
		}
		if answer.held.Runtime != results[0].held.Runtime {
			t.Errorf("racer %d was handed a different runtime from racer 0", index)
		}
	}
	if len(generations) != 1 {
		t.Errorf("the racers reported %d distinct generations %v, want one residency", len(generations), generations)
	}
	if attached != 1 {
		t.Errorf("%d of %d racers reported Attached true, want exactly one establishing call", attached, racers)
	}

	// (b) ONE OF EVERYTHING THE COMPOSITION OWNS.
	if launches := f.rig.Launches(); launches != 1 {
		t.Errorf("the rig was asked to launch %d times for one cold session, want 1", launches)
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 1 {
		t.Errorf("the registry holds %d residencies, want 1", len(entries))
	}
	f.svc.mu.Lock()
	consumers, sessions := len(f.svc.consumers), len(f.svc.sessions)
	f.svc.mu.Unlock()
	if consumers != 1 {
		t.Errorf("%d command consumers are registered for one session, want 1", consumers)
	}
	if sessions != 1 {
		t.Errorf("the composition tracks %d resident sessions, want 1", sessions)
	}

	// (c) THE CONTROL FOR (b). The counters are asked about a SECOND session, so
	// a count that answered "one" regardless would fail here. It also shows the
	// per-key slot is per KEY: a second session is not serialized behind the
	// first.
	other := sessionwire.SessionID("session-control")
	if _, err := f.svc.Attach(context.Background(), residency.Request{
		TenantID:  tenantA,
		SessionID: other,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	}); err != nil {
		t.Fatalf("the control attach failed: %v", err)
	}
	f.svc.mu.Lock()
	consumers, sessions = len(f.svc.consumers), len(f.svc.sessions)
	f.svc.mu.Unlock()
	if consumers != 2 || sessions != 2 {
		t.Fatalf("after an unrelated second attach the composition holds %d consumers and %d sessions, want 2 and 2; the counts in (b) cannot distinguish one from any other number", consumers, sessions)
	}
	if _, running := f.svc.ConsumerFor(registry.Key{TenantID: tenantA, SessionID: other}); !running {
		t.Fatalf("the control session started no consumer of its own")
	}
}

// ---------------------------------------------------------------------------
// 2. The session outlives the request that made it and the link that binds it
// ---------------------------------------------------------------------------

// TestTheResidentSessionOutlivesTheAttachRequestAndItsFactoryLink is the
// composed form of Harness's TestSessionOutlivesCreatingRequest (pkg/serve,
// session_lifetime_test.go).
//
// Harness's claim is about ONE lifetime: the HTTP request that created a
// session must not own it. Host has TWO, because a Host session is also reached
// over a long-lived link that a Factory replica may drop at any moment, and the
// second is the one no unit test in this module can make —
// internal/residency's TestTheSessionAndItsRuntimeOutliveTheRequest holds the
// first, and has no transport.
//
// THE PROBE IS POSITIVE AND THE CONTROL IS THE STOP. "Still resident" is
// asserted by driving the session — a fresh link binds it and receives a
// publication — rather than by reading a map, and the control at the end stops
// the Host and shows the same probe then fails, so the assertions above are not
// statements a dead Host would also satisfy.
func TestTheResidentSessionOutlivesTheAttachRequestAndItsFactoryLink(t *testing.T) {
	runtime := newPublishingSession()
	f := newFixture(t)
	f.rig.Session = runtime
	f.start()
	server := f.serve()

	attachCtx, cancelAttach := context.WithCancel(context.Background())
	if _, err := f.svc.Attach(attachCtx, residency.Request{
		TenantID:  tenantA,
		SessionID: sessionA,
		AgentID:   testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	}); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// (a) THE CREATING REQUEST ENDS. Everything below happens under a cancelled
	// attach context.
	cancelAttach()

	channel := hostlink.ChannelFor(keyA)
	first := dialFactoryLink(t, server.URL, tenantA)
	accepted(t, first.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))
	if reply := first.subscribe(3, channel); reply.Error != nil {
		t.Fatalf("subscribe on a bound channel was refused: %#v", *reply.Error)
	}
	runtime.commit(t, enduring(keyA, 1, `{"kind":"first"}`))
	if got := first.awaitPushes(1); got[0].Channel != channel {
		t.Fatalf("publication arrived on %q, want %q", got[0].Channel, channel)
	}

	// (b) THE LINK DROPS. A Factory replica disconnecting is ordinary, and the
	// session is Host's, not the link's.
	first.close()

	second := dialFactoryLink(t, server.URL, tenantA)
	accepted(t, second.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))
	if reply := second.subscribe(3, channel); reply.Error != nil {
		t.Fatalf("the rebinding link was refused a subscription: %#v", *reply.Error)
	}
	runtime.commit(t, enduring(keyA, 2, `{"kind":"second"}`))
	arrived := second.awaitPushes(1)
	var republished sessionwire.EnduringPublication
	if err := json.Unmarshal(arrived[0].Data, &republished); err != nil {
		t.Fatalf("the frame after a reconnect is not a Core publication: %v", err)
	}
	if republished.JournalSeq != 2 {
		t.Fatalf("the rebound link received seq %d, want the event committed after the reconnect", republished.JournalSeq)
	}
	if got := runtime.Released(); got != 0 {
		t.Fatalf("the runtime was released %d times by a request ending and a link dropping, want 0", got)
	}

	// (c) THE CONTROL. Stop the Host and the same probe fails, so "a bind
	// succeeded" above was a statement about a live session.
	if _, err := f.svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := runtime.Released(); got == 0 {
		t.Fatal("the control released nothing; Stop is meant to release the session the assertions above relied on being held")
	}
	if entries := f.svc.registry.Snapshot(); len(entries) != 0 {
		t.Fatalf("the registry still holds %d residencies after Stop, so the probe above cannot distinguish a live session from a dead one", len(entries))
	}
}

// ---------------------------------------------------------------------------
// 3. Events: once, in commit order, carrying what a client-side join needs
// ---------------------------------------------------------------------------

// TestEveryCommittedEventCrossesTheLinkOnceInCommitOrderCarryingItsJoinIdentities
// is the composed form of three Harness behaviours at once: the event stream
// (pkg/serve, TestHandleEventsStreamsEnduring), the ordering claim
// (TestEveryCommittedEventIsPublishedOnceAndInOrder, which internal/service
// already holds against a REAL harness session and a real durable read), and
// the exact join (pkg/serve, join_test.go TestClientSideExactJoin).
//
// HOST HAS NO SERVER-SIDE JOIN AND MUST NOT GROW ONE. Harness's exact join is a
// CLIENT-side property: a consumer reads its durable tail, then joins the live
// stream, and deduplicates on (journal_seq, event_id) with byte-identical
// bodies. Host preserves that behaviour not by implementing a join but by
// carrying the three things it needs — the committed EventID, the JournalSeq it
// was appended at, and the committed body unchanged — and by refusing to
// position a live subscription at all. The last of those is asserted on the
// resume point the runtime was actually asked for, which is the only place it
// is observable; internal/service documents the refusal and this is the
// composed evidence that the composition never supplies one.
//
// THE ASSERTION IS ON BYTES. Two JSON documents that decode alike differ in key
// order, and a consumer joining a durable tail to this stream compares bodies.
func TestEveryCommittedEventCrossesTheLinkOnceInCommitOrderCarryingItsJoinIdentities(t *testing.T) {
	runtime := newPublishingSession()
	f := newFixture(t)
	f.rig.Session = runtime
	f.start()
	server := f.serve()
	f.attach(tenantA, sessionA)

	channel := hostlink.ChannelFor(keyA)
	link := dialFactoryLink(t, server.URL, tenantA)
	accepted(t, link.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))
	if reply := link.subscribe(3, channel); reply.Error != nil {
		t.Fatalf("subscribe on a bound channel was refused: %#v", *reply.Error)
	}

	const events = 5
	bodies := make([]string, 0, events)
	for seq := uint64(1); seq <= events; seq++ {
		// Distinct key ORDER as well as distinct content, so a body that was
		// decoded and re-encoded anywhere on the path is visible as a
		// difference rather than as an equal decode.
		body := `{"seq":` + strconv.FormatUint(seq, 10) + `,"a":"x","kind":"message"}`
		bodies = append(bodies, body)
		runtime.commit(t, enduring(keyA, seq, body))
	}

	arrived := link.awaitPushes(events)
	if len(arrived) != events {
		t.Fatalf("%d publications crossed the link, want exactly %d", len(arrived), events)
	}
	for index, frame := range arrived {
		if frame.Channel != channel {
			t.Fatalf("publication %d arrived on %q, want the session's channel %q", index, frame.Channel, channel)
		}
		var published sessionwire.EnduringPublication
		if err := json.Unmarshal(frame.Data, &published); err != nil {
			t.Fatalf("publication %d is not a Core record: %v", index, err)
		}
		want := uint64(index + 1)
		if published.JournalSeq != want {
			t.Fatalf("publication %d carries seq %d, want %d; the stream is out of commit order", index, published.JournalSeq, want)
		}
		// THE THREE THINGS A CLIENT-SIDE EXACT JOIN NEEDS.
		if published.EventID != sessionwire.EventID("event-"+strconv.FormatUint(want, 10)) {
			t.Errorf("publication %d carries EventID %q, want the committed identity", index, published.EventID)
		}
		if published.CoveredThrough != published.JournalSeq {
			t.Errorf("publication %d covers through %d at seq %d; a live public event covers exactly the sequence it committed at", index, published.CoveredThrough, published.JournalSeq)
		}
		if string(published.Body) != bodies[index] {
			t.Errorf("publication %d body\n  %s\nis not the committed bytes\n  %s", index, published.Body, bodies[index])
		}
		if published.TenantID != tenantA || published.SessionID != sessionA {
			t.Errorf("publication %d names %q/%q, want the admitted pair", index, published.TenantID, published.SessionID)
		}
	}

	// THE LIVE SUBSCRIPTION IS NEVER POSITIONED. A Host that resumed a live
	// stream from a client-supplied point would be answering the durable read's
	// question on the live plane, which is the read plane this Host does not
	// serve.
	points := runtime.resumePoints()
	if len(points) != 1 {
		t.Fatalf("the runtime was subscribed %d times for one residency, want once", len(points))
	}
	if points[0] != "" {
		t.Errorf("the live subscription was opened at resume point %q, want the empty live tail", points[0])
	}

	// THE CONTROLS, and there are two because the ordering probe has two ways
	// to pass without meaning anything.
	//
	// (i) A relay that re-published the NEWEST event on every delivery produces
	// five frames and satisfies a count. The bodies are therefore checked for
	// pairwise distinctness, on both sides: five distinct things were committed
	// and five distinct things arrived.
	committedBodies := map[string]int{}
	arrivedBodies := map[string]int{}
	for index, frame := range arrived {
		committedBodies[bodies[index]]++
		var published sessionwire.EnduringPublication
		if err := json.Unmarshal(frame.Data, &published); err != nil {
			t.Fatalf("publication %d is not a Core record: %v", index, err)
		}
		arrivedBodies[string(published.Body)]++
	}
	if len(committedBodies) != events {
		t.Fatalf("the fixture committed %d distinct bodies for %d events; the byte-identity assertions above cannot tell one event from another", len(committedBodies), events)
	}
	if len(arrivedBodies) != events {
		t.Fatalf("%d distinct bodies arrived for %d events %v; the stream repeated an event", len(arrivedBodies), events, arrivedBodies)
	}

	// (ii) THE COUNT IS NOT A FIXED FIXTURE. A sixth event arrives sixth, so
	// "exactly five" above was a property of what was committed rather than a
	// ceiling the probe imposes.
	sixth := `{"seq":6,"a":"x","kind":"message"}`
	runtime.commit(t, enduring(keyA, events+1, sixth))
	continued := link.awaitPushes(events + 1)
	var last sessionwire.EnduringPublication
	if err := json.Unmarshal(continued[events].Data, &last); err != nil {
		t.Fatalf("the sixth publication is not a Core record: %v", err)
	}
	if last.JournalSeq != events+1 || string(last.Body) != sixth {
		t.Fatalf("the sixth publication is seq %d body %s, want seq %d body %s", last.JournalSeq, last.Body, events+1, sixth)
	}
}

// ---------------------------------------------------------------------------
// 4 and 5. Commands: idempotent through the inbox, and private end to end
// ---------------------------------------------------------------------------

// commandFixture is one started Host with a real durable command inbox, a
// runtime that records what it was driven with, and a served HostLink endpoint.
type commandFixture struct {
	*fixture
	store   *durableCommands
	runtime *publishingSession
	server  *httptest.Server
}

// newCommandFixture wires durableCommands into the composition's command seams.
//
// IT USED TO WIRE SIX AND NOW WIRES TWO. Inbox and Cursors are the consumption
// half and are what a composed Host reads; Records, Applications, Gates and
// InboxWrites were the APPLICATION half, and the composition no longer declares
// them — see commands.NoDispatch. The double still implements all six, because
// its own job is to be at least as strict as the seams it stands in for and the
// four unwired ones are what the attempt-aware applier will be measured against.
func newCommandFixture(t *testing.T) *commandFixture {
	t.Helper()
	store := newDurableCommands(keyA)
	runtime := newPublishingSession()
	runtime.effects = store

	f := newFixture(t, func(o *Options, _ *host.Options) {
		o.Inbox = store
		o.Cursors = store
	})
	f.rig.Session = runtime
	f.start()
	server := f.serve()
	f.attach(tenantA, sessionA)
	return &commandFixture{fixture: f, store: store, runtime: runtime, server: server}
}

// awaitBlocked blocks until this session's consumer has stopped at one command
// and reports what it stopped on.
//
// IT READS THE CONSUMER'S OWN LAST PASS rather than inferring a block from an
// absence. "The runtime was never driven" is also true of a consumer that was
// never started, of one that listed nothing, and of a link that never delivered;
// the pass result distinguishes all four, because it names the command the pass
// examined and the cause it stopped for.
func (c *commandFixture) awaitBlocked(command sessionwire.CommandID) commands.BlockedCommand {
	c.t.Helper()
	c.svc.mu.Lock()
	consumer := c.svc.consumers[keyA]
	c.svc.mu.Unlock()
	if consumer == nil {
		c.t.Fatal("the attached session has no consumer at all")
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if blocked := consumer.LastPass().Blocked; blocked != nil && blocked.CommandID == command {
			return *blocked
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			c.t.Fatalf("the consumer never blocked on %q; its last pass was %+v", command, consumer.LastPass())
			return commands.BlockedCommand{}
		}
	}
}

// awaitCursor blocks until the durable consumption cursor reaches want.
func (c *commandFixture) awaitCursor(want uint64) {
	c.t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if got := c.store.consumedCursor(); got >= want {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			c.t.Fatalf("the durable cursor is %d, want at least %d", c.store.consumedCursor(), want)
			return
		}
	}
}

// TestAComposedHostConsumesADeliveryAndRefusesToDispatchIt is the HARD BOUNDARY
// measured in composition, and it replaces a test that asserted the opposite.
//
// WHAT IT REPLACED AND WHY. TestADuplicateCommandDeliveryAppliesTheCommandOnce
// drove a whole apply through this composition — claim, payload load, journal
// prefix, runtime, terminal settlement — against a double of the LEGACY command
// family. A Host cannot reach that family on a session it can hold, so the
// behaviour it measured was one no deployment could ever produce. The
// idempotency claim underneath it is unchanged and is not this test's: a second
// delivery of a CommandID whose order the cursor has passed cannot be listed at
// all, which is the consumption mechanism and is measured against the RELEASED
// store in inbox_differential_test.go.
//
// THE PROPERTY HERE IS THAT A DELIVERED COMMAND REACHES THE CONSUMER AND STOPS.
// A Host running commands.NoDispatch lists the command, refuses it by name
// before touching any seam, and blocks the pass — leaving the durable record
// exactly where Factory put it, so the Host that comes after can still apply it.
//
// TWO POSITIVE CONTROLS, because every assertion below is an absence.
//
//	(1) the runtime CAN be driven and the counter CAN see it, shown by driving
//	    it directly through the same instrument the absence is read from;
//	(2) the consumer IS consuming, shown by settling the command terminally out
//	    of band and watching the durable cursor advance past it — which also
//	    shows the refusal is a decision about DISPATCH and not a consumer that
//	    is wedged, asleep or never started.
func TestAComposedHostConsumesADeliveryAndRefusesToDispatchIt(t *testing.T) {
	c := newCommandFixture(t)

	first := c.store.accept(t, "command-one", `{"blocks":[{"text":"hello"}]}`)
	channel := hostlink.ChannelFor(keyA)
	link := dialFactoryLink(t, c.server.URL, tenantA)
	accepted(t, link.rpc(2, hostlink.MethodBind, c.bindRequest(tenantA, sessionA, 9)))
	accepted(t, link.rpc(3, channel, sessionwire.HostLinkCommandDelivery{CommandID: first}))

	blocked := c.awaitBlocked(first)

	// (a) THE REFUSAL IS THE NAMED ONE. An untyped error here would be
	// indistinguishable from a store that was simply unreachable, and the whole
	// point of a typed refusal is that a later reader can tell a boundary from
	// an outage.
	var refusal *commands.ApplyError
	if !errors.As(blocked.Cause, &refusal) {
		t.Fatalf("the pass blocked with %T (%v), want a *commands.ApplyError", blocked.Cause, blocked.Cause)
	}
	if refusal.Refusal != commands.RefusalDispatchUnavailable {
		t.Errorf("the pass blocked with refusal %q, want %q", refusal.Refusal, commands.RefusalDispatchUnavailable)
	}
	if refusal.CommandID != first {
		t.Errorf("the refusal names command %q, want %q", refusal.CommandID, first)
	}

	// (b) NOTHING DURABLE MOVED. A refusal that had already claimed the record,
	// or moved it to applying, would be the exact state this boundary exists to
	// prevent: an effect nobody can settle.
	if got := c.store.stateOf(first); got != commands.StatePending {
		t.Errorf("the durable record is %q, want %q; the refusal moved a record it must leave alone", got, commands.StatePending)
	}
	if cursor := c.store.consumedCursor(); cursor != 0 {
		t.Errorf("the durable cursor advanced to %d past a command that was never applied", cursor)
	}

	// (c) THE RUNTIME WAS NEVER DRIVEN.
	if driven := c.runtime.appliedCommands(); len(driven) != 0 {
		t.Fatalf("the runtime was driven %d times by a Host that does not dispatch: %+v", len(driven), driven)
	}

	// (d) CONTROL 1. The instrument in (c) can see a drive.
	if err := c.runtime.ApplyCommand(context.Background(), department.RuntimeCommand{
		CommandID: "command-control",
		Kind:      string(commands.KindInput),
		Payload:   []byte(`{"blocks":[{"text":"control"}]}`),
	}); err != nil {
		t.Fatalf("driving the runtime directly: %v", err)
	}
	if driven := c.runtime.appliedCommands(); len(driven) != 1 {
		t.Fatalf("the runtime reports %d applications after being driven once; the absence in (c) is unfalsifiable", len(driven))
	}

	// (e) CONTROL 2. The consumer is consuming. The command is settled
	// terminally by somebody else — a predecessor Host, or Factory's deadline
	// reconciler — and this Host steps over it and advances its durable cursor,
	// which is the half of the lifecycle that is fully live.
	c.store.settle(first, commands.StateRejected)
	c.svc.Wake(keyA)
	c.awaitCursor(1)
}

// TestNoPrivatePayloadCrossesHostLinkInEitherDirection is the composed form of
// Harness's privacy behaviour (pkg/serve, privacy_visibility_test.go:
// TestReadHandlersRejectNonPublicEventsBeforeSuccess,
// TestHandleEventsSkipsNonPublicLiveDeliveries,
// TestEncodeDeliveryRejectsNonPublicVisibility).
//
// HARNESS FILTERS AND HOST STRUCTURALLY CANNOT CARRY IT. Harness's live plane
// serves both public and private material and refuses the private half at the
// encoder. Host's HostLink carries IDENTIFIERS: a command delivery is a
// CommandID and nothing else. So the property re-established here is stronger
// and simpler than a filter — the secret is never on the wire at all — and it is
// asserted on BYTES, over every frame the link was ever sent, because a filter
// that decoded and re-encoded could reintroduce it in a shape a struct
// comparison would not see.
//
// THE TWO HALVES ARE NOT HELD THE SAME WAY, AND SAYING SO IS THE POINT. This
// comment used to end "the private body is loaded from SessionStore AFTER the
// claim, on Host's own side, and handed to the runtime". A composed Host does
// none of that: commands.Command carries no payload, the only production reader
// of one is commands.Applier, and NewApplier has no production call site. The
// body therefore never enters the process, and the OUTBOUND assertion (c) cannot
// fail for any implementation of the code under test — it is held by
// CONSTRUCTION. It is kept as a forward guard against a Host that loads payloads
// again, and the construction itself is MEASURED by (a2) rather than asserted:
// the store counts private-body reads and the count must be zero. The INBOUND
// half (d) is a live assertion and fails closed today.
//
// THE INBOUND HALF MATTERS TOO. A Factory that could smuggle a body into a
// delivery would bypass the durable record the applier reads, so a delivery
// carrying an extra member is refused by Core's own strict decoding rather than
// tolerated.
//
// THE CONTROL PROVES THE SCANNER CAN SEE. A marker placed in the public body of
// a committed event does cross the wire and is found by the same scan, so "the
// secret was not found" is a statement about the secret and not about a scanner
// that looks at nothing.
func TestNoPrivatePayloadCrossesHostLinkInEitherDirection(t *testing.T) {
	const secret = "PRIVATE-PAYLOAD-9f3c1a-DO-NOT-PUT-ME-ON-THE-WIRE"
	const publicMarker = "PUBLIC-MARKER-4d2e77"

	c := newCommandFixture(t)
	command := c.store.accept(t, "command-private", `{"blocks":[{"text":"`+secret+`"}]}`)

	channel := hostlink.ChannelFor(keyA)
	link := dialFactoryLink(t, c.server.URL, tenantA)
	accepted(t, link.rpc(2, hostlink.MethodBind, c.bindRequest(tenantA, sessionA, 9)))
	if reply := link.subscribe(3, channel); reply.Error != nil {
		t.Fatalf("subscribe on a bound channel was refused: %#v", *reply.Error)
	}
	accepted(t, link.rpc(4, channel, sessionwire.HostLinkCommandDelivery{CommandID: command}))

	// (a) THE HOST DID REACH THE COMMAND. Without this the privacy claim below
	// is satisfied by a Host that listed nothing, consumed nothing and had no
	// opportunity to leak anything.
	//
	// IT USED TO ASSERT THE RUNTIME WAS DRIVEN WITH THE SECRET, which a composed
	// Host no longer does — see
	// TestAComposedHostConsumesADeliveryAndRefusesToDispatchIt. That is a
	// WEAKENING of (c) and the header says so; what is asserted instead is that
	// the Host reached this command at all, and that the store really holds the
	// secret, so the scan below has something to find.
	blocked := c.awaitBlocked(command)
	if blocked.CommandID != command {
		t.Fatalf("the consumer stopped at %q, want the command carrying the secret", blocked.CommandID)
	}
	if !strings.Contains(string(c.store.storedPayload(command)), secret) {
		t.Fatalf("the durable record does not carry the private body; the scan below would then prove nothing")
	}

	// (a2) AND THE HOST NEVER ASKED FOR THE BODY. This is the measured form of
	// "the secret never enters the process", which is what (c) actually rests on
	// now. It is a live assertion: wiring commands.Applier back into the
	// composition makes it fail, which is exactly when (c) stops being held by
	// construction and has to start being held by the scan.
	if loads := c.store.payloadLoadCount(); loads != 0 {
		t.Fatalf("the Host read the private command body %d times; a composed Host that does not dispatch reads none", loads)
	}

	// (b) THE PUBLIC CONTROL CROSSES.
	c.runtime.commit(t, enduring(keyA, 1, `{"kind":"message","text":"`+publicMarker+`"}`))
	link.awaitPushes(1)

	wire := link.received()
	if !strings.Contains(string(wire), publicMarker) {
		t.Fatalf("the public marker was not found in %d bytes of received frames; the scan cannot see what crosses and the negative below is unfalsifiable", len(wire))
	}

	// (c) THE SECRET DID NOT. HELD BY CONSTRUCTION TODAY — see the header and
	// (a2) — and retained as a forward guard rather than as a measurement of
	// this build. Control (b) proves the scanner can see, but it proves it on
	// the runtime-to-tail EVENT path, which is not the command-payload path;
	// no production code joins the two any more.
	if strings.Contains(string(wire), secret) {
		t.Fatalf("the private command payload crossed HostLink; found in the %d bytes this link received", len(wire))
	}

	// (d) THE INBOUND HALF. A delivery carrying a body is not a Core record and
	// is refused, so there is no shape in which a payload arrives over the link.
	smuggled := link.send(5, map[string]any{
		"id": 5,
		"rpc": map[string]any{
			"method": channel,
			"data":   map[string]any{"command_id": string(command), "payload": secret},
		},
	})
	// THE REFUSAL HAS NO CORE CLASS, AND THAT IS THE DESIGNED ANSWER, not a
	// gap this test works around. hostlink refuses a malformed body at the
	// transport rather than publishing a HostLinkError, because a Core class is
	// information about a session and a body that is not a Core record has not
	// established which session it is talking about. The claim here is that the
	// smuggled body does not get through; which refusal it gets is asserted as
	// what it is, and BOTH forms are accepted rather than only the one this
	// build happens to produce.
	if _, published := refusalOf(t, smuggled); published {
		t.Fatalf("a delivery carrying a payload was ACCEPTED as a Core record: %s", mustJSON(t, smuggled))
	}
	if smuggled.Error == nil {
		t.Fatalf("a delivery carrying a payload was neither refused at the transport nor answered with a Core class: %s", mustJSON(t, smuggled))
	}

	if got := len(c.runtime.appliedCommands()); got != 0 {
		t.Fatalf("the runtime was driven %d times by a Host that does not dispatch", got)
	}

	// (e) THE CONTROL FOR (a2). "The Host read no payload" is a zero, and a zero
	// is what a broken counter also reports. One read through the same method
	// must move it, or (a2) is the very thing this test is here to stop being.
	if _, err := c.store.LoadPayload(context.Background(), tenantA, sessionA, command); err != nil {
		t.Fatalf("the control read of the private payload failed: %v", err)
	}
	if loads := c.store.payloadLoadCount(); loads != 1 {
		t.Fatalf("the payload-load counter reports %d after exactly one read; the zero asserted in (a2) cannot be distinguished from a counter that never moves", loads)
	}
}

// ---------------------------------------------------------------------------
// 6. No read plane and no list plane
// ---------------------------------------------------------------------------

// harnessReadPlaneRoutes is the read plane Harness serves and Host does not.
//
// The paths are Harness's own, taken from pkg/serve/mux.go's routing table and
// from the handlers in handlers_read.go, handlers_capabilities.go and
// handlers_events.go. They are listed so this guard fails if Host ever grows
// one of them, rather than only if it grows something somebody remembered.
var harnessReadPlaneRoutes = []string{
	"/sessions",
	"/sessions/tenant-a/session-a",
	"/sessions/tenant-a/session-a/journal",
	"/sessions/tenant-a/session-a/events",
	"/sessions/tenant-a/session-a/status",
	"/v1/sessions",
	"/capabilities",
	"/healthz",
	"/",
}

// harnessReadPlaneMethods is the same plane in the shape Host's transport could
// express it: an RPC method name.
var harnessReadPlaneMethods = []string{
	"hostlink.list_sessions",
	"hostlink.sessions",
	"hostlink.status",
	"hostlink.journal",
	"hostlink.events",
	"hostlink.read",
	"hostlink.get",
	"hostlink.capabilities",
}

// TestHostServesNoReadOrListPlaneAndTheProbeCanSayOtherwise re-establishes the
// ABSENCE Harness's read plane creates, which 04-host.md step 2 requires and
// which is the one item in that list with no positive form.
//
// A NEGATIVE ASSERTION THAT CANNOT FAIL IS NOT AN ASSERTION, so every arm below
// is paired with a control taken through the SAME probe:
//
//   - the HTTP arm asserts nine Harness read routes answer 404, and the control
//     is the one path Host does serve, which does not;
//   - the RPC arm asserts eight read-shaped method names are refused on a
//     BOUND, authenticated link, and the control is hostlink.bind on that same
//     link, which is accepted;
//   - the source arm asserts the reserved method constants are exactly four,
//     and its control is that the parse found them at all — a parse that
//     matched nothing would otherwise satisfy "none of them is a read".
//
// THE THREE OVERLAP; THEY DO NOT CLOSE. The source arm misses a read method
// dispatched from a string literal or from a constant that does not begin with
// "Method"; the RPC arm misses any name absent from harnessReadPlaneMethods,
// which is eight literals and not a derivation. An earlier version of this
// comment claimed the two combine into a closure — "any method not one of the
// four declared constants is resolved as a channel and needs a binding" — and
// that is not true of the dispatch switch this Host actually has: a new read
// method arrives as a new CASE and never reaches the default arm that resolves
// a name as a channel. What the three arms give is a bound on the defect, not
// its absence; reservedHostLinkMethods' doc states the residue in full.
func TestHostServesNoReadOrListPlaneAndTheProbeCanSayOtherwise(t *testing.T) {
	runtime := newPublishingSession()
	f := newFixture(t)
	f.rig.Session = runtime
	f.start()
	server := f.serve()
	f.attach(tenantA, sessionA)

	// -- the HTTP arm -------------------------------------------------------
	handler := f.svc.Handler()
	for _, route := range harnessReadPlaneRoutes {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, route, nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("GET %s answered %d, want 404; Host serves no read or list plane", route, recorder.Code)
		}
	}
	// THE CONTROL. The one path Host does serve answers something else, so the
	// 404s above are a property of those routes and not of a handler that
	// answers 404 to everything.
	served := httptest.NewRecorder()
	handler.ServeHTTP(served, httptest.NewRequest(http.MethodGet, HostLinkPathPrefix+string(tenantA), nil))
	if served.Code == http.StatusNotFound {
		t.Fatalf("the HostLink path answered 404 as well, so the nine assertions above cannot distinguish an absent route from a handler that serves nothing")
	}

	// -- the RPC arm --------------------------------------------------------
	link := dialFactoryLink(t, server.URL, tenantA)
	// THE CONTROL FIRST, so the refusals below are known to be refusals of the
	// METHOD rather than of a link that cannot do anything at all.
	accepted(t, link.rpc(2, hostlink.MethodBind, f.bindRequest(tenantA, sessionA, 9)))

	id := uint32(10)
	for _, method := range harnessReadPlaneMethods {
		id++
		reply := link.rpc(id, method, map[string]any{"tenant_id": string(tenantA), "session_id": string(sessionA)})
		refusal, published := refusalOf(t, reply)
		switch {
		case published:
			// A Core class is a refusal and is what a method resolved as an
			// unbound channel produces.
			if refusal.Code == "" {
				t.Errorf("%s was refused with an empty Core class", method)
			}
		case reply.Error != nil:
			// A transport refusal is also a refusal.
		default:
			t.Errorf("%s was ACCEPTED; Host answers no read or list RPC", method)
		}
	}

	// -- the source arm -----------------------------------------------------
	declared, err := reservedHostLinkMethods("../realtime/hostlink")
	if err != nil {
		t.Fatalf("parse the hostlink method constants: %v", err)
	}
	if len(declared) == 0 {
		t.Fatal("the parse found no reserved method constants, so 'none of them is a read' is vacuous")
	}
	want := map[string]string{
		"MethodBind":        hostlink.MethodBind,
		"MethodUnbind":      hostlink.MethodUnbind,
		"MethodDrain":       hostlink.MethodDrain,
		"MethodDrainStatus": hostlink.MethodDrainStatus,
	}
	if len(declared) != len(want) {
		t.Fatalf("hostlink declares %d reserved RPC methods %v, want exactly the four in %v; a fifth reserved method is a new plane and must be reviewed as one", len(declared), declared, want)
	}
	for name, value := range want {
		got, held := declared[name]
		if !held {
			t.Errorf("the parse did not find %s, so it is not reading the declarations it claims to", name)
			continue
		}
		if got != value {
			t.Errorf("%s is declared as %q in source and imported as %q", name, got, value)
		}
	}
}

// reservedHostLinkMethods returns every reserved RPC method constant declared
// in the hostlink package's PRODUCTION source, as name to wire value.
//
// IT DERIVES THE SET FROM SOURCE RATHER THAN FROM THE PACKAGE'S OWN EXPORTS,
// which is the difference between a guard and a tautology. A check that
// enumerated hostlink.Method* through the importing package would agree with
// whatever that package declares and could never disagree with it; this reads
// the declarations and the caller compares them to a list written down here, so
// adding MethodListSessions fails without anybody editing this function.
//
// ITS LIMITS ARE A RESIDUE AND NOT A BOUNDARY. The list below is what this
// parse is known to miss; it is NOT a closure, and a spelling absent from it is
// unexamined rather than excluded:
//
//   - a method dispatched from a STRING LITERAL at the switch rather than
//     through a Method-prefixed constant;
//   - a constant whose name does not begin with "Method" — the prefix is the
//     convention this package follows and not a rule the compiler enforces;
//   - a value built by concatenation or from another constant, which is skipped
//     because it is not a basic string literal, and which is REPORTED as an
//     error rather than passed over, so it cannot become a silent hole;
//   - a method reached by a handler registered directly on the Centrifuge
//     client outside the dispatch switch;
//   - anything in a file this walk does not read: it reads the named directory
//     only, non-recursively, and skips _test.go files.
//
// WHAT THE BEHAVIOURAL ARM ACTUALLY COVERS, AND WHAT IT DOES NOT. An earlier
// version of this comment said the behavioural arm of
// TestHostServesNoReadOrListPlaneAndTheProbeCanSayOtherwise covers the first,
// second and fourth items above, "because a method that is not one of the four
// is resolved as a channel and refused without a binding whatever it is
// spelled". THAT SENTENCE IS FALSE AND IT IS FALSE IN THE DIRECTION THAT
// MATTERS. Multiplexer.dispatch is a switch, and a new read method arrives as a
// new CASE: it never reaches the default arm, so it is never resolved as a
// channel and never meets the binding check the arm's refusals demonstrate. A
// two-line session-listing RPC added as a case, spelled from a string literal or
// from a constant whose name does not begin with "Method", is invisible to the
// source arm AND to the behavioural arm, and survives this whole suite green.
// The hole is narrow — the case has to be added, and it has to avoid the naming
// convention — and it is a hole.
//
// STATED AS A RESIDUE RATHER THAN A BOUNDARY, which is what the list above is:
// the behavioural arm refutes only the SPELLINGS IT ENUMERATES in
// harnessReadPlaneMethods, by driving each one over a real bound link and
// requiring a refusal. It establishes nothing about a name it does not name.
// Residue items 1, 2 and 4 are therefore UNEXAMINED, not excluded, and the two
// arms together bound the defect rather than closing it: a new read plane is
// caught if it is declared as a Method-prefixed constant (source arm) or if it
// is spelled as one of the eight names enumerated here (behavioural arm), and
// is missed otherwise. Closing it properly needs a third arm over the dispatch
// switch's own case values, which is not written.
func reservedHostLinkMethods(directory string) (map[string]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	fileSet := token.NewFileSet()
	found := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(directory, name)
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return nil, err
		}
		{
			for _, declaration := range file.Decls {
				general, ok := declaration.(*ast.GenDecl)
				if !ok || general.Tok != token.CONST {
					continue
				}
				for _, spec := range general.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for index, name := range value.Names {
						if !strings.HasPrefix(name.Name, "Method") {
							continue
						}
						if index >= len(value.Values) {
							return nil, errors.New(path + ": " + name.Name + " declares no value of its own")
						}
						literal, ok := value.Values[index].(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							return nil, errors.New(path + ": " + name.Name + " is not a basic string literal, so this guard cannot read its wire value")
						}
						unquoted, err := strconv.Unquote(literal.Value)
						if err != nil {
							return nil, err
						}
						found[name.Name] = unquoted
					}
				}
			}
		}
	}
	return found, nil
}
