// Package harnesstest is TEST SUPPORT over the released harness runtime: a real
// harness session store, a real rig with one loop, and a recording inference
// client that answers every turn. It exists so that Host's tests about what a
// launch does to a DURABLE CONVERSATION — create, release, restore — run against
// harness's own journal rather than a fake that can only agree with itself.
//
// Nothing in a production path imports it. It names github.com/looprig/inference
// only because harness's loop.WithInference takes an inference.Client, which is
// the one seam a test must supply to run a turn without a model.
package harnesstest

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	"github.com/looprig/inference/stream"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// Backend is one in-memory storage composite. Tests that need two harness
// stores sharing a ledger but not a catalog build them from two Backends'
// parts with Compose.
func Backend(tb testing.TB) *storage.Composite {
	tb.Helper()
	return memstore.New()
}

// Compose assembles a composite from another composite's ledger and a second
// one's KV, so a store opened over it reads one journal and a DIFFERENT
// catalog. Leases, blobs and the ordered index come from the ledger's side.
func Compose(tb testing.TB, ledger, kv *storage.Composite) *storage.Composite {
	tb.Helper()
	composite, err := storage.NewCompositeWithOrderedIndex(ledger.Ledger, ledger.Leaser, kv.KV, ledger.Blobs, ledger.OrderedIndex)
	if err != nil {
		tb.Fatalf("harnesstest: compose a split backend: %v", err)
	}
	return composite
}

// Store opens a harness session store for one tenant over backend.
func Store(tb testing.TB, backend *storage.Composite, tenant sessionwire.TenantID) *harnessstore.Store {
	tb.Helper()
	store, err := harnessstore.Open(backend, harnessstore.WithTenant(tenant))
	if err != nil {
		tb.Fatalf("harnesstest: open the harness session store: %v", err)
	}
	return store
}

// Rig defines a real rig with one primer loop over store, answered by llm.
func Rig(tb testing.TB, store *harnessstore.Store, llm inference.Client) *rig.Rig {
	tb.Helper()
	definition, err := loop.Define(loop.WithName("agent"), loop.WithInference(llm, model.Model{
		Provider: "test", APIFormat: model.APIFormatOpenAI, BaseURL: "http://localhost", Name: "model",
	}))
	if err != nil {
		tb.Fatalf("harnesstest: define the loop: %v", err)
	}
	defined, err := rig.Define(rig.WithLoops(definition), rig.WithPrimers("agent"), rig.WithSessionStore(store))
	if err != nil {
		tb.Fatalf("harnesstest: define the rig: %v", err)
	}
	return defined
}

// RecordingLLM answers every turn with the text "ok" and records every request,
// so a test can read back the conversation a turn was run over.
type RecordingLLM struct {
	mu       sync.Mutex
	requests []inference.Request
}

// Invoke is unused by a streaming loop and refuses.
func (*RecordingLLM) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("harnesstest: Invoke is not used")
}

// Stream records the request and streams one text chunk.
func (l *RecordingLLM) Stream(_ context.Context, request inference.Request) (*stream.StreamReader[content.Chunk], error) {
	l.mu.Lock()
	l.requests = append(l.requests, request)
	l.mu.Unlock()
	sent := false
	return stream.NewStreamReader(func() (content.Chunk, error) {
		if sent {
			return nil, io.EOF
		}
		sent = true
		return &content.TextChunk{Text: "ok"}, nil
	}, nil), nil
}

// Requests returns every request recorded so far.
func (l *RecordingLLM) Requests() []inference.Request {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]inference.Request(nil), l.requests...)
}

// Events replays every event filed under id, in order. A stream that cannot be
// opened is reported as a failure rather than as an empty history.
func Events(tb testing.TB, store *harnessstore.Store, id uuid.UUID) []event.Event {
	tb.Helper()
	replayer, err := store.OpenInternalEventReplayer(id, harnessstore.ReplayRequest{FromSeq: 0})
	if err != nil {
		tb.Fatalf("harnesstest: open the replayer for %s: %v", id, err)
	}
	cursor, err := replayer.Open(context.Background(), journal.ReplayRequest{From: journal.Beginning()})
	if err != nil {
		tb.Fatalf("harnesstest: open the replay cursor for %s: %v", id, err)
	}
	defer cursor.Close()
	var events []event.Event
	for {
		ev, _, err := cursor.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			tb.Fatalf("harnesstest: replay %s: %v", id, err)
		}
		events = append(events, ev)
	}
}

// CountSessionStarted counts the SessionStarted records filed under id. A
// conversation that was restored has exactly one; one that was silently
// restarted has two.
func CountSessionStarted(tb testing.TB, store *harnessstore.Store, id uuid.UUID) int {
	tb.Helper()
	count := 0
	for _, ev := range Events(tb, store, id) {
		if _, started := ev.(event.SessionStarted); started {
			count++
		}
	}
	return count
}

// RigLauncher launches a real rig's sessions under a caller-named identity.
type RigLauncher struct{ rig *rig.Rig }

// Launcher wraps a real rig.
func Launcher(r *rig.Rig) RigLauncher { return RigLauncher{rig: r} }

// NewSessionUnder launches a new session under id with rig.WithSessionID.
func (l RigLauncher) NewSessionUnder(ctx context.Context, id uuid.UUID) (session.SessionController, error) {
	return l.rig.NewSession(ctx, rig.WithSessionID(id))
}
