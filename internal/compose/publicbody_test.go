package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"

	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/registry"
)

// pagedInbox serves a session's acceptance records in pages, as the store does.
type pagedInbox struct{ records []commands.Command }

func (p pagedInbox) ListOrdered(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, after uint64, limit int) ([]commands.Command, error) {
	var out []commands.Command
	for _, record := range p.records {
		if record.AcceptedOrder > after && len(out) < limit {
			out = append(out, record)
		}
	}
	return out, nil
}

func TestTransientProjectionDoesNotRetryAMappingRead(t *testing.T) {
	inbox := &flakyInbox{}
	inbox.failures.Store(1)
	ids := publicbody.Identities{
		RuntimeSessionID: uuid.MustParse(supervisedRuntime), SessionID: supervisedKey.SessionID,
		Commands: publicbody.NewIndex(inboxCommands{inbox: inbox, key: supervisedKey}),
	}
	_, transient := tailProjectors(ids, ids, nil, supervisedKey, backoff{floor: time.Second, ceiling: time.Second})
	body := json.RawMessage(causedBody())
	started := time.Now()
	_, err := transient(t.Context(), body, 0)
	var mapping *publicbody.MappingError
	if !errors.As(err, &mapping) || time.Since(started) > 100*time.Millisecond || inbox.reads.Load() != 1 {
		t.Fatalf("transient projection = (%v, reads %d), want one prompt mapping failure", err, inbox.reads.Load())
	}
}

type blockingFirstMapping struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (m *blockingFirstMapping) Read(_ context.Context, _, _ uint64) ([]publicbody.Pair, uint64, bool, error) {
	if m.calls.Add(1) == 1 {
		close(m.entered)
		<-m.release
	}
	return []publicbody.Pair{{Runtime: uuid.MustParse(supervisedCommand), Public: "command-public"}}, 1, true, nil
}

func TestBlockedTransientMappingCannotHoldTheEnduringIndex(t *testing.T) {
	source := &blockingFirstMapping{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(source.release) })
	identity := publicbody.Identities{RuntimeSessionID: uuid.MustParse(supervisedRuntime), SessionID: supervisedKey.SessionID}
	enduringIDs := identity
	enduringIDs.Commands = publicbody.NewIndex(source)
	transientIDs := identity
	transientIDs.Commands = publicbody.NewIndex(source)
	enduring, transient := tailProjectors(enduringIDs, transientIDs, slog.Default(), supervisedKey, fast)
	go func() { _, _ = transient(t.Context(), json.RawMessage(causedBody()), 0) }()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("transient mapping read did not begin")
	}
	done := make(chan error, 1)
	go func() {
		_, err := enduring(t.Context(), json.RawMessage(causedBody()), 1)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("enduring projection failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("enduring projection waited for the transient mapping read")
	}
}

// TestTheLiveMappingReadsEveryPageOfTheInbox: a command admitted past the
// first page is still named by its public id, and a miss is only concluded at
// the inbox's end — a miss is remembered, so concluding it early would drop
// that command's id from every later event for good.
func TestTheLiveMappingReadsEveryPageOfTheInbox(t *testing.T) {
	var inbox pagedInbox
	var last uuid.UUID
	for i := range inboxCommandPage + 3 {
		last = uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
		inbox.records = append(inbox.records, commands.Command{
			CommandID: sessionwire.CommandID(fmt.Sprintf("command-%d", i+1)), AcceptedOrder: uint64(i + 1), RuntimeCommandID: last,
		})
	}
	index := publicbody.NewIndex(inboxCommands{inbox: inbox, key: registry.Key{TenantID: "tenant-a", SessionID: "session-a"}})
	got, ok, err := index.PublicCommand(t.Context(), last, 1)
	if err != nil || !ok || got != sessionwire.CommandID(fmt.Sprintf("command-%d", inboxCommandPage+3)) {
		t.Fatalf("PublicCommand(last) = (%q, %v, %v), want the last admitted command", got, ok, err)
	}
}
