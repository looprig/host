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

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

type runtimeWithoutRigID struct{ department.Runtime }
type composeDualSource struct {
	committed <-chan sessionwire.EnduringPublication
	liveCalls int
}

func (s *composeDualSource) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	return s.committed, nil
}
func (s *composeDualSource) SubscribeLivePublic(context.Context) (<-chan department.LivePublication, error) {
	s.liveCalls++
	return nil, errors.New("mixed stream requested")
}

type composeTransientWire struct {
	wirePublications
	transient atomic.Int32
}

func (w *composeTransientWire) TryPublishEphemeral(string, []byte) bool {
	w.transient.Add(1)
	return true
}

type runtimeWithRigID struct{ department.Runtime }

func (runtimeWithRigID) RigSessionID() uuid.UUID { return uuid.MustParse(supervisedRuntime) }

func TestLiveTextDefaultsOffWithRigRuntime(t *testing.T) {
	s := &Service{options: Options{}}
	enduring, transient := s.publicProjectors(t.Context(), residency.OwnershipRequest{Key: supervisedKey, Runtime: runtimeWithRigID{}})
	if enduring == nil || transient != nil {
		t.Fatal("default composition did not retain only the enduring projector")
	}
}

func TestLiveTextOptInStillSuppressesRuntimeWithoutRigID(t *testing.T) {
	s := &Service{options: Options{LiveText: &LiveTextOptions{}}}
	enduring, transient := s.publicProjectors(t.Context(), residency.OwnershipRequest{Key: supervisedKey, Runtime: runtimeWithoutRigID{}})
	if enduring != nil || transient != nil {
		t.Fatal("runtime without rig ID received a transient projector")
	}
	committed := make(chan sessionwire.EnduringPublication, 1)
	committed <- sessionwire.EnduringPublication{TenantID: supervisedKey.TenantID, SessionID: supervisedKey.SessionID, EventID: "event-1", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{"type":"x"}`)}
	source := &composeDualSource{committed: committed}
	wire := &composeTransientWire{}
	tails, err := service.NewTails(service.TailOptions{Publications: wire, Routes: &countingRoutes{}})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := tails.PublishProjected(t.Context(), supervisedKey, source, enduring, transient)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Stop)
	deadline := time.Now().Add(time.Second)
	for tail.Published() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if tail.Published() != 1 || tail.EphemeralPublished() != 0 || wire.transient.Load() != 0 || source.liveCalls != 0 {
		t.Fatalf("no-ID runtime emitted transient frames: enduring %d, transient %d, attempts %d, mixed subscriptions %d", tail.Published(), tail.EphemeralPublished(), wire.transient.Load(), source.liveCalls)
	}
}

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
