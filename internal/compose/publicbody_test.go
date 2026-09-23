package compose

import (
	"context"
	"fmt"
	"testing"

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
