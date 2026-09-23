package compose

import (
	"context"
	"log/slog"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/publicbody"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

// inboxCommandPage is how many acceptance records one read of the live
// projection's mapping takes.
const inboxCommandPage = 256

// inboxCommands reads a session's admission mappings from its disposition
// inbox, in acceptance order. It is the live tail's source; host.PublicJournal
// reads the same mappings from the runtime journal's application prefixes.
type inboxCommands struct {
	inbox commands.Inbox
	key   registry.Key
}

// Read implements publicbody.Source. Acceptance orders are not journal
// sequences, so the bound is not consulted: it reads to the end of the inbox.
func (s inboxCommands) Read(ctx context.Context, after, _ uint64) ([]publicbody.Pair, uint64, bool, error) {
	records, err := s.inbox.ListOrdered(ctx, s.key.TenantID, s.key.SessionID, after, inboxCommandPage)
	if err != nil {
		return nil, after, false, err
	}
	pairs := make([]publicbody.Pair, 0, len(records))
	through := after
	for _, record := range records {
		if record.AcceptedOrder > through {
			through = record.AcceptedOrder
		}
		if record.RuntimeCommandID.IsZero() {
			continue
		}
		pairs = append(pairs, publicbody.Pair{Runtime: record.RuntimeCommandID, Public: record.CommandID})
	}
	return pairs, through, len(records) < inboxCommandPage, nil
}

// publicProjector is the live tail's W1 projection for one residency: every
// body is published under the public session id and the public command ids
// the client admitted, never the runtime's.
//
// A RUNTIME THAT REPORTS NO RUNTIME SESSION ID IS RELAYED VERBATIM, and said
// so. Only a runtime launched through department's rig adapter reports one;
// any other runtime's bodies are its own format, which this projection has no
// rule for, and inventing an identity to replace would be worse than none.
func (s *Service) publicProjector(ctx context.Context, request residency.OwnershipRequest) service.Projector {
	runtimeID, ok := department.RigSessionID(request.Runtime)
	if !ok || runtimeID.IsZero() {
		s.options.logger().LogAttrs(ctx, slog.LevelWarn,
			"host: the runtime reports no runtime session id, so its public bodies are relayed without the public-identity projection",
			slog.String("tenant_id", string(request.Key.TenantID)),
			slog.String("session_id", string(request.Key.SessionID)))
		return nil
	}
	ids := publicbody.Identities{
		RuntimeSessionID: runtimeID,
		SessionID:        request.Key.SessionID,
		Commands:         publicbody.NewIndex(inboxCommands{inbox: s.options.Inbox, key: request.Key}),
	}
	return publicbody.Projection(ids)
}
