package compose

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

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
	return retryingProjection(publicbody.Projection(ids), s.options.logger(), request.Key)
}

// The live projection's retry bounds (review F2). Variables so a test can
// shorten them.
var (
	projectionRetryFloor   = 50 * time.Millisecond
	projectionRetryCeiling = 5 * time.Second
)

// retryingProjection retries a projection whose command mapping could not be
// READ, with bounded backoff, until the tail's context ends.
//
// A TRANSIENT STORE ERROR MUST NOT END THE LIVE TAIL (review F2). The mapping is
// read from the disposition inbox, so a storage blip at the first
// command-caused event of a residency used to refuse the publication, and the
// session's viewers went dark for the rest of the residency. The relay
// goroutine blocks meanwhile; it is this session's alone. A cancelled context
// returns the context's error, which the relay records as a stop. Every other
// failure — a body that is not canonical — is returned at once: retrying it
// cannot change the answer.
func retryingProjection(project service.Projector, logger *slog.Logger, key registry.Key) service.Projector {
	return func(ctx context.Context, body json.RawMessage, seq uint64) (json.RawMessage, error) {
		backoff := projectionRetryFloor
		for attempt := 1; ; attempt++ {
			projected, err := project(ctx, body, seq)
			var mapping *publicbody.MappingError
			if err == nil || !errors.As(err, &mapping) {
				return projected, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			logger.LogAttrs(ctx, slog.LevelWarn,
				"host: the live tail could not read the session's command mapping; retrying the publication",
				slog.String("tenant_id", string(key.TenantID)),
				slog.String("session_id", string(key.SessionID)),
				slog.Uint64("journal_seq", seq),
				slog.Int("attempt", attempt),
				slog.String("error", err.Error()))
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			backoff = min(2*backoff, projectionRetryCeiling)
		}
	}
}

// The refused-tail restart bounds. Variables so a test can shorten them.
var (
	tailRestartFloor   = 100 * time.Millisecond
	tailRestartCeiling = 5 * time.Second
)

// supervisedTail is one session's live tail, restarted when it refuses a
// publication (review F2).
//
// A REFUSED TAIL USED TO BE OBSERVED BY NOBODY. The relay invalidated the
// session's routes — which makes every Factory replica reset its viewers from
// the durable journal — and returned, and nothing ever published on the
// session's channel again for the rest of the residency. Now the refusal is
// logged at ERROR with its cause and a new tail is subscribed from the live
// tip, so the viewers that reset durably have a stream to rejoin.
//
// A LOST tail is left ended, as before: its causes cannot be told apart and
// some of them are broken invariants a resubscribe loops against (see
// service.TailEndLost). A stopped one is Host's own decision.
type supervisedTail struct {
	mu      sync.Mutex
	current *service.Tail
	stopped bool
}

// Stop ends the current tail and every restart.
func (s *supervisedTail) Stop() {
	s.mu.Lock()
	s.stopped = true
	current := s.current
	s.mu.Unlock()
	current.Stop()
}

// superviseTail starts a tail and restarts it on refusal until ctx ends or
// Stop is called.
func (s *Service) superviseTail(ctx context.Context, key registry.Key, start func() (*service.Tail, error)) (*supervisedTail, error) {
	first, err := start()
	if err != nil {
		return nil, err
	}
	supervised := &supervisedTail{current: first}
	go s.restartRefused(ctx, key, supervised, start)
	return supervised, nil
}

func (s *Service) restartRefused(ctx context.Context, key registry.Key, supervised *supervisedTail, start func() (*service.Tail, error)) {
	logger := s.options.logger()
	attrs := []slog.Attr{slog.String("tenant_id", string(key.TenantID)), slog.String("session_id", string(key.SessionID))}
	backoff := tailRestartFloor
	for {
		supervised.mu.Lock()
		current := supervised.current
		supervised.mu.Unlock()
		select {
		case <-current.Done():
		case <-ctx.Done():
			return
		}
		end, cause := current.End()
		if end != service.TailEndRefused {
			return
		}
		logger.LogAttrs(ctx, slog.LevelError,
			"host: the session's live tail refused a publication; its routes were invalidated so viewers reset from the durable journal, and the tail is restarted",
			append(attrs, slog.Any("error", cause))...)
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(2*backoff, tailRestartCeiling)
			supervised.mu.Lock()
			stopped := supervised.stopped
			supervised.mu.Unlock()
			if stopped || ctx.Err() != nil {
				return
			}
			next, err := start()
			if err != nil {
				logger.LogAttrs(ctx, slog.LevelError, "host: the session's live tail could not be restarted; retrying",
					append(attrs, slog.String("error", err.Error()))...)
				continue
			}
			supervised.mu.Lock()
			if supervised.stopped {
				supervised.mu.Unlock()
				next.Stop()
				return
			}
			supervised.current = next
			supervised.mu.Unlock()
			break
		}
	}
}
