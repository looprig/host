package host

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/publicbody"
)

// RuntimeJournalReader is what PublicJournal reads a Host-owned session's
// runtime journal through: the public page it projects, and the privileged
// record read it takes the command mapping from. A *sessionstore.Store opened
// over the runtime's backend — the reader a Factory JournalResolver already
// returns — satisfies it.
type RuntimeJournalReader interface {
	ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
	ReadRuntimeJournal(context.Context, sessionstore.ReadRuntimeJournalRequest) (sessionstore.RuntimePage, error)
}

// ErrPublicJournalScope reports a read naming a journal other than the one a
// PublicJournal was bound to. A JournalResolver reads under the binding's
// RUNTIME session id, so a request naming anything else is a composition
// defect, and projecting it with this session's identities would be wrong.
var ErrPublicJournalScope = errors.New("host: the journal read names another tenant or runtime session than the public journal was bound to")

// PublicJournal is a Host-owned session's runtime journal as a client may see
// it (finding W1).
//
// A harness public body names the RUNTIME session id on every event and the
// RUNTIME command id on every event a command caused. Both are private. Host
// rewrites them on the live tail it relays; Factory serves /journal (and every
// live-tail repair) by reading the runtime journal DIRECTLY, never through
// Host, so the same rewrite has to be applied there or /journal hands a
// browser exactly what the live tail withholds. This is that rewrite, as the
// reader a Factory JournalResolver returns.
//
// IT IS THE LIVE TAIL'S FUNCTION, NOT A SECOND ONE. Both paths run
// internal/publicbody.Project over the same stored body with the same
// identities, so an event reads byte-for-byte the same live and durably:
//
//   - every `session_id` equal to the runtime session id becomes the public one;
//   - `cause.command_id` becomes the public command id the client admitted, read
//     here from the journal's own application prefix for that command, or is
//     removed (and an emptied `cause` with it) when the command has no public
//     identity — a machine-originated cause.
//
// Nothing else changes: sequences, event ids, coverage, cursors, member order,
// and every other byte of every body are the runtime journal's.
//
// It is safe for concurrent use.
type PublicJournal struct {
	reader   RuntimeJournalReader
	tenant   sessionwire.TenantID
	runtime  sessionwire.SessionID
	identity publicbody.Identities
}

// NewPublicJournal binds reader to one Host-owned session: its tenant, its
// PUBLIC session id, and the durable binding naming its runtime session.
//
// It keeps a command mapping for the life of the value, read incrementally
// from the journal; a composition that builds one per read pays a scan of the
// journal's records up to the page each time. PublicJournals keeps them.
func NewPublicJournal(reader RuntimeJournalReader, tenant sessionwire.TenantID, session sessionwire.SessionID, binding sessionstore.SessionBinding) (*PublicJournal, error) {
	journal, _, err := newPublicJournal(reader, tenant, session, binding)
	return journal, err
}

// newPublicJournal is NewPublicJournal, also returning the handle its mapping
// reads the runtime journal through.
func newPublicJournal(reader RuntimeJournalReader, tenant sessionwire.TenantID, session sessionwire.SessionID, binding sessionstore.SessionBinding) (*PublicJournal, *currentJournal, error) {
	if reader == nil {
		return nil, nil, errors.New("host: a public journal needs a runtime journal reader")
	}
	if err := tenant.Validate(); err != nil {
		return nil, nil, fmt.Errorf("host: public journal tenant: %w", err)
	}
	if err := session.Validate(); err != nil {
		return nil, nil, fmt.Errorf("host: public journal session: %w", err)
	}
	// THE ERROR NEVER CARRIES THE ID (review L3). The runtime session id is the
	// value this type exists to keep from a client, and a composition may
	// surface a resolver's error text; the parse error would quote it too.
	runtimeID, err := uuid.Parse(binding.RuntimeSessionID)
	if err != nil || runtimeID.IsZero() {
		return nil, nil, fmt.Errorf("host: the binding's runtime session id is not a runtime session: %w", ErrPublicJournalScope)
	}
	runtime := sessionwire.SessionID(binding.RuntimeSessionID)
	current := &currentJournal{}
	current.use(reader)
	index := publicbody.NewIndex(publicbody.JournalSource{Journal: current, TenantID: tenant, RuntimeSessionID: runtime})
	return &PublicJournal{
		reader:  reader,
		tenant:  tenant,
		runtime: runtime,
		identity: publicbody.Identities{
			RuntimeSessionID: runtimeID,
			SessionID:        session,
			Commands:         index,
		},
	}, current, nil
}

// ReadPublicJournal reads one page of the runtime journal and projects every
// body in it. The request must name the bound tenant and the RUNTIME session,
// as a Factory JournalResolver's reads do.
func (j *PublicJournal) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	if req.TenantID != j.tenant || req.SessionID != j.runtime {
		return sessionwire.JournalPage{}, ErrPublicJournalScope
	}
	page, err := j.reader.ReadPublicJournal(ctx, req)
	if err != nil {
		return sessionwire.JournalPage{}, err
	}
	events := make([]sessionwire.JournalEvent, len(page.Events))
	copy(events, page.Events)
	for i := range events {
		body, err := publicbody.Project(ctx, events[i].Body, j.identity, events[i].JournalSeq)
		if err != nil {
			return sessionwire.JournalPage{}, fmt.Errorf("host: project the public body at journal sequence %d: %w", events[i].JournalSeq, err)
		}
		events[i].Body = body
	}
	page.Events = events
	return page, nil
}

// PublicJournals keeps each session's command mapping across reads, so a
// JournalResolver can answer a PublicJournal per call without re-reading a
// journal's records from the start every time. It holds at most its capacity
// of sessions, forgetting the least recently used; a forgotten session costs
// one re-read, never a wrong answer — a mapping is immutable.
//
// It is safe for concurrent use.
type PublicJournals struct {
	capacity int

	mu      sync.Mutex
	clock   uint64
	indexes map[publicJournalKey]*cachedIndex
}

type publicJournalKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	runtime sessionwire.SessionID
}

type cachedIndex struct {
	index   *publicbody.Index
	current *currentJournal
	used    uint64
}

// currentJournal is the runtime journal a kept mapping reads through: the
// reader most recently handed for its session.
type currentJournal struct {
	mu     sync.Mutex
	reader publicbody.RuntimeJournal
}

// use makes reader the one the mapping reads through from now on.
func (c *currentJournal) use(reader publicbody.RuntimeJournal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reader = reader
}

// ReadRuntimeJournal reads through the current reader.
func (c *currentJournal) ReadRuntimeJournal(ctx context.Context, req sessionstore.ReadRuntimeJournalRequest) (sessionstore.RuntimePage, error) {
	c.mu.Lock()
	reader := c.reader
	c.mu.Unlock()
	return reader.ReadRuntimeJournal(ctx, req)
}

// DefaultPublicJournalCapacity is the session count NewPublicJournals keeps
// when given no positive capacity.
const DefaultPublicJournalCapacity = 1024

// NewPublicJournals returns an empty cache holding at most capacity sessions.
func NewPublicJournals(capacity int) *PublicJournals {
	if capacity <= 0 {
		capacity = DefaultPublicJournalCapacity
	}
	return &PublicJournals{capacity: capacity, indexes: map[publicJournalKey]*cachedIndex{}}
}

// Reader is NewPublicJournal sharing this cache's mapping for the session.
func (p *PublicJournals) Reader(reader RuntimeJournalReader, tenant sessionwire.TenantID, session sessionwire.SessionID, binding sessionstore.SessionBinding) (*PublicJournal, error) {
	fresh, current, err := newPublicJournal(reader, tenant, session, binding)
	if err != nil {
		return nil, err
	}
	key := publicJournalKey{tenant: tenant, session: session, runtime: fresh.runtime}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clock++
	if cached, ok := p.indexes[key]; ok {
		// THE MAPPING IS KEPT; THE READER IS NOT (review L1). A resolver may
		// hand a new or rotated store per call, and the kept mapping must go on
		// reading through the one it was handed most recently.
		cached.used = p.clock
		cached.current.use(reader)
		fresh.identity.Commands = cached.index
		return fresh, nil
	}
	if len(p.indexes) >= p.capacity {
		var oldest publicJournalKey
		first := true
		for candidate, cached := range p.indexes {
			if first || cached.used < p.indexes[oldest].used {
				oldest, first = candidate, false
			}
		}
		delete(p.indexes, oldest)
	}
	p.indexes[key] = &cachedIndex{index: fresh.identity.Commands.(*publicbody.Index), current: current, used: p.clock}
	return fresh, nil
}
