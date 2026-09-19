// Package gates publishes a resident session's open gates to the durable gate
// projection, so a Factory can show every AskUser or permission question to a
// user and admit the answer.
//
// THE RUNTIME'S JOURNAL IS THE AUTHORITY AND THE PROJECTION IS A COPY. A gate is
// open exactly when the runtime journaled its GateOpened and no GateResolved
// for it; harness has no other gate-closing event. So the Publisher folds the
// journal into the set of open gates and converges the projection on that set:
// it opens what the journal holds open and the projection lacks, and resolves
// what the projection holds and the journal has closed. It never decides a gate
// is open or closed on any other evidence.
//
// THE ORDER IS SUBSCRIBE, FENCE, FOLD, CONVERGE (the Host recipe's R3):
//
//  1. SUBSCRIBE to the runtime's committed stream FIRST. A delivery is only a
//     hint that the journal moved; what moved is read back out of the journal.
//     Subscribing before the fold means no append can fall between the two.
//  2. FENCE: one gate write under this Host's residency grant before anything
//     else, a resolve of a gate identity no runtime mints. sessionstore v0.12.0
//     raises the projection's residency mark to the writer's grant on every
//     successful gate write, so after it a predecessor's late write is refused.
//     It also narrows sessionstore's booked F2 window, whose mitigation is
//     exactly "one fencing write before opening any new gate".
//  3. FOLD the whole journal once, then only what was appended since
//     (positioned by the ledger sequence the fold last reached).
//  4. CONVERGE the projection on the fold.
//
// A LOST SUBSCRIPTION IS A GAP IN HINTS AND NOT IN STATE. The fold is positioned
// by sequence, so resubscribing and folding from the last sequence reached
// recovers everything the lost subscription would have hinted at. This is why
// the committed-output pump's stop-on-overflow does not matter here.
//
// A SUPERSEDED GRANT STOPS THE PUBLISHER FOR GOOD. A mark above this Host's
// grant means a successor has written; every later write would be refused, and
// retrying would only spend the store.
package gates

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	coresessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	harnesswire "github.com/looprig/harness/pkg/sessionwire"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/residency"
)

// FenceGateID is the gate identity the fencing write resolves.
//
// NO RUNTIME CAN MINT IT. Harness gate identities are UUIDs and this is not one,
// so a resolve of it can never close a real gate; it exists only so the store
// has a gate write to raise the residency mark on.
const FenceGateID coresessionwire.GateID = "host.residency-fence"

// Scope is one session's identities as the projection needs them, read from ONE
// read of the session's durable binding.
//
// THE PAIRING IS NOT CHECKED DOWNSTREAM. harness's ReadScope scopes events by
// RuntimeSessionID and answers under SessionID, and it says in terms that a
// mismatched pair projects another session's events under this one's identity.
// Taking both from one binding read is the only guard there is.
type Scope struct {
	TenantID         coresessionwire.TenantID
	SessionID        coresessionwire.SessionID
	AgentID          coresessionwire.AgentID
	RuntimeSessionID uuid.UUID
}

// Session is one resident session's durable gate surface, bound to this Host's
// residency grant over it by the implementation. No method takes an epoch: the
// grant is the implementation's, so no caller can name one.
type Session interface {
	// Scope reads the session's durable binding once and returns its
	// identities. It must be called before Replay.
	Scope(context.Context) (Scope, error)

	// Projected returns the gates the durable projection holds open.
	Projected(context.Context) ([]coresessionwire.GateProjection, error)

	// Open projects one open gate under the bound grant. Replaying an
	// identical open is idempotent.
	Open(context.Context, coresessionwire.GateProjection) error

	// Resolve removes one gate from the projection under the bound grant. A
	// gate the projection does not hold is not an error; the write still
	// raises the residency mark.
	Resolve(context.Context, coresessionwire.GateID) error

	// Replay visits the runtime journal's events in sequence order from the
	// inclusive sequence from, with each event's ledger sequence.
	Replay(ctx context.Context, from uint64, visit func(event.Event, uint64) error) error
}

// ErrUnpublishable is what a Session returns for a gate it can never project,
// such as one whose deadline intent a stale resolve tombstoned (sessionstore's
// booked F2 residue). The publisher records it and stops trying that gate.
var ErrUnpublishable = errors.New("gates: this gate can never be projected")

// ErrProjectionFull is what a Session returns for a gate the projection has no
// room for: sessionstore holds at most sessionstore.MaxCatalogOpenGates (16)
// open gates per session. The gate is NOT visible to Factory until another
// closes. The publisher logs it once per gate and tries it again only when the
// fold changes, never on a retry timer.
var ErrProjectionFull = errors.New("gates: the session's gate projection is full")

// Options configures a Publisher.
type Options struct {
	// Session is the session's durable gate surface. Required.
	Session Session

	// Hints is the runtime's committed stream. Required. Its deliveries are
	// hints only; see the package documentation.
	Hints department.PublicationSubscriber

	// After is the clock the retry waits on. Required.
	After func(time.Duration) <-chan time.Time

	// Retry is how long the publisher waits after a failed pass, or after
	// its subscription ended, before trying again. Required and positive.
	Retry time.Duration

	// OnConverged is called after each pass that converged the projection,
	// so a caller waiting on this Host's ownership of a gate can re-check.
	// Optional.
	OnConverged func()

	// Logger receives WARN records for failed passes. Optional.
	Logger *slog.Logger

	// StopBound is how long Stop waits for the publisher to return after
	// cancelling it. Zero means DefaultStopBound.
	StopBound time.Duration
}

// DefaultStopBound is Stop's wait when Options.StopBound is zero.
const DefaultStopBound = 5 * time.Second

// InvalidOptionsError reports a Publisher that may not run.
type InvalidOptionsError struct {
	Field string
}

func (e *InvalidOptionsError) Error() string {
	return "gates: invalid option " + e.Field
}

// openGate is one gate the fold holds open.
type openGate struct {
	opened event.GateOpened
	seq    uint64
}

// Publisher converges one session's durable gate projection on its runtime's
// journal, until it is stopped or superseded.
type Publisher struct {
	options Options
	logger  *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}

	// Fold state, owned by the run goroutine.
	scope         Scope
	scoped        bool
	fenced        bool
	open          map[gate.ID]openGate
	next          uint64
	dirty         bool
	unpublishable map[coresessionwire.GateID]bool
	full          map[coresessionwire.GateID]bool

	mu         sync.Mutex
	superseded bool
	passes     int
}

// Start validates the options and starts publishing. The returned Publisher
// runs until Stop, until ctx ends, or until its grant is superseded.
func Start(ctx context.Context, options Options) (*Publisher, error) {
	switch {
	case options.Session == nil:
		return nil, &InvalidOptionsError{Field: "Session"}
	case options.Hints == nil:
		return nil, &InvalidOptionsError{Field: "Hints"}
	case options.After == nil:
		return nil, &InvalidOptionsError{Field: "After"}
	case options.Retry <= 0:
		return nil, &InvalidOptionsError{Field: "Retry"}
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	runCtx, cancel := context.WithCancel(ctx)
	p := &Publisher{
		options:       options,
		logger:        logger,
		cancel:        cancel,
		done:          make(chan struct{}),
		open:          map[gate.ID]openGate{},
		next:          1,
		dirty:         true,
		unpublishable: map[coresessionwire.GateID]bool{},
		full:          map[coresessionwire.GateID]bool{},
	}
	go p.run(runCtx)
	return p, nil
}

// Stop ends publication and waits, BOUNDED, for the publisher to return.
//
// THE WAIT IS BOUNDED (quality gate F8). A store call that ignores its
// context would otherwise hold Stop — and with it the session's release, which
// runs Stop before the grace-bounded runtime release — forever. Past the bound
// Stop returns and the goroutine is abandoned to finish its call; the released
// stores honour cancellation, so this is a guard against a provider that does
// not. It reports whether the publisher returned in time.
func (p *Publisher) Stop() bool {
	p.cancel()
	bound := p.options.StopBound
	if bound <= 0 {
		bound = DefaultStopBound
	}
	select {
	case <-p.done:
		return true
	case <-p.options.After(bound):
		return false
	}
}

// Superseded reports whether the publisher stopped because a successor's
// grant fenced it.
func (p *Publisher) Superseded() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.superseded
}

// run is the publisher's loop: subscribe, then pass after every hint, and after
// Retry when a pass failed or the subscription ended.
func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)
	var hints <-chan coresessionwire.EnduringPublication
	for {
		if hints == nil {
			subscribed, err := p.options.Hints.SubscribeCommitted(ctx, "")
			if err != nil {
				p.warn(ctx, "host: the gate publisher could not subscribe to the runtime's committed stream", err)
			} else {
				hints = subscribed
			}
		}
		failed := false
		if err := p.pass(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, residency.ErrEpochSuperseded) {
				p.mu.Lock()
				p.superseded = true
				p.mu.Unlock()
				p.warn(ctx, "host: the gate publisher stopped because a successor's residency grant fenced it", err)
				return
			}
			p.warn(ctx, "host: a gate publication pass failed and will be retried", err)
			failed = true
		}
		var retry <-chan time.Time
		if failed || hints == nil {
			retry = p.options.After(p.options.Retry)
		}
		select {
		case <-ctx.Done():
			return
		case <-retry:
		case _, open := <-hints:
			if !open {
				// The subscription ended. Resubscribe after Retry; the next
				// pass folds from the last sequence reached, so nothing the
				// lost subscription carried is missed.
				hints = nil
				select {
				case <-ctx.Done():
					return
				case <-p.options.After(p.options.Retry):
				}
				continue
			}
			drainHints(hints)
		}
	}
}

// drainHints coalesces every hint already waiting into the pass about to run.
func drainHints(hints <-chan coresessionwire.EnduringPublication) {
	for {
		select {
		case _, open := <-hints:
			if !open {
				return
			}
		default:
			return
		}
	}
}

// pass reads the scope once, fences once, folds what the journal appended since
// the last pass, and converges the projection when the fold changed it or a
// previous pass failed.
func (p *Publisher) pass(ctx context.Context) error {
	if !p.scoped {
		scope, err := p.options.Session.Scope(ctx)
		if err != nil {
			return err
		}
		p.scope, p.scoped = scope, true
	}
	if !p.fenced {
		if err := p.options.Session.Resolve(ctx, FenceGateID); err != nil {
			return err
		}
		p.fenced = true
	}
	if err := p.fold(ctx); err != nil {
		return err
	}
	if !p.dirty {
		return nil
	}
	if err := p.converge(ctx); err != nil {
		return err
	}
	p.dirty = false
	p.mu.Lock()
	p.passes++
	p.mu.Unlock()
	if p.options.OnConverged != nil {
		p.options.OnConverged()
	}
	return nil
}

// fold applies every event from the next unread sequence to the open set.
func (p *Publisher) fold(ctx context.Context) error {
	return p.options.Session.Replay(ctx, p.next, func(ev event.Event, seq uint64) error {
		switch ev := ev.(type) {
		case event.GateOpened:
			p.open[ev.Gate.ID] = openGate{opened: ev, seq: seq}
			p.dirty = true
		case event.GateResolved:
			delete(p.open, ev.GateID)
			p.dirty = true
		}
		if seq >= p.next {
			p.next = seq + 1
		}
		return nil
	})
}

// converge opens every folded gate the projection lacks, oldest first, and
// resolves every projected gate the fold does not hold open.
//
// A GATE ALREADY PROJECTED IS LEFT AS IT STANDS, compared by identity alone. A
// predecessor may have projected it with a deadline computed by a different
// policy; re-opening it with this Host's would be a conflict and would change a
// projection a reader already published. The fencing write, not a re-open, is
// what takes ownership of it.
func (p *Publisher) converge(ctx context.Context) error {
	projected, err := p.options.Session.Projected(ctx)
	if err != nil {
		return err
	}
	have := make(map[coresessionwire.GateID]bool, len(projected))
	for _, projection := range projected {
		have[projection.GateID] = true
	}
	want := make(map[coresessionwire.GateID]bool, len(p.open))
	pending := make([]openGate, 0, len(p.open))
	for _, held := range p.open {
		id := coresessionwire.GateID(held.opened.Gate.ID.String())
		want[id] = true
		if !have[id] && !p.unpublishable[id] {
			pending = append(pending, held)
		}
	}
	slices.SortFunc(pending, func(a, b openGate) int {
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		default:
			return 0
		}
	})
	var failures []error
	for _, held := range pending {
		projection, err := Project(p.scope, held.opened, held.seq)
		if err == nil {
			err = p.options.Session.Open(ctx, projection)
		}
		switch {
		case err == nil:
		case errors.Is(err, residency.ErrEpochSuperseded):
			return err
		case errors.Is(err, ErrUnpublishable):
			p.unpublishable[coresessionwire.GateID(held.opened.Gate.ID.String())] = true
			p.warn(ctx, "host: a gate can never be projected and will not be retried", err)
		case errors.Is(err, ErrProjectionFull):
			// NOT A FAILED PASS. Retrying on the timer would change nothing
			// until a gate closes, and closing one changes the fold, which
			// marks the next pass dirty and tries this gate again. Until then
			// Factory cannot see it, which is logged once.
			id := coresessionwire.GateID(held.opened.Gate.ID.String())
			if !p.full[id] {
				p.full[id] = true
				p.warn(ctx, "host: a gate is not visible to Factory because the session's gate projection is full; it is published when another gate closes", err)
			}
		default:
			failures = append(failures, err)
		}
	}
	for _, projection := range projected {
		if want[projection.GateID] {
			continue
		}
		if err := p.options.Session.Resolve(ctx, projection.GateID); err != nil {
			if errors.Is(err, residency.ErrEpochSuperseded) {
				return err
			}
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// warn writes one WARN record naming the session.
func (p *Publisher) warn(ctx context.Context, message string, err error) {
	p.logger.LogAttrs(ctx, slog.LevelWarn, message,
		slog.String("tenant_id", string(p.scope.TenantID)),
		slog.String("session_id", string(p.scope.SessionID)),
		slog.String("error", err.Error()))
}

// Project builds the durable projection of one open gate.
//
// EVERYTHING IN IT IS A FUNCTION OF THE JOURNALED GateOpened AND ITS SEQUENCE,
// so a successor that folds the same journal projects the same bytes (the
// recipe's R1). The deadline is the event's durable creation time plus the
// gate's own response timeout, or a per-kind TTL compiled into Host when the
// gate has none; answerability is always resident, because this Host's runtime
// holds the gate.
func Project(scope Scope, opened event.GateOpened, seq uint64) (coresessionwire.GateProjection, error) {
	page, err := harnesswire.ProjectGatePage(harnesswire.ReadScope{
		TenantID:         scope.TenantID,
		SessionID:        scope.SessionID,
		AgentID:          scope.AgentID,
		Residency:        coresessionwire.SessionResidencyResident,
		RuntimeSessionID: scope.RuntimeSessionID,
	}, []harnesswire.OpenGate{{
		Event:         opened,
		JournalSeq:    seq,
		Deadline:      Deadline(opened),
		Answerability: coresessionwire.GateAnswerabilityResident,
	}}, seq, 1, "", "")
	if err != nil {
		return coresessionwire.GateProjection{}, err
	}
	return page.Gates[0], nil
}

// Deadline is the effective deadline of one journaled gate: its durable creation
// time plus its own response timeout when it carries one, and otherwise plus
// the TTL compiled for its kind.
func Deadline(opened event.GateOpened) time.Time {
	if timeout := opened.Gate.ResponsePolicy.Timeout; timeout > 0 {
		return opened.CreatedAt.Add(timeout)
	}
	return opened.CreatedAt.Add(TTL(opened.Gate.Kind))
}

// TTL is the deadline Host gives a gate whose journaled envelope carries no
// response timeout.
//
// IT IS COMPILED IN AND NEVER CONFIGURED, so two Hosts folding one journal
// project one deadline. A permission gate normally carries harness's own
// five-minute default in its journaled policy and never reaches this table;
// the entry covers a record written without one. An ask_user or form gate
// waits for a human by default, and a day is how long this Host will advertise
// that it is still waiting. An open-url gate is bound to a live out-of-band
// exchange and cannot outlive it for long.
//
// CHANGING A VALUE IS SAFE ACROSS VERSIONS for a gate already projected: the
// publisher never re-opens a projected gate, so a successor's different TTL is
// used only for a gate its predecessor never published.
func TTL(kind gate.Kind) time.Duration {
	switch kind {
	case gate.KindPermission:
		return 5 * time.Minute
	case gate.KindOpenURL:
		return time.Hour
	default:
		return 24 * time.Hour
	}
}
