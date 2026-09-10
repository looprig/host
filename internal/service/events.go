package service

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// Publications is the transport one session's committed tail is published
// through.
//
// IT TAKES A CHANNEL AND A PAYLOAD AND NOTHING ELSE, which is how "Host sends
// one message per event and fans out nowhere" is a property of the TYPE rather
// than of this package's restraint: there is no parameter a link, a replica or
// a subscriber set could ride in, so no future caller can address one even by
// accident. The fan-out belongs to the transport, which owns a queue per
// physical connection; Host owns the decision to publish once.
//
// The shape is centrifuge's own — a channel, bytes, no context — because the
// implementation is a call straight through to it, and a seam carrying a
// context nobody could honour would be describing a cancellation that does not
// exist.
type Publications interface {
	// Publish delivers one payload to every subscriber of one channel.
	Publish(channel string, payload []byte) error
}

// Routes is the routing table a lost tail invalidates, narrowed to the single
// method a tail may reach.
//
// It is ONE method and that method is session-scoped, for the reason
// Residencies is one read: a publisher that could bind, deliver or close a link
// would be able to repair its own loss by re-routing it, and the whole point of
// invalidating is that the repair is Factory's — a durable reset from its own
// cursor, not a hidden retry here. *hostlink.Multiplexer satisfies it; the
// conformance assertion lives in the test so this package does not need it at
// build time.
type Routes interface {
	// InvalidateSession drops every link's route to one session and returns
	// them.
	InvalidateSession(registry.Key) []hostlink.Binding
}

// TailOptions configures a Tails.
type TailOptions struct {
	Publications Publications
	Routes       Routes
}

// InvalidTailOptionsError reports an option a Tails may not run with.
type InvalidTailOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidTailOptionsError) Error() string {
	return "service: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Tails publishes committed session tails onto HostLink channels.
//
// WHAT IT DOES NOT DO IS THE SUBSTANCE. It does not read a journal, does not
// project an event, does not re-encode a body, and does not decide what a
// public body IS. Harness's committed-public-event capability supplies the
// exact canonical body the durable append stored, together with the EventID,
// JournalSeq and CoveredThrough that append committed under; this type relays
// all five unchanged. A second projection would be a second answer: a consumer
// joining its durable tail to this live stream dedupes on (sequence, event id)
// and compares bodies, and if the live body were re-derived it could differ
// from the stored one in key order or in a field a later projector added, with
// no error anywhere.
//
// Core's own EnduringPublication says the same from the other side: its
// MarshalJSON "keeps the canonical public body opaque … a Host relays
// that committed body rather than decoding and synthesizing it anew"
// (core@v0.7.0 sessionwire/v1/publications.go:152-154). Body is a
// json.RawMessage on both sides of this package, so the bytes are carried, not
// parsed.
type Tails struct {
	publications Publications
	routes       Routes
}

// NewTails validates the options and returns a publisher.
func NewTails(options TailOptions) (*Tails, error) {
	if options.Publications == nil {
		return nil, &InvalidTailOptionsError{
			Field:  "Publications",
			Reason: "is required; a tail with nowhere to publish is not a tail",
		}
	}
	if options.Routes == nil {
		return nil, &InvalidTailOptionsError{
			Field:  "Routes",
			Reason: "is required; a lost tail must invalidate its routes so Factory resets durably",
		}
	}
	return &Tails{publications: options.Publications, routes: options.Routes}, nil
}

// TailEnd is why one tail stopped.
type TailEnd string

const (
	// TailEndRunning reports a tail that has not ended.
	TailEndRunning TailEnd = "running"

	// TailEndStopped reports a tail this Host ended: the caller stopped it or
	// the context it was started under is done. It is the ONE ending that does
	// not invalidate a route, because the route did not fail — Host chose to
	// stop feeding it, and whoever chose that owns what happens to the route.
	TailEndStopped TailEnd = "stopped"

	// TailEndLost reports a tail that ended without Host asking: the runtime
	// stopped, the hub failed the subscription for an enduring overflow, or
	// Host's own egress overflowed. Every one of them means this Host can no
	// longer feed the live stream, so the routes are invalidated.
	//
	// THE CAUSES ARE NOT DISTINGUISHED, and that is a limit of the seam rather
	// than a decision taken here. department.PublicationSubscriber hands back a
	// channel and no termination cause, so Host sees a closed channel and
	// cannot tell egress overflow — which harness documents as retryable by
	// resubscribing — from hub.ErrCommittedBodyMissing or
	// hub.ErrCommitEventMismatch, which are broken invariants a resubscribe
	// loops forever against. Host therefore takes the safe intersection of the
	// two policies: invalidate, and never resubscribe on its own.
	TailEndLost TailEnd = "lost"

	// TailEndRefused reports a publication this Host would not put on the wire:
	// one naming another session, one Core's own encoder rejects, or one the
	// transport refused. It invalidates for the same reason a loss does — the
	// stream has a hole in it — and it is a separate value because the repair
	// is different and an operator must not read one as the other.
	TailEndRefused TailEnd = "refused"
)

// Tail is one running publisher for one session.
type Tail struct {
	key     registry.Key
	channel string

	stop context.CancelFunc
	done chan struct{}

	mu        sync.Mutex
	end       TailEnd
	cause     error
	published uint64
}

// Key is the session this tail publishes.
func (t *Tail) Key() registry.Key { return t.key }

// Channel is the HostLink channel this tail publishes on. It is minted by
// hostlink.ChannelFor and never parsed here.
func (t *Tail) Channel() string { return t.channel }

// Done closes when this tail has stopped publishing.
func (t *Tail) Done() <-chan struct{} { return t.done }

// Stop ends this tail. It is idempotent, and it does NOT invalidate the routes:
// see TailEndStopped.
func (t *Tail) Stop() { t.stop() }

// End reports why this tail stopped, and the underlying cause when there is
// one. It is TailEndRunning until Done closes.
func (t *Tail) End() (TailEnd, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.end, t.cause
}

// Published reports how many publications this tail put on the wire.
func (t *Tail) Published() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.published
}

// ErrForeignPublication is the refusal for a publication naming a session other
// than the one the tail was started for.
//
// It is fail-closed rather than a filter. The identities are stamped by the
// runtime adapter from the session it bound, so a mismatch is not a stray
// delivery to skip past — it means this tail's stream and its key disagree
// about whose events these are, and publishing either interpretation onto a
// channel derived from the key would put one session's events on another's
// route.
var ErrForeignPublication = errors.New("service: the tail delivered a publication naming another session")

// Publish subscribes to a session's committed tail and relays it onto that
// session's HostLink channel until the tail ends.
//
// The subscription is opened from the LIVE TAIL and cannot be positioned; the
// runtime adapter refuses a non-empty resume point, and this call never
// supplies one. A Factory that needs earlier events reads them durably.
func (t *Tails) Publish(
	ctx context.Context,
	key registry.Key,
	subscriber department.PublicationSubscriber,
) (*Tail, error) {
	if subscriber == nil {
		return nil, &InvalidTailOptionsError{
			Field:  "PublicationSubscriber",
			Reason: "is required; there is no tail without a runtime to subscribe to",
		}
	}
	if err := key.TenantID.Validate(); err != nil {
		return nil, &InvalidTailOptionsError{Field: "TenantID", Reason: "must be a valid Core tenant ID"}
	}
	if err := key.SessionID.Validate(); err != nil {
		return nil, &InvalidTailOptionsError{Field: "SessionID", Reason: "must be a valid Core session ID"}
	}

	runCtx, cancel := context.WithCancel(ctx)
	published, err := subscriber.SubscribeCommitted(runCtx, "")
	if err != nil {
		cancel()
		return nil, err
	}
	tail := &Tail{
		key:     key,
		channel: hostlink.ChannelFor(key),
		stop:    cancel,
		done:    make(chan struct{}),
		end:     TailEndRunning,
	}
	go t.relay(runCtx, tail, published)
	return tail, nil
}

// relay is one session's publishing goroutine.
//
// ONE GOROUTINE PER SESSION IS THE ISOLATION, and it is the only isolation this
// layer needs to provide. Every publication for a session is published ONCE, so
// no per-replica work happens here at all and a slow replica cannot be a slow
// path — the transport holds a queue per physical connection and a full one
// closes that connection alone. What could still couple two sessions is a
// shared goroutine, which is why there is not one.
func (t *Tails) relay(
	ctx context.Context,
	tail *Tail,
	published <-chan sessionwire.EnduringPublication,
) {
	defer close(tail.done)
	for {
		select {
		case <-ctx.Done():
			tail.finish(TailEndStopped, ctx.Err())
			return
		case publication, open := <-published:
			if !open {
				// The stream ended and Host did not ask it to. Read the context
				// once more: a cancellation races the close, and attributing
				// Host's own stop to a loss would invalidate routes nobody lost.
				if ctx.Err() != nil {
					tail.finish(TailEndStopped, ctx.Err())
					return
				}
				tail.finish(TailEndLost, nil)
				t.invalidate(tail)
				return
			}
			if err := t.publish(tail, publication); err != nil {
				tail.finish(TailEndRefused, err)
				t.invalidate(tail)
				return
			}
		}
	}
}

// publish puts ONE publication on the wire, once.
//
// The encoding is Core's. json.Marshal reaches EnduringPublication.MarshalJSON,
// which validates the record and writes Body through unchanged as a
// json.RawMessage, so the bytes the durable append stored are the bytes on the
// wire. A record Core refuses is not published at all: an unencodable
// publication is a hole in the stream, and a Factory must learn that from a
// reset rather than from a body it cannot parse.
func (t *Tails) publish(tail *Tail, publication sessionwire.EnduringPublication) error {
	if publication.TenantID != tail.key.TenantID || publication.SessionID != tail.key.SessionID {
		return ErrForeignPublication
	}
	payload, err := json.Marshal(publication)
	if err != nil {
		return err
	}
	if err := t.publications.Publish(tail.channel, payload); err != nil {
		return err
	}
	tail.count()
	return nil
}

// invalidate drops every route to this tail's session, which is what obliges
// each Factory replica to reset durably from its own cursor.
func (t *Tails) invalidate(tail *Tail) { t.routes.InvalidateSession(tail.key) }

// finish records the first ending and leaves a later one alone, so a stop
// racing a loss does not overwrite the reason the routes were invalidated for.
func (t *Tail) finish(end TailEnd, cause error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.end != TailEndRunning {
		return
	}
	t.end = end
	t.cause = cause
}

// count records one publication on the wire.
func (t *Tail) count() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.published++
}
