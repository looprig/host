package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/livetext"
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
	Publications  Publications
	Routes        Routes
	FlushInterval time.Duration
	// NewFlushTimer allows a controlled clock for interval tests.
	NewFlushTimer func(time.Duration) FlushTimer
	Logger        *slog.Logger
}

// FlushTimer is the small timer contract used by the live-text relay.
type FlushTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type wallFlushTimer struct{ *time.Timer }

func (t wallFlushTimer) C() <-chan time.Time { return t.Timer.C }

// InvalidTailOptionsError reports an option a Tails may not run with.
type InvalidTailOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidTailOptionsError) Error() string {
	return "service: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Tails publishes committed session tails and admitted transient text onto
// HostLink channels.
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
//
// THE ONE EXCEPTION IS A Projector (finding W1). A runtime's canonical body
// names its PRIVATE runtime session and command ids, and every reader of this
// stream is a client's, so a composed Host rewrites exactly those identities
// before publishing. That is not a second projection in the sense above: it is
// a deterministic function of the stored body that the durable read applies
// too (host.PublicJournal), so live and durable bodies stay byte-identical.
type Tails struct {
	publications  Publications
	routes        Routes
	flushInterval time.Duration
	newFlushTimer func(time.Duration) FlushTimer
	logger        *slog.Logger
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
	if options.FlushInterval < 0 {
		return nil, &InvalidTailOptionsError{Field: "FlushInterval", Reason: "must not be negative"}
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = 50 * time.Millisecond
	}
	if options.NewFlushTimer == nil {
		options.NewFlushTimer = func(d time.Duration) FlushTimer { return wallFlushTimer{time.NewTimer(d)} }
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
	return &Tails{publications: options.Publications, routes: options.Routes, flushInterval: options.FlushInterval, newFlushTimer: options.NewFlushTimer, logger: options.Logger}, nil
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
	// A mixed subscription carries a typed cause when its adapter sees an
	// enduring delivery without committed fields; Tail.End surfaces that cause.
	// A bare channel close still carries no cause, so Host cannot distinguish
	// hub backpressure from all upstream invariant failures. It invalidates
	// routes in either case and never resubscribes on its own.
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

	// rewrite is the tail's public-body projection, or nil for none.
	rewrite Projector

	mu           sync.Mutex
	end          TailEnd
	cause        error
	published    uint64
	ephPublished uint64
	ephDrops     uint64
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

// Published reports how many enduring publications this tail put on the wire.
func (t *Tail) Published() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.published
}

// EphemeralPublished reports admitted transient frames separately from the durable count.
func (t *Tail) EphemeralPublished() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ephPublished
}

// EphemeralDrops reports transient frames discarded at the relay. The counter
// carries no body or text.
func (t *Tail) EphemeralDrops() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ephDrops
}

// ErrTargetGenerationSuperseded reports a target-directory write refused
// because a NEWER incarnation of this same HostID — a higher host generation —
// has already written the row.
//
// IT IS PERMANENT FOR THIS PROCESS, which is what separates it from every other
// directory failure. The row's generation mark only rises, so no retry by this
// incarnation can ever succeed, and the row this Host would withdraw is no
// longer this Host's: it says whatever the newer incarnation last published. A
// drain that treated it as an ordinary failure aborted before releasing a
// single session, and the residency leases it held were then never handed back
// (the v0.3.0 same-HostID overlap).
var ErrTargetGenerationSuperseded = errors.New("service: a newer generation of this HostID owns the target row, so this incarnation can no longer write it")

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
	return t.PublishProjected(ctx, key, subscriber, nil)
}

// Projector rewrites one committed public body before it is published, and
// nothing else about the publication. A nil Projector relays bodies verbatim.
//
// IT IS THE ONE EXCEPTION TO "THE BYTES ARE CARRIED", and it is one on purpose
// (finding W1): a runtime's body names the RUNTIME session and command ids,
// which are private, and every consumer of this stream is a client's. The
// exception is a DETERMINISTIC function of the stored body and immutable
// identities, applied identically to the durable read (see host.PublicJournal),
// so the live body still equals the one /journal serves for the same event.
// A projection that fails refuses the publication (TailEndRefused): relaying
// the unprojected body would leak, and skipping it would leave a hole.
//
// It is handed the body and the event's journal sequence, and nothing it could
// use to move the event: identity, sequence and coverage are relayed unchanged.
type Projector func(ctx context.Context, body json.RawMessage, journalSeq uint64) (json.RawMessage, error)

// PublishProjected is Publish with each body rewritten by rewrite first.
func (t *Tails) PublishProjected(
	ctx context.Context,
	key registry.Key,
	subscriber department.PublicationSubscriber,
	rewrite Projector,
	ephemeralRewrite ...Projector,
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
	var published <-chan sessionwire.EnduringPublication
	var live <-chan department.LivePublication
	var err error
	var transient Projector
	if len(ephemeralRewrite) > 0 {
		transient = ephemeralRewrite[0]
	}
	if source, ok := subscriber.(department.LivePublicationSubscriber); ok && transient != nil {
		live, err = source.SubscribeLivePublic(runCtx)
	} else {
		published, err = subscriber.SubscribeCommitted(runCtx, "")
	}
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
		rewrite: rewrite,
	}
	if live != nil {
		go t.relayLive(runCtx, tail, live, transient)
	} else {
		go t.relay(runCtx, tail, published)
	}
	return tail, nil
}

const (
	liveProjectionQueue = 16
)

type projectedLiveText struct {
	publication sessionwire.EphemeralPublication
	text        livetext.Delta
	ok          bool
}

// relayLive preserves one producer order. Its only pending transient frame is
// flushed before the next enduring frame and after a short idle interval.
func (t *Tails) relayLive(ctx context.Context, tail *Tail, stream <-chan department.LivePublication, transient Projector) {
	defer close(tail.done)
	var pending *sessionwire.EphemeralPublication
	var pendingText livetext.Delta
	gapped := map[string]bool{}
	textKey := func(d livetext.Delta) string { return d.LoopID + "\x00" + d.TurnID }
	var timer FlushTimer
	var flushAt <-chan time.Time
	var projected <-chan projectedLiveText
	var cancelProjection context.CancelFunc
	queued := make([]sessionwire.EphemeralPublication, 0, liveProjectionQueue)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		flushAt = nil
	}
	flush := func() {
		if pending == nil {
			return
		}
		stopTimer()
		if !t.publishEphemeral(tail, *pending) {
			if timer == nil {
				timer = t.newFlushTimer(t.flushInterval)
			} else {
				timer.Reset(t.flushInterval)
			}
			flushAt = timer.C()
			return
		}
		pending = nil
	}
	clearBoundary := func(body json.RawMessage) {
		loopID, ok := livetext.BoundaryLoop(body)
		if !ok {
			return
		}
		for key := range gapped {
			if loopID == "" || len(key) > len(loopID) && key[:len(loopID)+1] == loopID+"\x00" {
				delete(gapped, key)
			}
		}
	}
	startProjection := func(value sessionwire.EphemeralPublication) {
		workerCtx, cancel := context.WithCancel(ctx)
		cancelProjection = cancel
		result := make(chan projectedLiveText, 1)
		projected = result
		go func() {
			defer cancel()
			body, err := transient(workerCtx, value.Body, 0)
			if err != nil || workerCtx.Err() != nil {
				result <- projectedLiveText{}
				return
			}
			value.Body = body
			decoded, ok := livetext.Parse(body)
			ok = ok && decoded.SessionID == string(tail.key.SessionID)
			result <- projectedLiveText{publication: value, text: decoded, ok: ok}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			stopTimer()
			if cancelProjection != nil {
				cancelProjection()
			}
			tail.finish(TailEndStopped, ctx.Err())
			return
		case <-flushAt:
			if projected != nil || len(queued) != 0 {
				timer.Reset(t.flushInterval)
				flushAt = timer.C()
				continue
			}
			flush()
		case result := <-projected:
			cancelProjection()
			projected = nil
			cancelProjection = nil
			if !result.ok {
				tail.dropEphemeral()
			} else {
				value, decoded := result.publication, result.text
				key := textKey(decoded)
				if gapped[key] {
					tail.dropEphemeral()
					goto projectionDone
				}
				if !fitsEphemeralFrame(value) {
					gapped[key] = true
					tail.dropEphemeral()
					goto projectionDone
				}
				merged := false
				if pending != nil && textKey(pendingText) == key {
					body, ok := livetext.Merge(pending.Body, value.Body)
					candidate := *pending
					candidate.Body = body
					if ok && fitsEphemeralFrame(candidate) {
						pending.Body = body
						pendingText.Chunk.Text += decoded.Chunk.Text
						merged = true
					} else {
						gapped[key] = true
						pending = nil
						stopTimer()
						tail.dropEphemeral()
						tail.dropEphemeral()
						goto projectionDone
					}
				}
				if !merged {
					flush()
					if pending != nil {
						tail.dropEphemeral()
						goto projectionDone
					}
					pending = &value
					pendingText = decoded
					if timer == nil {
						timer = t.newFlushTimer(t.flushInterval)
					} else {
						timer.Reset(t.flushInterval)
					}
					flushAt = timer.C()
				}
			}
		projectionDone:
			if len(queued) != 0 {
				value := queued[0]
				queued = queued[1:]
				startProjection(value)
			}
		case publication, open := <-stream:
			if !open {
				stopTimer()
				if cancelProjection != nil {
					cancelProjection()
				}
				if ctx.Err() != nil {
					tail.finish(TailEndStopped, ctx.Err())
				} else {
					tail.finish(TailEndLost, nil)
					t.invalidate(tail)
				}
				return
			}
			if publication.Terminal != nil {
				t.logger.LogAttrs(ctx, slog.LevelError, "host: live publication stream ended on a runtime invariant breach", slog.String("tenant_id", string(tail.key.TenantID)), slog.String("session_id", string(tail.key.SessionID)), slog.String("error", publication.Terminal.Error()))
				tail.finish(TailEndLost, publication.Terminal)
				t.invalidate(tail)
				return
			}
			if publication.Enduring != nil {
				if cancelProjection != nil {
					cancelProjection()
					cancelProjection = nil
					projected = nil
					tail.dropEphemeral()
				}
				for range queued {
					tail.dropEphemeral()
				}
				queued = queued[:0]
				flush()
				if pending != nil {
					pending = nil
					stopTimer()
					tail.dropEphemeral()
				}
				if err := t.publish(ctx, tail, *publication.Enduring); err != nil {
					if ctx.Err() != nil {
						tail.finish(TailEndStopped, ctx.Err())
					} else {
						tail.finish(TailEndRefused, err)
						t.invalidate(tail)
					}
					return
				}
				clearBoundary(publication.Enduring.Body)
				continue
			}
			if publication.Ephemeral == nil || transient == nil {
				tail.dropEphemeral()
				continue
			}
			value := *publication.Ephemeral
			if value.TenantID != tail.key.TenantID || value.SessionID != tail.key.SessionID {
				tail.dropEphemeral()
				continue
			}
			if projected != nil {
				if len(queued) == liveProjectionQueue {
					tail.dropEphemeral()
				} else {
					queued = append(queued, value)
				}
				continue
			}
			startProjection(value)
		}
	}
}

// TryPublishEphemeral must return promptly and reserve transport headroom for
// enduring/control frames. A transport without this admission is disabled.
type ephemeralPublisher interface {
	TryPublishEphemeral(channel string, payload []byte) bool
}

func fitsEphemeralFrame(publication sessionwire.EphemeralPublication) bool {
	payload, err := json.Marshal(publication)
	return err == nil && len(payload) <= 4096
}

func (t *Tails) publishEphemeral(tail *Tail, publication sessionwire.EphemeralPublication) bool {
	transport, ok := t.publications.(ephemeralPublisher)
	if !ok {
		return false
	}
	payload, err := json.Marshal(publication)
	if err != nil || len(payload) > 4096 || !transport.TryPublishEphemeral(tail.channel, payload) {
		return false
	}
	tail.mu.Lock()
	tail.ephPublished++
	tail.mu.Unlock()
	return true
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
			if err := t.publish(ctx, tail, publication); err != nil {
				// A STOP DURING A PUBLISH IS A STOP. A Projector blocked on a
				// read returns the context's error when Host stops the tail,
				// and recording that as a refusal would invalidate routes
				// nobody lost (review F2).
				if ctx.Err() != nil {
					tail.finish(TailEndStopped, ctx.Err())
					return
				}
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
func (t *Tails) publish(ctx context.Context, tail *Tail, publication sessionwire.EnduringPublication) error {
	if publication.TenantID != tail.key.TenantID || publication.SessionID != tail.key.SessionID {
		return ErrForeignPublication
	}
	if tail.rewrite != nil {
		body, err := tail.rewrite(ctx, publication.Body, publication.JournalSeq)
		if err != nil {
			return err
		}
		publication.Body = body
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

func (t *Tail) dropEphemeral() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ephDrops++
}
