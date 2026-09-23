package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/hub"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/rig"
	"github.com/looprig/harness/pkg/session"
	harnessstore "github.com/looprig/harness/pkg/sessionstore"
	"github.com/looprig/harness/pkg/workspacestore"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/harnessadapter"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/service"
)

// THE ROUTING TABLE IS THE REAL ONE, asserted at build time.
//
// A hand-written Routes fake would be free to answer in ways the Multiplexer
// cannot — to invalidate a link instead of a session, or to report routes it
// never held — and every step-3 assertion below would then be about a table
// that does not exist. This line is what makes the seam a narrowing of
// production rather than a description of it, and it is the same idiom
// hostlink's own test uses for its three collaborators.
var _ service.Routes = (*hostlink.Multiplexer)(nil)

// ---------------------------------------------------------------------------
// The transport seam
// ---------------------------------------------------------------------------

// recordingPublications records every message Host put on the wire, in order.
//
// It records the CHANNEL with each payload rather than only the payload,
// because "published once per event on the session's channel" is a claim about
// the pair: a build that published every event twice on one channel and a build
// that published it once on each of two channels are different defects and are
// indistinguishable from a payload list alone.
type recordingPublications struct {
	mu       sync.Mutex
	messages []publishedMessage
	err      error
	block    chan struct{}
}

type publishedMessage struct {
	channel string
	payload []byte
}

func (p *recordingPublications) Publish(channel string, payload []byte) error {
	p.mu.Lock()
	err, block := p.err, p.block
	p.mu.Unlock()
	if block != nil {
		<-block
	}
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, publishedMessage{channel: channel, payload: append([]byte(nil), payload...)})
	return nil
}

func (p *recordingPublications) recorded() []publishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishedMessage(nil), p.messages...)
}

func (p *recordingPublications) failWith(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// recordingRoutes records the sessions whose routes were invalidated.
type recordingRoutes struct {
	mu          sync.Mutex
	invalidated []registry.Key
}

func (r *recordingRoutes) InvalidateSession(key registry.Key) []hostlink.Binding {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidated = append(r.invalidated, key)
	return nil
}

// sessions returns every key whose routes were invalidated, in order.
func (r *recordingRoutes) sessions() []registry.Key {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registry.Key(nil), r.invalidated...)
}

func newTails(t *testing.T, publications service.Publications, routes service.Routes) *service.Tails {
	t.Helper()
	tails, err := service.NewTails(service.TailOptions{Publications: publications, Routes: routes})
	if err != nil {
		t.Fatalf("NewTails: %v", err)
	}
	return tails
}

// ---------------------------------------------------------------------------
// A live harness session over a real journal
// ---------------------------------------------------------------------------

// liveSession is a REAL hub over a REAL committed-bytes journal, reached through
// the REAL runtime adapter.
//
// Every part of the path that decides what Host publishes is production code:
// harness's JournalEventAppender writes the canonical public body and reports
// it, harness's Hub carries it forward on the committed stream, and Host's
// harnessadapter translates it into a Core publication. Nothing between the
// durable append and the assertion is a fake, which is the only way a
// byte-identity claim can mean anything — a fake that returned the same bytes
// it was handed would satisfy it by construction.
//
// WHAT IS A FAKE, AND WHY IT CANNOT MATTER HERE. The session CONTROLLER is a
// fake, because a *rig.Rig needs loops, primers and a model adapter no unit test
// can supply. It contributes nothing to the value under test: it answers
// CommittedPublicEvents with the real hub and every other SessionController
// method with a refusal, so the bytes still come from the journal.
//
// WHAT THIS FIXTURE IS NOT. It is NOT Host's attach path. A disposition-mode
// session cannot be attached end-to-end through residency.Manager today —
// today's fused Grant is refused by every disposition session, which is booked
// as task O3.4 — so this subscribes over a session created directly, exactly as
// the bundle-B assumption requires and no further.
type liveSession struct {
	key     registry.Key
	runtime department.PublicationSubscriber
	cold    *sessionstore.Store
	hub     *hub.Hub
	rigID   uuid.UUID
	factory *event.Factory
}

// newLiveSession assembles one session whose SessionID is the rig UUID's
// canonical rendering.
//
// THAT IDENTITY IS A CONSTRAINT OF THE SHARED BACKEND, NOT A HOST CONTRACT, and
// it is stated here so no assertion below is read as more than it is. On the
// legacy single-tenant layout the cold reader addresses a session by the Harness
// UUID and refuses anything else with KeyspaceError{legacy_session}; the
// session identity Host holds is the opaque string Factory admitted it under,
// and is not a UUID by contract. The cold reader in these tests therefore stands in for
// Factory's binding-routed durable read, and stands in for it only where the two
// identities coincide.
func newLiveSession(t *testing.T, tenant sessionwire.TenantID) *liveSession {
	t.Helper()
	backend := memstore.New()
	rigID := newUUID(t)

	harnessSide, err := harnessstore.Open(backend, harnessstore.WithTenant(tenant))
	if err != nil {
		t.Fatalf("open the harness session store: %v", err)
	}
	lease, err := harnessSide.AcquireLease(t.Context(), rigID)
	if err != nil {
		t.Fatalf("acquire the harness lease: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.WithoutCancel(t.Context())) })
	sessionJournal, err := harnessSide.OpenJournal(t.Context(), rigID, lease)
	if err != nil {
		t.Fatalf("open the harness journal: %v", err)
	}
	appender, err := journal.NewJournalEventAppenderChecked(sessionJournal)
	if err != nil {
		t.Fatalf("build the committed appender: %v", err)
	}
	// THE CAPABILITY IS CONSULTED, NOT ASSUMED. A journal without the
	// committed-bytes seam yields an appender that answers false here, the hub
	// then refuses the subscription, and every assertion below would fail for a
	// reason that has nothing to do with its subject.
	if !appender.SupportsCommittedPublicBodies() {
		t.Fatal("the journal under this fixture does not report stored canonical bodies, so nothing below is testing byte identity")
	}
	sessionHub := hub.New(rigID, hub.WithAppender(appender))

	adapter, err := harnessadapter.New(stubRigs{launcher: &stubLauncher{
		controller: &liveController{base: controllerBase{id: rigID}, hub: sessionHub},
	}})
	if err != nil {
		t.Fatalf("harnessadapter.New: %v", err)
	}
	key := registry.Key{TenantID: tenant, SessionID: sessionwire.SessionID(rigID.String())}
	launched, err := adapter.NewSession(t.Context(), department.RigCreateRequest{
		TenantID:  key.TenantID,
		SessionID: key.SessionID,
		AgentID:   "agent-a",
	})
	if err != nil {
		t.Fatalf("launch the session through the runtime adapter: %v", err)
	}
	// ASSERTED, NOT ASSUMED. department discovers the capability the same way,
	// and a launched session that did not carry it would otherwise reach Publish
	// as a nil interface and fail there for the wrong reason.
	runtime, ok := launched.(department.PublicationSubscriber)
	if !ok {
		t.Fatalf("the launched session is %T, which carries no committed tail", launched)
	}

	cold, err := sessionstore.Open(t.Context(), backend, sessionstore.WithLegacySingleTenant(tenant))
	if err != nil {
		t.Fatalf("open the cold public reader: %v", err)
	}
	t.Cleanup(func() { _ = cold.Close(context.WithoutCancel(t.Context())) })

	return &liveSession{
		key:     key,
		runtime: runtime,
		cold:    cold,
		hub:     sessionHub,
		rigID:   rigID,
		factory: event.NewFactory(uuid.New, time.Now),
	}
}

// appendPublicEvent durably appends one public enduring event and returns it.
func (s *liveSession) appendPublicEvent(t *testing.T) event.SessionStarted {
	t.Helper()
	header, err := s.factory.Stamp(event.Header{
		Coordinates: identity.Coordinates{SessionID: s.rigID},
	})
	if err != nil {
		t.Fatalf("stamp an event header: %v", err)
	}
	ev := event.SessionStarted{Header: header}
	if err := s.hub.PublishEventChecked(t.Context(), ev); err != nil {
		t.Fatalf("publish a public enduring event: %v", err)
	}
	return ev
}

// coldPage reads this session's whole public journal the way a reconnecting
// Factory would.
func (s *liveSession) coldPage(t *testing.T) sessionwire.JournalPage {
	t.Helper()
	page, err := s.cold.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{
		TenantID:  s.key.TenantID,
		SessionID: s.key.SessionID,
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("cold public read: %v", err)
	}
	return page
}

// ---------------------------------------------------------------------------
// Step 1 and step 2
// ---------------------------------------------------------------------------

// TestAPublishedTailIsTheCommittedJournalRecordItself is steps 1 and 2.
//
// It asserts four separate things about ONE event, because each of them can
// hold while another fails: that the event is already durable when Host
// publishes it, that the publication carries the EventID and journal sequence
// the append committed under, that the body Host put on the wire is BYTE-
// IDENTICAL to what a cold public read returns, and that Host published it once
// and on the session's own channel.
//
// THE COLD READ IS ASSERTED ON CONTENT, NOT ON THE ABSENCE OF AN ERROR.
// ReadPublicJournal answers an EMPTY PAGE without error for a session absent
// from the scope, so `err == nil` proves only that the request was well formed.
// The control at the end reads a session identity nothing ever created and
// requires an empty page, so "the page had my event in it" cannot be satisfied
// by a reader that answers every address alike.
//
// WHAT IT IS BOUNDED TO: an INLINE body. The released cold reader refuses a
// public reference whose SizeBytes exceeds the envelope's inline ceiling and
// fails the whole PAGE rather than the record, so byte identity is claimed here
// for bodies the read can serve and for no others. The live delivery is the
// strictly more available of the two sources; that asymmetry is harness's, is
// documented on event.Delivery.PublicBody, and is not repaired by anything here.
func TestAPublishedTailIsTheCommittedJournalRecordItself(t *testing.T) {
	t.Parallel()
	live := newLiveSession(t, "tenant-alpha")
	publications := &recordingPublications{}
	routes := &recordingRoutes{}

	tail, err := newTails(t, publications, routes).Publish(t.Context(), live.key, live.runtime)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(tail.Stop)

	appended := live.appendPublicEvent(t)
	waitFor(t, "exactly 1 message(s) on the wire", func() bool { return len(publications.recorded()) == 1 })

	// (a) IT IS ALREADY DURABLE. The cold read is taken AFTER the publication
	// was observed on the wire, and it must already contain the event: a live
	// stream that ran ahead of its own journal would let a Factory advance a
	// cursor past a record no durable read can serve.
	page := live.coldPage(t)
	if len(page.Events) != 1 {
		t.Fatalf("the cold public read returned %d events, want the one that was published; a live publication must not precede its durable record", len(page.Events))
	}
	stored := page.Events[0]

	message := publications.recorded()[0]
	if message.channel != hostlink.ChannelFor(live.key) {
		t.Fatalf("published on %q, want the session's channel %q", message.channel, hostlink.ChannelFor(live.key))
	}
	var published sessionwire.EnduringPublication
	if err := json.Unmarshal(message.payload, &published); err != nil {
		t.Fatalf("the payload Host put on the wire is not a Core publication: %v", err)
	}

	// (b) THE COMMITTED IDENTITIES. The EventID is compared against the
	// JOURNAL'S copy, which is the only comparison that can fail for the right
	// reason.
	//
	// MEASURED, AND IT CORRECTS AN ASSUMPTION THIS TEST WAS FIRST WRITTEN ON.
	// event.Delivery documents the committed EventID as "deliberately a distinct
	// FIELD from Header.EventID" — a distinct field, not a distinct VALUE. The
	// two carry the same identity: hub.deliver states the invariant as an
	// equality it enforces, "a public projection stamps its EventID from the
	// event's own header" (hub/hub.go:634-641), and fails every subscriber with
	// ErrCommitEventMismatch when it does not hold. So an assertion that they
	// DIFFER is false against the released module, and it was written and then
	// deleted here rather than adjusted, because a test asserting the opposite of
	// the contract passes only while a defect is present.
	if published.EventID != stored.EventID {
		t.Fatalf("published EventID %q, want the committed %q", published.EventID, stored.EventID)
	}
	if published.EventID != sessionwire.EventID(appended.EventID.String()) {
		t.Fatalf("published EventID %q, want it to equal the runtime header's %q; the hub enforces that equality and fails the stream when it breaks", published.EventID, appended.EventID)
	}
	if published.JournalSeq != stored.JournalSeq {
		t.Fatalf("published JournalSeq %d, want the committed %d", published.JournalSeq, stored.JournalSeq)
	}
	if published.JournalSeq == 0 {
		t.Fatal("published JournalSeq is 0; an enduring publication names the sequence it was appended at")
	}
	if published.CoveredThrough != published.JournalSeq {
		t.Fatalf("published CoveredThrough %d, want it to equal JournalSeq %d", published.CoveredThrough, published.JournalSeq)
	}
	if published.TenantID != live.key.TenantID || published.SessionID != live.key.SessionID {
		t.Fatalf("published identities %q/%q, want the admitted pair %q/%q", published.TenantID, published.SessionID, live.key.TenantID, live.key.SessionID)
	}

	// (c) BYTE IDENTITY, on the bytes and not on a decoded shape. Two JSON
	// documents that decode alike can differ in key order and in whitespace, and
	// a consumer joining a durable tail to this live stream compares the bodies.
	if !slices.Equal([]byte(published.Body), []byte(stored.Body)) {
		t.Fatalf("published body\n  %s\ndiffers from the cold read's\n  %s", published.Body, stored.Body)
	}
	if len(published.Body) == 0 {
		t.Fatal("the published body is empty, so byte identity above compared nothing")
	}

	// (d) ONCE. A second publication of the same event, or a second channel,
	// would already have failed the length check; this states it as its own
	// claim and gives the counter something to have been wrong about.
	if got := tail.Published(); got != 1 {
		t.Fatalf("the tail published %d times for one event, want once", got)
	}
	if got := len(publications.recorded()); got != 1 {
		t.Fatalf("%d messages reached the transport for one event, want one", got)
	}
	if got := routes.sessions(); len(got) != 0 {
		t.Fatalf("a healthy tail invalidated %v", got)
	}

	// THE CONTROL FOR (a) AND (c). A session identity nothing created comes back
	// with an empty page and no error, which is exactly why the assertions above
	// are on content.
	empty, err := live.cold.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{
		TenantID:  live.key.TenantID,
		SessionID: sessionwire.SessionID(newUUID(t).String()),
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("the control read errored rather than answering an empty page: %v", err)
	}
	if len(empty.Events) != 0 || empty.CapturedTip != 0 {
		t.Fatalf("a session nothing created answered %d events at tip %d; the reader answers every address alike and the page assertions above prove nothing", len(empty.Events), empty.CapturedTip)
	}
}

// TestEveryCommittedEventIsPublishedOnceAndInOrder is the multi-event form of
// the same claim: N events produce exactly N messages, on one channel, in
// journal order.
//
// It exists because the single-event test cannot separate "published once" from
// "published at all". A build that published only the first event, or that
// re-published the newest event on every delivery, passes the single-event
// assertions and fails here.
func TestEveryCommittedEventIsPublishedOnceAndInOrder(t *testing.T) {
	t.Parallel()
	live := newLiveSession(t, "tenant-alpha")
	publications := &recordingPublications{}
	tail, err := newTails(t, publications, &recordingRoutes{}).Publish(t.Context(), live.key, live.runtime)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(tail.Stop)

	const events = 5
	for range events {
		live.appendPublicEvent(t)
	}
	waitFor(t, "exactly one message per appended event on the wire", func() bool { return len(publications.recorded()) == events })

	page := live.coldPage(t)
	if len(page.Events) != events {
		t.Fatalf("the cold read holds %d events, want %d", len(page.Events), events)
	}
	messages := publications.recorded()
	if len(messages) != events {
		t.Fatalf("%d messages reached the transport, want exactly %d", len(messages), events)
	}
	channel := hostlink.ChannelFor(live.key)
	var previous uint64
	for i, message := range messages {
		if message.channel != channel {
			t.Fatalf("message %d went to %q, want the one session channel %q", i, message.channel, channel)
		}
		var published sessionwire.EnduringPublication
		if err := json.Unmarshal(message.payload, &published); err != nil {
			t.Fatalf("message %d is not a Core publication: %v", i, err)
		}
		if published.JournalSeq <= previous {
			t.Fatalf("message %d carries sequence %d, which does not advance on %d", i, published.JournalSeq, previous)
		}
		previous = published.JournalSeq
		if published.EventID != page.Events[i].EventID {
			t.Fatalf("message %d carries %q, want the journal's %q at that position", i, published.EventID, page.Events[i].EventID)
		}
		if !slices.Equal([]byte(published.Body), []byte(page.Events[i].Body)) {
			t.Fatalf("message %d body\n  %s\ndiffers from the cold read's\n  %s", i, published.Body, page.Events[i].Body)
		}
	}
}

// TestHostHoldsNoHandleThatCouldFanOutIsAStructuralClaim holds the second half
// of step 2 — "fanned out nowhere else by Host" — as a property of the TYPES
// rather than of a call count.
//
// A count of one is evidence about one build. What makes fan-out unrepresentable
// is that the only two collaborators a Tails holds are single-method interfaces,
// neither of which can name a link, a replica or a subscriber: Publications
// takes a channel and bytes, Routes takes a session key. There is no value here
// through which a per-replica send could be expressed even deliberately.
//
// WHAT IT DOES NOT CLAIM. It says nothing about what the TRANSPORT does with the
// one message, which is where "once per interested Factory replica" actually
// lives. See the package-level note in this file's TestReplicaFanOutIsTheTransports.
func TestHostHoldsNoHandleThatCouldFanOutIsAStructuralClaim(t *testing.T) {
	t.Parallel()
	for name, methods := range map[string][]string{
		"service.Publications": methodNames(t, (*service.Publications)(nil)),
		"service.Routes":       methodNames(t, (*service.Routes)(nil)),
	} {
		if len(methods) != 1 {
			t.Fatalf("%s has methods %v, want exactly one; a second method is a second thing a tail can reach", name, methods)
		}
	}
	// The floor, stated: this is about the declared seams, and a Tails that
	// acquired a THIRD collaborator would not be caught here. The exported-API
	// whitelist in TestPublishIsTheOnlyPublishingSurface is what catches a new
	// exported declaration; a new unexported field is caught by neither.
	if got := methodNames(t, (*service.Publications)(nil)); !slices.Equal(got, []string{"Publish"}) {
		t.Fatalf("Publications exposes %v, want only Publish", got)
	}
	if got := methodNames(t, (*service.Routes)(nil)); !slices.Equal(got, []string{"InvalidateSession"}) {
		t.Fatalf("Routes exposes %v, want only InvalidateSession", got)
	}
}

// ---------------------------------------------------------------------------
// Step 3
// ---------------------------------------------------------------------------

// TestALostTailInvalidatesItsRoutesAndPublishesNothingFurther is step 3's Host
// half.
//
// THE SPACE OF ENDINGS IS DERIVED FROM THE SEAM, not from the conditions the
// runbook names. department.PublicationSubscriber ends a tail by closing its
// channel, and there are exactly two ways that can happen: Host asked (its
// context ended), or it did not. The runbook names one member of the second
// class — an enduring queue overflow — and the mechanism has three others: the
// hub failing the subscription for an overflow of ITS egress, the hub failing it
// for a broken committed-bytes invariant, and Host's own egress overflowing in
// the runtime adapter. All four arrive here identically and all four invalidate.
//
// WHAT HOST CANNOT DO AND THEREFORE DOES NOT: tell them apart. Harness
// documents an egress-overflow loss as retryable by resubscribing and a
// ErrCommittedBodyMissing / ErrCommitEventMismatch loss as a broken invariant a
// resubscribe loops forever against. The channel carries no cause, so Host takes
// the safe intersection — invalidate, never resubscribe — and says so in
// TailEndLost's own documentation rather than pretending to a distinction.
func TestALostTailInvalidatesItsRoutesAndPublishesNothingFurther(t *testing.T) {
	t.Parallel()
	live := newLiveSession(t, "tenant-alpha")
	publications := &recordingPublications{}
	routes := &recordingRoutes{}
	tail, err := newTails(t, publications, routes).Publish(t.Context(), live.key, live.runtime)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// POSITIVE FIRST: the tail works, from a counter that was zero. Everything
	// after this is a claim about a loss, and a loss test that never established
	// delivery would pass against a build that never delivered.
	live.appendPublicEvent(t)
	waitFor(t, "exactly 1 message(s) on the wire", func() bool { return len(publications.recorded()) == 1 })
	if got := routes.sessions(); len(got) != 0 {
		t.Fatalf("routes were invalidated while the tail was healthy: %v", got)
	}

	// THE LOSS IS THE HUB'S OWN VALUE, delivered down the hub's own path.
	// AbortSession fails every subscription with the cause it is handed, and the
	// cause handed here is exactly what an ENDURING EGRESS OVERFLOW raises —
	// *hub.SubscriptionLossError with a nil Cause (hub/subscription.go:16-29).
	// Reaching that state by wedging a consumer and pushing 257 events would
	// exercise one more of harness's lines and none of Host's, and would make
	// this test's outcome depend on a timing window rather than on the value
	// under test.
	live.hub.AbortSession(&hub.SubscriptionLossError{DroppedClass: event.Enduring})

	select {
	case <-tail.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the tail did not end when its stream did")
	}
	end, cause := tail.End()
	if end != service.TailEndLost {
		t.Fatalf("tail ended as %q (%v), want %q: a stream that ended without Host asking is a loss", end, cause, service.TailEndLost)
	}
	if got := routes.sessions(); !slices.Equal(got, []registry.Key{live.key}) {
		t.Fatalf("invalidated %v, want exactly this session's routes", got)
	}
	if got := tail.Published(); got != 1 {
		t.Fatalf("the tail published %d times, want the one it delivered before the loss", got)
	}
}

// TestAStoppedTailDoesNotInvalidateItsRoutes is the control for the test above
// and a claim in its own right.
//
// Without it "a lost tail invalidates" is satisfied by a build that invalidates
// on EVERY ending, which would mean an ordinary Host-initiated teardown told
// every Factory replica to perform a durable reset it did not need. The two
// endings are opposite and the routing consequence is the whole difference.
func TestAStoppedTailDoesNotInvalidateItsRoutes(t *testing.T) {
	t.Parallel()
	live := newLiveSession(t, "tenant-alpha")
	publications := &recordingPublications{}
	routes := &recordingRoutes{}
	tail, err := newTails(t, publications, routes).Publish(t.Context(), live.key, live.runtime)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	live.appendPublicEvent(t)
	waitFor(t, "exactly 1 message(s) on the wire", func() bool { return len(publications.recorded()) == 1 })

	tail.Stop()
	tail.Stop() // idempotent
	select {
	case <-tail.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("a stopped tail did not finish")
	}
	if end, cause := tail.End(); end != service.TailEndStopped {
		t.Fatalf("tail ended as %q (%v), want %q", end, cause, service.TailEndStopped)
	}
	if got := routes.sessions(); len(got) != 0 {
		t.Fatalf("stopping a tail invalidated %v; the routes did not fail, Host stopped feeding them", got)
	}
}

// TestARefusedPublicationEndsTheTailAndInvalidates covers the endings that are
// Host's own refusal rather than the stream's: a transport that will not take
// the message, and a publication naming another session.
//
// Both leave a HOLE in what a Factory has been fed, which is the same condition
// a loss leaves and calls for the same repair — but the operator's repair
// differs, so the ending is a separate value rather than reusing TailEndLost.
func TestARefusedPublicationEndsTheTailAndInvalidates(t *testing.T) {
	t.Parallel()

	t.Run("the transport refuses the message", func(t *testing.T) {
		t.Parallel()
		live := newLiveSession(t, "tenant-alpha")
		refusal := errors.New("the transport is not accepting publications")
		publications := &recordingPublications{}
		routes := &recordingRoutes{}
		tail, err := newTails(t, publications, routes).Publish(t.Context(), live.key, live.runtime)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		t.Cleanup(tail.Stop)

		live.appendPublicEvent(t)
		waitFor(t, "exactly 1 message(s) on the wire", func() bool { return len(publications.recorded()) == 1 })
		publications.failWith(refusal)
		live.appendPublicEvent(t)

		select {
		case <-tail.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("the tail survived a transport refusal")
		}
		end, cause := tail.End()
		if end != service.TailEndRefused || !errors.Is(cause, refusal) {
			t.Fatalf("tail ended as %q (%v), want %q carrying the transport's error", end, cause, service.TailEndRefused)
		}
		if got := routes.sessions(); !slices.Equal(got, []registry.Key{live.key}) {
			t.Fatalf("invalidated %v, want exactly this session's routes", got)
		}
		if got := tail.Published(); got != 1 {
			t.Fatalf("the tail counted %d publications, want only the one that actually reached the wire", got)
		}
	})

	t.Run("the stream delivers another session's publication", func(t *testing.T) {
		t.Parallel()
		publications := &recordingPublications{}
		routes := &recordingRoutes{}
		key := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
		stream := make(chan sessionwire.EnduringPublication, 1)
		stream <- sessionwire.EnduringPublication{
			TenantID:       key.TenantID,
			SessionID:      "session-b",
			EventID:        "event-1",
			JournalSeq:     1,
			CoveredThrough: 1,
			Body:           json.RawMessage(`{"type":"session_started"}`),
		}
		tail, err := newTails(t, publications, routes).Publish(t.Context(), key, scriptedSubscriber{stream: stream})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		t.Cleanup(tail.Stop)

		select {
		case <-tail.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("a foreign publication did not end the tail")
		}
		end, cause := tail.End()
		if end != service.TailEndRefused || !errors.Is(cause, service.ErrForeignPublication) {
			t.Fatalf("tail ended as %q (%v), want %q carrying ErrForeignPublication", end, cause, service.TailEndRefused)
		}
		if got := publications.recorded(); len(got) != 0 {
			t.Fatalf("another session's event reached the wire: %v", got)
		}
		if got := routes.sessions(); !slices.Equal(got, []registry.Key{key}) {
			t.Fatalf("invalidated %v, want exactly this tail's session", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Step 4
// ---------------------------------------------------------------------------

// TestOneStuckSessionBlocksNeitherAnotherSessionNorAnotherReplica is step 4.
//
// THE FIXTURE STOPS THE TRANSPORT, which is the only place a slow Factory can
// be modelled from inside this package: Host publishes ONCE per event and never
// per replica, so "a slow replica" cannot be a slow path here at all — it is a
// full queue on one physical connection, which is the transport's own bound.
// What this test can establish, and does, is the property that makes that true:
// one session whose publish never returns does not stop another session's tail,
// and the blocked one recovers and delivers everything it held when the
// transport frees up. A shared goroutine, a shared lock or a shared buffer would
// each fail it.
//
// WHAT IT DOES NOT ESTABLISH is anything about the transport's per-connection
// queue. See TestReplicaFanOutIsTheTransports.
func TestOneStuckSessionBlocksNeitherAnotherSessionNorAnotherReplica(t *testing.T) {
	t.Parallel()
	stuck := newLiveSession(t, "tenant-alpha")
	moving := newLiveSession(t, "tenant-alpha")

	held := make(chan struct{})
	stuckPublications := &recordingPublications{block: held}
	movingPublications := &recordingPublications{}
	routes := &recordingRoutes{}

	stuckTail, err := newTails(t, stuckPublications, routes).Publish(t.Context(), stuck.key, stuck.runtime)
	if err != nil {
		t.Fatalf("Publish the stuck session: %v", err)
	}
	t.Cleanup(stuckTail.Stop)
	movingTail, err := newTails(t, movingPublications, routes).Publish(t.Context(), moving.key, moving.runtime)
	if err != nil {
		t.Fatalf("Publish the moving session: %v", err)
	}
	t.Cleanup(movingTail.Stop)

	// The stuck session produces first, so the moving session's progress below
	// cannot be an artefact of having gone first.
	const each = 3
	for range each {
		stuck.appendPublicEvent(t)
	}
	for range each {
		moving.appendPublicEvent(t)
	}

	waitFor(t, "the moving session to publish everything it produced", func() bool { return len(movingPublications.recorded()) == each })
	if got := len(stuckPublications.recorded()); got != 0 {
		t.Fatalf("the stuck transport recorded %d messages; the fixture is not actually blocking and this test measures nothing", got)
	}
	if got := movingTail.Published(); got != each {
		t.Fatalf("the moving tail published %d, want %d while its neighbour is wedged", got, each)
	}
	if end, _ := stuckTail.End(); end != service.TailEndRunning {
		t.Fatalf("the stuck tail ended as %q; being slow is not being lost", end)
	}
	if got := routes.sessions(); len(got) != 0 {
		t.Fatalf("a slow transport invalidated %v; backpressure is not a routing failure", got)
	}

	// AND IT RECOVERS. Without this the test would be satisfied by a build that
	// dropped the stuck session's events on the floor, which is exactly what
	// step 3 forbids for an enduring stream.
	close(held)
	waitFor(t, "the unblocked session to publish everything it was holding", func() bool { return len(stuckPublications.recorded()) == each })
	if got := stuckTail.Published(); got != each {
		t.Fatalf("the unblocked tail published %d, want all %d it was holding", got, each)
	}
}

// TestReplicaFanOutIsTheTransports records what this package does NOT test, as
// a test, so the gap is visible in a run rather than only in a comment.
//
// "Sent once per interested Factory replica" has two halves and they live in
// different places. Host's half — exactly one message per event, on the
// session's channel, addressed to no replica — is held by the tests above. The
// other half is the transport's: centrifuge@v0.38.0 delivers one publication to
// every subscriber of a channel by enqueueing it on each subscriber's OWN write
// queue (hub.go:892 -> client.go:669 -> writer.go:92), and an overflow of that
// queue returns DisconnectSlow (writer.go:92-102; disconnect.go:82-86, code
// 3008) which closes THAT connection alone — reaching hostlink's existing
// OnDisconnect and dropping only that link's bindings.
//
// THAT PATH IS NOT EXERCISED FROM HERE, and it cannot be: centrifuge may be
// imported only under internal/realtime/hostlink, tests included. An end-to-end
// replica-fan-out and slow-client test belongs in that package with real
// WebSocket clients and a configured ClientQueueMaxSize, and is not written.
func TestReplicaFanOutIsTheTransports(t *testing.T) {
	t.Parallel()
	t.Skip("the per-replica fan-out and the slow-client disconnect are the transport's; see this test's doc comment for what is and is not covered")
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

func TestNewTailsRefusesIncompleteOptions(t *testing.T) {
	t.Parallel()
	for field, options := range map[string]service.TailOptions{
		"Publications": {Routes: &recordingRoutes{}},
		"Routes":       {Publications: &recordingPublications{}},
	} {
		_, err := service.NewTails(options)
		var invalid *service.InvalidTailOptionsError
		if !errors.As(err, &invalid) {
			t.Fatalf("NewTails without %s = %v, want *InvalidTailOptionsError", field, err)
		}
		if invalid.Field != field {
			t.Fatalf("NewTails without %s named %q", field, invalid.Field)
		}
	}
}

func TestPublishRefusesWhatItCannotRoute(t *testing.T) {
	t.Parallel()
	tails := newTails(t, &recordingPublications{}, &recordingRoutes{})
	good := registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"}
	subscriber := scriptedSubscriber{stream: make(chan sessionwire.EnduringPublication)}

	for field, key := range map[string]registry.Key{
		"TenantID":  {SessionID: good.SessionID},
		"SessionID": {TenantID: good.TenantID},
	} {
		_, err := tails.Publish(t.Context(), key, subscriber)
		var invalid *service.InvalidTailOptionsError
		if !errors.As(err, &invalid) || invalid.Field != field {
			t.Fatalf("Publish with an invalid %s = %v, want *InvalidTailOptionsError naming it", field, err)
		}
	}
	if _, err := tails.Publish(t.Context(), good, nil); err == nil {
		t.Fatal("Publish with no subscriber succeeded")
	}

	// THE CONTROL: the same key and a real subscriber are accepted, so the
	// refusals above are about the field and not about the fixture.
	tail, err := tails.Publish(t.Context(), good, subscriber)
	if err != nil {
		t.Fatalf("Publish with a valid key: %v", err)
	}
	t.Cleanup(tail.Stop)
	if tail.Channel() != hostlink.ChannelFor(good) {
		t.Fatalf("Channel = %q, want %q", tail.Channel(), hostlink.ChannelFor(good))
	}
	if tail.Key() != good {
		t.Fatalf("Key = %#v, want %#v", tail.Key(), good)
	}
}

func TestPublishReportsASubscribeFailure(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("the runtime cannot serve committed public events")
	tails := newTails(t, &recordingPublications{}, &recordingRoutes{})
	_, err := tails.Publish(t.Context(), registry.Key{TenantID: "tenant-alpha", SessionID: "session-a"},
		scriptedSubscriber{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Publish = %v, want the runtime's error", err)
	}
}

// ---------------------------------------------------------------------------
// Test support
// ---------------------------------------------------------------------------

// scriptedSubscriber is a department.PublicationSubscriber over a channel a
// test owns. It stands in only where the publication CONTENT is the subject and
// a real journal would make the value under test unreachable — a foreign
// publication cannot be produced by a real runtime at all.
type scriptedSubscriber struct {
	stream <-chan sessionwire.EnduringPublication
	err    error
}

func (s scriptedSubscriber) SubscribeCommitted(context.Context, sessionwire.EventID) (<-chan sessionwire.EnduringPublication, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.stream, nil
}

// newUUID mints one identity, reporting a generator failure rather than
// swallowing it into a zero UUID that would address the wrong session.
func newUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("mint a session identity: %v", err)
	}
	return id
}

// methodNames returns the exported method names of an interface, given a nil
// pointer to it.
func methodNames(t *testing.T, pointer any) []string {
	t.Helper()
	interfaceType := reflect.TypeOf(pointer).Elem()
	names := make([]string, 0, interfaceType.NumMethod())
	for i := range interfaceType.NumMethod() {
		names = append(names, interfaceType.Method(i).Name)
	}
	slices.Sort(names)
	return names
}

// waitFor polls a condition a publishing goroutine satisfies. A goroutine's
// effect is not ordered against the call that provoked it, so the alternative is
// a sleep, which is either flaky or slow.
func waitFor(t *testing.T, want string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waited for %s and it never became true", want)
}

// ---------------------------------------------------------------------------
// A harness session controller over a real hub
// ---------------------------------------------------------------------------

// controllerBase is every session.SessionController method and nothing else, so
// the capability discovery the runtime adapter performs is asked the same
// question it will be asked in production.
type controllerBase struct{ id uuid.UUID }

func (c controllerBase) SessionID() uuid.UUID             { return c.id }
func (controllerBase) ActiveLoop() loop.Handle            { return nil }
func (controllerBase) Loop(uuid.UUID) (loop.Handle, bool) { return nil, false }
func (controllerBase) Submit(context.Context, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}

func (controllerBase) SubmitToLoop(context.Context, uuid.UUID, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}

func (controllerBase) Compact(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}

func (controllerBase) CompactToLoop(context.Context, uuid.UUID) (uuid.UUID, error) {
	return uuid.UUID{}, errNotImplemented
}

func (controllerBase) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errNotImplemented
}

func (controllerBase) RespondGate(context.Context, gate.GateResponse) error { return errNotImplemented }
func (controllerBase) Interrupt(context.Context) (bool, error)              { return false, errNotImplemented }
func (controllerBase) SetActiveLoop(context.Context, uuid.UUID) error       { return errNotImplemented }
func (controllerBase) LoopController(uuid.UUID) (loop.Controller, bool)     { return nil, false }
func (controllerBase) CheckpointWorkspace(context.Context) (workspacestore.Ref, error) {
	return "", errNotImplemented
}

func (controllerBase) RestoreWorkspace(context.Context, workspacestore.Ref) error {
	return errNotImplemented
}

func (controllerBase) Shutdown(context.Context) error { return errNotImplemented }

var errNotImplemented = errors.New("service_test: the fake controller implements only the capabilities under test")

// liveController is the five capabilities Host requires, four of them trivial
// and the fourth backed by a REAL hub.
type liveController struct {
	base controllerBase
	hub  *hub.Hub
	done chan struct{}
}

func (c *liveController) SessionID() uuid.UUID    { return c.base.SessionID() }
func (c *liveController) ActiveLoop() loop.Handle { return c.base.ActiveLoop() }
func (c *liveController) Loop(id uuid.UUID) (loop.Handle, bool) {
	return c.base.Loop(id)
}

func (c *liveController) Submit(ctx context.Context, blocks []content.Block) (uuid.UUID, error) {
	return c.base.Submit(ctx, blocks)
}

func (c *liveController) SubmitToLoop(ctx context.Context, id uuid.UUID, blocks []content.Block) (uuid.UUID, error) {
	return c.base.SubmitToLoop(ctx, id, blocks)
}

func (c *liveController) Compact(ctx context.Context) (uuid.UUID, error) {
	return c.base.Compact(ctx)
}

func (c *liveController) CompactToLoop(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	return c.base.CompactToLoop(ctx, id)
}

func (c *liveController) SubscribeEvents(filter event.EventFilter) (event.Subscription, error) {
	return c.base.SubscribeEvents(filter)
}

func (c *liveController) RespondGate(ctx context.Context, response gate.GateResponse) error {
	return c.base.RespondGate(ctx, response)
}

func (c *liveController) Interrupt(ctx context.Context) (bool, error) { return c.base.Interrupt(ctx) }

func (c *liveController) SetActiveLoop(ctx context.Context, id uuid.UUID) error {
	return c.base.SetActiveLoop(ctx, id)
}

func (c *liveController) LoopController(id uuid.UUID) (loop.Controller, bool) {
	return c.base.LoopController(id)
}

func (c *liveController) CheckpointWorkspace(ctx context.Context) (workspacestore.Ref, error) {
	return c.base.CheckpointWorkspace(ctx)
}

func (c *liveController) RestoreWorkspace(ctx context.Context, ref workspacestore.Ref) error {
	return c.base.RestoreWorkspace(ctx, ref)
}

func (c *liveController) Shutdown(ctx context.Context) error { return c.base.Shutdown(ctx) }

// WaitIdle satisfies session.IdleWaiter.
func (c *liveController) WaitIdle(context.Context) error { return nil }

// Done satisfies session.Liveness.
func (c *liveController) Done() <-chan struct{} {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	return c.done
}

// ReleaseResidency satisfies session.Releaser.
func (c *liveController) ReleaseResidency(context.Context) error { return nil }

// LeaseEpoch satisfies session.LeaseEpochReporter. This fixture runs a real hub
// over a test appender and holds no single-writer journal lease, so it reports
// the RUNTIME's grant as 1 rather than pretending to Host's residency epoch —
// which nothing here holds either.
func (c *liveController) LeaseEpoch() (uint64, bool) { return 1, true }

// PersistenceFaulted satisfies session.PersistenceFaultReporter: this runtime
// never faults.
func (c *liveController) PersistenceFaulted() <-chan struct{} { return nil }

// PersistenceFault satisfies session.PersistenceFaultReporter.
func (c *liveController) PersistenceFault() error { return nil }

// AbandonResidency satisfies session.ResidencyAbandoner.
func (c *liveController) AbandonResidency(context.Context) error { return nil }

// CommittedPublicEvents answers with the real hub, and answers the CAPABILITY
// question from the hub rather than unconditionally: a hub over an appender
// that cannot report stored bytes must report false, which is what the released
// session does.
func (c *liveController) CommittedPublicEvents() (session.CommittedPublicEventSource, bool) {
	if !c.hub.CommittedPublicEventsSupported() {
		return nil, false
	}
	return c, true
}

// SubscribeCommittedPublicEvents forwards to the hub, widening its concrete
// subscription to the released interface.
func (c *liveController) SubscribeCommittedPublicEvents(filter event.EventFilter) (event.Subscription, error) {
	return c.hub.SubscribeCommittedPublicEvents(filter)
}

// stubLauncher hands back one prepared controller.
type stubLauncher struct{ controller session.SessionController }

func (l *stubLauncher) NewSession(context.Context, ...rig.SessionOption) (session.SessionController, error) {
	return l.controller, nil
}

func (l *stubLauncher) RestoreSession(context.Context, uuid.UUID) (session.SessionController, error) {
	return l.controller, nil
}

// stubRigs resolves every launch to one launcher.
type stubRigs struct{ launcher harnessadapter.Launcher }

func (r stubRigs) RigForCreate(context.Context, department.RigCreateRequest) (harnessadapter.Launcher, error) {
	return r.launcher, nil
}

func (r stubRigs) RigForRestore(context.Context, uuid.UUID, department.RigRestoreRequest) (harnessadapter.Launcher, error) {
	return r.launcher, nil
}
