package harnessadapter

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"

	"github.com/looprig/host/department"
)

type liveController struct {
	*fullController
	subscription event.Subscription
	filters      *[]event.EventFilter
}

func (c liveController) SubscribeEvents(filter event.EventFilter) (event.Subscription, error) {
	*c.filters = append(*c.filters, filter)
	return c.subscription, nil
}

func liveDelta(chunk content.Chunk) event.Delivery {
	return event.Delivery{Event: event.TokenDelta{
		Header: event.Header{Coordinates: identity.Coordinates{
			SessionID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			LoopID:    uuid.MustParse("11111111-2222-3333-4444-555555555555"),
			TurnID:    uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa"),
		}},
		Chunk: chunk,
	}}
}

func TestLivePublicationsKeepTextAndEnduringInProducerOrder(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	live := runtime.(department.LivePublicationSubscriber)
	publications, err := live.SubscribeLivePublic(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(filters) != 1 || !filters[0].Enduring.All || !filters[0].Ephemeral.All {
		t.Fatalf("SubscribeEvents filters = %+v, want one mixed all-loops filter", filters)
	}
	subscription.deliveries <- liveDelta(&content.TextChunk{Text: "first"})
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "event-2", JournalSeq: 2, CoveredThrough: 2, PublicBody: []byte(`{"type":"SessionActive"}`)}
	subscription.deliveries <- liveDelta(&content.TextChunk{Text: "third"})
	for i, want := range []string{"first", "enduring", "third"} {
		select {
		case got := <-publications:
			if want == "enduring" {
				if got.Enduring == nil || got.Enduring.EventID != "event-2" || got.Ephemeral != nil {
					t.Fatalf("publication %d = %+v, want committed event", i, got)
				}
			} else if got.Ephemeral == nil || got.Enduring != nil || !bytes.Contains(got.Ephemeral.Body, []byte(want)) {
				t.Fatalf("publication %d = %+v, want text %q", i, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("publication %d did not arrive", i)
		}
	}
}

func TestLivePublicationsDropOtherChunksAndFailUncommittedEnduring(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	publications, err := runtime.(department.LivePublicationSubscriber).SubscribeLivePublic(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{Thinking: "secret-reasoning"})
	subscription.deliveries <- liveDelta(&content.ToolUseChunk{InputJSON: `{"secret":"tool"}`})
	subscription.deliveries <- liveDelta(&content.ImageChunk{})
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, JournalSeq: 3}
	select {
	case got, open := <-publications:
		if open {
			t.Fatalf("unexpected publication after uncommitted enduring: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("uncommitted enduring did not close the live tail")
	}
}

func TestLiveEphemeralPressureDoesNotSpendEnduringBuffer(t *testing.T) {
	subscription := &fakeSubscription{deliveries: make(chan event.Delivery, committedEgressBuffer+64)}
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	publications, err := runtime.(department.LivePublicationSubscriber).SubscribeLivePublic(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		subscription.deliveries <- liveDelta(&content.TextChunk{Text: "x"})
	}
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "after-pressure", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got, open := <-publications:
			if !open {
				t.Fatal("ephemeral pressure closed the pump")
			}
			if got.Enduring != nil && got.Enduring.EventID == "after-pressure" {
				return
			}
		case <-deadline:
			t.Fatal("enduring publication was delayed by ephemeral pressure")
		}
	}
}

func TestOversizedTextChunkIsDroppedBeforeTheEnduringEvent(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	publications, err := runtime.(department.LivePublicationSubscriber).SubscribeLivePublic(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	subscription.deliveries <- liveDelta(&content.TextChunk{Text: strings.Repeat("x", 4097)})
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
	select {
	case got := <-publications:
		if got.Enduring == nil || got.Enduring.EventID != "durable" {
			t.Fatalf("first publication = %+v, want enduring after oversized preview was dropped", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enduring publication was delayed")
	}
}
