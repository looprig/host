package harnessruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
		var missing *MissingCommittedFieldError
		if !open || !errors.As(got.Terminal, &missing) {
			t.Fatalf("terminal cause = %+v (open %v), want typed missing commit", got, open)
		}
		if _, open := <-publications; open {
			t.Fatal("live stream did not close after terminal cause")
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
	for i := 0; i < committedEgressBuffer; i++ {
		subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: fmt.Sprintf("event-%d", i), JournalSeq: uint64(i + 1), CoveredThrough: uint64(i + 1), PublicBody: []byte(`{"type":"SessionActive"}`)}
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(subscription.deliveries) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("pump did not consume 256 enduring frames")
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 64; i++ {
		subscription.deliveries <- liveDelta(&content.TextChunk{Text: "x"})
	}
	for len(subscription.deliveries) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("pump stopped consuming ephemerals")
		}
		time.Sleep(time.Millisecond)
	}
	if drops := runtime.(interface{ EphemeralDrops() uint64 }).EphemeralDrops(); drops == 0 {
		t.Fatal("extra ephemerals did not drop")
	}
	timeout := time.After(5 * time.Second)
	for {
		select {
		case got, open := <-publications:
			if !open {
				t.Fatal("ephemeral pressure closed the pump")
			}
			if got.Enduring != nil && got.Enduring.EventID == "event-255" {
				return
			}
		case <-timeout:
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

func TestReasoningSubscriptionProjectsOnlyVisibleThinkingInOrder(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	live, ok := runtime.(interface {
		SubscribeLivePublicWithReasoning(context.Context) (<-chan department.LivePublication, error)
	})
	if !ok {
		t.Fatal("bound runtime has no reasoning subscription")
	}
	publications, err := live.SubscribeLivePublicWithReasoning(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{Thinking: "why"})
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{Signature: "signature-only"})
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{ProviderState: []byte(`{"thoughtSignature":"opaque-only"}`)})
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{Thinking: "Gemini thought", ProviderState: []byte(`{"thoughtSignature":"opaque-with-text"}`)})
	subscription.deliveries <- liveDelta(&content.ToolUseChunk{InputJSON: `{"secret":"tool"}`})
	subscription.deliveries <- liveDelta(&content.ImageChunk{})
	subscription.deliveries <- liveDelta(&content.RefusalChunk{Text: "refused"})
	subscription.deliveries <- liveDelta(&content.TextChunk{Text: "answer"})
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
	for i, want := range []string{`"chunk_type":"thinking","thinking":"why"`, `"chunk_type":"thinking","thinking":"Gemini thought"`, `"chunk_type":"text","text":"answer"`, "durable"} {
		select {
		case got := <-publications:
			if want == "durable" {
				if got.Enduring == nil || got.Enduring.EventID != "durable" {
					t.Fatalf("publication %d = %+v", i, got)
				}
			} else if got.Ephemeral == nil || !bytes.Contains(got.Ephemeral.Body, []byte(want)) || bytes.Contains(got.Ephemeral.Body, []byte("thoughtSignature")) {
				t.Fatalf("publication %d = %+v, want %s", i, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("publication %d did not arrive", i)
		}
	}
}

func TestOversizedThinkingChunkIsDroppedBeforeEnduring(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	live := runtime.(department.ReasoningPublicationSubscriber)
	publications, err := live.SubscribeLivePublicWithReasoning(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	subscription.deliveries <- liveDelta(&content.ThinkingChunk{Thinking: strings.Repeat("x", 2049)})
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
	select {
	case got := <-publications:
		if got.Enduring == nil || got.Enduring.EventID != "durable" {
			t.Fatalf("first publication = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enduring event was delayed by oversized reasoning")
	}
}

func liveToolHeader() event.Header {
	return event.Header{
		EventID:   uuid.MustParse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
		CreatedAt: time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC),
		Coordinates: identity.Coordinates{
			SessionID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			LoopID:    uuid.MustParse("11111111-2222-3333-4444-555555555555"),
			TurnID:    uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa"),
			StepID:    uuid.MustParse("77777777-8888-9999-aaaa-bbbbbbbbbbbb"),
		},
	}
}

var liveToolExecution = uuid.MustParse("99999999-8888-7777-6666-555555555555")

func liveToolSteps() []event.Delivery {
	return []event.Delivery{
		{Event: event.ToolCallStarted{Header: liveToolHeader(), ToolExecutionID: liveToolExecution, ToolUseID: "toolu_1", ToolName: "Bash", Summary: "ls -la"}},
		{Event: event.ToolCallCompleted{Header: liveToolHeader(), ToolExecutionID: liveToolExecution, ToolUseID: "toolu_1", ToolName: "Bash", ElapsedMillis: 42, ResultPreview: "total 0"}},
	}
}

func TestLiveOptionsSubscriptionProjectsToolStepsWithJoinKey(t *testing.T) {
	subscription := newFakeSubscription(nil)
	var filters []event.EventFilter
	runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
	live, ok := runtime.(department.LiveOptionsSubscriber)
	if !ok {
		t.Fatal("bound runtime has no live-options subscription")
	}
	publications, err := live.SubscribeLivePublicWith(t.Context(), department.LiveOptions{IncludeToolSteps: true})
	if err != nil {
		t.Fatal(err)
	}
	// The model's raw tool arguments stream as a ToolUseChunk delta; they must
	// stay behind while the redacted summary crosses.
	subscription.deliveries <- liveDelta(&content.ToolUseChunk{InputJSON: `{"command":"echo sk-live-secret-marker"}`})
	for _, delivery := range liveToolSteps() {
		subscription.deliveries <- delivery
	}
	subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
	for i, want := range []string{
		`"type":"ToolCallStarted"`,
		`"type":"ToolCallCompleted"`,
		"durable",
	} {
		select {
		case got := <-publications:
			if want == "durable" {
				if got.Enduring == nil || got.Enduring.EventID != "durable" {
					t.Fatalf("publication %d = %+v, want the committed event", i, got)
				}
				continue
			}
			if got.Ephemeral == nil {
				t.Fatalf("publication %d = %+v, want a tool step", i, got)
			}
			body := got.Ephemeral.Body
			for _, member := range []string{want, `"tool_use_id":"toolu_1"`, `"tool_name":"Bash"`, `"tool_execution_id":"` + liveToolExecution.String() + `"`} {
				if !bytes.Contains(body, []byte(member)) {
					t.Fatalf("publication %d = %s, missing %s", i, body, member)
				}
			}
			if bytes.Contains(body, []byte("sk-live-secret-marker")) {
				t.Fatalf("publication %d leaked raw tool arguments: %s", i, body)
			}
			if i == 0 && !bytes.Contains(body, []byte(`"summary":"ls -la"`)) {
				t.Fatalf("started body = %s, want the audit summary", body)
			}
			if i == 1 && (!bytes.Contains(body, []byte(`"elapsed_ms":42`)) || !bytes.Contains(body, []byte(`"result_preview":"total 0"`))) {
				t.Fatalf("completed body = %s, want elapsed and preview", body)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("publication %d did not arrive", i)
		}
	}
}

func TestToolStepsStayBehindUnlessRequested(t *testing.T) {
	subscribe := map[string]func(runtime any) (<-chan department.LivePublication, error){
		"legacy text": func(r any) (<-chan department.LivePublication, error) {
			return r.(department.LivePublicationSubscriber).SubscribeLivePublic(t.Context())
		},
		"legacy reasoning": func(r any) (<-chan department.LivePublication, error) {
			return r.(department.ReasoningPublicationSubscriber).SubscribeLivePublicWithReasoning(t.Context())
		},
		"options off": func(r any) (<-chan department.LivePublication, error) {
			return r.(department.LiveOptionsSubscriber).SubscribeLivePublicWith(t.Context(), department.LiveOptions{IncludeReasoning: true})
		},
	}
	for name, open := range subscribe {
		t.Run(name, func(t *testing.T) {
			subscription := newFakeSubscription(nil)
			var filters []event.EventFilter
			runtime := boundFor(t, liveController{fullController: newFullController(newFakeSubscription(nil), nil, nil), subscription: subscription, filters: &filters})
			publications, err := open(runtime)
			if err != nil {
				t.Fatal(err)
			}
			for _, delivery := range liveToolSteps() {
				subscription.deliveries <- delivery
			}
			subscription.deliveries <- event.Delivery{Event: event.SessionActive{}, EventID: "durable", JournalSeq: 1, CoveredThrough: 1, PublicBody: []byte(`{"type":"SessionActive"}`)}
			select {
			case got := <-publications:
				if got.Enduring == nil || got.Enduring.EventID != "durable" {
					t.Fatalf("first publication = %+v, want tool steps dropped before the committed event", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("committed event did not arrive")
			}
			if drops := runtime.(interface{ EphemeralDrops() uint64 }).EphemeralDrops(); drops != 2 {
				t.Fatalf("EphemeralDrops = %d, want both tool steps counted", drops)
			}
		})
	}
}
