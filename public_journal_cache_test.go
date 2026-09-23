package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host"
)

const (
	cacheRuntimeSession = "0f4d2a6c-81b3-4e57-9c20-6a1e7d3b5f48"
	cacheRuntimeCommand = "ffffffff-ffff-4fff-8fff-ffffffffffff"
)

// countingJournal is a one-event runtime journal whose event a command caused.
type countingJournal struct{ runtimeReads atomic.Int32 }

func (j *countingJournal) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	body := `{"cause":{"command_id":"` + cacheRuntimeCommand + `"},"session_id":"` + cacheRuntimeSession + `","type":"TurnStarted"}`
	return sessionwire.JournalPage{
		Events:      []sessionwire.JournalEvent{{EventID: "event-2", JournalSeq: 2, Body: json.RawMessage(body)}},
		CapturedTip: 2, CoveredThrough: 2,
	}, nil
}

func (j *countingJournal) ReadRuntimeJournal(_ context.Context, req sessionstore.ReadRuntimeJournalRequest) (sessionstore.RuntimePage, error) {
	j.runtimeReads.Add(1)
	if req.FromSeq > 1 {
		return sessionstore.RuntimePage{}, nil
	}
	return sessionstore.RuntimePage{Records: []sessionstore.RuntimeRecord{{Seq: 1, Envelope: sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindApplicationPrefix, CommandID: "command-public", RuntimeCommandID: uuid.MustParse(cacheRuntimeCommand),
	}}}}, nil
}

func cacheBinding(runtime string) sessionstore.SessionBinding {
	return sessionstore.SessionBinding{StorageBindingID: "b", BindingVersion: "v1", RuntimeSessionID: runtime, ProtocolMode: sessionstore.ProtocolModeDisposition}
}

func readOnce(t *testing.T, journals *host.PublicJournals, journal *countingJournal, session sessionwire.SessionID, runtime string) string {
	t.Helper()
	reader, err := journals.Reader(journal, composeTenant, session, cacheBinding(runtime))
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	page, err := reader.ReadPublicJournal(t.Context(), sessionstore.ReadPublicJournalRequest{TenantID: composeTenant, SessionID: sessionwire.SessionID(runtime)})
	if err != nil {
		t.Fatalf("ReadPublicJournal: %v", err)
	}
	return string(page.Events[0].Body)
}

// TestPublicJournalsKeepAMappingAcrossReaders: a resolver answering a new
// PublicJournal per read does not re-read the journal's records for a
// mapping it already has — until the session is evicted.
func TestPublicJournalsKeepAMappingAcrossReaders(t *testing.T) {
	journal := &countingJournal{}
	journals := host.NewPublicJournals(1)
	want := `{"cause":{"command_id":"command-public"},"session_id":"` + string(composeSession) + `","type":"TurnStarted"}`
	if got := readOnce(t, journals, journal, composeSession, cacheRuntimeSession); got != want {
		t.Fatalf("projected %s, want %s", got, want)
	}
	reads := journal.runtimeReads.Load()
	if reads == 0 {
		t.Fatal("the mapping was never read")
	}
	readOnce(t, journals, journal, composeSession, cacheRuntimeSession)
	if got := journal.runtimeReads.Load(); got != reads {
		t.Fatalf("a second reader re-read the journal (%d reads, want %d)", got, reads)
	}
	// Another session evicts the first at capacity one; the first then reads again.
	other := "11111111-2222-4333-8444-555555555555"
	readOnce(t, journals, journal, "session-other", other)
	afterOther := journal.runtimeReads.Load()
	readOnce(t, journals, journal, composeSession, cacheRuntimeSession)
	if got := journal.runtimeReads.Load(); got == afterOther {
		t.Fatal("an evicted session's mapping was still cached")
	}
}

// TestAPublicJournalRefusesWhatItCannotProject.
func TestAPublicJournalRefusesWhatItCannotProject(t *testing.T) {
	journal := &countingJournal{}
	if _, err := host.NewPublicJournal(journal, composeTenant, composeSession, cacheBinding("not-a-uuid")); !errors.Is(err, host.ErrPublicJournalScope) {
		t.Fatalf("a binding naming no runtime session = %v, want ErrPublicJournalScope", err)
	}
	if _, err := host.NewPublicJournal(nil, composeTenant, composeSession, cacheBinding(cacheRuntimeSession)); err == nil {
		t.Fatal("a public journal with no reader was built")
	}
	reader, err := host.NewPublicJournal(journal, composeTenant, composeSession, cacheBinding(cacheRuntimeSession))
	if err != nil {
		t.Fatalf("NewPublicJournal: %v", err)
	}
	for _, req := range []sessionstore.ReadPublicJournalRequest{
		{TenantID: "tenant-other", SessionID: cacheRuntimeSession},
		{TenantID: composeTenant, SessionID: composeSession},
	} {
		if _, err := reader.ReadPublicJournal(t.Context(), req); !errors.Is(err, host.ErrPublicJournalScope) {
			t.Errorf("read %s/%s = %v, want ErrPublicJournalScope", req.TenantID, req.SessionID, err)
		}
	}
}

// TestPublicJournalsReadTheMappingThroughTheLatestReader is review L1: a
// resolver may hand a new store per call, and a kept mapping must not go on
// reading through the first one it was ever handed.
func TestPublicJournalsReadTheMappingThroughTheLatestReader(t *testing.T) {
	journals := host.NewPublicJournals(4)
	first, second := &countingJournal{}, &countingJournal{}
	// The first reader is only constructed with; the mapping is first read
	// through the second.
	if _, err := journals.Reader(first, composeTenant, composeSession, cacheBinding(cacheRuntimeSession)); err != nil {
		t.Fatalf("Reader: %v", err)
	}
	readOnce(t, journals, second, composeSession, cacheRuntimeSession)
	if first.runtimeReads.Load() != 0 || second.runtimeReads.Load() == 0 {
		t.Fatalf("mapping reads: first reader %d, second %d; want all through the second", first.runtimeReads.Load(), second.runtimeReads.Load())
	}
}

// TestAPublicJournalErrorDoesNotQuoteTheRuntimeSession is review L3.
func TestAPublicJournalErrorDoesNotQuoteTheRuntimeSession(t *testing.T) {
	const almost = "0f4d2a6c-81b3-4e57-9c20-6a1e7d3b5f4" // one digit short of a UUID
	_, err := host.NewPublicJournal(&countingJournal{}, composeTenant, composeSession, cacheBinding(almost))
	if err == nil || strings.Contains(err.Error(), almost) {
		t.Fatalf("NewPublicJournal = %v, want a refusal that does not quote the id", err)
	}
}
