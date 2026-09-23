package publicbody

import (
	"context"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"
)

// fakeRuntimeJournal serves records by sequence, one page per call.
type fakeRuntimeJournal struct {
	records  []sessionstore.RuntimeRecord
	page     int
	requests []sessionstore.ReadRuntimeJournalRequest
}

func (f *fakeRuntimeJournal) ReadRuntimeJournal(_ context.Context, req sessionstore.ReadRuntimeJournalRequest) (sessionstore.RuntimePage, error) {
	f.requests = append(f.requests, req)
	var out []sessionstore.RuntimeRecord
	for _, record := range f.records {
		if record.Seq >= req.FromSeq && len(out) < f.page {
			out = append(out, record)
		}
	}
	return sessionstore.RuntimePage{Records: out}, nil
}

// applicationPrefix is one application-prefix record.
func applicationPrefix(seq uint64, runtime string, public sessionwire.CommandID) sessionstore.RuntimeRecord {
	return sessionstore.RuntimeRecord{Seq: seq, Envelope: sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindApplicationPrefix, CommandID: public, RuntimeCommandID: uuid.MustParse(runtime),
	}}
}

// TestTheJournalSourceReadsApplicationPrefixesUpToTheEvent: only prefixes are
// mappings, and a read stops once it has passed the event being projected.
func TestTheJournalSourceReadsApplicationPrefixesUpToTheEvent(t *testing.T) {
	journal := &fakeRuntimeJournal{page: 2, records: []sessionstore.RuntimeRecord{
		{Seq: 1, Envelope: sessionstore.Envelope{Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "e1"}},
		applicationPrefix(2, runtimeCommand, publicCommand),
		{Seq: 3, Envelope: sessionstore.Envelope{Kind: sessionstore.EnvelopeKindCommandDisposition, CommandID: "other", RuntimeCommandID: uuid.MustParse(machineCommand)}},
		{Seq: 4, Envelope: sessionstore.Envelope{Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "e4"}},
		applicationPrefix(5, machineCommand, "late"),
	}}
	source := JournalSource{Journal: journal, TenantID: "tenant-a", RuntimeSessionID: runtimeSession}
	index := NewIndex(source)

	got, ok, err := index.PublicCommand(context.Background(), uuid.MustParse(runtimeCommand), 4)
	if err != nil || !ok || got != publicCommand {
		t.Fatalf("PublicCommand = (%q, %v, %v)", got, ok, err)
	}
	// The disposition frame names a command too, and is not a mapping.
	if _, ok, _ := index.PublicCommand(context.Background(), uuid.MustParse(machineCommand), 4); ok {
		t.Fatal("a disposition frame was read as an admission mapping")
	}
	for _, req := range journal.requests {
		if req.TenantID != "tenant-a" || req.SessionID != runtimeSession {
			t.Fatalf("read %s/%s, want the bound runtime journal", req.TenantID, req.SessionID)
		}
		if req.FromSeq > 5 {
			t.Fatalf("read from %d, past the bound", req.FromSeq)
		}
	}
	if last := journal.requests[len(journal.requests)-1]; last.FromSeq != 3 {
		t.Fatalf("the last read began at %d, want 3: the source read past the event it was asked about", last.FromSeq)
	}
}
