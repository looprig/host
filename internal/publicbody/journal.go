package publicbody

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// RuntimeJournal is the privileged read of a runtime journal's every record.
// *sessionstore.Store satisfies it.
type RuntimeJournal interface {
	ReadRuntimeJournal(context.Context, sessionstore.ReadRuntimeJournalRequest) (sessionstore.RuntimePage, error)
}

// journalPage is how many records one read of the journal's mappings takes.
const journalPage = 512

// JournalSource reads a session's admission mappings from the runtime
// journal's own APPLICATION PREFIXES: harness writes one for every command it
// applies, carrying the public CommandID and the RuntimeCommandID it was
// admitted under, before any event that command causes. Positions are journal
// sequences, so it reads only as far as the event being projected.
//
// It reads envelope FIELDS only. An application prefix has no body, and no
// runtime or public body is ever resolved here.
type JournalSource struct {
	Journal          RuntimeJournal
	TenantID         sessionwire.TenantID
	RuntimeSessionID sessionwire.SessionID
}

// Read implements Source.
func (s JournalSource) Read(ctx context.Context, after, bound uint64) ([]Pair, uint64, bool, error) {
	page, err := s.Journal.ReadRuntimeJournal(ctx, sessionstore.ReadRuntimeJournalRequest{
		TenantID:  s.TenantID,
		SessionID: s.RuntimeSessionID,
		FromSeq:   after + 1,
		Limit:     journalPage,
	})
	if err != nil {
		return nil, after, false, err
	}
	var pairs []Pair
	through := after
	for _, record := range page.Records {
		if record.Seq > through {
			through = record.Seq
		}
		envelope := record.Envelope
		if envelope.Kind != sessionstore.EnvelopeKindApplicationPrefix {
			continue
		}
		pairs = append(pairs, Pair{Runtime: envelope.RuntimeCommandID, Public: envelope.CommandID})
	}
	return pairs, through, len(page.Records) == 0 || through >= bound, nil
}
