package publicbody

import (
	"context"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// Pair is one immutable admission mapping.
type Pair struct {
	Runtime uuid.UUID
	Public  sessionwire.CommandID
}

// Source reads a session's admission mappings incrementally.
//
// Positions are the source's own (a journal sequence, an acceptance order) and
// only ever grow. Read returns the pairs recorded after `after`, the position
// it read through, and done when it has read enough to answer for an event at
// journal sequence bound — a source whose positions are not journal
// sequences reports done only at its end.
type Source interface {
	Read(ctx context.Context, after, bound uint64) (pairs []Pair, through uint64, done bool, err error)
}

// maxAbsent bounds the remembered misses. A miss is permanent — a mapping is
// admitted before the command is applied, so it is readable before any event
// names it — and remembering it saves a read per machine-caused event; the
// bound only costs a re-read when it is exceeded.
const maxAbsent = 4096

// Index is a Commands over a Source. It is safe for concurrent use; reads
// against one session are serialized.
type Index struct {
	source Source

	mu      sync.Mutex
	through uint64
	known   map[uuid.UUID]sessionwire.CommandID
	absent  map[uuid.UUID]struct{}
}

// NewIndex returns an empty index over source.
func NewIndex(source Source) *Index {
	return &Index{source: source, known: map[uuid.UUID]sessionwire.CommandID{}, absent: map[uuid.UUID]struct{}{}}
}

// PublicCommand implements Commands.
func (i *Index) PublicCommand(ctx context.Context, runtime uuid.UUID, bound uint64) (sessionwire.CommandID, bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if public, ok := i.known[runtime]; ok {
		return public, true, nil
	}
	if _, ok := i.absent[runtime]; ok {
		return "", false, nil
	}
	for {
		pairs, through, done, err := i.source.Read(ctx, i.through, bound)
		if err != nil {
			return "", false, err
		}
		for _, pair := range pairs {
			i.known[pair.Runtime] = pair.Public
		}
		progressed := through > i.through
		if progressed {
			i.through = through
		}
		if _, ok := i.known[runtime]; ok || done || !progressed {
			break
		}
	}
	if public, ok := i.known[runtime]; ok {
		return public, true, nil
	}
	if len(i.absent) >= maxAbsent {
		clear(i.absent)
	}
	i.absent[runtime] = struct{}{}
	return "", false, nil
}
