package sessionstoreadapter

import (
	"context"
	"errors"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host/internal/commands"
)

// ErrGateOwnerUnavailable is the refusal LoadGate returns for a gate that
// exists.
//
// THE BACKSTOP IS DECIDED FROM MEMBERS THE PROJECTION DOES NOT CARRY — finding
// F10. commands.Gate holds OwnerHostID and OwnerEpoch, and §9.4's release-race
// backstop resumes a gate response only while both still name this Host's
// current residency. Store.ReadGates answers with sessionwire.GatePage, whose
// GateProjection carries the gate id, kind, prompt, opening event and sequence,
// deadline and answerability — and names no owning Host and no lease epoch.
//
// ANSWERING WITH ZERO OWNERS WOULD BE WORSE THAN REFUSING, which is why this
// exists rather than a partially filled Gate. A zero HostID matches no live
// residency, so a caller comparing it against its own would refuse every gate
// response — but a caller comparing OwnerEpoch against zero, or one that treats
// "no owner" as "not owned by anyone else", would resume a gate whose runtime
// was released and relaunched. The seam cannot express "I know the gate is open
// and I do not know who owns it", so it refuses.
var ErrGateOwnerUnavailable = errors.New(
	"sessionstoreadapter: the released gate projection names no owning Host or lease epoch")

// LoadGate returns the durable gate and whether it exists.
//
// A gate that is NOT open is answerable soundly: absence from the page is the
// whole answer, and the owner members are documented as the state a gate was
// opened under. A gate that IS open is refused; see ErrGateOwnerUnavailable.
func (s *Store) LoadGate(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	gate sessionwire.GateID,
) (commands.Gate, bool, error) {
	page, err := s.store.ReadGates(ctx, sessionstore.ReadGatesRequest{
		TenantID:  tenant,
		SessionID: session,
	})
	if err != nil {
		return commands.Gate{}, false, err
	}
	for _, projection := range page.Gates {
		if projection.GateID != gate {
			continue
		}
		return commands.Gate{}, false, &GateOwnerUnavailableError{GateID: gate}
	}
	return commands.Gate{}, false, nil
}

// GateOwnerUnavailableError names the gate whose owner could not be read.
type GateOwnerUnavailableError struct {
	GateID sessionwire.GateID
}

func (e *GateOwnerUnavailableError) Error() string {
	return ErrGateOwnerUnavailable.Error() + ", so gate " + strconv.Quote(string(e.GateID)) +
		" cannot be decided"
}

func (e *GateOwnerUnavailableError) Unwrap() error { return ErrGateOwnerUnavailable }
