// A Host that can hold a session and cannot yet apply a command in it is an
// unusual thing to ship deliberately, so this file says why, at the place the
// refusal happens, for the person whose job is to remove it.
//
// apply.go is the durable protocol around ONE call into a runtime, and every
// seam it declares exists in sessionstore's LEGACY command family. A Host cannot
// reach that family: AcquireResidency pins ProtocolModeDisposition, and the
// disposition family's lifecycle settles a command from DURABLE EVIDENCE the
// caller does not author. The evidence is an application prefix carrying the
// attempt's identity, and the verifier requires AttemptID equality against the
// attempt being settled.
//
// harness v0.33.0 cannot produce one. runtimecommand.Admitted carries no attempt
// identity, so the prefix a dispatch commits names none, and no honest reader can
// supply an identity the record does not contain. The settlement design closes
// the other exit explicitly: "a legacy prefix followed by an uncertain external
// effect" may NOT be converted into not_applied, because the effect may have
// happened. A command dispatched under today's writer would therefore sit in
// `applying` forever, with a real runtime effect behind it, and no successor
// could ever settle it — which is strictly worse than a command that was never
// dispatched, because the second is still applicable by the Host that comes
// after.
//
// WHAT UNBLOCKS THIS, so it is not rediscovered: an attempt-aware harness writer
// that stamps the attempt identity into the application prefix, and a Host
// applier bound to the disposition family's edges (ClaimDispositionCommand,
// BeginDispositionAttempt, and settlement from the correlated evidence) rather
// than to the legacy CAS transitions apply.go models. Both are owed elsewhere;
// neither is a Host change alone.

package commands

import (
	"context"
	"strconv"
)

// RefusalDispatchUnavailable reports a command this Host will not drive into its
// runtime because no attempt-aware journal writer exists to correlate the
// effect with the attempt that authorized it.
//
// IT IS A REFUSAL AND NOT A REJECTION, on the same terms as every other value in
// the ApplyRefusal vocabulary: the durable record is left exactly where it was,
// the pass blocks at this command, and the consumer names it in
// PassResult.Blocked. A Host that REJECTED here would be authoring a terminal
// outcome for a command it simply cannot judge, and in disposition mode the
// store decides the terminal arm from evidence — there is no caller-authored
// rejection to make.
const RefusalDispatchUnavailable ApplyRefusal = "dispatch_unavailable"

// NoDispatch is the Processor a composed Host runs, and it dispatches nothing.
//
// IT REFUSES BEFORE IT READS OR WRITES ANYTHING, which is the property that
// makes it safe rather than merely inert. Every later refusal site in apply.go
// is reached with the record already claimed and, past BeginApplyingCommand,
// already in `applying`; a refusal there would durably create the state this
// boundary exists to prevent. Process below touches no seam at all, so a Host
// running it moves no command out of the state the store put it in.
//
// WHAT A HOST RUNNING IT STILL DOES, because "refuses to dispatch" is not
// "does nothing": the consumer lists the session's durable commands in
// acceptance order, steps over terminal records, and advances and saves the
// durable consumption cursor over them. That is the consumption half of the
// lifecycle and it is fully live. What stops is the application half.
type NoDispatch struct{}

// NoDispatch is a Processor, asserted here rather than in a test so that a drift
// in the seam is a compile failure.
var _ Processor = NoDispatch{}

// Process refuses the command and reports the state it was handed.
//
// THE REPORTED STATE IS THE RECORD'S OWN AND NOT A GUESS, AND NOTHING IN
// PRODUCTION READS IT ON THIS PATH. Reconcile reads Outcome.State only when the
// error is nil, and BlockedCommand.State is taken from the RECORD rather than
// from this outcome — so no caller named here consumes it today, and an earlier
// version of this comment implied one did. It is populated because a Processor
// that answered with a zero State would be lying about a record it declined to
// look at, and because the field is part of the seam's contract rather than of
// this implementation's convenience. The unit test asserts on it, which is what
// makes zeroing it a dying mutant rather than an invisible one.
func (NoDispatch) Process(_ context.Context, command Command) (Outcome, error) {
	return Outcome{State: command.State}, &ApplyError{
		Refusal:   RefusalDispatchUnavailable,
		CommandID: command.CommandID,
		Reason: "this Host consumes a session's durable command stream and does not drive commands into the runtime: " +
			"the runtime's journal writer stamps no attempt identity into an application prefix, so a dispatched command " +
			"could never be settled from evidence and would remain in " + strconv.Quote("applying") + " with its effect already taken",
	}
}
