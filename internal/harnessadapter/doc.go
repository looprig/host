// Package harnessadapter binds Host's department seams to the released
// github.com/looprig/harness v0.32.0 rig and session API.
//
// # Step-1 inventory
//
//	LOCAL SEAM                              RELEASED COUNTERPART
//	department.Rig.NewSession               rig.Rig.NewSession          (H1, H2)
//	department.Rig.RestoreSession           rig.Rig.RestoreSession      (H2)
//	department.RigSession.ID                session.Session.SessionID
//	department.IdleWaiter.WaitIdle          session.IdleWaiter          (H0)
//	department.Liveness.Done                session.Liveness            (H0)
//	department.Releaser.ReleaseResidency    session.Releaser            (H0)
//	department.PublicationSubscriber.Sub…   session.CommittedPublicEvent…
//	                                          …Provider/Source          (H0, H3, H4)
//	department.CommandApplier.ApplyCommand  runtimecommand.Provider/Applier
//	                                          (H0, H5, H6, H7, H8)
//
// NONE OF THE FIVE IS ON SessionController, AND ALL FIVE ARE PUBLISHED. That
// distinction is the whole of H0 and an earlier version of this file got it
// wrong. session.SessionController — the type rig.NewSession and
// rig.RestoreSession return — declares neither WaitIdle, nor Done, nor
// ReleaseResidency, nor a committed-public subscription, nor a command applier.
// But harness SEGREGATES them rather than omitting them: session.IdleWaiter,
// session.Liveness, session.Releaser, session.CommittedPublicEventSource with
// its session.CommittedPublicEventProvider, and runtimecommand.Applier with its
// runtimecommand.Provider are all exported, and each documents the assertion as
// the way a caller is meant to discover it.
//
// SO THIS PACKAGE ASSERTS ON HARNESS'S OWN NAMES, never on a private structural
// twin. A private interface cannot produce a compile failure against a foreign
// type — a released signature change would arrive as a silent ok == false and an
// IncapableSessionError at run time, which is the opposite of the guard such a
// declaration appears to offer.
//
// WHAT REMAINS UNENFORCED IS NARROWER, AND IT IS HARNESS'S OWN STATED HAZARD:
// the assertion is on the DYNAMIC type, so a wrapper that fails to forward a
// method silently opts its session out. session.IdleWaiter's doc says exactly
// that, and adds that nothing pins rig to keep returning the runtime type
// unwrapped.
//
// # Findings — the both-directions audit of step 2
//
// H1. NEWSESSION TAKES NO IDENTITIES, NO WORKSPACE ROOT AND NO STORAGE CONTEXT.
// department.RigCreateRequest carries TenantID, SessionID, AgentID, Placement,
// WorkspaceRoot and Storage.Namespace. rig.Rig.NewSession takes only
// SessionOptions, of which the released set is one: WithSeedSnapshot. The
// workspace root and the resource storage are fixed when the Rig is DEFINED, not
// when a session is created. So a Host that materializes a per-session workspace
// cannot hand it to a shared rig, and this adapter takes a per-request rig
// instead — see Rigs. The fake accepts all six fields and asserts on them.
//
// H2. RESTORESESSION TAKES ONLY A UUID. department.RigRestoreRequest carries the
// same six fields; rig.Rig.RestoreSession(ctx, id) accepts none of them. Every
// one is dropped. This adapter does not hide the drop and it cannot repair it:
// the request is forwarded to Rigs.RigForRestore, which is the one place a
// composition can honour a workspace root or a storage namespace, and past that
// seam only the UUID crosses. An adapter that took the request and forwarded
// nothing would look like it propagated six fields.
//
// H3. THE LIVE SUBSCRIPTION CANNOT BE POSITIONED. Host's PublicationSubscriber
// delivers "committed public journal events AFTER the given event", which is how
// a client joins the live path with a bounded journal read. event.EventFilter
// selects loops and classes; it has no start position and there is no other
// parameter. A non-empty resume point is therefore refused rather than silently
// served from now — a silently-repositioned subscription is a gap in a client's
// journal that neither side can see.
//
// H4. THE PUBLICATION IS ASSEMBLED, NOT PROJECTED. event.Delivery carries
// EventID, JournalSeq, PublicBody and CoveredThrough — everything
// sessionwire.EnduringPublication needs except the tenant and the session, which
// harness does not know. This adapter supplies them from the launch request. It
// also drops deliveries that are not committed publications, which the fake's
// closed channel never produces.
//
// H5. ADMITTED REQUIRES A LEASE EPOCH AND THE COMMAND SEAM CARRIES NONE.
// runtimecommand.Admitted.Validate refuses a zero LeaseEpoch outright.
// department.RuntimeCommand has no epoch member at all, and commands.Applier
// constructs it from a record without one. The epoch is supplied to this adapter
// at bind time instead; a session bound without one cannot apply anything.
//
// H6. HARNESS APPLIES ONLY input AND interrupt. runtimecommand.Kind is a closed
// two-member set. commands.Kind has five: create, restore, input, interrupt and
// gate_response. The three with no counterpart are refused here rather than
// forwarded as an unknown kind, which Admitted.Validate would reject after the
// application prefix had already been written.
//
// H7. AN input COMMAND MUST CARRY DECODED content.Blocks, AND HOST'S PAYLOAD IS
// OPAQUE BYTES. department.RuntimeCommand documents the body as travelling
// opaque because "Host is not the semantic validator of a command body —
// Harness is". Admitted.Blocks is []content.Block and Validate refuses an input
// with none, so somebody must decode. This adapter takes a decoder rather than
// guessing an encoding, and refuses when it has none.
//
// H8. AN OBJECT-REFERENCED PAYLOAD CANNOT CROSS AT ALL. RuntimeCommand.PayloadRef
// exists so a large private body is dereferenced by the runtime instead of
// travelling through Host's memory. Admitted has no reference member and no
// object reader, so a referenced payload is refused. The fake accepts one and
// records it.
//
// H9. HARNESS WRITES THE APPLICATION PREFIX ITSELF, AND WRITES IT UNDER ITS OWN
// SESSION SCOPE. harness/pkg/sessionstore opens the released store with
// WithLegacySingleTenant("local") and derives the session id from the Harness
// UUID, so every record a session commits — the opening fence, the public
// events, and the EnvelopeKindApplicationPrefix that Applier.ApplyRuntimeCommand
// writes before each effect — lands under ("local", <uuid>) and not under Host's
// (TenantID, SessionID). The two layouts are MUTUALLY EXCLUSIVE at the backend:
// whichever store initializes one first, the other refuses it with
// KeyspaceError{layout_mismatch}. Measured in both directions by
// TestHostCannotOpenABackendHarnessInitialized and
// TestHarnessCannotOpenABackendHostInitialized.
//
// THE CONSEQUENCE IS NOT "the correlation reports absent". It cannot:
// commands.Applier appends its own prefix under Host's scope BEFORE driving the
// runtime, so Host's scope always holds a prefix and the walk always finds one.
// The two states it does reach are these, and they are different failures.
//
//   - IN THE LIVE PATH THE OUTCOME IS "unresolved": the walk finds the prefix,
//     and behind it neither the effect — which harness committed in its own
//     scope — nor a superseding fence. Unresolved refuses BOTH settlements, so
//     the command is stuck in applying, completable by nobody and rejectable by
//     nobody. Under the released modules the apply path does not terminate at
//     all. Measured by sessionstoreadapter's
//     TestApplierRunsTheWholeProtocolAgainstTheReleasedStore, which drives two
//     passes and finds the record still applying after both.
//   - AFTER A TAKEOVER THE OUTCOME IS "abandoned": a successor Host's
//     OpenJournal commits an opening fence above the prefix's epoch, which is
//     precisely the shape the walk reports as abandoned.
//     commands.Application.provesNoEffect admits abandoned, so the settlement
//     that licenses a REJECTION opens over a command whose effect harness
//     durably committed. That is a rejection written over a committed effect,
//     and it is reached by crash-and-takeover rather than in steady state.
//
// Nothing in this package can fix either. Harness must accept Host's
// (TenantID, SessionID) and file its records under them, or SessionStore must
// offer a way for two stores to share a keyspace. It is a release owed.
package harnessadapter
