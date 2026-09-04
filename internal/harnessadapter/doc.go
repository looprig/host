// Package harnessadapter binds Host's department seams to the released
// github.com/looprig/harness v0.31.0 rig and session API.
//
// # Step-1 inventory
//
//	LOCAL SEAM                              RELEASED COUNTERPART
//	department.Rig.NewSession               rig.Rig.NewSession          (H1, H2)
//	department.Rig.RestoreSession           rig.Rig.RestoreSession      (H2)
//	department.RigSession.ID                session.Session.SessionID
//	department.IdleWaiter.WaitIdle          Session.WaitIdle
//	department.Liveness.Done                Session.Done
//	department.Releaser.ReleaseResidency    Session.ReleaseResidency
//	department.PublicationSubscriber.Sub…   Session.SubscribeCommittedPublicEvents (H3, H4)
//	department.CommandApplier.ApplyCommand  runtimecommand.Applier      (H5, H6, H7, H8)
//
// THREE OF THOSE ARE NOT ON A PUBLISHED INTERFACE. session.SessionController —
// the type rig.NewSession and rig.RestoreSession return — declares neither
// WaitIdle, nor Done, nor ReleaseResidency, nor a committed-public subscription,
// nor a command applier. Each is a method on harness's internal concrete
// session, reached by type assertion, exactly as department.adaptRigSession
// already reaches Host's. That is workable and it is also unenforced: nothing in
// the released API says a SessionController has them, so a future harness
// composition that returns a different implementation breaks Host at run time
// rather than at compile time.
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
// SESSION SCOPE. See the package test TestHarnessAndHostDoNotShareASessionScope,
// which measures it: harness/pkg/sessionstore opens the released store with
// WithLegacySingleTenant("local") and derives the session id from the Harness
// UUID, so every record a session commits — the opening fence, the public
// events, and the EnvelopeKindApplicationPrefix that Applier.ApplyRuntimeCommand
// writes — lands under ("local", <uuid>) and not under Host's (TenantID,
// SessionID). commands.Applier appends its OWN prefix under Host's scope and
// then asks Host's scope for the correlation, so against the released modules
// the correlation can only ever report absent. Nothing in this package can fix
// that; it is a release owed.
package harnessadapter
