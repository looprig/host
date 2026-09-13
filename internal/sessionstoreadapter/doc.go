// Package sessionstoreadapter binds Host's narrow local store seams to the
// released github.com/looprig/sessionstore v0.7.0 Store.
//
// EVERY ADAPTER HERE DELEGATES. Where residency and commands describe a concept
// the released module owns — the epoch fence, the inbox compare-and-swap, the
// journal application prefix, the durable Host route — the method below is a
// translation of identities and a classification of errors, and nothing more.
// A concept re-implemented here would be a second authority for a durable rule
// the store already enforces, which is the failure mode O3.3 exists to end
// rather than one to relocate.
//
// # What this package is the audit of
//
// Host's twelve accepted tasks were written against local interfaces and local
// fakes; no compiled Host file named sessionstore until O3.3. The table below is
// the step-1 inventory: every local seam that stands in for SessionStore, the
// released call it binds to, and — where there is none — the finding that
// replaces the binding. It is kept in the package that would have to change if
// any row became false.
//
//	LOCAL SEAM                          RELEASED COUNTERPART
//	host.SessionStore.LoadSession       Store.GetCatalogEntry            (F1, F13)
//	residency.SessionLeases.Acquire…    Store.AcquireResidency           (F16)
//	residency.Lease.Epoch               ResidencyGrant.Epoch
//	residency.Lease.Lost                ResidencyGrant.Lost              (F3, closed)
//	residency.Lease.Release             ResidencyGrant.Release
//	Store.OpenSession                   Store.OpenJournal                (F2, unwired)
//	commands.JournalWrites.AppendAp…    JournalWriter.Append             (F2, unwired)
//	residency.DurableStore.LoadSess…    Store.GetCatalogEntry            (F4, F5, F13)
//	residency.Workspaces.Ensure/Rel…    none                             (F6)
//	host.WorkspaceProvider.Ensure…      none                             (F6)
//	residency.Locations.PublishResi…    Store.PutHostRegistration        (F14)
//	residency.Locations.TombstoneRe…    Store.ClearHostRegistration
//	commands.Inbox.ListOrdered          none                             (F7)
//	commands.Cursors.LoadCursor         none                             (F8)
//	commands.CursorWrites.SaveCursor    none                             (F8)
//	commands.CommandRecords.LoadCom…    Store.GetCommand                 (F11, unwired)
//	commands.CommandRecords.LoadPay…    Store.GetCommand                 (F9)
//	commands.Applications.FindAppli…    Store.FindCommandApplication
//	commands.Gates.LoadGate             Store.ReadGates                  (F10)
//	commands.InboxWrites.ClaimComma…    Store.ClaimCommand               (F12, unwired)
//	commands.InboxWrites.BeginApply…    Store.BeginApplyingCommand       (unwired)
//	commands.InboxWrites.CompleteCo…    Store.CompleteCommand            (unwired)
//	commands.InboxWrites.RejectComm…    Store.RejectCommand              (unwired)
//	commands.Fence.Held/Lost/Write      none                             (F3)
//
// "UNWIRED" IS A STATEMENT ABOUT THE COMPOSITION, NOT ABOUT THIS PACKAGE. Every
// row so marked is bound, exported and tested against the released store here,
// and no composed Host calls it: compose.Options declares no session opener and
// no application seams, and the Processor a composed Host runs is
// the removed dispatch refusal. They are the legacy command family, which a Host cannot
// reach on a session it can hold, and they are what the attempt-aware applier
// will be rebuilt from rather than dead weight. Do not re-wire one without
// reading commands/dispatch.go.
//
// Seams NOT in this table stand in for nothing durable: registry.LocalRegistry,
// residency.HeartbeatRegistry, residency.TeardownObserver, residency.Ownership,
// residency.OwnershipHandle, residency.Admissions, commands.Processor,
// hostlink.Residencies, hostlink.Admission, hostlink.CommandConsumer(s) and
// hostlink.Authenticator are each satisfied by a Host type in this module. They
// are seams for composition, not stand-ins for a released module, and binding
// them here would invent a dependency rather than record one. host.AuthVerifier
// is the one seam whose counterpart is a module Host does not name at all
// (credentials/secrets), so it stays local by the same rule that kept drain out.
//
// # Findings — the both-directions audit of step 2
//
// Each row is a place a fake exhibits behaviour the released module does not,
// or a released behaviour no fake can produce. They are numbered because they
// are cited from the code that has to live with them.
//
// F1. host.SessionStore.LoadSession returns opaque []byte and the store returns
// a decoded CatalogEntry. The direction that fails is store→fake: no released
// call answers with bytes, so the adapter must choose an encoding, and any
// choice it makes is Host's rather than the store's. The seam has one
// production caller shape (existence and classification), so the adapter
// answers with the record's canonical JSON and callers are told not to parse it.
//
// F2. THE LEASE AND THE OPENING FENCE ARE ONE CALL, not two — AND THE FINDING IS
// NOW MOOT FOR HOST, which is worth more than the row itself. It described
// residency modelling AcquireSessionLease and CommitOpeningFence as separate
// seams while Store.OpenJournal fuses them, so a fake could produce "lease
// granted, fence refused, lease still held" and the released store could not.
// Host no longer commits an opening fence at all: the fence bound the session
// scope to ProtocolModeLegacy and therefore failed for every session a Host can
// hold, and the journal grant is the RUNTIME's. residency.JournalFencer is gone
// and nothing in a composed Host calls OpenSession. The row stays because
// Store.OpenSession is still exported here and still has this shape, and a
// future caller must know the fusion is the store's rather than this adapter's.
//
// F3. LEASE LOSS IS NOT OBSERVABLE AS AN EVENT — AND THE SIGNAL EXISTS ONE LAYER
// DOWN. residency.Lease.Lost is a channel a heartbeat and a drain supervisor
// select on. storage.Leaser HAS exactly that: its contract says "Lease.Lost
// closes when it can no longer safely assert ownership, including Release,
// expiry, or takeover". sessionstore acquires such a lease inside OpenJournal
// and KEEPS IT UNEXPORTED — JournalWriter publishes Epoch, Sequence, Append and
// Close and nothing else — so the capability Host needs is present in the
// dependency and swallowed by the module above it. That makes this a
// sessionstore release owed (expose the grant's loss channel), not a missing
// capability.
//
// Until then the adapter's Lost() is closed by the ADAPTER when a delegated call
// classifies as lease_lost or fenced, so it is SILENT FOR AN IDLE SESSION: a
// Host that stops writing stops learning. It is also why the takeover arm is
// unreachable from this package's tests — see TestLostReportsOnlyWhatThisGrantDid
// — and why O7.1's Fence adapter cannot be discharged by delegation; see the
// grantFence comment in applier_test.go.
//
// F3 AT O3.4 — THE RESIDENCY HALF IS NOW CLOSED AND THE JOURNAL HALF IS NOT.
// residency.Lease is no longer satisfied by Grant at all; it is satisfied by
// ResidencyLease in residency.go, whose Lost() is ResidencyGrant.Lost() —
// Storage's own lease channel, "the provider's actual notification, never
// inferred from journal events, release errors or caller cancellation". So the
// heartbeat and the drain supervisor now select on a real signal. What remains
// open is exactly what the paragraphs below describe: the JOURNAL grant still
// publishes no loss channel, so commands.Fence and Grant.Lost are still this
// adapter's own echo. The paragraphs are kept rather than rewritten because the
// finding they record did not go away, it narrowed.
//
// AND IT IS NOT A TAKEOVER CLAIM. ResidencyGrant's liveness is the provider's:
// its doc says memstore offers neither TTL nor crash takeover, so under this
// module's tests the only thing that closes the channel is a release. A real
// takeover measurement needs pgstore behind PGSTORE_TEST_DSN and this module has
// no such lane.
//
// F3 AT v0.6.0 — PARTIALLY DISCHARGED, AND NOT THE PART THIS SEAM NEEDS. The
// rebind re-ran this row against the released module and the answer changed
// without the finding going away, so it is recorded rather than deleted.
// v0.6.0 adds Store.AcquireResidency returning a *ResidencyGrant with exactly
// Epoch(), Lost() and Release() — the shape residency.Lease declares, and the
// loss channel this row said was owed. But it is a DIFFERENT LEASE. The grant
// is taken in the session's ".../residency/lease" namespace, its epoch is the
// distinct type ResidencyEpoch whose own doc says it "must never be compared
// with, or used as, a journal epoch", and the type states that "residency loss
// does not fence a journal: a successor must independently acquire the journal
// grant before application". JournalWriter — the object Grant below actually
// wraps — still publishes Epoch, Sequence, Append and Close and NO Lost. So the
// journal grant's loss is still unobservable, this package's Lost() is still
// the adapter's own signal, and everything above about idle silence still holds.
//
// F4. THE DURABLE STATE CARRIES NO NAMESPACE. residency.SessionState.Namespace
// is required whether or not the session exists, and is documented as read from
// the store "because the layout is SessionStore's". sessionstore v0.6.0 exposes
// no object namespace, prefix or layout accessor at all; the layout is internal.
// The adapter cannot answer this member from the store, so it is Host's to
// derive or SessionStore's to publish. Reported, not papered over.
//
// F5. THE DURABLE STATE CARRIES NO RIG SESSION ID — SUPERSEDED AT v0.6.0, AND
// REPLACED BY THE OPPOSITE DEFECT. As written against v0.1.0 this said
// CatalogRecord had no member for residency.SessionState.RigSessionID and no
// SessionPointerKind named one. v0.6.0 adds SessionBinding, immutable at
// creation and carried on CatalogRecord, whose RuntimeSessionID is exactly that
// durable member and is reachable through the GetCatalogEntry this package
// already calls. The store no longer has nothing to restore from.
//
// What replaces the finding is F11's shape in the other direction.
// SessionBinding.RuntimeSessionID is a bounded opaque UTF-8 string, validated by
// validateOpaque and by nothing else, and residency.SessionState.RigSessionID is
// a uuid.UUID. HOST'S SEAM IS THEREFORE TIGHTER THAN THE STORE: a binding
// written by an allocator that is not Harness is durable, readable and valid to
// the store, and cannot be represented in Host at all. That is not a reason to
// widen the seam blind — Host's restore really does hand the value to Harness,
// which really does want a UUID — but the refusal has to be Host's, stated, and
// reached before the launch rather than at a parse deep inside it.
//
// RELATED TO, AND NOT THE SAME AS, H9's COST 2 in internal/harnessadapter. They
// point in OPPOSITE directions over DIFFERENT subjects: this row is Host's seam
// being too narrow for a durable value the store will hand it, and Cost 2 is
// Harness's keyspace being too narrow for the identity Factory admitted. They
// are recorded separately because either could be fixed alone. But ONE decision
// about whether a Host session identity is a UUID probably settles both, so a
// reader resolving one should look at the other rather than assume it followed.
//
// F6. NO RELEASED CALL MATERIALIZES A WORKSPACE BY TENANT AND SESSION. Harness's
// workspacestore is content-addressed — Materialize(ctx, Ref, dest) — and picks
// neither the destination nor the pairing. EnsureWorkspace/ReleaseWorkspace are
// therefore a Host responsibility over a released Ref, not a delegation.
//
// F7. THERE IS NO PER-SESSION ORDERED INBOX READ. commands.Inbox.ListOrdered
// pages one session's commands strictly after an acceptance order. The store's
// only listing is ListDueCommands, which pages ONE CONTROL SHARD by deadline
// across every tenant and session, skipping rows it cannot decode. The two are
// not the same query and one cannot be built from the other without reading the
// whole shard. This is the seam the consumer's entire ordering rule stands on.
//
// F8. THERE IS NO DURABLE CONSUMPTION CURSOR. commands.Cursors is §10.4's
// per-session cursor. SessionPointerKind is a closed set of three roles —
// active-continuation, workspace-checkpoint, runtime-checkpoint — and none is a
// consumption cursor. F7 and F8 together are why internal/commands has no
// adapter for its read path here.
//
// F9. THE PAYLOAD IS NOT A SEPARATE LOAD. commands.CommandRecords declares
// LoadCommand and LoadPayload as two calls so that "the payload is loaded AFTER
// the claim" is a thing a test can see. InboxRecord carries Payload and
// PayloadRef inline, so one GetCommand answers both. The direction that fails is
// fake→store: a fake can refuse a payload load to a caller that has not claimed,
// and the store cannot — it hands the body to any reader of the record.
//
// F10. THE GATE PROJECTION NAMES NO OWNER. commands.Gate carries OwnerHostID and
// OwnerEpoch, and §9.4's release-race backstop is decided from them.
// Store.ReadGates answers with sessionwire.GatePage, whose GateProjection has
// gate id, kind, prompt, opened event and sequence, deadline and answerability —
// and no owning Host and no lease epoch. The adapter therefore CANNOT answer
// LoadGate soundly and refuses rather than answering with zero owners, which
// would read as "owned by no Host" and make every gate response resumable.
//
// F11. THE STORE IMPOSES NO UUID GRAMMAR ON THE RUNTIME COMMAND IDENTITY.
// sessionstore.RuntimeCommandID is validated as bounded opaque UTF-8, and the
// package says why: a grammar check would be a second statement of a rule it
// does not own. commands.Record and department.RuntimeCommand both declare
// uuid.UUID, so a durable record written by an allocator that is not Harness
// cannot be represented in Host at all. Measured in
// TestLoadCommandRefusesARuntimeIdentityThatIsNotAUUID, which first admits such
// a record through the store and only then finds the adapter refusing it.
//
// F12. A LOST COMPARE-AND-SWAP IS INDISTINGUISHABLE FROM A STORE FAULT.
// commands.ApplyRefusal declares RefusalClaimHeld and RefusalDeadlinePassed, and
// the applier reaches both from its OWN precondition checks against a record it
// has read; the store's claim_held, deadline and terminal codes arrive after
// that decision and reach the applier as RefusalStore. classifyInbox promotes
// only InboxErrorEpoch, which is an ownership statement, and leaves the other
// eighteen alone — asserted in both directions by
// TestClassifyInboxReportsASupersededEpochAsTheSentinel and
// TestClassifyInboxLeavesEveryOtherCodeAlone. THE DEFERRAL IS DELIBERATE:
// commands/apply.go wraps a store failure with its Cause preserved, so the code
// stays recoverable by errors.As and only the label is coarse; promoting one
// would make a lost race indistinguishable from a precondition the applier
// evaluated, which is the identical-error mask. Whether a lost race should be
// separately diagnosable is O5/O6's decision, not this adapter's.
//
// F13. A SESSION THAT NEVER EXISTED IS NOT A CATALOG ERROR. GetCatalogEntry for
// a session with no scope binding fails at the KEYSPACE, with
// *KeyspaceError{binding_not_found}, before the catalog is consulted at all. An
// adapter matching only CatalogErrorNotFound and CatalogErrorDeleted therefore
// reports the commonest case — a brand new session on the create path — as a
// store failure. This was found by running the create path against the store and
// could not have been found against residency's fakeDurable, which answers
// SessionState{Exists:false} and has no error to classify. All three arms are
// asserted, in both directions, by
// TestIsCatalogAbsentAdmitsEveryWayTheStoreSaysThereIsNoRecord and
// TestIsCatalogAbsentRefusesEveryFailureThatIsNotAbsence.
//
// F14. THE DURABLE HOST ROUTE CARRIES NO WIRE VERSION.
// sessionwire.HostLinkRegistryObservation has a Version whose own Validate
// refuses an unsupported value; PutHostRegistrationRequest has no version member
// at all, and the store stamps CurrentWireVersion when it projects the record
// back. So a wholly delegating PublishResidency would be LOOSER than the seam it
// satisfies for exactly one member — it would write an observation Core refuses
// and read it back as current. PublishResidency checks that member rather than
// delegating it; see UnsupportedWireVersionError.
//
// F15. THE ONE LEASE HOST DECLARES IS TWO GRANTS IN THE RELEASED MODULE, IN TWO
// EPOCH DOMAINS. residency.Lease fuses them: Epoch() is documented as the value
// Core publishes as the registry observation's lease_epoch, and the SAME value
// was what residency.JournalFencer.CommitOpeningFence stamped into the in-stream
// fence, before that seam was removed.
// v0.6.0 splits the two deliberately and gives them distinct types —
// ResidencyEpoch, the Host's orchestration grant, and JournalEpoch, "a runtime
// grant in a session's bound agent journal" — and forbids the identification in
// terms. Neither released type satisfies residency.Lease alone: ResidencyGrant
// has Lost() and an epoch that must not stamp a journal fence, JournalWriter has
// the journal epoch and no Lost().
//
// THE BOUND MATTERS AND AN EARLIER VERSION OF THIS ROW GOT IT WRONG. It said the
// fakes' single uint64 "is a state production has no way to produce". That is
// FALSE IN LEGACY MODE, which is the mode Host is bound to today: Grant.Epoch in
// journal.go returns JournalWriter.Epoch, residency's manager takes that one
// uint64 at manager.go:986 and both stamps the opening fence with it and
// publishes it as the registry observation's lease_epoch. So in legacy mode the
// RELEASED STORE genuinely produces one number playing every role, and fakeLease
// models it correctly.
//
// F15 IS A DISPOSITION-MODE FINDING. The conflation becomes wrong exactly where
// v0.6.0 introduced the split, and it has two live readers there: a disposition
// attempt records JournalEpoch and ResidencyEpoch as separate members, and the
// settlement fence orders AuthorJournalEpoch against AttemptJournalEpoch. A Host
// that copied its residency epoch into JournalEpoch would make a disposition
// command UNSETTLEABLE FOREVER. So the finding stands and is booked as O3.4;
// what was wrong was its scope, not its existence.
//
// F15 IS DISCHARGED AT O3.4, BY OWNERSHIP AND NOT BY FIELD COUNT. Host does not
// track two grants: it HOLDS one and READS the other. residency.Lease is narrowed
// to the residency grant and answers residency.ResidencyEpoch; the journal epoch
// is department.LeaseEpochReporter, a capability of the launched RUNTIME, and
// reaches Host as residency.JournalEpoch — a different defined type, so the
// substitution no longer compiles. Three structural consequences are worth
// naming, because each closes a place the fusion could come back:
//
//   - residency.JournalFencer is GONE, and its removal is the completion of this
//     line rather than a separate decision. CommitOpeningFence first lost its
//     epoch parameter — Host holds no journal grant, so there was no number it
//     could soundly name — and then lost its caller: Host makes no journal write
//     at any point, so there is nothing left for the fusion to come back into.
//   - harnessadapter's WithLeaseEpoch is GONE. It let a composition supply the
//     epoch every admitted command was applied under, and harness compares that
//     number for equality against the lease the runtime itself holds — right
//     only while two independent counters agreed. boundSession reads the
//     capability per command instead.
//   - the fakes are seeded from DIFFERENT BASES: residency mints residency epochs
//     from 1000 and testkit mints journal epochs from 1. Both used to start at 1,
//     which is why the fusion passed for twelve tasks.
//
// The LEGACY reading above stays true and stays here: against a legacy-mode
// session Store.OpenJournal really does hand out one number playing every role.
// It is recorded rather than deleted because the correction is bounded — legacy
// is not a supported deployment target, not a mode in which Host was wrong.
//
// F16. THE RESIDENCY GRANT IS ADMITTED ONLY FOR A DISPOSITION-MODE SESSION, AND
// A FAILED ACQUISITION CAN STILL OWE A RELEASE. Two properties of
// AcquireResidency that no fake in this module produces. First, it refuses any
// session whose immutable catalog binding is not ProtocolModeDisposition, with
// catalogInvalid("binding.protocol_mode"), and binds the session scope mode as a
// side effect; residency.SessionLeases.AcquireSessionLease grants
// unconditionally in every fake here. Host does not create catalog records — it
// only reads them — so WHICH protocol mode Host's sessions carry is decided by
// their creator and is a question for the program, not a defect in this module;
// it is recorded because a Host bound to AcquireResidency against legacy-mode
// sessions would fail at every attach with a validation error and no fake would
// have predicted it. Second, ResidencyAcquireCleanupError is an error that
// returns no grant AND leaves a provider lease and a Store admission held,
// obliging the caller to retry Release until it succeeds or Store.Close times
// out. AcquireSessionLease's (Lease, error) signature cannot represent "refused,
// and you still owe a release": every fake here treats a non-nil error as
// nothing acquired. THE FAKE IS LOOSER THAN THE DEPENDENCY in the way that leaks
// rather than the way that fails.
//
// # What step 4 could run against the adapter, and what it could not
//
// The instruction is to run the residency, commands and hostlink suites against
// both the fakes and the adapters. Which of them CAN run is itself a result:
//
//   - commands' APPLY path runs, end to end, against sessionstore v0.6.0 over
//     Storage's memory provider. TestApplierRunsTheWholeProtocolAgainstTheReleasedStore
//     drives commands.Applier.Process through the record read, the claim, the
//     payload load, the applying transition and the journal application prefix,
//     and then asserts the durable state a successor's recovery would read.
//   - commands' CONSUME path cannot run at all: it is built on Inbox.ListOrdered
//     and Cursors, and neither has a released counterpart (F7, F8).
//   - residency's LOCATIONS and DURABLE-STATE seams run individually, and the
//     ATTACH does not: it needs Workspaces, which delegates to nothing (F6), and
//     its lease/fence sequencing is a state the released store cannot produce
//     (F2). Building a two-seam shim over the fused call would have hidden F2
//     rather than reported it, so this package does not have one.
//   - hostlink's seams stand in for nothing durable, so there is nothing to
//     rebind: see the paragraph on composition seams above.
//
// # Two properties of this package's own tests, stated rather than assumed
//
// THE FINDING ROWS CITE TESTS BY NAME AND THE CITATIONS ARE GUARDED.
// TestCommentsCiteTestsThatExist in the root package resolves every Test…
// identifier appearing in a comment in this package and in harnessadapter
// against the module's real test functions. It exists because two rounds of this
// task shipped a row whose evidence did not exist, and because nothing else
// notices: TestDocCommentsNameTheirOwnDeclaration asks whether a comment names
// the declaration it is ATTACHED to, which is a different question.
//
// PublishResidency IS THE ONE PLACE THIS PACKAGE IS NOT PURELY MECHANICAL.
// Every other method here delegates; that one adds a refusal of its own for
// F14's member, because delegating it would be strictly looser than the seam.
// It is called out here so "the adapters are mechanical" is not read as covering
// this file without qualification.
package sessionstoreadapter
