// Package sessionstoreadapter binds Host's narrow local store seams to the
// released github.com/looprig/sessionstore v0.6.0 Store.
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
//	residency.SessionLeases.Acquire…    Store.OpenJournal                (F2, fused)
//	residency.Lease.Epoch               JournalWriter.Epoch
//	residency.Lease.Lost                none                             (F3)
//	residency.Lease.Release             JournalWriter.Close
//	residency.JournalFencer.Commit…     Store.OpenJournal                (F2, fused)
//	residency.DurableStore.LoadSess…    Store.GetCatalogEntry            (F4, F5, F13)
//	residency.Workspaces.Ensure/Rel…    none                             (F6)
//	host.WorkspaceProvider.Ensure…      none                             (F6)
//	residency.Locations.PublishResi…    Store.PutHostRegistration        (F14)
//	residency.Locations.TombstoneRe…    Store.ClearHostRegistration
//	commands.Inbox.ListOrdered          none                             (F7)
//	commands.Cursors.LoadCursor         none                             (F8)
//	commands.CursorWrites.SaveCursor    none                             (F8)
//	commands.CommandRecords.LoadCom…    Store.GetCommand                 (F11)
//	commands.CommandRecords.LoadPay…    Store.GetCommand                 (F9)
//	commands.Applications.FindAppli…    Store.FindCommandApplication
//	commands.Gates.LoadGate             Store.ReadGates                  (F10)
//	commands.InboxWrites.ClaimComma…    Store.ClaimCommand               (F12)
//	commands.InboxWrites.BeginApply…    Store.BeginApplyingCommand
//	commands.InboxWrites.CompleteCo…    Store.CompleteCommand
//	commands.InboxWrites.RejectComm…    Store.RejectCommand
//	commands.JournalWrites.AppendAp…    JournalWriter.Append
//	commands.Fence.Held/Lost/Write      none                             (F3)
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
// F2. THE LEASE AND THE OPENING FENCE ARE ONE CALL, not two. residency models
// AcquireSessionLease and CommitOpeningFence as separate seams, and step 3 of
// the attach can therefore fail with the lease held. Store.OpenJournal acquires
// the lease, reads the tip once and commits the opening fence at that tip, and
// releases the grant itself on every failure after the grant. The direction that
// fails is fake→store: a fake can produce "lease granted, fence refused, lease
// still held", and the released store cannot. openSession below fuses them and
// reports the fence conflict with the grant already gone.
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
// is what JournalFencer.CommitOpeningFence stamps into the in-stream fence.
// v0.6.0 splits the two deliberately and gives them distinct types —
// ResidencyEpoch, the Host's orchestration grant, and JournalEpoch, "a runtime
// grant in a session's bound agent journal" — and forbids the identification in
// terms. Neither released type satisfies residency.Lease alone: ResidencyGrant
// has Lost() and an epoch that must not stamp a journal fence, JournalWriter has
// the journal epoch and no Lost(). THE FAKES DO NOT EXPRESS THIS AND CANNOT BE
// MADE TO BY A FIXTURE — fakeLease hands out one uint64 that plays both roles,
// which is a state production has no way to produce. Whether Host tracks two
// grants or drops one is a design decision, not a rebind, and it is left to
// O5.3 rather than guessed at here.
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
