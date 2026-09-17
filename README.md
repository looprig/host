# looprig/host

`host` is Looprig's Department runtime host. A Host is a tenant-scoped process
that keeps runtime targets resident, consumes commands addressed to it, serves
the HostLink realtime surface, and drains on release.

Host is **consumed by** Factory. It imports no Factory package, no web or
terminal UI, and no product repository. That boundary is enforced by
[`import_boundary_test.go`](import_boundary_test.go), which parses the import
declarations of every Go file the module owns rather than searching source
text, and fails loudly if it walked nothing.

A nested module or repository under `host/` would fall outside that walk by
construction, so `nestedModuleAllowlist` is empty and a separate assertion fails
on any undeclared one: a silent hole is converted into a deliberate exemption.

Centrifuge is permitted in exactly one directory,
`internal/realtime/hostlink/`, and the scope is derived from a single constant
and compared segment by segment, so a sibling package under
`internal/realtime/` does not inherit it.

## Status

Department, residency, HostLink, warm release and drain are built.
`internal/sessionstoreadapter` binds them to the released
`github.com/looprig/sessionstore` v0.10.0 store, and `internal/harnessadapter` to
`github.com/looprig/harness` v0.34.0.

**A composed Host applies a command end to end: it attaches a disposition
session, consumes its durable command stream, claims a command under its
residency grant, authorizes exactly one dispatch attempt, drives the runtime with
that attempt's identity and settles the command from the runtime's own durable
disposition.** Read the next section before you plan around it; the mechanism is
measured, not cautionary.

## Known limitations you must read before deploying

These are properties of the code as it stands, not a backlog. Each is written
here because an operator who meets it for the first time in production has been
surprised by something this file could have told them.

### A HostLink drain is tenant-scoped, so whole-Host drain is the process lifecycle's alone

**This entry replaced a deployment gate.** It used to say a whole-Host drain
request naming an empty `TenantID` short-circuited the tenant check, so a link
authenticated as tenant-a could start a drain covering tenant-b's resident
sessions — availability, not confidentiality — and that **pooled multi-tenant
deployment was gated on it**. R-1 closed the hole and **that gate is lifted**.
What follows is what is now refused, stated no wider than it is.

**`internal/realtime/hostlink/drain.go`, `drainScope`.** Two requests are
**refused**, each with `RefusalWrongDrainScope` and the Core class
`runtime_unavailable`, and each **before the drain state machine** — a `Drainer`
that has begun cannot be un-begun.

1. **A request naming an empty `TenantID`** — the whole-Host scope — is refused
   **before the tenant comparison**, because there is no tenant on it to
   compare. Every link this package serves is tenant-authenticated
   (`NewMultiplexer` requires a valid `TenantID`), so that scope has no link it
   may arrive on.
2. **A request naming this Host's fixed session whose requesting tenant does not
   currently hold that session** is refused at the **last** rung, on a
   `Residencies.Get` of `{the link's tenant, the fixed session}`. Without it the
   resolver compared a session against a session and **attributed nothing**: a
   dedicated Host's `FixedSessionID` is one bare `SessionID` handed to *every*
   tenant's `Multiplexer`, so a link authenticated as tenant-b holding nothing
   could name it and begin the Host-wide drain that released tenant-a's session.

**The attribution in (2) is sound because of capacity, and it is sound only
because of capacity.** `Options.validatePlacement` pins `Capacity` to 1 for
dedicated placement, so a Host with a fixed session holds **at most one**
resident session; "this tenant holds the fixed session" and "this tenant owns
everything a Host-wide drain would touch" are therefore the same statement. On a
Host that could hold two tenants at once they would not be, and this rung would
not be sufficient.

**So exactly one drain is reachable over HostLink:** a **dedicated** Host's fixed
session, asked for by the tenant that **currently holds it**. **A pooled Host
answers no HostLink drain at all**: it has no fixed session, so the session
scope is refused too.

**A dedicated Host refuses every HostLink drain between process start and its
first attach, and that is a behaviour rather than a caveat — Factory will meet
it.** Rung (2) reads current residency, so until a session is resident there is
no tenant that holds the fixed session and every drain request is answered
`runtime_unavailable` with `RefusalWrongDrainScope`. **The reason is that the
refusal is the correct answer**: a Host holding nothing has nothing for a
HostLink drain to cover, and the alternative — acknowledging a drain of nothing
— would hand a placement controller a generation it could poll to "drained"
without anything having been released. **What a controller should do** is take
the process-lifecycle path (terminate the workload) for a Host it wants gone
before it has been used, and reserve the HostLink drain for a Host that is
actually holding the session it placed.

**The too-early refusal is indistinguishable from the refusal a tenant gets when
ANOTHER tenant holds this Host's fixed session** — same `Refusal`, same reason,
same Core class, because both are rung (2). It is **not** the refusal a foreign
tenant gets: a request naming a tenant other than the link's is refused three
rungs earlier with `RefusalForeignTenant`. So the one distinction a controller
cannot make is "nothing is attached yet" versus "someone else's session is
resident here".

**That is deliberate and must stay.** Distinguishing the two would disclose
**cross-tenant occupancy** — telling tenant-b that *somebody* is resident on a
Host advertising `cross_tenant_isolated` — which would pay for an availability
fix with a confidentiality leak, and confidentiality was never what R-1 was
about. The cost falls where it can be borne: a placement controller can resolve
the ambiguity from **its own attach state**, which it has and this Host does not
owe it. Do not "fix" this by splitting the refusal.

**AMENDED (O7.3): rung (2) is relaxed on the OBSERVATION path, and only there.**
`hostlink.drain_status` also admits the caller that **began this very drain**,
matched on the tenant **and** session the drain was begun with. Without it the
terminal `drained` answer was unreachable to the only caller entitled to it:
release drops the residency, so rung (2) started refusing the beginner at
exactly the instant the answer it was waiting for became true. **The
mutator path is NOT relaxed** — `hostlink.drain` after release is still refused,
because beginning a Host-wide drain is attributed to a holder. **It discloses
nothing new**: the match set is a subset of "callers that already received the
acknowledgement", a tenant that never held the fixed session began nothing and
is refused identically, and that is measured, after completion, by
`TestRelaxingRungEightForObservationDisclosesNothingToAnyoneElse`. Do not
generalise the relaxation to "a drain has begun" or to the session alone; either
hands cross-tenant occupancy to any link that can name the fixed session.

### The transport closes when the process stops, never as a drain step

**This entry replaced a design that made the terminal drain answer
unreachable.** `internal/lifecycle` used to hold a `Link` seam and close it as
the drain's last cleanup step — before `settledState` assigned the terminal
state. So a HostLink-initiated drain destroyed the connection carrying the
`drained` answer *before that answer existed*, and the disconnect a Factory was
left with is Centrifuge `DisconnectShutdown` **3001 in every outcome** —
**including the one `settledState` deliberately withholds `drained` for**. That
signal is therefore **ambiguous between "release finished, delete the workload"
and "FinishRelease refused, this Host still holds the lease"**, and Core says
directly that `HostLinkDrainObservation` exists "for a Factory to observe
without inferring release completion from a transport close".

`compose.Service.Stop` closes the transports now, after `Wait` returns.
**"LAST" is unchanged** — it is still after every release step — and
`TestStopReleasesTheSessionAndClosesEveryTransportLast` still holds it. A
failure to close is recorded under `lifecycle.StepCloseLink` rather than
returned. `TestTheDrainOwnsNoTransportShutdown` is the reflective trip-wire: no
field of `lifecycle.Options` may be a transport shutdown.

**A link-drained Host stays up and keeps answering**, with `Ready() == false`
and `Live() == true`. That is not a new cost — `Run` only leaves on
`ctx.Done()`, so such a Host lingered before this change too; it simply lingered
refusing connections. **`Live()`'s doc used to be a sentence wider than its
probe** (it claimed liveness turns false once release finishes); the code was
always `started && !stopped`, the **code is the correct one**, and the doc was
fixed to match.

**A released session's routes are dropped explicitly.** Closing a transport used
to drop its links' bindings as a side effect, so nothing needed to say it. With
the transport surviving, `sessionWork.stop` calls `InvalidateSession` for the
released key — **scoped to the session, not the link**, so releasing one session
does not cut a Factory's routes to the others on the same connection.

**A closed Host answers 503, not 400.** `resolve` returned an anonymous error
that the handler mapped to `400 "HostLink requires a valid tenant"`, so a
Factory reconnecting to a stopped Host read a *client* error naming its own
credential. It is `errHostClosed` now, answered `503`.

The whole-Host drain is reachable **only** through the process-lifecycle path —
`Service.Stop` calls `Drainer.StartDrain` with the zero scope directly and never
passes through this resolver — which is the path a platform's termination signal
already takes, and it is unchanged.

**What this does NOT claim.**

- **Not "scoped by construction".** An earlier version of this entry said the
  fixed-session drain was scoped by construction. It was not, and a gate probe
  demonstrated the hole on the tree that claimed it. What scopes it is rung (2),
  which is a **line you can point at**, resting on the capacity rule above.
- **Rung (2) is a residency read, not an ownership proof.** It says this tenant
  holds this session on this Host *now*. It is not the lease, it grants nothing,
  and it is not durable evidence of anything.
- **Not a confidentiality claim**, which was never at issue: the defect was
  availability. No tenant could read another's material or address its sessions.
- **Not a Factory drain path.** A `cmd/controller` gets no drain over HostLink;
  if one is needed it must be **tenant-scoped or service-principal**, and
  re-admitting the empty tenant would reopen exactly what this closed.

Measured, in both halves, by behaviour and not by reading:
`TestATenantLinkCannotDrainAnotherTenantsSessions` (two tenants, both holding
live resident sessions, one shared `Drainer`) and
`TestADedicatedHostRefusesADrainFromATenantThatDoesNotHoldItsSession` in
`internal/compose`; `TestAWholeHostDrainIsRefusedOnEveryTenantAuthenticatedLink`
and `TestADrainOfTheFixedSessionIsRefusedUnlessTheLinkSTenantHoldsIt` in
`hostlink`.

Closed as `O7.1-hostwide-drain-crosses-tenants` / R-1.

### A composed Host applies a command, and what that word means

**This entry replaces three earlier ones, and the first two were wrong in the
same direction — a sentence wider than its evidence.** The first said a Host
"can consume its inbox against a real store", which was an ADAPTER result stated
about a composed Host. The second said a composed Host "cannot attach a
disposition session, so it consumes nothing", which was true until the
attach-time journal fence was removed. **The third said a composed Host
deliberately refuses to dispatch, and that is now false too**: it described
`commands.NoDispatch`, which has been **deleted**, and listed as future work the
three-repository sequence that has since landed.

**What a composed Host does.** It attaches a disposition session, reaches
`BeginOwnership`, starts the session's durable command consumer, and for each
command in acceptance order: **claims** it under the residency GRANT this store
issued (`ClaimDispositionCommand` takes a `*ResidencyGrant`, not an epoch, so a
caller cannot name one); **authorizes one attempt**, recording an immutable
identity together with the RUNTIME's own journal grant; **dispatches** it,
carrying that identity to the runtime; and **settles** it from the durable
disposition the runtime wrote. It steps over terminal records and advances the
durable consumption cursor.

This is measured in composition
(`internal/compose.TestAComposedHostAppliesADeliveredCommandEndToEnd`) and end to
end against both released stores on separate backends
(`internal/settlement.TestACommandAppliesAndSettlesAcrossBothReleasedStores`).

**What `applied` means, exactly, and it is narrower than the word.** *Under the
attempt's journal grant, the runtime durably recorded that it accepted this
command into its execution path.* It does **not** mean a turn started; it does
**not** mean a turn folded; it does **not** mean a later `TurnRejected` cannot
follow; and it does **not** mean a queued input survives a crash — the runtime's
loop mailbox is in memory. **Do not widen this sentence**, including in any
user-facing vocabulary: a product-level "applied" for an input that a crash then
discards is a claim this Host does not make.

**Host authors no outcome.** `SettleDispositionCommand` takes a command, a
revision and a residency and nothing else; the store derives what it expects from
its own immutable record, obtains the evidence through its configured reader and
verifies it before the write. Host reports the arm the store chose.

**Missing evidence is not proof that nothing was applied.** A settlement that
cannot read a disposition leaves the command `applying` at an unmoved revision
and blocks the pass. A Host that re-dispatched on absence would re-drive an
effect that may already have committed. Host's honest exactly-once claim is
**"at most one authorized attempt, settled only from the runtime's durable
disposition"**.

**There is no Host-authored rejection once an attempt exists**, and none before
one either: the only pre-attempt reason to reject is the apply deadline, and
§10.4 gives that to Factory's deadline reconciler. `RejectDispositionCommand` is
deliberately not bound.

### What a deployment must wire, and what Host cannot check

A disposition session's journal is **not** in the orchestration store. The
immutable binding says where it is, and a harness `Store` answers only for the
keyspace it owns — it holds no registry of the bindings it serves, so it cannot
route on `Binding.StorageBindingID`. A product therefore supplies
`Bootstrap.JournalStores()`, one reader per binding; Host builds the router and
hands it to `Bootstrap.Store(ctx, evidence)`, which **must** open the store with
`sessionstore.WithDispositionEvidence`.

**Three wiring conditions are unenforceable, and they share one signature** —
each fails only after every dispatch has happened, strands the command
`applying` with a real effect behind it, and is invisible until the first
settlement:

| # | condition | diagnosed as |
|---|---|---|
| (a) | the product takes the reader and drops it | `evidence_unavailable` |
| (b) | the orchestration tenant and the journal store's tenant disagree | **`evidence_unroutable`** |
| (c) | a router key is present but names the **wrong** journal store | `evidence_unavailable` |

(b) and an unregistered binding are **machine-distinguishable inside Host** and
are reported as `evidence_unroutable`, which points an operator at the wiring
rather than at a runtime that did its job. **(c) is genuinely ambiguous at
settlement time and correctly so** — that store is healthy, at the correct
tenant, and truthfully reports it holds no such record. Closing it needs a
construction-time answer (a journal store declaring which bindings it serves)
that no released API offers; a harness `Tenant()` accessor would close (b) and
**not** (c).

**Operationally**, a blocked pass answers the delivery `accepted`, makes no
further progress, leaves `Consumer.Failures()` at zero (a blocked pass returns a
nil error) and logs nothing. The signal is the Prometheus gauge
**`host_sessions_command_blocked`**. Unlike before, a non-zero value **is** a
fault: it means a command this Host could not settle, not one it refused to
start.

### Host opens no journal writer, and must not

The attach sequence used to commit an **opening in-stream journal fence at step
3, before hydration**. It is gone. `Store.OpenJournal` binds the session scope
through `bindSessionScopeMode(..., ProtocolModeLegacy)`, which conflicts with the
disposition witness `AcquireResidency` pins — so that fence failed for **every**
session a Host can hold, and no composed Host ever reached `BeginOwnership`.

Under Bundle B, where disposition is the only supported mode, **the journal grant
is the runtime's and Host never takes one**. The journal epoch Host needs is read
from the launched runtime through `department.LeaseEpochReporter` and reported as
`residency.Residency.JournalEpoch`. `compose.Options` declares no session opener,
so the absence is held by the compiler, and
`internal/residency.TestNoProductionFileOpensAJournalWriter` holds it
structurally. **Do not reinstate it.**

## Build and test

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

## License

Apache 2.0. See [LICENSE](LICENSE).
