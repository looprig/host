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
`github.com/looprig/sessionstore` v0.8.0 store.

**A composed Host attaches a disposition session and consumes its durable
command stream. It does NOT dispatch a command into the runtime, by design and
explicitly.** Read the next section before you plan around it; the boundary is
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

**`internal/realtime/hostlink/drain.go`, `drainScope`.** A drain request naming
an empty `TenantID` is **refused** with `RefusalWrongDrainScope` and the Core
class `runtime_unavailable`, **before the tenant comparison and before the drain
state machine**. Every link this package serves is tenant-authenticated —
`NewMultiplexer` requires a valid `TenantID` — so the whole-Host scope has no
link it may arrive on. The one drain a link may begin is the one naming **its
own tenant** and this Host's **fixed session**, which is scoped by construction.

**A pooled Host therefore answers no HostLink drain at all.** It has no fixed
session, so the session scope is refused too. That is the intended shape: the
whole-Host drain is reachable **only** through the process-lifecycle path —
`Service.Stop` calls `Drainer.StartDrain` with the zero scope directly and never
passes through this resolver — which is the path a platform's termination signal
already takes, and it is unchanged.

**What this does NOT claim.** It is not a statement about confidentiality, which
was never at issue here. It does not give a Factory `cmd/controller` a drain
path over HostLink; if one is needed it must be **tenant-scoped or
service-principal**, and re-admitting the empty tenant here would reopen exactly
the hole this closed. And it says nothing about the other entries below.

Measured by `TestATenantLinkCannotDrainAnotherTenantsSessions` in
`internal/compose` — two tenants, both holding live resident sessions, over one
shared `Drainer` — and by
`TestAWholeHostDrainIsRefusedOnEveryTenantAuthenticatedLink` in `hostlink`.

Closed as `O7.1-hostwide-drain-crosses-tenants` / R-1.

### A composed Host consumes and does not dispatch

**This entry replaces two earlier ones, and both were wrong in the same
direction — a sentence wider than its evidence.** The first said a Host "can
consume its inbox against a real store", which was an ADAPTER result stated about
a composed Host. The second said a composed Host "cannot attach a disposition
session, so it consumes nothing", which was true until the attach-time journal
fence was removed and is now false.

**What a composed Host does.** It attaches a disposition session end to end,
reaches `BeginOwnership`, and starts the session's durable command consumer,
which lists the session's commands in acceptance order, steps over terminal
records and advances the durable consumption cursor. This is measured in
COMPOSITION — `internal/compose/attach_fence_test.go` and
`TestAComposedHostConsumesADeliveryAndRefusesToDispatchIt` — rather than at the
adapter, and the adapter-level differential
(`internal/compose/inbox_differential_test.go`) still holds the three consumption
methods against the released store.

**What it does not do, deliberately.** It refuses to drive any command into the
runtime. `commands.NoDispatch` is the Processor a composed Host runs; it refuses
every non-terminal command it is handed with
`ApplyRefusal("dispatch_unavailable")`, **before touching any durable seam**, so
the record stays exactly where Factory put it and a later Host can still apply
it. The pass blocks at that command and the session makes no further progress.

**What it looks like in production, because "by design" is not an excuse for an
invisible state.** The delivery is answered **`accepted`**; the session then
makes no further progress, permanently; `Consumer.Failures()` **stays at zero**
(a blocked pass returns a nil error); and **nothing is logged**. The one signal
that distinguishes this from an idle Host is the Prometheus gauge
**`host_sessions_command_blocked`**, which counts resident sessions whose
consumer has stopped at a command. **Expect it to equal the number of sessions
holding work.** A non-zero value is not a fault today; it becomes one once an
applier exists. Nothing on the Host side unsticks such a command — Factory's
apply-deadline reconciler is what eventually makes the record terminal.

**Why the refusal is explicit rather than an omission.** In disposition mode the
store settles a command from durable evidence: an application prefix carrying the
attempt's identity, verified for `AttemptID` equality. `harness` v0.33.0's
`runtimecommand.Admitted` carries **no attempt identity**, so a dispatch commits
a prefix no settler can ever match, and the settlement design **forbids**
converting "a legacy prefix followed by an uncertain external effect" into
`not_applied`. A dispatched command would therefore sit in `applying` **forever,
with a real runtime effect behind it** — strictly worse than one never
dispatched.

**What unblocks dispatch**, which is a three-repository sequence and not a Host
fix:

- an **attempt-aware harness writer** that stamps the attempt identity into the
  application prefix;
- a Host applier bound to the **disposition family's** edges rather than to the
  legacy CAS transitions `internal/commands/apply.go` still models.
  `sessionstore` v0.8.0 supplies the claim and pre-attempt reject edges
  (`ClaimDispositionCommand`, `RejectDispositionCommand`); the claim edge takes a
  `*ResidencyGrant` rather than an epoch, so the grant must reach the adapter,
  and it **cannot answer for a legacy session, cannot express a superseded
  residency through the exported API, and cannot be driven across a process
  boundary**. A rejection carries **no durable reason**, and rows written by
  v0.8.0 can never be backfilled with one.

Two consequences a reader planning that work must not get wrong. **There is no
Host-authored rejection once an attempt exists** — a runtime error with no
durable disposition leaves the command `applying` until a **successor runtime**
writes `not_applied` under a strictly later journal grant. And **missing evidence
is not proof that nothing was applied**, so a Host must never re-dispatch on its
absence. Host's honest exactly-once claim is therefore **"at most one authorized
attempt, settled only from the runtime's durable disposition"**.

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
