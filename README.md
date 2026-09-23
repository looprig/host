# looprig/host

`host` is Looprig's Department runtime host. A Host is a process
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
**Warm release takes effect from v0.7.0:** a pooled Host composed with
`host.Compose` releases a session that has been whole-session idle for
`Options.WarmTTL` — never one with a gate open, a turn in flight or durable
command work outstanding — and the next command re-places and restores it.
Before v0.7.0 no composed Host ever warm-released. When the countdown fires the
Host first halts the session's command consumer, waiting out a pass in flight
(a command admitted from then on is left pending for the next Host), then
re-confirms the session is still idle and has done no work since the countdown
was armed, then re-reads the durable inbox. The runtime release is bounded by
`Drain.Grace`. A runtime that does not release in time — a turn that started
inside the release window — is never torn down under that turn: the release is
**taken back**, and the session is an ordinary resident, admitting session
again under the same grant and generation (Factory can bind to it, wake it and
answer its gates), and the next idle countdown tries again. Only if that revert
is refused (the Host is draining, or the grant is gone) is the session held for
the drain. Both are counted in `host_release_failures_total`.

**A dedicated Host does not warm-release — a deliberate departure from runbook
O6.2** ("the same release behavior as pooled"). A warm-released dedicated
session would leave its Pod running with nothing resident while placement, the
controller's desire and the Pod all still name it, and no released Factory or
controller has been proven against that state. Its session ends through the
controller's drain-before-delete, as before.
`internal/sessionstoreadapter` binds them to the released
`github.com/looprig/sessionstore` v0.12.0 store, and `internal/harnessadapter` to
`github.com/looprig/harness` v0.37.1. Core is v0.11.0.

**host v0.7.1 pairs with harness v0.37.1**, which is v0.37.0 plus one further
fix, and both matter to a pooled fleet.

v0.37.0 closes a crash window where an admitted input could settle `applied`
with its effect lost, or a graceful shutdown could cancel an input the store
already recorded applied: see harness's own v0.37.0 release notes for the
mechanism (`Admission`, replay-from-intent on restore). It requires no Host
code change and is not a one-way upgrade — a v0.36.0 runtime still opens a
v0.37.0 journal, and a v0.37.0 runtime replays input debt a crashed v0.36.0
runtime owed. **Do not mix harness v0.36.0 and v0.37.0 (or v0.37.1) runtimes
over one Host pool**: only a v0.37.0-or-later runtime performs the
replay-on-restore that makes the fix effective, so a pool serving the same
sessions from both leaves the outcome dependent on which runtime happens to
restore a given session.

**v0.37.1 lifts the obligation that every pooled Host must mount the
workspace base at the same path.** Before it, restoring a session onto a Host
whose per-session workspace base differed from the one it was created under
(a pod-specific mount path, a rollout that changed the mount, pooled and
dedicated Pods with different paths, a symlink resolving differently per
node) was refused — `"restore rejected by policy: 1 warn category
(workspace)"` — and the session was silently never re-placed. Restore now
compares placements by mode alone; a placement-mode change, a different
exclusive or shared fixed root, a placement added or removed, and a
caller-supplied root still warn. The written fingerprint is byte-identical to
v0.37.0, so this is also not a one-way upgrade by itself — but **a fleet that
relocates workspace bases across Hosts needs v0.37.1 on every Host that may
restore one of its sessions**; a v0.37.0 Host still refuses a relocated
base.

**A composed Host applies a command end to end: it attaches a disposition
session, consumes its durable command stream, claims a command under its
residency grant, authorizes exactly one dispatch attempt, drives the runtime with
that attempt's identity and settles the command from the runtime's own durable
disposition.** Read the next section before you plan around it; the mechanism is
measured, not cautionary.

**Since v0.5.0 a session's FIRST command settles** — see "Upgrading to v0.5.0:
create and restore". **Since v0.4.0 every gate an agent raises reaches Factory,
and the user's answer settles through the same path** — see "Upgrading to
v0.4.0: gates".

## Deployment and operations

A product supplies the Department's launch targets and runtime adapter. The
advertised isolation class, placement and capacity describe what Factory may
select; they do not make a runtime compatible with a target. Compose the
product's registered target and verify the runtime adapter before publishing
it. A target declares an opaque runtime compatibility ID; restore refuses a
journal created under a different ID, so preserve that ID only for builds that
can actually read the same state. The generic `cmd/host` has an unconfigured
bootstrap and refuses to start without product collaborators.

`host.Run` serves HostLink, `/readyz`, `/healthz` and `/metrics` on one HTTP
listener (`HOST_LISTEN_ADDRESS` in `cmd/host`). HostLink is internal: keep the
whole listener on an internal network, including its probe and metrics paths.
HostLink verifies the tenant credential through the product's authenticator.
It is service-to-service and does not use browser Origin or CSRF checks.
`host.Run` uses plain HTTP; terminate TLS in front of the listener or supply a
secured listener in the product composition. The Host advertises a **bare**
internal endpoint; Factory v0.5.0 or later derives each tenant's HostLink path.
Older Factory versions dial the base verbatim and get 404. Roll Factory and
Host as a paired change when switching to the bare base.

Pooled means several sessions can reside on a Host; dedicated means one fixed
session (`HOST_FIXED_SESSION_ID`). Neither word describes durability. Recovery
requires a shared SessionStore backend, its leases and the Harness journal;
object bytes need a separately selected object store. Legacy SessionStore
`PutObject` and object-first retention are not a Host disposition
`SessionObjectStore`. A pooled Host's whole-process drain runs on shutdown;
HostLink drain is available only for a dedicated resident fixed session.
The dedicated workload controller must observe the terminal drain status
before it deletes that workload.

`Run` drains before shutting down the listener, so the drain-status observation
remains readable. Set `HOST_DRAIN_GRACE` inside the platform's termination grace
with margin; `HOST_DRAIN_IDLE_BOUNDARY` and `HOST_DRAIN_PUBLISH_BOUND` bound
parts of that work. `/readyz` reports admission readiness and `/healthz`
reports process liveness. A Host with an open gate must exit after drain;
releasing a gated runtime gracefully is unsupported.

The isolated `/metrics` registry exports `host_sessions` (by state),
`host_sessions_gate_waiting`, `host_sessions_command_blocked`,
`host_admission_weight_consumed`, `host_admission_weight_capacity`,
`host_command_lag`, `host_command_queue_depth`,
`host_release_failures_total` and, only when known,
`host_memory_budget_bytes`. Capacity and queue gauges are local Host state;
Factory backlog, resident wait, reconciliation and controller drain metrics
are not exported here. Watch `host_sessions_command_blocked`: a nonzero value
means a durable command cannot progress.

A Host advertises `hostlink.command.gate_response` only when its gate seams
are composed. Factory must check that capability before admitting an answer.
Keep every Factory gate reader on sessionstore v0.12.0 or later before a Host
publishes a gate. Once a Host has applied a gate response, do not roll it back
below v0.4.0; after a create or restore disposition, do not roll it back below
v0.5.0. A cold AskUser answer/resume remains unsupported.

## Upgrading to v0.8.0: a storage outage no longer wedges a session

Before v0.8.0 a brief PostgreSQL or S3 outage under a runtime's journal could wedge a
session for good. One failed journal write, and the command being applied stayed
`applying` under the runtime's own grant. No runtime may close its own attempt, so every
later command queued behind it until the apply deadline rejected it. There were two
shapes:

- **The runtime latched a persistence fault.** harness does this by design: the durable
  state can no longer be proven, so the runtime refuses every later command.
- **The runtime refused one command without faulting.** For example, its application
  prefix could not be written.

v0.8.0 treats either as loss of residency. Host watches harness v0.38.0's
`PersistenceFaulted`, and the disposition applier reports an attempt the runtime
failed and recorded nothing for. On either signal Host:

1. marks the session releasing;
2. stops its work;
3. abandons the runtime crash-equivalently with `AbandonResidency`, which writes nothing;
4. tombstones the route, releases the grant and credits the capacity.

Factory's pending sweep then re-places the session. The successor restores it from the
journal and settles the stranded command truthfully: `applied` if its effect is durable
(an owed input is re-run by the restore), otherwise closed `not_applied`. Every later
command is applied exactly once.

**Limits you must plan around:**

- The acknowledged words of a command closed `not_applied` must be resent, as after any
  crash.
- A runtime that does not offer `department.PersistenceFaults` is not supervised. It
  stays resident and blocked, and an ERROR is logged.
- If the outage is still on, the release's own durable writes fail. The grant stops
  renewing and lapses, and the registry row expires. Re-placement therefore waits for
  those bounds, and a restore attempted while storage is still down is refused.

**Pin harness ≥ v0.38.0 with host ≥ v0.8.0.** The reference adapter requires the two
capabilities.

## Upgrading to v0.5.0: create and restore

### Every session a v0.4.0 Host held was wedged on its first command

Factory admits a session's first command as a `create` disposition record (a
create's optional first message rides in its payload) and resumes a
non-resident session with a `restore`. `runtimecommand.Kind` named only `input`,
`interrupt` and `gate_response` through harness v0.35.0, so **this Host's
adapter refused both AFTER it had durably begun the dispatch attempt**. No
disposition frame was ever written, the store could never settle the record
(`sessionstore: inbox evidence (disposition)` — absence of evidence, not a kind
disagreement), the consumer never advanced its cursor past that command, and
`Closure.Validate` refused the same kinds so no successor could close it either.
The deadline sweeps skip an attempt-bearing record, so it never expired.

**The user-visible effect: the session existed, the agent was resident, and
nobody could talk to it.** The create's first message was lost, clients saw
`accepted` forever, and Factory's placement kept the session as open work
permanently.

harness v0.36.0 adds `KindCreate` and `KindRestore`, and this release drives
them:

- **A create carrying a first message.** Host reads the stored Core
  `CreateRequest` (`internal/createbody`) and hands its blocks to the
  composition's `BlockDecoder` **in the input-shaped body that decoder already
  reads**, byte-identical to the input Factory would have admitted for the same
  message. A composition therefore names ONE encoding, not two. The command
  settles `applied` and the message reaches the model.
- **A bare create and a restore.** Core's `CreateRequest.Blocks` is optional and
  `RestoreRequest` has no blocks member at all, so both cross carrying nothing
  and settle `applied` — their effect is residency, which this Host has already
  performed by the time the command is dispatched.
- **A create carrying a message with no decoder bound is refused**, exactly as
  an input is. A Host that applied an empty first turn would settle the record
  perfectly while dropping the user's first words in silence, which is worse
  than the wedge it replaces because the wedge was visible.

### Migrating a session a v0.4.0 Host stranded

Nothing needs to be done by hand. A create left `applying` with a durable
attempt is closed `not_applied` by the next Host to take the session, under its
own strictly later journal grant, and the session's command stream then
continues from the command behind it. That closure is possible only because
harness v0.36.0 widened `Closure.Validate` with the kinds; at v0.35.0 the
recovery path refused exactly what the adapter did.

**The create's first message is not recovered.** It was never delivered and the
command is settled as not applied; a client that wants those words sent must
send them again as an input.

### Every product runtime needs a recovery closer

A product runtime must implement `department.AttemptCloser` to recover a
predecessor's stranded attempt. This is not limited to migration from v0.4.0:
an ordinary pod eviction can leave an attempt in flight in an all-v0.5.0 fleet.
Without the closer, Host refuses recovery as `no_attempt_closer`, leaves that command
`applying`, and the consumer cannot advance past it. **The entire session's
command stream remains blocked**, including later input and interrupt commands;
the apply deadline cannot settle an attempt-bearing record. The bound journal
session must write the closure under its own strictly later journal grant. The
`department.AttemptCloser` documentation includes a copyable harness-backed
method.

Host logs `attempt_closer_unavailable` at WARN when it composes a launched
runtime without the capability, before starting its command consumer. This
check occurs at session attach because `host.Compose` has no launched runtime
to inspect. The warning does not refuse the attach; a session with no stranded
attempt can still run.

### ONE-WAY UPGRADE

Once a session's journal holds any `create` or `restore` application prefix or
disposition frame, harness ≤ v0.35.0 can neither replay nor reopen it — it fails
closed on the unknown kind. **Never roll a Host back below v0.5.0 (harness
v0.36.0) after it has applied either kind.** This is the same shape as v0.4.0's
`gate_response` note and stacks with it.

**Mixed fleets.** A create admitted while a v0.5.0 Host owns the session is
refused by a v0.4.0 successor after its own attempt, which re-strands the
record until a v0.5.0 Host takes it. **Do not run v0.4.0 and v0.5.0 Hosts over
one session pool.**

### v0.6.0: create bodies are checked before an attempt

After claiming a `create`, Host checks its stored Core request before minting
or recording an attempt. A malformed body or one naming another session or
command is rejected while the command has no attempt, freeing the session's
command stream. A body this Host may be too old to understand (`unknown_field`
or `unsupported_version`) stays claimed and unattempted; a newer Host can
take the claim, and Factory's apply deadline still bounds it. A body stored
only by object reference also blocks conservatively because this Host cannot
read it. Once an attempt exists, only runtime evidence settles the command.

The precheck validates the generic create envelope and block-array shape. A
product's block decoder can still refuse a block after the attempt; that
runtime-specific failure remains visible as an applying command until a
successor closes the attempt or an operator repairs the decoder.

## Upgrading to v0.4.0: gates

### Every AskUser and permission gate is published; the answer is a disposition command

- **Publication.** Each resident session runs a gate publisher
  (`internal/gates`). **At attach, under the fresh residency grant and before
  the runtime is restored**, the Host makes one **fencing write** to the
  projection (never a bare epoch; the store-issued `*ResidencyGrant`), so a
  predecessor that is not dead fails its ownership check before it can begin an
  attempt on an answer. The publisher then subscribes to the runtime's
  committed stream (hints only), fences again (a no-op), folds the runtime journal
  (`OpenEventReplayer`, positioned by ledger sequence) into the set of open gates,
  and converges the sessionstore projection on it: `OpenGate` for a gate the
  journal holds and the projection lacks, `ResolveGate` for one the journal has
  closed. A lost subscription is resumed from the last sequence folded. A
  superseded grant stops the publisher. A gate already projected (by a
  predecessor) is never re-opened. The projection holds at most 16 open gates
  per session (sessionstore); a gate beyond that is **not visible to Factory**
  until another closes, is logged once, and is published when one does.
- **The projection** is a function of the journaled `GateOpened` alone:
  `harness sessionwire.ProjectGatePage` under a `ReadScope` whose Core
  `SessionID` and `RuntimeSessionID` come from **one** read of the binding;
  answerability `resident`; deadline = the event's durable `CreatedAt` + the
  gate's `ResponsePolicy.Timeout`, or a per-kind TTL compiled into Host
  (permission 5m — harness journals its own 5m default, so this is only a
  fallback; ask_user and form 24h; open-url 1h).
- **Ownership is a residency epoch.** On a disposition session the projection's
  `CatalogRecord.LeaseEpoch` is the residency of the last Host to write a gate.
  `LoadGate` returns it as `OwnerEpoch` (and no Host id); it is compared with the
  Host's residency grant, never with a journal epoch.
- **Applying an answer.** A `gate_response` is checked after the claim and
  before the attempt — the last point a rejection is possible:
  - **rejected** (`RejectDispositionCommand`, no durable reason) when no Host
    could ever apply it: stored by object reference (no released Host
    dereferences one), not Core's strict `GateResponseRequest`, naming another
    session or command, a gate id that is not a non-zero UUID, or — for a gate
    the projection holds under this Host's mark — an `ExpectedOpen*` that is not
    the projected version;
  - **blocked, nothing written** for a limit of this Host: an unreadable
    projection, or a gate whose mark is not this Host's grant (fence not yet
    landed, or superseded). **A block holds the session's whole command stream**,
    interrupts included, until it clears; a claim that lapses while blocked is
    re-taken on the next pass;
  - **dispatched** otherwise, including for a gate the projection no longer
    holds — harness is the authority and settles an answer to a closed gate
    `no_op`.
  The adapter builds `Admitted.GateResponse` (source: user) under the
  authorized `AttemptID`, and the command settles from the runtime's evidence
  only.
- **What the outcomes mean.** `applied/applied`: the answer resolved the gate.
  `applied/no_op`: the gate was already closed (a second answer, a timeout that
  won, a gate closed at restore). `rejected/refused`: harness refused the answer
  (an action the gate does not offer, values its schema rejects), settled from
  its evidence one pass after the attempt. `rejected` with no attempt: the body
  could never apply.

### ONE-WAY UPGRADE

Once a session's journal holds any `gate_response` application — applied,
no_op or refused — harness ≤ v0.34.0 cannot replay or reopen it. **Never roll a
Host back below v0.4.0 (harness v0.35.0) after it has applied one.**

### Limits you must plan around (harness v0.35.0)

- **A gated session cannot be released gracefully, so its drain is
  crash-equivalent.** harness releases a session only when it is whole-session
  idle, and a session parked at a gate is not. The drain bounds that release by
  its grace (it used to hang forever) and then leaves the refused runtime
  **parked**: its context is not cancelled, so it writes nothing — no
  `TurnInterrupted`, no `GateResolved{abandoned}` — and the gate stays open and
  projected, as after a crash. The parked runtime keeps its journal lease until
  **its process exits**, so **another Host in the same process cannot attach
  that session until then** (it is refused `lease held`); `cmd/host` exits after
  its drain. The successor then restores the session as it restores a crashed
  Host's, and a permission gate is restored open.
  **A parked runtime is uncancelled, not stopped.** It keeps its goroutines,
  accepts a `Submit`, and keeps its own gate timers — so a permission gate's
  five-minute default can fire on a drained Host whose publisher has stopped,
  resolving the gate and continuing the turn against a stale projection.
  **A Host that drains with a gate open MUST exit**; `cmd/host` does. Note also
  that `Stop` skips the manager's context cancellation **for the whole
  process** when any one session's release was refused.
- **After a restore**, a permission gate is restored open and answerable, but
  the turn that was parked at it is `TurnInterrupted`: the approval is applied
  and nothing runs the tool. An **ask_user** gate is closed at restore
  (`restore_unavailable`) and an answer to it settles `no_op`. Both are booked
  for harness v0.36.0.
- A crash between the runtime's `GateResolved` and its disposition frame leaves
  the command `applying` forever (never a false `not_applied`): the same
  liveness gap input has.

### Capability: how a Factory knows a Host can apply a gate response

A Host that applies `gate_response` advertises Core's capability token
**`hostlink.command.gate_response`** (`sessionwire.HostLinkCapabilityGateResponse`,
core v0.11.0) in its connect reply's `hostlink_methods`, after the five reserved
methods. **A Factory admits — and wakes — a gate response only for a Host whose
reply `Supports` it**; no earlier Host advertises it. A token is not a method:
nothing dispatches on it, and sent as an RPC it is answered from the channel arm
like any unknown name. Host advertises it only when its composition wires both
gate seams (publication and the pre-attempt check); `host.Compose` always does.

The projection's residency mark equalling this Host's grant is **not** a
capability signal. It is the applier's **ownership** check (an answer to a gate
whose mark is not this Host's grant blocks until it is), and a Factory must not
read `CatalogRecord.LeaseEpoch` to decide anything.

**Mixed fleets.** An answer admitted while a v0.4.0 Host owns the session can
still be re-placed onto a v0.3.0 Host, which claims it, begins its attempt and
cannot apply it: it sits `applying` until a v0.4.0 successor settles it
`not_applied`. **Do not run v0.3.0 and v0.4.0 Hosts serving gating agents
together.** factory v0.5.0 refuses admission, and filters pooled placement, on
the token.

### Other v0.4.0 changes

- **A superseded generation still releases its sessions.** A newer incarnation
  of the same HostID started before the old one stops makes the old drain's
  target withdrawal fail permanently (`host target generation`); the drain used
  to abort there, before releasing any session lease, and every later Host got
  `epoch_mismatch`. The row now belongs to the newer generation, the drain skips
  it (`service.ErrTargetGenerationSuperseded`) and releases every session. A
  platform should still not overlap two generations of one HostID.
- **A successor takes a crashed predecessor's command claim**, and **a Host
  re-takes its own lapsed claim**, before the attempt (both used to resume at
  the attempt, which the store refuses, forever).
- **`Collaborators.Logger`** (optional `*slog.Logger`, nil discards): attach
  refusals at hydration are WARN with tenant, session, step, code and reason;
  placement-race refusals are DEBUG; a failed gate pass and a failed
  advertisement heartbeat are WARN. `cmd/host` logs JSON to stderr.

## Upgrading to v0.3.0

### `HOST_INTERNAL_ENDPOINT` is a BASE, and Factory must move with Host

One pooled Host serves several tenants, each at its own HostLink address:
`base + /hostlink/ + PathEscape(tenant)`. As of v0.3.0 the configured
`InternalEndpoint` (`HOST_INTERNAL_ENDPOINT`) is that **base** — a `ws`/`wss`
scheme and an authority, nothing else — and a Factory derives each tenant's
address with Core's `sessionwire.HostLinkEndpoint(base, tenant)`. Host
advertises the base unchanged in its capacity report and registry observation.

`host.New` / `host.Compose` now **refuse at startup**:

| value | refusal |
|---|---|
| not a valid internal endpoint (`http://…`, credentials, a query) | `*sessionwire.HostLinkEndpointError`, code `invalid_base` |
| the v0.2.1 per-tenant spelling `…/hostlink` or `…/hostlink/<tenant>` | code `base_names_tenant` |
| any other path (including an ingress prefix such as `https://gw/pods/h7/`), or a bare `#` | code `base_not_bare` |
| so long that not even a one-byte tenant fits | code `too_long` |
| an authority no client can dial although Core accepts it: `ws://h:`, `ws://h:99999`, `ws://h:0`, `ws://h:80:90` | `host.ErrUndiallableEndpoint` |

Each is an `*InvalidOptionsError{Code: invalid, Field: "InternalEndpoint"}`
whose cause is reachable with `errors.As` / `errors.Is`. A Host behind a
path-prefixed ingress must use host-based routing, a port per Host, or
in-cluster Service DNS.

**Compatibility window.** Every released Factory up to and including
**v0.3.0** dials the advertised endpoint **verbatim** — measured against
factory v0.3.0; no earlier one can derive, since `HostLinkEndpoint` first
shipped in core v0.10.0 — and dialling a bare base gets **404**, so such a
Factory cannot reach a v0.3.0 Host. **The failure is silent**: measured with
released factory v0.3.0, a created session stays `pending`, is placed
**nowhere**, and Factory **logs nothing** — a failed dial classifies as
`ErrHostUnreachable`, which it only counts. **Upgrade Factory together with
Host** (to the first Factory that derives addresses with `HostLinkEndpoint`). In the other
direction, a **v0.2.1 Host reconfigured to a bare base already works** with a
deriving Factory, because v0.2.1 already served every tenant at
`/hostlink/<tenant>` — so a fleet can move Factory first and then Host. A
controller that writes `…/hostlink/<tenant>` into `HOST_INTERNAL_ENDPOINT`
must write the bare base instead, or a v0.3.0 Host refuses to start.

Host's routing is unchanged, and the prefix is Core's own
`sessionwire.HostLinkPathPrefix`. `routes_endpoint_test.go` holds the real
`Service.Routes()` to `HostLinkEndpoint` over Core's golden tenant list and a
fuzz target, observing which tenant each derived address authenticates as.

### A re-placed session resumes its conversation instead of restarting it

Factory sends **every** attach as `create` (both inputs its attach mode is
chosen from are dead on a disposition record), and harness does not check
that a session id it is asked to create under is fresh: creating over an
existing conversation re-opens that stream, appends a second
`SessionStarted` and restores nothing, with no error. Before v0.3.0 a Host
launched every create fresh, so **a session released by one Host and placed
on another silently lost its conversation**.

As of v0.3.0:

- The runtime is launched under the runtime session id the session's
  **immutable binding** names (`SessionBinding.RuntimeSessionID` — the id
  Factory derived, never the Core session id), via `rig.WithSessionID`.
  `department.CreateRequest` / `RigCreateRequest` carry it as `RigSessionID`,
  and a Rig that launches or restores under any other identity is refused
  with `department.ErrRigSessionIdentity` (the mis-launched session is
  released).
- **A create is decided by the runtime's journal**, read under that id from
  the journal store registered for the session's (tenant, storage binding) in
  `Collaborators.JournalStores`: no conversation → create; a conversation →
  **restore it**. A restore still refuses when the target requires a
  checkpoint the session has not got. harness's catalog (`ReadMeta`) is
  consulted first and the ledger is the authority, because the catalog is a
  best-effort cache. Only a create consults the journal; an explicit
  `restore` is launched as before.
- **The two refusals carry different codes.** When **no journal store serves
  the session's binding**, Host cannot tell a new session from a re-placed
  one and refuses the create with **`runtime_unavailable`**. When the journal
  **read fails** (a storage fault), the attach is refused with the **empty,
  unclassified code**, like every other durable-read failure. Neither ever
  starts the conversation over. **Warning:** factory v0.3.0 treats both as
  "try the next candidate, retry next sweep". It only **counts** a
  `runtime_unavailable` refusal and never logs it, so a fleet-wide one shows
  up as sessions that never place, not as log lines; the empty-code refusal
  is logged at WARN ("a pooled candidate failed the attach"), carrying the
  Host id but not Host's reason. The attach error Host returns names the
  reason; a failed journal read is a `*RuntimeJournalProbeError` naming the
  tenant, binding and runtime session (v0.4.0).
- **A binding whose `RuntimeSessionID` is not a non-zero UUID is refused with
  `runtime_unavailable`** (v0.4.0; v0.3.0 gave it the empty code). It is a
  permanent statement about the session — the binding is immutable and every
  Host reads the same one — so the empty code, which reads as a transient
  store failure, was wrong. Core has no class meaning "and no Host ever will";
  `runtime_unavailable` ("this Host cannot run this session") is the nearest,
  and is what a restore with no durable state already gets.
- **The journal store for each binding must therefore be the released harness
  session store** (`*harness/pkg/sessionstore.Store`, or a value embedding
  one): the same journal the runtime writes and settlement reads.
  **`host.Compose` refuses a reader that is not one** (`InvalidCompositionError`
  on `Collaborators.JournalStores`), because every create routed to it would be
  refused.
- `Collaborators.RigSessionIDs` is **deprecated and optional**; it is
  consulted only for a record with no binding (legacy). Such a record's
  journal is never probed, so **a create over a legacy record is now
  refused** (v0.2.1 launched it); no shipped path creates one a Host can hold.
- **What the journal check does not close.** The journal is read under the
  residency lease, before the launch. A Host that reads "no conversation",
  stalls inside the launch and **loses its lease** can still create over a
  conversation a successor began meanwhile (measured: a second
  `SessionStarted`; the conversation survived the next restore). Host cannot
  close this alone; it needs harness to create-if-absent under its own
  journal lease, which is booked for harness.
- **Endpoints Host does not refuse although they are not usable from another
  pod:** the unspecified address (`ws://0.0.0.0:1`), a link-local zone
  (`ws://[fe80::1%25en0]:9000`), a raw non-ASCII IDNA host (advertised
  verbatim, not converted to punycode), and invalid DNS names such as
  `ws://h;x` and `ws://-h`. Core accepts all of them; they are booked.

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

**There is no Host-authored rejection once an attempt exists.** Before one,
Host rejects a `gate_response` or `create` whose immutable body no Host could
apply. The apply deadline is still Factory's
deadline reconciler's (§10.4), never a Host's.

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
