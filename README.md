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
`github.com/looprig/sessionstore` v0.7.0 store.

**Durable command consumption is bound at the adapter and does not run in a
composed Host.** Read the next section before you plan around it; the
distinction is measured, not cautionary.

## Known limitations you must read before deploying

These are properties of the code as it stands, not a backlog. Each is written
here because an operator who meets it for the first time in production has been
surprised by something this file could have told them.

### A whole-Host drain is not tenant-scoped, so pooled multi-tenant is not supported

**`internal/realtime/hostlink/drain.go:281`.** A drain request naming an empty
`TenantID` is a WHOLE-HOST drain, and `drainScope` returns the whole-Host scope
for it BEFORE the `request.TenantID != m.tenant` check that guards every
tenant-scoped request. This Host holds one `Multiplexer` per authenticated
tenant, but all of them share ONE `DrainStarter` — the Host-level `Drainer` — so
a link authenticated as tenant-a can start a drain that covers tenant-b's
resident sessions as well.

It is **availability, not confidentiality**: tenant-a learns nothing about
tenant-b, reads none of its material and cannot address its sessions. What it
can do is cause tenant-b's sessions to be checkpointed and released early.

**The consequence is a deployment constraint and it is the whole point of this
entry.** Run a Host with sessions from **one tenant only** — one Host process
per tenant, or a placement policy that never co-locates two. In that
configuration the defect is unreachable: the whole-Host drain covers exactly the
tenant that asked for it. **POOLED MULTI-TENANT DEPLOYMENT IS GATED ON THIS AND
MUST NOT BE ENABLED UNTIL IT IS FIXED.** The `IsolationClass` a Host advertises
is a statement to Factory and does not close it.

Tracked as `O7.1-hostwide-drain-crosses-tenants`.

### A composed Host cannot attach a disposition session, so it consumes nothing

**This entry replaces a narrower one that was wider than its evidence.** The
first version of it said a Host "can consume its inbox against a real store and
cannot yet apply a command end to end". The second half is right; **the first
half is not true of a composed Host**, and a limitations section that overstates
what works is the same defect it exists to disclose.

**What is measured.**

- A Host can take a **residency** grant on a disposition session.
  `AcquireResidency` requires `ProtocolModeDisposition` and succeeds.
  (`internal/sessionstoreadapter/residency_test.go`.)
- A Host **cannot** take that session's **journal** grant. `OpenSession` is
  refused: `Store.OpenJournal` binds the session scope through
  `bindSessionScopeMode(..., ProtocolModeLegacy)`
  (`sessionstore@v0.7.0 keyspace.go:320-327` → `bindProtocolMode`), which
  conflicts with a disposition witness. Measured by
  `TestTheResidencyGrantAndTheJournalGrantAreDifferentLeases`.
- **An attach commits the opening journal fence at step 3, before hydration**
  (`internal/residency/manager.go:1129-1133`, through
  `internal/compose/adapters.go:80` → `adapted.OpenSession`). So an attach of a
  disposition session **fails at `StepFence`**, every time.
- Steps 4 through 9 therefore never run. **Step 8 is where the command consumer
  is started** (`manager.go:1343`), so in a composed Host **no consumer exists
  and no command is consumed**.

**What "consumption works" is and is not.** `ListOrdered`, `LoadCursor` and
`SaveCursor` are bound to the released store and are verified against it —
including the strictly-after bound the idempotency property rests on and both of
`SaveDispositionCommandCursor`'s fences — by a **differential test that drives
the three adapter methods directly**
(`internal/compose/inbox_differential_test.go`). That is an **adapter-level**
result. **It is not a statement about a composed Host**, which never reaches
those methods.

**Applying a command is a three-repository sequence, not a Host fix.** Even with
the attach unblocked, two things the apply path needs do not exist:

- **`sessionstore` has no disposition claim edge.** `pending -> claimed` has no
  entry point (`disposition_settlement.go:89`) and there is **no in-package
  writer of a `DispositionClaim` at all** (`disposition_inbox.go:594-600`).
- **No evidence reader exists.** In disposition mode **the store decides the
  terminal arm from injected durable evidence**, and a caller supplies nothing
  an outcome is built from; `harness` v0.33.0's `runtimecommand.Admitted`
  carries no attempt identity, so nothing can produce that evidence today.

Two consequences follow that a reader planning this work must not get wrong.
**There is no Host-authored rejection once an attempt exists** — a runtime error
with no durable disposition leaves the command `applying` until a **successor
runtime** writes `not_applied` under a strictly later journal grant. And
**missing evidence is not proof that nothing was applied**, so a Host must never
re-dispatch on its absence. Host's honest exactly-once claim is therefore **"at
most one authorized attempt, settled only from the runtime's durable
disposition"**.

Migrating the transitions is **not** attempted here: it cannot be completed
against released dependencies, and a half-migration that removes the journal
grant from attach is a `residency.Manager` and `compose` change. Both are
v0.2.0 work.

## Build and test

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

## License

Apache 2.0. See [LICENSE](LICENSE).
