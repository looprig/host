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

Department, residency, command consumption, HostLink, warm release and drain are
built. `internal/sessionstoreadapter` binds them to the released
`github.com/looprig/sessionstore` v0.7.0 store.

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

### Host's command path names two families of the released store, and no session serves both

**`internal/sessionstoreadapter`.** The durable command STREAM and its
consumption CURSOR are bound over sessionstore's DISPOSITION family, which is
the only family a Host can reach: `AcquireResidency` pins
`ProtocolModeDisposition` and refuses anything else. The command RECORD read and
the four compare-and-swap TRANSITIONS an applier makes are still bound over the
LEGACY family, and a disposition-bound session refuses `OpenSession`, so the
applier cannot take the journal grant it needs.

So a Host can **consume** its inbox against a real store and cannot yet
**apply** a command end to end against the same session. The disposition-family
transition adapter is owed. Do not read a green composed test as evidence
otherwise; the composed apply path runs against a double, and
`internal/compose/inbox_differential_test.go` says exactly which half of it the
released store now backs.

## Build and test

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

## License

Apache 2.0. See [LICENSE](LICENSE).
