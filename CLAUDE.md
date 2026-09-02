# CLAUDE.md — host

`host` is the tenant-scoped Department runtime host. It owns resident sessions,
local registry, command consumption, the HostLink server, warm release and
drain. Factory consumes Host; Host never consumes Factory.

## Dependency boundary

`import_boundary_test.go` is the authority, not this list. Keep the two in step.

- Never import `github.com/looprig/factory`, `wui`, `tui`, or a product or
  integration repository (`carbon`, `client`, `kosa`, `policy53`, `capstan`,
  `tests`) from anywhere in this module, tests included.
- Centrifuge is allowed only under `internal/realtime/hostlink/`. A second
  package under `internal/realtime/` does not inherit that exemption. Widening
  it means editing `centrifugeExemptDir`, which is a visible diff.
- Name only published looprig versions, listed in `publishedLooprigVersions`.
  Adding a module there is the same decision as depending on it, and the
  version must exist on that module's remote first.
- No `replace` directives to local filesystem paths, and no vendoring. A
  `GOWORK=off` failure means a dependency release is still owed upstream; it is
  not a reason to add a `replace`.
- `drain` is unpublished. Define a narrow local interface and a fake; name the
  module only once its origin and tag exist.

## Code and security

- Keep public contracts transport-neutral and use Core's `sessionwire/v1`
  records. A Host type is not a Factory type.
- Return typed errors from package-level APIs and preserve causes with
  `Unwrap`.
- Scope every operation by authenticated tenant and session identity.
- Keep I/O bounded and context-aware.

## Testing and build

- Follow red-green-refactor, and mutation-test any guard that matters: add the
  breach, watch the specific named test fail, revert.
- Classify by parsing structure, never by searching source text. A substring
  ban has been defeated in this workspace by a newline and by an equivalent
  spelling.
- A guard must assert it reached its subject and must fail at zero files.
- Run every Go command standalone with `GOWORK=off`.
- Before committing, run `GOWORK=off go test -race ./...`, `GOWORK=off make
  check`, `git diff --check`, and inspect `git status --short`.
