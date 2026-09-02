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
  version must exist on that module's remote first. A forbidden module is
  rejected for BEING FORBIDDEN, before that map is consulted — the ban must not
  rest on the module being absent from a list of published ones, because
  Factory ships in this same program and an indirect requirement has no import
  for the import guard to see.
- A nested module or repository under `host/` is invisible to the import guard:
  the enumerator skips any directory holding `go.mod` or `.git`, which is right
  for "this module's content" and means the boundary is a property of the
  module, not of the tree. `nestedModuleAllowlist` is empty and
  `TestModuleContainsNoUndeclaredNestedModule` fails loudly on any undeclared
  one. Both walks share `modfiles.IsIgnoredDirectoryName` and
  `modfiles.IsBoundaryDirectory` rather than restating them, and the SETS those
  return are pinned by test — widening either is a hole in both walks at once,
  which sharing alone does not prevent. An oracle over a predicate that asks
  the FILESYSTEM a question must skip on what the volume RESOLVES, not on what
  it stores: case-insensitive APFS is case-preserving, so a stored-verbatim
  check passes and `Lstat` finds `.Git` anyway. If you publish a nested module from here — the workspace does this for
  `flow/store` and `pluto/cmd/pluto` — declare it there and decide, explicitly,
  whether the guard should reach into it.
- No `replace` directives to local filesystem paths, and no vendoring. A
  `GOWORK=off` failure means a dependency release is still owed upstream; it is
  not a reason to add a `replace`.
- `drain` is unpublished. Define a narrow local interface and a fake; name the
  module only once its origin and tag exist.

## Adding a boundary rule: the five mechanisms

`import_boundary_test.go` is five independent mechanisms, deliberately not
merged — the independence is worth more than the tidiness, and each has its own
failure mode. It has already misfired twice for want of an index: the reason map
was consumed by one guard and discarded by the other, and the file-level
structural predicate was left behind when the two directory ones were shared.
Route a new rule by asking what it is about.

1. **One import path, in one file** — `importVerdict`. Layering bans, the
   Centrifuge path scope. Return a reason; the message is consumed.
2. **One looprig module go.mod names** — `looprigDependencyViolation`. Shared by
   requirements and by versioned replacement targets, because a rule that
   applies to one and not the other is not a rule.
3. **The shape of a go.mod directive** — `replaceViolations`. The local
   filesystem path rule lives here. Anything about the module a directive
   NAMES belongs in 2, not here.
4. **What the walk covers at all** — `scanNestedModules` and
   `nestedModuleAllowlist`. A declared nested module buys the WHOLE guard
   rooted at it, recursively — the import scan and the nested-module walk — so
   declaring one never suspends the mechanism that would have reported what is
   inside it. Every floor the outer guard applies applies there too.
5. **What any walk can see** — the exported predicates in `internal/modfiles`.
   Every predicate deciding what a walk skips belongs there and needs a fuzz
   ORACLE in the consumer, not a sample table: a table is defeated by choosing a
   literal it does not list, and a widened predicate is a hole in every walk
   over this tree simultaneously — including `make fmt-check`, which pipes the
   same enumerator into gofmt.

An exclusion added to make a guard pass must itself be tested. A too-wide
exclusion produces exactly the same green as a correct one; that has been a live
defect here twice.

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
