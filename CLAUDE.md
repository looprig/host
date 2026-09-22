# CLAUDE.md — host

`host` is the Department runtime host, with tenant-scoped HostLink connections. It owns resident sessions,
local registry, command consumption, the HostLink server, warm release and
drain. Factory consumes Host; Host never consumes Factory.

`department/` holds the immutable Department: agent identity to launch target,
fixed at construction, plus the segregated Harness session capabilities Host
requires. `internal/harnessadapter` supplies the runtime adapter used by the
current composition. The adapter translates Core's opaque sessionwire IDs to
Harness's runtime identity; a Harness session does not directly satisfy the
Host capability interfaces.

## Dependency boundary

`import_boundary_test.go` is the authority, not this list. Keep the two in step.

- Never import `github.com/looprig/factory`, `wui`, `tui`, or a product or
  integration repository (`carbon`, `client`, `kosa`, `policy53`, `capstan`,
  `tests`) from anywhere in this module, tests included.
- Centrifuge is allowed only under `internal/realtime/hostlink/` **of this
  module root**. A second package under `internal/realtime/` does not inherit
  that exemption, and neither does a directory of that name inside a declared
  nested module — the guard classifies on the outermost-root-relative path.
  Widening it means editing `centrifugeExemptDir`, which is a visible diff.
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
  check passes and `Lstat` finds `.Git` anyway. If you publish a nested module
  from here — the workspace does this for `flow/store` and `pluto/cmd/pluto` —
  declare it in `nestedModuleAllowlist`. Declaring is not a suspension: it
  turns the guard ON for that directory, in full. See mechanism 4 below.
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
Route a new rule by asking what it is about. **Every mechanism has exactly one
site, and a new rule is added at that site — if a mechanism does not have one,
that is the bug.** Six rounds of review found six escapes and every one of them
was the same thing: a rule listed in one place and not in the adjacent place
that inherits from it. Enumerate CONSUMERS if you must enumerate something;
never enumerate the rules, because the rules are the side that grows.

1. **One import path, in one file** — `importVerdict`. Layering bans, the
   Centrifuge path scope. Return a reason; the message is consumed.
2. **One looprig module go.mod names** — `looprigDependencyViolation`. Shared by
   requirements and by versioned replacement targets, because a rule that
   applies to one and not the other is not a rule.
3. **The shape of a go.mod directive** — `replaceViolations`. The local
   filesystem path rule lives here. Anything about the module a directive
   NAMES belongs in 2, not here.
   2 and 3 are RUN from one place, `goModViolations`, which both the root guard
   and the nested extension point call. A new go.mod rule goes in there and
   both inherit it; adding it to either caller instead leaves the other silent,
   which is what a probe demonstrated with a seventh rule over the `exclude`
   directive before the two lists were merged.
   `TestGoModRulesHaveOneSite` holds the shape: a consumer calls
   `goModViolations` and no other `…Violations` function.
4. **What the walk covers at all** — `scanNestedModules` and
   `nestedModuleAllowlist`. A declared nested module buys the WHOLE guard: the
   import scan (mechanism 1), that module's own `go.mod` through both
   dependency mechanisms (2 and 3), and this walk again, recursively. Declaring
   one never suspends the mechanism that would have reported what is inside it.
   **A mechanism added to this file must be applied HERE, in the same edit.**
   Five review rounds found five escapes and all five had one shape: extend one
   mechanism into the nested scan, leave the others behind. The recursion that
   closed round 4 inherited mechanism 1's PATH SCOPE but re-anchored it — a
   declared module got its own Centrifuge exemption, widenable by creating a
   directory — and did not inherit 2 or 3 at all, so a declared module could
   `require` Factory indirectly and `replace` Core with `../../../core` while
   `make check` stayed green.
   Paths are classified relative to the OUTERMOST module root (`qualify`), at
   classification TIME and not at report time; that is what stops a path-scoped
   rule re-anchoring, and any path-scoped rule added to `importVerdict`
   inherits it by construction.
   Every mechanism now has one site: 1 through `scanImports`'s REQUIRED prefix
   parameter, 2 and 3 through `goModViolations`, 4 through the recursion, 5
   through the exported predicates and their oracles.
   The floors are deliberately NOT uniform: `files` and `imports` are floored
   because violations are derived from them, `productionFiles` is not, because
   a declared nested module may legitimately be test support and flooring it
   would force a false violation. That asymmetry is asserted, not assumed — see
   the "only test files" subtest. Do not apply "floor what the caller consumes"
   mechanically here.
5. **What any walk can see** — the exported predicates in `internal/modfiles`.
   Every predicate deciding what a walk skips belongs there and needs a fuzz
   ORACLE in the consumer, not a sample table: a table is defeated by choosing a
   literal it does not list, and a widened predicate is a hole in every walk
   over this tree simultaneously — including `make fmt-check`, which pipes the
   same enumerator into gofmt.

Mechanisms 1-5 are about the MODULE. `import_boundary_test.go` also carries
meta-guards, which are about the SHAPE OF THE GUARD and have no numbered slot:
`TestGoModRulesHaveOneSite` holds "every mechanism has one site" and
`TestDocCommentsNameTheirOwnDeclaration` holds "a doc comment documents the
declaration it is attached to". A third one goes here, not in the numbered
list — and it must state its own limits in its doc comment, because a
meta-guard that overstates its reach is the defect it exists to catch.

An exclusion added to make a guard pass must itself be tested. A too-wide
exclusion produces exactly the same green as a correct one; that has been a live
defect here twice.

A doc comment must be separated from an unrelated declaration below it by a
blank line. `gofmt`, `go vet` and `staticcheck` all pass an attached one, and
this repository has lost a doc comment to that exact slip three times, so
`TestDocCommentsNameTheirOwnDeclaration` parses every module-owned file and
fails when a declaration has NO doc comment of its own while a line opening
with its name sits inside someone else's. The rule is over that symptom rather
than over Go's naming convention: a comment that merely discusses another,
documented declaration is ordinary and is left alone, and stating it this way
catches both orderings of the merge — the first version of the check saw only
the one where the stolen doc came first.

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

## Operator boundary

`host.Run` puts HostLink and probes/metrics on one plain-HTTP listener. Keep
that listener internal, supply the product's tenant credential verifier, and
terminate TLS before it. No browser Origin or CSRF policy is applied to
HostLink. Pooled and dedicated are placement modes; shared store leases and
Harness journals provide recovery. Only a pooled Host warm-releases idle
sessions; a dedicated Host deliberately does not (a departure from runbook O6.2,
see README), so do not "fix" `compose.Service.warmSamples` without a
cross-module proof against Factory's dedicated placement and the controller. A dedicated controller owns
terminal-drain-before-delete. Do not infer a disposition object store from
SessionStore's legacy object-first `PutObject`. Gate responses require the
advertised capability and the one-way Host upgrade rules in README; cold
AskUser resume is unsupported.
