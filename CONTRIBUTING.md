# Contributing to looprig/host

Thanks for contributing. `host` owns Looprig's Department runtime host: resident
sessions, command consumption, the HostLink realtime surface, and drain.

## Before writing code

Read [`CLAUDE.md`](CLAUDE.md). Open an issue for non-trivial public API or
protocol changes so compatibility can be reviewed first.

Host is consumed by Factory and must not depend on it. Do not import Factory, a
UI module, or a product or integration repository from anywhere in this module,
tests included. Centrifuge belongs only under `internal/realtime/hostlink/`. Do
not add local `replace` directives or vendor dependencies.

`import_boundary_test.go` enforces all of the above. If a change of yours needs
the boundary moved, move it there in the same pull request and say why.

## Build and test

Run these before pushing:

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

Add tests before implementation, keep errors typed, and mutation-test guards:
introduce the breach, confirm the specific named test fails, then revert. Keep
one logical change per pull request and call out public API changes explicitly.
