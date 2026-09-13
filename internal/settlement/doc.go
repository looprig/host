// Package settlement holds the end-to-end proof that one command APPLIES and
// SETTLES across the three modules that had to release for it to be possible.
//
// IT IS ITS OWN PACKAGE BECAUSE IT IS THE ONLY PLACE BOTH STORES MEET.
// internal/compose runs against doubles and one released orchestration store;
// internal/harnessadapter runs against harness with no orchestration store at
// all. Neither can answer the question this package exists for — does a command
// dispatched by Host's own applier, under a residency grant the released store
// issued, reach a runtime that writes a real kind-5 disposition frame into a real
// harness journal, and does the released settlement then read that frame back
// and settle the command — and a claim that no single test makes is a claim
// nobody has measured.
//
// THE TWO STORES ARE ON TWO BACKENDS, which is the design and not a convenience.
// A disposition session's catalog needs the released store's MULTI-TENANT layout,
// while harness addresses its journals on the LEGACY single-tenant layout as
// "sessions/<uuid>"; pointing one store at both is a configuration harness
// refuses at the first catalog write. The binding is what joins them, and
// resolving it is the composition root's job.
//
// WHAT IS STILL A DOUBLE HERE, said plainly: the RUNTIME. Driving a real agent
// loop would need an inference provider and a whole rig, and what this package
// is about is the DURABLE protocol rather than what a model said. The double
// does exactly what the released writer does — appends the application prefix and
// then, as a separate bodiless frame, the disposition — through harness's own
// RuntimeCommandLog, so every durable byte on the journal side is written by
// harness's code rather than by this test's.
package settlement
