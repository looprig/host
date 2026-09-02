// Package host is the Looprig Department runtime host.
//
// A Host owns resident sessions for one tenant: it consumes commands, keeps
// runtime targets resident, serves the HostLink realtime surface, and drains
// on release. It is consumed BY Factory and imports nothing from it; the
// dependency boundary that states this is enforced by import_boundary_test.go
// rather than by convention.
//
// This package currently carries a placeholder Host. The Department it serves
// — the immutable set of launch targets, and the segregated runtime
// capabilities Host consumes — lives in the department subpackage; residency,
// command consumption and HostLink follow.
package host
