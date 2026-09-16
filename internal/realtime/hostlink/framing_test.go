package hostlink_test

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
)

// TestFramingNamesAndChannelBytesArePinned holds the HostLink framing to
// LITERAL BYTES rather than to whichever constant currently produces them.
//
// The literals below were taken from this package BEFORE its framing moved
// onto Core's shared names (core v0.8.0, hostlink_framing.go), and they are
// the property that move had to preserve: a Factory built against the old
// Host and a Host built against Core's names must mint the same method
// strings and the same channel for the same key, byte for byte. Asserting
// `hostlink.MethodBind == sessionwire.HostLinkMethodBind` would prove only
// that two constants agree, and both could move together; a literal cannot.
//
// The seven channel rows are Core's own goldens for HostLinkChannel, which
// were themselves derived by running THIS package's ChannelFor on a copy of
// Host pinned to core v0.7.0 (CODEX_RESULT_CORE_V080_ATTACH.md). Each row
// exercises one thing the encoding has to get right: the separator inside an
// identifier, the unpadded alphabet, non-ASCII bytes, and characters the
// standard alphabet would spell differently.
func TestFramingNamesAndChannelBytesArePinned(t *testing.T) {
	t.Parallel()

	for _, method := range []struct {
		name string
		got  string
		want string
	}{
		{"MethodBind", hostlink.MethodBind, "hostlink.bind"},
		{"MethodUnbind", hostlink.MethodUnbind, "hostlink.unbind"},
		{"MethodDrain", hostlink.MethodDrain, "hostlink.drain"},
		{"MethodDrainStatus", hostlink.MethodDrainStatus, "hostlink.drain_status"},
		{"ChannelPrefix", hostlink.ChannelPrefix, "hostlink.v1."},
	} {
		if method.got != method.want {
			t.Errorf("%s = %q, want %q", method.name, method.got, method.want)
		}
	}

	for _, row := range []struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		want    string
	}{
		{"tenant-1", "session-1", "hostlink.v1.dGVuYW50LTE.c2Vzc2lvbi0x"},
		{"a", "b", "hostlink.v1.YQ.Yg"},
		{"ab", "abc", "hostlink.v1.YWI.YWJj"},
		{"a.b", "c", "hostlink.v1.YS5i.Yw"},
		{"a", "b.c", "hostlink.v1.YQ.Yi5j"},
		{"~~~", "???", "hostlink.v1.fn5-.Pz8_"},
		{"tenant-é", "sess/ü+", "hostlink.v1.dGVuYW50LcOp.c2Vzcy_DvCs"},
	} {
		key := registry.Key{TenantID: row.tenant, SessionID: row.session}
		if got := hostlink.ChannelFor(key); got != row.want {
			t.Errorf("ChannelFor(%q, %q) = %q, want %q", row.tenant, row.session, got, row.want)
		}
	}
}
