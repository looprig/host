package lifecycle_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

// stubResidencies is the local registry's snapshot, narrowed. The concrete
// *registry.Registry is asserted to satisfy the seam below, so this double
// cannot drift looser than the type it stands in for.
type stubResidencies struct{ entries []registry.Entry }

func (s stubResidencies) Snapshot() []registry.Entry { return s.entries }

type stubLedger struct{ consumed uint64 }

func (s stubLedger) ConsumedWeight() uint64 { return s.consumed }

type stubGateWaits struct{ waiting uint64 }

func (s stubGateWaits) GateWaiting() uint64 { return s.waiting }

type stubBacklog struct{ lag, depth uint64 }

func (s stubBacklog) Backlog() (uint64, uint64) { return s.lag, s.depth }

// ---------------------------------------------------------------------------
// Conformance
// ---------------------------------------------------------------------------

// The concrete collaborators satisfy the seams, asserted at COMPILE TIME.
//
// A seam satisfied only by a test double is a seam nothing in production can be
// plugged into. These are `var _` declarations rather than a test body because
// a drift should be a build failure, and because the runtime form — comparing
// an interface variable holding a concrete type against nil — is a comparison
// that can never be false, which staticcheck reports as SA4023.
var (
	_ lifecycle.Residencies  = (*registry.Registry)(nil)
	_ residency.WarmObserver = (*lifecycle.Metrics)(nil)
	_ prometheus.Collector   = (*lifecycle.Metrics)(nil)
)

// ---------------------------------------------------------------------------
// The snapshot
// ---------------------------------------------------------------------------

// TestTheSnapshotCountsResidencyByState is the first metric group 04-host.md
// asks for, and it carries the ONE it cannot supply.
//
// ATTACHING IS NOT REPORTED, and that is a gap rather than an oversight.
// registry has three states — resident, releasing and draining — and Insert
// installs a residency directly as resident, so there is no attaching row for a
// count to read. Publishing an always-zero attaching gauge would be worse than
// omitting it: an operator watching an attach storm would see a flat line and
// conclude there was none. The wire member sessionwire.SessionResidencyAttaching
// exists and the local registry has no counterpart; closing that is a registry
// change, not a metrics one.
func TestTheSnapshotCountsResidencyByState(t *testing.T) {
	t.Parallel()

	metrics := newMetrics(t, func(o *lifecycle.MetricsOptions) {
		o.Residencies = stubResidencies{entries: []registry.Entry{
			{State: registry.StateResident},
			{State: registry.StateResident},
			{State: registry.StateResident},
			{State: registry.StateReleasing},
			{State: registry.StateReleasing},
			{State: registry.StateDraining},
		}}
	})

	snapshot := metrics.Snapshot()
	// Each count is a DIFFERENT number, so a snapshot that read the wrong
	// field cannot agree with the right one by coincidence.
	if snapshot.Resident != 3 {
		t.Errorf("Resident = %d, want 3", snapshot.Resident)
	}
	if snapshot.Releasing != 2 {
		t.Errorf("Releasing = %d, want 2", snapshot.Releasing)
	}
	if snapshot.Draining != 1 {
		t.Errorf("Draining = %d, want 1", snapshot.Draining)
	}
}

// TestTheSnapshotReportsTheLedgerAndItsCapacity holds the admission half. The
// consumed weight is read from the Host's ONE ledger rather than counted here:
// internal/service's own documentation names this task as a required consumer
// and says why a second count is an over-admission bug neither package's tests
// could see.
func TestTheSnapshotReportsTheLedgerAndItsCapacity(t *testing.T) {
	t.Parallel()

	metrics := newMetrics(t, func(o *lifecycle.MetricsOptions) {
		o.Ledger = stubLedger{consumed: 11}
		o.Capacity = 29
	})

	snapshot := metrics.Snapshot()
	if snapshot.AdmissionWeightConsumed != 11 {
		t.Errorf("AdmissionWeightConsumed = %d, want 11", snapshot.AdmissionWeightConsumed)
	}
	if snapshot.AdmissionWeightCapacity != 29 {
		t.Errorf("AdmissionWeightCapacity = %d, want 29", snapshot.AdmissionWeightCapacity)
	}
}

// TestTheOptionalSourcesReportZeroWhenAbsentAndTheirValueWhenPresent covers the
// gate-wait count and the command backlog, which a Host may be composed
// without.
//
// The absent case asserts ZERO AND NO PANIC, and the present case asserts three
// distinct values. A snapshot that ignored a source entirely passes the first
// and fails the second, which is why both halves are here.
func TestTheOptionalSourcesReportZeroWhenAbsentAndTheirValueWhenPresent(t *testing.T) {
	t.Parallel()

	bare := newMetrics(t)
	if snapshot := bare.Snapshot(); snapshot.GateWaiting != 0 || snapshot.CommandLag != 0 || snapshot.QueueDepth != 0 {
		t.Errorf("a Host composed without the optional sources reported %+v, want zeroes", snapshot)
	}

	full := newMetrics(t, func(o *lifecycle.MetricsOptions) {
		o.GateWaits = stubGateWaits{waiting: 5}
		o.Backlog = stubBacklog{lag: 7, depth: 13}
	})
	snapshot := full.Snapshot()
	if snapshot.GateWaiting != 5 {
		t.Errorf("GateWaiting = %d, want 5", snapshot.GateWaiting)
	}
	if snapshot.CommandLag != 7 {
		t.Errorf("CommandLag = %d, want 7", snapshot.CommandLag)
	}
	if snapshot.QueueDepth != 13 {
		t.Errorf("QueueDepth = %d, want 13", snapshot.QueueDepth)
	}
}

// ---------------------------------------------------------------------------
// Release failures
// ---------------------------------------------------------------------------

// TestReleaseFailuresCountFailedStepsAndNotFailedReleases is the metric whose
// definition is the whole of its value.
//
// A warm release that is RECORDED as released may carry failures: after step 2
// the release is committed and a failed step is recorded and continued past.
// Counting outcomes would therefore report zero for exactly the case an
// operator must reconcile — a released session whose checkpoint refused — so
// this counts STEPS. An abort before step 2 has taken nothing and is not a
// release failure at all.
func TestReleaseFailuresCountFailedStepsAndNotFailedReleases(t *testing.T) {
	t.Parallel()

	metrics := newMetrics(t)
	if got := metrics.Snapshot().ReleaseFailures; got != 0 {
		t.Fatalf("ReleaseFailures = %d on a fresh Metrics, want 0", got)
	}

	metrics.WarmRelease(residency.WarmOutcome{Kind: residency.WarmOutcomeReleased})
	if got := metrics.Snapshot().ReleaseFailures; got != 0 {
		t.Errorf("ReleaseFailures = %d after a clean release, want 0", got)
	}

	metrics.WarmRelease(residency.WarmOutcome{
		Kind: residency.WarmOutcomeReleased,
		Failures: []residency.WarmFailure{
			{Step: residency.WarmStepCheckpoint, Err: errors.New("the checkpoint refused")},
			{Step: residency.WarmStepReleaseLease, Err: errors.New("the lease would not release")},
		},
	})
	if got := metrics.Snapshot().ReleaseFailures; got != 2 {
		t.Errorf("ReleaseFailures = %d, want 2: a released session may carry failures an operator must reconcile", got)
	}

	// An abort before step 2 took nothing, and its recorded inbox failure is
	// not a release failure.
	metrics.WarmRelease(residency.WarmOutcome{
		Kind:     residency.WarmOutcomeAborted,
		Failures: []residency.WarmFailure{{Step: residency.WarmStepInbox, Err: errors.New("unreachable")}},
	})
	if got := metrics.Snapshot().ReleaseFailures; got != 2 {
		t.Errorf("ReleaseFailures = %d after an abort, want 2: an abort has taken nothing and is not a failed release", got)
	}

	// The counter only ever rises. A gauge here would let a Host that released
	// cleanly erase the evidence of one that did not.
	metrics.WarmRelease(residency.WarmOutcome{Kind: residency.WarmOutcomeReleased})
	if got := metrics.Snapshot().ReleaseFailures; got != 2 {
		t.Errorf("ReleaseFailures = %d after a later clean release, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// The memory budget
// ---------------------------------------------------------------------------

// TestTheMemoryBudgetIsCeilingTimesParallelismAndSaturates is 04-host.md's
// "Budget capture ceiling × maximum parallel materialized fallbacks for
// non-streaming bounded tools".
//
// UNKNOWN IS NOT ZERO AND IS NOT A DEFAULT, which is the runbook's "reject
// pooled admission for an unknown/unbounded descriptor rather than assuming
// preview size" expressed as a return value: Bytes reports (0, false), and a
// caller that treated the zero as a budget would admit without bound. The
// SATURATION half is the one a plain multiply gets wrong — two plausible
// operational numbers multiply past MaxUint64 and wrap to a tiny budget, which
// is a limit that refuses nothing and reads like one that does.
func TestTheMemoryBudgetIsCeilingTimesParallelismAndSaturates(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name      string
		budget    lifecycle.MemoryBudget
		want      uint64
		wantKnown bool
	}{
		{
			name:      "a described target",
			budget:    lifecycle.MemoryBudget{CaptureCeilingBytes: 4 << 20, MaxParallelFallbacks: 3},
			want:      12 << 20,
			wantKnown: true,
		},
		{
			name:   "no ceiling declared",
			budget: lifecycle.MemoryBudget{MaxParallelFallbacks: 3},
		},
		{
			name:   "no parallelism declared",
			budget: lifecycle.MemoryBudget{CaptureCeilingBytes: 4 << 20},
		},
		{
			name:   "nothing declared",
			budget: lifecycle.MemoryBudget{},
		},
		{
			name:      "a product that would wrap",
			budget:    lifecycle.MemoryBudget{CaptureCeilingBytes: math.MaxUint64/2 + 1, MaxParallelFallbacks: 4},
			want:      math.MaxUint64,
			wantKnown: true,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			got, known := row.budget.Bytes()
			if known != row.wantKnown {
				t.Fatalf("Bytes() known = %v, want %v", known, row.wantKnown)
			}
			if got != row.want {
				t.Errorf("Bytes() = %d, want %d", got, row.want)
			}
		})
	}
}

// TestTheSnapshotCarriesTheBudgetAndSaysWhenItIsUnknown holds that the
// unknown/known distinction survives into the snapshot rather than being
// flattened to a zero an exporter cannot tell from a budget of nothing.
func TestTheSnapshotCarriesTheBudgetAndSaysWhenItIsUnknown(t *testing.T) {
	t.Parallel()

	unknown := newMetrics(t).Snapshot()
	if unknown.MemoryBudgetKnown {
		t.Error("a Host composed without a capture descriptor reported a known memory budget")
	}
	if unknown.MemoryBudgetBytes != 0 {
		t.Errorf("MemoryBudgetBytes = %d with no descriptor, want 0", unknown.MemoryBudgetBytes)
	}

	known := newMetrics(t, func(o *lifecycle.MetricsOptions) {
		o.Budget = lifecycle.MemoryBudget{CaptureCeilingBytes: 1 << 20, MaxParallelFallbacks: 6}
	}).Snapshot()
	if !known.MemoryBudgetKnown {
		t.Fatal("a described budget was reported unknown")
	}
	if known.MemoryBudgetBytes != 6<<20 {
		t.Errorf("MemoryBudgetBytes = %d, want %d", known.MemoryBudgetBytes, 6<<20)
	}
}

// ---------------------------------------------------------------------------
// Construction and export
// ---------------------------------------------------------------------------

func TestNewMetricsRefusesAnUnusableConfiguration(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name  string
		spoil func(*lifecycle.MetricsOptions)
		field string
	}{
		{name: "no residencies", spoil: func(o *lifecycle.MetricsOptions) { o.Residencies = nil }, field: "Residencies"},
		{name: "no ledger", spoil: func(o *lifecycle.MetricsOptions) { o.Ledger = nil }, field: "Ledger"},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			options := validMetricsOptions()
			row.spoil(&options)
			metrics, err := lifecycle.NewMetrics(options)
			if metrics != nil {
				t.Error("a Metrics was returned alongside the refusal")
			}
			var invalid *lifecycle.InvalidOptionsError
			if !errors.As(err, &invalid) {
				t.Fatalf("NewMetrics = %v, want an *InvalidOptionsError", err)
			}
			if invalid.Field != row.field {
				t.Errorf("the refusal names field %q, want %q", invalid.Field, row.field)
			}
		})
	}
}

// TestTheCollectorPublishesEverySnapshotField is what makes these metrics
// reachable by an HPA at all, and it is derived rather than listed.
//
// A gauge added to the Snapshot and forgotten in Collect is invisible to every
// assertion about the Snapshot, and that is the failure this test exists for:
// it gathers through a real prometheus registry and requires one series per
// documented name, with the VALUE the snapshot carries. Hard-coding the name
// list is deliberate — the list is what a dashboard depends on, so it should be
// a visible diff when it changes.
func TestTheCollectorPublishesEverySnapshotField(t *testing.T) {
	t.Parallel()

	metrics := newMetrics(t, func(o *lifecycle.MetricsOptions) {
		o.Residencies = stubResidencies{entries: []registry.Entry{
			{State: registry.StateResident},
			{State: registry.StateResident},
			{State: registry.StateReleasing},
		}}
		o.Ledger = stubLedger{consumed: 11}
		o.Capacity = 29
		o.GateWaits = stubGateWaits{waiting: 5}
		o.Backlog = stubBacklog{lag: 7, depth: 13}
		o.Budget = lifecycle.MemoryBudget{CaptureCeilingBytes: 1 << 20, MaxParallelFallbacks: 6}
	})
	metrics.WarmRelease(residency.WarmOutcome{
		Kind:     residency.WarmOutcomeReleased,
		Failures: []residency.WarmFailure{{Step: residency.WarmStepCheckpoint, Err: errors.New("refused")}},
	})

	gatherer := prometheus.NewRegistry()
	if err := gatherer.Register(metrics); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			name := family.GetName()
			for _, label := range metric.GetLabel() {
				name += "{" + label.GetName() + "=" + label.GetValue() + "}"
			}
			// The value is read WITHOUT naming client_model. Declaring a
			// helper over *dto.Metric would import it, which makes an
			// indirect requirement direct and moves a line in go.mod for a
			// test convenience.
			if gauge := metric.GetGauge(); gauge != nil {
				got[name] = gauge.GetValue()
			} else {
				got[name] = metric.GetCounter().GetValue()
			}
		}
	}

	want := map[string]float64{
		"host_sessions{state=resident}":  2,
		"host_sessions{state=releasing}": 1,
		"host_sessions{state=draining}":  0,
		"host_sessions_gate_waiting":     5,
		"host_admission_weight_consumed": 11,
		"host_admission_weight_capacity": 29,
		"host_command_lag":               7,
		"host_command_queue_depth":       13,
		"host_release_failures_total":    1,
		"host_memory_budget_bytes":       6 << 20,
	}
	for name, value := range want {
		seen, published := got[name]
		if !published {
			t.Errorf("%s is not published; a snapshot field with no series is invisible to an autoscaler", name)
			continue
		}
		if seen != value {
			t.Errorf("%s = %v, want %v", name, seen, value)
		}
	}
	if len(got) != len(want) {
		t.Errorf("the collector published %d series and this test names %d:\n  got %v", len(got), len(want), got)
	}
}

// TestAnUnknownMemoryBudgetPublishesNoSeries is the other half of "unknown is
// not zero": a gauge reporting 0 bytes would tell an autoscaler this Host has
// no memory headroom, which is the opposite of "nobody has said".
func TestAnUnknownMemoryBudgetPublishesNoSeries(t *testing.T) {
	t.Parallel()

	gatherer := prometheus.NewRegistry()
	if err := gatherer.Register(newMetrics(t)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if strings.Contains(family.GetName(), "memory_budget") {
			t.Errorf("%s is published with no capture descriptor; unknown must be absent, not zero", family.GetName())
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validMetricsOptions() lifecycle.MetricsOptions {
	return lifecycle.MetricsOptions{
		Residencies: stubResidencies{},
		Ledger:      stubLedger{},
	}
}

func newMetrics(t *testing.T, configure ...func(*lifecycle.MetricsOptions)) *lifecycle.Metrics {
	t.Helper()
	options := validMetricsOptions()
	for _, apply := range configure {
		apply(&options)
	}
	metrics, err := lifecycle.NewMetrics(options)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	return metrics
}
