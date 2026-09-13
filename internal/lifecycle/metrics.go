package lifecycle

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
)

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

// Residencies is the local registry, narrowed to the snapshot a count reads.
//
// *registry.Registry satisfies it; the conformance assertion lives in this
// package's test so production here holds no mutable registry handle. What this
// interface can express is a READ of a slice of copies, which is what makes
// "metrics adjudicate nothing" structural rather than promised.
type Residencies interface {
	Snapshot() []registry.Entry
}

// Ledger is the Host's ONE admission ledger, narrowed to the charge.
//
// IT IS NOT RECOUNTED HERE. internal/service owns the only ledger and its own
// documentation names this task as a required consumer: two sources of
// "consumed" is an over-admission bug that neither package's tests could see,
// because each would be internally consistent while the Host admitted past its
// capacity. service.CapacityPublisher satisfies this as written.
type Ledger interface {
	ConsumedWeight() uint64
}

// GateWaits reports how many resident sessions are waiting on a gate.
//
// It is OPTIONAL because no production source exists in this module yet — a
// resident gate wait is §9.4 state that lives in the runtime, and the adapter
// that surfaces it is O7.1's composition. A Host composed without one reports
// zero, and the metric is documented as absent rather than as none.
type GateWaits interface {
	GateWaiting() uint64
}

// BlockedConsumers reports how many resident sessions have a durable command
// consumer that has stopped at a command it will not process.
//
// IT IS THE WEDGE'S ONLY OPERATIONAL SIGNATURE, and it exists because that
// condition had none. A blocked pass returns a NIL ERROR, so Consumer.Failures()
// stays at zero; nothing in internal/commands logs; and the HostLink delivery
// that triggered it was answered `accepted`. Without this gauge a wedged session
// is indistinguishable on every exported signal from a healthy Host with an empty
// inbox, and the first operator to meet it would have to diagnose it from source.
//
// ITS MEANING INVERTED WHEN THE DISPATCH BOUNDARY WAS REMOVED, and the HELP
// string a dashboard actually renders is the one that has to say so. A composed
// Host once refused EVERY dispatch, so a non-zero value was the designed steady
// state and expected to equal the number of sessions holding work. It now
// dispatches and settles, so a non-zero value is a FAULT: a command reached the
// runtime and could not be settled, or could not be dispatched at all. An
// operator reading the old sentence would have treated a real wedge as normal.
//
// IT IS OPTIONAL IN SHAPE AND WIRED IN FACT, which is the difference between it
// and GateWaits below. GateWaits has no production source at all; this one is
// satisfied by the composition, so the gauge is live in a real binary rather
// than reserved for a later one.
//
// A NON-ZERO VALUE IS NOT AN ERROR. It says work is durably present and this
// Host is not advancing past it — which is expected while dispatch is refused,
// and is a fault only once an applier exists. Alert on it changing meaning, not
// on it being non-zero.
type BlockedConsumers interface {
	// BlockedSessions returns the number of resident sessions whose last
	// consumption pass ended blocked at a command.
	BlockedSessions() uint64
}

// CommandBacklog reports how far behind durable command consumption is and how
// deep the local queue has grown. It is OPTIONAL for the same reason GateWaits
// is: internal/commands owns both numbers and the composition that wires them
// is O7.1's.
type CommandBacklog interface {
	// Backlog returns the durable lag and the local queue depth.
	Backlog() (lag uint64, depth uint64)
}

// ---------------------------------------------------------------------------
// The memory budget
// ---------------------------------------------------------------------------

// MemoryBudget is 04-host.md's weighted memory limit for a pooled Host:
// "capture ceiling × maximum parallel materialized fallbacks" for non-streaming
// bounded tools.
//
// NEITHER FIELD CAN BE DERIVED FROM department.Capabilities TODAY, and that is
// stated rather than papered over. Capabilities carries CaptureSafety — which
// is the CLASS of the highest-output tool — and no ceiling and no fallback
// parallelism, so this Host cannot compute a budget from what a target
// declares. The seam is here: a composition that knows those two numbers
// supplies them, and a Host that does not know them reports an UNKNOWN budget
// rather than a default.
//
// THE REJECTION HALF OF THE RUNBOOK'S RULE IS ALREADY HELD ELSEWHERE, and this
// type does not duplicate it. "Reject pooled admission for an unknown or
// unbounded descriptor rather than assuming preview size" is enforced by
// department.Capabilities.Validate, which refuses SupportsPooled alongside an
// unknown or unbounded CaptureSafety outright, and by service.placementSupported,
// which refuses admission for a target that does not support this Host's
// placement. What is left for this type is the SIZE of the budget, and an
// unknown size must not become a number.
type MemoryBudget struct {
	// CaptureCeilingBytes is the per-fallback bound a bounded-materialized
	// target enforces on its highest-output tool.
	CaptureCeilingBytes uint64

	// MaxParallelFallbacks is how many such materializations may be in flight
	// at once.
	MaxParallelFallbacks uint64
}

// Bytes returns the budget and whether it is known.
//
// UNKNOWN IS (0, false) AND NOT A DEFAULT. A caller that read the zero as a
// budget would admit without bound, which is the failure the runbook's "rather
// than assuming preview size" names.
//
// The multiply SATURATES. Two plausible operational numbers — a large ceiling
// and a modest parallelism — multiply past MaxUint64 and wrap to a tiny budget,
// which is a limit that refuses nothing while reading like one that does.
// Saturating to MaxUint64 is honest about the same thing the operator meant:
// more than this process has.
func (b MemoryBudget) Bytes() (uint64, bool) {
	if b.CaptureCeilingBytes == 0 || b.MaxParallelFallbacks == 0 {
		return 0, false
	}
	product := b.CaptureCeilingBytes * b.MaxParallelFallbacks
	if product/b.MaxParallelFallbacks != b.CaptureCeilingBytes {
		const maxUint64 = ^uint64(0)
		return maxUint64, true
	}
	return product, true
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

// Snapshot is this Host's load at one instant.
//
// THERE IS NO Attaching COUNT, and that is a gap rather than an omission.
// internal/registry has three states — resident, releasing and draining — and
// Insert installs a residency directly as resident, so there is no attaching
// row to count. An always-zero attaching gauge would be worse than none: an
// operator watching an attach storm would read a flat line as its absence.
// sessionwire.SessionResidencyAttaching exists on the wire and has no local
// counterpart; closing that is a registry change, not a metrics one.
type Snapshot struct {
	Resident  uint64
	Releasing uint64
	Draining  uint64

	// GateWaiting is resident sessions parked on a gate. Zero when no source
	// is composed, which is not the same as none; see GateWaits.
	GateWaiting uint64

	// BlockedSessions is resident sessions whose consumer has stopped at a
	// command. See BlockedConsumers for why a non-zero value is expected today.
	BlockedSessions uint64

	AdmissionWeightConsumed uint64
	AdmissionWeightCapacity uint64

	CommandLag uint64
	QueueDepth uint64

	// ReleaseFailures counts failed release STEPS and not failed releases. A
	// warm release recorded as released may carry failures — after its step 2
	// the release is committed and a failed step is recorded and continued past
	// — so counting outcomes would report zero for exactly the case an operator
	// must reconcile.
	ReleaseFailures uint64

	// MemoryBudgetBytes is meaningful only when MemoryBudgetKnown. See
	// MemoryBudget: unknown is not zero.
	MemoryBudgetBytes uint64
	MemoryBudgetKnown bool
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// MetricsOptions configures this Host's metrics.
type MetricsOptions struct {
	Residencies Residencies
	Ledger      Ledger

	// Capacity is the Host's configured admission capacity, supplied by the
	// composition rather than read from *host.Host, so this package keeps the
	// narrow-interface posture the rest of the module has.
	Capacity uint64

	// GateWaits, Backlog and Blocked are optional.
	GateWaits GateWaits
	Backlog   CommandBacklog
	Blocked   BlockedConsumers

	// Budget is the weighted memory limit. Its zero value is UNKNOWN.
	Budget MemoryBudget
}

// Metrics reports this Host's load, and is safe for concurrent use.
//
// HOST DOES NOT SCALE ITSELF. 04-host.md step 4: an HPA may consume these for
// pooled replicas, and scaling out creates capacity for NEW sessions — it does
// not migrate sessions pinned in an existing process. Nothing here acts on a
// number it publishes.
type Metrics struct {
	options MetricsOptions

	mu              sync.Mutex
	releaseFailures uint64
}

// Metrics consumes the warm releaser's outcomes directly. The assertion is here
// rather than in a test so a drift in either shape is a compile failure.
var _ residency.WarmObserver = (*Metrics)(nil)

// Metrics is a prometheus collector, which is how an autoscaler reaches it.
var _ prometheus.Collector = (*Metrics)(nil)

// NewMetrics validates the options and returns the collector.
func NewMetrics(options MetricsOptions) (*Metrics, error) {
	if options.Residencies == nil {
		return nil, &InvalidOptionsError{Field: "Residencies", Reason: "is required; a Host that cannot count what it holds resident cannot be scaled on it"}
	}
	if options.Ledger == nil {
		return nil, &InvalidOptionsError{Field: "Ledger", Reason: "is required, and it is internal/service's ONE ledger; counting admission weight here instead would be a second source of consumed"}
	}
	return &Metrics{options: options}, nil
}

// WarmRelease records one warm release attempt.
//
// AN ABORT IS NOT A FAILED RELEASE. It stopped at step 1, before anything was
// taken, and the session stays resident and is released on a later idle
// observation. Counting its recorded inbox failure would make an unreachable
// store look like a Host that cannot release.
func (m *Metrics) WarmRelease(outcome residency.WarmOutcome) {
	if outcome.Kind == residency.WarmOutcomeAborted {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseFailures += uint64(len(outcome.Failures))
}

// Snapshot reports this Host's load now.
func (m *Metrics) Snapshot() Snapshot {
	snapshot := Snapshot{
		AdmissionWeightConsumed: m.options.Ledger.ConsumedWeight(),
		AdmissionWeightCapacity: m.options.Capacity,
	}
	for _, entry := range m.options.Residencies.Snapshot() {
		switch entry.State {
		case registry.StateResident:
			snapshot.Resident++
		case registry.StateReleasing:
			snapshot.Releasing++
		case registry.StateDraining:
			snapshot.Draining++
		}
	}
	if m.options.GateWaits != nil {
		snapshot.GateWaiting = m.options.GateWaits.GateWaiting()
	}
	if m.options.Blocked != nil {
		snapshot.BlockedSessions = m.options.Blocked.BlockedSessions()
	}
	if m.options.Backlog != nil {
		snapshot.CommandLag, snapshot.QueueDepth = m.options.Backlog.Backlog()
	}
	snapshot.MemoryBudgetBytes, snapshot.MemoryBudgetKnown = m.options.Budget.Bytes()

	m.mu.Lock()
	snapshot.ReleaseFailures = m.releaseFailures
	m.mu.Unlock()
	return snapshot
}

// The published series. They are package-level descriptors so a dashboard
// depends on a list that changes only in a visible diff.
var (
	sessionsDesc = prometheus.NewDesc(
		"host_sessions",
		"Sessions this Host holds, by local residency state.",
		[]string{"state"}, nil,
	)
	blockedSessionsDesc = prometheus.NewDesc(
		"host_sessions_command_blocked",
		"Resident sessions whose durable command consumer has stopped at a command it CANNOT COMPLETE. A non-zero value is a FAULT and should be zero in a healthy Host: it means a command was dispatched into the runtime and could not then be settled, or could not be dispatched at all. It is the only signal that distinguishes a wedged session from an idle Host, because a blocked pass returns no error and logs nothing. Check the refusal: evidence_unavailable points at the runtime, evidence_unroutable at this deployment's settlement evidence wiring.",
		nil, nil,
	)
	gateWaitingDesc = prometheus.NewDesc(
		"host_sessions_gate_waiting",
		"Resident sessions parked on a gate. A gate waiter uses little CPU while retaining memory, which is why CPU alone is insufficient for pooled scaling.",
		nil, nil,
	)
	admissionConsumedDesc = prometheus.NewDesc(
		"host_admission_weight_consumed",
		"Admission weight currently charged against this Host's capacity.",
		nil, nil,
	)
	admissionCapacityDesc = prometheus.NewDesc(
		"host_admission_weight_capacity",
		"This Host's configured admission capacity.",
		nil, nil,
	)
	commandLagDesc = prometheus.NewDesc(
		"host_command_lag",
		"Durable command acceptance order not yet consumed by this Host.",
		nil, nil,
	)
	queueDepthDesc = prometheus.NewDesc(
		"host_command_queue_depth",
		"Commands buffered locally for application.",
		nil, nil,
	)
	releaseFailuresDesc = prometheus.NewDesc(
		"host_release_failures_total",
		"Release STEPS that did not succeed. A released session may contribute; an aborted release attempt does not.",
		nil, nil,
	)
	memoryBudgetDesc = prometheus.NewDesc(
		"host_memory_budget_bytes",
		"Weighted memory budget: capture ceiling times maximum parallel materialized fallbacks. ABSENT when no capture descriptor is composed, because an unknown budget is not a budget of zero.",
		nil, nil,
	)
)

// Describe sends every descriptor this collector can publish.
//
// THE MEMORY BUDGET IS DESCRIBED EVEN WHEN IT IS UNKNOWN, because Describe is
// about the collector's shape and Collect is about this instant. A descriptor
// withheld here would make registration's consistency checking depend on
// whether a descriptor happened to be composed.
func (m *Metrics) Describe(out chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		sessionsDesc, gateWaitingDesc, blockedSessionsDesc, admissionConsumedDesc,
		admissionCapacityDesc, commandLagDesc, queueDepthDesc, releaseFailuresDesc,
		memoryBudgetDesc,
	} {
		out <- desc
	}
}

// Collect publishes this instant's snapshot.
func (m *Metrics) Collect(out chan<- prometheus.Metric) {
	snapshot := m.Snapshot()
	gauge := func(desc *prometheus.Desc, value uint64, labels ...string) {
		out <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value), labels...)
	}
	gauge(sessionsDesc, snapshot.Resident, string(registry.StateResident))
	gauge(sessionsDesc, snapshot.Releasing, string(registry.StateReleasing))
	// DRAINING IS PUBLISHED AT ZERO, unlike the memory budget. Zero draining
	// sessions is a FACT this Host knows and an operator needs during a drain;
	// an unknown memory budget is the absence of one.
	gauge(sessionsDesc, snapshot.Draining, string(registry.StateDraining))
	gauge(gateWaitingDesc, snapshot.GateWaiting)
	gauge(blockedSessionsDesc, snapshot.BlockedSessions)
	gauge(admissionConsumedDesc, snapshot.AdmissionWeightConsumed)
	gauge(admissionCapacityDesc, snapshot.AdmissionWeightCapacity)
	gauge(commandLagDesc, snapshot.CommandLag)
	gauge(queueDepthDesc, snapshot.QueueDepth)
	out <- prometheus.MustNewConstMetric(releaseFailuresDesc, prometheus.CounterValue, float64(snapshot.ReleaseFailures))
	if snapshot.MemoryBudgetKnown {
		gauge(memoryBudgetDesc, snapshot.MemoryBudgetBytes)
	}
}
