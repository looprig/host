// Package service derives what a Host publishes about itself.
//
// Today that is the TARGET DIRECTORY of spec §15: one advertisement per
// Department LaunchTarget, describing what this Host can launch and how much
// room it has left. Factory pages these to choose a placement candidate and
// aggregates them into `/v1/agents`.
//
// WHAT THIS PACKAGE DOES NOT DO, because §15 says Factory must not find it
// here: it publishes NO SECOND CATALOGUE and COPIES NO RIG SUBAGENT ENTRY.
//
// Those two halves are ONE property, because a copied Rig subagent entry and an
// invented static row are the same thing seen from two sides: a published row
// whose agent this Department does not register. One pair of whitelists holds
// it. TestPublishedAgentsAreExactlyTheDepartmentsRegisteredAgents holds the
// DATA — the rows Publish returns are in bijection with Department.AgentIDs, in
// all three of initial publish, post-admission and drain.
// TestPublishIsTheOnlyPublishingSurface holds the API: this package's production
// files are enumerated, every exported top-level DECLARATION in them is
// enumerated, every exported STRUCT's fields are enumerated, and exactly one
// declaration lets an Advertisement reach a caller.
//
// EACH OF THOSE WORDS REPLACED A NARROWER ONE THAT A PROBE DEFEATED, and the
// four failures were one mistake: the guard named a proxy for its subject.
// Scoped to one file name, a second publishing surface in a new FILE escaped.
// Walking only *ast.FuncDecl, one declared as a func-typed VAR escaped.
// Collecting only named fields, an EMBEDDED struct added published fields
// invisibly. Checking only a function's RESULTS, a second catalogue delivered
// into a caller-supplied sink PARAMETER escaped, because "results" is a proxy
// for "reaches a caller". All four are closed and all four are re-probed. Both are stated as "enumerate
// what may appear and report everything else", because the alternative shape —
// searching for a second catalogue — cannot establish an absence.
//
// WHAT THOSE TWO DO NOT REACH, measured rather than assumed. They are scoped to
// this package: a second catalogue published from a DIFFERENT package is
// invisible to both, and so is anything a caller appends after Publish has
// returned. A probe confirmed the file scope was the live gap — a second
// exported publishing surface added in a new file of this package survived both
// guards until the FILE SET became part of the enumeration; it now fails. And
// note what the API guard does not claim: "nothing here reads a Rig" is true of
// the code as written and is not itself asserted anywhere. What is asserted is
// the consequence — that no row reaches the output whose agent the Department
// does not register — which is what a copied subagent listing would have to do
// to matter.
//
// NOTHING HERE WRITES. An Advertisement is a value describing a record; the
// SessionStore OrderedIndex write, its CAS, and the due reconciler that reaps a
// crashed Host's rows are later tasks. "Heartbeats move rank and expiry
// atomically" is held here only in the form this layer can hold it — both are
// fields of one derived record, so a writer CASes them together or not at all.
package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/registry"
)

// AdvertisementNamespace is the OrderedIndex namespace target advertisements
// live in. It obeys the storage name grammar — lower-case segments joined by
// '/' — because a namespace that did not could never be written.
const AdvertisementNamespace = "host/target-directory"

// rankingScopePrefix opens every ranking scope, so a scope is two segments and
// reads as what it is at a glance in a backend listing.
const rankingScopePrefix = "launchtarget/"

// Advertisement is one derived target-directory record.
//
// It is a VALUE and carries no method that writes. Report is the wire record a
// reader consumes; the seven fields around it are the OrderedIndex placement of
// that record — where it is stored (Namespace, StableKey), how it is ordered
// against other Hosts offering the same LaunchTarget (RankingScope, Rank,
// Ranked), when a due reconciler sees it (DueAt), and whether it is a
// gravestone rather than an offer (Tombstone).
type Advertisement struct {
	// Namespace is the OrderedIndex namespace. It is AdvertisementNamespace for
	// every record this package derives, and is carried on the value rather
	// than left implicit so a writer needs nothing but the Advertisement.
	Namespace string

	// RankingScope is the LaunchTarget scope this record is ranked within. It
	// is the stable key's tuple MINUS the host, because ranking exists to
	// compare Hosts offering the same target against each other.
	RankingScope string

	// StableKey identifies this record: (agent, runtime compatibility,
	// placement, host). It is a digest, not the tuple — see stableKey.
	StableKey string

	// Rank is available admission capacity, which is what §15 ranks by. It is
	// zero on a tombstone.
	Rank int64

	// Ranked reports whether this record appears in a ranked page at all. It is
	// false for a drained Host AND for a target this Host cannot place, and the
	// second half is a DECISION beyond what §15 spells out — the spec names
	// drain as un-ranking and does not say drain is the only thing that may.
	//
	// The distinction it rests on is between a row that is not a candidate NOW
	// and one that can never be a candidate HERE. A full but accepting Host has
	// zero available capacity and stays ranked, because the next heartbeat may
	// find room; a target whose declared placement this Host does not offer is
	// unplaceable for the life of the process, so ranking it only puts a row in
	// the page Factory must then discard. Tombstone, not Ranked, is what
	// distinguishes drain from either.
	//
	// It therefore coincides with Report.Accepting in every state this package
	// can derive. They are separate fields because they are read by different
	// parties — Accepting is record CONTENT a reader filters on, Ranked is an
	// INDEX operation the writer performs — and the coincidence is asserted by
	// test rather than assumed, in all three states.
	Ranked bool

	// DueAt is when a bounded due reconciler must look at this record. It is
	// ExpiresAt, so a Host that stops heartbeating is reaped by its own expiry.
	DueAt time.Time

	// Tombstone reports a record graceful drain has retired. It is a record and
	// not a deletion, so a reader that has already seen the offer learns it is
	// gone rather than waiting out the expiry.
	Tombstone bool

	// Report is the sessionwire observation itself, validated before it is
	// returned.
	Report sessionwire.HostLinkCapacityReport
}

// Expired reports whether a reader must treat this record as absent at now.
//
// The boundary is INCLUSIVE of the expiry instant: a record whose ExpiresAt has
// arrived is gone. The alternative reading keeps a lapsed advertisement alive
// for one instant, which buys nothing and makes "expired" disagree with
// Core's validateHostLinkTimes, where ExpiresAt must be strictly after
// ObservedAt.
func (a Advertisement) Expired(now time.Time) bool { return !now.Before(a.Report.ExpiresAt) }

// CapacityOptions configures a CapacityPublisher.
type CapacityOptions struct {
	// Host is the validated Host whose Department is advertised. Everything
	// else the derivation needs — identity, endpoint, isolation class,
	// placement, capacity, registry expiry, clock — is read from it, so there
	// is no second copy of a value host.New already checked.
	Host *host.Host

	// HostGeneration is this Host process's incarnation identity, which Core
	// requires to be non-zero on every HostLink record. It is NOT on
	// host.Options: a Host that restarts on the same configuration is a new
	// generation, so the value belongs to the run and not to the configuration.
	HostGeneration uint64
}

// admission is one charged ledger entry. The agent is kept alongside the weight
// so a re-admission under a different agent is a detectable conflict rather
// than a silent weight the ledger cannot attribute.
type admission struct {
	agent  sessionwire.AgentID
	weight uint64
}

// CapacityPublisher derives the Department's target advertisements.
//
// It holds the ONLY mutable state in this package: the admission ledger and the
// drain flag. Everything else is either read from the immutable Host or taken
// from the target snapshot below, so no value it derives from can drift from
// the one that was validated.
//
// THAT SENTENCE WAS FALSE WHEN IT WAS FIRST WRITTEN, which is why it now names
// the snapshot. Capabilities() and CompatibilityID() were re-read on every
// heartbeat, and they are caller code free to answer differently each time; the
// claim covered only the Host and read as though it covered everything. See
// launchable for the panic that cost.
//
// FOR O6.1 AND O6.3, WHICH ARRIVE LATER AND ELSEWHERE. 04-host.md puts weighted
// memory/admission limits in internal/residency and internal/lifecycle, and
// drain in internal/lifecycle/drain.go. Those must CONSUME this ledger and this
// flag, not keep their own. Two sources of "consumed" is an over-admission bug
// that neither package's tests could see — each would be internally consistent
// and the Host would admit past its capacity — and two sources of "draining" is
// a Host that stops accepting in one place while still advertising Accepting
// from the other. Whatever the final ownership, there must be exactly one of
// each, and this is the one that already exists.
type CapacityPublisher struct {
	host       *host.Host
	generation uint64

	// targets is the SNAPSHOT the whole package derives from, taken and
	// validated once. See launchable for why it is a snapshot and not a live
	// read, and byAgent for why both shapes are kept.
	targets []launchable
	byAgent map[sessionwire.AgentID]launchable

	mu       sync.Mutex
	admitted map[registry.Key]admission
	consumed uint64
	draining bool
}

// launchable is one LaunchTarget's description, SNAPSHOTTED AND VALIDATED at
// construction rather than re-read on every heartbeat.
//
// THE SNAPSHOT IS THE POINT, and it is not an optimisation. department.New
// validates a target's capabilities ONCE, at registration, and department's own
// type documentation says why that is all it can do: a Department "does not and
// cannot copy the TARGETS, which are interface values a caller may still hold
// and whose implementations may carry state". So Capabilities() is arbitrary
// caller code returning an arbitrary answer each time it is called, and the
// validated answer and the live one are not the same thing.
//
// Reading it live cost a PANIC on the periodic path: a target returning weight
// 1 at registration and 0 afterwards divided by zero inside Publish, on the
// heartbeat, in a file where every other fault is a typed refusal. Measured,
// not argued. The compatibility id is snapshotted for the same reason and a
// worse consequence — it is an input to StableKey, so a drifting one would
// silently start writing to a DIFFERENT record and orphan the live
// advertisement until it expired, with no error anywhere.
//
// The keys are derived here too, once, because they are pure functions of
// snapshotted values. That makes "a heartbeat refreshes the record it published"
// structural rather than something the derivation has to keep getting right.
type launchable struct {
	agent         sessionwire.AgentID
	compatibility department.CompatibilityID
	capabilities  department.Capabilities
	stableKey     string
	rankingScope  string
}

// NewCapacityPublisher validates options and returns a publisher.
//
// It SNAPSHOTS every LaunchTarget's description, validates it, and then derives
// the whole publication once, refusing if any record would be one Core rejects.
//
// The snapshot is what makes everything after this total: see launchable. The
// trial derivation is separate and is not belt-and-braces — HostGeneration is a
// required non-zero wire field that host.Options does not carry, and Clock is an
// injected interface that may return an instant Core refuses, so a publisher can
// be constructed that could never publish anything. Failing here hands the
// operator the configuration; failing at the first heartbeat hands them a Host
// that started and then went silent.
func NewCapacityPublisher(options CapacityOptions) (*CapacityPublisher, error) {
	if options.Host == nil {
		return nil, &InvalidCapacityOptionsError{Field: "Host", Reason: "must be set; there is nothing to advertise without one"}
	}
	agents := options.Host.Department().AgentIDs()
	publisher := &CapacityPublisher{
		host:       options.Host,
		generation: options.HostGeneration,
		targets:    make([]launchable, 0, len(agents)),
		byAgent:    make(map[sessionwire.AgentID]launchable, len(agents)),
		admitted:   map[registry.Key]admission{},
	}
	for _, agent := range agents {
		target, err := options.Host.Department().Target(agent)
		if err != nil {
			// Unreachable: AgentIDs lists the map Target reads and a Department
			// is immutable in its MAPPING, which is the half that is immutable.
			// Reported rather than ignored because the alternative is a
			// publisher that silently advertises fewer targets than the Host
			// serves.
			return nil, &InvalidCapacityOptionsError{
				Field:  "Host",
				Reason: "the Department lists agent " + strconv.Quote(string(agent)) + " and cannot resolve it",
				Cause:  err,
			}
		}
		snapshot := launchable{
			agent:         agent,
			compatibility: target.CompatibilityID(),
			capabilities:  target.Capabilities(),
		}
		// Re-validated HERE, against the values actually captured. department.New
		// validated what the target returned at REGISTRATION; this validates
		// what it returned just now, which is the pair of answers that can
		// differ. Without it a target already misbehaving at construction would
		// have its zero weight snapshotted and divided by on the first
		// heartbeat, which is the panic this snapshot exists to remove.
		if err := snapshot.compatibility.Validate(); err != nil {
			return nil, &InvalidCapacityOptionsError{
				Field:  "Host",
				Reason: "the launch target for agent " + strconv.Quote(string(agent)) + " reports an unusable compatibility id",
				Cause:  err,
			}
		}
		if err := snapshot.capabilities.Validate(); err != nil {
			return nil, &InvalidCapacityOptionsError{
				Field:  "Host",
				Reason: "the launch target for agent " + strconv.Quote(string(agent)) + " reports capabilities it could not have been registered with",
				Cause:  err,
			}
		}
		snapshot.stableKey = stableKey(agent, snapshot.compatibility, options.Host.Placement(), options.Host.ID())
		snapshot.rankingScope = rankingScope(agent, snapshot.compatibility, options.Host.Placement())
		publisher.targets = append(publisher.targets, snapshot)
		// byAgent holds the SAME values as targets. The slice is the publication
		// order and the map is Admit's lookup; deriving either from the other
		// per call would be the same answer at the cost of making the order or
		// the lookup a property of the caller rather than of the value.
		publisher.byAgent[agent] = snapshot
	}
	if _, err := publisher.Publish(); err != nil {
		return nil, &InvalidCapacityOptionsError{
			Field:  optionFieldFor(err),
			Reason: "this Host could not derive a publishable advertisement: " + err.Error(),
			Cause:  err,
		}
	}
	return publisher, nil
}

// optionFieldFor names the OPTION responsible for a wire field Core refused.
//
// It exists because the constructor previously hard-coded "HostGeneration" for
// both causes its own documentation names, so a Clock returning an instant Core
// refuses sent the operator to a knob that was already correct. The mapping is
// explicit and defaults to the Host, because the Clock, the endpoint, the
// isolation class and the identities all reach the wire through it.
func optionFieldFor(err error) string {
	var validation *sessionwire.RequestValidationError
	if errors.As(err, &validation) && validation.Field == "host_generation" {
		return "HostGeneration"
	}
	return "Host"
}

// Publish derives the current advertisement for every Department LaunchTarget.
//
// It is BOTH the initial publish and the heartbeat; there is no separate
// refresh path, because a heartbeat that derived its record differently from
// the first publish is exactly the drift that makes a stale advertisement hard
// to explain. The instant comes from the Host's Clock and the expiry from its
// validated RegistryExpiry, so the margin host.MinHeartbeatsBeforeExpiry
// enforces is the margin the record actually carries.
//
// A record Core would reject fails the whole call and the result is nil, so a
// caller cannot write the rows that happened to be derived first. The probe for
// that returns the rows derived so far alongside the error and is killed — but
// by the NIL, not by a genuinely partial slice: the only refusal reachable here
// is the observation instant, which is derived once and applies to every row at
// once, so no input this package accepts produces a partial publication to
// begin with.
func (p *CapacityPublisher) Publish() ([]Advertisement, error) {
	// NO INJECTED CALL HAPPENS UNDER THE LOCK, here or anywhere in this file.
	// sync.Mutex is not reentrant, so a Clock, a LaunchTarget or any other
	// caller-supplied implementation that called back into Draining,
	// ConsumedWeight, Admit or Release would deadlock the Host rather than
	// misbehave visibly. Nothing does that today; the fixtures in this
	// package's tests are enough to show these are arbitrary caller code.
	observed := p.host.Clock().Now()

	// One coherent read of the mutable state, then derive outside the lock.
	// Every row still comes from the SAME consumed and draining values, so a
	// publication cannot be internally inconsistent with the ledger — which is
	// the property that mattered about holding the lock, and it is preserved by
	// copying rather than by holding.
	p.mu.Lock()
	consumed, draining := p.consumed, p.draining
	p.mu.Unlock()

	expires := observed.Add(p.host.RegistryExpiry())
	advertisements := make([]Advertisement, 0, len(p.targets))

	for _, target := range p.targets {
		agent, capabilities := target.agent, target.capabilities
		accepting := !draining && placementSupported(capabilities, p.host.Placement())

		// Available capacity is how many MORE of THIS target fit, which is why
		// the admission weight divides rather than subtracts: a Host with three
		// units left and a weight-two target has room for one, not three. A
		// target that cannot be placed here at all has room for none of it,
		// whatever the arithmetic says.
		var available uint64
		if accepting {
			// The divisor cannot be zero: capabilities is the SNAPSHOT, and
			// NewCapacityPublisher refused any snapshot whose weight was zero.
			// Reading it live is what panicked here.
			available = (p.host.Capacity() - consumed) / capabilities.AdmissionWeight
		}

		report := sessionwire.HostLinkCapacityReport{
			Version:                sessionwire.CurrentWireVersion,
			HostID:                 p.host.ID(),
			HostGeneration:         p.generation,
			AgentID:                agent,
			RuntimeCompatibilityID: string(target.compatibility),
			Placement:              p.host.Placement(),
			InternalEndpoint:       p.host.InternalEndpoint(),
			IsolationClass:         p.host.IsolationClass(),
			Accepting:              accepting,
			AvailableCapacity:      available,
			ObservedAt:             observed,
			ExpiresAt:              expires,
		}
		if err := report.Validate(); err != nil {
			return nil, &AdvertisementError{AgentID: agent, Reason: "the derived observation is not a record a reader would accept", Cause: err}
		}
		advertisements = append(advertisements, Advertisement{
			Namespace:    AdvertisementNamespace,
			RankingScope: target.rankingScope,
			StableKey:    target.stableKey,
			Rank:         rankOf(available),
			Ranked:       accepting,
			DueAt:        expires,
			Tombstone:    draining,
			Report:       report,
		})
	}
	return advertisements, nil
}

// Admit charges one session's admission weight against this Host's capacity.
//
// It is IDEMPOTENT for a repeated (key, agent): a redelivered command must not
// charge twice, and Host's command delivery is explicitly redeliverable. The
// same key under a DIFFERENT agent is refused instead, because the ledger would
// then hold a weight attributable to neither.
func (p *CapacityPublisher) Admit(key registry.Key, agent sessionwire.AgentID) error {
	// Read through the Host BEFORE taking the lock, for the reason Publish
	// does. These particular accessors are pure reads of an immutable value and
	// could not deadlock — but TestNoInjectedCallHappensUnderTheLock does not
	// distinguish them from the injected collaborators p.host also reaches, and
	// that bluntness is deliberate: telling "safe accessor" from "caller code"
	// at each site is a judgement, and a judgement is what drifts.
	placement, capacity := p.host.Placement(), p.host.Capacity()

	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, held := p.admitted[key]; held {
		if existing.agent != agent {
			return &AdmissionConflictError{Key: key, Admitted: existing.agent, Requested: agent}
		}
		return nil
	}
	if p.draining {
		return &AdmissionRefusedError{
			Code:    sessionwire.HostLinkErrorNotAdmitting,
			AgentID: agent,
			Reason:  "this Host is draining and has stopped admitting new sessions",
		}
	}
	target, registered := p.byAgent[agent]
	if !registered {
		return &AdmissionRefusedError{
			Code:    sessionwire.HostLinkErrorRuntimeUnavailable,
			AgentID: agent,
			Reason:  "this Host's Department registers no launch target for that agent",
			Cause:   &department.UnknownAgentError{AgentID: agent},
		}
	}
	capabilities := target.capabilities
	if !placementSupported(capabilities, placement) {
		return &AdmissionRefusedError{
			Code:    sessionwire.HostLinkErrorRuntimeMismatch,
			AgentID: agent,
			Reason:  "the target does not support " + string(placement) + " placement, which is the only placement this Host offers",
		}
	}
	if capabilities.AdmissionWeight > capacity-p.consumed {
		return &AdmissionRefusedError{
			Code:    sessionwire.HostLinkErrorNoCapacity,
			AgentID: agent,
			Reason: "admission weight " + strconv.FormatUint(capabilities.AdmissionWeight, 10) +
				" exceeds the " + strconv.FormatUint(capacity-p.consumed, 10) + " remaining of capacity " +
				strconv.FormatUint(capacity, 10),
		}
	}
	p.admitted[key] = admission{agent: agent, weight: capabilities.AdmissionWeight}
	p.consumed += capabilities.AdmissionWeight
	return nil
}

// Release returns one admitted session's weight to this Host's capacity, and
// reports whether this call was the one that released it.
//
// A second release of the same key, or a release of a key never admitted,
// credits nothing and reports false. Either crediting would let the Host admit
// beyond its capacity, which is the one failure the ledger exists to prevent.
func (p *CapacityPublisher) Release(key registry.Key) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, admitted := p.admitted[key]
	if !admitted {
		return false
	}
	delete(p.admitted, key)
	p.consumed -= entry.weight
	return true
}

// BeginDrain stops admission and turns every advertisement into an unranked
// tombstone. It does NOT evict: the sessions already admitted stay charged
// until their own Release, which is the difference between draining and being
// emptied.
func (p *CapacityPublisher) BeginDrain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.draining = true
}

// Draining reports whether graceful drain has begun.
func (p *CapacityPublisher) Draining() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.draining
}

// ConsumedWeight reports the admission weight currently charged.
func (p *CapacityPublisher) ConsumedWeight() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.consumed
}

// rankOf converts available capacity to a rank, SATURATING rather than wrapping.
//
// §15 ranks by available capacity and the rank is a signed 64-bit value, while
// capacity is unsigned and host.New puts no ceiling on it — so a bare
// conversion wraps. Measured at Capacity = MaxUint64 before this existed: rank
// -1, which under descending-rank paging puts the emptiest Host in the workspace
// LAST. Core accepts the report either way, because AvailableCapacity is uint64
// and it is only the ORDERING that breaks, so nothing downstream would have
// reported it.
//
// The configuration is absurd and the failure is silent, which is the pair that
// makes it worth a line. Saturating is right rather than merely safe: every
// capacity at or above MaxInt64 is more room than any deployment can use, so
// collapsing them all to "most room there is" loses no ordering anyone needs,
// while wrapping inverts it.
func rankOf(available uint64) int64 {
	if available > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(available)
}

// placementSupported reports whether a target may run under this Host's
// placement.
//
// The pooled arm asks PoolingPermitted rather than SupportsPooled. THAT CHOICE
// IS UNKILLABLE AND SAYS SO: department.Capabilities.Validate rejects
// SupportsPooled with unsafe capture outright, so inside a Department the two
// answers are identical and a probe swapping one for the other survives every
// test in this package. It is measured, not assumed. The reason to keep the
// stricter call is that department's own documentation records the rejection as
// STRICTER THAN SPEC — §11.3 says such a target is dedicated-only, a narrowing —
// so if it is ever relaxed back to the spec's reading, this arm is the one that
// still holds and nothing here has to change. The dedicated arm is not in that
// position: a probe replacing it with a constant true is killed.
func placementSupported(capabilities department.Capabilities, placement sessionwire.HostPlacement) bool {
	switch placement {
	case sessionwire.HostPlacementPooled:
		return capabilities.PoolingPermitted()
	case sessionwire.HostPlacementDedicated:
		return capabilities.SupportsDedicated
	default:
		// Unreachable through host.New, which refuses any other value. Refusing
		// is the safe default anyway: an unknown placement is one this Host
		// cannot prove a target supports.
		return false
	}
}

// stableKey identifies one Host's advertisement of one LaunchTarget.
//
// It is a DIGEST rather than the tuple itself, for two reasons that are both
// about the identifiers being opaque. A stable key is 1 to 256 bytes and each
// of an AgentID, a compatibility id and a HostID may itself be 256, so the
// concatenation does not fit. And an opaque sessionwire value may contain any
// UTF-8 including the separator, so a concatenation is ambiguous: the tuple is
// length-prefixed before hashing so ("ab", "c") and ("a", "bc") cannot collide.
func stableKey(agent sessionwire.AgentID, compatibility department.CompatibilityID, placement sessionwire.HostPlacement, hostID sessionwire.HostID) string {
	return digest(string(agent), string(compatibility), string(placement), string(hostID))
}

// rankingScope is the stable key's tuple WITHOUT the host, so the Hosts
// offering one LaunchTarget rank against each other and against nothing else.
//
// It is prefixed and hex-encoded so the result satisfies the storage name
// grammar, which a raw AgentID need not: the grammar admits only lower-case
// segments and an agent identity is an opaque UTF-8 string.
func rankingScope(agent sessionwire.AgentID, compatibility department.CompatibilityID, placement sessionwire.HostPlacement) string {
	return rankingScopePrefix + digest(string(agent), string(compatibility), string(placement))
}

// digest hashes a length-prefixed tuple, so no two distinct tuples share an
// encoding whatever bytes their members contain.
func digest(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		// The length prefix is what makes this unambiguous; the separator alone
		// is not, because an opaque identity may contain the separator.
		hash.Write([]byte(strconv.Itoa(len(part)) + ":"))
		hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// AdmissionRefusedError reports an admission this Host may not accept.
//
// Code is one of Core's HostLinkErrorCode values rather than a locally invented
// vocabulary, because §15's routing algorithm tells Factory to discard its
// binding and re-run placement on exactly these: not_admitting, no_capacity,
// runtime_unavailable, runtime_mismatch. A local code would have to be
// translated at the wire edge, and a translation table is a second place for
// the meaning to drift.
type AdmissionRefusedError struct {
	Code    sessionwire.HostLinkErrorCode
	AgentID sessionwire.AgentID
	Reason  string
	Cause   error
}

// Error describes the refusal.
func (e *AdmissionRefusedError) Error() string {
	return "host/service: admission of " + strconv.Quote(string(e.AgentID)) + " refused (" + string(e.Code) + "): " + e.Reason
}

// Unwrap returns the typed error a lower layer produced, if any.
func (e *AdmissionRefusedError) Unwrap() error { return e.Cause }

// AdmissionConflictError reports one ledger key admitted under two agents.
//
// It is a DISTINCT type from AdmissionRefusedError and deliberately carries no
// HostLinkErrorCode: it is not a placement outcome Factory should retry
// elsewhere but a Host-side bookkeeping fault, and giving it a wire code would
// invite a caller to treat it as one.
type AdmissionConflictError struct {
	Key       registry.Key
	Admitted  sessionwire.AgentID
	Requested sessionwire.AgentID
}

// Error describes the conflict.
func (e *AdmissionConflictError) Error() string {
	return "host/service: session " + strconv.Quote(string(e.Key.SessionID)) + " is already admitted as " +
		strconv.Quote(string(e.Admitted)) + " and cannot also be admitted as " + strconv.Quote(string(e.Requested))
}

// AdvertisementError reports a derived record no reader would accept.
type AdvertisementError struct {
	AgentID sessionwire.AgentID
	Reason  string
	Cause   error
}

// Error describes the underivable advertisement.
//
// The cause is optional in the TEXT even though every construction site in this
// file supplies one, because the type is exported: a caller building one
// without a cause would otherwise crash inside the logging call, which is the
// worst place to crash. The other three error types here already guard this.
func (e *AdvertisementError) Error() string {
	message := "host/service: advertisement for " + strconv.Quote(string(e.AgentID)) + ": " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the typed error a lower layer produced, so a caller reaches
// Core's *RequestValidationError and the field it names.
func (e *AdvertisementError) Unwrap() error { return e.Cause }

// InvalidCapacityOptionsError reports an option a publisher may not run with.
type InvalidCapacityOptionsError struct {
	Field  string
	Reason string
	Cause  error
}

// Error names the option and why it was refused.
func (e *InvalidCapacityOptionsError) Error() string {
	return "host/service: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Unwrap returns the typed error a lower layer produced, if any.
func (e *InvalidCapacityOptionsError) Unwrap() error { return e.Cause }
