package hostlink

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/internal/registry"
)

// ---------------------------------------------------------------------------
// Collaborators
// ---------------------------------------------------------------------------

// LinkID identifies one PHYSICAL HostLink connection, and it is the unit of
// binding ownership rather than the Factory replica behind it.
//
// Two replicas of one Factory hold two links and therefore two independent
// binding sets over the same session, which is what makes losing one of them
// survivable for the other. A replica that reconnects gets a NEW LinkID and
// must bind again: nothing about a binding survives the connection that made
// it, because a binding is a route over that connection and nothing else.
type LinkID string

// Residencies is the read-only view of the sessions this Host holds resident.
//
// IT HAS EXACTLY ONE METHOD AND THAT METHOD IS A READ, which is how "a bind
// validates current ownership and never grants it" is structural here rather
// than promised in prose. The Multiplexer holds no other handle on residency:
// it cannot insert, cannot mark, cannot touch and cannot remove, because the
// only value it is given cannot express any of those. Two derived guards hold
// that: TestProductionHoldsNoMutableRegistryHandle takes the package's
// production files from the directory and the used-symbol set from the type
// checker, and TestCollaboratorInterfacesExposeOnlyReads takes the method set
// from this type. Neither can be satisfied by a method nobody thought to
// forbid.
type Residencies interface {
	// Get returns a COPY of the registry row for a key, and whether one is
	// held. registry.Entry is a copy by that package's contract, so a caller
	// mutating the result changes nothing.
	Get(registry.Key) (registry.Entry, bool)
}

// Admission reports whether this Host is accepting new work AT ALL, which is a
// Host-wide fact and not a session's.
//
// It is separate from the residency row on purpose, and the separation is what
// keeps not_admitting and releasing from masking each other. There are now
// THREE such facts, not two, and each is read from the place that owns it:
// this interface for the Host-wide drain, Entry.State for a residency whose
// release has been published, and Entry.Accepting for a resident session whose
// admission a warm release has just closed.
//
// The third became reachable at O6.1. Until then Entry.Accepting was a strict
// function of Entry.State — Insert set resident/true, MarkReleasing and
// BeginTeardown both set false — so a bind reading both fields would have had
// one decide every reachable case and the other decide none, which is an
// identical refusal standing in for two checks; the field consequently had no
// reader here. registry.StopAdmitting is what separated them, and Bind reads
// State FIRST so that the releasing ground is not swallowed by the admission
// one, which MarkReleasing also satisfies.
type Admission interface {
	// Draining reports that this Host has begun a drain and is publishing
	// accepting=false, so a new binding must not be established.
	Draining() bool
}

// CommandConsumer is the durable inbox consumer for one session, narrowed to
// the single method a HostLink command delivery may reach.
//
// Hint takes a CommandID AND NOTHING ELSE, which is the whole of §3's rule
// expressed as a type: there is no parameter a payload could ride in even if a
// future caller wanted to pass one. commands.Consumer satisfies it exactly;
// the conformance assertion lives in the test so this package does not name
// internal/commands in production.
type CommandConsumer interface {
	// Hint wakes consumption of an already accepted command. It is a wake and
	// not a delivery, so repeating it is harmless.
	Hint(sessionwire.CommandID)
}

// CommandConsumers resolves the durable inbox consumer that owns a session.
type CommandConsumers interface {
	// ConsumerFor returns the consumer for a resident session, and whether one
	// is running.
	ConsumerFor(registry.Key) (CommandConsumer, bool)
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

// Refusal is why a bind, unbind or command delivery was declined.
//
// IT IS FINER THAN sessionwire.HostLinkErrorCode AND DELIBERATELY SO. Core
// publishes six refusal classes and this package refuses on twelve grounds, so
// five of them answer runtime_unavailable on the wire: a foreign tenant, an
// unheld session, a foreign Host, a stale Host generation and an unbound
// channel are indistinguishable to a Factory ON PURPOSE, because telling them
// apart is telling a caller whether somebody else's session exists. The two
// capacity grounds likewise share no_capacity. What must NOT be
// indistinguishable is which check fired: that is the difference between a
// misconfigured tenant, a stale placement route and a Host that has restarted,
// which are three different repairs. The wire stays narrow and the local value
// stays exact, and an identical wire class is exactly how a refusal path
// acquires no test of its own.
type Refusal string

const (
	// RefusalForeignTenant reports a bind naming a tenant other than the one
	// this link authenticated as. Since H8 that is the whole of the rule: the
	// Host itself serves whatever tenants its isolation class permits.
	RefusalForeignTenant Refusal = "foreign_tenant"

	// RefusalUnknownSession reports a session this Host does not hold.
	RefusalUnknownSession Refusal = "unknown_session"

	// RefusalForeignHost reports a bind addressed to a different HostID, which
	// means Factory routed from a registry row that is not this Host's.
	RefusalForeignHost Refusal = "foreign_host"

	// RefusalStaleHostGeneration reports the right HostID at an earlier
	// incarnation. The route survived a restart the residency did not.
	RefusalStaleHostGeneration Refusal = "stale_host_generation"

	// RefusalEpochMismatch reports a lease epoch other than the one this
	// residency was established under.
	RefusalEpochMismatch Refusal = "epoch_mismatch"

	// RefusalRuntimeMismatch reports a runtime compatibility ID other than the
	// build actually launched under this residency.
	RefusalRuntimeMismatch Refusal = "runtime_mismatch"

	// RefusalReleasing reports a residency already on its way out. The session
	// stays durable and resumable; this route is about to disappear.
	RefusalReleasing Refusal = "releasing"

	// RefusalHostNotAdmitting reports a Host that has begun draining. The
	// session is still resident and still accepting through existing
	// bindings; what is refused is establishing a new one.
	RefusalHostNotAdmitting Refusal = "host_not_admitting"

	// RefusalSessionNotAdmitting reports a residency that is still RESIDENT and
	// has stopped admitting new work. It is the third fact, added by O6.1, and
	// it is not either of its neighbours: RefusalHostNotAdmitting is about this
	// whole Host and RefusalReleasing is about a residency whose state has
	// already moved. This one is a warm release that has closed admission and
	// not yet written its durable `releasing` observation.
	//
	// It publishes not_admitting, which is the class §9.3 names for a command
	// racing release, and which it shares with the Host-wide ground. The
	// repairs differ: a Factory re-places this session and leaves the Host
	// alone, where a draining Host must be avoided entirely.
	//
	// IT IS READ ON BOTH PATHS. Bind refuses a NEW binding, and Deliver refuses
	// a command over a RETAINED one — which §9.3 makes the likely path, since a
	// binding may remain through the warm TTL. The first round of O6.1 shipped
	// only the bind half, and a retained binding went on delivering into a
	// session that had stopped admitting.
	RefusalSessionNotAdmitting Refusal = "session_not_admitting"

	// RefusalResidencyReleased reports a delivery over a RETAINED binding whose
	// session this Host no longer holds resident.
	//
	// IT IS SEPARATE FROM RefusalUnknownSession, which answers the same
	// question on the bind path, and the difference is disclosure. A bind is
	// refused with runtime_unavailable precisely so that it cannot tell a
	// caller whether somebody else's session exists; a DELIVERY arrives over a
	// binding this Host itself granted, so the caller has already proved it
	// knew, and there is nothing left to withhold. That is what lets this one
	// publish the class §9.3 names for a command racing release —
	// not_admitting, on which a Factory invalidates the binding and re-runs
	// placement — instead of the deliberately uninformative one.
	RefusalResidencyReleased Refusal = "residency_released"

	// RefusalNoLinkCapacity reports one link's binding budget already spent.
	RefusalNoLinkCapacity Refusal = "no_link_capacity"

	// RefusalNoHostCapacity reports the Host-wide binding budget already spent.
	//
	// It is a separate value from the per-link one because they publish the
	// same Core class and the repairs differ: one Factory replica holding too
	// many routes is that replica's bug, and a Host at its ceiling is a
	// placement decision. One code for both would make each an equally good
	// explanation of the other.
	RefusalNoHostCapacity Refusal = "no_host_capacity"

	// RefusalNotBound reports a command delivery on a channel THIS LINK does
	// not hold a binding for, whether or not some other link does.
	RefusalNotBound Refusal = "not_bound"

	// RefusalMalformedRequest reports a body Core's own decoder refused. It is
	// the one refusal with no wire class, because Core's six classes are all
	// statements about ownership and a malformed body is a statement about the
	// caller.
	RefusalMalformedRequest Refusal = "malformed_request"

	// RefusalWrongDrainScope reports a session-scoped drain this Host cannot
	// answer: either it holds no fixed session and is drained as a whole, or
	// it holds one and the request names a different session.
	//
	// The two grounds share a value ON PURPOSE, unlike every other pair here.
	// Telling them apart would tell a caller whether this Host is dedicated
	// and to which session, which is the same disclosure the unheld-session
	// refusal exists to prevent. The Reason string carries the difference for
	// the operator; the value does not.
	RefusalWrongDrainScope Refusal = "wrong_drain_scope"

	// RefusalDrainUnsupported reports a Host composed without a drain state
	// machine. It is a DEPLOYMENT defect: this Host can never be drained
	// gracefully, and no retry will change that.
	RefusalDrainUnsupported Refusal = "drain_unsupported"

	// RefusalDrainUnavailable reports a drain state machine that could not
	// begin, or that answered with a status Core refuses to publish. It is a
	// RUNNING-HOST condition — a process already shutting down is the ordinary
	// case — and it is separate from RefusalDrainUnsupported because a retry
	// or a different Host is the repair for one and a redeploy is the repair
	// for the other.
	RefusalDrainUnavailable Refusal = "drain_unavailable"

	// RefusalNoDrainInProgress reports a status observation on a Host that has
	// begun no drain covering that scope. It is NOT a drained observation, and
	// the distinction is what stops a Factory whose drain request never
	// arrived from reading silence as completion.
	RefusalNoDrainInProgress Refusal = "no_drain_in_progress"
)

// BindError reports a declined bind, unbind or command delivery.
type BindError struct {
	// Refusal is which check fired, and is the field an operator reads.
	Refusal Refusal

	// Key is the session the request named, as far as it was parsed.
	Key registry.Key

	// Reason is human-readable detail. It never carries a credential, a
	// payload or another tenant's identifiers.
	Reason string

	// Cause is the typed error a decoder produced, if any.
	Cause error

	// wire is the narrow class Factory branches on, and is empty exactly when
	// the refusal has none. It is unexported so a caller must go through
	// HostLinkError and cannot read the empty value as a valid code.
	wire sessionwire.HostLinkErrorCode

	// currentLeaseEpoch and runtimeCompatibilityID are the safe details Core
	// attaches to two of its classes, and are set only for those.
	currentLeaseEpoch      uint64
	runtimeCompatibilityID string
}

func (e *BindError) Error() string {
	message := "hostlink: session " + strconv.Quote(string(e.Key.SessionID)) +
		" was refused (" + string(e.Refusal) + "): " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the typed error a decoder produced, if any.
func (e *BindError) Unwrap() error { return e.Cause }

// HostLinkError returns the Core record this refusal is published as, and
// whether it has one at all.
//
// It is a two-value accessor for the reason residency.AttachError.HostLinkCode
// is: the absent code is a real state, and a bare accessor would hand a caller
// a record Core's own Validate refuses.
func (e *BindError) HostLinkError() (sessionwire.HostLinkError, bool) {
	if e.wire == "" {
		return sessionwire.HostLinkError{}, false
	}
	return sessionwire.HostLinkError{
		Code:                   e.wire,
		CurrentLeaseEpoch:      e.currentLeaseEpoch,
		RuntimeCompatibilityID: e.runtimeCompatibilityID,
	}, true
}

// ---------------------------------------------------------------------------
// Multiplexer
// ---------------------------------------------------------------------------

// Binding is a COPY of one route: one link, one session, one channel.
type Binding struct {
	Link    LinkID
	Key     registry.Key
	Channel string

	// LeaseEpoch is the epoch the bind VALIDATED against. It is a record of
	// what was true at bind time and confers nothing; the lease itself is the
	// only thing that grants, and this package never touches it.
	LeaseEpoch uint64

	// IdempotencyKey is the most recent retry-stable key the Factory supplied.
	//
	// HOSTLINK DOES NOT DE-DUPLICATE ON IT, and that is a decision rather than
	// an omission: a repeated bind RE-VALIDATES current ownership instead of
	// replaying a cached acceptance, so a retry that arrives after the epoch
	// moved is refused rather than confirmed. Replaying would make the bind a
	// statement about the past, which is the one thing a routing validation
	// must not be. The key is recorded so an operator can correlate a retry
	// storm with one Factory attempt.
	IdempotencyKey string
}

// MultiplexerOptions configures a Multiplexer.
type MultiplexerOptions struct {
	// TenantID is the tenant every link on this Multiplexer authenticated as.
	//
	// IT IS THE LINK'S TENANT AND NO LONGER THE HOST'S. Human gate H8, answered
	// 2026-09-04, dropped the fixed tenant from host.Options: a pooled Host
	// advertising cross_tenant_isolated admits several tenants, and one
	// advertising tenant_exclusive is exclusive by PLACEMENT rather than by
	// construction. A composition therefore supplies this from the
	// authenticated service identity of the connection, not from the Host, and
	// a Host serving several tenants runs one Multiplexer per authenticated
	// tenant. The refusal below still says "this link is authenticated for
	// another tenant", which was always the accurate sentence.
	TenantID sessionwire.TenantID

	// HostID and HostGeneration are this Host process's identity and
	// incarnation, compared against every bind.
	HostID         sessionwire.HostID
	HostGeneration uint64

	Residencies Residencies
	Admission   Admission
	Consumers   CommandConsumers

	// DrainStarter and DrainObserver are the drain state machine this Host's
	// HostLink may begin and observe. They are OPTIONAL AND PAIRED: supplying
	// one without the other is refused, because a Host that can be told to
	// drain and cannot be asked how it is going, or the reverse, is a Factory
	// bug discovered in production instead of at construction.
	//
	// Their ABSENCE IS A REFUSAL AND NOT A DEFAULT. A Multiplexer built
	// without them answers both drain RPCs with RefusalDrainUnsupported, so a
	// Host with no drain to run cannot be made to look as though it started
	// one. That is the same shape as Config.Multiplexer's own absence.
	DrainStarter  DrainStarter
	DrainObserver DrainObserver

	// FixedSessionID is the sole session a DEDICATED Host holds, and its zero
	// value means this Host is pooled.
	//
	// It is read by the drain path only. It is NOT a second admission rule:
	// nothing on the bind path consults it, and the dedicated-attach refusal
	// lives in the residency manager where the durable state is. Putting a
	// second copy of "which sessions may this Host hold" on the routing table
	// would be two answers to one question.
	FixedSessionID sessionwire.SessionID

	// MaxBindingsPerLink and MaxBindings bound what one link and the whole
	// Host may hold. A binding is not free — it is a subscription and an
	// enduring-event queue — so an unbounded one is an unbounded Host.
	MaxBindingsPerLink int
	MaxBindings        int
}

// InvalidMultiplexerOptionsError reports an option a Multiplexer may not run
// with.
type InvalidMultiplexerOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidMultiplexerOptionsError) Error() string {
	return "hostlink: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Multiplexer holds every Factory route this Host currently serves, keyed by
// physical link, and is safe for concurrent use.
type Multiplexer struct {
	tenant      sessionwire.TenantID
	hostID      sessionwire.HostID
	generation  uint64
	residencies Residencies
	admission   Admission
	consumers   CommandConsumers

	drainStarter  DrainStarter
	drainObserver DrainObserver
	fixedSession  sessionwire.SessionID

	perLink int
	total   int

	mu    sync.Mutex
	links map[LinkID]map[string]Binding
	count int
}

// NewMultiplexer validates the options and returns an empty Multiplexer.
func NewMultiplexer(options MultiplexerOptions) (*Multiplexer, error) {
	if err := options.TenantID.Validate(); err != nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "TenantID", Reason: "must be a valid Core tenant ID"}
	}
	if err := options.HostID.Validate(); err != nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "HostID", Reason: "must be a valid Core host ID"}
	}
	if options.HostGeneration == 0 {
		return nil, &InvalidMultiplexerOptionsError{Field: "HostGeneration", Reason: "must be non-zero; Core refuses every HostLink record carrying a zero generation"}
	}
	if options.Residencies == nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "Residencies", Reason: "is required; a bind that cannot read current ownership cannot validate one"}
	}
	if options.Admission == nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "Admission", Reason: "is required; a draining Host must refuse new bindings"}
	}
	if options.Consumers == nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "Consumers", Reason: "is required; a command delivery with nothing to wake is not a delivery"}
	}
	if options.MaxBindingsPerLink <= 0 {
		return nil, &InvalidMultiplexerOptionsError{Field: "MaxBindingsPerLink", Reason: "must be positive"}
	}
	if options.MaxBindings <= 0 {
		return nil, &InvalidMultiplexerOptionsError{Field: "MaxBindings", Reason: "must be positive"}
	}
	if options.DrainStarter != nil && options.DrainObserver == nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "DrainObserver", Reason: "is required alongside DrainStarter; a drain nobody can observe is one Factory cannot wait for"}
	}
	if options.DrainObserver != nil && options.DrainStarter == nil {
		return nil, &InvalidMultiplexerOptionsError{Field: "DrainStarter", Reason: "is required alongside DrainObserver; a drain nobody can begin has nothing to observe"}
	}
	if options.FixedSessionID != "" {
		if err := options.FixedSessionID.Validate(); err != nil {
			return nil, &InvalidMultiplexerOptionsError{Field: "FixedSessionID", Reason: "must be a valid Core session ID; a dedicated Host cannot hold a session Core refuses"}
		}
	}
	return &Multiplexer{
		tenant:        options.TenantID,
		hostID:        options.HostID,
		generation:    options.HostGeneration,
		residencies:   options.Residencies,
		admission:     options.Admission,
		consumers:     options.Consumers,
		drainStarter:  options.DrainStarter,
		drainObserver: options.DrainObserver,
		fixedSession:  options.FixedSessionID,
		perLink:       options.MaxBindingsPerLink,
		total:         options.MaxBindings,
	}, nil
}

// Bind validates the ownership tuple a Factory presented and, if every part of
// it is currently true, records a route from this link to that session.
//
// IT GRANTS NOTHING. Every value it consults is read through Residencies, which
// has no mutator, and the only state it writes is this package's own routing
// table. A session that is not resident is not made resident by binding to it;
// an epoch that does not match is not adopted; a runtime that does not match is
// not relaunched. The whole method is a comparison followed by a map write.
func (m *Multiplexer) Bind(link LinkID, request sessionwire.HostLinkBindRequest) (Binding, error) {
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}
	if err := request.Validate(); err != nil {
		return Binding{}, &BindError{Refusal: RefusalMalformedRequest, Key: key, Reason: "the bind request is not a valid Core record", Cause: err}
	}
	if request.TenantID != m.tenant {
		// Returned BEFORE any residency read, and that ordering is the
		// mechanism rather than an optimization: this Host never looks up
		// another tenant's session at all, so no timing or state difference
		// can report whether one exists.
		return Binding{}, &BindError{
			Refusal: RefusalForeignTenant,
			Key:     key,
			Reason:  "this link is authenticated for another tenant",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if m.admission.Draining() {
		return Binding{}, &BindError{
			Refusal: RefusalHostNotAdmitting,
			Key:     key,
			Reason:  "this Host has begun draining and is not establishing new bindings",
			wire:    sessionwire.HostLinkErrorNotAdmitting,
		}
	}
	entry, held := m.residencies.Get(key)
	if !held {
		return Binding{}, &BindError{
			Refusal: RefusalUnknownSession,
			Key:     key,
			Reason:  "this Host holds no residency for the session",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostID != m.hostID {
		return Binding{}, &BindError{
			Refusal: RefusalForeignHost,
			Key:     key,
			Reason:  "the bind is addressed to another Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostGeneration != m.generation {
		return Binding{}, &BindError{
			Refusal: RefusalStaleHostGeneration,
			Key:     key,
			Reason:  "the bind names an earlier incarnation of this Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.LeaseEpoch != entry.LeaseEpoch {
		return Binding{}, &BindError{
			Refusal:           RefusalEpochMismatch,
			Key:               key,
			Reason:            "the bind names a lease epoch this residency was not established under",
			wire:              sessionwire.HostLinkErrorEpochMismatch,
			currentLeaseEpoch: entry.LeaseEpoch,
		}
	}
	if request.RuntimeCompatibilityID != string(entry.CompatibilityID) {
		return Binding{}, &BindError{
			Refusal:                RefusalRuntimeMismatch,
			Key:                    key,
			Reason:                 "the bind names a runtime build this residency did not launch",
			wire:                   sessionwire.HostLinkErrorRuntimeMismatch,
			runtimeCompatibilityID: string(entry.CompatibilityID),
		}
	}
	if entry.State != registry.StateResident {
		return Binding{}, &BindError{
			Refusal: RefusalReleasing,
			Key:     key,
			Reason:  "this residency is releasing and its route is about to disappear",
			wire:    sessionwire.HostLinkErrorReleasing,
		}
	}
	// STATE IS CHECKED FIRST AND THAT ORDER IS THE MECHANISM. MarkReleasing
	// sets State AND Accepting, so a releasing residency satisfies this
	// predicate too; reading admission first would answer not_admitting for
	// every releasing bind and leave RefusalReleasing unreachable through this
	// path. Reaching here means the residency is resident and has stopped
	// admitting on its own, which is the warm release's first step and nothing
	// else.
	if !entry.Accepting {
		return Binding{}, &BindError{
			Refusal: RefusalSessionNotAdmitting,
			Key:     key,
			Reason:  "this residency has stopped admitting new work and is being released",
			wire:    sessionwire.HostLinkErrorNotAdmitting,
		}
	}
	return m.record(link, key, request.LeaseEpoch, request.IdempotencyKey)
}

// record installs the validated route, or refuses it for want of budget.
//
// The capacity check is here rather than in Bind because it must be the LAST
// thing a bind does: refusing an invalid request for want of capacity would
// tell a Factory to retry elsewhere a bind that is wrong everywhere.
func (m *Multiplexer) record(link LinkID, key registry.Key, epoch uint64, idempotencyKey string) (Binding, error) {
	channel := ChannelFor(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	bindings := m.links[link]
	if _, bound := bindings[channel]; !bound {
		if len(bindings) >= m.perLink {
			return Binding{}, &BindError{
				Refusal: RefusalNoLinkCapacity,
				Key:     key,
				Reason:  "this link already holds its maximum number of bindings",
				wire:    sessionwire.HostLinkErrorNoCapacity,
			}
		}
		if m.count >= m.total {
			return Binding{}, &BindError{
				Refusal: RefusalNoHostCapacity,
				Key:     key,
				Reason:  "this Host already holds its maximum number of bindings",
				wire:    sessionwire.HostLinkErrorNoCapacity,
			}
		}
		if bindings == nil {
			bindings = map[string]Binding{}
			if m.links == nil {
				m.links = map[LinkID]map[string]Binding{}
			}
			m.links[link] = bindings
		}
		m.count++
	}
	binding := Binding{Link: link, Key: key, Channel: channel, LeaseEpoch: epoch, IdempotencyKey: idempotencyKey}
	bindings[channel] = binding
	return binding, nil
}

// Unbind removes one route. Repeating it is safe and repeating it for a route
// that was never held is safe, because an unbind is a statement about what this
// link no longer wants and never about what the Host owns.
//
// It validates the tenant and the addressed Host for the same reason Bind does
// — an unbind is still authenticated control-plane input — but it deliberately
// does NOT validate the lease epoch or the residency: a Factory that has just
// been told its epoch is stale must still be able to drop its route.
func (m *Multiplexer) Unbind(link LinkID, request sessionwire.HostLinkUnbindRequest) error {
	key := registry.Key{TenantID: request.TenantID, SessionID: request.SessionID}
	if err := request.Validate(); err != nil {
		return &BindError{Refusal: RefusalMalformedRequest, Key: key, Reason: "the unbind request is not a valid Core record", Cause: err}
	}
	if request.TenantID != m.tenant {
		return &BindError{
			Refusal: RefusalForeignTenant,
			Key:     key,
			Reason:  "this link is authenticated for another tenant",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if request.HostID != m.hostID {
		return &BindError{
			Refusal: RefusalForeignHost,
			Key:     key,
			Reason:  "the unbind is addressed to another Host",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	m.remove(link, ChannelFor(key))
	return nil
}

// remove drops one channel from one link's set, and the link when it empties.
func (m *Multiplexer) remove(link LinkID, channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bindings, held := m.links[link]
	if !held {
		return
	}
	if _, bound := bindings[channel]; !bound {
		return
	}
	delete(bindings, channel)
	m.count--
	if len(bindings) == 0 {
		delete(m.links, link)
	}
}

// Deliver wakes the durable consumer for the session THIS LINK is bound to on
// the named channel.
//
// The channel is never parsed. It is looked up in the set of channels this Host
// itself minted for this link, so a well-formed channel naming a session that
// is resident and bound on some OTHER link is refused here exactly as a
// nonsense string is: the route is the authority, not the shape of its name.
//
// The delivery record carries a CommandID and nothing else, and repeating one
// is harmless — Hint is a wake, so the second one either finds the pass already
// running or starts another that finds the durable record already terminal.
//
// HOLDING THE ROUTE IS NECESSARY AND NOT SUFFICIENT. The residency is re-read
// here on every delivery, because §9.3 lets a session binding outlive the
// admission it was granted under: "A session binding may remain through the
// warm TTL". A binding is therefore a statement about routing and never a
// standing permission to wake a runtime.
func (m *Multiplexer) Deliver(link LinkID, channel string, delivery sessionwire.HostLinkCommandDelivery) error {
	binding, bound := m.binding(link, channel)
	if !bound {
		return &BindError{
			Refusal: RefusalNotBound,
			Reason:  "this link holds no binding for the channel",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	if err := delivery.Validate(); err != nil {
		return &BindError{Refusal: RefusalMalformedRequest, Key: binding.Key, Reason: "the command delivery is not a valid Core record", Cause: err}
	}
	// THE RESIDENCY IS RE-READ ON EVERY DELIVERY, not once at bind time, and
	// that is §9.3's "commands racing release receive typed not_admitting"
	// landing on the arm a racing command actually takes. §9.3 also says "A
	// session binding may remain through the warm TTL", so the binding a
	// Factory holds outlives the residency's admission by design; checking only
	// at Bind left the RETAINED-binding path — the likely one — delivering into
	// a session that had stopped admitting, was releasing, or was gone.
	//
	// THE EPOCH FENCE DOES NOT COVER THIS. Warm release steps 2 through 4 leave
	// the same Host holding the same lease epoch, so the binding is exactly
	// current and nothing about it is stale except this read.
	//
	// The order is Bind's, for Bind's reason: MarkReleasing sets State AND
	// Accepting, so reading admission first would answer not_admitting for
	// every releasing delivery and leave RefusalReleasing unreachable here.
	entry, resident := m.residencies.Get(binding.Key)
	switch {
	case !resident:
		return &BindError{
			Refusal: RefusalResidencyReleased,
			Key:     binding.Key,
			Reason:  "this Host no longer holds the residency this binding names",
			wire:    sessionwire.HostLinkErrorNotAdmitting,
		}
	case entry.State != registry.StateResident:
		return &BindError{
			Refusal: RefusalReleasing,
			Key:     binding.Key,
			Reason:  "this residency is releasing and is no longer taking commands",
			wire:    sessionwire.HostLinkErrorReleasing,
		}
	case !entry.Accepting:
		return &BindError{
			Refusal: RefusalSessionNotAdmitting,
			Key:     binding.Key,
			Reason:  "this residency has stopped admitting new work and is being released",
			wire:    sessionwire.HostLinkErrorNotAdmitting,
		}
	}
	consumer, running := m.consumers.ConsumerFor(binding.Key)
	if !running {
		return &BindError{
			Refusal: RefusalUnknownSession,
			Key:     binding.Key,
			Reason:  "no durable inbox consumer is running for the session",
			wire:    sessionwire.HostLinkErrorRuntimeUnavailable,
		}
	}
	consumer.Hint(delivery.CommandID)
	return nil
}

// MaySubscribe reports whether this link may subscribe to a channel, which it
// may only when it holds the binding that minted it.
func (m *Multiplexer) MaySubscribe(link LinkID, channel string) bool {
	_, bound := m.binding(link, channel)
	return bound
}

// binding returns the route a link holds on a channel, by exact lookup.
func (m *Multiplexer) binding(link LinkID, channel string) (Binding, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	binding, bound := m.links[link][channel]
	return binding, bound
}

// CloseLink drops every route one link held, and returns them.
//
// IT TOUCHES NOTHING ELSE. Not the runtime, which this package cannot reach;
// not the residency, which it can only read; and not another link's routes,
// including another replica's route over the same session. A link is a route
// and losing it loses routes.
func (m *Multiplexer) CloseLink(link LinkID) []Binding {
	m.mu.Lock()
	defer m.mu.Unlock()
	bindings, held := m.links[link]
	if !held {
		return nil
	}
	delete(m.links, link)
	m.count -= len(bindings)
	return sortedBindings(bindings)
}

// InvalidateSession drops every link's route to ONE session and returns them,
// ordered by link.
//
// IT IS THE SESSION-SCOPED SIBLING OF CloseLink AND TOUCHES AS LITTLE. The
// residency is not released, the runtime is not stopped, no other session's
// route on any of these links is affected, and the physical connections stay
// up: what disappears is the routing this Host can no longer serve a live tail
// over, which is exactly what obliges a Factory to reset durably rather than
// keep advancing a cursor it is no longer being fed.
//
// THE SESSION SCOPE IS WHY IT IS NOT CloseLink. A tail lost for one session
// says nothing about the other sessions a Factory replica routes over the same
// physical link, and closing the link would invalidate all of them — turning a
// one-session durable reset into a whole-replica one. The transport has its own
// per-connection failure (a slow client's queue), and that one IS link-scoped
// because it is the connection that failed; this one is not.
func (m *Multiplexer) InvalidateSession(key registry.Key) []Binding {
	channel := ChannelFor(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	var dropped []Binding
	for link, bindings := range m.links {
		binding, bound := bindings[channel]
		if !bound {
			continue
		}
		dropped = append(dropped, binding)
		delete(bindings, channel)
		m.count--
		if len(bindings) == 0 {
			delete(m.links, link)
		}
	}
	sort.Slice(dropped, func(i, j int) bool { return dropped[i].Link < dropped[j].Link })
	return dropped
}

// LinkBindings returns the routes one link holds, ordered by channel.
func (m *Multiplexer) LinkBindings(link LinkID) []Binding {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sortedBindings(m.links[link])
}

// Replicas returns every link bound to one session, ordered, which is how many
// Factory replicas currently route to it.
func (m *Multiplexer) Replicas(key registry.Key) []LinkID {
	channel := ChannelFor(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	var links []LinkID
	for link, bindings := range m.links {
		if _, bound := bindings[channel]; bound {
			links = append(links, link)
		}
	}
	sort.Slice(links, func(i, j int) bool { return links[i] < links[j] })
	return links
}

// Len reports how many routes this Host holds across every link.
func (m *Multiplexer) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

func sortedBindings(bindings map[string]Binding) []Binding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]Binding, 0, len(bindings))
	for _, binding := range bindings {
		out = append(out, binding)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Channel < out[j].Channel })
	return out
}

// ChannelPrefix is the leading segment of every channel this package mints.
//
// Every reserved RPC method name is a constant that does not begin with it, so
// dispatch can tell a method from a channel without parsing either.
const ChannelPrefix = "hostlink.v1."

// ChannelFor returns the channel a session's publications travel on.
//
// The identifiers are base64url-encoded WITHOUT PADDING, and the property that
// matters is INJECTIVITY: two distinct keys cannot mint one channel however
// many separators their opaque identifiers contain. Core mints no SessionID —
// they are client-supplied strings — so one containing a '.' is an ordinary
// event, and a channel built by plain concatenation would let two tenants
// collide onto one route. The encoding is what makes that unrepresentable
// rather than unlikely. Its alphabet is A-Za-z0-9-_ and the prefix, so a
// channel also needs no escaping wherever it is later logged or compared.
//
// This says nothing about characters centrifuge@v0.38.0 treats specially,
// because it treats none specially: its own channel handling is exact string
// comparison and map keys, and the ':' and '#' conventions belong to
// Centrifugo, which this Host does not run. The one channel rule the library
// does enforce is a length ceiling, and MaxChannelBytes is what answers it.
func ChannelFor(key registry.Key) string {
	encoding := base64.RawURLEncoding
	return ChannelPrefix + encoding.EncodeToString([]byte(key.TenantID)) + "." + encoding.EncodeToString([]byte(key.SessionID))
}

// MaxChannelBytes is the longest channel ChannelFor can mint.
//
// It is DERIVED from the encoding and Core's own identity ceiling rather than
// measured from a sample, so a Core release that widens sessionwire.MaxIDBytes
// widens this with it. Centrifuge defaults ChannelMaxLength to 255
// (centrifuge@v0.38.0 node.go:138-140) and refuses a longer channel with
// ErrorBadRequest at subscribe (client.go:2804-2807), which two maximum-length
// Core identifiers exceed comfortably, so NewCentrifugeServer raises the node's
// ceiling to this value instead of leaving a legal session unroutable.
var MaxChannelBytes = len(ChannelPrefix) + 1 + 2*base64.RawURLEncoding.EncodedLen(sessionwire.MaxIDBytes)

// ---------------------------------------------------------------------------
// Transport dispatch
// ---------------------------------------------------------------------------

// MethodBind and MethodUnbind are two of the four reserved RPC method names;
// MethodDrain and MethodDrainStatus in drain.go are the others. Any other
// method is treated as a channel and resolved by exact lookup against the
// routes this link holds, so a command delivery names its binding and carries
// only a CommandID in its body.
const (
	MethodBind   = "hostlink.bind"
	MethodUnbind = "hostlink.unbind"
)

// errUnroutableRPC is returned to Centrifuge when a refusal has no Core class,
// which is the malformed-body case and nothing else.
var errUnroutableRPC = errors.New("hostlink: the RPC body is not a valid Core record")

// dispatch answers one RPC on one link.
//
// An ACCEPTED request returns an EMPTY body and a REFUSED one returns the Core
// HostLinkError as its body, which is the whole reply contract: a Factory needs
// no out-of-band signal to tell them apart because an empty body cannot be a
// valid HostLinkError — Core's own Validate refuses an absent code.
//
// THE TWO DRAIN METHODS ARE THE EXCEPTION and leave through their own return,
// because an acknowledgement carries a generation and so cannot be empty. See
// dispatchDrain for what keeps their two bodies apart.
func (m *Multiplexer) dispatch(link LinkID, method string, data []byte) ([]byte, error) {
	var err error
	switch method {
	case MethodDrain, MethodDrainStatus:
		return m.dispatchDrain(method, data)
	case MethodBind:
		var request sessionwire.HostLinkBindRequest
		if decodeErr := json.Unmarshal(data, &request); decodeErr != nil {
			err = &BindError{Refusal: RefusalMalformedRequest, Reason: "the bind body is not a valid Core record", Cause: decodeErr}
			break
		}
		_, err = m.Bind(link, request)
	case MethodUnbind:
		var request sessionwire.HostLinkUnbindRequest
		if decodeErr := json.Unmarshal(data, &request); decodeErr != nil {
			err = &BindError{Refusal: RefusalMalformedRequest, Reason: "the unbind body is not a valid Core record", Cause: decodeErr}
			break
		}
		err = m.Unbind(link, request)
	default:
		var delivery sessionwire.HostLinkCommandDelivery
		if decodeErr := json.Unmarshal(data, &delivery); decodeErr != nil {
			// The binding check still runs first, so an unbound link learns
			// nothing about the body it sent and a bound one learns nothing
			// about the sessions it did not bind.
			if _, bound := m.binding(link, method); !bound {
				err = &BindError{Refusal: RefusalNotBound, Reason: "this link holds no binding for the channel", wire: sessionwire.HostLinkErrorRuntimeUnavailable}
				break
			}
			err = &BindError{Refusal: RefusalMalformedRequest, Reason: "the command body is not a valid Core record", Cause: decodeErr}
			break
		}
		err = m.Deliver(link, method, delivery)
	}
	if err == nil {
		return nil, nil
	}
	var refusal *BindError
	if !errors.As(err, &refusal) {
		return nil, err
	}
	wire, published := refusal.HostLinkError()
	if !published {
		return nil, errUnroutableRPC
	}
	body, marshalErr := json.Marshal(wire)
	if marshalErr != nil {
		return nil, marshalErr
	}
	return body, nil
}

// install attaches this Multiplexer's handlers to one physical connection.
//
// The LinkID is the Centrifuge client ID, which is minted per connection, so a
// reconnect is a different link and inherits nothing. It takes a Centrifuge
// type and is unexported for that reason; nothing here crosses the package
// boundary.
func (m *Multiplexer) install(client *centrifuge.Client) {
	link := LinkID(client.ID())
	client.OnRPC(func(event centrifuge.RPCEvent, callback centrifuge.RPCCallback) {
		body, err := m.dispatch(link, event.Method, event.Data)
		if err != nil {
			callback(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
			return
		}
		callback(centrifuge.RPCReply{Data: body}, nil)
	})
	client.OnSubscribe(func(event centrifuge.SubscribeEvent, callback centrifuge.SubscribeCallback) {
		if !m.MaySubscribe(link, event.Channel) {
			callback(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
			return
		}
		callback(centrifuge.SubscribeReply{}, nil)
	})
	client.OnDisconnect(func(centrifuge.DisconnectEvent) {
		m.CloseLink(link)
	})
}
