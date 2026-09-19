package host

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/compose"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// ---------------------------------------------------------------------------
// The composition surface (B1)
// ---------------------------------------------------------------------------
//
// Until v0.2.0 a Host could be composed only by this module's own binary:
// cmd/host reached internal/compose, and a product that wanted a Host of its
// own had to vendor that file. What follows is the exported composition — a
// struct of collaborators, one constructor that opens the store itself, and a
// Service with the runtime verbs a caller needs — over internal/compose, which
// stays the composition root and decides no policy of its own either.
//
// IT IS A STRUCT OF COLLABORATORS AND NOT AN INTERFACE, so a collaborator
// added in a later minor is an added field rather than a new method every
// implementation must grow.

// EvidenceKey names the journal store that serves one tenant's sessions under
// one storage binding. The tenant is half of the key on purpose: a harness
// Store files ONE tenant's sessions and refuses a request naming another, so a
// table keyed on the binding alone could route tenant-b's settlement into
// tenant-a's journal — refused correctly, and only after the dispatch.
type EvidenceKey struct {
	TenantID         sessionwire.TenantID
	StorageBindingID string
}

// Workspaces materializes and releases session workspaces. It is
// WorkspaceProvider plus the release half the composition's warm release and
// drain need; a value satisfying it satisfies WorkspaceProvider too.
type Workspaces interface {
	EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error)
	// ReleaseWorkspace drops the local materialization. It never deletes
	// durable state.
	ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error
}

// NamespaceLayout answers the durable object prefix a session's runtime writes
// under. It is a REQUIRED collaborator: the released store publishes no layout
// accessor and the residency manager refuses a hydration with no namespace in
// both modes, so a Host composed without one could never attach a session.
type NamespaceLayout func(sessionwire.TenantID, sessionwire.SessionID) string

// RigSessionIDs answers Harness's own identity for a session whose durable
// record carries NO BINDING (a legacy record), and is consulted for nothing
// else.
//
// Deprecated: As of v0.3.0 the Harness identity of every bound session is read
// from its immutable durable binding (SessionBinding.RuntimeSessionID), which
// is where Factory records the identity it derived at create. That value is
// the authority — settlement reads the runtime's journal by it — and a second
// answer from a composition could only agree with it or name a journal
// nothing else reads. The collaborator is optional and is kept only so a
// v0.2.x composition still compiles; no session this workspace ships can be
// hosted without a binding. A legacy record's journal is never probed, so a
// create over one is now refused (v0.2.1 launched it).
type RigSessionIDs func(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uuid.UUID, error)

// WarmTimer is one session's warm countdown, in time.Timer's own three methods.
type WarmTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

// CompositionClock is the composition's single time source: the Host's Clock,
// the drain's bound and the warm releaser's countdown, all one timeline. Its
// zero value in Composition means the system clock.
type CompositionClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
	NewTimer(time.Duration) *time.Timer
	NewWarmTimer(time.Duration) WarmTimer
}

// Collaborators is everything a deployment supplies that this module cannot
// know: which store, which journals, which agents, how to checkpoint, who may
// connect, where workspaces live.
type Collaborators struct {
	// Backend is the storage composite the session store is opened over. It
	// is REQUIRED. Compose opens the store itself — see Compose for why a
	// caller may not hand one in.
	Backend *storage.Composite

	// StoreOptions are passed to sessionstore.Open ahead of the settlement
	// evidence option Compose adds. WithDispositionEvidence must NOT be among
	// them: the store refuses two, so a caller's would fail the composition
	// rather than silently replace the router Compose built.
	StoreOptions []sessionstore.Option

	// JournalStores are this deployment's runtime journal stores, one per
	// (tenant, storage binding): the settlement evidence readers, and — as of
	// v0.3.0 — what an attach reads to decide whether a session's conversation
	// already exists. REQUIRED and non-empty: a Host with no reader cannot
	// settle anything and is refused at composition rather than at the first
	// command.
	//
	// A CREATE IS DECIDED BY THE JOURNAL, NOT BY THE ATTACH MODE. Factory sends
	// every attach as create, and harness does not verify that the id it is
	// asked to create under is fresh: a create over an existing conversation
	// silently re-opens that stream and restores nothing. So Host looks first,
	// under the runtime session id the session's binding names. The reader
	// registered for the binding must therefore be the released harness
	// session store (*harness sessionstore.Store, or a value embedding one) —
	// the same journal the runtime writes. Compose REFUSES a reader that is
	// not one. A session whose binding has no reader at all is refused a
	// create with runtime_unavailable; a journal READ that fails refuses the
	// attach with the empty, unclassified code, like every other durable-read
	// failure. Neither ever starts a conversation over.
	JournalStores map[EvidenceKey]sessionstore.DispositionEvidenceReader

	// Registrar produces the Department this Host serves. REQUIRED.
	Registrar Registrar

	// Checkpointer commits the checkpoints a nonterminal release requires.
	// REQUIRED; see Checkpointer.
	Checkpointer Checkpointer

	// Auth verifies the service credential on every HostLink connection.
	// REQUIRED.
	Auth AuthVerifier

	// Workspaces materializes and releases session workspaces. REQUIRED.
	Workspaces Workspaces

	// NamespaceLayout is REQUIRED; see NamespaceLayout.
	NamespaceLayout NamespaceLayout

	// RigSessionIDs is OPTIONAL and consulted only for a record with no
	// binding.
	//
	// Deprecated: see RigSessionIDs.
	RigSessionIDs RigSessionIDs
}

// LinkOptions bound the HostLink transport.
type LinkOptions struct {
	// PingInterval and PongTimeout are the HostLink heartbeat. Optional and
	// paired: one without the other is refused.
	PingInterval time.Duration
	PongTimeout  time.Duration

	// MaxBindingsPerLink and MaxBindings bound what one link and one tenant's
	// routing table may hold. MaxTenantLinks bounds how many per-tenant
	// transports this Host allocates at once; it is a memory bound and not a
	// tenant allowlist.
	MaxBindingsPerLink int
	MaxBindings        int
	MaxTenantLinks     int
}

// DrainOptions bound the drain.
type DrainOptions struct {
	// Grace is the platform's termination grace; Run also bounds Stop by it.
	Grace time.Duration
	// IdleBoundary bounds the wait for a session to reach a safe boundary.
	IdleBoundary time.Duration
	// PublishBound bounds the nonaccepting publication.
	PublishBound time.Duration
}

// Composition is one runnable Host, described.
type Composition struct {
	// Options is the Host's configuration. ITS COLLABORATOR FIELDS MUST BE
	// NIL — Department, SessionStore, Workspaces, Clock and Auth — because
	// Compose supplies every one of them from Collaborators: the Department
	// from the Registrar, the store it opened, the Workspaces, the clock, the
	// verifier. A caller-supplied one would be a second store or a second
	// clock beside the one the composition runs on, and is refused.
	Options Options

	// Generation is this process's incarnation, non-zero. It belongs to the
	// RUN and not the configuration: a Host restarted on identical Options is
	// a new generation, which is why Options does not carry it.
	Generation uint64

	Link  LinkOptions
	Drain DrainOptions

	// CompatibilityTimeout bounds the synchronous compatibility wait; WorkPoll
	// is how often resident sessions are asked what they are doing. Both are
	// required and positive.
	CompatibilityTimeout time.Duration
	WorkPoll             time.Duration

	// Clock is optional; nil means the system clock.
	Clock CompositionClock

	Collaborators Collaborators
}

// InvalidCompositionError reports a Composition that may not run.
type InvalidCompositionError struct {
	Field  string
	Reason string
	Cause  error
}

func (e *InvalidCompositionError) Error() string {
	message := "host: invalid composition " + strconv.Quote(e.Field) + ": " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the lower layer's error, if any.
func (e *InvalidCompositionError) Unwrap() error { return e.Cause }

// HostLinkPathPrefix is the path every HostLink connection arrives under on
// the Handler; the segment after it is the tenant the connection claims and
// the server then authenticates. It is Core's sessionwire.HostLinkPathPrefix,
// the prefix sessionwire.HostLinkEndpoint derives every tenant's address with.
const HostLinkPathPrefix = compose.HostLinkPathPrefix

// Service is one composed Host: built, then started, then stopped.
type Service struct {
	host    *Host
	store   *sessionstore.Store
	inner   *compose.Service
	metrics http.Handler
}

// Compose validates the composition, opens the session store, builds the
// Department and every collaborator, and returns a Service with nothing
// running.
//
// COMPOSE OPENS THE STORE ITSELF, and that is the whole reason Collaborators
// carries a Backend and not a *sessionstore.Store. Settlement refuses without a
// configured evidence reader, and the released store publishes no way to ask
// an open one whether it has one; a deployment that opened the store and
// forgot the reader would attach, list, claim, dispatch, and fail at every
// settlement, leaving each command applying with a real effect behind it.
// Opening here means the router built from JournalStores is installed on
// every store this composition can run on, so that failure is unrepresentable
// rather than documented. Compose closes the store on any later refusal.
func Compose(ctx context.Context, blueprint Composition) (*Service, error) {
	if err := blueprint.validate(); err != nil {
		return nil, err
	}
	collaborators := blueprint.Collaborators

	readers := make(map[sessionstoreadapter.EvidenceKey]sessionstore.DispositionEvidenceReader, len(collaborators.JournalStores))
	for key, reader := range collaborators.JournalStores {
		readers[sessionstoreadapter.EvidenceKey{TenantID: key.TenantID, StorageBindingID: key.StorageBindingID}] = reader
	}
	router, err := sessionstoreadapter.NewTenantEvidenceRouter(readers)
	if err != nil {
		return nil, &InvalidCompositionError{Field: "Collaborators.JournalStores", Reason: "the settlement evidence table is unusable", Cause: err}
	}
	// EVERY READER MUST BE A HARNESS JOURNAL, refused here rather than at the
	// first placement. An attach reads the binding's journal to decide a
	// create, and a reader that cannot answer turns every create routed to it
	// into a runtime_unavailable refusal — and Factory sends every attach as a
	// create, so such a Host could never place a session while passing every
	// probe. Refusing it is this module's rule: fail at composition.
	for key, reader := range collaborators.JournalStores {
		if !sessionstoreadapter.IsRuntimeJournal(reader) {
			return nil, &InvalidCompositionError{
				Field:  "Collaborators.JournalStores",
				Reason: "the reader for tenant " + strconv.Quote(string(key.TenantID)) + " and binding " + strconv.Quote(key.StorageBindingID) + " is not a harness session store (*harness sessionstore.Store, or a value embedding one), so Host could not tell a new session from one it would restart and would refuse every create",
			}
		}
	}

	storeOptions := append(append([]sessionstore.Option(nil), collaborators.StoreOptions...), sessionstore.WithDispositionEvidence(router))
	// THE STORE OUTLIVES THE COMPOSING CALL. sessionstore.Open derives the
	// store's own context from the one it is opened with and cancels it with
	// that context, so a store opened on a caller's signal context dies the
	// moment the signal arrives -- BEFORE the drain that context was meant to
	// begin has published a single nonaccepting row. Measured: Run's drain
	// failed its target update that way. The store's lifetime is this
	// Service's, ended by Stop, and only ctx's VALUES are carried.
	store, err := sessionstore.Open(context.WithoutCancel(ctx), collaborators.Backend, storeOptions...)
	if err != nil {
		var option *sessionstore.InvalidOptionError
		if errors.As(err, &option) {
			return nil, &InvalidCompositionError{Field: "Collaborators.StoreOptions", Reason: "the session store refused its options; a WithDispositionEvidence among them collides with the router Compose installs", Cause: err}
		}
		return nil, &InvalidCompositionError{Field: "Collaborators.Backend", Reason: "the session store could not be opened over it", Cause: err}
	}
	closeStore := func() { _ = store.Close(context.WithoutCancel(ctx)) }

	registrations, err := collaborators.Registrar.Register(ctx)
	if err != nil {
		closeStore()
		return nil, &InvalidCompositionError{Field: "Collaborators.Registrar", Reason: "the registrar could not produce the Department", Cause: err}
	}
	dept, err := department.New(registrations)
	if err != nil {
		closeStore()
		return nil, &InvalidCompositionError{Field: "Collaborators.Registrar", Reason: "the registrations do not form a Department", Cause: err}
	}

	adapterOptions := []sessionstoreadapter.Option{
		sessionstoreadapter.WithNamespaceLayout(sessionstoreadapter.NamespaceLayout(collaborators.NamespaceLayout)),
		// The router is the journal reader as well as the evidence reader, so
		// the store a create consults is the one settlement reads: the
		// runtime's own journal for that (tenant, binding).
		sessionstoreadapter.WithRuntimeJournals(router),
	}
	if collaborators.RigSessionIDs != nil {
		adapterOptions = append(adapterOptions, sessionstoreadapter.WithRigSessionIDs(sessionstoreadapter.RigSessionIDs(collaborators.RigSessionIDs)))
	}
	adapted, err := sessionstoreadapter.New(store, adapterOptions...)
	if err != nil {
		closeStore()
		return nil, &InvalidCompositionError{Field: "Collaborators.Backend", Reason: "the session store could not be adapted", Cause: err}
	}

	clock := blueprint.clock()
	options := blueprint.Options.resolved()
	options.Department = dept
	options.SessionStore = adapted
	options.Workspaces = collaborators.Workspaces
	options.Clock = clock
	options.Auth = collaborators.Auth
	resolved, err := hostconfig.New(options)
	if err != nil {
		closeStore()
		return nil, exportOptionsError(err)
	}

	inner, err := compose.New(compose.Options{
		Host:                 resolved,
		HostGeneration:       blueprint.Generation,
		Clock:                clock,
		Leases:               adapted,
		Durable:              adapted,
		Locations:            adapted,
		Workspaces:           collaborators.Workspaces,
		Inbox:                adapted,
		Cursors:              adapted,
		Records:              adapted,
		Writers:              adapted,
		Targets:              adapted,
		Checkpointer:         collaborators.Checkpointer,
		Auth:                 collaborators.Auth,
		PingInterval:         blueprint.Link.PingInterval,
		PongTimeout:          blueprint.Link.PongTimeout,
		MaxBindingsPerLink:   blueprint.Link.MaxBindingsPerLink,
		MaxBindings:          blueprint.Link.MaxBindings,
		MaxTenantLinks:       blueprint.Link.MaxTenantLinks,
		Grace:                blueprint.Drain.Grace,
		IdleBoundary:         blueprint.Drain.IdleBoundary,
		PublishBound:         blueprint.Drain.PublishBound,
		CompatibilityTimeout: blueprint.CompatibilityTimeout,
		WorkPoll:             blueprint.WorkPoll,
		// WorkStates IS NOT WIRED, and neither cmd/host nor this surface has
		// ever wired it: the seam needs a reader of the runtime's gate state
		// that department.Runtime does not expose. A Host composed here never
		// warm-evicts and never returns a gate boundary from the compatibility
		// wait. Stated rather than fixed; see the v0.2.0 result document.
	})
	if err != nil {
		closeStore()
		return nil, exportComposeError(err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(inner.Metrics())
	return &Service{
		host:    &Host{resolved: resolved},
		store:   store,
		inner:   inner,
		metrics: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}, nil
}

// validate holds every rule Compose applies before it opens anything.
func (c Composition) validate() error {
	for _, supplied := range []struct {
		field string
		set   bool
	}{
		{"Options.Department", c.Options.Department != nil},
		{"Options.SessionStore", c.Options.SessionStore != nil},
		{"Options.Workspaces", c.Options.Workspaces != nil},
		{"Options.Clock", c.Options.Clock != nil},
		{"Options.Auth", c.Options.Auth != nil},
	} {
		if supplied.set {
			return &InvalidCompositionError{Field: supplied.field, Reason: "must be nil; Compose supplies this collaborator from Collaborators, and a second one beside it would run the Host on two"}
		}
	}
	if c.Generation == 0 {
		return &InvalidCompositionError{Field: "Generation", Reason: "must be non-zero; Core refuses every HostLink record carrying a zero generation"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Collaborators.Backend", c.Collaborators.Backend != nil},
		{"Collaborators.JournalStores", len(c.Collaborators.JournalStores) != 0},
		{"Collaborators.Registrar", c.Collaborators.Registrar != nil},
		{"Collaborators.Checkpointer", c.Collaborators.Checkpointer != nil},
		{"Collaborators.Auth", c.Collaborators.Auth != nil},
		{"Collaborators.Workspaces", c.Collaborators.Workspaces != nil},
		{"Collaborators.NamespaceLayout", c.Collaborators.NamespaceLayout != nil},
	} {
		if !required.present {
			return &InvalidCompositionError{Field: required.field, Reason: "must be set"}
		}
	}
	return nil
}

// clock is the composition's one time source, defaulting to the system's.
func (c Composition) clock() compositionClock {
	if c.Clock != nil {
		return compositionClock{inner: c.Clock}
	}
	return compositionClock{inner: systemClock{}}
}

// compositionClock narrows a CompositionClock to the three clock seams the
// internal packages declare, so all of them read one timeline.
type compositionClock struct {
	inner CompositionClock
}

var (
	_ compose.Clock    = compositionClock{}
	_ hostconfig.Clock = compositionClock{}
)

func (c compositionClock) Now() time.Time                         { return c.inner.Now() }
func (c compositionClock) After(d time.Duration) <-chan time.Time { return c.inner.After(d) }
func (c compositionClock) NewTimer(d time.Duration) *time.Timer   { return c.inner.NewTimer(d) }
func (c compositionClock) NewWarmTimer(d time.Duration) residency.WarmTimer {
	return c.inner.NewWarmTimer(d)
}

// systemClock is the production CompositionClock, over compose.SystemClock.
type systemClock struct{}

func (systemClock) Now() time.Time                         { return compose.SystemClock{}.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return compose.SystemClock{}.After(d) }
func (systemClock) NewTimer(d time.Duration) *time.Timer   { return compose.SystemClock{}.NewTimer(d) }
func (systemClock) NewWarmTimer(d time.Duration) WarmTimer {
	return compose.SystemClock{}.NewWarmTimer(d)
}

// exportComposeError re-spells internal/compose's option refusal as this
// package's, naming the same field.
func exportComposeError(err error) error {
	var invalid *compose.InvalidOptionsError
	if !errors.As(err, &invalid) {
		return err
	}
	field := invalid.Field
	switch field {
	case "PingInterval", "PongTimeout", "MaxBindingsPerLink", "MaxBindings", "MaxTenantLinks":
		field = "Link." + field
	case "Grace", "IdleBoundary", "PublishBound":
		field = "Drain." + field
	case "HostGeneration":
		field = "Generation"
	}
	return &InvalidCompositionError{Field: field, Reason: invalid.Reason, Cause: err}
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

// Host is the resolved configuration this Service runs.
func (s *Service) Host() *Host { return s.host }

// Department is the immutable set of launch targets this Service serves.
func (s *Service) Department() *department.Department { return s.host.Department() }

// Start publishes this Host's advertisements and begins its heartbeat. The
// first publication is synchronous, so a Host whose directory refuses it
// fails here rather than running invisibly. A Service starts once.
func (s *Service) Start(ctx context.Context) error { return s.inner.Start(ctx) }

// Ready reports whether this Host is ACCEPTING: the readiness probe's answer.
// A draining Host is not ready from the instant its ledger flips.
func (s *Service) Ready() bool { return s.inner.Ready() }

// Live reports whether this process should keep running: the liveness probe's
// answer. It stays true throughout and after a drain and turns false only when
// Stop has run, so a platform never kills a Host mid-checkpoint or before it
// has answered `drained` over HostLink.
func (s *Service) Live() bool { return s.inner.Live() }

// Handler serves HostLink at HostLinkPathPrefix + the tenant.
func (s *Service) Handler() http.Handler { return s.inner.Handler() }

// MetricsHandler serves this Host's Prometheus series, and only this Host's:
// the registry behind it holds one collector rather than the default
// registry's process, Go and dependency series. It is a handler and not a
// prometheus.Collector so that client_golang's types stay out of this API.
func (s *Service) MetricsHandler() http.Handler { return s.metrics }

// Routes serves HostLink, the two probes and the metrics on one handler:
// HostLinkPathPrefix, /readyz (Ready), /healthz (Live) and /metrics. It is
// what Run mounts, exported so a binary that owns its listener can mount the
// same four routes beside its own.
func (s *Service) Routes() http.Handler {
	routes := http.NewServeMux()
	routes.Handle(HostLinkPathPrefix, s.Handler())
	routes.Handle("/metrics", s.MetricsHandler())
	routes.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		probe(writer, s.Ready(), "accepting", "not accepting")
	})
	routes.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		probe(writer, s.Live(), "live", "stopped")
	})
	return routes
}

// probe writes one probe answer. 503 IS THE REFUSAL AND NOT 500: a platform
// treats any non-2xx as failure, so the code is for the human reading logs,
// and "deliberately not taking traffic" is what 503 means.
func probe(writer http.ResponseWriter, ok bool, yes, no string) {
	if !ok {
		http.Error(writer, no, http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(yes))
}

// AttachRequest asks this Host to make one session resident, in process.
type AttachRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	AgentID   sessionwire.AgentID

	// Mode is Core's closed attach mode: create or restore.
	Mode sessionwire.HostLinkAttachMode

	// RuntimeCompatibilityID is the build the session was placed on. Optional;
	// when set it must equal the target's, and a resident session's.
	RuntimeCompatibilityID string

	// ActorID is who the attach acts for — a service identity, not an end
	// user — and is REQUIRED so the attach can be attributed. TraceID is
	// optional correlation.
	ActorID string
	TraceID string
}

// Residency is what an attach reports.
type Residency struct {
	TenantID               sessionwire.TenantID
	SessionID              sessionwire.SessionID
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string

	// LeaseEpoch is this Host's residency grant: the value a bind presents.
	LeaseEpoch uint64

	// JournalEpoch is the RUNTIME's grant, read from the launched runtime and
	// never derived from LeaseEpoch; JournalEpochHeld is the half to branch
	// on, since a headless runtime legitimately holds none.
	JournalEpoch     uint64
	JournalEpochHeld bool

	// Attached reports whether THIS call established the residency. False
	// means the session was already resident and this call took nothing.
	Attached bool
}

// AttachError reports a refused or failed attach.
//
// Step names WHERE it failed. Code is the HostLink class a Factory branches
// on and is EMPTY when the failure is not a placement outcome — a store that
// failed, a launch that failed outright — which is Core's own rule for such
// failures. CurrentLeaseEpoch is set for epoch_mismatch when the OTHER
// holder's epoch was readable; it is that holder's, never this Host's, and a
// caller must not bind with it.
type AttachError struct {
	Step              string
	Code              sessionwire.HostLinkErrorCode
	CurrentLeaseEpoch uint64
	Reason            string
	Cause             error

	// Unreleased names every compensation that itself failed, so "the attach
	// failed and took nothing" can be told from "something is still held".
	Unreleased []string
}

func (e *AttachError) Error() string {
	message := "host: attach failed at " + e.Step + ": " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	if len(e.Unreleased) > 0 {
		message += " (and the rollback could not release " + strings.Join(e.Unreleased, "; ") + ")"
	}
	return message
}

// Unwrap returns the typed error a lower layer produced, if any.
func (e *AttachError) Unwrap() error { return e.Cause }

// HostLinkCode returns the refusal class Factory branches on, and whether this
// failure has one at all.
func (e *AttachError) HostLinkCode() (sessionwire.HostLinkErrorCode, bool) {
	return e.Code, e.Code != ""
}

// Attach makes a session resident on this Host, in process.
//
// It is the SAME entry point the hostlink.attach RPC reaches, so its
// semantics are the RPC's: serialized per session, idempotent on a resident
// session (Attached false, the existing epoch), refused with a HostLink class
// where placement should be re-run and with none where it should not.
func (s *Service) Attach(ctx context.Context, request AttachRequest) (Residency, error) {
	var mode residency.Mode
	switch request.Mode {
	case sessionwire.HostLinkAttachModeCreate:
		mode = residency.ModeCreate
	case sessionwire.HostLinkAttachModeRestore:
		mode = residency.ModeRestore
	default:
		return Residency{}, &AttachError{Step: string(residency.StepValidate), Reason: "the attach mode " + strconv.Quote(string(request.Mode)) + " is neither create nor restore"}
	}
	held, err := s.inner.Attach(ctx, residency.Request{
		TenantID:        request.TenantID,
		SessionID:       request.SessionID,
		AgentID:         request.AgentID,
		Mode:            mode,
		CompatibilityID: department.CompatibilityID(request.RuntimeCompatibilityID),
		Principal: residency.Principal{
			TenantID: request.TenantID,
			ActorID:  residency.ActorID(request.ActorID),
			TraceID:  residency.TraceID(request.TraceID),
		},
	})
	if err != nil {
		return Residency{}, exportAttachError(err)
	}
	return Residency{
		TenantID:               held.Key.TenantID,
		SessionID:              held.Key.SessionID,
		AgentID:                held.AgentID,
		RuntimeCompatibilityID: string(held.CompatibilityID),
		LeaseEpoch:             uint64(held.LeaseEpoch),
		JournalEpoch:           uint64(held.JournalEpoch),
		JournalEpochHeld:       held.JournalEpochHeld,
		Attached:               held.Attached,
	}, nil
}

// exportAttachError re-spells the manager's refusal as this package's, lifting
// the other holder's epoch through the one mechanism the HostLink path uses.
func exportAttachError(err error) error {
	var refused *residency.AttachError
	if !errors.As(err, &refused) {
		return err
	}
	exported := &AttachError{
		Step:       string(refused.Step),
		Code:       refused.Code,
		Reason:     refused.Reason,
		Cause:      err,
		Unreleased: append([]string(nil), refused.Unreleased...),
	}
	if refused.Code == sessionwire.HostLinkErrorEpochMismatch {
		exported.CurrentLeaseEpoch, _ = compose.LeaseHolderEpoch(err)
	}
	return exported
}

// DrainFailure is one drain step that did not succeed.
type DrainFailure struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Step      string
	Err       error
}

func (f DrainFailure) Error() string {
	return "host: drain of " + string(f.TenantID) + "/" + string(f.SessionID) + " failed at " + f.Step + ": " + f.Err.Error()
}

// Unwrap returns the step's own error.
func (f DrainFailure) Unwrap() error { return f.Err }

// DrainReport is what Stop reports: the drain's generation and terminal state
// and every step that did not succeed. A drained Host with failures IS
// drained; the failures are what an operator must reconcile, and Core's
// two-valued drain state cannot carry them over HostLink.
type DrainReport struct {
	Generation uint64
	State      sessionwire.HostLinkDrainState
	Failures   []DrainFailure
}

// StepCloseStore names the Stop step that closes the session store Compose
// opened; a failure there is reported on the DrainReport under this step.
const StepCloseStore = "close_store"

// Stop drains this Host, waits for the drain to finish, closes every tenant
// transport, and then closes the session store Compose opened. The store is
// last because everything before it may still write.
func (s *Service) Stop(ctx context.Context) (DrainReport, error) {
	report, err := s.inner.Stop(ctx)
	if err != nil {
		return DrainReport{}, err
	}
	exported := DrainReport{Generation: report.Generation, State: report.State}
	for _, failure := range report.Failures {
		exported.Failures = append(exported.Failures, DrainFailure{
			TenantID:  failure.Key.TenantID,
			SessionID: failure.Key.SessionID,
			Step:      string(failure.Step),
			Err:       failure.Err,
		})
	}
	if err := s.store.Close(ctx); err != nil {
		exported.Failures = append(exported.Failures, DrainFailure{Step: StepCloseStore, Err: err})
	}
	return exported, nil
}
