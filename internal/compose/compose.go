package compose

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/commands"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/lifecycle"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/registry"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

// Options configures the composition.
//
// Every field is required unless its documentation says otherwise. Nothing here
// restates a value host.New already checked: the Host is taken whole and every
// identity, endpoint, placement, capacity and timing is read from it.
type Options struct {
	// Host is the validated Host this composition runs.
	Host *hostconfig.Host

	// HostGeneration is this process's incarnation identity. Core refuses a
	// zero generation on every HostLink record, and the value belongs to the
	// RUN rather than the configuration — a Host restarted on identical options
	// is a new generation — which is why host.Options does not carry it.
	HostGeneration uint64

	// Clock is the composition's one time source. See Clock.
	Clock Clock

	// Durable seams, each already declared by the package that consumes it.
	//
	// THE APPLICATION SEAMS ARE ABSENT AND THAT IS THE BOUNDARY. An earlier
	// composition took Records, Applications, Gates, InboxWrites and a
	// SessionOpener, and wired them into commands.Applier. All five are over
	// sessionstore's LEGACY command family, which a Host cannot reach —
	// AcquireResidency pins ProtocolModeDisposition — and the opener's journal
	// writer was refused outright for every session this Host can hold. They are
	// not retained as unread options: an option a composition validates and never
	// consults is a configuration a deployment can get wrong with no consequence
	// and no signal. Records and Writers below say what replaced them.
	Leases     residency.SessionLeases
	Durable    residency.DurableStore
	Locations  residency.Locations
	Workspaces residency.Workspaces
	Inbox      commands.Inbox
	Cursors    commands.Cursors
	Targets    TargetDirectory

	// Records and Writers are the DISPOSITION family's application seams, and
	// they are what replaced the dispatch refusal that stood in their place.
	//
	// THE PARAGRAPH ABOVE USED TO SAY THE APPLICATION SEAMS WERE ABSENT AND THAT
	// THE ABSENCE WAS THE BOUNDARY. That was true of the LEGACY five — Records,
	// Applications, Gates, InboxWrites and a SessionOpener, all over a family a
	// Host cannot reach — and it stays true of them: none is retained here.
	// These two are the disposition family's, over edges this Host can actually
	// use, and they arrived with sessionstore v0.9.0's settlement and harness
	// v0.34.0's evidence writer.
	//
	// Writers IS A FACTORY AND NOT A WRITER, and that is the residency grant's
	// doing rather than a style choice. The claim edge takes a *ResidencyGrant
	// and derives the epoch from it, so no number a caller chose can reach the
	// record's high-water mark; a single shared writer would therefore have to
	// take a grant per call, handing that choice straight back. One writer is
	// bound per session, to the grant this Host holds for it.
	Records commands.DispositionRecords
	Writers DispositionWriters

	// Checkpointer is the product's release checkpoint. It is REQUIRED; see
	// host.Checkpointer for why an absent one may not become a no-op.
	Checkpointer hostconfig.Checkpointer

	// Auth verifies the service credential on every HostLink connection.
	Auth hostlink.Authenticator

	// PingInterval and PongTimeout are the HostLink heartbeat. They are
	// optional and paired; see hostlink.Config.
	PingInterval time.Duration
	PongTimeout  time.Duration

	// MaxBindingsPerLink and MaxBindings bound what one link and one tenant's
	// routing table may hold.
	MaxBindingsPerLink int
	MaxBindings        int

	// MaxTenantLinks bounds how many per-tenant transports this Host will
	// allocate at once.
	//
	// IT IS NOT A TENANT ALLOWLIST AND MUST NOT BECOME ONE. H8 made this Host's
	// admission rule the isolation class, derived from the admitted ledger, so
	// a bound here is a statement about memory and file descriptors and about
	// nothing else: the composition refuses a new ALLOCATION at the limit, and
	// refuses it to whichever tenant asks next rather than to the ones that did
	// not connect first. A link holding no bindings is reclaimed before the
	// refusal.
	MaxTenantLinks int

	// Grace, IdleBoundary and PublishBound are the drain's bounds. See
	// lifecycle.Options.
	Grace        time.Duration
	IdleBoundary time.Duration
	PublishBound time.Duration

	// CompatibilityTimeout bounds the synchronous compatibility wait. See
	// Service.AwaitSessionIdle.
	CompatibilityTimeout time.Duration

	// WorkPoll is how often this Host asks what its resident sessions are doing.
	//
	// A CADENCE IS REQUIRED BECAUSE WorkStates IS A PULL. That seam answers a
	// question and pushes nothing, so a session that went idle — or reached a
	// gate — after the last question is visible only when somebody asks again.
	//
	// ONE CADENCE SERVES BOTH READERS, and that is deliberate rather than
	// economical: the warm releaser and the compatibility wait are asking the
	// same question of the same source, and two intervals would let a
	// deployment configure a Host that evicts a session the compatibility wait
	// still believes is working. It is required even with no WorkStates source,
	// so a deployment that later supplies one does not discover it has no
	// cadence configured.
	WorkPoll time.Duration

	// WorkStates is the optional source of what resident sessions are doing.
	// See WorkStates for what a composition without one loses.
	WorkStates WorkStates

	// Budget is the weighted memory limit reported by metrics. Its zero value
	// is UNKNOWN, which is deliberately not a default; see
	// lifecycle.MemoryBudget.
	Budget lifecycle.MemoryBudget

	// Gates binds each resident session's gate publisher to its grant. It is
	// OPTIONAL here and the production composition always supplies it: a
	// composition without one publishes no gate, so a Factory can show no
	// AskUser or permission question from this Host and every answer is
	// refused as resolved. Internal fixtures that exercise nothing about gates
	// leave it nil.
	Gates GateSessions

	// GateReads is the durable gate projection a gate_response is checked
	// against before its attempt. OPTIONAL here, supplied by the production
	// composition: without it every gate_response blocks its session's pass
	// (commands.RefusalNoGateReader).
	GateReads commands.Gates

	// GateRetry is how long a gate publisher waits after a failed pass before
	// retrying. Zero means DefaultGateRetry.
	GateRetry time.Duration

	// Logger receives this Host's operator diagnostics. It is OPTIONAL and nil
	// discards: logging is never a precondition of running. What it says is
	// stated where each record is written; see logAttach.
	Logger *slog.Logger
}

// capabilities are the capability tokens this composition's links advertise.
//
// THE GATE-RESPONSE TOKEN IS ADVERTISED ONLY WHEN BOTH GATE SEAMS ARE WIRED.
// Gates alone would publish gates this Host then blocks every answer to
// (commands.RefusalNoGateReader); GateReads alone would check answers against
// a projection nothing publishes. Either half alone is a Host that cannot
// apply a gate_response, and a Factory must not be told it can.
func (s *Service) capabilities() []string {
	if s.options.Gates != nil && s.options.GateReads != nil {
		return []string{hostlink.CapabilityGateResponse}
	}
	return nil
}

// DefaultGateRetry is the gate publisher's retry wait when GateRetry is zero.
const DefaultGateRetry = time.Second

// gateRetry returns the configured retry wait or the default.
func (o Options) gateRetry() time.Duration {
	if o.GateRetry <= 0 {
		return DefaultGateRetry
	}
	return o.GateRetry
}

// discardLogger is what a composition without a Logger writes to.
var discardLogger = slog.New(slog.DiscardHandler)

// logger returns the configured Logger or the discard logger.
func (o Options) logger() *slog.Logger {
	if o.Logger == nil {
		return discardLogger
	}
	return o.Logger
}

// InvalidOptionsError reports a composition that may not run.
type InvalidOptionsError struct {
	Field  string
	Reason string
}

func (e *InvalidOptionsError) Error() string {
	return "compose: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// Service is one composed, runnable Host.
type Service struct {
	options Options

	registry  *registry.Registry
	capacity  *service.CapacityPublisher
	advertise *advertiser
	manager   *residency.Manager
	warm      *residency.WarmReleaser
	metrics   *lifecycle.Metrics
	drainer   *lifecycle.Drainer
	links     *links

	mu        sync.Mutex
	started   bool
	stopped   bool
	sessions  map[registry.Key]*resident
	consumers map[registry.Key]*commands.Consumer
	leases    map[registry.Key]residency.Lease

	// stopping closes when this Host is stopped, so a compatibility wait ends
	// with the process rather than outliving it.
	stopping chan struct{}

	heartbeatDone chan struct{}
	stopHeartbeat context.CancelFunc
}

// New validates the options and builds every collaborator, wired to each other.
//
// IT BUILDS AND DOES NOT START. A constructor that opened goroutines would make
// a refusal after the first one a partially started Host, and there is no such
// thing here: New either returns a Service with nothing running or returns an
// error and nothing at all.
func New(options Options) (*Service, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}

	index := registry.New(clockAdapter{options.Clock})
	capacity, err := service.NewCapacityPublisher(service.CapacityOptions{
		Host:           options.Host,
		HostGeneration: options.HostGeneration,
	})
	if err != nil {
		return nil, err
	}
	advertise := &advertiser{
		host:       options.Host,
		capacity:   capacity,
		directory:  options.Targets,
		registry:   index,
		locations:  options.Locations,
		generation: options.HostGeneration,
	}

	composed := &Service{
		options:   options,
		registry:  index,
		capacity:  capacity,
		advertise: advertise,
		sessions:  map[registry.Key]*resident{},
		consumers: map[registry.Key]*commands.Consumer{},
		leases:    map[registry.Key]residency.Lease{},
		stopping:  make(chan struct{}),
	}

	composed.warm, err = residency.NewWarmReleaser(residency.WarmOptions{
		Clock: warmClockAdapter{options.Clock},
		// THE TTL COMES FROM THE WIRED HOST, and until this line nothing in the
		// module read host.WarmTTL at all. O6.2 recorded that as owed here
		// precisely because its own warm test could only take a TTL from a Host
		// and hand it to the releaser by hand, which proves the releaser honours
		// a number and not that the Host's number reaches it.
		TTL:         options.Host.WarmTTL(),
		Registry:    index,
		Inbox:       composed,
		Consumption: composed,
		Admissions:  capacity,
		Observer:    composed,
	})
	if err != nil {
		return nil, err
	}

	ownership, err := residency.NewHeartbeatOwnership(residency.HeartbeatOptions{
		Host:           options.Host,
		HostGeneration: options.HostGeneration,
		Registry:       index,
		Locations:      options.Locations,
		Admissions:     capacity,
		Teardown:       composed,
	})
	if err != nil {
		return nil, err
	}
	composed.manager, err = residency.NewManager(residency.Options{
		Host:           options.Host,
		HostGeneration: options.HostGeneration,
		Registry:       index,
		Admissions:     capacity,
		Leases:         &leaseRecorder{service: composed, inner: options.Leases},
		Durable:        options.Durable,
		Workspaces:     options.Workspaces,
		Locations:      options.Locations,
		Ownership:      &sessionOwnership{service: composed, inner: ownership},
	})
	if err != nil {
		return nil, err
	}

	composed.metrics, err = lifecycle.NewMetrics(lifecycle.MetricsOptions{
		Residencies: index,
		Ledger:      capacity,
		Capacity:    options.Host.Capacity(),
		// THE WEDGE'S SIGNATURE, and the one optional metrics seam this
		// composition actually satisfies. A session whose command can be
		// dispatched but not SETTLED stops the pass at that command
		// permanently; without this the condition moves no counter, writes no
		// log and answers every delivery `accepted`. A non-zero value is a
		// FAULT rather than a designed state — see lifecycle.BlockedConsumers,
		// whose HELP string is what an operator actually reads.
		Blocked: composed,
		Budget:  options.Budget,
	})
	if err != nil {
		return nil, err
	}

	composed.links = &links{
		clock:    clockAdapter{options.Clock},
		max:      options.MaxTenantLinks,
		byTenant: map[sessionwire.TenantID]*tenantLink{},
		build:    composed.buildTenantLink,
	}

	composed.drainer, err = lifecycle.NewDrainer(lifecycle.Options{
		Generation:   options.HostGeneration,
		Clock:        clockAdapter{options.Clock},
		Admissions:   capacity,
		Advertiser:   advertise,
		Residents:    composed,
		Grace:        options.Grace,
		IdleBoundary: options.IdleBoundary,
		PublishBound: options.PublishBound,
	})
	if err != nil {
		return nil, err
	}
	return composed, nil
}

// validate holds every composition rule, in one place.
func (o Options) validate() error {
	if o.Host == nil {
		return &InvalidOptionsError{Field: "Host", Reason: "must be set"}
	}
	if o.HostGeneration == 0 {
		return &InvalidOptionsError{Field: "HostGeneration", Reason: "must be non-zero; Core refuses every HostLink record carrying a zero generation"}
	}
	for _, required := range []struct {
		field   string
		present bool
	}{
		{"Clock", o.Clock != nil},
		{"Leases", o.Leases != nil},
		{"Durable", o.Durable != nil},
		{"Locations", o.Locations != nil},
		{"Workspaces", o.Workspaces != nil},
		{"Inbox", o.Inbox != nil},
		{"Cursors", o.Cursors != nil},
		{"Records", o.Records != nil},
		{"Writers", o.Writers != nil},
		{"Targets", o.Targets != nil},
		{"Checkpointer", o.Checkpointer != nil},
		{"Auth", o.Auth != nil},
	} {
		if !required.present {
			return &InvalidOptionsError{Field: required.field, Reason: "must be set"}
		}
	}
	for _, positive := range []struct {
		field string
		value int
	}{
		{"MaxBindingsPerLink", o.MaxBindingsPerLink},
		{"MaxBindings", o.MaxBindings},
		{"MaxTenantLinks", o.MaxTenantLinks},
	} {
		if positive.value <= 0 {
			return &InvalidOptionsError{Field: positive.field, Reason: "must be positive"}
		}
	}
	for _, positive := range []struct {
		field string
		value time.Duration
	}{
		{"Grace", o.Grace},
		{"IdleBoundary", o.IdleBoundary},
		{"PublishBound", o.PublishBound},
		{"CompatibilityTimeout", o.CompatibilityTimeout},
		{"WorkPoll", o.WorkPoll},
	} {
		if positive.value <= 0 {
			return &InvalidOptionsError{Field: positive.field, Reason: "must be a positive duration"}
		}
	}
	// THE BOUND IS THE STORE'S AND IT IS CHECKED HERE RATHER THAN AT THE FIRST
	// HEARTBEAT. host.Options puts no ceiling on RegistryExpiry, so a Host with
	// an expiry beyond what the directory will store is constructible, passes
	// every option rule, and can never publish a single advertisement — it
	// starts and then goes silent, which is the failure mode this module
	// converts into a refusal everywhere else it can.
	if bound := o.Targets.MaxAdvertisementTTL(); o.Host.RegistryExpiry() > bound {
		return &InvalidOptionsError{
			Field: "Host.RegistryExpiry",
			Reason: "is " + o.Host.RegistryExpiry().String() + ", which exceeds the target directory's " + bound.String() +
				"; a Host whose expiry promise the directory refuses could never publish an advertisement",
		}
	}
	return nil
}

// Start publishes this Host's advertisements and begins its heartbeat.
//
// THE ORDER IS PUBLISH-THEN-BEAT AND THE FIRST PUBLICATION IS SYNCHRONOUS. A
// Start that returned before anything was durable would report a Host ready
// while a Factory paging candidates still could not see it, and a Host whose
// very first publication is refused — an expiry the directory rejects, a clock
// Core refuses, a store that is not there — must fail the way a configuration
// fails, at startup, rather than by being quietly unreachable.
//
// NOTHING ELSE IS STARTED HERE and the absences are deliberate. The residency
// manager, the consumers and the relays start per session, at attach. The HTTP
// listener is the caller's: Handler returns a handler and this package opens no
// socket, so a binary can mount HostLink beside its own probes on one server.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return errors.New("compose: a Service starts once")
	}
	s.mu.Unlock()

	if err := s.advertise.publish(ctx); err != nil {
		return err
	}

	beatCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	s.mu.Lock()
	s.started = true
	s.stopHeartbeat = cancel
	s.heartbeatDone = done
	s.mu.Unlock()

	go s.heartbeat(beatCtx, done)
	go s.sampleWork(beatCtx)
	return nil
}

// sampleWork asks the optional work-state source what each resident session is
// doing and hands the answer to the warm releaser.
//
// IT IS THE ONLY PRODUCTION CALLER OF WarmReleaser.Observe, and until it existed
// the warm release was a state machine nothing drove: O6.1 built it against a
// fake observer and recorded that the composition owed the wiring. A Host
// composed without a WorkStates source therefore never arms a warm countdown and
// never evicts, which is an absence a test asserts rather than a sentence.
func (s *Service) sampleWork(ctx context.Context) {
	if s.options.WorkStates == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.options.Clock.After(s.options.WorkPoll):
			for _, entry := range s.registry.Snapshot() {
				if state, known := s.options.WorkStates.WorkState(entry.Key); known {
					s.warm.Observe(entry.Key, state)
				}
			}
		}
	}
}

// heartbeat republishes the derivation on the Host's configured interval.
//
// A FAILED BEAT IS NOT FATAL AND IS NOT RETRIED FASTER. The record carries its
// own expiry and a reader treats an expired advertisement as absent, so a Host
// that cannot reach the directory disappears from placement on its own — which
// is the correct outcome — while a tighter retry loop against a store that is
// already struggling is the opposite of that.
func (s *Service) heartbeat(ctx context.Context, done chan struct{}) {
	defer close(done)
	interval := s.options.Host.RegistryHeartbeat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.options.Clock.After(interval):
			if err := s.advertise.publish(ctx); err != nil {
				// A superseded generation is said so, because it is permanent
				// for this process and has one remedy: stop it.
				s.options.logger().LogAttrs(ctx, slog.LevelWarn, "host: the target advertisement heartbeat failed",
					slog.Bool("generation_superseded", errors.Is(err, service.ErrTargetGenerationSuperseded)),
					slog.String("error", err.Error()))
			}
		}
	}
}

// Ready reports whether this Host is ACCEPTING.
//
// READY AND LIVE ARE NOT THE SAME QUESTION and this pair is the whole of
// 04-host.md's step 2. Ready is the readiness probe's answer and it is about
// admission: a Host that has begun a drain is not ready, so a platform stops
// routing new placements to it, and that is true from the instant the ledger
// flips rather than from the instant the last session is released.
func (s *Service) Ready() bool {
	s.mu.Lock()
	started, stopped := s.started, s.stopped
	s.mu.Unlock()
	return started && !stopped && !s.capacity.Draining()
}

// Live reports whether this process should keep running.
//
// IT REMAINS TRUE THROUGHOUT A DRAIN, and that is the half a platform gets
// wrong. A liveness probe that failed when a drain began would have the
// platform kill the process it had just asked to shut down gracefully — killing
// it in the middle of the checkpoints the drain exists to take.
//
// IT ALSO REMAINS TRUE AFTER THE DRAIN HAS FINISHED, and the previous sentence
// here said otherwise — it said liveness turns false "when this Host has
// finished releasing everything it owned, or has been stopped". The code has
// only ever read `started && !stopped`, which Stop alone sets, so the doc was a
// sentence wider than the probe. THE CODE IS THE CORRECT ONE and the doc was
// fixed to match it, not the reverse: a Host that has completed a
// link-initiated drain stays up ANSWERING `drained` over HostLink, because that
// terminal answer is what a placement controller deletes the workload on. A
// liveness probe that failed the moment release finished would have the
// platform kill the process before it could deliver it — reintroducing, as a
// SIGKILL, exactly the disconnect-instead-of-an-answer this design removed.
//
// So liveness turns false when, and only when, Stop has run.
func (s *Service) Live() bool {
	s.mu.Lock()
	started, stopped := s.started, s.stopped
	s.mu.Unlock()
	return started && !stopped
}

// Handler serves HostLink at HostLinkPathPrefix + the tenant.
func (s *Service) Handler() http.Handler { return s.links.handler() }

// Metrics is the Prometheus collector for this Host.
func (s *Service) Metrics() *lifecycle.Metrics { return s.metrics }

// Attach makes a session resident on this Host.
//
// It is the composition's ONE attach entry point. HostLink cannot be a second
// one: a bind validates current ownership and never grants it, and the routing
// table is handed a read-only view of the registry precisely so that it cannot.
func (s *Service) Attach(ctx context.Context, request residency.Request) (residency.Residency, error) {
	held, err := s.manager.Attach(ctx, request)
	if err != nil {
		s.logAttach(ctx, request, err)
	}
	return held, err
}

// logAttach records a refused attach for an operator.
//
// A HYDRATION REFUSAL IS A WARNING, and it is the one this Host was booked to
// log (v0.3.0's N3 and F12). Those refusals — no journal store serves the
// binding, a restore needs a checkpoint the session has not got, the binding
// names no runnable identity, the journal read failed — are about the SESSION
// or this Host's wiring, the same on every retry and on every Host with the
// same configuration, and factory v0.3.0 counts them without logging them. So
// the only place an operator can learn why a session never places is here.
//
// EVERY OTHER REFUSAL IS DEBUG. epoch_mismatch, no_capacity and not_admitting
// are the ordinary traffic of a placement race and of a draining Host; at WARN
// they would bury the records above.
func (s *Service) logAttach(ctx context.Context, request residency.Request, err error) {
	level := slog.LevelDebug
	attributes := []slog.Attr{
		slog.String("tenant_id", string(request.TenantID)),
		slog.String("session_id", string(request.SessionID)),
		slog.String("agent_id", string(request.AgentID)),
		slog.String("mode", string(request.Mode)),
	}
	var refused *residency.AttachError
	if errors.As(err, &refused) {
		code, _ := refused.HostLinkCode()
		attributes = append(attributes,
			slog.String("step", string(refused.Step)),
			slog.String("code", string(code)),
			slog.String("reason", refused.Reason))
		if refused.Step == residency.StepHydrate {
			level = slog.LevelWarn
		}
	}
	attributes = append(attributes, slog.String("error", err.Error()))
	s.options.logger().LogAttrs(ctx, level, "host: attach refused", attributes...)
}

// Stop drains this Host and waits for the drain to finish.
//
// THE ORDER IS THE REVERSE OF START, WITH THE DRAIN IN THE MIDDLE, and each
// step is here because something downstream depends on the one before it:
//
//  1. the drain stops admission, publishes nonaccepting durably, and only then
//     acknowledges — internal/lifecycle owns that sequence and this does not
//     restate it;
//  2. every resident session is released through BeginRelease, WaitIdle,
//     Checkpoint, ReleaseResidency and FinishRelease;
//  3. the advertisement heartbeat stops, AFTER the drain rather than before,
//     because a heartbeat stopped first would leave the last derivation this
//     Host published standing until it expired;
//  4. the warm releaser stops, so no countdown fires against a session the
//     drain has already released;
//  5. the residency manager closes;
//  6. every tenant transport closes, LAST, because Factory reads the bounded
//     drain-status observation through it.
//
// STEP 6 IS THIS FUNCTION'S, AND IT USED TO BE THE DRAIN'S. internal/lifecycle
// held a Link seam and closed it as the drain's last step, which meant a
// HostLink-INITIATED drain destroyed the transport carrying the terminal
// `drained` answer before that answer existed — and the disconnect a Factory
// was left with is the same Centrifuge 3001 whether release finished or
// FinishRelease refused, so it is ambiguous between "delete the workload" and
// "this Host still holds the lease". The drain now closes nothing; the PROCESS
// stopping closes it. "LAST" is unchanged and still asserted: this runs after
// Wait has returned, so it is after every release step.
//
// A LINK-DRAINED HOST THEREFORE STAYS UP AND KEEPS ANSWERING, which is not a
// new cost: Run already only leaves on ctx.Done, so such a Host lingered
// before this change too — it just lingered refusing connections instead of
// reporting `drained` with Ready false and Live true.
//
// THE CLOSE FAILURE IS RECORDED RATHER THAN RETURNED, under the same step name
// the drain used to book it under, because a transport that will not shut down
// is an operator's problem and not a reason to fail a Stop whose releases all
// succeeded.
func (s *Service) Stop(ctx context.Context) (lifecycle.Report, error) {
	if _, err := s.drainer.StartDrain(hostlink.DrainScope{}); err != nil {
		return lifecycle.Report{}, err
	}
	report := s.drainer.Wait()

	s.mu.Lock()
	cancel, done := s.stopHeartbeat, s.heartbeatDone
	if !s.stopped {
		s.stopped = true
		close(s.stopping)
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	s.warm.Stop()
	s.manager.Close()

	// STEP 6, AND IT IS LAST BECAUSE IT IS WRITTEN LAST. The previous version
	// of this function did not close transports at all — the drain did, inside
	// its own cleanup — and "last" was a property of a different function's
	// ordering. It is this one's now.
	//
	// IT TAKES STOP'S CONTEXT rather than a background one. The drain used a
	// background context for its cleanup because cleanup that stopped at the
	// platform grace would orphan a LEASE; a transport shutdown holds no lease,
	// and a caller that has given up waiting should not be held by a
	// counterparty's socket.
	if err := s.links.Close(ctx); err != nil {
		report.Failures = append(report.Failures, lifecycle.Failure{Step: lifecycle.StepCloseLink, Err: err})
	}
	return report, nil
}

// ResidentSessions returns the sessions this Host holds, as the drain's seam.
func (s *Service) ResidentSessions() []lifecycle.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := make([]lifecycle.Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		held = append(held, releaseSession{resident: session})
	}
	return held
}

// buildTenantLink constructs one authenticated tenant's routing table,
// transport and event relay.
//
// FixedSessionID is passed through because the drain scope reads it; it is not
// an admission rule here and nothing on the bind path consults it. The
// dedicated-attach refusal lives in the residency manager, where the durable
// state is.
func (s *Service) buildTenantLink(tenant sessionwire.TenantID) (*tenantLink, error) {
	mux, err := hostlink.NewMultiplexer(hostlink.MultiplexerOptions{
		TenantID:           tenant,
		HostID:             s.options.Host.ID(),
		HostGeneration:     s.options.HostGeneration,
		Residencies:        s.registry,
		Admission:          s.capacity,
		Consumers:          s,
		DrainStarter:       s.drainer,
		DrainObserver:      s.drainer,
		Attacher:           linkAttacher{service: s},
		FixedSessionID:     s.options.Host.FixedSessionID(),
		MaxBindingsPerLink: s.options.MaxBindingsPerLink,
		MaxBindings:        s.options.MaxBindings,
	})
	if err != nil {
		return nil, err
	}
	server, err := hostlink.NewCentrifugeServer(hostlink.Config{
		TenantID:      tenant,
		Authenticator: s.options.Auth,
		PingInterval:  s.options.PingInterval,
		PongTimeout:   s.options.PongTimeout,
		Multiplexer:   mux,
		Capabilities:  s.capabilities(),
	})
	if err != nil {
		return nil, err
	}
	tails, err := service.NewTails(service.TailOptions{Publications: server, Routes: mux})
	if err != nil {
		_ = server.Close(context.Background())
		return nil, err
	}
	return &tenantLink{tenant: tenant, mux: mux, server: server, tails: tails}, nil
}

// Department is the immutable set of launch targets this Host serves.
func (s *Service) Department() *department.Department { return s.options.Host.Department() }
