// Command host runs one headless Department runtime host.
//
// IT IS GENERIC AND IT REGISTERS NO AGENTS. Host serves whatever launch targets
// a PRODUCT gives it, and this binary reaches them through one injected seam —
// Bootstrap — rather than importing them. That is not a style preference: Host
// sits below every product in the release graph and import_boundary_test.go
// refuses a product import from anywhere in this module, tests included, so a
// binary that named Carbon's agents could not be built here at all.
//
// A PRODUCT CANNOT YET SHIP ITS OWN MAIN AGAINST THIS. Bootstrap and Run live
// in package main, and two of Bootstrap's five methods name internal/ types —
// hostlink.Authenticator and residency.Workspaces — which no other module can
// implement. So today a product vendors this file, or Host grows an exported
// composition surface. Which of those it will be is not decided here; what is
// decided is that this comment does not claim the second one already exists.
// It was four of seven until sessionstore v0.7.0 let the composition bind the
// durable inbox and its cursor itself.
package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
	"github.com/looprig/host/internal/compose"
	hostconfig "github.com/looprig/host/internal/hostconfig"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// Bootstrap is the product-specific seam this binary is constructed through.
//
// THE FIVE METHODS ARE THE THINGS HOST CANNOT KNOW, and the first three are
// the ones that are product knowledge by nature rather than by an owed release;
// the last two are below, with their reason. Which durable store
// this deployment runs against is an operational decision — Host names no
// storage backend and must not, since choosing one here would make every
// deployment carry every provider; which agents it serves is the product's; and
// how a session's state is checkpointed is the runtime's, which Host holds no
// reader for. Everything else is configuration.
type Bootstrap interface {
	// Store opens the durable session store this Host runs against, and MUST
	// open it with sessionstore.WithDispositionEvidence(evidence).
	//
	// THE READER IS A PARAMETER RATHER THAN A DOCUMENTED OBLIGATION, and that
	// is the whole reason this method's signature changed. Settlement refuses
	// outright without a configured reader — "a store with no reader has no way
	// to verify anything" — so a deployment that forgot one would attach, list,
	// claim, authorize an attempt, DISPATCH, and then fail at every settlement,
	// leaving each command applying with a real effect behind it. A product
	// cannot obtain one any other way: Host builds it, from the readers the
	// product declares below, and hands it over at the one call that can use it.
	//
	// WHAT THIS STILL CANNOT ENFORCE — THREE CONDITIONS, NOT ONE. An earlier
	// version of this paragraph named only the first, and an incomplete
	// enumeration at the seam a product must get right is worse than none: it
	// reads as the complete list.
	//
	// ALL THREE SHARE ONE SIGNATURE, which is why they belong together: each
	// fails only AFTER every dispatch has already happened, leaves the command
	// stranded `applying` with a real runtime effect behind it, and is invisible
	// until the first settlement.
	//
	//	(a) A PRODUCT TAKES THE READER AND DROPS IT. Host cannot make a product
	//	    pass it to Open, and the released store publishes no way to ask an
	//	    open store whether one is configured. What the parameter buys is that
	//	    the obligation is impossible to be unaware of and the object to
	//	    satisfy it with is already in hand.
	//
	//	(b) THE ORCHESTRATION TENANT AND THE JOURNAL STORE'S TENANT DISAGREE. A
	//	    harness Store files ONE tenant's sessions for its whole life and
	//	    refuses an evidence request naming another, because answering it from
	//	    that keyspace would be answering the wrong question confidently. The
	//	    two tenants are configured independently and no type relates them.
	//	    This one is at least DIAGNOSED: it surfaces as
	//	    commands.RefusalEvidenceUnroutable rather than as a silent runtime.
	//
	//	(c) A ROUTER KEY IS PRESENT BUT NAMES THE WRONG JOURNAL STORE. Host can
	//	    check that SOME reader is registered for a binding; it cannot check
	//	    that the reader registered actually SERVES it. That store is healthy,
	//	    at the correct tenant, and truthfully reports it holds no such record
	//	    — which is indistinguishable from the benign "the runtime wrote
	//	    nothing" case at settlement time, and correctly so. It is the one
	//	    cause that stays collapsed under RefusalEvidenceUnavailable.
	//
	// (c) NEEDS A CONSTRUCTION-TIME ANSWER AND IS BLOCKED UPSTREAM: a journal
	// store would have to declare which StorageBindingIDs it serves, and no
	// released API offers that. A harness Tenant() accessor would close (b) and
	// NOT (c) — do not book it as the answer to both.
	Store(ctx context.Context, evidence sessionstore.DispositionEvidenceReader) (*sessionstore.Store, error)

	// JournalStores are this deployment's settlement evidence readers, one per
	// storage binding, and they are the product's because the JOURNALS are.
	//
	// A DISPOSITION SESSION'S JOURNAL IS NOT IN THE ORCHESTRATION STORE. The
	// immutable binding says where it is, and a harness Store answers for the
	// one keyspace it owns — it holds no registry of the bindings it serves, so
	// it cannot route on Binding.StorageBindingID and says so. Selecting which
	// reader serves which binding is therefore the composition root's job, and
	// Host does that selection over this table.
	//
	// A HOST WITH AN EMPTY TABLE CANNOT SETTLE ANYTHING and is refused at
	// startup rather than at the first command.
	JournalStores() map[string]sessionstore.DispositionEvidenceReader

	// Registrar produces the Department registrations this deployment serves.
	Registrar() host.Registrar

	// Checkpointer commits the checkpoints a nonterminal release requires.
	Checkpointer() host.Checkpointer

	// Auth verifies the service credential a Factory presents on every
	// HostLink connection.
	//
	// IT IS THE PRODUCT'S BECAUSE THE IDENTITY PROVIDER IS. Host authenticates
	// a Factory's service identity, which is a deployment's mTLS or token
	// material, and this module names no provider and must not: a placeholder
	// that accepted anything would make every deployment that forgot to
	// replace it an open Host.
	Auth() hostlink.Authenticator

	// Workspaces is supplied by the deployment because workspace
	// materialization is not the session store's business and never was.
	//
	// INBOX AND CURSORS USED TO BE HERE AND ARE GONE, which is a release
	// landing rather than a design change. They were injected because
	// sessionstore v0.6.0 had no per-session ORDERED INBOX LISTING and no
	// durable CONSUMPTION CURSOR, so §10.4's "read the inbox in immutable
	// acceptance order, strictly after the cursor" had no released counterpart
	// and the alternative was faking one in production. v0.7.0 publishes both
	// over the DISPOSITION family — ListSessionDispositionCommands,
	// LoadDispositionCommandCursor and SaveDispositionCommandCursor — and
	// sessionstoreadapter binds them, so Run wires the adapted store and a
	// product supplies neither.
	Workspaces() residency.Workspaces
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := Run(ctx, os.LookupEnv, unconfiguredBootstrap{}); err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
}

// Run reads the configuration, composes the Host, serves it, and drains on
// cancellation.
//
// THE ORDER IS CONFIGURE, BUILD, PUBLISH, SERVE, DRAIN, and each refusal
// happens as early as it can be made. A Deployment with a malformed duration
// never opens a store; one whose Department is empty never publishes; one whose
// first advertisement the directory refuses fails at startup instead of running
// invisibly. What a platform sees in each case is a container that exited with a
// message, which is the failure an operator can act on.
func Run(ctx context.Context, lookup Environment, bootstrap Bootstrap) error {
	config, err := LoadConfig(lookup)
	if err != nil {
		return err
	}

	// THE EVIDENCE ROUTER IS BUILT BEFORE THE STORE IS OPENED, because the store
	// has to be opened WITH it: sessionstore.WithDispositionEvidence is an Open
	// option, and a store opened without one refuses every settlement. Building
	// it first also means a deployment whose routing table cannot answer is
	// refused before it takes a single durable admission.
	evidence, err := sessionstoreadapter.NewEvidenceRouter(bootstrap.JournalStores())
	if err != nil {
		return fmt.Errorf("bind settlement evidence: %w", err)
	}

	store, err := bootstrap.Store(ctx, evidence)
	if err != nil {
		return fmt.Errorf("open session store: %w", err)
	}
	defer func() { _ = store.Close(context.WithoutCancel(ctx)) }()

	registrations, err := bootstrap.Registrar().Register(ctx)
	if err != nil {
		return fmt.Errorf("register department: %w", err)
	}
	dept, err := department.New(registrations)
	if err != nil {
		return fmt.Errorf("build department: %w", err)
	}

	adapted, err := sessionstoreadapter.New(store)
	if err != nil {
		return fmt.Errorf("adapt session store: %w", err)
	}

	clock := compose.SystemClock{}
	built, err := hostconfig.New(hostconfig.Options{
		HostID:            config.HostID,
		InternalEndpoint:  config.InternalEndpoint,
		IsolationClass:    config.IsolationClass,
		Department:        dept,
		SessionStore:      adapted,
		Workspaces:        bootstrap.Workspaces(),
		Clock:             clock,
		Auth:              authAdapter{bootstrap.Auth()},
		Placement:         config.Placement,
		Capacity:          config.Capacity,
		FixedSessionID:    config.FixedSessionID,
		WarmTTL:           config.WarmTTL,
		RegistryHeartbeat: config.RegistryHeartbeat,
		RegistryExpiry:    config.RegistryExpiry,
		ClaimTTL:          config.ClaimTTL,
		ApplyDeadline:     config.ApplyDeadline,
		CommandQueueSize:  config.CommandQueueSize,
		ReconcileInterval: config.ReconcileInterval,
		ReconcileBatch:    config.ReconcileBatch,
	})
	if err != nil {
		return err
	}

	service, err := compose.New(compose.Options{
		Host:           built,
		HostGeneration: config.HostGeneration,
		Clock:          clock,
		Leases:         adapted,
		Durable:        adapted,
		Locations:      adapted,
		Workspaces:     bootstrap.Workspaces(),
		// THE ADAPTED STORE IS BOTH SEAMS. Before sessionstore v0.7.0 these
		// were the product's to supply and the generic binary's default
		// refused; the release closed the gap and the composition now names
		// one object for the durable inbox and its cursor, which is what makes
		// "the stream Host consumes is the stream its cursor indexes" true by
		// construction rather than by a convention a deployment could break.
		//
		// THE LEGACY APPLICATION SEAMS ARE STILL NOT WIRED, and the adapter
		// still exports them. This binary once composed a journal opener and
		// the four legacy-family command seams into an applier; a Host cannot
		// reach that family on a session it can hold, so compose.Options names
		// none of them and wiring one here would be the mistake the removed
		// dispatch refusal was written to make visible.
		//
		// THE DISPOSITION SEAMS ARE WIRED, AND THAT IS WHAT REPLACED THE
		// BOUNDARY. Records is the adapted store's disposition record read;
		// Writers binds one writer per session to the RESIDENCY GRANT this Host
		// holds for it, which is what the released claim edge requires and what
		// a per-call epoch parameter would have handed back to a caller.
		Inbox:                adapted,
		Cursors:              adapted,
		Records:              adapted,
		Writers:              adapted,
		Targets:              adapted,
		Checkpointer:         bootstrap.Checkpointer(),
		Auth:                 bootstrap.Auth(),
		PingInterval:         config.PingInterval,
		PongTimeout:          config.PongTimeout,
		MaxBindingsPerLink:   config.MaxBindingsPerLink,
		MaxBindings:          config.MaxBindings,
		MaxTenantLinks:       config.MaxTenantLinks,
		Grace:                config.Grace,
		IdleBoundary:         config.IdleBoundary,
		PublishBound:         config.PublishBound,
		CompatibilityTimeout: config.CompatibilityTimeout,
		WorkPoll:             config.WorkPoll,
	})
	if err != nil {
		return err
	}

	if err := service.Start(ctx); err != nil {
		return err
	}

	server := &http.Server{
		Addr:              config.ListenAddress,
		Handler:           mux(service),
		ReadHeaderTimeout: 10 * time.Second,
	}
	listening := make(chan error, 1)
	go func() { listening <- server.ListenAndServe() }()

	select {
	case err := <-listening:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}

	// THE DRAIN RUNS FIRST AND THE LISTENER CLOSES AFTER IT. Factory reads the
	// bounded drain-status observation over HostLink, so a listener closed at
	// the signal would leave it inferring completion from a disconnect — the
	// inference the status observation exists to replace.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.Grace)
	defer cancel()
	report, drainErr := service.Stop(drainCtx)
	shutdownErr := server.Shutdown(drainCtx)
	for _, failure := range report.Failures {
		fmt.Fprintf(os.Stderr, "host: drain failure: session=%s/%s step=%s: %v\n",
			failure.Key.TenantID, failure.Key.SessionID, failure.Step, failure.Err)
	}
	return errors.Join(drainErr, shutdownErr)
}

// mux serves HostLink, the two probes and the metrics on one listener.
//
// READINESS AND LIVENESS ARE TWO ROUTES BECAUSE THEY ARE TWO QUESTIONS, and
// serving one answer at both is the mistake this split exists to prevent. A
// draining Host is NOT ready — a platform must stop sending it new placements
// the instant the ledger flips — and it IS live, because the platform must not
// kill the process it has just asked to shut down gracefully, in the middle of
// the checkpoints the drain exists to take.
func mux(service *compose.Service) http.Handler {
	routes := http.NewServeMux()
	routes.Handle(compose.HostLinkPathPrefix, service.Handler())
	routes.Handle("/metrics", promhttp.HandlerFor(collector(service), promhttp.HandlerOpts{}))
	routes.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		probe(writer, service.Ready(), "accepting", "not accepting")
	})
	routes.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		probe(writer, service.Live(), "live", "stopped")
	})
	return routes
}

// probe writes one probe answer.
//
// 503 IS THE REFUSAL AND NOT 500. A Kubernetes probe treats any non-2xx as a
// failure, so the code is for the human reading the logs, and "the service is
// deliberately not taking traffic" is what 503 means.
func probe(writer http.ResponseWriter, ok bool, yes, no string) {
	if !ok {
		http.Error(writer, no, http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(yes))
}

// unconfiguredBootstrap is what the generic binary runs with, and it refuses.
//
// IT IS NOT A DEVELOPMENT DEFAULT. A Bootstrap that quietly opened an in-memory
// store and registered nothing would produce a Host that started, advertised
// zero targets, and looked healthy to every probe while being able to serve
// nothing — which is the failure that is hardest to notice. Refusing names the
// missing piece instead.
type unconfiguredBootstrap struct{}

var errNoBootstrap = errors.New("this binary is generic and registers no agents; a product supplies a Bootstrap and calls Run")

// Store refuses.
func (unconfiguredBootstrap) Store(context.Context, sessionstore.DispositionEvidenceReader) (*sessionstore.Store, error) {
	return nil, errNoBootstrap
}

// JournalStores is one refusing reader, and it is ONE rather than none on
// purpose: NewEvidenceRouter refuses an empty table, so a generic binary with no
// readers would fail at "bind settlement evidence" and never reach the refusal
// this type exists to give. The registered reader refuses every request, which
// is the same answer one binding late.
func (b unconfiguredBootstrap) JournalStores() map[string]sessionstore.DispositionEvidenceReader {
	return map[string]sessionstore.DispositionEvidenceReader{"unconfigured": b}
}

// ReadDispositionEvidence refuses, and never answers empty: the released reader
// contract says absence is not a disposition and a zero value settles nothing,
// so a reader with nothing to read must return an error.
func (unconfiguredBootstrap) ReadDispositionEvidence(
	context.Context, sessionstore.DispositionEvidenceRequest,
) (sessionstore.DispositionEvidence, error) {
	return sessionstore.DispositionEvidence{}, errNoBootstrap
}

// Registrar refuses.
func (unconfiguredBootstrap) Registrar() host.Registrar {
	return host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
		return nil, errNoBootstrap
	})
}

// Checkpointer refuses.
func (unconfiguredBootstrap) Checkpointer() host.Checkpointer { return unconfiguredBootstrap{} }

// Auth refuses.
func (unconfiguredBootstrap) Auth() hostlink.Authenticator { return unconfiguredBootstrap{} }

// Workspaces refuses.
func (unconfiguredBootstrap) Workspaces() residency.Workspaces { return unconfiguredBootstrap{} }

// VerifyTenant refuses.
func (unconfiguredBootstrap) VerifyTenant(context.Context, sessionwire.TenantID, string) error {
	return errNoBootstrap
}

// EnsureWorkspace refuses.
func (unconfiguredBootstrap) EnsureWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) (string, error) {
	return "", errNoBootstrap
}

// ReleaseWorkspace refuses.
func (unconfiguredBootstrap) ReleaseWorkspace(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return errNoBootstrap
}

// Checkpoint refuses.
func (unconfiguredBootstrap) Checkpoint(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return errNoBootstrap
}

// authAdapter widens the HostLink authenticator to the one host.Options
// requires, which is the same method under a different declaration.
//
// The two interfaces are byte-identical in shape and are declared in two
// packages because neither may import the other. Naming ONE object behind both
// is what makes "the credential HostLink checks is the credential the Host was
// configured with" true by construction rather than by a convention a
// composition could break.
type authAdapter struct {
	inner hostlink.Authenticator
}

// VerifyTenant forwards.
func (a authAdapter) VerifyTenant(ctx context.Context, tenant sessionwire.TenantID, credential string) error {
	return a.inner.VerifyTenant(ctx, tenant, credential)
}

// collector is the registry the metrics route gathers from.
//
// IT IS A FRESH REGISTRY AND NOT THE DEFAULT ONE. prometheus.DefaultRegisterer
// carries the process and Go collectors plus anything any dependency registered
// into it at init, so a Host that served it would publish whatever Centrifuge's
// own metrics happened to be called this release. One registry with one
// collector is what makes the published series this Host's own.
func collector(service *compose.Service) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(service.Metrics())
	return registry
}
