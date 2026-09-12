// Command host runs one headless Department runtime host.
//
// IT IS GENERIC AND IT REGISTERS NO AGENTS. Host serves whatever launch targets
// a PRODUCT gives it, and this binary reaches them through one injected seam —
// Bootstrap — rather than importing them. That is not a style preference: Host
// sits below every product in the release graph and import_boundary_test.go
// refuses a product import from anywhere in this module, tests included, so a
// binary that named Carbon's agents could not be built here at all.
//
// A product ships its own main, a dozen lines long, that supplies a Bootstrap
// and calls Run.
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
	"github.com/looprig/host/internal/commands"
	"github.com/looprig/host/internal/compose"
	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/sessionstoreadapter"
)

// Bootstrap is the product-specific seam this binary is constructed through.
//
// THE THREE METHODS ARE THE THREE THINGS HOST CANNOT KNOW. Which durable store
// this deployment runs against is an operational decision — Host names no
// storage backend and must not, since choosing one here would make every
// deployment carry every provider; which agents it serves is the product's; and
// how a session's state is checkpointed is the runtime's, which Host holds no
// reader for. Everything else is configuration.
type Bootstrap interface {
	// Store opens the durable session store this Host runs against.
	Store(context.Context) (*sessionstore.Store, error)

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

	// Workspaces, Inbox and Cursors are supplied by the deployment BECAUSE THE
	// RELEASED STORE DOES NOT SATISFY THEM, which is a finding rather than a
	// design.
	//
	// sessionstore v0.6.0 has no per-session ORDERED INBOX LISTING and no
	// durable CONSUMPTION CURSOR: its command surface is Admit/Claim/
	// BeginApplying/Complete/Reject plus a cross-session due queue, and §10.4's
	// "read the inbox in immutable acceptance order, strictly after the cursor"
	// has no released counterpart at all. Workspace materialization is not that
	// store's business in the first place. Rather than fake any of the three in
	// production, they are injected and the gap is reported upward; see the
	// O7.1 result's owed list.
	Workspaces() residency.Workspaces
	Inbox() commands.Inbox
	Cursors() commands.Cursors
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

	store, err := bootstrap.Store(ctx)
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
	built, err := host.New(host.Options{
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
		OpenSession: func(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (compose.JournalGrant, error) {
			return adapted.OpenSession(ctx, tenant, session)
		},
		Inbox:                bootstrap.Inbox(),
		Cursors:              bootstrap.Cursors(),
		Records:              adapted,
		Applications:         adapted,
		Gates:                adapted,
		InboxWrites:          adapted,
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
func (unconfiguredBootstrap) Store(context.Context) (*sessionstore.Store, error) {
	return nil, errNoBootstrap
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

// Inbox refuses.
func (unconfiguredBootstrap) Inbox() commands.Inbox { return unconfiguredBootstrap{} }

// Cursors refuses.
func (unconfiguredBootstrap) Cursors() commands.Cursors { return unconfiguredBootstrap{} }

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

// ListOrdered refuses.
func (unconfiguredBootstrap) ListOrdered(context.Context, sessionwire.TenantID, sessionwire.SessionID, uint64, int) ([]commands.Command, error) {
	return nil, errNoBootstrap
}

// LoadCursor refuses.
func (unconfiguredBootstrap) LoadCursor(context.Context, sessionwire.TenantID, sessionwire.SessionID) (uint64, error) {
	return 0, errNoBootstrap
}

// SaveCursor refuses.
func (unconfiguredBootstrap) SaveCursor(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, order uint64, epoch uint64) error {
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
