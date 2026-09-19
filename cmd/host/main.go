// Command host runs one headless Department runtime host.
//
// IT IS GENERIC AND IT REGISTERS NO AGENTS. Host serves whatever launch targets
// a PRODUCT gives it, and this binary reaches them through one injected seam —
// Bootstrap — rather than importing them. That is not a style preference: Host
// sits below every product in the release graph and import_boundary_test.go
// refuses a product import from anywhere in this module, tests included, so a
// binary that named Carbon's agents could not be built here at all.
//
// A PRODUCT CAN SHIP ITS OWN MAIN AGAINST THIS MODULE SINCE v0.2.0. Everything
// this file does after reading its environment is host.Run over a
// host.Composition, both exported; Bootstrap here is one way of gathering the
// collaborators host.Collaborators asks for, and a product that has them in
// hand calls host.Run directly. This file no longer names an internal package.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"

	"github.com/looprig/host"
	"github.com/looprig/host/department"
)

// Bootstrap is the product-specific seam this binary is constructed through:
// the things Host cannot know, gathered for host.Collaborators.
type Bootstrap interface {
	// Backend is the storage composite the session store is opened over, plus
	// any sessionstore options the deployment needs (a legacy single tenant,
	// limits, a logger).
	//
	// IT IS THE BACKEND AND NOT THE STORE, and that is the difference between
	// this seam and its v0.1.0 predecessor, which asked a product to OPEN the
	// store with the evidence reader Host handed it. Host could hand the
	// reader over and could not make a product pass it to Open, and a store
	// opened without one attaches, dispatches, and then fails every settlement.
	// host.Compose opens the store itself now, so the reader is installed on
	// every store a Host runs on; what a product chooses here is which
	// backend, and nothing about how settlement is verified. A
	// WithDispositionEvidence among the returned options fails the
	// composition rather than replacing the router.
	Backend(ctx context.Context) (*storage.Composite, []sessionstore.Option, error)

	// JournalStores are this deployment's settlement evidence readers, one per
	// (tenant, storage binding), and they are the product's because the
	// JOURNALS are. A harness Store answers for the one tenant and keyspace it
	// owns and holds no registry of the bindings it serves, so selecting which
	// reader serves which session is the composition root's job; the tenant is
	// half of the key so a store serving another tenant's journal is refused
	// at the router rather than at the journal store, after the dispatch.
	JournalStores() map[host.EvidenceKey]sessionstore.DispositionEvidenceReader

	// Registrar produces the Department registrations this deployment serves.
	Registrar() host.Registrar

	// Checkpointer commits the checkpoints a nonterminal release requires.
	Checkpointer() host.Checkpointer

	// Auth verifies the service credential a Factory presents on every
	// HostLink connection. It is the product's because the identity provider
	// is; a placeholder that accepted anything would make every deployment
	// that forgot to replace it an open Host.
	Auth() host.AuthVerifier

	// Workspaces materializes and releases session workspaces, which is not
	// the session store's business and never was.
	Workspaces() host.Workspaces

	// NamespaceLayout answers the durable object prefix a session's runtime
	// writes under. The released store publishes no layout accessor and a
	// hydration with no namespace is refused in both modes, so a Host without
	// one can never attach a session — which is what v0.1.0's binary shipped
	// as, and why host.Compose requires it.
	NamespaceLayout() host.NamespaceLayout
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := Run(ctx, os.LookupEnv, unconfiguredBootstrap{}); err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
}

// Run reads the configuration, gathers the product's collaborators, and hands
// them to host.Run, which composes, serves and drains.
//
// CONFIGURATION IS REFUSED BEFORE THE BACKEND IS ASKED FOR. Opening a backend
// is the expensive, side-effecting step, and the message an operator wants
// about a malformed duration is about the variable they got wrong.
func Run(ctx context.Context, lookup Environment, bootstrap Bootstrap) error {
	config, err := LoadConfig(lookup)
	if err != nil {
		return err
	}
	backend, storeOptions, err := bootstrap.Backend(ctx)
	if err != nil {
		return fmt.Errorf("open storage backend: %w", err)
	}
	return host.Run(ctx, host.Composition{
		Options: host.Options{
			HostID:            config.HostID,
			InternalEndpoint:  config.InternalEndpoint,
			IsolationClass:    config.IsolationClass,
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
		},
		Generation: config.HostGeneration,
		Link: host.LinkOptions{
			PingInterval:       config.PingInterval,
			PongTimeout:        config.PongTimeout,
			MaxBindingsPerLink: config.MaxBindingsPerLink,
			MaxBindings:        config.MaxBindings,
			MaxTenantLinks:     config.MaxTenantLinks,
		},
		Drain: host.DrainOptions{
			Grace:        config.Grace,
			IdleBoundary: config.IdleBoundary,
			PublishBound: config.PublishBound,
		},
		CompatibilityTimeout: config.CompatibilityTimeout,
		WorkPoll:             config.WorkPoll,
		Collaborators: host.Collaborators{
			Backend:         backend,
			StoreOptions:    storeOptions,
			JournalStores:   bootstrap.JournalStores(),
			Registrar:       bootstrap.Registrar(),
			Checkpointer:    bootstrap.Checkpointer(),
			Auth:            bootstrap.Auth(),
			Workspaces:      bootstrap.Workspaces(),
			NamespaceLayout: bootstrap.NamespaceLayout(),
		},
	}, host.ListenOptions{Address: config.ListenAddress})
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

// Backend refuses.
func (unconfiguredBootstrap) Backend(context.Context) (*storage.Composite, []sessionstore.Option, error) {
	return nil, nil, errNoBootstrap
}

// JournalStores is one refusing reader, and it is ONE rather than none on
// purpose: an empty table is refused by the composition before the refusal
// this type exists to give. The registered reader refuses every request, which
// is the same answer one binding late.
func (b unconfiguredBootstrap) JournalStores() map[host.EvidenceKey]sessionstore.DispositionEvidenceReader {
	return map[host.EvidenceKey]sessionstore.DispositionEvidenceReader{{TenantID: "unconfigured", StorageBindingID: "unconfigured"}: b}
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
func (unconfiguredBootstrap) Auth() host.AuthVerifier { return unconfiguredBootstrap{} }

// Workspaces refuses.
func (unconfiguredBootstrap) Workspaces() host.Workspaces { return unconfiguredBootstrap{} }

// NamespaceLayout answers the empty prefix, which the composition's hydration
// refuses; the binary never gets that far.
func (unconfiguredBootstrap) NamespaceLayout() host.NamespaceLayout {
	return func(sessionwire.TenantID, sessionwire.SessionID) string { return "" }
}

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
