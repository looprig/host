package host_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host"
	"github.com/looprig/sessionstore"
)

type delayedProviderClose struct {
	entered chan struct{}
	release chan struct{}
}

func (c *delayedProviderClose) Close(context.Context) error {
	close(c.entered)
	<-c.release
	return nil
}

func TestCloseUnstartedClosesComposedServiceWithoutPublishing(t *testing.T) {
	f := newComposeFixture(t)
	service, err := host.Compose(t.Context(), f.blueprint(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CloseUnstarted(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.CloseUnstarted(t.Context()); err != nil {
		t.Fatal(err)
	}
	if service.Ready() || service.Live() {
		t.Fatal("disposed host is active")
	}
	if err := service.Start(t.Context()); !errors.Is(err, host.ErrServiceDisposed) {
		t.Fatalf("Start after dispose: %v", err)
	}
	if _, err := service.Stop(context.Background()); !errors.Is(err, host.ErrServiceDisposed) {
		t.Fatalf("Stop after dispose: %v", err)
	}
	if _, err := service.Attach(t.Context(), host.AttachRequest{Mode: sessionwire.HostLinkAttachModeCreate}); !errors.Is(err, host.ErrServiceDisposed) {
		t.Fatalf("Attach after dispose: %v", err)
	}
	// Compose owns its SessionStore, not the caller's Storage backend.
	other := f.otherParty(t)
	_ = other
}

func TestCloseUnstartedRefusesStartedService(t *testing.T) {
	f := newComposeFixture(t)
	service, err := host.Compose(t.Context(), f.blueprint(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.CloseUnstarted(t.Context()); !errors.Is(err, host.ErrServiceActive) {
		t.Fatalf("CloseUnstarted after Start: %v", err)
	}
	if !service.Ready() {
		t.Fatal("refused disposal changed readiness")
	}
	if _, err := service.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseUnstartedCallerCancellationDoesNotCancelOwnedStoreClose(t *testing.T) {
	f := newComposeFixture(t)
	closer := &delayedProviderClose{entered: make(chan struct{}), release: make(chan struct{})}
	blueprint := f.blueprint(t)
	blueprint.Collaborators.StoreOptions = append(blueprint.Collaborators.StoreOptions, sessionstore.WithProviderOwnership(closer))
	service, err := host.Compose(t.Context(), blueprint)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() { first <- service.CloseUnstarted(ctx) }()
	<-closer.entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait: %v", err)
	}
	later := make(chan error, 1)
	go func() { later <- service.CloseUnstarted(context.Background()) }()
	select {
	case err := <-later:
		t.Fatalf("returned before owned store closed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(closer.release)
	if err := <-later; err != nil {
		t.Fatal(err)
	}
	f.otherParty(t) // The caller's backend remains usable after the owned store closes.
}
