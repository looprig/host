package compose

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/host/internal/realtime/hostlink"
	"github.com/looprig/host/internal/residency"
	"github.com/looprig/host/internal/service"
)

type blockedPublishDirectory struct {
	*fakeDirectory
	entered chan struct{}
	release chan struct{}
	commit  bool
	err     error
}

func (d *blockedPublishDirectory) PublishTarget(ctx context.Context, row service.Advertisement) error {
	close(d.entered)
	<-d.release
	if d.commit {
		if err := d.fakeDirectory.PublishTarget(ctx, row); err != nil {
			return err
		}
	}
	return d.err
}

func TestCloseUnstartedRefusesConcurrentStartAndAllowsFailedStartDisposal(t *testing.T) {
	f := newFixture(t)
	failure := errors.New("publication failed")
	blocked := &blockedPublishDirectory{fakeDirectory: f.directory, entered: make(chan struct{}), release: make(chan struct{}), err: failure}
	f.svc.advertise.directory = blocked
	started := make(chan error, 1)
	go func() { started <- f.svc.Start(context.Background()) }()
	<-blocked.entered
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); !errors.Is(err, ErrServiceActive) {
		t.Fatalf("active Start: %v", err)
	}
	if _, err := f.svc.Stop(t.Context()); !errors.Is(err, ErrServiceActive) {
		t.Fatalf("Stop during Start: %v", err)
	}
	close(blocked.release)
	if err := <-started; !errors.Is(err, failure) {
		t.Fatalf("Start: %v", err)
	}
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(f.directory.publications()) != 0 {
		t.Fatal("failed publication committed")
	}
}

func TestPrestartDrainRefusalLeavesDisposalAvailable(t *testing.T) {
	f := newFixture(t)
	link, err := f.svc.links.resolve(tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := link.mux.Attach(t.Context(), f.attachRequest(tenantA, sessionA)); !errors.Is(err, ErrServiceNotStarted) {
		t.Fatalf("prestart HostLink Attach: %v", err)
	}
	if _, err := f.svc.Attach(t.Context(), residency.Request{
		TenantID: tenantA, SessionID: sessionA, AgentID: testAgent,
		Mode:      residency.ModeCreate,
		Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
	}); !errors.Is(err, ErrServiceNotStarted) {
		t.Fatalf("prestart Attach: %v", err)
	}
	if _, err := f.svc.StartDrain(hostlink.DrainScope{}); !errors.Is(err, ErrServiceNotStarted) {
		t.Fatalf("prestart drain: %v", err)
	}
	if _, err := f.svc.Stop(t.Context()); !errors.Is(err, ErrServiceNotStarted) {
		t.Fatalf("prestart Stop: %v", err)
	}
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := link.mux.Attach(t.Context(), f.attachRequest(tenantA, sessionA)); !errors.Is(err, ErrServiceDisposed) {
		t.Fatalf("HostLink Attach after disposal: %v", err)
	}
}

func TestCloseUnstartedLeavesCommittedAdvertisementToExpire(t *testing.T) {
	f := newFixture(t)
	failure := errors.New("reply lost")
	blocked := &blockedPublishDirectory{fakeDirectory: f.directory, entered: make(chan struct{}), release: make(chan struct{}), commit: true, err: failure}
	f.svc.advertise.directory = blocked
	started := make(chan error, 1)
	go func() { started <- f.svc.Start(context.Background()) }()
	<-blocked.entered
	close(blocked.release)
	if err := <-started; !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := len(f.directory.publications()); got != 1 {
		t.Fatalf("committed rows: %d", got)
	}
	if got := len(f.directory.withdrawals()); got != 0 {
		t.Fatalf("disposal withdrew %d rows", got)
	}
}

func TestCloseUnstartedCancellationOnlyBoundsCallerWait(t *testing.T) {
	f := newFixture(t)
	closing := make(chan struct{})
	release := make(chan struct{})
	firstCtx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		first <- f.svc.CloseUnstarted(firstCtx, func() error { close(closing); <-release; return nil })
	}()
	<-closing
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait: %v", err)
	}
	later := make(chan error, 1)
	go func() {
		later <- f.svc.CloseUnstarted(context.Background(), func() error { t.Error("second cleanup invoked"); return nil })
	}()
	select {
	case err := <-later:
		t.Fatalf("returned before cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-later; err != nil {
		t.Fatal(err)
	}
}

func TestCloseUnstartedRefusesInFlightAndResidentAttach(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.store.acquireEntered = make(chan struct{})
	f.store.acquireRelease = make(chan struct{})
	attached := make(chan error, 1)
	go func() {
		_, err := f.svc.Attach(context.Background(), residency.Request{
			TenantID: tenantA, SessionID: sessionA, AgentID: testAgent,
			Mode:      residency.ModeCreate,
			Principal: residency.Principal{TenantID: tenantA, ActorID: "actor-a"},
		})
		attached <- err
	}()
	<-f.store.acquireEntered
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); !errors.Is(err, ErrServiceActive) {
		t.Fatalf("during Attach: %v", err)
	}
	close(f.store.acquireRelease)
	if err := <-attached; err != nil {
		t.Fatal(err)
	}
	if err := f.svc.CloseUnstarted(t.Context(), func() error { return nil }); !errors.Is(err, ErrServiceActive) {
		t.Fatalf("with residency: %v", err)
	}
}
