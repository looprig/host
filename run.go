package host

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// ListenOptions is where Run serves.
type ListenOptions struct {
	// Address is the TCP address to listen on. Ignored when Listener is set.
	Address string

	// Listener is an already-bound listener, for a platform that hands one in
	// or a test that needs to know the port. Run closes it on return.
	Listener net.Listener

	// ReadHeaderTimeout bounds a client's request headers; zero means ten
	// seconds. It is never unbounded: an unbounded header read is a slowloris.
	ReadHeaderTimeout time.Duration
}

// Run composes the Host, starts it, serves Routes until ctx is done or the
// listener fails, then drains within Drain.Grace and shuts the listener down.
//
// THE ORDER IS COMPOSE, START, SERVE, DRAIN, CLOSE, and each refusal happens
// as early as it can be made: a Composition that cannot be validated opens no
// store; one whose first advertisement the directory refuses fails at Start
// rather than running invisibly. The drain runs BEFORE the listener closes,
// because a Factory reads the bounded drain-status observation over HostLink
// and a listener closed at the signal would leave it inferring completion from
// a disconnect.
//
// A drain failure is returned as an error, one DrainFailure per failed step
// joined with the shutdown's own, because a process that drained with
// failures has left something for an operator to reconcile and must not exit
// zero. Run's caller decides whether that is fatal.
func Run(ctx context.Context, blueprint Composition, listen ListenOptions) error {
	service, err := Compose(ctx, blueprint)
	if err != nil {
		return err
	}
	if err := service.Start(ctx); err != nil {
		_, _ = service.Stop(context.WithoutCancel(ctx))
		return err
	}

	timeout := listen.ReadHeaderTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	server := &http.Server{
		Addr:              listen.Address,
		Handler:           service.Routes(),
		ReadHeaderTimeout: timeout,
	}
	listening := make(chan error, 1)
	go func() {
		if listen.Listener != nil {
			listening <- server.Serve(listen.Listener)
			return
		}
		listening <- server.ListenAndServe()
	}()

	select {
	case err := <-listening:
		if !errors.Is(err, http.ErrServerClosed) {
			_, _ = service.Stop(context.WithoutCancel(ctx))
			return err
		}
	case <-ctx.Done():
	}

	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blueprint.Drain.Grace)
	defer cancel()
	report, drainErr := service.Stop(drainCtx)
	shutdownErr := server.Shutdown(drainCtx)
	failures := make([]error, 0, len(report.Failures)+2)
	failures = append(failures, drainErr, shutdownErr)
	for _, failure := range report.Failures {
		failures = append(failures, failure)
	}
	return errors.Join(failures...)
}
