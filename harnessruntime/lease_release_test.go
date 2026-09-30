package harnessruntime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
)

// leaseFailingController is a full controller whose release and abandon both
// tear down but report the journal lease still held, as harness v0.45.0 does.
type leaseFailingController struct {
	*fullController
}

var errLeaseDown = errors.New("lease store down")

func (leaseFailingController) ReleaseResidency(context.Context) error {
	return &session.LeaseReleaseError{Lease: errLeaseDown}
}

func (leaseFailingController) AbandonResidency(context.Context) error {
	return fmtWrap(&session.LeaseReleaseError{Lease: errLeaseDown})
}

func fmtWrap(err error) error { return errors.Join(errors.New("session: abandon"), err) }

// HARNESS'S CONSUMER OBLIGATION (v0.45.0): a *session.LeaseReleaseError from
// ReleaseResidency or AbandonResidency means the residency is STILL HELD. The
// bound session says so in Host's vocabulary, department.ErrResidencyStillHeld,
// and keeps harness's error reachable.
func TestALeaseReleaseErrorIsReportedAsResidencyStillHeld(t *testing.T) {
	controller := leaseFailingController{fullController: newFullController(newFakeSubscription(nil), nil, nil)}
	bound := boundFor(t, controller)
	for name, call := range map[string]func() error{
		"release": func() error { return bound.(department.Releaser).ReleaseResidency(t.Context()) },
		"abandon": func() error { return bound.(department.PersistenceFaults).AbandonResidency(t.Context()) },
	} {
		err := call()
		var released *session.LeaseReleaseError
		if !errors.Is(err, department.ErrResidencyStillHeld) || !errors.As(err, &released) || !errors.Is(err, errLeaseDown) {
			t.Errorf("%s = %v, want ErrResidencyStillHeld with the LeaseReleaseError and its cause", name, err)
		}
	}
}

// joiningController models harness v0.45.0's teardown: the first call begins
// the teardown (liveness ends), finishes it whatever its context says, and then
// reports the context's error beside a clean cleanup; a second call JOINS the
// teardown and answers with its own cleanup result.
type joiningController struct {
	*fullController
	joinResult error
	releases   int
	abandons   int
}

func (c *joiningController) begin(ctx context.Context) error {
	close(c.livenessPart.done)
	<-ctx.Done()
	return fmt.Errorf("session: context done: %w", ctx.Err())
}

func (c *joiningController) ReleaseResidency(ctx context.Context) error {
	c.releases++
	if c.releases == 1 {
		return c.begin(ctx)
	}
	return c.joinResult
}

func (c *joiningController) AbandonResidency(ctx context.Context) error {
	c.abandons++
	if c.abandons == 1 {
		return c.begin(ctx)
	}
	return c.joinResult
}

// A HARNESS TEARDOWN THAT FINISHED AFTER ITS CALLER'S DEADLINE IS REPORTED BY
// ITS OWN OUTCOME. The adapter knows harness answers a second call on a begun
// teardown with a join, so it reads the real cleanup result that way; Host's
// runtime-neutral teardown never makes a second call itself.
func TestATeardownPastItsDeadlineReportsItsOwnOutcome(t *testing.T) {
	for _, row := range []struct {
		name string
		join error
		want func(error) bool
	}{
		{"clean", nil, func(err error) bool { return err == nil }},
		{"lease kept", &session.LeaseReleaseError{Lease: errLeaseDown}, func(err error) bool {
			return errors.Is(err, department.ErrResidencyStillHeld)
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			for _, call := range []string{"release", "abandon"} {
				controller := &joiningController{fullController: newFullController(newFakeSubscription(nil), nil, nil), joinResult: row.join}
				bound := boundFor(t, controller)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				var err error
				if call == "release" {
					err = bound.(department.Releaser).ReleaseResidency(ctx)
				} else {
					err = bound.(department.PersistenceFaults).AbandonResidency(ctx)
				}
				if !row.want(err) {
					t.Errorf("%s past its deadline = %v", call, err)
				}
			}
		})
	}

	t.Run("a refusal before the teardown began is not joined", func(t *testing.T) {
		controller := &refusingController{fullController: newFullController(newFakeSubscription(nil), nil, nil)}
		bound := boundFor(t, controller)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := bound.(department.Releaser).ReleaseResidency(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("release = %v, want the refusal", err)
		}
		if controller.calls != 1 {
			t.Fatalf("called %d times, want once: nothing began, so there is nothing to join", controller.calls)
		}
	})
}

// refusingController refuses a release on its context (the idle wait) without
// beginning any teardown.
type refusingController struct {
	*fullController
	calls int
}

func (c *refusingController) ReleaseResidency(ctx context.Context) error {
	c.calls++
	return fmt.Errorf("not idle: %w", ctx.Err())
}
