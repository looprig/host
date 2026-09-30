package harnessruntime

import (
	"context"
	"errors"
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
