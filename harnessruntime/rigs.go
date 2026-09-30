package harnessruntime

import (
	"context"
	"errors"
	"maps"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/rig"

	"github.com/looprig/host/department"
)

// SharedRig resolves every launch, create and restore alike, onto one rig.
//
// It is the right shape only when nothing about the rig varies per launch:
// the rig's workspace root and journal store are fixed by rig.Define (H1), so
// a product that materializes a workspace per session, or journals each tenant
// into its own store, wants RigPerTenant or RigsFunc instead.
func SharedRig(r *rig.Rig) Rigs {
	return RigsFunc(
		func(context.Context, department.RigCreateRequest) (Launcher, error) { return launcherOf(r) },
		func(context.Context, uuid.UUID, department.RigRestoreRequest) (Launcher, error) { return launcherOf(r) },
	)
}

// RigPerTenant resolves each launch onto the rig registered for the request's
// tenant, and refuses a tenant it does not know with ErrUnknownTenant. The map
// is copied, so a later write to the caller's map changes nothing.
//
// It is the shape a product with one harness journal store per tenant needs:
// each rig is defined with rig.WithSessionStore over that tenant's store, and
// a session can never be launched onto another tenant's journal.
func RigPerTenant(rigs map[sessionwire.TenantID]*rig.Rig) Rigs {
	table := maps.Clone(rigs)
	lookup := func(tenant sessionwire.TenantID) (Launcher, error) {
		r, ok := table[tenant]
		if !ok {
			return nil, &UnknownTenantError{TenantID: tenant}
		}
		return launcherOf(r)
	}
	return RigsFunc(
		func(_ context.Context, request department.RigCreateRequest) (Launcher, error) {
			return lookup(request.TenantID)
		},
		func(_ context.Context, _ uuid.UUID, request department.RigRestoreRequest) (Launcher, error) {
			return lookup(request.TenantID)
		},
	)
}

// RigsFunc adapts a pair of functions to Rigs. Both are required; New refuses
// a RigsFunc missing either with ErrNoRigs.
func RigsFunc(
	create func(context.Context, department.RigCreateRequest) (Launcher, error),
	restore func(context.Context, uuid.UUID, department.RigRestoreRequest) (Launcher, error),
) Rigs {
	return rigsFunc{create: create, restore: restore}
}

type rigsFunc struct {
	create  func(context.Context, department.RigCreateRequest) (Launcher, error)
	restore func(context.Context, uuid.UUID, department.RigRestoreRequest) (Launcher, error)
}

func (f rigsFunc) RigForCreate(ctx context.Context, request department.RigCreateRequest) (Launcher, error) {
	return f.create(ctx, request)
}

func (f rigsFunc) RigForRestore(ctx context.Context, id uuid.UUID, request department.RigRestoreRequest) (Launcher, error) {
	return f.restore(ctx, id, request)
}

// launcherOf widens a rig to a Launcher, refusing a nil one with ErrNoRig
// rather than handing back an interface that holds a nil pointer.
func launcherOf(r *rig.Rig) (Launcher, error) {
	if r == nil {
		return nil, ErrNoRig
	}
	return r, nil
}

// ErrUnknownTenant is the cause RigPerTenant reports for a tenant it has no rig
// for. Reach it with errors.Is; the error itself is an *UnknownTenantError.
var ErrUnknownTenant = errors.New("harnessruntime: no rig is registered for the tenant")

// UnknownTenantError names the tenant RigPerTenant could not resolve.
type UnknownTenantError struct {
	TenantID sessionwire.TenantID
}

func (e *UnknownTenantError) Error() string {
	return "harnessruntime: no rig is registered for tenant " + strconv.Quote(string(e.TenantID))
}

// Is makes an *UnknownTenantError match ErrUnknownTenant.
func (e *UnknownTenantError) Is(target error) bool { return target == ErrUnknownTenant }
