package harnessruntime

import (
	sessionwire "github.com/looprig/core/sessionwire/v1"

	"github.com/looprig/host/department"
)

// Target builds the launch target a harness-backed product registers with
// Host: department.NewRigTarget over New(rigs, options...), with
// capabilities.Recovery = {AttemptCloser: true, PersistenceFaults: true} forced
// on.
//
// THE DECLARATION IS FORCED BECAUSE THE ADAPTER HONOURS IT BY CONSTRUCTION.
// Every session it binds must be a session.PersistenceFaultReporter and a
// session.ResidencyAbandoner or the launch is refused, and every bound session
// carries CloseAttempt, forwarding to harness's runtimecommand.AttemptCloser.
// So a Target always passes host.Compose's RuntimeProfileDurable check, and the
// department's launch-time verification can never find a declared capability
// absent. Whatever the caller put in capabilities.Recovery is overwritten.
//
// The compatibility id is the product's: it names the runtime build, and a
// restore onto a different one is refused before launch.
func Target(
	rigs Rigs,
	compatibility department.CompatibilityID,
	capabilities department.Capabilities,
	options ...Option,
) (department.LaunchTarget, error) {
	adapter, err := New(rigs, options...)
	if err != nil {
		return nil, err
	}
	capabilities.Recovery = department.Recovery{AttemptCloser: true, PersistenceFaults: true}
	return department.NewRigTarget(adapter, compatibility, capabilities)
}

// Registration is the one-line Registrar entry a single-agent product needs.
//
//	target, err := harnessruntime.Target(harnessruntime.SharedRig(r), "my-build-1", caps)
//	...
//	Registrar: host.RegistrarFunc(func(context.Context) ([]department.Registration, error) {
//	    return []department.Registration{harnessruntime.Registration("assistant", target)}, nil
//	}),
func Registration(agent sessionwire.AgentID, target department.LaunchTarget) department.Registration {
	return department.Registration{AgentID: agent, Target: target}
}
