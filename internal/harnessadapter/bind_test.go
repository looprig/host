package harnessadapter

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/session"

	"github.com/looprig/host/department"
)

const (
	testTenant  = sessionwire.TenantID("tenant-a")
	testSession = sessionwire.SessionID("session-a")
)

// capabilities is what department.adaptRigSession discovers on a RigSession by
// assertion. Asserting the bound value against it here is the same question
// department will ask, spelled once.
type capabilities interface {
	department.RigSession
	department.IdleWaiter
	department.Liveness
	department.Releaser
	department.PublicationSubscriber
	department.CommandApplier
	department.LeaseEpochReporter
}

// The bound session carries every capability department discovers, so a rig
// session adapted here reaches department.Runtime rather than being refused with
// IncapableRuntimeError. This is a build failure if a method is dropped.
var _ capabilities = (*boundSession)(nil)

func newAdapter(t *testing.T, rigs Rigs, options ...Option) *Adapter {
	t.Helper()
	adapted, err := New(rigs, options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return adapted
}

// ---------------------------------------------------------------------------
// bind
// ---------------------------------------------------------------------------

// A session carrying every capability binds, and each capability is REACHED
// rather than merely discovered: the test drives all four through the bound
// value, so a bind that stored the wrong field would fail here.
func TestBindReachesEveryDiscoveredCapability(t *testing.T) {
	released := 0
	var filters []event.EventFilter
	subscription := newFakeSubscription(nil)
	controller := newFullController(subscription, &filters, &released)

	adapter := newAdapter(t, stubRigs{})
	bound, err := adapter.bind(controller, testTenant, testSession)
	if err != nil {
		t.Fatalf("bind a fully capable session: %v", err)
	}

	if bound.ID() != controller.SessionID() {
		t.Fatalf("ID = %v, want the controller's %v", bound.ID(), controller.SessionID())
	}

	// EVERY FIELD IS ASSERTED BEFORE IT IS DEREFERENCED, and that is discipline
	// rather than belt-and-braces. A bind that stopped populating one leaves a
	// nil capability, and the drive-through below would then die by PANIC — which
	// is not an assertion kill, and which aborts the package so the sibling test
	// that WOULD have caught it never runs. Measured: removing bind's idle arm
	// panicked here and masked two real assertion kills.
	adapted, ok := bound.(*boundSession)
	if !ok {
		t.Fatalf("bind returned %T, want *boundSession", bound)
	}
	for name, populated := range map[string]bool{
		"idle":      adapted.idle != nil,
		"live":      adapted.live != nil,
		"releaser":  adapted.releaser != nil,
		"committed": adapted.committed != nil,
	} {
		if !populated {
			t.Fatalf("bind left the %s capability nil on a fully capable session", name)
		}
	}

	runtime, ok := bound.(capabilities)
	if !ok {
		t.Fatalf("the bound session is %T, which department could not adapt", bound)
	}
	if err := runtime.WaitIdle(t.Context()); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	select {
	case <-runtime.Done():
		t.Fatal("Done was already closed")
	default:
	}
	if err := runtime.ReleaseResidency(t.Context()); err != nil {
		t.Fatalf("ReleaseResidency: %v", err)
	}
	if released != 1 {
		t.Fatalf("ReleaseResidency reached the controller %d times, want once", released)
	}
	if _, err := runtime.SubscribeCommitted(t.Context(), ""); err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}
	if len(filters) != 1 {
		t.Fatalf("the controller saw %d subscriptions, want one", len(filters))
	}
}

// EACH CAPABILITY IS INDEPENDENTLY REQUIRED, and the space is derived from the
// composition rather than picked: a controller is assembled from the base plus
// every part EXCEPT one, and the omitted part is the one that must be named.
func TestBindRefusesASessionShortOfAnyCapability(t *testing.T) {
	for _, tt := range []struct {
		omit    string
		build   func() session.SessionController
		missing string
	}{
		{
			omit:    "WaitIdle",
			missing: "session.IdleWaiter",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					faultsPart
					livenessPart
					releaserPart
					committedPart
					leaseEpochPart
				}{committedPart: committedPart{available: true}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			omit:    "Done",
			missing: "session.Liveness",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					faultsPart
					idlePart
					releaserPart
					committedPart
					leaseEpochPart
				}{committedPart: committedPart{available: true}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			omit:    "ReleaseResidency",
			missing: "session.Releaser",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					faultsPart
					idlePart
					livenessPart
					committedPart
					leaseEpochPart
				}{committedPart: committedPart{available: true}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			omit:    "CommittedPublicEvents",
			missing: "session.CommittedPublicEventProvider",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					faultsPart
					idlePart
					livenessPart
					releaserPart
					leaseEpochPart
				}{leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			// D3. A faulted runtime that Host cannot see is a session wedged for
			// good, so the fault report is required of every harness session.
			omit:    "PersistenceFaulted",
			missing: "session.PersistenceFaultReporter",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					idlePart
					livenessPart
					releaserPart
					committedPart
					leaseEpochPart
					abandonerOnly
				}{committedPart: committedPart{available: true}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			// D3. Without the crash-equivalent release a faulted runtime could
			// only be shut down, which would make the session terminal.
			omit:    "AbandonResidency",
			missing: "session.ResidencyAbandoner",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					idlePart
					livenessPart
					releaserPart
					committedPart
					leaseEpochPart
					reporterOnly
				}{committedPart: committedPart{available: true}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}
			},
		},
		{
			// O3.4. The epoch an admitted command names is the RUNTIME's grant,
			// read through this capability, so a controller that cannot report
			// one is refused at bind rather than at the first command — which is
			// after Host has published the residency route.
			omit:    "LeaseEpoch",
			missing: "session.LeaseEpochReporter",
			build: func() session.SessionController {
				return &struct {
					controllerBase
					faultsPart
					idlePart
					livenessPart
					releaserPart
					committedPart
				}{committedPart: committedPart{available: true}}
			},
		},
	} {
		t.Run(tt.omit, func(t *testing.T) {
			adapter := newAdapter(t, stubRigs{})
			_, err := adapter.bind(tt.build(), testTenant, testSession)
			var incapable *IncapableSessionError
			if !errors.As(err, &incapable) {
				t.Fatalf("bind without %s = %v, want IncapableSessionError", tt.omit, err)
			}
			if !slices.Contains(incapable.Missing, tt.missing) {
				t.Fatalf("Missing = %v, want it to name %q", incapable.Missing, tt.missing)
			}
			if len(incapable.Missing) != 1 {
				t.Fatalf("Missing = %v, want exactly the omitted capability", incapable.Missing)
			}
		})
	}
}

// M1's SECOND HALF, AND THE REASON THE TWO-RESULT FORM IS NOT A STYLE CHOICE.
// A session that declares SubscribeCommittedPublicEvents but reports the
// capability unavailable is exactly harness's headless case, and the live
// runtime declares that method unconditionally — so a bare assertion on
// session.CommittedPublicEventSource is vacuously true for every real session
// and this refusal never happens. It must happen at bind, before Host publishes
// a residency route a client would then attach to.
func TestBindRefusesASessionWhoseCommittedEventsAreUnavailable(t *testing.T) {
	controller := &struct {
		controllerBase
		faultsPart
		idlePart
		livenessPart
		releaserPart
		committedPart
		leaseEpochPart
	}{committedPart: committedPart{available: false}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}

	// THE CONTROL: the same value DOES satisfy the source interface, so an
	// adapter asserting on the source would have bound it.
	if _, ok := session.SessionController(controller).(session.CommittedPublicEventSource); !ok {
		t.Fatal("the fixture does not satisfy CommittedPublicEventSource, so it cannot show what a bare assertion would accept")
	}

	adapter := newAdapter(t, stubRigs{})
	_, err := adapter.bind(controller, testTenant, testSession)
	var incapable *IncapableSessionError
	if !errors.As(err, &incapable) {
		t.Fatalf("bind of a headless session = %v, want IncapableSessionError", err)
	}
	if len(incapable.Missing) != 1 || !strings.Contains(incapable.Missing[0], "committed public events") {
		t.Fatalf("Missing = %v, want the unavailable committed-event capability", incapable.Missing)
	}
}

// A provider that reports available and hands back a nil source is refused too.
// The two results are independent and a caller that trusted only the boolean
// would dereference nothing on the first subscribe.
func TestBindRefusesANilCommittedEventSource(t *testing.T) {
	controller := &struct {
		controllerBase
		faultsPart
		idlePart
		livenessPart
		releaserPart
		nilSourceProvider
		leaseEpochPart
	}{leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}

	adapter := newAdapter(t, stubRigs{})
	if _, err := adapter.bind(controller, testTenant, testSession); err == nil {
		t.Fatal("bind accepted a provider that reported available with a nil source")
	}
}

// A session short of everything names everything, rather than the first thing.
func TestBindNamesEveryMissingCapabilityAtOnce(t *testing.T) {
	adapter := newAdapter(t, stubRigs{})
	_, err := adapter.bind(controllerBase{}, testTenant, testSession)
	var incapable *IncapableSessionError
	if !errors.As(err, &incapable) {
		t.Fatalf("bind of a bare controller = %v, want IncapableSessionError", err)
	}
	if len(incapable.Missing) != 7 {
		t.Fatalf("Missing = %v, want all seven capabilities", incapable.Missing)
	}
	if !strings.Contains(incapable.Error(), "session.IdleWaiter") {
		t.Fatalf("Error() = %q, want it to name the missing capabilities", incapable.Error())
	}
}

// BOTH WAYS A LAUNCH CAN HAND BACK NO SESSION, and the typed nil is the one the
// released rig actually produces: rig.newSession assigns a concrete
// *sessionruntime.Session and widens it on return, so a lifecycle reporting
// success with no session yields a non-nil interface holding a nil pointer.
//
// THE TYPED-NIL ROW IS NOT ABOUT A NICER ERROR. Every capability assertion below
// SUCCEEDS on a typed nil — a nil *nilSessionController satisfies each interface
// — so a bind that only compared against nil would return a usable-looking
// boundSession, Host would publish the residency route, and the panic would
// arrive at the first ID() call against a session it had already advertised.
func TestBindRefusesEveryWayALaunchHandsBackNoSession(t *testing.T) {
	for _, tt := range []struct {
		name       string
		controller session.SessionController
	}{
		{name: "nil interface", controller: nil},
		{name: "typed nil", controller: (*nilSessionController)(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			adapter := newAdapter(t, stubRigs{})
			bound, err := adapter.bind(tt.controller, testTenant, testSession)
			if !errors.Is(err, ErrNoSession) {
				t.Fatalf("bind(%s) = (%v, %v), want ErrNoSession", tt.name, bound, err)
			}
			if bound != nil {
				t.Fatalf("bind(%s) returned a session alongside its refusal", tt.name)
			}
		})
	}
}

// TestATypedNilControllerPassesEveryGateInBind is the premise of the typed-nil
// row, asserted rather than assumed.
//
// IT CHECKS THE FOURTH GATE BY CALLING IT, and an earlier version did not — it
// checked four type ASSERTIONS, and bind's fourth gate is not one. That gap was
// not theoretical: the fixture answered (nil, false), so under the mutation the
// row was meant to kill, bind refused the typed nil as INCAPABLE and the failure
// message named a missing capability rather than an absent session. The mutant
// died for the wrong reason and the one test written to exclude that could not
// see it, because it never ran the call bind runs.
//
// So the assertions and the capability are checked separately below. If either
// stops holding, the typed-nil row stops proving anything about isNil and this
// says so directly instead of leaving it to be inferred from a message nobody
// reads.
func TestATypedNilControllerPassesEveryGateInBind(t *testing.T) {
	// THE VALUE ARRIVES THROUGH A SLICE so its concrete type is not statically
	// known at the comparison. Written as a direct assignment the compiler folds
	// `controller == nil` to false and staticcheck reports the comparison as
	// never true (SA4023) — which is correct about the code and wrong about the
	// point: the property under test is precisely that a non-nil INTERFACE can
	// hold a nil pointer, and it has to be evaluated rather than constant-folded.
	controller := []session.SessionController{(*nilSessionController)(nil)}[0]
	if controller == nil {
		t.Fatal("the fixture is a nil interface, so it cannot show what a bare comparison misses")
	}
	for name, satisfied := range map[string]bool{
		"session.IdleWaiter": func() bool { _, ok := controller.(session.IdleWaiter); return ok }(),
		"session.Liveness":   func() bool { _, ok := controller.(session.Liveness); return ok }(),
		"session.Releaser":   func() bool { _, ok := controller.(session.Releaser); return ok }(),
		"session.CommittedPublicEventProvider": func() bool {
			_, ok := controller.(session.CommittedPublicEventProvider)
			return ok
		}(),
	} {
		if !satisfied {
			t.Fatalf("a typed nil does not satisfy %s, so bind would refuse it as incapable rather than as absent", name)
		}
	}

	// THE FOURTH GATE, RUN THE WAY bind RUNS IT. Satisfying
	// CommittedPublicEventProvider is not enough: bind calls the method and
	// refuses on either result, so a fixture that asserts but answers
	// (nil, false) fails at this gate and never reaches the guard under test.
	provider, ok := controller.(session.CommittedPublicEventProvider)
	if !ok {
		t.Fatal("the fixture does not provide committed public events")
	}
	source, available := provider.CommittedPublicEvents()
	if !available {
		t.Fatal("the fixture reports committed public events unavailable, so bind refuses it as incapable and the typed-nil row proves nothing about isNil")
	}
	if source == nil {
		t.Fatal("the fixture reports the capability available with a nil source, which bind refuses for the same reason")
	}
}

// A LAUNCH THAT PRODUCES A TYPED NIL IS REFUSED BEFORE ANYTHING IS PUBLISHED.
// This is the same guard reached through the seam a real rig arrives on, rather
// than by calling bind directly.
func TestLaunchRefusesARigThatReturnsATypedNilSession(t *testing.T) {
	launcher := &fakeLauncher{controller: (*nilSessionController)(nil)}
	adapter := newAdapter(t, stubRigs{launcher: launcher})

	if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, ErrNoSession) {
		t.Fatalf("NewSession = %v, want ErrNoSession", err)
	}
	if _, err := adapter.RestoreSession(t.Context(), uuid.UUID{}, department.RigRestoreRequest{}); !errors.Is(err, ErrNoSession) {
		t.Fatalf("RestoreSession = %v, want ErrNoSession", err)
	}
}

// ---------------------------------------------------------------------------
// NewSession and RestoreSession
// ---------------------------------------------------------------------------

func TestNewSessionResolvesLaunchesAndBinds(t *testing.T) {
	created := 0
	var creates []department.RigCreateRequest
	launcher := &fakeLauncher{controller: newFullController(newFakeSubscription(nil), nil, nil), created: &created}
	adapter := newAdapter(t, stubRigs{launcher: launcher, creates: &creates})

	request := department.RigCreateRequest{
		TenantID:      testTenant,
		SessionID:     testSession,
		AgentID:       sessionwire.AgentID("agent-a"),
		Placement:     sessionwire.HostPlacementPooled,
		WorkspaceRoot: "/w/a",
		Storage:       department.StorageContext{Namespace: "objects/a"},
	}
	launched, err := adapter.NewSession(t.Context(), request)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if launched == nil {
		t.Fatal("NewSession returned no session")
	}
	if created != 1 {
		t.Fatalf("the launcher was called %d times, want once", created)
	}
	// H1: the whole request reaches the RESOLVER, which is the only seam that
	// can honour a workspace root or a storage namespace, because rig.NewSession
	// accepts neither.
	if len(creates) != 1 || creates[0] != request {
		t.Fatalf("the resolver saw %+v, want the whole request %+v", creates, request)
	}
}

func TestRestoreSessionForwardsTheHarnessIdentityAndTheWholeRequest(t *testing.T) {
	var restored []uuid.UUID
	var restores []department.RigRestoreRequest
	launcher := &fakeLauncher{controller: newFullController(newFakeSubscription(nil), nil, nil), restored: &restored}
	adapter := newAdapter(t, stubRigs{launcher: launcher, restores: &restores})

	id := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	request := department.RigRestoreRequest{
		TenantID:      testTenant,
		SessionID:     testSession,
		AgentID:       sessionwire.AgentID("agent-a"),
		WorkspaceRoot: "/w/a",
		Storage:       department.StorageContext{Namespace: "objects/a"},
	}
	if _, err := adapter.RestoreSession(t.Context(), id, request); err != nil {
		t.Fatalf("RestoreSession: %v", err)
	}
	if len(restored) != 1 || restored[0] != id {
		t.Fatalf("the launcher restored %v, want %v", restored, id)
	}
	// H2: the request reaches the resolver and NOTHING past it. That is the drop
	// this adapter reports rather than hides.
	if len(restores) != 1 || restores[0] != request {
		t.Fatalf("the resolver saw %+v, want the whole request", restores)
	}
}

func TestLaunchReportsAResolverFailure(t *testing.T) {
	sentinel := errors.New("no rig for this agent")
	adapter := newAdapter(t, stubRigs{err: sentinel})

	if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, sentinel) {
		t.Fatalf("NewSession = %v, want the resolver's error", err)
	}
	if _, err := adapter.RestoreSession(t.Context(), uuid.UUID{}, department.RigRestoreRequest{}); !errors.Is(err, sentinel) {
		t.Fatalf("RestoreSession = %v, want the resolver's error", err)
	}
}

// BOTH WAYS A RESOLVER CAN HAND BACK NOTHING. The typed nil is the one a real
// implementation produces and the one a bare `== nil` walks past.
func TestLaunchRefusesAResolverThatReportsSuccessWithNoRig(t *testing.T) {
	for _, tt := range []struct {
		name     string
		launcher Launcher
	}{
		{name: "nil interface", launcher: nil},
		{name: "typed nil", launcher: (*fakeLauncher)(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			adapter := newAdapter(t, stubRigs{launcher: tt.launcher})
			if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, ErrNoRig) {
				t.Fatalf("NewSession = %v, want ErrNoRig", err)
			}
			if _, err := adapter.RestoreSession(t.Context(), uuid.UUID{}, department.RigRestoreRequest{}); !errors.Is(err, ErrNoRig) {
				t.Fatalf("RestoreSession = %v, want ErrNoRig", err)
			}
		})
	}
}

func TestLaunchReportsALaunchFailure(t *testing.T) {
	sentinel := errors.New("the rig refused to launch")
	adapter := newAdapter(t, stubRigs{launcher: &fakeLauncher{err: sentinel}})

	if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, sentinel) {
		t.Fatalf("NewSession = %v, want the launch error", err)
	}
	if _, err := adapter.RestoreSession(t.Context(), uuid.UUID{}, department.RigRestoreRequest{}); !errors.Is(err, sentinel) {
		t.Fatalf("RestoreSession = %v, want the launch error", err)
	}
}

func TestLaunchRefusesARigThatReportsSuccessAndReturnsNoSession(t *testing.T) {
	adapter := newAdapter(t, stubRigs{launcher: &fakeLauncher{controller: nil}})
	if _, err := adapter.NewSession(t.Context(), department.RigCreateRequest{}); !errors.Is(err, ErrNoSession) {
		t.Fatalf("NewSession = %v, want ErrNoSession", err)
	}
}

// ---------------------------------------------------------------------------
// SubscribeCommitted and pump
// ---------------------------------------------------------------------------

func boundFor(t *testing.T, controller session.SessionController, options ...Option) capabilities {
	t.Helper()
	adapter := newAdapter(t, stubRigs{}, options...)
	launched, err := adapter.bind(controller, testTenant, testSession)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	runtime, ok := launched.(capabilities)
	if !ok {
		t.Fatalf("the bound session is %T, which department could not adapt", launched)
	}
	return runtime
}

// H3, MEASURED. A positioned resume is refused rather than silently served from
// the live tail, and the refusal happens BEFORE any subscription is opened —
// otherwise the caller would be charged for a stream it cannot use.
func TestSubscribeCommittedRefusesAPositionedResume(t *testing.T) {
	var filters []event.EventFilter
	controller := newFullController(newFakeSubscription(nil), &filters, nil)
	runtime := boundFor(t, controller)

	published, err := runtime.SubscribeCommitted(t.Context(), sessionwire.EventID("event-7"))
	if !errors.Is(err, ErrResumeUnsupported) {
		t.Fatalf("SubscribeCommitted after an event = %v, want ErrResumeUnsupported", err)
	}
	if published != nil {
		t.Fatal("a refused subscription returned a channel")
	}
	if len(filters) != 0 {
		t.Fatalf("a refused subscription still opened %d streams", len(filters))
	}

	// THE OTHER DIRECTION: the empty resume point is served.
	if _, err := runtime.SubscribeCommitted(t.Context(), ""); err != nil {
		t.Fatalf("SubscribeCommitted from the live tail: %v", err)
	}
	if len(filters) != 1 {
		t.Fatalf("the live subscription opened %d streams, want one", len(filters))
	}
	if !filters[0].Enduring.All {
		t.Fatalf("the filter is %+v, want every loop's enduring events", filters[0])
	}
}

func TestSubscribeCommittedReportsASubscribeFailure(t *testing.T) {
	sentinel := errors.New("the hub cannot serve committed public events")
	controller := &struct {
		controllerBase
		faultsPart
		idlePart
		livenessPart
		releaserPart
		committedPart
		leaseEpochPart
	}{committedPart: committedPart{available: true, subscribeErr: sentinel}, leaseEpochPart: leaseEpochPart{epoch: 7, held: true}}

	runtime := boundFor(t, controller)
	if _, err := runtime.SubscribeCommitted(t.Context(), ""); !errors.Is(err, sentinel) {
		t.Fatalf("SubscribeCommitted = %v, want the hub's error", err)
	}
}

// H4, MEASURED. A delivery that is not a committed publication is DROPPED, and
// the discriminator is a delivery that would otherwise arrive with a zero
// EventID — a value Core's EnduringPublication does not accept. The committed
// one that follows it proves the pump did not simply stop.
func TestPumpDropsEveryDeliveryThatIsNotACommittedPublication(t *testing.T) {
	subscription := newFakeSubscription(nil)
	controller := newFullController(subscription, nil, nil)
	runtime := boundFor(t, controller)

	published, err := runtime.SubscribeCommitted(t.Context(), "")
	if err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}

	// An ephemeral delivery: never persisted, never sequenced, no public body.
	subscription.deliveries <- event.Delivery{}
	// An enduring delivery from a hub whose appender cannot report the stored
	// bytes: sequenced, but with none of the three publication members.
	subscription.deliveries <- event.Delivery{JournalSeq: 11}
	// A committed publication. THE TWO SEQUENCES DIFFER DELIBERATELY. Harness
	// documents CoveredThrough as equal to this append's own JournalSeq, so a
	// realistic fixture makes the two indistinguishable and either could be
	// carried into the other's field undetected — measured: both swaps survived.
	// A consumer joining a durable tail to this live stream branches on the pair,
	// so they are given different values here and asserted separately.
	subscription.deliveries <- event.Delivery{
		JournalSeq:     12,
		EventID:        "event-12",
		PublicBody:     []byte(`{"type":"step_done"}`),
		CoveredThrough: 11,
	}

	select {
	case publication := <-published:
		if publication.EventID != sessionwire.EventID("event-12") {
			t.Fatalf("the first publication is %q, want the committed one; an uncommitted delivery crossed", publication.EventID)
		}
		if publication.JournalSeq != 12 {
			t.Fatalf("publication JournalSeq = %d, want 12", publication.JournalSeq)
		}
		if publication.CoveredThrough != 11 {
			t.Fatalf("publication CoveredThrough = %d, want 11; it is a separate member and must not be taken from JournalSeq",
				publication.CoveredThrough)
		}
		if publication.TenantID != testTenant || publication.SessionID != testSession {
			t.Fatalf("publication identities are %q/%q, want the bound pair", publication.TenantID, publication.SessionID)
		}
		if string(publication.Body) != `{"type":"step_done"}` {
			t.Fatalf("publication body = %q, want the committed bytes", publication.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no publication arrived")
	}
}

// The pump ends with the subscription, closes the channel it owns, and closes
// the subscription it was handed. A consumer selecting on the channel learns the
// stream ended rather than blocking forever.
func TestPumpEndsAndClosesWhenTheSubscriptionEnds(t *testing.T) {
	closed := 0
	subscription := newFakeSubscription(&closed)
	runtime := boundFor(t, newFullController(subscription, nil, nil))

	published, err := runtime.SubscribeCommitted(t.Context(), "")
	if err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}
	close(subscription.deliveries)

	select {
	case _, open := <-published:
		if open {
			t.Fatal("a publication arrived from a closed stream")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the publication channel did not close when the stream ended")
	}
	waitFor(t, func() bool { return closed == 1 })
}

// The caller's context ends the pump, which is the only bound on it when a hub
// keeps a stream open forever.
func TestPumpEndsWhenTheCallersContextIsDone(t *testing.T) {
	closed := 0
	subscription := newFakeSubscription(&closed)
	runtime := boundFor(t, newFullController(subscription, nil, nil))

	ctx, cancel := context.WithCancel(t.Context())
	published, err := runtime.SubscribeCommitted(ctx, "")
	if err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}
	cancel()

	select {
	case _, open := <-published:
		if open {
			t.Fatal("a publication arrived after the context ended")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the publication channel did not close when the context ended")
	}
	waitFor(t, func() bool { return closed == 1 })
}

// waitFor polls a condition the pump goroutine satisfies, because a goroutine's
// last act is not ordered against the channel close a consumer observes.
func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the condition was not reached before the deadline")
}

// TestThePumpAbsorbsABurstThenGivesUpRatherThanBlockingItsProducer holds the
// bound on Host's own egress, and it is a fix rather than a description: the
// pump published into an UNBUFFERED channel with a blocking send, so a Host
// consumer that paused for one instant stopped the pump, and a stopped pump is
// the only reader of the hub's 256-slot egress buffer. The cost was not Host's
// to pay — the hub's class-aware policy would eventually fail the subscription
// for an enduring overflow, so a momentary Host pause was charged as a LOST
// TAIL, attributed to the hub, with no record on Host's side of what happened.
//
// THE SET OF CLASSES HERE IS ONE, DERIVED. The hub applies drop-ephemeral /
// fail-enduring, but nothing ephemeral can reach this channel: the committed
// stream skips an ephemeral event outright (hub.go:653-660), the filter this
// adapter opens is enduring-only, and pump forwards only a delivery that
// answers Delivery.Committed. So the only policy this buffer can implement is
// the enduring one — absorb, then terminate — and there is no droppable case to
// write. That is why overflow ends the pump instead of skipping a publication.
//
// WHAT THE ASSERTIONS ARE. The producer completing is asserted against a
// DEADLINE rather than by simply running, because the defect's signature is a
// blocked send and a test that blocked would be killed by timeout — which is
// not an assertion kill and reports nothing. The count of absorbed publications
// is the value assertion: it must be exactly the buffer, so neither a smaller
// buffer nor an unbounded one passes.
func TestThePumpAbsorbsABurstThenGivesUpRatherThanBlockingItsProducer(t *testing.T) {
	closed := 0
	// UNBUFFERED, so every send measures the pump's own absorption and nothing
	// else. A buffered fixture would credit the pump with the fixture's slack.
	subscription := &fakeSubscription{deliveries: make(chan event.Delivery), closed: &closed}
	runtime := boundFor(t, newFullController(subscription, nil, nil))

	published, err := runtime.SubscribeCommitted(t.Context(), "")
	if err != nil {
		t.Fatalf("SubscribeCommitted: %v", err)
	}

	// NOBODY READS published. That is the whole fixture: a Host consumer that
	// has stopped taking publications.
	const burst = committedEgressBuffer + 2
	sent := make(chan int, 1)
	go func() {
		count := 0
		for i := 1; i <= burst; i++ {
			select {
			case subscription.deliveries <- event.Delivery{
				JournalSeq:     uint64(i),
				EventID:        "event-" + strconv.Itoa(i),
				PublicBody:     []byte(`{"type":"step_done"}`),
				CoveredThrough: uint64(i),
			}:
				count++
			case <-time.After(5 * time.Second):
				sent <- count
				return
			}
		}
		sent <- count
	}()

	// THE EXPECTED COUNT IS DERIVED, and it is neither the burst nor the
	// buffer. The pump takes one delivery out of the stream BEFORE it discovers
	// its own buffer is full, so exactly buffer+1 sends are taken; the next one
	// has no reader at all, because a pump that has given up must stop reading.
	// Stating it exactly is what separates the fix from the defect: the blocking
	// build takes 1.
	const taken = committedEgressBuffer + 1
	select {
	case count := <-sent:
		if count != taken {
			t.Fatalf("the producer placed %d of %d deliveries before it stopped being taken, want %d; the pump blocked its producer", count, burst, taken)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the producer never finished")
	}

	// The pump gave up rather than blocking: the channel it owns closes and the
	// subscription it was handed is closed, which is what tells the consumer the
	// tail is over and obliges a durable reset.
	absorbed := 0
	deadline := time.After(10 * time.Second)
	for open := true; open; {
		select {
		case _, more := <-published:
			if more {
				absorbed++
				continue
			}
			open = false
		case <-deadline:
			t.Fatalf("the publication channel never closed after %d publications; the pump is still holding the tail open", absorbed)
		}
	}
	if absorbed != committedEgressBuffer {
		t.Fatalf("the pump absorbed %d publications, want exactly the %d-slot egress buffer", absorbed, committedEgressBuffer)
	}
	waitFor(t, func() bool { return closed == 1 })
}

// THE WRAPPER MUST FORWARD THE EPOCH, AND A WRAPPER THAT DOES NOT IS SILENT.
// harness's capability doc names this as the hazard the two-result form cannot
// protect against: "a wrapper around a live session that does not forward the
// method silently opts its wrapped session out". boundSession IS such a wrapper,
// and Host reads the journal epoch through department.Runtime — so a forwarder
// answering (0, false) would make every resident session look like one that holds
// no journal grant, and Host would refuse commands it could in fact apply.
//
// MEASURED: a boundSession.LeaseEpoch returning a constant (0, false) survived
// this package's entire suite before this test existed, because admit reads the
// field directly and nothing exercised the forwarded method.
//
// BOTH ANSWERS, because a forwarder that answered a constant (7, true) would pass
// a one-row version of this.
func TestTheBoundSessionForwardsTheRuntimesLeaseEpoch(t *testing.T) {
	for _, row := range []struct {
		name  string
		part  leaseEpochPart
		epoch uint64
		held  bool
	}{
		{name: "a held grant", part: leaseEpochPart{epoch: 12, held: true}, epoch: 12, held: true},
		{name: "no grant", part: leaseEpochPart{}, epoch: 0, held: false},
	} {
		t.Run(row.name, func(t *testing.T) {
			controller := &struct {
				controllerBase
				faultsPart
				idlePart
				livenessPart
				releaserPart
				committedPart
				leaseEpochPart
			}{committedPart: committedPart{available: true}, leaseEpochPart: row.part}

			runtime := boundFor(t, controller)
			epoch, held := runtime.LeaseEpoch()
			if epoch != row.epoch || held != row.held {
				t.Fatalf("LeaseEpoch() = (%d, %t), want the controller's (%d, %t)", epoch, held, row.epoch, row.held)
			}
		})
	}
}

// TestTheBoundSessionForwardsThePersistenceFaultPair holds D3's adapter half: a
// wrapper that did not forward harness's fault broadcast and abandon would leave
// every faulted session resident and wedged with the capability one layer down.
func TestTheBoundSessionForwardsThePersistenceFaultPair(t *testing.T) {
	fault := errors.New("journal append failed")
	faulted := make(chan struct{})
	close(faulted)
	abandoned := 0
	abandonErr := errors.New("lease release failed")
	controller := newFullController(nil, nil, nil)
	controller.faultsPart = faultsPart{faulted: faulted, fault: fault, abandoned: &abandoned, err: abandonErr}

	adapter := newAdapter(t, stubRigs{})
	launched, err := adapter.bind(controller, testTenant, testSession)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	faults, ok := launched.(department.PersistenceFaults)
	if !ok {
		t.Fatalf("the bound session %T does not offer department.PersistenceFaults", launched)
	}
	if faults.PersistenceFaulted() != (<-chan struct{})(faulted) {
		t.Error("PersistenceFaulted is not the runtime's own channel")
	}
	if got := faults.PersistenceFault(); !errors.Is(got, fault) {
		t.Errorf("PersistenceFault = %v, want the runtime's fault", got)
	}
	if got := faults.AbandonResidency(t.Context()); !errors.Is(got, abandonErr) || abandoned != 1 {
		t.Errorf("AbandonResidency = %v after %d calls, want the runtime's answer after 1", got, abandoned)
	}
}
