package main

import (
	"errors"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Config is everything this binary reads from its environment.
//
// IT VALIDATES NOTHING IT DOES NOT OWN. Every value here is handed to host.New
// or to the composition, which already refuse an incoherent configuration with a
// typed, coded error; re-checking a range here would be a second rule free to
// drift from the one that decides. What this type owns is PARSING — turning a
// string into a duration, a number or an enumerated member — and reporting the
// variable a person must fix.
type Config struct {
	HostID           sessionwire.HostID
	HostGeneration   uint64
	InternalEndpoint sessionwire.InternalEndpoint
	IsolationClass   sessionwire.HostIsolationClass
	Placement        sessionwire.HostPlacement
	Capacity         uint64
	FixedSessionID   sessionwire.SessionID

	WarmTTL           time.Duration
	RegistryHeartbeat time.Duration
	RegistryExpiry    time.Duration
	ClaimTTL          time.Duration
	ApplyDeadline     time.Duration
	ReconcileInterval time.Duration
	CommandQueueSize  int
	ReconcileBatch    int

	ListenAddress      string
	PingInterval       time.Duration
	PongTimeout        time.Duration
	MaxBindingsPerLink int
	MaxBindings        int
	MaxTenantLinks     int

	Grace                time.Duration
	IdleBoundary         time.Duration
	PublishBound         time.Duration
	CompatibilityTimeout time.Duration
	WorkPoll             time.Duration
}

// ConfigError reports one environment variable this binary could not use.
type ConfigError struct {
	Variable string
	Reason   string
	Cause    error
}

func (e *ConfigError) Error() string {
	return "host: " + e.Variable + ": " + e.Reason
}

// Unwrap returns the parse failure underneath.
func (e *ConfigError) Unwrap() error { return e.Cause }

// Environment is the lookup a Config is read from. It is os.LookupEnv's shape,
// so a test supplies a map instead of mutating the process.
type Environment func(string) (string, bool)

// LoadConfig reads a Config.
//
// EVERY VARIABLE IS REQUIRED AND THERE ARE NO DEFAULTS, which is deliberate and
// is the same decision host.Options makes: a Host is a piece of infrastructure
// whose timings decide whether a Factory can see it, and a default heartbeat
// chosen by this file would be a policy nobody wrote down. A missing variable is
// named; it is never filled in.
func LoadConfig(lookup Environment) (Config, error) {
	reader := &environmentReader{lookup: lookup}
	config := Config{
		HostID:           sessionwire.HostID(reader.text("HOST_ID")),
		HostGeneration:   reader.unsigned("HOST_GENERATION"),
		InternalEndpoint: sessionwire.InternalEndpoint(reader.text("HOST_INTERNAL_ENDPOINT")),
		IsolationClass:   sessionwire.HostIsolationClass(reader.text("HOST_ISOLATION_CLASS")),
		Placement:        sessionwire.HostPlacement(reader.text("HOST_PLACEMENT")),
		Capacity:         reader.unsigned("HOST_CAPACITY"),

		WarmTTL:           reader.duration("HOST_WARM_TTL"),
		RegistryHeartbeat: reader.duration("HOST_REGISTRY_HEARTBEAT"),
		RegistryExpiry:    reader.duration("HOST_REGISTRY_EXPIRY"),
		ClaimTTL:          reader.duration("HOST_CLAIM_TTL"),
		ApplyDeadline:     reader.duration("HOST_APPLY_DEADLINE"),
		ReconcileInterval: reader.duration("HOST_RECONCILE_INTERVAL"),
		CommandQueueSize:  reader.count("HOST_COMMAND_QUEUE_SIZE"),
		ReconcileBatch:    reader.count("HOST_RECONCILE_BATCH"),

		ListenAddress:      reader.text("HOST_LISTEN_ADDRESS"),
		MaxBindingsPerLink: reader.count("HOST_MAX_BINDINGS_PER_LINK"),
		MaxBindings:        reader.count("HOST_MAX_BINDINGS"),
		MaxTenantLinks:     reader.count("HOST_MAX_TENANT_LINKS"),

		Grace:                reader.duration("HOST_DRAIN_GRACE"),
		IdleBoundary:         reader.duration("HOST_DRAIN_IDLE_BOUNDARY"),
		PublishBound:         reader.duration("HOST_DRAIN_PUBLISH_BOUND"),
		CompatibilityTimeout: reader.duration("HOST_COMPATIBILITY_TIMEOUT"),
		WorkPoll:             reader.duration("HOST_WORK_POLL"),
	}
	// FixedSessionID and the HostLink heartbeat are the only OPTIONAL values,
	// and each is optional because a rule elsewhere already decides when it must
	// be present: host.Options refuses a dedicated Host without a fixed session
	// and a pooled Host with one, and hostlink.Config refuses a ping without a
	// pong. Defaulting either here would move that decision into this file.
	if value, supplied := lookup("HOST_FIXED_SESSION_ID"); supplied {
		config.FixedSessionID = sessionwire.SessionID(value)
	}
	if _, supplied := lookup("HOST_PING_INTERVAL"); supplied {
		config.PingInterval = reader.duration("HOST_PING_INTERVAL")
		config.PongTimeout = reader.duration("HOST_PONG_TIMEOUT")
	}
	if reader.err != nil {
		return Config{}, reader.err
	}
	return config, nil
}

// environmentReader accumulates the FIRST failure and goes on reading.
//
// It reports one variable rather than all of them, and that is a choice worth
// naming: a list of twenty faults from a Deployment that set nothing is less
// useful than the first one, and stopping at the first keeps every later parse
// from reporting a fault that is really the absence of a value.
type environmentReader struct {
	lookup Environment
	err    error
}

// text reads a required non-empty variable.
func (r *environmentReader) text(name string) string {
	if r.err != nil {
		return ""
	}
	value, supplied := r.lookup(name)
	if !supplied || value == "" {
		r.err = &ConfigError{Variable: name, Reason: "must be set", Cause: errors.New("not set")}
		return ""
	}
	return value
}

// duration reads a required variable as a Go duration.
func (r *environmentReader) duration(name string) time.Duration {
	value := r.text(name)
	if r.err != nil {
		return 0
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		r.err = &ConfigError{Variable: name, Reason: "is not a duration: " + err.Error(), Cause: err}
		return 0
	}
	return parsed
}

// unsigned reads a required variable as a non-negative 64-bit number.
func (r *environmentReader) unsigned(name string) uint64 {
	value := r.text(name)
	if r.err != nil {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		r.err = &ConfigError{Variable: name, Reason: "is not a non-negative number: " + err.Error(), Cause: err}
		return 0
	}
	return parsed
}

// count reads a required variable as a machine-word count.
func (r *environmentReader) count(name string) int {
	value := r.text(name)
	if r.err != nil {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		r.err = &ConfigError{Variable: name, Reason: "is not a number: " + err.Error(), Cause: err}
		return 0
	}
	return parsed
}
