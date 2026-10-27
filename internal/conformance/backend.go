package conformance

import (
	"context"
	"time"

	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// Backend is the boundary between the runner and a storage backend. It creates stores for
// scenarios. Actual adapters live in the external test package.
type Backend interface {
	// Name is "memory", "dynamodb", "sqlite", "spanner" or "bigtable".
	Name() string
	// Injectable tells whether the backend can inject the fault. A scenario with a fault
	// that cannot be injected is reported as unverified.
	Injectable(f FaultSpec) bool
	// Seed puts the seed items of the scenario before the store is created, with
	// a privilege that has no faults injected.
	Seed(ctx context.Context, items []map[string]any) error
	// Open creates a store for one scenario. The returned error may be an *OperationError.
	Open(ctx context.Context, cfg StoreConfig, inj Injection) (Store, error)
}

// ScenarioOwner provisions an isolated destination before Seed or Open.
// Cleanup belongs to the scenario even when public construction fails.
type ScenarioOwner interface {
	Prepare(context.Context, *ScenarioPlan) (Backend, func() error, error)
}

// ConfigurationSeeder is implemented by a backend whose destination keeps the store configuration
// (SQLite and Spanner). SeedConfiguration replaces the configuration the destination holds with the values of the
// scenario (seed.configuration) after the destination is provisioned and before the store is opened.
type ConfigurationSeeder interface {
	SeedConfiguration(ctx context.Context, values map[string]any) error
}

// WriteLockHolder is implemented by a backend whose destination has one write lock for the whole file
// (SQLite). HoldWriteLock takes that lock from a connection other than the store's, and keeps it until
// ReleaseWriteLock.
type WriteLockHolder interface {
	HoldWriteLock(ctx context.Context) error
	ReleaseWriteLock(ctx context.Context) error
}

// StoreCreator is implemented by a backend that can create a second store in the middle of a
// scenario. shared is the store to share records with, or nil for a new, independent store.
type StoreCreator interface {
	CreateStore(ctx context.Context, cfg StoreConfig, inj Injection, shared Store) (Store, error)
}

// PayloadMutator is implemented by a store that keeps the payload objects it was given and the ones
// it returned, by operation number, so that the runner can mutate them in place.
type PayloadMutator interface {
	// MutateWrittenPayload mutates the payload that the given operation passed to the library.
	MutateWrittenPayload(operation int, pointer string, value any) error
	// MutateReadPayload mutates the index-th payload that the given read operation got back.
	MutateReadPayload(operation, index int, pointer string, value any) error
}

// ObservationReader collects actual observations after an operation, including
// construction failures. Expected item values are used only to select read keys.
type ObservationReader interface {
	Observe(context.Context, int, StepPlan) (map[string]any, error)
}

type operationContextKey struct{}

// OperationNumber identifies construction (0) or a one-based scenario operation.
func OperationNumber(ctx context.Context) int {
	n, _ := ctx.Value(operationContextKey{}).(int)
	return n
}

// Store is an opened store. Each method maps to one operation of a step.
type Store interface {
	PersistEvent(ctx context.Context, ev Event) error
	PersistEventAndSnapshot(ctx context.Context, ev Event, sn Snapshot) error
	GetLatestSnapshotByID(ctx context.Context, aid AggregateIDArg) (SnapshotRead, error)
	GetEventsByIDSinceSeqNr(ctx context.Context, aid AggregateIDArg, since int64) ([]Event, error)
	// Notifications returns the failure notifications caught so far (for example "retention-failure").
	Notifications() []string
	Close() error
}

// StoreConfig is the store setting of a scenario. A nil pointer means the key was absent or null.
type StoreConfig struct {
	// RetentionCount is nil for retention_count=null (no history).
	RetentionCount  *int64
	RetentionMode   string
	TTLGraceSeconds *int64
	LayoutVersion   *int64
	RetryLimit      *int64
	// TTLGraceDays is the grace period in days of a Spanner TTL store (ttl_grace_days); it is not ttl_grace_seconds.
	TTLGraceDays *int64
	// TransactionRetryLimit is the number of re-runs of a Spanner transaction after an abort (the first attempt is not counted).
	TransactionRetryLimit *int64
	// EnvelopeLimitBytes is the envelope size limit of a Bigtable store (envelope_limit_bytes). It is nil when the key is absent.
	EnvelopeLimitBytes *int64
	// ChangeFeed asks for a change feed. An absent key means false.
	ChangeFeed bool
	// LockWaitMilliseconds is the busy wait of a SQLite store. It is nil when the key is absent.
	LockWaitMilliseconds *int64
	// ClockEpochSeconds is the initial clock of the scenario, if it has one.
	ClockEpochSeconds *int64
}

// Injection is what the runner hands to the backend to inject faults and hooks.
type Injection struct {
	// Faults are all faults of the scenario. The backend calls TryApply when it can apply one.
	Faults []*Fault
	// Hooks carries the clock, the waits and the faults that go through testhook.
	Hooks *testhook.Hooks
	// Plan supplies input fixtures for declared interleaved writes, never expectations.
	Plan *ScenarioPlan
}

// AggregateIDArg is the aggregate ID given by type name and value.
type AggregateIDArg struct {
	TypeName string
	Value    string
}

// Event is the event of a fixture, converted without any validation.
type Event struct {
	AggregateID AggregateIDArg
	SeqNr       int64
	OccurredAt  time.Time
	// Payload is the JSON of the payload.
	Payload  []byte
	Manifest string
}

// Snapshot is the snapshot of a fixture, converted without any validation.
type Snapshot struct {
	SeqNr int64
	// Aggregate is the JSON of the aggregate state.
	Aggregate []byte
	Manifest  string
}

// SnapshotRead is the result of getLatestSnapshotById.
type SnapshotRead struct {
	// Found is false when there is no result (expect.result is none).
	Found     bool
	HeadSeqNr int64
	Snapshot  *Snapshot
}

// OperationError is an error with its category. The runner compares the category, never the text.
type OperationError struct {
	// Category is one of optimistic-lock, contract-violation, serialization, configuration, storage.
	Category string
	Message  string
}

func (e *OperationError) Error() string { return e.Message }
