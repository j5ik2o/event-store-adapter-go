package pkg

import (
	"fmt"
)

// AggregateId is the interface that represents the aggregate id of DDD.
type AggregateId interface {
	fmt.Stringer

	// GetTypeName returns the type name of the aggregate id.
	GetTypeName() string

	// GetValue returns the value of the aggregate id.
	GetValue() string

	// AsString returns the string representation of the aggregate id.
	//
	// The string representation is {TypeName}-{Value}.
	AsString() string
}

// Event is the interface that represents the domain event of DDD.
type Event interface {
	fmt.Stringer
	// GetId returns the id of the event.
	GetId() string

	// GetTypeName returns the type name of the event.
	GetTypeName() string

	// GetAggregateId returns the aggregate id of the event.
	GetAggregateId() AggregateId

	// GetSeqNr returns the sequence number of the event.
	GetSeqNr() uint64

	// IsCreated returns true if the event is created.
	IsCreated() bool

	// GetOccurredAt returns the occurred at of the event.
	GetOccurredAt() uint64
}

// Aggregate is the interface that represents the aggregate of DDD.
type Aggregate interface {
	fmt.Stringer

	// GetId returns the id of the aggregate.
	GetId() AggregateId

	// GetSeqNr returns the sequence number of the aggregate.
	GetSeqNr() uint64

	// GetVersion returns the version of the aggregate.
	GetVersion() uint64

	// WithVersion returns a new aggregate with the specified version.
	WithVersion(version uint64) Aggregate
}

// AggregateConverter is the function type that converts map[string]interface{} to Aggregate.
type AggregateConverter func(map[string]interface{}) (Aggregate, error)

// EventConverter is the function type that converts map[string]interface{} to Event.
type EventConverter func(map[string]interface{}) (Event, error)

// AggregateResult is the result of aggregate.
type AggregateResult struct {
	aggregate Aggregate
}

// Present returns true if the aggregate is not nil.
func (a *AggregateResult) Present() bool {
	return a.aggregate != nil
}

// Empty returns true if the aggregate is nil.
func (a *AggregateResult) Empty() bool {
	return !a.Present()
}

// Aggregate returns the aggregate.
func (a *AggregateResult) Aggregate() Aggregate {
	if a.Empty() {
		panic("aggregate is nil")
	}
	return a.aggregate
}

// EventSerializer is an interface that serializes and deserializes events.
type EventSerializer interface {
	// Serialize serializes the event.
	Serialize(event Event) ([]byte, error)
	// Deserialize deserializes the event.
	Deserialize(data []byte, eventMap *map[string]interface{}) error
}

// SnapshotSerializer is an interface that serializes and deserializes snapshots.
type SnapshotSerializer interface {
	// Serialize serializes the aggregate.
	Serialize(aggregate Aggregate) ([]byte, error)
	// Deserialize deserializes the aggregate.
	Deserialize(data []byte, aggregateMap *map[string]interface{}) error
}

// EventStoreBaseError is a base error of EventStore.
type EventStoreBaseError struct {
	// Message is a message of the error.
	Message string
	// Cause is a cause of the error.
	Cause error
}

// Error returns the message of the error.
func (e *EventStoreBaseError) Error() string {
	return e.Message
}

// OptimisticLockError is an error that occurs when the version of the aggregate does not match.
type OptimisticLockError struct {
	EventStoreBaseError
}

// NewOptimisticLockError is the constructor of OptimisticLockError.
func NewOptimisticLockError(message string, cause error) *OptimisticLockError {
	return &OptimisticLockError{EventStoreBaseError{message, cause}}
}

// SerializationError is the error type that occurs when serialization fails.
type SerializationError struct {
	EventStoreBaseError
}

// NewSerializationError is the constructor of SerializationError.
func NewSerializationError(message string, cause error) *SerializationError {
	return &SerializationError{EventStoreBaseError{message, cause}}
}

// DeserializationError is the error type that occurs when deserialization fails.
type DeserializationError struct {
	EventStoreBaseError
}

// NewDeserializationError is the constructor of DeserializationError.
func NewDeserializationError(message string, cause error) *DeserializationError {
	return &DeserializationError{EventStoreBaseError{message, cause}}
}

// IOError is the error type that occurs when IO fails.
type IOError struct {
	EventStoreBaseError
}

// NewIOError is the constructor of IOError.
func NewIOError(message string, cause error) *IOError {
	return &IOError{EventStoreBaseError{message, cause}}
}
