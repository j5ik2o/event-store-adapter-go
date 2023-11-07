package event_store_adapter_go

import (
	"fmt"
)

type AggregateId interface {
	fmt.Stringer
	GetTypeName() string
	GetValue() string
	AsString() string
}

type Event interface {
	fmt.Stringer
	GetId() string
	GetTypeName() string
	GetAggregateId() AggregateId
	GetSeqNr() uint64
	IsCreated() bool
	GetOccurredAt() uint64
}

type Aggregate interface {
	fmt.Stringer
	GetId() AggregateId
	GetSeqNr() uint64
	GetVersion() uint64
	WithVersion(version uint64) Aggregate
}

type AggregateConverter func(map[string]interface{}) (Aggregate, error)
type EventConverter func(map[string]interface{}) (Event, error)

type AggregateResult struct {
	aggregate Aggregate
}

func (awsv *AggregateResult) Present() bool {
	return awsv.aggregate != nil
}

func (awsv *AggregateResult) Empty() bool {
	return !awsv.Present()
}

func (awsv *AggregateResult) Aggregate() Aggregate {
	if awsv.Empty() {
		panic("aggregate is nil")
	}
	return awsv.aggregate
}

type EventSerializer interface {
	Serialize(event Event) ([]byte, error)
	Deserialize(data []byte, eventMap *map[string]interface{}) error
}

type SnapshotSerializer interface {
	Serialize(aggregate Aggregate) ([]byte, error)
	Deserialize(data []byte, aggregateMap *map[string]interface{}) error
}

type EventStoreBaseError struct {
	Message string
	Cause   error
}

func (e *EventStoreBaseError) Error() string {
	return e.Message
}

type OptimisticLockError struct {
	EventStoreBaseError
}

func NewOptimisticLockError(message string, cause error) *OptimisticLockError {
	return &OptimisticLockError{EventStoreBaseError{message, cause}}
}

type SerializationError struct {
	EventStoreBaseError
}

func NewSerializationError(message string, cause error) *SerializationError {
	return &SerializationError{EventStoreBaseError{message, cause}}
}

type DeserializationError struct {
	EventStoreBaseError
}

func NewDeserializationError(message string, cause error) *DeserializationError {
	return &DeserializationError{EventStoreBaseError{message, cause}}
}

type IOError struct {
	EventStoreBaseError
}

func NewIOError(message string, cause error) *IOError {
	return &IOError{EventStoreBaseError{message, cause}}
}
