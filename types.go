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
	cause   error
}

func (e *EventStoreBaseError) Error() string {
	return e.Message
}

type OptimisticLockError struct {
	EventStoreBaseError
}

type SerializationError struct {
	EventStoreBaseError
}

type DeserializationError struct {
	EventStoreBaseError
}

type IOError struct {
	EventStoreBaseError
}
