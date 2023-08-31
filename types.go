package event_store_adapter_go

import (
	"fmt"
)

type AggregateId interface {
	fmt.Stringer
	GetTypeName() string
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
}

type AggregateConverter func(map[string]interface{}) (Aggregate, error)
type EventConverter func(map[string]interface{}) (Event, error)

type AggregateWithSeqNrWithVersion struct {
	Aggregate Aggregate
	SeqNr     uint64
	Version   uint64
}

type EventSerializer interface {
	Serialize(event Event) ([]byte, error)
	Deserialize(data []byte, eventMap *map[string]interface{}) error
}

type SnapshotSerializer interface {
	Serialize(aggregate Aggregate) ([]byte, error)
	Deserialize(data []byte, aggregateMap *map[string]interface{}) error
}
