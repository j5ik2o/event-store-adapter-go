package event_store_adapter_go

import "fmt"

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
