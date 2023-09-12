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
}

type AggregateConverter func(map[string]interface{}) (Aggregate, error)
type EventConverter func(map[string]interface{}) (Event, error)

type AggregateWithSeqNrWithVersion struct {
	aggregate Aggregate
	seqNr     *uint64
	version   *uint64
}

func (awsv *AggregateWithSeqNrWithVersion) Present() bool {
	return awsv.aggregate != nil && awsv.seqNr != nil && awsv.version != nil
}

func (awsv *AggregateWithSeqNrWithVersion) Empty() bool {
	return !awsv.Present()
}

func (awsv *AggregateWithSeqNrWithVersion) Aggregate() Aggregate {
	if awsv.Empty() {
		panic("aggregate is nil")
	}
	return awsv.aggregate
}

func (awsv *AggregateWithSeqNrWithVersion) SeqNr() uint64 {
	if awsv.Empty() {
		panic("seqNr is nil")
	}
	return *awsv.seqNr
}

func (awsv *AggregateWithSeqNrWithVersion) Version() uint64 {
	if awsv.Empty() {
		panic("version is nil")
	}
	return *awsv.version
}

type EventSerializer interface {
	Serialize(event Event) ([]byte, error)
	Deserialize(data []byte, eventMap *map[string]interface{}) error
}

type SnapshotSerializer interface {
	Serialize(aggregate Aggregate) ([]byte, error)
	Deserialize(data []byte, aggregateMap *map[string]interface{}) error
}
