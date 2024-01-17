package event_store_adapter_go

type EventStore interface {
	GetLatestSnapshotById(aggregateId AggregateId) (*AggregateResult, error)
	GetEventsByIdSinceSeqNr(aggregateId AggregateId, seqNr uint64) ([]Event, error)
	PersistEvent(event Event, version uint64) error
	PersistEventAndSnapshot(event Event, aggregate Aggregate) error
}
