package event_store_adapter_go

// EventStore is the interface for persisting events and snapshots.
type EventStore interface {
	// GetLatestSnapshotById returns the latest snapshot of the aggregate.
	GetLatestSnapshotById(aggregateId AggregateId) (*AggregateResult, error)
	// GetEventsByIdSinceSeqNr returns the events of the aggregate since the specified sequence number.
	GetEventsByIdSinceSeqNr(aggregateId AggregateId, seqNr uint64) ([]Event, error)
	// PersistEvent persists the event.
	PersistEvent(event Event, version uint64) error
	// PersistEventAndSnapshot persists the event and the snapshot.
	PersistEventAndSnapshot(event Event, aggregate Aggregate) error
}
