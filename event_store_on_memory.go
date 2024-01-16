package event_store_adapter_go

type EventStoreOnMemory struct {
	events    map[string][]Event
	versions  map[string]uint64
	snapshots map[string]Aggregate
}

func NewEventStoreOnMemory() *EventStoreOnMemory {
	return &EventStoreOnMemory{events: make(map[string][]Event), versions: make(map[string]uint64), snapshots: make(map[string]Aggregate)}
}

func (es *EventStoreOnMemory) GetLatestSnapshotById(aggregateId AggregateId) (*AggregateResult, error) {
	snapshot := es.snapshots[aggregateId.AsString()]
	if snapshot != nil {
		return &AggregateResult{aggregate: snapshot}, nil
	}
	return &AggregateResult{}, nil
}

func (es *EventStoreOnMemory) GetEventsByIdSinceSeqNr(aggregateId AggregateId, seqNr uint64) ([]Event, error) {
	result := make([]Event, 0)
	for _, event := range es.events[aggregateId.AsString()] {
		if event.GetSeqNr() >= seqNr {
			result = append(result, event.(Event))
		}
	}
	return result, nil
}

func (es *EventStoreOnMemory) PersistEvent(event Event, version uint64) error {
	if event.IsCreated() {
		panic("event is created")
	}
	if es.versions[event.GetAggregateId().AsString()] != version {
		return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", nil)
	}
	es.events[event.GetAggregateId().AsString()] = append(es.events[event.GetAggregateId().AsString()], event)
	return nil
}

func (es *EventStoreOnMemory) PersistEventAndSnapshot(event Event, aggregate Aggregate) error {
	if !event.IsCreated() {
		version := es.versions[event.GetAggregateId().AsString()]
		if version != aggregate.GetVersion() {
			return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", nil)
		}
	}
	es.events[event.GetAggregateId().AsString()] = append(es.events[event.GetAggregateId().AsString()], event)
	es.snapshots[event.GetAggregateId().AsString()] = aggregate
	es.versions[event.GetAggregateId().AsString()] = aggregate.GetVersion()
	return nil
}
