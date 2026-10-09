package memory

import (
	"strings"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// getEventsByIDSinceSeqNr atomically copies all serialized events at or above seqNr.
// It validates and fixes the query before waiting, then constructs independent
// envelopes under one read lock for the whole store.
func (s *Store) getEventsByIDSinceSeqNr(id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		if violation, ok := err.(*eventstore.ContractViolationError); ok {
			violation.SeqNr = &seqNr
		}
		return nil, err
	}
	if err := seqNr.Validate(); err != nil {
		return nil, err
	}
	typeName, value, _ := strings.Cut(aid, "-")
	fixedID, err := eventstore.NewAggregateID(typeName, value)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.hooks.Fail(testhook.Point{
		Phase:       testhook.PhaseReadEvents,
		AggregateID: aid,
		SeqNr:       int64(seqNr),
	}); err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	current := s.records[aid]
	if current == nil {
		return nil, nil
	}
	var events []eventstore.EventEnvelope[[]byte]
	for _, saved := range current.journal {
		if saved.seqNr < seqNr {
			continue
		}
		copied := saved.clone()
		event, err := eventstore.NewEventEnvelope(fixedID, copied.seqNr, copied.occurredAt, copied.payload,
			eventstore.WithManifest(copied.manifest))
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}
