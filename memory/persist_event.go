package memory

import (
	"bytes"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// persistEvent commits an already serialized event. Validation and payload copying
// precede locking; head comparison, preparation and publication share the store lock.
func (s *Store) persistEvent(event eventstore.EventEnvelope[[]byte]) error {
	if err := event.Validate(); err != nil {
		return err
	}
	candidate := storedEvent{
		aggregateID: event.AggregateID(),
		seqNr:       event.SeqNr(),
		occurredAt:  event.OccurredAt(),
		manifest:    event.Manifest(),
		payload:     bytes.Clone(event.Payload()),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var journal []storedEvent
	if current := s.records[candidate.aggregateID]; current != nil {
		journal = current.journal
	}
	var headSeqNr eventstore.SeqNr
	if len(journal) > 0 {
		headSeqNr = journal[len(journal)-1].seqNr
	}
	if candidate.seqNr <= headSeqNr {
		return &eventstore.OptimisticLockError{
			AggregateID: candidate.aggregateID,
			SeqNr:       candidate.seqNr,
			HeadSeqNr:   &headSeqNr,
		}
	}
	if candidate.seqNr != headSeqNr+1 {
		return &eventstore.ContractViolationError{Rule: "W-8", SeqNr: &candidate.seqNr}
	}

	next := &record{journal: make([]storedEvent, len(journal)+1)}
	copy(next.journal, journal)
	next.journal[len(journal)] = candidate
	if err := s.hooks.Fail(testhook.Point{
		Phase:       testhook.PhaseCommit,
		AggregateID: candidate.aggregateID,
		SeqNr:       int64(candidate.seqNr),
		Payload:     bytes.Clone(candidate.payload),
	}); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	s.records[candidate.aggregateID] = next
	return nil
}
