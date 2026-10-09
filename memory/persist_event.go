package memory

import (
	"bytes"
	"context"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// persistEvent commits an already serialized event. Validation and payload copying
// precede locking; head comparison, preparation and publication share the store lock.
func (s *Store) persistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return s.commit(ctx, prepareStoredEvent(event), nil)
}

// prepareStoredEvent fixes the validated event's metadata and owns its payload bytes.
func prepareStoredEvent(event eventstore.EventEnvelope[[]byte]) storedEvent {
	return storedEvent{
		aggregateID: event.AggregateID(),
		seqNr:       event.SeqNr(),
		occurredAt:  event.OccurredAt(),
		manifest:    event.Manifest(),
		payload:     bytes.Clone(event.Payload()),
	}
}

// commit prepares a complete next record and publishes it at one point under the store lock.
// Candidates already own their bytes. Saved records remain immutable during preparation.
func (s *Store) commit(ctx context.Context, candidate storedEvent, snapshot *storedSnapshot) error {
	var retentionErr error
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		if retentionErr != nil {
			s.notifyRetentionFailure(ctx, candidate.aggregateID, retentionErr)
		}
	}()
	var journal []storedEvent
	current := s.records[candidate.aggregateID]
	if current != nil {
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
	if current != nil {
		next.current = current.current
		next.history = current.history
	}
	if snapshot != nil {
		next.current = snapshot
		if s.settings.RetentionCount != nil {
			history := make([]storedSnapshot, len(next.history)+1)
			copy(history, next.history)
			history[len(next.history)] = *snapshot
			next.history = history
		}
	}
	if err := s.hooks.Fail(testhook.Point{
		Phase:       testhook.PhaseCommit,
		AggregateID: candidate.aggregateID,
		SeqNr:       int64(candidate.seqNr),
		Payload:     bytes.Clone(candidate.payload),
	}); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	s.records[candidate.aggregateID] = next
	var justWritten eventstore.SeqNr
	if snapshot != nil {
		justWritten = snapshot.seqNr
	}
	retentionErr = s.retainHistory(candidate.aggregateID, next, justWritten)
	return nil
}
