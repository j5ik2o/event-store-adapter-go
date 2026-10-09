package memory

import (
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// getLatestSnapshotByID atomically copies the serialized current snapshot and head number.
// It validates and fixes the ID before waiting, then constructs an independent result
// under one read lock for the whole store. Restoration belongs to the caller after return.
func (s *Store) getLatestSnapshotByID(id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.hooks.Fail(testhook.Point{
		Phase:       testhook.PhaseReadSnapshot,
		AggregateID: aid,
	}); err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	current := s.records[aid]
	if current == nil {
		return nil, nil
	}
	result := &eventstore.SnapshotRead[[]byte]{HeadSeqNr: current.journal[len(current.journal)-1].seqNr}
	if current.current != nil {
		copied := current.current.clone()
		snapshot, err := eventstore.NewSnapshotEnvelope(copied.payload, copied.seqNr,
			eventstore.WithManifest(copied.manifest))
		if err != nil {
			return nil, err
		}
		result.Snapshot = &snapshot
	}
	return result, nil
}
