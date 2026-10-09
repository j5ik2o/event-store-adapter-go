package memory

import (
	"bytes"
	"context"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

// persistEventAndSnapshot commits an already serialized event and snapshot together.
// Both envelopes are validated and their bytes are fixed before taking the store lock.
func (s *Store) persistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	seqNr, snapshotSeqNr := event.SeqNr(), snapshot.SeqNr()
	if seqNr != snapshotSeqNr {
		return &eventstore.ContractViolationError{Rule: "W-9", SeqNr: &seqNr, SnapshotSeqNr: &snapshotSeqNr}
	}
	candidate := prepareStoredEvent(event)
	preparedSnapshot := &storedSnapshot{
		seqNr:    snapshotSeqNr,
		manifest: snapshot.Manifest(),
		payload:  bytes.Clone(snapshot.Aggregate()),
	}
	return s.commit(ctx, candidate, preparedSnapshot)
}
