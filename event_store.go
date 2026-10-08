package eventstore

import "context"

// EventStore provides four synchronous operations for arbitrary domain values.
// Cancellation and deadlines are conveyed through the first context argument.
type EventStore[E, A any] interface {
	// PersistEvent appends one event. The expected head follows from event.SeqNr().
	PersistEvent(ctx context.Context, event EventEnvelope[E]) error

	// PersistEventAndSnapshot appends an event and snapshot with matching numbers (W-9).
	PersistEventAndSnapshot(ctx context.Context, event EventEnvelope[E], snapshot SnapshotEnvelope[A]) error

	// GetLatestSnapshotByID returns (nil, nil) when there is no head (R-1).
	GetLatestSnapshotByID(ctx context.Context, id AggregateID) (*SnapshotRead[A], error)

	// GetEventsByIDSinceSeqNr returns all events at or above seqNr in ascending order.
	GetEventsByIDSinceSeqNr(ctx context.Context, id AggregateID, seqNr SeqNr) ([]EventEnvelope[E], error)
}

// SnapshotRead keeps the snapshot's number separate from the head number (R-2, R-3).
type SnapshotRead[A any] struct {
	// Snapshot is nil when a head exists without a snapshot.
	Snapshot *SnapshotEnvelope[A]
	// HeadSeqNr is returned even when Snapshot is nil.
	HeadSeqNr SeqNr
}
