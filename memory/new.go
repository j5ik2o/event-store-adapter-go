package memory

import (
	"context"
	"errors"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

// New connects payload serializers to a Store. Instances using the same Store
// share its records, immutable settings and lock. Nil dependencies are rejected
// at construction with a ConfigurationError.
// Snapshot and event reads copy bytes atomically under the Store's read lock;
// payload restoration runs in the common operation entry after unlocking.
func New[E, A any](
	store *Store,
	eventSerializer eventstore.Serializer[E],
	snapshotSerializer eventstore.Serializer[A],
) (eventstore.EventStore[E, A], error) {
	return eventstore.NewOperationEntry(eventSerializer, snapshotSerializer,
		func(...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
			if store == nil {
				return nil, &eventstore.ConfigurationError{Cause: errors.New("memory store is nil")}
			}
			return &storageBoundary{store: store}, nil
		})
}

type storageBoundary struct {
	store *Store
}

func (b *storageBoundary) PersistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	return b.store.persistEvent(ctx, event)
}

func (b *storageBoundary) PersistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	return b.store.persistEventAndSnapshot(ctx, event, snapshot)
}

func (b *storageBoundary) GetLatestSnapshotByID(_ context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	return b.store.getLatestSnapshotByID(id)
}

func (b *storageBoundary) GetEventsByIDSinceSeqNr(_ context.Context, id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	return b.store.getEventsByIDSinceSeqNr(id, seqNr)
}
