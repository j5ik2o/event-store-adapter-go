package sqlite

import (
	"context"
	"errors"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
)

// New connects payload serializers to a Store. Instances using the same Store share its connection.
// A nil Store is rejected at construction with a ConfigurationError.
// Payload restoration runs in the common operation entry after the Store has finished reading.
func New[E, A any](
	store *Store,
	eventSerializer eventstore.Serializer[E],
	snapshotSerializer eventstore.Serializer[A],
) (eventstore.EventStore[E, A], error) {
	return eventstore.NewOperationEntry(eventSerializer, snapshotSerializer,
		func(...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
			if store == nil {
				return nil, &eventstore.ConfigurationError{Cause: errors.New("sqlite store is nil")}
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

func (b *storageBoundary) GetLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	return b.store.getLatestSnapshotByID(ctx, id)
}

func (b *storageBoundary) GetEventsByIDSinceSeqNr(ctx context.Context, id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	return b.store.getEventsByIDSinceSeqNr(ctx, id, seqNr)
}
