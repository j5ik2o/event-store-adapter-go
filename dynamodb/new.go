package dynamodb

import (
	"context"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// New validates dependencies and options, confirms the stored configuration and
// connects payload serializers to the DynamoDB storage boundary. Tables and the
// snapshot history index must be provisioned by the caller.
// Latest snapshot reads use strongly consistent BatchGetItem reads of head and
// current snapshot. These two reads are not atomic (R-8): a concurrent write can
// yield an older head together with a newer snapshot. Their numbers remain independent.
func New[E, A any](
	ctx context.Context,
	client *awsdynamodb.Client,
	cfg Config,
	eventSerializer eventstore.Serializer[E],
	snapshotSerializer eventstore.Serializer[A],
	opts ...eventstore.Option,
) (eventstore.EventStore[E, A], error) {
	return newWithHooks(ctx, client, cfg, eventSerializer, snapshotSerializer, nil, opts...)
}

func newWithHooks[E, A any](ctx context.Context, client *awsdynamodb.Client, cfg Config, eventSerializer eventstore.Serializer[E], snapshotSerializer eventstore.Serializer[A], hooks *testhook.Hooks, opts ...eventstore.Option) (eventstore.EventStore[E, A], error) {
	return eventstore.NewOperationEntry(eventSerializer, snapshotSerializer,
		func(opts ...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
			return open(ctx, client, cfg, hooks, opts...)
		}, opts...)
}

func (s *opened) PersistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	return s.persistPreparedEvent(ctx, event)
}

func (s *opened) PersistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	return s.persistPreparedEventAndSnapshot(ctx, event, snapshot)
}
