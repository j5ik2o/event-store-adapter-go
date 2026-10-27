package spanner

import (
	"context"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// New validates the settings, verifies the provisioned tables and the config row, and connects the payload
// serializers to the Spanner storage boundary. It never runs DDL and never writes. The client belongs to the
// caller, who closes it. Calling New again with the same destination after a restart is the normal way to open it.
// Reads use one strongly consistent read-only transaction; appends and retention run in SERIALIZABLE read-write
// transactions that are re-run after an abort up to Config.TransactionRetryLimit times.
func New[E, A any](
	ctx context.Context,
	client *gspanner.Client,
	cfg Config,
	eventSerializer eventstore.Serializer[E],
	snapshotSerializer eventstore.Serializer[A],
	opts ...eventstore.Option,
) (eventstore.EventStore[E, A], error) {
	return eventstore.NewOperationEntry(eventSerializer, snapshotSerializer,
		func(opts ...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
			checked, err := validate(client, cfg, opts...)
			if err != nil {
				return nil, err
			}
			observed, err := observe(ctx, client, checked.tables)
			if err != nil {
				return nil, err
			}
			if err := reconcile(observed, checked); err != nil {
				return nil, err
			}
			return &storageBoundary{client: client, settings: checked, hooks: checked.common.Hooks}, nil
		}, opts...)
}

type storageBoundary struct {
	client   *gspanner.Client
	settings settings
	hooks    *testhook.Hooks
}
