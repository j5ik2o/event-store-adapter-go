package bigtable

import (
	"context"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// New validates the settings, verifies the provisioned tables and both configuration rows, and connects the payload
// serializers to the Bigtable storage boundary. It never creates tables, column families or rows. The clients belong
// to the caller, who configures their authentication, app profile (a single-cluster routing policy, BT-1) and
// connection and who closes them. data serves reads and conditional writes; admin serves the schema check.
// Calling New again with the same destination after a restart is the normal way to open it.
func New[E, A any](
	ctx context.Context,
	data *gbigtable.Client,
	admin *gbigtable.AdminClient,
	cfg Config,
	eventSerializer eventstore.Serializer[E],
	snapshotSerializer eventstore.Serializer[A],
	opts ...eventstore.Option,
) (eventstore.EventStore[E, A], error) {
	return eventstore.NewOperationEntry(eventSerializer, snapshotSerializer,
		func(opts ...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
			checked, err := validate(data, cfg, opts...)
			if err != nil {
				return nil, err
			}
			if admin == nil {
				return nil, configurationError(errNilAdmin)
			}
			if err := observe(ctx, data, admin, checked); err != nil {
				return nil, err
			}
			return &storageBoundary{data: data, settings: checked, hooks: checked.common.Hooks}, nil
		}, opts...)
}

type storageBoundary struct {
	data     *gbigtable.Client
	settings settings
	hooks    *testhook.Hooks
}
