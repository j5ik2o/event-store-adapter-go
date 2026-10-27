package bigtable_test

import (
	"context"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
)

type orderEvent struct{ Name string }
type orderState struct{ Total int }

// ExampleNew mirrors the factory of the README: it is compiled with the package, so a change of the public types
// that the documentation shows breaks the build. It has no Output comment because it needs provisioned tables.
func ExampleNew() {
	openBigtable := func(ctx context.Context, data *gbigtable.Client, admin *gbigtable.AdminClient) (eventstore.EventStore[orderEvent, orderState], error) {
		keep, err := eventstore.KeepLatest(3)
		if err != nil {
			return nil, err
		}
		return bigtable.New(ctx, data, admin, bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"},
			eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
			eventstore.WithRetentionCount(keep))
	}
	_ = openBigtable
}

// ExampleInitialize mirrors the one-time setup of the README.
func ExampleInitialize() {
	prepare := func(ctx context.Context, data *gbigtable.Client) error {
		keep, err := eventstore.KeepLatest(3)
		if err != nil {
			return err
		}
		return bigtable.Initialize(ctx, data, bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"},
			eventstore.WithRetentionCount(keep))
	}
	_ = prepare
}
