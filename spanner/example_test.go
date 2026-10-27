package spanner_test

import (
	"context"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
)

type orderEvent struct{ Name string }
type orderState struct{ Total int }

// ExampleNew mirrors the factory of the README: it is compiled with the package, so a change of the public types
// that the documentation shows breaks the build. It has no Output comment because it needs a provisioned database.
func ExampleNew() {
	openSpanner := func(ctx context.Context, client *gspanner.Client) (eventstore.EventStore[orderEvent, orderState], error) {
		keep, err := eventstore.KeepLatest(3)
		if err != nil {
			return nil, err
		}
		return spanner.New(ctx, client, spanner.Config{TablePrefix: "esa"},
			eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
			eventstore.WithRetentionCount(keep))
	}
	_ = openSpanner
}
