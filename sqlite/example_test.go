package sqlite_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
)

func ExampleNew() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "esa-sqlite-example")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "events.db")

	keep, _ := eventstore.KeepLatest(2)
	opts := []eventstore.Option{eventstore.WithRetentionCount(keep)}
	if err := sqlite.Initialize(ctx, path, sqlite.Config{}, opts...); err != nil {
		panic(err)
	}
	store, err := sqlite.Open(ctx, path, sqlite.Config{}, opts...)
	if err != nil {
		panic(err)
	}
	es, err := sqlite.New(store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	if err != nil {
		panic(err)
	}

	id, _ := eventstore.NewAggregateID("Account", "1")
	now := time.Unix(1720000000, 0)
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		event, _ := eventstore.NewEventEnvelope(id, seq, now, fmt.Sprint("event-", seq))
		if seq == 2 {
			snapshot, _ := eventstore.NewSnapshotEnvelope("state-2", seq)
			err = es.PersistEventAndSnapshot(ctx, event, snapshot)
		} else {
			err = es.PersistEvent(ctx, event)
		}
		if err != nil {
			panic(err)
		}
	}

	// A later start opens the same file with Open only; Initialize would fail on an initialized file.
	if err := store.Close(); err != nil {
		panic(err)
	}
	store, err = sqlite.Open(ctx, path, sqlite.Config{}, opts...)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	es, err = sqlite.New(store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	if err != nil {
		panic(err)
	}
	latest, err := es.GetLatestSnapshotByID(ctx, id)
	if err != nil {
		panic(err)
	}
	fmt.Println(latest.HeadSeqNr, latest.Snapshot.SeqNr())
	// Output: 3 2
}
