# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml?query=branch%3Amain)
[![Go module](https://img.shields.io/github/v/tag/j5ik2o/event-store-adapter-go?sort=semver&label=go%20module)](https://pkg.go.dev/github.com/j5ik2o/event-store-adapter-go/v5)
[![Go Reference](https://pkg.go.dev/badge/github.com/j5ik2o/event-store-adapter-go/v5.svg)](https://pkg.go.dev/github.com/j5ik2o/event-store-adapter-go/v5)
[![Go](https://img.shields.io/github/go-mod/go-version/j5ik2o/event-store-adapter-go.svg)](https://github.com/j5ik2o/event-store-adapter-go/blob/main/go.mod)
[![Contract v5](https://img.shields.io/badge/contract-v5-blueviolet.svg)](https://github.com/j5ik2o/event-store-adapter/tree/contract-v5/docs/spec)
[![Storages](https://img.shields.io/badge/storages-memory%20%7C%20DynamoDB%20%7C%20SQLite%20%7C%20Spanner%20%7C%20Bigtable-informational.svg)](https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec/storage)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/j5ik2o/event-store-adapter-go#license)

## Overview

This library provides a synchronous event store for CQRS/Event Sourcing with Memory, DynamoDB, SQLite, Spanner and Bigtable backends.

[日本語](./README.ja.md)

This library follows [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md); its major version 5 matches the contract revision.

Other implementations:

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
- [for .NET](https://github.com/j5ik2o/event-store-adapter-dotnet)
- [for PHP](https://github.com/j5ik2o/event-store-adapter-php)

See also the [common documents](https://github.com/j5ik2o/event-store-adapter).

To change this repository, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Installation

```sh
go get github.com/j5ik2o/event-store-adapter-go/v5
```

## Quick start

Domain events and aggregate state can be arbitrary Go types. Metadata belongs to `EventEnvelope[E]` and `SnapshotEnvelope[A]`; only the payload is passed to `Serializer[T]`. An aggregate ID implements `TypeName() string` and `Value() string`. The library builds `type-name + "-" + value`; type names cannot contain `-`, and the whole ID must fit in 1024 UTF-8 bytes.

This runnable example uses all four public operations and prints `3 2`. See [example_test.go](example_test.go) and the [repository tests](test/user_account_repository_test.go) for the same restoration flow.

```go
package main

import (
	"context"
	"fmt"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/memory"
)

type orderID string

func (id orderID) TypeName() string { return "Order" }
func (id orderID) Value() string    { return string(id) }

type orderEvent struct {
	Added int `json:"added"`
}
type orderState struct {
	Total int `json:"total"`
}

func main() {
	ctx := context.Background()
	keep, err := eventstore.KeepLatest(2)
	if err != nil {
		panic(err)
	}
	state, err := memory.NewStore(eventstore.WithRetentionCount(keep))
	if err != nil {
		panic(err)
	}
	store, err := memory.New(state, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState]())
	if err != nil {
		panic(err)
	}
	id := orderID("9")
	first, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(1), time.Now(), orderEvent{Added: 1})
	if err != nil {
		panic(err)
	}
	snapshot, err := eventstore.NewSnapshotEnvelope(orderState{Total: 1}, eventstore.SeqNr(1))
	if err != nil {
		panic(err)
	}
	if err := store.PersistEventAndSnapshot(ctx, first, snapshot); err != nil {
		panic(err)
	}
	second, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(2), time.Now(), orderEvent{Added: 2})
	if err != nil {
		panic(err)
	}
	if err := store.PersistEvent(ctx, second); err != nil {
		panic(err)
	}

	read, err := store.GetLatestSnapshotByID(ctx, id)
	if err != nil {
		panic(err)
	}
	restored := orderState{}
	since := eventstore.SeqNr(1)
	if read != nil && read.Snapshot != nil {
		restored = read.Snapshot.Aggregate()
		since = read.Snapshot.SeqNr() + 1
	}
	events, err := store.GetEventsByIDSinceSeqNr(ctx, id, since)
	if err != nil {
		panic(err)
	}
	for _, event := range events {
		restored.Total += event.Payload().Added
	}
	fmt.Println(restored.Total, read.HeadSeqNr)
}
```

Events use consecutive `SeqNr` values starting at 1; the allowed range is 0–9007199254740991, with 0 reserved for non-event sequence values. `OccurredAt` is a `time.Time` within the signed 64-bit Unix nanosecond range. Optional `WithManifest` metadata defaults to an empty string.

`PersistEvent` derives the expected previous head from the event number. `PersistEventAndSnapshot` requires equal event and snapshot numbers and commits them together. There is no version argument or `IsCreated` flag.

`GetLatestSnapshotByID` returns `(nil, nil)` when there is no head. An existing head can have a nil `Snapshot`. Restore from `Snapshot.SeqNr()+1`, or 1 when there is no snapshot. `HeadSeqNr` is independent of the snapshot number and is not a restoration start position. Event reads include the supplied number and return every matching event in ascending order.

## Storages

### Memory

The memory storage lives in a single process. It does not persist to files, survive restarts, or share data across processes.

### DynamoDB

Provision the three tables and history index described in [DYNAMODB_DATABASE_SCHEMA.md](docs/DYNAMODB_DATABASE_SCHEMA.md), then supply an AWS SDK v2 client. Keep the three tables in one Region and write to them only from that Region. Global tables (multi-Region replicas) are not supported. With the domain types above, the factory is:

```go
import (
    "context"
    awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/dynamodb"
)

func openDynamoDB(ctx context.Context, client *awsdb.Client) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return nil, err }
    return dynamodb.New(ctx, client, dynamodb.Config{
        JournalTableName: "journal",
        SnapshotTableName: "snapshot",
        HeadTableName: "head",
        SnapshotHistoryIndexName: "snapshot-history",
    }, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

The factory checks configuration items in all three tables and creates them transactionally when all are absent. It does not create tables. Reads use strongly consistent BatchGetItem and journal Query requests and consume every page. The head and current snapshot read is not atomic; a concurrent write can return different snapshot and head numbers.

### SQLite

The SQLite storage keeps events and snapshots in one local database file, in the same five-table layout that the Rust v5 implementation uses; see [SQLITE_DATABASE_SCHEMA.md](docs/SQLITE_DATABASE_SCHEMA.md). Initialize a new file once with `sqlite.Initialize`, then open it with `sqlite.Open` and connect serializers with `sqlite.New`. `Open` never creates or initializes a file. With the domain types above:

```go
import (
    "context"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
)

// Run once for a new file. Initialize fails on a file that is already initialized.
func initializeSQLite(ctx context.Context, path string) error {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return err }
    return sqlite.Initialize(ctx, path, sqlite.Config{}, eventstore.WithRetentionCount(keep))
}

// Run at every start. Open never creates or initializes a file.
func openSQLite(ctx context.Context, path string) (*sqlite.Store, eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return nil, nil, err }
    store, err := sqlite.Open(ctx, path, sqlite.Config{}, eventstore.WithRetentionCount(keep))
    if err != nil { return nil, nil, err }
    es, err := sqlite.New(store, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState]())
    if err != nil { _ = store.Close(); return nil, nil, err }
    return store, es, nil
}
```

Call `Store.Close` when finished. Use a local file system. TTL retention and a change feed are configuration errors. An append takes the write lock before it compares the head; if another connection holds the lock for longer than `Config.LockWait` (default 5 seconds), the append fails with a storage error.

### Spanner

The Spanner storage keeps events and snapshots in five tables of one Cloud Spanner database (GoogleSQL dialect), in the same row format as the JavaScript v5 implementation; see [SPANNER_DATABASE_SCHEMA.md](docs/SPANNER_DATABASE_SCHEMA.md). You create the tables and one configuration row yourself, once, with the DDL of that document; the library never runs DDL. Supply a `*spanner.Client` from `cloud.google.com/go/spanner`, which you close yourself. Because the layout is provisioned outside the program, the same factory runs at the first start and at every restart. With the domain types above:

```go
import (
    "context"
    gspanner "cloud.google.com/go/spanner"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/spanner"
)

// Run at every start, the first one included. New verifies the layout and never changes it.
func openSpanner(ctx context.Context, client *gspanner.Client) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return nil, err }
    return spanner.New(ctx, client, spanner.Config{TablePrefix: "esa"},
        eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

`TablePrefix` derives the five table names. The retention count and mode must agree with the configuration row. A change feed is a configuration error. Appends run in SERIALIZABLE transactions and are re-run after an abort up to `Config.TransactionRetryLimit` times (default 5); reads use one strongly consistent read-only transaction. If the response of a commit is lost, the append fails with a storage error whose outcome is unknown.

### Bigtable

The Bigtable storage keeps events and snapshots in two tables of one Cloud Bigtable instance, a journal table and a head table, in the same row format as the Rust v5 implementation; see [BIGTABLE_DATABASE_SCHEMA.md](docs/BIGTABLE_DATABASE_SCHEMA.md). You create the two tables and their column families yourself with `cbt` (or any admin client); the library never creates tables. Supply a `*bigtable.Client` and a `*bigtable.AdminClient` from `cloud.google.com/go/bigtable` (imported as `gbigtable` below), which you configure (authentication, an app profile pinned to one cluster, the connection) and close yourself. The configuration rows are written once by `bigtable.Initialize`; every start, the first one included, then opens the store with `bigtable.New`. With the domain types above:

```go
import (
    "context"
    gbigtable "cloud.google.com/go/bigtable"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
)

var bigtableConfig = bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"}

// Run once, before the first start, while no writer is running. It writes only the two configuration rows.
func prepareBigtable(ctx context.Context, data *gbigtable.Client) error {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return err }
    return bigtable.Initialize(ctx, data, bigtableConfig, eventstore.WithRetentionCount(keep))
}

// Run at every start, the first one included. New verifies the tables and never changes them.
func openBigtable(ctx context.Context, data *gbigtable.Client, admin *gbigtable.AdminClient) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return nil, err }
    return bigtable.New(ctx, data, admin, bigtableConfig,
        eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

`Initialize` fails with a configuration error when either table already has a configuration row, so call it only once; the schema document explains how to recover from an interrupted initialization. The retention count and `Config.EnvelopeLimitBytes` must agree with the configuration rows. TTL retention and a change feed are configuration errors. The commit point of an append is a conditional write on the head row; if its response is lost, the append fails with a storage error whose outcome is unknown and the write is not repeated. A data client built over your own gRPC connection must allow received messages as large as `Config.EnvelopeLimitBytes` plus the row overhead (row key and cell metadata), because the default gRPC limit is 4 MiB.

## Serialization

`Serializer[T]` serializes only the payload or the aggregate state; envelope metadata is not passed to it. Domain types need no library-specific interface. Implement `Serialize(value T) ([]byte, error)` and `Deserialize(data []byte) (T, error)`.

`NewJSONSerializer[T]()` uses `encoding/json`. Generic JSON numbers are restored as `json.Number`, and an input that contains more than one JSON value fails.

An error returned by a serializer is classified as `SerializationError` (`KindSerialization`). A nil serializer is a `ConfigurationError`.

## Retention

Omitting the retention count, or supplying `WithRetentionCount(NoRetention())`, keeps the current snapshot without history. `KeepLatest(n)` requires `n >= 1`; the current snapshot is also stored in the history and counts toward `n`, so the newest `n` snapshots are kept. Memory and SQLite support delete retention only. DynamoDB supports `RetentionDelete` (default) and `RetentionTTL`, with `WithTTLGraceSeconds` specifying a non-negative grace period in seconds. Spanner supports `RetentionDelete` (default) and `RetentionTTL`; its TTL grace period is `Config.TTLGraceDays` in days, and a non-zero `WithTTLGraceSeconds` is a configuration error there. Bigtable supports delete retention only.

Retention runs synchronously after a successful snapshot/history write. Failure is logged and can also be observed through `WithRetentionFailureHandler(func(context.Context, error))`; the committed write still returns success.

## Errors

Use `KindOf(err)` (returning kind and a boolean) or `errors.As` to distinguish optimistic lock, contract violation, serialization, configuration and storage errors. `errors.Is` and `Unwrap` preserve the underlying cause. DynamoDB unprocessed-key and delete retries share `Config.ConfigurationReadRetryLimit` (default 10 retries after the first request, 0 disables retries), with 50ms exponential backoff capped at 1 second; context cancellation interrupts waits.

## Migration

See [DYNAMODB_DATABASE_SCHEMA.md](docs/DYNAMODB_DATABASE_SCHEMA.md) for attributes, permissions and retention, and [MIGRATION_GUIDE.md](docs/MIGRATION_GUIDE.md) for code and old-data migration.

## Conformance

The `conformance/` directory holds the shared conformance data (version `1.3.0`, see [conformance/README.md](conformance/README.md)). The `lint` job in CI verifies it with `tools/conformance/manifest.py verify` and SHA-256 checks.

The `conformance` job in CI runs `go test -count=1 -v ./internal/conformance/...` with `CONFORMANCE_REPORT` set, and uploads `conformance-report.json` as an artifact. The runner covers the Memory, DynamoDB, SQLite, Spanner and Bigtable storages; DynamoDB uses DynamoDB Local, Spanner uses the Cloud Spanner emulator and Bigtable uses the Cloud Bigtable emulator, each started in a container, and SQLite uses local files. A required SQLite, Spanner or Bigtable case that is not executed fails the job. Without `CONFORMANCE_REPORT`, the test is skipped.

## License

Dual-licensed under MIT or Apache-2.0 (`MIT OR Apache-2.0`), at your option. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
