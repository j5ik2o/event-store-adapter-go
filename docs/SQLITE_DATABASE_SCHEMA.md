# SQLite storage schema

[日本語](SQLITE_DATABASE_SCHEMA.ja.md)

## Layout

This is layout version 1 of the SQLite profile of [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md), used by the Go `/v5` module. The file format is the one the Rust v5 implementation writes; this package adds no layout of its own. One database file holds five `STRICT` tables:

| Table | Primary key | Purpose |
|---|---|---|
| journal | (`aid`, `seq_nr`) | Every event |
| head | `aid` | The latest event of each aggregate and its type name |
| snapshot_current | `aid` | The newest snapshot |
| snapshot_history | (`aid`, `seq_nr`) | Older snapshots, kept only when a retention count is set |
| config | `singleton` (always 1) | The store ID, layout version and fixed settings |

`aid` is the validated type name, a hyphen, and the ID value. It is declared `TEXT COLLATE BINARY`, and its primary key index is also `BINARY`, so `User-A` and `User-a` are different aggregates. There is no `head_transition` table and no change feed.

## Configuration

`sqlite.Config` has two fields. `LockWait` is the time to wait for a lock held by another connection: nil means 5 seconds, zero means no wait, and a negative value or more than 2147483647 milliseconds is a configuration error. `ChangeFeed` must be false; true is a configuration error. The common `Option` values are `WithRetentionCount`, `WithRetentionMode` (only `RetentionDelete`; `RetentionTTL` is a configuration error even without a count) and `WithRetentionFailureHandler`.

```go
import (
    "context"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
)

func openSQLite(ctx context.Context, path string) (*sqlite.Store, eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return nil, nil, err }
    opts := []eventstore.Option{eventstore.WithRetentionCount(keep)}
    // Run Initialize once for a new file. Open never creates or initializes a file.
    if err := sqlite.Initialize(ctx, path, sqlite.Config{}, opts...); err != nil { return nil, nil, err }
    store, err := sqlite.Open(ctx, path, sqlite.Config{}, opts...)
    if err != nil { return nil, nil, err }
    es, err := sqlite.New(store, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState]())
    if err != nil { _ = store.Close(); return nil, nil, err }
    return store, es, nil
}
```

Call `Store.Close` when you are done. A second `Close` returns nil, and operations after `Close` fail with a storage error.

## Initialization

`sqlite.Initialize` creates the five tables and one `config` row in one transaction. It accepts only a new file or a database that has no application table, index, view or trigger; anything else, including an initialized file, is a configuration error and nothing is created. Settings are checked before the file is created.

`sqlite.Open` never creates a file: a missing file is a storage error. It then checks, in one read transaction, that:

- the SQLite library is 3.51.3 or later (compared per number);
- every table is `STRICT`, with the expected columns, types, `NOT NULL` flags and primary keys, and `aid` is compared as `BINARY` by both the column and the primary key index;
- `config` has one row with layout version 1, a lowercase UUID `store_id`, `retention_mode = 'delete'`, `change_feed_enabled = 0`, and a `keep_snapshot_count` equal to the retention count of the client (NULL for no history).

Any difference is a configuration error. Open does not repair or write to the file, except that it switches the database to WAL mode. Every connection sets and reads back `locking_mode = NORMAL`, `journal_mode = WAL`, `synchronous = FULL`, `read_uncommitted = OFF` and the lock wait.

## Records

| Column | Type | Meaning |
|---|---|---|
| `aid` | TEXT | Aggregate ID |
| `seq_nr` | INTEGER | 1 through 9007199254740991 |
| `occurred_at_ns` / `event_occurred_at_ns` | INTEGER | Unix nanoseconds |
| `manifest` / `event_manifest` | TEXT | Envelope manifest |
| `payload` / `event_payload` | BLOB | Serialized payload or aggregate state |
| `type_name` (head) | TEXT | Type name of the aggregate |

## Writes and reads

An append takes the write lock first (`BEGIN IMMEDIATE`), then compares the head. A number at or below the head is an optimistic lock error; a number that is not head + 1 is a contract violation (W-8). The journal row, the head and, for a pair write, the current snapshot and the history row are committed in one transaction. If the lock is not available within `LockWait`, the append fails with a storage error, never an optimistic lock error, and nothing is committed.

The head and the current snapshot are read in one read transaction, and so are the head and the journal rows up to it. A gap in the journal is a storage error; a shortened result is never returned as a success. Connections of other Store values for the same file are safe to use at the same time.

## Retention

Omitted retention means no history. With `KeepLatest(n)`, each pair write inserts the history row, and then a second `BEGIN IMMEDIATE` transaction deletes the oldest rows beyond `n` after the append is committed. `snapshot_current`, `head` and the journal are never deleted, and an event-only append does not touch the history. A retention failure is logged, passed to `WithRetentionFailureHandler` after the Store lock is released, and does not change the result of the committed append, even if the handler or the log output panics.

## Operations

- Use a local file system. SQLite WAL mode does not work over network file systems.
- The file gains `-wal` and `-shm` files next to it while it is open; keep them with the database when copying.
- Permissions: the process needs read and write access to the directory of the file.
- A file written by this package is read and written by the Rust v5 implementation, and the reverse. There is no migration step between them.
