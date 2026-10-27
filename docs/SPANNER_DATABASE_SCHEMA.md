# Spanner storage schema

[日本語](SPANNER_DATABASE_SCHEMA.ja.md)

## Layout

This is layout version 1 of the Spanner profile of [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md), used by the Go `/v5` module. The rows are the ones the JavaScript v5 implementation writes; this package adds no layout of its own. Only the GoogleSQL dialect is supported. All five tables live in the same database, and every table name is derived from one prefix as `<prefix>_<logical name>`: with the prefix `esa` the tables are `esa_journal`, `esa_head`, `esa_snapshot_current`, `esa_snapshot_history` and `esa_config`. The prefix and every derived name must be GoogleSQL identifiers (`[A-Za-z_][A-Za-z0-9_]*`) of at most 128 characters.

`spanner.New` never runs DDL. It only verifies the tables and the configuration row, and it creates nothing that is missing. You create the tables and the configuration row yourself with the procedure below. The examples use the prefix `esa`; replace it with your own prefix.

<!-- spanner-schema:tables -->
```sql
CREATE TABLE esa_journal (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  occurred_at_ns INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_head (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  type_name STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  event_occurred_at_ns INT64 NOT NULL,
  event_manifest STRING(MAX) NOT NULL,
  event_payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_current (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_config (
  singleton INT64 NOT NULL,
  store_id STRING(36) NOT NULL,
  layout_version INT64 NOT NULL,
  keep_snapshot_count INT64,
  retention_mode STRING(8) NOT NULL,
  ttl_grace_days INT64,
  change_feed_enabled BOOL NOT NULL
) PRIMARY KEY (singleton);
```

`esa_snapshot_history` depends on the retention mode. Use one of the two definitions.

Delete mode removes old snapshot history rows immediately. There is no row deletion policy:

<!-- spanner-schema:snapshot-history-delete -->
```sql
CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr);
```

TTL mode marks old rows and lets Spanner remove them later. The row deletion policy carries the grace period in days. `7` is only an example, not a default:

<!-- spanner-schema:snapshot-history-ttl -->
```sql
CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr),
  ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL 7 DAY));
```

## Configuration

`spanner.Config` has four fields:

| Field | Meaning |
|---|---|
| `TablePrefix` | The prefix of the five table names. Required. |
| `TTLGraceDays` | The grace period of TTL retention in days. At least 1 with `RetentionTTL`, and 0 otherwise. It must equal `ttl_grace_days` of the configuration row and the `INTERVAL` of the row deletion policy. It is not `WithTTLGraceSeconds`: a non-zero `WithTTLGraceSeconds` is a configuration error. |
| `TransactionRetryLimit` | How many times an aborted transaction is re-run; the first attempt is not counted. Nil means 5, zero means no re-run, and a negative value is a configuration error. |
| `ChangeFeed` | Must be false. This version provides no change feed; true is a configuration error. |

The common `Option` values are `WithRetentionCount`, `WithRetentionMode` (`RetentionDelete` or `RetentionTTL`; TTL needs a retention count) and `WithRetentionFailureHandler`.

`esa_config` holds exactly one row, with `singleton = 1`:

- `store_id`: a lowercase UUID string.
- `layout_version`: the `INT64` value `1`.
- `keep_snapshot_count`: `NULL` for no snapshot history. Otherwise a positive integer that must equal the retention count of every client.
- `retention_mode`: `delete` or `ttl`.
- `ttl_grace_days`: a positive number of days in TTL mode, otherwise `NULL`.
- `change_feed_enabled`: record `FALSE`. A row with `TRUE` is rejected as a configuration error.

For delete mode without snapshot history, set `keep_snapshot_count` to `NULL` and `retention_mode` to `'delete'`. TTL mode requires a retention count, so `keep_snapshot_count` must be a positive integer there.

Delete mode, keeping the newest 3 history rows per aggregate:

<!-- spanner-schema:config-delete -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled)
VALUES (1, GENERATE_UUID(), 1, 3, 'delete', NULL, FALSE);
```

TTL mode, keeping the newest 3 history rows and a grace period of 7 days. The `7` must equal the `INTERVAL 7 DAY` of the row deletion policy and `Config.TTLGraceDays`:

<!-- spanner-schema:config-ttl -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled)
VALUES (1, GENERATE_UUID(), 1, 3, 'ttl', 7, FALSE);
```

## Initialization

Run this procedure outside the application, once, while no client is writing. The application never creates or repairs the layout, so it calls `spanner.New` in the same way on the first start and at every restart.

1. Stop every writer of the store.
2. Run the DDL above, with your prefix, and wait for it to finish. Use the `esa_snapshot_history` definition for your retention mode.
3. Confirm that each of the five tables is empty (`SELECT COUNT(*) FROM esa_journal;` and the same for the other four).
4. Write the single configuration row with the matching `INSERT` above.
5. Start the writers. With the domain types of the [README](../README.md), the factory is:

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

`spanner.New` returns a configuration error when anything it checks is missing or does not match. It checks that the database uses the GoogleSQL dialect; the column names and types and the primary keys of the five tables; the configuration row (`store_id` as a lowercase UUID, `layout_version` 1, `keep_snapshot_count` equal to the retention count of the client, `retention_mode`, `ttl_grace_days`, and `change_feed_enabled` as `FALSE`); and the row deletion policy of `esa_snapshot_history`, which must match the grace period in TTL mode and be absent in delete mode. If the destination cannot be read, it returns a storage error. It does not repair anything. The client belongs to the caller, who closes it.

DDL changes while the store is in use, and data changes that bypass the library, are not supported.

## Records

Columns:

- `hash_prefix`: the FNV-1a 64 hash of `aid`, as 8 bytes in big-endian order. It is not converted to a signed integer and is not reduced by a shard count. Keys are compared as the exact pair `(hash_prefix, aid)`; if two hashes collide, different `aid` values are still different keys.
- `aid`: `typeName-value`. The UTF-8 size is at most 1024 bytes, which the library checks before writing.
- `seq_nr`: 1 to 2^53 - 1.
- `occurred_at_ns` and `event_occurred_at_ns`: signed 64-bit nanoseconds since the Unix epoch, stored as `INT64`, not as `TIMESTAMP`.
- `esa_snapshot_history.expired_at`: the time at which a row was marked for TTL. It is not the event's occurrence time. It stays `NULL` for rows that are not marked, and TTL never removes those.
- `esa_head` holds the latest committed event with its metadata and payload. `esa_snapshot_current` holds the latest snapshot, and its row is absent if no snapshot has been written.

## Writes and reads

Writes run in a SERIALIZABLE read-write transaction. If the transaction is aborted, it is re-run with a new transaction up to `TransactionRetryLimit` times, with the same event and the same sequence number; beyond the limit the write fails with a storage error and nothing is committed.

Inside the transaction, the head `seq_nr` of the aggregate is read first. A new number that is less than or equal to the head number is an optimistic lock error; a number greater than head + 1 is a contract violation (rule `W-8`). Otherwise the journal row and the head row (inserted for the first event, updated afterwards) are committed in one transaction. A write with a snapshot also inserts or updates the `esa_snapshot_current` row in the same transaction, and, when retention is configured, adds an `esa_snapshot_history` row with `expired_at` set to `NULL`. `occurred_at_ns` is the event time in nanoseconds.

If the commit fails, or its response is lost, the outcome is unknown: the write fails with a storage error, and nothing compensates it. Sending the same event again then fails with an optimistic lock error if the first commit had succeeded.

The head and the current snapshot are read in one strongly consistent read-only transaction, and so are the head and the journal rows up to it (a primary key range). A gap in the journal, or a stored number out of range, is a storage error; a shortened result is never returned as a success.

## Retention

Omitted retention means no history. With `KeepLatest(n)`, each write with a snapshot inserts a history row. After the write is committed, separate SERIALIZABLE transactions keep the newest `n` unmarked history rows of the aggregate:

- Delete mode deletes the older rows.
- TTL mode sets `expired_at` of the older rows once. Marked rows are not counted again, and Spanner removes them after the grace period.

A part handles at most 1000 rows; when more remain, the candidates are selected again for the next part. `esa_snapshot_current`, `esa_head` and the journal are never deleted, and an event-only write does not touch the history. A retention failure is logged and passed to `WithRetentionFailureHandler`, and it does not change the result of the committed write, even if the handler or the log output panics.

## Operations

- Create the layout and the configuration row outside the application; give the application's identity only the data access it needs.
- To change the retention count or the retention mode, stop every writer and change the DDL, the row deletion policy and the configuration row together. The library has no tool for it.
- In TTL mode, Spanner removes expired rows in the background and not immediately. Unmarked rows are never removed by TTL.
- The tests of this repository run against the Cloud Spanner emulator in a container; they never connect to a real service.
- A destination written by the JavaScript v5 implementation is read and written by this package, and the reverse. There is no migration step between them.
