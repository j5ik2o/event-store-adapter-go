# Migration guide

[日本語](MIGRATION_GUIDE.ja.md)

## Overview

This guide covers the migration to the envelope API.

This library follows [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md); its major version 5 matches the contract revision. The Go module path is `github.com/j5ik2o/event-store-adapter-go/v5`.

## Code changes

### Migration to the envelope API

1. Replace old `pkg` imports with the module root (`eventstore`) and `memory` or `dynamodb`. The old package and its aliases are removed.
2. Pass `context.Context` first to each of the four operations and to `dynamodb.New`.
3. Implement `AggregateID` using `TypeName()` and `Value()`. Remove `AsString`, KeyResolver and shard configuration. Type names cannot contain `-`; the resulting ID has a 1024-byte UTF-8 limit.
4. Use arbitrary event/state types and `Serializer[T]` (for example `NewJSONSerializer[T]()`). Remove Event/Aggregate interfaces and map converters. Put metadata in validated `NewEventEnvelope`/`NewSnapshotEnvelope` values; optional manifest is specified with `WithManifest`.
5. Remove version arguments, version comparison and `IsCreated`. Assign consecutive event numbers starting at 1. A pair write requires the same event and snapshot number; the previous head is inferred from the event number.
6. Use `memory.NewStore` then `memory.New`, `sqlite.Initialize`, `sqlite.Open` then `sqlite.New`, or `dynamodb.New` with `Config` naming distinct journal/snapshot/head tables and the history index, or `spanner.New` with a `Config` naming the table prefix of an externally provisioned layout, or `bigtable.Initialize` once and then `bigtable.New` with a `Config` naming the journal and head tables. Settings are supplied through `Option`. No history is the default or `NoRetention()`; positive `KeepLatest(n)` replaces keep-snapshot options. Zero is a configuration error, not a history count. Memory and SQLite reject TTL mode; DynamoDB accepts `RetentionTTL` and non-negative `WithTTLGraceSeconds`.
7. Handle the five error kinds with `KindOf`/`errors.As`, and preserve causes through `Unwrap`. Retention failure logs/notifications do not undo a successful write.

Restoration begins at the snapshot's next event, or at 1 without a snapshot. An absent head returns nil. Do not use `HeadSeqNr` as the starting position; it can differ from the snapshot number, especially during concurrent DynamoDB reads.

```go
read, err := store.GetLatestSnapshotByID(ctx, id)
if err != nil { return err }
if read == nil { return fmt.Errorf("aggregate not found") }
since := eventstore.SeqNr(1)
var aggregate orderState
if read.Snapshot != nil {
    aggregate = read.Snapshot.Aggregate()
    since = read.Snapshot.SeqNr() + 1
}
events, err := store.GetEventsByIDSinceSeqNr(ctx, id, since)
if err != nil { return err }
for _, event := range events {
    aggregate.Total += event.Payload().Added
}
```

The snippet uses the domain types from [README](../README.md). The runnable [example](../example_test.go) and [repository tests](../test/user_account_repository_test.go) cover snapshot 1 with head 2 and replay without a snapshot.

## Data migration

### Existing Go data

Go migration support consists of these instructions. There is no migration tool, old-layout reader, automatic conversion or runtime fallback. The Rust v3 migration tool targets a different old layout and must not be used for Go data.

1. Stop old writes and retain a backup. Use the **old library version and its API** to read the old journal and snapshots, including every page. Record each aggregate's event order, complete metadata, payload and state before changing clients.
2. In user-owned migration code, separate metadata from embedded domain payload/state. Determine the old ID and event type definitions. The old `GetOccurredAt() uint64` does not define a time unit: consult the application's old definition before converting to `time.Time`. Do not guess a unit or reconstruct lost time precision or type information.
3. Provision a **new three-table layout**, its history GSI and optional TTL as described in [DYNAMODB_DATABASE_SCHEMA](DYNAMODB_DATABASE_SCHEMA.md). Let `dynamodb.New` create the matching layout-version-1 configuration items; do not mix old and new tables.
4. For each aggregate, create validated envelopes and write events through the new API in preserved order with consecutive numbers **1, 2, 3, …**. Build any snapshot from the state reached at that number and use a pair write with the same number. An old snapshot alone does not replace the event history needed for this rewrite.

The old two-table sharded keys and snapshot version attributes cannot be reused in place. Time conversion and metadata/payload separation belong to the application's migration code.

### SQLite

The Go library before v5 had no SQLite storage, so there is no old SQLite data to migrate and this repository provides no SQLite migration tool. To adopt SQLite, start from a new file:

1. Choose a path on a local file system and call `sqlite.Initialize` once with the retention option you will use later.
2. Call `sqlite.Open` with the same option, then `sqlite.New` with your serializers. Do not call `Initialize` again on the same file; it fails with a configuration error.
3. To move data from another storage, read the events there and write them through the new API with consecutive numbers starting at 1, as in the steps above. See [SQLITE_DATABASE_SCHEMA](SQLITE_DATABASE_SCHEMA.md) for the file layout. A file written by the Rust v5 implementation can be opened directly.

### Spanner

The Go library before v5 had no Spanner storage, so there is no old Spanner data to migrate and this repository provides no Spanner migration tool. To adopt Spanner, start from a new layout:

1. Create the five tables and the single configuration row outside the application, with the DDL and the `INSERT` of [SPANNER_DATABASE_SCHEMA](SPANNER_DATABASE_SCHEMA.md) for the retention mode you will use. The library never runs DDL.
2. In the application, create a `*spanner.Client` and call `spanner.New` with the same retention option and, for TTL retention, `Config.TTLGraceDays`. Call it in the same way at the first start and at every restart; there is nothing to initialize from the program.
3. To move data from another storage, read the events there and write them through the new API with consecutive numbers starting at 1, as in the steps above. A destination written by the JavaScript v5 implementation can be opened directly.

### Bigtable

The Go library before v5 had no Bigtable storage, so there is no old Bigtable data to migrate and this repository provides no Bigtable migration tool. To adopt Bigtable, start from two new tables:

1. Create the journal and head tables and their column families outside the application, with the `cbt` commands of [BIGTABLE_DATABASE_SCHEMA](BIGTABLE_DATABASE_SCHEMA.md). The library never creates tables.
2. Once, while no writer is running, call `bigtable.Initialize` with the retention option you will use later. It writes only the two configuration rows and fails with a configuration error if either table already has one.
3. In the application, create a `*bigtable.Client` and a `*bigtable.AdminClient` and call `bigtable.New` with the same retention option and `Config`. Call it in the same way at the first start and at every restart. If an initialization stops halfway, follow the recovery procedure of the schema document.
4. To move data from another storage, read the events there and write them through the new API with consecutive numbers starting at 1, as in the steps above. A pair of tables written by the Rust v5 implementation can be opened directly.

## Verification and cutover

5. Read back through the new API and compare event counts, metadata, payload and restored state with the retained old read results. Switch application reads/writes only after these checks; keep the backup until the migration has been accepted.
