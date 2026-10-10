# Migration to the envelope API

[日本語](MIGRATION_GUIDE.ja.md)

The source implements common specification v4; the Go module path remains `github.com/j5ik2o/event-store-adapter-go/v2`. This does not change the distribution version or announce a release.

## Code changes

1. Replace old `pkg` imports with the module root (`eventstore`) and `memory` or `dynamodb`. The old package and its aliases are removed.
2. Pass `context.Context` first to each of the four operations and to `dynamodb.New`.
3. Implement `AggregateID` using `TypeName()` and `Value()`. Remove `AsString`, KeyResolver and shard configuration. Type names cannot contain `-`; the resulting ID has a 1024-byte UTF-8 limit.
4. Use arbitrary event/state types and `Serializer[T]` (for example `NewJSONSerializer[T]()`). Remove Event/Aggregate interfaces and map converters. Put metadata in validated `NewEventEnvelope`/`NewSnapshotEnvelope` values; optional manifest is specified with `WithManifest`.
5. Remove version arguments, version comparison and `IsCreated`. Assign consecutive event numbers starting at 1. A pair write requires the same event and snapshot number; the previous head is inferred from the event number.
6. Use `memory.NewStore` then `memory.New`, or `dynamodb.New` with `Config` naming distinct journal/snapshot/head tables and the history index. Settings are supplied through `Option`. No history is the default or `NoRetention()`; positive `KeepLatest(n)` replaces keep-snapshot options. Zero is an error, not a history count. Memory rejects TTL mode; DynamoDB accepts `RetentionTTL` and non-negative `WithTTLGraceSeconds`.
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

## Existing Go data

Go migration support consists of these instructions. There is no migration tool, old-layout reader, automatic conversion or runtime fallback. The Rust v3 migration tool targets a different old layout and must not be used for Go data.

1. Stop old writes and retain a backup. Use the **old library version and its API** to read the old journal and snapshots, including every page. Record each aggregate's event order, complete metadata, payload and state before changing clients.
2. In user-owned migration code, separate metadata from embedded domain payload/state. Determine the old ID and event type definitions. The old `GetOccurredAt() uint64` does not define a time unit: consult the application's old definition before converting to `time.Time`. Do not guess a unit or reconstruct lost time precision or type information.
3. Provision a **new three-table layout**, its history GSI and optional TTL as described in [DATABASE_SCHEMA](DATABASE_SCHEMA.md). Let `dynamodb.New` create the matching layout-version-1 configuration items; do not mix old and new tables.
4. For each aggregate, create validated envelopes and write events through the new API in preserved order with consecutive numbers **1, 2, 3, …**. Build any snapshot from the state reached at that number and use a pair write with the same number. An old snapshot alone does not replace the event history needed for this rewrite.
5. Read back through the new API and compare event counts, metadata, payload and restored state with the retained old read results. Switch application reads/writes only after these checks; keep the backup until the migration has been accepted.

The old two-table sharded keys and snapshot version attributes cannot be reused in place. Time conversion and metadata/payload separation belong to the application's migration code.
