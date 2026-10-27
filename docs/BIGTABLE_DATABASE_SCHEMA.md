# Bigtable storage schema

This document describes the 5.x layout used by the Go `bigtable` package (`github.com/j5ik2o/event-store-adapter-go/v5/bigtable`). The library writes the configuration rows only through the explicit `bigtable.Initialize`; the regular `bigtable.New` never creates tables, column families, or configuration rows. Tables and column families are always created by you. The layout is the one of the Rust v5 implementation.

The regular store reads only this layout. The Go library before v5 had no Bigtable storage, so there is nothing to migrate; see the [migration guide](MIGRATION_GUIDE.md). The first release of this storage does not provide a change feed.

## Layout

### Tables and keys

The store uses two tables in one Bigtable instance, a journal table and a head table. They must be different tables: giving the same table ID for both is a configuration error.

| Table | Column families | Row key | Rows |
|:------|:----------------|:--------|:-----|
| journal | `event`, `history`, `config` | `P(aid) \|\| U64BE(seq_nr)` | one per event, plus the configuration row |
| head | `head`, `snapshot`, `config` | `P(aid)` | one per aggregate, plus the configuration row |

`aid` is the UTF-8 string `type_name-value`, assembled from `AggregateID`. The type name cannot contain a hyphen; the value may. The complete string is limited to 1024 bytes. An empty type name or value is allowed.

```text
P(aid)             = H(aid) || L16(length(A)) || A
head row key       = P(aid)
journal row key    = P(aid) || U64BE(seq_nr)
configuration row  = 0x00   (a single byte, in both tables)
```

- `A` is the UTF-8 bytes of `aid`.
- `H(aid)` is the 8-byte big-endian [FNV-1a 64](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function) hash of `A`.
- `L16` is the byte length of `A` as an unsigned 16-bit big-endian integer. Because the length is part of the key, another aggregate's key never starts with `P(aid)` even when two hashes collide.
- `U64BE(seq_nr)` is the sequence number as an unsigned 64-bit big-endian integer, so the key order is the sequence order. No decimal string, `Display` output, or `prefix + 0xff` is used to build a range.
- A `#`, a NUL, or non-ASCII text in `aid` is just bytes. It is never interpreted as a separator.
- The configuration row key `0x00` is one byte, so it never equals a data row key, which always starts with the 8-byte hash and the 2-byte length.

#### Column families and garbage collection

Create all of the column families before the first `bigtable.Initialize`. The `cbt` commands below create the two tables and every family of the example names `esa_journal` and `esa_head` with the garbage-collection rules above:

<!-- bigtable-schema:tables -->
```sh
cbt createtable esa_journal
cbt createfamily esa_journal event
cbt createfamily esa_journal history
cbt createfamily esa_journal config
cbt setgcpolicy esa_journal event maxversions=1
cbt setgcpolicy esa_journal history maxversions=1
cbt createtable esa_head
cbt createfamily esa_head head
cbt createfamily esa_head snapshot
cbt createfamily esa_head config
cbt setgcpolicy esa_head head maxversions=1
cbt setgcpolicy esa_head snapshot maxversions=1
```

The rules the library requires:

| Table | Family | Garbage-collection rule |
|:------|:-------|:------------------------|
| journal | `event` | keep at most one version, no age rule |
| journal | `history` | keep at most one version, no age rule |
| journal | `config` | a version rule is optional, no age rule |
| head | `head` | keep at most one version, no age rule |
| head | `snapshot` | keep at most one version, no age rule |
| head | `config` | a version rule is optional, no age rule |

`bigtable.New` reads each table's schema with `GetTable` (through the admin client) and rejects a missing family, or a data family whose rule is not exactly "keep at most one version", with a configuration error. Do not set an age rule on a data family: expired cells would lose data. Do not set an age rule on a `config` family either: the configuration row is written with cell timestamp 0, so an age rule would expire it at once, and every later `New` and initialization check would fail. Correctness never depends on how soon garbage collection runs, because every read asks for only the latest cell of a column, and every write sets the cell timestamp to 0 and deletes the older versions of a column it replaces.

## Configuration

### Configuration rows

Each table holds one configuration row, row key `0x00`, with the column `config:metadata`. The value is a UTF-8 JSON object, identical in both tables:

| Field | Type | Requirement |
|:------|:-----|:------------|
| `store_id` | string | Lowercase hyphenated UUID, generated at initialization |
| `layout_version` | integer | 1 |
| `keep_snapshot_count` | null or string | `null` for "current snapshot only", otherwise a positive integer written as a decimal string without an exponent or leading zeros |
| `retention_mode` | string | `delete` |
| `envelope_limit_bytes` | integer | 17 through 8388608, default 1048576 |

`keep_snapshot_count` is a string only because a count can be as large as a signed 64-bit integer, which a double-precision JSON number cannot always represent.

`New` verifies the column families of both tables and then reads both configuration rows. A missing row, invalid JSON, a duplicate JSON key, a missing field, or a field of the wrong type or shape is a configuration error. Differences are compared by value, not by key order or whitespace. Any difference between the two rows, a `layout_version` other than 1, a `retention_mode` other than `delete`, a `store_id` that is not a lowercase UUID, or a difference from the client's settings (the history count or `Config.EnvelopeLimitBytes`) is a configuration error. `New` never repairs or completes a configuration row.

Settings are validated before any request is sent: a nil client, an empty or equal pair of table IDs, TTL retention, `Config.ChangeFeed`, and an envelope limit outside the range above are configuration errors. The envelope limit `0` means the default 1048576.

## Initialization

Initialize two new tables, which hold no data rows and no configuration rows, exactly once before the first `New`, while no writer is running. The program calls `Initialize` itself once (for example from a setup command); every start, the first one included, then calls `New`:

```go
keep, err := eventstore.KeepLatest(2)
if err != nil {
    return err
}
cfg := bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"}
if err := bigtable.Initialize(ctx, data, cfg, eventstore.WithRetentionCount(keep)); err != nil {
    return err
}
```

`Initialize` validates the settings and the table IDs, and reads both tables' configuration rows. If either table already has one, it returns a configuration error without writing. Otherwise it generates one `store_id` and writes the same JSON, first to the journal table and then to the head table, each as a conditional write that succeeds only when the row's `config:metadata` does not exist. It never creates tables, column families, or data rows, and it does not check the column families itself: a missing family makes the write fail with a storage error, and `New` is the call that rejects a wrong family or rule.

To prepare the configuration rows by hand, write the JSON above to `config:metadata` of row key `0x00` in both tables with a Bigtable client of your choice (cell timestamp 0). The two values must be the same, and the column families must exist first.

### Recovering from an interrupted initialization

The two configuration rows are written one after the other, and Bigtable has no transaction across rows or tables. If the call stops after the first write (a crash, a lost connection, an error from the second table), only the journal table has a configuration row. The library does not complete the missing row on its own, because a configuration the library did not just write cannot be trusted. In that state `Initialize` returns a configuration error (a row already exists), and `New` returns a configuration error because the head table has no row.

To recover:

1. Keep every writer and reader of the two tables stopped.
2. Read the row with key `0x00` in both tables. If both tables have it, the initialization finished; do not delete anything. If only one table has it, check that neither table has a data row, that is, that the store was never used. If a data row exists, this is not an interrupted initialization and this procedure does not apply: do not delete the row.
3. Delete the leftover row with key `0x00` from the table that has it (a `MutateRow` with a `DeleteFromRow` mutation). With the Go client (`gbigtable` is `cloud.google.com/go/bigtable`):

```go
m := gbigtable.NewMutation()
m.DeleteRow()
err := data.Open("esa_journal").Apply(ctx, "\x00", m) // the table that has the row
```

4. Call `Initialize` again with the same settings.

## Records

### Journal rows

One event becomes one row in the journal table. The row is created only if `event:envelope` does not exist, and it is never overwritten or deleted.

| Cell | Value |
|:-----|:------|
| `event:envelope` | The event envelope for the row key's sequence number. Never deleted |
| `event:has_snapshot` | ASCII `1` if that append carried a snapshot, otherwise ASCII `0`. Never deleted |
| `history:state` | ASCII `active` when a history snapshot is first written for the row, `retired` after it is deleted. Absent when no history was written |
| `history:envelope` | The history snapshot envelope. Deleted when the state becomes `retired` |

No `history` cell is written when no history count is configured. The cell timestamp is 0 (a fixed value); it is only used for Bigtable's own bookkeeping, and `occurred_at` is never derived from it.

### Head rows

The head table keeps one small row per aggregate. It never holds all events or `n` history snapshots, so it does not grow with the history count.

| Cell | Value |
|:-----|:------|
| `head:seq_nr` | Last committed sequence number as 20 zero-padded ASCII digits. Used to compare the head |
| `head:last_event` | The event envelope for that sequence number |
| `head:has_snapshot` | ASCII `1` if that append carried a snapshot, otherwise ASCII `0` |
| `snapshot:current` | The current snapshot envelope; absent when the aggregate has no snapshot |

When `head:has_snapshot` is `1`, the sequence number in `snapshot:current` equals `head:seq_nr`. An event-only append keeps the existing `snapshot:current` and sets `head:has_snapshot` to `0`. The snapshot's sequence number is never above the head's.

### Envelopes

The library builds each envelope as a fixed binary layout. `U16`, `U32`, and `U64` are big-endian; `I64` is a big-endian signed 64-bit two's complement integer. The leading `0x01` is the format version. The serializers receive only the payload.

```text
event    = 0x01 || U16(len(aid)) || aid || U64(seq_nr)
           || I64(occurred_at_ns) || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
snapshot = 0x01 || U64(seq_nr)
           || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
```

Lengths are byte counts. Unused trailing bytes, a length mismatch, or an unknown format version is a storage error when read. A payload that cannot be decoded from a well-formed envelope is a serialization error. The occurrence time is kept in nanoseconds in the envelope, never in the Bigtable cell timestamp (which has microsecond precision).

#### Envelope size limit

`Config.EnvelopeLimitBytes` caps one serialized envelope including its metadata. The default is 1 MiB (1,048,576 bytes), the maximum is 8 MiB (8,388,608), and the minimum is 17 bytes (the smallest snapshot envelope). The value is stored in both configuration rows, and every client must use the same one. An input over the limit is rejected before any write as a `ContractViolationError` with rule `BT-16` that names the sequence number. Change the limit only by stopping every client and updating both configuration rows to the same new value, then opening every client again to check them. A data client built over your own gRPC connection must allow received messages as large as the limit plus the row overhead (the default of gRPC is 4 MiB).

## Writes and reads

Input checks and serialization finish before any request. `PersistEvent` and `PersistEventAndSnapshot` then run these steps:

1. Read the aggregate's head row with strong consistency. No row means the current sequence number `h` is 0. Otherwise validate `h` and the last envelope. A row with a missing required cell is never treated as a new aggregate.
2. Compare the input's sequence number with `h`. Only the next contiguous number continues.
3. If `h > 0`, write the head's last event `h` to the journal row `h` with one conditional write (and, when a history count is set and the head says `has_snapshot = 1`, the snapshot of `h` in the same write). If this write cannot be confirmed, the append fails with a storage error and the head does not advance.
4. Run a conditional write on the head row. A new aggregate is created on the side where `head:seq_nr` does not exist; an update succeeds only on the side where `head:seq_nr` is the `h` read in step 1, and it replaces `head:seq_nr`, `head:last_event`, and `head:has_snapshot` (and `snapshot:current` when a snapshot is attached). **This conditional write is the commit point.**
5. Write the new last event to the journal and run the retention step. A failure here is reported and left for later; it never turns the committed append into a failure.

If the commit-point write is not applied because another write was committed after step 1, the result is an optimistic-lock error. A new-aggregate write that loses to an earlier creation is the same error. If the response of the commit-point write is lost, the result is a storage error that does not claim the write failed. A number that skips the next expected one (or a first event other than 1) is a contract violation.

This keeps one invariant: when the head is at `h`, events `1` through `h - 1` are in the journal and event `h` is in the head. A snapshot attached to an append is written to its journal row before the next append, so the latest not-yet-written snapshot is always in `snapshot:current`.

`GetLatestSnapshotByID` reads the head row once and returns `head:seq_nr` and, if present, `snapshot:current`. No head returns `nil`; a head without a snapshot returns a `SnapshotRead` with a nil snapshot and the head's sequence number.

`GetEventsByIDSinceSeqNr` reads the head first and keeps its `h` and last event for the whole call. With `start = max(1, seq_nr)`:

1. No head, or `start > h`: an empty list, with no journal request.
2. `start == h`: only the head's last event, with no journal request.
3. `start < h`: the journal rows from `start` up to, and excluding, `h` (a range from `P || U64BE(start)` to `P || U64BE(h)`), followed by the head's last event.

The head is not read again if it advances during the call. Rows at or above `h` are never mixed in. A `ReadRows` stream that ends early is resumed after the last completed row; an incomplete row is read again. A gap inside the requested range is a storage error.

## Retention

`keep_snapshot_count` is `null` when no retention count is supplied (or `NoRetention()`), and `n` for `KeepLatest(n)`. With `null`, no history is kept and only `snapshot:current` holds a snapshot. With `n`, the history is the active `history` cells of the aggregate's journal rows together with the head's current snapshot that is not yet in a journal row, so the head's snapshot counts toward the `n`. `snapshot:current` is never deleted by retention.

After an append that carried a snapshot commits, the store fixes the head position `h`, reads the aggregate's journal rows from `P || U64BE(1)` up to, and excluding, `P || U64BE(h + 1)`, and lists the rows whose `history:state` is `active`. When `h` carried a snapshot and has no row yet, `h` is added to the list; a `retired` row is never made `active` again. The newest `n` stay, and each older row is retired with one conditional write that succeeds only if its `history:state` is still `active`: it deletes `history:envelope` and sets `history:state` to `retired`. The `event` cells stay. Only the state column is read, so no history payload is transferred.

A row that was retired is never filled again. A late writer that copies the same history finds `event:envelope` already there and writes nothing, so a deleted history does not come back. If retention fails, a `log/slog` error with aid and error is emitted and `WithRetentionFailureHandler` is called, the committed append still succeeds, and the next snapshot append retries; a panic in the log path or the handler is recovered. The cost of retention grows with the aggregate's journal length. TTL retention is not provided; `retention_mode` is always `delete`.

### Changing the history count

Changing `keep_snapshot_count` is an offline administration procedure; the stored value in both configuration rows and every client's retention option must agree.

1. Stop every client of the store and make sure no append, read, write-out, or retention work is still running. Record the current and target settings, and the progress, so that an interrupted run can resume.
2. For every head row (every row except key `0x00`), read `h` and the last event, and complete the journal write of row `h` by the **old** history count: if the old setting kept history and the last event carried a snapshot, include that snapshot in the same first write. This keeps a not-yet-written snapshot from being lost from the new selection.
3. For every aggregate, read all active history up to `h`. Keep the newest `n` of the target (or none when the target is `null`) and retire the rest with the conditional write above. Leave the event envelopes and flags, the head, and `snapshot:current` unchanged. Never remove a `retired` state and never recreate history.
4. When every aggregate is done, update `keep_snapshot_count` in the configuration rows of both tables to the target. Open every client again with the new options so they check the configuration, then resume work.

If a step fails, keep the clients stopped and rerun with the same target. A write-out or retirement that already finished does not bring history back when it is repeated. If only one of the two configuration rows has been updated, do not resume until both match. Deleted history cannot be restored, so a rollback of the whole store is not assumed. Raising the count, or setting a count where there was none, does not recreate history that was deleted or never kept; the new setting applies from the next snapshot write.

## Operations

### App profile, connection, and authentication

Every read, write, retention, and configuration check of a running system must use an app profile that is pinned to one cluster and allows single-row transactions. If the instance has replica clusters, point the adapter's operations at that one cluster. During an outage of that cluster the store returns storage errors and waits for recovery. Multi-cluster writes, multi-cluster routing, and automatic failover are not supported. The library does not check the app profile; you create it and pass its ID as `ClientConfig.AppProfile` when you create the data client. An empty ID uses the instance's default app profile, which is only correct when that profile already meets these requirements.

The library takes two clients from `cloud.google.com/go/bigtable` and never creates or closes them:

| Client | Use |
|:-------|:----|
| `*bigtable.Client` (data) | `ReadRows` and `CheckAndMutateRow` only |
| `*bigtable.AdminClient` | `GetTable` (`TableInfo`), only when `New` opens the store, to check the column families |

- **Emulator**: connect both clients to the emulator yourself (for example by setting `BIGTABLE_EMULATOR_HOST`, which the SDK honors, or by passing `option.WithGRPCConn` of a plaintext connection). No credentials are added.
- **Production Cloud Bigtable**: build both clients with your own authentication (for example application default credentials or `option.WithTokenSource`). Authentication is the clients' concern: the library adds no credentials, and it does not cache or refresh a token. A failing credential makes the request fail before it is sent, and that operation returns a storage error.
- **Retries**: a conditional write is never retried by the library or the SDK. If its response is lost, the operation returns a storage error that does not say whether the write was applied; send the same append again and expect an optimistic lock error if it was.

