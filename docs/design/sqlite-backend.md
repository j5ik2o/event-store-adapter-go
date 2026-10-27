# SQLite storage design record

[日本語](sqlite-backend.ja.md)

This record maps the rules of the SQLite profile of [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) (`storage/sqlite.md`) that the shared conformance data does not cover to the tests of this repository. It records the evidence at the time the `sqlite` package was added. The current behavior is described in [SQLITE_DATABASE_SCHEMA.md](../SQLITE_DATABASE_SCHEMA.md).

## Decisions

| Decision | Choice | Reason |
|---|---|---|
| Driver | `modernc.org/sqlite` v1.59.0 | Pure Go, so `go test -race` and CI need no cgo. v1.60.0 and later require `go 1.26.0`, which would raise the minimum Go version of this module. The bundled SQLite is 3.53.4, above the required 3.51.3. |
| Public shape | `Initialize`, `Open`, `Store.Close`, `New` | `Open` returns a `*Store` that owns the file and the connection; `New` connects serializers, like `memory.NewStore` and `memory.New`. |
| Connection | One `*sql.Conn` per `Store`, used under a mutex | The connection is never shared by two operations (SQ-17) and every connection gets the settings of SQ-1. |
| Transactions | `BEGIN`, `BEGIN IMMEDIATE`, `COMMIT` and `ROLLBACK` run as SQL | An append needs `BEGIN IMMEDIATE`; the driver's `_txlock` would apply to every transaction. |
| Layout | Same DDL words as the Rust v5 `layout.rs` | Both languages read and write the same file. No layout of this package. |
| Column collation of `aid` | Compound `SELECT ... WHERE 0 UNION ALL SELECT ?, 0 UNION ALL SELECT ?, 1 ORDER BY aid, ordinal` with the pairs ('a','A') and ('x ','x') | The driver does not expose `sqlite3_table_column_metadata`. The probe reads no stored row and writes nothing, and the collation of a compound `ORDER BY` follows the left-most `SELECT`. The primary key index is read with `pragma_index_xinfo`. |
| Store ID | Random version 4 UUID in lowercase | No new dependency. |
| Time | `UnixNano()` as INTEGER; read back with `time.Unix(0, n).UTC()` | The accepted range of `OccurredAt` is already checked by the envelope. |

## Rules and evidence

Tests are in `sqlite/` unless a path is given. "Conformance" means a case of the distributed data run by `internal/conformance` against a real SQLite file (`TestPublicSQLiteConformance`, `TestConformance`).

| Rule | Evidence |
|---|---|
| SQ-1, SQ-14 connection settings | `TestConnectionEnablesWriteAheadLogForFileInUse`, `TestConnectionConvertsRollbackJournalFileToWriteAheadLog`. `synchronous`, `locking_mode`, `read_uncommitted` and `busy_timeout` are set and read back by every connection in `connection.go`; they cannot be observed from another connection, so the lock wait is measured by `TestLockWaitReturnsStorageErrorAfterWaitingAndKeepsStoreUsable` and `TestLockWaitZeroFailsImmediately`. |
| SQ-15 library version | `TestMeetsMinimumVersion` (numeric comparison per component). |
| SQ-2, SQ-16 layout check | `TestOpenRejects` (15 broken files), `TestOpenAcceptsHandBuiltRustLayout`, `TestAidCollationAcceptsDefaultBinary`, `TestAidCollationRejectsMisconfiguredFiles` (column and primary key index disagree; the file is unchanged afterwards). |
| SQ-19, SQ-20 configuration agreement | `TestOpenRejectsUnsupportedSettings`, `TestOpenAcceptsRecordedRetentionCount`, `TestInitializeRejectsUnsupportedSettingsBeforeCreatingFile`; conformance `sqlite-ttl-unsupported`, `sqlite-change-feed-unsupported`, `sqlite-seeded-*`. |
| SQ-D6 explicit initialization | `TestInitializeCreatesRustLayoutInNewFile`, `TestInitializeRejectsInitializedFileWithoutChangingIt`, `TestInitializeRejectsDatabaseWithApplicationObjects`, `TestInitializeAcceptsEmptyExistingDatabase`, `TestOpenDoesNotCreateMissingFile`, `TestOpenDoesNotInitializeEmptyDatabase`. |
| SQ-3, SQ-4 values and parameters | `TestAppendKeepsPrefixRelatedAggregatesApart`, `TestRustFormatGoWrittenRowsAreReadableBySameStatementsAsRust`; ranges are validated by the envelopes (conformance value cases). |
| SQ-5, SQ-17 append order | `TestAppendChecksHeadAfterAcquiringWriteLock` (a head advanced while waiting for the lock is the head that is compared), `TestAppendEventOnlyThenSnapshotAndEventsAreReadFromRealFile`. |
| SQ-6 commit point | `TestAppendCommitFailureLeavesNothingAndRetrySucceeds`, `TestAppendWithCancelledContextFailsWithoutPartialWrite`. |
| SQ-7 busy is a storage error | `TestLockWaitReturnsStorageErrorAfterWaitingAndKeepsStoreUsable`; conformance `sqlite-write-lock-busy`. |
| Concurrent connections | `TestConcurrentAppendFromSeparateConnectionsCommitsExactlyOne`. |
| SQ-8, SQ-9 read transactions and gaps | `TestReadSnapshotAndHeadComeFromOnePointInTime`, `TestReadEventsReturnContiguousRangeUpToHeadOfOneReadTime`, `TestReadRejectsJournalGapAsStorageError`, `TestReadRejectsHeadWithoutJournalRowAsStorageError`, `TestReadEventsFromHeadAndBeyond`. |
| SQ-10 retention | `TestRetentionKeepsNewestHistoryRightAfterReturn`, `TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates`, `TestRetentionDeletesOldestSnapshotsThroughHook`. |
| SQ-11, S-4 retention failure | `TestRetentionFailureKeepsCommittedAppendAndNotifies`, `TestRetentionNotificationPanicDoesNotFailCommittedAppend` (the handler and the log path are asserted to be called, and a panic in either does not change the result), `TestRetentionFailureReleasesWriteLockBeforeNotifying`. |
| Rust v5 file format | `TestRustFormatInitializedTablesMatchRustDefinitions`, `TestRustFormatGoReadsAndAppendsToRustWrittenRows`, `TestRustFormatGoWrittenRowsAreReadableBySameStatementsAsRust`. The Rust binary is not run because the reference copy is read-only; the DDL and the statements are copied from `layout.rs` and `backend.rs`. |
| SQ-12, SQ-13, SQ-18 change feed | Not provided in the first version (SQ-D8). Requests for it are configuration errors (see SQ-20 above). |
| Public usage | `ExampleNew` imports the package from outside and prints `3 2`. |
| Conformance gate | `internal/conformance/required.json` lists 68 SQLite cases. `TestLoadRequired` checks the list against the data, and the CI `conformance` job fails when a listed case is not executed or does not succeed. |

## Request splitting and size limits

DynamoDB splits oversized writes and large batches into several requests (item size limit, 25-item batches, unprocessed keys). SQLite does not: an append is a single transaction on a single connection, and retention deletes rows one by one inside one transaction. The rules about request splitting therefore have no SQLite counterpart, and no test is mapped to them.

## Known limits of the evidence

- The settings `synchronous`, `locking_mode` and `read_uncommitted` are verified by reading them back at connection time; no test observes them from outside.
- A library older than 3.51.3 cannot be provided in a test; only the comparison function is tested.
- The point at which the read hooks run inside the read transaction is an implementation detail; the read tests check that head and snapshot belong to the same point in time and that events are contiguous.
