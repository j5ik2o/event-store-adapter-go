# Spanner storage design record

[日本語](spanner-backend.ja.md)

This record maps the rules of the Spanner profile of [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) (`storage/spanner.md`) and of the hash (`storage/hash.md`) to the tests of this repository, including the rules that the shared conformance data does not cover. It records the evidence at the time the `spanner` package was added. The current behavior is described in [SPANNER_DATABASE_SCHEMA.md](../SPANNER_DATABASE_SCHEMA.md).

## Decisions

| Decision | Choice | Reason |
|---|---|---|
| Client library | `cloud.google.com/go/spanner` v1.96.1 | The latest release at the time. It needs `go 1.26.0`, which the module already declares. It also raises `google.golang.org/grpc` to v1.83.2, `google.golang.org/protobuf` to v1.36.11 and, among the indirect dependencies, OpenTelemetry to v1.45, `golang.org/x/crypto`, `golang.org/x/text` and `go-logr/logr`; `go get -u` was not used. |
| Public shape | `spanner.New(ctx, client, Config, eventSerializer, snapshotSerializer, opts...)` | Same shape as `dynamodb.New`. The client belongs to the caller, so the storage owns no resource and has no `Close`, and the same call serves the first start and every restart. |
| DDL | Never executed by the library | SPN-D5. The schema document carries the statements; there is no `Initialize`. |
| Transactions | `NewReadWriteStmtBasedTransactionWithOptions` with SERIALIZABLE, one transaction per attempt, with an own loop | The client's automatic re-run has no upper limit. The loop counts attempts, re-reads the head in every attempt, and waits 20 ms, doubling up to 1 s, between attempts. |
| Abort limit | Default 5 re-runs, `Config.TransactionRetryLimit` | The profile leaves the default to each language; the JavaScript implementation uses 5. |
| TTL grace | `Config.TTLGraceDays` in days | SPN-D7. It is not `WithTTLGraceSeconds`, which is a configuration error when non-zero. |
| Key hash | `internal/keyhash` | Not a public API. Shared by the storage and the conformance runner. |
| Emulator | `gcr.io/cloud-spanner-emulator/emulator:1.5.58` pinned by digest, one container per test package | The digest is the one of the JavaScript implementation. One container per package keeps overlapping transactions of different packages from aborting each other. |
| Conformance runner | One database; every scenario gets a new table prefix and its own DDL | The way the JavaScript runner works. |

## Rules and evidence

Tests are in `spanner/` unless a path is given. `internal/keyhash`, `internal/testhook`, `internal/spannertest` and `internal/conformance` hold the others. "Conformance" means a case of the distributed data run by `internal/conformance` against the real emulator (`TestPublicSpannerConformance`, `TestConformance`); the 79 applicable cases are listed in `internal/conformance/required.json` and a listed case that is not executed fails the job.

| Rule | Evidence |
|---|---|
| SPN-1, SPN-6, SPN-11 (SERIALIZABLE writes, strong reads) | `TestTransactionOptionsAppendIsASerializableReadWriteTransaction`, `TestTransactionOptionsRetentionIsAnotherSerializableReadWriteTransaction`, `TestTransactionOptionsReadsUseOneStrongReadOnlyTransactionEach`. A recording gRPC interceptor keeps the requests that really reach the emulator and the tests assert `isolation_level:SERIALIZABLE` and `strong:true`; the emulator cannot show the isolation from the server side. |
| SPN-2, SPN-16 (layout and configuration check, TTL grace days in three places) | `TestNewRejectsLayoutsThatDoNotMatchTheProfile`, `TestNewAcceptsTTLLayoutWhenGraceDaysAgreeInPolicyConfigAndClient`, `TestNewRejectsTablesThatDifferFromTheSharedStorageFormat`, `TestNewRejectsAnUnprovisionedPrefixWithoutCreatingAnything`, `TestNewReportsAnUnreadableDestinationAsStorageError`; conformance `spanner-seeded-*` and `spanner-ttl-grace-days-not-positive` through `TestPublicSpannerConformance`. |
| SPN-D1b (five tables from one prefix) | `TestNewDerivesTheFiveTableNamesFromThePrefix`, `TestNewRejectsPrefixesThatAreNotIdentifiersBeforeReadingStorage`, `TestNewAcceptsTheLongestPrefixWhoseDerivedNamesFit`. |
| SPN-3 (number ranges, nanosecond time as INT64) | Shared values and scenarios (`core-time-roundtrip-*`, `core-seq-*`) through `TestPublicSpannerConformance`; `TestAppendEventOnlyThenSnapshotAndEventsAreReadFromTheEmulator` (nanosecond round trip), `TestJSFormatGoWritesRowsThatJavaScriptReads` (`occurred_at_ns` is INT64 nanoseconds), `TestReadRejectsStoredSequenceNumbersOutOfRangeAsStorageError`. |
| SPN-4, K-1, K-2 (key = exact (hash_prefix, aid); FNV-1a 64 as 8 big-endian bytes, no shard count) | `TestFNV1a64MatchesTheReferenceTable`, `TestPrefixIsTheUnsignedHashInEightBigEndianBytes`, `TestPrefixKeepsTheTopBitOfTheHash`, `TestAppendStoresTheKeyAsTheFNVHashPrefixOfTheFullAggregateID`, `TestReadByExactKeyReturnsNothingWhenOnlyALongerIDHasAHead`; conformance `hash-fnv1a64-*` through `TestRunFNV1a64ValueCases` and `TestPublicSpannerConformance`; aid length cases (`aid-*-1024`, `aid-*-1025`) in the shared data. |
| SPN-5 (lossless INT64) | Go's `int64` carries `INT64` without conversion; `TestPublicSpannerConformance` runs the boundary values, and `TestReadRejectsStoredSequenceNumbersOutOfRangeAsStorageError` rejects a stored value out of range. The JavaScript-specific part of the rule does not apply to Go. |
| SPN-6 (append in one transaction: head check, W-8, rows) | `TestAppendRejectsDuplicateAsOptimisticLockWithHead`, `TestAppendRejectsAStaleNumberBelowTheHeadAsOptimisticLock`, `TestAppendRejectsGapAsContractViolation`, `TestAppendRejectsSnapshotNumberMismatchWithoutWriting`, `TestConcurrentAppendFromSeparateClientsCommitsExactlyOne`, `TestRefusedCommitLeavesNothingAndIsNotReRun`, `TestAppendWithCancelledContextFailsWithoutPartialWrite`. |
| SPN-7 (re-run after ABORTED with the same envelope, bounded) | `TestAbortedCommitIsReRunWithinTheLimitWithTheSameEnvelope`, `TestAbortedCommitBeyondTheLimitFailsAsStorageErrorAndCommitsNothing`, `TestHooks_CommitResponse`, `TestInjectedError_Aborted`; conformance `spanner-aborted-retry-within-limit` and `spanner-aborted-retry-limit-exceeded`. The abort is injected through a test hook: the emulator aborts one of two overlapping read-write transactions, so an abort made by overlapping transactions inside a commit hook would risk a deadlock. The real abort path is exercised by `TestConcurrentAppendFromSeparateClientsCommitsExactlyOne`. |
| SPN-8 (lost commit response = storage error of unknown outcome, no compensation) | `TestCommitResponseLostIsAnIndeterminateStorageErrorWithoutCompensation`; conformance `spanner-commit-response-lost`. |
| SPN-9 (head and snapshot in one strong read-only transaction) | `TestReadSnapshotAndHeadComeFromOnePointInTime`, `TestReadSeesWritesOfAnotherClientImmediately`, `TestTransactionOptionsReadsUseOneStrongReadOnlyTransactionEach`; conformance `core-snapshot-behind-head`. |
| SPN-10 (events by primary key range up to h; request splitting) | `TestReadEventsReturnContiguousRangeUpToTheHeadOfOneReadTime`, `TestReadEventsFromHeadAndBeyond`, `TestReadRejectsJournalGapAsStorageError`, `TestReadRejectsHeadWithoutJournalRowAsStorageError`, `TestRetentionSplitsManyCandidatesAndLeavesOnlyTheNewestRows` (2400 obsolete rows are handled in parts of at most 1000). The result of a read is not split by this package: the client library streams one range and the journal is checked for gaps while it is consumed. |
| SPN-11, SPN-12 (retention after commit; delete or mark once; marked rows not counted) | `TestRetentionKeepsNewestHistoryRightAfterReturnInDeleteMode`, `TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates`, `TestRetentionDeletesOldestSnapshotsThroughHook`, `TestRetentionRunsAfterTheAppendIsCommittedInASeparateTransaction`, `TestTTLRetentionMarksOldRowsOnceAndDoesNotCountMarkedRows`, `TestJSFormatTTLLayoutIsAcceptedAndMarkedRowsAreWrittenWithATimestamp`; conformance `core-retention-*`. |
| SPN-13 (retention failure never changes the committed append) | `TestRetentionFailureKeepsCommittedAppendAndNotifies` (the log path and the handler are both asserted to be called), `TestRetentionNotificationPanicDoesNotFailCommittedAppend` (a panic in the handler, in the log handler or in both), `TestRetentionFailureNotifiesOnlyAfterTheAppendTransactionEnded`; conformance `core-retention-failure-after-commit` and `core-retention-query-failure`. |
| SPN-14, SPN-15 (change feed) | Not provided (SPN-D8). Only SPN-17 applies; this is a rule limited to versions that provide a change feed. |
| SPN-17 (change feed requested = configuration error) | `TestNewRejectsInvalidSettingsBeforeReadingStorage` (setting), `TestNewRejectsLayoutsThatDoNotMatchTheProfile` (row with `change_feed_enabled` TRUE); conformance `spanner-change-feed-unsupported` and `spanner-seeded-change-feed-enabled`. |
| Format shared with the JavaScript v5 implementation | `TestJSFormatGoReadsRowsWrittenByJavaScript`, `TestJSFormatGoWritesRowsThatJavaScriptReads`; `TestSchemaDocumentStatementsProvisionAnOpenableDestination` runs the statements of the schema document as written. |
| Documentation examples (first start and restart) | `TestReadmeFlow` (the same factory call at the first start and after a restart with a fresh client), `ExampleNew` (compiled with the package so that the public types match), `TestSchemaDocumentEnglishAndJapaneseCarryTheSameStatements`. |
| Test infrastructure | `TestEmulatorImageIsPinnedByDigest`, `TestStartFailureIsReportedNotSkipped`, `TestEmulatorLifecycle`, `TestMain` (an emulator that does not start fails the run), `TestConformanceSetupFailureReport`, `TestLoadRequired` (a missing required Spanner case stops the gate). |

## Rules outside the shared data

The shared conformance data does not cover these rules, so the tests above carry the whole evidence:

- **Storage format.** The five tables, the columns, the primary keys, the `hash_prefix` bytes, the INT64 nanoseconds and the unmarked history rows are checked against rows and statements written independently of the Go code (`js_format_test.go`, with the JavaScript DDL and statements copied as text and the hash computed with the standard library).
- **Concurrency.** Two clients appending the same first event commit exactly one (`TestConcurrentAppendFromSeparateClientsCommitsExactlyOne`); a read that overlaps a commit returns one point in time (`TestReadSnapshotAndHeadComeFromOnePointInTime`, `TestReadEventsReturnContiguousRangeUpToTheHeadOfOneReadTime`).
- **Request splitting.** Retention handles at most 1000 rows per transaction (`TestRetentionSplitsManyCandidatesAndLeavesOnlyTheNewestRows`). Reading a journal range is one streamed read and is not split.

## Limits of the evidence

- A real ABORTED cannot be made on the emulator without a deadlock risk inside a commit hook, so the re-run is proved with an injected abort (`InjectedError.Aborted`) and the real path only by the concurrent append test.
- The isolation level and the read bound are proved by the requests the client sends, not by the server.
- The reselection of candidates after each part of a split retention is an internal step; the tests prove the number and size of the parts and the final history only.
- The exact waiting times between re-runs are not part of the profile; the tests prove only that waits grow, stay within 1 s and happen before each re-run.
- Physical removal by the row deletion policy, service quotas and the PostgreSQL dialect are not tested; the profile excludes them (SPN-D1a, section 9).
