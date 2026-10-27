# Bigtable storage design record

[日本語](bigtable-backend.ja.md)

This record maps the rules of the Bigtable profile of [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) (`storage/bigtable.md`) and of the hash (`storage/hash.md`) to the tests of this repository, including the rules that the distributed conformance data does not cover.

## Decisions

| Decision | Choice | Reason |
|---|---|---|
| Client library | `cloud.google.com/go/bigtable` v1.58.0 | The latest release at the time (published 2026-09-25). It needs `go 1.26.0`, which the module already declares. Only the modules it requires were added; no dependency was updated in bulk. |
| Public shape | `bigtable.New(ctx, data, admin, Config, eventSerializer, snapshotSerializer, opts...)` and `bigtable.Initialize(ctx, data, Config, opts...)` | Same shape as `spanner.New`. The clients belong to the caller, so the storage owns no resource and has no `Close`; authentication, the app profile and the connection are the caller's. The data client serves reads and conditional writes, the admin client only the schema check. |
| Initialization | `Initialize` writes only the two configuration rows | BT-D1. It never creates tables or families, so the recovery procedure of the schema document is complete. |
| Row filters | Exact-match filters built from column ranges and value ranges `[v, v+0x00)`; no regular expression | A regular expression would have to escape arbitrary bytes. The SDK documents both ranges as start-inclusive and end-exclusive. |
| Conditional writes | `NewCondMutation` with one side nil, `Apply` with `GetCondMutationResult` | The SDK sends one `CheckAndMutateRow` and does not retry it; the tests confirm that one request is sent. A nil side is accepted by the SDK. |
| Reads | `ReadRow` for the head, `ReadRows` over a key range for the journal | The SDK resumes an interrupted `ReadRows` after the last row it handed over. |
| Notifications | `log/slog` plus `Config.JournalWriteFailureHandler` and `WithRetentionFailureHandler`, each wrapped in `recover` | The same two paths for both failures; the conformance runner observes them without replacing the global logger. |
| Key hash | `internal/keyhash`, with a test hook `OnKeyHash` | Not a public API. The hook is the only way to produce a hash collision. |
| Envelope | `internal/bigtableenvelope` | Shared by the storage and the conformance runner. |
| Emulator | `gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators`, pinned by digest, started with testcontainers; clients connect through an explicit plain-text gRPC connection | The image and digest are the ones of the Rust and Java implementations. `BIGTABLE_EMULATOR_HOST` is never set because the SDK would then replace the connection that the credential and interceptor tests pass in. |

## Rules and evidence

Tests are in `bigtable/` unless a path is given. `internal/keyhash`, `internal/testhook`, `internal/bigtableenvelope`, `internal/bigtabletest` and `internal/conformance` hold the others. "Conformance" means a case of the distributed data run by `internal/conformance` against the real emulator.

| Rule | Evidence |
|---|---|
| BT-1, BT-D3 (app profile pinned to one cluster) | Not checked by the library: the app profile belongs to the data client that the caller builds (`ClientConfig.AppProfile`), and a library cannot see whether it is pinned. The schema document states the requirement under Operations. No test. |
| BT-2 (column families and garbage-collection rules) | `TestNewRejectsEveryLayoutDefectAsAConfigurationError`, `TestEmulatorLifecycle`, `TestSchemaDocumentProceduresProvisionAnOpenableDestination`. A missing family, a data family that keeps two versions and a data family with an age rule are configuration errors; the emulator reports the garbage-collection rule that the check reads; the `cbt` steps of the schema document, applied as written, give a layout that opens. |
| BT-3, BT-4, K-1, K-2 (row keys; FNV-1a 64 as 8 big-endian bytes; U64BE sequence numbers) | `TestFNV1a64MatchesTheReferenceTable`, `TestPrefixIsTheUnsignedHashInEightBigEndianBytes`, `TestRowKeyLayoutIsHashLengthAidAndBigEndianSeqNr`, `TestHashCollisionKeepsAggregatesApart`, `TestRowKeyLengthStopsAPrefixAidFromSharingARange`, `TestRowKeyConfigurationRowIsNeverReadOrRetained`, `TestKeyHash`, `TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells`. The keys are compared with keys built independently with the standard library hash; a forced hash collision does not mix aggregates, and the length in the key keeps a prefix aid apart; the one-byte configuration row is never read as data. |
| BT-5 (create a journal row only when `event:envelope` is absent) | `TestAppendEventOnlyThenSnapshotAndEventsAreReadFromTheEmulator`, `TestAppendConcurrentWritersFromSeparateClientsCommitExactlyOne`, `TestRetentionRetiredRowIsNeverRevivedByALateWriter`. Conformance cases `bigtable-previous-event-write-failure` and `bigtable-new-tail-write-failure` run through `TestPublicBigtableConformance`. |
| BT-6 (cell timestamp 0; replace = delete all versions + set; condition on the newest cell) | `TestCellVersion`. Every cell read back has timestamp 0 and every replaced column holds one version. |
| BT-7, BT-D7 (append steps 1 to 5; commit point = conditional write on the head) | `TestAppendFailureOfThePreviousJournalWriteLeavesTheHeadUnchanged`, `TestTailWrite`, `TestLostConditionalUpdate`, `TestAppendRejectsADuplicateNumberAsOptimisticLock`, `TestAppendRejectsAGapAsContractViolation`, `TestAppendRejectsSnapshotNumberMismatchWithoutWriting`. A failed write-out of the previous event leaves the head unchanged; a failed tail write-out is reported through the log and `Config.JournalWriteFailureHandler` (both asserted to be called, and a panic in either is recovered) and the append still succeeds; a lost response is a storage error and the conditional write is sent once. Conformance cases `bigtable-previous-event-write-failure`, `bigtable-new-tail-write-failure` and `bigtable-cas-response-lost`. |
| BT-8, BT-9 (one head read; fixed h for the whole call) | `TestReadReadsTheHeadOnceAndTheJournalOnlyBelowIt`, `TestReadDoesNotMixInEventsCommittedAfterTheHeadWasRead`. ReadRows requests are counted with an interceptor on a real connection: head only, or head and one journal range. |
| BT-10 (row and cell splitting; resume; no gap) | `TestInterruptedReadRows`, `TestReadReportsAMissingJournalRowAsAStorageError`, `TestReadReportsAMalformedHeadOrEnvelopeAsAStorageError`, `TestReadFailsWhileDeserializingAPayloadAsASerializationError`. A stream cut after its first message is resumed after the last completed row and returns all 12 large rows once, in order. |
| BT-11, BT-12 (retention; retire only active rows; event cells stay) | `TestRetentionKeepsTheNewestNAndRetiresTheRest`, `TestRetentionWritesNoHistoryWithoutACount`, `TestRetentionCountsTheHeadSnapshotThatIsNotYetInAJournalRow`, `TestRetentionRetiredRowIsNeverRevivedByALateWriter`, `TestRetentionLeavesTheConfigurationRowAlone`. History is read back cell by cell: retired rows lose only `history:envelope`. |
| BT-13, BT-D4 (a retention failure never changes the committed append; TTL not provided) | `TestRetentionFailureDoesNotChangeTheCommittedAppend`, `TestRetentionNotificationPanicDoesNotFailCommittedAppend`, `TestNewValidatesOptionsBeforeAnyRequest`. The log path and the handler are both asserted to be called, and a panic in either is recovered. TTL retention is a configuration error. |
| BT-14, BT-15, BT-D5 (change feed) | Not provided (BT-D8). The emulator does not support change streams either. No test. |
| BT-16, BT-D6b (envelope size limit) | `TestEnvelopeLimit`, `TestNewAcceptsTheEnvelopeLimitBoundaries`, `TestNewRejectsEveryLayoutDefectAsAConfigurationError`, `TestNewValidatesOptionsBeforeAnyRequest`. An envelope exactly at the limit is stored and one byte over is rejected before any write; an 8 MiB envelope round-trips through the emulator; a configuration row with another limit is a configuration error. Conformance cases `bigtable-envelope-over-limit` and the seeded limit mismatch. |
| BT-17, BT-D8 (a change feed request is a configuration error) | `TestNewValidatesOptionsBeforeAnyRequest`. Checked before any request is sent. Conformance case `bigtable-change-feed-unsupported`. |
| BT-D1, BT-D2 (initialization outside the library; history in the event's row) | `TestNewAcceptsAProvisionedLayoutAndWritesNothing`, `TestConfigurationRow`, `TestInitializeWritesTheSameConfigurationToBothTables`, `TestInitializeTwiceIsAConfigurationErrorThatWritesNothing`, `TestInitializeRefusesAHalfInitializedPairAndTheDocumentedRecoveryRepairsIt`. `New` writes nothing; the configuration rows are compared by value; a half-initialized pair is repaired with the documented procedure. |
| BT-D6a (envelope binary format) | `TestDistributedValues`, `TestDecodeRejectsMalformedEnvelopes`, `TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended`. The ten distributed value cases (encode, decode back, malformed bytes) run in `internal/bigtableenvelope` and, through `TestPublicBigtableConformance`, against the runner. |
| Authentication (data and admin requests) | `TestCredentials`. Per-request credentials supplied by the caller are asked for on the data and the admin API; a failing credential writes nothing and is a storage error. |
| Format shared with the Rust v5 implementation | `TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended`, `TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells`. Rows assembled by hand in the Rust layout are read and extended by Go; rows written by Go are compared byte by byte. |
| Documentation examples (first start and restart) | `TestReadmeFlow`, `TestSchemaDocumentEnglishAndJapaneseCarryTheSameProcedures`, `TestSchemaDocumentHasTheSevenSectionsAndTheRecoveryProcedure`. `ExampleNew` and `ExampleInitialize` are compiled with the package so that the public types match the README. |
| Test infrastructure | `TestEmulatorImageIsPinnedByDigest`, `TestStartFailureIsReportedNotSkipped`, `TestMain`, `TestConformanceSetupFailureReport`, `TestLoadRequired`, `TestPublicBigtableConformance`, `TestBigtablePhases`. An emulator that does not start fails the run; a required Bigtable case that is not executed fails the gate. |

## Rules outside the shared data

The shared conformance data does not cover these rules, so the tests above carry the whole evidence:

- **Storage format.** The row keys, the cell names, the cell values and the envelopes are checked against rows written independently of the Go code (`rust_format_test.go`).
- **Concurrency.** Two clients appending the same first event commit exactly one (`TestAppendConcurrentWritersFromSeparateClientsCommitExactlyOne`); a read that overlaps a commit keeps the head it read first (`TestReadDoesNotMixInEventsCommittedAfterTheHeadWasRead`).
- **Request splitting.** An interrupted `ReadRows` stream resumes after the last completed row (`TestInterruptedReadRows`). Retention retires one row per conditional write and reads only the state column.

## Limits of the evidence

- The credential tests observe what the client sends (the credential is asked for on every request) with an interceptor; the emulator does not check the `authorization` header, so the server side is not proved.
- A real lost response is simulated by an interceptor that drops the answer after the emulator applied the write; no network fault is injected.
- Whether the emulator and a production cluster agree on garbage-collection rule reporting is proved only for the emulator.
- Service quotas, multi-cluster routing and change streams are not tested; the profile excludes them (BT-D3, BT-D8).
