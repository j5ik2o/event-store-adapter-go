> この文書は [spanner-backend.md](spanner-backend.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Spanner の保存先の設計記録

[English](spanner-backend.md)

この記録は、[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) の Spanner プロファイル（`storage/spanner.md`）とハッシュ（`storage/hash.md`）の規則を、共通の適合データが扱わない規則も含めて、このリポジトリの試験に対応づけます。`spanner` パッケージを追加した時点の証拠を残すもので、現在の振る舞いは [SPANNER_DATABASE_SCHEMA.ja.md](../SPANNER_DATABASE_SCHEMA.ja.md) にあります。

## 決定

| 決定 | 選択 | 理由 |
|---|---|---|
| クライアントライブラリ | `cloud.google.com/go/spanner` v1.96.1 | そのときの最新版。`go 1.26.0` が必要で、モジュールはすでにそれを宣言している。あわせて `google.golang.org/grpc` が v1.83.2、`google.golang.org/protobuf` が v1.36.11、間接依存の OpenTelemetry が v1.45、`golang.org/x/crypto`、`golang.org/x/text`、`go-logr/logr` が上がる。`go get -u` は使っていない。 |
| 公開する形 | `spanner.New(ctx, client, Config, eventSerializer, snapshotSerializer, opts...)` | `dynamodb.New` と同じ形。クライアントは呼び出し側のもので、保存先が持つ資源はなく `Close` もない。初回の起動でも再起動でも同じ呼び出しで動く。 |
| DDL | ライブラリは実行しない | SPN-D5。スキーマ文書に文があり、`Initialize` はない。 |
| トランザクション | SERIALIZABLE を指定した `NewReadWriteStmtBasedTransactionWithOptions`。試行ごとに 1 つ、自前のループ | クライアントの自動のやり直しには上限がない。ループが試行を数え、毎回ヘッドを読み直し、試行の間に 20 ms から倍々に最大 1 s まで待つ。 |
| 中断の上限 | 既定は 5 回のやり直し、`Config.TransactionRetryLimit` | プロファイルは既定値を各言語に任せている。JavaScript の実装は 5。 |
| 猶予の日数 | `Config.TTLGraceDays`（日数） | SPN-D7。`WithTTLGraceSeconds` は 0 以外なら設定エラー。 |
| キーのハッシュ | `internal/keyhash` | 公開 API ではない。保存先と適合試験の実行器が共有する。 |
| エミュレーター | `gcr.io/cloud-spanner-emulator/emulator:1.5.58` を digest で固定。試験パッケージごとにコンテナ 1 つ | digest は JavaScript の実装と同じ。パッケージごとに 1 つにして、別のパッケージの重なったトランザクションが互いを中断させないようにする。 |
| 適合試験の実行器 | データベース 1 つ。場面ごとに新しい表の接頭辞と専用の DDL | JavaScript の実行器と同じ方式。 |

## 規則と証拠

試験は、パスを書いていないものは `spanner/` にあります。ほかは `internal/keyhash`、`internal/testhook`、`internal/spannertest`、`internal/conformance` にあります。「適合データ」は、配布データの場面を `internal/conformance` が本物のエミュレーターで実行するもの（`TestPublicSpannerConformance`、`TestConformance`）です。対象の 79 件は `internal/conformance/required.json` に並び、実行されなかった必須ケースがあるとジョブは失敗します。

| 規則 | 証拠 |
|---|---|
| SPN-1, SPN-6, SPN-11（SERIALIZABLE の書き込みと強整合の読み取り） | `TestTransactionOptionsAppendIsASerializableReadWriteTransaction`, `TestTransactionOptionsRetentionIsAnotherSerializableReadWriteTransaction`, `TestTransactionOptionsReadsUseOneStrongReadOnlyTransactionEach`。gRPC の記録用インターセプターが、エミュレーターに実際に届いた要求を残し、試験が `isolation_level:SERIALIZABLE` と `strong:true` を確かめる。エミュレーターはサーバー側から分離レベルを見せないため。 |
| SPN-2, SPN-16（配置と設定の照合、3 か所の猶予の日数） | `TestNewRejectsLayoutsThatDoNotMatchTheProfile`, `TestNewAcceptsTTLLayoutWhenGraceDaysAgreeInPolicyConfigAndClient`, `TestNewRejectsTablesThatDifferFromTheSharedStorageFormat`, `TestNewRejectsAnUnprovisionedPrefixWithoutCreatingAnything`, `TestNewReportsAnUnreadableDestinationAsStorageError`。適合データの `spanner-seeded-*` と `spanner-ttl-grace-days-not-positive` は `TestPublicSpannerConformance` で実行する。 |
| SPN-D1b（1 つの接頭辞から 5 つの表） | `TestNewDerivesTheFiveTableNamesFromThePrefix`, `TestNewRejectsPrefixesThatAreNotIdentifiersBeforeReadingStorage`, `TestNewAcceptsTheLongestPrefixWhoseDerivedNamesFit`。 |
| SPN-3（番号の範囲、INT64 のナノ秒） | 共通の値と場面（`core-time-roundtrip-*`、`core-seq-*`）を `TestPublicSpannerConformance` で実行する。`TestAppendEventOnlyThenSnapshotAndEventsAreReadFromTheEmulator`（ナノ秒の往復）、`TestJSFormatGoWritesRowsThatJavaScriptReads`（`occurred_at_ns` は INT64 のナノ秒）、`TestReadRejectsStoredSequenceNumbersOutOfRangeAsStorageError`。 |
| SPN-4, K-1, K-2（キーは `(hash_prefix, aid)` の完全一致。FNV-1a 64 の 8 バイトのビッグエンディアン、シャード数なし） | `TestFNV1a64MatchesTheReferenceTable`, `TestPrefixIsTheUnsignedHashInEightBigEndianBytes`, `TestPrefixKeepsTheTopBitOfTheHash`, `TestAppendStoresTheKeyAsTheFNVHashPrefixOfTheFullAggregateID`, `TestReadByExactKeyReturnsNothingWhenOnlyALongerIDHasAHead`。適合データの `hash-fnv1a64-*` は `TestRunFNV1a64ValueCases` と `TestPublicSpannerConformance` で、aid の長さ（`aid-*-1024`、`aid-*-1025`）は共通データで確かめる。 |
| SPN-5（INT64 を欠落なく扱う） | Go の `int64` は変換なしで `INT64` を運ぶ。境界値は `TestPublicSpannerConformance` で実行し、範囲外の保存値は `TestReadRejectsStoredSequenceNumbersOutOfRangeAsStorageError` で拒否する。規則のうち JavaScript 固有の部分は Go には当てはまらない。 |
| SPN-6（1 つのトランザクションでの追記。ヘッドの照合、W-8、行） | `TestAppendRejectsDuplicateAsOptimisticLockWithHead`, `TestAppendRejectsAStaleNumberBelowTheHeadAsOptimisticLock`, `TestAppendRejectsGapAsContractViolation`, `TestAppendRejectsSnapshotNumberMismatchWithoutWriting`, `TestConcurrentAppendFromSeparateClientsCommitsExactlyOne`, `TestRefusedCommitLeavesNothingAndIsNotReRun`, `TestAppendWithCancelledContextFailsWithoutPartialWrite`。 |
| SPN-7（ABORTED のあと同じ封筒でやり直す。上限つき） | `TestAbortedCommitIsReRunWithinTheLimitWithTheSameEnvelope`, `TestAbortedCommitBeyondTheLimitFailsAsStorageErrorAndCommitsNothing`, `TestHooks_CommitResponse`, `TestInjectedError_Aborted`。適合データの `spanner-aborted-retry-within-limit` と `spanner-aborted-retry-limit-exceeded`。中断は試験用のフックで差し込む。エミュレーターは重なった 2 つの読み書きトランザクションの片方を中断させるので、コミットのフックの中で重ねて中断を作るとデッドロックの恐れがあるため。本物の中断の経路は `TestConcurrentAppendFromSeparateClientsCommitsExactlyOne` が通る。 |
| SPN-8（コミット応答の消失は結果不明の保存先エラー。補償しない） | `TestCommitResponseLostIsAnIndeterminateStorageErrorWithoutCompensation`。適合データの `spanner-commit-response-lost`。 |
| SPN-9（ヘッドとスナップショットを強整合の読み取り専用トランザクション 1 つで） | `TestReadSnapshotAndHeadComeFromOnePointInTime`, `TestReadSeesWritesOfAnotherClientImmediately`, `TestTransactionOptionsReadsUseOneStrongReadOnlyTransactionEach`。適合データの `core-snapshot-behind-head`。 |
| SPN-10（h までを主キーの範囲で読む。要求の分割） | `TestReadEventsReturnContiguousRangeUpToTheHeadOfOneReadTime`, `TestReadEventsFromHeadAndBeyond`, `TestReadRejectsJournalGapAsStorageError`, `TestReadRejectsHeadWithoutJournalRowAsStorageError`, `TestRetentionSplitsManyCandidatesAndLeavesOnlyTheNewestRows`（古い 2400 行を 1000 件以下の部分に分けて扱う）。読み取りの結果をこのパッケージは分割しない。クライアントライブラリが 1 つの範囲をストリームで返し、消費しながら journal の欠番を検査する。 |
| SPN-11, SPN-12（確定後の保持。削除、または一度だけ印を付ける。印のある行は数えない） | `TestRetentionKeepsNewestHistoryRightAfterReturnInDeleteMode`, `TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates`, `TestRetentionDeletesOldestSnapshotsThroughHook`, `TestRetentionRunsAfterTheAppendIsCommittedInASeparateTransaction`, `TestTTLRetentionMarksOldRowsOnceAndDoesNotCountMarkedRows`, `TestJSFormatTTLLayoutIsAcceptedAndMarkedRowsAreWrittenWithATimestamp`。適合データの `core-retention-*`。 |
| SPN-13（保持の失敗は確定済みの追記を変えない） | `TestRetentionFailureKeepsCommittedAppendAndNotifies`（ログの経路と通知先の両方が呼ばれたことを assert する）、`TestRetentionNotificationPanicDoesNotFailCommittedAppend`（通知先、ログの受け手、その両方の panic）、`TestRetentionFailureNotifiesOnlyAfterTheAppendTransactionEnded`。適合データの `core-retention-failure-after-commit` と `core-retention-query-failure`。 |
| SPN-14, SPN-15（変更フィード） | 提供しない（SPN-D8）。適用されるのは SPN-17 だけで、この 2 つは変更フィードを提供する版に限った規則である。 |
| SPN-17（変更フィードを求めると設定エラー） | `TestNewRejectsInvalidSettingsBeforeReadingStorage`（設定）、`TestNewRejectsLayoutsThatDoNotMatchTheProfile`（`change_feed_enabled` が TRUE の行）。適合データの `spanner-change-feed-unsupported` と `spanner-seeded-change-feed-enabled`。 |
| JavaScript v5 の実装と共有する形式 | `TestJSFormatGoReadsRowsWrittenByJavaScript`, `TestJSFormatGoWritesRowsThatJavaScriptReads`。`TestSchemaDocumentStatementsProvisionAnOpenableDestination` は、スキーマ文書の文をそのまま実行する。 |
| 文書の例（初回の起動と再起動） | `TestReadmeFlow`（初回の起動と、新しいクライアントでの再起動で、同じファクトリの呼び出し）、`ExampleNew`（パッケージと一緒にコンパイルされ、公開する型との一致を保つ）、`TestSchemaDocumentEnglishAndJapaneseCarryTheSameStatements`。 |
| 試験の基盤 | `TestEmulatorImageIsPinnedByDigest`, `TestStartFailureIsReportedNotSkipped`, `TestEmulatorLifecycle`, `TestMain`（起動しないエミュレーターは実行を失敗にする）、`TestConformanceSetupFailureReport`, `TestLoadRequired`（Spanner の必須ケースが欠けるとゲートが止まる）。 |

## 共通データの外にある規則

次の規則は共通の適合データが扱わないので、上の試験が証拠のすべてです。

- **保存形式。** 5 つの表、列、主キー、`hash_prefix` のバイト、INT64 のナノ秒、印のない履歴の行は、Go のコードとは独立に書いた行と文で確かめます（`js_format_test.go`。JavaScript の DDL と文をテキストで写し、ハッシュは標準ライブラリで求める）。
- **同時実行。** 2 つのクライアントが同じ最初のイベントを追記すると、ちょうど 1 つが確定します（`TestConcurrentAppendFromSeparateClientsCommitsExactlyOne`）。確定と重なった読み取りは、1 つの時点を返します（`TestReadSnapshotAndHeadComeFromOnePointInTime`、`TestReadEventsReturnContiguousRangeUpToTheHeadOfOneReadTime`）。
- **要求の分割。** 保持処理は 1 つのトランザクションで 1000 行までを扱います（`TestRetentionSplitsManyCandidatesAndLeavesOnlyTheNewestRows`）。journal の範囲の読み取りは 1 つのストリームで、分割しません。

## 証拠の限界

- 本物の ABORTED は、コミットのフックの中ではデッドロックの恐れがあるため、エミュレーターで作れません。やり直しは差し込んだ中断（`InjectedError.Aborted`）で証明し、本物の経路は同時の追記の試験だけが通ります。
- 分離レベルと読み取りの範囲は、サーバーではなく、クライアントが送った要求で証明します。
- 分割した保持処理で各部分のあとに候補を選び直すのは内部の手順です。試験は部分の数と大きさ、最終的な履歴だけを証明します。
- やり直しの間の待ちの具体的な時間はプロファイルの対象外です。試験は、待ちが縮まず、1 秒以内で、やり直しの前に入ることだけを証明します。
- 行削除ポリシーによる物理的な削除、サービスの容量上限、PostgreSQL の方言は試験しません。プロファイルが除外しています（SPN-D1a、第 9 章）。
