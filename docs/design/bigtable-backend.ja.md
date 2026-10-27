> この文書は [bigtable-backend.md](bigtable-backend.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Bigtable の保存先の設計記録

[English](bigtable-backend.md)

この記録は、[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) の Bigtable プロファイル（`storage/bigtable.md`）とハッシュ（`storage/hash.md`）の規則を、このリポジトリの試験に対応づけます。配布された適合データが扱わない規則も含みます。

## 決定

| 決定 | 選択 | 理由 |
|---|---|---|
| クライアントライブラリ | `cloud.google.com/go/bigtable` v1.58.0 | 当時の最新版です（2026-09-25公開）。`go 1.26.0`を要求し、モジュールはすでにそれを宣言しています。それが要求するモジュールだけを足し、依存の一括更新はしていません。 |
| 公開する形 | `bigtable.New(ctx, data, admin, Config, eventSerializer, snapshotSerializer, opts...)`と`bigtable.Initialize(ctx, data, Config, opts...)` | `spanner.New`と同じ形です。クライアントは呼び出し側のものなので、保存先は資源を持たず`Close`もありません。認証、app profile、接続は呼び出し側の責務です。データのクライアントは読み取りと条件付き更新、管理用のクライアントはスキーマの確認だけに使います。 |
| 初期化 | `Initialize`は2つの設定の行だけを書く | BT-D1です。テーブルや列ファミリは作らないので、スキーマ文書の戻し方の手順で完結します。 |
| 行のフィルター | 列の範囲と値の範囲`[v, v+0x00)`による完全一致のフィルター。正規表現は使わない | 正規表現では任意のバイトをエスケープしなければなりません。SDKは、2つの範囲を始端を含み終端を含まないと文書化しています。 |
| 条件付き更新 | 片側をnilにした`NewCondMutation`と、`GetCondMutationResult`を付けた`Apply` | SDKは`CheckAndMutateRow`を1回送り、再試行しません。試験で要求が1回であることを確かめます。片側のnilはSDKが受け付けます。 |
| 読み取り | headは`ReadRow`、journalはキー範囲の`ReadRows` | SDKは、途中で切れた`ReadRows`を、渡した最後の行の次から再開します。 |
| 通知 | `log/slog`と`Config.JournalWriteFailureHandler`・`WithRetentionFailureHandler`。それぞれ`recover`で包む | 2つの失敗で同じ2つの経路を使います。適合の実行器は、グローバルなログを差し替えずに観測できます。 |
| キーのハッシュ | `internal/keyhash`と、試験用フック`OnKeyHash` | 公開APIではありません。ハッシュの衝突を起こせるのはこのフックだけです。 |
| 封筒 | `internal/bigtableenvelope` | 保存先と適合の実行器が共有します。 |
| エミュレーター | `gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators`をdigestで固定し、testcontainersで起動する。クライアントは明示的な平文のgRPC接続でつなぐ | イメージとdigestはRust版とJava版と同じです。`BIGTABLE_EMULATOR_HOST`を設定すると、認証情報やinterceptorの試験が渡す接続をSDKが差し替えてしまうので、設定しません。 |

## 規則と証拠

試験は、パスを書いたもの以外は`bigtable/`にあります。`internal/keyhash`・`internal/testhook`・`internal/bigtableenvelope`・`internal/bigtabletest`・`internal/conformance`にその他の試験があります。「適合」は、配布データのケースを`internal/conformance`が実際のエミュレーターで実行することです。

| 規則 | 証拠 |
|---|---|
| BT-1・BT-D3（1つのクラスタに固定したapp profile） | ライブラリは検査しません。app profileは呼び出し側が作るデータのクライアントの設定（`ClientConfig.AppProfile`）で、固定されているかどうかはライブラリから見えないからです。要件はスキーマ文書の「運用」に書いています。試験はありません。 |
| BT-2（列ファミリとガベージコレクションの規則） | `TestNewRejectsEveryLayoutDefectAsAConfigurationError`, `TestEmulatorLifecycle`, `TestSchemaDocumentProceduresProvisionAnOpenableDestination`. 列ファミリの欠け、2版を残すデータの列ファミリ、年齢による規則のあるデータの列ファミリは設定エラーです。確認が読むガベージコレクションの規則をエミュレーターが返すこと、スキーマ文書の`cbt`の手順をそのまま適用すると開ける配置になることも確かめます。 |
| BT-3・BT-4・K-1・K-2（行キー、8バイトのビッグエンディアンのFNV-1a 64、U64BEの番号） | `TestFNV1a64MatchesTheReferenceTable`, `TestPrefixIsTheUnsignedHashInEightBigEndianBytes`, `TestRowKeyLayoutIsHashLengthAidAndBigEndianSeqNr`, `TestHashCollisionKeepsAggregatesApart`, `TestRowKeyLengthStopsAPrefixAidFromSharingARange`, `TestRowKeyConfigurationRowIsNeverReadOrRetained`, `TestKeyHash`, `TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells`. キーは、標準ライブラリのハッシュで独立に組み立てたキーと比べます。ハッシュを衝突させても集約は混ざらず、キーの長さが前方一致するaidを分け、1バイトの設定の行がデータとして読まれることはありません。 |
| BT-5（`event:envelope`がないときだけjournalの行を作る） | `TestAppendEventOnlyThenSnapshotAndEventsAreReadFromTheEmulator`, `TestAppendConcurrentWritersFromSeparateClientsCommitExactlyOne`, `TestRetentionRetiredRowIsNeverRevivedByALateWriter`. 適合ケース`bigtable-previous-event-write-failure`と`bigtable-new-tail-write-failure`を`TestPublicBigtableConformance`で実行します。 |
| BT-6（セルの時刻0、全版の削除と書き込みによる置き換え、最新のセルへの条件） | `TestCellVersion`. 読み戻したすべてのセルの時刻は0で、置き換えた列はどれも1版だけです。 |
| BT-7・BT-D7（追記の手順1〜5、確定点はheadの条件付き更新） | `TestAppendFailureOfThePreviousJournalWriteLeavesTheHeadUnchanged`, `TestTailWrite`, `TestLostConditionalUpdate`, `TestAppendRejectsADuplicateNumberAsOptimisticLock`, `TestAppendRejectsAGapAsContractViolation`, `TestAppendRejectsSnapshotNumberMismatchWithoutWriting`. 直前のイベントの書き出しが失敗してもheadは進みません。末尾の書き出しの失敗は、ログと`Config.JournalWriteFailureHandler`で知らせます（どちらも呼ばれたことを確かめ、どちらのpanicも回収します）。追記は成功のままです。応答が失われると保存先エラーになり、条件付き更新は1回しか送りません。適合ケースは`bigtable-previous-event-write-failure`・`bigtable-new-tail-write-failure`・`bigtable-cas-response-lost`です。 |
| BT-8・BT-9（headの読み取りは1回、呼び出しの間hを固定） | `TestReadReadsTheHeadOnceAndTheJournalOnlyBelowIt`, `TestReadDoesNotMixInEventsCommittedAfterTheHeadWasRead`. 実際の接続のinterceptorでReadRowsの要求を数えます。headだけか、headとjournalの範囲1つです。 |
| BT-10（行とセルの分割、再開、欠番なし） | `TestInterruptedReadRows`, `TestReadReportsAMissingJournalRowAsAStorageError`, `TestReadReportsAMalformedHeadOrEnvelopeAsAStorageError`, `TestReadFailsWhileDeserializingAPayloadAsASerializationError`. 最初のメッセージの後で切ったストリームは、最後に完成した行の次から再開し、大きな12行を一度ずつ順に返します。 |
| BT-11・BT-12（保持、activeな行だけを退役、eventのセルは残す） | `TestRetentionKeepsTheNewestNAndRetiresTheRest`, `TestRetentionWritesNoHistoryWithoutACount`, `TestRetentionCountsTheHeadSnapshotThatIsNotYetInAJournalRow`, `TestRetentionRetiredRowIsNeverRevivedByALateWriter`, `TestRetentionLeavesTheConfigurationRowAlone`. 履歴はセルごとに読み戻します。退役した行が失うのは`history:envelope`だけです。 |
| BT-13・BT-D4（保持の失敗は確定済みの追記を変えない、期限切れ方式なし） | `TestRetentionFailureDoesNotChangeTheCommittedAppend`, `TestRetentionNotificationPanicDoesNotFailCommittedAppend`, `TestNewValidatesOptionsBeforeAnyRequest`. ログの経路とハンドラが呼ばれたことを確かめ、どちらのpanicも回収します。期限切れ方式は設定エラーです。 |
| BT-14・BT-15・BT-D5（変更フィード） | 提供しません（BT-D8）。エミュレーターも変更の購読に対応しません。試験はありません。 |
| BT-16・BT-D6b（封筒の上限） | `TestEnvelopeLimit`, `TestNewAcceptsTheEnvelopeLimitBoundaries`, `TestNewRejectsEveryLayoutDefectAsAConfigurationError`, `TestNewValidatesOptionsBeforeAnyRequest`. 上限ちょうどの封筒は保存でき、1バイト超は書き込みの前に拒否します。8MiBの封筒はエミュレーターで往復できます。別の上限を記録した設定の行は設定エラーです。適合ケースは`bigtable-envelope-over-limit`と、種まきした上限の食い違いです。 |
| BT-17・BT-D8（変更フィードを求める設定は設定エラー） | `TestNewValidatesOptionsBeforeAnyRequest`. 要求を送る前に検査します。適合ケースは`bigtable-change-feed-unsupported`です。 |
| BT-D1・BT-D2（ライブラリの外での初期化、履歴はイベントの行に置く） | `TestNewAcceptsAProvisionedLayoutAndWritesNothing`, `TestConfigurationRow`, `TestInitializeWritesTheSameConfigurationToBothTables`, `TestInitializeTwiceIsAConfigurationErrorThatWritesNothing`, `TestInitializeRefusesAHalfInitializedPairAndTheDocumentedRecoveryRepairsIt`. `New`は何も書きません。設定の行は値で比べます。片側だけ初期化された状態は、文書の手順で戻せます。 |
| BT-D6a（封筒のバイナリ形式） | `TestDistributedValues`, `TestDecodeRejectsMalformedEnvelopes`, `TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended`. 分配された10件の値のケース（符号化、復元、壊れたバイト列）を`internal/bigtableenvelope`で実行し、`TestPublicBigtableConformance`で実行器からも実行します。 |
| 認証（データと管理の要求） | `TestCredentials`. 呼び出し側が渡す要求ごとの認証情報は、データAPIでも管理APIでも呼ばれます。失敗する認証情報では何も書かれず、保存先エラーになります。 |
| Rust v5の実装と共有する形式 | `TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended`, `TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells`. Rustの配置で手作業で組み立てた行をGoが読んで続きを書けること、Goが書いた行をバイト単位で比べることを確かめます。 |
| 文書の例（初回の起動と再起動） | `TestReadmeFlow`, `TestSchemaDocumentEnglishAndJapaneseCarryTheSameProcedures`, `TestSchemaDocumentHasTheSevenSectionsAndTheRecoveryProcedure`. `ExampleNew`と`ExampleInitialize`はパッケージと一緒にコンパイルされ、公開する型がREADMEと一致することを確かめます。 |
| 試験の基盤 | `TestEmulatorImageIsPinnedByDigest`, `TestStartFailureIsReportedNotSkipped`, `TestMain`, `TestConformanceSetupFailureReport`, `TestLoadRequired`, `TestPublicBigtableConformance`, `TestBigtablePhases`. 起動しないエミュレーターは実行を失敗させ、実行されなかった必須のBigtableのケースはゲートを失敗させます。 |

## 共有データの外にある規則

共有の適合データはこれらの規則を扱わないので、上の試験が証拠のすべてです。

- **保存形式.** 行キー、セルの名前、セルの値、封筒を、Goのコードとは独立に書いた行と比べます（`rust_format_test.go`）。
- **同時実行.** 2つのクライアントが同じ最初のイベントを追記すると、ちょうど1つだけが確定します（`TestAppendConcurrentWritersFromSeparateClientsCommitExactlyOne`）。確定と重なる読み取りは、最初に読んだheadを保ちます（`TestReadDoesNotMixInEventsCommittedAfterTheHeadWasRead`）。
- **要求の分割.** 途中で切れた`ReadRows`のストリームは、最後に完成した行の次から再開します（`TestInterruptedReadRows`）。保持は、1行ごとに1回の条件付き更新で退役させ、状態の列だけを読みます。

## 証拠の限界

- 認証情報の試験は、interceptorで、クライアントが要求ごとに認証情報を求めることを観測します。エミュレーターは`authorization`ヘッダーを検査しないので、サーバー側は証明できません。
- 応答の消失は、エミュレーターが書き込みを適用した後に答えを捨てるinterceptorで模擬します。ネットワーク障害は差し込みません。
- エミュレーターと本番のクラスタでガベージコレクションの規則の報告が一致するかは、エミュレーターについてだけ証明しています。
- サービスの上限、複数クラスタのルーティング、変更の購読は試験しません。プロファイルが対象外にしています（BT-D3、BT-D8）。
