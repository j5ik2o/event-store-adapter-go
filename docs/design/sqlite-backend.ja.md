> この文書は [sqlite-backend.md](sqlite-backend.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# SQLite の保存先の設計記録

[English](sqlite-backend.md)

この記録は、[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) の SQLite プロファイル（`storage/sqlite.md`）の規則のうち、共有の適合データが扱わないものを、このリポジトリの試験に対応づけます。`sqlite` パッケージを加えた時点の証拠を記録するもので、現在の振る舞いは [SQLITE_DATABASE_SCHEMA.ja.md](../SQLITE_DATABASE_SCHEMA.ja.md) に書きます。

## 決定

| 決定 | 選択 | 理由 |
|---|---|---|
| ドライバー | `modernc.org/sqlite` v1.59.0 | cgo を使わない Go の実装なので、`go test -race` と CI に cgo が要りません。v1.60.0 以降は `go 1.26.0` を要求し、このモジュールの Go の最低版を上げてしまいます。同梱の SQLite は 3.53.4 で、要求の 3.51.3 以上を満たします。 |
| 公開する形 | `Initialize`、`Open`、`Store.Close`、`New` | `Open` はファイルと接続を持つ `*Store` を返し、`New` が serializer をつなぎます。`memory.NewStore` と `memory.New` の流れと同じです。 |
| 接続 | `Store` ごとに `*sql.Conn` を 1 本持ち、mutex の下で使う | 接続を 2 つの操作で共有せず（SQ-17）、すべての接続に SQ-1 の設定を適用します。 |
| トランザクション | `BEGIN`、`BEGIN IMMEDIATE`、`COMMIT`、`ROLLBACK` を SQL として実行する | 追記には `BEGIN IMMEDIATE` が要ります。ドライバーの `_txlock` はすべてのトランザクションに効いてしまいます。 |
| 配置 | Rust v5 の `layout.rs` と同じ DDL の語 | 両言語が同じファイルを読み書きします。このパッケージ独自の配置はありません。 |
| `aid` の列の照合順序 | 複合 SELECT `... WHERE 0 UNION ALL SELECT ?, 0 UNION ALL SELECT ?, 1 ORDER BY aid, ordinal` を ('a','A') と ('x ','x') の 2 組で実行する | ドライバーは `sqlite3_table_column_metadata` を公開していません。この判定は保存済みの行を読まず、書き込みもしません。複合 SELECT の `ORDER BY` の照合順序は左端の SELECT に従います。主キーの索引は `pragma_index_xinfo` で読みます。 |
| ストア ID | 小文字のランダムな UUID（版 4） | 依存を増やしません。 |
| 時刻 | `UnixNano()` を INTEGER で保存し、`time.Unix(0, n).UTC()` で戻す | `OccurredAt` の範囲は封筒がすでに検査します。 |

## 規則と証拠

試験は、パスを書いたもの以外は `sqlite/` にあります。「適合」は、`internal/conformance` が実際の SQLite ファイルに対して実行する配布データのケースです（`TestPublicSQLiteConformance`、`TestConformance`）。

| 規則 | 証拠 |
|---|---|
| SQ-1、SQ-14 接続の設定 | `TestConnectionEnablesWriteAheadLogForFileInUse`、`TestConnectionConvertsRollbackJournalFileToWriteAheadLog`。`synchronous`・`locking_mode`・`read_uncommitted`・`busy_timeout` は `connection.go` が全接続で設定し、読み直します。別の接続から観測できないので、ロックの待ち時間は `TestLockWaitReturnsStorageErrorAfterWaitingAndKeepsStoreUsable` と `TestLockWaitZeroFailsImmediately` で実測します。 |
| SQ-15 ライブラリの版 | `TestMeetsMinimumVersion`（数字ごとの比較）。 |
| SQ-2、SQ-16 配置の照合 | `TestOpenRejects`（壊れたファイル 15 種）、`TestOpenAcceptsHandBuiltRustLayout`、`TestAidCollationAcceptsDefaultBinary`、`TestAidCollationRejectsMisconfiguredFiles`（列と主キーの索引が食い違うファイル。拒否のあとファイルは変わらない）。 |
| SQ-19、SQ-20 設定の一致 | `TestOpenRejectsUnsupportedSettings`、`TestOpenAcceptsRecordedRetentionCount`、`TestInitializeRejectsUnsupportedSettingsBeforeCreatingFile`。適合の `sqlite-ttl-unsupported`、`sqlite-change-feed-unsupported`、`sqlite-seeded-*`。 |
| SQ-D6 明示的な初期化 | `TestInitializeCreatesRustLayoutInNewFile`、`TestInitializeRejectsInitializedFileWithoutChangingIt`、`TestInitializeRejectsDatabaseWithApplicationObjects`、`TestInitializeAcceptsEmptyExistingDatabase`、`TestOpenDoesNotCreateMissingFile`、`TestOpenDoesNotInitializeEmptyDatabase`。 |
| SQ-3、SQ-4 値とパラメータ | `TestAppendKeepsPrefixRelatedAggregatesApart`、`TestRustFormatGoWrittenRowsAreReadableBySameStatementsAsRust`。範囲は封筒が検査します（適合の値のケース）。 |
| SQ-5、SQ-17 追記の順序 | `TestAppendChecksHeadAfterAcquiringWriteLock`（ロックを待つ間に進んだヘッドと照合する）、`TestAppendEventOnlyThenSnapshotAndEventsAreReadFromRealFile`。 |
| SQ-6 確定点 | `TestAppendCommitFailureLeavesNothingAndRetrySucceeds`、`TestAppendWithCancelledContextFailsWithoutPartialWrite`。 |
| SQ-7 busy は保存先エラー | `TestLockWaitReturnsStorageErrorAfterWaitingAndKeepsStoreUsable`。適合の `sqlite-write-lock-busy`。 |
| 並行した別接続 | `TestConcurrentAppendFromSeparateConnectionsCommitsExactlyOne`。 |
| SQ-8、SQ-9 読み取りのトランザクションと欠番 | `TestReadSnapshotAndHeadComeFromOnePointInTime`、`TestReadEventsReturnContiguousRangeUpToHeadOfOneReadTime`、`TestReadRejectsJournalGapAsStorageError`、`TestReadRejectsHeadWithoutJournalRowAsStorageError`、`TestReadEventsFromHeadAndBeyond`。 |
| SQ-10 保持 | `TestRetentionKeepsNewestHistoryRightAfterReturn`、`TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates`、`TestRetentionDeletesOldestSnapshotsThroughHook`。 |
| SQ-11、S-4 保持の失敗 | `TestRetentionFailureKeepsCommittedAppendAndNotifies`、`TestRetentionNotificationPanicDoesNotFailCommittedAppend`（通知関数とログ経路が実際に呼ばれたことを assert し、どちらが panic しても結果が変わらないことを確かめる）、`TestRetentionFailureReleasesWriteLockBeforeNotifying`。 |
| Rust v5 のファイル形式 | `TestRustFormatInitializedTablesMatchRustDefinitions`、`TestRustFormatGoReadsAndAppendsToRustWrittenRows`、`TestRustFormatGoWrittenRowsAreReadableBySameStatementsAsRust`。参照コピーが読み取り専用なので Rust のバイナリは実行せず、`layout.rs` と `backend.rs` の DDL と文を写しています。 |
| SQ-12、SQ-13、SQ-18 変更フィード | 初版は提供しません（SQ-D8）。求める設定は設定エラーです（上の SQ-20）。 |
| 公開した使い方 | `ExampleNew` が外からパッケージを import し、`3 2` を出力します。 |
| 適合の関門 | `internal/conformance/required.json` に SQLite のケースを 68 件挙げています。`TestLoadRequired` が一覧とデータを照合し、挙げたケースが実行されないか成功しないと、CI の `conformance` ジョブが失敗します。 |

## 要求の分割と大きさの上限

DynamoDB は、大きすぎる書き込みや大きなバッチを複数の要求に分けます（項目の大きさの上限、25 件のバッチ、未処理のキー）。SQLite は分けません。追記は 1 本の接続の 1 つのトランザクションで、保持の削除も 1 つのトランザクションの中で 1 行ずつ行います。そのため、要求の分割に関する規則に当たる SQLite の処理はなく、対応づける試験もありません。

## 証拠の限界

- `synchronous`・`locking_mode`・`read_uncommitted` は、接続時の読み直しで確かめます。外から観測する試験はありません。
- 3.51.3 より古いライブラリは試験で用意できないので、比較する関数だけを試験しています。
- 読み取りのフックが読み取りトランザクションのどこで動くかは実装の詳細です。読み取りの試験は、ヘッドとスナップショットが同じ時点のものであることと、イベントが連続していることを確かめます。
