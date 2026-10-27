> この文書は [SQLITE_DATABASE_SCHEMA.md](SQLITE_DATABASE_SCHEMA.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# SQLite の保存先のスキーマ

[English](SQLITE_DATABASE_SCHEMA.md)

## 配置

これは、Go の `/v5` モジュールが使う、[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) の SQLite プロファイルの配置の版 1 です。ファイルの形式は Rust v5 の実装が書くものと同じで、このパッケージは独自の配置を足しません。1 つのデータベースファイルに、5 つの `STRICT` の表があります。

| 表 | 主キー | 役割 |
|---|---|---|
| journal | (`aid`, `seq_nr`) | すべてのイベント |
| head | `aid` | 集約ごとの最新のイベントと型名 |
| snapshot_current | `aid` | 最新のスナップショット |
| snapshot_history | (`aid`, `seq_nr`) | スナップショットの履歴。保持件数を指定したときだけ残す。ペア書き込みは新しいスナップショットを `snapshot_current` と履歴の両方に入れるので、最新のスナップショットは 2 つの表に重複して入る |
| config | `singleton`（常に 1） | ストア ID、配置の版、固定の設定 |

`aid` は、検証済みの型名、ハイフン、ID の値をつなげたものです。宣言は `TEXT COLLATE BINARY` で、主キーの索引も `BINARY` なので、`User-A` と `User-a` は別の集約です。`head_transition` の表も変更フィードもありません。

## 設定

`sqlite.Config` の項目は 2 つです。`LockWait` は別の接続が持つロックを待つ時間で、nil なら 5 秒、0 なら待たず、負の値や 2147483647 ミリ秒を超える値は設定エラーです。`ChangeFeed` は false でなければならず、true は設定エラーです。共通の `Option` は `WithRetentionCount`、`WithRetentionMode`（`RetentionDelete` だけ。`RetentionTTL` は件数がなくても設定エラー）、`WithRetentionFailureHandler` です。

```go
import (
    "context"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
)

// 新しいファイルでだけ一度実行する。初期化済みのファイルでは Initialize は失敗する。
func initializeSQLite(ctx context.Context, path string) error {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return err }
    return sqlite.Initialize(ctx, path, sqlite.Config{}, eventstore.WithRetentionCount(keep))
}

// 起動のたびに実行する。Open はファイルを作らず、初期化もしない。
func openSQLite(ctx context.Context, path string) (*sqlite.Store, eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return nil, nil, err }
    store, err := sqlite.Open(ctx, path, sqlite.Config{}, eventstore.WithRetentionCount(keep))
    if err != nil { return nil, nil, err }
    es, err := sqlite.New(store, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState]())
    if err != nil { _ = store.Close(); return nil, nil, err }
    return store, es, nil
}
```

使い終わったら `Store.Close` を呼びます。2 回目の `Close` は nil を返し、`Close` の後の操作は保存先エラーになります。

## 初期化

`sqlite.Initialize` は、5 つの表と `config` の 1 行を、1 つのトランザクションで作ります。対象は、新しいファイル、またはアプリケーション用の表・索引・ビュー・トリガーが 1 つもないデータベースだけです。初期化済みのファイルを含め、それ以外は設定エラーで、何も作りません。設定の検査は、ファイルを作る前に済ませます。

`sqlite.Open` はファイルを作りません。ファイルがなければ保存先エラーです。そのうえで、1 つの読み取りトランザクションで次を確かめます。

- SQLite 本体が 3.51.3 以上であること（数字ごとに比べる）。
- すべての表が `STRICT` で、列・型・`NOT NULL`・主キーが期待どおりであり、`aid` が列の宣言でも主キーの索引でも `BINARY` で比較されること。
- `config` が 1 行で、配置の版が 1、`store_id` が小文字の UUID、`retention_mode = 'delete'`、`change_feed_enabled = 0` であり、`keep_snapshot_count` がクライアントの保持件数と同じであること（保持なしは NULL）。

食い違いは設定エラーです。Open はファイルを直さず、書き込みもしません。ただし、データベースを WAL に切り替えます。すべての接続で、`locking_mode = NORMAL`、`journal_mode = WAL`、`synchronous = FULL`、`read_uncommitted = OFF` とロックの待ち時間を設定し、読み直して確かめます。

## 記録

| 列 | 型 | 意味 |
|---|---|---|
| `aid` | TEXT | 集約の ID |
| `seq_nr` | INTEGER | 1 以上 9007199254740991 以下 |
| `occurred_at_ns` / `event_occurred_at_ns` | INTEGER | Unix 時刻のナノ秒 |
| `manifest` / `event_manifest` | TEXT | 封筒のマニフェスト |
| `payload` / `event_payload` | BLOB | 直列化したペイロードまたは集約の状態 |
| `type_name`（head） | TEXT | 集約の型名 |

## 書き込みと読み取り

追記は、先に書き込みのロック（`BEGIN IMMEDIATE`）を取り、そのあとでヘッドを照合します。番号がヘッド以下なら楽観ロックのエラー、ヘッド + 1 でなければ契約違反（W-8）です。journal の行、ヘッド、ペア書き込みでは最新のスナップショットと履歴の行を、1 つのトランザクションで確定します。`LockWait` の間にロックを取れなければ、追記は楽観ロックではなく保存先エラーで失敗し、何も確定しません。

ヘッドと最新のスナップショットは 1 つの読み取りトランザクションで読み、ヘッドとそこまでの journal の行も 1 つの読み取りトランザクションで読みます。journal の欠番は保存先エラーで、短くなった結果を成功として返すことはありません。同じファイルを開いた別の Store の接続と同時に使えます。

## 保持

保持を指定しなければ履歴はありません。`KeepLatest(n)` では、ペア書き込みのたびに履歴の行を足し、追記の確定後に、2 つ目の `BEGIN IMMEDIATE` のトランザクションで、`n` 件を超える古い行を削除します。`snapshot_current`・`head`・journal は削除しません。イベントだけの追記は履歴に触れません。保持の失敗はログに残り、Store のロックを外してから `WithRetentionFailureHandler` に渡されます。確定した追記の結果は変わらず、通知先やログ出力が panic しても同じです。

## 運用

- ローカルのファイルシステムを使ってください。SQLite の WAL はネットワークファイルシステムでは動きません。
- 開いている間、ファイルの隣に `-wal` と `-shm` のファイルができます。コピーするときは一緒に扱ってください。
- 権限: プロセスには、ファイルのあるディレクトリの読み書きの権限が要ります。
- このパッケージが書いたファイルは Rust v5 の実装が読み書きでき、逆も同じです。両者の間の移行手順はありません。
