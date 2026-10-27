> この文書は [SPANNER_DATABASE_SCHEMA.md](SPANNER_DATABASE_SCHEMA.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Spanner の保存先のスキーマ

[English](SPANNER_DATABASE_SCHEMA.md)

## 配置

これは、Go の `/v5` モジュールが使う、[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) の Spanner プロファイルの配置の版 1 です。行の形は JavaScript v5 の実装が書くものと同じで、このパッケージは独自の配置を足しません。GoogleSQL の方言だけを扱います。5 つの表は同じデータベースに置き、表の名前はすべて 1 つの接頭辞から `<接頭辞>_<論理名>` として導きます。接頭辞が `esa` なら、表は `esa_journal`、`esa_head`、`esa_snapshot_current`、`esa_snapshot_history`、`esa_config` です。接頭辞と導いた名前は、128 文字以下の GoogleSQL の識別子（`[A-Za-z_][A-Za-z0-9_]*`）でなければなりません。

`spanner.New` は DDL を実行しません。表と設定の行を照合するだけで、足りないものを作りません。表と設定の行は、下の手順で自分で作ります。例の接頭辞は `esa` です。自分の接頭辞に読み替えてください。

<!-- spanner-schema:tables -->
```sql
CREATE TABLE esa_journal (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  occurred_at_ns INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_head (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  type_name STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  event_occurred_at_ns INT64 NOT NULL,
  event_manifest STRING(MAX) NOT NULL,
  event_payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_current (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_config (
  singleton INT64 NOT NULL,
  store_id STRING(36) NOT NULL,
  layout_version INT64 NOT NULL,
  keep_snapshot_count INT64,
  retention_mode STRING(8) NOT NULL,
  ttl_grace_days INT64,
  change_feed_enabled BOOL NOT NULL
) PRIMARY KEY (singleton);
```

`esa_snapshot_history` は保持の方式で変わります。2 つの定義のどちらかを使います。

削除方式は、古いスナップショットの履歴の行をすぐに削除します。行削除ポリシーはありません。

<!-- spanner-schema:snapshot-history-delete -->
```sql
CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr);
```

期限切れ方式は、古い行に印を付け、Spanner があとで削除します。行削除ポリシーが猶予の日数を持ちます。`7` は例であり、既定値ではありません。

<!-- spanner-schema:snapshot-history-ttl -->
```sql
CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr),
  ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL 7 DAY));
```

## 設定

`spanner.Config` の項目は 4 つです。

| 項目 | 意味 |
|---|---|
| `TablePrefix` | 5 つの表の名前の接頭辞。必須。 |
| `TTLGraceDays` | 期限切れ方式の猶予の日数。`RetentionTTL` では 1 以上、それ以外では 0。設定の行の `ttl_grace_days` と、行削除ポリシーの `INTERVAL` に等しくなければならない。`WithTTLGraceSeconds` とは別で、0 以外の `WithTTLGraceSeconds` は設定エラー。 |
| `TransactionRetryLimit` | 中断されたトランザクションをやり直す回数。初回は数えない。nil は 5、0 はやり直さず、負の値は設定エラー。 |
| `ChangeFeed` | false でなければならない。この版は変更フィードを提供せず、true は設定エラー。 |

共通の `Option` は `WithRetentionCount`、`WithRetentionMode`（`RetentionDelete` か `RetentionTTL`。期限切れ方式には保持件数が必要）、`WithRetentionFailureHandler` です。

`esa_config` には、`singleton = 1` の行がちょうど 1 つあります。

- `store_id`: 小文字の UUID の文字列。
- `layout_version`: `INT64` の値 `1`。
- `keep_snapshot_count`: スナップショットの履歴がなければ `NULL`。あれば正の整数で、すべてのクライアントの保持件数に等しくなければならない。
- `retention_mode`: `delete` か `ttl`。
- `ttl_grace_days`: 期限切れ方式では正の日数、それ以外は `NULL`。
- `change_feed_enabled`: `FALSE` を記録する。`TRUE` の行は設定エラーとして拒否される。

履歴のない削除方式では、`keep_snapshot_count` を `NULL`、`retention_mode` を `'delete'` にします。期限切れ方式には保持件数が必要なので、`keep_snapshot_count` は正の整数です。

削除方式で、集約ごとに新しい履歴 3 件を残す場合:

<!-- spanner-schema:config-delete -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled)
VALUES (1, GENERATE_UUID(), 1, 3, 'delete', NULL, FALSE);
```

期限切れ方式で、新しい履歴 3 件を残し、猶予を 7 日にする場合。`7` は、行削除ポリシーの `INTERVAL 7 DAY` と `Config.TTLGraceDays` に等しくなければなりません。

<!-- spanner-schema:config-ttl -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled)
VALUES (1, GENERATE_UUID(), 1, 3, 'ttl', 7, FALSE);
```

## 初期化

この手順は、どのクライアントも書き込んでいないときに、アプリケーションの外で一度だけ行います。アプリケーションは配置を作ることも直すこともしないので、初回の起動でも再起動でも、同じ形で `spanner.New` を呼びます。

1. ストアの書き込み側をすべて止めます。
2. 上の DDL を、自分の接頭辞で実行し、終わるのを待ちます。`esa_snapshot_history` は保持の方式に合う定義を使います。
3. 5 つの表がすべて空であることを確かめます（`SELECT COUNT(*) FROM esa_journal;` と、ほかの 4 つの表の同じ問い合わせ）。
4. 対応する上の `INSERT` で、設定の行を 1 つ書きます。
5. 書き込み側を起動します。[README](../README.ja.md) のドメインの型を使うと、ファクトリは次のとおりです。

```go
import (
    "context"
    gspanner "cloud.google.com/go/spanner"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/spanner"
)

// Run at every start, the first one included. New verifies the layout and never changes it.
func openSpanner(ctx context.Context, client *gspanner.Client) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return nil, err }
    return spanner.New(ctx, client, spanner.Config{TablePrefix: "esa"},
        eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

`spanner.New` は、照合する項目のどれかが足りないか、合わないと、設定エラーを返します。照合するのは次の項目です。データベースが GoogleSQL の方言であること。5 つの表の列の名前と型、主キー。設定の行（`store_id` が小文字の UUID、`layout_version` が 1、`keep_snapshot_count` がクライアントの保持件数に等しいこと、`retention_mode`、`ttl_grace_days`、`change_feed_enabled` が `FALSE`）。`esa_snapshot_history` の行削除ポリシー（期限切れ方式では猶予の日数に合い、削除方式ではないこと）。保存先を読めないときは、保存先エラーを返します。何も修復しません。クライアントは呼び出し側のもので、閉じるのも呼び出し側です。

ストアの使用中の DDL の変更と、ライブラリを通さないデータの変更は、サポートしません。

## 記録

列:

- `hash_prefix`: `aid` の FNV-1a 64 のハッシュを、ビッグエンディアンの 8 バイトにしたもの。符号付き整数へ変換せず、シャード数で割りません。キーは `(hash_prefix, aid)` の組の完全一致で比べます。2 つのハッシュが衝突しても、`aid` が違えば別のキーです。
- `aid`: `typeName-value`。UTF-8 で 1024 バイト以下で、ライブラリが書き込みの前に確かめます。
- `seq_nr`: 1 から 2^53 - 1。
- `occurred_at_ns` と `event_occurred_at_ns`: Unix エポックからの符号付き 64 ビットのナノ秒。`TIMESTAMP` ではなく `INT64` で保存します。
- `esa_snapshot_history.expired_at`: 期限切れの印を付けた時刻。イベントの発生時刻ではありません。印のない行では `NULL` のままで、期限切れ方式でもそれらは削除されません。
- `esa_head` は、確定した最新のイベントを、メタデータとペイロードごと持ちます。`esa_snapshot_current` は最新のスナップショットを持ち、スナップショットを書いていなければ行がありません。

## 書き込みと読み取り

書き込みは SERIALIZABLE の読み書きトランザクションで行います。トランザクションが中断されたら、新しいトランザクションで、同じイベントと同じ番号のまま、`TransactionRetryLimit` 回までやり直します。上限を超えると保存先エラーで失敗し、何も確定しません。

トランザクションの中では、まず集約のヘッドの `seq_nr` を読みます。新しい番号がヘッドの番号以下なら楽観ロックのエラー、ヘッド + 1 より大きければ契約違反（規則 `W-8`）です。そうでなければ、journal の行と head の行（最初のイベントは挿入、以後は更新）を 1 つのトランザクションで確定します。スナップショット付きの書き込みは、同じトランザクションで `esa_snapshot_current` の行も挿入か更新し、保持を設定していれば `expired_at` を `NULL` にした `esa_snapshot_history` の行も足します。`occurred_at_ns` は、イベントの時刻のナノ秒です。

コミットが失敗するか、その応答が失われたときは、結果が分かりません。書き込みは保存先エラーで失敗し、補償の書き込みもしません。同じイベントをもう一度送ると、最初のコミットが成功していれば楽観ロックのエラーになります。

ヘッドと最新のスナップショットは、強整合の読み取り専用トランザクション 1 つで読みます。ヘッドとそこまでの journal の行（主キーの範囲）も同じです。journal の欠番や、範囲外の保存された番号は保存先エラーで、短くなった結果を成功として返すことはありません。

## 保持

保持を省略すると、履歴は作りません。`KeepLatest(n)` では、スナップショット付きの書き込みごとに履歴の行を挿入します。書き込みが確定したあと、別々の SERIALIZABLE トランザクションが、その集約の印のない履歴の新しい `n` 件を残します。

- 削除方式は、古い行を削除します。
- 期限切れ方式は、古い行の `expired_at` に一度だけ印を付けます。印のある行は再び数えず、猶予のあとで Spanner が削除します。

1 つの部分で扱う行は 1000 件までです。残りがあるときは、次の部分のために候補を選び直します。`esa_snapshot_current`、`esa_head`、journal は削除しません。イベントだけの書き込みは履歴に触れません。保持の失敗は、ログに記録され、`WithRetentionFailureHandler` に渡されます。確定済みの書き込みの結果は変わらず、ハンドラやログの出力が panic しても同じです。

## 運用

- 配置と設定の行はアプリケーションの外で作ります。アプリケーションの権限には、必要なデータの操作だけを与えます。
- 保持件数や保持の方式を変えるときは、書き込み側をすべて止めて、DDL、行削除ポリシー、設定の行を一緒に変えます。ライブラリにはそのためのツールがありません。
- 期限切れ方式では、Spanner が期限切れの行をバックグラウンドで削除し、すぐには削除しません。印のない行は削除されません。
- このリポジトリの試験は、コンテナの Cloud Spanner エミュレーターで動かし、本物のサービスには接続しません。
- JavaScript v5 の実装が書いた保存先は、このパッケージで読み書きでき、その逆もできます。両者の間の移行の手順はありません。
