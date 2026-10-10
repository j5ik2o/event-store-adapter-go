# DynamoDB のテーブル構成

[English](DATABASE_SCHEMA.md)

共通仕様 v4 に対応する配置の版は1です。Go module は `/v2` です。`dynamodb.New` の前に、利用者が異なる3表と snapshot の履歴 GSI を作ります。

| 表 | パーティションキー | ソートキー | 追加設定 |
|---|---|---|---|
| journal | `aid` S | `seq_nr` N | GSI・Streams・TTL なし |
| snapshot | `aid` S | `skey` N | 履歴 GSI: (`aid` S, `active_history_seq_nr` N)、`KEYS_ONLY`。Streams なし。TTL 保持時は属性 `ttl` の TTL を有効化 |
| head | `aid` S | なし | Streams を `NEW_IMAGE` で有効化。GSI・TTL なし |

`aid` は検査済みの型名・ハイフン・ID値を連結します。シャードと KeyResolver は使いません。

## 保存属性

| 項目 | 属性の全集合（DynamoDB の型） |
|---|---|
| journal のイベント | `aid` S、`seq_nr` N、`occurred_at` N、`manifest` S、`payload` B |
| current snapshot（`skey=0`） | `aid` S、`skey` N、`seq_nr` N、`manifest` S、`payload` B、`last_updated_at` N |
| 印なし履歴（`skey=seq_nr`） | current snapshot の属性に `active_history_seq_nr` N を追加 |
| 印付き履歴 | current snapshot の属性に `ttl` N を追加。`active_history_seq_nr` なし |
| head | `aid` S、`type_name` S、`seq_nr` N、`events` L |

`events` の要素は M が1件で、`seq_nr` N・`occurred_at` N・`manifest` S・`payload` B を持ちます。イベントだけの書込を含め、直前の確定書込のイベントです。

`occurred_at` は符号付き Unix ナノ秒です。`last_updated_at` は書いたイベントの Unix ミリ秒で、参考情報です。B は直列化した domain データだけを持ちます。manifest の既定は空の S です。current snapshot と設定項目は TTL と履歴 index 属性を持ちません。`version` 属性はありません。

## 設定項目

factory は次の予約キーを BatchGetItem で強整合読み取りします。

| 表 | キー |
|---|---|
| journal | `aid="__config__"`、`seq_nr=0` |
| snapshot | `aid="__config__"`、`skey=0` |
| head | `aid="__config__"` |

各項目はキー属性、`layout_version` N（`1`）、`store_id` S だけを持ちます。3件とも存在しない場合は1つの store ID を生成し、条件付き Put を1 transaction で行います。存在する場合は3件の ID と対応配置版が一致しなければなりません。一部だけの存在や不一致は設定エラーです。生成競合では3件を読み直し、その後も存在しなければ保存先エラーを返します。Streams の利用側は head の設定レコードを読み飛ばします。

## 書込・読取・保持

書込は journal と head を1 transaction で確定し、pair 書込では current snapshot と、保持件数を指定した場合の履歴も含めます。直前の head が `event.SeqNr()-1` と一致する必要があります。head の付随属性を含めた見積りが409600バイトを超える項目は、要求送信前に失敗します。

最新 snapshot は head と current snapshot を BatchGetItem で強整合読み取りしますが、原子的ではなく番号が異なる場合があります。イベントは journal 本体から指定番号を含めて昇順・強整合・全頁で読みます。復元は snapshot の次番号、snapshot がなければ1から始めます。

保持件数の省略時は履歴なしです。正の n 件を指定すると、current を除いた最新の印なし履歴 n 件を残します。疎な履歴 GSI を結果整合で全頁 Query し、今書いた履歴が見えなければ加えます。削除方式は最大25件ずつ送ります。TTL 方式は印付け時点の Unix 秒と猶予秒の和を `ttl` に入れ、`active_history_seq_nr` を除去します。印付き履歴は GSI と保持件数の対象から外れ、期限を先送りしません。TTL による物理削除は非同期です。

保持失敗はログと任意の通知に伝え、確定済み書込の成功を変えません。BatchGetItem の未処理キーと BatchWriteItem の未処理削除だけを再要求します。既定の再要求上限は初回を除いて10回です。

## 実行用の権限

REGION・ACCOUNT・表名・index 名を置き換えてください。設定生成・公開4操作・両保持方式を含む例です。BatchGetItem・BatchWriteItem は独自の IAM action を必要とします。TransactWriteItems は内部の PutItem・UpdateItem の権限で許可され、同名の IAM action はありません。[AWS の action 一覧](https://docs.aws.amazon.com/service-authorization/latest/reference/list_dynamodb.html) を参照してください。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:BatchGetItem", "dynamodb:PutItem"],
      "Resource": [
        "arn:aws:dynamodb:REGION:ACCOUNT:table/journal",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/snapshot",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/head"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:UpdateItem",
      "Resource": [
        "arn:aws:dynamodb:REGION:ACCOUNT:table/head",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/snapshot"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:Query",
      "Resource": [
        "arn:aws:dynamodb:REGION:ACCOUNT:table/journal",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/snapshot/index/snapshot-history"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:BatchWriteItem",
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/snapshot"
    }
  ]
}
```

表作成と試験観測には別の管理用 Client を使います。CreateTable・DescribeTable・DescribeTimeToLive・UpdateTimeToLive・DeleteTable と、seed 投入・観測用の権限が必要です。実行用の操作は Scan と表作成を行いません。DynamoDB Local は IAM を検査しないため、Local の試験はこの policy の権限検証ではありません。Streams 購読の権限は購読アプリケーションが管理します。
