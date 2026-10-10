# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![Go project version](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go.svg)](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

このライブラリは、CQRS/Event Sourcing 用の同期イベントストアを Memory と DynamoDB で提供します。

[English](./README.md)

共通仕様 v4 に従います。Go の module パスは `github.com/j5ik2o/event-store-adapter-go/v2` のままです。共通仕様の版と Go の配布版は別のものです。

## 使い方

イベントと集約状態は任意の Go 型を使えます。metadata は `EventEnvelope[E]`・`SnapshotEnvelope[A]` に置き、`Serializer[T]` は payload だけを扱います。集約 ID は `TypeName() string` と `Value() string` を実装します。ライブラリが「型名 + "-" + 値」を組み立てます。型名に `-` を含めず、全体を UTF-8 で1024バイト以下にします。

次の実行可能な例は公開4操作を使い、`3 2` を出力します。[example_test.go](example_test.go) と [repository の試験](test/user_account_repository_test.go) も同じ復元手順を使います。

```go
package main

import (
	"context"
	"fmt"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/memory"
)

type orderID string

func (id orderID) TypeName() string { return "Order" }
func (id orderID) Value() string    { return string(id) }

type orderEvent struct {
	Added int `json:"added"`
}
type orderState struct {
	Total int `json:"total"`
}

func main() {
	ctx := context.Background()
	keep, err := eventstore.KeepLatest(2)
	if err != nil {
		panic(err)
	}
	state, err := memory.NewStore(eventstore.WithRetentionCount(keep))
	if err != nil {
		panic(err)
	}
	store, err := memory.New(state, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState]())
	if err != nil {
		panic(err)
	}
	id := orderID("9")
	first, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(1), time.Now(), orderEvent{Added: 1})
	if err != nil {
		panic(err)
	}
	snapshot, err := eventstore.NewSnapshotEnvelope(orderState{Total: 1}, eventstore.SeqNr(1))
	if err != nil {
		panic(err)
	}
	if err := store.PersistEventAndSnapshot(ctx, first, snapshot); err != nil {
		panic(err)
	}
	second, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(2), time.Now(), orderEvent{Added: 2})
	if err != nil {
		panic(err)
	}
	if err := store.PersistEvent(ctx, second); err != nil {
		panic(err)
	}

	read, err := store.GetLatestSnapshotByID(ctx, id)
	if err != nil {
		panic(err)
	}
	restored := orderState{}
	since := eventstore.SeqNr(1)
	if read != nil && read.Snapshot != nil {
		restored = read.Snapshot.Aggregate()
		since = read.Snapshot.SeqNr() + 1
	}
	events, err := store.GetEventsByIDSinceSeqNr(ctx, id, since)
	if err != nil {
		panic(err)
	}
	for _, event := range events {
		restored.Total += event.Payload().Added
	}
	fmt.Println(restored.Total, read.HeadSeqNr)
}
```

イベントの `SeqNr` は1から始まる連番です。番号の範囲は0〜9007199254740991で、イベントに0は使えません。`OccurredAt` は符号付き64ビットの Unix ナノ秒で表せる `time.Time` です。任意指定の `WithManifest` は、省略時に空文字列となります。

`PersistEvent` はイベント番号から直前の head 番号を求めて照合します。`PersistEventAndSnapshot` はイベントと snapshot の番号が等しいことを要求し、一緒に確定します。version 引数と `IsCreated` は使いません。

`GetLatestSnapshotByID` は head がなければ `(nil, nil)` を返します。head があっても `Snapshot` が nil の場合があります。復元は `Snapshot.SeqNr()+1`、snapshot がなければ1から行います。`HeadSeqNr` は snapshot の番号とは独立しており、復元の開始位置に使いません。イベント読取は指定番号を含み、該当する全イベントを昇順で返します。

## DynamoDB

[DATABASE_SCHEMA.ja.md](docs/DATABASE_SCHEMA.ja.md) の3表と履歴 index を事前に作り、AWS SDK v2 の Client を渡します。上の domain 型を使う factory は次のとおりです。

```go
import (
    "context"
    awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
    "github.com/j5ik2o/event-store-adapter-go/v2/dynamodb"
)

func openDynamoDB(ctx context.Context, client *awsdb.Client) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(2)
    if err != nil { return nil, err }
    return dynamodb.New(ctx, client, dynamodb.Config{
        JournalTableName: "journal",
        SnapshotTableName: "snapshot",
        HeadTableName: "head",
        SnapshotHistoryIndexName: "snapshot-history",
    }, eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

factory は3表の設定項目を照合し、すべて存在しない場合に transaction で作ります。表自体は作りません。head と current snapshot の BatchGetItem、journal の Query は強整合で読み、全頁を取得します。head と current snapshot の読取は原子的ではなく、並行書込により異なる番号が返る場合があります。

## 保持とエラー

保持件数を省略するか `WithRetentionCount(NoRetention())` を渡すと、current snapshot だけを保存して履歴を作りません。`KeepLatest(n)` は `n >= 1` を要求し、current snapshot は件数に含めません。Memory は削除方式、DynamoDB は `RetentionDelete`（既定）と `RetentionTTL` に対応します。`WithTTLGraceSeconds` は0以上の猶予秒を指定します。

保持処理は snapshot と履歴の確定後に同期で行います。失敗はログに記録され、`WithRetentionFailureHandler(func(context.Context, error))` でも通知できます。確定済み書込は成功のまま返ります。

`KindOf(err)`（分類と bool を返す）または `errors.As` で、楽観ロック・契約違反・直列化・設定・保存先の5分類を区別します。`errors.Is` と `Unwrap` で元の原因を取得できます。DynamoDB の未処理キーと削除の再要求は `Config.ConfigurationReadRetryLimit` を共有します。既定は初回を除いて10回、0は再要求なしです。待機は50ミリ秒から倍増し、最大1秒となり、context の取消で中断できます。

## テーブル仕様と移行

属性・権限・保持は [DATABASE_SCHEMA.ja.md](docs/DATABASE_SCHEMA.ja.md)、コードと旧データの移行は [MIGRATION_GUIDE.ja.md](docs/MIGRATION_GUIDE.ja.md) を参照してください。

## ライセンス

MITライセンスです。詳細は[LICENSE](LICENSE)を参照してください。

## 他の言語のための実装

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
- [for .NET](https://github.com/j5ik2o/event-store-adapter-dotnet)
- [for PHP](https://github.com/j5ik2o/event-store-adapter-php)
