> この文書は [README.md](README.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![Go project version](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go.svg)](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-APACHE2.0-blue.svg)](https://opensource.org/licenses/apache-2-0)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

## 概要

このライブラリは、CQRS/Event Sourcing 用の同期イベントストアを Memory、DynamoDB、SQLite、Spanner、Bigtable で提供します。

[English](./README.md)

[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) に従います。メジャーの番号 5 は、共通契約の版と同じです。

他の言語のための実装:

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
- [for .NET](https://github.com/j5ik2o/event-store-adapter-dotnet)
- [for PHP](https://github.com/j5ik2o/event-store-adapter-php)

[共通文書](https://github.com/j5ik2o/event-store-adapter)も参照してください。

このリポジトリを変更するときは [CONTRIBUTING.ja.md](CONTRIBUTING.ja.md) を参照してください。

## インストール

導入:

```sh
go get github.com/j5ik2o/event-store-adapter-go/v5
```

## はじめに

イベントと集約状態は任意の Go 型を使えます。metadata は `EventEnvelope[E]`・`SnapshotEnvelope[A]` に置き、`Serializer[T]` は payload だけを扱います。集約 ID は `TypeName() string` と `Value() string` を実装します。ライブラリが「型名 + "-" + 値」を組み立てます。型名に `-` を含めず、全体を UTF-8 で1024バイト以下にします。

次の実行可能な例は公開4操作を使い、`3 2` を出力します。[example_test.go](example_test.go) と [repository の試験](test/user_account_repository_test.go) も同じ復元手順を使います。

```go
package main

import (
	"context"
	"fmt"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/memory"
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

## 保存先

### メモリ

メモリの保存先は単一プロセスのメモリだけを使います。ファイルへの保存、再起動後の回復、プロセス間の共有はしません。

### DynamoDB

[DYNAMODB_DATABASE_SCHEMA.ja.md](docs/DYNAMODB_DATABASE_SCHEMA.ja.md) の3表と履歴 index を事前に作り、AWS SDK v2 の Client を渡します。3 つの表は同じリージョンに置き、書き込みもそのリージョンからだけ行ってください。グローバルテーブル（複数リージョンのレプリカ）は範囲外です。上の domain 型を使う factory は次のとおりです。

```go
import (
    "context"
    awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/dynamodb"
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

### SQLite

SQLite の保存先は、イベントとスナップショットを 1 つのローカルのデータベースファイルに保存します。配置は Rust v5 の実装と同じ 5 つの表です。詳しくは [SQLITE_DATABASE_SCHEMA.ja.md](docs/SQLITE_DATABASE_SCHEMA.ja.md) を参照してください。新しいファイルは `sqlite.Initialize` で一度だけ初期化し、`sqlite.Open` で開き、`sqlite.New` で serializer をつなぎます。`Open` はファイルを作らず、初期化もしません。上の domain 型を使うと次のとおりです。

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

使い終わったら `Store.Close` を呼びます。ローカルのファイルシステムを使ってください。TTL 方式の保持と変更フィードは設定エラーです。追記は、ヘッドを照合する前に書き込みのロックを取ります。別の接続が `Config.LockWait`（既定は 5 秒）より長くロックを持つと、追記は保存先エラーで失敗します。

### Spanner

Spanner の保存先は、イベントとスナップショットを 1 つの Cloud Spanner データベース（GoogleSQL の方言）の 5 つの表に保存します。行の形は JavaScript v5 の実装と同じです。詳しくは [SPANNER_DATABASE_SCHEMA.ja.md](docs/SPANNER_DATABASE_SCHEMA.ja.md) を参照してください。表と設定の行は、その文書の DDL を使って、自分で一度だけ作ります。ライブラリは DDL を実行しません。`cloud.google.com/go/spanner` の `*spanner.Client` を渡します。クライアントを閉じるのは呼び出し側です。配置はプログラムの外で用意するので、初回の起動でも再起動でも、同じファクトリを呼びます。上のドメインの型を使うと:

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

`TablePrefix` が 5 つの表の名前を導きます。保持件数と方式は、設定の行と一致していなければなりません。変更フィードは設定エラーです。追記は SERIALIZABLE のトランザクションで行い、中断されたら `Config.TransactionRetryLimit` 回（既定は 5）までやり直します。読み取りは強整合の読み取り専用トランザクション 1 つで行います。コミットの応答が失われたときの追記は、結果が分からない保存先エラーで失敗します。

### Bigtable

Bigtable の保存先は、イベントとスナップショットを 1 つの Cloud Bigtable インスタンスの 2 つの表（journal 表と head 表）に保存します。行の形は Rust v5 の実装と同じです。詳しくは [BIGTABLE_DATABASE_SCHEMA.ja.md](docs/BIGTABLE_DATABASE_SCHEMA.ja.md) を参照してください。2 つの表と列ファミリは、`cbt` など管理用のクライアントで自分で作ります。ライブラリは表を作りません。`cloud.google.com/go/bigtable` の `*bigtable.Client` と `*bigtable.AdminClient`（下では `gbigtable` の名前で import します）を渡します。認証、1 つのクラスタに固定した app profile、接続は呼び出し側が設定し、クライアントを閉じるのも呼び出し側です。設定の行は `bigtable.Initialize` で一度だけ書き、初回の起動を含むすべての起動で `bigtable.New` を使ってストアを開きます。上のドメインの型を使うと:

```go
import (
    "context"
    gbigtable "cloud.google.com/go/bigtable"
    eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
    "github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
)

var bigtableConfig = bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"}

// Run once, before the first start, while no writer is running. It writes only the two configuration rows.
func prepareBigtable(ctx context.Context, data *gbigtable.Client) error {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return err }
    return bigtable.Initialize(ctx, data, bigtableConfig, eventstore.WithRetentionCount(keep))
}

// Run at every start, the first one included. New verifies the tables and never changes them.
func openBigtable(ctx context.Context, data *gbigtable.Client, admin *gbigtable.AdminClient) (eventstore.EventStore[orderEvent, orderState], error) {
    keep, err := eventstore.KeepLatest(3)
    if err != nil { return nil, err }
    return bigtable.New(ctx, data, admin, bigtableConfig,
        eventstore.NewJSONSerializer[orderEvent](), eventstore.NewJSONSerializer[orderState](),
        eventstore.WithRetentionCount(keep))
}
```

どちらかの表に設定の行がすでにあると `Initialize` は設定エラーで失敗するので、一度だけ呼びます。初期化が途中で止まったときの戻し方は、スキーマ文書にあります。保持件数と `Config.EnvelopeLimitBytes` は、設定の行と一致していなければなりません。期限切れ方式と変更フィードは設定エラーです。追記の確定点は head の行への条件付き更新です。その応答が失われたときの追記は、結果が分からない保存先エラーで失敗し、書き込みは繰り返しません。自分の gRPC 接続の上にデータのクライアントを作るときは、封筒の上限（最大 8 MiB）の大きさのメッセージを受け取れるように設定してください。

## 直列化

`Serializer[T]` は payload または集約状態だけを直列化し、封筒の metadata は渡されません。domain 型にライブラリ固有の interface は不要です。`Serialize(value T) ([]byte, error)` と `Deserialize(data []byte) (T, error)` を実装します。

`NewJSONSerializer[T]()` は `encoding/json` を使います。型の決まっていない JSON の数値は `json.Number` として復元し、JSON の値が2つ以上ある入力は失敗にします。

serializer が返したエラーは `SerializationError`（`KindSerialization`）に分類されます。serializer が nil の場合は `ConfigurationError` です。

## 保持

保持件数を省略するか `WithRetentionCount(NoRetention())` を渡すと、current snapshot だけを保存して履歴を作りません。`KeepLatest(n)` は `n >= 1` を要求し、current snapshot は件数に含めません。Memory と SQLite は削除方式だけ、DynamoDB は `RetentionDelete`（既定）と `RetentionTTL` に対応します。`WithTTLGraceSeconds` は0以上の猶予秒を指定します。Spanner も `RetentionDelete`（既定）と `RetentionTTL` に対応し、期限切れ方式の猶予は `Config.TTLGraceDays`（日数）で指定します。Spanner で `WithTTLGraceSeconds` に 0 以外を渡すと設定エラーです。Bigtable は削除方式だけに対応します。

保持処理は snapshot と履歴の確定後に同期で行います。失敗はログに記録され、`WithRetentionFailureHandler(func(context.Context, error))` でも通知できます。確定済み書込は成功のまま返ります。

## エラー

`KindOf(err)`（分類と bool を返す）または `errors.As` で、楽観ロック・契約違反・直列化・設定・保存先の5分類を区別します。`errors.Is` と `Unwrap` で元の原因を取得できます。DynamoDB の未処理キーと削除の再要求は `Config.ConfigurationReadRetryLimit` を共有します。既定は初回を除いて10回、0は再要求なしです。待機は50ミリ秒から倍増し、最大1秒となり、context の取消で中断できます。

## 移行

属性・権限・保持は [DYNAMODB_DATABASE_SCHEMA.ja.md](docs/DYNAMODB_DATABASE_SCHEMA.ja.md)、コードと旧データの移行は [MIGRATION_GUIDE.ja.md](docs/MIGRATION_GUIDE.ja.md) を参照してください。

## 適合

`conformance/` に共通の適合テストデータ（版 `1.3.0`、[conformance/README.md](conformance/README.md) を参照）を置いています。CI の `lint` ジョブが `tools/conformance/manifest.py verify` と SHA-256 で照合します。

CI の `conformance` ジョブは、`CONFORMANCE_REPORT` を設定して `go test -count=1 -v ./internal/conformance/...` を実行し、`conformance-report.json` を成果物として上げます。実行器は Memory、DynamoDB、SQLite、Spanner、Bigtable の保存先を対象とし、DynamoDB は DynamoDB Local、Spanner は Cloud Spanner エミュレーター、Bigtable は Cloud Bigtable エミュレーターをそれぞれコンテナで起動して使い、SQLite はローカルのファイルを使います。実行されなかった SQLite・Spanner・Bigtable の必須ケースがあると、ジョブは失敗します。`CONFORMANCE_REPORT` がなければ、この試験は飛ばされます。

## ライセンス

MITライセンスです。詳細は[LICENSE-MIT](LICENSE-MIT)を参照してください。リポジトリには Apache License 2.0 の本文も [LICENSE-APACHE](LICENSE-APACHE) にあります。
