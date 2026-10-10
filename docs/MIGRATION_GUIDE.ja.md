# 封筒 API への移行

[English](MIGRATION_GUIDE.md)

共通仕様 v4 に対応する Source です。Go module パスは `github.com/j5ik2o/event-store-adapter-go/v2` のままです。配布版番号の変更やリリースの案内ではありません。

## コードの移行

1. 旧 `pkg` の import を module ルートの `eventstore` と `memory` または `dynamodb` に置き換えます。旧 package と互換 alias は削除されています。
2. 公開4操作と `dynamodb.New` の先頭に `context.Context` を渡します。
3. `TypeName()`・`Value()` で `AggregateID` を実装します。`AsString`・KeyResolver・シャード設定を外します。型名に `-` を含めず、組み立てた ID を UTF-8 で1024バイト以下にします。
4. イベントと状態には任意の domain 型、直列化には `Serializer[T]`（例: `NewJSONSerializer[T]()`）を使います。旧 Event/Aggregate interface と map converter を外し、metadata を検査済みの `NewEventEnvelope`・`NewSnapshotEnvelope` に分けます。任意の manifest は `WithManifest` で指定します。
5. version 引数・version 照合・`IsCreated` を外し、イベントに1から始まる連番を付けます。pair 書込ではイベントと snapshot の番号を一致させます。直前の head はイベント番号から求められます。
6. `memory.NewStore` と `memory.New`、または3表と履歴 index を `Config` で指定する `dynamodb.New` を使います。設定は `Option` で渡します。既定と `NoRetention()` は履歴なし、保持には正の `KeepLatest(n)` を使います。0は設定エラーです。Memory は TTL 方式を受け付けません。DynamoDB は `RetentionTTL` と0以上の `WithTTLGraceSeconds` に対応します。
7. `KindOf`・`errors.As` で5分類を判別し、`Unwrap` で原因を扱います。保持失敗のログ・通知は、確定済み書込を失敗に変えません。

復元は snapshot の次番号、snapshot がなければ1から行います。head がなければ nil が返ります。`HeadSeqNr` を開始位置に使いません。特に DynamoDB の並行読取では、head と snapshot の番号が異なる場合があります。

```go
read, err := store.GetLatestSnapshotByID(ctx, id)
if err != nil { return err }
if read == nil { return fmt.Errorf("aggregate not found") }
since := eventstore.SeqNr(1)
var aggregate orderState
if read.Snapshot != nil {
    aggregate = read.Snapshot.Aggregate()
    since = read.Snapshot.SeqNr() + 1
}
events, err := store.GetEventsByIDSinceSeqNr(ctx, id, since)
if err != nil { return err }
for _, event := range events {
    aggregate.Total += event.Payload().Added
}
```

[README](../README.ja.md) の domain 型を使う抜粋です。[実行可能な例](../example_test.go) と [repository の試験](../test/user_account_repository_test.go) は、snapshot 1・head 2 の場合と、snapshot なしでの復元を検証します。

## Go の旧データ

Go の移行支援はこの手順書です。移行ツール・旧配置 reader・自動変換・実行時 fallback は提供しません。Rust v3 の移行ツールは異なる旧配置を対象とするため、Go のデータに流用しません。

1. 旧書込を停止し、バックアップを保存します。**旧版のライブラリと旧 API** で journal と snapshot を全頁読みます。Client を切り替える前に、集約ごとのイベント順序、完全な metadata、payload、状態を記録します。
2. 利用者の移行コードで、埋め込まれた metadata と domain の payload・状態を分けます。旧 ID とイベント型の定義を確認します。旧 `GetOccurredAt() uint64` は時刻の単位を定めていません。アプリケーションの旧定義を確認してから `time.Time` に変換し、単位や失われた精度・型情報を推測しません。
3. [DATABASE_SCHEMA](DATABASE_SCHEMA.ja.md) の**新しい3表配置**、履歴 GSI、必要に応じた TTL を作ります。`dynamodb.New` に配置版1の一致する設定項目を作らせます。旧表と新表を混ぜません。
4. 集約ごとに検査済み封筒を作り、元の順序を保って **1, 2, 3, … の連続番号**で新 API に書き直します。snapshot はその番号までの状態から作り、同じ番号の pair 書込で保存します。旧 snapshot だけでは、この書直しに必要なイベント履歴の代わりになりません。
5. 新 API で読み戻し、イベント件数・metadata・payload・復元状態を旧版で取得した記録と比較します。確認後にアプリケーションの読取と書込を切り替え、移行が受け入れられるまでバックアップを保存します。

旧2表のシャード付きキーと snapshot の version 属性をそのまま再利用できません。時刻の変換と metadata/payload の分離は利用者の移行コードが担当します。
