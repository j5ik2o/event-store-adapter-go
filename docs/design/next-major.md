# 次のメジャー版（v2.0.0）の設計

このファイルは設計文書である。コードは書かない。指揮役がレビューし、9章の判断（2026-10-06 に決定済み）を反映してから実装に使う。

## 読み方

- 規則番号は、ハブ（j5ik2o/event-store-adapter）の `docs/spec/core-contract.md`・`docs/spec/storage/dynamodb.md`・`docs/spec/storage/memory.md` のものである。「CR」は `conformance/README.md`、「IP」は実装計画の節を指す。
- この文書は仕様を変えない。規則も足さない。仕様の読み方が分からない点は10章に書く。
- 2章の宣言は「この言語で実際に書く形」の提案である。パッケージの置き場は9章で決めた。モジュールのルート（`eventstore`）に封筒・エラー・操作の型を置き、保存先は `memory` と `dynamodb` に分ける。
- 現行コードの事実は、2026-10-06 時点の `pkg/` と `test/` を読んで確認した。

---

## 1. 目的と範囲

### 1.1 目的

新しい共通契約（封筒・ヘッド・分類したエラー・適合テストデータ）に合わせて、このライブラリを書き直す。

### 1.2 最初のメジャーに含めるもの（IP-D3・IP-D4）

| 項目 | 内容 |
|---|---|
| 中核 | 封筒・集約 ID・検査・シリアライザ・5分類のエラー・4つの操作 |
| メモリ | 単一プロセスのメモリ実装。適合も宣言する（IP-D4） |
| DynamoDB | 3テーブル（journal・snapshot・head）と設定項目 |
| 適合テストデータの実行器 | `conformance/` を読み、メモリと DynamoDB で全ケースを実行する |
| 文書 | README・DATABASE_SCHEMA・MIGRATION_GUIDE を新契約に合わせる（IP 5-5・5-6） |

### 1.3 外すもの

- 旧データを自動で読む機能。Go 用の移行ツール。手順書だけを用意する（IP-D8、8章）。
- SQLite・Bigtable・Spanner などの他の保存先（IP 4.2）。
- メモリの TTL 方式と変更フィード（MEM-12・MEM-13。要求は設定エラー）。
- 変更フィードの補助（ヘッド遷移を組み立てる関数と、再同期（DY-15）の補助）。最初のメジャーに含めない（2026-10-06 オーナー決定、IP 10）。head テーブルの Streams（NEW_IMAGE）への記録は DY-3・DY-12 のとおり残るので、利用者は自分で読める。公開 API に変更フィードの関数はない。

### 1.4 次のメジャーの版の番号

現行は `v1.0.197`（`version` ファイル）。次のメジャーは `v2.0.0` とする（IP 4.2）。モジュールのパスは `github.com/j5ik2o/event-store-adapter-go/v2` に変える。パスを変えないと、v2 のタグを Go のツールチェーンが受け付けない（IP 4.2）。

Go には Snapshot を公開する仕組みがない。モジュールのパスを `/v2` にすれば、利用者は main のコミットを疑似バージョン（タグのない `/v2` では `v2.0.0-<時刻>-<コミット>` の形）で取得できる。これを次のメジャーの開発版とする。PR ごとに squash マージする（IP 3）。7章で扱う。

`version` ファイルは、手動起動だけの `.github/workflows/release.yml` が、`git describe` で得た最新のタグから版を計算して上書きする。手で `version` を変えても効かないので、変えない。正式な `v2.0.0` は、オーナーの承認の後に `level=major` で手動起動して出す。この文書では、リリースの仕組みの変更を提案しない。

`/v2` に変えた後は、main から `v1.x` のタグを打てない。現行メジャーの修正が要るときは、実装計画 3章のとおり、最後のタグから `release/1.x` の保守ブランチを切る。CI と版上げのワークフローは、その時点で対応させる。

---

## 2. 公開 API

以下の宣言は提案である。`context.Context` は第1引数に置く。panic は使わない。

### 2.1 集約 ID（T-1・T-11・T-12）

利用者の `String()` や `AsString()` を保存キーに使わない（T-1）。ライブラリが型名と値から組み立てる。

```go
// AggregateID は集約 ID の型名と値を返す。T-1。
// 文字列表現（{型名}-{値}）はライブラリが組み立てる。
type AggregateID interface {
    TypeName() string // T-11: "-" を含まない
    Value() string
}

// AidString は T-1 の集約 ID 文字列を組み立てる。
// 型名に "-" を含むときは T-11、合計が 1024 バイトを超えるときは T-12 の契約違反を返す。
// 長さは len(string)（UTF-8 のバイト数）で数える。文字数ではない。
func AidString(id AggregateID) (string, error)

// NewAggregateID は型名と値から AggregateID を作る補助関数。
func NewAggregateID(typeName, value string) (AggregateID, error)
```

- 検査は封筒の構築時が基本である。保存先の各操作の入口でも同じ検査関数を呼び直す。Go では `EventEnvelope[E]{}` のゼロ値や、コンストラクタを通さない封筒を作れるので、それを弾くためである。ゼロ値の封筒は、aid が空で T-2 の必須要素が欠けるので契約違反になる。`AggregateID` は interface なので、利用者は `nil` を渡せる。`AggregateID` を受け取る `AidString`・`NewEventEnvelope` と、読み取りの操作（`GetLatestSnapshotByID`・`GetEventsByIDSinceSeqNr`）は、`TypeName()` を呼ぶ前に `nil` を検査し、panic せずに契約違反を返す。書き込みの操作はイベント封筒を受け取るので、入口でイベント封筒の必須要素を検査し直す（T-2。ゼロ値の封筒は aid が空になる）。スナップショット封筒（`NewSnapshotEnvelope`）は `aggregate` と `seqNr` を位置引数で受け取るので、欠落は型が許さない。`seqNr` の 0 は T-9 の有効な値で、欠落ではない（番号 0 のスナップショットは、組になるイベントの W-6 で拒まれる。適合データの `core-zero-snapshot`）。このため、スナップショット封筒に T-10 の欠落の検査は要らない（型で欠落を表せない場合の免除、P-42）。必須要素の欠落を契約違反にすることは、共通契約の T-2・T-10 が定める（P-42）。封筒の外（`AidString` と読み取りの操作）で受けた `nil` の `AggregateID` は、仕様が規則番号を定めない引数の誤りである。集約 ID は封筒の必須要素 aggregate_id と同じ値なので、規則番号は `T-2` とする（設計の判断）。メッセージには、欠けたのが seq_nr のときを除き、与えられた seq_nr を含める（E-3）。中核の単体試験で、ゼロ値の封筒と `nil` の `AggregateID`（構築・`AidString`・読み取り）が panic せずに契約違反になることを確かめる（7.2 の5番・6番）。seq_nr 0 は W-6。W-9（スナップショットとイベントの番号の比較）は入口だけで行う。MEM-5 は T-9・T-11・T-12・T-13 の検査を要求する。
- 空の型名・空の値は許す。仕様（T-11・T-12）は禁じておらず、aid 文字列は `-` を含むので空にならない（指揮役の回答、2026-10-06）。適合データにこのケースはないので、中核の単体試験で、型名が空（`-値`）・値が空（`型名-`）・両方が空（`-`）を確かめる。
- 保存先は検査済みの文字列だけを使う。DynamoDB の PK は aid 文字列そのもの（DY-16）。
- ハッシュで識別しない。前方一致で選ばない（MEM-5）。

### 2.2 整数と時刻の型（T-9・T-13・1.5）

```go
// SeqNr は集約の連番。T-9: 0 以上 2^53-1 以下。
type SeqNr int64

const MaxSeqNr SeqNr = 1<<53 - 1

// Validate は T-9 を検査する。0 は有効（一般値）。
func (n SeqNr) Validate() error

// ValidateAsEventSeqNr はイベントの seq_nr として検査する。
// 0 は W-6 の契約違反。範囲外は T-9 の契約違反。
func (n SeqNr) ValidateAsEventSeqNr() error
```

- 符号付き `int64` を使う（9.3 で決定）。負数（-1）や 2^53 のケースを、型の変換で消さずに契約違反として返せる。
- 時刻は `time.Time` を使う。`occurred_at` は `UnixNano()` を呼ぶ前に、T-13 の範囲（エポックからのナノ秒が符号付き64bitに収まる）を検査する。範囲外は契約違反。`time.Time` は年の範囲が広く、`UnixNano()` は範囲外で未定義値を返すため、先に検査する。
- Go の `time.Time` はナノ秒を保てる。T-3 の「標準時刻型の精度まで丸めてよい」に該当する丸めは、通常は生じない。実行器は `representation.time_precision = nanoseconds` のケースを実行する（5章）。

### 2.3 イベント封筒（T-2・T-3・T-5・T-13）

封筒は不変にする。フィールドは非公開にし、アクセサで読む（T-5。要素の追加が既存の利用コードを壊さない）。

```go
// EventEnvelope は集約に追記する1件のイベント。T-2。
type EventEnvelope[E any] struct {
    aggregateID string // 検査済みの aid 文字列
    seqNr       SeqNr
    occurredAt  time.Time
    manifest    string
    payload     E
}

// NewEventEnvelope は封筒を作る。次の検査を行い、違反は契約違反を返す。
//   - aid: T-1・T-11・T-12
//   - seqNr: T-9（0 は W-6 として ValidateAsEventSeqNr で検査）
//   - occurredAt: T-13
// manifest 以外は必須（T-2）。manifest の省略時は空文字列。
func NewEventEnvelope[E any](
    id AggregateID,
    seqNr SeqNr,
    occurredAt time.Time,
    payload E,
    opts ...EnvelopeOption,
) (EventEnvelope[E], error)

// WithManifest は manifest を指定する。省略時は "" （T-2）。
func WithManifest(manifest string) EnvelopeOption

func (e EventEnvelope[E]) AggregateID() string
func (e EventEnvelope[E]) SeqNr() SeqNr
func (e EventEnvelope[E]) OccurredAt() time.Time // T-3: ストア側時刻で置き換えない
func (e EventEnvelope[E]) Manifest() string
func (e EventEnvelope[E]) Payload() E
```

- ライブラリは manifest と payload を解釈しない（T-4）。
- `occurred_at` を保存先の時刻で置き換えない（T-3）。DynamoDB では occurred_at をエポックナノ秒の N で保存する（5章の属性表）。
- 必須の payload と、値としての JSON `null` を区別する。`null` 自体は禁止しない。

### 2.4 スナップショット封筒（T-10）

```go
// SnapshotEnvelope は集約の状態の保存形。T-10。
// ヘッドの seq_nr は含めない（ADR-0002）。書き込みと読み取りで同じ型を使う。
type SnapshotEnvelope[A any] struct {
    aggregate A
    seqNr     SeqNr
    manifest  string
}

// aggregate と seqNr は必須。manifest の省略時は空文字列（T-10）。
func NewSnapshotEnvelope[A any](
    aggregate A,
    seqNr SeqNr,
    opts ...EnvelopeOption,
) (SnapshotEnvelope[A], error)

func (s SnapshotEnvelope[A]) Aggregate() A
func (s SnapshotEnvelope[A]) SeqNr() SeqNr
func (s SnapshotEnvelope[A]) Manifest() string

// SnapshotRead は読み取りの結果。R-2・R-3。
type SnapshotRead[A any] struct {
    // Snapshot は nil のとき「ヘッドはあるがスナップショットがない」（R-3）。
    Snapshot *SnapshotEnvelope[A]
    // HeadSeqNr は読み取り時点のヘッドの seq_nr。スナップショットがなくても返す（R-3）。
    HeadSeqNr SeqNr
}
```

### 2.5 payload とシリアライザ（T-6・T-7・T-8）

ドメイン型にライブラリのインターフェイスを求めない（T-6）。型パラメーターの制約は `any` とする。

```go
// Serializer は payload だけを直列化する。T-7。
// 封筒のメタデータ（aid・seq_nr・occurred_at・manifest）は渡さない。
type Serializer[T any] interface {
    Serialize(value T) ([]byte, error)
    Deserialize(data []byte) (T, error)
}

// NewJSONSerializer は既定のシリアライザ。T-8。encoding/json を使う。
func NewJSONSerializer[T any]() Serializer[T]
```

- 直列化・復元の失敗は「直列化」分類のエラー（2.7）に包んで返す。元のエラーは `Unwrap()` で取得できる。
- 既定の JSON は、キー順と空白の差を許す。配列の順序・null・真偽値・文字列・数値の区別は保つ（CR）。

### 2.6 4つの操作（3章・W-3〜W-9・R-1〜R-8）

```go
type EventStore[E, A any] interface {
    // 3.1。追記するイベントは1件（H-3）。
    // 期待値の引数は取らない。照合の期待値は event.SeqNr() から決まる（W-3・W-4）。
    PersistEvent(ctx context.Context, event EventEnvelope[E]) error

    // 3.2。snapshot.SeqNr() != event.SeqNr() は W-9 の契約違反。
    PersistEventAndSnapshot(
        ctx context.Context,
        event EventEnvelope[E],
        snapshot SnapshotEnvelope[A],
    ) error

    // 3.4。R-1: ヘッドがなければ (nil, nil)。エラーにしない。
    // R-2・R-3: あれば SnapshotRead を返す。
    GetLatestSnapshotByID(ctx context.Context, id AggregateID) (*SnapshotRead[A], error)

    // 3.5。R-4: seqNr 以上を昇順。R-5: 全件を読み切る。R-6: 封筒を返す。
    GetEventsByIDSinceSeqNr(
        ctx context.Context,
        id AggregateID,
        seqNr SeqNr,
    ) ([]EventEnvelope[E], error)
}
```

書き込みの規則（W-3〜W-9）は次のとおり。

| 状況 | 結果 | 規則 |
|---|---|---|
| `seq_nr = 1`、ヘッドなし | 新規作成 | W-3 |
| `seq_nr = 1`、ヘッドあり | 楽観ロック | W-3 |
| `seq_nr = 0` | 契約違反 | W-6 |
| `seq_nr > 1` で `event.seq_nr == ヘッド + 1` | 更新 | W-4・W-8 |
| `seq_nr > 1` で `event.seq_nr <= ヘッド` | 楽観ロック | W-8 |
| `seq_nr >= ヘッド + 2`、またはヘッドなしで `seq_nr >= 2` | 契約違反（飛び番） | W-8 |
| 同じ `seq_nr` の2件目 | 楽観ロック | W-7 |
| `snapshot.seq_nr != event.seq_nr` | 契約違反 | W-9 |

- 照合はヘッドの seq_nr に対して行う。スナップショットは使わない（H-2）。
- 確定は1つの原子的な操作で、ヘッド・読み取り・変更フィードに同時に現れる（H-1）。
- R-8: メモリは原子的に読む（MEM-8）。DynamoDB は非原子的に読む（DY-9）。各実装の doc コメントに明記する。

**非同期の表現**: すべての操作は同期の関数で、`context.Context` を受け取る。呼び出し側が goroutine で呼ぶ。キャンセルと期限は Context で伝える。独自の Future や channel の API は設けない。言語に任せる点（共通契約 8章）に当たる。

### 2.7 エラー分類（4章・E-1〜E-3）

5分類を、型と判別用の値の両方で区別できるようにする（E-1）。

```go
// Kind はエラーの分類。E-1。
type Kind int

const (
    KindOptimisticLock Kind = iota + 1 // 楽観ロック
    KindContractViolation              // 契約違反
    KindSerialization                  // 直列化（直列化・復元の失敗）
    KindConfiguration                  // 設定
    KindStorage                        // 保存先
)

// Error は分類したエラーの共通の形。
type Error interface {
    error
    Kind() Kind
    Unwrap() error // 原因。nil のこともある。
}

// 具象型。errors.As で取り出せる。
type OptimisticLockError struct { /* aid 文字列・追記しようとした seq_nr・ヘッド seq_nr（分かれば） */ }
type ContractViolationError struct { /* 規則番号・関係する seq_nr */ }
type SerializationError struct{ /* 原因 */ }
type ConfigurationError struct{ /* 原因 */ }
type StorageError struct{ /* 原因 */ }

// 利用者は次のように分類を区別する。
//   var lock *eventstore.OptimisticLockError
//   if errors.As(err, &lock) { ... }
//   if kind, ok := eventstore.KindOf(err); ok && kind == eventstore.KindStorage { ... }
func KindOf(err error) (Kind, bool)
```

- 実行器は、型または `Kind` を5分類へ対応付ける。メッセージ文字列から分類を推測しない（CR）。
- 現行の `IOError` は「保存先」、`SerializationError` と `DeserializationError` は「直列化」へ統合する。

メッセージの規則は次のとおり。

| 分類 | メッセージに含める | 含めない | 規則 |
|---|---|---|---|
| 楽観ロック | aid 文字列、追記しようとした seq_nr、（分かれば）ヘッド seq_nr | 接続文字列・資格情報・SDK の生のエラー文 | E-2 |
| 契約違反 | 違反した規則番号（例: `W-9`）、関係する seq_nr（あれば。欠けたのが seq_nr のときや T-11 のように関係する seq_nr がなければ、規則番号だけ）。W-9 は異なるスナップショット番号も含める | ヘッド番号を必須にしない | E-3 |
| 契約違反（D-7 のサイズ超過） | 分類だけを検査。規則番号・メッセージの条件は足さない | — | E-3・CR |

- `OptimisticLockError` の `Error()` に原因のエラー文を連結しない。原因は `Unwrap()` だけで返す。SDK の生のエラー文が楽観ロックのメッセージに入らないようにするため（E-2）。この制約は楽観ロックだけに限る。ほかの分類は、Go の慣習どおり `Error()` に原因の文を `: ` でつないでよい。
- 保存先エラーには、読み取ったデータの欠損も含める（4章の分類）。設定エラーには、生成時の不正な値と、保存先に記録された設定との食い違いを含める（P-40）。

### 2.8 設定（S-1・MEM-3・DY-8）

```go
// RetentionCount は保持件数。S-1: 「なし」か1以上。0は設定エラー。
// ゼロ値 RetentionCount{} は無効な値とする。「なし」は NoRetention() で明示する。
type RetentionCount struct{ /* 非公開 */ }
func NoRetention() RetentionCount
func KeepLatest(n int) (RetentionCount, error) // n < 1 は ConfigurationError

type RetentionMode int
const (
    RetentionDelete RetentionMode = iota + 1 // 履歴を削除する
    RetentionTTL                             // TTL の印を付ける（DynamoDB だけ）
)

// 共通のオプション。
type Option func(*options) error
func WithRetentionCount(c RetentionCount) Option
func WithRetentionMode(m RetentionMode) Option
// WithTTLGraceSeconds は TTL の猶予を整数の秒で指定する。TTL 方式のときだけ意味を持つ。
// 負の値は ConfigurationError。指定しなければ 0。
// 適合データの ttl_grace_seconds が整数の秒（minimum: 0）で、仕様に既定値がないため、型を秒にそろえる。
func WithTTLGraceSeconds(seconds int64) Option
func WithRetentionFailureHandler(h func(ctx context.Context, err error)) Option // S-4。ログに追加する通知。ログの代わりではない（MEM-11）
```

DynamoDB 固有の設定は次のとおり。

```go
package dynamodb

type Config struct {
    JournalTableName  string
    SnapshotTableName string
    HeadTableName     string
    SnapshotHistoryIndexName string // 履歴の疎な GSI の名前
    // 設定照合（DY-8）の再要求回数の上限。初回は数えない。
    // nil なら既定の10回。0 なら再要求しない。負の値は ConfigurationError。
    ConfigurationReadRetryLimit *int
}

func New[E, A any](
    ctx context.Context,
    client *dynamodb.Client,
    cfg Config,
    eventSerializer eventstore.Serializer[E],
    snapshotSerializer eventstore.Serializer[A],
    opts ...eventstore.Option,
) (eventstore.EventStore[E, A], error)
```

- テーブルとインデックスの名前は設定で与える。ライブラリは3テーブルを作らない（`dynamodb.md` 3章）。
- 規範は上限値や既定値を定めない。設計として次のとおり決める。
  - 再要求回数の既定値は10回（`ConfigurationReadRetryLimit` が nil のとき）。ゼロ値の `Config` でも既定の回数だけ再要求するので、初回の `UnprocessedKeys` だけで保存先エラーにはならない（DY-8 の趣旨）。
  - 待ち時間は、最初が50ミリ秒で、毎回2倍、上限1秒（揺らぎなし）。
- 設定エラーになる値:
  - 保持件数 0（S-1）。`KeepLatest(0)` と、ゼロ値の `RetentionCount{}`。`memory.NewStore` と `dynamodb.New` も、受け取った保持件数を検査し直して 0 を `ConfigurationError` で拒否する。適合の `core-retention-zero` は、ストア生成時の `initialization.expect.error` で設定エラーを期待するので、オプションの構築（`KeepLatest` など）の失敗も、実行器が生成の失敗として扱う（5.2）。
  - メモリで TTL 方式を要求（MEM-3・MEM-12）。
  - メモリで変更フィードを要求（MEM-3・MEM-13）。
  - DynamoDB の設定照合で、3項目の一部だけの存在・store_id の不一致・layout_version の違い（DY-8・P-40）。
- 保持件数が「なし」のときは、`retention_mode` を無視し、エラーにしない。適合データの大半が `retention_count: null` と `retention_mode: delete` の組だからである。ただし、メモリで TTL 方式を指定した場合は、MEM-12 により保持件数にかかわらず設定エラーにする（設計の判断。適合データにメモリの TTL の組はない）。

メモリのコンストラクタは次の形とする。

```go
package memory

// Store は保存先。MEM-2: 明示的に共有するときだけ同じ Store を渡す。
type Store struct{ /* 非公開: 記録・設定・排他制御 */ }
func NewStore(opts ...eventstore.Option) (*Store, error) // MEM-3: 生成時に設定を検査

// New は Store を使うインスタンスを作る。同じ *Store を渡した場合だけ記録・設定・排他制御を共有する。
func New[E, A any](
    store *Store,
    eventSerializer eventstore.Serializer[E],
    snapshotSerializer eventstore.Serializer[A],
) eventstore.EventStore[E, A]
```

### 2.9 保持処理の失敗を知らせる経路（S-4・MEM-11）

保持処理の失敗は、書き込みの結果を変えない。通知用の公開 API は規範が固定しない。

- **メモリ（MEM-11・MEM-D7）**: ログで通知する。排他制御を解いた後に、標準の `log/slog` でログを出す。この通知は必須で、`WithRetentionFailureHandler` の指定の有無に左右されない。
- **DynamoDB（S-4）**: `log/slog` のログを必須とし、コールバックを追加の経路とする（9.6 で決定）。
- `WithRetentionFailureHandler` のコールバックは、ログに**追加**する通知経路である。ログの代わりにはならない。メモリでは、排他制御を解いた後に呼ぶ。
- ログ出力とコールバックの失敗や panic は、確定した書き込みの結果を変えない（MEM-11）。

### 2.10 現行の公開 API との対応表

| 現行（`pkg`） | 次のメジャー | 規則・備考 |
|---|---|---|
| `AggregateId`（`GetTypeName`・`GetValue`・`AsString`・`fmt.Stringer`） | `AggregateID`（`TypeName`・`Value`）。文字列はライブラリが `AidString` で組み立てる | T-1・T-11・T-12 |
| `Event` インターフェイス（`GetId`・`GetTypeName`・`IsCreated`・`GetOccurredAt` など） | 削除。`EventEnvelope[E]` に置き換える。`IsCreated` は廃止（`seq_nr == 1` で決まる） | T-2・T-6・W-3 |
| `Aggregate` インターフェイス（`GetVersion`・`WithVersion`） | 削除。`SnapshotEnvelope[A]`。version は廃止 | T-10・H-2 |
| `AggregateResult`（`Present`・`Empty`・`Aggregate`、空のとき panic） | `SnapshotRead[A]`（ヘッド番号つき）。panic しない | R-1〜R-3 |
| `EventStore.PersistEvent(event, version)` | `PersistEvent(ctx, event)`。version 引数を削除 | W-3・W-8 |
| `EventStore.PersistEventAndSnapshot(event, aggregate)` | `PersistEventAndSnapshot(ctx, event, snapshot)` | W-9 |
| `GetLatestSnapshotById(id)` | `GetLatestSnapshotByID(ctx, id)`（名前の `Id` を `ID` にそろえる。9.5） | R-1〜R-3 |
| `GetEventsByIdSinceSeqNr(id, uint64)` | `GetEventsByIDSinceSeqNr(ctx, id, SeqNr)` | R-4〜R-6 |
| `EventConverter`・`AggregateConverter` | 削除。`Serializer[T].Deserialize` に置き換える | T-6・T-7 |
| `EventSerializer`・`SnapshotSerializer`（`map[string]interface{}` 経由） | `Serializer[T]`。payload だけ | T-7 |
| `DefaultEventSerializer`・`DefaultSnapshotSerializer` | `NewJSONSerializer[T]` | T-8 |
| `EventStoreBaseError`・`OptimisticLockError`・`SerializationError`・`DeserializationError`・`IOError` | 5分類の型。`Unwrap` を持つ | E-1・E-2 |
| `NewOptimisticLockError` などのコンストラクタ | 非公開にする（利用者が作る必要はない） | — |
| `KeyResolver`・`DefaultKeyResolver`・シャード数 | 削除。PK は aid 文字列そのもの | DY-16 |
| `EventStoreOnDynamoDB`・`NewEventStoreOnDynamoDB` | `dynamodb.New` | DY-2〜DY-19 |
| `EventStoreOption`・`WithKeepSnapshot`・`WithDeleteTtl`・`WithKeepSnapshotCount`・`WithKeyResolver`・`WithEventSerializer`・`WithSnapshotSerializer` | `Config` と `Option`。保持件数・保持の方式・猶予秒・失敗通知 | S-1・S-4 |
| `WithKeepSnapshot(bool)` | 削除。保持件数「なし」が「履歴なし」 | S-1 |
| `EventStoreOnMemory`・`NewEventStoreOnMemory` | `memory.New` と `memory.Store` | MEM-1〜MEM-13 |
| `pkg/common`（試験用のテーブル作成・LocalStack クライアント） | 削除（旧 API を削除する PR）。試験の補助は `internal/` に新しく作る | IP 4.2 |
| モジュールのパス `.../event-store-adapter-go` | `.../event-store-adapter-go/v2` | IP 4.2 |

---

## 3. メモリの実装方針

メモリ実装は MEM-1〜MEM-13 を次のように満たす。

| 規則 | 方針 |
|---|---|
| MEM-1 | プロセス内のメモリだけに持つ。ファイルへ書かない。利用中の全消去の API は設けない |
| MEM-2 | `memory.NewStore` は毎回空の独立した `Store` を作る。パッケージ変数や名前で共有しない。同じ `*Store` を渡した `New` のインスタンスだけが、記録・設定・排他制御を共有する |
| MEM-3 | `NewStore` で設定を検査する。保持件数 0・TTL 方式・変更フィードの要求は `ConfigurationError`。設定は `Store` が不変で持つ |
| MEM-4 | `Store` に1つの `sync.RWMutex` を置く。範囲は保存先全体。複数の goroutine から呼べる。確定前の途中状態は、ロックの外から読めない |
| MEM-5 | 記録のキーは aid 文字列（T-1）の完全一致。`map[string]*record`。aid と seq_nr は別の値。ハッシュだけで識別しない。前方一致で選ばない。T-9・T-11・T-12・T-13 は入口で検査する |
| MEM-6 | メタデータと payload を別のフィールドで持つ。payload は `Serializer` で直列化したバイト列として保持し、取得時に復元して新しい値を返す。取得結果や入力の変更が保存値を変えない。T-6 を超える要件（複製のインターフェイスなど）は課さない |
| MEM-7 | 入力検査と payload の直列化は、ロックを取る前に行う。ロックの中で、ヘッドの読み取り・照合・変更の準備・まとめて公開・保持処理を行う。準備中の失敗は記録を変えない。イベントだけで `seq_nr = 1` の新規作成もできる |
| MEM-8 | `GetLatestSnapshotByID` は、読み取りロックの中で最新のスナップショット封筒とヘッド seq_nr を同時に確保する（原子的） |
| MEM-9 | `GetEventsByIDSinceSeqNr` は、読み取りロックの中でイベントを全件確保する。ロックを外してから復元する |
| MEM-10 | 保持件数がなければ、現在のスナップショットだけを持つ。n が指定されたら、新しい n 件を残し、古い順に取り除く。確定後に同じロックの中で行う。イベントだけの追記でも、取り残しを片付ける。ジャーナルとヘッドは取り除かない |
| MEM-11 | 保持の失敗は、書き込みの成功を変えない。ロックを解いた後に、必須のログ（2.9）で知らせる。コールバックは追加の通知とする。次の追記後の保持処理で再試行する |
| MEM-12 | 最初の版では TTL 方式を提供しない。要求は設定エラー |
| MEM-13 | 変更フィードを提供しない。要求は設定エラー |

### 3.1 内部のデータ構造

```go
type record struct {
    head      head                  // seq_nr と、確定したイベント封筒（1件）。H-4
    journal   []storedEvent         // seq_nr 昇順
    current   *storedSnapshot       // 現在のスナップショット
    history   []storedSnapshot      // 履歴。seq_nr 昇順
}
```

- 保存する封筒は、メタデータと直列化済みの payload のバイト列を持つ。ヘッドの seq_nr は `journal` の最後の要素から導出できるので、別の変数で並行管理しない。導出できない値（ヘッドが持つイベント封筒）だけを別に持つ。
- 取得結果は毎回新しく構築する。保存したバイト列をそのまま返さない。

### 3.2 書き込みの手順

1. 入口の検査（T-9・W-6・W-9・T-11〜T-13）。ロックの前。封筒の構築時の検査を同じ関数で呼び直し、ゼロ値の封筒を弾く（2.1）。W-9 はここだけで行う。
2. payload と、あればスナップショットの直列化。失敗は `SerializationError`。ロックの前。
3. ロックを取る。
4. ヘッドを読む。W-3・W-7・W-8 で照合する。違反は `OptimisticLockError` か `ContractViolationError`（飛び番）。
5. 変更を作業用の値に組み立てる。失敗しても記録は変わらない。
6. 記録へまとめて公開する（確定。H-1）。
7. 保持処理（MEM-10）。失敗は変数に保持する。
8. ロックを解く。
9. 失敗があれば通知する（MEM-11）。

---

## 4. DynamoDB の実装方針

### 4.1 使う SDK とその版

現行の `go.mod` に固定されている版を使う。

| モジュール | 版 |
|---|---|
| `github.com/aws/aws-sdk-go-v2` | v1.47.1 |
| `github.com/aws/aws-sdk-go-v2/service/dynamodb` | v1.70.0 |
| `github.com/aws/aws-sdk-go-v2/config` | v1.33.6 |
| `github.com/aws/aws-sdk-go-v2/credentials` | v1.20.6 |

- 版の更新は、この文書の範囲外。更新するときは別の判断とする。
- クライアントはコンストラクタが受け取る（`*dynamodb.Client`）。クライアントの作成（認証・リージョン・endpoint）は利用者が行う。

### 4.2 差し込みの仕組みの置き場所（IP 6）

- 試験用の障害の差し込みは、SDK の `APIOptions`（Smithy のミドルウェア）で行う。**本番のコードには入れない。** 試験の側が `dynamodb.Options.APIOptions` に関数を渡して、クライアントを作る。
- ミドルウェアは Initialize または Finalize の段階に置く。操作名（`TransactWriteItems`・`BatchWriteItem`・`UpdateItem`・`Query`・`BatchGetItem`）と、入力の内容から、段階（5章の phase）を判定する。保持処理の要求だけを失敗させる。
- 試験では、SDK の自動再試行を無効にする（`RetryMaxAttempts = 1`）。生成競合・再要求の回数を実行器が数えられるようにするため。
- `replace-request` は、次の handler を呼ばずに結果を返す（何も確定しない）。`replace-response` は、次の handler を呼んだ後に結果を差し替える（副作用は残る）。

### 4.3 3テーブルと設定項目（DY-2・DY-3・DY-8・DY-16〜DY-19・D-3・D-4）

| テーブル | キー | GSI | Streams | TTL |
|---|---|---|---|---|
| journal | PK `aid`(S)、SK `seq_nr`(N) | なし | 無効 | なし |
| snapshot | PK `aid`(S)、SK `skey`(N) | `(aid, active_history_seq_nr(N))`、射影 KEYS_ONLY | 無効 | 属性 `ttl`。TTL 方式のときだけ有効（DY-2） |
| head | PK `aid`(S)、SK なし | なし | 有効・NEW_IMAGE（DY-3・DY-12・D-4） | なし |

- 3テーブルの作成はライブラリの外にある。3テーブルは同じリージョンに置く（DY-18）。
- 現在・印付き履歴・設定項目は GSI に載らない（D-3）。
- journal の Streams は変更フィードの供給源にしない（4.1 の仕様）。

項目の属性（`dynamodb.md` 5章）は次のとおり。

| 項目 | 属性 |
|---|---|
| ジャーナル | `aid`(S)・`seq_nr`(N)・`occurred_at`(N、エポックナノ秒)・`manifest`(S)・`payload`(B) |
| スナップショット（現在） | `aid`・`skey`(N、0)・`seq_nr`(N)・`manifest`(S)・`payload`(B)・`last_updated_at`(N、occurred_at のミリ秒)。`active_history_seq_nr` も `ttl` も持たない |
| スナップショット（履歴） | 現在と同じ属性（`skey` は履歴の seq_nr）。印がない間だけ `active_history_seq_nr`(N)。TTL の印で `ttl`(N、エポック秒)。期限のない項目は `ttl` 属性を持たない |
| ヘッド | `aid`(S)・`type_name`(S)・`seq_nr`(N)・`events`(L)。要素は M（`seq_nr`・`occurred_at`・`manifest`・`payload`）で1件 |
| 設定項目 | `aid = "__config__"`。SK は journal が `seq_nr=0`、snapshot が `skey=0`、head は SK なし。属性は `store_id`(S)・`layout_version`(N、初版は1)だけ |

- `type_name` は aid の型名から得る。
- PK は aid 文字列そのもの（DY-16）。論理シャードやハッシュを使わない。journal の SK は seq_nr、snapshot の skey は現在が 0・履歴がその seq_nr（DY-17）。
- 集約の操作は、aid に絞った `GetItem`・`BatchGetItem`・`Query`・`TransactWriteItems`・`BatchWriteItem`・`UpdateItem` だけで行う。Scan は使わない（DY-19）。
- 現在の履歴の印（`active_history_seq_nr`）の有無で、GSI に載るかが決まる。

### 4.4 設定項目の照合（DY-8・P-19・P-40）

`New` は次の状態遷移で3テーブルの設定項目を確かめる。

1. 3つの設定項目を、1回の `BatchGetItem`（`ConsistentRead=true`）で読む。`Responses` を蓄積する。
2. `UnprocessedKeys` があれば、そのキーだけを指数バックオフで強整合のまま再要求する。未処理がなくなるまで「存在しない」と判定しない。上限に達したら、設定エラーではなく保存先エラーにする。
3. 3つともなければ、新しい `store_id` を作り、1回の `TransactWriteItems` で `attribute_not_exists(aid)` 条件つきの Put を3件行う。条件が成立しなければ、応答を捨てて、3件を強整合で読み直し、手順4へ進む（P-19）。取り消し理由が `TransactionConflict`（別の生成が書いている最中）のときも、同じく読み直す。読み直しても3つともなければ、作成を繰り返さずに保存先エラーを返す（DY-8、P-44）。適合データにこの場面はないので、単体試験で、読み直しで設定項目が見つかる場合と、3つともなくて保存先エラーになる場合を確かめる。この読み直しにも手順2（DY-8）を当てはめる。`UnprocessedKeys` は上限まで再要求し、未処理が残る間は判定しない。上限に達したら保存先エラーにする。
4. 3つともあり、`store_id` と `layout_version` が3件で一致し、自分の版（1）と同じなら、続行する。
5. それ以外（一部だけ・`store_id` の不一致・`layout_version` の違い）は設定エラー（P-40）。

設定項目は条件つき Put だけで作る。

権限は、試験用とライブラリ用に分ける。

- ライブラリ用: 3テーブル（と GSI）に対する `BatchGetItem`・`PutItem`（設定項目）・`TransactWriteItems`・`Query`・`BatchWriteItem`・`UpdateItem` だけ。テーブルの作成はしない。
- 試験用: 障害を差し込まない別のクライアントで、テーブルの作成と削除、`PutItem`（`seed.items` と、生成競合の `install_items`）、`GetItem`・`Query`・`DescribeTable`・`DescribeTimeToLive` を行う。
- DynamoDB Local は IAM を検査しないので、試験ではクライアントを分けて区別する。実サービス向けの IAM の例は、文書の PR（7.2）で書く。

### 4.5 書き込み（D-5・D-6・D-7・W-8）

1回の書き込みは1つの `TransactWriteItems` で行う。最大4アクション。

| アクション | 条件 |
|---|---|
| Put journal | `attribute_not_exists(aid)` |
| Put head（`seq_nr = 1`、新規作成） | `attribute_not_exists(aid)` |
| Update head（`seq_nr > 1`） | `seq_nr = :prev`。`:prev` は `event.seq_nr - 1`。`seq_nr` と `events` を上書きする |
| Put snapshot（現在。スナップショットがあるとき） | 条件なし |
| Put snapshot（履歴。保持件数を設定しているとき） | 条件なし |

- ヘッドの Put と Update は `ReturnValuesOnConditionCheckFailure=ALL_OLD` を指定する（D-5）。
- 失敗の分類は次のとおり。

| 失敗 | 分類 | 規則 |
|---|---|---|
| 新規作成でヘッドの条件が不成立 | 楽観ロック | W-3 |
| 更新でヘッドの条件が不成立 | 旧ヘッドの seq_nr と比べる。`event.seq_nr <= 旧ヘッド` は楽観ロック。`event.seq_nr >= 旧ヘッド + 2` は契約違反（飛び番）。旧項目が返らなければ旧ヘッド 0 とみなす | W-8・D-5 |
| ジャーナルの条件が不成立 | 楽観ロック | W-7 |
| `TransactionConflict` | 楽観ロック | D-6 |
| スロットリング・通信失敗・その他 | 保存先 | — |

取り消し理由が複数の項目に付いたときは、どれかの項目に `TransactionConflict` があれば楽観ロック（D-6）にする。なければ、ヘッドの条件不成立、ジャーナルの条件不成立、その他の順に見て、最初に当たったもので分類する（`dynamodb.md` 6.2、P-43）。適合データには理由が複数の項目に付く場面がないので、単体試験で確かめる（例: ヘッドの条件不成立が飛び番を示し、別の項目が `TransactionConflict` を返したときに楽観ロックになる）。

- 分類のための追加読み取りはしない（D-5）。`CancellationReasons` の `Item` の旧 seq_nr を使う。
- D-7: 項目サイズを書き込み前に見積もる。上限は 409600 バイト。payload はジャーナルとヘッドの両方に載る。見積もる項目と属性は次のとおり。
  - (a) ジャーナル: `aid`・`seq_nr`・`occurred_at`・`manifest`・`payload`。
  - (b) ヘッド: `aid`・`type_name`・`seq_nr`・`events`（L の中の M 1件。`seq_nr`・`occurred_at`・`manifest`・`payload`）。
  - (c) 現在のスナップショット: `aid`・`skey`・`seq_nr`・`manifest`・`payload`・`last_updated_at`。
  - (d) 履歴のスナップショット: (c) に `active_history_seq_nr` を加える（書き込み時は `ttl` を持たない）。
  - 計算: 属性名の UTF-8 バイト数と、値の大きさを足す。S は UTF-8 のバイト数、B はバイト数、N は多めに見積もって21バイト、L と M は3バイトに要素ごとの1バイトと中身を足す。
  - 送る項目のどれか1つでも409600バイトを超えたら、送らずに `ContractViolationError` を返す。
  - 適合データの4件との対応: `dynamodb-item-size-event` はジャーナルとヘッドの payload、`-snapshot` は現在（と履歴）の payload、`-manifest` はジャーナルとヘッドの manifest、`-head-overhead` はヘッドの `type_name` と aid と payload。
  - データは厳密な境界を求めない（`conformance/README.md`）ので、多めの見積もりで足りる。
- 楽観ロックのメッセージには、E-2 のとおり aid 文字列・seq_nr・（分かれば）ヘッド seq_nr だけを含める。

### 4.6 読み取り（DY-9・DY-10・DY-11・R-8）

- `GetLatestSnapshotByID`: head と snapshot の現在（`skey=0`）を1回の `BatchGetItem`（強整合）で読む。`UnprocessedKeys` は読み切るまで再要求する（DY-9）。再要求は、残ったキーだけを強整合のまま指数バックオフで行い、上限と待ち時間は設定照合（2.8）と同じ（`ConfigurationReadRetryLimit`。既定10回。初回は数えない）にする。上限に達したら、未処理のキーを「ない」と判定せず、`StorageError` を返す（設計の判断。DY-9 は上限を定めないが、上限がないと期限のない `context` で呼び出しが戻らないおそれがある）。ヘッドがなければ「なし」。あれば封筒（なければなし）とヘッド seq_nr の組（DY-10）。
- 2項目の読み取りは原子的でない（R-8）。`TransactGetItems` は使わない（P-25）。この性質を doc コメントに書く。
- `GetEventsByIDSinceSeqNr`: journal を `aid = :aid AND seq_nr >= :seq_nr`、`ConsistentRead=true` で `Query` する。昇順。`LastEvaluatedKey` が返る間は読み切る（DY-11・R-5）。

### 4.7 保持処理（8章・D-9・P-18・P-24・S-3）

履歴を書いた書き込みの確定後だけ、保持処理を行う（D-9）。書き込みの呼び出しの中で、確定の応答の後、戻る前に同期で行う。失敗しても書き込みの結果は変えない。保持件数を n とする。

1. 履歴 GSI を `aid = :aid`、`ScanIndexForward=false` で `Query` し、読み切る（KEYS_ONLY）。結果整合の読み取り（DY-18）。
2. 今書いた履歴を加える。すでに見えていれば重ねない。降順の先頭 n 件を残し、それより古いものを対象にする（S-2）。
3. 削除方式は、`BatchWriteItem` を25件ずつ送る（P-18）。`UnprocessedItems` は、残った項目だけを指数バックオフで再送する。再送の上限と待ち時間は、設定照合（2.8）と同じく `ConfigurationReadRetryLimit`（既定10回。初回は数えない）と、最初50ミリ秒・毎回2倍・上限1秒とする。上限に達したら、残りを削除せずに保持の失敗として手順5で知らせ、書き込みの結果は変えない。残った履歴は、次の保持処理で再び対象になる（設計の判断。仕様は再送することだけを定め、上限を定めない。保持を書き込みの呼び出しの中で行うので、上限がないと呼び出しが戻らないおそれがある）。
4. TTL 方式は、1件ずつ `UpdateItem`: `SET #ttl = :expires REMOVE active_history_seq_nr`、条件 `attribute_exists(active_history_seq_nr)`。`#ttl` は `ExpressionAttributeNames`。`:expires` は印付け時点のエポック秒 + `WithTTLGraceSeconds` の猶予秒（指定しなければ 0）。猶予秒に上限はないので、`int64` の加算はあふれうる。エポック秒と猶予秒はどちらも 0 以上の `int64` なので、和を `uint64` で計算し、`strconv.FormatUint` の 10 進文字列で N 属性に書く。単体試験で、猶予秒が `math.MaxInt64` の境界を確かめる。条件が失敗したら、印付け済みとして読み飛ばす。
5. 失敗は書き込みの結果を変えない。S-4 の通知経路で知らせる。

- 件数を数えてから超過分を選ぶ方式は使わない（P-24）。現行の `getSnapshotCount` に当たる処理は廃止する。
- 印付き履歴は件数に数えない。期限は先送りしない（S-3）。印の条件（`attribute_exists(active_history_seq_nr)`）が、これを保つ。

### 4.8 変更フィード（1.3・DY-12・DY-13・DY-15）

- head の Streams を NEW_IMAGE で有効にする配置を維持する（DY-3・DY-12）。利用者は Streams のレコードを自分で読める。
- ヘッド遷移を組み立てる関数と、再同期（DY-15）の補助は、最初のメジャーに含めない（2026-10-06 オーナー決定、IP 10）。公開 API に変更フィードの関数はない。

---

## 5. 適合テストデータの実行器

実行器は Go の試験として書く。配置は `conformance/` を読み取り専用で使う。置き場は `internal/conformance` とする。

### 5.1 データの読み方

- JSON は `json.Decoder` に `UseNumber()` を設定して読む。seq_nr は `math/big.Int` で保持し、`SeqNr` や `uint64` へ直接デコードしない。-1 や 2^53 を扱うため。
- `epoch_nanoseconds` は10進文字列を `big.Int` で扱う。浮動小数点を介さない。
- 重複キー・`NaN`・`Infinity` を拒否する。文字列の値と属性名を書き換えない。
- 各 JSON の `format` と `version` を確認する。`manifest.json` と照合する。
- **時刻の精度**:
  - `precision_policy = native-time-type` の成功ケースでは、入力を `time.Time` へ変換した値を期待値とする。`expect.value` は丸め前の参照値。変換した値と実際の値を報告する。
  - `representation.time_precision` がある場合、Go の `time.Time` はナノ秒型なので `nanoseconds` のケースを実行する。`milliseconds` は対象外とし、理由を記録する。
  - 印のないケースは全部実行する。
  - `representation.signed_seq_nr = true` は、`SeqNr`（符号付き）で表せるので実行する。`SeqNr` は符号付き（9.3）なので「表現不能」にはならない。ほかに型で表せないケースがあれば、「表現不能」と理由を報告する（5.6）。
- **値の表の操作**（公開 API の同名関数は要求されない。実行器が対応付ける）:

| 操作 | 対応付け |
|---|---|
| `buildAid` | `user_string` を返す試験用の `AggregateID` を用意し、`AidString` が型名と値から組み立てることを確かめる（T-1・T-11・T-12） |
| `validateSeqNr`（`context=value`） | `SeqNr.Validate`（T-9。0 は有効） |
| `validateSeqNr`（`context=event`） | `NewEventEnvelope` を呼ぶ。0 は W-6 の契約違反。実行器で同じ式を計算しない |
| `validateOccurredAt` | 型名 `ConformanceTime`・値がケース ID の集約を使う。1番から `event_seq_nr - 1` 番までを時刻 `1970-01-01T00:00:00.123000000Z` で `PersistEvent` する。次に入力時刻の `event_seq_nr` 番を書く。payload は空オブジェクト、manifest は空文字列。成功ケースは1番を読み戻して比較する。範囲外ケースは7番で契約違反を比較する（T-13） |
| `fnv1a64` | 最初のメジャーにはハッシュを使う保存先がない（メモリ・DynamoDB とも aid 文字列そのものをキーにする）。適合データの FNV-1a 64 の4件（K-1）は、理由を付けて「対象外」と報告し、成功にも失敗にも数えない。ハッシュを使う保存先を出す段階 5 で実行する（実装計画 5章の受け入れ条件1。2026-10-06 オーナー決定） |

- **payload と集約状態の比較**: 既定の JSON シリアライザで直列化・復元した JSON 値を比較する。キー順と空白は無視する。配列の順序・null・真偽値・文字列・数値は保つ。真偽値と数値を同一視しない。Unicode の正規化はしない。
- **generators**: `target`（ケースを根とする JSON Pointer）・`character`（Unicode 1文字）・`byte_length`（UTF-8 の総バイト数）。fixtures 内の空文字列が対象。`~0` と `~1` を復号する。Schema 検査は展開前、操作は展開後の値で行う。400KB は 409600 バイト、1MB は 1048576 バイト。

### 5.2 場面の実行手順と、ストアを場面ごとに分ける方法

1. `backends` に、実行する保存先（`memory` か `dynamodb`）があるか確認する。なければ「対象外」と報告する。`requires=["ttl"]` は TTL 方式を要求する。v1 の TTL 場面は DynamoDB だけ（MEM-12）。
2. 各場面を独立したストアで実行する。メモリは毎回 `memory.NewStore` で空の `Store` を作る。DynamoDB は、場面ごとに一意なテーブル名と GSI 名を実行器が割り当てて、3テーブルを同じリージョンに作る。他のケースの項目を使い回さない。
3. `seed.items` があれば、ストア生成前に、試験用の権限（障害を差し込まない別のクライアント。4.4）で入れる。ライブラリ用のクライアントでは入れない。
4. `store` の設定でストアを生成する。`retention_count=null` は `NoRetention()`（履歴なし）。`retention_mode` は delete か ttl。`ttl_grace_seconds` は `WithTTLGraceSeconds` の猶予秒。共通名を `Config`・`Option` へ対応付ける。生成前の障害を先に登録する。オプションの構築（`KeepLatest(0)` など）の失敗も、生成の失敗として扱い、手順5で比べる。
5. `initialization` があれば、生成結果を検査する。`initialization.expect.error` は、オプションの構築の失敗と、`New`・`NewStore` の失敗の、どちらでも観測する。生成失敗のケースは操作列がない（例: `core-retention-zero`）。
6. generators を展開し、`fixtures.events`・`fixtures.snapshots` を、各操作の直前に封筒として構築する。無効な入力のために封筒の構築が失敗したら、その操作の失敗として捕捉する。実行器の事前検査で、ライブラリの検査を代替しない。
7. `steps` を配列順に、並行実行せずに実行する。
8. 各操作の `expect` と `observe` を検査する。`expect` は `success`・`none`・`snapshot`（`head_seq_nr` と封筒の組）・`events`（順序込み）・`error`。保持の失敗や遅延がある場面は、保持・検査フックの完了後に観測する。フックが書き込みの成功・失敗を変えてはならない。
9. `error` は、`Kind` を5分類へ対応付けて比べる。`error.rule`・`must_contain`・`must_not_contain` はメッセージで検査する。分類をメッセージから推測しない。
10. `retry_limit` は設定照合の再要求回数の上限（初回は数えない）。同じ未処理応答の列で、上限到達を再現する。指数バックオフは、時計フックで実時間を短縮する。

### 5.3 フックの置き場所

| フック | 置き場所 | 内容 |
|---|---|---|
| 保持の決定的実行（delete / ttl） | トリガーは置かない。保持処理は書き込みの呼び出しの中で、確定の後に同期で行う（メモリは同じロックの中。MEM-10。DynamoDB は確定の応答の後で戻る前。D-9）。失敗しても書き込みの結果は変えない。**公開 API は増やさない** | 操作が返った後に観測する |
| 内部履歴（`observe.history`） | DynamoDB: snapshot テーブルを Query して `active`（印なし）と `marked`（`ttl` あり）に分ける。メモリ: `Store` の内部の論理履歴を `internal/testhook` 経由で読む | その集約の履歴だけ。`active` と `marked` を完全一致。`absent` は存在してはならない履歴。現在のスナップショットと設定項目は数えない |
| メモリの保持の失敗（再試行と通知） | 論理履歴のフック（候補選択と削除）が `storage-error` を返す。通知は `WithRetentionFailureHandler` の捕捉関数と、差し替えた `slog` のハンドラーで捕捉する。再試行は、次の追記の後に `observe.history` で、取り残しが片付いたことを確かめる | MEM-10・MEM-11。書き込みの結果は変えない |
| 失敗通知（`observe.notifications`） | `WithRetentionFailureHandler` に試験用の捕捉関数を渡す。メモリの必須のログは、`slog` のハンドラー差し替えでも捕捉できる | 分類 `retention-failure`。空配列は失敗通知なし。同じ最終失敗のログが複数あれば1つに正規化してよい |
| SDK 要求（`observe.requests`） | `APIOptions` のミドルウェアで、送信前の入力を記録する | 式と属性名・値の束縛を解析した構造で比較する（空白・節の順序・AND の順序は比較しない）。`update.set`・`update.remove`・`condition`、`key_condition.all`（`eq`/`gte` と `aggregate_id`・`seq_nr` への束縛）、TTL の `#ttl` と `expression_attribute_names`、`expires`（エポック秒）、`initial_batch_sizes`（再送を除く削除バッチ件数）、`no_requests_in_phases`・`request_count`・`minimum_request_count`。`requests` の要素は、実際の別々の要求に配列順で対応付ける。ページ送り・未処理キーの再要求・削除バッチ分割は、段階の要求列全体で検査する |
| 属性（`observe.items`・`seed.items`） | 実行器が `GetItem` で物理項目を読む | `table` は journal / snapshot / head の設定済みテーブル名。`attributes` は S/N/B/L/M で、属性集合を完全一致。N は10進文字列を整数として比較。L の中の M は `nested_attributes`。`binary_json` は B の復元結果を JSON で比較。`bindings` の `generated-store-id` は、最初の実際の `store_id` を束縛し、3項目で同じ値であることを検査する。属性集合・型・リスト件数の検査は、除外前の実際の項目全体で行う |
| 時計（`clock.epoch_seconds`・操作の `clock_epoch_seconds`） | 内部の時計の差し替え（保持処理に渡す `func() time.Time`）と、待ち時間の差し替え。公開 API では `WithClock` のような引数を増やさず、`internal/testhook` から注入する | 期限は「印付け時刻 + `ttl_grace_seconds`」。v1 は 2100年の時計 |
| 配置照合（`dynamodb/layout.json`） | 実際の3テーブルを `DescribeTable` と `DescribeTimeToLive` で照合する | テーブル名と GSI 名は、設定値へ束縛する |

- 試験用のフック（時計・待ち時間、メモリの論理履歴の障害と観測）の型と登録関数は、公開パッケージに依存しない `internal/testhook` に置く。型は aid の文字列・seq_nr・バイト列だけを使い、中核の型に依存しない。
- `memory` と `dynamodb` は `internal/testhook` を import して、非公開のフィールドで受ける。実行器（`internal/conformance`）は `internal/testhook` を通してストアに登録する。依存の向きは「公開パッケージ → `internal/testhook`」と「実行器 → 公開パッケージと `internal/testhook`」で、循環しない。公開 API は増やさない。

### 5.4 障害の差し込み

`faults` の `operation` は、0 がストア生成、1以上が1始まりの操作番号。差し込めない場合は「未検証」と報告し、成功に集計しない。

**発火の数え方**: 登録した障害ごとに、実行器が発火の数を持つ。フックやミドルウェアが障害を適用するたびに1増やす。場面の終わりに、`repeat` が `count` の場合は指定した回数ちょうど、`until-operation-finishes` の場合は1回以上であることを確かめる。合わなければ、どの障害かを示して場面を失敗にする。

`phase` は、`conformance/schema/common.schema.json` の列挙どおり12種類。

| phase | DynamoDB の差し込み | メモリの差し込み |
|---|---|---|
| `serialize-event` | イベント用のシリアライザの包み（試験用の `Serializer` 実装）が失敗を返す | 同じ |
| `serialize-snapshot` | スナップショット用のシリアライザの包みが失敗を返す | 同じ |
| `deserialize-event` | 復元時の包みが失敗を返す | 同じ（取得時の復元） |
| `deserialize-snapshot` | 同じ | 同じ |
| `commit` | `TransactWriteItems` の入力を `APIOptions` で捕らえ、`replace-request`（何も確定しない）か `replace-response`（確定済みで応答を差し替える）。`TransactionCanceledException` は `replace-request` | 確定前（まとめて公開する前）に、論理履歴のフックが失敗を返す。ヘッド・ジャーナル・スナップショットに変更を残さない（MEM-7） |
| `read-events` | journal の `Query` を差し替える | ロックの中のイベント取得のフックが失敗を返す |
| `read-snapshot` | `BatchGetItem` を差し替える。`sdk-response` は `unprocessed_keys` を返す。`read-interleave` は 5.5（DynamoDB だけ） | ロックの中の「ヘッドとスナップショットの組」の取得のフックが失敗を返す |
| `retention-query` | 履歴 GSI の `Query` を差し替える。`history_pages`・`omit_just_written_history` は応答計画に従う | 論理履歴の候補選択のフックへ対応付ける（`conformance/README.md`）。`storage-error` は失敗を返す。`sdk-response` の `history_pages` は、保持処理が読んだ印のない履歴の seq_nr の列（降順）のページ列として、そのまま返す。今書いた履歴を自動で足さない。今書いた履歴を足すことと重複を除くことは、ライブラリが DynamoDB と同じ手順で行う。`omit_just_written_history=true` は、実行器がページ列に今書いた番号がないことを確かめる（メモリの実データには出てこない）。削除の物理的な結果は、実際の論理履歴に反映して検査する |
| `retention-delete` | `BatchWriteItem` を差し替える。`unprocessed_first_n` は先頭 n 件を `UnprocessedItems` にして、残りは実処理する | 論理履歴の削除前のフックが失敗を返す |
| `retention-mark` | `UpdateItem` を差し替える | メモリの場面にこの段階はない（実データで確認。DynamoDB だけ。MEM-12） |
| `configuration-read` | 設定照合の `BatchGetItem` を差し替える。`unprocessed_keys` を含む | メモリの場面にこの段階はない（実データで確認。DynamoDB だけ） |
| `configuration-create` | 設定作成の `TransactWriteItems` を差し替える。`install_items` がある生成競合は、実行器が別に先に書いた項目を確定させる | メモリの場面にこの段階はない（実データで確認。DynamoDB だけ） |

メモリの場面にある障害は、`commit`・`read-*`（`read-events`・`read-snapshot`）・`serialize-*`・`deserialize-*`・`retention-query`・`retention-delete` だけである。`read-interleave`・`configuration-read`・`configuration-create`・`retention-mark` は DynamoDB だけである。

- `kind` と注入の方式:
  - `serialization-error`: シリアライザの該当する段階を失敗させる。
  - `storage-error`: 保存先・保持フックの最終失敗。`details.scope=final-retention-failure` は、候補選択後・削除前に保持全体を失敗させる。
  - `sdk-error`: SDK の自動再試行を無効にして、`details.code` と `cancellation_reasons` から SDK の例外を組み立てて返す。
  - `sdk-response`: 応答計画（`responses`・`unprocessed_keys`・`history_pages`・`omit_just_written_history`・`unprocessed_first_n`）に従う。`history_pages` は GSI 応答の履歴 seq_nr のページ列をそのまま返し、今書いた履歴を自動で足さない。ページごとに `LastEvaluatedKey` と `ExclusiveStartKey` を対応付ける。削除・TTL 更新・書き込みの物理結果は実際に反映して検査する。
  - `read-interleave`: 5.5。
- `repeat` は `{"mode":"count","count":1}` か `{"mode":"until-operation-finishes"}`。
- v1 の書き込み失敗と保持失敗はすべて `replace-request`。
- **`cancellation_reasons`**: トランザクションの各項目に1要素。書き込みは journal・head・current-snapshot・history-snapshot の順（存在するアクションだけ）。設定作成は `configuration:journal`・`configuration:snapshot`・`configuration:head`。失敗していない項目の `code` は文字列 `None`。実行器は、対象名を実際の要求内のアクションに照合して並べ直す。この順をライブラリの要求順として強制しない。ヘッドの `ConditionalCheckFailed` は `old_head_seq_nr` を持つ（`null` は旧項目が返らない）。D-5 の分類はこの旧項目を使う。ジャーナルの条件不成立は W-7、`TransactionConflict` は D-6 の楽観ロック、ヘッド以外のスロットリングは保存先エラー。

### 5.5 `read-interleave`（DY-9 の非原子的応答）

実時間の競争は使わない。決定的に再現する。

1. `BatchGetItem` を送る直前のミドルウェアで、旧ヘッドを捕捉する。
2. `interleaved_operation` の追記を、別のクライアント（障害を差し込まない）で確定する。
3. 元の `BatchGetItem` を送る。
4. 応答の中のヘッドを、手順1で捕捉した旧ヘッドへ差し替える。
5. 呼び出し側が受け取る `SnapshotRead` を `expect` と比べる。

`read-interleave` は、実データでは DynamoDB の場面（`dynamodb/read.json`）だけにある。メモリでは実行しない（`backends` にない）。メモリは MEM-8 で原子的に読むので、この状態が起きない（R-8）。

### 5.6 報告の形と CI での実行

- 報告の状態は「成功・失敗・対象外・未検証・表現不能」の5つ。
- CI へ次を出す: データの版と `manifest` の照合結果、言語・実装版・保存先、ケース ID と規則番号ごとの上の5状態、失敗した操作番号と期待値・実際の値。1ケースが複数の規則を持つときは、全規則へ対応付ける。途中で期待と異なったら、成功と報告しない（IP 5）。
- 表現能力の違いによる選択・任意の能力・削除済み規則・呼び出し側の推奨は、理由を記録する。条件を満たさないケース・必須ケースを飛ばした結果・障害を差し込めなかった結果を、成功に集計しない。
- 集計の正本は JSON ファイル1つ（`conformance-report.json`。CI の成果物として保存する）とする。中身は、データの版、`manifest` の照合結果、言語・実装版・保存先、ケースごとの `id`・`rules`・`status`・理由・失敗した操作番号・期待値・実際の値、規則番号ごとの状態別の件数。`go test -json` の出力はログとして残すだけにする。
- 配布の検証: `conformance/` を同一内容で写し、`.gitattributes` で改行変換を止める。CI は `python3 tools/conformance/manifest.py verify` と、`manifest.json` の SHA-256 の照合を行う（現行の `.github/workflows/ci.yml` の `lint` ジョブに既にある）。manifest を作り直して差分を隠さない。
- 適合の実行は、既存の `test` ジョブとは別のジョブ `conformance` として足す（DynamoDB Local のコンテナが要る）。`ci-success` の `needs` に加える。必須の範囲の決め方は7.4。

---

## 6. 試験環境

### 6.1 DynamoDB Local 3.3.1

イメージを digest で固定する（IP 4.1。2026-10-05）。

```text
amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab
```

起動の例（この文書では実行しない）:

```sh
docker run --rm -p 8000:8000 \
  amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab \
  -jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry
```

- リージョンは `us-east-1`、資格情報はダミーにする。DynamoDB 用と Streams 用の両方のクライアントに endpoint を明示する。根拠は、ハブの `tools/spikes/dynamodb-emulators/` の記録（`README.md`・`probe.py`）。
- Go の試験からは、`testcontainers-go` の汎用コンテナで同じ digest を使う案。現行の `go.mod` にすでに `testcontainers-go v0.44.0` がある。LocalStack のモジュールは不要になるので、移行後に外す。
- SDK のクライアントには、`BaseEndpoint` を明示する。Streams のクライアントにも endpoint を明示する。
- Streams の ARN のリージョンは `ddblocal` になる。ARN からリージョンを推測しない。

### 6.2 今の試験（LocalStack）からの移し方

現行の試験は `localstack/localstack:2.1.0` を使う（`test/event_store_on_dynamodb_test.go`・`test/event_store_on_dynamodb_regression_test.go`・`test/user_account_repository_test.go`）。テーブル作成の補助は `pkg/common/dynamodb.go`（`CreateJournalTable`・`CreateSnapshotTable`・`CreateDynamoDBClient`）にある。

1. `pkg/common` と LocalStack の旧い試験は、旧 API を削除する PR（7.2 の13番）まで、そのまま動かす。
2. 新しい試験基盤（DynamoDB Local の起動と、3テーブル・GSI・Streams・TTL の作成の補助）は、`internal/` に新しく作る（7.2 の4番）。
3. 現行の回帰試験（全件読み取り・新しい履歴の保持）の意図は、新しい API の書き込み・読み取り・保持の PR（7.2 の10・11番）で引き継ぐ。
4. 旧 API の削除の PR（7.2 の13番）で、旧い試験・`pkg/common`・LocalStack のモジュールを外す（IP 4.2）。

### 6.3 メモリ固有の試験

適合データに加えて、メモリ実装に次の試験を足す（7.2 の8番）。

- MEM-2: 別々の `NewStore` は独立している。同じ `*Store` を渡した `New` だけが記録・設定・排他制御を共有する。
- MEM-4: 同じ seq_nr を複数の goroutine から並行に追記すると、成功は1件だけで、ほかは楽観ロックになる。
- MEM-4: 読み取りと追記が重なっても、確定前の途中の状態を読まない。
- MEM-6: 入力の値と取得結果を変更しても、保存した値が変わらない。
- CI で、メモリのパッケージと適合の実行器を `go test -race` で動かす。現行の `make test` は `-race` を付けていないので、`conformance` ジョブ（5.6）か、メモリ用の別の手順で付ける。

---

## 7. main への入れ方

### 7.1 方針

- 各 PR を main へ squash マージする（IP 3）。次のメジャーの開発版は、モジュールのパスを `/v2` にした main を、疑似バージョンで取得する形で提供する（1.4）。
- worker は PR の作成までを行う。マージはコーディネーターが行う（IP 9）。
- 1つの PR が、1つの規則群に対応する（IP 4.2）。
- 最初の PR は、次のメジャー版の下準備（モジュールのパスの変更）とする。

### 7.2 PR の列

| 順 | 範囲 | 対応する規則群 | 備考 |
|---|---|---|---|
| 1 | 下準備。`go.mod` のパスを `/v2` に変え、全 import を追従する。`version` は変えない（1.4） | IP 3・IP 4.2 | 既存の実装は動かしたまま。CI は現行のまま通る |
| 2 | 実行器1: データの読み込み、`manifest` の照合、値の表、報告、必須の一覧ファイル、CI のジョブ `conformance` | CR・IP 5 | 接続していない保存先は「未検証」。成功と報告しない。7.4 の仕組みで CI は落ちない |
| 3 | 実行器2: 場面の実行器、障害とフックの枠組み（`internal/testhook`）、発火の数え方 | CR | フックは型と登録の口だけを定義する。型は aid の文字列・seq_nr・バイト列だけを使い、中核の型に依存しない |
| 4 | DynamoDB Local の基盤と観測（コンテナ、3テーブルの作成、SDK 要求の記録、項目の読み取り） | IP 4.1 | `internal/` に新しく作る。旧い試験と `pkg/common` はそのまま動かす（6.2） |
| 5 | 中核1: エラーの分類と値の型の検査（`SeqNr`・`AggregateID`・`Kind`） | T-1・T-9・T-11〜T-13・E-1〜E-3 | 旧実装と別のパッケージで併存する |
| 6 | 中核2: 封筒とシリアライザ | T-2〜T-8・T-10 | |
| 7 | 中核3: 操作のインターフェイス・`SnapshotRead`・`Option` | W・R・S-1 | |
| 8 | メモリと共通の場面の接続、`go test -race`、メモリ固有の試験（6.3） | MEM-1〜MEM-13 | 対象ケースを必須の一覧に加える。フックを呼ぶ場所（論理履歴の候補選択・削除・確定前・読み取り）を実装する。依存の向きは「メモリ → `internal/testhook` ← 実行器」 |
| 9 | DynamoDB: 3テーブル・配置の照合（`layout.json`）・設定照合 | DY-2・DY-3・DY-8・DY-16〜DY-19・D-1〜D-4 | `layout.json` と `item-shapes.json` に一致 |
| 10 | DynamoDB: 書き込み・読み取り | D-5〜D-7・DY-9〜DY-11 | 障害・要求・属性の検査を足す。旧い回帰試験の意図を引き継ぐ（6.2） |
| 11 | DynamoDB: 保持処理 | S-1〜S-4・D-9・P-18・P-24 | TTL の場面を含める。障害と要求の検査を必須にする |
| 12 | 文書（README・DATABASE_SCHEMA・MIGRATION_GUIDE・移行の案内）と利用例の移行 | IP 5-5・IP 5-6・IP-D8 | 実サービス向けの IAM の例もここで書く（4.4） |
| 13 | 旧 API の削除（旧い試験・`pkg/common`・LocalStack のモジュールを含む） | — | 旧 API を使う箇所を、すべて移行してから削除する |

- 変更フィードの補助は、最初のメジャーに含めない（1.3）ので、PR の列に入れない。
- 配置の照合（`layout.json`）は、9番の PR に入れる。

### 7.3 旧 API との共存と削除の時点

- 1番から12番の間は、旧 API（`pkg`）と新 API が一時的に併存する。併存は、main の CI を通し続けるための移行作業用で、利用者への互換の約束ではない。
- 新 API は、旧 `pkg` のパッケージとは別のパッケージに置く。モジュールのルートの `eventstore` と、旧 `/v2/pkg` は別のパッケージなので、旧と新で同じ名前の型を衝突させない（9.1）。
- 旧 API の削除は13番。旧 API の利用箇所（`test/`・利用例）をすべて移行してから消す。正式な `v2.0.0` に、旧形式の fallback・alias・変換は残さない。
- 正式リリースは、受入条件（IP 5）の達成と、オーナーの承認の後に行う。`release.yml` は手動起動のみで、この制御を維持する。

### 7.4 各 PR で main の CI を通し続ける方法

- 既存の `lint`（manifest 照合と `make vet`）・`test`（`make test`）・`release-checks` を維持する。
- 必須の一覧ファイル `internal/conformance/required.json` を置く。保存先ごとにケース ID を列挙し、2番の PR では空にする。全ケースが「未検証」でも、一覧が空なので CI は落ちない。
- CI のジョブ `conformance` を `ci-success` の `needs` に加える（2番の PR）。ジョブが失敗するのは、次の場合だけにする。
  - データか `manifest` が合わない。
  - 一覧にあるケースが「失敗」か「未検証」になる、または登録した障害が発火しない（5.4）。
  - 実行器自体のエラー。
- 一覧にないケースは報告するだけで、CI を落とさない。以後の PR は、接続した規則群のケースを一覧に足す。一覧から外すときは、コーディネーターのレビューを経る。
- Docker が要る試験は、`test` と別のジョブ `conformance` に分ける（所要時間の上限は現行の `test` が10分）。

---

## 8. 移行の案内

現行メジャー（v1.x）の利用者向けの案内を、12番の PR で `docs/MIGRATION_GUIDE.md`・`docs/MIGRATION_GUIDE.ja.md` に書く。この文書では内容を決める。

### 8.1 コードの移行

1. import のパスを `github.com/j5ik2o/event-store-adapter-go/v2` へ変える。
2. 呼び出しに `context.Context` を渡す。
3. 集約 ID を `AggregateID`（型名と値）で作る。文字列表現はライブラリが組み立てる。
4. イベントとスナップショットを、封筒（`EventEnvelope`・`SnapshotEnvelope`）で渡す。メタデータと payload は分ける。
5. `version` を使った照合をやめる。照合は `event.SeqNr()` から決まる。`PersistEvent` の version 引数と `IsCreated` は廃止。
6. `seq_nr` を、1から始まる連番として採番する。0 は契約違反（W-6）。
7. 復元は、`GetLatestSnapshotByID` が返すスナップショットがあれば `Snapshot.SeqNr() + 1` から、なければ 1 から `GetEventsByIDSinceSeqNr` を呼んで行う（R-3・R-4）。ヘッド seq_nr は復元の開始位置に使わない。復元後の到達確認に使える。
8. エラーは `errors.As` か `KindOf` で5分類を区別する。panic に依存した処理を外す。
9. 設定を `Config` と `Option` へ移す。保持件数 0 は設定エラー。`WithKeepSnapshot(false)` は、保持件数「なし」に当たる。
10. `KeyResolver` とシャード数を外す。

### 8.2 旧データの扱い（IP-D8）

- Go の旧配置（2テーブル・シャードつきの PK・snapshot の version）に対して、**移行ツールは提供しない。手順書だけを用意する。**
- 手順書の流れ（案）:
  1. 旧 API でイベントとスナップショットを読む。
  2. 利用者が、メタデータと payload を分けて、新しい封筒を作る。
  3. 新しい配置（3テーブルと設定項目）を作り、新 API で seq_nr 1 から順に書き直す。
- 旧 `occurred_at` の単位は、型で決まっていない（現行の `GetOccurredAt() uint64`）。利用者が定義を確認して、`time.Time` へ変換する。単位や失われた精度を、文書が推測しない。
- rs v3 の DynamoDB の移行ツール（ハブの `dynamodb.md` 11章）を、Go にそのまま適用しない。

---

## 9. 判断が要る点

この章の項目は、2026-10-06 に決めた。決めた人と理由を残す。選択肢の表は、理由の補足として残す。

### 9.1 パッケージ名と構成（`pkg` と `/v2`）

**決定**: B。モジュールのルート（`eventstore`）に封筒・エラー・操作の型を置き、保存先ごとに `memory` と `dynamodb` のパッケージに分ける。試験の補助は `internal/` に置く。
**決めた人**: オーナー（2026-10-06）。
**理由**: 依存方向が明確（保存先 → 中核）で、メモリだけを使う利用者に AWS SDK の import が増えない。名前が内容を表す。旧 `pkg` とは別のパッケージになるので、併存期間（7.3）に型名が衝突しない。

| 選択肢 | 内容 | 利点 | 欠点 |
|---|---|---|---|
| A. `pkg` を続ける | `github.com/j5ik2o/event-store-adapter-go/v2/pkg` | import の変更が `/v2` だけで済む | `pkg` は内容を表さない名前。メモリと DynamoDB が同じパッケージに混ざり、AWS SDK を使わない利用者にも入る |
| B. ルートに中核、保存先を別のパッケージにする（決定） | ルート `eventstore`（中核）、`memory`、`dynamodb` | 依存方向が明確。名前が内容を表す | import の変更が増える |
| C. 中核を専用の `core` パッケージに置き、ルートは空 | `/v2/core`、`/v2/memory`、`/v2/dynamodb` | ルートの名前の衝突がない | ルートに型がなく、発見しにくい |

### 9.2 欠番

変更フィードの補助は、最初のメジャーに含めないと決めた（2026-10-06 オーナー決定、IP 10）。選択肢の判断が不要になったので、この項目を欠番とする。他の項目の番号は変えない。本文の扱いは 1.3 と 4.8。

### 9.3 `SeqNr` を符号付きにするか

**決定**: A の `int64`。
**決めた人**: 指揮役（2026-10-06）。
**理由**: 適合データの負数ケースと 2^53 のケースを実行でき、報告の「表現不能」が減る。範囲外は契約違反として返せる（T-9）。

| 選択肢 | 内容 | 利点 | 欠点 |
|---|---|---|---|
| A. `int64`（決定） | 符号付き | 負数と 2^53 を型で表せる | 負数が型として表せてしまうので、検査が必要 |
| B. `uint64`（現行と同じ） | 符号なし | 負数を型が防ぐ | 負数のケースが「表現不能」になる |

### 9.4 payload の型の表し方

**決定**: A のジェネリクス `EventEnvelope[E]`。
**決めた人**: 指揮役（2026-10-06）。T-6 はどちらも許す。
**理由**: T-6 が payload に型の要件を課さず、`any` の制約で足りるうえ、型安全を得られる。複数の型は、利用者が1つの和型（インターフェイスと manifest による分岐）を作る。

| 選択肢 | 内容 | 利点 | 欠点 |
|---|---|---|---|
| A. ジェネリクス（決定） | 型パラメーター | 型安全。取り出しの型変換が要らない | 型パラメーターが API 全体に広がる |
| B. `any` | 実行時の型 | 1つのストアで複数の型を扱いやすい | 型安全でない。型アサーションが要る |

### 9.5 `GetLatestSnapshotByID` の命名

**決定**: A の `ID`。
**決めた人**: 指揮役（2026-10-06）。
**理由**: Go の慣習に合う。メジャーの置き換えと同時に行えるので、移行が同時に必要でも追加の負担にならない。

| 選択肢 | 利点 | 欠点 |
|---|---|---|
| A. `ID` にそろえる（決定） | Go の慣習に合う | 現行の名前から変わる |
| B. 現行の `Id` を保つ | 名前の変更がない | Go の慣習から外れる |

### 9.6 保持処理の通知経路

**決定**: A。ログ（`log/slog`）を必須とし、コールバック（`WithRetentionFailureHandler`）を追加の経路として公開する。メモリと DynamoDB の両方。
**決めた人**: 指揮役（2026-10-06）。S-4・MEM-11・MEM-D7 に沿う。
**理由**: 適合の検査（`observe.notifications`）と利用者の監視が確実に受けられ、メモリの必須のログも保たれる。

| 選択肢 | 内容 | 利点 | 欠点 |
|---|---|---|---|
| A. ログ（必須）に加えてコールバックを公開（決定） | 2.9 | 利用者が結果を自分の仕組みに接続できる。ログは常に出る | オプションが1つ増える |
| B. `slog` のログだけ | 公開 API を増やさない | 最小 | 試験で捕捉するには、ログのハンドラーを差し替える必要がある |

---

## 10. 未解決の疑問

現時点で未解決の疑問はない。旧1〜9は、指揮役のレビュー（1回目）と2026-10-06の決定で本文に反映した。その後に出た疑問も、2026-10-06 の仕様の決定（P-42〜P-44。P-45 は rs の移行の手順なので関係しない）で本文に反映した。

反映済みの疑問:

- 障害の段階は12種類（5.4）。
- `fnv1a64` は最初のメジャーでは対象外（5.1）。
- 保持件数「なし」と `retention_mode` の組は、方式を無視する（2.8）。
- 設定照合の再要求回数の既定値は10回（2.8）。
- メモリの場面にある障害の段階は5.4のとおり。
- ゼロ値の封筒と `nil` の `AggregateID` は契約違反（2.1。共通契約 T-2・T-10、P-42）。
- 取り消し理由が複数の項目に付いたときの優先順（4.5。`dynamodb.md` 6.2、P-43）。
- 設定項目の作成で `TransactionConflict` が返ったときの読み直し（4.4。DY-8、P-44）。
- D-7 の見積もりは多めに見積もる方式（4.5）。
- DynamoDB Local の起動オプションは6.1のとおり。
- 試験用フックは `internal/testhook` に置く（5.3）。

書いている途中で仕様の読み方に迷った点が出たら、ここに書く。
