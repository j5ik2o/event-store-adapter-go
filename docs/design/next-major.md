> This document is the design record written while building v5. For current usage, see [README.md](../../README.md) and the current code.
>
> Japanese version: [next-major.ja.md](next-major.ja.md)

<a id="次のメジャー版v500の設計"></a>

# Design for the next major version (v5.0.0)

This file is a design document. It contains no code. The lead reviews it and reflects the decisions in chapter 9 (decided on 2026-10-06) before it is used for implementation.

<a id="読み方"></a>

## How to read this document

- Rule numbers are those of `docs/spec/core-contract.md`, `docs/spec/storage/dynamodb.md`, and `docs/spec/storage/memory.md` in the hub (j5ik2o/event-store-adapter). "CR" refers to `conformance/README.md`, and "IP" refers to a section of the implementation plan.
- This document does not change the specification. It adds no rules. Points where the reading of the specification is unclear are written in chapter 10.
- The declarations in chapter 2 are proposals for "the form actually written in this language". The package layout was decided in chapter 9. The envelope, error, and operation types are placed at the module root (`eventstore`), and the storage backends are split into `memory` and `dynamodb`.
- The facts about the current code were confirmed by reading `pkg/` and `test/` as of 2026-10-06.

---

<a id="1-目的と範囲"></a>

## 1. Purpose and scope

<a id="11-目的"></a>

### 1.1 Purpose

Rewrite this library to match the new [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md) (envelope, head, classified errors, and conformance test data). The major number 5 is the same as the version of the common contract.

<a id="12-最初のメジャーに含めるものip-d3ip-d4"></a>

### 1.2 What the first major version includes (IP-D3, IP-D4)

| Item | Content |
|---|---|
| Core | Envelope, aggregate ID, validation, serializer, five error classes, four operations |
| Memory | A single-process in-memory implementation. It also declares conformance (IP-D4) |
| DynamoDB | Three tables (journal, snapshot, head) and configuration items |
| Conformance test data runner | Reads `conformance/` and runs all cases on memory and DynamoDB |
| Documents | Align the README, DATABASE_SCHEMA, and MIGRATION_GUIDE with the new contract (IP 5-5, IP 5-6) |

<a id="13-外すもの"></a>

### 1.3 What is excluded

- A feature that reads old data automatically. A migration tool for Go. Only a procedure document is provided (IP-D8, chapter 8).
- Other storage backends such as SQLite, Bigtable, and Spanner (IP 4.2).
- The memory TTL mode and the change feed (MEM-12, MEM-13. Requesting them is a configuration error).
- Change feed helpers (a function that builds head transitions, and a helper for resynchronization (DY-15)). They are not included in the first major version (owner decision of 2026-10-06, IP 10). Recording to the Streams (NEW_IMAGE) of the head table remains as in DY-3 and DY-12, so users can read it themselves. The public API has no change feed function.

<a id="14-次のメジャーの版の番号"></a>

### 1.4 Version number of the next major version

The current version is `v1.0.197` (the `version` file). The next major version is `v5.0.0` (IP 4.2). The module path changes to `github.com/j5ik2o/event-store-adapter-go/v5`. Without changing the path, the Go toolchain does not accept a v5 tag (IP 4.2).

Go has no mechanism for publishing a snapshot build. If the module path is `/v5`, users can fetch a commit of main as a pseudo-version (in the form `v5.0.0-<time>-<commit>` for an untagged `/v5`). This serves as the development build of the next major version. Each PR is squash-merged (IP 3). Chapter 7 covers this.

The `version` file is rewritten by `.github/workflows/release.yml`, which can only be started manually. If the input `version` is empty, the workflow calculates the version from the latest tag obtained with `git describe` and the `level`, and overwrites the file. Changing `version` by hand has no effect, so it is not changed. The official `v5.0.0` is released after the owner's approval, by starting the workflow manually with the input `version=5.0.0`.

After the change to `/v5`, tags of the form `v1.x` can no longer be created from main. When a fix is needed for the current major version, a maintenance branch `release/1.x` is created from the last tag, as in chapter 3 of the implementation plan. CI and the version-bump workflow will be adapted at that point.

---

<a id="2-公開-api"></a>

## 2. Public API

The declarations below are proposals. `context.Context` is the first argument. panic is not used.

<a id="21-集約-idt-1t-11t-12"></a>

### 2.1 Aggregate ID (T-1, T-11, T-12)

The user's `String()` or `AsString()` is not used as the storage key (T-1). The library builds the key from the type name and the value.

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

- Validation is done basically when the envelope is constructed. The same validation function is called again at the entry of each storage operation. In Go, a zero value such as `EventEnvelope[E]{}` and an envelope built without going through the constructor can exist, and the repeated validation rejects them. A zero-value envelope has an empty aid and lacks a required element of T-2, so it is a contract violation. `AggregateID` is an interface, so the user can pass `nil`. `AidString` and `NewEventEnvelope`, which receive an `AggregateID`, and the read operations (`GetLatestSnapshotByID` and `GetEventsByIDSinceSeqNr`) check for `nil` before calling `TypeName()`, and return a contract violation without panicking. The write operations receive an event envelope, so they validate the required elements of the event envelope again at the entry (T-2. A zero-value envelope has an empty aid). The snapshot envelope (`NewSnapshotEnvelope`) receives `aggregate` and `seqNr` as positional arguments, so the type does not allow a missing value. A `seqNr` of 0 is a valid value in T-9 and is not a missing value (a snapshot with number 0 is rejected by W-6 of the paired event. The conformance data `core-zero-snapshot`). Therefore the snapshot envelope needs no T-10 check for missing values (an exemption for the case where the type cannot express a missing value, P-42). The common contract T-2 and T-10 define that a missing required element is a contract violation (P-42). A `nil` `AggregateID` received outside the envelope (`AidString` and the read operations) is an argument error for which the specification defines no rule number. The aggregate ID is the same value as the required element aggregate_id of the envelope, so the rule number is `T-2` (design decision). The message includes the given seq_nr, except when the missing element is the seq_nr (E-3). The unit tests of the core confirm that a zero-value envelope and a `nil` `AggregateID` (construction, `AidString`, reads) become a contract violation without panicking (items 5 and 6 of 7.2). seq_nr 0 is W-6. W-9 (comparing the snapshot number and the event number) is done only at the entry. MEM-5 requires the checks of T-9, T-11, T-12, and T-13.
- An empty type name and an empty value are allowed. The specification (T-11, T-12) does not prohibit them, and the aid string contains `-`, so it is never empty (the lead's answer, 2026-10-06). The conformance data has no such case, so the unit tests of the core confirm an empty type name (`-value`), an empty value (`typeName-`), and both empty (`-`).
- The storage backends use only the validated string. The PK of DynamoDB is the aid string itself (DY-16).
- Identification by hash is not used. Selection by prefix match is not used (MEM-5).

<a id="22-整数と時刻の型t-9t-1315"></a>

### 2.2 Integer and time types (T-9, T-13, 1.5)

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

- A signed `int64` is used (decided in 9.3). Cases such as a negative number (-1) and 2^53 can be returned as contract violations without being erased by a type conversion.
- Time uses `time.Time`. Before calling `UnixNano()`, `occurred_at` is checked against the T-13 range (the nanoseconds from the epoch fit in a signed 64-bit integer). Out of range is a contract violation. `time.Time` has a wide range of years, and `UnixNano()` returns an undefined value when out of range, so the check comes first.
- Go's `time.Time` can keep nanoseconds. The rounding that T-3 allows ("rounding to the precision of the standard time type is allowed") does not normally occur. The runner executes the cases with `representation.time_precision = nanoseconds` (chapter 5).

<a id="23-イベント封筒t-2t-3t-5t-13"></a>

### 2.3 Event envelope (T-2, T-3, T-5, T-13)

The envelope is immutable. Its fields are unexported and are read through accessors (T-5. Adding an element does not break existing user code).

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

- The library does not interpret the manifest or the payload (T-4).
- `occurred_at` is not replaced with the time of the storage backend (T-3). DynamoDB stores occurred_at as an N in epoch nanoseconds (the attribute table in chapter 5).
- A required payload is distinguished from a JSON `null` as a value. `null` itself is not prohibited.

<a id="24-スナップショット封筒t-10"></a>

### 2.4 Snapshot envelope (T-10)

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

<a id="25-payload-とシリアライザt-6t-7t-8"></a>

### 2.5 Payload and serializer (T-6, T-7, T-8)

The domain types are not required to implement a library interface (T-6). The type parameter constraint is `any`.

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

- A serialization or deserialization failure is returned wrapped in an error of the "serialization" class (2.7). The original error can be obtained with `Unwrap()`.
- The default JSON allows differences in key order and whitespace. It preserves the distinction among array order, null, booleans, strings, and numbers (CR).

<a id="26-4つの操作3章w-3w-9r-1r-8"></a>

### 2.6 The four operations (chapter 3, W-3 to W-9, R-1 to R-8)

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

The write rules (W-3 to W-9) are as follows.

| Situation | Result | Rule |
|---|---|---|
| `seq_nr = 1`, no head | Create | W-3 |
| `seq_nr = 1`, head exists | Optimistic lock | W-3 |
| `seq_nr = 0` | Contract violation | W-6 |
| `seq_nr > 1` and `event.seq_nr == head + 1` | Update | W-4, W-8 |
| `seq_nr > 1` and `event.seq_nr <= head` | Optimistic lock | W-8 |
| `seq_nr >= head + 2`, or no head and `seq_nr >= 2` | Contract violation (gap) | W-8 |
| A second event with the same `seq_nr` | Optimistic lock | W-7 |
| `snapshot.seq_nr != event.seq_nr` | Contract violation | W-9 |

- The comparison is made against the seq_nr of the head. The snapshot is not used (H-2).
- The commit is a single atomic operation, and it appears at the same time in the head, in reads, and in the change feed (H-1).
- R-8: Memory reads atomically (MEM-8). DynamoDB reads non-atomically (DY-9). Each implementation states this in its doc comment.

**Representation of asynchrony**: Every operation is a synchronous function and receives a `context.Context`. The caller invokes it in a goroutine. Cancellation and deadlines are conveyed through the Context. No original Future or channel API is provided. This falls under "points left to the language" (chapter 8 of the common contract).

<a id="27-エラー分類4章e-1e-3"></a>

### 2.7 Error classes (chapter 4, E-1 to E-3)

The five classes can be distinguished both by type and by a discriminating value (E-1).

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

- The runner maps a type or a `Kind` to the five classes. It does not infer the class from the message string (CR).
- The current `IOError` is merged into "storage", and `SerializationError` and `DeserializationError` are merged into "serialization".

The rules for messages are as follows.

| Class | Include in the message | Do not include | Rule |
|---|---|---|---|
| Optimistic lock | The aid string, the seq_nr that was about to be appended, and (if known) the head seq_nr | Connection strings, credentials, raw error text of the SDK | E-2 |
| Contract violation | The rule number that was violated (for example `W-9`), and the related seq_nr (if any; when the missing element is the seq_nr, or when there is no related seq_nr as in T-11, only the rule number). W-9 also includes the different snapshot number | Do not make the head number mandatory | E-3 |
| Contract violation (D-7 size exceeded) | Only the class is checked. No condition on the rule number or message is added | — | E-3, CR |

- The `Error()` of `OptimisticLockError` does not concatenate the text of the cause. The cause is returned only through `Unwrap()`. This keeps the raw error text of the SDK out of the optimistic lock message (E-2). This restriction applies only to the optimistic lock. The other classes may join the text of the cause to `Error()` with `: `, following Go convention.
- The storage error also includes a loss of the data that was read (the classification in chapter 4). The configuration error includes an invalid value at construction time and a mismatch with the configuration recorded in the storage (P-40).

<a id="28-設定s-1mem-3dy-8"></a>

### 2.8 Configuration (S-1, MEM-3, DY-8)

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

The DynamoDB-specific configuration is as follows.

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

- The table and index names are given by the configuration. The library does not create the three tables (chapter 3 of `dynamodb.md`).
- The normative text defines no upper limit or default value. As a design, the following is decided.
  - The default number of re-requests is 10 (when `ConfigurationReadRetryLimit` is nil). Even a zero-value `Config` re-requests the default number of times, so a storage error does not occur just because of the first `UnprocessedKeys` (the intent of DY-8).
  - The wait time starts at 50 milliseconds, doubles each time, and is capped at 1 second (no jitter).
- Values that cause a configuration error:
  - A retention count of 0 (S-1). `KeepLatest(0)` and the zero-value `RetentionCount{}`. `memory.NewStore` and `dynamodb.New` also check the retention count they receive again and reject 0 with a `ConfigurationError`. The conformance case `core-retention-zero` expects a configuration error in `initialization.expect.error` at store creation, so the runner also treats a failure in constructing an option (such as `KeepLatest`) as a creation failure (5.2).
  - Requesting the TTL mode on memory (MEM-3, MEM-12).
  - Requesting the change feed on memory (MEM-3, MEM-13).
  - In the DynamoDB configuration check, only some of the three items exist, the store_id differs, or the layout_version differs (DY-8, P-40).
- When the retention count is "none", `retention_mode` is ignored and is not an error. This is because most of the conformance data uses the pair `retention_count: null` and `retention_mode: delete`. However, when the TTL mode is specified on memory, it is a configuration error regardless of the retention count, because of MEM-12 (design decision. The conformance data has no memory TTL pair).

The constructor of memory takes the following form.

```go
package memory

// Store は保存先。MEM-2: 明示的に共有するときだけ同じ Store を渡す。
type Store struct{ /* 非公開: 記録・設定・排他制御 */ }
func NewStore(opts ...eventstore.Option) (*Store, error) // MEM-3: 生成時に設定を検査

// New は Store を使うインスタンスを作る。同じ *Store を渡した場合だけ記録・設定・排他制御を共有する。
// nil の Store または必須シリアライザは、生成時に ConfigurationError を返す。
func New[E, A any](
    store *Store,
    eventSerializer eventstore.Serializer[E],
    snapshotSerializer eventstore.Serializer[A],
) (eventstore.EventStore[E, A], error)
```

<a id="29-保持処理の失敗を知らせる経路s-4mem-11"></a>

### 2.9 Path for reporting retention failures (S-4, MEM-11)

A retention failure does not change the result of the write. The normative text does not fix a public API for the notification.

- **Memory (MEM-11, MEM-D7)**: Notify by log. After releasing the lock, write a log with the standard `log/slog`. This notification is mandatory and does not depend on whether `WithRetentionFailureHandler` is specified.
- **DynamoDB (S-4)**: The `log/slog` log is mandatory, and the callback is an additional path (decided in 9.6).
- The callback of `WithRetentionFailureHandler` is a notification path **added** to the log. It does not replace the log. On memory, it is called after the lock is released.
- A failure or panic in the log output or the callback does not change the result of the committed write (MEM-11).

<a id="210-現行の公開-api-との対応表"></a>

### 2.10 Correspondence with the current public API

| Current (`pkg`) | Next major version | Rule / note |
|---|---|---|
| `AggregateId` (`GetTypeName`, `GetValue`, `AsString`, `fmt.Stringer`) | `AggregateID` (`TypeName`, `Value`). The library builds the string with `AidString` | T-1, T-11, T-12 |
| The `Event` interface (`GetId`, `GetTypeName`, `IsCreated`, `GetOccurredAt`, and others) | Removed. Replaced by `EventEnvelope[E]`. `IsCreated` is abolished (decided by `seq_nr == 1`) | T-2, T-6, W-3 |
| The `Aggregate` interface (`GetVersion`, `WithVersion`) | Removed. `SnapshotEnvelope[A]`. version is abolished | T-10, H-2 |
| `AggregateResult` (`Present`, `Empty`, `Aggregate`; panics when empty) | `SnapshotRead[A]` (with the head number). Does not panic | R-1 to R-3 |
| `EventStore.PersistEvent(event, version)` | `PersistEvent(ctx, event)`. The version argument is removed | W-3, W-8 |
| `EventStore.PersistEventAndSnapshot(event, aggregate)` | `PersistEventAndSnapshot(ctx, event, snapshot)` | W-9 |
| `GetLatestSnapshotById(id)` | `GetLatestSnapshotByID(ctx, id)` (`Id` in the name is changed to `ID`. 9.5) | R-1 to R-3 |
| `GetEventsByIdSinceSeqNr(id, uint64)` | `GetEventsByIDSinceSeqNr(ctx, id, SeqNr)` | R-4 to R-6 |
| `EventConverter`, `AggregateConverter` | Removed. Replaced by `Serializer[T].Deserialize` | T-6, T-7 |
| `EventSerializer`, `SnapshotSerializer` (via `map[string]interface{}`) | `Serializer[T]`. Payload only | T-7 |
| `DefaultEventSerializer`, `DefaultSnapshotSerializer` | `NewJSONSerializer[T]` | T-8 |
| `EventStoreBaseError`, `OptimisticLockError`, `SerializationError`, `DeserializationError`, `IOError` | The types of the five classes. They have `Unwrap` | E-1, E-2 |
| Constructors such as `NewOptimisticLockError` | Made unexported (users do not need to create them) | — |
| `KeyResolver`, `DefaultKeyResolver`, the number of shards | Removed. The PK is the aid string itself | DY-16 |
| `EventStoreOnDynamoDB`, `NewEventStoreOnDynamoDB` | `dynamodb.New` | DY-2 to DY-19 |
| `EventStoreOption`, `WithKeepSnapshot`, `WithDeleteTtl`, `WithKeepSnapshotCount`, `WithKeyResolver`, `WithEventSerializer`, `WithSnapshotSerializer` | `Config` and `Option`. Retention count, retention mode, grace seconds, failure notification | S-1, S-4 |
| `WithKeepSnapshot(bool)` | Removed. A retention count of "none" means "no history" | S-1 |
| `EventStoreOnMemory`, `NewEventStoreOnMemory` | `memory.New` and `memory.Store` | MEM-1 to MEM-13 |
| `pkg/common` (table creation for tests, the LocalStack client) | Removed (in the PR that removes the old API). Test helpers are newly created under `internal/` | IP 4.2 |
| Module path `.../event-store-adapter-go` | `.../event-store-adapter-go/v5` | IP 4.2 |

---

<a id="3-メモリの実装方針"></a>

## 3. Implementation approach for memory

The memory implementation satisfies MEM-1 to MEM-13 as follows.

| Rule | Approach |
|---|---|
| MEM-1 | Hold data only in process memory. Do not write to files. Provide no API to clear everything during use |
| MEM-2 | `memory.NewStore` creates an empty, independent `Store` every time. Nothing is shared through package variables or names. Only the `New` instances that are given the same `*Store` share the records, the configuration, and the lock |
| MEM-3 | `NewStore` validates the configuration. A retention count of 0, a TTL mode request, or a change feed request is a `ConfigurationError`. The `Store` holds the configuration immutably |
| MEM-4 | Place one `sync.RWMutex` on the `Store`. Its scope is the whole storage. It can be called from multiple goroutines. The intermediate state before commit cannot be read from outside the lock |
| MEM-5 | The key of a record is an exact match of the aid string (T-1). `map[string]*record`. aid and seq_nr are separate values. Identification by hash alone is not used. Selection by prefix match is not used. T-9, T-11, T-12, and T-13 are checked at the entry |
| MEM-6 | Hold the metadata and the payload in separate fields. The payload is held as bytes serialized by the `Serializer`, and is deserialized at retrieval to return a new value. Changing a retrieved result or an input does not change the stored value. No requirement beyond T-6 (such as a copy interface) is imposed |
| MEM-7 | Input validation and payload serialization are done before taking the lock. Inside the lock, read the head, compare, prepare the changes, publish them together, and run retention. A failure during preparation does not change the records. A new creation with `seq_nr = 1` is possible with an event alone |
| MEM-8 | `GetLatestSnapshotByID` obtains the latest snapshot envelope and the head seq_nr at the same time inside the read lock (atomic) |
| MEM-9 | `GetEventsByIDSinceSeqNr` obtains all the events inside the read lock. It deserializes after releasing the lock |
| MEM-10 | If there is no retention count, hold only the current snapshot. If n is specified, keep the newest n and remove the older ones in order from the oldest. This is done inside the same lock after the commit. Leftovers are cleaned up even on an event-only append. The journal and the head are not removed |
| MEM-11 | A retention failure does not change the success of the write. After releasing the lock, report it by the mandatory log (2.9). The callback is an additional notification. It is retried by the retention run after the next append |
| MEM-12 | The first version does not provide the TTL mode. Requesting it is a configuration error |
| MEM-13 | The change feed is not provided. Requesting it is a configuration error |

<a id="31-内部のデータ構造"></a>

### 3.1 Internal data structures

```go
type record struct {
    head      head                  // seq_nr と、確定したイベント封筒（1件）。H-4
    journal   []storedEvent         // seq_nr 昇順
    current   *storedSnapshot       // 現在のスナップショット
    history   []storedSnapshot      // 履歴。seq_nr 昇順
}
```

- The stored envelope holds the metadata and the bytes of the serialized payload. The head seq_nr can be derived from the last element of `journal`, so it is not managed in parallel by a separate variable. Only the value that cannot be derived (the event envelope held by the head) is held separately.
- The retrieved result is built anew every time. The stored bytes are not returned as they are.

<a id="32-書き込みの手順"></a>

### 3.2 Write procedure

1. Entry validation (T-9, W-6, W-9, T-11 to T-13). Before the lock. The validation done at envelope construction is called again with the same function, rejecting a zero-value envelope (2.1). W-9 is done only here.
2. Serialize the payload and, if present, the snapshot. A failure is a `SerializationError`. Before the lock.
3. Take the lock.
4. Read the head. Compare by W-3, W-7, and W-8. A violation is an `OptimisticLockError` or a `ContractViolationError` (gap).
5. Assemble the changes into a working value. Even on failure the records do not change.
6. Publish the changes to the records together (commit. H-1).
7. Retention (MEM-10). A failure is held in a variable.
8. Release the lock.
9. If there was a failure, notify (MEM-11).

---

<a id="4-dynamodb-の実装方針"></a>

## 4. Implementation approach for DynamoDB

<a id="41-使う-sdk-とその版"></a>

### 4.1 The SDK used and its versions

Use the versions pinned in the current `go.mod`.

| Module | Version |
|---|---|
| `github.com/aws/aws-sdk-go-v2` | v1.47.1 |
| `github.com/aws/aws-sdk-go-v2/service/dynamodb` | v1.70.0 |
| `github.com/aws/aws-sdk-go-v2/config` | v1.33.6 |
| `github.com/aws/aws-sdk-go-v2/credentials` | v1.20.6 |

- Updating the versions is outside the scope of this document. Updating them is a separate decision.
- The constructor receives the client (`*dynamodb.Client`). The user creates the client (authentication, region, endpoint).

<a id="42-差し込みの仕組みの置き場所ip-6"></a>

### 4.2 Where the injection mechanism lives (IP 6)

- Fault injection for tests is done with the SDK's `APIOptions` (Smithy middleware). **It is not put into production code.** The test side builds the client by passing a function to `dynamodb.Options.APIOptions`.
- Place the middleware at the Initialize or Finalize step. From the operation name (`TransactWriteItems`, `BatchWriteItem`, `UpdateItem`, `Query`, `BatchGetItem`) and the content of the input, determine the phase (the phase in chapter 5). Fail only the requests of the retention processing.
- In tests, disable the SDK's automatic retry (`RetryMaxAttempts = 1`). This lets the runner count the creation conflicts and the re-requests.
- `replace-request` returns a result without calling the next handler (nothing is committed). `replace-response` replaces the result after calling the next handler (the side effects remain).

<a id="43-3テーブルと設定項目dy-2dy-3dy-8dy-16dy-19d-3d-4"></a>

### 4.3 Three tables and configuration items (DY-2, DY-3, DY-8, DY-16 to DY-19, D-3, D-4)

| Table | Key | GSI | Streams | TTL |
|---|---|---|---|---|
| journal | PK `aid`(S), SK `seq_nr`(N) | None | Disabled | None |
| snapshot | PK `aid`(S), SK `skey`(N) | `(aid, active_history_seq_nr(N))`, projection KEYS_ONLY | Disabled | Attribute `ttl`. Enabled only in the TTL mode (DY-2) |
| head | PK `aid`(S), no SK | None | Enabled, NEW_IMAGE (DY-3, DY-12, D-4) | None |

- Creating the three tables is outside the library. The three tables are placed in the same region (DY-18).
- The current snapshot, marked history, and configuration items do not appear in the GSI (D-3).
- The Streams of the journal is not used as a source of the change feed (the specification of 4.1).

The attributes of the items (chapter 5 of `dynamodb.md`) are as follows.

| Item | Attributes |
|---|---|
| Journal | `aid`(S), `seq_nr`(N), `occurred_at`(N, epoch nanoseconds), `manifest`(S), `payload`(B) |
| Snapshot (current) | `aid`, `skey`(N, 0), `seq_nr`(N), `manifest`(S), `payload`(B), `last_updated_at`(N, occurred_at in milliseconds). It has neither `active_history_seq_nr` nor `ttl` |
| Snapshot (history) | The same attributes as the current one (`skey` is the seq_nr of the history). `active_history_seq_nr`(N) only while it is not marked. `ttl`(N, epoch seconds) when marked for TTL. An item without an expiry has no `ttl` attribute |
| Head | `aid`(S), `type_name`(S), `seq_nr`(N), `events`(L). The element is one M (`seq_nr`, `occurred_at`, `manifest`, `payload`) |
| Configuration item | `aid = "__config__"`. The SK is `seq_nr=0` for the journal, `skey=0` for the snapshot, and none for the head. The only attributes are `store_id`(S) and `layout_version`(N, 1 in the first version) |

- `type_name` is obtained from the type name of the aid.
- The PK is the aid string itself (DY-16). No logical shard or hash is used. The SK of the journal is the seq_nr, and the skey of the snapshot is 0 for the current one and the seq_nr of the history otherwise (DY-17).
- Operations on an aggregate use only `GetItem`, `BatchGetItem`, `Query`, `TransactWriteItems`, `BatchWriteItem`, and `UpdateItem` narrowed to the aid. Scan is not used (DY-19).
- Whether the item appears in the GSI is decided by the presence of the history mark (`active_history_seq_nr`).

<a id="44-設定項目の照合dy-8p-19p-40"></a>

### 4.4 Checking the configuration items (DY-8, P-19, P-40)

`New` checks the configuration items of the three tables with the following state transitions.

1. Read the three configuration items with one `BatchGetItem` (`ConsistentRead=true`). Accumulate the `Responses`.
2. If there are `UnprocessedKeys`, re-request only those keys with exponential backoff, keeping strong consistency. Do not decide "does not exist" until nothing is unprocessed. When the limit is reached, return a storage error, not a configuration error.
3. If none of the three exists, create a new `store_id` and perform three Puts with the condition `attribute_not_exists(aid)` in one `TransactWriteItems`. If the condition is not satisfied, discard the response, read the three again with strong consistency, and proceed to step 4 (P-19). The same re-read is done when the cancellation reason is `TransactionConflict` (another creation is writing at that moment). If none of the three exists even after the re-read, return a storage error without repeating the creation (DY-8, P-44). The conformance data has no such scene, so the unit tests confirm both the case where the re-read finds the configuration items and the case where none of the three exists and a storage error results. Step 2 (DY-8) also applies to this re-read. `UnprocessedKeys` are re-requested up to the limit, and no decision is made while some remain unprocessed. When the limit is reached, return a storage error.
4. If all three exist, `store_id` and `layout_version` match across the three, and they equal its own version (1), continue.
5. Otherwise (only some exist, a `store_id` mismatch, or a `layout_version` difference), it is a configuration error (P-40).

Configuration items are created only with a conditional Put.

Permissions are divided between the test and the library.

- For the library: only `BatchGetItem`, `PutItem` (configuration items), `TransactWriteItems`, `Query`, `BatchWriteItem`, and `UpdateItem` on the three tables (and the GSI). It does not create tables.
- For tests: a separate client without injected faults performs table creation and deletion, `PutItem` (`seed.items` and the `install_items` of a creation conflict), `GetItem`, `Query`, `DescribeTable`, and `DescribeTimeToLive`.
- DynamoDB Local does not check IAM, so the tests distinguish them by using separate clients. An IAM example for the real service is written in the documentation PR (7.2).

<a id="45-書き込みd-5d-6d-7w-8"></a>

### 4.5 Writes (D-5, D-6, D-7, W-8)

One write is performed with one `TransactWriteItems`. At most four actions.

| Action | Condition |
|---|---|
| Put journal | `attribute_not_exists(aid)` |
| Put head (`seq_nr = 1`, new creation) | `attribute_not_exists(aid)` |
| Update head (`seq_nr > 1`) | `seq_nr = :prev`. `:prev` is `event.seq_nr - 1`. Overwrites `seq_nr` and `events` |
| Put snapshot (current. When there is a snapshot) | No condition |
| Put snapshot (history. When a retention count is set) | No condition |

- The Put and Update of the head specify `ReturnValuesOnConditionCheckFailure=ALL_OLD` (D-5).
- The classification of failures is as follows.

| Failure | Class | Rule |
|---|---|---|
| The head condition fails on new creation | Optimistic lock | W-3 |
| The head condition fails on update | Compare with the seq_nr of the old head. `event.seq_nr <= old head` is an optimistic lock. `event.seq_nr >= old head + 2` is a contract violation (gap). If the old item is not returned, treat the old head as 0 | W-8, D-5 |
| The journal condition fails | Optimistic lock | W-7 |
| `TransactionConflict` | Optimistic lock | D-6 |
| Throttling, communication failure, others | Storage | — |

When cancellation reasons are attached to multiple items, if any item has `TransactionConflict`, it is an optimistic lock (D-6). Otherwise, look in the order of the head condition failure, the journal condition failure, and others, and classify by the first one that matches (`dynamodb.md` 6.2, P-43). The conformance data has no scene where reasons are attached to multiple items, so the unit tests confirm it (for example, when a head condition failure indicates a gap and another item returns `TransactionConflict`, the result is an optimistic lock).

- No additional read is made for classification (D-5). The old seq_nr in the `Item` of `CancellationReasons` is used.
- D-7: Estimate the item size before writing. The limit is 409600 bytes. The payload is carried in both the journal and the head. The items and attributes to estimate are as follows.
  - (a) Journal: `aid`, `seq_nr`, `occurred_at`, `manifest`, `payload`.
  - (b) Head: `aid`, `type_name`, `seq_nr`, `events` (one M inside the L: `seq_nr`, `occurred_at`, `manifest`, `payload`).
  - (c) Current snapshot: `aid`, `skey`, `seq_nr`, `manifest`, `payload`, `last_updated_at`.
  - (d) History snapshot: (c) plus `active_history_seq_nr` (it has no `ttl` at write time).
  - Calculation: add the UTF-8 byte count of the attribute names and the size of the values. S is the UTF-8 byte count, B is the byte count, N is estimated generously as 21 bytes, and L and M are 3 bytes plus 1 byte per element plus the contents.
  - If any one of the items to be sent exceeds 409600 bytes, return a `ContractViolationError` without sending.
  - Correspondence with the four cases of the conformance data: `dynamodb-item-size-event` is the payload of the journal and the head, `-snapshot` is the payload of the current (and history) snapshot, `-manifest` is the manifest of the journal and the head, and `-head-overhead` is the `type_name`, the aid, and the payload of the head.
  - The data does not require an exact boundary (`conformance/README.md`), so a generous estimate is enough.
- The optimistic lock message contains only the aid string, the seq_nr, and (if known) the head seq_nr, as in E-2.

<a id="46-読み取りdy-9dy-10dy-11r-8"></a>

### 4.6 Reads (DY-9, DY-10, DY-11, R-8)

- `GetLatestSnapshotByID`: Read the head and the current snapshot (`skey=0`) with one `BatchGetItem` (strongly consistent). Re-request `UnprocessedKeys` until everything is read (DY-9). The re-request covers only the remaining keys, with exponential backoff and strong consistency, and the limit and wait times are the same as in the configuration check (2.8) (`ConfigurationReadRetryLimit`. Default 10. The first request is not counted). When the limit is reached, the unprocessed keys are not judged to be "absent", and a `StorageError` is returned (design decision. DY-9 defines no limit, but without a limit the call may not return with a `context` that has no deadline). If there is no head, the result is "none". If there is one, the result is a pair of the envelope (none if absent) and the head seq_nr (DY-10).
- Reading the two items is not atomic (R-8). `TransactGetItems` is not used (P-25). This property is written in the doc comment.
- `GetEventsByIDSinceSeqNr`: `Query` the journal with `aid = :aid AND seq_nr >= :seq_nr` and `ConsistentRead=true`. Ascending order. Read to the end while `LastEvaluatedKey` is returned (DY-11, R-5).

<a id="47-保持処理8章d-9p-18p-24s-3"></a>

### 4.7 Retention (chapter 8, D-9, P-18, P-24, S-3)

Retention is performed only after the commit of a write that wrote a history (D-9). It is performed synchronously inside the write call, after the commit response and before returning. Even if it fails, the result of the write does not change. Let the retention count be n.

1. `Query` the history GSI with `aid = :aid` and `ScanIndexForward=false`, and read it to the end (KEYS_ONLY). It is an eventually consistent read (DY-18).
2. Add the history just written. If it is already visible, do not duplicate it. Keep the first n in descending order, and make the older ones the targets (S-2).
3. In the delete mode, send `BatchWriteItem` 25 items at a time (P-18). For `UnprocessedItems`, resend only the remaining items with exponential backoff. As with the configuration check (2.8), the resend limit and wait times are `ConfigurationReadRetryLimit` (default 10. The first request is not counted), with the first wait 50 milliseconds, doubling each time, and a cap of 1 second. When the limit is reached, do not delete the rest, report it as a retention failure in step 5, and do not change the result of the write. The remaining history becomes a target again in the next retention run (design decision. The specification defines only that items are resent and defines no limit. Retention runs inside the write call, so without a limit the call may not return).
4. In the TTL mode, one `UpdateItem` at a time: `SET #ttl = :expires REMOVE active_history_seq_nr`, with the condition `attribute_exists(active_history_seq_nr)`. `#ttl` is in `ExpressionAttributeNames`. `:expires` is the epoch seconds at the time of marking plus the grace seconds of `WithTTLGraceSeconds` (0 if not specified). The grace seconds have no upper limit, so the `int64` addition can overflow. The epoch seconds and the grace seconds are both `int64` values of 0 or more, so calculate the sum as a `uint64` and write it to the N attribute as a decimal string with `strconv.FormatUint`. The unit tests confirm the boundary where the grace seconds are `math.MaxInt64`. If the condition fails, skip it as already marked.
5. A failure does not change the result of the write. Report it through the notification path of S-4.

- The approach of counting first and then selecting the excess is not used (P-24). The processing corresponding to the current `getSnapshotCount` is abolished.
- Marked history is not counted in the count. The expiry is not postponed (S-3). The mark condition (`attribute_exists(active_history_seq_nr)`) preserves this.

<a id="48-変更フィード13dy-12dy-13dy-15"></a>

### 4.8 Change feed (1.3, DY-12, DY-13, DY-15)

- Keep the layout in which the Streams of the head is enabled with NEW_IMAGE (DY-3, DY-12). Users can read the Streams records themselves.
- The function that builds head transitions and the helper for resynchronization (DY-15) are not included in the first major version (owner decision of 2026-10-06, IP 10). The public API has no change feed function.

---

<a id="5-適合テストデータの実行器"></a>

## 5. Runner for the conformance test data

The runner is written as a Go test. The layout uses `conformance/` read-only. The location is `internal/conformance`.

<a id="51-データの読み方"></a>

### 5.1 How to read the data

- Read the JSON with a `json.Decoder` with `UseNumber()` set. Hold the seq_nr as a `math/big.Int`, and do not decode it directly into `SeqNr` or `uint64`. This is to handle -1 and 2^53.
- Handle `epoch_nanoseconds` as a decimal string with `big.Int`. Do not go through floating point.
- Reject duplicate keys, `NaN`, and `Infinity`. Do not rewrite string values or attribute names.
- Check the `format` and `version` of each JSON. Cross-check with `manifest.json`.
- **Time precision**:
  - For a success case with `precision_policy = native-time-type`, the expected value is the value converted into `time.Time` from the input. `expect.value` is the reference value before rounding. Report the converted value and the actual value.
  - When `representation.time_precision` is present, Go's `time.Time` is a nanosecond type, so execute the `nanoseconds` cases. `milliseconds` is out of scope, and the reason is recorded.
  - Execute all cases that have no mark.
  - `representation.signed_seq_nr = true` is executed because it can be represented by `SeqNr` (signed). `SeqNr` is signed (9.3), so it does not become "not representable". If there are other cases that the type cannot represent, report "not representable" with the reason (5.6).
- **Operations of the value table** (a public API function with the same name is not required. The runner maps them):

| Operation | Mapping |
|---|---|
| `buildAid` | Prepare a test `AggregateID` that returns `user_string`, and confirm that `AidString` builds it from the type name and the value (T-1, T-11, T-12) |
| `validateSeqNr` (`context=value`) | `SeqNr.Validate` (T-9. 0 is valid) |
| `validateSeqNr` (`context=event`) | Call `NewEventEnvelope`. 0 is a W-6 contract violation. The runner does not compute the same expression itself |
| `validateOccurredAt` | Use an aggregate whose type name is `ConformanceTime` and whose value is the case ID. `PersistEvent` numbers 1 through `event_seq_nr - 1` at the time `1970-01-01T00:00:00.123000000Z`. Then write number `event_seq_nr` with the input time. The payload is an empty object and the manifest is an empty string. A success case reads number 1 back and compares it. An out-of-range case compares a contract violation at number 7 (T-13) |
| `fnv1a64` | The first major version has no storage backend that uses hashes (memory and DynamoDB both use the aid string itself as the key). The four FNV-1a 64 cases of the conformance data (K-1) are reported as "out of scope" with a reason, and are counted neither as success nor as failure. They are run at stage 5, when a storage backend that uses hashes is released (condition 1 of chapter 5 of the implementation plan. Owner decision of 2026-10-06) |

- **Comparison of payloads and aggregate state**: Compare the JSON values serialized and deserialized with the default JSON serializer. Ignore key order and whitespace. Preserve array order, null, booleans, strings, and numbers. Do not treat booleans and numbers as the same. Do not normalize Unicode.
- **generators**: `target` (a JSON Pointer rooted at the case), `character` (one Unicode character), and `byte_length` (the total UTF-8 bytes). The target is an empty string in the fixtures. Decode `~0` and `~1`. Schema validation is done before expansion, and operations use the values after expansion. 400KB is 409600 bytes, and 1MB is 1048576 bytes.

<a id="52-場面の実行手順とストアを場面ごとに分ける方法"></a>

### 5.2 Scene execution procedure and how to separate stores per scene

1. Check whether `backends` contains the storage backend to run (`memory` or `dynamodb`). If not, report "out of scope". `requires=["ttl"]` requires the TTL mode. In v1, the TTL scenes are for DynamoDB only (MEM-12).
2. Run each scene on an independent store. Memory creates an empty `Store` with `memory.NewStore` each time. For DynamoDB, the runner assigns a unique table name and GSI name per scene and creates the three tables in the same region. Items of other cases are not reused.
3. If there is `seed.items`, insert them before creating the store, with the test permissions (a separate client without injected faults. 4.4). They are not inserted with the library's client.
4. Create the store with the `store` configuration. `retention_count=null` is `NoRetention()` (no history). `retention_mode` is delete or ttl. `ttl_grace_seconds` is the grace seconds of `WithTTLGraceSeconds`. Map the common names to `Config` and `Option`. Register the faults before creation first. A failure in constructing an option (such as `KeepLatest(0)`) is also treated as a creation failure and compared in step 5.
5. If there is `initialization`, check the creation result. `initialization.expect.error` is observed both for a failure in constructing an option and for a failure of `New` or `NewStore`. A case where creation fails has no operation sequence (for example, `core-retention-zero`).
6. Expand the generators, and build `fixtures.events` and `fixtures.snapshots` as envelopes just before each operation. If constructing an envelope fails because of invalid input, catch it as a failure of that operation. The runner's pre-checks do not substitute for the library's validation.
7. Execute the `steps` in array order, without running them concurrently.
8. Check the `expect` and `observe` of each operation. `expect` is one of `success`, `none`, `snapshot` (a pair of `head_seq_nr` and the envelope), `events` (including order), and `error`. For a scene with a retention failure or delay, observe after the retention and inspection hooks complete. A hook must not change the success or failure of the write.
9. For `error`, map `Kind` to the five classes and compare. `error.rule`, `must_contain`, and `must_not_contain` are checked on the message. The class is not inferred from the message.
10. `retry_limit` is the upper limit of the re-requests of the configuration check (the first request is not counted). Reproduce reaching the limit with the same sequence of unprocessed responses. For exponential backoff, shorten the real time with a clock hook.

<a id="53-フックの置き場所"></a>

### 5.3 Where the hooks live

| Hook | Location | Content |
|---|---|---|
| Deterministic execution of retention (delete / ttl) | No trigger is placed. Retention runs synchronously inside the write call, after the commit (memory inside the same lock. MEM-10. DynamoDB after the commit response and before returning. D-9). Even if it fails, the result of the write does not change. **No public API is added** | Observe after the operation returns |
| Internal history (`observe.history`) | DynamoDB: `Query` the snapshot table and split into `active` (no mark) and `marked` (with `ttl`). Memory: read the internal logical history of the `Store` through `internal/testhook` | Only the history of that aggregate. Exact match of `active` and `marked`. `absent` is a history that must not exist. The current snapshot and the configuration items are not counted |
| Memory retention failure (retry and notification) | The hooks of the logical history (candidate selection and deletion) return `storage-error`. The notification is captured by the capture function of `WithRetentionFailureHandler` and a replaced `slog` handler. For the retry, after the next append, confirm with `observe.history` that the leftovers were cleaned up | MEM-10, MEM-11. The result of the write does not change |
| Failure notification (`observe.notifications`) | Pass a test capture function to `WithRetentionFailureHandler`. The mandatory log of memory can also be captured by replacing the `slog` handler | Class `retention-failure`. An empty array means no failure notification. If there are multiple logs of the same final failure, they may be normalized into one |
| SDK requests (`observe.requests`) | Record the input before sending with `APIOptions` middleware | Compare as a structure obtained by parsing the expressions and the bindings of attribute names and values (whitespace, clause order, and AND order are not compared). `update.set`, `update.remove`, `condition`, `key_condition.all` (`eq`/`gte` and the bindings to `aggregate_id` and `seq_nr`), the `#ttl` of TTL and `expression_attribute_names`, `expires` (epoch seconds), `initial_batch_sizes` (the number of delete batches excluding resends), `no_requests_in_phases`, `request_count`, and `minimum_request_count`. The elements of `requests` are matched, in array order, to the actual separate requests. Paging, re-requests of unprocessed keys, and the splitting of delete batches are checked over the whole sequence of requests of the phase |
| Attributes (`observe.items`, `seed.items`) | The runner reads the physical item with `GetItem` | `table` is the configured table name of journal / snapshot / head. `attributes` are S/N/B/L/M, and the attribute set must match exactly. N is compared as an integer from a decimal string. An M inside an L uses `nested_attributes`. `binary_json` compares the deserialized result of B as JSON. The `generated-store-id` of `bindings` binds the first actual `store_id` and checks that the three items have the same value. The checks of the attribute set, types, and list length are done on the whole actual item before exclusion |
| Clock (`clock.epoch_seconds`, `clock_epoch_seconds` of an operation) | Replace the internal clock (the `func() time.Time` passed to the retention processing) and the wait time. Do not add an argument such as `WithClock` to the public API; inject from `internal/testhook` | The expiry is "mark time + `ttl_grace_seconds`". v1 uses a clock of the year 2100 |
| Layout check (`dynamodb/layout.json`) | Check the actual three tables with `DescribeTable` and `DescribeTimeToLive` | The table names and GSI names are bound to the configured values |

- The types and registration functions of the test hooks (the clock and wait time, and the faults and observation of the memory logical history) are placed in `internal/testhook`, which does not depend on the public packages. The types use only the aid string, the seq_nr, and bytes, and do not depend on the core types.
- `memory` and `dynamodb` import `internal/testhook` and receive them in unexported fields. The runner (`internal/conformance`) registers with the store through `internal/testhook`. The dependency direction is "public packages → `internal/testhook`" and "runner → public packages and `internal/testhook`", so there is no cycle. The public API does not grow.

<a id="54-障害の差し込み"></a>

### 5.4 Fault injection

The `operation` of `faults` is 0 for store creation, and 1 or more is a 1-based operation number. If injection is impossible, report "unverified" and do not count it as success.

**How firings are counted**: The runner keeps a firing count for each registered fault. Each time a hook or middleware applies a fault, it adds 1. At the end of the scene, confirm that the count is exactly the specified number when `repeat` is `count`, and at least 1 when it is `until-operation-finishes`. If they do not match, fail the scene, indicating which fault.

`phase` has 12 kinds, as in the enumeration of `conformance/schema/common.schema.json`.

| phase | DynamoDB injection | Memory injection |
|---|---|---|
| `serialize-event` | The event serializer wrapper (a test `Serializer` implementation) returns a failure | Same |
| `serialize-snapshot` | The snapshot serializer wrapper returns a failure | Same |
| `deserialize-event` | The wrapper at deserialization returns a failure | Same (deserialization at retrieval) |
| `deserialize-snapshot` | Same | Same |
| `commit` | Capture the input of `TransactWriteItems` with `APIOptions`, and use `replace-request` (nothing is committed) or `replace-response` (already committed, and the response is replaced). `TransactionCanceledException` is `replace-request` | Before the commit (before publishing together), the hook of the logical history returns a failure. No change is left in the head, the journal, or the snapshot (MEM-7) |
| `read-events` | Replace the `Query` of the journal | The hook for retrieving events inside the lock returns a failure |
| `read-snapshot` | Replace `BatchGetItem`. `sdk-response` returns `unprocessed_keys`. `read-interleave` is 5.5 (DynamoDB only) | The hook for retrieving the "pair of head and snapshot" inside the lock returns a failure |
| `retention-query` | Replace the `Query` of the history GSI. `history_pages` and `omit_just_written_history` follow the response plan | Map to the hook for candidate selection of the logical history (`conformance/README.md`). `storage-error` returns a failure. `history_pages` of `sdk-response` is returned as is, as the list of pages of the sequence (descending) of the seq_nr of unmarked history that the retention read. The history just written is not added automatically. Adding the history just written and removing duplicates are done by the library with the same procedure as DynamoDB. For `omit_just_written_history=true`, the runner confirms that the number just written is not in the page list (it does not appear in the real memory data). The physical result of deletion is checked by reflecting it in the actual logical history |
| `retention-delete` | Replace `BatchWriteItem`. `unprocessed_first_n` makes the first n items `UnprocessedItems` and processes the rest for real | The hook before deletion of the logical history returns a failure |
| `retention-mark` | Replace `UpdateItem` | Memory scenes have no such phase (confirmed on the real data. DynamoDB only. MEM-12) |
| `configuration-read` | Replace the `BatchGetItem` of the configuration check. Includes `unprocessed_keys` | Memory scenes have no such phase (confirmed on the real data. DynamoDB only) |
| `configuration-create` | Replace the `TransactWriteItems` of configuration creation. For a creation conflict with `install_items`, the runner commits the item it wrote separately beforehand | Memory scenes have no such phase (confirmed on the real data. DynamoDB only) |

The faults in memory scenes are only `commit`, `read-*` (`read-events`, `read-snapshot`), `serialize-*`, `deserialize-*`, `retention-query`, and `retention-delete`. `read-interleave`, `configuration-read`, `configuration-create`, and `retention-mark` are DynamoDB only.

- `kind` and the injection method:
  - `serialization-error`: Fail the corresponding phase of the serializer.
  - `storage-error`: The final failure of the storage or the retention hook. `details.scope=final-retention-failure` fails the whole retention after candidate selection and before deletion.
  - `sdk-error`: Disable the SDK's automatic retry, and build and return an SDK exception from `details.code` and `cancellation_reasons`.
  - `sdk-response`: Follow the response plan (`responses`, `unprocessed_keys`, `history_pages`, `omit_just_written_history`, `unprocessed_first_n`). `history_pages` returns the pages of the history seq_nr of the GSI response as is, and does not add the history just written automatically. Map `LastEvaluatedKey` and `ExclusiveStartKey` for each page. The physical results of deletes, TTL updates, and writes are actually reflected and checked.
  - `read-interleave`: 5.5.
- `repeat` is `{"mode":"count","count":1}` or `{"mode":"until-operation-finishes"}`.
- In v1, all write failures and retention failures are `replace-request`.
- **`cancellation_reasons`**: One element per item of the transaction. For writes, the order is journal, head, current-snapshot, history-snapshot (only the actions that exist). For configuration creation, they are `configuration:journal`, `configuration:snapshot`, and `configuration:head`. The `code` of an item that did not fail is the string `None`. The runner matches the target names against the actions in the actual request and reorders them. This order is not enforced as the library's request order. A `ConditionalCheckFailed` of the head has `old_head_seq_nr` (`null` means the old item is not returned). The classification of D-5 uses this old item. A journal condition failure is W-7, `TransactionConflict` is the optimistic lock of D-6, and throttling of an item other than the head is a storage error.

<a id="55-read-interleavedy-9-の非原子的応答"></a>

### 5.5 `read-interleave` (the non-atomic response of DY-9)

Real-time races are not used. It is reproduced deterministically.

1. In the middleware just before sending `BatchGetItem`, capture the old head.
2. Commit the append of `interleaved_operation` with a separate client (without injected faults).
3. Send the original `BatchGetItem`.
4. Replace the head in the response with the old head captured in step 1.
5. Compare the `SnapshotRead` that the caller receives with `expect`.

In the real data, `read-interleave` exists only in the DynamoDB scenes (`dynamodb/read.json`). It is not run on memory (it is not in `backends`). Memory reads atomically by MEM-8, so this state does not occur (R-8).

<a id="56-報告の形と-ci-での実行"></a>

### 5.6 Report format and running in CI

- The report states are five: "success, failure, out of scope, unverified, not representable".
- Output the following to CI: the data version and the result of the `manifest` check, the language, implementation version, and storage backend, the five states above per case ID and rule number, and the failed operation number with the expected and actual values. When one case has multiple rules, map it to all the rules. If something differs from the expectation midway, do not report success (IP 5).
- Record the reason for selections due to differences in expressive power, optional capabilities, deleted rules, and caller recommendations. Do not count cases whose conditions are not met, results that skipped a required case, or results where a fault could not be injected as success.
- The source of truth for the summary is one JSON file (`conformance-report.json`. Saved as a CI artifact). Its content is the data version, the result of the `manifest` check, the language, implementation version, and storage backend, per case the `id`, `rules`, `status`, reason, failed operation number, expected and actual values, and the counts per state for each rule number. The output of `go test -json` is kept only as a log.
- Verification of distribution: copy `conformance/` with identical content and stop newline conversion with `.gitattributes`. CI runs `python3 tools/conformance/manifest.py verify` and the SHA-256 check of `manifest.json` (already present in the `lint` job of the current `.github/workflows/ci.yml`). Do not rebuild the manifest to hide a difference.
- The conformance run is added as a job `conformance` separate from the existing `test` job (a DynamoDB Local container is required). It is added to the `needs` of `ci-success`. How to decide the required scope is in 7.4.

---

<a id="6-試験環境"></a>

## 6. Test environment

<a id="61-dynamodb-local-331"></a>

### 6.1 DynamoDB Local 3.3.1

Pin the image by digest (IP 4.1. 2026-10-05).

```text
amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab
```

Example of starting it (not executed in this document):

```sh
docker run --rm -p 8000:8000 \
  amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab \
  -jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry
```

- The region is `us-east-1`, and the credentials are dummies. Specify the endpoint explicitly in both the DynamoDB client and the Streams client. The basis is the record in the hub's `tools/spikes/dynamodb-emulators/` (`README.md`, `probe.py`).
- From Go tests, the idea is to use the same digest with a generic container of `testcontainers-go`. The current `go.mod` already has `testcontainers-go v0.44.0`. The LocalStack module becomes unnecessary, so it is removed after the migration.
- Specify `BaseEndpoint` explicitly in the SDK client. Specify the endpoint explicitly in the Streams client as well.
- The region in the ARN of Streams is `ddblocal`. Do not infer the region from the ARN.

<a id="62-今の試験localstackからの移し方"></a>

### 6.2 How to migrate from the current tests (LocalStack)

The current tests use `localstack/localstack:2.1.0` (`test/event_store_on_dynamodb_test.go`, `test/event_store_on_dynamodb_regression_test.go`, `test/user_account_repository_test.go`). The table creation helpers are in `pkg/common/dynamodb.go` (`CreateJournalTable`, `CreateSnapshotTable`, `CreateDynamoDBClient`).

1. `pkg/common` and the old LocalStack tests keep running as they are until the PR that removes the old API (item 13 of 7.2).
2. The new test foundation (starting DynamoDB Local, and helpers for creating the three tables, the GSI, Streams, and TTL) is newly created under `internal/` (item 4 of 7.2).
3. The intent of the current regression tests (reading all items, retaining new history) is carried over in the PRs for writes, reads, and retention of the new API (items 10 and 11 of 7.2).
4. In the PR that removes the old API (item 13 of 7.2), remove the old tests, `pkg/common`, and the LocalStack module (IP 4.2).

<a id="63-メモリ固有の試験"></a>

### 6.3 Memory-specific tests

In addition to the conformance data, add the following tests to the memory implementation (item 8 of 7.2).

- MEM-2: Separate `NewStore` calls are independent. Only the `New` instances given the same `*Store` share the records, the configuration, and the lock.
- MEM-4: When the same seq_nr is appended concurrently from multiple goroutines, exactly one succeeds and the others become optimistic locks.
- MEM-4: Even when a read and an append overlap, the read never sees an intermediate state before commit.
- MEM-6: Changing an input value or a retrieved result does not change the stored value.
- In CI, run the memory package and the conformance runner with `go test -race`. The current `make test` does not add `-race`, so add it in the `conformance` job (5.6) or in a separate memory-specific step.

---

<a id="7-main-への入れ方"></a>

## 7. How to bring this into main

<a id="71-方針"></a>

### 7.1 Policy

- Squash-merge each PR into main (IP 3). The development build of the next major version is provided by fetching main, whose module path is `/v5`, as a pseudo-version (1.4).
- The worker does everything up to creating the PR. The coordinator does the merge (IP 9).
- One PR corresponds to one rule group (IP 4.2).
- The first PR is the preparation for the next major version (changing the module path).

<a id="72-pr-の列"></a>

### 7.2 Sequence of PRs

| Order | Scope | Corresponding rule group | Notes |
|---|---|---|---|
| 1 | Preparation. Change the path in `go.mod` to `/v5` and follow it in all imports. Do not change `version` (1.4) | IP 3, IP 4.2 | The existing implementation keeps working. CI passes as it is now |
| 2 | Runner 1: data loading, `manifest` check, the value table, the report, the required-list file, and the CI job `conformance` | CR, IP 5 | A storage backend that is not connected is "unverified". It is not reported as success. With the mechanism of 7.4, CI does not fail |
| 3 | Runner 2: the scene runner, the framework for faults and hooks (`internal/testhook`), and the counting of firings | CR | The hooks define only the types and the registration entry points. The types use only the aid string, the seq_nr, and bytes, and do not depend on the core types |
| 4 | The DynamoDB Local foundation and observation (container, creation of the three tables, recording of SDK requests, reading of items) | IP 4.1 | Newly created under `internal/`. The old tests and `pkg/common` keep running as they are (6.2) |
| 5 | Core 1: error classes and validation of value types (`SeqNr`, `AggregateID`, `Kind`) | T-1, T-9, T-11 to T-13, E-1 to E-3 | Coexists with the old implementation in a separate package |
| 6 | Core 2: envelopes and serializers | T-2 to T-8, T-10 | |
| 7 | Core 3: operation interface, `SnapshotRead`, `Option` | W, R, S-1 | |
| 8 | Connect memory to the common scenes, `go test -race`, and memory-specific tests (6.3) | MEM-1 to MEM-13 | Add the target cases to the required list. Implement the places that call the hooks (candidate selection of the logical history, deletion, before commit, reads). The dependency direction is "memory → `internal/testhook` ← runner" |
| 9 | DynamoDB: three tables, layout check (`layout.json`), configuration check | DY-2, DY-3, DY-8, DY-16 to DY-19, D-1 to D-4 | Matches `layout.json` and `item-shapes.json` |
| 10 | DynamoDB: writes and reads | D-5 to D-7, DY-9 to DY-11 | Add checks for faults, requests, and attributes. Carry over the intent of the old regression tests (6.2) |
| 11 | DynamoDB: retention | S-1 to S-4, D-9, P-18, P-24 | Include the TTL scenes. Make the checks of faults and requests mandatory |
| 12 | Documents (README, DATABASE_SCHEMA, MIGRATION_GUIDE, migration guidance) and migration of the usage examples | IP 5-5, IP 5-6, IP-D8 | The IAM example for the real service is also written here (4.4) |
| 13 | Remove the old API (including the old tests, `pkg/common`, and the LocalStack module) | — | Remove it after migrating all places that use the old API |

- Change feed helpers are not included in the first major version (1.3), so they are not in the sequence of PRs.
- The layout check (`layout.json`) is included in the PR of item 9.

<a id="73-旧-api-との共存と削除の時点"></a>

### 7.3 Coexistence with the old API and the point of removal

- From item 1 to item 12, the old API (`pkg`) and the new API temporarily coexist. The coexistence is a migration work to keep passing the CI of main, and it is not a compatibility promise to users.
- The new API is placed in a package separate from the old `pkg` package. The `eventstore` at the module root and the old `pkg` (`/v2/pkg` in the module path during the coexistence period) are separate packages, so types with the same name do not collide between old and new (9.1). The module path moved to `/v5` after `pkg` was removed, so `/v5/pkg` does not exist.
- The old API is removed in item 13. Remove it after migrating all places that use the old API (`test/` and the usage examples). The official `v5.0.0` keeps no fallback, alias, or conversion for the old format.
- The official release is made after the acceptance conditions (IP 5) are achieved and the owner approves. `release.yml` is manual-start only, and this control is maintained.

<a id="74-各-pr-で-main-の-ci-を通し続ける方法"></a>

### 7.4 How to keep passing the CI of main in each PR

- Maintain the existing `lint` (manifest check and `make vet`), `test` (`make test`), and `release-checks`.
- Place the required-list file `internal/conformance/required.json`. List the case IDs per storage backend, and leave it empty in the PR of item 2. Even if all cases are "unverified", the list is empty, so CI does not fail.
- Add the CI job `conformance` to the `needs` of `ci-success` (the PR of item 2). The job fails only in the following cases.
  - The data or the `manifest` does not match.
  - A case in the list becomes "failure" or "unverified", or a registered fault does not fire (5.4).
  - An error of the runner itself.
- Cases not in the list are only reported and do not fail CI. Later PRs add the cases of the rule group they connect to the list. Removing a case from the list goes through the coordinator's review.
- Tests that need Docker are split into the job `conformance`, separate from `test` (the time limit of the current `test` is 10 minutes).

---

<a id="8-移行の案内"></a>

## 8. Migration guidance

The guidance for users of the current major version (v1.x) is written in `docs/MIGRATION_GUIDE.md` and `docs/MIGRATION_GUIDE.ja.md` in the PR of item 12. This document decides the content.

<a id="81-コードの移行"></a>

### 8.1 Migrating the code

1. Change the import path to `github.com/j5ik2o/event-store-adapter-go/v5`.
2. Pass a `context.Context` to the calls.
3. Create the aggregate ID with `AggregateID` (type name and value). The library builds the string representation.
4. Pass events and snapshots in envelopes (`EventEnvelope`, `SnapshotEnvelope`). Separate the metadata and the payload.
5. Stop the comparison that uses `version`. The comparison is decided from `event.SeqNr()`. The version argument of `PersistEvent` and `IsCreated` are abolished.
6. Number `seq_nr` as a sequence starting from 1. 0 is a contract violation (W-6).
7. Restore by calling `GetEventsByIDSinceSeqNr` from `Snapshot.SeqNr() + 1` if `GetLatestSnapshotByID` returned a snapshot, and from 1 if not (R-3, R-4). The head seq_nr is not used as the start position of restoration. It can be used to confirm that restoration reached the head.
8. Distinguish the five classes of errors with `errors.As` or `KindOf`. Remove processing that depends on panic.
9. Move the configuration to `Config` and `Option`. A retention count of 0 is a configuration error. `WithKeepSnapshot(false)` corresponds to a retention count of "none".
10. Remove `KeyResolver` and the number of shards.

<a id="82-旧データの扱いip-d8"></a>

### 8.2 Handling old data (IP-D8)

- For the old Go layout (two tables, the sharded PK, and the snapshot version), **no migration tool is provided. Only a procedure document is prepared.**
- The flow of the procedure document (draft):
  1. Read the events and snapshots with the old API.
  2. The user separates the metadata and the payload and builds new envelopes.
  3. Create the new layout (three tables and the configuration items), and write again in order from seq_nr 1 with the new API.
- The unit of the old `occurred_at` is not determined by the type (the current `GetOccurredAt() uint64`). The user confirms the definition and converts it to `time.Time`. The document does not guess the unit or the lost precision.
- The DynamoDB migration tool of rs v3 (chapter 11 of the hub's `dynamodb.md`) is not applied to Go as it is.

---

<a id="9-判断が要る点"></a>

## 9. Points that need a decision

The items in this chapter were decided on 2026-10-06. The decider and the reason are kept. The tables of options are kept as supplementary material for the reasons.

<a id="91-パッケージ名と構成pkg-と-v5"></a>

### 9.1 Package name and layout (`pkg` and `/v5`)

**Decision**: B. Place the envelope, error, and operation types at the module root (`eventstore`), and split the storage backends into the `memory` and `dynamodb` packages. Test helpers are placed under `internal/`.
**Decided by**: the owner (2026-10-06).
**Reason**: The dependency direction is clear (storage backend → core), and users who use only memory do not get an extra import of the AWS SDK. The names express the content. They are separate packages from the old `pkg`, so type names do not collide during the coexistence period (7.3).

| Option | Content | Advantage | Disadvantage |
|---|---|---|---|
| A. Keep `pkg` | `github.com/j5ik2o/event-store-adapter-go/v5/pkg` | The import change is only `/v5` | `pkg` is a name that does not express the content. Memory and DynamoDB are mixed in the same package, and even users who do not use the AWS SDK get it |
| B. Core at the root, storage backends in separate packages (decided) | Root `eventstore` (core), `memory`, `dynamodb` | The dependency direction is clear. The names express the content | More import changes |
| C. Put the core in a dedicated `core` package and leave the root empty | `/v5/core`, `/v5/memory`, `/v5/dynamodb` | No name collision at the root | There are no types at the root, so it is hard to discover |

<a id="92-欠番"></a>

### 9.2 Missing number

It was decided that change feed helpers are not included in the first major version (owner decision of 2026-10-06, IP 10). A decision among options is no longer needed, so this item is a missing number. The numbers of the other items are not changed. The handling in the body is in 1.3 and 4.8.

<a id="93-seqnr-を符号付きにするか"></a>

### 9.3 Whether `SeqNr` is signed

**Decision**: A, `int64`.
**Decided by**: the lead (2026-10-06).
**Reason**: The negative-number cases and the 2^53 cases of the conformance data can be executed, and "not representable" in the report decreases. Out of range can be returned as a contract violation (T-9).

| Option | Content | Advantage | Disadvantage |
|---|---|---|---|
| A. `int64` (decided) | Signed | Negative numbers and 2^53 can be represented by the type | A negative number can be represented as a type, so validation is needed |
| B. `uint64` (same as now) | Unsigned | The type prevents negative numbers | The negative-number cases become "not representable" |

<a id="94-payload-の型の表し方"></a>

### 9.4 How to represent the payload type

**Decision**: A, the generic `EventEnvelope[E]`.
**Decided by**: the lead (2026-10-06). T-6 allows either.
**Reason**: T-6 imposes no type requirement on the payload, the `any` constraint is enough, and type safety is gained. For multiple types, the user builds one sum type (branching by an interface and the manifest).

| Option | Content | Advantage | Disadvantage |
|---|---|---|---|
| A. Generics (decided) | Type parameter | Type safe. No type conversion is needed when retrieving | The type parameter spreads over the whole API |
| B. `any` | Runtime type | Easy to handle multiple types in one store | Not type safe. Type assertions are needed |

<a id="95-getlatestsnapshotbyid-の命名"></a>

### 9.5 Naming of `GetLatestSnapshotByID`

**Decision**: A, `ID`.
**Decided by**: the lead (2026-10-06).
**Reason**: It fits Go convention. It can be done at the same time as the major replacement, so even if migration is needed anyway, it adds no extra burden.

| Option | Advantage | Disadvantage |
|---|---|---|
| A. Align to `ID` (decided) | Fits Go convention | Changes from the current name |
| B. Keep the current `Id` | No name change | Departs from Go convention |

<a id="96-保持処理の通知経路"></a>

### 9.6 Notification path for retention

**Decision**: A. Make the log (`log/slog`) mandatory, and publish the callback (`WithRetentionFailureHandler`) as an additional path. For both memory and DynamoDB.
**Decided by**: the lead (2026-10-06). It follows S-4, MEM-11, and MEM-D7.
**Reason**: The conformance check (`observe.notifications`) and the user's monitoring can be received reliably, and the mandatory log of memory is also kept.

| Option | Content | Advantage | Disadvantage |
|---|---|---|---|
| A. Publish the callback in addition to the log (mandatory) (decided) | 2.9 | Users can connect the result to their own mechanism. The log is always emitted | One more option |
| B. Only the `slog` log | Does not grow the public API | Minimal | To capture it in tests, the log handler must be replaced |

---

<a id="10-未解決の疑問"></a>

## 10. Open questions

There are no open questions at present. The old items 1 to 9 were reflected in the body through the lead's review (the first round) and the decisions of 2026-10-06. The questions that arose afterwards were also reflected in the body through the specification decisions of 2026-10-06 (P-42 to P-44. P-45 is the migration procedure for rs, so it is not relevant).

Questions already reflected:

- There are 12 phases of faults (5.4).
- `fnv1a64` is out of scope in the first major version (5.1).
- For the pair of a retention count of "none" and `retention_mode`, the mode is ignored (2.8).
- The default number of re-requests for the configuration check is 10 (2.8).
- The phases of faults that exist in memory scenes are as in 5.4.
- A zero-value envelope and a `nil` `AggregateID` are contract violations (2.1. T-2 and T-10 of the common contract, P-42).
- The priority order when cancellation reasons are attached to multiple items (4.5. `dynamodb.md` 6.2, P-43).
- Re-reading when `TransactionConflict` is returned while creating the configuration items (4.4. DY-8, P-44).
- The estimate of D-7 is a generous estimate (4.5).
- The startup options of DynamoDB Local are as in 6.1.
- The test hooks are placed in `internal/testhook` (5.3).

If a point arises while writing where the reading of the specification is unclear, write it here.
