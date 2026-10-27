> この文書は [BIGTABLE_DATABASE_SCHEMA.md](BIGTABLE_DATABASE_SCHEMA.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Bigtable の保存先のスキーマ

Go の`bigtable`パッケージ（`github.com/j5ik2o/event-store-adapter-go/v5/bigtable`）が使う5系の実配置を説明します。ライブラリが設定の行を作るのは、明示的な`bigtable.Initialize`だけです。通常の`bigtable.New`は、テーブル・列ファミリ・設定の行を作りません。テーブルと列ファミリは、常に利用者が作ります。配置は Rust v5 の実装と同じです。

通常のstoreはこの配置だけを読み取ります。v5より前の Go ライブラリには Bigtable の保存先がなかったので、移行するものはありません。[移行ガイド](MIGRATION_GUIDE.ja.md)を参照してください。この保存先の最初の版は、変更フィードを提供しません。

## 配置

### テーブルとキー

1つのBigtableインスタンスに、journalテーブルとheadテーブルの2つのテーブルを使います。2つは別のテーブルでなければならず、同じテーブルIDを両方に指定すると設定エラーです。

| テーブル | 列ファミリ | 行キー | 行 |
|:---------|:-----------|:-------|:---|
| journal | `event`・`history`・`config` | `P(aid) \|\| U64BE(seq_nr)` | イベントごとに1行と、設定の行 |
| head | `head`・`snapshot`・`config` | `P(aid)` | 集約ごとに1行と、設定の行 |

`aid`は`AggregateID`から組み立てるUTF-8文字列`type_name-value`です。型名にハイフンは使えませんが、値には使えます。文字列全体は1024バイト以下です。型名や値が空でもかまいません。

```text
P(aid)             = H(aid) || L16(length(A)) || A
headの行キー       = P(aid)
journalの行キー    = P(aid) || U64BE(seq_nr)
設定の行のキー     = 0x00   （1バイト。両方のテーブルで同じ）
```

- `A`は`aid`のUTF-8のバイト列です。
- `H(aid)`は、`A`の[FNV-1a 64](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)ハッシュを、8バイトのビッグエンディアンにした値です。
- `L16`は、`A`のバイト長を符号なし16ビットのビッグエンディアンにした値です。長さがキーに含まれるので、2つのハッシュが衝突しても、別の集約のキーが`P(aid)`で始まることはありません。
- `U64BE(seq_nr)`は、シーケンス番号を符号なし64ビットのビッグエンディアンにした値です。キーの順序がシーケンスの順序と一致します。範囲を作るために、10進文字列・言語の`Display`の出力・`prefix + 0xff`は使いません。
- `aid`に含まれる`#`・NUL・非ASCIIの文字は、ただのバイトです。区切り文字としては解釈しません。
- 設定の行のキー`0x00`は1バイトなので、データの行のキー（先頭に8バイトのハッシュと2バイトの長さが必ず付く）と一致することはありません。

#### 列ファミリとガベージコレクション

最初の`bigtable.Initialize`の前に、すべての列ファミリを作ってください。次の`cbt`コマンドは、例の名前`esa_journal`と`esa_head`の2つのテーブルと、すべての列ファミリを、上のガベージコレクションの規則で作ります。

<!-- bigtable-schema:tables -->
```sh
cbt createtable esa_journal
cbt createfamily esa_journal event
cbt createfamily esa_journal history
cbt createfamily esa_journal config
cbt setgcpolicy esa_journal event maxversions=1
cbt setgcpolicy esa_journal history maxversions=1
cbt createtable esa_head
cbt createfamily esa_head head
cbt createfamily esa_head snapshot
cbt createfamily esa_head config
cbt setgcpolicy esa_head head maxversions=1
cbt setgcpolicy esa_head snapshot maxversions=1
```

ライブラリが要求する規則は次のとおりです。

| テーブル | 列ファミリ | ガベージコレクションの規則 |
|:---------|:-----------|:---------------------------|
| journal | `event` | 最大1版を残す。年齢による規則はなし |
| journal | `history` | 最大1版を残す。年齢による規則はなし |
| journal | `config` | 版数の規則は任意。年齢による規則はなし |
| head | `head` | 最大1版を残す。年齢による規則はなし |
| head | `snapshot` | 最大1版を残す。年齢による規則はなし |
| head | `config` | 版数の規則は任意。年齢による規則はなし |

`bigtable.New`は、各テーブルのschemaを（管理用クライアントの）`GetTable`で読み、列ファミリが欠けている場合と、データの列ファミリの規則が「最大1版を残す」だけではない場合を、設定エラーとして拒否します。データの列ファミリに年齢による規則を設定しないでください。期限切れのセルがデータを失わせます。`config`の列ファミリにも年齢による規則を設定しないでください。設定の行はセルの時刻0で書くので、年齢による規則があるとすぐに期限切れになり、以後の`New`と初期化の確認が失敗します。正しさは、ガベージコレクションがいつ走るかに依存しません。読み取りは常に列ごとの最新の1セルだけを求め、書き込みはセルの時刻を0にして、置き換える列の古い版を削除するからです。

## 設定

### 設定の行

各テーブルは、行キー`0x00`の設定の行を1つ持ち、列`config:metadata`にUTF-8のJSONオブジェクトを保存します。値は2つのテーブルで同じです。

| フィールド | 型 | 要件 |
|:-----------|:---|:-----|
| `store_id` | 文字列 | ハイフン区切りの小文字のUUID。初期化のときに生成する |
| `layout_version` | 整数 | 1 |
| `keep_snapshot_count` | nullまたは文字列 | 「今のsnapshotだけ」なら`null`。それ以外は、指数表記も先頭のゼロもない10進の文字列で書いた正の整数 |
| `retention_mode` | 文字列 | `delete` |
| `envelope_limit_bytes` | 整数 | 17以上8388608以下。既定は1048576 |

`keep_snapshot_count`だけが文字列なのは、件数が符号付き64ビットの大きさまで取りえて、倍精度のJSONの数値では正確に表せない値があるためです。

`New`は、2つのテーブルの列ファミリを照合し、続けて2つの設定の行を読みます。行がない、JSONとして読めない、キーが重複している、フィールドが欠けている、フィールドの型や形が違う場合は、設定エラーです。比較は値で行い、キーの順序や空白は見ません。2つの行の間に違いがある、`layout_version`が1でない、`retention_mode`が`delete`でない、`store_id`が小文字のUUIDでない、またはクライアントの設定（履歴件数と`Config.EnvelopeLimitBytes`）と違う場合も、設定エラーです。`New`が設定の行を修復したり補ったりすることはありません。

要求を送る前に、設定を検査します。nilのクライアント、空または同じ2つのテーブルID、期限切れ方式、`Config.ChangeFeed`、上の範囲の外の封筒の上限は設定エラーです。封筒の上限の`0`は既定の1048576を表します。

## 初期化

データの行も設定の行もない新しい2つのテーブルを、最初の`New`の前に、書き手がいない状態で1回だけ初期化します。プログラムは`Initialize`を（たとえば準備用のコマンドから）一度だけ呼び、初回を含むすべての起動で`New`を呼びます。

```go
keep, err := eventstore.KeepLatest(2)
if err != nil {
    return err
}
cfg := bigtable.Config{JournalTableID: "esa_journal", HeadTableID: "esa_head"}
if err := bigtable.Initialize(ctx, data, cfg, eventstore.WithRetentionCount(keep)); err != nil {
    return err
}
```

`Initialize`は、設定とテーブルIDを検査し、2つのテーブルの設定の行を読みます。どちらかに既にあれば、何も書かずに設定エラーを返します。なければ、1つの`store_id`を生成し、同じJSONを、先にjournalテーブル、次にheadテーブルへ書きます。どちらも、行の`config:metadata`がまだないときだけ成功する条件付きの書き込みです。テーブル・列ファミリ・データの行は作りません。また、列ファミリの照合もしません。列ファミリが欠けていれば、書き込みが保存先エラーで失敗します。誤った列ファミリや規則を拒否するのは`New`です。

設定の行を手作業で用意する場合は、好きなBigtableのクライアントで、上のJSONを両方のテーブルの行キー`0x00`の`config:metadata`へ書きます。2つの値は同じでなければならず、列ファミリを先に作っておく必要があります。

### 中断した初期化の回復

2つの設定の行は1つずつ順に書かれ、Bigtableには行をまたぐトランザクションも、テーブルをまたぐトランザクションもありません。1つ目を書いた後で呼び出しが止まる（クラッシュ、接続の切断、2つ目のテーブルからの誤り）と、journalテーブルにだけ設定の行が残ります。ライブラリは、足りない行を自動では補いません。ライブラリが今書いたのではない設定は信頼できないからです。この状態では、`Initialize`は（すでに行があるため）設定エラーを返し、`New`は、headテーブルに行がないため設定エラーを返します。

回復の手順は次のとおりです。

1. 2つのテーブルへの書き手と読み手を、すべて止めたままにします。
2. 両方のテーブルで、行キー`0x00`の行を読みます。両方にあれば、初期化は終わっているので、何も削除しません。片方にだけあれば、どちらのテーブルにもデータの行がないこと、つまりstoreがまだ一度も使われていないことを確認します。データの行があれば、中断した初期化ではなく、この手順の対象外です。行を削除しないでください。
3. 行がある側のテーブルから、残った行キー`0x00`の行を削除します（`DeleteFromRow`の変更を使った`MutateRow`です）。Go のクライアントでは次のとおりです（`gbigtable`は`cloud.google.com/go/bigtable`です）。

```go
m := gbigtable.NewMutation()
m.DeleteRow()
err := data.Open("esa_journal").Apply(ctx, "\x00", m) // 行がある側のテーブル
```

4. 同じ設定で`Initialize`をもう一度呼びます。

## 記録

### Journalの行

イベント1件が、journalテーブルの1行になります。行は`event:envelope`がないときだけ作り、上書きも削除もしません。

| セル | 値 |
|:-----|:---|
| `event:envelope` | 行キーのシーケンス番号のイベント封筒。削除しない |
| `event:has_snapshot` | その追記がsnapshotを伴うならASCIIの`1`、そうでなければASCIIの`0`。削除しない |
| `history:state` | 行に履歴snapshotを初めて書いたときはASCIIの`active`、削除した後は`retired`。履歴を書かなかった行にはない |
| `history:envelope` | 履歴snapshotの封筒。状態を`retired`にするときに削除する |

履歴件数を設定していない場合は、`history`のセルを書きません。セルの時刻は0（固定値）です。これはBigtable自身の管理にだけ使い、`occurred_at`をセルの時刻から求めることはありません。

### Headの行

headテーブルは、集約ごとに小さな1行を持ちます。全イベントや`n`件の履歴snapshotは置かないので、履歴件数が増えてもheadの行は育ちません。

| セル | 値 |
|:-----|:---|
| `head:seq_nr` | 確定した最後のシーケンス番号を、ゼロ詰めの20桁のASCII数字にしたもの。headの照合に使う |
| `head:last_event` | そのシーケンス番号のイベント封筒 |
| `head:has_snapshot` | その追記がsnapshotを伴うならASCIIの`1`、そうでなければASCIIの`0` |
| `snapshot:current` | 今のsnapshot封筒。集約にsnapshotがなければセルもない |

`head:has_snapshot`が`1`のとき、`snapshot:current`のシーケンス番号は`head:seq_nr`と等しくなります。イベントだけの追記は、既存の`snapshot:current`を残したまま、`head:has_snapshot`を`0`にします。snapshotの番号がheadの番号を超えることはありません。

### 封筒

ライブラリは、封筒を次の固定のバイナリ形式で組み立てます。`U16`・`U32`・`U64`はビッグエンディアン、`I64`はビッグエンディアンの符号付き64ビットの2の補数です。先頭の`0x01`は形式の版番号です。serializerが受け取るのはpayloadだけです。

```text
event    = 0x01 || U16(len(aid)) || aid || U64(seq_nr)
           || I64(occurred_at_ns) || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
snapshot = 0x01 || U64(seq_nr)
           || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
```

長さはバイト数です。読むときに、使われていない末尾・長さの不一致・未知の形式の版があれば、保存先エラーです。正しい封筒のpayloadを復元できない場合は、直列化エラーです。発生時刻はナノ秒で封筒に持ち、Bigtableのセルの時刻（マイクロ秒の精度）には持ちません。

#### 封筒の大きさの上限

`Config.EnvelopeLimitBytes`は、メタデータを含む直列化後の1封筒の上限です。既定は1MiB（1,048,576バイト）、最大は8MiB（8,388,608バイト）、最小は17バイト（最小のsnapshot封筒）です。値は2つの設定の行に保存され、すべてのクライアントが同じ値を使う必要があります。上限を超える入力は、書き込みの前に、シーケンス番号を含む規則`BT-16`の`ContractViolationError`として拒否します。上限を変える場合は、全クライアントを止め、2つの設定の行を同じ新しい値に更新してから、全クライアントを開き直して照合します。自分の gRPC 接続の上に作ったデータのクライアントは、上限に行の大きさを足した大きさのメッセージを受け取れるように設定してください（gRPC の既定は4MiBです）。

## 書き込みと読み取り

入力の検査と直列化は、要求を送る前に終えます。その後の`PersistEvent`と`PersistEventAndSnapshot`は、次の手順で動きます。

1. 集約のheadの行を強整合で読みます。行がなければ、今のシーケンス番号`h`は0です。あれば、`h`と最後の封筒を検証します。必要なセルが欠けた行を、新しい集約とみなすことはありません。
2. 入力のシーケンス番号を`h`と比べます。次の連続した番号だけが続きます。
3. `h > 0`なら、headの最後のイベント`h`を、journalの行`h`へ、1回の条件付きの書き込みで書きます（履歴件数が設定され、headが`has_snapshot = 1`なら、`h`のsnapshotも同じ書き込みで書きます）。この書き込みを確認できなければ、追記は保存先エラーになり、headは進みません。
4. headの行に条件付きの書き込みをします。新しい集約は`head:seq_nr`がない側で作ります。更新は、`head:seq_nr`が手順1で読んだ`h`である側だけで成功し、`head:seq_nr`・`head:last_event`・`head:has_snapshot`（snapshotを伴うなら`snapshot:current`も）を置き換えます。**この条件付きの書き込みが確定点です。**
5. 新しい最後のイベントをjournalへ書き、保持の処理を行います。ここでの失敗は通知して後回しにし、確定した追記を失敗に変えることはありません。

確定点の書き込みが、手順1の後に別の書き込みが確定していたために適用されなかった場合は、楽観ロックのエラーです。新しい集約の書き込みが、先行する作成に負けた場合も同じです。確定点の書き込みの応答を失った場合は、失敗したと断定しない保存先エラーです。次に期待する番号を飛ばす番号（または1以外の最初のイベント）は契約違反です。

これにより、1つの不変条件が保たれます。headが`h`のとき、イベント`1`から`h - 1`はjournalにあり、イベント`h`はheadにあります。追記に付いたsnapshotは、次の追記の前にjournalの行へ書かれるので、まだ書かれていない最新のsnapshotは、常に`snapshot:current`にあります。

`GetLatestSnapshotByID`は、headの行を1回読み、`head:seq_nr`と、あれば`snapshot:current`を返します。headなしは`nil`、snapshotのないheadは、snapshotがnilでheadのシーケンス番号を持つ`SnapshotRead`です。

`GetEventsByIDSinceSeqNr`は、最初にheadを読み、その`h`と最後のイベントを呼び出しの間ずっと使います。`start = max(1, seq_nr)`として、次のように分かれます。

1. headがない、または`start > h`: 空のリスト。journalへの要求はしません。
2. `start == h`: headの最後のイベントだけ。journalへの要求はしません。
3. `start < h`: `start`から`h`の手前までのjournalの行（`P || U64BE(start)`から`P || U64BE(h)`まで）と、その後ろにheadの最後のイベント。

呼び出しの途中でheadが進んでも、読み直しません。`h`以上の行は混ぜません。途中で終わった`ReadRows`のストリームは、最後に完了した行の次から再開し、不完全な行は読み直します。要求した範囲の中の欠番は保存先エラーです。

## 保持

`keep_snapshot_count`は、保持件数がない（または`NoRetention()`）なら`null`、`KeepLatest(n)`なら`n`です。`null`なら履歴を保持せず、`snapshot:current`だけがsnapshotを持ちます。`n`なら、履歴は、集約のjournalの行にある`active`な`history`のセルと、まだjournalの行にないheadの今のsnapshotの組です。つまり、headのsnapshotも`n`件に数えます。`snapshot:current`は、保持の処理で削除しません。

snapshotを伴う追記が確定した後、storeはheadの位置`h`を固定し、集約のjournalの行を`P || U64BE(1)`から`P || U64BE(h + 1)`の手前まで読んで、`history:state`が`active`の行を一覧にします。`h`がsnapshotを伴い、まだ行がない場合は、`h`も一覧に加えます。`retired`の行を`active`に戻すことはありません。新しい`n`件を残し、それより古い各行は、`history:state`がまだ`active`のときだけ成功する1回の条件付きの書き込みで退役させます。`history:envelope`を削除し、`history:state`を`retired`にします。`event`のセルは残します。読むのは状態の列だけなので、履歴のpayloadは転送しません。

退役させた行を、もう一度埋めることはありません。遅れた書き手が同じ履歴をコピーしようとしても、`event:envelope`が既にあるので何も書かず、削除した履歴は戻りません。保持が失敗しても、aidとerrorを含む`log/slog`のエラーを出し、`WithRetentionFailureHandler`を呼ぶだけで、確定済みの追記は成功し、次のsnapshotを伴う追記でやり直します。ログの経路やハンドラでpanicが起きても回収します。保持の費用は、集約のjournalの長さに応じて増えます。TTL方式は提供しません。`retention_mode`は常に`delete`です。

### 履歴件数を変更する

`keep_snapshot_count`の変更は、停止して行う管理手順です。2つの設定の行に保存された値と、全クライアントの保持のオプションが一致する必要があります。

1. storeを使う全クライアントを止め、進行中の追記・読取・書き出し・保持の処理がないことを確認します。変更前と変更先の設定と、進捗を記録し、中断しても再開できるようにします。
2. すべてのheadの行（キー`0x00`の行を除くすべての行）について、`h`と最後のイベントを読み、journalの行`h`の書き出しを、**変更前の**履歴件数で完了させます。変更前に履歴を保持していて、最後のイベントがsnapshotを伴うなら、そのsnapshotも同じ最初の書き込みに含めます。まだ書かれていないsnapshotが、新しい選択から落ちるのを防ぐためです。
3. 集約ごとに、`h`までの`active`な履歴をすべて読みます。変更先の新しい`n`件を残し（変更先が`null`なら残さず）、残りを上の条件付きの書き込みで退役させます。イベントの封筒とフラグ、head、`snapshot:current`は変更しません。`retired`の状態は消さず、履歴は再作成しません。
4. 全集約が済んだら、両方のテーブルの設定の行の`keep_snapshot_count`を変更先の値に更新します。全クライアントを新しいオプションで開き直して設定を照合してから、処理を再開します。

途中で失敗したら、クライアントを止めたまま、同じ変更先で再実行します。済んだ書き出しや退役を繰り返しても、履歴は戻りません。2つの設定の行の片方だけを更新した場合は、両方が一致するまで再開しません。削除した履歴は戻せないので、storeの全体を元に戻すことは前提にしません。件数を増やす場合や、なしから件数を設定する場合も、削除済み・未保持の過去の履歴は補いません。新しい設定は、次のsnapshot書込から適用されます。

## 運用

### App profile・接続・認証

稼働中のシステムの読み取り・書き込み・保持・設定の照合は、すべて、1つのクラスタに固定し、単一行トランザクションを許可するapp profileを使ってください。インスタンスにレプリカのクラスタがある場合は、アダプターの操作をその1つのクラスタに向けます。そのクラスタの停止中は、storeが保存先エラーを返し、復旧を待ちます。複数クラスタへの書き込み、複数クラスタへのルーティング、自動フェイルオーバーは対象外です。ライブラリはapp profileを検査しません。利用者が作り、データのクライアントを作るときに`ClientConfig.AppProfile`で渡します。空のIDはインスタンスの既定のapp profileを使うので、それがこれらの要件を満たすときだけ正しい設定です。

ライブラリは`cloud.google.com/go/bigtable`の2つのクライアントを受け取り、作ることも閉じることもしません。

| クライアント | 用途 |
|:-------------|:-----|
| `*bigtable.Client`（データ） | `ReadRows`と`CheckAndMutateRow`だけ |
| `*bigtable.AdminClient` | `GetTable`（`TableInfo`）。`New`がstoreを開くとき、列ファミリを照合するためだけ |

- **エミュレーター**: 2つのクライアントを、利用者が自分でエミュレーターへ接続します（SDKが認識する`BIGTABLE_EMULATOR_HOST`を設定するか、平文の接続の`option.WithGRPCConn`を渡します）。認証情報は付きません。
- **本番のCloud Bigtable**: 2つのクライアントを、利用者の認証（たとえばアプリケーションのデフォルト認証情報や`option.WithTokenSource`）で作ります。認証はクライアントの責務です。ライブラリは認証情報を付けず、トークンの保持も更新もしません。認証情報が失敗すると、要求は送られず、その操作は保存先エラーを返します。
- **再試行**: 条件付きの書き込みは、ライブラリもSDKも再試行しません。応答が失われると、書き込みが適用されたかどうか分からない保存先エラーを返します。同じ追記をもう一度送り、適用済みなら楽観ロックのエラーになると考えてください。
