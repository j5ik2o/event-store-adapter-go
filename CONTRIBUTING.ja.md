> この文書は [CONTRIBUTING.md](CONTRIBUTING.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# event-store-adapter-go への貢献

## 概要

この案内は、このリポジトリを変更する開発者向けです。ライブラリの使い方は [README.ja.md](README.ja.md) を参照してください。

## 共通の契約と判断

このライブラリは、すべての実装が共有する共通契約に従います。仕様と設計判断はハブのリポジトリにあります。

- 仕様: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec>
- 設計判断（ADR）: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/adr>

このリポジトリの一部のファイルは共通データの写しで、CI が `.github/workflows/ci.yml`（`lint` ジョブ）に書かれた SHA-256 と照合します。

- `conformance/manifest.json`（CI は `python3 tools/conformance/manifest.py verify` も実行します）
- `api-surface/surface.json`（更新の手順は [api-surface/README.md](api-surface/README.md) に従います）
- `user-docs/template.json` と `tools/user-docs/check.py`

## ローカルの準備

- Go: `go.mod` は `go 1.25.0` と `toolchain go1.27.3` を宣言しています。CI は Go 1.27 を使います（`actions/setup-go`）。
- Python 3: CI は Python 3.15 で、`tools/` 以下のスクリプトと `.github/` 以下の試験を実行します。 `tools/user-docs/check.py` は Python 3.10 以上が必要です。
- Docker: `make test` は `./...` を実行し、`internal/dynamodbtest` の試験を含みます。この試験は testcontainers-go で DynamoDB Local のコンテナを起動します。DynamoDB の適合の実行器も DynamoDB Local のコンテナを使います。
- 依存は `make prepare`（`go mod download`）で取得します。
- 道具は `make install-tools`（staticcheck と goimports。どちらも `@latest`）または `bash setup.sh`（staticcheck の `@latest`）で入れます。CI は staticcheck を `v0.8.2` に固定しています。

## 検査

CI（`.github/workflows/ci.yml`）は次を実行します。変更提案を出す前に、同じコマンドを手元で実行してください。

| CI のジョブ | コマンド |
|---|---|
| `lint` | `python3 tools/conformance/manifest.py verify`、上に挙げた共通ファイルの SHA-256 の照合、`python3 tools/user-docs/check.py`、`make vet`（`go vet ./...`）、`go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...` |
| `test` | `make test`（`go test -race -v ./...`） |
| `release-checks` | `python3 -m unittest discover -s .github -p 'test_*.py' -v`、`go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.7` と `actionlint` |
| `conformance` | 環境変数 `CONFORMANCE_REPORT` に報告ファイルのパスを設定して `go test -count=1 -v ./internal/conformance/...`（未設定のときは試験が飛ばされます） |

`Makefile` のその他のコマンド:

- `make fmt` は `gofmt -s -w .` を実行し、ファイルをその場で書き換えます。
- `make check` は `lint`、`vet`、`fmt`、`test` をこの順に実行します。
- API の公開形の検査 `go test ./internal/apisurface/... -v` は、`make test` にも含まれます。

## 設計の経緯

[docs/design/next-major.ja.md](docs/design/next-major.ja.md) は、v5 を作る際の設計の記録です。当時の記録なので、現在の振る舞いは README と現在のコードで確認してください。

## 変更とリリース

- 変更提案は `main` に向けて出します。`CI Success` ジョブは、`lint`、`test`、`release-checks`、`conformance` のすべての成功を要求します。
- リリースは `Release` ワークフロー（`.github/workflows/release.yml`、`workflow_dispatch`）を手動で起動して行います。入力は `level`（`auto`、`patch`、`minor`、`major`）と `version` です。このワークフローが `version` ファイルを更新し、タグと GitHub Release を作るので、`version` を手で編集しないでください。
- `level=auto` のときは、Conventional Commits の件名から上げ幅を決めます。`feat` と `revert` は minor、型の後ろの `!` または `BREAKING CHANGE:` または `BREAKING-CHANGE:` のフッターは major、それ以外は patch です（`.github/semver-level.py`）。
- Renovate は、接頭辞 `chore(deps):` で依存の更新を出します。minor、patch、pin、digest の更新は自動でマージされます（`renovate.json`）。
