# Contributing to event-store-adapter-go

[日本語](./CONTRIBUTING.ja.md)

## Overview

This guide is for developers who change this repository. For how to use the library, see [README.md](README.md).

## Shared contract and decisions

This library follows the common contract shared by all implementations. The specification and the design decisions are kept in the hub repository:

- Specification: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec>
- Design decisions (ADR): <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/adr>

Some files in this repository are copies of shared data, and CI checks them against SHA-256 values written in `.github/workflows/ci.yml` (the `lint` job):

- `conformance/manifest.json` (CI also runs `python3 tools/conformance/manifest.py verify`)
- `api-surface/surface.json` (to update it, follow [api-surface/README.md](api-surface/README.md))
- `user-docs/template.json` and `tools/user-docs/check.py`

## Local setup

- Go: `go.mod` declares `go 1.25.0` and `toolchain go1.27.3`. CI uses Go 1.27 (`actions/setup-go`).
- Python 3: CI uses Python 3.15 to run the scripts under `tools/` and the tests under `.github/`. `tools/user-docs/check.py` requires Python 3.10 or later.
- Docker: `make test` runs `./...`, which includes the tests in `internal/dynamodbtest`. They start DynamoDB Local in a container with testcontainers-go. The conformance runner for DynamoDB also uses a DynamoDB Local container. The SQLite tests (`sqlite/` and the SQLite part of the conformance runner) need no container: they use SQLite files in temporary directories and the pure Go driver `modernc.org/sqlite`, so no cgo or emulator is required.
- Download the dependencies with `make prepare` (`go mod download`).
- Install the tools with `make install-tools` (staticcheck and goimports, both `@latest`) or with `bash setup.sh` (staticcheck `@latest`). CI pins staticcheck to `v0.8.2`.

## Checks

CI (`.github/workflows/ci.yml`) runs the following. Run the same commands locally before opening a pull request.

| CI job | Commands |
|---|---|
| `lint` | `python3 tools/conformance/manifest.py verify`, the SHA-256 checks of the shared files listed above, `python3 tools/user-docs/check.py`, `make vet` (`go vet ./...`), `go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...` |
| `test` | `make test` (`go test -race -v ./...`) |
| `release-checks` | `python3 -m unittest discover -s .github -p 'test_*.py' -v`, `go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.7` and `actionlint` |
| `conformance` | `go test -count=1 -v ./internal/conformance/...` with the environment variable `CONFORMANCE_REPORT` set to the path of the report file (the test is skipped when it is not set). It runs the Memory, DynamoDB and SQLite storages; `internal/conformance/required.json` lists the cases each storage must pass, and a listed case that is not executed fails the job |

Other commands from the `Makefile`:

- `make fmt` runs `gofmt -s -w .` and rewrites files in place.
- `make check` runs `lint`, `vet`, `fmt`, and `test` in this order.
- The API surface check, `go test ./internal/apisurface/... -v`, is also covered by `make test`.

## Design history

[docs/design/next-major.md](docs/design/next-major.md) is the design record written while building v5. It is a record of that time, so check the README and the current code for the current behavior.

## Changes and releases

- Open pull requests against `main`. The `CI Success` job requires `lint`, `test`, `release-checks`, and `conformance` to succeed.
- Releases are started manually with the `Release` workflow (`.github/workflows/release.yml`, `workflow_dispatch`). Its inputs are `level` (`auto`, `patch`, `minor`, or `major`) and `version`. The workflow updates the `version` file and creates the tag and the GitHub Release, so do not edit `version` by hand.
- With `level=auto`, the bump is decided from Conventional Commits subjects: `feat` and `revert` give a minor bump, `!` after the type or a `BREAKING CHANGE:` or `BREAKING-CHANGE:` footer gives a major bump, and anything else gives a patch bump (`.github/semver-level.py`).
- Renovate opens dependency updates with the prefix `chore(deps):`. Minor, patch, pin, and digest updates are merged automatically (`renovate.json`).
