# API surface check (Go)

`surface.json` is a verbatim copy of the language-neutral API surface defined in
[j5ik2o/event-store-adapter](https://github.com/j5ik2o/event-store-adapter/tree/main/api-surface)
(surface format 0.1.0, adopted in stages; not yet an acceptance criterion). CI verifies its SHA-256 in the `lint` job.
`go.json` maps the common names to the public API of the root package `eventstore`.

## How to run

```sh
go test ./internal/apisurface/... -v   # also runs as part of `make test`
```

`internal/apisurface` type-checks the root package with `go/types`, reads
`surface.json` and `go.json`, and reports every difference as
`<surface path>: <message>`. A surface item without a mapping in `go.json` is reported
too, never skipped.

## What is checked

- Each data type exists with the mapped type parameters, and each element is readable
  (a method or a struct field) with a type that matches its value kind
  (for example `OccurredAt()` returns `time.Time`, `Payload()` returns `E`).
- A constructor accepts every `required: true` element as an argument and none of the
  `required: false` elements. Optional elements are variadic options (`...EnvelopeOption`).
- The four store operations exist on `EventStore[E, A]` with inputs and outputs that follow
  the mapping rules: a leading `context.Context`, a trailing `error`, `optional<T>` as `*T`,
  `list<T>` as `[]T`, and `none` as `error` only.
- The five error categories have a type that implements the `Error` interface.

`internal/apisurface/testdata/` holds small packages that each deviate from the surface in
one place; the tests assert that the check reports exactly that deviation.

## What is not checked

Behavior (validation rules, ordering, optimistic locking, rounding) is verified by the
conformance data under `conformance/`. The check also does not cover which operation returns
which error category, what `Kind()` returns, or the `memory` and `dynamodb` packages.

## Updating `surface.json`

Replace the copy with the new upstream file, then update the SHA-256 in
`.github/workflows/ci.yml` (`lint` job) and re-run the tests.
