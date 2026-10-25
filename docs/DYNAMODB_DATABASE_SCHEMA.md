# DynamoDB storage schema

[日本語](DYNAMODB_DATABASE_SCHEMA.ja.md)

## Layout

This is layout version 1 for [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md), used by the Go `/v5` module; its major version 5 matches the contract revision. The caller provisions three distinct tables and the snapshot history GSI before `dynamodb.New`.

| Table | Partition key | Sort key | Additional configuration |
|---|---|---|---|
| journal | `aid` S | `seq_nr` N | No GSI, Streams or TTL |
| snapshot | `aid` S | `skey` N | History GSI: (`aid` S, `active_history_seq_nr` N), `KEYS_ONLY`; no Streams; enable TTL on `ttl` when using TTL retention |
| head | `aid` S | None | Streams enabled, `NEW_IMAGE`; no GSI or TTL |

Keep the three tables in one Region and write to them only from that Region. Global tables (multi-Region replicas) are not supported.

`aid` is the validated type name, a hyphen, and the ID value. The library does not use shards or a KeyResolver.

## Configuration

`dynamodb.Config` names the externally provisioned resources. `JournalTableName`, `SnapshotTableName`, `HeadTableName` and `SnapshotHistoryIndexName` cannot be empty, and the three table names must be distinct. A violation is a configuration error. A nil client is also a configuration error.

`Config.ConfigurationReadRetryLimit` does not count the initial request. Nil means 10 retries, 0 disables retries, and a negative value is a configuration error.

The common `Option` values can also be passed to `dynamodb.New`: `WithRetentionCount`, `WithRetentionMode` (`RetentionDelete` is the default; `RetentionTTL` is also supported), `WithTTLGraceSeconds` (non-negative) and `WithRetentionFailureHandler`.

## Initialization

### Configuration items

The factory strongly reads the following reserved keys with BatchGetItem:

| Table | Key |
|---|---|
| journal | `aid="__config__"`, `seq_nr=0` |
| snapshot | `aid="__config__"`, `skey=0` |
| head | `aid="__config__"` |

Each has only its key attributes, `layout_version` N (`1`) and `store_id` S. If all three are absent, the factory generates one store ID and conditionally writes all three in one transaction. If present, all three must share the ID and supported layout version. Partial or mismatched configuration fails with a configuration error. A creation race rereads all three; persistent absence returns a storage error. Head configuration records must be ignored by a Streams consumer.

## Records

### Stored attributes

| Item | Complete attribute set (DynamoDB types) |
|---|---|
| journal event | `aid` S, `seq_nr` N, `occurred_at` N, `manifest` S, `payload` B |
| current snapshot (`skey=0`) | `aid` S, `skey` N, `seq_nr` N, `manifest` S, `payload` B, `last_updated_at` N |
| active history (`skey=seq_nr`) | Current snapshot attributes plus `active_history_seq_nr` N |
| marked history | Current snapshot attributes plus `ttl` N; no `active_history_seq_nr` |
| head | `aid` S, `type_name` S, `seq_nr` N, `events` L |

`events` contains exactly one M value: `seq_nr` N, `occurred_at` N, `manifest` S and `payload` B. It is the event from the last committed write, including event-only writes.

`occurred_at` stores signed Unix nanoseconds; `last_updated_at` stores the written event's Unix milliseconds as reference information. Payload B contains only serialized domain data. Manifest defaults to an empty S. Current snapshots and configuration items have neither TTL nor the active-history index attribute. There is no `version` attribute.

## Writes and reads

Each write is one transaction containing the journal event and the head update, plus the current snapshot and optional history for a pair write. The previous head must equal `event.SeqNr()-1`. Oversized items (over 409600 estimated bytes, including head overhead) fail before sending a write.

Latest snapshot reads strongly read head and current snapshot together with BatchGetItem, but are not atomic. Their numbers can differ. Events are strongly read directly from journal in ascending order, including the supplied sequence number and all pages. Restore after the snapshot's number, or from 1 without a snapshot.

## Retention

Omitted retention means no history. Positive retention keeps the newest n active history entries, excluding current. Retention queries the sparse history GSI with eventual consistency, adds the just-written history if absent and consumes all pages. Delete mode sends batches of at most 25; TTL mode sets `ttl` to marking-time Unix seconds plus the configured grace and removes `active_history_seq_nr`. Marked entries leave the GSI, do not count toward n and are not given a later deadline. TTL deletion is asynchronous.

Retention failures are logged and optionally notified without changing the successful committed write. Unprocessed BatchGetItem keys and BatchWriteItem deletes are retried using only pending entries; the configured retry limit defaults to 10 after the initial request.

## Operations

### Runtime permissions

Replace REGION, ACCOUNT and table/index names in this example. It covers configuration opening, four operations and both retention modes. BatchGetItem and BatchWriteItem need their own IAM actions. TransactWriteItems is authorized through its PutItem/UpdateItem actions; it is not an IAM action. See the [AWS action reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_dynamodb.html).

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

Provisioning and test observation use a separate administrative client with CreateTable, DescribeTable, DescribeTimeToLive, UpdateTimeToLive, DeleteTable and seed/observation permissions. Runtime operations do not Scan or create tables. DynamoDB Local does not enforce IAM; these policy grants are not validated by Local tests. Streams subscription permissions belong to the subscribing application.
