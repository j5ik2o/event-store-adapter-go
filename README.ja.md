# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![Go project version](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go.svg)](https://badge.fury.io/go/github.com%2Fj5ik2o%2Fevent-store-adapter-go)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

このライブラリは、DynamoDBをCQRS/Event Sourcing用のEvent Storeにするためのものです。

[English](./README.md)

## 使い方

EventStoreを使えば、Event Sourcing対応リポジトリを簡単に実装できます。

```go
type UserAccountRepository struct {
    eventStore         *EventStore
    aggregateConverter AggregateConverter
    eventConverter     EventConverter
}

func (r *UserAccountRepository) Store(event Event, version uint64) error {
    return r.eventStore.PersistEvent(event, version)
}

func (r *UserAccountRepository) Store(event Event, aggregate Aggregate) error {
    return r.eventStore.PersistEventAndSnapshot(event, aggregate)
}

func (r *userAccountRepository) findById(id esag.AggregateId) (*userAccount, error) {
	result, err := r.eventStore.GetLatestSnapshotById(id, r.aggregateConverter)
	if err != nil {
		return nil, err
	}
	if result.Empty() {
		return nil, fmt.Errorf("not found")
	} else {
		events, err := r.eventStore.GetEventsByIdSinceSeqNr(id, result.Aggregate().GetSeqNr()+1, r.eventConverter)
		if err != nil {
			return nil, err
		}
		return replayUserAccount(events, result.Aggregate().(*userAccount)), nil
	}
}
```

以下はリポジトリの使用例です。

```go
eventStore := NewEventStoreOnDynamoDB(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
repository := NewUserAccountRepository(eventStore)

userAccount1, userAccountCreated := NewUserAccount(UserAccountId{Value: "1"}, "test")
// Store an aggregate with a create event
err = repository.Store(userAccountCreated, userAccount1.Version, userAccount1)
if err != nil {
    return err
}

// Replay the aggregate from the event store
userAccount2, err := repository.FindById(&initial.Id)
if err != nil {
	  return err
}

// Execute a command on the aggregate
userAccountUpdated, userAccountRenamed := userAccount2.ChangeName("test2")

// Store the new event without a snapshot
err = repository.StoreEvent(userAccountRenamed, userAccountUpdated.Version)
// Store the new event with a snapshot
// err = repository.StoreEventAndSnapshot(userAccountRenamed, userAccountUpdated)
if err != nil {
    return err
}
```

## テーブル仕様

[docs/DATABASE_SCHEMA.ja.md](docs/DATABASE_SCHEMA.ja.md)を参照してください。

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

