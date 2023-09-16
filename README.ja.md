# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/j5ik2o/event-store-adapter-go.svg?style=flat-square)](https://github.com/j5ik2o/event-store-adapter-go/releases/latest)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

このライブラリは、DynamoDBをEvent Sourcing用のEvent Storeにするためのものです。

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

func (r *UserAccountRepository) FindById(id AggregateId) (*UserAccount, error) {
    result, err := r.eventStore.GetSnapshotById(id, r.aggregateConverter)
    if err != nil {
        return nil, err
    }
    events, err := r.eventStore.GetEventsByIdAndSeqNr(id, result.SeqNr, r.eventConverter)
    if err != nil {
        return nil, err
    }
    return ReplayUserAccount(events, result.Aggregate.(*UserAccount), result.Version), nil
}
```

以下はリポジトリの使用例です。

```go
eventStore := NewEventStore(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
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
