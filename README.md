# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/j5ik2o/event-store-adapter-go.svg?style=flat-square)](https://github.com/j5ik2o/event-store-adapter-go/releases/latest)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

This library is designed to turn DynamoDB into an Event Store for Event Sourcing.

[日本語](./README.ja.md)

## Usage

You can easily implement an Event Sourcing-enabled repository using EventStore.

```go
type UserAccountRepository struct {
    eventStore         *EventStore
    aggregateConverter AggregateConverter
    eventConverter     EventConverter
}

func (r *UserAccountRepository) Store(event Event, version uint64, aggregate Aggregate) error {
    return r.eventStore.StoreEventAndSnapshotOpt(event, version, aggregate)
}

func (r *UserAccountRepository) FindById(id AggregateId) (*UserAccount, error) {
    result, err := r.eventStore.GetLatestSnapshotById(id, r.aggregateConverter)
    if err != nil {
        return nil, err
    }
    events, err := r.eventStore.GetEventsByIdSinceSeqNr(id, result.SeqNr()+1, r.eventConverter)
    if err != nil {
        return nil, err
    }
    return ReplayUserAccount(events, result.Aggregate().(*UserAccount), result.Version()), nil
}
```

The following is an example of the repository usage

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
userAccountUpdated, userAccountReanmed := userAccount2.ChangeName("test2")

// Store the new event without a snapshot
err = repository.Store(userAccountReanmed, userAccountUpdated.Version, nil)
// Store the new event with a snapshot
// err = repository.Store(userAccountReanmed, userAccountUpdated.Version, userAccountUpdated)
if err != nil {
    return err
}
```

## Table Specifications

See [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md).

## License.

MIT License. See [LICENSE](LICENSE) for details.

## Other language implementations

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
