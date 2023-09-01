# event-store-adapter-go

[![CI](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml/badge.svg)](https://github.com/j5ik2o/event-store-adapter-go/actions/workflows/ci.yml)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-go)](https://github.com/XAMPPRocky/tokei)

This library is designed to turn DynamoDB into an Event Store for Event Sourcing.


## Usage

You can easily implement an Event Sourcing-enabled repository using EventStore.

```go
type UserAccountRepository struct {
    eventStore         *EventStore
    aggregateConverter AggregateConverter
    eventConverter     EventConverter
}

func (r *UserAccountRepository) Store(event Event, version uint64, aggregate Aggregate) error {
    return r.eventStore.StoreEventWithSnapshot(event, version, aggregate)
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

The following is an example of the repository usage

```go
eventStore := NewEventStore(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
repository := NewUserAccountRepository(eventStore)

userAccount1, userAccountCreated := NewUserAccount(UserAccountId{Value: "1"}, "test")
// Store an aggregate with a create event
err = repository.Store(userAccountCreated, initial.Version, initial)
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
if err != nil {
    return err
}
```
