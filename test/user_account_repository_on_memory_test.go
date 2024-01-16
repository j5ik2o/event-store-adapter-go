package test

import (
	"fmt"
	"testing"

	esag "github.com/j5ik2o/event-store-adapter-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type userAccountRepositoryOnMemory struct {
	eventStore esag.EventStore
}

func newUserAccountOnMemory(eventStore esag.EventStore) *userAccountRepository {
	return &userAccountRepository{
		eventStore: eventStore,
	}
}

func (r *userAccountRepositoryOnMemory) storeEvent(event esag.Event, version uint64) error {
	return r.eventStore.PersistEvent(event, version)
}

func (r *userAccountRepositoryOnMemory) storeEventAndSnapshot(event esag.Event, aggregate esag.Aggregate) error {
	return r.eventStore.PersistEventAndSnapshot(event, aggregate)
}

func (r *userAccountRepositoryOnMemory) findById(id esag.AggregateId) (*userAccount, error) {
	result, err := r.eventStore.GetLatestSnapshotById(id)
	if err != nil {
		return nil, err
	}
	if result.Empty() {
		return nil, fmt.Errorf("not found")
	} else {
		events, err := r.eventStore.GetEventsByIdSinceSeqNr(id, result.Aggregate().GetSeqNr()+1)
		if err != nil {
			return nil, err
		}
		return replayUserAccount(events, result.Aggregate().(*userAccount)), nil
	}
}

func Test_Repository_OnMemory_StoreAndFindById(t *testing.T) {
	// Given
	eventStore := esag.NewEventStoreOnMemory()

	// When
	repository := newUserAccountRepository(eventStore)
	initial, userAccountCreated := newUserAccount(newUserAccountId("1"), "test")
	err := repository.storeEventAndSnapshot(userAccountCreated, initial)
	require.Nil(t, err)
	actual, err := repository.findById(&initial.Id)
	require.Nil(t, err)

	assert.Equal(t, actual, initial)

	result, err := actual.Rename("test2")
	require.Nil(t, err)
	result.Aggregate.Version = actual.Version

	err = repository.storeEventAndSnapshot(result.Event, result.Aggregate)
	require.Nil(t, err)
	actual2, err := repository.findById(&initial.Id)
	require.Nil(t, err)
	assert.Equal(t, actual2.Name, "test2")

}
