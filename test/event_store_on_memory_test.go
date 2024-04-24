package test

import (
	event_store_adapter_go "github.com/j5ik2o/event-store-adapter-go/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_EventStoreOnMemory_WriteAndRead(t *testing.T) {
	// Given
	eventStore := event_store_adapter_go.NewEventStoreOnMemory()
	require.NotNil(t, eventStore)

	userAccountId1 := newUserAccountId("1")

	initial, userAccountCreated := newUserAccount(userAccountId1, "test")
	require.NotNil(t, initial)
	require.NotNil(t, userAccountCreated)
	t.Logf("initial: %v", initial)
	t.Logf("userAccountCreated: %v", userAccountCreated)

	err := eventStore.PersistEventAndSnapshot(
		userAccountCreated,
		initial,
	)
	require.Nil(t, err)

	updated, err := initial.Rename("test2")
	require.NotNil(t, updated)
	require.Nil(t, err)
	t.Logf("updated: %v", updated)

	err = eventStore.PersistEvent(
		updated.Event,
		updated.Aggregate.Version,
	)
	require.Nil(t, err)

	snapshotResult, err := eventStore.GetLatestSnapshotById(&userAccountId1)
	require.NotNil(t, snapshotResult)
	require.Nil(t, err)
	t.Logf("snapshotResult: %v", snapshotResult)

	userAccount1, ok := snapshotResult.Aggregate().(*userAccount)
	require.NotNil(t, userAccount1)
	require.NotNil(t, ok)
	t.Logf("UserAccount: %v", userAccount1)

	events, err := eventStore.GetEventsByIdSinceSeqNr(&userAccountId1, userAccount1.GetSeqNr()+1)
	require.NotNil(t, events)
	require.Nil(t, err)
	t.Logf("Events: %v", events)

	actual := replayUserAccount(events, snapshotResult.Aggregate().(*userAccount))
	require.NotNil(t, actual)
	t.Logf("Actual: %v", actual)

	expect, _ := newUserAccount(userAccountId1, "test2")
	require.NotNil(t, expect)

	// Then
	assert.True(t, actual.Equals(expect))

}

// Persists an event and a snapshot for a user account
func Test_EventStoreOnMemory_PersistsEventAndSnapshot(t *testing.T) {
	eventStore := event_store_adapter_go.NewEventStoreOnMemory()
	require.NotNil(t, eventStore)

	userAccountId1 := newUserAccountId("1")

	initial, userAccountCreated := newUserAccount(userAccountId1, "test")
	require.NotNil(t, initial)
	require.NotNil(t, userAccountCreated)

	err := eventStore.PersistEventAndSnapshot(
		userAccountCreated,
		initial,
	)
	require.Nil(t, err)

}
