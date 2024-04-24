package test

import (
	"context"
	"fmt"
	"github.com/j5ik2o/event-store-adapter-go/pkg"
	"github.com/j5ik2o/event-store-adapter-go/pkg/common"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

type userAccountRepository struct {
	eventStore pkg.EventStore
}

func newUserAccountRepository(eventStore pkg.EventStore) *userAccountRepository {
	return &userAccountRepository{
		eventStore: eventStore,
	}
}

func (r *userAccountRepository) storeEvent(event pkg.Event, version uint64) error {
	return r.eventStore.PersistEvent(event, version)
}

func (r *userAccountRepository) storeEventAndSnapshot(event pkg.Event, aggregate pkg.Aggregate) error {
	return r.eventStore.PersistEventAndSnapshot(event, aggregate)
}

func (r *userAccountRepository) findById(id pkg.AggregateId) (*userAccount, error) {
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

func Test_Repository_DynamoDB_StoreAndFindById(t *testing.T) {
	ctx := context.Background()
	container, err := localstack.RunContainer(
		ctx,
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: "localstack/localstack:2.1.0",
				Env: map[string]string{
					"SERVICES":              "dynamodb",
					"DEFAULT_REGION":        "us-east-1",
					"EAGER_SERVICE_LOADING": "1",
					"DYNAMODB_SHARED_DB":    "1",
					"DYNAMODB_IN_MEMORY":    "1",
				},
			},
		}),
	)
	require.Nil(t, err)
	assert.NotNil(t, container)
	dynamodbClient, err := common.CreateDynamoDBClient(t, ctx, container)
	require.Nil(t, err)
	assert.NotNil(t, dynamodbClient)
	err = common.CreateJournalTable(t, ctx, dynamodbClient, "journal", "journal-aid-index")
	require.Nil(t, err)
	err = common.CreateSnapshotTable(t, ctx, dynamodbClient, "snapshot", "snapshot-aid-index")
	require.Nil(t, err)

	eventConverter := func(m map[string]interface{}) (pkg.Event, error) {
		aggregateMap, ok := m["AggregateId"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("AggregateId is not a map")
		}
		aggregateId, ok := aggregateMap["Value"].(string)
		if !ok {
			return nil, fmt.Errorf("Value is not a float64")
		}
		switch m["TypeName"].(string) {
		case "UserAccountCreated":
			userAccountId := newUserAccountId(aggregateId)
			return newUserAccountCreated(
				m["Id"].(string),
				&userAccountId,
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		case "UserAccountNameChanged":
			userAccountId := newUserAccountId(aggregateId)
			return newUserAccountNameChanged(
				m["Id"].(string),
				&userAccountId,
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		default:
			return nil, fmt.Errorf("unknown event type")
		}
	}
	aggregateConverter := func(m map[string]interface{}) (pkg.Aggregate, error) {
		idMap, ok := m["Id"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Id is not a map")
		}
		value, ok := idMap["Value"].(string)
		if !ok {
			return nil, fmt.Errorf("Value is not a float64")
		}
		userAccountId := newUserAccountId(value)
		result, _ := newUserAccount(userAccountId, m["Name"].(string))
		return result, nil
	}

	eventStore, err := pkg.NewEventStoreOnDynamoDB(
		dynamodbClient,
		"journal",
		"snapshot",
		"journal-aid-index",
		"snapshot-aid-index",
		1,
		eventConverter,
		aggregateConverter)
	require.Nil(t, err)

	repository := newUserAccountRepository(eventStore)
	initial, userAccountCreated := newUserAccount(newUserAccountId("1"), "test")
	err = repository.storeEventAndSnapshot(userAccountCreated, initial)
	require.Nil(t, err)
	actual, err := repository.findById(&initial.Id)
	require.Nil(t, err)

	assert.Equal(t, initial, actual)

	result, err := actual.Rename("test2")
	require.Nil(t, err)
	result.Aggregate.Version = actual.Version

	err = repository.storeEventAndSnapshot(result.Event, result.Aggregate)
	require.Nil(t, err)
	actual2, err := repository.findById(&initial.Id)
	require.Nil(t, err)
	assert.Equal(t, "test2", actual2.Name)

}

func Test_Repository_OnMemory_StoreAndFindById(t *testing.T) {
	eventStore := pkg.NewEventStoreOnMemory()
	repository := newUserAccountRepository(eventStore)
	initial, userAccountCreated := newUserAccount(newUserAccountId("1"), "test")

	err := repository.storeEventAndSnapshot(userAccountCreated, initial)
	require.Nil(t, err)
	actual, err := repository.findById(&initial.Id)
	require.Nil(t, err)

	assert.Equal(t, initial, actual)

	result, err := actual.Rename("test2")
	require.Nil(t, err)

	err = repository.storeEventAndSnapshot(result.Event, result.Aggregate)
	require.Nil(t, err)
	actual2, err := repository.findById(&initial.Id)
	require.Nil(t, err)
	assert.Equal(t, "test2", actual2.Name)

}
