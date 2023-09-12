package example

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	esag "github.com/j5ik2o/event-store-adapter-go"
	"github.com/j5ik2o/event-store-adapter-go/common"
)

type userAccountRepository struct {
	eventStore         *esag.EventStore
	aggregateConverter esag.AggregateConverter
	eventConverter     esag.EventConverter
}

func newUserAccountRepository(eventStore *esag.EventStore) *userAccountRepository {
	return &userAccountRepository{
		eventStore: eventStore,
		aggregateConverter: func(m map[string]interface{}) (esag.Aggregate, error) {
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
		},
		eventConverter: func(m map[string]interface{}) (esag.Event, error) {
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
		},
	}
}

func (r *userAccountRepository) store(event esag.Event, version uint64, aggregate esag.Aggregate) error {
	return r.eventStore.StoreEventAndSnapshotOpt(event, version, aggregate)
}

func (r *userAccountRepository) findById(id esag.AggregateId) (*userAccount, error) {
	result, err := r.eventStore.GetLatestSnapshotById(id, r.aggregateConverter)
	if err != nil {
		return nil, err
	}
	events, err := r.eventStore.GetEventsByIdSinceSeqNr(id, result.SeqNr(), r.eventConverter)
	if err != nil {
		return nil, err
	}
	return replayUserAccount(events, result.Aggregate().(*userAccount), result.Version()), nil
}

func Test_RepositoryStoreAndFindById(t *testing.T) {
	// Given
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
	eventStore, err := esag.NewEventStore(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
	require.Nil(t, err)

	// When
	repository := newUserAccountRepository(eventStore)
	initial, userAccountCreated := newUserAccount(newUserAccountId("1"), "test")
	err = repository.store(userAccountCreated, initial.Version, initial)
	require.Nil(t, err)
	actual, err := repository.findById(&initial.Id)
	require.Nil(t, err)

	assert.Equal(t, actual, initial)

	result, err := actual.Rename("test2")
	require.Nil(t, err)

	err = repository.store(result.Event, actual.Version, nil)
	require.Nil(t, err)
	actual2, err := repository.findById(&initial.Id)
	require.Nil(t, err)
	assert.Equal(t, actual2.Name, "test2")

}
