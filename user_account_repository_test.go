package event_store_adapter_go

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"testing"
)

type UserAccountRepository struct {
	eventStore         *EventStore
	aggregateConverter AggregateConverter
	eventConverter     EventConverter
}

func NewUserAccountRepository(eventStore *EventStore) *UserAccountRepository {
	return &UserAccountRepository{
		eventStore: eventStore,
		aggregateConverter: func(m map[string]interface{}) (Aggregate, error) {
			idMap, ok := m["Id"].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("Id is not a map")
			}
			value, ok := idMap["Value"].(string)
			if !ok {
				return nil, fmt.Errorf("Value is not a float64")
			}
			result, _ := NewUserAccount(UserAccountId{Value: value}, m["Name"].(string))
			return result, nil
		},
		eventConverter: func(m map[string]interface{}) (Event, error) {
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
				return NewUserAccountCreated(
					m["Id"].(string),
					&UserAccountId{Value: aggregateId},
					uint64(m["SeqNr"].(float64)),
					m["Name"].(string),
					uint64(m["OccurredAt"].(float64)),
				), nil
			case "UserAccountNameChanged":
				return NewUserAccountNameChanged(
					m["Id"].(string),
					&UserAccountId{Value: aggregateId},
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

func Test_StoreAndFindById(t *testing.T) {
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
	dynamodbClient, err := createDynamoDBClient(t, ctx, container)
	require.Nil(t, err)
	assert.NotNil(t, dynamodbClient)
	err = createJournalTable(t, ctx, dynamodbClient, "journal", "journal-aid-index")
	require.Nil(t, err)
	err = createSnapshotTable(t, ctx, dynamodbClient, "snapshot", "snapshot-aid-index")
	require.Nil(t, err)
	eventStore := NewEventStore(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", &DefaultKeyResolver{}, 1)

	// When
	repository := NewUserAccountRepository(eventStore)
	initial, userAccountCreated := NewUserAccount(UserAccountId{Value: "1"}, "test")
	err = repository.Store(userAccountCreated, initial.Version, initial)
	require.Nil(t, err)
	actual, err := repository.FindById(&initial.Id)
	require.Nil(t, err)

	assert.Equal(t, initial, actual)

	result, err := actual.Rename("test2")
	require.Nil(t, err)

	err = repository.Store(result.event, actual.Version, nil)
}
