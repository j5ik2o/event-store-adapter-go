package example

import (
	"context"
	"fmt"
	"testing"

	event_store_adapter_go "github.com/j5ik2o/event-store-adapter-go"
	"github.com/j5ik2o/event-store-adapter-go/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func Test_WriteAndRead(t *testing.T) {
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

	// When
	eventStore, err := event_store_adapter_go.NewEventStore(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
	require.Nil(t, err)
	initial, userAccountCreated := newUserAccount(userAccountId{Value: "1"}, "test")
	err = eventStore.StoreEventWithSnapshot(
		userAccountCreated,
		initial.Version,
		initial,
	)
	require.Nil(t, err)
	result, err := initial.Rename("test2")
	err = eventStore.StoreEventWithSnapshot(
		result.Event,
		result.Aggregate.Version,
		nil,
	)
	require.Nil(t, err)
	snapshotResult, err := eventStore.GetSnapshotById(&userAccountId{Value: "1"}, func(m map[string]interface{}) (event_store_adapter_go.Aggregate, error) {
		idMap, ok := m["Id"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Id is not a map")
		}
		value, ok := idMap["Value"].(string)
		if !ok {
			return nil, fmt.Errorf("Value is not a float64")
		}
		result, _ := newUserAccount(userAccountId{Value: value}, m["Name"].(string))
		return result, nil
	})
	require.Nil(t, err)

	userAccount1, ok := snapshotResult.Aggregate.(*userAccount)
	require.NotNil(t, ok)
	t.Logf("UserAccount: %s, seqNr: %d, version: %d", userAccount1, snapshotResult.SeqNr, snapshotResult.Version)
	events, err := eventStore.GetEventsByIdAndSeqNr(&userAccountId{Value: "1"}, 0, func(m map[string]interface{}) (event_store_adapter_go.Event, error) {
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
			return newUserAccountCreated(
				m["Id"].(string),
				&userAccountId{Value: aggregateId},
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		case "UserAccountNameChanged":
			return newUserAccountNameChanged(
				m["Id"].(string),
				&userAccountId{Value: aggregateId},
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		default:
			return nil, fmt.Errorf("unknown event type")
		}
	})
	require.Nil(t, err)
	t.Logf("Events: %v", events)

	actual := replayUserAccount(events, snapshotResult.Aggregate.(*userAccount), snapshotResult.Version)
	aggregate, _ := newUserAccount(userAccountId{Value: "1"}, "test2")

	// Then
	assert.True(t, actual.Equals(aggregate))

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err.Error())
		}
	}()
}