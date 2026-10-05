package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/j5ik2o/event-store-adapter-go/pkg"
	"github.com/j5ik2o/event-store-adapter-go/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func newDynamoDBRegressionStore(t *testing.T, options ...pkg.EventStoreOption) (*pkg.EventStoreOnDynamoDB, *dynamodb.Client) {
	t.Helper()
	ctx := context.Background()
	container, err := localstack.Run(ctx, "localstack/localstack:2.1.0",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
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
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	client, err := common.CreateDynamoDBClient(t, ctx, container)
	require.NoError(t, err)
	require.NoError(t, common.CreateJournalTable(t, ctx, client, "journal", "journal-aid-index"))
	require.NoError(t, common.CreateSnapshotTable(t, ctx, client, "snapshot", "snapshot-aid-index"))
	eventConverter := func(m map[string]interface{}) (pkg.Event, error) {
		id := newUserAccountId(m["AggregateId"].(map[string]interface{})["Value"].(string))
		switch m["TypeName"].(string) {
		case "UserAccountCreated":
			return newUserAccountCreated(m["Id"].(string), &id, uint64(m["SeqNr"].(float64)), m["Name"].(string), uint64(m["OccurredAt"].(float64))), nil
		case "UserAccountNameChanged":
			return newUserAccountNameChanged(m["Id"].(string), &id, uint64(m["SeqNr"].(float64)), m["Name"].(string), uint64(m["OccurredAt"].(float64))), nil
		default:
			return nil, fmt.Errorf("unknown event type: %v", m["TypeName"])
		}
	}
	snapshotConverter := func(m map[string]interface{}) (pkg.Aggregate, error) {
		return &userAccount{
			Id:   newUserAccountId(m["Id"].(map[string]interface{})["Value"].(string)),
			Name: m["Name"].(string), SeqNr: uint64(m["SeqNr"].(float64)), Version: uint64(m["Version"].(float64)),
		}, nil
	}
	store, err := pkg.NewEventStoreOnDynamoDB(client, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1, eventConverter, snapshotConverter, options...)
	require.NoError(t, err)
	return store, client
}

func Test_EventStoreOnDynamoDB_GetEventsReadsAllPages(t *testing.T) {
	store, _ := newDynamoDBRegressionStore(t)
	id := newUserAccountId("large-events")
	name := strings.Repeat("x", 100*1024)
	aggregate, created := newUserAccount(id, name)
	require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
	expected := []pkg.Event{created}
	for i := 1; i < 16; i++ {
		updated, err := aggregate.Rename(name)
		require.NoError(t, err)
		require.NoError(t, store.PersistEvent(updated.Event, aggregate.GetVersion()))
		expected = append(expected, updated.Event)
		aggregate = updated.Aggregate.WithVersion(aggregate.GetVersion() + 1).(*userAccount)
	}

	events, err := store.GetEventsByIdSinceSeqNr(&id, 1)
	require.NoError(t, err)
	require.Equal(t, 16, len(events))
	for i, event := range events {
		require.Equal(t, uint64(i+1), event.GetSeqNr())
		require.Equal(t, expected[i].GetId(), event.GetId())
		if i == 0 {
			require.Equal(t, name, event.(*userAccountCreated).Name)
		} else {
			require.Equal(t, name, event.(*userAccountNameChanged).Name)
		}
	}
}
