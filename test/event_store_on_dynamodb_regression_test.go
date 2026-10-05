package test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

// Historical snapshots have no read API, so inspect their persisted retention state.
func activeSnapshotSeqNrs(t *testing.T, client *dynamodb.Client, id *userAccountId) []uint64 {
	t.Helper()
	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName: aws.String("snapshot"), ConsistentRead: aws.Bool(true),
		FilterExpression:          aws.String("#aid = :aid"),
		ExpressionAttributeNames:  map[string]string{"#aid": "aid"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: id.AsString()}},
	})
	var seqNrs []uint64
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(context.Background())
		require.NoError(t, err)
		for _, item := range result.Items {
			if item["ttl"].(*types.AttributeValueMemberN).Value == "0" {
				seqNr, err := strconv.ParseUint(item["seq_nr"].(*types.AttributeValueMemberN).Value, 10, 64)
				require.NoError(t, err)
				seqNrs = append(seqNrs, seqNr)
			}
		}
	}
	sort.Slice(seqNrs, func(i, j int) bool { return seqNrs[i] < seqNrs[j] })
	return seqNrs
}

func Test_EventStoreOnDynamoDB_RetentionKeepsNewestSnapshot(t *testing.T) {
	for _, mode := range []string{"delete", "ttl"} {
		t.Run(mode, func(t *testing.T) {
			options := []pkg.EventStoreOption{pkg.WithKeepSnapshot(true), pkg.WithKeepSnapshotCount(1)}
			if mode == "ttl" {
				options = append(options, pkg.WithDeleteTtl(time.Hour))
			}
			store, client := newDynamoDBRegressionStore(t, options...)
			id := newUserAccountId("retention-order")
			aggregate, created := newUserAccount(id, "first")
			require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
			updated, err := aggregate.Rename("second")
			require.NoError(t, err)
			require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
			require.Equal(t, []uint64{0, 2}, activeSnapshotSeqNrs(t, client, &id))
			latest, err := store.GetLatestSnapshotById(&id)
			require.NoError(t, err)
			require.Equal(t, "second", latest.Aggregate().(*userAccount).Name)
		})
	}
}

func Test_EventStoreOnDynamoDB_WritesSucceedBelowRetentionCount(t *testing.T) {
	for _, mode := range []string{"delete", "ttl"} {
		for _, persistSnapshot := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/snapshot=%t", mode, persistSnapshot), func(t *testing.T) {
				options := []pkg.EventStoreOption{pkg.WithKeepSnapshot(true), pkg.WithKeepSnapshotCount(3)}
				if mode == "ttl" {
					options = append(options, pkg.WithDeleteTtl(time.Hour))
				}
				store, client := newDynamoDBRegressionStore(t, options...)
				id := newUserAccountId("below-retention-count")
				aggregate, created := newUserAccount(id, "first")
				require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
				for seqNr := uint64(2); seqNr <= 4; seqNr++ {
					updated, err := aggregate.Rename(fmt.Sprintf("name-%d", seqNr))
					require.NoError(t, err)
					if persistSnapshot {
						err = store.PersistEventAndSnapshot(updated.Event, updated.Aggregate)
					} else {
						err = store.PersistEvent(updated.Event, aggregate.GetVersion())
					}
					// The transaction must not be reported as failed by retention cleanup.
					require.NoError(t, err)
					aggregate = updated.Aggregate.WithVersion(aggregate.GetVersion() + 1).(*userAccount)
				}
				events, err := store.GetEventsByIdSinceSeqNr(&id, 1)
				require.NoError(t, err)
				require.Len(t, events, 4)
				latest, err := store.GetLatestSnapshotById(&id)
				require.NoError(t, err)
				require.Equal(t, uint64(4), latest.Aggregate().GetVersion())
				if persistSnapshot {
					require.Equal(t, []uint64{0, 2, 3, 4}, activeSnapshotSeqNrs(t, client, &id))
				} else {
					require.Equal(t, []uint64{0, 1}, activeSnapshotSeqNrs(t, client, &id))
				}
			})
		}
	}
}

func Test_EventStoreOnDynamoDB_TtlRetentionIgnoresMarkedSnapshots(t *testing.T) {
	store, client := newDynamoDBRegressionStore(t,
		pkg.WithKeepSnapshot(true), pkg.WithKeepSnapshotCount(3), pkg.WithDeleteTtl(time.Hour))
	id := newUserAccountId("marked-snapshot-retention")
	aggregate, created := newUserAccount(id, "first")
	require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
	for seqNr := 2; seqNr <= 3; seqNr++ {
		updated, err := aggregate.Rename(fmt.Sprintf("name-%d", seqNr))
		require.NoError(t, err)
		require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
		aggregate = updated.Aggregate.WithVersion(aggregate.GetVersion() + 1).(*userAccount)
	}
	result, err := client.Scan(context.Background(), &dynamodb.ScanInput{TableName: aws.String("snapshot"), ConsistentRead: aws.Bool(true)})
	require.NoError(t, err)
	keys := make(map[string]map[string]types.AttributeValue)
	for _, item := range result.Items {
		keys[item["seq_nr"].(*types.AttributeValueMemberN).Value] = map[string]types.AttributeValue{"pkey": item["pkey"], "skey": item["skey"]}
	}
	markedTtl := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
	// Reproduce the older retention implementation's state: only snapshot 3 is marked.
	_, err = client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("snapshot"), Key: keys["3"],
		UpdateExpression:          aws.String("SET #ttl = :ttl"),
		ExpressionAttributeNames:  map[string]string{"#ttl": "ttl"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":ttl": &types.AttributeValueMemberN{Value: markedTtl}},
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2}, activeSnapshotSeqNrs(t, client, &id))
	require.NoError(t, pkg.WithKeepSnapshotCount(2)(store))
	updated, err := aggregate.Rename("fourth")
	require.NoError(t, err)
	require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
	require.Equal(t, []uint64{0, 2, 4}, activeSnapshotSeqNrs(t, client, &id))
	for seqNr, key := range keys {
		item, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String("snapshot"), Key: key, ConsistentRead: aws.Bool(true)})
		require.NoError(t, err)
		ttl := item.Item["ttl"].(*types.AttributeValueMemberN).Value
		if seqNr == "1" {
			require.NotEqual(t, "0", ttl)
		} else if seqNr == "3" {
			require.Equal(t, markedTtl, ttl)
		} else {
			require.Equal(t, "0", ttl)
		}
	}
}

func Test_EventStoreOnDynamoDB_TtlRetentionReadsPastMarkedPages(t *testing.T) {
	store, client := newDynamoDBRegressionStore(t,
		pkg.WithKeepSnapshot(true), pkg.WithKeepSnapshotCount(1), pkg.WithDeleteTtl(time.Hour))
	id := newUserAccountId("marked-pages")
	aggregate, created := newUserAccount(id, "first")
	require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
	// Each subsequent cleanup must pass the already marked oldest snapshots,
	// including pages with no matching items, to find the next active snapshot.
	for seqNr := 2; seqNr <= 5; seqNr++ {
		updated, err := aggregate.Rename(fmt.Sprintf("name-%d", seqNr))
		require.NoError(t, err)
		require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
		aggregate = updated.Aggregate.WithVersion(aggregate.GetVersion() + 1).(*userAccount)
		require.Equal(t, []uint64{0, uint64(seqNr)}, activeSnapshotSeqNrs(t, client, &id))
	}
}

func Test_EventStoreOnDynamoDB_RetentionCountsAllSnapshotPages(t *testing.T) {
	for _, mode := range []string{"delete", "ttl"} {
		t.Run(mode, func(t *testing.T) {
			options := []pkg.EventStoreOption{pkg.WithKeepSnapshot(true), pkg.WithKeepSnapshotCount(20)}
			if mode == "ttl" {
				options = append(options, pkg.WithDeleteTtl(time.Hour))
			}
			store, client := newDynamoDBRegressionStore(t, options...)
			id := newUserAccountId("large-snapshots")
			name := strings.Repeat("x", 100*1024)
			aggregate, created := newUserAccount(id, name)
			require.NoError(t, store.PersistEventAndSnapshot(created, aggregate))
			for seqNr := 2; seqNr <= 16; seqNr++ {
				updated, err := aggregate.Rename(name)
				require.NoError(t, err)
				require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
				aggregate = updated.Aggregate.WithVersion(aggregate.GetVersion() + 1).(*userAccount)
			}
			// The count query must read beyond DynamoDB's 1MB page boundary.
			require.NoError(t, pkg.WithKeepSnapshotCount(12)(store))
			updated, err := aggregate.Rename(name)
			require.NoError(t, err)
			require.NoError(t, store.PersistEventAndSnapshot(updated.Event, updated.Aggregate))
			require.Equal(t, []uint64{0, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, activeSnapshotSeqNrs(t, client, &id))
		})
	}
}
