package event_store_adapter_go

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func createJournalTable(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string, gsiName string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pkey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("skey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("aid"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("seq_nr"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pkey"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("skey"),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiName),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("aid"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("seq_nr"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
	})
	if err != nil {
		return err
	}
	t.Log("created journal table")
	return nil
}

func createSnapshotTable(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string, gsiName string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pkey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("skey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("aid"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("seq_nr"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pkey"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("skey"),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiName),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("aid"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("seq_nr"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
	})
	if err != nil {
		return err
	}
	t.Log("created snapshot table")
	return nil
}

func createDynamoDBClient(t *testing.T, ctx context.Context, l *localstack.LocalStackContainer) (*dynamodb.Client, error) {
	mappedPort, err := l.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		return nil, err
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return nil, err
	}
	defer provider.Close()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		return nil, err
	}

	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           fmt.Sprintf("http://%s:%d", host, mappedPort.Int()),
				SigningRegion: region,
			}, nil
		})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("x", "x", "x")),
	)
	if err != nil {
		return nil, err
	}

	client := dynamodb.NewFromConfig(awsCfg)
	t.Log("created dynamodb client")
	return client, nil
}

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
	dynamodbClient, err := createDynamoDBClient(t, ctx, container)
	require.Nil(t, err)
	assert.NotNil(t, dynamodbClient)
	err = createJournalTable(t, ctx, dynamodbClient, "journal", "journal-aid-index")
	require.Nil(t, err)
	err = createSnapshotTable(t, ctx, dynamodbClient, "snapshot", "snapshot-aid-index")
	require.Nil(t, err)

	// When
	eventStore := NewEventStoreWithDefaults(dynamodbClient, "journal", "snapshot", "journal-aid-index", "snapshot-aid-index", 1)
	initial, userAccountCreated := NewUserAccount(UserAccountId{Value: "1"}, "test")
	err = eventStore.StoreEventWithSnapshot(
		userAccountCreated,
		initial.Version,
		initial,
	)
	require.Nil(t, err)
	result, err := initial.Rename("test2")
	err = eventStore.StoreEventWithSnapshot(
		result.event,
		result.aggregate.Version,
		nil,
	)
	require.Nil(t, err)
	snapshotResult, err := eventStore.GetSnapshotById(&UserAccountId{Value: "1"}, func(m map[string]interface{}) (Aggregate, error) {
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
	})
	require.Nil(t, err)

	userAccount, ok := snapshotResult.Aggregate.(*UserAccount)
	require.NotNil(t, ok)
	t.Logf("UserAccount: %s, seqNr: %d, version: %d", userAccount, snapshotResult.SeqNr, snapshotResult.Version)
	events, err := eventStore.GetEventsByIdAndSeqNr(&UserAccountId{Value: "1"}, 0, func(m map[string]interface{}) (Event, error) {
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
	})
	require.Nil(t, err)
	t.Logf("Events: %v", events)

	actual := ReplayUserAccount(events, snapshotResult.Aggregate.(*UserAccount), snapshotResult.Version)
	aggregate, _ := NewUserAccount(UserAccountId{Value: "1"}, "test2")

	// Then
	assert.True(t, actual.Equals(aggregate))

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err.Error())
		}
	}()
}
