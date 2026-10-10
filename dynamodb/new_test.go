package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

type readUnitInputKey struct{}

// SDK doubles here use explicit storage fixtures. Local tests observe actual SDK
// responses independently; this double only isolates boundary control flow.
func readUnitClient(recorder *dynamodbtest.Recorder, response func(any) (any, error)) *awsdynamodb.Client {
	return awsdynamodb.New(awsdynamodb.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:1"), RetryMaxAttempts: 1,
		Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		APIOptions: []func(*middleware.Stack) error{recorder.APIOption, func(stack *middleware.Stack) error {
			if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("read-unit-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				return next.HandleInitialize(context.WithValue(ctx, readUnitInputKey{}, in.Parameters), in)
			}), middleware.Before); err != nil {
				return err
			}
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("read-unit-response", func(ctx context.Context, _ middleware.FinalizeInput, _ middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				out, err := response(ctx.Value(readUnitInputKey{}))
				return middleware.FinalizeOutput{Result: out}, middleware.Metadata{}, err
			}), middleware.Before)
		}},
	})
}

func readUnitStore(t *testing.T, recorder *dynamodbtest.Recorder, limit int, hooks *testhook.Hooks, response func(any) (any, error)) *opened {
	t.Helper()
	cfg := configurationSettings(t)
	cfg.configurationReadRetryLimit = limit
	return &opened{client: readUnitClient(recorder, response), settings: cfg, hooks: hooks}
}

func TestDynamoDBFactoryUsesCommonEntry(t *testing.T) {
	r := dynamodbtest.NewRecorder()
	client := readUnitClient(r, func(input any) (any, error) {
		_, ok := input.(*awsdynamodb.BatchGetItemInput)
		require.True(t, ok)
		items := map[string][]map[string]types.AttributeValue{}
		for _, action := range configurationWrite(configurationSettings(t), "stored-unit-id").TransactItems {
			items[aws.ToString(action.Put.TableName)] = []map[string]types.AttributeValue{action.Put.Item}
		}
		return &awsdynamodb.BatchGetItemOutput{Responses: items}, nil
	})
	store, err := New(t.Context(), client, validConfig(), eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	require.NoError(t, err)
	var violation *eventstore.ContractViolationError
	ctx := dynamodbtest.WithOperation(t.Context(), 1)
	result, err := store.GetEventsByIDSinceSeqNr(ctx, nil, 7)
	require.Nil(t, result)
	require.ErrorAs(t, err, &violation)
	require.Equal(t, "T-2", violation.Rule)
	require.Equal(t, eventstore.SeqNr(7), *violation.SeqNr)
	_, err = store.GetLatestSnapshotByID(ctx, nil)
	require.ErrorAs(t, err, &violation)
	require.Empty(t, r.Requests(1))
}
