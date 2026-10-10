package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

func TestOpenUsesOptionHooksDuringFirstConfigurationRead(t *testing.T) {
	recorder := dynamodbtest.NewRecorder()
	options := configurationUnitClient(recorder).Options()
	reads := 0
	options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
		var input *awsdynamodb.BatchGetItemInput
		if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("unit-configuration-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			input = in.Parameters.(*awsdynamodb.BatchGetItemInput)
			return next.HandleInitialize(ctx, in)
		}), middleware.Before); err != nil {
			return err
		}
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("unit-configuration-response", func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			reads++
			out := &awsdynamodb.BatchGetItemOutput{}
			if reads == 1 {
				out.UnprocessedKeys = input.RequestItems
			} else {
				out.Responses = map[string][]map[string]types.AttributeValue{}
				for table, request := range input.RequestItems {
					item := request.Keys[0]
					item["store_id"] = &types.AttributeValueMemberS{Value: "unit-input-store"}
					item["layout_version"] = &types.AttributeValueMemberN{Value: "1"}
					out.Responses[table] = []map[string]types.AttributeValue{item}
				}
			}
			return middleware.FinalizeOutput{Result: out}, middleware.Metadata{}, nil
		}), middleware.Before)
	})
	hooks := testhook.New()
	var waits []time.Duration
	hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
	opened, err := open(dynamodbtest.WithOperation(t.Context(), 0), awsdynamodb.New(options), validConfig(), nil, func(o *storeoptions.Options) error {
		o.Hooks = hooks
		return nil
	})
	require.NoError(t, err)
	require.Same(t, hooks, opened.hooks)
	require.Equal(t, []time.Duration{50 * time.Millisecond}, waits)
	require.Len(t, recorder.Requests(0), 2)
	require.Equal(t, "unit-input-store", opened.storeID)
}

func configurationSettings(t *testing.T) settings {
	t.Helper()
	settings, err := validateConfig(configurationUnitClient(dynamodbtest.NewRecorder()), validConfig())
	require.NoError(t, err)
	return settings
}

func TestConfigurationKeysAndConditionalWrite(t *testing.T) {
	cfg := configurationSettings(t)
	keys := configurationKeys(cfg)
	require.Len(t, keys, 3)
	write := configurationWrite(cfg, "explicit-input-id")
	require.Len(t, write.TransactItems, 3)
	for _, action := range write.TransactItems {
		put := action.Put
		require.NotNil(t, put)
		read := keys[aws.ToString(put.TableName)]
		require.True(t, aws.ToBool(read.ConsistentRead))
		require.Len(t, read.Keys, 1)
		require.True(t, configurationKeyMatches(put.Item, read.Keys[0]))
		require.Len(t, put.Item, len(read.Keys[0])+2)
		require.Equal(t, &types.AttributeValueMemberS{Value: "explicit-input-id"}, put.Item["store_id"])
		require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, put.Item["layout_version"])
		require.Equal(t, "attribute_not_exists(aid)", aws.ToString(put.ConditionExpression))
	}
	for _, tc := range []struct{ table, sortKey string }{{"journal", "seq_nr"}, {"snapshot", "skey"}, {"head", ""}} {
		key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}}
		if tc.sortKey != "" {
			key[tc.sortKey] = &types.AttributeValueMemberN{Value: "0"}
		}
		require.Equal(t, key, keys[tc.table].Keys[0])
	}
}

func TestConfigurationNumberAndKeyMatching(t *testing.T) {
	for _, value := range []string{"1", "1.0", "1e0"} {
		require.True(t, configurationNumberEquals(value, "1"))
	}
	for _, value := range []string{"2", "1.5", "invalid"} {
		require.False(t, configurationNumberEquals(value, "1"))
	}
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}, "seq_nr": &types.AttributeValueMemberN{Value: "0"}}
	for _, item := range []map[string]types.AttributeValue{
		{"aid": &types.AttributeValueMemberS{Value: "other"}, "seq_nr": key["seq_nr"]},
		{"aid": key["aid"], "seq_nr": &types.AttributeValueMemberN{Value: "1"}},
		{"aid": key["aid"], "seq_nr": &types.AttributeValueMemberS{Value: "0"}},
		{"aid": &types.AttributeValueMemberN{Value: "0"}, "seq_nr": key["seq_nr"]},
	} {
		require.False(t, configurationKeyMatches(item, key))
	}
}

func TestMatchConfiguration(t *testing.T) {
	cfg := configurationSettings(t)
	id, exists, err := matchConfiguration(cfg, nil)
	require.NoError(t, err)
	require.False(t, exists)
	require.Empty(t, id)
	items := map[string]map[string]types.AttributeValue{}
	for _, action := range configurationWrite(cfg, "seed-id").TransactItems {
		items[aws.ToString(action.Put.TableName)] = action.Put.Item
	}
	id, exists, err = matchConfiguration(cfg, items)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "seed-id", id)
	for _, table := range []string{"journal", "snapshot", "head"} {
		t.Run(table, func(t *testing.T) {
			for _, attribute := range []string{"store_id", "layout_version"} {
				original := items[table][attribute]
				delete(items[table], attribute)
				_, _, err := matchConfiguration(cfg, items)
				requireKind(t, err, eventstore.KindConfiguration)
				items[table][attribute] = original
			}
			original := items[table]
			delete(items, table)
			_, _, err := matchConfiguration(cfg, items)
			requireKind(t, err, eventstore.KindConfiguration)
			items[table] = original
		})
	}
}

func TestConfigurationCreateRaceReasons(t *testing.T) {
	for _, code := range []string{"ConditionalCheckFailed", "TransactionConflict"} {
		for index := 0; index < 3; index++ {
			reasons := make([]types.CancellationReason, 3)
			reasons[index].Code = aws.String(code)
			err := fmt.Errorf("SDK wrapper: %w", &types.TransactionCanceledException{CancellationReasons: reasons})
			require.True(t, configurationCreateRace(err))
		}
	}
	require.True(t, configurationCreateRace(&types.ConditionalCheckFailedException{}))
	require.False(t, configurationCreateRace(&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("ProvisionedThroughputExceeded")}}}))
	require.False(t, configurationCreateRace(&types.ProvisionedThroughputExceededException{}))
}
