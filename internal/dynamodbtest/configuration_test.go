package dynamodbtest

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/stretchr/testify/require"
)

func configurationFixtureInputs(storeID string) []map[string]any {
	var fixtures []map[string]any
	for _, table := range []string{"journal", "snapshot", "head"} {
		attributes := map[string]any{"aid": "S", "store_id": "S", "layout_version": "N"}
		values := map[string]any{"aid": "__config__", "store_id": storeID, "layout_version": "1"}
		if table == "journal" {
			attributes["seq_nr"] = "N"
			values["seq_nr"] = "0"
		}
		if table == "snapshot" {
			attributes["skey"] = "N"
			values["skey"] = "0"
		}
		fixtures = append(fixtures, map[string]any{"table": table, "attributes": attributes, "values": values})
	}
	return fixtures
}

func configurationBatchInput(names map[Table]string) *dynamodb.BatchGetItemInput {
	input := &dynamodb.BatchGetItemInput{RequestItems: make(map[string]types.KeysAndAttributes)}
	for table, name := range names {
		key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}}
		if table == "journal" {
			key["seq_nr"] = &types.AttributeValueMemberN{Value: "0"}
		}
		if table == "snapshot" {
			key["skey"] = &types.AttributeValueMemberN{Value: "0"}
		}
		input.RequestItems[name] = types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{key}, ConsistentRead: aws.Bool(true)}
	}
	return input
}

func TestConfigurationSDKTargetMapping(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j", "snapshot": "s", "head": "h"}}
	read := configurationBatchInput(tables.names)
	require.Equal(t, "configuration-read", tables.configurationPhase(read))
	write := &dynamodb.TransactWriteItemsInput{}
	for _, table := range []Table{"head", "journal", "snapshot"} {
		key := read.RequestItems[tables.names[table]].Keys[0]
		write.TransactItems = append(write.TransactItems, types.TransactWriteItem{Put: &types.Put{TableName: aws.String(tables.names[table]), Item: key}})
	}
	require.Equal(t, "configuration-create", tables.configurationPhase(write))
	var canceled *types.TransactionCanceledException
	require.ErrorAs(t, tables.configurationSDKError(write, map[string]any{
		"code": "TransactionCanceledException", "cancellation_reasons": []any{
			map[string]any{"target": "configuration:journal", "code": "ConditionalCheckFailed"},
			map[string]any{"target": "configuration:head", "code": "TransactionConflict"},
		},
	}), &canceled)
	require.Equal(t, "TransactionConflict", aws.ToString(canceled.CancellationReasons[0].Code))
	require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[1].Code))
	require.Equal(t, "None", aws.ToString(canceled.CancellationReasons[2].Code))
	var condition *types.ConditionalCheckFailedException
	require.ErrorAs(t, tables.configurationSDKError(write, map[string]any{"code": "ConditionalCheckFailedException"}), &condition)
	var throughput *types.ProvisionedThroughputExceededException
	require.ErrorAs(t, tables.configurationSDKError(read, map[string]any{"code": "ProvisionedThroughputExceededException"}), &throughput)

	read.RequestItems["j"].Keys[0]["aid"] = &types.AttributeValueMemberS{Value: "Order-1"}
	require.Empty(t, tables.configurationPhase(read), "product keys must not trigger configuration faults")
	require.Empty(t, tables.configurationPhase(&dynamodb.QueryInput{}))
	require.Empty(t, tables.configurationPhase(&dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{{Delete: &types.Delete{}}}}))
	require.Empty(t, tables.configurationPhase(&dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"other": {Keys: []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "__config__"}}}}}}))
}

func TestConfigurationFixturesRejectInvalidInputs(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j"}}
	for _, fixture := range []map[string]any{
		{}, {"table": "unknown"}, {"table": "journal"},
		{"table": "journal", "attributes": map[string]any{"aid": "S"}, "values": map[string]any{}},
		{"table": "journal", "attributes": map[string]any{"aid": "B"}, "values": map[string]any{"aid": "__config__"}},
	} {
		require.Error(t, tables.PutConfigurationItems(context.Background(), []map[string]any{fixture}))
	}
}

func TestConfigurationSDKFaultApplication(t *testing.T) {
	e := startEnvironment(t)
	t.Run("response is partitioned from actual Local output", func(t *testing.T) {
		tables, _ := createTables(t, e, false)
		require.NoError(t, tables.PutConfigurationItems(context.Background(), configurationFixtureInputs("actual-seed")))
		input := configurationBatchInput(tables.names)
		actual, err := e.NewClient().BatchGetItem(context.Background(), input)
		require.NoError(t, err)
		fault := conformance.FaultSpec{Operation: 0, Phase: "configuration-read", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"unprocessed_keys": []any{"snapshot:__config__:0", "head:__config__"}}}
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{fault})
		recorder := NewRecorder()
		out, err := e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection)).BatchGetItem(WithOperation(context.Background(), 0), input)
		require.NoError(t, err)
		require.Equal(t, actual.Responses[tables.names["journal"]], out.Responses[tables.names["journal"]])
		for _, table := range []Table{"snapshot", "head"} {
			require.Empty(t, out.Responses[tables.names[table]])
			require.Equal(t, input.RequestItems[tables.names[table]].Keys, out.UnprocessedKeys[tables.names[table]].Keys)
			item, err := tables.GetItem(context.Background(), table, input.RequestItems[tables.names[table]].Keys[0])
			require.NoError(t, err)
			require.Equal(t, actual.Responses[tables.names[table]][0], item, "withholding a response does not remove data")
		}
		require.Len(t, recorder.Requests(0), 1)
		require.Equal(t, 1, injection.Faults[0].Fired())
		require.NoError(t, finish())
		// Also preserve the unprocessed map returned through the SDK fault wiring.
		actual.UnprocessedKeys = out.UnprocessedKeys
		partitioned, err := tables.partitionConfigurationResponse(input, actual, fault.Details)
		require.NoError(t, err)
		require.Equal(t, out.UnprocessedKeys, partitioned.UnprocessedKeys)
		partitioned.Responses[tables.names["journal"]][0]["store_id"].(*types.AttributeValueMemberS).Value = "changed-copy"
		require.Equal(t, "actual-seed", actual.Responses[tables.names["journal"]][0]["store_id"].(*types.AttributeValueMemberS).Value)
	})
	t.Run("request replacement installs only explicit inputs and retains recording", func(t *testing.T) {
		tables, _ := createTables(t, e, false)
		var fixtures []any
		for _, fixture := range configurationFixtureInputs("winner-input") {
			fixtures = append(fixtures, fixture)
		}
		fault := conformance.FaultSpec{Operation: 0, Phase: "configuration-create", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"code": "TransactionCanceledException", "install_items": fixtures, "cancellation_reasons": []any{map[string]any{"target": "configuration:journal", "code": "ConditionalCheckFailed"}}}}
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{fault})
		read := configurationBatchInput(tables.names)
		write := &dynamodb.TransactWriteItemsInput{}
		for _, table := range []Table{"head", "journal", "snapshot"} {
			item := cloneValue(reflect.ValueOf(read.RequestItems[tables.names[table]].Keys[0])).Interface().(map[string]types.AttributeValue)
			item["store_id"] = &types.AttributeValueMemberS{Value: "request-input"}
			item["layout_version"] = &types.AttributeValueMemberN{Value: "1"}
			write.TransactItems = append(write.TransactItems, types.TransactWriteItem{Put: &types.Put{TableName: aws.String(tables.names[table]), Item: item, ConditionExpression: aws.String("attribute_not_exists(aid)")}})
		}
		recorder := NewRecorder()
		_, err := e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection)).TransactWriteItems(WithOperation(context.Background(), 0), write)
		var canceled *types.TransactionCanceledException
		require.ErrorAs(t, err, &canceled)
		require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[1].Code))
		require.Len(t, recorder.Requests(0), 1)
		require.Equal(t, write, rawInput[dynamodb.TransactWriteItemsInput](t, recorder.Requests(0)[0]))
		for table, name := range tables.names {
			item, err := tables.GetItem(context.Background(), table, read.RequestItems[name].Keys[0])
			require.NoError(t, err)
			require.Equal(t, &types.AttributeValueMemberS{Value: "winner-input"}, item["store_id"])
		}
		require.NoError(t, finish())
	})
	t.Run("unsupported and other-operation faults remain unfired", func(t *testing.T) {
		tables, _ := createTables(t, e, false)
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{
			{Operation: 0, Phase: "configuration-read", Kind: "read-interleave", Repeat: "count", Count: 1},
			{Operation: 1, Phase: "configuration-read", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"unprocessed_keys": []any{"head:__config__"}}},
		})
		_, err := e.NewClient(tables.ConfigurationAPIOption(injection)).BatchGetItem(WithOperation(context.Background(), 0), configurationBatchInput(tables.names))
		require.NoError(t, err)
		for _, fault := range injection.Faults {
			require.Zero(t, fault.Fired())
		}
		require.ErrorContains(t, finish(), "fired 0 times")
	})
}

func TestConfigurationSDKConcurrentFaultApplications(t *testing.T) {
	e := startEnvironment(t)
	tables, _ := createTables(t, e, false)
	require.NoError(t, tables.PutConfigurationItems(context.Background(), configurationFixtureInputs("parallel-seed")))
	injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{{Operation: 0, Phase: "configuration-read", Kind: "sdk-response", Repeat: "count", Count: 7, Injection: "replace-response", Details: map[string]any{"unprocessed_keys": []any{"head:__config__"}}}})
	recorder := NewRecorder()
	client := e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection))
	type result struct {
		out *dynamodb.BatchGetItemOutput
		err error
	}
	results := make(chan result, 20)
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Go(func() {
			out, err := client.BatchGetItem(WithOperation(context.Background(), 0), configurationBatchInput(tables.names))
			results <- result{out, err}
		})
	}
	workers.Wait()
	close(results)
	applied := 0
	for result := range results {
		require.NoError(t, result.err)
		if len(result.out.UnprocessedKeys) > 0 {
			applied++
		}
	}
	require.Equal(t, 7, applied)
	require.Equal(t, 7, injection.Faults[0].Fired())
	require.Len(t, recorder.Requests(0), 20)
	require.NoError(t, finish())
}
