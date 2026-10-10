package dynamodbtest

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/stretchr/testify/require"
)

func TestRequestObservationContinuation(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "real-journal", "head": "real-head", "snapshot": "real-snapshot"}, index: "real-index"}
	step := conformance.StepPlan{Op: "getEventsByIdSinceSeqNr", AID: conformance.AggregateIDArg{TypeName: "Order", Value: "9"}, SeqNr: big.NewInt(1)}
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "3"}}
	first := &dynamodb.QueryInput{TableName: aws.String("real-journal"), ConsistentRead: aws.Bool(true), ScanIndexForward: aws.Bool(true), KeyConditionExpression: aws.String("(#seq >= :since) AND (#aid = :id)"), ExpressionAttributeNames: map[string]string{"#seq": "seq_nr", "#aid": "aid"}, ExpressionAttributeValues: map[string]types.AttributeValue{":since": &types.AttributeValueMemberN{Value: "1"}, ":id": &types.AttributeValueMemberS{Value: "Order-9"}}}
	second := *first
	second.ExclusiveStartKey = key
	requests := []Request{{API: "Query", Input: first}, {API: "Query", Input: &second}}
	responses := []Response{{API: "Query", Result: &dynamodb.QueryOutput{LastEvaluatedKey: key}}, {API: "Query", Result: &dynamodb.QueryOutput{}}}
	got, err := tables.ObserveRequests(context.Background(), requests, responses, step, nil)
	require.NoError(t, err)
	require.Equal(t, true, got[0]["constraints"].(map[string]any)["follow_last_evaluated_key"])
	require.Equal(t, map[string]any{"all": []any{map[string]any{"attribute": "seq_nr", "operator": "gte", "argument": "seq_nr"}, map[string]any{"attribute": "aid", "operator": "eq", "argument": "aggregate_id"}}}, got[0]["constraints"].(map[string]any)["key_condition"])
	second.ExclusiveStartKey = nil
	got, err = tables.ObserveRequests(context.Background(), requests, responses, step, nil)
	require.NoError(t, err)
	require.Equal(t, false, got[0]["constraints"].(map[string]any)["follow_last_evaluated_key"])
	_, err = tables.ObserveRequests(context.Background(), requests, responses[:1], step, nil)
	require.Error(t, err)
	responses[0].API = "BatchGetItem"
	_, err = tables.ObserveRequests(context.Background(), requests, responses, step, nil)
	require.Error(t, err)
}

func TestRequestObservationUnprocessedAndTTL(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "journal", "head": "head", "snapshot": "snapshot"}}
	step := conformance.StepPlan{Op: "getLatestSnapshotById", AID: conformance.AggregateIDArg{TypeName: "Order", Value: "9"}}
	aid := &types.AttributeValueMemberS{Value: "Order-9"}
	keys := map[string]types.KeysAndAttributes{
		"head":     {Keys: []map[string]types.AttributeValue{{"aid": aid}}, ConsistentRead: aws.Bool(true)},
		"snapshot": {Keys: []map[string]types.AttributeValue{{"aid": aid, "skey": &types.AttributeValueMemberN{Value: "0"}}}, ConsistentRead: aws.Bool(true)},
	}
	pending := map[string]types.KeysAndAttributes{"head": keys["head"]}
	requests := []Request{{API: "BatchGetItem", Input: &dynamodb.BatchGetItemInput{RequestItems: keys}}, {API: "BatchGetItem", Input: &dynamodb.BatchGetItemInput{RequestItems: pending}}}
	responses := []Response{{API: "BatchGetItem", Result: &dynamodb.BatchGetItemOutput{UnprocessedKeys: pending}}, {API: "BatchGetItem", Result: &dynamodb.BatchGetItemOutput{}}}
	got, err := tables.ObserveRequests(context.Background(), requests, responses, step, []time.Duration{50 * time.Millisecond})
	require.NoError(t, err)
	require.Equal(t, true, got[0]["constraints"].(map[string]any)["head_and_current_snapshot"])
	require.Equal(t, true, got[1]["constraints"].(map[string]any)["only_unprocessed_keys"])
	require.Equal(t, true, got[1]["constraints"].(map[string]any)["exponential_backoff"])
	input := &dynamodb.UpdateItemInput{UpdateExpression: aws.String("REMOVE active_history_seq_nr SET #ttl = :expires"), ExpressionAttributeNames: map[string]string{"#ttl": "ttl"}, ExpressionAttributeValues: map[string]types.AttributeValue{":expires": &types.AttributeValueMemberN{Value: "4102444860"}}}
	require.Equal(t, map[string]any{"remove": []any{"active_history_seq_nr"}, "set": map[string]any{"ttl": map[string]any{"value_binding": "expires"}}}, updateObservation(input))
	require.Equal(t, map[string]any{"attribute_exists": "aid"}, conditionObservation(aws.String(" attribute_exists(#aid) "), map[string]string{"#aid": "aid"}))
	require.Contains(t, conditionObservation(aws.String("bad"), nil), "unparsed")
}
