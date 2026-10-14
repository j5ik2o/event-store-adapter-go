package dynamodbtest

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func scopeAid(v string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: v}}
}

func TestRequestScope(t *testing.T) {
	query := func(expression string) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			KeyConditionExpression:    aws.String(expression),
			ExpressionAttributeNames:  map[string]string{"#aid": "aid", "#seq": "seq_nr"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": &types.AttributeValueMemberS{Value: "Order-9"}, ":since": &types.AttributeValueMemberN{Value: "1"}},
		}
	}
	requests := []Request{
		{API: "GetItem", Input: &dynamodb.GetItemInput{Key: scopeAid("Order-9")}},
		{API: "UpdateItem", Input: &dynamodb.UpdateItemInput{Key: scopeAid("Order-9")}},
		{API: "BatchGetItem", Input: &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
			"head": {Keys: []map[string]types.AttributeValue{scopeAid("Order-9")}},
			"snap": {Keys: []map[string]types.AttributeValue{scopeAid("Customer-5")}},
		}}},
		{API: "TransactWriteItems", Input: &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{Item: scopeAid("Order-9")}},
			{Update: &types.Update{Key: scopeAid("Order-9")}},
			{Delete: &types.Delete{Key: scopeAid("Order-9")}},
			{ConditionCheck: &types.ConditionCheck{Key: scopeAid("Order-9")}},
		}}},
		{API: "BatchWriteItem", Input: &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
			"journal": {{PutRequest: &types.PutRequest{Item: scopeAid("Order-9")}}, {DeleteRequest: &types.DeleteRequest{Key: scopeAid("Order-9")}}},
		}}},
		{API: "Query", Input: query("(#seq >= :since) AND (#aid = :id)")},
		{API: "Query", Input: query("")},
		{API: "Query", Input: query("#seq >= :since")},
		{API: "GetItem", Input: &dynamodb.GetItemInput{Key: map[string]types.AttributeValue{"aid": &types.AttributeValueMemberN{Value: "1"}}}},
		{API: "Scan", Input: &dynamodb.ScanInput{}},
	}
	got := RequestScope(requests)
	require.Len(t, got, len(requests))
	for i, api := range []string{"GetItem", "UpdateItem", "BatchGetItem", "TransactWriteItems", "BatchWriteItem", "Query", "Query", "Query", "GetItem", "Scan"} {
		require.Equal(t, api, got[i]["api"])
	}
	require.Equal(t, []any{"Order-9"}, got[0]["aids"])
	require.Equal(t, []any{"Order-9"}, got[1]["aids"])
	require.ElementsMatch(t, []any{"Order-9", "Customer-5"}, got[2]["aids"], "every key of every table is reported")
	require.Equal(t, []any{"Order-9", "Order-9", "Order-9", "Order-9"}, got[3]["aids"])
	require.Equal(t, []any{"Order-9", "Order-9"}, got[4]["aids"])
	require.Equal(t, []any{"Order-9"}, got[5]["aids"], "the #aid name is resolved and only the aid equality counts")
	require.Empty(t, got[6]["aids"], "a query without a key condition has no aid")
	require.Empty(t, got[7]["aids"], "a key condition without an aid equality has no aid")
	require.Equal(t, []any{nil}, got[8]["aids"], "a non-string aid is reported as nil")
	require.Empty(t, got[9]["aids"])
}
