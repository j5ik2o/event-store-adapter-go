package dynamodbtest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func rawInput[T any](t *testing.T, request Request) *T {
	t.Helper()
	input, ok := request.Input.(*T)
	require.True(t, ok, "raw SDK input type: %T", request.Input)
	return input
}

func TestRequestRecording(t *testing.T) {
	// Given a recording client, When real SDK operations run, Then raw inputs retain request order and targets.
	e := startEnvironment(t)
	_, names := createTables(t, e, false)
	r := NewRecorder()
	client := e.NewClient(r.APIOption)
	ctx := WithOperation(context.Background(), 1)
	put := &dynamodb.PutItemInput{TableName: aws.String(names["journal"]), Item: journalItem("A")}
	_, err := client.PutItem(ctx, put)
	require.NoError(t, err)
	key := journalItem("")
	delete(key, "payload")
	get := &dynamodb.GetItemInput{TableName: aws.String(names["journal"]), Key: key, ConsistentRead: aws.Bool(true)}
	_, err = client.GetItem(ctx, get)
	require.NoError(t, err)
	requests := r.Requests(1)
	require.Len(t, requests, 2)
	require.Equal(t, "PutItem", requests[0].API)
	require.Equal(t, "GetItem", requests[1].API)
	for _, request := range requests {
		require.Equal(t, 1, request.Operation)
		require.Equal(t, []RequestTarget{{TableName: names["journal"]}}, request.Targets)
	}
	require.Equal(t, put, rawInput[dynamodb.PutItemInput](t, requests[0]))
	require.Equal(t, get, rawInput[dynamodb.GetItemInput](t, requests[1]))
	tx := &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String(names["journal"]), Item: journalItem("transaction"), ConditionExpression: aws.String("attribute_exists(#aid)"), ExpressionAttributeNames: map[string]string{"#aid": "aid"}}},
		{Put: &types.Put{TableName: aws.String(names["head"]), Item: map[string]types.AttributeValue{"aid": key["aid"], "seq_nr": key["seq_nr"]}}},
	}}
	_, err = client.TransactWriteItems(ctx, tx)
	require.NoError(t, err)
	requests = r.Requests(1)
	require.Len(t, requests, 3)
	require.Equal(t, "TransactWriteItems", requests[2].API)
	require.Equal(t, 1, requests[2].Operation)
	require.Equal(t, []RequestTarget{{TableName: names["journal"]}, {TableName: names["head"]}}, requests[2].Targets)
	recordedTx := rawInput[dynamodb.TransactWriteItemsInput](t, requests[2])
	require.NotNil(t, tx.ClientRequestToken)
	require.NotEmpty(t, *tx.ClientRequestToken)
	require.NotNil(t, recordedTx.ClientRequestToken)
	require.NotEmpty(t, *recordedTx.ClientRequestToken)
	require.Equal(t, *tx.ClientRequestToken, *recordedTx.ClientRequestToken)
	require.Equal(t, tx, recordedTx)
	query := &dynamodb.QueryInput{TableName: aws.String(names["journal"]), KeyConditionExpression: aws.String("#aid = :aid"), ExpressionAttributeNames: map[string]string{"#aid": "aid"}, ExpressionAttributeValues: map[string]types.AttributeValue{":aid": key["aid"]}, Limit: aws.Int32(1)}
	_, err = client.Query(ctx, query)
	require.NoError(t, err)
	batchGet := &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		names["journal"]: {Keys: []map[string]types.AttributeValue{key}, ConsistentRead: aws.Bool(true)},
		names["head"]:    {Keys: []map[string]types.AttributeValue{{"aid": key["aid"]}}},
	}}
	_, err = client.BatchGetItem(ctx, batchGet)
	require.NoError(t, err)
	batchWrite := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		names["journal"]: {{PutRequest: &types.PutRequest{Item: journalItem("batch")}}},
		names["head"]:    {{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"aid": key["aid"], "seq_nr": key["seq_nr"]}}}},
	}}
	_, err = client.BatchWriteItem(ctx, batchWrite)
	require.NoError(t, err)
	update := &dynamodb.UpdateItemInput{TableName: aws.String(names["journal"]), Key: key, UpdateExpression: aws.String("SET #payload = :payload"), ConditionExpression: aws.String("attribute_exists(aid)"), ExpressionAttributeNames: map[string]string{"#payload": "payload"}, ExpressionAttributeValues: map[string]types.AttributeValue{":payload": journalItem("updated")["payload"]}}
	_, err = client.UpdateItem(ctx, update)
	require.NoError(t, err)
	description, err := e.NewClient().DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(names["snapshot"])})
	require.NoError(t, err)
	indexQuery := &dynamodb.QueryInput{TableName: aws.String(names["snapshot"]), IndexName: description.Table.GlobalSecondaryIndexes[0].IndexName, KeyConditionExpression: query.KeyConditionExpression, ExpressionAttributeNames: query.ExpressionAttributeNames, ExpressionAttributeValues: query.ExpressionAttributeValues}
	_, err = client.Query(ctx, indexQuery)
	require.NoError(t, err)
	requests = r.Requests(1)
	require.Len(t, requests, 8)
	require.Equal(t, query, rawInput[dynamodb.QueryInput](t, requests[3]))
	require.Equal(t, batchGet, rawInput[dynamodb.BatchGetItemInput](t, requests[4]))
	require.Equal(t, batchWrite, rawInput[dynamodb.BatchWriteItemInput](t, requests[5]))
	require.Equal(t, update, rawInput[dynamodb.UpdateItemInput](t, requests[6]))
	require.Equal(t, indexQuery, rawInput[dynamodb.QueryInput](t, requests[7]))
	for i, api := range []string{"Query", "BatchGetItem", "BatchWriteItem", "UpdateItem", "Query"} {
		require.Equal(t, api, requests[i+3].API)
		require.Equal(t, 1, requests[i+3].Operation)
	}
	require.Equal(t, []RequestTarget{{TableName: names["journal"]}}, requests[3].Targets)
	for _, i := range []int{4, 5} {
		require.ElementsMatch(t, []RequestTarget{{TableName: names["journal"]}, {TableName: names["head"]}}, requests[i].Targets)
	}
	require.Equal(t, []RequestTarget{{TableName: names["journal"]}}, requests[6].Targets)
	require.Equal(t, []RequestTarget{{TableName: names["snapshot"], IndexName: aws.ToString(indexQuery.IndexName)}}, requests[7].Targets)
	// Administrative observations do not add library requests.
	readJournal(t, e, names["journal"])
	require.Len(t, r.Requests(1), 8)
}

func TestRequestRecordingSeparatesFailuresAndCopiesInputs(t *testing.T) {
	// Given distinct operation numbers, When input changes and a request fails, Then prior records remain independent.
	e := startEnvironment(t)
	_, names := createTables(t, e, false)
	r := NewRecorder()
	client := e.NewClient(r.APIOption)
	input := &dynamodb.PutItemInput{TableName: aws.String(names["journal"]), Item: journalItem("original")}
	input.Item["nested"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"binary": &types.AttributeValueMemberB{Value: []byte("nested-original")}}}}}
	_, err := client.PutItem(WithOperation(context.Background(), 1), input)
	require.NoError(t, err)
	input.Item["payload"].(*types.AttributeValueMemberB).Value[0] = 'X'
	input.Item["nested"].(*types.AttributeValueMemberL).Value[0].(*types.AttributeValueMemberM).Value["binary"].(*types.AttributeValueMemberB).Value[0] = 'X'
	input.Item["payload"] = journalItem("changed")["payload"]
	input.TableName = aws.String("missing-table")
	_, err = client.PutItem(WithOperation(context.Background(), 2), input)
	require.Error(t, err)
	one, two := r.Requests(1), r.Requests(2)
	require.Len(t, one, 1)
	require.Len(t, two, 1)
	require.Equal(t, 2, two[0].Operation)
	require.Equal(t, "PutItem", two[0].API)
	require.Equal(t, []RequestTarget{{TableName: "missing-table"}}, two[0].Targets)
	expected := journalItem("original")
	expected["nested"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"binary": &types.AttributeValueMemberB{Value: []byte("nested-original")}}}}}
	require.Equal(t, expected, rawInput[dynamodb.PutItemInput](t, one[0]).Item)
	require.Equal(t, names["journal"], *rawInput[dynamodb.PutItemInput](t, one[0]).TableName)
	require.Equal(t, "missing-table", *rawInput[dynamodb.PutItemInput](t, two[0]).TableName)
	require.Empty(t, r.Requests(3))
	// Mutating a returned record must not change the next observation.
	one[0].Targets[0].TableName = "changed-target"
	rawInput[dynamodb.PutItemInput](t, one[0]).Item["payload"].(*types.AttributeValueMemberB).Value[0] = 'X'
	rawInput[dynamodb.PutItemInput](t, one[0]).Item["nested"].(*types.AttributeValueMemberL).Value[0].(*types.AttributeValueMemberM).Value["binary"].(*types.AttributeValueMemberB).Value[0] = 'X'
	require.Equal(t, expected, rawInput[dynamodb.PutItemInput](t, r.Requests(1)[0]).Item)
	require.Equal(t, []RequestTarget{{TableName: names["journal"]}}, r.Requests(1)[0].Targets)
}
