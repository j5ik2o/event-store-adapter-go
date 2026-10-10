package dynamodbtest

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
)

func TestDeferReadKeysPreservesActualData(t *testing.T) {
	input := &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		"head":     {Keys: []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "Order-9"}}}, ConsistentRead: aws.Bool(true)},
		"snapshot": {Keys: []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "skey": &types.AttributeValueMemberN{Value: "0"}}}, ConsistentRead: aws.Bool(true)},
	}}
	actual := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{
		"head":     {{"seq_nr": &types.AttributeValueMemberN{Value: "3"}}},
		"snapshot": {{"payload": &types.AttributeValueMemberB{Value: []byte("actual")}}},
	}}
	awsmiddleware.SetRequestIDMetadata(&actual.ResultMetadata, "actual-batch-request")
	out, err := DeferReadKeys(input, actual, "snapshot")
	require.NoError(t, err)
	require.Equal(t, actual.Responses["head"], out.Responses["head"])
	require.NotContains(t, out.Responses, "snapshot")
	require.Equal(t, input.RequestItems["snapshot"], out.UnprocessedKeys["snapshot"])
	again, err := DeferReadKeys(input, out, "snapshot")
	require.NoError(t, err)
	require.Len(t, again.UnprocessedKeys["snapshot"].Keys, 1)
	out.UnprocessedKeys["snapshot"].Keys[0]["aid"].(*types.AttributeValueMemberS).Value = "changed"
	require.Equal(t, "Order-9", input.RequestItems["snapshot"].Keys[0]["aid"].(*types.AttributeValueMemberS).Value)
	require.Equal(t, []byte("actual"), actual.Responses["snapshot"][0]["payload"].(*types.AttributeValueMemberB).Value)
	awsmiddleware.SetRequestIDMetadata(&out.ResultMetadata, "changed-batch-request")
	requestID, ok := awsmiddleware.GetRequestIDMetadata(actual.ResultMetadata)
	require.True(t, ok)
	require.Equal(t, "actual-batch-request", requestID)
	_, err = DeferReadKeys(input, actual, "missing")
	require.Error(t, err)
}

func TestReadObserverCopiesOriginalAndReceived(t *testing.T) {
	actual := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{{"payload": &types.AttributeValueMemberB{Value: []byte("actual")}}}}
	before, after := 0, 0
	observer := &ReadObserver{
		Match: func(input any) bool {
			q, ok := input.(*dynamodb.QueryInput)
			return ok && aws.ToString(q.TableName) == "journal"
		},
		Before: func(context.Context, any) error { before++; return nil },
		After: func(_ context.Context, _ any, result any) (any, error) {
			after++
			out := result.(*dynamodb.QueryOutput)
			out.Items[0]["payload"].(*types.AttributeValueMemberB).Value[0] = 'X'
			return out, nil
		},
	}
	stack := middleware.NewStack("read-test", func() any { return nil })
	require.NoError(t, observer.APIOption(stack))
	var sdkResult any = actual
	var sdkError error
	metadata := middleware.Metadata{}
	awsmiddleware.SetRequestIDMetadata(&metadata, "actual-request-metadata")
	require.NoError(t, stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("unit-sdk-response", func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		return middleware.FinalizeOutput{Result: sdkResult}, metadata, sdkError
	}), middleware.Before))
	result, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), &dynamodb.QueryInput{TableName: aws.String("journal")}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
		return actual, middleware.Metadata{}, nil
	}))
	require.NoError(t, err)
	require.Equal(t, 1, before)
	require.Equal(t, 1, after)
	require.Equal(t, []byte("actual"), actual.Items[0]["payload"].(*types.AttributeValueMemberB).Value)
	result.(*dynamodb.QueryOutput).Items[0]["payload"].(*types.AttributeValueMemberB).Value[1] = 'Y'
	records := observer.Results(1)
	require.Len(t, records, 1)
	require.Equal(t, []byte("actual"), records[0].Original.(*dynamodb.QueryOutput).Items[0]["payload"].(*types.AttributeValueMemberB).Value)
	require.Equal(t, []byte("Xctual"), records[0].Received.(*dynamodb.QueryOutput).Items[0]["payload"].(*types.AttributeValueMemberB).Value)
	requestID, ok := awsmiddleware.GetRequestIDMetadata(records[0].Original.(*dynamodb.QueryOutput).ResultMetadata)
	require.True(t, ok)
	require.Equal(t, "actual-request-metadata", requestID)
	records[0].Original.(*dynamodb.QueryOutput).Items = nil
	require.Len(t, observer.Results(1)[0].Original.(*dynamodb.QueryOutput).Items, 1)
	awsmiddleware.SetRequestIDMetadata(&records[0].Original.(*dynamodb.QueryOutput).ResultMetadata, "changed-original")
	awsmiddleware.SetRequestIDMetadata(&records[0].Received.(*dynamodb.QueryOutput).ResultMetadata, "changed-received")
	for _, result := range []any{observer.Results(1)[0].Original, observer.Results(1)[0].Received} {
		id, ok := awsmiddleware.GetRequestIDMetadata(result.(*dynamodb.QueryOutput).ResultMetadata)
		require.True(t, ok)
		require.Equal(t, "actual-request-metadata", id)
	}
	cause := errors.New("actual SDK cause")
	sdkResult, sdkError = nil, cause
	_, _, err = stack.HandleMiddleware(WithOperation(t.Context(), 2), &dynamodb.QueryInput{TableName: aws.String("journal")}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, cause }))
	require.ErrorIs(t, err, cause)
	require.Equal(t, cause, observer.Results(2)[0].OriginalError)
	require.Equal(t, 1, after)
	sdkResult, sdkError = nil, nil
	for _, input := range []any{&dynamodb.QueryInput{TableName: aws.String("other")}, &dynamodb.PutItemInput{}} {
		_, _, err = stack.HandleMiddleware(WithOperation(t.Context(), 3), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, nil }))
		require.NoError(t, err)
	}
	require.Empty(t, observer.Results(3))
	require.Equal(t, 2, before)
	_, _, err = stack.HandleMiddleware(WithOperation(t.Context(), 4), &dynamodb.QueryInput{TableName: aws.String("journal")}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, nil }))
	require.ErrorContains(t, err, "no SDK result")
}

func TestReadObserverBatchMetadataAndBeforeFailure(t *testing.T) {
	metadata := middleware.Metadata{}
	awsmiddleware.SetRequestIDMetadata(&metadata, "batch-sdk-request")
	original := &dynamodb.BatchGetItemOutput{}
	copy := copyReadResult(original, metadata).(*dynamodb.BatchGetItemOutput)
	id, ok := awsmiddleware.GetRequestIDMetadata(copy.ResultMetadata)
	require.True(t, ok)
	require.Equal(t, "batch-sdk-request", id)
	_, ok = awsmiddleware.GetRequestIDMetadata(original.ResultMetadata)
	require.False(t, ok)
	cause := errors.New("before observer failure")
	observer := &ReadObserver{Before: func(context.Context, any) error { return cause }}
	stack := middleware.NewStack("before-read-test", func() any { return nil })
	require.NoError(t, observer.APIOption(stack))
	calls := 0
	_, _, err := stack.HandleMiddleware(t.Context(), &dynamodb.BatchGetItemInput{}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
		calls++
		return nil, middleware.Metadata{}, nil
	}))
	require.ErrorIs(t, err, cause)
	require.Zero(t, calls)
	require.Empty(t, observer.Results(0))
}
