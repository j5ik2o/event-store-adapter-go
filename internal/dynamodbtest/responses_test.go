package dynamodbtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func TestResponseRecorder(t *testing.T) {
	r := &ResponseRecorder{}
	stack := middleware.NewStack("response-test", func() any { return nil })
	require.NoError(t, r.APIOption(stack))
	actual := &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"payload": &types.AttributeValueMemberB{Value: []byte("actual")}}}
	var cause error
	metadata := middleware.Metadata{}
	metadata.Set("sdk", "actual metadata")
	require.NoError(t, stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("unit-sdk-response", func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		return middleware.FinalizeOutput{Result: actual}, metadata, cause
	}), middleware.Before))
	input := &dynamodb.GetItemInput{TableName: aws.String("real-head")}
	_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, nil }))
	require.NoError(t, err)
	input.TableName = aws.String("changed")
	actual.Item["payload"].(*types.AttributeValueMemberB).Value[0] = 'X'
	records := r.Results(1)
	require.Len(t, records, 1)
	require.Equal(t, "real-head", *records[0].Input.(*dynamodb.GetItemInput).TableName)
	result := records[0].Result.(*dynamodb.GetItemOutput)
	require.Equal(t, []byte("actual"), result.Item["payload"].(*types.AttributeValueMemberB).Value)
	require.Equal(t, "actual metadata", result.ResultMetadata.Get("sdk"))
	result.ResultMetadata.Set("sdk", "changed")
	result.Item = nil
	require.Equal(t, "actual metadata", r.Results(1)[0].Result.(*dynamodb.GetItemOutput).ResultMetadata.Get("sdk"))
	require.NotNil(t, r.Results(1)[0].Result.(*dynamodb.GetItemOutput).Item)
	cause = errors.New("real SDK failure")
	_, _, err = stack.HandleMiddleware(WithOperation(t.Context(), 2), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, cause }))
	require.ErrorIs(t, err, cause)
	require.Equal(t, cause.Error(), r.Results(2)[0].Error)
	require.ErrorIs(t, r.Results(2)[0].Cause, cause)
	require.Empty(t, r.Results(3))
	_, _, _ = stack.HandleMiddleware(t.Context(), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) { return nil, middleware.Metadata{}, nil }))
	require.Empty(t, r.Results(0), "untagged administrative operations are excluded")
}

func TestResponseRecorderPreservesSerializableServiceCause(t *testing.T) {
	apiError := &types.TransactionCanceledException{Message: aws.String("actual service cancellation"), CancellationReasons: []types.CancellationReason{{Code: aws.String("ConditionalCheckFailed"), Item: map[string]types.AttributeValue{"seq_nr": &types.AttributeValueMemberN{Value: "2"}}}}}
	cause := &smithyhttp.ResponseError{Err: apiError, Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 400, Request: &http.Request{GetBody: func() (io.ReadCloser, error) { return nil, nil }}}}}
	r := &ResponseRecorder{}
	stack := middleware.NewStack("service-error-recording", func() any { return nil })
	require.NoError(t, r.APIOption(stack))
	_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), &dynamodb.TransactWriteItemsInput{}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
		return nil, middleware.Metadata{}, cause
	}))
	require.ErrorIs(t, err, apiError)
	records := r.Results(1)
	require.ErrorIs(t, records[0].Cause, apiError)
	require.ErrorIs(t, records[0].Original.Cause, apiError)
	raw, err := json.Marshal(records)
	require.NoError(t, err)
	require.Contains(t, string(raw), "actual service cancellation")
	require.Contains(t, string(raw), "CancellationReasons")
	require.Contains(t, string(raw), "ConditionalCheckFailed")
	require.Contains(t, string(raw), "seq_nr")
	require.NotContains(t, string(raw), "GetBody")
}

func TestResponseRecorderSeparatesOriginalFromReceived(t *testing.T) {
	actual := &dynamodb.QueryOutput{Count: 4}
	for _, n := range []string{"1", "2", "3", "4"} {
		actual.Items = append(actual.Items, map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: n}, "payload": &types.AttributeValueMemberB{Value: bytes.Repeat([]byte("x"), 320022)}})
	}
	observer := &ReadObserver{After: func(_ context.Context, _ any, output any) (any, error) {
		return BoundEventQuery(output.(*dynamodb.QueryOutput))
	}}
	r := &ResponseRecorder{}
	stack := middleware.NewStack("original-response-test", func() any { return nil })
	require.NoError(t, observer.APIOption(stack))
	require.NoError(t, r.APIOption(stack))
	require.NoError(t, stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("unit-deserialize", func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleDeserialize(ctx, in)
		out.Result = out.RawResponse
		return out, metadata, err
	}), middleware.Before))
	_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), &dynamodb.QueryInput{TableName: aws.String("journal")}, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
		return actual, middleware.Metadata{}, nil
	}))
	require.NoError(t, err)
	records := r.Results(1)
	require.Len(t, records, 1)
	require.Len(t, records[0].Original.Result.(*dynamodb.QueryOutput).Items, 4)
	require.Len(t, records[0].Result.(*dynamodb.QueryOutput).Items, 3)
	require.Empty(t, records[0].Original.Result.(*dynamodb.QueryOutput).LastEvaluatedKey)
	require.NotEmpty(t, records[0].Result.(*dynamodb.QueryOutput).LastEvaluatedKey)
	records[0].Original.Result.(*dynamodb.QueryOutput).Items = nil
	require.Len(t, r.Results(1)[0].Original.Result.(*dynamodb.QueryOutput).Items, 4)
}
