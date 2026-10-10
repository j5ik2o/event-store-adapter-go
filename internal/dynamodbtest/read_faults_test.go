package dynamodbtest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/stretchr/testify/require"
)

func TestReadFailure(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "real-journal"}}
	injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "read-events", Kind: "storage-error", Injection: "replace-request", Repeat: "count", Count: 1, Details: map[string]any{"message": "read unavailable"}}})
	stack := middleware.NewStack("read-failure-test", func() any { return nil })
	require.NoError(t, tables.ReadFailureAPIOption(injection)(stack))
	input := &dynamodb.QueryInput{TableName: aws.String("real-journal")}
	sends := 0
	handler := middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
		sends++
		return &dynamodb.QueryOutput{}, middleware.Metadata{}, nil
	})
	_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), input, handler)
	require.ErrorContains(t, err, "read unavailable")
	require.Zero(t, sends)
	_, _, err = stack.HandleMiddleware(WithOperation(t.Context(), 1), input, handler)
	require.NoError(t, err)
	require.Equal(t, 1, sends)
	require.NoError(t, finish())
	require.Equal(t, 1, injection.Faults[0].Fired())
	require.Equal(t, "read-events", tables.ReadPhase(input))
	require.Equal(t, "", tables.ReadPhase(&dynamodb.QueryInput{TableName: aws.String("unrelated")}))
	for _, tc := range []struct{ aid, expected string }{{"__config__", ""}, {"Order-9", "read-snapshot"}} {
		require.Equal(t, tc.expected, tables.ReadPhase(&dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"head": {Keys: []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: tc.aid}}}}}}))
	}
}
