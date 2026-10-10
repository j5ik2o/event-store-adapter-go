package dynamodbtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/stretchr/testify/require"
)

func retentionTestTables() *Tables {
	return &Tables{names: map[Table]string{"snapshot": "s", "journal": "j", "head": "h"}, index: "history"}
}
func retentionTestKey(n string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-1"}, "skey": &types.AttributeValueMemberN{Value: n}}
}
func retentionTestQuery() *dynamodb.QueryInput {
	return &dynamodb.QueryInput{TableName: aws.String("s"), IndexName: aws.String("history"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: "Order-1"}}}
}

func TestRetentionPhaseTargets(t *testing.T) {
	tables := retentionTestTables()
	query := retentionTestQuery()
	batch := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"s": {{DeleteRequest: &types.DeleteRequest{Key: retentionTestKey("1")}}}}}
	mark := &dynamodb.UpdateItemInput{TableName: aws.String("s"), Key: retentionTestKey("1"), ConditionExpression: aws.String("attribute_exists(active_history_seq_nr)")}
	require.Equal(t, "retention-query", tables.retentionPhase(query))
	require.Equal(t, "retention-delete", tables.retentionPhase(batch))
	require.Equal(t, "retention-mark", tables.retentionPhase(mark))
	query.IndexName = aws.String("other")
	require.Empty(t, tables.retentionPhase(query))
	batch.RequestItems["s"][0].DeleteRequest.Key = retentionTestKey("0")
	require.Empty(t, tables.retentionPhase(batch))
	mark.Key["aid"] = &types.AttributeValueMemberS{Value: "__config__"}
	require.Empty(t, tables.retentionPhase(mark))
	require.Empty(t, tables.retentionPhase(commitInput()))
}

func TestRetentionFaultOrderAndApplicationCounts(t *testing.T) {
	for _, method := range []string{"replace-request", "replace-response"} {
		t.Run(method, func(t *testing.T) {
			specs := []conformance.FaultSpec{}
			for _, message := range []string{"first", "second"} {
				specs = append(specs, conformance.FaultSpec{Operation: 1, Phase: "retention-query", Kind: "storage-error", Repeat: "count", Count: 1, Injection: method, Details: map[string]any{"message": message}})
			}
			injection, finish := conformance.NewOperationInjection(1, specs)
			option := retentionTestTables().RetentionAPIOption(injection, nil)
			calls := 0
			for _, message := range []string{"first", "second"} {
				stack := middleware.NewStack("retention-test", func() any { return nil })
				require.NoError(t, option(stack))
				_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), retentionTestQuery(), middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
					calls++
					return &dynamodb.QueryOutput{}, middleware.Metadata{}, nil
				}))
				require.ErrorContains(t, err, message)
			}
			if method == "replace-request" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 2, calls)
			}
			require.Equal(t, 1, injection.Faults[0].Fired())
			require.Equal(t, 1, injection.Faults[1].Fired())
			require.NoError(t, finish())
		})
	}
}

func TestRetentionPagePlanCountsOnceAndMatchesContinuation(t *testing.T) {
	for _, method := range []string{"replace-request", "replace-response"} {
		t.Run(method, func(t *testing.T) {
			injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "retention-query", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: method, Details: map[string]any{"history_pages": []any{[]any{json.Number("4"), json.Number("3")}, []any{}, []any{json.Number("3"), json.Number("1")}}}}})
			option := retentionTestTables().RetentionAPIOption(injection, nil)
			input := retentionTestQuery()
			calls := 0
			for page := range 3 {
				stack := middleware.NewStack("retention-pages", func() any { return nil })
				require.NoError(t, option(stack))
				result, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
					calls++
					return &dynamodb.QueryOutput{}, middleware.Metadata{}, nil
				}))
				require.NoError(t, err)
				out := result.(*dynamodb.QueryOutput)
				want := [][]string{{"4", "3"}, {}, {"3", "1"}}[page]
				require.Len(t, out.Items, len(want))
				for i, n := range want {
					require.Equal(t, &types.AttributeValueMemberN{Value: n}, out.Items[i]["skey"])
				}
				if page < 2 {
					require.NotEmpty(t, out.LastEvaluatedKey)
				} else {
					require.Empty(t, out.LastEvaluatedKey)
				}
				input.ExclusiveStartKey = out.LastEvaluatedKey
			}
			require.Equal(t, 1, injection.Faults[0].Fired())
			require.NoError(t, finish())
			if method == "replace-request" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 3, calls)
			}
		})
	}
	plan := &historyResponsePlan{pages: [][]int64{{1}}}
	input := retentionTestQuery()
	input.ExclusiveStartKey = retentionTestKey("2")
	_, err := plan.response(input, nil)
	require.ErrorContains(t, err, "continuation")
}

func TestRetentionInvalidPlanAndOriginalFailureAreUnfired(t *testing.T) {
	for _, name := range []string{"invalid plan", "original failure", "wrong operation"} {
		t.Run(name, func(t *testing.T) {
			injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "retention-query", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"history_pages": []any{[]any{json.Number("1")}}}}})
			if name == "invalid plan" {
				injection.Faults[0].Spec.Details["history_pages"] = []any{[]any{"bad"}}
			}
			stack := middleware.NewStack("retention-unfired", func() any { return nil })
			require.NoError(t, retentionTestTables().RetentionAPIOption(injection, nil)(stack))
			op := 1
			if name == "wrong operation" {
				op = 2
			}
			_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), op), retentionTestQuery(), middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
				if name == "original failure" {
					return nil, middleware.Metadata{}, errors.New("SDK failed")
				}
				return &dynamodb.QueryOutput{}, middleware.Metadata{}, nil
			}))
			if name == "wrong operation" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, injection.Faults[0].Fired())
			require.ErrorContains(t, finish(), "fired 0 times")
		})
	}
}

func TestRetentionPartialDeleteCountsOnlyAfterSDKApplication(t *testing.T) {
	tables := retentionTestTables()
	processed := 0
	stackClient := dynamodb.New(dynamodb.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:1"), RetryMaxAttempts: 1, APIOptions: []func(*middleware.Stack) error{func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("partial-test", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			input := in.Parameters.(*dynamodb.BatchWriteItemInput)
			processed += len(input.RequestItems["s"])
			return middleware.InitializeOutput{Result: &dynamodb.BatchWriteItemOutput{}}, middleware.Metadata{}, nil
		}), middleware.Before)
	}}})
	tables.client = stackClient
	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"s": {{DeleteRequest: &types.DeleteRequest{Key: retentionTestKey("1")}}, {DeleteRequest: &types.DeleteRequest{Key: retentionTestKey("2")}}}}}
	observed := 0
	out, err := tables.partitionHistoryDelete(t.Context(), input, map[string]any{"unprocessed_first_n": json.Number("1")}, func(in *dynamodb.BatchWriteItemInput, out *dynamodb.BatchWriteItemOutput, err error) {
		observed++
		require.NoError(t, err)
		require.NotNil(t, out)
		require.Len(t, in.RequestItems["s"], 1)
	})
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 1, observed)
	require.Equal(t, input.RequestItems["s"][:1], out.UnprocessedItems["s"])
	_, err = tables.partitionHistoryDelete(t.Context(), input, map[string]any{"unprocessed_first_n": json.Number("-1")}, nil)
	require.Error(t, err)
	for _, code := range []string{"ProvisionedThroughputExceededException", "ConditionalCheckFailedException"} {
		err, buildErr := retentionSDKError(conformance.FaultSpec{Kind: "sdk-error", Details: map[string]any{"code": code}})
		require.NoError(t, buildErr)
		require.Error(t, err)
	}
	_, err = retentionSDKError(conformance.FaultSpec{Kind: "sdk-error"})
	require.Error(t, err)
	for _, sdkFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("SDK failure=%v", sdkFails), func(t *testing.T) {
			fixture := retentionTestTables()
			cause := errors.New("partial SDK failed")
			fixture.client = dynamodb.New(dynamodb.Options{Region: "us-east-1", APIOptions: []func(*middleware.Stack) error{func(stack *middleware.Stack) error {
				return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("partial-application", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					batch := in.Parameters.(*dynamodb.BatchWriteItemInput)
					require.Equal(t, input.RequestItems["s"][1:], batch.RequestItems["s"])
					if sdkFails {
						return middleware.InitializeOutput{}, middleware.Metadata{}, cause
					}
					return middleware.InitializeOutput{Result: &dynamodb.BatchWriteItemOutput{}}, middleware.Metadata{}, nil
				}), middleware.Before)
			}}})
			injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "retention-delete", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"unprocessed_first_n": json.Number("1")}}})
			stack := middleware.NewStack("partial-count", func() any { return nil })
			require.NoError(t, fixture.RetentionAPIOption(injection, nil)(stack))
			result, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), input, middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
				t.Fatal("original full delete must not run")
				return nil, middleware.Metadata{}, nil
			}))
			if sdkFails {
				require.ErrorIs(t, err, cause)
				require.Zero(t, injection.Faults[0].Fired())
				require.ErrorContains(t, finish(), "fired 0 times")
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, injection.Faults[0].Fired())
				require.NoError(t, finish())
				require.Equal(t, input.RequestItems["s"][:1], result.(*dynamodb.BatchWriteItemOutput).UnprocessedItems["s"])
			}
		})
	}
}
