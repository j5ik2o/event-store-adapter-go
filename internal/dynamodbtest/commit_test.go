package dynamodbtest

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/stretchr/testify/require"
)

func commitInput() *dynamodb.TransactWriteItemsInput {
	aid := &types.AttributeValueMemberS{Value: "Order-1"}
	return &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("j"), Item: map[string]types.AttributeValue{"aid": aid, "seq_nr": &types.AttributeValueMemberN{Value: "2"}}}},
		{Update: &types.Update{TableName: aws.String("h"), Key: map[string]types.AttributeValue{"aid": aid}}},
	}}
}

func TestCommitTargetsAndCancellationPositions(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j", "snapshot": "s", "head": "h"}}
	for _, reverse := range []bool{false, true} {
		input := commitInput()
		if reverse {
			input.TransactItems[0], input.TransactItems[1] = input.TransactItems[1], input.TransactItems[0]
		}
		targets := tables.commitTargets(input)
		require.ElementsMatch(t, []string{"journal", "head"}, targets)
		for _, oldHead := range []any{nil, json.Number("9007199254740991")} {
			err, buildErr := tables.commitSDKError(input, map[string]any{"code": "TransactionCanceledException", "cancellation_reasons": []any{
				map[string]any{"target": "head", "code": "ConditionalCheckFailed", "old_head_seq_nr": oldHead},
			}})
			require.NoError(t, buildErr)
			var canceled *types.TransactionCanceledException
			require.ErrorAs(t, err, &canceled)
			require.Len(t, canceled.CancellationReasons, 2)
			for i, target := range targets {
				if target == "journal" {
					require.Equal(t, "None", aws.ToString(canceled.CancellationReasons[i].Code))
				} else {
					require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[i].Code))
					if oldHead == nil {
						require.Empty(t, canceled.CancellationReasons[i].Item)
					} else {
						require.Equal(t, &types.AttributeValueMemberN{Value: "9007199254740991"}, canceled.CancellationReasons[i].Item["seq_nr"])
					}
				}
			}
		}
	}
	input := commitInput()
	input.TransactItems[1] = types.TransactWriteItem{Put: &types.Put{TableName: aws.String("h"), Item: map[string]types.AttributeValue{"aid": input.TransactItems[0].Put.Item["aid"]}}}
	require.Equal(t, []string{"journal", "head"}, tables.commitTargets(input))
	input.TransactItems[0].Put.Item["aid"] = &types.AttributeValueMemberS{Value: "__config__"}
	require.Nil(t, tables.commitTargets(input))
	_, err := tables.commitSDKError(commitInput(), map[string]any{"code": "TransactionCanceledException", "cancellation_reasons": []any{map[string]any{"target": "snapshot", "code": "TransactionConflict"}}})
	require.ErrorContains(t, err, "was not requested")
}

func TestCommitFaultApplication(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j", "head": "h"}}
	for _, injectionMethod := range []string{"replace-request", "replace-response"} {
		t.Run(injectionMethod, func(t *testing.T) {
			injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: injectionMethod, Details: map[string]any{"code": "ProvisionedThroughputExceededException"}}})
			r := NewRecorder()
			stack := middleware.NewStack("commit-test", func() any { return nil })
			require.NoError(t, r.APIOption(stack))
			require.NoError(t, tables.CommitAPIOption(injection)(stack))
			calls := 0
			_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), commitInput(), middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
				calls++
				return nil, middleware.Metadata{}, nil
			}))
			var throughput *types.ProvisionedThroughputExceededException
			require.ErrorAs(t, err, &throughput)
			require.Len(t, r.Requests(1), 1)
			if injectionMethod == "replace-request" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
			require.Equal(t, 1, injection.Faults[0].Fired())
			require.NoError(t, finish())
		})
	}
}

func TestCommitUnfiredFaults(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j", "head": "h"}}
	for _, tc := range []struct {
		name      string
		operation int
		details   map[string]any
	}{
		{"wrong operation tag", 2, map[string]any{"code": "ProvisionedThroughputExceededException"}},
		{"failed preparation", 1, map[string]any{"code": "TransactionCanceledException", "cancellation_reasons": []any{map[string]any{"target": "snapshot", "code": "TransactionConflict"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: tc.details}})
			stack := middleware.NewStack("commit-unfired", func() any { return nil })
			require.NoError(t, tables.CommitAPIOption(injection)(stack))
			calls := 0
			_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), tc.operation), commitInput(), middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
				calls++
				return nil, middleware.Metadata{}, nil
			}))
			if tc.operation == 2 {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			} else {
				require.Error(t, err)
				require.Zero(t, calls)
			}
			require.Zero(t, injection.Faults[0].Fired())
			require.ErrorContains(t, finish(), "fired 0 times")
		})
	}
}

func TestCommitConcurrentFaultApplication(t *testing.T) {
	tables := &Tables{names: map[Table]string{"journal": "j", "head": "h"}}
	injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"message": "commit failed"}}})
	var calls, failures atomic.Int32
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			stack := middleware.NewStack("commit-concurrent", func() any { return nil })
			if err := tables.CommitAPIOption(injection)(stack); err != nil {
				failures.Add(100)
				return
			}
			_, _, err := stack.HandleMiddleware(WithOperation(t.Context(), 1), commitInput(), middleware.HandlerFunc(func(context.Context, any) (any, middleware.Metadata, error) {
				calls.Add(1)
				return nil, middleware.Metadata{}, nil
			}))
			if err != nil {
				failures.Add(1)
			}
		})
	}
	workers.Wait()
	require.EqualValues(t, 1, failures.Load())
	require.EqualValues(t, 7, calls.Load())
	require.Equal(t, 1, injection.Faults[0].Fired())
	require.NoError(t, finish())
}
