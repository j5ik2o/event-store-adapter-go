package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/require"
)

type pairUnitInputKey struct{}

func pairUnitClient(r *dynamodbtest.Recorder, respond func(any) (any, error)) *awsdynamodb.Client {
	return awsdynamodb.New(awsdynamodb.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:1"), RetryMaxAttempts: 1,
		Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		APIOptions: []func(*middleware.Stack) error{r.APIOption, func(stack *middleware.Stack) error {
			if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("pair-unit-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				return next.HandleInitialize(context.WithValue(ctx, pairUnitInputKey{}, in.Parameters), in)
			}), middleware.Before); err != nil {
				return err
			}
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("pair-unit-response", func(ctx context.Context, _ middleware.FinalizeInput, _ middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				result, err := respond(ctx.Value(pairUnitInputKey{}))
				return middleware.FinalizeOutput{Result: result}, middleware.Metadata{}, err
			}), middleware.Before)
		}},
	})
}

func pairSnapshot[T any](t *testing.T, value T, seqNr eventstore.SeqNr, manifest string) eventstore.SnapshotEnvelope[T] {
	t.Helper()
	snapshot, err := eventstore.NewSnapshotEnvelope(value, seqNr, eventstore.WithManifest(manifest))
	require.NoError(t, err)
	return snapshot
}

func TestPersistEventAndSnapshotTransaction(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, seqNr := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprintf("history=%v/seq=%d", history, seqNr), func(t *testing.T) {
				r := dynamodbtest.NewRecorder()
				store := &opened{settings: configurationSettings(t)}
				if history {
					store.settings.common = storeoptions.Options{RetentionCount: aws.Int(2), RetentionMode: storeoptions.RetentionDelete}
				}
				store.client = pairUnitClient(r, func(input any) (any, error) {
					switch input.(type) {
					case *awsdynamodb.TransactWriteItemsInput:
						return &awsdynamodb.TransactWriteItemsOutput{}, nil
					case *awsdynamodb.QueryInput:
						return &awsdynamodb.QueryOutput{}, nil
					default:
						t.Fatalf("unexpected request %T", input)
						return nil, nil
					}
				})
				event := eventEnvelope(t, "型", "値", seqNr, time.Unix(0, -1), []byte{1, 2, 3}, "文")
				snapshot := pairSnapshot(t, []byte{4, 5}, seqNr, "状")
				serializer := eventSerializer[[]byte]{serialize: func(b []byte) ([]byte, error) { return b, nil }}
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, serializer, event, snapshot))
				requests := r.Requests(1)
				wantActions, wantRequests := 3, 1
				if history {
					wantActions, wantRequests = 4, 2
				}
				require.Len(t, requests, wantRequests)
				input := requests[0].Input.(*awsdynamodb.TransactWriteItemsInput)
				require.Len(t, input.TransactItems, wantActions)
				current := input.TransactItems[2].Put.Item
				require.Len(t, current, 6)
				require.Equal(t, &types.AttributeValueMemberN{Value: "0"}, current["skey"])
				require.Equal(t, &types.AttributeValueMemberN{Value: "-1"}, current["last_updated_at"])
				require.Equal(t, &types.AttributeValueMemberS{Value: "状"}, current["manifest"])
				require.Equal(t, &types.AttributeValueMemberB{Value: []byte{4, 5}}, current["payload"])
				if history {
					row := input.TransactItems[3].Put.Item
					require.Len(t, row, 7)
					require.Equal(t, row["seq_nr"], row["skey"])
					require.Equal(t, row["seq_nr"], row["active_history_seq_nr"])
					c, err := itemSizeUpperBound(current)
					require.NoError(t, err)
					h, err := itemSizeUpperBound(row)
					require.NoError(t, err)
					// UTF-8 names and values, three N values at 21 bytes each.
					require.Equal(t, 118, c)
					require.Equal(t, 160, h)
				}
			})
		}
	}
}

func TestPersistEventAndSnapshotPreparationDoesNotSend(t *testing.T) {
	for _, name := range []string{"event", "snapshot", "number", "event serializer", "snapshot serializer", "current size", "history size"} {
		t.Run(name, func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			store := &opened{client: configurationUnitClient(r), settings: configurationSettings(t)}
			store.settings.common.RetentionCount = aws.Int(1)
			event := eventEnvelope(t, "Order", "pair", 1, time.Unix(0, 1), "event", "")
			snapshot := pairSnapshot(t, "state", 1, "")
			calls := [2]int{}
			cause := errors.New("serializer failed")
			serializers := [2]eventstore.Serializer[string]{}
			for i := range serializers {
				serializers[i] = eventSerializer[string]{serialize: func(value string) ([]byte, error) {
					calls[i]++
					if name == []string{"event serializer", "snapshot serializer"}[i] {
						return nil, cause
					}
					if i == 1 && name == "current size" {
						return make([]byte, 409600), nil
					}
					// Names 43 + aid 10 + three N values at 21 = 116 before payload.
					if i == 1 && name == "history size" {
						return make([]byte, 409600-116), nil
					}
					return []byte(value), nil
				}}
			}
			switch name {
			case "event":
				event = eventstore.EventEnvelope[string]{}
			case "snapshot":
				snapshot = eventstore.SnapshotEnvelope[string]{}
			case "number":
				snapshot = pairSnapshot(t, "state", 2, "")
			}
			err := persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, serializers[0], serializers[1], event, snapshot)
			kind := eventstore.KindContractViolation
			if name == "event serializer" || name == "snapshot serializer" {
				kind = eventstore.KindSerialization
				require.ErrorIs(t, err, cause)
			}
			requireEventKind(t, err, kind)
			if name == "event" || name == "snapshot" || name == "number" {
				require.Equal(t, [2]int{}, calls)
			}
			require.Empty(t, r.Requests(1))
		})
	}
}

func TestPersistEventAndSnapshotConflictPositions(t *testing.T) {
	for _, actions := range []int{3, 4} {
		for position := range actions {
			t.Run(fmt.Sprintf("%d/%d", actions, position), func(t *testing.T) {
				r := dynamodbtest.NewRecorder()
				reasons := make([]types.CancellationReason, actions)
				for i := range reasons {
					reasons[i].Code = aws.String("None")
				}
				reasons[1] = types.CancellationReason{Code: aws.String("ConditionalCheckFailed"), Item: map[string]types.AttributeValue{"seq_nr": &types.AttributeValueMemberN{Value: "1"}}}
				reasons[position].Code = aws.String("TransactionConflict")
				cause := &types.TransactionCanceledException{CancellationReasons: reasons}
				store := &opened{settings: configurationSettings(t), client: pairUnitClient(r, func(any) (any, error) { return nil, cause })}
				if actions == 4 {
					store.settings.common.RetentionCount = aws.Int(1)
				}
				event := eventEnvelope(t, "Order", "conflict", 4, time.Unix(0, 1), "event", "")
				err := persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, pairSnapshot(t, "state", 4, ""))
				requireEventKind(t, err, eventstore.KindOptimisticLock)
				require.ErrorIs(t, err, cause)
				require.Len(t, r.Requests(1), 1, "classification and failed commits must not read or retain")
			})
		}
	}
}
