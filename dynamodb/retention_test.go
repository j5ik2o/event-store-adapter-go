package dynamodb

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

func TestRetentionSelectsAllPagesAndJustWritten(t *testing.T) {
	r := dynamodbtest.NewRecorder()
	store := &opened{settings: configurationSettings(t)}
	store.settings.common.RetentionCount = aws.Int(2)
	pages := [][]eventstore.SeqNr{{4, 3}, {3, 2, 1}}
	query := 0
	var deleted []map[string]types.AttributeValue
	start := historyKey("Order-1", 3)
	start["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: "3"}
	store.client = pairUnitClient(r, func(input any) (any, error) {
		switch input := input.(type) {
		case *awsdynamodb.QueryInput:
			require.Equal(t, "history", aws.ToString(input.IndexName))
			require.False(t, aws.ToBool(input.ConsistentRead))
			require.False(t, aws.ToBool(input.ScanIndexForward))
			require.Equal(t, &types.AttributeValueMemberS{Value: "Order-1"}, input.ExpressionAttributeValues[":aid"])
			out := &awsdynamodb.QueryOutput{}
			for _, n := range pages[query] {
				out.Items = append(out.Items, historyKey("Order-1", n))
			}
			if query == 0 {
				require.Empty(t, input.ExclusiveStartKey)
				out.LastEvaluatedKey = start
			} else {
				require.Equal(t, start, input.ExclusiveStartKey)
			}
			query++
			return out, nil
		case *awsdynamodb.BatchWriteItemInput:
			for _, write := range input.RequestItems["snapshot"] {
				deleted = append(deleted, write.DeleteRequest.Key)
			}
			return &awsdynamodb.BatchWriteItemOutput{}, nil
		default:
			t.Fatalf("unexpected request %T", input)
			return nil, nil
		}
	})
	require.NoError(t, store.retainHistory(dynamodbtest.WithOperation(t.Context(), 1), "Order-1", 5))
	require.Equal(t, 2, query)
	require.Equal(t, []map[string]types.AttributeValue{historyKey("Order-1", 1), historyKey("Order-1", 2), historyKey("Order-1", 3)}, deleted)
}

func TestRetentionQueryFailureDoesNotDelete(t *testing.T) {
	for _, name := range []string{"SDK", "different aid", "invalid skey", "current key"} {
		t.Run(name, func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			cause := errors.New("query failed")
			store := &opened{settings: configurationSettings(t)}
			store.settings.common.RetentionCount = aws.Int(1)
			store.client = pairUnitClient(r, func(input any) (any, error) {
				require.IsType(t, &awsdynamodb.QueryInput{}, input)
				if name == "SDK" {
					return nil, cause
				}
				item := historyKey("Order-1", 1)
				switch name {
				case "different aid":
					item["aid"] = &types.AttributeValueMemberS{Value: "Other-1"}
				case "invalid skey":
					delete(item, "skey")
				case "current key":
					item["skey"] = &types.AttributeValueMemberN{Value: "0"}
				}
				return &awsdynamodb.QueryOutput{Items: []map[string]types.AttributeValue{item}}, nil
			})
			err := store.retainHistory(dynamodbtest.WithOperation(t.Context(), 1), "Order-1", 2)
			require.Error(t, err)
			if name == "SDK" {
				require.ErrorIs(t, err, cause)
			}
			require.Len(t, r.Requests(1), 1)
		})
	}
}

func TestRetentionDeleteBatchesAndRetriesOnlyPending(t *testing.T) {
	for _, limit := range []int{0, 1, 7} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			hooks := testhook.New()
			var sleeps []time.Duration
			hooks.SetSleeper(func(d time.Duration) { sleeps = append(sleeps, d) })
			store := &opened{settings: configurationSettings(t), hooks: hooks}
			store.settings.configurationReadRetryLimit = limit
			var sizes []int
			var pending map[string][]types.WriteRequest
			store.client = pairUnitClient(r, func(input any) (any, error) {
				batch := input.(*awsdynamodb.BatchWriteItemInput)
				writes := batch.RequestItems["snapshot"]
				sizes = append(sizes, len(writes))
				if len(sizes) == 1 {
					pending = map[string][]types.WriteRequest{"snapshot": writes[:1]}
				} else if len(writes) == 1 {
					require.Equal(t, pending, batch.RequestItems)
				} else {
					require.Len(t, writes, 2)
				}
				if len(sizes) <= limit+1 {
					return &awsdynamodb.BatchWriteItemOutput{UnprocessedItems: pending}, nil
				}
				return &awsdynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{"snapshot": nil}}, nil
			})
			seqs := make([]eventstore.SeqNr, 27)
			for i := range seqs {
				seqs[i] = eventstore.SeqNr(i + 1)
			}
			err := store.deleteHistory(dynamodbtest.WithOperation(t.Context(), 1), "Order-1", seqs)
			require.ErrorContains(t, err, "retry limit")
			require.Len(t, sizes, limit+1)
			require.Equal(t, 25, sizes[0])
			for _, size := range sizes[1:] {
				require.Equal(t, 1, size)
			}
			want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
			require.Equal(t, want[:limit], append([]time.Duration{}, sleeps...))
		})
	}
	// A successful retry finishes its initial batch before the next 25-item chunk.
	r := dynamodbtest.NewRecorder()
	hooks := testhook.New()
	hooks.SetSleeper(func(time.Duration) {})
	store := &opened{settings: configurationSettings(t), hooks: hooks}
	var sizes []int
	store.client = pairUnitClient(r, func(input any) (any, error) {
		batch := input.(*awsdynamodb.BatchWriteItemInput)
		writes := batch.RequestItems["snapshot"]
		sizes = append(sizes, len(writes))
		if len(sizes) == 1 {
			return &awsdynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{"snapshot": writes[:2]}}, nil
		}
		return &awsdynamodb.BatchWriteItemOutput{}, nil
	})
	seqs := make([]eventstore.SeqNr, 52)
	for i := range seqs {
		seqs[i] = eventstore.SeqNr(i + 1)
	}
	require.NoError(t, store.deleteHistory(t.Context(), "Order-1", seqs))
	require.Equal(t, []int{25, 2, 25, 2}, sizes)
}

func TestRetentionTTLUsesMarkTimeAndSkipsConditionFailure(t *testing.T) {
	r := dynamodbtest.NewRecorder()
	hooks := testhook.New()
	clock := int64(4102444800)
	hooks.SetClock(func() time.Time { n := clock; clock++; return time.Unix(n, 999) })
	store := &opened{settings: configurationSettings(t), hooks: hooks}
	store.settings.common.TTLGraceSeconds = math.MaxInt64
	var expires []string
	store.client = pairUnitClient(r, func(input any) (any, error) {
		mark := input.(*awsdynamodb.UpdateItemInput)
		require.Equal(t, "attribute_exists(active_history_seq_nr)", aws.ToString(mark.ConditionExpression))
		require.Equal(t, "SET #ttl = :expires REMOVE active_history_seq_nr", aws.ToString(mark.UpdateExpression))
		require.Equal(t, map[string]string{"#ttl": "ttl"}, mark.ExpressionAttributeNames)
		expires = append(expires, mark.ExpressionAttributeValues[":expires"].(*types.AttributeValueMemberN).Value)
		if len(expires) == 1 {
			return nil, &types.ConditionalCheckFailedException{}
		}
		return &awsdynamodb.UpdateItemOutput{}, nil
	})
	require.NoError(t, store.markHistory(t.Context(), "Order-1", []eventstore.SeqNr{1, 2}))
	require.Equal(t, []string{"9223372040957220607", "9223372040957220608"}, expires)
	store.client = pairUnitClient(r, func(any) (any, error) { return nil, errors.New("mark failed") })
	require.ErrorContains(t, store.markHistory(t.Context(), "Order-1", []eventstore.SeqNr{3}), "mark failed")
}

type retentionLogHandler struct {
	records       []slog.Record
	err           error
	panicOnHandle bool
}

func (*retentionLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *retentionLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())
	if h.panicOnHandle {
		panic("logger failed")
	}
	return h.err
}
func (h *retentionLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *retentionLogHandler) WithGroup(string) slog.Handler      { return h }

func TestRetentionNotificationFailuresPreserveSuccess(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, name := range []string{"log only", "callback", "log error", "log panic", "callback panic", "both panic"} {
		t.Run(name, func(t *testing.T) {
			logger := &retentionLogHandler{panicOnHandle: name == "log panic" || name == "both panic"}
			if name == "log error" {
				logger.err = errors.New("log failure")
			}
			slog.SetDefault(slog.New(logger))
			store := &opened{settings: configurationSettings(t)}
			cause := errors.New("retention cause")
			calls := 0
			if name != "log only" {
				store.settings.common.RetentionFailureHandler = func(ctx context.Context, err error) {
					calls++
					require.Equal(t, t.Context(), ctx)
					requireEventKind(t, err, eventstore.KindStorage)
					require.ErrorIs(t, err, cause)
					if name == "callback panic" || name == "both panic" {
						panic("callback failed")
					}
				}
			}
			require.NotPanics(t, func() { store.notifyRetentionFailure(t.Context(), "Order-1", cause) })
			require.Len(t, logger.records, 1)
			if name != "log only" {
				require.Equal(t, 1, calls)
			}
		})
	}
}
