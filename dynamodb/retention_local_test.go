package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

type retentionSDKResult struct {
	API       string
	Input     any
	Output    any
	Error     error
	RequestID string
}
type retentionSDKResults struct {
	mu      sync.Mutex
	results []retentionSDKResult
}
type retentionSDKInputKey struct{}

func (r *retentionSDKResults) apiOption(stack *middleware.Stack) error {
	if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("observe-retention-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
		return next.HandleInitialize(context.WithValue(ctx, retentionSDKInputKey{}, in.Parameters), in)
	}), middleware.Before); err != nil {
		return err
	}
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("observe-original-retention-result", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleFinalize(ctx, in)
		input := ctx.Value(retentionSDKInputKey{})
		switch input.(type) {
		case *awsdynamodb.QueryInput, *awsdynamodb.BatchWriteItemInput, *awsdynamodb.UpdateItemInput:
			id, _ := awsmiddleware.GetRequestIDMetadata(metadata)
			r.mu.Lock()
			r.results = append(r.results, retentionSDKResult{API: middleware.GetOperationName(ctx), Input: input, Output: out.Result, Error: err, RequestID: id})
			r.mu.Unlock()
		}
		return out, metadata, err
	}), middleware.Before)
}
func (r *retentionSDKResults) calls() []retentionSDKResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]retentionSDKResult(nil), r.results...)
}
func traceRetentionSDK(t *testing.T, r *retentionSDKResults) {
	t.Helper()
	for _, call := range r.calls() {
		require.NotEmpty(t, call.RequestID)
		data := map[string]any{"original_sdk_api": call.API, "original_sdk_input": call.Input, "original_sdk_output": call.Output, "request_id": call.RequestID, "error_type": fmt.Sprintf("%T", call.Error)}
		if call.Error != nil {
			var code smithy.APIError
			if errors.As(call.Error, &code) {
				data["error_code"] = code.ErrorCode()
			}
			data["cause_type"] = fmt.Sprintf("%T", errors.Unwrap(call.Error))
		}
		traceEvent(t, data)
	}
}

// Fixtures are explicit history records installed through the independent client.
// They are never generated from the expected retained rows or the production helper.
func seedRetentionHistory(t *testing.T, e *dynamodbtest.Environment, cfg Config, aid string, n int) {
	t.Helper()
	for seq := 1; seq <= n; seq++ {
		number := strconv.Itoa(seq)
		_, err := e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.SnapshotTableName), Item: map[string]types.AttributeValue{
			"aid": &types.AttributeValueMemberS{Value: aid}, "skey": &types.AttributeValueMemberN{Value: number}, "seq_nr": &types.AttributeValueMemberN{Value: number},
			"manifest": &types.AttributeValueMemberS{Value: "fixture"}, "payload": &types.AttributeValueMemberB{Value: []byte(`"fixture state"`)},
			"last_updated_at": &types.AttributeValueMemberN{Value: "0"}, "active_history_seq_nr": &types.AttributeValueMemberN{Value: number},
		}})
		require.NoError(t, err)
	}
}
func appendRetentionEvents(t *testing.T, store *opened, value string, n int) {
	t.Helper()
	for seq := 1; seq <= n; seq++ {
		require.NoError(t, persistEvent(t.Context(), store, eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Order", value, eventstore.SeqNr(seq), time.Unix(0, int64(seq)), "fixture event", "fixture")))
	}
}
func historyActive(t *testing.T, tables *dynamodbtest.Tables, aid string, want []int64) {
	t.Helper()
	history, err := tables.History(t.Context(), aid)
	require.NoError(t, err)
	require.Equal(t, want, history.Active)
	traceEvent(t, map[string]any{"observed_history": history})
}
func loggedRetentionFailure(t *testing.T, logger *retentionLogHandler, cause error) {
	t.Helper()
	require.Len(t, logger.records, 1)
	require.Equal(t, slog.LevelError, logger.records[0].Level)
	found := false
	logger.records[0].Attrs(func(attr slog.Attr) bool {
		if attr.Key == "error" {
			failure, ok := attr.Value.Any().(error)
			require.True(t, ok)
			requireEventKind(t, failure, eventstore.KindStorage)
			if cause != nil {
				require.ErrorIs(t, failure, cause)
			}
			found = true
		}
		return true
	})
	require.True(t, found)
}

func TestRetentionLocal(t *testing.T) {
	e := configurationEnvironment(t)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	t.Run("all pages omit current history and remove duplicates", func(t *testing.T) {
		for _, omit := range []bool{true, false} {
			t.Run(fmt.Sprint(omit), func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				firstPage := []any{json.Number("4"), json.Number("3")}
				if !omit {
					firstPage = []any{json.Number("5"), json.Number("4")}
				}
				injection, finish := conformance.NewOperationInjection(99, []conformance.FaultSpec{{Operation: 99, Phase: "retention-query", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"history_pages": []any{firstPage, []any{json.Number("3"), json.Number("2"), json.Number("1")}}, "omit_just_written_history": omit}}})
				r, commitSDK, retentionSDK := dynamodbtest.NewRecorder(), &eventSDKResults{}, &retentionSDKResults{}
				count, err := eventstore.KeepLatest(2)
				require.NoError(t, err)
				logger := &retentionLogHandler{}
				slog.SetDefault(slog.New(logger))
				store, err := open(t.Context(), e.NewClient(r.APIOption, commitSDK.apiOption, retentionSDK.apiOption, tables.RetentionAPIOption(injection, nil)), cfg, nil, eventstore.WithRetentionCount(count))
				require.NoError(t, err)
				appendRetentionEvents(t, store, "pages", 4)
				seedRetentionHistory(t, e, cfg, "Order-pages", 4)
				seedRetentionHistory(t, e, cfg, "Order-pages-other", 3)
				otherBefore := observedEventRows(t, tables, "Order-pages-other")
				event := eventEnvelope(t, "Order", "pages", 5, time.Unix(0, 123456789), "event", "")
				snapshot := pairSnapshot(t, "state", 5, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot))
				require.NoError(t, finish())
				require.Equal(t, 1, injection.Faults[0].Fired())
				historyActive(t, tables, event.AggregateID(), []int64{4, 5})
				require.Equal(t, otherBefore, observedEventRows(t, tables, "Order-pages-other"))
				require.Empty(t, logger.records)
				assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
				requests := r.Requests(99)
				require.Len(t, requests, 4)
				require.Equal(t, "Query", requests[1].API)
				require.Equal(t, "Query", requests[2].API)
				second := requests[2].Input.(*awsdynamodb.QueryInput)
				last := firstPage[len(firstPage)-1].(json.Number).String()
				require.Equal(t, &types.AttributeValueMemberN{Value: last}, second.ExclusiveStartKey["skey"])
				original := retentionSDK.calls()
				require.Len(t, original, 3)
				firstOriginal := original[0].Output.(*awsdynamodb.QueryOutput)
				found := false
				for _, item := range firstOriginal.Items {
					if item["skey"].(*types.AttributeValueMemberN).Value == "5" {
						found = true
					}
				}
				require.True(t, found, "real SDK result includes just-written history separately from the response plan")
				tracePairSDK(t, commitSDK)
				traceRetentionSDK(t, retentionSDK)
				traceEvent(t, map[string]any{"tables": cfg, "response_plan": injection.Faults[0].Spec.Details, "fault_fired": injection.Faults[0].Fired(), "requests": requests, "rows": observedEventRows(t, tables, event.AggregateID())})
			})
		}
	})
	t.Run("delete batches process only actual unprocessed retries", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		injection, finish := conformance.NewOperationInjection(99, []conformance.FaultSpec{{Operation: 99, Phase: "retention-delete", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"unprocessed_first_n": json.Number("2")}}})
		r, retentionSDK := dynamodbtest.NewRecorder(), &retentionSDKResults{}
		hooks := testhook.New()
		var sleeps []time.Duration
		hooks.SetSleeper(func(d time.Duration) { sleeps = append(sleeps, d) })
		count, err := eventstore.KeepLatest(1)
		require.NoError(t, err)
		partialCalls := 0
		option := tables.RetentionAPIOption(injection, func(input *awsdynamodb.BatchWriteItemInput, out *awsdynamodb.BatchWriteItemOutput, err error) {
			partialCalls++
			require.NoError(t, err)
			require.Len(t, input.RequestItems[cfg.SnapshotTableName], 23)
			require.Empty(t, out.UnprocessedItems)
			traceEvent(t, map[string]any{"partial_original_sdk_input": input, "partial_original_sdk_output": out})
		})
		store, err := open(t.Context(), e.NewClient(r.APIOption, retentionSDK.apiOption, option), cfg, hooks, eventstore.WithRetentionCount(count))
		require.NoError(t, err)
		appendRetentionEvents(t, store, "batches", 52)
		seedRetentionHistory(t, e, cfg, "Order-batches", 52)
		event := eventEnvelope(t, "Order", "batches", 53, time.Unix(0, 123), "event", "")
		snapshot := pairSnapshot(t, "state", 53, "")
		require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot))
		require.NoError(t, finish())
		require.Equal(t, 1, injection.Faults[0].Fired())
		require.Equal(t, 1, partialCalls)
		require.Equal(t, []time.Duration{50 * time.Millisecond}, sleeps)
		requests := r.Requests(99)
		var batches [][]types.WriteRequest
		for _, request := range requests {
			if input, ok := request.Input.(*awsdynamodb.BatchWriteItemInput); ok {
				batches = append(batches, input.RequestItems[cfg.SnapshotTableName])
			}
		}
		require.Len(t, batches, 4)
		sizes := []int{}
		for _, batch := range batches {
			sizes = append(sizes, len(batch))
		}
		require.Equal(t, []int{25, 2, 25, 2}, sizes)
		require.Equal(t, batches[0][:2], batches[1])
		historyActive(t, tables, event.AggregateID(), []int64{53})
		assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
		traceRetentionSDK(t, retentionSDK)
		traceEvent(t, map[string]any{"tables": cfg, "requests": requests, "sleeps": sleeps, "fault_fired": injection.Faults[0].Fired(), "partial_original_sdk_calls": partialCalls})
	})
	t.Run("delete retry limits leave pending and recover on same opened", func(t *testing.T) {
		for _, limit := range []int{0, 6} {
			t.Run(strconv.Itoa(limit), func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				cfg.ConfigurationReadRetryLimit = &limit
				injection, finish := conformance.NewOperationInjection(99, []conformance.FaultSpec{{Operation: 99, Phase: "retention-delete", Kind: "sdk-response", Repeat: "count", Count: limit + 1, Injection: "replace-request", Details: map[string]any{"unprocessed_first_n": json.Number("1")}}})
				r, retentionSDK := dynamodbtest.NewRecorder(), &retentionSDKResults{}
				hooks := testhook.New()
				var sleeps []time.Duration
				hooks.SetSleeper(func(d time.Duration) { sleeps = append(sleeps, d) })
				count, err := eventstore.KeepLatest(1)
				require.NoError(t, err)
				logger := &retentionLogHandler{}
				slog.SetDefault(slog.New(logger))
				var notifications []error
				partialCalls := 0
				option := tables.RetentionAPIOption(injection, func(input *awsdynamodb.BatchWriteItemInput, out *awsdynamodb.BatchWriteItemOutput, err error) {
					partialCalls++
					require.NoError(t, err)
					require.Len(t, input.RequestItems[cfg.SnapshotTableName], 4)
					traceEvent(t, map[string]any{"partial_original_sdk_input": input, "partial_original_sdk_output": out})
				})
				store, err := open(t.Context(), e.NewClient(r.APIOption, retentionSDK.apiOption, option), cfg, hooks, eventstore.WithRetentionCount(count), eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) { notifications = append(notifications, err) }))
				require.NoError(t, err)
				appendRetentionEvents(t, store, "limit", 5)
				seedRetentionHistory(t, e, cfg, "Order-limit", 5)
				event := eventEnvelope(t, "Order", "limit", 6, time.Unix(0, 123), "event", "")
				snapshot := pairSnapshot(t, "state", 6, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot))
				require.NoError(t, finish())
				require.Equal(t, limit+1, injection.Faults[0].Fired())
				require.Equal(t, 1, partialCalls)
				require.Len(t, notifications, 1)
				loggedRetentionFailure(t, logger, nil)
				historyActive(t, tables, event.AggregateID(), []int64{1, 6})
				assertEventAttributes(t, tables, event, []byte(`"event"`))
				assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
				assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), true)
				requests := r.Requests(99)
				require.Len(t, requests, limit+3)
				for _, request := range requests[3:] {
					writes := request.Input.(*awsdynamodb.BatchWriteItemInput).RequestItems[cfg.SnapshotTableName]
					require.Len(t, writes, 1)
					require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, writes[0].DeleteRequest.Key["skey"])
				}
				wantSleeps := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second}
				require.Equal(t, wantSleeps[:limit], append([]time.Duration{}, sleeps...))
				before := observedEventRows(t, tables, event.AggregateID())["snapshot"]
				require.NoError(t, persistEvent(dynamodbtest.WithOperation(t.Context(), 100), store, eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Order", "limit", 7, time.Unix(0, 1), "single", "")))
				assertEventRequest(t, r, 100, cfg, 7)
				require.Equal(t, before, observedEventRows(t, tables, event.AggregateID())["snapshot"])
				require.Len(t, notifications, 1)
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 101), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Order", "limit", 8, time.Unix(0, 1), "recovery", ""), pairSnapshot(t, "recovered", 8, "")))
				historyActive(t, tables, event.AggregateID(), []int64{8})
				require.Len(t, notifications, 1)
				require.Len(t, logger.records, 1)
				traceRetentionSDK(t, retentionSDK)
				traceEvent(t, map[string]any{"tables": cfg, "retry_limit": limit, "sleeps": sleeps, "fault_fired": injection.Faults[0].Fired(), "requests_failed_retention": requests, "requests_event_only": r.Requests(100), "requests_recovery": r.Requests(101), "notification_count": len(notifications), "rows_after_recovery": observedEventRows(t, tables, event.AggregateID())})
			})
		}
	})
	t.Run("TTL conditional skip and uint64 expiration remain fixed", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, true)
		injection, finish := conformance.NewOperationInjection(99, []conformance.FaultSpec{{Operation: 99, Phase: "retention-query", Kind: "sdk-response", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"history_pages": []any{[]any{json.Number("5"), json.Number("4"), json.Number("3"), json.Number("2"), json.Number("1")}}, "omit_just_written_history": true}}})
		r, retentionSDK := dynamodbtest.NewRecorder(), &retentionSDKResults{}
		hooks := testhook.New()
		clock := int64(4102444800)
		hooks.SetClock(func() time.Time { return time.Unix(clock, 999) })
		count, err := eventstore.KeepLatest(2)
		require.NoError(t, err)
		logger := &retentionLogHandler{}
		slog.SetDefault(slog.New(logger))
		store, err := open(t.Context(), e.NewClient(r.APIOption, retentionSDK.apiOption, tables.RetentionAPIOption(injection, nil)), cfg, hooks, eventstore.WithRetentionCount(count), eventstore.WithRetentionMode(eventstore.RetentionTTL), eventstore.WithTTLGraceSeconds(math.MaxInt64))
		require.NoError(t, err)
		appendRetentionEvents(t, store, "ttl", 5)
		seedRetentionHistory(t, e, cfg, "Order-ttl", 5)
		// An independent real update marks row 1 before the stale index response plan.
		_, err = e.NewClient().UpdateItem(t.Context(), &awsdynamodb.UpdateItemInput{TableName: aws.String(cfg.SnapshotTableName), Key: eventKey("Order-ttl", "skey", 1), UpdateExpression: aws.String("SET #ttl = :expires REMOVE active_history_seq_nr"), ConditionExpression: aws.String("attribute_exists(active_history_seq_nr)"), ExpressionAttributeNames: map[string]string{"#ttl": "ttl"}, ExpressionAttributeValues: map[string]types.AttributeValue{":expires": &types.AttributeValueMemberN{Value: "4102444900"}}})
		require.NoError(t, err)
		event := eventEnvelope(t, "Order", "ttl", 6, time.Unix(0, 123), "event", "")
		snapshot := pairSnapshot(t, "state", 6, "")
		require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot))
		require.NoError(t, finish())
		require.Empty(t, logger.records)
		history, err := tables.History(t.Context(), event.AggregateID())
		require.NoError(t, err)
		require.Equal(t, []int64{5, 6}, history.Active)
		require.Equal(t, []dynamodbtest.MarkedHistory{{SeqNr: 1, TTL: "4102444900"}, {SeqNr: 2, TTL: "9223372040957220607"}, {SeqNr: 3, TTL: "9223372040957220607"}, {SeqNr: 4, TTL: "9223372040957220607"}}, history.Marked)
		for _, marked := range history.Marked {
			item, err := tables.GetItem(t.Context(), "snapshot", eventKey(event.AggregateID(), "skey", eventstore.SeqNr(marked.SeqNr)))
			require.NoError(t, err)
			require.NotContains(t, item, "active_history_seq_nr")
			require.IsType(t, &types.AttributeValueMemberN{}, item["ttl"])
			require.Len(t, item, 7)
		}
		calls := retentionSDK.calls()
		require.Len(t, calls, 5)
		require.IsType(t, &awsdynamodb.UpdateItemInput{}, calls[1].Input)
		var condition *types.ConditionalCheckFailedException
		require.ErrorAs(t, calls[1].Error, &condition)
		require.NoError(t, calls[2].Error)
		assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
		assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), true)
		clock += 100
		require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 100), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Order", "ttl", 7, time.Unix(0, 1), "later", ""), pairSnapshot(t, "later-state", 7, "")))
		after, err := tables.History(t.Context(), event.AggregateID())
		require.NoError(t, err)
		require.Equal(t, []int64{6, 7}, after.Active)
		require.Equal(t, history.Marked, after.Marked[:4])
		require.Equal(t, dynamodbtest.MarkedHistory{SeqNr: 5, TTL: "9223372040957220707"}, after.Marked[4])
		require.Empty(t, logger.records)
		traceRetentionSDK(t, retentionSDK)
		traceEvent(t, map[string]any{"tables": cfg, "fault_fired": injection.Faults[0].Fired(), "history_before_clock_change": history, "history_after_clock_change": after, "first_requests": r.Requests(99), "later_requests": r.Requests(100)})
	})
	t.Run("each retention phase failure reports and recovers on same opened", func(t *testing.T) {
		for _, phase := range []string{"retention-query", "retention-delete", "retention-mark"} {
			t.Run(phase, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, phase == "retention-mark")
				injection, finish := conformance.NewOperationInjection(2, []conformance.FaultSpec{{Operation: 2, Phase: phase, Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"code": "ProvisionedThroughputExceededException"}}})
				r, commitSDK, retentionSDK := dynamodbtest.NewRecorder(), &eventSDKResults{}, &retentionSDKResults{}
				hooks := testhook.New()
				hooks.SetClock(func() time.Time { return time.Unix(4102444800, 0) })
				count, err := eventstore.KeepLatest(1)
				require.NoError(t, err)
				var notifications []error
				logger := &retentionLogHandler{}
				slog.SetDefault(slog.New(logger))
				opts := []eventstore.Option{eventstore.WithRetentionCount(count), eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) { notifications = append(notifications, err) })}
				if phase == "retention-mark" {
					opts = append(opts, eventstore.WithRetentionMode(eventstore.RetentionTTL))
				}
				store, err := open(t.Context(), e.NewClient(r.APIOption, commitSDK.apiOption, retentionSDK.apiOption, tables.RetentionAPIOption(injection, nil)), cfg, hooks, opts...)
				require.NoError(t, err)
				serializer := eventstore.NewJSONSerializer[string]()
				first := eventEnvelope(t, "Order", "recover", 1, time.Unix(0, 123), "first", "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, serializer, first, pairSnapshot(t, "first-state", 1, "")))
				second := eventEnvelope(t, "Order", "recover", 2, time.Unix(0, 456), "second", "")
				snapshot := pairSnapshot(t, "second-state", 2, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 2), store, serializer, serializer, second, snapshot))
				require.NoError(t, finish())
				require.Equal(t, 1, injection.Faults[0].Fired())
				require.Len(t, notifications, 1)
				var sdkError *types.ProvisionedThroughputExceededException
				require.ErrorAs(t, notifications[0], &sdkError)
				requireEventKind(t, notifications[0], eventstore.KindStorage)
				loggedRetentionFailure(t, logger, sdkError)
				historyActive(t, tables, second.AggregateID(), []int64{1, 2})
				assertEventAttributes(t, tables, second, []byte(`"second"`))
				assertPairSnapshot(t, tables, second, snapshot, []byte(`"second-state"`), false)
				assertPairSnapshot(t, tables, second, snapshot, []byte(`"second-state"`), true)
				before := observedEventRows(t, tables, second.AggregateID())["snapshot"]
				originalCount := len(retentionSDK.calls())
				require.NoError(t, persistEvent(dynamodbtest.WithOperation(t.Context(), 3), store, serializer, eventEnvelope(t, "Order", "recover", 3, time.Unix(0, 1), "single", "")))
				assertEventRequest(t, r, 3, cfg, 3)
				require.Len(t, retentionSDK.calls(), originalCount)
				require.Equal(t, before, observedEventRows(t, tables, second.AggregateID())["snapshot"])
				require.Len(t, notifications, 1)
				fourth := eventEnvelope(t, "Order", "recover", 4, time.Unix(0, 1), "recovered", "")
				fourthSnapshot := pairSnapshot(t, "recovered-state", 4, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 4), store, serializer, serializer, fourth, fourthSnapshot))
				historyActive(t, tables, second.AggregateID(), []int64{4})
				require.Len(t, notifications, 1)
				require.Len(t, logger.records, 1)
				assertEventAttributes(t, tables, fourth, []byte(`"recovered"`))
				assertPairSnapshot(t, tables, fourth, fourthSnapshot, []byte(`"recovered-state"`), false)
				if phase == "retention-mark" {
					history, err := tables.History(t.Context(), second.AggregateID())
					require.NoError(t, err)
					require.Equal(t, []dynamodbtest.MarkedHistory{{SeqNr: 1, TTL: "4102444800"}, {SeqNr: 2, TTL: "4102444800"}}, history.Marked)
				}
				tracePairSDK(t, commitSDK)
				traceRetentionSDK(t, retentionSDK)
				traceEvent(t, map[string]any{"tables": cfg, "failed_phase": phase, "fault_fired": injection.Faults[0].Fired(), "failure_type": fmt.Sprintf("%T", notifications[0]), "cause_type": fmt.Sprintf("%T", errors.Unwrap(notifications[0])), "requests_failure": r.Requests(2), "requests_event_only": r.Requests(3), "requests_recovery": r.Requests(4), "rows_after_recovery": observedEventRows(t, tables, second.AggregateID())})
			})
		}
	})
	t.Run("logging and callback failures leave pair successful", func(t *testing.T) {
		for _, name := range []string{"no callback", "log error", "log panic", "callback panic", "both panic"} {
			t.Run(name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "retention-query", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-request"}})
				logger := &retentionLogHandler{panicOnHandle: name == "log panic" || name == "both panic"}
				if name == "log error" {
					logger.err = errors.New("logging failed")
				}
				slog.SetDefault(slog.New(logger))
				count, err := eventstore.KeepLatest(1)
				require.NoError(t, err)
				opts := []eventstore.Option{eventstore.WithRetentionCount(count)}
				callbacks := 0
				if name != "no callback" {
					opts = append(opts, eventstore.WithRetentionFailureHandler(func(context.Context, error) {
						callbacks++
						if name == "callback panic" || name == "both panic" {
							panic("callback failed")
						}
					}))
				}
				store, err := open(t.Context(), e.NewClient(tables.RetentionAPIOption(injection, nil)), cfg, nil, opts...)
				require.NoError(t, err)
				event := eventEnvelope(t, "Order", "notify", 1, time.Unix(0, 123), "event", "")
				snapshot := pairSnapshot(t, "state", 1, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot))
				require.NoError(t, finish())
				require.Len(t, logger.records, 1)
				if name != "no callback" {
					require.Equal(t, 1, callbacks)
				}
				assertEventAttributes(t, tables, event, []byte(`"event"`))
				assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
				assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), true)
				traceEvent(t, map[string]any{"tables": cfg, "notification_case": name, "log_count": len(logger.records), "callback_count": callbacks, "fault_fired": injection.Faults[0].Fired()})
			})
		}
	})
}
