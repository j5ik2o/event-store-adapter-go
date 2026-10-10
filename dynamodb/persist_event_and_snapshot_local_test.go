package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

func assertPairSnapshot[E, A any](t *testing.T, tables *dynamodbtest.Tables, event eventstore.EventEnvelope[E], snapshot eventstore.SnapshotEnvelope[A], payload []byte, history bool) {
	t.Helper()
	key := eventstore.SeqNr(0)
	if history {
		key = snapshot.SeqNr()
	}
	item, err := tables.GetItem(t.Context(), "snapshot", eventKey(event.AggregateID(), "skey", key))
	require.NoError(t, err)
	want := map[string]types.AttributeValue{
		"aid": &types.AttributeValueMemberS{Value: event.AggregateID()}, "skey": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(key), 10)},
		"seq_nr": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(snapshot.SeqNr()), 10)}, "manifest": &types.AttributeValueMemberS{Value: snapshot.Manifest()},
		"payload": &types.AttributeValueMemberB{Value: payload}, "last_updated_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(event.OccurredAt().UnixMilli(), 10)},
	}
	if history {
		want["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(key), 10)}
	}
	require.Equal(t, want, item, "the independent observer compares every attribute and S/N/B type")
	traceEvent(t, map[string]any{"snapshot": item})
}

func assertPairRequest(t *testing.T, r *dynamodbtest.Recorder, operation int, cfg Config, seqNr eventstore.SeqNr, history, committed bool) {
	t.Helper()
	requests := r.Requests(operation)
	require.NotEmpty(t, requests)
	require.Equal(t, "TransactWriteItems", requests[0].API)
	input := requests[0].Input.(*awsdynamodb.TransactWriteItemsInput)
	want := 3
	if history {
		want = 4
	}
	require.Len(t, input.TransactItems, want)
	require.Equal(t, cfg.JournalTableName, aws.ToString(input.TransactItems[0].Put.TableName))
	require.Equal(t, "attribute_not_exists(aid)", aws.ToString(input.TransactItems[0].Put.ConditionExpression))
	if seqNr == 1 {
		head := input.TransactItems[1].Put
		require.NotNil(t, head)
		require.Equal(t, cfg.HeadTableName, aws.ToString(head.TableName))
		require.Equal(t, types.ReturnValuesOnConditionCheckFailureAllOld, head.ReturnValuesOnConditionCheckFailure)
	} else {
		head := input.TransactItems[1].Update
		require.NotNil(t, head)
		require.Equal(t, cfg.HeadTableName, aws.ToString(head.TableName))
		require.Equal(t, types.ReturnValuesOnConditionCheckFailureAllOld, head.ReturnValuesOnConditionCheckFailure)
		require.Equal(t, &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr-1), 10)}, head.ExpressionAttributeValues[":prev"])
	}
	for i, action := range input.TransactItems[2:] {
		require.Equal(t, cfg.SnapshotTableName, aws.ToString(action.Put.TableName))
		require.Nil(t, action.Put.ConditionExpression)
		key := "0"
		if i == 1 {
			key = strconv.FormatInt(int64(seqNr), 10)
		}
		require.Equal(t, &types.AttributeValueMemberN{Value: key}, action.Put.Item["skey"])
	}
	if !history || !committed {
		require.Len(t, requests, 1, "no classification reads or retention on this path")
	}
	if history && committed {
		require.GreaterOrEqual(t, len(requests), 2)
		require.Equal(t, "Query", requests[1].API)
		for _, request := range requests[1:] {
			switch input := request.Input.(type) {
			case *awsdynamodb.QueryInput:
				require.Equal(t, cfg.SnapshotTableName, aws.ToString(input.TableName))
				require.Equal(t, cfg.SnapshotHistoryIndexName, aws.ToString(input.IndexName))
				require.False(t, aws.ToBool(input.ScanIndexForward))
				require.False(t, aws.ToBool(input.ConsistentRead))
				require.Equal(t, "aid = :aid", aws.ToString(input.KeyConditionExpression))
				require.Equal(t, input.ExpressionAttributeValues[":aid"], requests[0].Input.(*awsdynamodb.TransactWriteItemsInput).TransactItems[0].Put.Item["aid"])
			case *awsdynamodb.BatchWriteItemInput:
				require.Len(t, input.RequestItems, 1)
				require.NotEmpty(t, input.RequestItems[cfg.SnapshotTableName])
			case *awsdynamodb.UpdateItemInput:
				require.Equal(t, cfg.SnapshotTableName, aws.ToString(input.TableName))
			default:
				t.Fatalf("unexpected post-commit request %s", request.API)
			}
		}
	}
	traceEvent(t, map[string]any{"operation": operation, "requests": requests})
}

func tracePairSDK(t *testing.T, sdk *eventSDKResults) {
	t.Helper()
	for _, call := range sdk.calls() {
		requestID, hasID := awsmiddleware.GetRequestIDMetadata(call.Metadata)
		require.True(t, hasID, "original SDK metadata must include its request id")
		require.NotEmpty(t, requestID)
		observed := map[string]any{"original_sdk_input": call.Input, "original_sdk_output": call.Output, "request_id": requestID, "original_error_type": fmt.Sprintf("%T", call.Error)}
		if call.Error != nil {
			var operation *smithy.OperationError
			if errors.As(call.Error, &operation) {
				observed["operation_error"] = operation.OperationName
			}
			var canceled *types.TransactionCanceledException
			if errors.As(call.Error, &canceled) {
				observed["original_cancellation_reasons"] = canceled.CancellationReasons
			}
			observed["cause_type"] = fmt.Sprintf("%T", errors.Unwrap(call.Error))
		}
		traceEvent(t, observed)
	}
}

func TestPersistEventAndSnapshotLocal(t *testing.T) {
	e := configurationEnvironment(t)
	t.Run("attributes bytes ownership and event-only mixed writes", func(t *testing.T) {
		for _, mode := range []string{"omitted", "none", "history"} {
			t.Run(mode, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, true)
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				var notifications atomic.Int32
				opts := []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), eventstore.WithRetentionFailureHandler(func(context.Context, error) { notifications.Add(1) })}
				if mode == "none" {
					opts = append(opts, eventstore.WithRetentionCount(eventstore.NoRetention()))
				}
				if mode == "history" {
					count, err := eventstore.KeepLatest(2)
					require.NoError(t, err)
					opts = append(opts, eventstore.WithRetentionCount(count))
				}
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil, opts...)
				require.NoError(t, err)
				id := &mutableEventID{typeName: "Order", value: "item-1"}
				firstPayload := eventDomain{AggregateID: "payload-id", SeqNr: "payload-seq", Items: []string{"最初"}}
				state := eventDomain{AggregateID: "state-id", SeqNr: "state-seq", Items: []string{"状態"}}
				first, err := eventstore.NewEventEnvelope(id, 1, time.Unix(1780000000, 123456789), firstPayload, eventstore.WithManifest(" event/型\x00 "))
				require.NoError(t, err)
				firstSnapshot := pairSnapshot(t, state, 1, " state/型\x00 ")
				id.typeName, id.value = "Changed-Type", "changed"
				var scratch [512]byte
				var serialized []eventDomain
				serializer := eventSerializer[eventDomain]{serialize: func(value eventDomain) ([]byte, error) {
					serialized = append(serialized, value)
					data, err := json.Marshal(value)
					copy(scratch[:], data)
					return scratch[:len(data)], err
				}}
				firstBytes, err := json.Marshal(firstPayload)
				require.NoError(t, err)
				stateBytes, err := json.Marshal(state)
				require.NoError(t, err)
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, serializer, first, firstSnapshot))
				firstPayload.Items[0], state.Items[0] = "input changed", "state changed"
				for i := range scratch {
					scratch[i] = '!'
				}
				assertEventAttributes(t, tables, first, firstBytes)
				assertPairSnapshot(t, tables, first, firstSnapshot, stateBytes, false)
				if mode == "history" {
					assertPairSnapshot(t, tables, first, firstSnapshot, stateBytes, true)
				}
				assertPairRequest(t, r, 1, cfg, 1, mode == "history", true)
				before := observedEventRows(t, tables, first.AggregateID())["snapshot"]
				second := eventEnvelope(t, "Order", "item-1", 2, time.Unix(0, 12), eventDomain{Items: []string{"single"}}, "")
				require.NoError(t, persistEvent(dynamodbtest.WithOperation(t.Context(), 2), store, serializer, second))
				assertEventRequest(t, r, 2, cfg, 2)
				require.Equal(t, before, observedEventRows(t, tables, first.AggregateID())["snapshot"])
				third := eventEnvelope(t, "Order", "item-1", 3, time.Unix(0, -123456789), eventDomain{Items: []string{"pair"}}, "")
				thirdSnapshot := pairSnapshot(t, eventDomain{Items: []string{"new state"}}, 3, "")
				require.NoError(t, persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 3), store, serializer, serializer, third, thirdSnapshot))
				thirdBytes, err := json.Marshal(third.Payload())
				require.NoError(t, err)
				snapshotBytes, err := json.Marshal(thirdSnapshot.Aggregate())
				require.NoError(t, err)
				for i := range scratch {
					scratch[i] = '?'
				}
				assertEventAttributes(t, tables, third, thirdBytes)
				assertPairSnapshot(t, tables, third, thirdSnapshot, snapshotBytes, false)
				if mode == "history" {
					assertPairSnapshot(t, tables, third, thirdSnapshot, snapshotBytes, true)
					assertPairSnapshot(t, tables, first, firstSnapshot, stateBytes, true)
				}
				assertPairRequest(t, r, 3, cfg, 3, mode == "history", true)
				rows := observedEventRows(t, tables, first.AggregateID())
				journal := rows["journal"].([]map[string]types.AttributeValue)
				require.Len(t, journal, 3)
				require.Equal(t, &types.AttributeValueMemberB{Value: firstBytes}, journal[0]["payload"])
				require.Len(t, serialized, 5)
				require.Equal(t, "payload-id", serialized[0].AggregateID)
				require.Equal(t, "state-id", serialized[1].AggregateID)
				require.Zero(t, notifications.Load())
				require.Len(t, sdk.calls(), 3)
				tracePairSDK(t, sdk)
				traceEvent(t, map[string]any{"tables": cfg, "rows_after_buffer_reuse": rows, "notifications": notifications.Load()})
			})
		}
	})
	t.Run("natural cancellations preserve every committed row", func(t *testing.T) {
		for _, history := range []bool{false, true} {
			for _, tc := range []struct {
				name            string
				head, requested eventstore.SeqNr
				collision       bool
				kind            eventstore.Kind
			}{
				{"new", 1, 1, false, eventstore.KindOptimisticLock}, {"duplicate", 2, 2, false, eventstore.KindOptimisticLock}, {"stale", 3, 2, false, eventstore.KindOptimisticLock}, {"gap", 1, 3, false, eventstore.KindContractViolation}, {"missing", 0, 2, false, eventstore.KindContractViolation}, {"journal", 1, 2, true, eventstore.KindOptimisticLock},
			} {
				t.Run(fmt.Sprintf("history=%v/%s", history, tc.name), func(t *testing.T) {
					tables, cfg := configurationTables(t, e, false)
					r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
					var opts []eventstore.Option
					if history {
						count, err := eventstore.KeepLatest(5)
						require.NoError(t, err)
						opts = append(opts, eventstore.WithRetentionCount(count))
					}
					store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil, opts...)
					require.NoError(t, err)
					serializer := eventstore.NewJSONSerializer[string]()
					for n := eventstore.SeqNr(1); n <= tc.head; n++ {
						event := eventEnvelope(t, "Order", "cancel", n, time.Unix(0, 123), "committed", "")
						require.NoError(t, persistEventAndSnapshot(t.Context(), store, serializer, serializer, event, pairSnapshot(t, "state", n, "")))
					}
					event := eventEnvelope(t, "Order", "cancel", tc.requested, time.Unix(0, 987654321), "candidate", "candidate")
					if tc.collision {
						item := eventKey(event.AggregateID(), "seq_nr", tc.requested)
						item["payload"] = &types.AttributeValueMemberB{Value: []byte("fixture")}
						_, err := e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.JournalTableName), Item: item})
						require.NoError(t, err)
					}
					before := observedEventRows(t, tables, event.AggregateID())
					err = persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, serializer, serializer, event, pairSnapshot(t, "candidate-state", tc.requested, "candidate"))
					requireEventKind(t, err, tc.kind)
					var canceled *types.TransactionCanceledException
					require.ErrorAs(t, err, &canceled)
					actions := 3
					if history {
						actions = 4
					}
					require.Len(t, canceled.CancellationReasons, actions)
					if tc.collision {
						require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[0].Code))
						require.Equal(t, "None", aws.ToString(canceled.CancellationReasons[1].Code))
					} else {
						require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[1].Code))
						if tc.head > 0 {
							require.Equal(t, before["head"], canceled.CancellationReasons[1].Item)
						} else {
							require.Empty(t, canceled.CancellationReasons[1].Item)
						}
					}
					for _, reason := range canceled.CancellationReasons[2:] {
						require.Equal(t, "None", aws.ToString(reason.Code))
					}
					calls := sdk.calls()
					require.ErrorIs(t, err, calls[len(calls)-1].Error)
					assertPairRequest(t, r, 99, cfg, tc.requested, history, false)
					require.Equal(t, before, observedEventRows(t, tables, event.AggregateID()))
					tracePairSDK(t, sdk)
					traceEvent(t, map[string]any{"tables": cfg, "rows_before": before, "rows_after": observedEventRows(t, tables, event.AggregateID()), "kind": tc.kind, "cause_type": fmt.Sprintf("%T", errors.Unwrap(err))})
				})
			}
		}
	})
	t.Run("concurrent same aid number commits one complete pair", func(t *testing.T) {
		for _, history := range []bool{false, true} {
			for _, seq := range []eventstore.SeqNr{1, 2} {
				t.Run(fmt.Sprintf("history=%v/seq=%d", history, seq), func(t *testing.T) {
					tables, cfg := configurationTables(t, e, false)
					r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
					var opts []eventstore.Option
					if history {
						count, err := eventstore.KeepLatest(3)
						require.NoError(t, err)
						opts = append(opts, eventstore.WithRetentionCount(count))
					}
					store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil, opts...)
					require.NoError(t, err)
					serializer := eventstore.NewJSONSerializer[string]()
					if seq == 2 {
						require.NoError(t, persistEventAndSnapshot(t.Context(), store, serializer, serializer, eventEnvelope(t, "Race", "pair", 1, time.Unix(0, 1), "baseline", ""), pairSnapshot(t, "baseline-state", 1, "")))
					}
					results := make([]error, 2)
					events := make([]eventstore.EventEnvelope[string], 2)
					snapshots := make([]eventstore.SnapshotEnvelope[string], 2)
					start := make(chan struct{})
					var workers sync.WaitGroup
					for i := range events {
						events[i] = eventEnvelope(t, "Race", "pair", seq, time.Unix(0, int64(i+2)), fmt.Sprintf("candidate-%d", i), fmt.Sprintf("event-%d", i))
						snapshots[i] = pairSnapshot(t, fmt.Sprintf("state-%d", i), seq, fmt.Sprintf("snapshot-%d", i))
						workers.Go(func() {
							<-start
							results[i] = persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), i+1), store, serializer, serializer, events[i], snapshots[i])
						})
					}
					close(start)
					workers.Wait()
					winner, successes := -1, 0
					for i, err := range results {
						if err == nil {
							winner = i
							successes++
						} else {
							requireEventKind(t, err, eventstore.KindOptimisticLock)
						}
						assertPairRequest(t, r, i+1, cfg, seq, history, err == nil)
					}
					require.Equal(t, 1, successes)
					payload, err := json.Marshal(events[winner].Payload())
					require.NoError(t, err)
					state, err := json.Marshal(snapshots[winner].Aggregate())
					require.NoError(t, err)
					assertEventAttributes(t, tables, events[winner], payload)
					assertPairSnapshot(t, tables, events[winner], snapshots[winner], state, false)
					if history {
						assertPairSnapshot(t, tables, events[winner], snapshots[winner], state, true)
					}
					rows := observedEventRows(t, tables, events[winner].AggregateID())
					require.Len(t, rows["journal"], int(seq))
					tracePairSDK(t, sdk)
					traceEvent(t, map[string]any{"tables": cfg, "winner": winner, "successes": successes, "rows": rows})
				})
			}
		}
	})
	t.Run("preparation failure sends zero and leaves fault unfired", func(t *testing.T) {
		for _, name := range []string{"event", "snapshot", "number", "event serializer", "snapshot serializer", "event size", "event manifest size", "head size", "current size", "history size", "snapshot manifest size"} {
			t.Run(name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				injection, finish := conformance.NewOperationInjection(99, []conformance.FaultSpec{{Operation: 99, Phase: "commit", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-request"}})
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				count, err := eventstore.KeepLatest(3)
				require.NoError(t, err)
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil, eventstore.WithRetentionCount(count))
				require.NoError(t, err)
				serializer := eventstore.NewJSONSerializer[string]()
				require.NoError(t, persistEventAndSnapshot(t.Context(), store, serializer, serializer, eventEnvelope(t, "Order", "pair", 1, time.Unix(0, 1), "baseline", ""), pairSnapshot(t, "state", 1, "")))
				event := eventEnvelope(t, "Order", "pair", 2, time.Unix(0, 2), "candidate", "")
				snapshot := pairSnapshot(t, "candidate-state", 2, "")
				cause := errors.New("serializer cause")
				calls := [2]int{}
				serializers := [2]eventstore.Serializer[string]{}
				for i := range serializers {
					serializers[i] = eventSerializer[string]{serialize: func(value string) ([]byte, error) {
						calls[i]++
						if name == []string{"event serializer", "snapshot serializer"}[i] {
							return nil, cause
						}
						if i == 0 && name == "event size" || i == 1 && name == "current size" {
							return make([]byte, 409600), nil
						}
						if i == 0 && name == "head size" {
							return make([]byte, 408000), nil
						}
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
					snapshot = pairSnapshot(t, "state", 3, "")
				case "event manifest size":
					event = eventEnvelope(t, "Order", "pair", 2, time.Unix(0, 2), "event", strings.Repeat("文", 136534))
				case "head size":
					event = eventEnvelope(t, strings.Repeat("x", 1000), "v", 2, time.Unix(0, 2), "event", "")
				case "snapshot manifest size":
					snapshot = pairSnapshot(t, "state", 2, strings.Repeat("文", 136534))
				}
				before := observedEventRows(t, tables, "Order-pair")
				sdkBefore := len(sdk.calls())
				err = persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 99), store, serializers[0], serializers[1], event, snapshot)
				kind := eventstore.KindContractViolation
				if strings.Contains(name, "serializer") {
					kind = eventstore.KindSerialization
					require.ErrorIs(t, err, cause)
				}
				requireEventKind(t, err, kind)
				require.Empty(t, r.Requests(99))
				require.Len(t, sdk.calls(), sdkBefore)
				require.Zero(t, injection.Faults[0].Fired())
				require.ErrorContains(t, finish(), "fired 0 times")
				require.Equal(t, before, observedEventRows(t, tables, "Order-pair"))
				if name == "event" || name == "snapshot" || name == "number" {
					require.Equal(t, [2]int{}, calls)
				} else if name == "event serializer" {
					require.Equal(t, [2]int{1, 0}, calls)
				} else {
					require.Equal(t, [2]int{1, 1}, calls)
				}
				traceEvent(t, map[string]any{"tables": cfg, "failure_stage": "persistEventAndSnapshot", "case": name, "serializer_calls": calls, "requests": r.Requests(99), "original_sdk_delta": len(sdk.calls()) - sdkBefore, "fault_fired": injection.Faults[0].Fired(), "rows_before": before, "rows_after": observedEventRows(t, tables, "Order-pair")})
			})
		}
	})
	t.Run("injected cancellation priorities at every action", func(t *testing.T) {
		for _, actions := range []int{3, 4} {
			targets := []string{"journal", "head", "current-snapshot"}
			if actions == 4 {
				targets = append(targets, "history-snapshot")
			}
			for _, target := range append(targets, "head gap", "journal condition", "current throttle", "history throttle") {
				if actions == 3 && target == "history throttle" {
					continue
				}
				t.Run(fmt.Sprintf("actions=%d/%s", actions, target), func(t *testing.T) {
					tables, cfg := configurationTables(t, e, false)
					reasons := []any{map[string]any{"target": "head", "code": "ConditionalCheckFailed", "old_head_seq_nr": json.Number("2")}}
					kind := eventstore.KindOptimisticLock
					switch target {
					case "head gap":
						reasons = append(reasons, map[string]any{"target": "journal", "code": "ConditionalCheckFailed"})
						kind = eventstore.KindContractViolation
					case "journal condition":
						reasons = []any{map[string]any{"target": "journal", "code": "ConditionalCheckFailed"}}
					case "current throttle", "history throttle":
						which := "current-snapshot"
						if target == "history throttle" {
							which = "history-snapshot"
						}
						reasons = []any{map[string]any{"target": which, "code": "ThrottlingError"}}
						kind = eventstore.KindStorage
					default:
						reasons = append(reasons, map[string]any{"target": target, "code": "TransactionConflict"})
					}
					injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"code": "TransactionCanceledException", "cancellation_reasons": reasons}}})
					r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
					var opts []eventstore.Option
					if actions == 4 {
						count, err := eventstore.KeepLatest(1)
						require.NoError(t, err)
						opts = append(opts, eventstore.WithRetentionCount(count))
					}
					store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil, opts...)
					require.NoError(t, err)
					event := eventEnvelope(t, "Order", "injected", 4, time.Unix(0, 1), "event", "")
					before := observedEventRows(t, tables, event.AggregateID())
					err = persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, pairSnapshot(t, "state", 4, ""))
					requireEventKind(t, err, kind)
					require.NotNil(t, errors.Unwrap(err))
					require.Empty(t, sdk.calls())
					require.Equal(t, 1, injection.Faults[0].Fired())
					require.NoError(t, finish())
					assertPairRequest(t, r, 1, cfg, 4, actions == 4, false)
					require.Equal(t, before, observedEventRows(t, tables, event.AggregateID()))
					var canceled *types.TransactionCanceledException
					require.ErrorAs(t, err, &canceled)
					require.Len(t, canceled.CancellationReasons, actions)
					traceEvent(t, map[string]any{"tables": cfg, "fault_input": injection.Faults[0].Spec, "replacement_reasons": canceled.CancellationReasons, "kind": kind, "cause_type": fmt.Sprintf("%T", errors.Unwrap(err)), "original_sdk_calls": len(sdk.calls()), "fault_fired": injection.Faults[0].Fired(), "rows": before})
				})
			}
		}
	})
	t.Run("response failure leaves actual pair committed without retention", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-response"}})
		r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
		count, err := eventstore.KeepLatest(1)
		require.NoError(t, err)
		store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil, eventstore.WithRetentionCount(count))
		require.NoError(t, err)
		event := eventEnvelope(t, "Order", "response", 1, time.Unix(0, 123), "event", "")
		snapshot := pairSnapshot(t, "state", 1, "")
		err = persistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), event, snapshot)
		requireEventKind(t, err, eventstore.KindStorage)
		require.Len(t, sdk.calls(), 1)
		require.NoError(t, sdk.calls()[0].Error)
		require.NoError(t, finish())
		assertPairRequest(t, r, 1, cfg, 1, true, false)
		assertEventAttributes(t, tables, event, []byte(`"event"`))
		assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), false)
		assertPairSnapshot(t, tables, event, snapshot, []byte(`"state"`), true)
		tracePairSDK(t, sdk)
	})
}
