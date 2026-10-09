package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

type eventSDKInputKey struct{}
type eventSDKResult struct {
	Input  *awsdynamodb.TransactWriteItemsInput
	Output *awsdynamodb.TransactWriteItemsOutput
	Error  error
}
type eventSDKResults struct {
	mu      sync.Mutex
	results []eventSDKResult
}

// Installed before CommitAPIOption, this observes the original SDK handler.
// A replace-request fault never reaches this observer; a response replacement
// leaves the successful original SDK result here, separate from the injected error.
func (r *eventSDKResults) apiOption(stack *middleware.Stack) error {
	if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("observe-event-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
		return next.HandleInitialize(context.WithValue(ctx, eventSDKInputKey{}, in.Parameters), in)
	}), middleware.Before); err != nil {
		return err
	}
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("observe-original-event-result", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleFinalize(ctx, in)
		if input, ok := ctx.Value(eventSDKInputKey{}).(*awsdynamodb.TransactWriteItemsInput); ok && len(input.TransactItems) == 2 {
			output, _ := out.Result.(*awsdynamodb.TransactWriteItemsOutput)
			r.mu.Lock()
			r.results = append(r.results, eventSDKResult{Input: input, Output: output, Error: err})
			r.mu.Unlock()
		}
		return out, metadata, err
	}), middleware.Before)
}

func (r *eventSDKResults) calls() []eventSDKResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]eventSDKResult(nil), r.results...)
}

func traceEvent(t *testing.T, observed map[string]any) {
	t.Helper()
	data, err := json.Marshal(observed)
	require.NoError(t, err)
	t.Log(string(data))
}

func eventKey(aid, sortKey string, seqNr eventstore.SeqNr) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: aid}}
	if sortKey != "" {
		key[sortKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr), 10)}
	}
	return key
}

func observedEventRows(t *testing.T, tables *dynamodbtest.Tables, aid string) map[string]any {
	t.Helper()
	rows := make(map[string]any)
	for _, table := range []dynamodbtest.Table{"journal", "snapshot"} {
		name, err := tables.TableName(table)
		require.NoError(t, err)
		items, err := tables.QueryAll(t.Context(), awsdynamodb.QueryInput{
			TableName: aws.String(name), ConsistentRead: aws.Bool(true),
			KeyConditionExpression:    aws.String("aid = :aid"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: aid}},
		})
		require.NoError(t, err)
		rows[string(table)] = items
	}
	head, err := tables.GetItem(t.Context(), "head", eventKey(aid, "", 0))
	require.NoError(t, err)
	rows["head"] = head
	return rows
}

func assertEventAttributes[T any](t *testing.T, tables *dynamodbtest.Tables, event eventstore.EventEnvelope[T], payload []byte) {
	t.Helper()
	journal, err := tables.GetItem(t.Context(), "journal", eventKey(event.AggregateID(), "seq_nr", event.SeqNr()))
	require.NoError(t, err)
	metadata := map[string]types.AttributeValue{
		"seq_nr":      &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(event.SeqNr()), 10)},
		"occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(event.OccurredAt().UnixNano(), 10)},
		"manifest":    &types.AttributeValueMemberS{Value: event.Manifest()},
		"payload":     &types.AttributeValueMemberB{Value: payload},
	}
	require.Len(t, journal, 5)
	require.Equal(t, &types.AttributeValueMemberS{Value: event.AggregateID()}, journal["aid"])
	for name, expected := range metadata {
		require.Equal(t, expected, journal[name], name)
	}
	head, err := tables.GetItem(t.Context(), "head", eventKey(event.AggregateID(), "", 0))
	require.NoError(t, err)
	typeName, _, _ := strings.Cut(event.AggregateID(), "-")
	require.Len(t, head, 4)
	require.Equal(t, journal["aid"], head["aid"])
	require.Equal(t, &types.AttributeValueMemberS{Value: typeName}, head["type_name"])
	require.Equal(t, metadata["seq_nr"], head["seq_nr"])
	list, ok := head["events"].(*types.AttributeValueMemberL)
	require.True(t, ok, "events must be L")
	require.Len(t, list.Value, 1)
	nested, ok := list.Value[0].(*types.AttributeValueMemberM)
	require.True(t, ok, "events element must be M")
	require.Equal(t, metadata, nested.Value)
	traceEvent(t, map[string]any{"journal": journal, "head": head})
}

func assertEventRequest(t *testing.T, r *dynamodbtest.Recorder, operation int, cfg Config, seqNr eventstore.SeqNr) {
	t.Helper()
	requests := r.Requests(operation)
	require.Len(t, requests, 1, "event-only sends one transaction and no reads, retention, TTL or deletes")
	require.Equal(t, "TransactWriteItems", requests[0].API)
	input, ok := requests[0].Input.(*awsdynamodb.TransactWriteItemsInput)
	require.True(t, ok)
	require.Len(t, input.TransactItems, 2)
	journal := input.TransactItems[0].Put
	require.NotNil(t, journal)
	require.Equal(t, cfg.JournalTableName, aws.ToString(journal.TableName))
	normalize := func(expr *string) string { return strings.Join(strings.Fields(aws.ToString(expr)), "") }
	require.Equal(t, "attribute_not_exists(aid)", normalize(journal.ConditionExpression))
	if seqNr == 1 {
		head := input.TransactItems[1].Put
		require.NotNil(t, head)
		require.Equal(t, cfg.HeadTableName, aws.ToString(head.TableName))
		require.Equal(t, "attribute_not_exists(aid)", normalize(head.ConditionExpression))
		require.Equal(t, types.ReturnValuesOnConditionCheckFailureAllOld, head.ReturnValuesOnConditionCheckFailure)
	} else {
		head := input.TransactItems[1].Update
		require.NotNil(t, head)
		require.Equal(t, cfg.HeadTableName, aws.ToString(head.TableName))
		condition := strings.Split(normalize(head.ConditionExpression), "=")
		require.Len(t, condition, 2)
		require.Equal(t, "seq_nr", condition[0])
		require.Equal(t, &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr-1), 10)}, head.ExpressionAttributeValues[condition[1]])
		expression := strings.TrimSpace(aws.ToString(head.UpdateExpression))
		require.True(t, strings.HasPrefix(expression, "SET "))
		set := make(map[string]types.AttributeValue)
		for _, assignment := range strings.Split(strings.TrimPrefix(expression, "SET "), ",") {
			parts := strings.Split(assignment, "=")
			require.Len(t, parts, 2)
			set[strings.TrimSpace(parts[0])] = head.ExpressionAttributeValues[strings.TrimSpace(parts[1])]
		}
		require.Len(t, set, 2)
		require.Equal(t, &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr), 10)}, set["seq_nr"])
		require.IsType(t, &types.AttributeValueMemberL{}, set["events"])
		require.Equal(t, types.ReturnValuesOnConditionCheckFailureAllOld, head.ReturnValuesOnConditionCheckFailure)
	}
	traceEvent(t, map[string]any{"operation": operation, "request": requests[0]})
}

type eventDomain struct {
	AggregateID string
	SeqNr       string
	Items       []string
}
type mutableEventID struct{ typeName, value string }

func (id *mutableEventID) TypeName() string { return id.typeName }
func (id *mutableEventID) Value() string    { return id.value }

func TestPersistEventLocal(t *testing.T) {
	e := configurationEnvironment(t)
	t.Run("new and continuous preserve attributes domain and bytes", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, true)
		r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
		count, err := eventstore.KeepLatest(1)
		require.NoError(t, err)
		var notifications atomic.Int32
		store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil,
			eventstore.WithRetentionCount(count), eventstore.WithRetentionMode(eventstore.RetentionTTL),
			eventstore.WithRetentionFailureHandler(func(context.Context, error) { notifications.Add(1) }))
		require.NoError(t, err)
		id := &mutableEventID{typeName: "Order", value: "item-1"}
		firstPayload := eventDomain{AggregateID: "payload-id", SeqNr: "payload-number", Items: []string{"最初"}}
		first, err := eventstore.NewEventEnvelope(id, 1, time.Unix(1780000000, 123456789), firstPayload, eventstore.WithManifest(" 型\x00 "))
		require.NoError(t, err)
		secondPayload := eventDomain{AggregateID: "other-payload-id", Items: []string{"次"}}
		second, err := eventstore.NewEventEnvelope(id, 2, time.Unix(0, -123456789), secondPayload)
		require.NoError(t, err)
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
		require.NoError(t, persistEvent(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, first))
		assertEventAttributes(t, tables, first, firstBytes)
		assertEventRequest(t, r, 1, cfg, 1)

		// Explicit independent snapshot setup; event-only must leave both rows intact.
		current := eventKey(first.AggregateID(), "skey", 0)
		current["seq_nr"] = &types.AttributeValueMemberN{Value: "1"}
		current["manifest"] = &types.AttributeValueMemberS{Value: "snapshot"}
		current["payload"] = &types.AttributeValueMemberB{Value: []byte("state")}
		current["last_updated_at"] = &types.AttributeValueMemberN{Value: "1780000000123"}
		history := eventKey(first.AggregateID(), "skey", 1)
		for name, value := range current {
			if name != "skey" {
				history[name] = value
			}
		}
		history["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: "1"}
		for _, item := range []map[string]types.AttributeValue{current, history} {
			_, err := e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.SnapshotTableName), Item: item})
			require.NoError(t, err)
		}
		firstPayload.Items[0] = "changed input"
		require.NoError(t, persistEvent(dynamodbtest.WithOperation(t.Context(), 2), store, serializer, second))
		secondBytes, err := json.Marshal(secondPayload)
		require.NoError(t, err)
		assertEventAttributes(t, tables, second, secondBytes)
		assertEventRequest(t, r, 2, cfg, 2)
		for i := range scratch {
			scratch[i] = '!'
		}
		rows := observedEventRows(t, tables, first.AggregateID())
		journal := rows["journal"].([]map[string]types.AttributeValue)
		require.Len(t, journal, 2)
		require.Equal(t, &types.AttributeValueMemberB{Value: firstBytes}, journal[0]["payload"])
		require.Equal(t, &types.AttributeValueMemberB{Value: secondBytes}, journal[1]["payload"])
		require.Equal(t, []map[string]types.AttributeValue{current, history}, rows["snapshot"])
		require.Zero(t, notifications.Load())
		require.Len(t, serialized, 2)
		require.Equal(t, first.Payload().AggregateID, serialized[0].AggregateID)
		require.Equal(t, secondPayload, serialized[1])
		calls := sdk.calls()
		require.Len(t, calls, 2)
		for _, call := range calls {
			require.NoError(t, call.Error)
			require.NotNil(t, call.Output)
		}
		traceEvent(t, map[string]any{"tables": cfg, "rows_after_buffer_reuse": rows, "original_sdk": calls, "notifications": notifications.Load()})
	})

	t.Run("exact epoch nanosecond boundaries", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		store, err := open(t.Context(), e.NewClient(), cfg, nil)
		require.NoError(t, err)
		for i, nanos := range []int64{math.MinInt64, math.MaxInt64} {
			event := eventEnvelope(t, "Clock", "bounds", eventstore.SeqNr(i+1), time.Unix(0, nanos), "payload", "")
			require.NoError(t, persistEvent(t.Context(), store, eventstore.NewJSONSerializer[string](), event))
			assertEventAttributes(t, tables, event, []byte(`"payload"`))
		}
	})

	t.Run("natural cancellations do not partially commit", func(t *testing.T) {
		for _, tc := range []struct {
			name                 string
			headSeqNr, requested eventstore.SeqNr
			journalCollision     bool
			kind                 eventstore.Kind
		}{
			{"new duplicate", 1, 1, false, eventstore.KindOptimisticLock},
			{"update duplicate", 2, 2, false, eventstore.KindOptimisticLock},
			{"stale update", 3, 2, false, eventstore.KindOptimisticLock},
			{"gap", 1, 3, false, eventstore.KindContractViolation},
			{"missing head", 0, 2, false, eventstore.KindContractViolation},
			{"journal condition only", 1, 2, true, eventstore.KindOptimisticLock},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil)
				require.NoError(t, err)
				for seq := eventstore.SeqNr(1); seq <= tc.headSeqNr; seq++ {
					require.NoError(t, persistEvent(t.Context(), store, eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Order", "cancel", seq, time.Unix(0, 123), "committed", "")))
				}
				event := eventEnvelope(t, "Order", "cancel", tc.requested, time.Unix(0, 987654321), "candidate", "candidate-manifest")
				if tc.journalCollision {
					item := eventKey(event.AggregateID(), "seq_nr", tc.requested)
					item["occurred_at"] = &types.AttributeValueMemberN{Value: "123"}
					item["manifest"] = &types.AttributeValueMemberS{Value: "fixture"}
					item["payload"] = &types.AttributeValueMemberB{Value: []byte("independent journal fixture")}
					_, err := e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.JournalTableName), Item: item})
					require.NoError(t, err)
				}
				before := observedEventRows(t, tables, event.AggregateID())
				err = persistEvent(dynamodbtest.WithOperation(t.Context(), 99), store, eventstore.NewJSONSerializer[string](), event)
				requireEventKind(t, err, tc.kind)
				var operation *smithy.OperationError
				require.ErrorAs(t, errors.Unwrap(err), &operation)
				require.Equal(t, "TransactWriteItems", operation.OperationName)
				var canceled *types.TransactionCanceledException
				require.ErrorAs(t, err, &canceled)
				require.Len(t, canceled.CancellationReasons, 2)
				if tc.journalCollision {
					require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[0].Code))
					require.Equal(t, "None", aws.ToString(canceled.CancellationReasons[1].Code))
				} else {
					require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[1].Code))
					if tc.requested > tc.headSeqNr {
						require.Equal(t, "None", aws.ToString(canceled.CancellationReasons[0].Code))
					}
					if tc.headSeqNr == 0 {
						require.Empty(t, canceled.CancellationReasons[1].Item)
					} else {
						require.Equal(t, before["head"], canceled.CancellationReasons[1].Item, "ALL_OLD must return the actual old head at its action position")
					}
				}
				if tc.kind == eventstore.KindContractViolation {
					var violation *eventstore.ContractViolationError
					require.ErrorAs(t, err, &violation)
					require.Equal(t, "W-8", violation.Rule)
					require.Equal(t, tc.requested, *violation.SeqNr)
				}
				calls := sdk.calls()
				require.Len(t, calls, int(tc.headSeqNr)+1)
				actual := calls[len(calls)-1]
				require.ErrorIs(t, err, actual.Error)
				assertEventRequest(t, r, 99, cfg, tc.requested)
				after := observedEventRows(t, tables, event.AggregateID())
				require.Equal(t, before, after)
				traceEvent(t, map[string]any{"tables": cfg, "cancellation_reasons": canceled.CancellationReasons, "original_sdk_error": actual.Error.Error(), "unwrap_type": fmt.Sprintf("%T", errors.Unwrap(err)), "rows_before": before, "rows_after": after})
			})
		}
	})

	t.Run("concurrent same aid and number commit once", func(t *testing.T) {
		for _, seqNr := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprint(seqNr), func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption), cfg, nil)
				require.NoError(t, err)
				if seqNr == 2 {
					require.NoError(t, persistEvent(t.Context(), store, eventstore.NewJSONSerializer[string](), eventEnvelope(t, "Race", "same", 1, time.Unix(0, 1), "baseline", "")))
				}
				start := make(chan struct{})
				var workers sync.WaitGroup
				results := make([]error, 2)
				events := make([]eventstore.EventEnvelope[string], 2)
				for i := range events {
					events[i] = eventEnvelope(t, "Race", "same", seqNr, time.Unix(0, int64(i+2)), fmt.Sprintf("candidate-%d", i), fmt.Sprintf("manifest-%d", i))
					workers.Go(func() {
						<-start
						results[i] = persistEvent(dynamodbtest.WithOperation(t.Context(), i+1), store, eventstore.NewJSONSerializer[string](), events[i])
					})
				}
				close(start)
				workers.Wait()
				winner, successes := -1, 0
				for i, err := range results {
					if err == nil {
						winner, successes = i, successes+1
					} else {
						requireEventKind(t, err, eventstore.KindOptimisticLock)
						var canceled *types.TransactionCanceledException
						require.ErrorAs(t, err, &canceled)
						traceEvent(t, map[string]any{"loser": i, "cancellation_reasons": canceled.CancellationReasons})
					}
					assertEventRequest(t, r, i+1, cfg, seqNr)
				}
				require.Equal(t, 1, successes)
				payload, err := json.Marshal(events[winner].Payload())
				require.NoError(t, err)
				assertEventAttributes(t, tables, events[winner], payload)
				rows := observedEventRows(t, tables, events[winner].AggregateID())
				require.Len(t, rows["journal"], int(seqNr))
				require.Empty(t, rows["snapshot"])
				for _, call := range sdk.calls() {
					observed := map[string]any{"original_sdk_input": call.Input, "original_sdk_output": call.Output}
					if call.Error != nil {
						observed["original_sdk_error"] = call.Error.Error()
						var canceled *types.TransactionCanceledException
						require.ErrorAs(t, call.Error, &canceled)
						observed["original_cancellation_reasons"] = canceled.CancellationReasons
					}
					traceEvent(t, observed)
				}
				traceEvent(t, map[string]any{"tables": cfg, "successes": successes, "winner": winner, "rows": rows})
			})
		}
	})

	t.Run("preparation failures send zero and leave commit fault unfired", func(t *testing.T) {
		for _, name := range []string{"zero envelope", "zero number", "negative number", "number above maximum", "time outside range", "invalid type", "aid too long", "serializer", "journal payload size", "manifest size", "head size"} {
			t.Run(name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-request"}})
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil)
				require.NoError(t, err)
				serializerCalls := 0
				cause := errors.New("serializer failure")
				payloadBytes := 5
				serializer := eventSerializer[string]{serialize: func(string) ([]byte, error) {
					serializerCalls++
					if name == "serializer" {
						return nil, cause
					}
					return make([]byte, payloadBytes), nil
				}}
				id := eventstore.AggregateID(&mutableEventID{typeName: "Order", value: "preparation"})
				seqNr, at, manifest := eventstore.SeqNr(1), time.Unix(0, 123), ""
				switch name {
				case "zero number":
					seqNr = 0
				case "negative number":
					seqNr = -1
				case "number above maximum":
					seqNr = eventstore.MaxSeqNr + 1
				case "time outside range":
					at = time.Unix(0, math.MaxInt64).Add(time.Nanosecond)
				case "invalid type":
					id = &mutableEventID{typeName: "Order-Item", value: "preparation"}
				case "aid too long":
					id = &mutableEventID{typeName: "Order", value: strings.Repeat("x", 1024)}
				case "journal payload size":
					payloadBytes = 409600
				case "manifest size":
					manifest = strings.Repeat("文", 136534)
				case "head size":
					id = &mutableEventID{typeName: strings.Repeat("x", 1000), value: "v"}
					payloadBytes = 408000
				}
				var event eventstore.EventEnvelope[string]
				if name != "zero envelope" {
					event, err = eventstore.NewEventEnvelope(id, seqNr, at, "payload", eventstore.WithManifest(manifest))
				}
				stage := "constructor"
				if err == nil {
					stage = "persistEvent"
					err = persistEvent(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, event)
				}
				kind := eventstore.KindContractViolation
				if name == "serializer" {
					kind = eventstore.KindSerialization
					require.Equal(t, cause, errors.Unwrap(err))
				}
				requireEventKind(t, err, kind)
				require.Empty(t, r.Requests(1))
				require.Empty(t, sdk.calls())
				require.Zero(t, injection.Faults[0].Fired())
				require.ErrorContains(t, finish(), "fired 0 times")
				if name == "serializer" || strings.HasSuffix(name, "size") {
					require.Equal(t, 1, serializerCalls)
				} else {
					require.Zero(t, serializerCalls)
				}
				traceEvent(t, map[string]any{"tables": cfg, "failure_stage": stage, "kind": kind, "serializer_calls": serializerCalls, "requests": r.Requests(1), "original_sdk_calls": len(sdk.calls()), "fault_fired": injection.Faults[0].Fired()})
			})
		}
	})

	t.Run("injected cancellation priorities and storage failures", func(t *testing.T) {
		for _, tc := range []struct {
			name, journalCode, headCode, code string
			kind                              eventstore.Kind
		}{
			{"journal conflict before head gap", "TransactionConflict", "ConditionalCheckFailed", "TransactionCanceledException", eventstore.KindOptimisticLock},
			{"head conflict before journal condition", "ConditionalCheckFailed", "TransactionConflict", "TransactionCanceledException", eventstore.KindOptimisticLock},
			{"head gap before journal condition", "ConditionalCheckFailed", "ConditionalCheckFailed", "TransactionCanceledException", eventstore.KindContractViolation},
			{"journal throttle", "ProvisionedThroughputExceeded", "None", "TransactionCanceledException", eventstore.KindStorage},
			{"head throttle", "None", "ThrottlingError", "TransactionCanceledException", eventstore.KindStorage},
			{"SDK throughput", "", "", "ProvisionedThroughputExceededException", eventstore.KindStorage},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				fault := conformance.FaultSpec{Operation: 1, Phase: "commit", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: map[string]any{"code": tc.code}}
				if tc.code == "TransactionCanceledException" {
					fault.Details["cancellation_reasons"] = []any{
						map[string]any{"target": "head", "code": tc.headCode, "old_head_seq_nr": json.Number("2")},
						map[string]any{"target": "journal", "code": tc.journalCode},
					}
				}
				injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{fault})
				r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
				store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil)
				require.NoError(t, err)
				event := eventEnvelope(t, "Order", "injected", 4, time.Unix(0, 123456789), "candidate", "")
				before := observedEventRows(t, tables, event.AggregateID())
				err = persistEvent(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), event)
				requireEventKind(t, err, tc.kind)
				require.NotNil(t, errors.Unwrap(err))
				var operation *smithy.OperationError
				require.ErrorAs(t, errors.Unwrap(err), &operation)
				assertEventRequest(t, r, 1, cfg, 4)
				require.Empty(t, sdk.calls(), "replace-request never invokes the original SDK handler")
				require.Equal(t, 1, injection.Faults[0].Fired())
				require.NoError(t, finish())
				require.Equal(t, before, observedEventRows(t, tables, event.AggregateID()))
				if tc.code == "TransactionCanceledException" {
					var canceled *types.TransactionCanceledException
					require.ErrorAs(t, err, &canceled)
					require.Len(t, canceled.CancellationReasons, 2)
					require.Equal(t, tc.journalCode, aws.ToString(canceled.CancellationReasons[0].Code))
					require.Equal(t, tc.headCode, aws.ToString(canceled.CancellationReasons[1].Code))
					traceEvent(t, map[string]any{"injected_cancellation_reasons": canceled.CancellationReasons})
				}
				traceEvent(t, map[string]any{"tables": cfg, "fault_input": fault, "injected_error": errors.Unwrap(err).Error(), "original_sdk_calls": len(sdk.calls()), "fault_fired": injection.Faults[0].Fired(), "rows": before})
			})
		}
	})

	t.Run("response failure preserves actual committed transaction", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		injection, finish := conformance.NewOperationInjection(1, []conformance.FaultSpec{{Operation: 1, Phase: "commit", Kind: "storage-error", Repeat: "count", Count: 1, Injection: "replace-response", Details: map[string]any{"message": "response lost after commit"}}})
		r, sdk := dynamodbtest.NewRecorder(), &eventSDKResults{}
		store, err := open(t.Context(), e.NewClient(r.APIOption, sdk.apiOption, tables.CommitAPIOption(injection)), cfg, nil)
		require.NoError(t, err)
		event := eventEnvelope(t, "Order", "response", 1, time.Unix(0, 123), "committed", "")
		err = persistEvent(dynamodbtest.WithOperation(t.Context(), 1), store, eventstore.NewJSONSerializer[string](), event)
		requireEventKind(t, err, eventstore.KindStorage)
		calls := sdk.calls()
		require.Len(t, calls, 1)
		require.NoError(t, calls[0].Error)
		require.NotNil(t, calls[0].Output)
		require.Equal(t, 1, injection.Faults[0].Fired())
		require.NoError(t, finish())
		assertEventRequest(t, r, 1, cfg, 1)
		assertEventAttributes(t, tables, event, []byte(`"committed"`))
		traceEvent(t, map[string]any{"tables": cfg, "original_sdk": calls, "injected_error": errors.Unwrap(err).Error(), "fault_fired": injection.Faults[0].Fired()})
	})
}
