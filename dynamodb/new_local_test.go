package dynamodb_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/dynamodb"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

func publicEnvironment(t *testing.T) *dynamodbtest.Environment {
	t.Helper()
	e, err := dynamodbtest.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, e.Close(ctx))
	})
	return e
}

func TestDynamoDBPublicLocalRetentionFailureOption(t *testing.T) {
	e := publicEnvironment(t)
	tables, cfg := publicTables(t, e)
	// A real SDK retention query fails after the real paired write commits.
	// The observation client continues to use the actually provisioned index.
	cfg.SnapshotHistoryIndexName += "-missing"
	r := dynamodbtest.NewRecorder()
	count, err := eventstore.KeepLatest(1)
	require.NoError(t, err)
	var notifications []error
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := dynamodb.New(t.Context(), e.NewClient(r.APIOption), cfg, eventstore.NewJSONSerializer[arbitraryEvent](), eventstore.NewJSONSerializer[arbitraryState](),
		eventstore.WithRetentionCount(count), eventstore.WithRetentionMode(eventstore.RetentionDelete),
		eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) { notifications = append(notifications, err) }))
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Order", "9")
	require.NoError(t, err)
	event, err := eventstore.NewEventEnvelope(id, 1, time.Unix(0, 123456789).UTC(), arbitraryEvent{Count: 1})
	require.NoError(t, err)
	state := arbitraryState{Values: map[string]int{"total": 1}}
	snapshot, err := eventstore.NewSnapshotEnvelope(state, 1, eventstore.WithManifest("state/任意"))
	require.NoError(t, err)
	require.NoError(t, store.PersistEventAndSnapshot(dynamodbtest.WithOperation(t.Context(), 1), event, snapshot))
	require.Len(t, notifications, 1)
	kind, ok := eventstore.KindOf(notifications[0])
	require.True(t, ok)
	require.Equal(t, eventstore.KindStorage, kind)
	require.NotNil(t, errors.Unwrap(notifications[0]))
	require.Contains(t, logs.String(), "dynamodb snapshot retention failed")
	require.Contains(t, logs.String(), "Order-9")
	requests := r.Requests(1)
	require.Len(t, requests, 2)
	require.Equal(t, "TransactWriteItems", requests[0].API)
	require.Equal(t, "Query", requests[1].API)
	assertPublicSnapshot(t, store, id, 1, 1, state)
	events, err := store.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Equal(t, []eventstore.EventEnvelope[arbitraryEvent]{event}, events)
	history, err := tables.History(t.Context(), "Order-9")
	require.NoError(t, err)
	require.Equal(t, []int64{1}, history.Active)
	t.Logf("public paired commit succeeded; real retention SDK failure notified once and logged; actual history=[1]")
}

func publicTables(t *testing.T, e *dynamodbtest.Environment) (*dynamodbtest.Tables, dynamodb.Config) {
	t.Helper()
	tables, err := e.CreateTables(t.Context(), false)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, tables.Close(ctx))
	})
	j, err := tables.TableName("journal")
	require.NoError(t, err)
	s, err := tables.TableName("snapshot")
	require.NoError(t, err)
	h, err := tables.TableName("head")
	require.NoError(t, err)
	layout, err := tables.Describe(t.Context())
	require.NoError(t, err)
	return tables, dynamodb.Config{JournalTableName: j, SnapshotTableName: s, HeadTableName: h, SnapshotHistoryIndexName: aws.ToString(layout["snapshot"].Description.GlobalSecondaryIndexes[0].IndexName)}
}

func publicKey(aid, sortKey string, n eventstore.SeqNr) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: aid}}
	if sortKey != "" {
		key[sortKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(n), 10)}
	}
	return key
}

func assertPublicSnapshot(t *testing.T, store eventstore.EventStore[arbitraryEvent, arbitraryState], id eventstore.AggregateID, head, snapshot eventstore.SeqNr, state arbitraryState) {
	t.Helper()
	out, err := store.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	if head == 0 {
		require.Nil(t, out)
		return
	}
	require.Equal(t, head, out.HeadSeqNr)
	if snapshot == 0 {
		require.Nil(t, out.Snapshot)
		return
	}
	require.Equal(t, snapshot, out.Snapshot.SeqNr())
	require.Equal(t, state, out.Snapshot.Aggregate())
	require.Equal(t, "state/任意", out.Snapshot.Manifest())
}

func TestDynamoDBPublicLocalLifecycleSharingIsolationAndOwnership(t *testing.T) {
	e := publicEnvironment(t)
	tables, cfg := publicTables(t, e)
	r := dynamodbtest.NewRecorder()
	observer := &dynamodbtest.ReadObserver{}
	keep, err := eventstore.KeepLatest(1)
	require.NoError(t, err)
	store, err := dynamodb.New(dynamodbtest.WithOperation(t.Context(), 0), e.NewClient(r.APIOption, observer.APIOption), cfg,
		eventstore.NewJSONSerializer[arbitraryEvent](), eventstore.NewJSONSerializer[arbitraryState](),
		eventstore.WithRetentionCount(keep), eventstore.WithRetentionMode(eventstore.RetentionDelete))
	require.NoError(t, err)
	requests := r.Requests(0)
	require.Len(t, requests, 2)
	require.Equal(t, "BatchGetItem", requests[0].API)
	require.Equal(t, "TransactWriteItems", requests[1].API)
	for _, request := range requests[0].Input.(*awsdynamodb.BatchGetItemInput).RequestItems {
		require.True(t, aws.ToBool(request.ConsistentRead))
		require.Len(t, request.Keys, 1)
	}
	configuration := observer.Results(0)
	require.Len(t, configuration, 1)
	for _, items := range configuration[0].Original.(*awsdynamodb.BatchGetItemOutput).Responses {
		require.Empty(t, items)
	}
	id, err := eventstore.NewAggregateID("Order", "9")
	require.NoError(t, err)
	assertPublicSnapshot(t, store, id, 0, 0, arbitraryState{})
	out, err := store.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Empty(t, out)
	at := time.Date(2026, 10, 5, 0, 0, 0, 123456789, time.UTC)
	var written []eventstore.EventEnvelope[arbitraryEvent]
	state := arbitraryState{Values: map[string]int{"total": 2}}
	for n := eventstore.SeqNr(1); n <= 4; n++ {
		payload := arbitraryEvent{Changes: []string{"change", "任意"}, Count: int(n)}
		event, err := eventstore.NewEventEnvelope(id, n, at, payload, eventstore.WithManifest("event/任意"))
		require.NoError(t, err)
		written = append(written, event)
		ctx := dynamodbtest.WithOperation(t.Context(), int(n))
		if n%2 == 0 {
			state = arbitraryState{Values: map[string]int{"total": int(n)}}
			snapshot, err := eventstore.NewSnapshotEnvelope(state, n, eventstore.WithManifest("state/任意"))
			require.NoError(t, err)
			require.NoError(t, store.PersistEventAndSnapshot(ctx, event, snapshot))
			assertPublicSnapshot(t, store, id, n, n, state)
		} else {
			require.NoError(t, store.PersistEvent(ctx, event))
			assertPublicSnapshot(t, store, id, n, n-1, state)
		}
		tx := r.Requests(int(n))[0].Input.(*awsdynamodb.TransactWriteItemsInput)
		if n%2 == 0 {
			require.Len(t, tx.TransactItems, 4)
		} else {
			require.Len(t, tx.TransactItems, 2)
		}
	}
	for _, request := range r.Requests(4)[1:] {
		if q, ok := request.Input.(*awsdynamodb.QueryInput); ok {
			require.Equal(t, cfg.SnapshotHistoryIndexName, aws.ToString(q.IndexName))
		}
	}
	history, err := tables.History(t.Context(), "Order-9")
	require.NoError(t, err)
	require.Equal(t, []int64{4}, history.Active)
	require.Empty(t, history.Marked)
	readCtx := dynamodbtest.WithOperation(t.Context(), 10)
	out, err = store.GetEventsByIDSinceSeqNr(readCtx, id, 2)
	require.NoError(t, err)
	require.Equal(t, written[1:], out)
	q := r.Requests(10)[0].Input.(*awsdynamodb.QueryInput)
	require.Equal(t, cfg.JournalTableName, aws.ToString(q.TableName))
	require.Nil(t, q.IndexName)
	require.True(t, aws.ToBool(q.ConsistentRead))
	require.True(t, aws.ToBool(q.ScanIndexForward))
	require.Equal(t, "aid = :aid AND seq_nr >= :seq_nr", aws.ToString(q.KeyConditionExpression))
	require.Nil(t, q.Limit)
	actual := observer.Results(10)[0].Original.(*awsdynamodb.QueryOutput)
	requestID, ok := awsmiddleware.GetRequestIDMetadata(actual.ResultMetadata)
	require.True(t, ok)
	require.NotEmpty(t, requestID)
	t.Logf("actual SDK Query: items=%d request_id=%s lower_bound=2 consistent=true ascending=true", len(actual.Items), requestID)
	// A prefixed aggregate must not appear in the exact-ID query.
	other, err := eventstore.NewAggregateID("Order", "90")
	require.NoError(t, err)
	otherEvent, err := eventstore.NewEventEnvelope(other, 1, at, arbitraryEvent{Count: 99})
	require.NoError(t, err)
	require.NoError(t, store.PersistEvent(t.Context(), otherEvent))
	out, err = store.GetEventsByIDSinceSeqNr(t.Context(), id, 2)
	require.NoError(t, err)
	require.Equal(t, written[1:], out)
	// Two public factories using the same tables share actual storage.
	shared, err := dynamodb.New(t.Context(), e.NewClient(), cfg, eventstore.NewJSONSerializer[arbitraryEvent](), eventstore.NewJSONSerializer[arbitraryState]())
	require.NoError(t, err)
	assertPublicSnapshot(t, shared, id, 4, 4, state)
	state.Values["total"] = -1
	written[3].Payload().Changes[0] = "changed input"
	out[0].Payload().Changes[0] = "changed result"
	read, err := shared.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	read.Snapshot.Aggregate().Values["total"] = -2
	assertPublicSnapshot(t, store, id, 4, 4, arbitraryState{Values: map[string]int{"total": 4}})
	out, err = shared.GetEventsByIDSinceSeqNr(t.Context(), id, 2)
	require.NoError(t, err)
	for _, event := range out {
		require.Equal(t, []string{"change", "任意"}, event.Payload().Changes)
	}
	// Physical attributes and types follow item-shapes, independent of product reads.
	journal, err := tables.GetItem(t.Context(), "journal", publicKey("Order-9", "seq_nr", 4))
	require.NoError(t, err)
	head, err := tables.GetItem(t.Context(), "head", publicKey("Order-9", "", 0))
	require.NoError(t, err)
	current, err := tables.GetItem(t.Context(), "snapshot", publicKey("Order-9", "skey", 0))
	require.NoError(t, err)
	require.Equal(t, map[string]types.AttributeValue{
		"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "4"}, "occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(at.UnixNano(), 10)},
		"manifest": &types.AttributeValueMemberS{Value: "event/任意"}, "payload": &types.AttributeValueMemberB{Value: []byte(`{"Changes":["change","任意"],"Count":4}`)},
	}, journal)
	metadata := map[string]types.AttributeValue{}
	for name, value := range journal {
		if name != "aid" {
			metadata[name] = value
		}
	}
	require.Equal(t, map[string]types.AttributeValue{"aid": journal["aid"], "type_name": &types.AttributeValueMemberS{Value: "Order"}, "seq_nr": journal["seq_nr"], "events": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: metadata}}}}, head)
	require.Equal(t, map[string]types.AttributeValue{
		"aid": journal["aid"], "skey": &types.AttributeValueMemberN{Value: "0"}, "seq_nr": journal["seq_nr"], "manifest": &types.AttributeValueMemberS{Value: "state/任意"},
		"payload": &types.AttributeValueMemberB{Value: []byte(`{"Values":{"total":4}}`)}, "last_updated_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(at.UnixMilli(), 10)},
	}, current)
	t.Logf("physical item shapes: journal=%d head=%d current=%d; history=[4]; input/result mutation left storage intact", len(journal), len(head), len(current))
	_, isolatedCfg := publicTables(t, e)
	isolated, err := dynamodb.New(t.Context(), e.NewClient(), isolatedCfg, eventstore.NewJSONSerializer[arbitraryEvent](), eventstore.NewJSONSerializer[arbitraryState]())
	require.NoError(t, err)
	assertPublicSnapshot(t, isolated, id, 0, 0, arbitraryState{})
	isolationEvent, err := eventstore.NewEventEnvelope(id, 1, at, arbitraryEvent{Count: 100})
	require.NoError(t, err)
	require.NoError(t, isolated.PersistEvent(t.Context(), isolationEvent))
	isolatedEvents, err := isolated.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Equal(t, []eventstore.EventEnvelope[arbitraryEvent]{isolationEvent}, isolatedEvents)
	assertPublicSnapshot(t, store, id, 4, 4, arbitraryState{Values: map[string]int{"total": 4}})
	// Existing write classifications reach the public caller through this factory.
	err = store.PersistEvent(t.Context(), written[0])
	var lock *eventstore.OptimisticLockError
	require.ErrorAs(t, err, &lock)
	require.Equal(t, eventstore.KindOptimisticLock, lock.Kind())
	require.NotNil(t, errors.Unwrap(err))
	gap, err := eventstore.NewEventEnvelope(id, 6, at, arbitraryEvent{})
	require.NoError(t, err)
	err = store.PersistEvent(t.Context(), gap)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, "W-8", violation.Rule)
}
