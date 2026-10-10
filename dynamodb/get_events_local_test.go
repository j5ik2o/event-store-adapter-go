package dynamodb

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

type readEventDomain struct {
	Number int
	Values []string
}

func naturalReadFixture(t *testing.T, store eventstore.EventStore[readEventDomain, string]) []eventstore.EventEnvelope[readEventDomain] {
	t.Helper()
	var events []eventstore.EventEnvelope[readEventDomain]
	// Four independent items, each below 400 KiB, with more than 1 MiB of
	// manifest bytes in total. The shared canonical payload fixture is untouched.
	random := rand.New(rand.NewSource(1))
	for n := eventstore.SeqNr(1); n <= 4; n++ {
		alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		data := make([]byte, 400000)
		for i := range data {
			data[i] = alphabet[random.Intn(len(alphabet))]
		}
		manifest := string(data)
		event := eventEnvelope(t, "Order", "9", n, time.Unix(0, 123456789).UTC(), readEventDomain{Number: int(n), Values: []string{"任意", strconv.FormatInt(int64(n), 10)}}, manifest)
		require.NoError(t, store.PersistEvent(t.Context(), event))
		events = append(events, event)
	}
	return events
}

func logQueryObservations(t *testing.T, observer *dynamodbtest.ReadObserver, operation int) {
	t.Helper()
	for i, observation := range observer.Results(operation) {
		if observation.OriginalError != nil {
			t.Logf("SDK Query[%d] original_error=%T %v", i, observation.OriginalError, observation.OriginalError)
			continue
		}
		original := observation.Original.(*awsdynamodb.QueryOutput)
		received := observation.Received.(*awsdynamodb.QueryOutput)
		id, ok := awsmiddleware.GetRequestIDMetadata(original.ResultMetadata)
		require.True(t, ok)
		require.NotEmpty(t, id)
		traceEvent(t, map[string]any{"page": i, "original_count": original.Count, "scanned_count": original.ScannedCount, "original_last_evaluated_key": original.LastEvaluatedKey, "received_count": received.Count, "received_last_evaluated_key": received.LastEvaluatedKey, "sdk_request_id": id})
	}
}

func assertNaturalRequests(t *testing.T, r *dynamodbtest.Recorder, observer *dynamodbtest.ReadObserver, cfg Config, operation int) {
	t.Helper()
	requests, observations := r.Requests(operation), observer.Results(operation)
	require.GreaterOrEqual(t, len(requests), 2, "the real first page must have a natural 1 MiB LEK")
	require.Len(t, observations, len(requests))
	for i, request := range requests {
		q := request.Input.(*awsdynamodb.QueryInput)
		traceEvent(t, map[string]any{"sdk_request_number": i, "sdk_query_input": q})
		require.Equal(t, cfg.JournalTableName, aws.ToString(q.TableName))
		require.Nil(t, q.IndexName)
		require.Nil(t, q.Limit)
		require.Nil(t, q.FilterExpression)
		require.True(t, aws.ToBool(q.ConsistentRead))
		require.True(t, aws.ToBool(q.ScanIndexForward))
		require.Equal(t, "aid = :aid AND seq_nr >= :seq_nr", aws.ToString(q.KeyConditionExpression))
		require.Equal(t, &types.AttributeValueMemberS{Value: "Order-9"}, q.ExpressionAttributeValues[":aid"])
		require.Equal(t, &types.AttributeValueMemberN{Value: "0"}, q.ExpressionAttributeValues[":seq_nr"])
		if i == 0 {
			require.Empty(t, q.ExclusiveStartKey)
		} else {
			previous := observations[i-1].Original.(*awsdynamodb.QueryOutput)
			require.NotEmpty(t, previous.LastEvaluatedKey)
			require.Equal(t, previous.LastEvaluatedKey, q.ExclusiveStartKey)
		}
	}
}

func TestDynamoDBEventsLocalNaturalPagesAndRealMiddlePageFailure(t *testing.T) {
	e := configurationEnvironment(t)
	for _, scenario := range []string{"all pages", "middle page SDK failure", "middle page stored corruption"} {
		t.Run(scenario, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			frozenCfg := cfg
			r := dynamodbtest.NewRecorder()
			observer := &dynamodbtest.ReadObserver{Match: productReadMatch(cfg)}
			es := &readCodec[readEventDomain]{base: eventstore.NewJSONSerializer[readEventDomain]()}
			ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
			store, err := New(t.Context(), e.NewClient(r.APIOption, observer.APIOption), cfg, es, ss)
			require.NoError(t, err)
			written := naturalReadFixture(t, store)
			var physicalBytes int
			for n := eventstore.SeqNr(1); n <= 4; n++ {
				physical, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", n))
				require.NoError(t, err)
				require.Len(t, physical, 5)
				require.Len(t, physical["manifest"].(*types.AttributeValueMemberS).Value, 400000)
				size, err := itemSizeUpperBound(physical)
				require.NoError(t, err)
				require.Less(t, size, 409600)
				physicalBytes += size
			}
			require.Greater(t, physicalBytes, 1048576)
			id := &changingReadID{typeName: "Order", value: "9"}
			calls := 0
			deleted := 0
			observer.Before = func(ctx context.Context, _ any) error {
				calls++
				id.typeName, id.value = "Changed", "caller"
				if scenario == "middle page SDK failure" && calls == 2 {
					deleted++
					_, err := e.NewClient().DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: aws.String(frozenCfg.JournalTableName)})
					return err
				}
				return nil
			}
			if scenario == "middle page stored corruption" {
				item, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", 4))
				require.NoError(t, err)
				item["payload"] = &types.AttributeValueMemberS{Value: "invalid B"}
				_, err = e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.JournalTableName), Item: item})
				require.NoError(t, err)
				physical, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", 4))
				require.NoError(t, err)
				require.Equal(t, item, physical)
			}
			cfg.JournalTableName = "changed-after-generation"
			out, err := store.GetEventsByIDSinceSeqNr(dynamodbtest.WithOperation(t.Context(), 1), id, 0)
			logQueryObservations(t, observer, 1)
			assertNaturalRequests(t, r, observer, frozenCfg, 1)
			if scenario == "all pages" {
				require.NoError(t, err)
				require.Equal(t, written, out)
				require.Equal(t, 4, es.deserializeCalls)
				for _, observation := range observer.Results(1) {
					require.Equal(t, observation.Original, observation.Received)
				}
			} else {
				require.True(t, out == nil, "failed read returned %d events", len(out))
				requireKind(t, err, eventstore.KindStorage)
				require.Zero(t, es.deserializeCalls)
				if scenario == "middle page SDK failure" {
					var missing *types.ResourceNotFoundException
					require.ErrorAs(t, err, &missing)
					require.ErrorIs(t, err, observer.Results(1)[1].OriginalError)
					require.Equal(t, 1, deleted)
				}
			}
			require.Zero(t, ss.deserializeCalls)
			t.Logf("independent fixture: exactly 4 real items, total estimated bytes=%d, original SDK requests=%d, no Limit/Filter", physicalBytes, len(r.Requests(1)))
		})
	}
}

func TestDynamoDBEventsLocalDedicatedRestorationAndBytesOwnership(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	observer := &dynamodbtest.ReadObserver{Match: productReadMatch(cfg)}
	var delivered *awsdynamodb.QueryOutput
	observer.After = func(_ context.Context, _ any, result any) (any, error) {
		delivered = result.(*awsdynamodb.QueryOutput)
		return result, nil
	}
	es := &readCodec[readEventDomain]{base: eventstore.NewJSONSerializer[readEventDomain]()}
	ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	store, err := New(t.Context(), e.NewClient(observer.APIOption), cfg, es, ss)
	require.NoError(t, err)
	var written []eventstore.EventEnvelope[readEventDomain]
	for n := eventstore.SeqNr(1); n <= 3; n++ {
		event := eventEnvelope(t, "Order", "9", n, time.Unix(0, 123456789).UTC(), readEventDomain{Number: int(n), Values: []string{"arbitrary domain"}}, "event/任意")
		require.NoError(t, store.PersistEvent(t.Context(), event))
		written = append(written, event)
	}
	es.deserialize = func(data []byte) (readEventDomain, error) {
		value, err := es.base.Deserialize(data)
		require.NotContains(t, string(data), "manifest")
		require.NotContains(t, string(data), "aid")
		clear(data)
		return value, err
	}
	out, err := store.GetEventsByIDSinceSeqNr(dynamodbtest.WithOperation(t.Context(), 1), readID(t), 1)
	require.NoError(t, err)
	require.Equal(t, written, out)
	for i, item := range delivered.Items {
		physical, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", eventstore.SeqNr(i+1)))
		require.NoError(t, err)
		require.Equal(t, physical, item)
		require.Equal(t, physical, observer.Results(1)[0].Original.(*awsdynamodb.QueryOutput).Items[i])
	}
	out[0].Payload().Values[0] = "changed returned value"
	cause := errors.New("dedicated event restore failed after one successful event")
	calls := 0
	es.deserialize = func(data []byte) (readEventDomain, error) {
		calls++
		if calls == 2 {
			return readEventDomain{}, cause
		}
		return es.base.Deserialize(data)
	}
	out, err = store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 0)
	require.Nil(t, out)
	requireKind(t, err, eventstore.KindSerialization)
	require.Equal(t, cause, errors.Unwrap(err))
	require.Equal(t, 2, calls)
	require.Zero(t, ss.deserializeCalls)
	es.deserialize = nil
	out, err = store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 0)
	require.NoError(t, err)
	require.Equal(t, "arbitrary domain", out[0].Payload().Values[0])
	logQueryObservations(t, observer, 1)
}

func TestDynamoDBEventsLocalInputRejectionAndValidBounds(t *testing.T) {
	e := configurationEnvironment(t)
	_, cfg := configurationTables(t, e, false)
	r := dynamodbtest.NewRecorder()
	es := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	store, err := New(t.Context(), e.NewClient(r.APIOption), cfg, es, ss)
	require.NoError(t, err)
	for i, tc := range []struct {
		name, rule string
		id         eventstore.AggregateID
		n          eventstore.SeqNr
	}{
		{"nil ID", "T-2", nil, 7}, {"typed nil ID", "T-2", (*changingReadID)(nil), 8},
		{"type hyphen", "T-11", &changingReadID{typeName: "Bad-Type", value: "9"}, 9},
		{"ID byte length", "T-12", &changingReadID{typeName: "Order", value: strings.Repeat("界", 342)}, 10},
		{"negative", "T-9", readID(t), -1}, {"above maximum", "T-9", readID(t), eventstore.MaxSeqNr + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operation := i + 1
			out, err := store.GetEventsByIDSinceSeqNr(dynamodbtest.WithOperation(t.Context(), operation), tc.id, tc.n)
			require.Nil(t, out)
			requireEventKind(t, err, eventstore.KindContractViolation)
			var violation *eventstore.ContractViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, tc.rule, violation.Rule)
			require.Equal(t, tc.n, *violation.SeqNr)
			if tc.rule != "T-9" {
				snapshot, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), operation), tc.id)
				require.Nil(t, snapshot)
				requireEventKind(t, err, eventstore.KindContractViolation)
			}
			require.Empty(t, r.Requests(operation))
			require.Zero(t, es.serializeCalls)
			require.Zero(t, ss.serializeCalls)
			require.Zero(t, es.deserializeCalls)
			require.Zero(t, ss.deserializeCalls)
		})
	}
	for i, n := range []eventstore.SeqNr{0, eventstore.MaxSeqNr} {
		operation := i + 20
		out, err := store.GetEventsByIDSinceSeqNr(dynamodbtest.WithOperation(t.Context(), operation), readID(t), n)
		require.NoError(t, err)
		require.Empty(t, out)
		require.Len(t, r.Requests(operation), 1)
		require.Equal(t, &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(n), 10)}, r.Requests(operation)[0].Input.(*awsdynamodb.QueryInput).ExpressionAttributeValues[":seq_nr"])
	}
}

func TestDynamoDBEventsLocalStoredCorruption(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	es := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	store, err := New(t.Context(), e.NewClient(), cfg, es, ss)
	require.NoError(t, err)
	persistReadPair(t, store, 1)
	for _, tc := range []struct {
		name   string
		change func(map[string]types.AttributeValue)
	}{
		{"missing manifest", func(i map[string]types.AttributeValue) { delete(i, "manifest") }},
		{"wrong binary type", func(i map[string]types.AttributeValue) { i["payload"] = &types.AttributeValueMemberS{Value: "text"} }},
		{"fractional time", func(i map[string]types.AttributeValue) { i["occurred_at"] = &types.AttributeValueMemberN{Value: "0.1"} }},
		{"time overflow", func(i map[string]types.AttributeValue) {
			i["occurred_at"] = &types.AttributeValueMemberN{Value: "9223372036854775808"}
		}},
		{"fractional sequence", func(i map[string]types.AttributeValue) { i["seq_nr"] = &types.AttributeValueMemberN{Value: "1.5"} }},
		{"event zero", func(i map[string]types.AttributeValue) { i["seq_nr"] = &types.AttributeValueMemberN{Value: "0"} }},
		{"sequence above maximum", func(i map[string]types.AttributeValue) {
			i["seq_nr"] = &types.AttributeValueMemberN{Value: "9007199254740992"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", 1))
			require.NoError(t, err)
			item, err := tables.GetItem(t.Context(), "journal", eventKey("Order-9", "seq_nr", 1))
			require.NoError(t, err)
			tc.change(item)
			_, err = e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.JournalTableName), Item: item})
			require.NoError(t, err)
			key := map[string]types.AttributeValue{"aid": item["aid"], "seq_nr": item["seq_nr"]}
			t.Cleanup(func() {
				_, err := e.NewClient().DeleteItem(context.Background(), &awsdynamodb.DeleteItemInput{TableName: aws.String(cfg.JournalTableName), Key: key})
				require.NoError(t, err)
				_, err = e.NewClient().PutItem(context.Background(), &awsdynamodb.PutItemInput{TableName: aws.String(cfg.JournalTableName), Item: good})
				require.NoError(t, err)
			})
			physical, err := tables.GetItem(t.Context(), "journal", key)
			require.NoError(t, err)
			require.Equal(t, item, physical)
			out, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 0)
			require.Nil(t, out)
			requireKind(t, err, eventstore.KindStorage)
			require.Zero(t, es.deserializeCalls)
			require.Zero(t, ss.deserializeCalls)
			t.Logf("actual PutItem corruption: journal case=%s Storage; restoration calls=0", tc.name)
		})
	}
}

func TestDynamoDBEventsLocalConcurrentReadersAndWriter(t *testing.T) {
	e := configurationEnvironment(t)
	_, cfg := configurationTables(t, e, false)
	store, err := New(t.Context(), e.NewClient(), cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	require.NoError(t, err)
	start := make(chan struct{})
	failures := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		for n := eventstore.SeqNr(1); n <= 4; n++ {
			id, _ := eventstore.NewAggregateID("Order", "9")
			event, err := eventstore.NewEventEnvelope(id, n, time.Unix(0, 123), "event")
			if err == nil {
				err = store.PersistEvent(t.Context(), event)
			}
			if err != nil {
				failures <- err
				return
			}
		}
	}()
	for i := 0; i < 2; i++ {
		go func() {
			defer workers.Done()
			<-start
			id, _ := eventstore.NewAggregateID("Order", "9")
			for j := 0; j < 10; j++ {
				if _, err := store.GetEventsByIDSinceSeqNr(t.Context(), id, 0); err != nil {
					failures <- err
					return
				}
				if _, err := store.GetLatestSnapshotByID(t.Context(), id); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	out, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 0)
	require.NoError(t, err)
	require.Len(t, out, 4)
	snapshot, err := store.GetLatestSnapshotByID(t.Context(), readID(t))
	require.NoError(t, err)
	require.Nil(t, snapshot.Snapshot)
	require.Equal(t, eventstore.SeqNr(4), snapshot.HeadSeqNr)
}
