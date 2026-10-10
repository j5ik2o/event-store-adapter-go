package dynamodb

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

type readCodec[T any] struct {
	base                             eventstore.Serializer[T]
	serialize                        func(T) ([]byte, error)
	deserialize                      func([]byte) (T, error)
	serializeCalls, deserializeCalls int
}

func (c *readCodec[T]) Serialize(value T) ([]byte, error) {
	c.serializeCalls++
	if c.serialize != nil {
		return c.serialize(value)
	}
	return c.base.Serialize(value)
}
func (c *readCodec[T]) Deserialize(data []byte) (T, error) {
	c.deserializeCalls++
	if c.deserialize != nil {
		return c.deserialize(data)
	}
	return c.base.Deserialize(data)
}

type changingReadID struct{ typeName, value string }

func (id *changingReadID) TypeName() string { return id.typeName }
func (id *changingReadID) Value() string    { return id.value }

func productReadMatch(cfg Config) func(any) bool {
	return func(input any) bool {
		switch input := input.(type) {
		case *awsdynamodb.QueryInput:
			return aws.ToString(input.TableName) == cfg.JournalTableName && input.IndexName == nil
		case *awsdynamodb.BatchGetItemInput:
			if len(input.RequestItems) == 0 {
				return false
			}
			for table, request := range input.RequestItems {
				if table != cfg.HeadTableName && table != cfg.SnapshotTableName {
					return false
				}
				for _, key := range request.Keys {
					id, ok := key["aid"].(*types.AttributeValueMemberS)
					if !ok || id.Value == "__config__" {
						return false
					}
				}
			}
			return true
		}
		return false
	}
}

func snapshotReadMatch(cfg Config) func(any) bool {
	match := productReadMatch(cfg)
	return func(input any) bool {
		_, batch := input.(*awsdynamodb.BatchGetItemInput)
		return batch && match(input)
	}
}

func persistReadPair(t *testing.T, store eventstore.EventStore[string, string], n eventstore.SeqNr) {
	t.Helper()
	event := eventEnvelope(t, "Order", "9", n, time.Unix(0, 123456789), "event "+strconv.FormatInt(int64(n), 10), "event/任意")
	snapshot, err := eventstore.NewSnapshotEnvelope("state "+strconv.FormatInt(int64(n), 10), n, eventstore.WithManifest("state/任意"))
	require.NoError(t, err)
	require.NoError(t, store.PersistEventAndSnapshot(t.Context(), event, snapshot))
}

func logBatchObservations(t *testing.T, observer *dynamodbtest.ReadObserver, operation int) {
	t.Helper()
	for i, observation := range observer.Results(operation) {
		if observation.OriginalError != nil {
			t.Logf("SDK BatchGet[%d] original_error=%T %v", i, observation.OriginalError, observation.OriginalError)
			continue
		}
		original := observation.Original.(*awsdynamodb.BatchGetItemOutput)
		received := observation.Received.(*awsdynamodb.BatchGetItemOutput)
		id, ok := awsmiddleware.GetRequestIDMetadata(original.ResultMetadata)
		require.True(t, ok)
		require.NotEmpty(t, id)
		traceEvent(t, map[string]any{"request": i, "original_tables": len(original.Responses), "original_unprocessed_keys": original.UnprocessedKeys, "received_tables": len(received.Responses), "received_unprocessed_keys": received.UnprocessedKeys, "sdk_request_id": id})
	}
}

func TestDynamoDBSnapshotLocalUnprocessedAndFixedInputs(t *testing.T) {
	e := configurationEnvironment(t)
	for _, tc := range []struct {
		name             string
		limit, deferrals int
		success          bool
	}{
		{"success", 2, 2, true}, {"zero", 0, 1, false}, {"exhausted", 2, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			limit := tc.limit
			cfg.ConfigurationReadRetryLimit = &limit
			r := dynamodbtest.NewRecorder()
			hooks := testhook.New()
			var waits []time.Duration
			hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
			id := &changingReadID{typeName: "Order", value: "9"}
			observer := &dynamodbtest.ReadObserver{Match: snapshotReadMatch(cfg)}
			calls := 0
			observer.After = func(_ context.Context, input, actual any) (any, error) {
				calls++
				id.typeName, id.value = "Changed", "id"
				if calls <= tc.deferrals {
					return dynamodbtest.DeferReadKeys(input.(*awsdynamodb.BatchGetItemInput), actual.(*awsdynamodb.BatchGetItemOutput), cfg.SnapshotTableName)
				}
				return actual, nil
			}
			es := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
			ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
			store, err := newWithHooks(t.Context(), e.NewClient(r.APIOption, observer.APIOption), cfg, es, ss, hooks)
			require.NoError(t, err)
			persistReadPair(t, store, 1)
			// The same store must use its generation-time retry limit after this pointer changes.
			limit = 0
			out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), id)
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, eventstore.SeqNr(1), out.HeadSeqNr)
				require.Equal(t, "state 1", out.Snapshot.Aggregate())
				require.Equal(t, 1, ss.deserializeCalls)
			} else {
				require.Nil(t, out)
				requireKind(t, err, eventstore.KindStorage)
				require.Zero(t, ss.deserializeCalls)
			}
			require.Zero(t, es.deserializeCalls)
			require.Equal(t, tc.limit+1, calls)
			require.Len(t, waits, tc.limit)
			for i, d := range waits {
				require.Equal(t, 50*time.Millisecond*time.Duration(1<<i), d)
			}
			requests := r.Requests(1)
			observations := observer.Results(1)
			require.Len(t, requests, calls)
			require.Len(t, observations, calls)
			for i, request := range requests {
				input := request.Input.(*awsdynamodb.BatchGetItemInput)
				if i == 0 {
					require.Len(t, input.RequestItems, 2)
				} else {
					require.Equal(t, observations[i-1].Received.(*awsdynamodb.BatchGetItemOutput).UnprocessedKeys, input.RequestItems)
				}
				for table, keys := range input.RequestItems {
					require.True(t, aws.ToBool(keys.ConsistentRead))
					require.Len(t, keys.Keys, 1)
					require.Equal(t, &types.AttributeValueMemberS{Value: "Order-9"}, keys.Keys[0]["aid"])
					if table == cfg.SnapshotTableName {
						require.Equal(t, &types.AttributeValueMemberN{Value: "0"}, keys.Keys[0]["skey"])
					}
				}
			}
			original := observations[0].Original.(*awsdynamodb.BatchGetItemOutput)
			received := observations[0].Received.(*awsdynamodb.BatchGetItemOutput)
			require.Len(t, original.Responses, 2)
			require.Empty(t, original.UnprocessedKeys)
			require.NotContains(t, received.Responses, cfg.SnapshotTableName)
			physical, err := tables.GetItem(t.Context(), "snapshot", eventKey("Order-9", "skey", 0))
			require.NoError(t, err)
			require.Equal(t, original.Responses[cfg.SnapshotTableName][0], physical)
			logBatchObservations(t, observer, 1)
		})
	}
}

func TestDynamoDBSnapshotLocalNormalOmissions(t *testing.T) {
	e := configurationEnvironment(t)
	for _, missing := range []string{"head", "snapshot"} {
		t.Run(missing, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			r := dynamodbtest.NewRecorder()
			observer := &dynamodbtest.ReadObserver{Match: snapshotReadMatch(cfg)}
			store, err := New(t.Context(), e.NewClient(r.APIOption, observer.APIOption), cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
			require.NoError(t, err)
			persistReadPair(t, store, 1)
			before, err := store.GetLatestSnapshotByID(t.Context(), readID(t))
			require.NoError(t, err)
			require.NotNil(t, before.Snapshot)
			table := cfg.HeadTableName
			key := eventKey("Order-9", "", 0)
			if missing == "snapshot" {
				table = cfg.SnapshotTableName
				key = eventKey("Order-9", "skey", 0)
			}
			_, err = e.NewClient().DeleteItem(t.Context(), &awsdynamodb.DeleteItemInput{TableName: aws.String(table), Key: key})
			require.NoError(t, err)
			out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
			require.NoError(t, err)
			if missing == "head" {
				require.Nil(t, out)
			} else {
				require.Equal(t, eventstore.SeqNr(1), out.HeadSeqNr)
				require.Nil(t, out.Snapshot)
			}
			require.Len(t, r.Requests(1), 1)
			observation := observer.Results(1)[0]
			original := observation.Original.(*awsdynamodb.BatchGetItemOutput)
			require.Empty(t, original.Responses[table])
			require.Empty(t, original.UnprocessedKeys)
			require.Equal(t, original, observation.Received)
			physical, err := tables.GetItem(t.Context(), dynamodbtest.Table(missing), key)
			require.NoError(t, err)
			require.Empty(t, physical)
			logBatchObservations(t, observer, 1)
		})
	}
}

func TestDynamoDBSnapshotLocalLegalInterleaveAndLaterAppend(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	r := dynamodbtest.NewRecorder()
	observer := &dynamodbtest.ReadObserver{Match: snapshotReadMatch(cfg)}
	serializer := eventstore.NewJSONSerializer[string]()
	store, err := New(t.Context(), e.NewClient(r.APIOption, observer.APIOption), cfg, serializer, serializer)
	require.NoError(t, err)
	writer, err := New(t.Context(), e.NewClient(), cfg, serializer, serializer)
	require.NoError(t, err)
	persistReadPair(t, writer, 1)
	var captured map[string]types.AttributeValue
	fired := 0
	observer.Before = func(ctx context.Context, _ any) error {
		if fired != 0 {
			return nil
		}
		fired++
		var err error
		captured, err = tables.GetItem(ctx, "head", eventKey("Order-9", "", 0))
		if err != nil {
			return err
		}
		persistReadPair(t, writer, 2)
		return nil
	}
	observer.After = func(_ context.Context, _ any, result any) (any, error) {
		out := result.(*awsdynamodb.BatchGetItemOutput)
		if fired == 1 {
			out.Responses[cfg.HeadTableName] = []map[string]types.AttributeValue{captured}
			fired++
		}
		return out, nil
	}
	out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
	require.NoError(t, err)
	require.Equal(t, eventstore.SeqNr(1), out.HeadSeqNr)
	require.Equal(t, eventstore.SeqNr(2), out.Snapshot.SeqNr())
	require.Equal(t, "state 2", out.Snapshot.Aggregate())
	require.Equal(t, 2, fired)
	require.Len(t, r.Requests(1), 1)
	observation := observer.Results(1)[0]
	original := observation.Original.(*awsdynamodb.BatchGetItemOutput)
	received := observation.Received.(*awsdynamodb.BatchGetItemOutput)
	require.Equal(t, &types.AttributeValueMemberN{Value: "2"}, original.Responses[cfg.HeadTableName][0]["seq_nr"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, received.Responses[cfg.HeadTableName][0]["seq_nr"])
	require.Equal(t, original.Responses[cfg.SnapshotTableName], received.Responses[cfg.SnapshotTableName])
	physical, err := tables.GetItem(t.Context(), "head", eventKey("Order-9", "", 0))
	require.NoError(t, err)
	require.Equal(t, original.Responses[cfg.HeadTableName][0], physical)
	require.NoError(t, store.PersistEvent(t.Context(), eventEnvelope(t, "Order", "9", 3, time.Unix(0, 123), "event 3", "")))
	out, err = store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 2), readID(t))
	require.NoError(t, err)
	require.Equal(t, eventstore.SeqNr(3), out.HeadSeqNr)
	require.Equal(t, eventstore.SeqNr(2), out.Snapshot.SeqNr())
	events, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 3)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "event 3", events[0].Payload())
	logBatchObservations(t, observer, 1)
	logBatchObservations(t, observer, 2)
}

func TestDynamoDBSnapshotLocalStoredCorruption(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	es := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	store, err := New(t.Context(), e.NewClient(), cfg, es, ss)
	require.NoError(t, err)
	persistReadPair(t, store, 1)
	for _, tc := range []struct {
		name, table string
		change      func(map[string]types.AttributeValue)
	}{
		{"missing manifest", "snapshot", func(i map[string]types.AttributeValue) { delete(i, "manifest") }},
		{"wrong payload type", "snapshot", func(i map[string]types.AttributeValue) { i["payload"] = &types.AttributeValueMemberS{Value: "bytes"} }},
		{"fractional sequence", "snapshot", func(i map[string]types.AttributeValue) { i["seq_nr"] = &types.AttributeValueMemberN{Value: "1.5"} }},
		{"negative sequence", "snapshot", func(i map[string]types.AttributeValue) { i["seq_nr"] = &types.AttributeValueMemberN{Value: "-1"} }},
		{"sequence above maximum", "snapshot", func(i map[string]types.AttributeValue) {
			i["seq_nr"] = &types.AttributeValueMemberN{Value: "9007199254740992"}
		}},
		{"fractional update time", "snapshot", func(i map[string]types.AttributeValue) {
			i["last_updated_at"] = &types.AttributeValueMemberN{Value: "0.1"}
		}},
		{"wrong head type", "head", func(i map[string]types.AttributeValue) { i["type_name"] = &types.AttributeValueMemberS{Value: "Other"} }},
		{"missing head events", "head", func(i map[string]types.AttributeValue) { delete(i, "events") }},
		{"empty head events", "head", func(i map[string]types.AttributeValue) {
			i["events"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{}}
		}},
		{"inconsistent head sequence", "head", func(i map[string]types.AttributeValue) { i["seq_nr"] = &types.AttributeValueMemberN{Value: "2"} }},
		{"head occurrence overflow", "head", func(i map[string]types.AttributeValue) {
			i["events"].(*types.AttributeValueMemberL).Value[0].(*types.AttributeValueMemberM).Value["occurred_at"] = &types.AttributeValueMemberN{Value: "9223372036854775808"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := cfg.HeadTableName
			key := eventKey("Order-9", "", 0)
			if tc.table == "snapshot" {
				table = cfg.SnapshotTableName
				key = eventKey("Order-9", "skey", 0)
			}
			good, err := tables.GetItem(t.Context(), dynamodbtest.Table(tc.table), key)
			require.NoError(t, err)
			item, err := tables.GetItem(t.Context(), dynamodbtest.Table(tc.table), key)
			require.NoError(t, err)
			tc.change(item)
			_, err = e.NewClient().PutItem(t.Context(), &awsdynamodb.PutItemInput{TableName: aws.String(table), Item: item})
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := e.NewClient().PutItem(context.Background(), &awsdynamodb.PutItemInput{TableName: aws.String(table), Item: good})
				require.NoError(t, err)
			})
			physical, err := tables.GetItem(t.Context(), dynamodbtest.Table(tc.table), key)
			require.NoError(t, err)
			require.Equal(t, item, physical)
			out, err := store.GetLatestSnapshotByID(t.Context(), readID(t))
			require.Nil(t, out)
			requireKind(t, err, eventstore.KindStorage)
			require.Zero(t, es.deserializeCalls)
			require.Zero(t, ss.deserializeCalls)
			t.Logf("actual PutItem corruption: table=%s case=%s Storage; restoration calls=0", tc.table, tc.name)
		})
	}
}

func TestDynamoDBSnapshotLocalBytesOwnershipAndDeserializeFailure(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	observer := &dynamodbtest.ReadObserver{Match: snapshotReadMatch(cfg)}
	var delivered *awsdynamodb.BatchGetItemOutput
	observer.After = func(_ context.Context, _ any, result any) (any, error) {
		delivered = result.(*awsdynamodb.BatchGetItemOutput)
		return result, nil
	}
	es := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	ss := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	ss.deserialize = func(data []byte) (string, error) {
		require.Equal(t, []byte(`"state 1"`), data)
		value, err := ss.base.Deserialize(data)
		clear(data)
		return value, err
	}
	store, err := New(t.Context(), e.NewClient(observer.APIOption), cfg, es, ss)
	require.NoError(t, err)
	persistReadPair(t, store, 1)
	out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
	require.NoError(t, err)
	require.Equal(t, "state 1", out.Snapshot.Aggregate())
	require.Equal(t, []byte(`"state 1"`), delivered.Responses[cfg.SnapshotTableName][0]["payload"].(*types.AttributeValueMemberB).Value)
	physical, err := tables.GetItem(t.Context(), "snapshot", eventKey("Order-9", "skey", 0))
	require.NoError(t, err)
	require.Equal(t, physical, observer.Results(1)[0].Original.(*awsdynamodb.BatchGetItemOutput).Responses[cfg.SnapshotTableName][0])
	require.Zero(t, es.deserializeCalls)
	cause := errors.New("dedicated snapshot restore failed")
	ss.deserialize = func([]byte) (string, error) { return "", cause }
	out, err = store.GetLatestSnapshotByID(t.Context(), readID(t))
	require.Nil(t, out)
	requireKind(t, err, eventstore.KindSerialization)
	require.Equal(t, cause, errors.Unwrap(err))
	ss.deserialize = nil
	out, err = store.GetLatestSnapshotByID(t.Context(), readID(t))
	require.NoError(t, err)
	require.Equal(t, "state 1", out.Snapshot.Aggregate())
	logBatchObservations(t, observer, 1)
}

func TestDynamoDBSnapshotLocalRealSDKFailure(t *testing.T) {
	e := configurationEnvironment(t)
	_, cfg := configurationTables(t, e, false)
	observer := &dynamodbtest.ReadObserver{Match: snapshotReadMatch(cfg)}
	r := dynamodbtest.NewRecorder()
	serializer := &readCodec[string]{base: eventstore.NewJSONSerializer[string]()}
	store, err := New(t.Context(), e.NewClient(r.APIOption, observer.APIOption), cfg, serializer, serializer)
	require.NoError(t, err)
	persistReadPair(t, store, 1)
	fired := 0
	observer.Before = func(ctx context.Context, _ any) error {
		fired++
		_, err := e.NewClient().DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: aws.String(cfg.SnapshotTableName)})
		return err
	}
	out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
	require.Nil(t, out)
	requireKind(t, err, eventstore.KindStorage)
	var missing *types.ResourceNotFoundException
	require.ErrorAs(t, err, &missing)
	require.ErrorIs(t, err, observer.Results(1)[0].OriginalError)
	require.Equal(t, 1, fired)
	require.Len(t, r.Requests(1), 1)
	require.Zero(t, serializer.deserializeCalls)
	logBatchObservations(t, observer, 1)
}

func TestDynamoDBSnapshotLocalSharedSerializerBuffer(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	base := eventstore.NewJSONSerializer[string]()
	buffer := make([]byte, 64)
	serialize := func(value string) ([]byte, error) {
		data, err := base.Serialize(value)
		if err != nil {
			return nil, err
		}
		copy(buffer, data)
		return buffer[:len(data)], nil
	}
	es := &readCodec[string]{base: base, serialize: serialize}
	ss := &readCodec[string]{base: base, serialize: serialize}
	store, err := New(t.Context(), e.NewClient(), cfg, es, ss)
	require.NoError(t, err)
	persistReadPair(t, store, 1)
	clear(buffer)
	require.Equal(t, 1, es.serializeCalls)
	require.Equal(t, 1, ss.serializeCalls)
	events, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "event 1", events[0].Payload())
	snapshot, err := store.GetLatestSnapshotByID(t.Context(), readID(t))
	require.NoError(t, err)
	require.Equal(t, "state 1", snapshot.Snapshot.Aggregate())
	for _, tc := range []struct {
		table, sortKey, data string
		n                    eventstore.SeqNr
	}{{"journal", "seq_nr", `"event 1"`, 1}, {"snapshot", "skey", `"state 1"`, 0}} {
		item, err := tables.GetItem(t.Context(), dynamodbtest.Table(tc.table), eventKey("Order-9", tc.sortKey, tc.n))
		require.NoError(t, err)
		require.Equal(t, []byte(tc.data), item["payload"].(*types.AttributeValueMemberB).Value)
	}
}
