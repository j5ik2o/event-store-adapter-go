package dynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBEventsUnitAllPagesIncludingEmpty(t *testing.T) {
	r := dynamodbtest.NewRecorder()
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "2"}}
	pages := []*awsdynamodb.QueryOutput{
		{LastEvaluatedKey: key},
		{Items: []map[string]types.AttributeValue{readEventFixture("2"), readEventFixture("4")}},
	}
	calls := 0
	store := readUnitStore(t, r, 0, nil, func(input any) (any, error) {
		q := input.(*awsdynamodb.QueryInput)
		require.Equal(t, "journal", aws.ToString(q.TableName))
		require.Nil(t, q.IndexName)
		require.Nil(t, q.Limit)
		require.True(t, aws.ToBool(q.ConsistentRead))
		require.True(t, aws.ToBool(q.ScanIndexForward))
		require.Equal(t, "aid = :aid AND seq_nr >= :seq_nr", aws.ToString(q.KeyConditionExpression))
		require.Equal(t, &types.AttributeValueMemberS{Value: "Order-9"}, q.ExpressionAttributeValues[":aid"])
		require.Equal(t, &types.AttributeValueMemberN{Value: "2"}, q.ExpressionAttributeValues[":seq_nr"])
		if calls == 0 {
			require.Empty(t, q.ExclusiveStartKey)
		} else {
			require.Equal(t, key, q.ExclusiveStartKey)
		}
		out := pages[calls]
		calls++
		return out, nil
	})
	out, err := store.GetEventsByIDSinceSeqNr(dynamodbtest.WithOperation(t.Context(), 1), readID(t), 2)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, r.Requests(1), 2)
	for i, n := range []eventstore.SeqNr{2, 4} {
		require.Equal(t, n, out[i].SeqNr())
		require.Equal(t, "Order-9", out[i].AggregateID())
		require.Equal(t, int64(123456789), out[i].OccurredAt().UnixNano())
		require.Equal(t, "event/任意", out[i].Manifest())
		require.Equal(t, []byte("event bytes"), out[i].Payload())
	}
}

func TestDynamoDBEventsUnitStorageFailureHasNoPartialResult(t *testing.T) {
	cause := errors.New("page SDK error")
	otherAid := readEventFixture("3")
	otherAid["aid"] = &types.AttributeValueMemberS{Value: "Order-90"}
	for _, tc := range []struct {
		name  string
		item  map[string]types.AttributeValue
		cause error
	}{
		{name: "page failure", cause: cause}, {name: "descending", item: readEventFixture("1")},
		{name: "duplicate", item: readEventFixture("2")}, {name: "other aid", item: otherAid}, {name: "range", item: readEventFixture("9007199254740992")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			store := readUnitStore(t, dynamodbtest.NewRecorder(), 0, nil, func(any) (any, error) {
				calls++
				if calls == 1 {
					return &awsdynamodb.QueryOutput{Items: []map[string]types.AttributeValue{readEventFixture("2")}, LastEvaluatedKey: map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "2"}}}, nil
				}
				return &awsdynamodb.QueryOutput{Items: []map[string]types.AttributeValue{tc.item}}, tc.cause
			})
			out, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 2)
			require.Nil(t, out)
			requireKind(t, err, eventstore.KindStorage)
			require.Equal(t, 2, calls)
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
				require.NotNil(t, errors.Unwrap(err))
			}
		})
	}
	store := readUnitStore(t, dynamodbtest.NewRecorder(), 0, nil, func(any) (any, error) {
		return &awsdynamodb.QueryOutput{Items: []map[string]types.AttributeValue{readEventFixture("1")}}, nil
	})
	out, err := store.GetEventsByIDSinceSeqNr(t.Context(), readID(t), 2)
	require.Nil(t, out)
	requireKind(t, err, eventstore.KindStorage)
}
