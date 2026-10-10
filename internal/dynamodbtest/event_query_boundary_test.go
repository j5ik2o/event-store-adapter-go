package dynamodbtest

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestEventQueryBoundary(t *testing.T) {
	item := func(n, size int) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"aid":     &types.AttributeValueMemberS{Value: "Order-9"},
			"seq_nr":  &types.AttributeValueMemberN{Value: strconv.Itoa(n)},
			"payload": &types.AttributeValueMemberB{Value: bytes.Repeat([]byte("x"), size)},
		}
	}
	original := &dynamodb.QueryOutput{Count: 4, ScannedCount: 4, Items: []map[string]types.AttributeValue{item(1, 320022), item(2, 320022), item(3, 320022), item(4, 320022)}}
	original.ResultMetadata.Set("original", "retained")
	bounded, err := BoundEventQuery(original)
	require.NoError(t, err)
	require.Len(t, bounded.Items, 3)
	require.Equal(t, int32(3), bounded.Count)
	require.Equal(t, "3", bounded.LastEvaluatedKey["seq_nr"].(*types.AttributeValueMemberN).Value)
	require.Equal(t, original.Items[:3], bounded.Items)
	require.Equal(t, "retained", bounded.ResultMetadata.Get("original"))
	require.Len(t, original.Items, 4)
	require.Empty(t, original.LastEvaluatedKey)
	bounded.Items[0]["payload"].(*types.AttributeValueMemberB).Value[0] = 'y'
	require.Equal(t, byte('x'), original.Items[0]["payload"].(*types.AttributeValueMemberB).Value[0])
	for _, page := range []*dynamodb.QueryOutput{
		{Count: 1, Items: []map[string]types.AttributeValue{item(4, 320022)}},
		{Count: 1, Items: []map[string]types.AttributeValue{item(1, 100)}, LastEvaluatedKey: map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "1"}}},
		{},
	} {
		got, err := BoundEventQuery(page)
		require.NoError(t, err)
		require.Same(t, page, got, "normal pages and their LEK are retained")
	}
	_, err = BoundEventQuery(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{item(1, 1048576)}})
	require.Error(t, err)
	unsupported := item(1, 10)
	unsupported["invalid"] = &types.AttributeValueMemberBOOL{Value: true}
	_, err = BoundEventQuery(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{unsupported}})
	require.Error(t, err)
	missingKey := item(1, 600000)
	delete(missingKey, "seq_nr")
	_, err = BoundEventQuery(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{missingKey, item(2, 600000)}})
	require.Error(t, err)
}
