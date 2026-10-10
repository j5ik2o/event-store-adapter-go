package dynamodbtest

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestItemObservationAndSeedAttributes(t *testing.T) {
	binary := map[string]any{"events[0].payload": map[string]any{"number": json.Number("9007199254740993")}}
	nested := map[string]any{"events[0]": map[string]any{"seq_nr": "N", "payload": "B"}}
	events, err := seedAttribute("L", "events", []any{map[string]any{"seq_nr": "9007199254740993"}}, binary, nested)
	require.NoError(t, err)
	item := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: "9007199254740993"}, "events": events}
	observed, err := ItemObservation("head", item)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"aid": "S", "seq_nr": "N", "events": "L"}, observed["attributes"])
	require.Equal(t, nested, observed["nested_attributes"])
	require.Equal(t, binary, observed["binary_json"])
	require.Equal(t, []any{map[string]any{"seq_nr": "9007199254740993"}}, observed["values"].(map[string]any)["events"])
	for _, tc := range []struct {
		kind, path string
		value      any
	}{
		{"S", "aid", 1}, {"N", "seq_nr", 1}, {"B", "missing", nil}, {"L", "events", "wrong"}, {"M", "missing", map[string]any{}}, {"BOOL", "invalid", true},
	} {
		_, err := seedAttribute(tc.kind, tc.path, tc.value, binary, nested)
		require.Error(t, err)
	}
	for _, bad := range []types.AttributeValue{&types.AttributeValueMemberN{Value: "1.5"}, &types.AttributeValueMemberBOOL{Value: true}, &types.AttributeValueMemberB{Value: []byte("invalid")}} {
		_, err := ItemObservation("head", map[string]types.AttributeValue{"invalid": bad})
		require.Error(t, err)
	}
}
