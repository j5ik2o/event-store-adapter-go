package dynamodb

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/stretchr/testify/require"
)

func readEventFixture(n string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "seq_nr": &types.AttributeValueMemberN{Value: n},
		"occurred_at": &types.AttributeValueMemberN{Value: "123456789"}, "manifest": &types.AttributeValueMemberS{Value: "event/任意"},
		"payload": &types.AttributeValueMemberB{Value: []byte("event bytes")},
	}
}

func readHeadFixture(n string) map[string]types.AttributeValue {
	metadata := readEventFixture(n)
	delete(metadata, "aid")
	return map[string]types.AttributeValue{
		"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "type_name": &types.AttributeValueMemberS{Value: "Order"},
		"seq_nr": &types.AttributeValueMemberN{Value: n}, "events": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: metadata}}},
	}
}

func readSnapshotFixture(n string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "skey": &types.AttributeValueMemberN{Value: "0"},
		"seq_nr": &types.AttributeValueMemberN{Value: n}, "last_updated_at": &types.AttributeValueMemberN{Value: "123"},
		"manifest": &types.AttributeValueMemberS{Value: "snapshot/任意"}, "payload": &types.AttributeValueMemberB{Value: []byte("snapshot bytes")},
	}
}

func readID(t *testing.T) eventstore.AggregateID {
	t.Helper()
	id, err := eventstore.NewAggregateID("Order", "9")
	require.NoError(t, err)
	return id
}

func TestDynamoDBReadItemsInteger(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int64
	}{
		{"0", 0}, {"1.0", 1}, {"1e2", 100}, {"-0.1E1", -1},
		{"9223372036854775807", math.MaxInt64}, {"-9223372036854775808", math.MinInt64},
	} {
		n, err := readInteger(map[string]types.AttributeValue{"n": &types.AttributeValueMemberN{Value: tc.text}}, "n")
		require.NoError(t, err, tc.text)
		require.Equal(t, tc.want, n)
	}
	for _, text := range []string{"", "1.1", "1e-1", "1/1", "0x10", "NaN", "9223372036854775808", "-9223372036854775809"} {
		_, err := readInteger(map[string]types.AttributeValue{"n": &types.AttributeValueMemberN{Value: text}}, "n")
		require.Error(t, err, text)
	}
}

func TestDynamoDBReadItemsRequiredAttributes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func() map[string]types.AttributeValue
		read    func(map[string]types.AttributeValue) error
	}{
		{"event", func() map[string]types.AttributeValue { return readEventFixture("1") }, func(item map[string]types.AttributeValue) error {
			if err := readAid(item, "Order-9"); err != nil {
				return err
			}
			_, err := readEventMetadata(readID(t), item)
			return err
		}},
		{"head", func() map[string]types.AttributeValue { return readHeadFixture("1") }, func(item map[string]types.AttributeValue) error {
			_, err := readHead(readID(t), "Order-9", item)
			return err
		}},
		{"snapshot", func() map[string]types.AttributeValue { return readSnapshotFixture("1") }, func(item map[string]types.AttributeValue) error {
			_, err := readCurrentSnapshot("Order-9", item)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.read(tc.fixture()))
			for attribute := range tc.fixture() {
				t.Run(attribute, func(t *testing.T) {
					item := tc.fixture()
					delete(item, attribute)
					require.Error(t, tc.read(item), "missing")
					item = tc.fixture()
					item[attribute] = &types.AttributeValueMemberBOOL{Value: true}
					require.Error(t, tc.read(item), "wrong type")
				})
			}
		})
	}
	for attribute := range readEventFixture("1") {
		if attribute == "aid" {
			continue
		}
		item := readHeadFixture("1")
		metadata := item["events"].(*types.AttributeValueMemberL).Value[0].(*types.AttributeValueMemberM).Value
		delete(metadata, attribute)
		_, err := readHead(readID(t), "Order-9", item)
		require.Error(t, err, attribute)
	}
}

func TestDynamoDBReadItemsRangesAndHeadStructure(t *testing.T) {
	id := readID(t)
	for _, number := range []string{"0", "-1", "9007199254740992", "1.5"} {
		_, err := readEventMetadata(id, readEventFixture(number))
		require.Error(t, err)
		_, err = readHead(id, "Order-9", readHeadFixture(number))
		require.Error(t, err)
	}
	for _, n := range []string{"0", "9007199254740991"} {
		s, err := readCurrentSnapshot("Order-9", readSnapshotFixture(n))
		require.NoError(t, err)
		require.Equal(t, n, strconv.FormatInt(int64(s.SeqNr()), 10))
	}
	for _, n := range []string{"-1", "9007199254740992", "1.5"} {
		_, err := readCurrentSnapshot("Order-9", readSnapshotFixture(n))
		require.Error(t, err)
	}
	for _, n := range []int64{math.MinInt64, math.MaxInt64} {
		item := readEventFixture("9007199254740991")
		item["occurred_at"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
		e, err := readEventMetadata(id, item)
		require.NoError(t, err)
		require.Equal(t, n, e.OccurredAt().UnixNano())
		require.Equal(t, "event/任意", e.Manifest())
		require.Equal(t, []byte("event bytes"), e.Payload())
	}
	for _, change := range []func(map[string]types.AttributeValue){
		func(h map[string]types.AttributeValue) { h["aid"] = &types.AttributeValueMemberS{Value: "Order-other"} },
		func(h map[string]types.AttributeValue) { h["type_name"] = &types.AttributeValueMemberS{Value: "Other"} },
		func(h map[string]types.AttributeValue) { h["seq_nr"] = &types.AttributeValueMemberN{Value: "2"} },
		func(h map[string]types.AttributeValue) { h["events"] = &types.AttributeValueMemberL{} },
		func(h map[string]types.AttributeValue) {
			h["events"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "event"}}}
		},
		func(h map[string]types.AttributeValue) {
			l := h["events"].(*types.AttributeValueMemberL)
			l.Value = append(l.Value, l.Value[0])
		},
	} {
		h := readHeadFixture("1")
		change(h)
		_, err := readHead(id, "Order-9", h)
		require.Error(t, err)
	}
	for _, tc := range []struct{ name, value string }{
		{"skey", "1"}, {"last_updated_at", "0.1"}, {"last_updated_at", "9223372036854775808"},
		{"last_updated_at", strconv.FormatInt(time.Unix(0, math.MinInt64).UnixMilli()-1, 10)},
		{"last_updated_at", strconv.FormatInt(time.Unix(0, math.MaxInt64).UnixMilli()+1, 10)},
	} {
		item := readSnapshotFixture("1")
		item[tc.name] = &types.AttributeValueMemberN{Value: tc.value}
		_, err := readCurrentSnapshot("Order-9", item)
		require.Error(t, err)
	}
}
