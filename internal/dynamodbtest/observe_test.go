package dynamodbtest

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestPhysicalObservations(t *testing.T) {
	// Given actual typed SDK writes, When read through the observer, Then keys, types and values survive.
	e := startEnvironment(t)
	tables, names := createTables(t, e, false)
	item := journalItem(`{"number":3}`)
	item["occurred_at"] = &types.AttributeValueMemberN{Value: "9007199254740993"}
	item["manifest"] = &types.AttributeValueMemberS{Value: ""}
	_, err := e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["journal"]), Item: item})
	require.NoError(t, err)
	key := journalItem("")
	delete(key, "payload")
	actual, err := tables.GetItem(context.Background(), Table("journal"), key)
	require.NoError(t, err)
	require.Equal(t, item, actual)
	event := map[string]types.AttributeValue{"seq_nr": item["seq_nr"], "occurred_at": item["occurred_at"], "manifest": item["manifest"], "payload": item["payload"]}
	head := map[string]types.AttributeValue{"aid": key["aid"], "seq_nr": key["seq_nr"], "type_name": &types.AttributeValueMemberS{Value: "Order"}, "events": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: event}}}}
	_, err = e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["head"]), Item: head})
	require.NoError(t, err)
	actual, err = tables.GetItem(context.Background(), Table("head"), map[string]types.AttributeValue{"aid": key["aid"]})
	require.NoError(t, err)
	require.Equal(t, head, actual)
	for _, row := range []struct{ skey, seq, active, ttl string }{{"0", "3", "", ""}, {"3", "3", "3", ""}, {"2", "2", "", "4102444860"}} {
		snapshot := map[string]types.AttributeValue{"aid": key["aid"], "skey": &types.AttributeValueMemberN{Value: row.skey}, "seq_nr": &types.AttributeValueMemberN{Value: row.seq}, "manifest": &types.AttributeValueMemberS{Value: ""}, "payload": &types.AttributeValueMemberB{Value: []byte(`{"total":3}`)}, "last_updated_at": &types.AttributeValueMemberN{Value: "123"}}
		if row.active != "" {
			snapshot["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: row.active}
		}
		if row.ttl != "" {
			snapshot["ttl"] = &types.AttributeValueMemberN{Value: row.ttl}
		}
		_, err = e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["snapshot"]), Item: snapshot})
		require.NoError(t, err)
		actual, err = tables.GetItem(context.Background(), Table("snapshot"), map[string]types.AttributeValue{"aid": key["aid"], "skey": snapshot["skey"]})
		require.NoError(t, err)
		require.Equal(t, snapshot, actual)
	}
	for kind, name := range names {
		config := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}, "store_id": &types.AttributeValueMemberS{Value: "store-a"}, "layout_version": &types.AttributeValueMemberN{Value: "1"}}
		configKey := map[string]types.AttributeValue{"aid": config["aid"]}
		if kind == "journal" {
			config["seq_nr"] = &types.AttributeValueMemberN{Value: "0"}
			configKey["seq_nr"] = config["seq_nr"]
		}
		if kind == "snapshot" {
			config["skey"] = &types.AttributeValueMemberN{Value: "0"}
			configKey["skey"] = config["skey"]
		}
		_, err = e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: config})
		require.NoError(t, err)
		actual, err = tables.GetItem(context.Background(), Table(kind), configKey)
		require.NoError(t, err)
		require.Equal(t, config, actual)
	}
}

func TestPhysicalObservationsReadEveryPageWithoutInventingItems(t *testing.T) {
	// Given three real rows and a one-row page limit, When queried, Then all rows and no absent rows return.
	e := startEnvironment(t)
	tables, names := createTables(t, e, true)
	var expected []map[string]types.AttributeValue
	for i := 1; i <= 3; i++ {
		item := journalItem("row")
		item["seq_nr"] = &types.AttributeValueMemberN{Value: strconv.Itoa(i)}
		item["occurred_at"] = &types.AttributeValueMemberN{Value: strconv.Itoa(i)}
		expected = append(expected, item)
		_, err := e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["journal"]), Item: item})
		require.NoError(t, err)
	}
	query := dynamodb.QueryInput{TableName: aws.String(names["journal"]), KeyConditionExpression: aws.String("aid = :aid"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: "Order-1"}}, ConsistentRead: aws.Bool(true), Limit: aws.Int32(1)}
	first, err := e.NewClient().Query(context.Background(), &query)
	require.NoError(t, err)
	require.Len(t, first.Items, 1)
	require.NotEmpty(t, first.LastEvaluatedKey)
	actual, err := tables.QueryAll(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	query.FilterExpression = aws.String("occurred_at >= :minimum")
	query.ExpressionAttributeValues[":minimum"] = &types.AttributeValueMemberN{Value: "2"}
	filtered, err := e.NewClient().Query(context.Background(), &query)
	require.NoError(t, err)
	require.Empty(t, filtered.Items)
	require.NotEmpty(t, filtered.LastEvaluatedKey)
	actual, err = tables.QueryAll(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, expected[1:], actual)
	query.FilterExpression = nil
	delete(query.ExpressionAttributeValues, ":minimum")
	query.ExpressionAttributeValues[":aid"] = &types.AttributeValueMemberS{Value: "absent"}
	actual, err = tables.QueryAll(context.Background(), query)
	require.NoError(t, err)
	require.Empty(t, actual)
	// A base-table history query must retain marked rows that are absent from the sparse GSI.
	var history []map[string]types.AttributeValue
	for _, row := range []struct{ key, active, ttl string }{{"0", "", ""}, {"1", "1", ""}, {"2", "", "4102444860"}, {"3", "3", ""}, {"4", "4", ""}, {"5", "5", ""}} {
		item := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-1"}, "skey": &types.AttributeValueMemberN{Value: row.key}, "seq_nr": &types.AttributeValueMemberN{Value: row.key}}
		item["payload"] = &types.AttributeValueMemberB{Value: bytes.Repeat([]byte("x"), 380*1024)}
		if row.active != "" {
			item["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: row.active}
		}
		if row.ttl != "" {
			item["ttl"] = &types.AttributeValueMemberN{Value: row.ttl}
		}
		_, err := e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["snapshot"]), Item: item})
		require.NoError(t, err)
		if row.key != "0" {
			history = append(history, item)
		}
	}
	query = dynamodb.QueryInput{TableName: aws.String(names["snapshot"]), KeyConditionExpression: aws.String("aid = :aid AND skey > :current"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: "Order-1"}, ":current": &types.AttributeValueMemberN{Value: "0"}}, ConsistentRead: aws.Bool(true), Limit: aws.Int32(1)}
	actual, err = tables.QueryAll(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, history, actual)
	query.Limit = nil
	firstHistory, err := e.NewClient().Query(context.Background(), &query)
	require.NoError(t, err)
	require.NotEmpty(t, firstHistory.LastEvaluatedKey, "history fixture must exceed the real SDK page size")
	for _, aid := range []string{"Order-10", "__config__"} {
		item := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: aid}, "skey": &types.AttributeValueMemberN{Value: "0"}, "seq_nr": &types.AttributeValueMemberN{Value: "99"}}
		if aid == "Order-10" {
			item["skey"] = &types.AttributeValueMemberN{Value: "99"}
			item["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: "99"}
		}
		_, err = e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["snapshot"]), Item: item})
		require.NoError(t, err)
	}
	observed, err := tables.History(context.Background(), "Order-1")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 4, 5}, observed.Active)
	require.Equal(t, []MarkedHistory{{SeqNr: 2, TTL: "4102444860"}}, observed.Marked)
	observed, err = tables.History(context.Background(), "absent")
	require.NoError(t, err)
	require.Empty(t, observed.Active)
	require.Empty(t, observed.Marked)
}
