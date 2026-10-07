package dynamodbtest

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type LayoutObservation map[Table]TableObservation
type TableObservation struct {
	Description *types.TableDescription
	TTL         *types.TimeToLiveDescription
}
type HistoryObservation struct {
	Active []int64
	Marked []MarkedHistory
}
type MarkedHistory struct {
	SeqNr int64
	TTL   string
}

func (t *Tables) Describe(ctx context.Context) (LayoutObservation, error) {
	layout := LayoutObservation{}
	for table, name := range t.names {
		out, err := t.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err != nil {
			return nil, err
		}
		ttl, err := t.client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
		if err != nil {
			return nil, err
		}
		layout[table] = TableObservation{Description: out.Table, TTL: ttl.TimeToLiveDescription}
	}
	return layout, nil
}

func (t *Tables) GetItem(ctx context.Context, table Table, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	name, err := t.TableName(table)
	if err != nil {
		return nil, err
	}
	out, err := t.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(name), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	return out.Item, nil
}

func (t *Tables) QueryAll(ctx context.Context, input dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	for {
		out, err := t.client.Query(ctx, &input)
		if err != nil {
			return nil, err
		}
		items = append(items, out.Items...)
		if len(out.LastEvaluatedKey) == 0 {
			return items, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (t *Tables) History(ctx context.Context, aid string) (HistoryObservation, error) {
	name, err := t.TableName("snapshot")
	if err != nil {
		return HistoryObservation{}, err
	}
	items, err := t.QueryAll(ctx, dynamodb.QueryInput{TableName: aws.String(name), ConsistentRead: aws.Bool(true), KeyConditionExpression: aws.String("aid = :aid AND skey > :zero"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: aid}, ":zero": &types.AttributeValueMemberN{Value: "0"}}})
	if err != nil {
		return HistoryObservation{}, err
	}
	result := HistoryObservation{}
	for _, item := range items {
		key, ok := item["skey"].(*types.AttributeValueMemberN)
		if !ok {
			return HistoryObservation{}, fmt.Errorf("history skey is not N")
		}
		seq, err := strconv.ParseInt(key.Value, 10, 64)
		if err != nil {
			return HistoryObservation{}, fmt.Errorf("history skey: %w", err)
		}
		if ttl, exists := item["ttl"]; exists {
			number, ok := ttl.(*types.AttributeValueMemberN)
			if !ok {
				return HistoryObservation{}, fmt.Errorf("history ttl is not N")
			}
			result.Marked = append(result.Marked, MarkedHistory{SeqNr: seq, TTL: number.Value})
		} else {
			result.Active = append(result.Active, seq)
		}
	}
	return result, nil
}
