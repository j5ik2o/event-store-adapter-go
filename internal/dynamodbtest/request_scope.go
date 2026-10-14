package dynamodbtest

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// RequestScope reports, for every recorded request, its API name and the aid of each item it
// reads or writes. An aid that is missing or not a string is reported as nil. A request without
// a readable aid reports an empty list. It never looks at the expected aggregate.
func RequestScope(requests []Request) []map[string]any {
	out := make([]map[string]any, 0, len(requests))
	for _, r := range requests {
		out = append(out, map[string]any{"api": r.API, "aids": requestAids(r.Input)})
	}
	return out
}

func requestAids(input any) []any {
	aids := []any{}
	add := func(item map[string]types.AttributeValue) {
		if s, ok := item["aid"].(*types.AttributeValueMemberS); ok {
			aids = append(aids, s.Value)
			return
		}
		aids = append(aids, nil)
	}
	switch in := input.(type) {
	case *dynamodb.GetItemInput:
		add(in.Key)
	case *dynamodb.UpdateItemInput:
		add(in.Key)
	case *dynamodb.BatchGetItemInput:
		for _, keys := range in.RequestItems {
			for _, key := range keys.Keys {
				add(key)
			}
		}
	case *dynamodb.TransactWriteItemsInput:
		for _, item := range in.TransactItems {
			if item.Put != nil {
				add(item.Put.Item)
			}
			if item.Update != nil {
				add(item.Update.Key)
			}
			if item.Delete != nil {
				add(item.Delete.Key)
			}
			if item.ConditionCheck != nil {
				add(item.ConditionCheck.Key)
			}
		}
	case *dynamodb.BatchWriteItemInput:
		for _, writes := range in.RequestItems {
			for _, w := range writes {
				if w.PutRequest != nil {
					add(w.PutRequest.Item)
				}
				if w.DeleteRequest != nil {
					add(w.DeleteRequest.Key)
				}
			}
		}
	case *dynamodb.QueryInput:
		if aws.ToString(in.KeyConditionExpression) == "" {
			return aids
		}
		for _, clause := range expressionAnd.Split(aws.ToString(in.KeyConditionExpression), -1) {
			m := comparison.FindStringSubmatch(strings.Trim(strings.TrimSpace(clause), "()"))
			if m == nil || m[2] != "=" || resolveName(m[1], in.ExpressionAttributeNames) != "aid" {
				continue
			}
			if s, ok := in.ExpressionAttributeValues[m[3]].(*types.AttributeValueMemberS); ok {
				aids = append(aids, s.Value)
			} else {
				aids = append(aids, nil)
			}
		}
	}
	return aids
}
