package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

func configurationKeys(cfg settings) map[string]types.KeysAndAttributes {
	keys := make(map[string]types.KeysAndAttributes, 3)
	for _, entry := range []struct{ table, sortKey string }{
		{cfg.journalTableName, "seq_nr"}, {cfg.snapshotTableName, "skey"}, {cfg.headTableName, ""},
	} {
		key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}}
		if entry.sortKey != "" {
			key[entry.sortKey] = &types.AttributeValueMemberN{Value: "0"}
		}
		keys[entry.table] = types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{key}, ConsistentRead: aws.Bool(true)}
	}
	return keys
}

func readConfiguration(ctx context.Context, client *awsdynamodb.Client, cfg settings, hooks *testhook.Hooks) (map[string]map[string]types.AttributeValue, error) {
	requested := configurationKeys(cfg)
	pending := requested
	items := make(map[string]map[string]types.AttributeValue, 3)
	delay := 50 * time.Millisecond
	for retries := 0; ; retries++ {
		out, err := client.BatchGetItem(ctx, &awsdynamodb.BatchGetItemInput{RequestItems: pending})
		if err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		for table, responses := range out.Responses {
			for _, item := range responses {
				for _, key := range requested[table].Keys {
					if configurationKeyMatches(item, key) {
						items[table] = item
					}
				}
			}
		}
		pending = make(map[string]types.KeysAndAttributes, len(out.UnprocessedKeys))
		for table, keys := range out.UnprocessedKeys {
			if len(keys.Keys) != 0 {
				keys.ConsistentRead = aws.Bool(true)
				pending[table] = keys
			}
		}
		if len(pending) == 0 {
			return items, nil
		}
		if retries == cfg.configurationReadRetryLimit {
			return nil, &eventstore.StorageError{Cause: fmt.Errorf("configuration read retry limit %d reached with unprocessed keys", cfg.configurationReadRetryLimit)}
		}
		hooks.Sleep(delay)
		delay = min(2*delay, time.Second)
	}
}

func configurationKeyMatches(item, key map[string]types.AttributeValue) bool {
	for name, expected := range key {
		switch expected := expected.(type) {
		case *types.AttributeValueMemberS:
			actual, ok := item[name].(*types.AttributeValueMemberS)
			if !ok || actual.Value != expected.Value {
				return false
			}
		case *types.AttributeValueMemberN:
			actual, ok := item[name].(*types.AttributeValueMemberN)
			if !ok || !configurationNumberEquals(actual.Value, expected.Value) {
				return false
			}
		}
	}
	return true
}

func configurationNumberEquals(actual, expected string) bool {
	a, ok := new(big.Rat).SetString(actual)
	if !ok {
		return false
	}
	b, ok := new(big.Rat).SetString(expected)
	return ok && a.Cmp(b) == 0
}

func matchConfiguration(cfg settings, items map[string]map[string]types.AttributeValue) (string, bool, error) {
	if len(items) == 0 {
		return "", false, nil
	}
	var storeID string
	for i, table := range []string{cfg.journalTableName, cfg.snapshotTableName, cfg.headTableName} {
		item, exists := items[table]
		if !exists {
			return "", false, &eventstore.ConfigurationError{Cause: errors.New("configuration exists in only some tables")}
		}
		id, ok := item["store_id"].(*types.AttributeValueMemberS)
		if !ok {
			return "", false, &eventstore.ConfigurationError{Cause: errors.New("configuration store_id is not S")}
		}
		version, ok := item["layout_version"].(*types.AttributeValueMemberN)
		if !ok || !configurationNumberEquals(version.Value, "1") {
			return "", false, &eventstore.ConfigurationError{Cause: errors.New("configuration layout_version is not the supported N version 1")}
		}
		if i == 0 {
			storeID = id.Value
		} else if id.Value != storeID {
			return "", false, &eventstore.ConfigurationError{Cause: errors.New("configuration store_id differs between tables")}
		}
	}
	return storeID, true, nil
}

func configurationWrite(cfg settings, storeID string) *awsdynamodb.TransactWriteItemsInput {
	keys := configurationKeys(cfg)
	input := &awsdynamodb.TransactWriteItemsInput{}
	for _, table := range []string{cfg.journalTableName, cfg.snapshotTableName, cfg.headTableName} {
		item := keys[table].Keys[0]
		item["store_id"] = &types.AttributeValueMemberS{Value: storeID}
		item["layout_version"] = &types.AttributeValueMemberN{Value: "1"}
		input.TransactItems = append(input.TransactItems, types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String(table), Item: item, ConditionExpression: aws.String("attribute_not_exists(aid)"),
		}})
	}
	return input
}

func configurationCreateRace(err error) bool {
	var condition *types.ConditionalCheckFailedException
	if errors.As(err, &condition) {
		return true
	}
	var canceled *types.TransactionCanceledException
	if errors.As(err, &canceled) {
		for _, reason := range canceled.CancellationReasons {
			switch aws.ToString(reason.Code) {
			case "ConditionalCheckFailed", "TransactionConflict":
				return true
			}
		}
	}
	return false
}
