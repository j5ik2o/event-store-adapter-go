package dynamodbtest

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

// PutConfigurationItems installs explicit seed.items or fault install_items using
// the tables' observation client. Expectations are never an input to this helper.
func (t *Tables) PutConfigurationItems(ctx context.Context, fixtures []map[string]any) error {
	for _, fixture := range fixtures {
		table, ok := fixture["table"].(string)
		if !ok {
			return fmt.Errorf("configuration fixture table is not a string")
		}
		name, err := t.TableName(Table(table))
		if err != nil {
			return err
		}
		attributes, aok := fixture["attributes"].(map[string]any)
		values, vok := fixture["values"].(map[string]any)
		if !aok || !vok {
			return fmt.Errorf("configuration fixture needs attributes and values")
		}
		item := make(map[string]types.AttributeValue, len(attributes))
		for attribute, kind := range attributes {
			value, ok := values[attribute].(string)
			if !ok {
				return fmt.Errorf("configuration fixture %s has no string value", attribute)
			}
			switch kind {
			case "S":
				item[attribute] = &types.AttributeValueMemberS{Value: value}
			case "N":
				item[attribute] = &types.AttributeValueMemberN{Value: value}
			default:
				return fmt.Errorf("unsupported configuration fixture attribute type %v", kind)
			}
		}
		if _, err := t.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
			return err
		}
	}
	return nil
}

type configurationInputKey struct{}

// ConfigurationAPIOption connects operation-0 Faults to configuration SDK calls.
// Initialize carries the real parameters; Finalize runs after the existing
// Serialize recorder, so even replace-request failures retain their inputs.
func (t *Tables) ConfigurationAPIOption(injection conformance.Injection) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("configuration-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			return next.HandleInitialize(context.WithValue(ctx, configurationInputKey{}, in.Parameters), in)
		}), middleware.Before); err != nil {
			return err
		}
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("configuration-fault", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			input := ctx.Value(configurationInputKey{})
			phase := t.configurationPhase(input)
			for _, fault := range injection.Faults {
				if phase == "" || fault.Spec.Phase != phase || !configurationFaultSupported(fault.Spec) || !fault.TryApply() {
					continue
				}
				if fault.Spec.Injection == "replace-request" {
					if raw, present := fault.Spec.Details["install_items"]; present {
						list, ok := raw.([]any)
						if !ok {
							return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("install_items is not an array")
						}
						fixtures := make([]map[string]any, len(list))
						for i, entry := range list {
							fixture, ok := entry.(map[string]any)
							if !ok {
								return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("install_items[%d] is not an object", i)
							}
							fixtures[i] = fixture
						}
						if err := t.PutConfigurationItems(ctx, fixtures); err != nil {
							return middleware.FinalizeOutput{}, middleware.Metadata{}, err
						}
					}
					return middleware.FinalizeOutput{}, middleware.Metadata{}, t.configurationSDKError(input, fault.Spec.Details)
				}
				out, metadata, err := next.HandleFinalize(ctx, in)
				if err != nil {
					return out, metadata, err
				}
				actual, ok := out.Result.(*dynamodb.BatchGetItemOutput)
				if !ok {
					return out, metadata, fmt.Errorf("configuration response is %T", out.Result)
				}
				out.Result, err = t.partitionConfigurationResponse(input.(*dynamodb.BatchGetItemInput), actual, fault.Spec.Details)
				return out, metadata, err
			}
			return next.HandleFinalize(ctx, in)
		}), middleware.Before)
	}
}

func configurationFaultSupported(spec conformance.FaultSpec) bool {
	return spec.Kind == "sdk-error" && spec.Injection == "replace-request" ||
		spec.Phase == "configuration-read" && spec.Kind == "sdk-response" && spec.Injection == "replace-response"
}

func (t *Tables) configurationPhase(input any) string {
	switch input := input.(type) {
	case *dynamodb.BatchGetItemInput:
		count := 0
		for table, request := range input.RequestItems {
			for _, key := range request.Keys {
				if _, err := t.configurationKeyName(table, key); err != nil {
					return ""
				}
				count++
			}
		}
		if count > 0 {
			return "configuration-read"
		}
	case *dynamodb.TransactWriteItemsInput:
		if len(input.TransactItems) == 0 {
			return ""
		}
		for _, action := range input.TransactItems {
			if action.Put == nil {
				return ""
			}
			if _, err := t.configurationKeyName(aws.ToString(action.Put.TableName), action.Put.Item); err != nil {
				return ""
			}
		}
		return "configuration-create"
	}
	return ""
}

func (t *Tables) configurationKeyName(tableName string, item map[string]types.AttributeValue) (string, error) {
	aid, ok := item["aid"].(*types.AttributeValueMemberS)
	if !ok || aid.Value != "__config__" {
		return "", fmt.Errorf("not a configuration key")
	}
	for table, name := range t.names {
		if name != tableName {
			continue
		}
		sortKey := ""
		switch table {
		case "journal":
			sortKey = "seq_nr"
		case "snapshot":
			sortKey = "skey"
		}
		if sortKey == "" {
			return string(table) + ":__config__", nil
		}
		value, ok := item[sortKey].(*types.AttributeValueMemberN)
		if !ok || value.Value != "0" {
			return "", fmt.Errorf("not a configuration sort key")
		}
		return string(table) + ":__config__:0", nil
	}
	return "", fmt.Errorf("not a configured table")
}

// partitionConfigurationResponse removes deferred items from the real Responses
// and copies only their real request keys into UnprocessedKeys. Existing real
// unprocessed keys and all other real response fields are retained.
func (t *Tables) partitionConfigurationResponse(input *dynamodb.BatchGetItemInput, actual *dynamodb.BatchGetItemOutput, details map[string]any) (*dynamodb.BatchGetItemOutput, error) {
	out := cloneValue(reflect.ValueOf(actual)).Interface().(*dynamodb.BatchGetItemOutput)
	if out.UnprocessedKeys == nil {
		out.UnprocessedKeys = make(map[string]types.KeysAndAttributes)
	}
	list, ok := details["unprocessed_keys"].([]any)
	if !ok {
		return nil, fmt.Errorf("unprocessed_keys is not an array")
	}
	for _, raw := range list {
		wanted, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("unprocessed key is not a string")
		}
		found := false
		for table, request := range input.RequestItems {
			for _, key := range request.Keys {
				name, err := t.configurationKeyName(table, key)
				if err != nil {
					return nil, err
				}
				if name != wanted {
					continue
				}
				found = true
				pending, exists := out.UnprocessedKeys[table]
				if !exists {
					pending = request
					pending.Keys = nil
				}
				duplicate := false
				for _, existing := range pending.Keys {
					duplicate = duplicate || reflect.DeepEqual(existing, key)
				}
				if !duplicate {
					pending.Keys = append(pending.Keys, cloneValue(reflect.ValueOf(key)).Interface().(map[string]types.AttributeValue))
				}
				out.UnprocessedKeys[table] = pending
				var processed []map[string]types.AttributeValue
				for _, item := range out.Responses[table] {
					itemName, err := t.configurationKeyName(table, item)
					if err != nil {
						return nil, err
					}
					if itemName != wanted {
						processed = append(processed, item)
					}
				}
				out.Responses[table] = processed
			}
		}
		if !found {
			return nil, fmt.Errorf("unprocessed key %q was not requested", wanted)
		}
	}
	return out, nil
}

func (t *Tables) configurationSDKError(input any, details map[string]any) error {
	switch details["code"] {
	case "ConditionalCheckFailedException":
		return &types.ConditionalCheckFailedException{Message: aws.String("injected configuration condition failure")}
	case "ProvisionedThroughputExceededException":
		return &types.ProvisionedThroughputExceededException{Message: aws.String("injected configuration throughput failure")}
	case "TransactionCanceledException":
		tx, ok := input.(*dynamodb.TransactWriteItemsInput)
		if !ok {
			return fmt.Errorf("transaction cancellation requires TransactWriteItems")
		}
		list, ok := details["cancellation_reasons"].([]any)
		if !ok {
			return fmt.Errorf("cancellation_reasons is not an array")
		}
		reasons := make([]types.CancellationReason, len(tx.TransactItems))
		for i := range reasons {
			reasons[i].Code = aws.String("None")
		}
		for _, entry := range list {
			reason, ok := entry.(map[string]any)
			if !ok {
				return fmt.Errorf("cancellation reason is not an object")
			}
			code, ok := reason["code"].(string)
			if !ok {
				return fmt.Errorf("cancellation reason code is not a string")
			}
			found := false
			for i, action := range tx.TransactItems {
				if action.Put == nil {
					continue
				}
				for table, name := range t.names {
					if name == aws.ToString(action.Put.TableName) && reason["target"] == "configuration:"+string(table) {
						reasons[i].Code = aws.String(code)
						found = true
					}
				}
			}
			if !found {
				return fmt.Errorf("cancellation target %v was not requested", reason["target"])
			}
		}
		return &types.TransactionCanceledException{Message: aws.String("injected configuration cancellation"), CancellationReasons: reasons}
	default:
		return fmt.Errorf("unsupported configuration SDK error %v", details["code"])
	}
}
