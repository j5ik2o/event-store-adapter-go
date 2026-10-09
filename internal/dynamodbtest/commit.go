package dynamodbtest

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

type commitInputKey struct{}

// CommitAPIOption applies operation-scoped commit faults using the existing
// accounting and request recorder. Request replacements never call the SDK;
// response replacements run only after a successful real SDK call.
func (t *Tables) CommitAPIOption(injection conformance.Injection) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("commit-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			return next.HandleInitialize(context.WithValue(ctx, commitInputKey{}, in.Parameters), in)
		}), middleware.Before); err != nil {
			return err
		}
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("commit-fault", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			input, ok := ctx.Value(commitInputKey{}).(*dynamodb.TransactWriteItemsInput)
			operation, tagged := ctx.Value(operationKey{}).(int)
			if !ok || !tagged || t.commitTargets(input) == nil {
				return next.HandleFinalize(ctx, in)
			}
			var out middleware.FinalizeOutput
			var metadata middleware.Metadata
			called := false
			for _, fault := range injection.Faults {
				if fault.Spec.Operation != operation || !commitFaultSupported(fault.Spec) || !fault.CanApply() {
					continue
				}
				if fault.Spec.Injection == "replace-request" && called {
					continue
				}
				if fault.Spec.Injection == "replace-response" && !called {
					var err error
					out, metadata, err = next.HandleFinalize(ctx, in)
					if err != nil {
						return out, metadata, err
					}
					called = true
				}
				var replacement error
				applied, err := fault.TryApplyWith(func() error {
					if fault.Spec.Kind == "storage-error" {
						message, ok := fault.Spec.Details["message"].(string)
						if !ok {
							message = "injected commit storage failure"
						}
						replacement = errors.New(message)
						return nil
					}
					var err error
					replacement, err = t.commitSDKError(input, fault.Spec.Details)
					return err
				})
				if err != nil {
					return out, metadata, err
				}
				if applied {
					return out, metadata, replacement
				}
			}
			if called {
				return out, metadata, nil
			}
			return next.HandleFinalize(ctx, in)
		}), middleware.Before)
	}
}

func commitFaultSupported(spec conformance.FaultSpec) bool {
	if spec.Phase != "commit" || spec.Kind != "sdk-error" && spec.Kind != "storage-error" {
		return false
	}
	if spec.Injection == "replace-request" {
		return true
	}
	return spec.Injection == "replace-response" && spec.Details["code"] != "TransactionCanceledException"
}

// commitTargets names the real actions without imposing an action order.
// Configuration opening has reserved keys and belongs to its existing middleware.
func (t *Tables) commitTargets(input *dynamodb.TransactWriteItemsInput) []string {
	if len(input.TransactItems) != 2 {
		return nil
	}
	targets := make([]string, len(input.TransactItems))
	for i, action := range input.TransactItems {
		var table string
		var item map[string]types.AttributeValue
		switch {
		case action.Put != nil:
			table, item = aws.ToString(action.Put.TableName), action.Put.Item
		case action.Update != nil:
			table, item = aws.ToString(action.Update.TableName), action.Update.Key
		default:
			return nil
		}
		aid, ok := item["aid"].(*types.AttributeValueMemberS)
		if !ok || aid.Value == "__config__" {
			return nil
		}
		switch table {
		case t.names["journal"]:
			if action.Put == nil {
				return nil
			}
			targets[i] = "journal"
		case t.names["head"]:
			targets[i] = "head"
		default:
			return nil
		}
	}
	if targets[0] == targets[1] {
		return nil
	}
	return targets
}

func (t *Tables) commitSDKError(input *dynamodb.TransactWriteItemsInput, details map[string]any) (error, error) {
	switch details["code"] {
	case "ProvisionedThroughputExceededException":
		return &types.ProvisionedThroughputExceededException{Message: aws.String("injected commit throughput failure")}, nil
	case "TransactionCanceledException":
		targets := t.commitTargets(input)
		if targets == nil {
			return nil, fmt.Errorf("cancellation requires an event-only transaction")
		}
		list, ok := details["cancellation_reasons"].([]any)
		if !ok {
			return nil, fmt.Errorf("cancellation_reasons is not an array")
		}
		reasons := make([]types.CancellationReason, len(targets))
		for i := range reasons {
			reasons[i].Code = aws.String("None")
		}
		for _, raw := range list {
			reason, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("cancellation reason is not an object")
			}
			code, ok := reason["code"].(string)
			if !ok {
				return nil, fmt.Errorf("cancellation reason code is not a string")
			}
			found := false
			for i, target := range targets {
				if reason["target"] != target {
					continue
				}
				found = true
				reasons[i].Code = aws.String(code)
				if target == "head" && code == "ConditionalCheckFailed" && reason["old_head_seq_nr"] != nil {
					number := fmt.Sprint(reason["old_head_seq_nr"])
					if _, err := strconv.ParseInt(number, 10, 64); err != nil {
						return nil, fmt.Errorf("old_head_seq_nr: %w", err)
					}
					reasons[i].Item = map[string]types.AttributeValue{"seq_nr": &types.AttributeValueMemberN{Value: number}}
				}
			}
			if !found {
				return nil, fmt.Errorf("cancellation target %v was not requested", reason["target"])
			}
		}
		return &types.TransactionCanceledException{Message: aws.String("injected commit cancellation"), CancellationReasons: reasons}, nil
	default:
		return nil, fmt.Errorf("unsupported commit SDK error %v", details["code"])
	}
}
