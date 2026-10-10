package dynamodbtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

type retentionInputKey struct{}
type historyResponsePlan struct {
	pages  [][]int64
	page   int
	start  map[string]types.AttributeValue
	method string
}

// RetentionAPIOption targets only this fixture's history requests. A page plan
// counts once when installed; its continuation pages do not count as new faults.
// observePartial records the independent SDK call that processes a delete subset.
func (t *Tables) RetentionAPIOption(injection conformance.Injection, observePartial func(*dynamodb.BatchWriteItemInput, *dynamodb.BatchWriteItemOutput, error)) func(*middleware.Stack) error {
	var mu sync.Mutex
	plans := make(map[int]*historyResponsePlan)
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("retention-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			return next.HandleInitialize(context.WithValue(ctx, retentionInputKey{}, in.Parameters), in)
		}), middleware.Before); err != nil {
			return err
		}
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("retention-fault", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			input := ctx.Value(retentionInputKey{})
			phase := t.retentionPhase(input)
			operation, tagged := ctx.Value(operationKey{}).(int)
			if phase == "" || !tagged {
				return next.HandleFinalize(ctx, in)
			}
			mu.Lock()
			defer mu.Unlock()
			var out middleware.FinalizeOutput
			var metadata middleware.Metadata
			called := false
			call := func() error {
				var err error
				out, metadata, err = next.HandleFinalize(ctx, in)
				called = true
				return err
			}
			if plan := plans[operation]; phase == "retention-query" && plan != nil {
				if plan.method == "replace-response" {
					if err := call(); err != nil {
						return out, metadata, err
					}
				}
				result, err := plan.response(input.(*dynamodb.QueryInput), out.Result)
				if plan.page == len(plan.pages) {
					delete(plans, operation)
				}
				out.Result = result
				return out, metadata, err
			}
			for _, fault := range injection.Faults {
				if fault.Spec.Operation != operation || fault.Spec.Phase != phase || !retentionFaultSupported(fault.Spec) || !fault.CanApply() {
					continue
				}
				if fault.Spec.Injection == "replace-request" && called {
					continue
				}
				if fault.Spec.Injection == "replace-response" && !called {
					if err := call(); err != nil {
						return out, metadata, err
					}
				}
				var replacement error
				applied, err := fault.TryApplyWith(func() error {
					if fault.Spec.Kind == "storage-error" || fault.Spec.Kind == "sdk-error" {
						var err error
						replacement, err = retentionSDKError(fault.Spec)
						return err
					}
					if phase == "retention-query" {
						pages, err := retentionHistoryPages(fault.Spec.Details)
						if err != nil {
							return err
						}
						plan := &historyResponsePlan{pages: pages, method: fault.Spec.Injection}
						result, err := plan.response(input.(*dynamodb.QueryInput), out.Result)
						if err != nil {
							return err
						}
						out.Result = result
						if plan.page < len(pages) {
							plans[operation] = plan
						}
						return nil
					}
					result, err := t.partitionHistoryDelete(ctx, input.(*dynamodb.BatchWriteItemInput), fault.Spec.Details, observePartial)
					if err == nil {
						out.Result = result
					}
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

func retentionFaultSupported(spec conformance.FaultSpec) bool {
	if spec.Injection != "replace-request" && spec.Injection != "replace-response" {
		return false
	}
	switch spec.Kind {
	case "storage-error", "sdk-error":
		return true
	case "sdk-response":
		return spec.Phase == "retention-query" || spec.Phase == "retention-delete" && spec.Injection == "replace-request"
	}
	return false
}

func (t *Tables) retentionPhase(input any) string {
	switch input := input.(type) {
	case *dynamodb.QueryInput:
		if aws.ToString(input.TableName) == t.names["snapshot"] && aws.ToString(input.IndexName) == t.index {
			aid, ok := input.ExpressionAttributeValues[":aid"].(*types.AttributeValueMemberS)
			if ok && aid.Value != "__config__" {
				return "retention-query"
			}
		}
	case *dynamodb.BatchWriteItemInput:
		if len(input.RequestItems) != 1 {
			return ""
		}
		writes := input.RequestItems[t.names["snapshot"]]
		if len(writes) == 0 {
			return ""
		}
		for _, write := range writes {
			if write.DeleteRequest == nil || !historyRequestKey(write.DeleteRequest.Key) {
				return ""
			}
		}
		return "retention-delete"
	case *dynamodb.UpdateItemInput:
		if aws.ToString(input.TableName) == t.names["snapshot"] && historyRequestKey(input.Key) && aws.ToString(input.ConditionExpression) == "attribute_exists(active_history_seq_nr)" {
			return "retention-mark"
		}
	}
	return ""
}

func historyRequestKey(key map[string]types.AttributeValue) bool {
	aid, ok := key["aid"].(*types.AttributeValueMemberS)
	if !ok || aid.Value == "__config__" {
		return false
	}
	number, ok := key["skey"].(*types.AttributeValueMemberN)
	if !ok {
		return false
	}
	n, err := strconv.ParseInt(number.Value, 10, 64)
	return err == nil && n > 0
}

func retentionSDKError(spec conformance.FaultSpec) (error, error) {
	if spec.Kind == "storage-error" {
		message, ok := spec.Details["message"].(string)
		if !ok {
			message = "injected retention storage failure"
		}
		return errors.New(message), nil
	}
	switch spec.Details["code"] {
	case "ProvisionedThroughputExceededException":
		return &types.ProvisionedThroughputExceededException{Message: aws.String("injected retention throughput failure")}, nil
	case "ConditionalCheckFailedException":
		return &types.ConditionalCheckFailedException{Message: aws.String("injected retention condition failure")}, nil
	default:
		return nil, fmt.Errorf("unsupported retention SDK error %v", spec.Details["code"])
	}
}

func retentionHistoryPages(details map[string]any) ([][]int64, error) {
	list, ok := details["history_pages"].([]any)
	if !ok || len(list) == 0 {
		return nil, errors.New("history_pages must contain at least one page")
	}
	pages := make([][]int64, len(list))
	for i, raw := range list {
		page, ok := raw.([]any)
		if !ok {
			return nil, errors.New("history page is not an array")
		}
		for _, value := range page {
			n, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
			if err != nil || n < 1 {
				return nil, errors.New("history sequence is not a positive integer")
			}
			pages[i] = append(pages[i], n)
		}
	}
	return pages, nil
}

func (p *historyResponsePlan) response(input *dynamodb.QueryInput, original any) (*dynamodb.QueryOutput, error) {
	if !reflect.DeepEqual(input.ExclusiveStartKey, p.start) {
		return nil, errors.New("history plan continuation does not match the request")
	}
	aid, ok := input.ExpressionAttributeValues[":aid"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, errors.New("history query has no aid binding")
	}
	out := &dynamodb.QueryOutput{}
	if actual, ok := original.(*dynamodb.QueryOutput); ok {
		*out = *actual
	}
	out.Items, out.LastEvaluatedKey = nil, nil
	for _, n := range p.pages[p.page] {
		number := strconv.FormatInt(n, 10)
		out.Items = append(out.Items, map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: aid.Value}, "skey": &types.AttributeValueMemberN{Value: number}, "active_history_seq_nr": &types.AttributeValueMemberN{Value: number}})
	}
	out.Count, out.ScannedCount = int32(len(out.Items)), int32(len(out.Items))
	p.page++
	if p.page < len(p.pages) {
		if len(out.Items) > 0 {
			p.start = out.Items[len(out.Items)-1]
		} else if p.start == nil {
			p.start = map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: aid.Value}, "skey": &types.AttributeValueMemberN{Value: "1"}, "active_history_seq_nr": &types.AttributeValueMemberN{Value: "1"}}
		}
		out.LastEvaluatedKey = p.start
	}
	return out, nil
}

// Only the processed subset reaches the independent client. Deferred requests
// remain physically present, and real UnprocessedItems from that SDK are retained.
func (t *Tables) partitionHistoryDelete(ctx context.Context, input *dynamodb.BatchWriteItemInput, details map[string]any, observe func(*dynamodb.BatchWriteItemInput, *dynamodb.BatchWriteItemOutput, error)) (*dynamodb.BatchWriteItemOutput, error) {
	n, err := strconv.Atoi(fmt.Sprint(details["unprocessed_first_n"]))
	if err != nil || n < 0 {
		return nil, errors.New("unprocessed_first_n is not a non-negative integer")
	}
	writes := input.RequestItems[t.names["snapshot"]]
	n = min(n, len(writes))
	out := &dynamodb.BatchWriteItemOutput{UnprocessedItems: make(map[string][]types.WriteRequest)}
	if n < len(writes) {
		processed := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{t.names["snapshot"]: writes[n:]}}
		out, err = t.client.BatchWriteItem(ctx, processed)
		if observe != nil {
			observe(processed, out, err)
		}
		if err != nil {
			return nil, err
		}
		out = cloneValue(reflect.ValueOf(out)).Interface().(*dynamodb.BatchWriteItemOutput)
		if out.UnprocessedItems == nil {
			out.UnprocessedItems = make(map[string][]types.WriteRequest)
		}
	}
	if n > 0 {
		out.UnprocessedItems[t.names["snapshot"]] = append(out.UnprocessedItems[t.names["snapshot"]], writes[:n]...)
	}
	return out, nil
}
