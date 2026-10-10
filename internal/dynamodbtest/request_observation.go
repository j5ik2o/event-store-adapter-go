package dynamodbtest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

var expressionAnd = regexp.MustCompile(`(?i)\s+AND\s+`)
var comparison = regexp.MustCompile(`^\s*([#\w]+)\s*(>=|=)\s*(:\w+)\s*$`)
var attributeCondition = regexp.MustCompile(`^\s*(attribute_exists|attribute_not_exists)\s*\(\s*([#\w]+)\s*\)\s*$`)
var updateClause = regexp.MustCompile(`(?i)\b(SET|REMOVE)\b`)

func resolveName(name string, names map[string]string) string {
	if strings.HasPrefix(name, "#") {
		return names[name]
	}
	return name
}

func conditionObservation(expression *string, names map[string]string) map[string]any {
	m := attributeCondition.FindStringSubmatch(aws.ToString(expression))
	if m == nil {
		return map[string]any{"unparsed": aws.ToString(expression)}
	}
	return map[string]any{m[1]: resolveName(m[2], names)}
}

func keyConditionObservation(input *dynamodb.QueryInput, step conformance.StepPlan) map[string]any {
	all := []any{}
	for _, clause := range expressionAnd.Split(aws.ToString(input.KeyConditionExpression), -1) {
		m := comparison.FindStringSubmatch(strings.Trim(strings.TrimSpace(clause), "()"))
		if m == nil {
			return map[string]any{"unparsed": clause}
		}
		name := resolveName(m[1], input.ExpressionAttributeNames)
		argument := "unbound"
		value := input.ExpressionAttributeValues[m[3]]
		if a, ok := value.(*types.AttributeValueMemberS); ok && name == "aid" && a.Value == step.AID.TypeName+"-"+step.AID.Value {
			argument = "aggregate_id"
		}
		if n, ok := value.(*types.AttributeValueMemberN); ok && name == "seq_nr" && step.SeqNr != nil && n.Value == step.SeqNr.String() {
			argument = "seq_nr"
		}
		op := "eq"
		if m[2] == ">=" {
			op = "gte"
		}
		all = append(all, map[string]any{"attribute": name, "operator": op, "argument": argument})
	}
	return map[string]any{"all": all}
}

func updateObservation(input *dynamodb.UpdateItemInput) map[string]any {
	expression := aws.ToString(input.UpdateExpression)
	clauses := updateClause.FindAllStringIndex(expression, -1)
	out := map[string]any{}
	for i, location := range clauses {
		end := len(expression)
		if i+1 < len(clauses) {
			end = clauses[i+1][0]
		}
		kind := strings.ToUpper(expression[location[0]:location[1]])
		body := expression[location[1]:end]
		if kind == "REMOVE" {
			remove := []any{}
			for _, name := range strings.Split(body, ",") {
				remove = append(remove, resolveName(strings.TrimSpace(name), input.ExpressionAttributeNames))
			}
			out["remove"] = remove
		} else {
			set := map[string]any{}
			for _, entry := range strings.Split(body, ",") {
				parts := strings.Split(entry, "=")
				if len(parts) != 2 {
					out["unparsed"] = entry
					continue
				}
				name, binding := resolveName(strings.TrimSpace(parts[0]), input.ExpressionAttributeNames), strings.TrimSpace(parts[1])
				if name == "ttl" {
					if _, ok := input.ExpressionAttributeValues[binding].(*types.AttributeValueMemberN); ok {
						set[name] = map[string]any{"value_binding": "expires"}
						continue
					}
				}
				set[name] = map[string]any{"value_binding": binding}
			}
			out["set"] = set
		}
	}
	return out
}

// ObserveRequests resolves real SDK parameters and checks continuation against
// the preceding delivered responses. It does not receive observation expectations.
func (t *Tables) ObserveRequests(ctx context.Context, requests []Request, responses []Response, step conformance.StepPlan, waits []time.Duration) ([]map[string]any, error) {
	out := []map[string]any{}
	if len(requests) != len(responses) {
		return nil, fmt.Errorf("request/response recording count differs: %d/%d", len(requests), len(responses))
	}
	var previousQuery map[string]*dynamodb.QueryOutput = map[string]*dynamodb.QueryOutput{}
	queryCount, queryValid := map[string]int{}, map[string]bool{}
	var previousBatch *dynamodb.BatchGetItemOutput
	var pendingDelete map[string][]types.WriteRequest
	initialSizes := []any{}
	retryDelete, deleteValid := false, true
	var justWritten string
	deleted := map[string]bool{}
	for i, request := range requests {
		if request.API != responses[i].API {
			return nil, fmt.Errorf("request/response sequence does not match at %d", i)
		}
		constraints := map[string]any{}
		phase := t.configurationPhase(request.Input)
		if phase == "" {
			phase = t.retentionPhase(request.Input)
		}
		if phase == "" {
			phase = t.ReadPhase(request.Input)
		}
		if phase == "" {
			if tx, ok := request.Input.(*dynamodb.TransactWriteItemsInput); ok && t.commitTargets(tx) != nil {
				phase = "commit"
			} else {
				phase = "classify-condition-failure-read"
			}
		}
		if strings.HasPrefix(step.Op, "persist") && (phase == "read-events" || phase == "read-snapshot") {
			phase = "classify-condition-failure-read"
		}
		switch input := request.Input.(type) {
		case *dynamodb.BatchGetItemInput:
			keys := []any{}
			consistent := true
			hasHead, hasCurrent := false, false
			for table, attributes := range input.RequestItems {
				consistent = consistent && aws.ToBool(attributes.ConsistentRead)
				for _, key := range attributes.Keys {
					name, err := t.configurationKeyName(table, key)
					if err == nil {
						keys = append(keys, name)
					} else {
						aid, ok := key["aid"].(*types.AttributeValueMemberS)
						matchingAid := ok && aid.Value == step.AID.TypeName+"-"+step.AID.Value
						if table == t.names["head"] && matchingAid {
							hasHead = len(attributes.Keys) == 1
						}
						if n, ok := key["skey"].(*types.AttributeValueMemberN); ok && table == t.names["snapshot"] && n.Value == "0" && matchingAid {
							hasCurrent = len(attributes.Keys) == 1
						}
					}
				}
			}
			constraints["keys"], constraints["consistent_read_all_tables"] = keys, consistent
			constraints["head_and_current_snapshot"] = hasHead && hasCurrent && len(input.RequestItems) == 2
			constraints["only_unprocessed_keys"] = previousBatch != nil && len(previousBatch.UnprocessedKeys) > 0 && reflect.DeepEqual(input.RequestItems, previousBatch.UnprocessedKeys)
			backoff := len(waits) > 0
			for j, wait := range waits {
				backoff = backoff && wait == min(50*time.Millisecond*time.Duration(1<<min(j, 5)), time.Second)
			}
			constraints["exponential_backoff"] = backoff
			previousBatch, _ = responses[i].Result.(*dynamodb.BatchGetItemOutput)
		case *dynamodb.TransactWriteItemsInput:
			putTables := []any{}
			sameID, layout := true, int64(0)
			storeID := ""
			conditions := []any{}
			for _, action := range input.TransactItems {
				if action.Put != nil {
					for table, name := range t.names {
						if name == aws.ToString(action.Put.TableName) {
							putTables = append(putTables, string(table))
						}
					}
					conditions = append(conditions, conditionObservation(action.Put.ConditionExpression, action.Put.ExpressionAttributeNames))
					if id, ok := action.Put.Item["store_id"].(*types.AttributeValueMemberS); ok {
						if storeID == "" {
							storeID = id.Value
						}
						sameID = sameID && id.Value == storeID
					} else {
						sameID = false
					}
					if n, ok := action.Put.Item["layout_version"].(*types.AttributeValueMemberN); ok {
						if layout == 0 {
							fmt.Sscan(n.Value, &layout)
						}
						sameID = sameID && n.Value == fmt.Sprint(layout)
					}
					if aws.ToString(action.Put.TableName) == t.names["snapshot"] {
						if n, ok := action.Put.Item["skey"].(*types.AttributeValueMemberN); ok && n.Value != "0" {
							justWritten = n.Value
						}
					}
					if aws.ToString(action.Put.TableName) == t.names["head"] {
						constraints["head_return_values_on_condition_check_failure"] = string(action.Put.ReturnValuesOnConditionCheckFailure)
					}
				}
				if action.Update != nil && aws.ToString(action.Update.TableName) == t.names["head"] {
					constraints["head_return_values_on_condition_check_failure"] = string(action.Update.ReturnValuesOnConditionCheckFailure)
				}
			}
			constraints["put_tables"], constraints["same_store_id"], constraints["layout_version"] = putTables, sameID && storeID != "", layout
			if len(conditions) > 0 {
				equal := true
				for _, c := range conditions {
					equal = equal && reflect.DeepEqual(c, conditions[0])
				}
				if equal {
					constraints["condition"] = conditions[0]
				}
			}
			previousBatch = nil
		case *dynamodb.QueryInput:
			constraints["table"] = "unknown"
			for table, name := range t.names {
				if name == aws.ToString(input.TableName) {
					constraints["table"] = string(table)
				}
			}
			constraints["consistent_read"], constraints["scan_index_forward"] = aws.ToBool(input.ConsistentRead), input.ScanIndexForward == nil || *input.ScanIndexForward
			constraints["key_condition"] = keyConditionObservation(input, step)
			if aws.ToString(input.IndexName) == t.index {
				constraints["index"] = "configured-history-index"
				layout, err := t.Describe(ctx)
				if err != nil {
					return nil, err
				}
				for _, index := range layout["snapshot"].Description.GlobalSecondaryIndexes {
					if aws.ToString(index.IndexName) == t.index && index.Projection != nil {
						constraints["projection"] = string(index.Projection.ProjectionType)
					}
				}
			}
			if queryCount[phase] == 0 {
				queryValid[phase] = len(input.ExclusiveStartKey) == 0
			} else {
				previous := previousQuery[phase]
				queryValid[phase] = queryValid[phase] && previous != nil && len(previous.LastEvaluatedKey) > 0 && reflect.DeepEqual(input.ExclusiveStartKey, previous.LastEvaluatedKey)
			}
			queryCount[phase]++
			previousQuery[phase], _ = responses[i].Result.(*dynamodb.QueryOutput)
		case *dynamodb.BatchWriteItemInput:
			retry := len(pendingDelete) > 0
			if retry {
				retryDelete = true
				deleteValid = deleteValid && reflect.DeepEqual(input.RequestItems, pendingDelete)
			} else {
				size := 0
				for _, writes := range input.RequestItems {
					size += len(writes)
				}
				initialSizes = append(initialSizes, size)
			}
			for _, writes := range input.RequestItems {
				for _, write := range writes {
					if write.DeleteRequest != nil {
						if n, ok := write.DeleteRequest.Key["skey"].(*types.AttributeValueMemberN); ok {
							deleted[n.Value] = true
						}
					}
				}
			}
			pendingDelete = nil
			if response, ok := responses[i].Result.(*dynamodb.BatchWriteItemOutput); ok {
				pendingDelete = response.UnprocessedItems
			}
		case *dynamodb.UpdateItemInput:
			constraints["expression_attribute_names"], constraints["condition"], constraints["update"] = input.ExpressionAttributeNames, conditionObservation(input.ConditionExpression, input.ExpressionAttributeNames), updateObservation(input)
			for _, value := range input.ExpressionAttributeValues {
				if n, ok := value.(*types.AttributeValueMemberN); ok {
					constraints["expires"] = json.Number(n.Value)
				}
			}
			if n, ok := input.Key["skey"].(*types.AttributeValueMemberN); ok {
				constraints["target_seq_nrs"] = []any{json.Number(n.Value)}
			}
		}
		out = append(out, map[string]any{"api": request.API, "phase": phase, "constraints": constraints})
	}
	for _, entry := range out {
		phase := fmt.Sprint(entry["phase"])
		constraints := entry["constraints"].(map[string]any)
		if entry["api"] == "Query" {
			last := previousQuery[phase]
			constraints["follow_last_evaluated_key"] = queryCount[phase] > 1 && queryValid[phase] && last != nil && len(last.LastEvaluatedKey) == 0
			constraints["include_just_written_history"] = justWritten != "" && !deleted[justWritten]
		}
		if entry["api"] == "BatchWriteItem" {
			constraints["initial_batch_sizes"], constraints["retry_unprocessed_items"] = initialSizes, retryDelete && deleteValid && len(pendingDelete) == 0
		}
	}
	value, err := conformance.JSONValue(out)
	if err != nil {
		return nil, err
	}
	list := value.([]any)
	normalized := make([]map[string]any, len(list))
	for i, value := range list {
		normalized[i] = value.(map[string]any)
	}
	return normalized, nil
}
