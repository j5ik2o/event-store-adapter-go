package conformance_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
)

// preparedBackend retains observation but hides provisioning already owned here.
type preparedBackend struct {
	conformance.Backend
	reader conformance.ObservationReader
}

func (b preparedBackend) Observe(ctx context.Context, operation int, step conformance.StepPlan) (map[string]any, error) {
	return b.reader.Observe(ctx, operation, step)
}

func (b *publicBackend) RunLayout(ctx context.Context, c conformance.LayoutCase) (res conformance.CaseResult) {
	res = conformance.CaseResult{ID: c.ID, Backend: b.name, Rules: c.Rules, Status: conformance.StatusFailure}
	fail := func(err error) conformance.CaseResult { res.Reason = err.Error(); return res }
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	if err != nil {
		return fail(err)
	}
	var scenario *conformance.ScenarioCase
	for i := range data.Scenarios {
		if data.Scenarios[i].File == "dynamodb/item-shapes.json" {
			scenario = &data.Scenarios[i]
			break
		}
	}
	if scenario == nil {
		return fail(fmt.Errorf("distributed item-shapes input is missing"))
	}
	prepared, cleanup, err := b.Prepare(ctx, scenario.Plan)
	if err != nil {
		return fail(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			res.Status, res.Reason = conformance.StatusFailure, err.Error()
		}
	}()
	actual := prepared.(*publicBackend)
	results := conformance.RunBackend(ctx, &conformance.Data{Scenarios: []conformance.ScenarioCase{*scenario}}, preparedBackend{Backend: actual, reader: actual})
	if results[0].Status != conformance.StatusSuccess {
		return fail(fmt.Errorf("public item-shapes operations: %s", results[0].Reason))
	}
	res.Operations = results[0].Operations
	res.Faults = results[0].Faults
	layout, err := actual.tables.Describe(ctx)
	if err != nil {
		return fail(err)
	}
	tables := []any{}
	for _, table := range []dynamodbtest.Table{"journal", "snapshot", "head"} {
		observation := layout[table]
		description := observation.Description
		key := func(schema []types.KeySchemaElement, kind types.KeyType) any {
			for _, element := range schema {
				if element.KeyType == kind {
					for _, definition := range description.AttributeDefinitions {
						if aws.ToString(definition.AttributeName) == aws.ToString(element.AttributeName) {
							return map[string]any{"name": aws.ToString(element.AttributeName), "type": string(definition.AttributeType)}
						}
					}
				}
			}
			return nil
		}
		indexes := []any{}
		for _, index := range description.GlobalSecondaryIndexes {
			name := aws.ToString(index.IndexName)
			if name == actual.tables.HistoryIndexName() {
				name = "configured-history-index"
			}
			indexes = append(indexes, map[string]any{"name_binding": name, "partition_key": key(index.KeySchema, types.KeyTypeHash), "sort_key": key(index.KeySchema, types.KeyTypeRange), "projection": string(index.Projection.ProjectionType)})
		}
		enabled := description.StreamSpecification != nil && aws.ToBool(description.StreamSpecification.StreamEnabled)
		var view any
		if enabled {
			view = string(description.StreamSpecification.StreamViewType)
		}
		ttlMode := "never"
		var ttlAttribute any
		if observation.TTL.TimeToLiveStatus == types.TimeToLiveStatusEnabled {
			ttlMode, ttlAttribute = "retention-mode-ttl", aws.ToString(observation.TTL.AttributeName)
		}
		tables = append(tables, map[string]any{"name": string(table), "partition_key": key(description.KeySchema, types.KeyTypeHash), "sort_key": key(description.KeySchema, types.KeyTypeRange), "gsi": indexes, "streams": map[string]any{"enabled": enabled, "view_type": view}, "ttl": map[string]any{"enabled_when": ttlMode, "attribute": ttlAttribute}})
	}
	if !reflect.DeepEqual(c.Raw["tables"], tables) {
		return fail(fmt.Errorf("actual table layout differs: %v", tables))
	}
	items := []any{}
	for _, operation := range res.Operations {
		if obs, ok := operation.Observation.(map[string]any); ok {
			if list, ok := obs["items"].([]any); ok {
				items = append(items, list...)
			}
		}
	}
	for _, table := range []dynamodbtest.Table{"journal", "snapshot", "head"} {
		key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}}
		if table == "journal" {
			key["seq_nr"] = &types.AttributeValueMemberN{Value: "0"}
		}
		if table == "snapshot" {
			key["skey"] = &types.AttributeValueMemberN{Value: "0"}
		}
		item, err := actual.tables.GetItem(ctx, table, key)
		if err != nil {
			return fail(err)
		}
		observed, err := dynamodbtest.ItemObservation(string(table), item)
		if err != nil {
			return fail(err)
		}
		items = append(items, observed)
	}
	for _, raw := range c.Raw["items"].([]any) {
		wanted := raw.(map[string]any)
		found := false
		for _, raw := range items {
			observed := raw.(map[string]any)
			if wanted["table"] == observed["table"] && reflect.DeepEqual(wanted["attributes"], observed["attributes"]) {
				if nested, exists := wanted["nested_attributes"]; !exists || reflect.DeepEqual(nested, observed["nested_attributes"]) {
					found = true
					break
				}
			}
		}
		if !found {
			return fail(fmt.Errorf("no actual item matches layout attribute shape %v", wanted["attributes"]))
		}
	}
	aid := scenario.Plan.Steps[len(scenario.Plan.Steps)-1].AID
	snapshot, _ := actual.tables.TableName("snapshot")
	indexed, err := actual.tables.QueryAll(ctx, awsdb.QueryInput{TableName: aws.String(snapshot), IndexName: aws.String(actual.tables.HistoryIndexName()), KeyConditionExpression: aws.String("aid = :aid"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: aid.TypeName + "-" + aid.Value}}})
	if err != nil {
		return fail(err)
	}
	if len(indexed) != 1 {
		return fail(fmt.Errorf("actual sparse GSI has %d entries, expected only active history", len(indexed)))
	}
	for _, item := range indexed {
		if len(item) != 3 || item["aid"] == nil || item["skey"] == nil || item["active_history_seq_nr"] == nil {
			return fail(fmt.Errorf("GSI projection differs: %v", item))
		}
	}
	configurationIndexed, err := actual.tables.QueryAll(ctx, awsdb.QueryInput{TableName: aws.String(snapshot), IndexName: aws.String(actual.tables.HistoryIndexName()), KeyConditionExpression: aws.String("aid = :aid"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: "__config__"}}})
	if err != nil {
		return fail(err)
	}
	if len(configurationIndexed) != 0 {
		return fail(fmt.Errorf("configuration appears in the sparse history index"))
	}
	res.Actual = map[string]any{"describe": layout, "tables": tables, "items": items, "gsi_items": indexed, "configuration_gsi_items": configurationIndexed}
	res.Status = conformance.StatusSuccess
	return res
}
