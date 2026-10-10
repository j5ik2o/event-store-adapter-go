package dynamodbtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

// PutSeedItems installs only declared inputs, with the independent client.
func (t *Tables) PutSeedItems(ctx context.Context, fixtures []map[string]any) error {
	for _, fixture := range fixtures {
		name, err := t.TableName(Table(fmt.Sprint(fixture["table"])))
		if err != nil {
			return err
		}
		attrs, ok := fixture["attributes"].(map[string]any)
		if !ok {
			return fmt.Errorf("seed attributes are missing")
		}
		values, _ := fixture["values"].(map[string]any)
		binary, _ := fixture["binary_json"].(map[string]any)
		nested, _ := fixture["nested_attributes"].(map[string]any)
		item := map[string]types.AttributeValue{}
		for attribute, kind := range attrs {
			value, err := seedAttribute(fmt.Sprint(kind), attribute, values[attribute], binary, nested)
			if err != nil {
				return err
			}
			item[attribute] = value
		}
		if _, err := t.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
			return err
		}
	}
	return nil
}

func seedAttribute(kind, path string, value any, binary, nested map[string]any) (types.AttributeValue, error) {
	switch kind {
	case "S":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("seed %s is not S", path)
		}
		return &types.AttributeValueMemberS{Value: v}, nil
	case "N":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("seed %s is not N", path)
		}
		return &types.AttributeValueMemberN{Value: v}, nil
	case "B":
		v, exists := binary[path]
		if !exists {
			return nil, fmt.Errorf("seed binary %s is missing", path)
		}
		raw, err := json.Marshal(v)
		return &types.AttributeValueMemberB{Value: raw}, err
	case "L":
		list, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("seed %s is not L", path)
		}
		out := &types.AttributeValueMemberL{}
		for i, v := range list {
			entry, err := seedAttribute("M", fmt.Sprintf("%s[%d]", path, i), v, binary, nested)
			if err != nil {
				return nil, err
			}
			out.Value = append(out.Value, entry)
		}
		return out, nil
	case "M":
		attrs, ok := nested[path].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("seed nested attributes %s are missing", path)
		}
		values, _ := value.(map[string]any)
		out := &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}
		for name, typ := range attrs {
			v, err := seedAttribute(fmt.Sprint(typ), path+"."+name, values[name], binary, nested)
			if err != nil {
				return nil, err
			}
			out.Value[name] = v
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported seed attribute type %s", kind)
	}
}

// ItemObservation describes the complete actual item before any comparison.
func ItemObservation(table string, item map[string]types.AttributeValue) (map[string]any, error) {
	attrs, values, binary, nested := map[string]any{}, map[string]any{}, map[string]any{}, map[string]any{}
	for name, attribute := range item {
		kind, value, err := observeAttribute(name, attribute, binary, nested)
		if err != nil {
			return nil, err
		}
		attrs[name] = kind
		if kind != "B" {
			values[name] = value
		}
	}
	return map[string]any{"table": table, "attributes": attrs, "values": values, "binary_json": binary, "nested_attributes": nested}, nil
}

func observeAttribute(path string, attribute types.AttributeValue, binary, nested map[string]any) (string, any, error) {
	switch a := attribute.(type) {
	case *types.AttributeValueMemberS:
		return "S", a.Value, nil
	case *types.AttributeValueMemberN:
		n, ok := new(big.Int).SetString(a.Value, 10)
		if !ok {
			return "", nil, fmt.Errorf("observed N %s is not an integer", path)
		}
		return "N", n.String(), nil
	case *types.AttributeValueMemberB:
		value, err := conformance.JSONValue(json.RawMessage(a.Value))
		if err != nil {
			return "", nil, err
		}
		binary[path] = value
		return "B", nil, nil
	case *types.AttributeValueMemberL:
		list := []any{}
		for i, a := range a.Value {
			_, value, err := observeAttribute(fmt.Sprintf("%s[%d]", path, i), a, binary, nested)
			if err != nil {
				return "", nil, err
			}
			list = append(list, value)
		}
		return "L", list, nil
	case *types.AttributeValueMemberM:
		attrs, values := map[string]any{}, map[string]any{}
		for name, a := range a.Value {
			kind, value, err := observeAttribute(path+"."+name, a, binary, nested)
			if err != nil {
				return "", nil, err
			}
			attrs[name] = kind
			if kind != "B" {
				values[name] = value
			}
		}
		nested[path] = attrs
		return "M", values, nil
	default:
		return "", nil, fmt.Errorf("unsupported observed attribute %s (%T)", path, attribute)
	}
}
