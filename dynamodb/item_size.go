package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxItemBytes = 409600

// itemSizeUpperBound follows design 4.5: UTF-8 names/S, raw B, 21-byte N,
// and a three-byte L/M overhead plus one byte per nested element.
func itemSizeUpperBound(item map[string]types.AttributeValue) (int, error) {
	size := 0
	for name, value := range item {
		valueSize, err := attributeSizeUpperBound(value)
		if err != nil {
			return 0, err
		}
		size += len(name) + valueSize
	}
	return size, nil
}

func attributeSizeUpperBound(value types.AttributeValue) (int, error) {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return len(value.Value), nil
	case *types.AttributeValueMemberB:
		return len(value.Value), nil
	case *types.AttributeValueMemberN:
		return 21, nil
	case *types.AttributeValueMemberL:
		size := 3
		for _, element := range value.Value {
			elementSize, err := attributeSizeUpperBound(element)
			if err != nil {
				return 0, err
			}
			size += 1 + elementSize
		}
		return size, nil
	case *types.AttributeValueMemberM:
		size, err := itemSizeUpperBound(value.Value)
		return 3 + len(value.Value) + size, err
	default:
		return 0, fmt.Errorf("unsupported event attribute type %T", value)
	}
}
