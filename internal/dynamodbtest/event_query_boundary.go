package dynamodbtest

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const eventResponseByteLimit = 1048576

// BoundEventQuery is the approved test response boundary for Local 3.3.1,
// whose original distributed four-envelope response exceeded 1MiB without LEK.
// It uses only received attributes: UTF-8 names/S/N and raw B bytes. A normal
// page is preserved; an oversized page returns its maximal contiguous prefix
// and that prefix's real key. ReadObserver retains the unmodified original.
func BoundEventQuery(actual *dynamodb.QueryOutput) (*dynamodb.QueryOutput, error) {
	size, prefix := 0, 0
	for _, item := range actual.Items {
		itemBytes := 0
		for name, attribute := range item {
			itemBytes += len(name)
			switch value := attribute.(type) {
			case *types.AttributeValueMemberS:
				itemBytes += len(value.Value)
			case *types.AttributeValueMemberN:
				itemBytes += len(value.Value)
			case *types.AttributeValueMemberB:
				itemBytes += len(value.Value)
			default:
				return nil, fmt.Errorf("unsupported event response attribute %s (%T)", name, attribute)
			}
		}
		if size+itemBytes > eventResponseByteLimit {
			break
		}
		size += itemBytes
		prefix++
	}
	if prefix == len(actual.Items) {
		return actual, nil
	}
	if prefix == 0 {
		return nil, fmt.Errorf("one event response item exceeds the test response boundary")
	}
	last := actual.Items[prefix-1]
	aid, aidOK := last["aid"].(*types.AttributeValueMemberS)
	seq, seqOK := last["seq_nr"].(*types.AttributeValueMemberN)
	if !aidOK || !seqOK {
		return nil, fmt.Errorf("event response prefix has no real journal key")
	}
	out := copyReadResult(actual, actual.ResultMetadata).(*dynamodb.QueryOutput)
	out.Items = out.Items[:prefix]
	out.Count = int32(prefix)
	out.LastEvaluatedKey = map[string]types.AttributeValue{
		"aid":    &types.AttributeValueMemberS{Value: aid.Value},
		"seq_nr": &types.AttributeValueMemberN{Value: seq.Value},
	}
	return out, nil
}
