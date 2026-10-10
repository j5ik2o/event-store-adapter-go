package dynamodb

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

func (s *opened) GetEventsByIDSinceSeqNr(ctx context.Context, id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	input := &awsdynamodb.QueryInput{
		TableName: aws.String(s.settings.journalTableName), ConsistentRead: aws.Bool(true), ScanIndexForward: aws.Bool(true),
		KeyConditionExpression: aws.String("aid = :aid AND seq_nr >= :seq_nr"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aid},
			":seq_nr": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr), 10)},
		},
	}
	var events []eventstore.EventEnvelope[[]byte]
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		for _, item := range out.Items {
			if err := readAid(item, aid); err != nil {
				return nil, &eventstore.StorageError{Cause: err}
			}
			event, err := readEventMetadata(id, item)
			if err != nil {
				return nil, &eventstore.StorageError{Cause: err}
			}
			if event.SeqNr() < seqNr || len(events) > 0 && event.SeqNr() <= events[len(events)-1].SeqNr() {
				return nil, &eventstore.StorageError{Cause: fmt.Errorf("stored events violate the inclusive lower bound or ascending order")}
			}
			events = append(events, event)
		}
		if len(out.LastEvaluatedKey) == 0 {
			return events, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}
