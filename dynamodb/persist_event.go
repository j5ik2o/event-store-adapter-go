package dynamodb

import (
	"context"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

const (
	journalAction = 0
	headAction    = 1
)

// persistEvent connects the confirmed internal open to the real event-only write.
// It does not implement the other operations of the public EventStore interface.
func persistEvent[E any](ctx context.Context, store *opened, serializer eventstore.Serializer[E], event eventstore.EventEnvelope[E]) error {
	prepared, err := eventstore.PrepareEvent(serializer, event)
	if err != nil {
		return err
	}
	return store.persistPreparedEvent(ctx, prepared)
}

func (s *opened) persistPreparedEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	journal, head := eventItems(event)
	for _, item := range []map[string]types.AttributeValue{journal, head} {
		size, err := itemSizeUpperBound(item)
		if err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		if size > maxItemBytes {
			seqNr := event.SeqNr()
			return &eventstore.ContractViolationError{Rule: "D-7", SeqNr: &seqNr}
		}
	}
	_, err := s.client.TransactWriteItems(ctx, eventWrite(s.settings, event.SeqNr(), journal, head))
	if err != nil {
		return classifyWriteError(err, event.AggregateID(), event.SeqNr())
	}
	return nil
}

func eventItems(event eventstore.EventEnvelope[[]byte]) (journal, head map[string]types.AttributeValue) {
	seqNr := &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(event.SeqNr()), 10)}
	metadata := map[string]types.AttributeValue{
		"seq_nr":      seqNr,
		"occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(event.OccurredAt().UnixNano(), 10)},
		"manifest":    &types.AttributeValueMemberS{Value: event.Manifest()},
		"payload":     &types.AttributeValueMemberB{Value: event.Payload()},
	}
	aid := &types.AttributeValueMemberS{Value: event.AggregateID()}
	journal = make(map[string]types.AttributeValue, len(metadata)+1)
	journal["aid"] = aid
	for name, value := range metadata {
		journal[name] = value
	}
	typeName, _, _ := strings.Cut(event.AggregateID(), "-")
	head = map[string]types.AttributeValue{
		"aid":       aid,
		"type_name": &types.AttributeValueMemberS{Value: typeName},
		"seq_nr":    seqNr,
		"events":    &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberM{Value: metadata}}},
	}
	return journal, head
}

func eventWrite(cfg settings, seqNr eventstore.SeqNr, journal, head map[string]types.AttributeValue) *awsdynamodb.TransactWriteItemsInput {
	input := &awsdynamodb.TransactWriteItemsInput{TransactItems: make([]types.TransactWriteItem, 2)}
	input.TransactItems[journalAction].Put = &types.Put{
		TableName: aws.String(cfg.journalTableName), Item: journal,
		ConditionExpression: aws.String("attribute_not_exists(aid)"),
	}
	if seqNr == 1 {
		input.TransactItems[headAction].Put = &types.Put{
			TableName: aws.String(cfg.headTableName), Item: head,
			ConditionExpression:                 aws.String("attribute_not_exists(aid)"),
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}
	} else {
		input.TransactItems[headAction].Update = &types.Update{
			TableName: aws.String(cfg.headTableName), Key: map[string]types.AttributeValue{"aid": head["aid"]},
			ConditionExpression: aws.String("seq_nr = :prev"),
			UpdateExpression:    aws.String("SET seq_nr = :seq_nr, events = :events"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":prev":   &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr-1), 10)},
				":seq_nr": head["seq_nr"],
				":events": head["events"],
			},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}
	}
	return input
}
