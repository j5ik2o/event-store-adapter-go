package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

// persistEventAndSnapshot connects common preparation to the confirmed internal
// open. All items commit together; only a successful history write runs retention.
func persistEventAndSnapshot[E, A any](ctx context.Context, store *opened, eventSerializer eventstore.Serializer[E], snapshotSerializer eventstore.Serializer[A], event eventstore.EventEnvelope[E], snapshot eventstore.SnapshotEnvelope[A]) error {
	preparedEvent, preparedSnapshot, err := eventstore.PrepareEventAndSnapshot(eventSerializer, snapshotSerializer, event, snapshot)
	if err != nil {
		return err
	}
	return store.persistPreparedEventAndSnapshot(ctx, preparedEvent, preparedSnapshot)
}

func (s *opened) persistPreparedEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	journal, head := eventItems(event)
	current := snapshotItem(event, snapshot, false)
	items := []map[string]types.AttributeValue{journal, head, current}
	input := eventWrite(s.settings, event.SeqNr(), journal, head)
	input.TransactItems = append(input.TransactItems, types.TransactWriteItem{Put: &types.Put{
		TableName: aws.String(s.settings.snapshotTableName), Item: current,
	}})
	if s.settings.common.RetentionCount != nil {
		history := snapshotItem(event, snapshot, true)
		items = append(items, history)
		input.TransactItems = append(input.TransactItems, types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String(s.settings.snapshotTableName), Item: history,
		}})
	}
	for _, item := range items {
		size, err := itemSizeUpperBound(item)
		if err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		if size > maxItemBytes {
			seqNr := event.SeqNr()
			return &eventstore.ContractViolationError{Rule: "D-7", SeqNr: &seqNr}
		}
	}
	if _, err := s.client.TransactWriteItems(ctx, input); err != nil {
		return classifyWriteError(err, event.AggregateID(), event.SeqNr())
	}
	if s.settings.common.RetentionCount != nil {
		if err := s.retainHistory(ctx, event.AggregateID(), event.SeqNr()); err != nil {
			s.notifyRetentionFailure(ctx, event.AggregateID(), err)
		}
	}
	return nil
}

func snapshotItem(event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte], history bool) map[string]types.AttributeValue {
	seqNr := strconv.FormatInt(int64(snapshot.SeqNr()), 10)
	item := map[string]types.AttributeValue{
		"aid":             &types.AttributeValueMemberS{Value: event.AggregateID()},
		"skey":            &types.AttributeValueMemberN{Value: "0"},
		"seq_nr":          &types.AttributeValueMemberN{Value: seqNr},
		"manifest":        &types.AttributeValueMemberS{Value: snapshot.Manifest()},
		"payload":         &types.AttributeValueMemberB{Value: snapshot.Aggregate()},
		"last_updated_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(event.OccurredAt().UnixMilli(), 10)},
	}
	if history {
		item["skey"] = &types.AttributeValueMemberN{Value: seqNr}
		item["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: seqNr}
	}
	return item
}
