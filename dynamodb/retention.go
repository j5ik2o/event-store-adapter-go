package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
)

func (s *opened) retainHistory(ctx context.Context, aid string, justWritten eventstore.SeqNr) error {
	selected := map[eventstore.SeqNr]struct{}{justWritten: {}}
	input := &awsdynamodb.QueryInput{
		TableName: aws.String(s.settings.snapshotTableName), IndexName: aws.String(s.settings.snapshotHistoryIndexName),
		KeyConditionExpression: aws.String("aid = :aid"), ScanIndexForward: aws.Bool(false), ConsistentRead: aws.Bool(false),
		ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: aid}},
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return err
		}
		for _, item := range out.Items {
			itemAid, ok := item["aid"].(*types.AttributeValueMemberS)
			if !ok || itemAid.Value != aid {
				return errors.New("history query returned a different aggregate")
			}
			key, ok := item["skey"].(*types.AttributeValueMemberN)
			if !ok {
				return errors.New("history skey is not N")
			}
			n, err := strconv.ParseInt(key.Value, 10, 64)
			seqNr := eventstore.SeqNr(n)
			if err != nil || seqNr.ValidateAsEventSeqNr() != nil {
				return errors.New("history skey is not a valid history sequence number")
			}
			selected[seqNr] = struct{}{}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	seqNrs := make([]eventstore.SeqNr, 0, len(selected))
	for seqNr := range selected {
		seqNrs = append(seqNrs, seqNr)
	}
	slices.Sort(seqNrs)
	count := *s.settings.common.RetentionCount
	if len(seqNrs) <= count {
		return nil
	}
	obsolete := seqNrs[:len(seqNrs)-count]
	if s.settings.common.RetentionMode == storeoptions.RetentionTTL {
		return s.markHistory(ctx, aid, obsolete)
	}
	return s.deleteHistory(ctx, aid, obsolete)
}

func historyKey(aid string, seqNr eventstore.SeqNr) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"aid":  &types.AttributeValueMemberS{Value: aid},
		"skey": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(seqNr), 10)},
	}
}

func (s *opened) deleteHistory(ctx context.Context, aid string, seqNrs []eventstore.SeqNr) error {
	for batch := range slices.Chunk(seqNrs, 25) {
		writes := make([]types.WriteRequest, len(batch))
		for i, seqNr := range batch {
			writes[i].DeleteRequest = &types.DeleteRequest{Key: historyKey(aid, seqNr)}
		}
		pending := map[string][]types.WriteRequest{s.settings.snapshotTableName: writes}
		delay := 50 * time.Millisecond
		for retries := 0; ; retries++ {
			out, err := s.client.BatchWriteItem(ctx, &awsdynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return err
			}
			pending = make(map[string][]types.WriteRequest, len(out.UnprocessedItems))
			for table, unprocessed := range out.UnprocessedItems {
				if len(unprocessed) != 0 {
					pending[table] = unprocessed
				}
			}
			if len(pending) == 0 {
				break
			}
			if retries == s.settings.configurationReadRetryLimit {
				return fmt.Errorf("history delete retry limit %d reached with unprocessed items", s.settings.configurationReadRetryLimit)
			}
			s.hooks.Sleep(delay)
			delay = min(2*delay, time.Second)
		}
	}
	return nil
}

func (s *opened) markHistory(ctx context.Context, aid string, seqNrs []eventstore.SeqNr) error {
	for _, seqNr := range seqNrs {
		expires := uint64(s.hooks.Now().Unix()) + uint64(s.settings.common.TTLGraceSeconds)
		_, err := s.client.UpdateItem(ctx, &awsdynamodb.UpdateItemInput{
			TableName: aws.String(s.settings.snapshotTableName), Key: historyKey(aid, seqNr),
			UpdateExpression:          aws.String("SET #ttl = :expires REMOVE active_history_seq_nr"),
			ConditionExpression:       aws.String("attribute_exists(active_history_seq_nr)"),
			ExpressionAttributeNames:  map[string]string{"#ttl": "ttl"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":expires": &types.AttributeValueMemberN{Value: strconv.FormatUint(expires, 10)}},
		})
		var alreadyMarked *types.ConditionalCheckFailedException
		if err != nil && !errors.As(err, &alreadyMarked) {
			return err
		}
	}
	return nil
}

func (s *opened) notifyRetentionFailure(ctx context.Context, aid string, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("dynamodb retention failed: aid=%q: %w", aid, cause)}
	runRetentionNotification(func() {
		slog.ErrorContext(ctx, "dynamodb snapshot retention failed", "aid", aid, "error", failure)
	})
	if handler := s.settings.common.RetentionFailureHandler; handler != nil {
		runRetentionNotification(func() { handler(ctx, failure) })
	}
}

func runRetentionNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}
