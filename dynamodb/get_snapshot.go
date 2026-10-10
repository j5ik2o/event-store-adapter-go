package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

// GetLatestSnapshotByID reads head and current snapshot strongly consistently,
// but not atomically (R-8). A newer snapshot may accompany an older head.
func (s *opened) GetLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	pending := map[string]types.KeysAndAttributes{
		s.settings.headTableName: {ConsistentRead: aws.Bool(true), Keys: []map[string]types.AttributeValue{
			{"aid": &types.AttributeValueMemberS{Value: aid}},
		}},
		s.settings.snapshotTableName: {ConsistentRead: aws.Bool(true), Keys: []map[string]types.AttributeValue{
			{"aid": &types.AttributeValueMemberS{Value: aid}, "skey": &types.AttributeValueMemberN{Value: "0"}},
		}},
	}
	items := make(map[string]map[string]types.AttributeValue, 2)
	delay := 50 * time.Millisecond
	for retries := 0; ; retries++ {
		if cause := context.Cause(ctx); cause != nil {
			return nil, &eventstore.StorageError{Cause: cause}
		}
		out, err := s.client.BatchGetItem(ctx, &awsdynamodb.BatchGetItemInput{RequestItems: pending})
		if err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		for table, responses := range out.Responses {
			if table != s.settings.headTableName && table != s.settings.snapshotTableName || len(responses) > 1 {
				return nil, &eventstore.StorageError{Cause: fmt.Errorf("latest snapshot read returned unexpected items")}
			}
			for _, item := range responses {
				if _, exists := items[table]; exists {
					return nil, &eventstore.StorageError{Cause: fmt.Errorf("latest snapshot read returned a duplicate item")}
				}
				items[table] = item
			}
		}
		pending = make(map[string]types.KeysAndAttributes, len(out.UnprocessedKeys))
		for table, keys := range out.UnprocessedKeys {
			if len(keys.Keys) != 0 {
				keys.ConsistentRead = aws.Bool(true)
				pending[table] = keys
			}
		}
		if len(pending) == 0 {
			break
		}
		if retries == s.settings.configurationReadRetryLimit {
			return nil, &eventstore.StorageError{Cause: fmt.Errorf("latest snapshot read retry limit %d reached with unprocessed keys", s.settings.configurationReadRetryLimit)}
		}
		if err := s.waitForSnapshotRetry(ctx, delay); err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		delay = min(2*delay, time.Second)
	}
	head, exists := items[s.settings.headTableName]
	if !exists {
		return nil, nil
	}
	headSeqNr, err := readHead(id, aid, head)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	result := &eventstore.SnapshotRead[[]byte]{HeadSeqNr: headSeqNr}
	if current, exists := items[s.settings.snapshotTableName]; exists {
		snapshot, err := readCurrentSnapshot(aid, current)
		if err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		result.Snapshot = &snapshot
	}
	return result, nil
}

func (s *opened) waitForSnapshotRetry(ctx context.Context, delay time.Duration) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	completed := make(chan struct{})
	go func() {
		s.hooks.Sleep(delay)
		close(completed)
	}()
	select {
	case <-ctx.Done():
	case <-completed:
	}
	return context.Cause(ctx)
}
