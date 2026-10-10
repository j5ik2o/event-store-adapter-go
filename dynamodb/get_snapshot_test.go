package dynamodb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBSnapshotUnitPresenceAndIndependentNumbers(t *testing.T) {
	for _, tc := range []struct {
		name               string
		head, snapshot     map[string]types.AttributeValue
		headNr, snapshotNr eventstore.SeqNr
	}{
		{name: "absent"}, {name: "snapshot only", snapshot: readSnapshotFixture("2")},
		{name: "head only", head: readHeadFixture("3"), headNr: 3},
		{name: "equal", head: readHeadFixture("2"), snapshot: readSnapshotFixture("2"), headNr: 2, snapshotNr: 2},
		{name: "later head", head: readHeadFixture("3"), snapshot: readSnapshotFixture("2"), headNr: 3, snapshotNr: 2},
		{name: "later snapshot", head: readHeadFixture("1"), snapshot: readSnapshotFixture("2"), headNr: 1, snapshotNr: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			store := readUnitStore(t, r, 0, nil, func(input any) (any, error) {
				batch := input.(*awsdynamodb.BatchGetItemInput)
				require.Len(t, batch.RequestItems, 2)
				require.Equal(t, []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "Order-9"}}}, batch.RequestItems["head"].Keys)
				require.Equal(t, []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "Order-9"}, "skey": &types.AttributeValueMemberN{Value: "0"}}}, batch.RequestItems["snapshot"].Keys)
				for _, request := range batch.RequestItems {
					require.True(t, aws.ToBool(request.ConsistentRead))
				}
				out := &awsdynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{}}
				if tc.head != nil {
					out.Responses["head"] = []map[string]types.AttributeValue{tc.head}
				}
				if tc.snapshot != nil {
					out.Responses["snapshot"] = []map[string]types.AttributeValue{tc.snapshot}
				}
				return out, nil
			})
			out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
			require.NoError(t, err)
			require.Len(t, r.Requests(1), 1)
			if tc.head == nil {
				require.Nil(t, out)
				return
			}
			require.Equal(t, tc.headNr, out.HeadSeqNr)
			if tc.snapshot == nil {
				require.Nil(t, out.Snapshot)
				return
			}
			require.Equal(t, tc.snapshotNr, out.Snapshot.SeqNr())
			require.Equal(t, "snapshot/任意", out.Snapshot.Manifest())
			require.Equal(t, []byte("snapshot bytes"), out.Snapshot.Aggregate())
		})
	}
}

func TestDynamoDBSnapshotUnitUnprocessedBudget(t *testing.T) {
	for _, tc := range []struct {
		name            string
		limit, deferred int
		success         bool
	}{
		{"success", 2, 2, true}, {"zero", 0, 1, false}, {"exhausted", 2, 3, false}, {"backoff cap", 6, 6, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			hooks := testhook.New()
			var waits []time.Duration
			hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
			calls := 0
			store := readUnitStore(t, r, tc.limit, hooks, func(input any) (any, error) {
				batch := input.(*awsdynamodb.BatchGetItemInput)
				for _, keys := range batch.RequestItems {
					require.True(t, aws.ToBool(keys.ConsistentRead))
				}
				out := &awsdynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{}}
				if calls == 0 {
					require.Len(t, batch.RequestItems, 2)
					out.Responses["head"] = []map[string]types.AttributeValue{readHeadFixture("3")}
				} else {
					require.Len(t, batch.RequestItems, 1)
				}
				require.Len(t, batch.RequestItems["snapshot"].Keys, 1)
				if calls < tc.deferred {
					keys := batch.RequestItems["snapshot"]
					keys.ConsistentRead = aws.Bool(false)
					out.UnprocessedKeys = map[string]types.KeysAndAttributes{"snapshot": keys, "head": {}}
				} else {
					out.Responses["snapshot"] = []map[string]types.AttributeValue{readSnapshotFixture("2")}
				}
				calls++
				return out, nil
			})
			out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(t.Context(), 1), readID(t))
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, eventstore.SeqNr(3), out.HeadSeqNr)
				require.Equal(t, eventstore.SeqNr(2), out.Snapshot.SeqNr())
			} else {
				require.Nil(t, out)
				requireKind(t, err, eventstore.KindStorage)
			}
			require.Equal(t, min(tc.limit, tc.deferred)+1, calls)
			require.Len(t, waits, calls-1)
			require.Len(t, r.Requests(1), calls)
			for i, d := range waits {
				require.Equal(t, min(50*time.Millisecond*time.Duration(1<<i), time.Second), d)
			}
		})
	}
}

func TestDynamoDBSnapshotUnitStorageFailures(t *testing.T) {
	cause := errors.New("SDK failed")
	for _, tc := range []struct {
		name     string
		response *awsdynamodb.BatchGetItemOutput
		cause    error
	}{
		{name: "SDK", cause: cause},
		{name: "invalid head", response: &awsdynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{"head": {readHeadFixture("0")}}}},
		{name: "invalid snapshot", response: &awsdynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{"head": {readHeadFixture("1")}, "snapshot": {readSnapshotFixture("-1")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := readUnitStore(t, dynamodbtest.NewRecorder(), 0, nil, func(any) (any, error) { return tc.response, tc.cause })
			out, err := store.GetLatestSnapshotByID(t.Context(), readID(t))
			require.Nil(t, out)
			requireKind(t, err, eventstore.KindStorage)
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
				require.NotNil(t, errors.Unwrap(err))
			}
		})
	}
}

func TestDynamoDBSnapshotUnitContextCancellation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		calls, waits int
	}{
		{"before first request", 0, 0},
		{"before retry wait", 1, 0},
		{"at wait completion", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("snapshot caller canceled")
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(cause)
			r := dynamodbtest.NewRecorder()
			hooks := testhook.New()
			var waits atomic.Int32
			hooks.SetSleeper(func(time.Duration) {
				waits.Add(1)
				cancel(cause)
			})
			calls := 0
			store := readUnitStore(t, r, 2, hooks, func(input any) (any, error) {
				calls++
				batch := input.(*awsdynamodb.BatchGetItemInput)
				if tc.name == "before retry wait" {
					cancel(cause)
				}
				return &awsdynamodb.BatchGetItemOutput{
					Responses:       map[string][]map[string]types.AttributeValue{"head": {readHeadFixture("1")}},
					UnprocessedKeys: map[string]types.KeysAndAttributes{"snapshot": batch.RequestItems["snapshot"]},
				}, nil
			})
			if tc.name == "before first request" {
				cancel(cause)
			}

			out, err := store.GetLatestSnapshotByID(dynamodbtest.WithOperation(ctx, 1), readID(t))

			require.Nil(t, out)
			requireKind(t, err, eventstore.KindStorage)
			var storage *eventstore.StorageError
			require.ErrorAs(t, err, &storage)
			require.ErrorIs(t, err, cause)
			require.Equal(t, cause, errors.Unwrap(storage))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.Equal(t, tc.calls, calls)
			require.Len(t, r.Requests(1), tc.calls)
			require.Equal(t, int32(tc.waits), waits.Load())
		})
	}
}

func TestDynamoDBSnapshotUnitDefaultRetryWait(t *testing.T) {
	for _, ending := range []string{"before wait", "cancel", "deadline", "normal", "normal cap"} {
		t.Run(ending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &opened{}
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				cause := errors.New("snapshot caller canceled")
				delay := time.Second
				if ending == "normal" {
					delay = 50 * time.Millisecond
				}
				const deadline = 10 * time.Millisecond
				if ending == "deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, deadline)
					defer stop()
					cause = context.DeadlineExceeded
				}
				start := time.Now()
				if ending == "before wait" {
					cancel(cause)
					require.ErrorIs(t, store.waitForSnapshotRetry(ctx, delay), cause)
					require.Zero(t, time.Since(start))
					return
				}
				result := make(chan error, 1)
				go func() { result <- store.waitForSnapshotRetry(ctx, delay) }()
				synctest.Wait()
				require.NoError(t, ctx.Err(), "Context must be active when the default retry wait starts")
				select {
				case <-result:
					t.Fatal("default retry wait returned before cancellation or its duration")
				default:
				}

				var elapsed time.Duration
				switch ending {
				case "cancel":
					cancel(cause)
				case "deadline":
					elapsed = deadline
					time.Sleep(deadline - time.Nanosecond)
					synctest.Wait()
					require.NoError(t, ctx.Err())
					time.Sleep(time.Nanosecond)
				case "normal", "normal cap":
					elapsed = delay
					time.Sleep(delay - time.Nanosecond)
					synctest.Wait()
					select {
					case <-result:
						t.Fatal("default retry wait completed before its full duration")
					default:
					}
					time.Sleep(time.Nanosecond)
				}
				synctest.Wait()
				select {
				case err := <-result:
					if ending == "normal" || ending == "normal cap" {
						require.NoError(t, err)
						require.NoError(t, ctx.Err())
					} else {
						require.ErrorIs(t, err, cause)
						require.Equal(t, cause, context.Cause(ctx))
					}
				case <-time.After(delay):
					t.Fatal("default retry wait did not terminate")
				}
				require.Equal(t, elapsed, time.Since(start))
				t.Logf("default wait: %s, elapsed=%s, cause=%v; isolated wait must terminate before the test returns", ending, time.Since(start), context.Cause(ctx))
			})
		})
	}
}
