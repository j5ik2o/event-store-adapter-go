package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/dynamodb"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

type arbitraryEvent struct {
	Changes []string
	Count   int
}
type arbitraryState struct{ Values map[string]int }

type nilSerializer[T any] struct{}

func (*nilSerializer[T]) Serialize(T) ([]byte, error)   { panic("nil serializer called") }
func (*nilSerializer[T]) Deserialize([]byte) (T, error) { panic("nil serializer called") }

func TestDynamoDBFactoryExternalValidation(t *testing.T) {
	for _, tc := range []struct {
		name                                                              string
		change                                                            func(*dynamodb.Config)
		clientNil, eventNil, snapshotNil, typedEventNil, typedSnapshotNil bool
		opts                                                              []eventstore.Option
	}{
		{name: "nil client", clientNil: true}, {name: "nil event", eventNil: true}, {name: "nil snapshot", snapshotNil: true},
		{name: "typed nil event", typedEventNil: true}, {name: "typed nil snapshot", typedSnapshotNil: true},
		{name: "journal", change: func(c *dynamodb.Config) { c.JournalTableName = "" }},
		{name: "snapshot", change: func(c *dynamodb.Config) { c.SnapshotTableName = "" }},
		{name: "head", change: func(c *dynamodb.Config) { c.HeadTableName = "" }},
		{name: "index", change: func(c *dynamodb.Config) { c.SnapshotHistoryIndexName = "" }},
		{name: "duplicate tables", change: func(c *dynamodb.Config) { c.HeadTableName = c.JournalTableName }},
		{name: "negative retry", change: func(c *dynamodb.Config) { c.ConfigurationReadRetryLimit = aws.Int(-1) }},
		{name: "nil option", opts: []eventstore.Option{nil}},
		{name: "retention zero", opts: []eventstore.Option{eventstore.WithRetentionCount(eventstore.RetentionCount{})}},
		{name: "negative grace", opts: []eventstore.Option{eventstore.WithTTLGraceSeconds(-1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := dynamodbtest.NewRecorder()
			client := awsdynamodb.New(awsdynamodb.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:1"), RetryMaxAttempts: 1,
				Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""), APIOptions: []func(*middleware.Stack) error{r.APIOption}})
			if tc.clientNil {
				client = nil
			}
			cfg := dynamodb.Config{JournalTableName: "journal", SnapshotTableName: "snapshot", HeadTableName: "head", SnapshotHistoryIndexName: "history"}
			if tc.change != nil {
				tc.change(&cfg)
			}
			var es eventstore.Serializer[arbitraryEvent] = eventstore.NewJSONSerializer[arbitraryEvent]()
			var ss eventstore.Serializer[arbitraryState] = eventstore.NewJSONSerializer[arbitraryState]()
			if tc.eventNil {
				es = nil
			}
			if tc.snapshotNil {
				ss = nil
			}
			if tc.typedEventNil {
				es = (*nilSerializer[arbitraryEvent])(nil)
			}
			if tc.typedSnapshotNil {
				ss = (*nilSerializer[arbitraryState])(nil)
			}
			store, err := dynamodb.New(dynamodbtest.WithOperation(context.Background(), 0), client, cfg, es, ss, tc.opts...)
			require.Nil(t, store)
			kind, ok := eventstore.KindOf(err)
			require.True(t, ok)
			require.Equal(t, eventstore.KindConfiguration, kind)
			var classified *eventstore.ConfigurationError
			require.ErrorAs(t, err, &classified)
			require.NotNil(t, classified.Unwrap())
			require.Empty(t, r.Requests(0))
		})
	}
}
