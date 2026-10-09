package dynamodb

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/require"
)

func validConfig() Config {
	return Config{JournalTableName: "journal", SnapshotTableName: "snapshot", HeadTableName: "head", SnapshotHistoryIndexName: "history"}
}

func configurationUnitClient(recorder *dynamodbtest.Recorder) *awsdynamodb.Client {
	return awsdynamodb.New(awsdynamodb.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:1"),
		Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""), RetryMaxAttempts: 1,
		APIOptions: []func(*middleware.Stack) error{recorder.APIOption},
	})
}

func requireKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok)
	require.Equal(t, expected, kind)
	var classified eventstore.Error
	require.ErrorAs(t, err, &classified)
	require.NotNil(t, classified.Unwrap())
}

func TestOpenRejectsConfigurationBeforeRequests(t *testing.T) {
	// Given invalid input, When the actual internal open runs, Then no SDK request is made.
	for _, tc := range []struct {
		name      string
		cfg       func(*Config)
		nilClient bool
		opts      []eventstore.Option
	}{
		{name: "nil client", nilClient: true},
		{name: "empty journal", cfg: func(c *Config) { c.JournalTableName = "" }},
		{name: "empty snapshot", cfg: func(c *Config) { c.SnapshotTableName = "" }},
		{name: "empty head", cfg: func(c *Config) { c.HeadTableName = "" }},
		{name: "empty history index", cfg: func(c *Config) { c.SnapshotHistoryIndexName = "" }},
		{name: "journal snapshot duplicate", cfg: func(c *Config) { c.SnapshotTableName = c.JournalTableName }},
		{name: "journal head duplicate", cfg: func(c *Config) { c.HeadTableName = c.JournalTableName }},
		{name: "snapshot head duplicate", cfg: func(c *Config) { c.HeadTableName = c.SnapshotTableName }},
		{name: "all tables identical", cfg: func(c *Config) { c.HeadTableName = c.JournalTableName; c.SnapshotTableName = c.JournalTableName }},
		{name: "negative retry", cfg: func(c *Config) { c.ConfigurationReadRetryLimit = aws.Int(-1) }},
		{name: "nil option", opts: []eventstore.Option{nil}},
		{name: "invalid retention value", opts: []eventstore.Option{eventstore.WithRetentionCount(eventstore.RetentionCount{})}},
		{name: "invalid retention mode", opts: []eventstore.Option{func(o *storeoptions.Options) error { o.RetentionCount = aws.Int(1); o.RetentionMode = 99; return nil }}},
		{name: "negative TTL grace", opts: []eventstore.Option{eventstore.WithTTLGraceSeconds(-1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := dynamodbtest.NewRecorder()
			client := configurationUnitClient(recorder)
			if tc.nilClient {
				client = nil
			}
			cfg := validConfig()
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			state, err := open(dynamodbtest.WithOperation(context.Background(), 0), client, cfg, nil, tc.opts...)
			require.Nil(t, state)
			requireKind(t, err, eventstore.KindConfiguration)
			var configuration *eventstore.ConfigurationError
			require.ErrorAs(t, err, &configuration)
			require.Empty(t, recorder.Requests(0))
		})
	}
}

func TestValidateConfigDefaultsAndCommonSettings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit *int
		want  int
	}{
		{"default", nil, 10}, {"zero", aws.Int(0), 0}, {"explicit", aws.Int(3), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.ConfigurationReadRetryLimit = tc.limit
			count, err := eventstore.KeepLatest(2)
			require.NoError(t, err)
			called := 0
			applied := eventstore.Option(func(o *storeoptions.Options) error { called++; return eventstore.WithRetentionCount(count)(o) })
			settings, err := validateConfig(configurationUnitClient(dynamodbtest.NewRecorder()), cfg, applied, eventstore.WithRetentionMode(eventstore.RetentionTTL), eventstore.WithTTLGraceSeconds(7))
			require.NoError(t, err)
			require.Equal(t, 1, called)
			require.Equal(t, tc.want, settings.configurationReadRetryLimit)
			require.Equal(t, 2, *settings.common.RetentionCount)
			require.Equal(t, storeoptions.RetentionTTL, settings.common.RetentionMode)
			require.Equal(t, int64(7), settings.common.TTLGraceSeconds)
		})
	}
	settings, err := validateConfig(configurationUnitClient(dynamodbtest.NewRecorder()), validConfig(), eventstore.WithRetentionMode(99))
	require.NoError(t, err, "a mode without history is ignored by the common contract")
	require.Nil(t, settings.common.RetentionCount)
}

func TestValidateConfigPreservesOptionCause(t *testing.T) {
	cause := errors.New("option cause")
	_, err := validateConfig(configurationUnitClient(dynamodbtest.NewRecorder()), validConfig(), func(*storeoptions.Options) error { return &eventstore.ConfigurationError{Cause: cause} })
	requireKind(t, err, eventstore.KindConfiguration)
	require.ErrorIs(t, err, cause)
}
