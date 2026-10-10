// Package dynamodb provides an event store using the three-table DynamoDB layout.
package dynamodb

import (
	"errors"
	"fmt"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
)

// Config names the externally provisioned tables and snapshot history index.
type Config struct {
	JournalTableName         string
	SnapshotTableName        string
	HeadTableName            string
	SnapshotHistoryIndexName string
	// ConfigurationReadRetryLimit excludes the initial request. Nil means 10,
	// zero disables re-requests, and negative values are configuration errors.
	ConfigurationReadRetryLimit *int
}

// settings owns the validated values, including copies of caller-owned pointers.
type settings struct {
	journalTableName            string
	snapshotTableName           string
	headTableName               string
	snapshotHistoryIndexName    string
	configurationReadRetryLimit int
	common                      storeoptions.Options
}

func validateConfig(client *awsdynamodb.Client, cfg Config, opts ...eventstore.Option) (settings, error) {
	configurationError := func(cause error) error {
		return &eventstore.ConfigurationError{Cause: cause}
	}
	if client == nil {
		return settings{}, configurationError(errors.New("DynamoDB client is nil"))
	}
	for _, field := range []struct{ name, value string }{
		{"JournalTableName", cfg.JournalTableName},
		{"SnapshotTableName", cfg.SnapshotTableName},
		{"HeadTableName", cfg.HeadTableName},
		{"SnapshotHistoryIndexName", cfg.SnapshotHistoryIndexName},
	} {
		if field.value == "" {
			return settings{}, configurationError(fmt.Errorf("%s is empty", field.name))
		}
	}
	if cfg.JournalTableName == cfg.SnapshotTableName || cfg.JournalTableName == cfg.HeadTableName || cfg.SnapshotTableName == cfg.HeadTableName {
		return settings{}, configurationError(errors.New("DynamoDB table names must be distinct"))
	}
	retryLimit := 10
	if cfg.ConfigurationReadRetryLimit != nil {
		retryLimit = *cfg.ConfigurationReadRetryLimit
	}
	if retryLimit < 0 {
		return settings{}, configurationError(fmt.Errorf("configuration read retry limit is negative: %d", retryLimit))
	}
	common, err := storeoptions.Apply(configurationError, opts...)
	if err != nil {
		return settings{}, err
	}
	if common.RetentionCount != nil {
		count := *common.RetentionCount
		common.RetentionCount = &count
	}
	return settings{
		journalTableName: cfg.JournalTableName, snapshotTableName: cfg.SnapshotTableName,
		headTableName: cfg.HeadTableName, snapshotHistoryIndexName: cfg.SnapshotHistoryIndexName,
		configurationReadRetryLimit: retryLimit, common: common,
	}, nil
}
