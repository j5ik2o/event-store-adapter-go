// Package spanner provides an event store on Cloud Spanner (GoogleSQL dialect) in the five-table layout of the v5
// storage profile. The tables and the configuration row are provisioned outside the library; New only verifies them.
package spanner

import (
	"errors"
	"fmt"
	"regexp"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
)

const (
	defaultTransactionRetryLimit = 5
	maxIdentifierLength          = 128
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config names the externally provisioned tables and the behavior of the Spanner transactions.
type Config struct {
	// TablePrefix derives the five table names <prefix>_journal, _head, _snapshot_current, _snapshot_history and
	// _config. The prefix and every derived name must be GoogleSQL identifiers of at most 128 characters.
	TablePrefix string
	// TTLGraceDays is the grace period in days of TTL retention. It must be at least 1 with
	// eventstore.RetentionTTL and 0 otherwise. It must equal ttl_grace_days of the config row and the
	// INTERVAL of the row deletion policy of the history table.
	TTLGraceDays int64
	// TransactionRetryLimit is the number of re-runs after an aborted transaction; the first attempt is not
	// counted. Nil means 5, zero disables re-runs, and a negative value is a configuration error.
	TransactionRetryLimit *int
	// ChangeFeed must be false: this version provides no change feed.
	ChangeFeed bool
}

// tableNames are the five tables derived from the prefix.
type tableNames struct {
	journal, head, snapshotCurrent, snapshotHistory, config string
}

type settings struct {
	tables     tableNames
	graceDays  int64
	retryLimit int
	common     storeoptions.Options
}

func configurationError(cause error) error {
	return &eventstore.ConfigurationError{Cause: cause}
}

func deriveTables(prefix string) (tableNames, error) {
	if !identifier.MatchString(prefix) {
		return tableNames{}, fmt.Errorf("table prefix %q is not a GoogleSQL identifier", prefix)
	}
	names := tableNames{
		journal: prefix + "_journal", head: prefix + "_head", snapshotCurrent: prefix + "_snapshot_current",
		snapshotHistory: prefix + "_snapshot_history", config: prefix + "_config",
	}
	for _, name := range []string{names.journal, names.head, names.snapshotCurrent, names.snapshotHistory, names.config} {
		if len(name) > maxIdentifierLength {
			return tableNames{}, fmt.Errorf("table name %q is longer than %d characters", name, maxIdentifierLength)
		}
	}
	return names, nil
}

// validate checks everything that does not need the destination. No request is sent.
func validate(client *gspanner.Client, cfg Config, opts ...eventstore.Option) (settings, error) {
	if client == nil {
		return settings{}, configurationError(errors.New("the Spanner client is nil"))
	}
	tables, err := deriveTables(cfg.TablePrefix)
	if err != nil {
		return settings{}, configurationError(err)
	}
	if cfg.ChangeFeed {
		return settings{}, configurationError(errors.New("a change feed is not supported by this storage"))
	}
	retryLimit := defaultTransactionRetryLimit
	if cfg.TransactionRetryLimit != nil {
		retryLimit = *cfg.TransactionRetryLimit
	}
	if retryLimit < 0 {
		return settings{}, configurationError(fmt.Errorf("transaction retry limit is negative: %d", retryLimit))
	}
	common, err := storeoptions.Apply(configurationError, opts...)
	if err != nil {
		return settings{}, err
	}
	if common.RetentionCount != nil {
		count := *common.RetentionCount
		common.RetentionCount = &count
	}
	if common.TTLGraceSeconds != 0 {
		return settings{}, configurationError(errors.New("TTL grace is configured in days with Config.TTLGraceDays, not with WithTTLGraceSeconds"))
	}
	ttl := common.RetentionCount != nil && common.RetentionMode == storeoptions.RetentionTTL
	switch {
	case common.RetentionCount == nil && common.RetentionMode == storeoptions.RetentionTTL:
		return settings{}, configurationError(errors.New("TTL retention needs a retention count"))
	case ttl && cfg.TTLGraceDays < 1:
		return settings{}, configurationError(fmt.Errorf("TTL retention needs Config.TTLGraceDays of at least 1: %d", cfg.TTLGraceDays))
	case !ttl && cfg.TTLGraceDays != 0:
		return settings{}, configurationError(fmt.Errorf("Config.TTLGraceDays is only for TTL retention: %d", cfg.TTLGraceDays))
	}
	return settings{tables: tables, graceDays: cfg.TTLGraceDays, retryLimit: retryLimit, common: common}, nil
}

func (s settings) ttl() bool {
	return s.common.RetentionCount != nil && s.common.RetentionMode == storeoptions.RetentionTTL
}
