// Package sqlite provides an event store backed by one local SQLite file in the
// five-table layout of the v5 SQLite profile.
//
// Initialize creates the layout in a new file or an empty database. Open never
// initializes: it verifies the file and returns a Store that owns one connection.
// New connects payload serializers to a Store.
package sqlite

import (
	"errors"
	"fmt"
	"math"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
)

// defaultLockWait is how long a connection waits for a lock held by another connection.
const defaultLockWait = 5 * time.Second

// Config holds the SQLite-specific settings. Common settings (retention) are passed as eventstore.Option.
type Config struct {
	// LockWait is the busy wait for the write lock. Nil means 5 seconds, zero means no wait,
	// and a negative value or more than math.MaxInt32 milliseconds is a configuration error.
	LockWait *time.Duration
	// ChangeFeed asks for a change feed. This storage does not provide one, so true is a configuration error (SQ-20).
	ChangeFeed bool
}

// settings owns the validated values.
type settings struct {
	lockWait time.Duration
	common   storeoptions.Options
}

// keepSnapshotCount returns the retention count as stored in config.keep_snapshot_count (nil means no history).
func (s settings) keepSnapshotCount() *int64 {
	if s.common.RetentionCount == nil {
		return nil
	}
	count := int64(*s.common.RetentionCount)
	return &count
}

// validateConfig checks every setting before the file is touched.
func validateConfig(cfg Config, opts ...eventstore.Option) (settings, error) {
	configurationError := func(cause error) error {
		return &eventstore.ConfigurationError{Cause: cause}
	}
	common, err := storeoptions.Apply(configurationError, opts...)
	if err != nil {
		return settings{}, err
	}
	switch common.RetentionMode {
	case storeoptions.RetentionDelete:
	case storeoptions.RetentionTTL:
		return settings{}, configurationError(errors.New("sqlite does not support TTL retention"))
	default:
		return settings{}, configurationError(fmt.Errorf("invalid retention mode: %d", common.RetentionMode))
	}
	if common.RetentionCount != nil {
		count := *common.RetentionCount
		common.RetentionCount = &count
	}
	if cfg.ChangeFeed {
		return settings{}, configurationError(errors.New("sqlite does not support a change feed"))
	}
	wait := defaultLockWait
	if cfg.LockWait != nil {
		wait = *cfg.LockWait
	}
	if wait < 0 || wait.Milliseconds() > math.MaxInt32 {
		return settings{}, configurationError(fmt.Errorf("lock wait out of range: %s", wait))
	}
	return settings{lockWait: wait, common: common}, nil
}
