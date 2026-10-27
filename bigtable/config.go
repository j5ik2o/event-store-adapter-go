// Package bigtable provides an event store on Cloud Bigtable in the two-table layout of the v5 storage profile
// (a journal table and a head table). The tables, the column families and the configuration rows are provisioned
// outside the library; New only verifies them, and Initialize only writes the configuration rows.
package bigtable

import (
	"context"
	"errors"
	"fmt"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
)

const (
	defaultEnvelopeLimitBytes = 1048576
	minEnvelopeLimitBytes     = 17
	maxEnvelopeLimitBytes     = 8388608
)

// Config names the externally provisioned tables and the settings that are recorded in their configuration rows.
type Config struct {
	// JournalTableID and HeadTableID are the two tables of one Bigtable instance. They must be different tables.
	JournalTableID, HeadTableID string
	// EnvelopeLimitBytes caps one serialized envelope (BT-16). Zero means 1048576; the range is 17 through 8388608.
	// It must equal envelope_limit_bytes of both configuration rows.
	EnvelopeLimitBytes int
	// ChangeFeed must be false: this version provides no change feed.
	ChangeFeed bool
	// JournalWriteFailureHandler is called when the write-out of a committed event to its journal row fails after the
	// commit point (BT-7 step 5). The append has already succeeded; the handler only reports. A panic in it is
	// recovered. The failure is also logged.
	JournalWriteFailureHandler func(context.Context, error)
}

type settings struct {
	journalTable, headTable string
	envelopeLimit           int
	journalWriteFailure     func(context.Context, error)
	common                  storeoptions.Options
}

func configurationError(cause error) error {
	return &eventstore.ConfigurationError{Cause: cause}
}

func storageError(cause error) error {
	return &eventstore.StorageError{Cause: cause}
}

// keepSnapshotCount is the history count recorded in the configuration rows; nil means "current snapshot only".
func (s settings) keepSnapshotCount() *int {
	if s.common.RetentionCount == nil {
		return nil
	}
	count := *s.common.RetentionCount
	return &count
}

// validate checks everything that does not need the destination. No request is sent.
func validate(data *gbigtable.Client, cfg Config, opts ...eventstore.Option) (settings, error) {
	if data == nil {
		return settings{}, configurationError(errors.New("the Bigtable data client is nil"))
	}
	if cfg.JournalTableID == "" || cfg.HeadTableID == "" {
		return settings{}, configurationError(errors.New("the journal and head table IDs are required"))
	}
	if cfg.JournalTableID == cfg.HeadTableID {
		return settings{}, configurationError(fmt.Errorf("the journal and head tables must differ: %q", cfg.JournalTableID))
	}
	if cfg.ChangeFeed {
		return settings{}, configurationError(errors.New("a change feed is not supported by this storage"))
	}
	limit := cfg.EnvelopeLimitBytes
	if limit == 0 {
		limit = defaultEnvelopeLimitBytes
	}
	if limit < minEnvelopeLimitBytes || limit > maxEnvelopeLimitBytes {
		return settings{}, configurationError(fmt.Errorf("the envelope limit is outside %d through %d: %d", minEnvelopeLimitBytes, maxEnvelopeLimitBytes, limit))
	}
	common, err := storeoptions.Apply(configurationError, opts...)
	if err != nil {
		return settings{}, err
	}
	if common.RetentionMode == storeoptions.RetentionTTL {
		return settings{}, configurationError(errors.New("bigtable does not support TTL retention"))
	}
	if common.TTLGraceSeconds != 0 {
		return settings{}, configurationError(errors.New("bigtable does not support a TTL grace period"))
	}
	if common.RetentionCount != nil {
		count := *common.RetentionCount
		common.RetentionCount = &count
	}
	return settings{journalTable: cfg.JournalTableID, headTable: cfg.HeadTableID, envelopeLimit: limit,
		journalWriteFailure: cfg.JournalWriteFailureHandler, common: common}, nil
}

var errNilAdmin = errors.New("the Bigtable admin client is nil")
