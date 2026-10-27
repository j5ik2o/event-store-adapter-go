package spannertest

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
)

// ConfigRow is the single row of <prefix>_config that an operator writes after the DDL.
type ConfigRow struct {
	StoreID           string
	LayoutVersion     int64
	KeepSnapshotCount *int64
	RetentionMode     string
	TTLGraceDays      *int64
	ChangeFeedEnabled bool
}

// LayoutDDL returns the DDL of the five tables for prefix, in the layout of the JavaScript v5 implementation.
// graceDays > 0 adds the row deletion policy of TTL mode to the history table; 0 creates the delete mode layout.
func LayoutDDL(prefix string, graceDays int64) []string {
	history := `CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr)`
	if graceDays > 0 {
		history += fmt.Sprintf(",\n  ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL %d DAY))", graceDays)
	}
	statements := []string{
		`CREATE TABLE esa_journal (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  occurred_at_ns INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid, seq_nr)`,
		`CREATE TABLE esa_head (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  type_name STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  event_occurred_at_ns INT64 NOT NULL,
  event_manifest STRING(MAX) NOT NULL,
  event_payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid)`,
		`CREATE TABLE esa_snapshot_current (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid)`,
		history,
		`CREATE TABLE esa_config (
  singleton INT64 NOT NULL,
  store_id STRING(36) NOT NULL,
  layout_version INT64 NOT NULL,
  keep_snapshot_count INT64,
  retention_mode STRING(8) NOT NULL,
  ttl_grace_days INT64,
  change_feed_enabled BOOL NOT NULL
) PRIMARY KEY (singleton)`,
	}
	for i := range statements {
		statements[i] = strings.ReplaceAll(statements[i], "esa_", prefix+"_")
	}
	return statements
}

// InsertConfig writes the configuration row of prefix.
func (d *Database) InsertConfig(ctx context.Context, prefix string, row ConfigRow) error {
	client, err := d.adminClient(ctx)
	if err != nil {
		return err
	}
	nullable := func(v *int64) spanner.NullInt64 {
		if v == nil {
			return spanner.NullInt64{}
		}
		return spanner.NullInt64{Int64: *v, Valid: true}
	}
	_, err = client.Apply(ctx, []*spanner.Mutation{spanner.InsertMap(prefix+"_config", map[string]any{
		"singleton": int64(1), "store_id": row.StoreID, "layout_version": row.LayoutVersion,
		"keep_snapshot_count": nullable(row.KeepSnapshotCount), "retention_mode": row.RetentionMode,
		"ttl_grace_days": nullable(row.TTLGraceDays), "change_feed_enabled": row.ChangeFeedEnabled,
	})})
	return err
}
