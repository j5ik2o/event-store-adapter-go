package spanner

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	gspanner "cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type column struct{ name, typ string }

type expectedTable struct {
	columns    []column
	primaryKey []string
}

var (
	keyColumns = []column{{"hash_prefix", "BYTES(8)"}, {"aid", "STRING(1024)"}}
	seqColumn  = column{"seq_nr", "INT64"}
	manifest   = column{"manifest", "STRING(MAX)"}
	payload    = column{"payload", "BYTES(MAX)"}
)

func withKey(rest ...column) []column {
	return append(append([]column{}, keyColumns...), rest...)
}

// logicalTables lists the five tables in the order they are reported.
var logicalTables = []string{"journal", "head", "snapshotCurrent", "snapshotHistory", "config"}

// expected is the layout of the JavaScript v5 implementation: the same columns and primary keys.
var expected = map[string]expectedTable{
	"journal": {
		columns:    withKey(seqColumn, column{"occurred_at_ns", "INT64"}, manifest, payload),
		primaryKey: []string{"hash_prefix", "aid", "seq_nr"},
	},
	"head": {
		columns: withKey(column{"type_name", "STRING(1024)"}, seqColumn, column{"event_occurred_at_ns", "INT64"},
			column{"event_manifest", "STRING(MAX)"}, column{"event_payload", "BYTES(MAX)"}),
		primaryKey: []string{"hash_prefix", "aid"},
	},
	"snapshotCurrent": {
		columns:    withKey(seqColumn, manifest, payload),
		primaryKey: []string{"hash_prefix", "aid"},
	},
	"snapshotHistory": {
		columns:    withKey(seqColumn, manifest, payload, column{"expired_at", "TIMESTAMP"}),
		primaryKey: []string{"hash_prefix", "aid", "seq_nr"},
	},
	"config": {
		columns: []column{{"singleton", "INT64"}, {"store_id", "STRING(36)"}, {"layout_version", "INT64"},
			{"keep_snapshot_count", "INT64"}, {"retention_mode", "STRING(8)"}, {"ttl_grace_days", "INT64"},
			{"change_feed_enabled", "BOOL"}},
		primaryKey: []string{"singleton"},
	},
}

func (t tableNames) byLogical(logical string) string {
	switch logical {
	case "journal":
		return t.journal
	case "head":
		return t.head
	case "snapshotCurrent":
		return t.snapshotCurrent
	case "snapshotHistory":
		return t.snapshotHistory
	default:
		return t.config
	}
}

// configRow is the single row of the config table.
type configRow struct {
	storeID           gspanner.NullString
	layoutVersion     gspanner.NullInt64
	keepSnapshotCount gspanner.NullInt64
	retentionMode     gspanner.NullString
	ttlGraceDays      gspanner.NullInt64
	changeFeed        gspanner.NullBool
}

// observedLayout is what the destination holds, read in one strongly consistent read-only transaction.
type observedLayout struct {
	dialect           string
	columns           map[string][]column
	primaryKeys       map[string][]string
	rowDeletionPolicy *string
	config            *configRow
}

var (
	lowercaseUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	olderThanDays = regexp.MustCompile(`(?i)^OLDER_THAN\(\s*expired_at\s*,\s*INTERVAL\s+(\d+)\s+DAY\s*\)$`)
)

func mismatch(format string, args ...any) error {
	return configurationError(fmt.Errorf("Spanner layout mismatch: "+format, args...))
}

// reconcile compares the observed destination with the client settings (SPN-2, SPN-16, SPN-17). It is pure.
func reconcile(observed observedLayout, s settings) error {
	if observed.dialect != "GOOGLE_STANDARD_SQL" {
		return mismatch("the database dialect must be GoogleSQL, got %q", observed.dialect)
	}
	for _, logical := range logicalTables {
		want := expected[logical]
		have := observed.columns[logical]
		for _, c := range want.columns {
			found := false
			for _, h := range have {
				if h == c {
					found = true
					break
				}
			}
			if !found {
				return mismatch("%s must have column %s %s", logical, c.name, c.typ)
			}
		}
		key := observed.primaryKeys[logical]
		if len(key) != len(want.primaryKey) {
			return mismatch("%s primary key does not match", logical)
		}
		for i := range key {
			if key[i] != want.primaryKey[i] {
				return mismatch("%s primary key does not match", logical)
			}
		}
	}
	row := observed.config
	if row == nil {
		return mismatch("the config row is missing")
	}
	if !row.storeID.Valid || !lowercaseUUID.MatchString(row.storeID.StringVal) {
		return mismatch("store_id is not a lowercase UUID")
	}
	if !row.layoutVersion.Valid || row.layoutVersion.Int64 != 1 {
		return mismatch("layout_version must be 1")
	}
	var wantKeep *int64
	if s.common.RetentionCount != nil {
		n := int64(*s.common.RetentionCount)
		wantKeep = &n
	}
	if row.keepSnapshotCount.Valid != (wantKeep != nil) || (wantKeep != nil && row.keepSnapshotCount.Int64 != *wantKeep) {
		return mismatch("keep_snapshot_count differs from the retention count of the client")
	}
	if !row.retentionMode.Valid || (row.retentionMode.StringVal != "delete" && row.retentionMode.StringVal != "ttl") {
		return mismatch("retention_mode must be delete or ttl")
	}
	wantMode := "delete"
	if s.ttl() {
		wantMode = "ttl"
	}
	if row.retentionMode.StringVal != wantMode {
		return mismatch("retention_mode differs from the retention mode of the client")
	}
	if !row.changeFeed.Valid {
		return mismatch("change_feed_enabled must be FALSE, not NULL")
	}
	if row.changeFeed.Bool {
		return configurationError(errors.New("the config row enables a change feed, which this storage does not support"))
	}
	policy := ""
	if observed.rowDeletionPolicy != nil {
		policy = *observed.rowDeletionPolicy
	}
	if s.ttl() {
		if !row.ttlGraceDays.Valid || row.ttlGraceDays.Int64 != s.graceDays {
			return mismatch("ttl_grace_days differs from Config.TTLGraceDays")
		}
		match := olderThanDays.FindStringSubmatch(policy)
		if match == nil {
			return mismatch("the history table has no matching row deletion policy")
		}
		days, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || days != s.graceDays {
			return mismatch("the row deletion policy days differ from Config.TTLGraceDays")
		}
		return nil
	}
	if row.ttlGraceDays.Valid {
		return mismatch("ttl_grace_days must be NULL in delete mode")
	}
	if observed.rowDeletionPolicy != nil {
		return mismatch("the history table must not have a row deletion policy in delete mode")
	}
	return nil
}

// observe reads the dialect, the columns, the primary keys, the row deletion policy and the config row in one
// strongly consistent read-only transaction. A failure is a storage error.
func observe(ctx context.Context, client *gspanner.Client, tables tableNames) (observedLayout, error) {
	tx := client.ReadOnlyTransaction()
	defer tx.Close()
	result := observedLayout{columns: map[string][]column{}, primaryKeys: map[string][]string{}}
	byName := map[string]string{}
	var names []string
	for _, logical := range logicalTables {
		byName[tables.byLogical(logical)] = logical
		names = append(names, tables.byLogical(logical))
	}
	each := func(statement gspanner.Statement, f func(*gspanner.Row) error) error {
		rows := tx.Query(ctx, statement)
		defer rows.Stop()
		for {
			row, err := rows.Next()
			if errors.Is(err, iterator.Done) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := f(row); err != nil {
				return err
			}
		}
	}
	read := func() error {
		if err := each(gspanner.NewStatement("SELECT option_value FROM information_schema.database_options WHERE option_name = 'database_dialect'"), func(row *gspanner.Row) error {
			var value gspanner.NullString
			if err := row.Columns(&value); err != nil {
				return err
			}
			result.dialect = value.StringVal
			return nil
		}); err != nil {
			return err
		}
		columns := gspanner.Statement{SQL: "SELECT table_name, column_name, spanner_type FROM information_schema.columns WHERE table_schema = '' AND table_name IN UNNEST(@tables) ORDER BY table_name, ordinal_position", Params: map[string]any{"tables": names}}
		if err := each(columns, func(row *gspanner.Row) error {
			var table, name, typ string
			if err := row.Columns(&table, &name, &typ); err != nil {
				return err
			}
			result.columns[byName[table]] = append(result.columns[byName[table]], column{name, typ})
			return nil
		}); err != nil {
			return err
		}
		keys := gspanner.Statement{SQL: "SELECT table_name, column_name FROM information_schema.index_columns WHERE index_name = 'PRIMARY_KEY' AND table_schema = '' AND table_name IN UNNEST(@tables) ORDER BY table_name, ordinal_position", Params: map[string]any{"tables": names}}
		if err := each(keys, func(row *gspanner.Row) error {
			var table, name string
			if err := row.Columns(&table, &name); err != nil {
				return err
			}
			result.primaryKeys[byName[table]] = append(result.primaryKeys[byName[table]], name)
			return nil
		}); err != nil {
			return err
		}
		policy := gspanner.Statement{SQL: "SELECT row_deletion_policy_expression FROM information_schema.tables WHERE table_schema = '' AND table_name = @table", Params: map[string]any{"table": tables.snapshotHistory}}
		if err := each(policy, func(row *gspanner.Row) error {
			var value gspanner.NullString
			if err := row.Columns(&value); err != nil {
				return err
			}
			if value.Valid {
				result.rowDeletionPolicy = &value.StringVal
			}
			return nil
		}); err != nil {
			return err
		}
		// The config row is read only when its columns are complete; otherwise the column check reports it.
		complete := true
		for _, want := range expected["config"].columns {
			found := false
			for _, have := range result.columns["config"] {
				found = found || have == want
			}
			complete = complete && found
		}
		if !complete {
			return nil
		}
		return each(gspanner.NewStatement("SELECT store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled FROM `"+tables.config+"` WHERE singleton = 1"), func(row *gspanner.Row) error {
			var c configRow
			if err := row.Columns(&c.storeID, &c.layoutVersion, &c.keepSnapshotCount, &c.retentionMode, &c.ttlGraceDays, &c.changeFeed); err != nil {
				return err
			}
			result.config = &c
			return nil
		})
	}
	if err := read(); err != nil {
		return observedLayout{}, storageError(fmt.Errorf("the Spanner layout could not be read: %w", err))
	}
	return result, nil
}
