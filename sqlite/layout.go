package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
)

// seqNrColumn is the seq_nr column: 1 through 2^53-1 (SQ-16). The definitions below follow the
// Rust v5 layout word for word so that both languages write the same file format.
const seqNrColumn = "seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991)"

// createTableStatements creates the five STRICT tables. head_transition is not created (SQ-20).
var createTableStatements = []string{
	`CREATE TABLE journal (
       aid TEXT COLLATE BINARY NOT NULL,
       ` + seqNrColumn + `,
       occurred_at_ns INTEGER NOT NULL,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT`,
	`CREATE TABLE head (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       type_name TEXT NOT NULL,
       ` + seqNrColumn + `,
       event_occurred_at_ns INTEGER NOT NULL,
       event_manifest TEXT NOT NULL,
       event_payload BLOB NOT NULL
     ) STRICT`,
	`CREATE TABLE snapshot_current (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       ` + seqNrColumn + `,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL
     ) STRICT`,
	`CREATE TABLE snapshot_history (
       aid TEXT COLLATE BINARY NOT NULL,
       ` + seqNrColumn + `,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT`,
	`CREATE TABLE config (
       singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
       store_id TEXT NOT NULL,
       layout_version INTEGER NOT NULL,
       keep_snapshot_count INTEGER CHECK (keep_snapshot_count IS NULL OR keep_snapshot_count >= 1),
       retention_mode TEXT NOT NULL CHECK (retention_mode = 'delete'),
       change_feed_enabled INTEGER NOT NULL CHECK (change_feed_enabled IN (0, 1))
     ) STRICT`,
}

// column is the expectation for one column: name, type, NOT NULL (0 or 1) and the position in the primary key (0 if none).
type column struct {
	name, typ     string
	notNull, pkAt int64
}

type expectedTable struct {
	name    string
	columns []column
}

var expectedTables = []expectedTable{
	{"journal", []column{{"aid", "TEXT", 1, 1}, {"seq_nr", "INTEGER", 1, 2}, {"occurred_at_ns", "INTEGER", 1, 0}, {"manifest", "TEXT", 1, 0}, {"payload", "BLOB", 1, 0}}},
	{"head", []column{{"aid", "TEXT", 1, 1}, {"type_name", "TEXT", 1, 0}, {"seq_nr", "INTEGER", 1, 0}, {"event_occurred_at_ns", "INTEGER", 1, 0}, {"event_manifest", "TEXT", 1, 0}, {"event_payload", "BLOB", 1, 0}}},
	{"snapshot_current", []column{{"aid", "TEXT", 1, 1}, {"seq_nr", "INTEGER", 1, 0}, {"manifest", "TEXT", 1, 0}, {"payload", "BLOB", 1, 0}}},
	{"snapshot_history", []column{{"aid", "TEXT", 1, 1}, {"seq_nr", "INTEGER", 1, 2}, {"manifest", "TEXT", 1, 0}, {"payload", "BLOB", 1, 0}}},
	{"config", []column{{"singleton", "INTEGER", 1, 1}, {"store_id", "TEXT", 1, 0}, {"layout_version", "INTEGER", 1, 0}, {"keep_snapshot_count", "INTEGER", 0, 0}, {"retention_mode", "TEXT", 1, 0}, {"change_feed_enabled", "INTEGER", 1, 0}}},
}

func layoutMismatch(detail string, args ...any) error {
	return &eventstore.ConfigurationError{Cause: fmt.Errorf("SQLite layout mismatch: "+detail, args...)}
}

// isUninitialized reports whether the database has no application table, index, view or trigger (SQ-D6).
func isUninitialized(ctx context.Context, q queryer) (bool, error) {
	var objects int64
	if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE lower(name) NOT GLOB 'sqlite_*'").Scan(&objects); err != nil {
		return false, err
	}
	return objects == 0, nil
}

// queryer is implemented by *sql.Conn.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// verifyLayout checks that the five tables are STRICT, that the columns and primary keys are as expected, and that aid
// is compared as BINARY both by its column declaration and by its primary key index. It never writes and never repairs.
func verifyLayout(ctx context.Context, q queryer) error {
	for _, table := range expectedTables {
		var strict int64
		err := q.QueryRowContext(ctx, `SELECT "strict" FROM pragma_table_list WHERE schema = 'main' AND name = ?`, table.name).Scan(&strict)
		if errors.Is(err, sql.ErrNoRows) {
			return layoutMismatch("table %s is missing", table.name)
		}
		if err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		if strict != 1 {
			return layoutMismatch("table %s is not STRICT", table.name)
		}
		actual, err := readColumns(ctx, q, table.name)
		if err != nil {
			return err
		}
		if len(actual) != len(table.columns) {
			return layoutMismatch("table %s has %d columns", table.name, len(actual))
		}
		hasAid := false
		for i, want := range table.columns {
			if actual[i] != want {
				return layoutMismatch("column %d of table %s differs", i, table.name)
			}
			hasAid = hasAid || want.name == "aid"
		}
		if hasAid {
			binary, err := aidCollationsAreBinary(ctx, q, table.name)
			if err != nil {
				return &eventstore.StorageError{Cause: err}
			}
			if !binary {
				return layoutMismatch("aid of table %s is not compared as BINARY", table.name)
			}
		}
	}
	return nil
}

func readColumns(ctx context.Context, q queryer, table string) ([]column, error) {
	rows, err := q.QueryContext(ctx, `SELECT name, type, "notnull", pk FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	defer rows.Close()
	var columns []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typ, &c.notNull, &c.pkAt); err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		columns = append(columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	return columns, nil
}

// aidCollationsAreBinary checks the declared collation of the aid column and the collation of the primary key index.
// Both decide uniqueness and the meaning of "aid = ?", so both must be BINARY.
//
// pragma_table_info does not return the collation and the driver does not expose sqlite3_table_column_metadata.
// The declared collation is therefore observed with a compound SELECT that reads no stored row and writes nothing:
// the ORDER BY of a compound SELECT uses the collation of the column of its left-most SELECT, which is the aid column.
// Two probe pairs separate BINARY from NOCASE ('a' and 'A') and from RTRIM ('x ' and 'x').
func aidCollationsAreBinary(ctx context.Context, q queryer, table string) (bool, error) {
	// table is always one of the constants in expectedTables.
	probe := `SELECT aid, 0 AS ordinal FROM "` + table + `" WHERE 0 UNION ALL SELECT ?, 0 UNION ALL SELECT ?, 1 ORDER BY aid, ordinal`
	for _, pair := range [][2]string{{"a", "A"}, {"x ", "x"}} {
		var first string
		var ordinal int64
		if err := q.QueryRowContext(ctx, probe, pair[0], pair[1]).Scan(&first, &ordinal); err != nil {
			return false, err
		}
		if ordinal != 1 {
			return false, nil
		}
	}
	rows, err := q.QueryContext(ctx, `SELECT x.coll FROM pragma_index_list(?, 'main') AS l, pragma_index_xinfo(l.name, 'main') AS x
	  WHERE l.origin = 'pk' AND x.key = 1 AND x.name = 'aid'`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var collations []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		collations = append(collations, name)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return len(collations) == 1 && strings.EqualFold(collations[0], "BINARY"), nil
}
