package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // driver "sqlite": used only to inspect and prepare real database files from outside the Store.
)

// The five statements below are copied from the Rust v5 implementation
// (lib/src/sqlite/layout.rs:11-48). Tests build files in the Rust layout from them.
const (
	seqNrColumn = "seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991)"

	journalDDL = `CREATE TABLE journal (
       aid TEXT COLLATE BINARY NOT NULL,
       ` + seqNrColumn + `,
       occurred_at_ns INTEGER NOT NULL,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT`
	headDDL = `CREATE TABLE head (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       type_name TEXT NOT NULL,
       ` + seqNrColumn + `,
       event_occurred_at_ns INTEGER NOT NULL,
       event_manifest TEXT NOT NULL,
       event_payload BLOB NOT NULL
     ) STRICT`
	snapshotCurrentDDL = `CREATE TABLE snapshot_current (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       ` + seqNrColumn + `,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL
     ) STRICT`
	snapshotHistoryDDL = `CREATE TABLE snapshot_history (
       aid TEXT COLLATE BINARY NOT NULL,
       ` + seqNrColumn + `,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT`
	configDDL = `CREATE TABLE config (
       singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
       store_id TEXT NOT NULL,
       layout_version INTEGER NOT NULL,
       keep_snapshot_count INTEGER CHECK (keep_snapshot_count IS NULL OR keep_snapshot_count >= 1),
       retention_mode TEXT NOT NULL CHECK (retention_mode = 'delete'),
       change_feed_enabled INTEGER NOT NULL CHECK (change_feed_enabled IN (0, 1))
     ) STRICT`

	validStoreID = "6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b"
)

// layoutFile is the content of a hand-built database file: the DDL of the five tables and the config row.
type layoutFile struct {
	journal, head, current, history, config string
	configRow                               string
}

func rustLayout() layoutFile {
	return layoutFile{
		journal: journalDDL, head: headDDL, current: snapshotCurrentDDL, history: snapshotHistoryDDL, config: configDDL,
		configRow: fmt.Sprintf("INSERT INTO config VALUES (1, '%s', 1, NULL, 'delete', 0)", validStoreID),
	}
}

func (l layoutFile) statements() []string {
	statements := []string{l.journal, l.head, l.current, l.history, l.config}
	if l.configRow != "" {
		statements = append(statements, l.configRow)
	}
	return statements
}

// writeLayoutFile creates a WAL database at path with exactly the given definitions. It uses no Store code.
func writeLayoutFile(t *testing.T, path string, layout layoutFile) {
	t.Helper()
	db := rawDB(t, path)
	_, err := db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	for _, statement := range layout.statements() {
		if statement == "" {
			continue
		}
		_, err := db.Exec(statement)
		require.NoError(t, err, statement)
	}
	require.NoError(t, db.Close())
}

// rawDB opens a connection that is independent of any Store. Callers use it to prepare, hold locks and inspect.
func rawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "events.db")
}

func initialized(t *testing.T, opts ...eventstore.Option) string {
	t.Helper()
	path := dbPath(t)
	require.NoError(t, sqlite.Initialize(t.Context(), path, sqlite.Config{}, opts...))
	return path
}

func openStore(t *testing.T, path string, cfg sqlite.Config, opts ...eventstore.Option) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(t.Context(), path, cfg, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newEntry(t *testing.T, store *sqlite.Store) eventstore.EventStore[string, string] {
	t.Helper()
	entry, err := sqlite.New(store, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	require.NoError(t, err)
	return entry
}

func keepOption(t *testing.T, n int) eventstore.Option {
	t.Helper()
	count, err := eventstore.KeepLatest(n)
	require.NoError(t, err)
	return eventstore.WithRetentionCount(count)
}

func hooksOption(h *testhook.Hooks) eventstore.Option {
	return func(o *storeoptions.Options) error {
		o.Hooks = h
		return nil
	}
}

func aggregate(t *testing.T, typeName, value string) eventstore.AggregateID {
	t.Helper()
	id, err := eventstore.NewAggregateID(typeName, value)
	require.NoError(t, err)
	return id
}

func eventAt(t *testing.T, id eventstore.AggregateID, seq eventstore.SeqNr, payload string) eventstore.EventEnvelope[string] {
	t.Helper()
	event, err := eventstore.NewEventEnvelope(id, seq, time.Unix(1720000000+int64(seq), 123456789), payload,
		eventstore.WithManifest("event/v1"))
	require.NoError(t, err)
	return event
}

func snapshotAt(t *testing.T, seq eventstore.SeqNr, state string) eventstore.SnapshotEnvelope[string] {
	t.Helper()
	snapshot, err := eventstore.NewSnapshotEnvelope(state, seq, eventstore.WithManifest("snapshot/v1"))
	require.NoError(t, err)
	return snapshot
}

func requireKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok, "error must use the existing classification: %v", err)
	require.Equal(t, expected, kind, "%v", err)
}

func queryInt(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRowContext(context.Background(), query, args...).Scan(&n), query)
	return n
}

func queryInts(t *testing.T, db *sql.DB, query string, args ...any) []int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		out = append(out, n)
	}
	require.NoError(t, rows.Err())
	return out
}

// schemaFingerprint returns every schema definition and every config row, to prove that opening wrote nothing.
func schemaFingerprint(t *testing.T, path string) string {
	t.Helper()
	db := rawDB(t, path)
	var parts []string
	rows, err := db.Query("SELECT type, name, coalesce(sql, '') FROM sqlite_schema ORDER BY name")
	require.NoError(t, err)
	for rows.Next() {
		var kind, name, definition string
		require.NoError(t, rows.Scan(&kind, &name, &definition))
		parts = append(parts, kind+"|"+name+"|"+definition)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	if queryInt(t, db, "SELECT count(*) FROM sqlite_schema WHERE name = 'config'") == 1 {
		rows, err = db.Query("SELECT singleton, store_id, layout_version, coalesce(keep_snapshot_count, -1), retention_mode, change_feed_enabled FROM config")
		require.NoError(t, err)
		for rows.Next() {
			var a, c, d, f int64
			var b, e string
			require.NoError(t, rows.Scan(&a, &b, &c, &d, &e, &f))
			parts = append(parts, fmt.Sprint("config|", a, b, c, d, e, f))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return strings.Join(parts, "\n")
}
