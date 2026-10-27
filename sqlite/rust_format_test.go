package sqlite_test

import (
	"strings"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The SQL below is copied from the Rust v5 implementation (lib/src/sqlite/backend.rs).
// The Rust binary is not run: the reference copy is read-only, so the DDL and the statements are mirrored here.
const (
	rustInsertJournal   = "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (?1, ?2, ?3, ?4, ?5)"
	rustInsertHead      = "INSERT INTO head (aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
	rustUpsertCurrent   = "INSERT INTO snapshot_current (aid, seq_nr, manifest, payload) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(aid) DO UPDATE SET seq_nr = excluded.seq_nr, manifest = excluded.manifest, payload = excluded.payload"
	rustInsertHistory   = "INSERT INTO snapshot_history (aid, seq_nr, manifest, payload) VALUES (?1, ?2, ?3, ?4)"
	rustReadEventsSQL   = "SELECT seq_nr, occurred_at_ns, manifest, payload FROM journal WHERE aid = ?1 AND seq_nr >= ?2 AND seq_nr <= ?3 ORDER BY seq_nr"
	rustConfigInsert    = "INSERT INTO config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled) VALUES (1, ?1, 1, ?2, 'delete', 0)"
	rustHeadSnapshotSQL = "SELECT h.seq_nr, c.seq_nr, c.manifest, c.payload FROM head h LEFT JOIN snapshot_current c ON c.aid = h.aid WHERE h.aid = ?1"
)

func normalizeSQL(s string) string { return strings.Join(strings.Fields(s), " ") }

func TestRustFormatInitializedTablesMatchRustDefinitions(t *testing.T) {
	path := initialized(t)
	db := rawDB(t, path)
	for table, want := range map[string]string{
		"journal": journalDDL, "head": headDDL, "snapshot_current": snapshotCurrentDDL,
		"snapshot_history": snapshotHistoryDDL, "config": configDDL,
	} {
		var definition string
		require.NoError(t, db.QueryRow("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?", table).Scan(&definition))
		assert.Equal(t, normalizeSQL(want), normalizeSQL(definition), table)
	}
	assert.EqualValues(t, 5, queryInt(t, db, "SELECT count(*) FROM sqlite_schema WHERE type = 'table'"))
}

func TestRustFormatGoReadsAndAppendsToRustWrittenRows(t *testing.T) {
	path := dbPath(t)
	db := rawDB(t, path)
	_, err := db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	for _, statement := range []string{journalDDL, headDDL, snapshotCurrentDDL, snapshotHistoryDDL, configDDL} {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}
	_, err = db.Exec(rustConfigInsert, validStoreID, 2)
	require.NoError(t, err)
	occurred := time.Date(2026, 10, 26, 1, 2, 3, 456789012, time.UTC)
	aid := "RustAccount-r1"
	for seq, payload := range map[int64]string{1: `"first"`, 2: `"second"`} {
		_, err = db.Exec(rustInsertJournal, aid, seq, occurred.UnixNano()+seq, "rust/event", []byte(payload))
		require.NoError(t, err)
	}
	_, err = db.Exec(rustInsertHead, aid, "RustAccount", 2, occurred.UnixNano()+2, "rust/event", []byte(`"second"`))
	require.NoError(t, err)
	_, err = db.Exec(rustUpsertCurrent, aid, 2, "rust/snapshot", []byte(`"rust-state"`))
	require.NoError(t, err)
	_, err = db.Exec(rustInsertHistory, aid, 2, "rust/snapshot", []byte(`"rust-state"`))
	require.NoError(t, err)

	entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 2)))
	id := aggregate(t, "RustAccount", "r1")

	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, eventstore.SeqNr(2), latest.HeadSeqNr)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, "rust-state", latest.Snapshot.Aggregate())
	assert.Equal(t, "rust/snapshot", latest.Snapshot.Manifest())
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "first", events[0].Payload())
	assert.Equal(t, "rust/event", events[1].Manifest())
	assert.True(t, time.Unix(0, occurred.UnixNano()+2).Equal(events[1].OccurredAt()))

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 3, "go-third"), snapshotAt(t, 3, "go-state")))
	assert.Equal(t, []int64{2, 3}, historySeqNrs(t, path, aid))
}

func TestRustFormatGoWrittenRowsAreReadableBySameStatementsAsRust(t *testing.T) {
	path := initialized(t, keepOption(t, 2))
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 2)))
	id := aggregate(t, "GoAccount", "g1")
	event := eventAt(t, id, 1, "go-one")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), event, snapshotAt(t, 1, "go-state")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "go-two")))
	db := rawDB(t, path)

	rows, err := db.Query(rustReadEventsSQL, "GoAccount-g1", 1, 2)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var seq, ns int64
		var manifest string
		var payload []byte
		require.NoError(t, rows.Scan(&seq, &ns, &manifest, &payload))
		assert.Equal(t, event.OccurredAt().UnixNano()-int64(1)*int64(time.Second)+seq*int64(time.Second), ns, "occurred_at_ns is Unix nanoseconds")
		assert.Equal(t, "event/v1", manifest)
		got = append(got, string(payload))
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{`"go-one"`, `"go-two"`}, got, "payloads are the serializer's bytes stored as BLOB")

	var headSeq, snapSeq int64
	var snapManifest string
	var snapPayload []byte
	require.NoError(t, db.QueryRow(rustHeadSnapshotSQL, "GoAccount-g1").Scan(&headSeq, &snapSeq, &snapManifest, &snapPayload))
	assert.EqualValues(t, 2, headSeq)
	assert.EqualValues(t, 1, snapSeq)
	assert.Equal(t, "snapshot/v1", snapManifest)
	assert.Equal(t, `"go-state"`, string(snapPayload))
	var typeName string
	require.NoError(t, db.QueryRow("SELECT type_name FROM head WHERE aid = 'GoAccount-g1'").Scan(&typeName))
	assert.Equal(t, "GoAccount", typeName)
	assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM snapshot_history WHERE aid = 'GoAccount-g1'"))
	assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM pragma_table_list WHERE name = 'journal' AND strict = 1"))
}
