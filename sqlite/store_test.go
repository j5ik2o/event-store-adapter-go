package sqlite_test

import (
	"math"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lowercaseUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestInitializeCreatesRustLayoutInNewFile(t *testing.T) {
	path := dbPath(t)

	require.NoError(t, sqlite.Initialize(t.Context(), path, sqlite.Config{}, keepOption(t, 3)))

	require.FileExists(t, path)
	db := rawDB(t, path)
	for _, table := range []string{"journal", "head", "snapshot_current", "snapshot_history", "config"} {
		assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM pragma_table_list WHERE name = ? AND strict = 1", table), table)
	}
	assert.EqualValues(t, 0, queryInt(t, db, "SELECT count(*) FROM sqlite_schema WHERE name = 'head_transition'"))
	var storeID, mode string
	var layout, keep, feed int64
	require.NoError(t, db.QueryRow("SELECT store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled FROM config WHERE singleton = 1").Scan(&storeID, &layout, &keep, &mode, &feed))
	assert.Regexp(t, lowercaseUUID, storeID)
	assert.EqualValues(t, 1, layout)
	assert.EqualValues(t, 3, keep)
	assert.Equal(t, "delete", mode)
	assert.EqualValues(t, 0, feed)
	var journalMode string
	require.NoError(t, db.QueryRow("PRAGMA journal_mode").Scan(&journalMode))
	assert.Equal(t, "wal", journalMode)
}

func TestInitializeWithoutRetentionStoresNullCount(t *testing.T) {
	path := initialized(t)

	var keepIsNull bool
	require.NoError(t, rawDB(t, path).QueryRow("SELECT keep_snapshot_count IS NULL FROM config").Scan(&keepIsNull))
	assert.True(t, keepIsNull)
}

func TestInitializeGeneratesDistinctStoreIDs(t *testing.T) {
	var ids []string
	for range 2 {
		path := initialized(t)
		var id string
		require.NoError(t, rawDB(t, path).QueryRow("SELECT store_id FROM config").Scan(&id))
		ids = append(ids, id)
	}
	assert.NotEqual(t, ids[0], ids[1])
}

func TestInitializeRejectsInitializedFileWithoutChangingIt(t *testing.T) {
	path := initialized(t)
	before := schemaFingerprint(t, path)

	err := sqlite.Initialize(t.Context(), path, sqlite.Config{})

	requireKind(t, err, eventstore.KindConfiguration)
	assert.Equal(t, before, schemaFingerprint(t, path))
}

func TestInitializeRejectsDatabaseWithApplicationObjects(t *testing.T) {
	path := dbPath(t)
	db := rawDB(t, path)
	_, err := db.Exec("CREATE TABLE unrelated (x INTEGER)")
	require.NoError(t, err)

	err = sqlite.Initialize(t.Context(), path, sqlite.Config{})

	requireKind(t, err, eventstore.KindConfiguration)
	assert.EqualValues(t, 0, queryInt(t, db, "SELECT count(*) FROM sqlite_schema WHERE name IN ('config', 'journal')"))
}

func TestInitializeAcceptsEmptyExistingDatabase(t *testing.T) {
	path := dbPath(t)
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	require.NoError(t, sqlite.Initialize(t.Context(), path, sqlite.Config{}))

	assert.EqualValues(t, 1, queryInt(t, rawDB(t, path), "SELECT count(*) FROM config"))
}

func TestInitializeRejectsUnsupportedSettingsBeforeCreatingFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  sqlite.Config
		opts []eventstore.Option
	}{
		{"ttl retention", sqlite.Config{}, []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL)}},
		{"ttl retention with count", sqlite.Config{}, []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 2)}},
		{"change feed", sqlite.Config{ChangeFeed: true}, nil},
		{"negative lock wait", sqlite.Config{LockWait: durationPtr(-time.Millisecond)}, nil},
		{"lock wait over int32 milliseconds", sqlite.Config{LockWait: durationPtr(time.Duration(math.MaxInt32+1) * time.Millisecond)}, nil},
		{"nil option", sqlite.Config{}, []eventstore.Option{nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := dbPath(t)

			err := sqlite.Initialize(t.Context(), path, tc.cfg, tc.opts...)

			requireKind(t, err, eventstore.KindConfiguration)
			assert.NoFileExists(t, path)
		})
	}
}

func durationPtr(d time.Duration) *time.Duration { return &d }

func TestOpenDoesNotCreateMissingFile(t *testing.T) {
	path := dbPath(t)

	store, err := sqlite.Open(t.Context(), path, sqlite.Config{})

	requireKind(t, err, eventstore.KindStorage)
	assert.Nil(t, store)
	assert.NoFileExists(t, path)
}

func TestOpenDoesNotInitializeEmptyDatabase(t *testing.T) {
	path := dbPath(t)
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	_, err := sqlite.Open(t.Context(), path, sqlite.Config{})

	requireKind(t, err, eventstore.KindConfiguration)
	assert.EqualValues(t, 0, queryInt(t, rawDB(t, path), "SELECT count(*) FROM sqlite_schema"))
}

func TestOpenRejectsUnsupportedSettings(t *testing.T) {
	path := initialized(t)
	before := schemaFingerprint(t, path)
	for _, tc := range []struct {
		name string
		cfg  sqlite.Config
		opts []eventstore.Option
	}{
		{"ttl retention", sqlite.Config{}, []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL)}},
		{"change feed", sqlite.Config{ChangeFeed: true}, nil},
		{"negative lock wait", sqlite.Config{LockWait: durationPtr(-time.Second)}, nil},
		{"lock wait over int32 milliseconds", sqlite.Config{LockWait: durationPtr(time.Duration(math.MaxInt32+1) * time.Millisecond)}, nil},
		{"retention count differs from the file", sqlite.Config{}, []eventstore.Option{keepOption(t, 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlite.Open(t.Context(), path, tc.cfg, tc.opts...)

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, store)
		})
	}
	assert.Equal(t, before, schemaFingerprint(t, path))
}

func TestOpenAcceptsRecordedRetentionCount(t *testing.T) {
	path := initialized(t, keepOption(t, 2))

	store := openStore(t, path, sqlite.Config{}, keepOption(t, 2))

	require.NotNil(t, store)
}

func TestOpenAcceptsZeroLockWait(t *testing.T) {
	path := initialized(t)

	store := openStore(t, path, sqlite.Config{LockWait: durationPtr(0)})

	require.NotNil(t, store)
}

func TestCloseIsIdempotentAndLaterOperationsFailAsStorageErrors(t *testing.T) {
	path := initialized(t)
	store, err := sqlite.Open(t.Context(), path, sqlite.Config{})
	require.NoError(t, err)
	entry := newEntry(t, store)
	id := aggregate(t, "Account", "close")

	require.NoError(t, store.Close())
	require.NoError(t, store.Close())

	requireKind(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "x")), eventstore.KindStorage)
	_, err = entry.GetLatestSnapshotByID(t.Context(), id)
	requireKind(t, err, eventstore.KindStorage)
	_, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	requireKind(t, err, eventstore.KindStorage)
}

func TestReopenKeepsEventsAndSnapshots(t *testing.T) {
	path := initialized(t)
	id := aggregate(t, "Account", "reopen")
	first, err := sqlite.Open(t.Context(), path, sqlite.Config{})
	require.NoError(t, err)
	entry := newEntry(t, first)
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state-1")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))
	require.NoError(t, first.Close())

	second := openStore(t, path, sqlite.Config{})
	reopened := newEntry(t, second)

	latest, err := reopened.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, eventstore.SeqNr(2), latest.HeadSeqNr)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, eventstore.SeqNr(1), latest.Snapshot.SeqNr())
	assert.Equal(t, "state-1", latest.Snapshot.Aggregate())
	events, err := reopened.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "one", events[0].Payload())
	assert.Equal(t, "two", events[1].Payload())
	assert.Equal(t, "event/v1", events[1].Manifest())
	assert.True(t, time.Unix(1720000002, 123456789).Equal(events[1].OccurredAt()))
	require.NoError(t, reopened.PersistEvent(t.Context(), eventAt(t, id, 3, "three")))
}

func TestNewRejectsNilStore(t *testing.T) {
	entry, err := sqlite.New[string, string](nil, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

	requireKind(t, err, eventstore.KindConfiguration)
	assert.Nil(t, entry)
}

func TestConnectionEnablesWriteAheadLogForFileInUse(t *testing.T) {
	path := initialized(t)
	store := openStore(t, path, sqlite.Config{})
	_ = store

	var journalMode string
	require.NoError(t, rawDB(t, path).QueryRow("PRAGMA journal_mode").Scan(&journalMode))
	assert.Equal(t, "wal", journalMode)
}

func TestConnectionConvertsRollbackJournalFileToWriteAheadLog(t *testing.T) {
	path := dbPath(t)
	db := rawDB(t, path)
	var mode string
	require.NoError(t, db.QueryRow("PRAGMA journal_mode=DELETE").Scan(&mode))
	for _, statement := range rustLayout().statements() {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	openStore(t, path, sqlite.Config{})

	require.NoError(t, rawDB(t, path).QueryRow("PRAGMA journal_mode").Scan(&mode))
	assert.Equal(t, "wal", mode)
}

func TestOpenRejects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(l *layoutFile)
	}{
		{"journal is not STRICT", func(l *layoutFile) { l.journal = strings.TrimSuffix(l.journal, "STRICT") }},
		{"head is not STRICT", func(l *layoutFile) { l.head = strings.TrimSuffix(l.head, "STRICT") }},
		{"snapshot_current is not STRICT", func(l *layoutFile) { l.current = strings.TrimSuffix(l.current, "STRICT") }},
		{"snapshot_history is not STRICT", func(l *layoutFile) { l.history = strings.TrimSuffix(l.history, "STRICT") }},
		{"config is not STRICT", func(l *layoutFile) { l.config = strings.TrimSuffix(l.config, "STRICT") }},
		{"head lacks event_payload", func(l *layoutFile) {
			l.head = strings.Replace(l.head, ",\n       event_payload BLOB NOT NULL", "", 1)
		}},
		{"journal column has the wrong type", func(l *layoutFile) {
			l.journal = strings.Replace(l.journal, "payload BLOB NOT NULL", "payload TEXT NOT NULL", 1)
		}},
		{"journal primary key lacks seq_nr", func(l *layoutFile) {
			l.journal = strings.Replace(l.journal, "PRIMARY KEY (aid, seq_nr)", "PRIMARY KEY (aid)", 1)
		}},
		{"snapshot_history lacks a table", func(l *layoutFile) { l.history = "" }},
		{"layout_version is not 1", func(l *layoutFile) {
			l.configRow = "INSERT INTO config VALUES (1, '" + validStoreID + "', 2, NULL, 'delete', 0)"
		}},
		{"recorded retention count differs", func(l *layoutFile) {
			l.configRow = "INSERT INTO config VALUES (1, '" + validStoreID + "', 1, 5, 'delete', 0)"
		}},
		{"change_feed_enabled is 1", func(l *layoutFile) {
			l.configRow = "INSERT INTO config VALUES (1, '" + validStoreID + "', 1, NULL, 'delete', 1)"
		}},
		{"store_id has uppercase letters", func(l *layoutFile) {
			l.configRow = "INSERT INTO config VALUES (1, '" + strings.ToUpper(validStoreID) + "', 1, NULL, 'delete', 0)"
		}},
		{"store_id lacks hyphens", func(l *layoutFile) {
			l.configRow = "INSERT INTO config VALUES (1, '" + strings.ReplaceAll(validStoreID, "-", "") + "', 1, NULL, 'delete', 0)"
		}},
		{"config has no row", func(l *layoutFile) { l.configRow = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout := rustLayout()
			tc.mutate(&layout)
			path := dbPath(t)
			writeLayoutFile(t, path, layout)

			store, err := sqlite.Open(t.Context(), path, sqlite.Config{})

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, store)
		})
	}
}

func TestOpenAcceptsHandBuiltRustLayout(t *testing.T) {
	path := dbPath(t)
	writeLayoutFile(t, path, rustLayout())

	store := openStore(t, path, sqlite.Config{})

	entry := newEntry(t, store)
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Account", "hand"), 1, "x")))
}

func withoutAidCollation(l layoutFile) layoutFile {
	const declared = "aid TEXT COLLATE BINARY NOT NULL"
	l.journal = strings.Replace(l.journal, declared, "aid TEXT NOT NULL", 1)
	l.head = strings.Replace(l.head, declared, "aid TEXT NOT NULL", 1)
	l.current = strings.Replace(l.current, declared, "aid TEXT NOT NULL", 1)
	l.history = strings.Replace(l.history, declared, "aid TEXT NOT NULL", 1)
	return l
}

func TestAidCollationAcceptsDefaultBinary(t *testing.T) {
	layout := withoutAidCollation(rustLayout())
	require.NotContains(t, layout.journal+layout.head+layout.current+layout.history, "COLLATE")
	path := dbPath(t)
	writeLayoutFile(t, path, layout)
	store := openStore(t, path, sqlite.Config{})
	entry := newEntry(t, store)
	upper, lower := aggregate(t, "User", "A"), aggregate(t, "User", "a")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, upper, 1, "upper")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, lower, 1, "lower")))

	assert.EqualValues(t, 2, queryInt(t, rawDB(t, path), "SELECT count(*) FROM head"))
	for id, want := range map[eventstore.AggregateID]string{upper: "upper", lower: "lower"} {
		events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, want, events[0].Payload())
	}
}

func TestAidCollationRejectsMisconfiguredFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(l *layoutFile)
	}{
		{"column is NOCASE and only the primary key index is BINARY", func(l *layoutFile) {
			l.journal = strings.Replace(l.journal, "aid TEXT COLLATE BINARY NOT NULL", "aid TEXT COLLATE NOCASE NOT NULL", 1)
			l.journal = strings.Replace(l.journal, "PRIMARY KEY (aid, seq_nr)", "PRIMARY KEY (aid COLLATE BINARY, seq_nr)", 1)
		}},
		{"column is RTRIM and only the primary key index is BINARY", func(l *layoutFile) {
			l.head = strings.Replace(l.head, "aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY", "aid TEXT COLLATE RTRIM NOT NULL", 1)
			l.head = strings.Replace(l.head, "event_payload BLOB NOT NULL\n     ) STRICT", "event_payload BLOB NOT NULL,\n       PRIMARY KEY (aid COLLATE BINARY)\n     ) STRICT", 1)
		}},
		{"column is BINARY and the primary key index is NOCASE", func(l *layoutFile) {
			l.current = strings.Replace(l.current, "aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY", "aid TEXT NOT NULL", 1)
			l.current = strings.Replace(l.current, "payload BLOB NOT NULL\n     ) STRICT", "payload BLOB NOT NULL,\n       PRIMARY KEY (aid COLLATE NOCASE)\n     ) STRICT", 1)
		}},
		{"history column is NOCASE and its index is BINARY", func(l *layoutFile) {
			l.history = strings.Replace(l.history, "aid TEXT COLLATE BINARY NOT NULL", "aid TEXT COLLATE NOCASE NOT NULL", 1)
			l.history = strings.Replace(l.history, "PRIMARY KEY (aid, seq_nr)", "PRIMARY KEY (aid COLLATE BINARY, seq_nr)", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout := rustLayout()
			tc.mutate(&layout)
			path := dbPath(t)
			writeLayoutFile(t, path, layout)
			before := schemaFingerprint(t, path)

			store, err := sqlite.Open(t.Context(), path, sqlite.Config{})

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, store)
			assert.Equal(t, before, schemaFingerprint(t, path), "validation must not modify the file")
		})
	}
}
