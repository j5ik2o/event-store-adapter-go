package spanner_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// javascriptDDL is the DDL of the JavaScript v5 implementation (packages/library/docs/SPANNER_DATABASE_SCHEMA.md),
// copied here as text so that the test does not share any definition with the Go side. graceDays > 0 selects the TTL
// definition of the history table.
func javascriptDDL(prefix string, graceDays int64) []string {
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

// referenceHashPrefix computes the key prefix with the standard library, independently of the implementation under test:
// FNV-1a 64 of the UTF-8 aid, as 8 big-endian bytes.
func referenceHashPrefix(aid string) []byte {
	h := fnv.New64a()
	_, _ = h.Write([]byte(aid))
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, h.Sum64())
	return out
}

func execute(t *testing.T, client *gspanner.Client, sql string, params map[string]any) {
	t.Helper()
	statement := gspanner.NewStatement(sql)
	statement.Params = params
	_, err := client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *gspanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, statement)
		return err
	})
	require.NoError(t, err, sql)
}

// provisionJavaScriptLayout builds the destination exactly as the JavaScript documentation does (the DDL, then the
// configuration row with GENERATE_UUID()), without any helper of the Go side.
func provisionJavaScriptLayout(t *testing.T, keep int, graceDays int64) *layout {
	t.Helper()
	prefix := fmt.Sprintf("j%d", prefixCount.Add(1))
	require.NoError(t, database.ApplyDDL(t.Context(), javascriptDDL(prefix, graceDays)...))
	raw := newClient(t)
	mode, grace := "delete", "NULL"
	if graceDays > 0 {
		mode, grace = "ttl", fmt.Sprint(graceDays)
	}
	execute(t, raw, fmt.Sprintf("INSERT INTO %s_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled) VALUES (1, GENERATE_UUID(), 1, %d, '%s', %s, FALSE)",
		prefix, keep, mode, grace), nil)
	return &layout{prefix: prefix, raw: raw, spec: layoutSpec{keep: ptr(int64(keep)), graceDays: graceDays}}
}

func TestJSFormatGoReadsRowsWrittenByJavaScript(t *testing.T) {
	l := provisionJavaScriptLayout(t, 2, 0)
	aid := "Account-js"
	hp := referenceHashPrefix(aid)
	occurred := time.UnixMilli(1720000000123).UTC()
	nanos := occurred.UnixMilli() * 1_000_000 // the JavaScript implementation writes epoch milliseconds times 1,000,000
	key := map[string]any{"hp": hp, "aid": aid}
	insert := func(sql string, extra map[string]any) {
		params := map[string]any{}
		for k, v := range key {
			params[k] = v
		}
		for k, v := range extra {
			params[k] = v
		}
		execute(t, l.raw, strings.ReplaceAll(sql, "esa_", l.prefix+"_"), params)
	}
	for seq := int64(1); seq <= 2; seq++ {
		insert("INSERT INTO esa_journal (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, @aid, @seq, @nanos, @manifest, @payload)",
			map[string]any{"seq": seq, "nanos": nanos, "manifest": "event/v1", "payload": []byte(fmt.Sprintf(`"js-%d"`, seq))})
	}
	insert("INSERT INTO esa_head (hash_prefix, aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload) VALUES (@hp, @aid, @type_name, @seq, @nanos, @manifest, @payload)",
		map[string]any{"type_name": "Account", "seq": int64(2), "nanos": nanos, "manifest": "event/v1", "payload": []byte(`"js-2"`)})
	insert("INSERT INTO esa_snapshot_current (hash_prefix, aid, seq_nr, manifest, payload) VALUES (@hp, @aid, @seq, @manifest, @payload)",
		map[string]any{"seq": int64(2), "manifest": "snapshot/v1", "payload": []byte(`"js-state-2"`)})
	insert("INSERT INTO esa_snapshot_history (hash_prefix, aid, seq_nr, manifest, payload, expired_at) VALUES (@hp, @aid, @seq, @manifest, @payload, NULL)",
		map[string]any{"seq": int64(2), "manifest": "snapshot/v1", "payload": []byte(`"js-state-2"`)})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "js")

	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, eventstore.SeqNr(2), latest.HeadSeqNr)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, "js-state-2", latest.Snapshot.Aggregate())
	assert.Equal(t, "snapshot/v1", latest.Snapshot.Manifest())
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, []string{"js-1", "js-2"}, []string{events[0].Payload(), events[1].Payload()})
	assert.True(t, events[0].OccurredAt().Equal(occurred), "occurred time is read back from INT64 nanoseconds: %v", events[0].OccurredAt())

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 3, "go-3"), snapshotAt(t, 3, "go-state-3")),
		"the Go implementation continues a JavaScript-written stream without a gap or a conflict")
	assert.EqualValues(t, 3, l.count(t, "journal"))
	assert.Equal(t, []int64{2, 3}, l.historySeqNrs(t, aid), "retention is applied over rows the JavaScript implementation wrote")
}

func TestJSFormatGoWritesRowsThatJavaScriptReads(t *testing.T) {
	l := provisionJavaScriptLayout(t, 2, 0)
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "go")
	aid := "Account-go"
	occurred := time.UnixMilli(1720000000123).UTC()
	event, err := eventstore.NewEventEnvelope(id, 1, occurred, "go-event", eventstore.WithManifest("event/v1"))
	require.NoError(t, err)

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), event, snapshotAt(t, 1, "go-state")))

	// The statements below are the JavaScript implementation's read statements (spanner-get-events.ts and
	// spanner-get-latest-snapshot.ts): exact (hash_prefix, aid) comparison with the reference hash.
	hp := referenceHashPrefix(aid)
	read := func(sql string) *gspanner.Row {
		statement := gspanner.NewStatement(strings.ReplaceAll(sql, "esa_", l.prefix+"_"))
		statement.Params = map[string]any{"hp": hp, "aid": aid}
		rows := l.raw.Single().Query(t.Context(), statement)
		defer rows.Stop()
		row, err := rows.Next()
		require.NoError(t, err, sql)
		return row
	}
	var storedPrefix []byte
	var storedAid, typeName, manifest string
	var seq, nanos int64
	var payload []byte
	require.NoError(t, read("SELECT hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload FROM esa_journal WHERE hash_prefix = @hp AND aid = @aid").
		Columns(&storedPrefix, &storedAid, &seq, &nanos, &manifest, &payload))
	assert.Equal(t, hp, storedPrefix, "hash_prefix is FNV-1a 64 as 8 big-endian bytes")
	assert.Equal(t, aid, storedAid)
	assert.EqualValues(t, 1, seq)
	assert.EqualValues(t, 1720000000123*1_000_000, nanos, "occurred_at_ns is INT64 nanoseconds, equal to epoch milliseconds times 1,000,000")
	assert.Equal(t, "event/v1", manifest)
	assert.Equal(t, `"go-event"`, string(payload))

	var headSeq int64
	require.NoError(t, read("SELECT type_name, seq_nr FROM esa_head WHERE hash_prefix = @hp AND aid = @aid").Columns(&typeName, &headSeq))
	assert.Equal(t, "Account", typeName)
	assert.EqualValues(t, 1, headSeq)
	var currentManifest string
	var currentPayload []byte
	require.NoError(t, read("SELECT manifest, payload FROM esa_snapshot_current WHERE hash_prefix = @hp AND aid = @aid").Columns(&currentManifest, &currentPayload))
	assert.Equal(t, "snapshot/v1", currentManifest)
	assert.Equal(t, `"go-state"`, string(currentPayload))
	var expiredAt gspanner.NullTime
	require.NoError(t, read("SELECT expired_at FROM esa_snapshot_history WHERE hash_prefix = @hp AND aid = @aid").Columns(&expiredAt))
	assert.False(t, expiredAt.Valid, "a history row written by the Go implementation is unmarked")
}

func TestJSFormatTTLLayoutIsAcceptedAndMarkedRowsAreWrittenWithATimestamp(t *testing.T) {
	l := provisionJavaScriptLayout(t, 1, 7)
	entry, err := l.tryOpen(l.raw, spanner.Config{TablePrefix: l.prefix, TTLGraceDays: 7}, ttlOptions(t)...)
	require.NoError(t, err)
	id := aggregate(t, "Account", "ttl")

	persistSnapshots(t, entry, id, 1, 3)

	aid := "Account-ttl"
	assert.Equal(t, []int64{3}, l.unmarkedHistorySeqNrs(t, aid), "only the newest row stays unmarked")
	assert.Equal(t, []int64{1, 2, 3}, l.historySeqNrs(t, aid), "TTL mode marks rows; Spanner removes them later")
	marked := queryInts(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_history")+" WHERE aid = @aid AND expired_at IS NOT NULL ORDER BY seq_nr", map[string]any{"aid": aid})
	assert.Equal(t, []int64{1, 2}, marked)
}
