package bigtable_test

import (
	"context"
	"encoding/binary"
	"testing"

	gbigtable "cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rustEventEnvelope builds the event envelope of the Rust implementation by hand (BT-D6a), without the library.
func rustEventEnvelope(aid string, seq uint64, occurredNs int64, manifest string, payload []byte) []byte {
	out := []byte{1}
	out = binary.BigEndian.AppendUint16(out, uint16(len(aid)))
	out = append(out, aid...)
	out = binary.BigEndian.AppendUint64(out, seq)
	out = binary.BigEndian.AppendUint64(out, uint64(occurredNs))
	out = binary.BigEndian.AppendUint32(out, uint32(len(manifest)))
	out = append(out, manifest...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

func rustSnapshotEnvelope(seq uint64, manifest string, payload []byte) []byte {
	out := binary.BigEndian.AppendUint64([]byte{1}, seq)
	out = binary.BigEndian.AppendUint32(out, uint32(len(manifest)))
	out = append(out, manifest...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

func setCell(m *gbigtable.Mutation, family, column string, value []byte) {
	m.Set(family, column, 0, value)
}

// TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended: a destination written in the Rust v5 layout is read
// and appended to by the Go store.
func TestRustFormatRowsWrittenByTheRustLayoutAreReadAndExtended(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	aid := "Account-rust"
	ctx := context.Background()
	// Journal row 1 and head at 2, as the Rust store leaves them.
	journalRow := gbigtable.NewMutation()
	setCell(journalRow, "event", "envelope", rustEventEnvelope(aid, 1, 1_000_000_001, "event/v1", []byte(`"from-rust-1"`)))
	setCell(journalRow, "event", "has_snapshot", []byte("1"))
	setCell(journalRow, "history", "state", []byte("active"))
	setCell(journalRow, "history", "envelope", rustSnapshotEnvelope(1, "snapshot/v1", []byte(`"rust-state-1"`)))
	require.NoError(t, l.data.Open(l.journal).Apply(ctx, journalKey(aid, 1), journalRow))
	headRow := gbigtable.NewMutation()
	setCell(headRow, "head", "seq_nr", []byte("00000000000000000002"))
	setCell(headRow, "head", "last_event", rustEventEnvelope(aid, 2, 2_000_000_002, "event/v1", []byte(`"from-rust-2"`)))
	setCell(headRow, "head", "has_snapshot", []byte("1"))
	setCell(headRow, "snapshot", "current", rustSnapshotEnvelope(2, "snapshot/v1", []byte(`"rust-state-2"`)))
	require.NoError(t, l.data.Open(l.head).Apply(ctx, string(rowPrefix(aid)), headRow))
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "rust")

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, []string{"from-rust-1", "from-rust-2"}, []string{events[0].Payload(), events[1].Payload()})
	assert.EqualValues(t, 1_000_000_001, events[0].OccurredAt().UnixNano())
	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, "rust-state-2", latest.Snapshot.Aggregate())

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 3, "go-3"), snapshotAt(t, 3, "go-state-3")))
	events, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 3)
}

// TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells pins the bytes of the Go-written rows.
func TestRustFormatRowsWrittenByTheGoStoreHaveTheRustKeysAndCells(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "go")
	aid := aidOf(t, id)
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state")))

	journal := cells(t, l.data, l.journal, journalKey(aid, 2))
	head := cells(t, l.data, l.head, string(rowPrefix(aid)))

	wantEvent := rustEventEnvelope(aid, 2, eventAt(t, id, 2, "").OccurredAt().UnixNano(), "event/v1", []byte(`"two"`))
	assert.Equal(t, string(wantEvent), journal["event:envelope"])
	assert.Equal(t, "1", journal["event:has_snapshot"])
	assert.Equal(t, "active", journal["history:state"])
	assert.Equal(t, string(rustSnapshotEnvelope(2, "snapshot/v1", []byte(`"state"`))), journal["history:envelope"])
	assert.Equal(t, string(wantEvent), head["head:last_event"])
	assert.Equal(t, "00000000000000000002", head["head:seq_nr"])
	assert.Equal(t, "1", head["head:has_snapshot"])
	assert.Equal(t, string(rustSnapshotEnvelope(2, "snapshot/v1", []byte(`"state"`))), head["snapshot:current"])
}
