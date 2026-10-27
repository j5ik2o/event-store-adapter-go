package spanner_test

import (
	"context"
	"fmt"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSnapshotAndHeadComeFromOnePointInTime(t *testing.T) {
	l := provision(t, layoutSpec{})
	hooks := testhook.New()
	entry := l.newEntry(t, hooksOption(hooks))
	writer := l.store(t, newClient(t))
	id := aggregate(t, "Account", "snap-read")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state-1")))
	interleavedEvent, interleavedSnapshot := eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2")
	calls := 0
	hooks.OnFail(testhook.PhaseReadSnapshot, func(testhook.Point) error {
		calls++
		if calls == 1 {
			// Another client commits a new head and snapshot after the head was read and before the snapshot is read.
			assert.NoError(t, writer.PersistEventAndSnapshot(context.Background(), interleavedEvent, interleavedSnapshot))
		}
		return nil
	})

	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)

	require.NoError(t, err)
	require.NotNil(t, latest)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, 1, calls, "the read-snapshot hook must be reached during the read")
	assert.Equal(t, latest.HeadSeqNr, latest.Snapshot.SeqNr(), "head and snapshot must come from the same read time")
	assert.Equal(t, fmt.Sprintf("state-%d", latest.HeadSeqNr), latest.Snapshot.Aggregate())
}

func TestReadEventsReturnContiguousRangeUpToTheHeadOfOneReadTime(t *testing.T) {
	l := provision(t, layoutSpec{})
	hooks := testhook.New()
	entry := l.newEntry(t, hooksOption(hooks))
	writer := l.store(t, newClient(t))
	id := aggregate(t, "Account", "events-read")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))
	appended := eventAt(t, id, 3, "three")
	calls := 0
	hooks.OnFail(testhook.PhaseReadEvents, func(testhook.Point) error {
		calls++
		if calls == 1 {
			assert.NoError(t, writer.PersistEvent(context.Background(), appended))
		}
		return nil
	})

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)

	require.NoError(t, err)
	assert.Equal(t, 1, calls, "the read-events hook must be reached during the read")
	require.Len(t, events, 2, "the head of the read time is 2: the event committed in between is not part of this read")
	for i, event := range events {
		assert.Equal(t, eventstore.SeqNr(i+1), event.SeqNr(), "events must be contiguous from the requested number")
	}
}

func TestReadEventsFromHeadAndBeyond(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "since")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq))))
	}

	atHead, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 3)
	require.NoError(t, err)
	require.Len(t, atHead, 1)
	beyond, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 4)
	require.NoError(t, err)
	assert.Empty(t, beyond)
	unknown, err := entry.GetEventsByIDSinceSeqNr(t.Context(), aggregate(t, "Account", "none"), 0)
	require.NoError(t, err)
	assert.Empty(t, unknown)
}

func deleteRows(t *testing.T, l *layout, sql string, params map[string]any) {
	t.Helper()
	execute(t, l.raw, sql, params)
}

func TestReadRejectsJournalGapAsStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "missing-row")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq))))
	}
	deleteRows(t, l, "DELETE FROM "+l.table("journal")+" WHERE aid = 'Account-missing-row' AND seq_nr = 2", nil)

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)

	requireKind(t, err, eventstore.KindStorage)
	assert.Nil(t, events, "a shortened result must not be returned as a success")
}

func TestReadRejectsHeadWithoutJournalRowAsStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "short-tail")
	for seq := eventstore.SeqNr(1); seq <= 2; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq))))
	}
	deleteRows(t, l, "DELETE FROM "+l.table("journal")+" WHERE aid = 'Account-short-tail' AND seq_nr = 2", nil)

	_, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)

	requireKind(t, err, eventstore.KindStorage)
}

func TestReadRejectsStoredSequenceNumbersOutOfRangeAsStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "range")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))
	execute(t, l.raw, "UPDATE "+l.table("head")+" SET seq_nr = 0 WHERE aid = 'Account-range'", nil)

	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)

	requireKind(t, err, eventstore.KindStorage)
	assert.Nil(t, latest)
	_, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	requireKind(t, err, eventstore.KindStorage)
}

func TestReadFailsAsStorageErrorWhenTheDestinationIsGone(t *testing.T) {
	l := provision(t, layoutSpec{})
	client, err := database.NewClient(t.Context())
	require.NoError(t, err)
	entry := l.store(t, client)
	id := aggregate(t, "Account", "gone")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	client.Close()

	_, err = entry.GetLatestSnapshotByID(t.Context(), id)
	requireKind(t, err, eventstore.KindStorage)
	_, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	requireKind(t, err, eventstore.KindStorage)
	err = entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two"))
	requireKind(t, err, eventstore.KindStorage)
}

func TestReadSeesWritesOfAnotherClientImmediately(t *testing.T) {
	l := provision(t, layoutSpec{})
	reader, writer := l.store(t, newClient(t)), l.store(t, newClient(t))
	id := aggregate(t, "Account", "strong")

	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, writer.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
		latest, err := reader.GetLatestSnapshotByID(t.Context(), id)
		require.NoError(t, err)
		require.NotNil(t, latest, "a strong read observes the write that just returned")
		assert.Equal(t, seq, latest.HeadSeqNr)
	}
}
