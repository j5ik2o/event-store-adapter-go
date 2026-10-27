package bigtable_test

import (
	"context"
	"testing"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recordedStore(t *testing.T, l *layout) (eventstore.EventStore[string, string], *requestRecorder) {
	t.Helper()
	recorder := &requestRecorder{}
	data := newDataClient(t, connect(t, interceptors(recorder, nil, nil)...))
	es, ss := serializers()
	entry, err := bigtable.New(t.Context(), data, l.admin, l.config(), es, ss, l.options(t)...)
	require.NoError(t, err)
	return entry, recorder
}

func TestReadReadsTheHeadOnceAndTheJournalOnlyBelowIt(t *testing.T) {
	l := provision(t, layoutSpec{})
	writer := l.newEntry(t)
	id := aggregate(t, "Account", "ranges")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, writer.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
	}
	for _, tc := range []struct {
		name      string
		start     eventstore.SeqNr
		wantCount int
		wantReads int
	}{
		{"start below the head reads the head and the journal range", 1, 3, 2},
		{"start at the head returns the last event from the head only", 3, 1, 1},
		{"start above the head returns nothing without a journal request", 4, 0, 1},
		{"start 0 behaves as 1", 0, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, recorder := recordedStore(t, l)
			before := recorder.countOf("ReadRows")

			events, err := reader.GetEventsByIDSinceSeqNr(t.Context(), id, tc.start)

			require.NoError(t, err)
			assert.Len(t, events, tc.wantCount)
			assert.Equal(t, tc.wantReads, recorder.countOf("ReadRows")-before)
		})
	}
	t.Run("an aggregate without a head returns an empty list after one request", func(t *testing.T) {
		reader, recorder := recordedStore(t, l)
		before := recorder.countOf("ReadRows")

		events, err := reader.GetEventsByIDSinceSeqNr(t.Context(), aggregate(t, "Account", "unknown"), 1)

		require.NoError(t, err)
		assert.Empty(t, events)
		assert.Equal(t, 1, recorder.countOf("ReadRows")-before)
	})
}

func TestReadDoesNotMixInEventsCommittedAfterTheHeadWasRead(t *testing.T) {
	l := provision(t, layoutSpec{})
	writer := l.newEntry(t)
	id := aggregate(t, "Account", "advance")
	for seq := eventstore.SeqNr(1); seq <= 2; seq++ {
		require.NoError(t, writer.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
	}
	hooks := testhook.New()
	advanced := false
	hooks.OnFail(testhook.PhaseReadEvents, func(testhook.Point) error {
		if !advanced {
			advanced = true
			require.NoError(t, writer.PersistEvent(context.Background(), eventAt(t, id, 3, "late")))
			require.NoError(t, writer.PersistEvent(context.Background(), eventAt(t, id, 4, "later")))
		}
		return nil
	})
	reader := l.newEntry(t, hooksOption(hooks))

	events, err := reader.GetEventsByIDSinceSeqNr(t.Context(), id, 1)

	require.NoError(t, err)
	require.True(t, advanced, "the hook right after the head read must actually run")
	require.Len(t, events, 2, "the call keeps the head it read first")
	assert.Equal(t, eventstore.SeqNr(2), events[1].SeqNr())
}

func TestReadReportsAMissingJournalRowAsAStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "hole")
	for seq := eventstore.SeqNr(1); seq <= 4; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
	}
	mutation := gbigtable.NewMutation()
	mutation.DeleteRow()
	require.NoError(t, l.data.Open(l.journal).Apply(t.Context(), journalKey(aidOf(t, id), 2), mutation))

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)

	requireKind(t, err, eventstore.KindStorage)
	assert.Nil(t, events)
}

func TestReadReportsAMalformedHeadOrEnvelopeAsAStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "corrupt")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "x")))
	mutation := gbigtable.NewMutation()
	mutation.DeleteCellsInColumn("head", "last_event")
	require.NoError(t, l.data.Open(l.head).Apply(t.Context(), string(rowPrefix(aidOf(t, id))), mutation))

	_, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	requireKind(t, err, eventstore.KindStorage)
	_, err = entry.GetLatestSnapshotByID(t.Context(), id)
	requireKind(t, err, eventstore.KindStorage)
}

func TestReadFailsWhileDeserializingAPayloadAsASerializationError(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "serde")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "x")))
	numbers, err := bigtable.New(t.Context(), l.data, l.admin, l.config(), eventstore.NewJSONSerializer[int](), eventstore.NewJSONSerializer[int](), l.options(t)...)
	require.NoError(t, err)

	_, err = numbers.GetEventsByIDSinceSeqNr(t.Context(), id, 1)

	requireKind(t, err, eventstore.KindSerialization)
}
