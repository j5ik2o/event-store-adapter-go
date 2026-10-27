package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSnapshotAndHeadComeFromOnePointInTime(t *testing.T) {
	path := initialized(t)
	hooks := testhook.New()
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, hooksOption(hooks)))
	writer := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "snap-read")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state-1")))
	interleavedEvent, interleavedSnapshot := eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2")
	calls := 0
	hooks.OnFail(testhook.PhaseReadSnapshot, func(testhook.Point) error {
		calls++
		if calls == 1 {
			// Another connection commits a new head and snapshot while the read is in progress.
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

func TestReadEventsReturnContiguousRangeUpToHeadOfOneReadTime(t *testing.T) {
	path := initialized(t)
	hooks := testhook.New()
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, hooksOption(hooks)))
	writer := newEntry(t, openStore(t, path, sqlite.Config{}))
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
	require.GreaterOrEqual(t, len(events), 2)
	for i, event := range events {
		assert.Equal(t, eventstore.SeqNr(i+1), event.SeqNr(), "events must be contiguous from the requested number")
	}
}

func TestReadEventsFromHeadAndBeyond(t *testing.T) {
	entry := newEntry(t, openStore(t, initialized(t), sqlite.Config{}))
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

func TestReadRejectsJournalGapAsStorageError(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "missing-row")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq))))
	}
	_, err := rawDB(t, path).Exec("DELETE FROM journal WHERE aid = 'Account-missing-row' AND seq_nr = 2")
	require.NoError(t, err)

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)

	requireKind(t, err, eventstore.KindStorage)
	assert.Nil(t, events, "a shortened result must not be returned as a success")
}

func TestReadRejectsHeadWithoutJournalRowAsStorageError(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "short-tail")
	for seq := eventstore.SeqNr(1); seq <= 2; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq))))
	}
	_, err := rawDB(t, path).Exec("DELETE FROM journal WHERE aid = 'Account-short-tail' AND seq_nr = 2")
	require.NoError(t, err)

	_, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)

	requireKind(t, err, eventstore.KindStorage)
}

func persistSnapshots(t *testing.T, entry eventstore.EventStore[string, string], id eventstore.AggregateID, from, to eventstore.SeqNr) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq)), snapshotAt(t, seq, fmt.Sprint("state-", seq))))
	}
}

func historySeqNrs(t *testing.T, path, aid string) []int64 {
	t.Helper()
	return queryInts(t, rawDB(t, path), "SELECT seq_nr FROM snapshot_history WHERE aid = ? ORDER BY seq_nr", aid)
}

func TestRetentionKeepsNewestHistoryRightAfterReturn(t *testing.T) {
	for _, tc := range []struct {
		name string
		keep int
		want []int64
	}{
		{"none", 0, []int64{}},
		{"one", 1, []int64{4}},
		{"two", 2, []int64{3, 4}},
		{"larger than the number of snapshots", 9, []int64{1, 2, 3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []eventstore.Option
			if tc.keep > 0 {
				opts = append(opts, keepOption(t, tc.keep))
			}
			path := initialized(t, opts...)
			entry := newEntry(t, openStore(t, path, sqlite.Config{}, opts...))
			id := aggregate(t, "Account", "retention")

			persistSnapshots(t, entry, id, 1, 4)

			assert.Equal(t, tc.want, historySeqNrs(t, path, "Account-retention"))
			assert.EqualValues(t, 4, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"), "events are never removed")
			latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			require.NotNil(t, latest.Snapshot)
			assert.Equal(t, eventstore.SeqNr(4), latest.Snapshot.SeqNr())
			assert.EqualValues(t, 4, queryInt(t, rawDB(t, path), "SELECT seq_nr FROM snapshot_current WHERE aid = 'Account-retention'"))
		})
	}
}

func TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates(t *testing.T) {
	path := initialized(t, keepOption(t, 2))
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 2)))
	id, other := aggregate(t, "Account", "history"), aggregate(t, "Account", "history-extra")
	persistSnapshots(t, entry, id, 1, 3)
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 4, "event only")))
	assert.Equal(t, []int64{2, 3}, historySeqNrs(t, path, "Account-history"), "an event-only append must not touch history")

	persistSnapshots(t, entry, other, 1, 1)

	assert.Equal(t, []int64{2, 3}, historySeqNrs(t, path, "Account-history"))
	assert.Equal(t, []int64{1}, historySeqNrs(t, path, "Account-history-extra"))
}

func TestRetentionDeletesOldestSnapshotsThroughHook(t *testing.T) {
	hooks := testhook.New()
	path := initialized(t, keepOption(t, 3))
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 3), hooksOption(hooks)))
	var deleted []int64
	hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
		deleted = append(deleted, pt.SeqNrs...)
		return nil
	})

	persistSnapshots(t, entry, aggregate(t, "Account", "hook"), 1, 6)

	slices.Sort(deleted)
	assert.Equal(t, []int64{1, 2, 3}, deleted)
	assert.Equal(t, []int64{4, 5, 6}, historySeqNrs(t, path, "Account-hook"))
}

type captureHandler struct {
	mu      sync.Mutex
	records int
	panics  bool
}

func (*captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	h.records++
	h.mu.Unlock()
	if h.panics {
		panic(errors.New("assertion failed inside the logging path"))
	}
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }
func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records
}

func captureLog(t *testing.T, panics bool) *captureHandler {
	t.Helper()
	previous := slog.Default()
	handler := &captureHandler{panics: panics}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func TestRetentionFailureKeepsCommittedAppendAndNotifies(t *testing.T) {
	for _, phase := range []testhook.Phase{testhook.PhaseRetentionQuery, testhook.PhaseRetentionDelete} {
		t.Run(string(phase), func(t *testing.T) {
			log := captureLog(t, false)
			hooks := testhook.New()
			injected := &testhook.InjectedError{Phase: phase, Message: "retention refused"}
			armed := false
			hooks.OnFail(phase, func(testhook.Point) error {
				if !armed {
					return nil
				}
				return injected
			})
			var notified []error
			handler := eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) { notified = append(notified, err) })
			path := initialized(t, keepOption(t, 1))
			entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 1), hooksOption(hooks), handler))
			id := aggregate(t, "Account", "retention-failure")
			persistSnapshots(t, entry, id, 1, 1)
			armed = true

			err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2"))

			require.NoError(t, err, "a retention failure must not change the committed append")
			db := rawDB(t, path)
			assert.EqualValues(t, 2, queryInt(t, db, "SELECT count(*) FROM journal"))
			assert.EqualValues(t, 2, queryInt(t, db, "SELECT seq_nr FROM head"))
			assert.EqualValues(t, 2, queryInt(t, db, "SELECT seq_nr FROM snapshot_current"))
			require.Len(t, notified, 1, "the notification path must actually be called")
			requireKind(t, notified[0], eventstore.KindStorage)
			assert.ErrorIs(t, notified[0], injected)
			assert.Equal(t, 1, log.count(), "the log path must actually be called")
		})
	}
}

func TestRetentionNotificationPanicDoesNotFailCommittedAppend(t *testing.T) {
	for _, tc := range []struct {
		name      string
		logPanics bool
		handler   bool
	}{
		{"handler panics", false, true},
		{"log handler panics", true, false},
		{"both panic", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLog(t, tc.logPanics)
			hooks := testhook.New()
			hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
				return &testhook.InjectedError{Phase: pt.Phase, Message: "retention refused"}
			})
			calls := 0
			opts := []eventstore.Option{keepOption(t, 1), hooksOption(hooks)}
			if tc.handler {
				opts = append(opts, eventstore.WithRetentionFailureHandler(func(context.Context, error) {
					calls++
					panic(errors.New("assertion failed inside the handler"))
				}))
			}
			path := initialized(t, keepOption(t, 1))
			entry := newEntry(t, openStore(t, path, sqlite.Config{}, opts...))
			id := aggregate(t, "Account", "panic")
			persistSnapshots(t, entry, id, 1, 1)

			var err error
			require.NotPanics(t, func() {
				err = entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2"))
			})

			require.NoError(t, err)
			assert.EqualValues(t, 2, queryInt(t, rawDB(t, path), "SELECT seq_nr FROM head"))
			assert.EqualValues(t, 2, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
			assert.Positive(t, log.count(), "the log path must actually be called")
			if tc.handler {
				assert.Equal(t, 1, calls, "the failure handler must actually be called")
			}
		})
	}
}

func TestRetentionFailureReleasesWriteLockBeforeNotifying(t *testing.T) {
	path := initialized(t, keepOption(t, 1))
	hooks := testhook.New()
	hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
		return &testhook.InjectedError{Phase: pt.Phase, Message: "retention refused"}
	})
	var observed error
	var entry eventstore.EventStore[string, string]
	id := aggregate(t, "Account", "reentrant")
	next := eventAt(t, id, 3, "three")
	handler := eventstore.WithRetentionFailureHandler(func(ctx context.Context, _ error) {
		// A handler may call the same Store again: the Store must not hold its own lock while notifying.
		observed = entry.PersistEvent(ctx, next)
	})
	entry = newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 1), hooksOption(hooks), handler))
	persistSnapshots(t, entry, id, 1, 1)

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2")))

	require.NoError(t, observed)
	assert.EqualValues(t, 3, queryInt(t, rawDB(t, path), "SELECT seq_nr FROM head"))
}
