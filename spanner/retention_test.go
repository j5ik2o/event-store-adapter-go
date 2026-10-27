package spanner_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionKeepsNewestHistoryRightAfterReturnInDeleteMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		keep *int64
		want []int64
	}{
		{"none", nil, []int64{}},
		{"one", ptr(int64(1)), []int64{4}},
		{"two", ptr(int64(2)), []int64{3, 4}},
		{"larger than the number of snapshots", ptr(int64(9)), []int64{1, 2, 3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := provision(t, layoutSpec{keep: tc.keep})
			entry := l.newEntry(t)
			id := aggregate(t, "Account", "retention")

			persistSnapshots(t, entry, id, 1, 4)

			assert.Equal(t, tc.want, l.historySeqNrs(t, "Account-retention"))
			assert.EqualValues(t, 4, l.count(t, "journal"), "events are never removed")
			latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			require.NotNil(t, latest.Snapshot)
			assert.Equal(t, eventstore.SeqNr(4), latest.Snapshot.SeqNr())
			assert.EqualValues(t, 4, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_current")+" WHERE aid = 'Account-retention'", nil))
		})
	}
}

func TestRetentionIgnoresEventOnlyAppendsAndOtherAggregates(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	entry := l.newEntry(t)
	id, other := aggregate(t, "Account", "history"), aggregate(t, "Account", "history-extra")
	persistSnapshots(t, entry, id, 1, 3)
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 4, "event only")))
	assert.Equal(t, []int64{2, 3}, l.historySeqNrs(t, "Account-history"), "an event-only append must not touch history")

	persistSnapshots(t, entry, other, 1, 1)

	assert.Equal(t, []int64{2, 3}, l.historySeqNrs(t, "Account-history"))
	assert.Equal(t, []int64{1}, l.historySeqNrs(t, "Account-history-extra"))
}

func TestRetentionDeletesOldestSnapshotsThroughHook(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(3))})
	hooks := testhook.New()
	entry := l.newEntry(t, hooksOption(hooks))
	var deleted []int64
	hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
		deleted = append(deleted, pt.SeqNrs...)
		return nil
	})

	persistSnapshots(t, entry, aggregate(t, "Account", "hook"), 1, 6)

	slices.Sort(deleted)
	assert.Equal(t, []int64{1, 2, 3}, deleted)
	assert.Equal(t, []int64{4, 5, 6}, l.historySeqNrs(t, "Account-hook"))
}

func TestRetentionRunsAfterTheAppendIsCommittedInASeparateTransaction(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	hooks := testhook.New()
	id := aggregate(t, "Account", "order")
	var visibleAtQuery int64 = -1
	hooks.OnFail(testhook.PhaseRetentionQuery, func(testhook.Point) error {
		// The append is already visible to an independent client when retention starts.
		visibleAtQuery = queryInt(t, l.raw, "SELECT COUNT(*) FROM "+l.table("snapshot_history")+" WHERE aid = 'Account-order' AND seq_nr = 2", nil)
		return nil
	})
	entry := l.newEntry(t, hooksOption(hooks))
	persistSnapshots(t, entry, id, 1, 1)

	persistSnapshots(t, entry, id, 2, 2)

	assert.EqualValues(t, 1, visibleAtQuery, "retention must run after the append is committed")
}

func TestRetentionSplitsManyCandidatesAndLeavesOnlyTheNewestRows(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	hooks := testhook.New()
	id := aggregate(t, "Account", "batches")
	aid := "Account-batches"
	hp := referenceHashPrefix(aid)
	// A stream with 2500 events and 2400 history rows, prepared the way another writer would have left it.
	var mutations []*gspanner.Mutation
	for n := int64(1); n <= 2500; n++ {
		mutations = append(mutations, gspanner.InsertMap(l.table("journal"), map[string]any{
			"hash_prefix": hp, "aid": aid, "seq_nr": n, "occurred_at_ns": int64(1720000000000000000), "manifest": "event/v1", "payload": []byte(`"x"`)}))
		if n <= 2400 {
			mutations = append(mutations, gspanner.InsertMap(l.table("snapshot_history"), map[string]any{
				"hash_prefix": hp, "aid": aid, "seq_nr": n, "manifest": "snapshot/v1", "payload": []byte(`"s"`)}))
		}
	}
	mutations = append(mutations,
		gspanner.InsertMap(l.table("head"), map[string]any{"hash_prefix": hp, "aid": aid, "type_name": "Account", "seq_nr": int64(2500),
			"event_occurred_at_ns": int64(1720000000000000000), "event_manifest": "event/v1", "event_payload": []byte(`"x"`)}),
		gspanner.InsertMap(l.table("snapshot_current"), map[string]any{"hash_prefix": hp, "aid": aid, "seq_nr": int64(2400), "manifest": "snapshot/v1", "payload": []byte(`"s"`)}))
	_, err := l.raw.Apply(t.Context(), mutations)
	require.NoError(t, err)
	var batches [][]int64
	hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
		batches = append(batches, slices.Clone(pt.SeqNrs))
		return nil
	})
	entry := l.newEntry(t, hooksOption(hooks))

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2501, "next"), snapshotAt(t, 2501, "state-2501")))

	require.GreaterOrEqual(t, len(batches), 3, "2400 obsolete rows do not fit in one part")
	total := 0
	for _, batch := range batches {
		assert.NotEmpty(t, batch)
		assert.LessOrEqual(t, len(batch), 1000, "a part holds at most 1000 rows")
		total += len(batch)
	}
	assert.Equal(t, 2400, total, "every obsolete row is handled exactly once across the parts")
	assert.Equal(t, []int64{2501}, l.historySeqNrs(t, aid))
	assert.EqualValues(t, 2501, l.count(t, "journal"), "events are never removed")
}

func TestTTLRetentionMarksOldRowsOnceAndDoesNotCountMarkedRows(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2)), graceDays: 7})
	hooks := testhook.New()
	var marked [][]int64
	hooks.OnFail(testhook.PhaseRetentionMark, func(pt testhook.Point) error {
		marked = append(marked, slices.Clone(pt.SeqNrs))
		return nil
	})
	entry, err := l.tryOpen(l.raw, l.config(), append(ttlOptionsKeeping(t, 2), hooksOption(hooks))...)
	require.NoError(t, err)
	id := aggregate(t, "Account", "ttl")
	aid := "Account-ttl"

	persistSnapshots(t, entry, id, 1, 3)
	assert.Equal(t, [][]int64{{1}}, marked, "the first old row is marked when it falls out of the newest two")
	assert.Equal(t, []int64{2, 3}, l.unmarkedHistorySeqNrs(t, aid))
	firstMark := queryTimes(t, l, aid)
	require.Contains(t, firstMark, int64(1))

	persistSnapshots(t, entry, id, 4, 5)

	assert.Equal(t, [][]int64{{1}, {2}, {3}}, marked, "each row is marked exactly once; a marked row is never counted or marked again")
	assert.Equal(t, []int64{4, 5}, l.unmarkedHistorySeqNrs(t, aid))
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, l.historySeqNrs(t, aid), "TTL mode leaves physical removal to Spanner")
	assert.Equal(t, firstMark[1], queryTimes(t, l, aid)[1], "the time a row was marked is not rewritten by later appends")
	assert.EqualValues(t, 5, l.count(t, "journal"))
}

func ttlOptionsKeeping(t *testing.T, n int) []eventstore.Option {
	t.Helper()
	return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, n)}
}

// queryTimes returns the expired_at of every marked row, as text, keyed by the sequence number.
func queryTimes(t *testing.T, l *layout, aid string) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	for _, seq := range queryInts(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_history")+" WHERE aid = @aid AND expired_at IS NOT NULL", map[string]any{"aid": aid}) {
		out[seq] = queryString(t, l.raw, "SELECT CAST(expired_at AS STRING) FROM "+l.table("snapshot_history")+" WHERE aid = @aid AND seq_nr = @seq", map[string]any{"aid": aid, "seq": seq})
	}
	return out
}

func TestRetentionFailureKeepsCommittedAppendAndNotifies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase testhook.Phase
		spec  layoutSpec
		ttl   bool
	}{
		{"delete: query", testhook.PhaseRetentionQuery, layoutSpec{keep: ptr(int64(1))}, false},
		{"delete: delete", testhook.PhaseRetentionDelete, layoutSpec{keep: ptr(int64(1))}, false},
		{"ttl: mark", testhook.PhaseRetentionMark, layoutSpec{keep: ptr(int64(1)), graceDays: 7}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLog(t, false)
			l := provision(t, tc.spec)
			hooks := testhook.New()
			injected := &testhook.InjectedError{Phase: tc.phase, Message: "retention refused"}
			armed := false
			hooks.OnFail(tc.phase, func(testhook.Point) error {
				if !armed {
					return nil
				}
				return injected
			})
			var notified []error
			handler := eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) { notified = append(notified, err) })
			entry := l.newEntry(t, hooksOption(hooks), handler)
			id := aggregate(t, "Account", "retention-failure")
			persistSnapshots(t, entry, id, 1, 1)
			armed = true

			err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2"))

			require.NoError(t, err, "a retention failure must not change the committed append")
			assert.EqualValues(t, 2, l.count(t, "journal"))
			assert.EqualValues(t, 2, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("head"), nil))
			assert.EqualValues(t, 2, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_current"), nil))
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
			l := provision(t, layoutSpec{keep: ptr(int64(1))})
			hooks := testhook.New()
			hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
				return &testhook.InjectedError{Phase: pt.Phase, Message: "retention refused"}
			})
			calls := 0
			opts := []eventstore.Option{hooksOption(hooks)}
			if tc.handler {
				opts = append(opts, eventstore.WithRetentionFailureHandler(func(context.Context, error) {
					calls++
					panic(errors.New("assertion failed inside the handler"))
				}))
			}
			entry := l.newEntry(t, opts...)
			id := aggregate(t, "Account", "panic")
			persistSnapshots(t, entry, id, 1, 1)

			var err error
			require.NotPanics(t, func() {
				err = entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2"))
			})

			require.NoError(t, err)
			assert.EqualValues(t, 2, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("head"), nil))
			assert.EqualValues(t, 2, l.count(t, "journal"))
			assert.Positive(t, log.count(), "the log path must actually be called")
			if tc.handler {
				assert.Equal(t, 1, calls, "the failure handler must actually be called")
			}
		})
	}
}

func TestRetentionFailureNotifiesOnlyAfterTheAppendTransactionEnded(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	hooks := testhook.New()
	hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
		return &testhook.InjectedError{Phase: pt.Phase, Message: "retention refused"}
	})
	var observed error
	var entry eventstore.EventStore[string, string]
	id := aggregate(t, "Account", "reentrant")
	next := eventAt(t, id, 3, "three")
	handler := eventstore.WithRetentionFailureHandler(func(ctx context.Context, _ error) {
		// A handler may call the same store again: the store must not hold a transaction while notifying.
		observed = entry.PersistEvent(ctx, next)
	})
	entry = l.newEntry(t, hooksOption(hooks), handler)
	persistSnapshots(t, entry, id, 1, 1)

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2")))

	require.NoError(t, observed)
	assert.EqualValues(t, 3, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("head"), nil))
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
