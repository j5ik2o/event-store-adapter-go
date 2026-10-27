package bigtable_test

import (
	"context"
	"errors"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// historyStates returns the history:state of every journal row of aid in sequence order (empty when absent).
func historyStates(t *testing.T, l *layout, aid string, to uint64) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	for seq := uint64(1); seq <= to; seq++ {
		row := cells(t, l.data, l.journal, journalKey(aid, seq))
		if row == nil {
			continue
		}
		out[seq] = row["history:state"]
	}
	return out
}

func TestRetentionKeepsTheNewestNAndRetiresTheRest(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "keep")
	persistSnapshots(t, entry, id, 1, 5)
	aid := aidOf(t, id)

	states := historyStates(t, l, aid, 5)

	assert.Equal(t, map[uint64]string{1: "retired", 2: "retired", 3: "retired", 4: "active", 5: "active"}, states)
	for seq := uint64(1); seq <= 5; seq++ {
		row := cells(t, l.data, l.journal, journalKey(aid, seq))
		assert.Contains(t, row, "event:envelope", "the event cell is never deleted (seq %d)", seq)
		if states[seq] == "retired" {
			assert.NotContains(t, row, "history:envelope", "a retired row loses its history payload")
		} else {
			assert.Contains(t, row, "history:envelope")
		}
	}
	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, eventstore.SeqNr(5), latest.Snapshot.SeqNr(), "snapshot:current is never deleted")
}

func TestRetentionWritesNoHistoryWithoutACount(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "none")
	persistSnapshots(t, entry, id, 1, 3)

	for seq, state := range historyStates(t, l, aidOf(t, id), 3) {
		assert.Empty(t, state, "no history cell for seq %d", seq)
	}
}

func TestRetentionCountsTheHeadSnapshotThatIsNotYetInAJournalRow(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	hooks := testhook.New()
	failAt(hooks, testhook.PhaseJournalWriteTail, "tail refused")
	entry := l.newEntry(t, hooksOption(hooks))
	id := aggregate(t, "Account", "headcount")

	persistSnapshots(t, entry, id, 1, 2)

	states := historyStates(t, l, aidOf(t, id), 2)
	assert.NotEqual(t, "active", states[1], "with n=1 the head's snapshot (seq 2) is the one that stays")
}

func TestRetentionRetiredRowIsNeverRevivedByALateWriter(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	entry := l.newEntry(t)
	late := l.newEntry(t)
	id := aggregate(t, "Account", "late")
	persistSnapshots(t, entry, id, 1, 3)
	aid := aidOf(t, id)
	require.Equal(t, "retired", cells(t, l.data, l.journal, journalKey(aid, 1))["history:state"])

	err := late.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "stale"), snapshotAt(t, 1, "stale"))

	requireKind(t, err, eventstore.KindOptimisticLock)
	row := cells(t, l.data, l.journal, journalKey(aid, 1))
	assert.Equal(t, "retired", row["history:state"])
	assert.NotContains(t, row, "history:envelope", "deleted history does not come back")
	assert.NotEmpty(t, row["event:envelope"], "the event cell is untouched")
}

func TestRetentionLeavesTheConfigurationRowAlone(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	entry := l.newEntry(t)
	persistSnapshots(t, entry, aggregate(t, "Account", "cfgrow"), 1, 3)

	row := cells(t, l.data, l.journal, "\x00")

	assert.Contains(t, row, "config:metadata")
	assert.NotContains(t, row, "history:state", "the configuration row is not subject to retention")
}

func TestRetentionFailureDoesNotChangeTheCommittedAppend(t *testing.T) {
	for _, phase := range []testhook.Phase{testhook.PhaseRetentionQuery, testhook.PhaseRetentionDelete} {
		t.Run(string(phase), func(t *testing.T) {
			log := captureLog(t, false)
			l := provision(t, layoutSpec{keep: ptr(int64(1))})
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
			entry := l.newEntry(t, hooksOption(hooks), handler)
			id := aggregate(t, "Account", "retention-failure")
			persistSnapshots(t, entry, id, 1, 1)
			armed = true

			err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2"))

			require.NoError(t, err, "a retention failure must not change the committed append")
			assert.Equal(t, "00000000000000000002", cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))["head:seq_nr"])
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
			failAt(hooks, testhook.PhaseRetentionDelete, "retention refused")
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
			assert.Equal(t, "00000000000000000002", cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))["head:seq_nr"])
			assert.Positive(t, log.count(), "the log path must actually be called")
			if tc.handler {
				assert.Equal(t, 1, calls, "the failure handler must actually be called")
			}
		})
	}
}
