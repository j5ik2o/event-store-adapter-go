package bigtable_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendEventOnlyThenSnapshotAndEventsAreReadFromTheEmulator(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "basic")

	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Nil(t, latest, "no head means no result")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	latest, err = entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, eventstore.SeqNr(1), latest.HeadSeqNr)
	assert.Nil(t, latest.Snapshot, "a head without a snapshot is returned with a nil snapshot")

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state-2")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 3, "three")))
	latest, err = entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, eventstore.SeqNr(3), latest.HeadSeqNr)
	require.NotNil(t, latest.Snapshot)
	assert.Equal(t, eventstore.SeqNr(2), latest.Snapshot.SeqNr())
	assert.Equal(t, "snapshot/v1", latest.Snapshot.Manifest())
	assert.Equal(t, "state-2", latest.Snapshot.Aggregate())

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 2)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, []string{"two", "three"}, []string{events[0].Payload(), events[1].Payload()})
	assert.True(t, events[0].OccurredAt().Equal(time.Unix(1720000002, 123456789)), "nanosecond precision survives the round trip")

	aid := aidOf(t, id)
	head := cells(t, l.data, l.head, string(rowPrefix(aid)))
	assert.Equal(t, "00000000000000000003", head["head:seq_nr"], "head:seq_nr is 20 zero-padded digits")
	assert.Equal(t, "0", head["head:has_snapshot"])
	assert.Contains(t, head, "snapshot:current", "an event-only append keeps the existing snapshot")
	assert.Equal(t, "1", cells(t, l.data, l.journal, journalKey(aid, 2))["event:has_snapshot"])
}

func TestAppendReopenedStoreSeesTheSameHistory(t *testing.T) {
	l := provision(t, layoutSpec{})
	id := aggregate(t, "Account", "reopen")
	first := l.newEntry(t)
	require.NoError(t, first.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))

	reopened := l.newEntry(t)

	require.NoError(t, reopened.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))
	events, err := reopened.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 2)
}

func TestAppendRejectsADuplicateNumberAsOptimisticLock(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "stale")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
	}

	err := entry.PersistEvent(t.Context(), eventAt(t, id, 1, "old"))

	requireKind(t, err, eventstore.KindOptimisticLock)
	var conflict *eventstore.OptimisticLockError
	require.ErrorAs(t, err, &conflict)
	require.NotNil(t, conflict.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(3), *conflict.HeadSeqNr)
	err = entry.PersistEvent(t.Context(), eventAt(t, id, 3, "dup"))
	requireKind(t, err, eventstore.KindOptimisticLock)
}

func TestAppendRejectsAGapAsContractViolation(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "gap")

	first := entry.PersistEvent(t.Context(), eventAt(t, id, 2, "skips one"))
	requireKind(t, first, eventstore.KindContractViolation)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, first, &violation)
	assert.Equal(t, "W-8", violation.Rule)

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	requireKind(t, entry.PersistEvent(t.Context(), eventAt(t, id, 3, "skips one")), eventstore.KindContractViolation)
	assert.Len(t, l.journalKeys(t), 2, "the rejected appends wrote nothing")
}

func TestAppendRejectsSnapshotNumberMismatchWithoutWriting(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, aggregate(t, "Account", "mismatch"), 1, "one"), snapshotAt(t, 2, "state"))

	requireKind(t, err, eventstore.KindContractViolation)
	assert.Equal(t, []string{"\x00"}, l.journalKeys(t))
	assert.Equal(t, []string{"\x00"}, rowKeys(t, l.data, l.head))
}

func TestAppendConcurrentWritersFromSeparateClientsCommitExactlyOne(t *testing.T) {
	l := provision(t, layoutSpec{})
	other := newDataClient(t)
	first := l.newEntry(t)
	second, err := bigtable.New(t.Context(), other, l.admin, l.config(), eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), l.options(t)...)
	require.NoError(t, err)
	for _, name := range []string{"a", "b", "c"} {
		id := aggregate(t, "Account", "race-"+name)
		start := make(chan struct{})
		results := make([]error, 2)
		var wg sync.WaitGroup
		event := eventAt(t, id, 1, "writer")
		for i, entry := range []eventstore.EventStore[string, string]{first, second} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i] = entry.PersistEvent(t.Context(), event)
			}()
		}
		close(start)
		wg.Wait()

		succeeded := 0
		for _, result := range results {
			if result == nil {
				succeeded++
			} else {
				requireKind(t, result, eventstore.KindOptimisticLock)
			}
		}
		assert.Equal(t, 1, succeeded, "exactly one writer commits")
		events, err := first.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
		require.NoError(t, err)
		assert.Len(t, events, 1)
	}
}

func TestAppendFailureOfThePreviousJournalWriteLeavesTheHeadUnchanged(t *testing.T) {
	l := provision(t, layoutSpec{})
	hooks := testhook.New()
	entry := l.newEntry(t, hooksOption(hooks))
	id := aggregate(t, "Account", "previous")
	// Hold the tail write of seq 1 back so that the append of seq 2 has to write row 1 itself.
	failAt(hooks, testhook.PhaseJournalWriteTail, "tail refused")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	hooks2 := testhook.New()
	failAt(hooks2, testhook.PhaseJournalWritePrevious, "previous refused")
	failing := l.newEntry(t, hooksOption(hooks2))

	err := failing.PersistEvent(t.Context(), eventAt(t, id, 2, "two"))

	requireKind(t, err, eventstore.KindStorage)
	head := cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))
	assert.Equal(t, "00000000000000000001", head["head:seq_nr"], "the head does not advance when the previous event cannot be written")
}

// TestCellVersion pins BT-6: cells are written with timestamp 0, a replaced column holds a single version, and the
// condition looks only at the newest cell.
func TestCellVersion(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "cells")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, "x")))
	}

	row, err := l.data.Open(l.head).ReadRow(t.Context(), string(rowPrefix(aidOf(t, id))))
	require.NoError(t, err)

	versions := map[string]int{}
	for _, items := range row {
		for _, item := range items {
			versions[item.Column]++
			assert.EqualValues(t, 0, item.Timestamp)
		}
	}
	for column, n := range versions {
		assert.Equal(t, 1, n, "%s keeps one version after replacements", column)
	}
	assert.Equal(t, "00000000000000000003", cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))["head:seq_nr"])
	requireKind(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "again")), eventstore.KindOptimisticLock)
}

// TestTailWrite pins step 5 of BT-7: a failure of the write-out after the commit is reported, never returned.
func TestTailWrite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		logPanics    bool
		handlerPanic bool
	}{
		{"failure is reported through both paths", false, false},
		{"handler panics", false, true},
		{"log handler panics", true, false},
		{"both panic", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLog(t, tc.logPanics)
			l := provision(t, layoutSpec{})
			hooks := testhook.New()
			failAt(hooks, testhook.PhaseJournalWriteTail, "tail refused")
			var mu sync.Mutex
			var notified []error
			cfg := l.config()
			cfg.JournalWriteFailureHandler = func(_ context.Context, err error) {
				mu.Lock()
				notified = append(notified, err)
				mu.Unlock()
				if tc.handlerPanic {
					panic(errors.New("assertion failed inside the handler"))
				}
			}
			entry, err := l.tryOpen(cfg, append(l.options(t), hooksOption(hooks))...)
			require.NoError(t, err)
			id := aggregate(t, "Account", "tail")

			require.NotPanics(t, func() {
				err = entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one"))
			})

			require.NoError(t, err, "the committed append stays a success")
			mu.Lock()
			require.Len(t, notified, 1, "the notification path must actually be called")
			requireKind(t, notified[0], eventstore.KindStorage)
			mu.Unlock()
			assert.Positive(t, log.count(), "the log path must actually be called")
			assert.Equal(t, "00000000000000000001", cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))["head:seq_nr"])

			clean := l.newEntry(t)
			require.NoError(t, clean.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))
			events, err := clean.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
			require.NoError(t, err)
			require.Len(t, events, 2, "the next append writes the missing row, so the read has no gap")
		})
	}
}

func TestEnvelopeLimit(t *testing.T) {
	const limit = 1024
	l := provision(t, layoutSpec{limit: limit})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "limit")
	// The JSON serializer quotes the payload, so a payload of n bytes becomes n+2 bytes in the envelope.
	envelopeOverhead := 1 + 2 + len("Account-limit") + 8 + 8 + 4 + len("event/v1") + 4 + 2
	exact := strings.Repeat("a", limit-envelopeOverhead)

	t.Run("an envelope exactly at the limit is stored", func(t *testing.T) {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, exact)))
	})
	t.Run("one byte over is rejected before any write and names BT-16 and the seq_nr", func(t *testing.T) {
		before := l.journalKeys(t)

		err := entry.PersistEvent(t.Context(), eventAt(t, id, 2, exact+"a"))

		requireKind(t, err, eventstore.KindContractViolation)
		var violation *eventstore.ContractViolationError
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, "BT-16", violation.Rule)
		require.NotNil(t, violation.SeqNr)
		assert.Equal(t, eventstore.SeqNr(2), *violation.SeqNr)
		assert.Equal(t, before, l.journalKeys(t))
		head := cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))
		assert.Equal(t, "00000000000000000001", head["head:seq_nr"])
	})
	t.Run("a snapshot over the limit is rejected as well", func(t *testing.T) {
		err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "ok"), snapshotAt(t, 2, strings.Repeat("s", limit)))

		requireKind(t, err, eventstore.KindContractViolation)
		var violation *eventstore.ContractViolationError
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, "BT-16", violation.Rule)
	})
	t.Run("the largest allowed limit round-trips through the emulator", func(t *testing.T) {
		big := provision(t, layoutSpec{limit: 8388608})
		bigEntry := big.newEntry(t)
		bigID := aggregate(t, "Account", "eight-mib")
		payload := strings.Repeat("z", 8388608-envelopeOverhead-len("Account-eight-mib")+len("Account-limit"))

		require.NoError(t, bigEntry.PersistEvent(t.Context(), eventAt(t, bigID, 1, payload)))

		events, err := bigEntry.GetEventsByIDSinceSeqNr(t.Context(), bigID, 1)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, payload, events[0].Payload())
	})
}
