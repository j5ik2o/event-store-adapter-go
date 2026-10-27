package spanner_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
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
	assert.Equal(t, "Account-basic", events[0].AggregateID())
	assert.True(t, events[0].OccurredAt().Equal(time.Unix(1720000002, 123456789)), "nanosecond precision survives the round trip")

	assert.EqualValues(t, 3, queryInt(t, l.raw, "SELECT COUNT(*) FROM "+l.table("journal")+" WHERE aid = 'Account-basic'", nil))
	assert.EqualValues(t, 3, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("head")+" WHERE aid = 'Account-basic'", nil))
	assert.EqualValues(t, 2, queryInt(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_current")+" WHERE aid = 'Account-basic'", nil))
}

func TestAppendStoresTheKeyAsTheFNVHashPrefixOfTheFullAggregateID(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	short, long := aggregate(t, "Order", "1"), aggregate(t, "Order", "10")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, long, 1, "long")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, short, 1, "short")))

	for _, aid := range []string{"Order-1", "Order-10"} {
		statement := map[string]any{"hp": referenceHashPrefix(aid), "aid": aid}
		for _, table := range []string{"journal", "head"} {
			assert.EqualValues(t, 1, queryInt(t, l.raw, "SELECT COUNT(*) FROM "+l.table(table)+" WHERE hash_prefix = @hp AND aid = @aid", statement), table+" "+aid)
		}
	}
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), short, 1)
	require.NoError(t, err)
	require.Len(t, events, 1, "an aggregate whose ID is a prefix of another is read by exact key, not by prefix match")
	assert.Equal(t, "short", events[0].Payload())
}

func TestReadByExactKeyReturnsNothingWhenOnlyALongerIDHasAHead(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Order", "10"), 1, "long")))

	latest, err := entry.GetLatestSnapshotByID(t.Context(), aggregate(t, "Order", "1"))

	require.NoError(t, err)
	assert.Nil(t, latest)
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), aggregate(t, "Order", "1"), 0)
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestAppendRejectsDuplicateAsOptimisticLockWithHead(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "dup")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))

	err := entry.PersistEvent(t.Context(), eventAt(t, id, 2, "again"))

	requireKind(t, err, eventstore.KindOptimisticLock)
	var conflict *eventstore.OptimisticLockError
	require.ErrorAs(t, err, &conflict)
	require.NotNil(t, conflict.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(2), *conflict.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(2), conflict.SeqNr)
	assert.Equal(t, "Account-dup", conflict.AggregateID)
	assert.EqualValues(t, 2, l.count(t, "journal"))
}

func TestAppendRejectsAStaleNumberBelowTheHeadAsOptimisticLock(t *testing.T) {
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
}

func TestAppendRejectsGapAsContractViolation(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "gap")

	first := entry.PersistEvent(t.Context(), eventAt(t, id, 2, "skips one"))
	requireKind(t, first, eventstore.KindContractViolation)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, first, &violation)
	assert.Equal(t, "W-8", violation.Rule)

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	later := entry.PersistEvent(t.Context(), eventAt(t, id, 3, "skips one"))
	requireKind(t, later, eventstore.KindContractViolation)
	assert.EqualValues(t, 1, l.count(t, "journal"))
}

func TestAppendRejectsSnapshotNumberMismatchWithoutWriting(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "mismatch")

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 2, "state"))

	requireKind(t, err, eventstore.KindContractViolation)
	assert.Zero(t, l.count(t, "journal"))
}

func TestConcurrentAppendFromSeparateClientsCommitsExactlyOne(t *testing.T) {
	l := provision(t, layoutSpec{})
	first, second := l.store(t, newClient(t)), l.store(t, newClient(t))
	for round := range 3 {
		id := aggregate(t, "Account", "race-"+string(rune('a'+round)))
		start := make(chan struct{})
		results := make([]error, 2)
		var wg sync.WaitGroup
		event := eventAt(t, id, 1, "writer")
		for i, entry := range []eventstore.EventStore[string, string]{first, second} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i] = entry.PersistEvent(context.Background(), event)
			}()
		}
		close(start)
		wg.Wait()

		succeeded, conflicts := 0, 0
		for _, err := range results {
			if err == nil {
				succeeded++
				continue
			}
			requireKind(t, err, eventstore.KindOptimisticLock)
			conflicts++
		}
		assert.Equal(t, 1, succeeded, "round %d", round)
		assert.Equal(t, 1, conflicts, "round %d", round)
		assert.EqualValues(t, 1, queryInt(t, l.raw, "SELECT COUNT(*) FROM "+l.table("journal")+" WHERE aid = @aid", map[string]any{"aid": "Account-race-" + string(rune('a'+round))}))
	}
}

// abortedHook aborts the first abortFirst commits of the aggregate's sequence number onlySeq (every sequence number when
// onlySeq is 0) and lets the others through. Every call is recorded.
func abortedHook(attempts *[]testhook.Point, abortFirst int, onlySeq int64) func(testhook.Point) error {
	aborted := 0
	return func(pt testhook.Point) error {
		*attempts = append(*attempts, pt)
		if (onlySeq == 0 || pt.SeqNr == onlySeq) && aborted < abortFirst {
			aborted++
			return &testhook.InjectedError{Phase: pt.Phase, Message: "aborted", Aborted: true}
		}
		return nil
	}
}

func TestAbortedCommitIsReRunWithinTheLimitWithTheSameEnvelope(t *testing.T) {
	l := provision(t, layoutSpec{})
	hooks := testhook.New()
	var waits []time.Duration
	hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
	var attempts []testhook.Point
	hooks.OnFail(testhook.PhaseCommit, abortedHook(&attempts, 2, 2))
	cfg := l.config()
	cfg.TransactionRetryLimit = ptr(3)
	entry, err := l.tryOpen(l.raw, cfg, hooksOption(hooks))
	require.NoError(t, err)
	id := aggregate(t, "Order", "9")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))

	require.Len(t, attempts, 4, "seq 1 commits at once; seq 2 is aborted twice and succeeds on the third attempt")
	assert.EqualValues(t, 1, attempts[0].SeqNr)
	for _, retried := range attempts[1:] {
		assert.EqualValues(t, 2, retried.SeqNr, "a re-run keeps the same sequence number")
		assert.True(t, bytes.Equal(attempts[1].Payload, retried.Payload), "a re-run keeps the same payload")
	}
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 2, "the event is readable exactly once")
	assert.EqualValues(t, 2, l.count(t, "journal"))
	require.Len(t, waits, 2, "one wait before each re-run")
	for i, wait := range waits {
		assert.Positive(t, wait)
		assert.LessOrEqual(t, wait, time.Second)
		if i > 0 {
			assert.GreaterOrEqual(t, wait, waits[i-1], "the wait does not shrink")
		}
	}
}

func TestAbortedCommitBeyondTheLimitFailsAsStorageErrorAndCommitsNothing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    *int
		attempts int
	}{
		{"limit 2", ptr(2), 3},
		{"limit 0 means no re-run", ptr(0), 1},
		{"default limit 5", nil, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := provision(t, layoutSpec{})
			hooks := testhook.New()
			hooks.SetSleeper(func(time.Duration) {})
			var attempts []testhook.Point
			hooks.OnFail(testhook.PhaseCommit, abortedHook(&attempts, 1_000, 0))
			cfg := l.config()
			cfg.TransactionRetryLimit = tc.limit
			entry, err := l.tryOpen(l.raw, cfg, hooksOption(hooks))
			require.NoError(t, err)

			err = entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Order", "9"), 1, "one"))

			requireKind(t, err, eventstore.KindStorage)
			assert.Len(t, attempts, tc.attempts, "the first attempt is not counted against the limit")
			assert.Zero(t, l.count(t, "journal"))
			assert.Zero(t, l.count(t, "head"))
		})
	}
}

func TestCommitResponseLostIsAnIndeterminateStorageErrorWithoutCompensation(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	hooks := testhook.New()
	lost := &testhook.InjectedError{Phase: testhook.PhaseCommit, Message: "response lost"}
	responses := 0
	armed := true
	hooks.OnCommitResponse(func(testhook.Point) error {
		responses++
		if armed {
			return lost
		}
		return nil
	})
	entry := l.newEntry(t, hooksOption(hooks))
	id := aggregate(t, "Order", "lost")

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state"))

	requireKind(t, err, eventstore.KindStorage)
	assert.ErrorIs(t, err, lost)
	assert.Equal(t, 1, responses)
	assert.EqualValues(t, 1, l.count(t, "journal"), "the commit succeeded on the server: nothing is rolled back or compensated")
	assert.EqualValues(t, 1, l.count(t, "head"))
	assert.EqualValues(t, 1, l.count(t, "snapshot_current"))
	assert.EqualValues(t, 1, l.count(t, "snapshot_history"))

	armed = false
	again := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state"))
	requireKind(t, again, eventstore.KindOptimisticLock)
	assert.Equal(t, 1, responses, "an append that stops at the head comparison never reaches the commit response")
	assert.EqualValues(t, 1, l.count(t, "journal"))
}

func TestRefusedCommitLeavesNothingAndIsNotReRun(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	hooks := testhook.New()
	fail := true
	var seen []int64
	hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
		seen = append(seen, pt.SeqNr)
		if fail {
			return &testhook.InjectedError{Phase: pt.Phase, Message: "commit refused"}
		}
		return nil
	})
	entry := l.newEntry(t, hooksOption(hooks))
	id := aggregate(t, "Account", "commit")

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state"))

	requireKind(t, err, eventstore.KindStorage)
	assert.Equal(t, []int64{1}, seen, "a failure that is not an abort is not re-run")
	for _, table := range []string{"journal", "head", "snapshot_current", "snapshot_history"} {
		assert.Zero(t, l.count(t, table), table)
	}

	fail = false
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))
	assert.EqualValues(t, 1, l.count(t, "journal"))
	assert.EqualValues(t, 1, l.count(t, "snapshot_history"))
}

func TestAppendWithCancelledContextFailsWithoutPartialWrite(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	id := aggregate(t, "Account", "cancelled")

	err := entry.PersistEvent(ctx, eventAt(t, id, 1, "x"))

	require.Error(t, err)
	assert.Zero(t, l.count(t, "journal"))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "x")), "the store stays usable after a cancelled call")
}

func TestEventOnlyAppendWritesNoSnapshotRowsAndHistoryRowsAreUnmarked(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(3))})
	entry := l.newEntry(t)
	id := aggregate(t, "Account", "history")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	assert.Zero(t, l.count(t, "snapshot_current"))
	assert.Zero(t, l.count(t, "snapshot_history"))

	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 2, "two"), snapshotAt(t, 2, "state")))
	assert.Equal(t, []int64{2}, l.unmarkedHistorySeqNrs(t, "Account-history"), "a new history row is written with expired_at NULL")
}

func TestNoRetentionKeepsNoHistoryRows(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)

	persistSnapshots(t, entry, aggregate(t, "Account", "nohistory"), 1, 2)

	assert.EqualValues(t, 1, l.count(t, "snapshot_current"))
	assert.Zero(t, l.count(t, "snapshot_history"))
}
