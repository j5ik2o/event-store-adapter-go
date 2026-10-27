package sqlite_test

import (
	"context"
	"sync"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendEventOnlyThenSnapshotAndEventsAreReadFromRealFile(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
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

	db := rawDB(t, path)
	assert.EqualValues(t, 3, queryInt(t, db, "SELECT count(*) FROM journal WHERE aid = 'Account-basic'"))
	assert.EqualValues(t, 3, queryInt(t, db, "SELECT seq_nr FROM head WHERE aid = 'Account-basic'"))
	assert.EqualValues(t, 2, queryInt(t, db, "SELECT seq_nr FROM snapshot_current WHERE aid = 'Account-basic'"))
}

func TestAppendRejectsDuplicateAsOptimisticLockWithHead(t *testing.T) {
	entry := newEntry(t, openStore(t, initialized(t), sqlite.Config{}))
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
}

func TestAppendRejectsGapAsContractViolation(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "gap")

	first := entry.PersistEvent(t.Context(), eventAt(t, id, 2, "skips one"))
	requireKind(t, first, eventstore.KindContractViolation)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, first, &violation)
	assert.Equal(t, "W-8", violation.Rule)

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	later := entry.PersistEvent(t.Context(), eventAt(t, id, 3, "skips one"))
	requireKind(t, later, eventstore.KindContractViolation)
	assert.EqualValues(t, 1, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
}

func TestAppendRejectsSnapshotNumberMismatchWithoutWriting(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "mismatch")

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 2, "state"))

	requireKind(t, err, eventstore.KindContractViolation)
	assert.EqualValues(t, 0, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
}

func TestAppendKeepsPrefixRelatedAggregatesApart(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	short, long := aggregate(t, "Account", "1"), aggregate(t, "Account", "10")

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, long, 1, "long")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, short, 1, "short")))

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), short, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "short", events[0].Payload())
}

func TestAppendChecksHeadAfterAcquiringWriteLock(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	id := aggregate(t, "Account", "order")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
	other, err := rawDB(t, path).Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	// The other connection takes the write lock and advances the head to 2, but has not committed yet.
	_, err = other.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, err)
	_, err = other.ExecContext(t.Context(), "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES ('Account-order', 2, 1, 'event/v1', X'22726169766522')")
	require.NoError(t, err)
	_, err = other.ExecContext(t.Context(), "UPDATE head SET seq_nr = 2 WHERE aid = 'Account-order'")
	require.NoError(t, err)
	waiting := eventAt(t, id, 2, "waits")
	result := make(chan error, 1)
	go func() { result <- entry.PersistEvent(context.Background(), waiting) }()
	select {
	case err := <-result:
		t.Fatalf("append must wait for the write lock instead of finishing early: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	_, err = other.ExecContext(t.Context(), "COMMIT")
	require.NoError(t, err)

	select {
	case err := <-result:
		requireKind(t, err, eventstore.KindOptimisticLock)
		var conflict *eventstore.OptimisticLockError
		require.ErrorAs(t, err, &conflict)
		require.NotNil(t, conflict.HeadSeqNr, "the head compared must be the one committed while waiting")
		assert.Equal(t, eventstore.SeqNr(2), *conflict.HeadSeqNr)
	case <-time.After(10 * time.Second):
		t.Fatal("append did not finish after the write lock was released")
	}
	assert.EqualValues(t, 2, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
}

func TestAppendCommitFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	path := initialized(t, keepOption(t, 2))
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
	entry := newEntry(t, openStore(t, path, sqlite.Config{}, keepOption(t, 2), hooksOption(hooks)))
	id := aggregate(t, "Account", "commit")
	db := rawDB(t, path)

	err := entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state"))

	requireKind(t, err, eventstore.KindStorage)
	assert.Equal(t, []int64{1}, seen, "the commit hook must be reached")
	for _, table := range []string{"journal", "head", "snapshot_current", "snapshot_history"} {
		assert.EqualValues(t, 0, queryInt(t, db, "SELECT count(*) FROM "+table), table)
	}
	// The failed append must not leave its connection holding the write lock.
	lock, lockErr := db.Conn(t.Context())
	require.NoError(t, lockErr)
	_, lockErr = lock.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, lockErr)
	_, lockErr = lock.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, lockErr)
	require.NoError(t, lock.Close())

	fail = false
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))
	assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM journal"))
	assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM snapshot_history"))
}

func TestConcurrentAppendFromSeparateConnectionsCommitsExactlyOne(t *testing.T) {
	path := initialized(t)
	first := newEntry(t, openStore(t, path, sqlite.Config{}))
	second := newEntry(t, openStore(t, path, sqlite.Config{}))
	db := rawDB(t, path)
	for round := range 5 {
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
			switch {
			case err == nil:
				succeeded++
			default:
				requireKind(t, err, eventstore.KindOptimisticLock)
				conflicts++
			}
		}
		assert.Equal(t, 1, succeeded, "round %d", round)
		assert.Equal(t, 1, conflicts, "round %d", round)
		aid := "Account-race-" + string(rune('a'+round))
		assert.EqualValues(t, 1, queryInt(t, db, "SELECT count(*) FROM journal WHERE aid = ?", aid))
	}
}

func TestLockWaitReturnsStorageErrorAfterWaitingAndKeepsStoreUsable(t *testing.T) {
	path := initialized(t)
	wait := 200 * time.Millisecond
	entry := newEntry(t, openStore(t, path, sqlite.Config{LockWait: &wait}))
	id := aggregate(t, "Account", "busy")
	holder, err := rawDB(t, path).Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	_, err = holder.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, err)

	started := time.Now()
	err = entry.PersistEvent(t.Context(), eventAt(t, id, 1, "blocked"))
	elapsed := time.Since(started)

	requireKind(t, err, eventstore.KindStorage)
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the append must wait for the configured time")
	assert.Less(t, elapsed, 5*time.Second)
	_, err = holder.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)
	assert.EqualValues(t, 0, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "after release")))
	assert.EqualValues(t, 1, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
}

func TestLockWaitZeroFailsImmediately(t *testing.T) {
	path := initialized(t)
	zero := time.Duration(0)
	entry := newEntry(t, openStore(t, path, sqlite.Config{LockWait: &zero}))
	holder, err := rawDB(t, path).Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	_, err = holder.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, err)

	started := time.Now()
	err = entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Account", "zero"), 1, "x"))

	requireKind(t, err, eventstore.KindStorage)
	assert.Less(t, time.Since(started), 2*time.Second)
}

func TestAppendWithCancelledContextFailsWithoutPartialWrite(t *testing.T) {
	path := initialized(t)
	entry := newEntry(t, openStore(t, path, sqlite.Config{}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := entry.PersistEvent(ctx, eventAt(t, aggregate(t, "Account", "cancelled"), 1, "x"))

	require.Error(t, err)
	assert.EqualValues(t, 0, queryInt(t, rawDB(t, path), "SELECT count(*) FROM journal"))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Account", "cancelled"), 1, "x")), "the store stays usable after a cancelled call")
}
