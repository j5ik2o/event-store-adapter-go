package memory

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type snapshotReadResult struct {
	read *eventstore.SnapshotRead[[]byte]
	err  error
}

func requireLatestSnapshot(t *testing.T, actual *eventstore.SnapshotRead[[]byte], head eventstore.SeqNr, snapshot *eventstore.SnapshotEnvelope[[]byte]) {
	t.Helper()
	require.NotNil(t, actual)
	assert.Equal(t, head, actual.HeadSeqNr)
	if snapshot == nil {
		require.Nil(t, actual.Snapshot)
		t.Logf("head=%d snapshot=nil", actual.HeadSeqNr)
		return
	}
	require.NotNil(t, actual.Snapshot)
	require.NoError(t, actual.Snapshot.Validate())
	assert.Equal(t, snapshot.SeqNr(), actual.Snapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), actual.Snapshot.Manifest())
	assert.Equal(t, snapshot.Aggregate(), actual.Snapshot.Aggregate())
	t.Logf("head=%d snapshot=%d; metadata and bytes match independently fixed expectations", actual.HeadSeqNr, actual.Snapshot.SeqNr())
}

func TestGetLatestSnapshotTracksHeadAndCurrent(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "latest")
	require.NoError(t, err)

	missing, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	require.Nil(t, missing)

	require.NoError(t, store.persistEvent(newTestEvent(t, "latest", 1, []byte("first"))))
	headOnly, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, headOnly, 1, nil)

	second := newTestSnapshot(t, 2, []byte{255, 0, 13, 10})
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "latest", 2, []byte("second")), second))
	pair, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, pair, 2, &second)

	require.NoError(t, store.persistEvent(newTestEvent(t, "latest", 3, []byte("third"))))
	advanced, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, advanced, 3, &second)

	fourth, err := eventstore.NewSnapshotEnvelope([]byte("fourth\x00state"), 4)
	require.NoError(t, err)
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "latest", 4, []byte("fourth")), fourth))
	latest, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, latest, 4, &fourth)
	// Earlier results remain values from their own read boundaries.
	requireLatestSnapshot(t, headOnly, 1, nil)
	requireLatestSnapshot(t, pair, 2, &second)
	requireLatestSnapshot(t, advanced, 3, &second)
}

func TestGetLatestSnapshotUsesExactAggregateID(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	ids := []*readTestID{
		{"Account", "key"}, {"Account", "key-extra"}, {"account", "key"},
		{"Account", " key "}, {"Account", "é"}, {"Account", "e\u0301"},
		{"", "value-with-hyphens"}, {"Account", ""}, {"", ""},
		{"X", strings.Repeat("界", 340) + "ab"}, // Exactly 1024 UTF-8 bytes including X-.
	}
	var snapshots []eventstore.SnapshotEnvelope[[]byte]
	for i, id := range ids {
		event, err := eventstore.NewEventEnvelope(id, 1, time.Unix(0, 123456789), []byte("event"))
		require.NoError(t, err)
		snapshot, err := eventstore.NewSnapshotEnvelope([]byte{255, 0, byte(i)}, 1,
			eventstore.WithManifest(fmt.Sprintf("snapshot/%d", i)))
		require.NoError(t, err)
		require.NoError(t, store.persistEventAndSnapshot(event, snapshot))
		snapshots = append(snapshots, snapshot)
	}

	for i, id := range ids {
		actual, err := store.getLatestSnapshotByID(id)
		require.NoError(t, err)
		requireLatestSnapshot(t, actual, 1, &snapshots[i])
	}
	prefix, err := store.getLatestSnapshotByID(&readTestID{"Account", "ke"})
	require.NoError(t, err)
	require.Nil(t, prefix, "a prefix must not select either key or key-extra")
}

func TestGetLatestSnapshotRejectsInvalidIDBeforeLock(t *testing.T) {
	var nilID *readTestID
	for _, tc := range []struct {
		name string
		id   eventstore.AggregateID
		rule string
	}{
		{"nil ID", nil, "T-2"},
		{"typed nil ID", nilID, "T-2"},
		{"hyphen in type", &readTestID{"Bad-Type", "value"}, "T-11"},
		{"ID too long", &readTestID{"X", strings.Repeat("a", 1023)}, "T-12"},
		{"UTF-8 ID too long", &readTestID{"X", strings.Repeat("界", 341)}, "T-12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			store.hooks = testhook.New()
			var reads atomic.Int32
			store.hooks.OnFail(testhook.PhaseReadSnapshot, func(testhook.Point) error { reads.Add(1); return nil })
			store.mu.Lock()
			unlock := sync.OnceFunc(store.mu.Unlock)
			result := make(chan snapshotReadResult, 1)
			done := make(chan struct{})
			t.Cleanup(func() { unlock(); waitForReadSignal(t, done, "invalid-ID snapshot reader exit") })
			go func() {
				read, err := store.getLatestSnapshotByID(tc.id)
				result <- snapshotReadResult{read, err}
				close(done)
			}()

			waitForReadSignal(t, done, "ID rejection while the store is write-locked")
			actual := <-result

			require.Nil(t, actual.read)
			requireKind(t, actual.err, eventstore.KindContractViolation)
			var violation *eventstore.ContractViolationError
			require.ErrorAs(t, actual.err, &violation)
			assert.Equal(t, tc.rule, violation.Rule)
			assert.Zero(t, reads.Load())
		})
	}
}

func TestGetLatestSnapshotFixesAggregateIDBeforeWaiting(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id := &observedReadID{typeName: "Account", value: "fixed-with-hyphens", captured: make(chan struct{})}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	snapshot := newTestSnapshot(t, 1, []byte("original-state"))
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "fixed-with-hyphens", 1, []byte("original")), snapshot))
	otherID, err := eventstore.NewAggregateID("Changed", "caller")
	require.NoError(t, err)
	otherEvent, err := eventstore.NewEventEnvelope(otherID, 1, time.Unix(0, 123456789), []byte("other"))
	require.NoError(t, err)
	require.NoError(t, store.persistEventAndSnapshot(otherEvent, newTestSnapshot(t, 1, []byte("other-state"))))
	store.mu.Lock()
	unlock := sync.OnceFunc(store.mu.Unlock)
	result := make(chan snapshotReadResult, 1)
	done := make(chan struct{})
	t.Cleanup(func() { unlock(); waitForReadSignal(t, done, "fixed-ID snapshot reader exit") })
	go func() {
		read, err := store.getLatestSnapshotByID(id)
		result <- snapshotReadResult{read, err}
		close(done)
	}()
	waitForReadSignal(t, id.captured, "caller ID capture")
	id.mu.Lock()
	id.typeName, id.value = "Changed", "caller"
	id.mu.Unlock()
	select {
	case <-done:
		t.Error("valid snapshot read returned while the store was write-locked")
	default:
	}

	unlock()
	waitForReadSignal(t, done, "fixed-ID snapshot read completion")
	actual := <-result

	require.NoError(t, actual.err)
	requireLatestSnapshot(t, actual.read, 1, &snapshot)
	id.mu.Lock()
	assert.Equal(t, 1, id.typeCalls)
	assert.Equal(t, 1, id.valueCalls)
	id.mu.Unlock()
	again, err := store.getLatestSnapshotByID(fixedID)
	require.NoError(t, err)
	requireLatestSnapshot(t, again, 1, &snapshot)
	t.Log("caller type and value changed during lock wait; each was captured once and the original aid selected its snapshot")
}

func TestGetLatestSnapshotSharesStoreAndIsolatesSeparateStores(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	firstCaller, secondCaller := store, store
	id, err := eventstore.NewAggregateID("Account", "shared-snapshot")
	require.NoError(t, err)
	snapshot := newTestSnapshot(t, 1, []byte("shared-state"))
	require.NoError(t, firstCaller.persistEventAndSnapshot(newTestEvent(t, "shared-snapshot", 1, []byte("first")), snapshot))
	before, err := secondCaller.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, before, 1, &snapshot)

	require.NoError(t, secondCaller.persistEvent(newTestEvent(t, "shared-snapshot", 2, []byte("second"))))
	after, err := firstCaller.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, after, 2, &snapshot)
	separate, err := NewStore()
	require.NoError(t, err)
	empty, err := separate.getLatestSnapshotByID(id)
	require.NoError(t, err)
	require.Nil(t, empty)
	isolated := newTestSnapshot(t, 1, []byte("isolated-state"))
	require.NoError(t, separate.persistEventAndSnapshot(newTestEvent(t, "shared-snapshot", 1, []byte("isolated")), isolated))
	other, err := separate.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, other, 1, &isolated)
	unchanged, err := firstCaller.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, unchanged, 2, &snapshot)
	requireLatestSnapshot(t, before, 1, &snapshot)
}

func TestGetLatestSnapshotProtectsInputValues(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id := &readTestID{"Account", "input-snapshot"}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	event := newTestEvent(t, "input-snapshot", 1, []byte("original-event"))
	snapshot := newTestSnapshot(t, 1, []byte{255, 0, 13, 10})
	expected := newTestSnapshot(t, 1, []byte{255, 0, 13, 10})
	require.NoError(t, store.persistEventAndSnapshot(event, snapshot))
	before, err := store.getLatestSnapshotByID(fixedID)
	require.NoError(t, err)
	requireLatestSnapshot(t, before, 1, &expected)

	event.Payload()[0] = 'X'
	snapshot.Aggregate()[0] = 77
	event = newTestEvent(t, "replacement", 9, []byte("replaced-event"))
	snapshot = newTestSnapshot(t, 9, []byte("replaced-state"))
	id.typeName, id.value = "Changed", "caller"
	after, err := store.getLatestSnapshotByID(fixedID)

	require.NoError(t, err)
	requireLatestSnapshot(t, after, 1, &expected)
	requireLatestSnapshot(t, before, 1, &expected)
	assert.Equal(t, eventstore.SeqNr(9), event.SeqNr())
	assert.Equal(t, eventstore.SeqNr(9), snapshot.SeqNr())
}

func TestGetLatestSnapshotProtectsReturnedValues(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "returned-snapshot")
	require.NoError(t, err)
	snapshot := newTestSnapshot(t, 1, []byte{255, 0, 13, 10})
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "returned-snapshot", 1, []byte("first")), snapshot))
	require.NoError(t, store.persistEvent(newTestEvent(t, "returned-snapshot", 2, []byte("second"))))
	first, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, first, 2, &snapshot)
	second, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, second, 2, &snapshot)

	first.Snapshot.Aggregate()[0] = 88
	first.HeadSeqNr = 9
	*first.Snapshot = newTestSnapshot(t, 9, []byte("replacement"))
	requireLatestSnapshot(t, second, 2, &snapshot)
	first.Snapshot = nil
	after, err := store.getLatestSnapshotByID(id)

	require.NoError(t, err)
	requireLatestSnapshot(t, after, 2, &snapshot)
	requireLatestSnapshot(t, second, 2, &snapshot)
}

func TestGetLatestSnapshotPreservesNilAndEmptyBytes(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "empty-snapshot")
	require.NoError(t, err)
	require.NoError(t, store.persistEvent(newTestEvent(t, "empty-snapshot", 1, nil)))
	headOnly, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, headOnly, 1, nil)

	nilSnapshot := newTestSnapshot(t, 2, nil)
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "empty-snapshot", 2, nil), nilSnapshot))
	nilBytes, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, nilBytes, 2, &nilSnapshot)
	require.Nil(t, nilBytes.Snapshot.Aggregate())

	emptySnapshot := newTestSnapshot(t, 3, []byte{})
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "empty-snapshot", 3, nil), emptySnapshot))
	emptyBytes, err := store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, emptyBytes, 3, &emptySnapshot)
	require.NotNil(t, emptyBytes.Snapshot.Aggregate())
	require.Empty(t, emptyBytes.Snapshot.Aggregate())
	requireLatestSnapshot(t, nilBytes, 2, &nilSnapshot)
}

func TestGetLatestSnapshotReadFailurePreservesRecordsAndReleasesLock(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "failed-snapshot")
	require.NoError(t, err)
	snapshot := newTestSnapshot(t, 1, []byte("original-state"))
	require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "failed-snapshot", 1, []byte("first")), snapshot))
	require.NoError(t, store.persistEvent(newTestEvent(t, "failed-snapshot", 2, []byte("second"))))
	cause := &testhook.InjectedError{Phase: testhook.PhaseReadSnapshot, Message: "read failed"}
	store.hooks = testhook.New()
	reads := 0
	store.hooks.OnFail(testhook.PhaseReadSnapshot, func(pt testhook.Point) error {
		reads++
		assert.Equal(t, "Account-failed-snapshot", pt.AggregateID)
		if reads == 1 {
			return cause
		}
		return nil
	})

	actual, err := store.getLatestSnapshotByID(id)

	require.Nil(t, actual)
	requireKind(t, err, eventstore.KindStorage)
	var storage *eventstore.StorageError
	require.ErrorAs(t, err, &storage)
	assert.ErrorIs(t, err, cause)
	assert.ErrorIs(t, storage.Unwrap(), cause)
	require.Equal(t, 1, reads)
	require.True(t, store.mu.TryLock(), "a failed snapshot read must release its lock")
	store.mu.Unlock()
	actual, err = store.getLatestSnapshotByID(id)
	require.NoError(t, err)
	requireLatestSnapshot(t, actual, 2, &snapshot)
	assert.Equal(t, 2, reads)
}

func TestGetLatestSnapshotWaitsForPublicationAndHoldsStoreReadLock(t *testing.T) {
	for _, tc := range []struct {
		name     string
		previous bool
		pair     bool
	}{
		{"pair create", false, true},
		{"pair update", true, true},
		{"event-only append", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			id := &observedReadID{typeName: "Account", value: "interleaved-snapshot", captured: make(chan struct{})}
			fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
			require.NoError(t, err)
			previous := newTestSnapshot(t, 1, []byte("previous-state"))
			seq := eventstore.SeqNr(1)
			if tc.previous {
				require.NoError(t, store.persistEventAndSnapshot(newTestEvent(t, "interleaved-snapshot", 1, []byte("previous")), previous))
				seq = 2
			}
			before, err := store.getLatestSnapshotByID(fixedID)
			require.NoError(t, err)
			if tc.previous {
				requireLatestSnapshot(t, before, 1, &previous)
			} else {
				require.Nil(t, before)
			}
			event := newTestEvent(t, "interleaved-snapshot", seq, []byte("candidate"))
			snapshot := newTestSnapshot(t, seq, []byte("candidate-state"))
			commitEntered, releaseCommit := make(chan struct{}), make(chan struct{})
			readEntered, releaseRead := make(chan struct{}), make(chan struct{})
			unblockCommit := sync.OnceFunc(func() { close(releaseCommit) })
			unblockRead := sync.OnceFunc(func() { close(releaseRead) })
			store.hooks = testhook.New()
			store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
				assert.Equal(t, "Account-interleaved-snapshot", pt.AggregateID)
				assert.Equal(t, int64(seq), pt.SeqNr)
				close(commitEntered)
				<-releaseCommit
				return nil
			})
			store.hooks.OnFail(testhook.PhaseReadSnapshot, func(pt testhook.Point) error {
				assert.Equal(t, "Account-interleaved-snapshot", pt.AggregateID)
				close(readEntered)
				<-releaseRead
				return nil
			})
			writeResult := make(chan error, 1)
			writeDone := make(chan struct{})
			t.Cleanup(func() {
				unblockCommit()
				unblockRead()
				waitForReadSignal(t, writeDone, "stopped snapshot writer exit")
			})
			go func() {
				if tc.pair {
					writeResult <- store.persistEventAndSnapshot(event, snapshot)
				} else {
					writeResult <- store.persistEvent(event)
				}
				close(writeDone)
			}()
			waitForReadSignal(t, commitEntered, "real append before publication")
			if store.mu.TryRLock() {
				store.mu.RUnlock()
				t.Error("read lock was available before publication")
			}
			if store.mu.TryLock() {
				store.mu.Unlock()
				t.Error("write lock was available before publication")
			}
			readResult := make(chan snapshotReadResult, 1)
			readDone := make(chan struct{})
			t.Cleanup(func() { unblockCommit(); unblockRead(); waitForReadSignal(t, readDone, "snapshot reader exit") })
			go func() {
				read, err := store.getLatestSnapshotByID(id)
				readResult <- snapshotReadResult{read, err}
				close(readDone)
			}()
			waitForReadSignal(t, id.captured, "snapshot read entry during the stopped append")
			select {
			case <-readEntered:
				t.Error("read-snapshot hook ran before publication")
			default:
			}
			select {
			case <-readDone:
				t.Error("snapshot read returned before publication")
			default:
			}

			unblockCommit()
			waitForReadSignal(t, writeDone, "append publication")
			require.NoError(t, <-writeResult)
			waitForReadSignal(t, readEntered, "snapshot read hook after publication")
			if store.mu.TryLock() {
				store.mu.Unlock()
				t.Error("snapshot read hook did not hold the store read lock")
			}
			if store.mu.TryRLock() {
				store.mu.RUnlock()
			} else {
				t.Error("a snapshot read must permit another store read lock")
			}
			unblockRead()
			waitForReadSignal(t, readDone, "snapshot read completion")
			actual := <-readResult

			require.NoError(t, actual.err)
			if tc.pair {
				requireLatestSnapshot(t, actual.read, seq, &snapshot)
			} else {
				requireLatestSnapshot(t, actual.read, seq, &previous)
			}
			if tc.previous {
				requireLatestSnapshot(t, before, 1, &previous)
			}
			require.True(t, store.mu.TryLock(), "the returned result must be outside the store read lock")
			store.mu.Unlock()
			t.Logf("%s stopped at the real commit hook; reader entered during the stop, waited and returned the complete committed head/current pair", tc.name)
		})
	}
}
