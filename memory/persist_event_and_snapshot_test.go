package memory

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPairTestStore(t *testing.T, history bool) *Store {
	t.Helper()
	var opts []eventstore.Option
	if history {
		count, err := eventstore.KeepLatest(3)
		require.NoError(t, err)
		opts = append(opts, eventstore.WithRetentionCount(count))
	}
	store, err := NewStore(opts...)
	require.NoError(t, err)
	return store
}

func newTestSnapshot(t *testing.T, seqNr eventstore.SeqNr, payload []byte) eventstore.SnapshotEnvelope[[]byte] {
	t.Helper()
	snapshot, err := eventstore.NewSnapshotEnvelope(payload, seqNr, eventstore.WithManifest(" snapshot/v1\x00日本語 "))
	require.NoError(t, err)
	return snapshot
}

// Expected values come from the input envelopes, independently of storage copy helpers.
// The real event read and internal observation must agree with those values.
func requirePairState(t *testing.T, store *Store, id eventstore.AggregateID, events []eventstore.EventEnvelope[[]byte], snapshots []eventstore.SnapshotEnvelope[[]byte], history bool) {
	t.Helper()
	aid, err := eventstore.AidString(id)
	require.NoError(t, err)
	actual, err := store.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, events, actual)
	head, state := store.observeState(aid)
	if len(events) == 0 {
		require.Nil(t, head)
		require.Nil(t, state)
		return
	}
	expected := &record{journal: make([]storedEvent, len(events))}
	for i, event := range events {
		expected.journal[i] = storedEvent{
			aggregateID: event.AggregateID(), seqNr: event.SeqNr(), occurredAt: event.OccurredAt(),
			manifest: event.Manifest(), payload: event.Payload(),
		}
	}
	for _, snapshot := range snapshots {
		value := storedSnapshot{seqNr: snapshot.SeqNr(), manifest: snapshot.Manifest(), payload: snapshot.Aggregate()}
		expected.current = &value
		if history {
			expected.history = append(expected.history, value)
		}
	}
	require.Equal(t, expected, state, "journal/current/history must match the committed inputs")
	require.Equal(t, &expected.journal[len(events)-1], head, "head must be the last committed event")
	var historySeqNrs []eventstore.SeqNr
	for _, snapshot := range state.history {
		historySeqNrs = append(historySeqNrs, snapshot.seqNr)
	}
	var currentSeqNr eventstore.SeqNr
	if state.current != nil {
		currentSeqNr = state.current.seqNr
	}
	t.Logf("head=%d journal=%d current=%d history=%v; metadata, manifest, nanoseconds and bytes match committed inputs", head.seqNr, len(state.journal), currentSeqNr, historySeqNrs)
}

func requirePairViolation(t *testing.T, err error, rule string, seqNr, snapshotSeqNr *eventstore.SeqNr) {
	t.Helper()
	requireKind(t, err, eventstore.KindContractViolation)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, err, &violation)
	assert.Equal(t, rule, violation.Rule)
	assert.Equal(t, seqNr, violation.SeqNr)
	assert.Equal(t, snapshotSeqNr, violation.SnapshotSeqNr)
	assert.Contains(t, err.Error(), rule)
	if seqNr != nil {
		assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", *seqNr))
	}
	if snapshotSeqNr != nil {
		assert.Contains(t, err.Error(), fmt.Sprintf("snapshot_seq_nr=%d", *snapshotSeqNr))
	}
	t.Logf("ContractViolation rule=%s message=%q", violation.Rule, err.Error())
}

func requirePairConflict(t *testing.T, err error, aid string, seqNr, headSeqNr eventstore.SeqNr) {
	t.Helper()
	requireKind(t, err, eventstore.KindOptimisticLock)
	var conflict *eventstore.OptimisticLockError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, aid, conflict.AggregateID)
	assert.Equal(t, seqNr, conflict.SeqNr)
	require.NotNil(t, conflict.HeadSeqNr)
	assert.Equal(t, headSeqNr, *conflict.HeadSeqNr)
	assert.Contains(t, err.Error(), aid)
	assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", seqNr))
	assert.Contains(t, err.Error(), fmt.Sprintf("head_seq_nr=%d", headSeqNr))
}

func TestPersistEventAndSnapshotCommitsCurrentAndHistory(t *testing.T) {
	for _, settings := range []string{"omitted", "explicit no retention", "history"} {
		t.Run(settings, func(t *testing.T) {
			history := settings == "history"
			store := newPairTestStore(t, history)
			if settings == "explicit no retention" {
				var err error
				store, err = NewStore(eventstore.WithRetentionCount(eventstore.NoRetention()))
				require.NoError(t, err)
			}
			id, err := eventstore.NewAggregateID("Account", "continuous-pair")
			require.NoError(t, err)
			events := newReadTestEvents(t, id)
			first := newTestSnapshot(t, 1, []byte{0, 255, 13, 10})
			second, err := eventstore.NewSnapshotEnvelope([]byte("second\x00state"), 2)
			require.NoError(t, err)
			requirePairState(t, store, id, nil, nil, history)

			require.NoError(t, store.persistEventAndSnapshot(events[0], first))
			requirePairState(t, store, id, events[:1], []eventstore.SnapshotEnvelope[[]byte]{first}, history)
			oldHead, oldState := store.observeState(events[0].AggregateID())
			oldRead, err := store.getEventsByIDSinceSeqNr(id, 0)
			require.NoError(t, err)

			require.NoError(t, store.persistEventAndSnapshot(events[1], second))
			requirePairState(t, store, id, events[:2], []eventstore.SnapshotEnvelope[[]byte]{first, second}, history)
			assert.Equal(t, eventstore.SeqNr(1), oldHead.seqNr)
			require.Len(t, oldState.journal, 1)
			assert.Equal(t, first.SeqNr(), oldState.current.seqNr)
			assert.Equal(t, first.Aggregate(), oldState.current.payload)
			requireReadEvents(t, events[:1], oldRead)

			// Event-only appends must preserve the snapshot fields of the extended record.
			require.NoError(t, store.persistEvent(events[2]))
			requirePairState(t, store, id, events, []eventstore.SnapshotEnvelope[[]byte]{first, second}, history)
			fourth := newTestEvent(t, "continuous-pair", 4, []byte("fourth"))
			fourthSnapshot := newTestSnapshot(t, 4, []byte("fourth-state"))
			require.NoError(t, store.persistEventAndSnapshot(fourth, fourthSnapshot))
			requirePairState(t, store, id, append(events, fourth), []eventstore.SnapshotEnvelope[[]byte]{first, second, fourthSnapshot}, history)
		})
	}
}

func TestPersistEventAndSnapshotSequenceChecksPreserveRecords(t *testing.T) {
	store := newPairTestStore(t, true)
	id, err := eventstore.NewAggregateID("Account", "pair-sequence")
	require.NoError(t, err)
	store.hooks = testhook.New()
	commitCalls := 0
	store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commitCalls++; return nil })
	var events []eventstore.EventEnvelope[[]byte]
	var snapshots []eventstore.SnapshotEnvelope[[]byte]
	for _, tc := range []struct {
		name string
		seq  eventstore.SeqNr
		kind eventstore.Kind
	}{
		{"missing head", 2, eventstore.KindContractViolation},
		{"create", 1, 0},
		{"duplicate create", 1, eventstore.KindOptimisticLock},
		{"gap after create", 3, eventstore.KindContractViolation},
		{"append", 2, 0},
		{"duplicate append", 2, eventstore.KindOptimisticLock},
		{"stale pair", 1, eventstore.KindOptimisticLock},
		{"gap after append", 4, eventstore.KindContractViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := newTestEvent(t, "pair-sequence", tc.seq, []byte(tc.name))
			snapshot := newTestSnapshot(t, tc.seq, []byte("snapshot-"+tc.name))
			beforeHead, beforeState := store.observeState(event.AggregateID())

			err := store.persistEventAndSnapshot(event, snapshot)

			switch tc.kind {
			case 0:
				require.NoError(t, err)
				events = append(events, event)
				snapshots = append(snapshots, snapshot)
			case eventstore.KindOptimisticLock:
				require.NotNil(t, beforeHead)
				requirePairConflict(t, err, event.AggregateID(), tc.seq, beforeHead.seqNr)
			case eventstore.KindContractViolation:
				requirePairViolation(t, err, "W-8", &tc.seq, nil)
			}
			if tc.kind != 0 {
				head, state := store.observeState(event.AggregateID())
				assert.Equal(t, beforeHead, head)
				assert.Equal(t, beforeState, state)
			}
			assert.Equal(t, len(events), commitCalls, "rejected writes must not reach publication")
			requirePairState(t, store, id, events, snapshots, true)
		})
	}
	assert.Equal(t, 2, commitCalls)
}

func TestPersistEventAndSnapshotRejectsInvalidInputsBeforeLock(t *testing.T) {
	for _, tc := range []struct {
		name string
		rule string
		seq  eventstore.SeqNr
	}{
		{"snapshot zero", "W-9", 0},
		{"snapshot differs", "W-9", 3},
		{"unconstructed event", "T-2", 2},
		{"unconstructed snapshot", "T-10", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newPairTestStore(t, true)
			id, err := eventstore.NewAggregateID("Account", "pair-invalid")
			require.NoError(t, err)
			first := newTestEvent(t, "pair-invalid", 1, []byte("first"))
			firstSnapshot := newTestSnapshot(t, 1, []byte("first-state"))
			require.NoError(t, store.persistEventAndSnapshot(first, firstSnapshot))
			beforeHead, beforeState := store.observeState(first.AggregateID())
			event := newTestEvent(t, "pair-invalid", 2, []byte("rejected"))
			snapshot := newTestSnapshot(t, tc.seq, []byte("rejected-state"))
			if tc.rule == "T-2" {
				event = eventstore.EventEnvelope[[]byte]{}
			}
			if tc.rule == "T-10" {
				snapshot = eventstore.SnapshotEnvelope[[]byte]{}
			}
			store.hooks = testhook.New()
			var commits atomic.Int32
			store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commits.Add(1); return nil })
			store.mu.Lock()
			unlock := sync.OnceFunc(store.mu.Unlock)
			result := make(chan error, 1)
			done := make(chan struct{})
			t.Cleanup(func() { unlock(); waitForReadSignal(t, done, "invalid pair writer exit") })
			go func() { result <- store.persistEventAndSnapshot(event, snapshot); close(done) }()

			waitForReadSignal(t, done, "pair rejection before taking the store lock")
			err = <-result
			if tc.rule == "W-9" {
				seq := event.SeqNr()
				requirePairViolation(t, err, tc.rule, &seq, &tc.seq)
			} else {
				requirePairViolation(t, err, tc.rule, nil, nil)
			}
			assert.Zero(t, commits.Load())
			unlock()
			head, state := store.observeState(first.AggregateID())
			assert.Equal(t, beforeHead, head)
			assert.Equal(t, beforeState, state)
			emptyHead, emptyState := store.observeState("")
			assert.Nil(t, emptyHead)
			assert.Nil(t, emptyState)
			requirePairState(t, store, id, []eventstore.EventEnvelope[[]byte]{first}, []eventstore.SnapshotEnvelope[[]byte]{firstSnapshot}, true)
		})
	}
}

func TestPersistEventAndSnapshotRejectsZeroEventConstruction(t *testing.T) {
	store := newPairTestStore(t, true)
	id, err := eventstore.NewAggregateID("Account", "pair-zero")
	require.NoError(t, err)
	first := newTestEvent(t, "pair-zero", 1, []byte("first"))
	firstSnapshot := newTestSnapshot(t, 1, []byte("first-state"))
	require.NoError(t, store.persistEventAndSnapshot(first, firstSnapshot))
	beforeHead, beforeState := store.observeState(first.AggregateID())
	snapshot := newTestSnapshot(t, 0, []byte("zero-state"))

	event, err := eventstore.NewEventEnvelope(id, 0, time.Unix(0, 123456789), []byte("zero"))

	zero := eventstore.SeqNr(0)
	requirePairViolation(t, err, "W-6", &zero, nil)
	// A failed construction returns an unconstructed envelope; its entrance error is T-2.
	err = store.persistEventAndSnapshot(event, snapshot)
	requirePairViolation(t, err, "T-2", nil, nil)
	head, state := store.observeState(first.AggregateID())
	assert.Equal(t, beforeHead, head)
	assert.Equal(t, beforeState, state)
	requirePairState(t, store, id, []eventstore.EventEnvelope[[]byte]{first}, []eventstore.SnapshotEnvelope[[]byte]{firstSnapshot}, true)
}

func TestPersistEventAndSnapshotPreparationFailureAndRetry(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, seq := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprintf("history=%v/seq=%d", history, seq), func(t *testing.T) {
				store := newPairTestStore(t, history)
				id, err := eventstore.NewAggregateID("Account", "pair-failure")
				require.NoError(t, err)
				var events []eventstore.EventEnvelope[[]byte]
				var snapshots []eventstore.SnapshotEnvelope[[]byte]
				if seq == 2 {
					first := newTestEvent(t, "pair-failure", 1, []byte("first"))
					firstSnapshot := newTestSnapshot(t, 1, []byte("first-state"))
					require.NoError(t, store.persistEventAndSnapshot(first, firstSnapshot))
					events = append(events, first)
					snapshots = append(snapshots, firstSnapshot)
				}
				event := newTestEvent(t, "pair-failure", seq, []byte("candidate"))
				snapshot := newTestSnapshot(t, seq, []byte("candidate-state"))
				beforeHead, beforeState := store.observeState(event.AggregateID())
				cause := &testhook.InjectedError{Phase: testhook.PhaseCommit, Message: "pair preparation failed"}
				store.hooks = testhook.New()
				calls := 0
				store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
					calls++
					assert.Equal(t, event.AggregateID(), pt.AggregateID)
					assert.Equal(t, int64(seq), pt.SeqNr)
					assert.Equal(t, event.Payload(), pt.Payload)
					assert.Equal(t, beforeState, store.records[pt.AggregateID], "all actual fields must still be the previous record at preparation")
					if calls == 1 {
						return cause
					}
					return nil
				})

				err = store.persistEventAndSnapshot(event, snapshot)

				requireKind(t, err, eventstore.KindStorage)
				var storage *eventstore.StorageError
				require.ErrorAs(t, err, &storage)
				assert.ErrorIs(t, err, cause)
				assert.ErrorIs(t, storage.Unwrap(), cause)
				assert.Equal(t, 1, calls)
				head, state := store.observeState(event.AggregateID())
				assert.Equal(t, beforeHead, head)
				assert.Equal(t, beforeState, state)
				requirePairState(t, store, id, events, snapshots, history)

				require.NoError(t, store.persistEventAndSnapshot(event, snapshot))

				assert.Equal(t, 2, calls)
				requirePairState(t, store, id, append(events, event), append(snapshots, snapshot), history)
				t.Logf("Storage=1 same-number retry success=1 commit hooks=%d; failed preparation left the complete record unchanged", calls)
			})
		}
	}
}

func TestPersistEventAndSnapshotOwnsInputReturnedAndObservedValues(t *testing.T) {
	store := newPairTestStore(t, true)
	id := &readTestID{"Account", "pair-copies"}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	inputs := newReadTestEvents(t, id)[:2]
	snapshots := []eventstore.SnapshotEnvelope[[]byte]{
		newTestSnapshot(t, 1, []byte{255, 0, 13, 10}),
		newTestSnapshot(t, 2, []byte("second-state")),
	}
	var expectedEvents []eventstore.EventEnvelope[[]byte]
	var expectedSnapshots []eventstore.SnapshotEnvelope[[]byte]
	for i, event := range inputs {
		copiedEvent, err := eventstore.NewEventEnvelope(fixedID, event.SeqNr(), event.OccurredAt(), bytes.Clone(event.Payload()), eventstore.WithManifest(event.Manifest()))
		require.NoError(t, err)
		expectedEvents = append(expectedEvents, copiedEvent)
		copiedSnapshot, err := eventstore.NewSnapshotEnvelope(bytes.Clone(snapshots[i].Aggregate()), snapshots[i].SeqNr(), eventstore.WithManifest(snapshots[i].Manifest()))
		require.NoError(t, err)
		expectedSnapshots = append(expectedSnapshots, copiedSnapshot)
	}
	store.hooks = testhook.New()
	var hookPayloads [][]byte
	store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
		i := int(pt.SeqNr) - 1
		// The entrance must already own both byte slices and the event metadata.
		inputs[i].Payload()[0] = 99
		snapshots[i].Aggregate()[0] = 88
		pt.Payload[0] = 77
		hookPayloads = append(hookPayloads, pt.Payload)
		inputs[i] = newTestEvent(t, "replacement", 9, []byte("replaced-event"))
		snapshots[i] = newTestSnapshot(t, 9, []byte("replaced-state"))
		return nil
	})
	for i := range inputs {
		require.NoError(t, store.persistEventAndSnapshot(inputs[i], snapshots[i]))
	}
	id.typeName, id.value = "Changed", "caller"
	for _, payload := range hookPayloads {
		payload[1] = 66
	}
	requirePairState(t, store, fixedID, expectedEvents, expectedSnapshots, true)
	returned, err := store.getEventsByIDSinceSeqNr(fixedID, 0)
	require.NoError(t, err)
	otherReturned, err := store.getEventsByIDSinceSeqNr(fixedID, 0)
	require.NoError(t, err)
	head, observed := store.observeState(expectedEvents[0].AggregateID())
	otherHead, otherObserved := store.observeState(expectedEvents[0].AggregateID())

	for i := range returned {
		returned[i].Payload()[0] = 55
	}
	returned[0] = newTestEvent(t, "returned-replacement", 9, []byte("returned"))
	returned = append(returned, returned[0])
	head.payload[0] = 44
	assert.Equal(t, expectedEvents[1].Payload(), observed.journal[1].payload, "head and journal observation bytes must be independent")
	head.aggregateID, head.seqNr, head.occurredAt, head.manifest = "Changed-head", 9, time.Unix(1, 0), "changed"
	observed.current.payload[0] = 33
	assert.Equal(t, expectedSnapshots[1].Aggregate(), observed.history[1].payload, "current and history observation bytes must be independent")
	observed.current.seqNr, observed.current.manifest = 9, "changed"
	for i := range observed.journal {
		observed.journal[i].payload[0] = 22
		observed.journal[i].aggregateID = "Changed-journal"
		observed.journal[i].seqNr = 9
		observed.journal[i].occurredAt = time.Unix(2, 0)
		observed.journal[i].manifest = "changed"
	}
	for i := range observed.history {
		observed.history[i].payload[0] = 11
		observed.history[i].seqNr, observed.history[i].manifest = 9, "changed"
	}
	observed.journal = append(observed.journal, observed.journal[0])
	observed.history = append(observed.history, observed.history[0])

	requirePairState(t, store, fixedID, expectedEvents, expectedSnapshots, true)
	requireReadEvents(t, expectedEvents, otherReturned)
	afterHead, afterState := store.observeState(expectedEvents[0].AggregateID())
	assert.Equal(t, otherHead, afterHead)
	assert.Equal(t, otherObserved, afterState)
	require.Len(t, returned, 3)
	require.Len(t, observed.journal, 3)
	require.Len(t, observed.history, 3)
	t.Log("input metadata/bytes, hook bytes, returned envelopes and all observation fields changed; saved record and separate results remain unchanged")
}

func TestPersistEventAndSnapshotPreservesNilAndEmptyBytes(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, payload := range [][]byte{nil, {}} {
			for _, aggregate := range [][]byte{nil, {}} {
				t.Run(fmt.Sprintf("history=%v/event-nil=%v/snapshot-nil=%v", history, payload == nil, aggregate == nil), func(t *testing.T) {
					store := newPairTestStore(t, history)
					id, err := eventstore.NewAggregateID("Account", "pair-empty")
					require.NoError(t, err)
					event := newTestEvent(t, "pair-empty", 1, payload)
					snapshot := newTestSnapshot(t, 1, aggregate)

					require.NoError(t, store.persistEventAndSnapshot(event, snapshot))

					requirePairState(t, store, id, []eventstore.EventEnvelope[[]byte]{event}, []eventstore.SnapshotEnvelope[[]byte]{snapshot}, history)
				})
			}
		}
	}
}

func TestPersistEventAndSnapshotSharesStoreAndIsolatesSeparateStores(t *testing.T) {
	store := newPairTestStore(t, true)
	firstCaller, secondCaller := store, store
	id, err := eventstore.NewAggregateID("Account", "pair-shared")
	require.NoError(t, err)
	events := newReadTestEvents(t, id)[:2]
	snapshots := []eventstore.SnapshotEnvelope[[]byte]{
		newTestSnapshot(t, 1, []byte("first-state")),
		newTestSnapshot(t, 2, []byte("second-state")),
	}

	require.NoError(t, firstCaller.persistEventAndSnapshot(events[0], snapshots[0]))
	requirePairState(t, secondCaller, id, events[:1], snapshots[:1], true)
	require.NoError(t, secondCaller.persistEventAndSnapshot(events[1], snapshots[1]))
	requirePairState(t, firstCaller, id, events, snapshots, true)
	beforeHead, beforeState := store.observeState(events[0].AggregateID())
	separate := newPairTestStore(t, true)
	requirePairState(t, separate, id, nil, nil, true)
	otherEvent := newTestEvent(t, "pair-shared", 1, []byte("isolated-event"))
	otherSnapshot := newTestSnapshot(t, 1, []byte("isolated-state"))
	require.NoError(t, separate.persistEventAndSnapshot(otherEvent, otherSnapshot))

	requirePairState(t, separate, id, []eventstore.EventEnvelope[[]byte]{otherEvent}, []eventstore.SnapshotEnvelope[[]byte]{otherSnapshot}, true)
	requirePairState(t, store, id, events, snapshots, true)
	head, state := store.observeState(events[0].AggregateID())
	assert.Equal(t, beforeHead, head)
	assert.Equal(t, beforeState, state)
}

func TestPersistEventAndSnapshotParallelCommits(t *testing.T) {
	const workers = 32
	for _, history := range []bool{false, true} {
		for _, seq := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprintf("history=%v/seq=%d", history, seq), func(t *testing.T) {
				store := newPairTestStore(t, history)
				id, err := eventstore.NewAggregateID("Account", "parallel-pair")
				require.NoError(t, err)
				var previousEvents []eventstore.EventEnvelope[[]byte]
				var previousSnapshots []eventstore.SnapshotEnvelope[[]byte]
				if seq == 2 {
					first := newTestEvent(t, "parallel-pair", 1, []byte("first"))
					firstSnapshot := newTestSnapshot(t, 1, []byte("first-state"))
					require.NoError(t, store.persistEventAndSnapshot(first, firstSnapshot))
					previousEvents = append(previousEvents, first)
					previousSnapshots = append(previousSnapshots, firstSnapshot)
				}
				var events []eventstore.EventEnvelope[[]byte]
				var snapshots []eventstore.SnapshotEnvelope[[]byte]
				for i := 0; i < workers; i++ {
					event, err := eventstore.NewEventEnvelope(id, seq, time.Unix(int64(i), int64(i)+123456789), []byte(fmt.Sprintf("event-%d", i)), eventstore.WithManifest(fmt.Sprintf("event/v%d", i)))
					require.NoError(t, err)
					snapshot, err := eventstore.NewSnapshotEnvelope([]byte(fmt.Sprintf("state-%d", i)), seq, eventstore.WithManifest(fmt.Sprintf("snapshot/v%d", i)))
					require.NoError(t, err)
					events = append(events, event)
					snapshots = append(snapshots, snapshot)
				}
				store.hooks = testhook.New()
				var commits atomic.Int32
				store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commits.Add(1); return nil })
				type outcome struct {
					worker int
					err    error
				}
				results := make(chan outcome, workers)
				start := make(chan struct{})
				var ready, finished sync.WaitGroup
				ready.Add(workers)
				finished.Add(workers)
				t.Cleanup(finished.Wait)
				for i := 0; i < workers; i++ {
					go func() {
						defer finished.Done()
						ready.Done()
						<-start
						results <- outcome{worker: i, err: store.persistEventAndSnapshot(events[i], snapshots[i])}
					}()
				}
				ready.Wait()
				close(start)
				successes, conflicts, winner := 0, 0, -1
				for i := 0; i < workers; i++ {
					result := <-results
					if result.err == nil {
						successes++
						winner = result.worker
					} else {
						requirePairConflict(t, result.err, events[result.worker].AggregateID(), seq, seq)
						conflicts++
					}
				}
				finished.Wait()

				require.Equal(t, 1, successes)
				assert.Equal(t, workers-1, conflicts)
				assert.Equal(t, int32(1), commits.Load())
				requirePairState(t, store, id, append(previousEvents, events[winner]), append(previousSnapshots, snapshots[winner]), history)
				t.Logf("goroutines=%d success=%d OptimisticLock=%d commit hooks=%d winner=%d; head/journal/current/history contain the same winner's complete pair", workers, successes, conflicts, commits.Load(), winner)
			})
		}
	}
}

func TestPersistEventAndSnapshotHoldsStoreLockUntilPublication(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, seq := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprintf("history=%v/seq=%d", history, seq), func(t *testing.T) {
				store := newPairTestStore(t, history)
				id := &observedReadID{typeName: "Account", value: "stopped-pair", captured: make(chan struct{})}
				fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
				require.NoError(t, err)
				var previousEvents []eventstore.EventEnvelope[[]byte]
				var previousSnapshots []eventstore.SnapshotEnvelope[[]byte]
				if seq == 2 {
					first := newTestEvent(t, "stopped-pair", 1, []byte("first"))
					firstSnapshot := newTestSnapshot(t, 1, []byte("first-state"))
					require.NoError(t, store.persistEventAndSnapshot(first, firstSnapshot))
					previousEvents = append(previousEvents, first)
					previousSnapshots = append(previousSnapshots, firstSnapshot)
				}
				event := newTestEvent(t, "stopped-pair", seq, []byte("candidate"))
				snapshot := newTestSnapshot(t, seq, []byte("candidate-state"))
				beforeHead, beforeState := store.observeState(event.AggregateID())
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				var beforePublication *record
				store.hooks = testhook.New()
				store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
					if pt.AggregateID == event.AggregateID() {
						beforePublication = store.records[pt.AggregateID]
						close(entered)
						<-release
					}
					return nil
				})
				readEntered := make(chan struct{})
				notifyRead := sync.OnceFunc(func() { close(readEntered) })
				store.hooks.OnFail(testhook.PhaseReadEvents, func(testhook.Point) error { notifyRead(); return nil })
				writeResult := make(chan error, 1)
				writeDone := make(chan struct{})
				t.Cleanup(func() { unblock(); waitForReadSignal(t, writeDone, "stopped pair writer exit") })
				go func() { writeResult <- store.persistEventAndSnapshot(event, snapshot); close(writeDone) }()
				waitForReadSignal(t, entered, "real pair preparation before publication")
				assert.Equal(t, beforeState, beforePublication, "actual journal/current/history must still be the complete old record")
				if beforeHead != nil {
					assert.Equal(t, *beforeHead, beforePublication.journal[len(beforePublication.journal)-1])
				}
				if store.mu.TryLock() {
					store.mu.Unlock()
					t.Error("store write lock was released before publication")
				}
				if store.mu.TryRLock() {
					store.mu.RUnlock()
					t.Error("store read lock was available before publication")
				}
				readResult := make(chan eventReadResult, 1)
				readDone := make(chan struct{})
				t.Cleanup(func() { unblock(); waitForReadSignal(t, readDone, "pair reader exit") })
				go func() {
					events, err := store.getEventsByIDSinceSeqNr(id, 0)
					readResult <- eventReadResult{events, err}
					close(readDone)
				}()
				waitForReadSignal(t, id.captured, "real event read entry during the stopped pair write")
				type observation struct {
					head  *storedEvent
					state *record
				}
				observedResult := make(chan observation, 1)
				observeStarted, observeDone := make(chan struct{}), make(chan struct{})
				t.Cleanup(func() { unblock(); waitForReadSignal(t, observeDone, "pair observer exit") })
				go func() {
					close(observeStarted)
					head, state := store.observeState(event.AggregateID())
					observedResult <- observation{head, state}
					close(observeDone)
				}()
				waitForReadSignal(t, observeStarted, "internal observation start during the stopped pair write")
				other := newTestEvent(t, "stopped-pair-other", 1, []byte("other"))
				otherSnapshot := newTestSnapshot(t, 1, []byte("other-state"))
				otherResult := make(chan error, 1)
				otherStarted, otherDone := make(chan struct{}), make(chan struct{})
				t.Cleanup(func() { unblock(); waitForReadSignal(t, otherDone, "other aggregate writer exit") })
				go func() {
					close(otherStarted)
					otherResult <- store.persistEventAndSnapshot(other, otherSnapshot)
					close(otherDone)
				}()
				waitForReadSignal(t, otherStarted, "another aggregate writer during the stopped pair write")
				for _, check := range []struct {
					name   string
					signal <-chan struct{}
				}{
					{"event read hook", readEntered}, {"event read", readDone},
					{"internal observation", observeDone}, {"other aggregate write", otherDone},
				} {
					select {
					case <-check.signal:
						t.Errorf("%s completed before pair publication", check.name)
					default:
					}
				}
				independent := newPairTestStore(t, history)
				independentEvent := newTestEvent(t, "stopped-pair", 1, []byte("independent"))
				independentSnapshot := newTestSnapshot(t, 1, []byte("independent-state"))
				require.NoError(t, independent.persistEventAndSnapshot(independentEvent, independentSnapshot))
				requirePairState(t, independent, fixedID, []eventstore.EventEnvelope[[]byte]{independentEvent}, []eventstore.SnapshotEnvelope[[]byte]{independentSnapshot}, history)

				unblock()

				waitForReadSignal(t, writeDone, "pair publication")
				waitForReadSignal(t, readDone, "event read after pair publication")
				waitForReadSignal(t, observeDone, "internal observation after pair publication")
				waitForReadSignal(t, otherDone, "other aggregate publication")
				require.NoError(t, <-writeResult)
				require.NoError(t, <-otherResult)
				actualRead := <-readResult
				require.NoError(t, actualRead.err)
				expectedEvents := append(previousEvents, event)
				expectedSnapshots := append(previousSnapshots, snapshot)
				requireReadEvents(t, expectedEvents, actualRead.events)
				requirePairState(t, store, fixedID, expectedEvents, expectedSnapshots, history)
				observed := <-observedResult
				head, state := store.observeState(event.AggregateID())
				assert.Equal(t, head, observed.head)
				assert.Equal(t, state, observed.state)
				otherID, err := eventstore.NewAggregateID("Account", "stopped-pair-other")
				require.NoError(t, err)
				requirePairState(t, store, otherID, []eventstore.EventEnvelope[[]byte]{other}, []eventstore.SnapshotEnvelope[[]byte]{otherSnapshot}, history)
				t.Log("stopped at the real pre-publication hook; old record preserved, same-Store readers/writer waited, separate Store progressed; readers obtained the complete committed pair")
			})
		}
	}
}
