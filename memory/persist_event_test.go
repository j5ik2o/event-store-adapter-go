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

func TestPersistEventSequenceChecksPreserveRecords(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	store.hooks = testhook.New()
	commitCalls := 0
	store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commitCalls++; return nil })
	counts := map[eventstore.Kind]int{}
	successes := 0
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
		{"stale event", 1, eventstore.KindOptimisticLock},
		{"gap after append", 4, eventstore.KindContractViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := newTestEvent(t, "sequence", tc.seq, []byte(tc.name))
			beforeHead, beforeJournal := store.observe(event.AggregateID())

			err := store.persistEvent(event)

			if tc.kind == 0 {
				require.NoError(t, err)
				successes++
			} else {
				requireKind(t, err, tc.kind)
				counts[tc.kind]++
				if tc.kind == eventstore.KindOptimisticLock {
					var conflict *eventstore.OptimisticLockError
					require.ErrorAs(t, err, &conflict)
					assert.Equal(t, event.AggregateID(), conflict.AggregateID)
					assert.Equal(t, tc.seq, conflict.SeqNr)
					require.NotNil(t, beforeHead)
					require.NotNil(t, conflict.HeadSeqNr)
					assert.Equal(t, beforeHead.seqNr, *conflict.HeadSeqNr)
					assert.Contains(t, err.Error(), event.AggregateID())
					assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", tc.seq))
					assert.Contains(t, err.Error(), fmt.Sprintf("head_seq_nr=%d", beforeHead.seqNr))
				} else {
					var violation *eventstore.ContractViolationError
					require.ErrorAs(t, err, &violation)
					assert.Equal(t, "W-8", violation.Rule)
					require.NotNil(t, violation.SeqNr)
					assert.Equal(t, tc.seq, *violation.SeqNr)
					assert.Contains(t, err.Error(), "W-8")
					assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", tc.seq))
				}
			}
			head, journal := store.observe(event.AggregateID())
			if tc.kind != 0 {
				assert.Equal(t, beforeHead, head)
				assert.Equal(t, beforeJournal, journal)
			} else {
				require.NotNil(t, head)
				assert.Equal(t, tc.seq, head.seqNr)
				require.Len(t, journal, successes)
				assert.Equal(t, *head, journal[len(journal)-1])
				assert.Equal(t, event.Payload(), head.payload)
			}
			assert.Equal(t, successes, commitCalls, "rejections must not reach publication")
		})
	}
	assert.Equal(t, 2, successes)
	assert.Equal(t, 3, counts[eventstore.KindOptimisticLock])
	assert.Equal(t, 3, counts[eventstore.KindContractViolation])
	t.Logf("success=%d OptimisticLock=%d ContractViolation=%d commit hooks=%d head=2 journal=2", successes, counts[eventstore.KindOptimisticLock], counts[eventstore.KindContractViolation], commitCalls)
}

func TestPersistEventRejectsUnconstructedEnvelope(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	first := newTestEvent(t, "validated", 1, []byte("first"))
	require.NoError(t, store.persistEvent(first))
	beforeHead, beforeJournal := store.observe(first.AggregateID())
	store.hooks = testhook.New()
	commitCalls := 0
	store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commitCalls++; return nil })

	err = store.persistEvent(eventstore.EventEnvelope[[]byte]{})

	requireKind(t, err, eventstore.KindContractViolation)
	var violation *eventstore.ContractViolationError
	require.ErrorAs(t, err, &violation)
	assert.Equal(t, "T-2", violation.Rule)
	head, journal := store.observe(first.AggregateID())
	assert.Equal(t, beforeHead, head)
	assert.Equal(t, beforeJournal, journal)
	emptyHead, emptyJournal := store.observe("")
	assert.Nil(t, emptyHead)
	assert.Empty(t, emptyJournal)
	assert.Zero(t, commitCalls)
}

func TestPersistEventPreparationFailureAndRetry(t *testing.T) {
	for _, seq := range []eventstore.SeqNr{1, 2} {
		t.Run(fmt.Sprintf("seq=%d", seq), func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			if seq == 2 {
				require.NoError(t, store.persistEvent(newTestEvent(t, "failure", 1, []byte("first"))))
			}
			event := newTestEvent(t, "failure", seq, []byte("candidate"))
			beforeHead, beforeJournal := store.observe(event.AggregateID())
			cause := &testhook.InjectedError{Phase: testhook.PhaseCommit, Message: "prepare failed"}
			store.hooks = testhook.New()
			calls := 0
			store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
				calls++
				assert.Equal(t, event.AggregateID(), pt.AggregateID)
				assert.Equal(t, int64(seq), pt.SeqNr)
				assert.Equal(t, event.Payload(), pt.Payload)
				if calls == 1 {
					return cause
				}
				return nil
			})

			err = store.persistEvent(event)

			requireKind(t, err, eventstore.KindStorage)
			var storage *eventstore.StorageError
			require.ErrorAs(t, err, &storage)
			assert.ErrorIs(t, err, cause)
			assert.ErrorIs(t, storage.Unwrap(), cause)
			assert.Equal(t, 1, calls)
			head, journal := store.observe(event.AggregateID())
			assert.Equal(t, beforeHead, head)
			assert.Equal(t, beforeJournal, journal)

			require.NoError(t, store.persistEvent(event))

			assert.Equal(t, 2, calls)
			head, journal = store.observe(event.AggregateID())
			require.NotNil(t, head)
			assert.Equal(t, seq, head.seqNr)
			require.Len(t, journal, int(seq))
			assert.Equal(t, []byte("candidate"), head.payload)
			for i := range beforeJournal {
				assert.Equal(t, beforeJournal[i], journal[i])
			}
			t.Logf("Storage=1 retry success=1 commit hooks=%d head=%d journal=%d; failed preparation left records unchanged", calls, head.seqNr, len(journal))
		})
	}
}

func TestPersistEventProtectsInputAndObservation(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	first := newTestEvent(t, "copies", 1, []byte{0, 255, 13, 10})
	second := newTestEvent(t, "copies", 2, []byte("second"))
	require.NoError(t, store.persistEvent(first))
	firstHead, firstJournal := store.observe(first.AggregateID())
	first.Payload()[0] = 99
	require.NoError(t, store.persistEvent(second))
	second.Payload()[0] = 'X'
	head, journal := store.observe(first.AggregateID())
	require.NotNil(t, head)
	require.Len(t, journal, 2)
	assert.Equal(t, []byte{0, 255, 13, 10}, journal[0].payload)
	assert.Equal(t, []byte("second"), head.payload)
	assert.Equal(t, first.AggregateID(), head.aggregateID)
	assert.Equal(t, second.SeqNr(), head.seqNr)
	assert.Equal(t, second.OccurredAt(), head.occurredAt)
	assert.Equal(t, second.Manifest(), head.manifest)
	assert.Equal(t, *firstHead, journal[0])
	assert.Equal(t, firstJournal[0], journal[0])
	wantHead, wantJournal := store.observe(first.AggregateID())

	head.payload[0] = 'Y'
	head.aggregateID = "Account-changed"
	head.seqNr = 9
	head.occurredAt = time.Unix(1, 0)
	head.manifest = "changed"
	journal[0].payload[0] = 88
	journal[1].payload[0] = 'Z'
	journal[1] = journal[0]
	journal = append(journal, journal[0])
	assert.Len(t, journal, 3)
	firstJournal[0].payload[1] = 77

	gotHead, gotJournal := store.observe(first.AggregateID())
	assert.Equal(t, wantHead, gotHead)
	assert.Equal(t, wantJournal, gotJournal)
	require.Len(t, gotJournal, 2)
	assert.Equal(t, *gotHead, gotJournal[1])
	t.Log("success=2 head=2 journal=2; input and observation mutation did not change saved bytes or metadata")
}

func TestPersistEventCopiesBeforeCommitHook(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	input := []byte("original")
	event := newTestEvent(t, "hook-copy", 1, input)
	original := event
	store.hooks = testhook.New()
	var hookPayload []byte
	store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
		hookPayload = pt.Payload
		assert.Equal(t, []byte("original"), hookPayload)
		input[0] = 'X'
		hookPayload[1] = 'Y'
		event = newTestEvent(t, "changed", 9, []byte("replacement"))
		return nil
	})

	require.NoError(t, store.persistEvent(event))
	hookPayload[2] = 'Z'

	head, journal := store.observe(original.AggregateID())
	require.NotNil(t, head)
	require.Len(t, journal, 1)
	assert.Equal(t, []byte("original"), head.payload)
	assert.Equal(t, original.AggregateID(), head.aggregateID)
	assert.Equal(t, original.SeqNr(), head.seqNr)
	assert.Equal(t, original.OccurredAt(), head.occurredAt)
	assert.Equal(t, original.Manifest(), head.manifest)
	assert.Equal(t, *head, journal[0])
	otherHead, otherJournal := store.observe(event.AggregateID())
	assert.Nil(t, otherHead)
	assert.Empty(t, otherJournal)
}

func TestPersistEventPreservesNilAndEmptyBytes(t *testing.T) {
	for _, payload := range [][]byte{nil, {}} {
		t.Run(fmt.Sprintf("nil=%v", payload == nil), func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			event := newTestEvent(t, "empty", 1, payload)

			require.NoError(t, store.persistEvent(event))

			head, journal := store.observe(event.AggregateID())
			require.NotNil(t, head)
			require.Len(t, journal, 1)
			assert.Equal(t, payload, head.payload)
			assert.Equal(t, payload, journal[0].payload)
		})
	}
}

func TestPersistEventUsesExactAggregateID(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	for _, value := range []string{"key", "key-extra"} {
		event := newTestEvent(t, value, 1, []byte(value))
		require.NoError(t, store.persistEvent(event))
	}

	for _, value := range []string{"key", "key-extra"} {
		head, journal := store.observe("Account-" + value)
		require.NotNil(t, head)
		assert.Equal(t, "Account-"+value, head.aggregateID)
		assert.Equal(t, []byte(value), head.payload)
		assert.Len(t, journal, 1)
	}
	head, journal := store.observe("Account-ke")
	assert.Nil(t, head)
	assert.Empty(t, journal)
}

func TestPersistEventParallelCommits(t *testing.T) {
	const workers = 32
	for _, seq := range []eventstore.SeqNr{1, 2} {
		t.Run(fmt.Sprintf("seq=%d", seq), func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			if seq == 2 {
				require.NoError(t, store.persistEvent(newTestEvent(t, "parallel", 1, []byte("first"))))
			}
			store.hooks = testhook.New()
			var commitCalls atomic.Int32
			store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { commitCalls.Add(1); return nil })
			type outcome struct {
				payload []byte
				err     error
			}
			results := make(chan outcome, workers)
			start := make(chan struct{})
			var ready sync.WaitGroup
			ready.Add(workers)
			for i := 0; i < workers; i++ {
				event := newTestEvent(t, "parallel", seq, []byte(fmt.Sprintf("worker-%d", i)))
				go func() {
					ready.Done()
					<-start
					results <- outcome{payload: event.Payload(), err: store.persistEvent(event)}
				}()
			}
			ready.Wait()
			close(start)
			successes, conflicts := 0, 0
			var winner []byte
			for i := 0; i < workers; i++ {
				result := <-results
				if result.err == nil {
					successes++
					winner = bytes.Clone(result.payload)
				} else {
					requireKind(t, result.err, eventstore.KindOptimisticLock)
					var conflict *eventstore.OptimisticLockError
					require.ErrorAs(t, result.err, &conflict)
					assert.Equal(t, "Account-parallel", conflict.AggregateID)
					assert.Equal(t, seq, conflict.SeqNr)
					require.NotNil(t, conflict.HeadSeqNr)
					assert.Equal(t, seq, *conflict.HeadSeqNr)
					conflicts++
				}
			}

			assert.Equal(t, 1, successes)
			assert.Equal(t, workers-1, conflicts)
			assert.Equal(t, int32(1), commitCalls.Load())
			head, journal := store.observe("Account-parallel")
			require.NotNil(t, head)
			assert.Equal(t, seq, head.seqNr)
			require.Len(t, journal, int(seq))
			assert.Equal(t, winner, head.payload)
			assert.Equal(t, *head, journal[len(journal)-1])
			if seq == 2 {
				assert.Equal(t, []byte("first"), journal[0].payload)
			}
			t.Logf("goroutines=%d success=%d OptimisticLock=%d commit hooks=%d head=%d journal=%d", workers, successes, conflicts, commitCalls.Load(), head.seqNr, len(journal))
		})
	}
}

func TestPersistEventHoldsStoreLockUntilPublication(t *testing.T) {
	for _, seq := range []eventstore.SeqNr{1, 2} {
		t.Run(fmt.Sprintf("seq=%d", seq), func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			if seq == 2 {
				require.NoError(t, store.persistEvent(newTestEvent(t, "locked", 1, []byte("first"))))
			}
			event := newTestEvent(t, "locked", seq, []byte("candidate"))
			beforeHead, beforeJournal := store.observe(event.AggregateID())
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			store.hooks = testhook.New()
			var beforePublication []storedEvent
			store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
				if pt.AggregateID == event.AggregateID() {
					if current := store.records[pt.AggregateID]; current != nil {
						for _, saved := range current.journal {
							beforePublication = append(beforePublication, saved.clone())
						}
					}
					close(entered)
					<-release
				}
				return nil
			})
			writeResult := make(chan error, 1)
			go func() { writeResult <- store.persistEvent(event) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("write did not reach the pre-publication hook")
			}
			assert.Equal(t, beforeJournal, beforePublication, "prepared event must not yet be visible in actual records")
			if store.mu.TryLock() {
				store.mu.Unlock()
				t.Error("store write lock was released before publication")
			}
			if store.mu.TryRLock() {
				store.mu.RUnlock()
				t.Error("store read lock was available before publication")
			}
			type observation struct {
				head    *storedEvent
				journal []storedEvent
			}
			readResult := make(chan observation, 1)
			go func() {
				head, journal := store.observe(event.AggregateID())
				readResult <- observation{head, journal}
			}()
			other := newTestEvent(t, "locked-other", 1, []byte("other"))
			otherResult := make(chan error, 1)
			go func() { otherResult <- store.persistEvent(other) }()
			independentStore, err := NewStore()
			require.NoError(t, err)
			require.NoError(t, independentStore.persistEvent(newTestEvent(t, "locked", 1, []byte("independent"))))

			unblock()

			require.NoError(t, <-writeResult)
			require.NoError(t, <-otherResult)
			observed := <-readResult
			require.NotNil(t, observed.head)
			assert.Equal(t, seq, observed.head.seqNr)
			require.Len(t, observed.journal, int(seq))
			assert.Equal(t, *observed.head, observed.journal[len(observed.journal)-1])
			assert.Equal(t, []byte("candidate"), observed.head.payload)
			if beforeHead != nil {
				assert.Equal(t, *beforeHead, observed.journal[0])
			}
			otherHead, otherJournal := store.observe(other.AggregateID())
			require.NotNil(t, otherHead)
			assert.Equal(t, []byte("other"), otherHead.payload)
			assert.Len(t, otherJournal, 1)
			t.Logf("store lock held at commit hook; observer saw head=%d journal=%d after publication; other aggregate and independent Store committed", observed.head.seqNr, len(observed.journal))
		})
	}
}
