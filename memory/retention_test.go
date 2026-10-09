package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retentionLogHandler struct {
	mu       sync.Mutex
	records  []slog.Record
	contexts []context.Context
	handle   func(context.Context, slog.Record) error
}

func (*retentionLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *retentionLogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.contexts = append(h.contexts, ctx)
	h.mu.Unlock()
	if h.handle != nil {
		return h.handle(ctx, record)
	}
	return nil
}
func (h *retentionLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *retentionLogHandler) WithGroup(string) slog.Handler      { return h }

func captureRetentionLog(t *testing.T) *retentionLogHandler {
	t.Helper()
	previous := slog.Default()
	handler := &retentionLogHandler{}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func newRetentionStore(t *testing.T, count int, opts ...eventstore.Option) *Store {
	t.Helper()
	if count > 0 {
		retention, err := eventstore.KeepLatest(count)
		require.NoError(t, err)
		opts = append(opts, eventstore.WithRetentionCount(retention))
	}
	store, err := NewStore(opts...)
	require.NoError(t, err)
	return store
}

func newBytesEntry(t *testing.T, store *Store) eventstore.EventStore[[]byte, []byte] {
	t.Helper()
	entry, err := New(store, eventstore.NewJSONSerializer[[]byte](), eventstore.NewJSONSerializer[[]byte]())
	require.NoError(t, err)
	return entry
}

func historyNumbers(t *testing.T, store *Store, aid string) []int64 {
	t.Helper()
	_, state := store.observeState(aid)
	require.NotNil(t, state)
	numbers := make([]int64, len(state.history))
	for i, snapshot := range state.history {
		numbers[i] = int64(snapshot.seqNr)
	}
	return numbers
}

func requireRetentionUnlocked(t *testing.T, store *Store) {
	t.Helper()
	if store.mu.TryLock() {
		store.mu.Unlock()
	} else {
		t.Error("retention notification still holds the Store lock")
	}
}

func TestRetentionKeepsNewestHistoryAndAllEvents(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		opts    []eventstore.Option
		history []int64
	}{
		{"omitted", 0, nil, []int64{}},
		{"explicit none", 0, []eventstore.Option{eventstore.WithRetentionCount(eventstore.NoRetention())}, []int64{}},
		{"one", 1, nil, []int64{6}},
		{"multiple", 3, nil, []int64{4, 5, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRetentionStore(t, tc.count, tc.opts...)
			first, second := newBytesEntry(t, store), newBytesEntry(t, store)
			id, err := eventstore.NewAggregateID("Account", "history")
			require.NoError(t, err)
			var deleted []int64
			store.hooks = testhook.New()
			store.hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error {
				deleted = append(deleted, pt.SeqNrs...)
				return nil
			})
			for seq := eventstore.SeqNr(1); seq <= 6; seq++ {
				event := newTestEvent(t, "history", seq, []byte(fmt.Sprint(seq)))
				if seq == 3 {
					require.NoError(t, second.PersistEvent(t.Context(), event))
				} else {
					require.NoError(t, first.PersistEventAndSnapshot(t.Context(), event, newTestSnapshot(t, seq, []byte(fmt.Sprint(seq)))))
				}
			}
			assert.Equal(t, tc.history, historyNumbers(t, store, "Account-history"))
			if tc.count == 1 {
				assert.Equal(t, []int64{1, 2, 4, 5}, deleted)
			} else if tc.count == 3 {
				assert.Equal(t, []int64{1, 2}, deleted)
			} else {
				assert.Empty(t, deleted)
			}
			latest, err := second.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			expectedSnapshot := newTestSnapshot(t, 6, []byte("6"))
			requireLatestSnapshot(t, latest, 6, &expectedSnapshot)
			events, err := second.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
			require.NoError(t, err)
			require.Len(t, events, 6)
			for i, event := range events {
				assert.Equal(t, eventstore.SeqNr(i+1), event.SeqNr())
				assert.Equal(t, []byte(fmt.Sprint(i+1)), event.Payload())
			}
			// A prefix-related aid in the same Store does not enter this retention selection.
			require.NoError(t, first.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "history-extra", 1, []byte("other")), newTestSnapshot(t, 1, []byte("other-state"))))
			assert.Equal(t, tc.history, historyNumbers(t, store, "Account-history"))
			otherHistory := []int64{}
			if tc.count > 0 {
				otherHistory = []int64{1}
			}
			assert.Equal(t, otherHistory, historyNumbers(t, store, "Account-history-extra"))
			t.Logf("real history=%v removed=%v head=6 current=6 journal=6 across two entries sharing one Store", tc.history, deleted)
		})
	}
}

func TestRetentionFailureKeepsCommitAndRecoversOnSameStore(t *testing.T) {
	for _, phase := range []testhook.Phase{testhook.PhaseRetentionQuery, testhook.PhaseRetentionDelete} {
		for _, nextPair := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/next pair=%v", phase, nextPair), func(t *testing.T) {
				log := captureRetentionLog(t)
				type contextKey struct{}
				ctx := context.WithValue(t.Context(), contextKey{}, "public write context")
				cause := &testhook.InjectedError{Phase: phase, Message: "retention failed"}
				var notifications []error
				var store *Store
				store = newRetentionStore(t, 1, eventstore.WithRetentionFailureHandler(func(received context.Context, err error) {
					requireRetentionUnlocked(t, store)
					assert.Equal(t, ctx, received)
					notifications = append(notifications, err)
				}))
				log.handle = func(context.Context, slog.Record) error { requireRetentionUnlocked(t, store); return nil }
				entry := newBytesEntry(t, store)
				id, err := eventstore.NewAggregateID("Account", "recover")
				require.NoError(t, err)
				first, second, third := newTestEvent(t, "recover", 1, []byte("one")), newTestEvent(t, "recover", 2, []byte("two")), newTestEvent(t, "recover", 3, []byte("three"))
				require.NoError(t, entry.PersistEventAndSnapshot(ctx, first, newTestSnapshot(t, 1, []byte("state-one"))))
				store.hooks = testhook.New()
				failed := false
				store.hooks.OnFail(phase, func(testhook.Point) error {
					if !failed {
						failed = true
						return cause
					}
					return nil
				})
				secondSnapshot := newTestSnapshot(t, 2, []byte("state-two"))

				require.NoError(t, entry.PersistEventAndSnapshot(ctx, second, secondSnapshot))
				assert.Equal(t, []int64{1, 2}, historyNumbers(t, store, first.AggregateID()))
				latest, err := entry.GetLatestSnapshotByID(ctx, id)
				require.NoError(t, err)
				requireLatestSnapshot(t, latest, 2, &secondSnapshot)
				journal, err := entry.GetEventsByIDSinceSeqNr(ctx, id, 0)
				require.NoError(t, err)
				requireReadEvents(t, []eventstore.EventEnvelope[[]byte]{first, second}, journal)
				require.Len(t, notifications, 1)
				requireKind(t, notifications[0], eventstore.KindStorage)
				assert.ErrorIs(t, notifications[0], cause)
				assert.Contains(t, errors.Unwrap(notifications[0]).Error(), first.AggregateID())
				require.Len(t, log.records, 1)
				assert.Equal(t, []context.Context{ctx}, log.contexts)
				attributes := map[string]any{}
				log.records[0].Attrs(func(attr slog.Attr) bool { attributes[attr.Key] = attr.Value.Any(); return true })
				assert.Equal(t, first.AggregateID(), attributes["aid"])
				assert.ErrorIs(t, attributes["error"].(error), cause)
				var removed []int64
				store.hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error { removed = append(removed, pt.SeqNrs...); return nil })

				if nextPair {
					require.NoError(t, entry.PersistEventAndSnapshot(ctx, third, newTestSnapshot(t, 3, []byte("state-three"))))
					assert.Equal(t, []int64{3}, historyNumbers(t, store, first.AggregateID()))
					assert.Equal(t, []int64{1, 2}, removed)
				} else {
					require.NoError(t, entry.PersistEvent(ctx, third))
					assert.Equal(t, []int64{2}, historyNumbers(t, store, first.AggregateID()))
					assert.Equal(t, []int64{1}, removed)
					current, err := entry.GetLatestSnapshotByID(ctx, id)
					require.NoError(t, err)
					requireLatestSnapshot(t, current, 3, &secondSnapshot)
				}
				assert.Len(t, notifications, 1)
				assert.Len(t, log.records, 1)
				t.Logf("%s failed after real pair 2 committed; one log and callback preserved cause/context/aid; same Store next pair=%v removed old history %v", phase, nextPair, removed)
			})
		}
	}
}

func TestRetentionHistoryPagesMergeAndFailureRecovery(t *testing.T) {
	for _, pages := range [][][]int64{{{3, 2}, {2, 1}}, {{4, 3}, {3, 2}, {1}}} {
		t.Run(fmt.Sprint(pages), func(t *testing.T) {
			store := newRetentionStore(t, 3)
			entry := newBytesEntry(t, store)
			for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
				require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "pages", seq, []byte("event")), newTestSnapshot(t, seq, []byte("state"))))
			}
			store.hooks = testhook.New()
			var justWritten []int64
			store.hooks.OnHistoryPages(func(aid string, seq int64) ([][]int64, bool, error) {
				assert.Equal(t, "Account-pages", aid)
				justWritten = append(justWritten, seq)
				return pages, true, nil
			})
			var deleted []int64
			store.hooks.OnFail(testhook.PhaseRetentionDelete, func(pt testhook.Point) error { deleted = append(deleted, pt.SeqNrs...); return nil })
			require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "pages", 4, []byte("four")), newTestSnapshot(t, 4, []byte("four-state"))))
			assert.Equal(t, []int64{1}, deleted)
			assert.Equal(t, []int64{2, 3, 4}, historyNumbers(t, store, "Account-pages"))
			store.hooks.OnHistoryPages(func(_ string, seq int64) ([][]int64, bool, error) {
				justWritten = append(justWritten, seq)
				return [][]int64{{4, 3}, {2}}, true, nil
			})
			require.NoError(t, entry.PersistEvent(t.Context(), newTestEvent(t, "pages", 5, []byte("five"))))
			assert.Equal(t, []int64{4, 0}, justWritten)
			assert.Equal(t, []int64{2, 3, 4}, historyNumbers(t, store, "Account-pages"))
			assert.Equal(t, []int64{1}, deleted)
		})
	}
	t.Run("page read failure", func(t *testing.T) {
		log := captureRetentionLog(t)
		store := newRetentionStore(t, 1)
		entry := newBytesEntry(t, store)
		require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "page-failure", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
		store.hooks = testhook.New()
		cause := errors.New("history pages failed")
		calls := 0
		store.hooks.OnHistoryPages(func(string, int64) ([][]int64, bool, error) {
			calls++
			if calls == 1 {
				return nil, true, cause
			}
			return nil, false, nil
		})
		require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "page-failure", 2, []byte("two")), newTestSnapshot(t, 2, []byte("two-state"))))
		assert.Equal(t, []int64{1, 2}, historyNumbers(t, store, "Account-page-failure"))
		require.Len(t, log.records, 1)
		require.NoError(t, entry.PersistEvent(t.Context(), newTestEvent(t, "page-failure", 3, []byte("three"))))
		assert.Equal(t, []int64{2}, historyNumbers(t, store, "Account-page-failure"))
		assert.Equal(t, 2, calls)
	})
}

func TestRetentionNotificationErrorsAndPanicsKeepWriteSuccessful(t *testing.T) {
	for _, behavior := range []string{"log error", "log panic", "callback panic", "both panic", "no callback"} {
		t.Run(behavior, func(t *testing.T) {
			log := captureRetentionLog(t)
			callbackCalls := 0
			var opts []eventstore.Option
			if behavior != "no callback" {
				opts = append(opts, eventstore.WithRetentionFailureHandler(func(context.Context, error) {
					callbackCalls++
					if behavior == "callback panic" || behavior == "both panic" {
						panic("callback failed")
					}
				}))
			}
			store := newRetentionStore(t, 1, opts...)
			entry := newBytesEntry(t, store)
			require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "notify", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
			log.handle = func(context.Context, slog.Record) error {
				requireRetentionUnlocked(t, store)
				if behavior == "log panic" || behavior == "both panic" {
					panic("log failed")
				}
				return errors.New("log handler failed")
			}
			store.hooks = testhook.New()
			store.hooks.OnFail(testhook.PhaseRetentionDelete, func(testhook.Point) error { return errors.New("delete failed") })

			require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "notify", 2, []byte("two")), newTestSnapshot(t, 2, []byte("two-state"))))
			assert.Equal(t, []int64{1, 2}, historyNumbers(t, store, "Account-notify"))
			id, err := eventstore.NewAggregateID("Account", "notify")
			require.NoError(t, err)
			read, err := entry.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			assert.Equal(t, eventstore.SeqNr(2), read.HeadSeqNr)
			assert.Equal(t, []byte("two-state"), read.Snapshot.Aggregate())
			assert.Len(t, log.records, 1)
			if behavior == "no callback" {
				assert.Zero(t, callbackCalls)
			} else {
				assert.Equal(t, 1, callbackCalls)
			}
		})
	}
}

func TestRetentionCallbackReentersRealStoreAndRecovers(t *testing.T) {
	log := captureRetentionLog(t)
	id, err := eventstore.NewAggregateID("Account", "reentry")
	require.NoError(t, err)
	var entry eventstore.EventStore[[]byte, []byte]
	var callbackRead *eventstore.SnapshotRead[[]byte]
	var callbackEvents []eventstore.EventEnvelope[[]byte]
	var readErr, eventsErr, appendErr error
	callbackCalls := 0
	third := newTestEvent(t, "reentry", 3, []byte("three"))
	store := newRetentionStore(t, 1, eventstore.WithRetentionFailureHandler(func(ctx context.Context, _ error) {
		callbackCalls++
		callbackRead, readErr = entry.GetLatestSnapshotByID(ctx, id)
		callbackEvents, eventsErr = entry.GetEventsByIDSinceSeqNr(ctx, id, 0)
		appendErr = entry.PersistEvent(ctx, third)
	}))
	entry = newBytesEntry(t, store)
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "reentry", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
	store.hooks = testhook.New()
	failed := false
	store.hooks.OnFail(testhook.PhaseRetentionDelete, func(testhook.Point) error {
		if !failed {
			failed = true
			return errors.New("delete failed once")
		}
		return nil
	})
	second := newTestEvent(t, "reentry", 2, []byte("two"))
	secondSnapshot := newTestSnapshot(t, 2, []byte("two-state"))
	finished := make(chan struct{})
	var writeErr error
	go func() { writeErr = entry.PersistEventAndSnapshot(t.Context(), second, secondSnapshot); close(finished) }()
	waitForReadSignal(t, finished, "real retention callback read and append on the same Store")
	require.NoError(t, writeErr)
	require.NoError(t, readErr)
	require.NoError(t, eventsErr)
	require.NoError(t, appendErr)
	requireLatestSnapshot(t, callbackRead, 2, &secondSnapshot)
	require.Len(t, callbackEvents, 2)
	assert.Equal(t, []int64{2}, historyNumbers(t, store, "Account-reentry"))
	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	requireLatestSnapshot(t, latest, 3, &secondSnapshot)
	assert.Equal(t, 1, callbackCalls)
	assert.Len(t, log.records, 1)
	t.Log("real callback read committed pair 2 then appended event-only 3 to the same Store; old history removed and head advanced without deadlock")
}

func TestRetentionUsesStoreOwnedSettingsAcrossEntries(t *testing.T) {
	log := captureRetentionLog(t)
	count := 2
	var applied *storeoptions.Options
	optionCalls, originalCalls, replacementCalls := 0, 0, 0
	store, err := NewStore(func(o *storeoptions.Options) error {
		optionCalls++
		applied = o
		o.RetentionCount = &count
		o.RetentionFailureHandler = func(context.Context, error) { originalCalls++ }
		return nil
	})
	require.NoError(t, err)
	first := newBytesEntry(t, store)
	require.NoError(t, first.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "owned-settings", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
	count = 0
	applied.RetentionCount = nil
	applied.RetentionMode = storeoptions.RetentionTTL
	applied.RetentionFailureHandler = func(context.Context, error) { replacementCalls++ }
	second := newBytesEntry(t, store)
	for seq := eventstore.SeqNr(2); seq <= 3; seq++ {
		require.NoError(t, second.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "owned-settings", seq, []byte("event")), newTestSnapshot(t, seq, []byte("state"))))
	}
	assert.Equal(t, []int64{2, 3}, historyNumbers(t, store, "Account-owned-settings"))
	store.hooks = testhook.New()
	failed := false
	store.hooks.OnFail(testhook.PhaseRetentionDelete, func(testhook.Point) error {
		if !failed {
			failed = true
			return errors.New("delete failed once")
		}
		return nil
	})
	require.NoError(t, first.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "owned-settings", 4, []byte("four")), newTestSnapshot(t, 4, []byte("four-state"))))
	assert.Equal(t, []int64{2, 3, 4}, historyNumbers(t, store, "Account-owned-settings"))
	require.NoError(t, second.PersistEvent(t.Context(), newTestEvent(t, "owned-settings", 5, []byte("five"))))
	assert.Equal(t, []int64{3, 4}, historyNumbers(t, store, "Account-owned-settings"))
	assert.Equal(t, 1, optionCalls)
	assert.Equal(t, 1, originalCalls)
	assert.Zero(t, replacementCalls)
	assert.Len(t, log.records, 1)
}

func TestRetentionDoesNotRunBeforeSuccessfulCommit(t *testing.T) {
	for _, pair := range []bool{false, true} {
		t.Run(fmt.Sprintf("pair=%v", pair), func(t *testing.T) {
			log := captureRetentionLog(t)
			callbackCalls := 0
			store := newRetentionStore(t, 1, eventstore.WithRetentionFailureHandler(func(context.Context, error) { callbackCalls++ }))
			entry := newBytesEntry(t, store)
			require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "before-commit", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
			headBefore, recordBefore := store.observeState("Account-before-commit")
			store.hooks = testhook.New()
			cause := errors.New("commit failed")
			store.hooks.OnFail(testhook.PhaseCommit, func(testhook.Point) error { return cause })
			queryCalls, deleteCalls := 0, 0
			store.hooks.OnFail(testhook.PhaseRetentionQuery, func(testhook.Point) error { queryCalls++; return nil })
			store.hooks.OnFail(testhook.PhaseRetentionDelete, func(testhook.Point) error { deleteCalls++; return nil })
			second := newTestEvent(t, "before-commit", 2, []byte("two"))
			var writeErr error
			if pair {
				writeErr = entry.PersistEventAndSnapshot(t.Context(), second, newTestSnapshot(t, 2, []byte("two-state")))
			} else {
				writeErr = entry.PersistEvent(t.Context(), second)
			}
			requireKind(t, writeErr, eventstore.KindStorage)
			assert.ErrorIs(t, writeErr, cause)
			headAfter, recordAfter := store.observeState("Account-before-commit")
			assert.Equal(t, headBefore, headAfter)
			assert.Equal(t, recordBefore, recordAfter)
			assert.Zero(t, queryCalls)
			assert.Zero(t, deleteCalls)
			assert.Zero(t, callbackCalls)
			assert.Empty(t, log.records)
		})
	}
}

type signalBytesSerializer struct {
	base   eventstore.Serializer[[]byte]
	signal func()
}

func (s *signalBytesSerializer) Serialize(value []byte) ([]byte, error) {
	s.signal()
	return s.base.Serialize(value)
}
func (s *signalBytesSerializer) Deserialize(data []byte) ([]byte, error) {
	return s.base.Deserialize(data)
}

func TestRetentionHoldsWholeStoreLockAfterPublication(t *testing.T) {
	for _, phase := range []testhook.Phase{testhook.PhaseRetentionQuery, testhook.PhaseRetentionDelete} {
		t.Run(string(phase), func(t *testing.T) {
			store := newRetentionStore(t, 1)
			writer, reader := newBytesEntry(t, store), newBytesEntry(t, store)
			require.NoError(t, writer.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "locked-retention", 1, []byte("one")), newTestSnapshot(t, 1, []byte("one-state"))))
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			store.hooks = testhook.New()
			var committedHead, committedCurrent eventstore.SeqNr
			var committedHistory []int64
			store.hooks.OnFail(phase, func(pt testhook.Point) error {
				if pt.AggregateID == "Account-locked-retention" {
					current := store.records[pt.AggregateID]
					committedHead = current.journal[len(current.journal)-1].seqNr
					committedCurrent = current.current.seqNr
					for _, snapshot := range current.history {
						committedHistory = append(committedHistory, int64(snapshot.seqNr))
					}
					close(entered)
					<-release
				}
				return nil
			})
			writeDone, readDone, eventsDone, otherDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var writeErr, readErr, eventsErr, otherErr error
			var read *eventstore.SnapshotRead[[]byte]
			var journal []eventstore.EventEnvelope[[]byte]
			second, snapshot := newTestEvent(t, "locked-retention", 2, []byte("two")), newTestSnapshot(t, 2, []byte("two-state"))
			t.Cleanup(func() { unblock(); waitForReadSignal(t, writeDone, "retention writer exit") })
			go func() { writeErr = writer.PersistEventAndSnapshot(t.Context(), second, snapshot); close(writeDone) }()
			waitForReadSignal(t, entered, "retention after real publication")
			assert.Equal(t, eventstore.SeqNr(2), committedHead)
			assert.Equal(t, eventstore.SeqNr(2), committedCurrent)
			assert.Equal(t, []int64{1, 2}, committedHistory)
			if store.mu.TryLock() {
				store.mu.Unlock()
				t.Error("write lock released during retention")
			}
			if store.mu.TryRLock() {
				store.mu.RUnlock()
				t.Error("read lock available during retention")
			}
			id := &observedReadID{typeName: "Account", value: "locked-retention", captured: make(chan struct{})}
			t.Cleanup(func() { unblock(); waitForReadSignal(t, readDone, "retention reader exit") })
			go func() { read, readErr = reader.GetLatestSnapshotByID(t.Context(), id); close(readDone) }()
			waitForReadSignal(t, id.captured, "real public read entry during retention")
			eventID := &observedReadID{typeName: "Account", value: "locked-retention", captured: make(chan struct{})}
			t.Cleanup(func() { unblock(); waitForReadSignal(t, eventsDone, "retention event reader exit") })
			go func() {
				journal, eventsErr = reader.GetEventsByIDSinceSeqNr(t.Context(), eventID, 0)
				close(eventsDone)
			}()
			waitForReadSignal(t, eventID.captured, "real public event read entry during retention")
			otherEntered := make(chan struct{})
			otherSerializer := &signalBytesSerializer{base: eventstore.NewJSONSerializer[[]byte](), signal: sync.OnceFunc(func() { close(otherEntered) })}
			other, err := New(store, otherSerializer, eventstore.NewJSONSerializer[[]byte]())
			require.NoError(t, err)
			otherEvent := newTestEvent(t, "locked-retention-other", 1, []byte("other"))
			t.Cleanup(func() { unblock(); waitForReadSignal(t, otherDone, "other aggregate writer exit") })
			go func() { otherErr = other.PersistEvent(t.Context(), otherEvent); close(otherDone) }()
			waitForReadSignal(t, otherEntered, "other aggregate's public write entry")
			for _, done := range []<-chan struct{}{readDone, eventsDone, otherDone} {
				select {
				case <-done:
					t.Error("same-Store operation completed during retention")
				default:
				}
			}
			independent := newBytesEntry(t, newRetentionStore(t, 1))
			require.NoError(t, independent.PersistEventAndSnapshot(t.Context(), newTestEvent(t, "locked-retention", 1, []byte("isolated")), newTestSnapshot(t, 1, []byte("isolated-state"))))
			unblock()
			waitForReadSignal(t, writeDone, "retention completion")
			waitForReadSignal(t, readDone, "read after retention")
			waitForReadSignal(t, eventsDone, "event read after retention")
			waitForReadSignal(t, otherDone, "other write after retention")
			require.NoError(t, writeErr)
			require.NoError(t, readErr)
			require.NoError(t, eventsErr)
			require.NoError(t, otherErr)
			requireLatestSnapshot(t, read, 2, &snapshot)
			require.Len(t, journal, 2)
			assert.Equal(t, eventstore.SeqNr(1), journal[0].SeqNr())
			assert.Equal(t, eventstore.SeqNr(2), journal[1].SeqNr())
			assert.Equal(t, []int64{2}, historyNumbers(t, store, "Account-locked-retention"))
			t.Log("real pair 2 was committed while retention held the whole Store lock; both shared entry reads and other-aid write waited; separate Store progressed")
		})
	}
}
