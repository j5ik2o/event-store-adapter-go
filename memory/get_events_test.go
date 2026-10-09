package memory

import (
	"bytes"
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

type readTestID struct{ typeName, value string }

func (id *readTestID) TypeName() string { return id.typeName }
func (id *readTestID) Value() string    { return id.value }
func (*readTestID) String() string      { return "caller-defined-display" }
func (*readTestID) AsString() string    { return "caller-defined-key" }

// observedReadID reports when the caller's value has been captured and permits
// synchronized mutation while the read operation waits for the store lock.
type observedReadID struct {
	mu                    sync.Mutex
	typeName, value       string
	typeCalls, valueCalls int
	captured              chan struct{}
	notify                sync.Once
}

func (id *observedReadID) TypeName() string {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.typeCalls++
	return id.typeName
}

func (id *observedReadID) Value() string {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.valueCalls++
	value := id.value
	id.notify.Do(func() { close(id.captured) })
	return value
}

type eventReadResult struct {
	events []eventstore.EventEnvelope[[]byte]
	err    error
}

func newReadTestEvents(t *testing.T, id eventstore.AggregateID) []eventstore.EventEnvelope[[]byte] {
	t.Helper()
	var events []eventstore.EventEnvelope[[]byte]
	for i, input := range []struct {
		at       time.Time
		manifest string
		payload  []byte
	}{
		{time.Unix(-1, 123456789), " event/v1\x00日本語 ", []byte{255, 0, 13, 10}},
		{time.Unix(0, 987654321), "", []byte(`{"value":"two"}`)},
		{time.Unix(1720000000, 765432109), " unparsed/v3 ", []byte("third\x00payload")},
	} {
		event, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(i+1), input.at, input.payload,
			eventstore.WithManifest(input.manifest))
		require.NoError(t, err)
		events = append(events, event)
	}
	return events
}

func requireReadEvents(t *testing.T, expected, actual []eventstore.EventEnvelope[[]byte]) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i, event := range expected {
		assert.Equal(t, event.AggregateID(), actual[i].AggregateID(), "event %d ID", i)
		assert.Equal(t, event.SeqNr(), actual[i].SeqNr(), "event %d sequence", i)
		assert.Equal(t, event.OccurredAt(), actual[i].OccurredAt(), "event %d time", i)
		assert.Equal(t, event.Manifest(), actual[i].Manifest(), "event %d manifest", i)
		assert.Equal(t, event.Payload(), actual[i].Payload(), "event %d bytes", i)
	}
}

func waitForReadSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestGetEventsByIDSinceSeqNr(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "read")
	require.NoError(t, err)
	events := newReadTestEvents(t, id)
	for _, event := range events {
		require.NoError(t, store.persistEvent(event))
	}
	missing, err := eventstore.NewAggregateID("Account", "missing")
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		id       eventstore.AggregateID
		start    eventstore.SeqNr
		expected []eventstore.EventEnvelope[[]byte]
	}{
		{"start zero", id, 0, events},
		{"inclusive start", id, 2, events[1:]},
		{"last event", id, 3, events[2:]},
		{"past last", id, 4, nil},
		{"maximum start", id, eventstore.MaxSeqNr, nil},
		{"missing ID", missing, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := store.getEventsByIDSinceSeqNr(tc.id, tc.start)

			require.NoError(t, err)
			requireReadEvents(t, tc.expected, actual)
			t.Logf("start=%d events=%d; all metadata and bytes match committed inputs", tc.start, len(actual))
		})
	}
}

func TestGetEventsUsesExactAggregateID(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	ids := []*readTestID{
		{"Account", "key"}, {"Account", "key-extra"}, {"account", "key"},
		{"Account", " key "}, {"Account", "é"}, {"Account", "e\u0301"},
		{"", "value-with-hyphens"}, {"Account", ""}, {"", ""},
		{"X", strings.Repeat("界", 340) + "ab"}, // Exactly 1024 UTF-8 bytes including X-.
	}
	var expected []eventstore.EventEnvelope[[]byte]
	for i, id := range ids {
		event, err := eventstore.NewEventEnvelope(id, 1, time.Unix(int64(i), 123456789), []byte(fmt.Sprint(i)))
		require.NoError(t, err)
		require.Equal(t, id.typeName+"-"+id.value, event.AggregateID())
		require.NoError(t, store.persistEvent(event))
		expected = append(expected, event)
	}
	for i, id := range ids {
		actual, err := store.getEventsByIDSinceSeqNr(id, 0)

		require.NoError(t, err)
		requireReadEvents(t, expected[i:i+1], actual)
	}
	actual, err := store.getEventsByIDSinceSeqNr(&readTestID{"Account", "ke"}, 0)
	require.NoError(t, err)
	require.Empty(t, actual, "a prefix must not select either key or key-extra")
}

func TestGetEventsRejectsInvalidInputsBeforeLock(t *testing.T) {
	var nilID *readTestID
	for _, tc := range []struct {
		name  string
		id    eventstore.AggregateID
		start eventstore.SeqNr
		rule  string
	}{
		{"nil ID", nil, 2, "T-2"},
		{"typed nil ID", nilID, 2, "T-2"},
		{"hyphen in type", &readTestID{"Bad-Type", "value"}, 2, "T-11"},
		{"ID too long", &readTestID{"X", strings.Repeat("a", 1023)}, 2, "T-12"},
		{"UTF-8 ID too long", &readTestID{"X", strings.Repeat("界", 341)}, 2, "T-12"},
		{"negative start", &readTestID{"Account", "valid"}, -1, "T-9"},
		{"start above maximum", &readTestID{"Account", "valid"}, eventstore.MaxSeqNr + 1, "T-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			store.hooks = testhook.New()
			var reads atomic.Int32
			store.hooks.OnFail(testhook.PhaseReadEvents, func(testhook.Point) error { reads.Add(1); return nil })
			store.mu.Lock()
			result := make(chan eventReadResult, 1)
			done := make(chan struct{})
			t.Cleanup(func() {
				store.mu.Unlock()
				waitForReadSignal(t, done, "invalid-input reader exit")
			})
			go func() {
				events, err := store.getEventsByIDSinceSeqNr(tc.id, tc.start)
				result <- eventReadResult{events, err}
				close(done)
			}()

			waitForReadSignal(t, done, "input rejection while the store is write-locked")
			actual := <-result

			require.Empty(t, actual.events)
			requireKind(t, actual.err, eventstore.KindContractViolation)
			var violation *eventstore.ContractViolationError
			require.ErrorAs(t, actual.err, &violation)
			assert.Equal(t, tc.rule, violation.Rule)
			require.NotNil(t, violation.SeqNr)
			assert.Equal(t, tc.start, *violation.SeqNr)
			assert.Zero(t, reads.Load())
		})
	}
}

func TestGetEventsFixesInputsBeforeWaiting(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id := &observedReadID{typeName: "Account", value: "fixed-with-hyphens", captured: make(chan struct{})}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	expected := newReadTestEvents(t, fixedID)
	for _, event := range expected {
		require.NoError(t, store.persistEvent(event))
	}
	require.NoError(t, store.persistEvent(newTestEvent(t, "changed", 1, []byte("other"))))
	store.mu.Lock()
	unlock := sync.OnceFunc(store.mu.Unlock)
	result := make(chan eventReadResult, 1)
	done := make(chan struct{})
	t.Cleanup(func() { unlock(); waitForReadSignal(t, done, "fixed-input reader exit") })
	start := eventstore.SeqNr(2)
	go func(start eventstore.SeqNr) {
		events, err := store.getEventsByIDSinceSeqNr(id, start)
		result <- eventReadResult{events, err}
		close(done)
	}(start)
	waitForReadSignal(t, id.captured, "caller ID capture")
	id.mu.Lock()
	id.typeName, id.value = "Account", "changed"
	id.mu.Unlock()
	start = eventstore.MaxSeqNr
	select {
	case <-done:
		t.Error("valid read returned while the store was write-locked")
	default:
	}

	unlock()
	waitForReadSignal(t, done, "fixed-input read completion")
	actual := <-result

	require.NoError(t, actual.err)
	requireReadEvents(t, expected[1:], actual.events)
	id.mu.Lock()
	defer id.mu.Unlock()
	assert.Equal(t, 1, id.typeCalls)
	assert.Equal(t, 1, id.valueCalls)
	assert.Equal(t, eventstore.MaxSeqNr, start)
	t.Log("caller ID and start changed during lock wait; original ID and start=2 selected events 2 and 3")
}

func TestGetEventsSharesStoreAndIsolatesSeparateStores(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	firstCaller, secondCaller := store, store
	id, err := eventstore.NewAggregateID("Account", "shared-read")
	require.NoError(t, err)
	expected := newReadTestEvents(t, id)
	require.NoError(t, firstCaller.persistEvent(expected[0]))
	before, err := secondCaller.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected[:1], before)

	require.NoError(t, firstCaller.persistEvent(expected[1]))
	after, err := secondCaller.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected[:2], after)
	requireReadEvents(t, expected[:1], before)
	separate, err := NewStore()
	require.NoError(t, err)
	empty, err := separate.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	require.Empty(t, empty)
	isolated := newTestEvent(t, "shared-read", 1, []byte("isolated"))
	require.NoError(t, separate.persistEvent(isolated))
	other, err := separate.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, []eventstore.EventEnvelope[[]byte]{isolated}, other)
	unchanged, err := secondCaller.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected[:2], unchanged)
}

func TestGetEventsProtectsInputValues(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id := &readTestID{"Account", "input-read"}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	inputs := newReadTestEvents(t, id)
	var expected []eventstore.EventEnvelope[[]byte]
	for _, event := range inputs {
		independent, err := eventstore.NewEventEnvelope(fixedID, event.SeqNr(), event.OccurredAt(), bytes.Clone(event.Payload()),
			eventstore.WithManifest(event.Manifest()))
		require.NoError(t, err)
		expected = append(expected, independent)
		require.NoError(t, store.persistEvent(event))
	}
	before, err := store.getEventsByIDSinceSeqNr(fixedID, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected, before)

	for i := range inputs {
		inputs[i].Payload()[0] = 77
		inputs[i] = newTestEvent(t, "replacement", 9, []byte("replaced"))
	}
	id.typeName, id.value = "Changed", "input"
	after, err := store.getEventsByIDSinceSeqNr(fixedID, 0)

	require.NoError(t, err)
	requireReadEvents(t, expected, before)
	requireReadEvents(t, expected, after)
}

func TestGetEventsProtectsReturnedValues(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "returned-read")
	require.NoError(t, err)
	expected := newReadTestEvents(t, id)
	for _, event := range expected {
		require.NoError(t, store.persistEvent(event))
	}
	first, err := store.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected, first)
	second, err := store.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected, second)

	for i := range first {
		first[i].Payload()[0] = 88
	}
	first[0] = newTestEvent(t, "replacement", 9, []byte("replaced"))
	first = append(first, first[0])
	after, err := store.getEventsByIDSinceSeqNr(id, 0)

	require.NoError(t, err)
	require.Len(t, first, 4)
	requireReadEvents(t, expected, second)
	requireReadEvents(t, expected, after)
}

func TestGetEventsPreservesNilAndEmptyBytes(t *testing.T) {
	for _, payload := range [][]byte{nil, {}} {
		t.Run(fmt.Sprintf("nil=%v", payload == nil), func(t *testing.T) {
			store, err := NewStore()
			require.NoError(t, err)
			id, err := eventstore.NewAggregateID("Account", "empty-read")
			require.NoError(t, err)
			event := newTestEvent(t, "empty-read", 1, payload)
			require.NoError(t, store.persistEvent(event))

			actual, err := store.getEventsByIDSinceSeqNr(id, 0)

			require.NoError(t, err)
			requireReadEvents(t, []eventstore.EventEnvelope[[]byte]{event}, actual)
		})
	}
}

func TestGetEventsReadFailurePreservesRecordsAndReleasesLock(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Account", "failed-read")
	require.NoError(t, err)
	expected := newReadTestEvents(t, id)
	for _, event := range expected {
		require.NoError(t, store.persistEvent(event))
	}
	cause := &testhook.InjectedError{Phase: testhook.PhaseReadEvents, Message: "read failed"}
	store.hooks = testhook.New()
	reads := 0
	store.hooks.OnFail(testhook.PhaseReadEvents, func(pt testhook.Point) error {
		reads++
		assert.Equal(t, "Account-failed-read", pt.AggregateID)
		assert.Equal(t, int64(0), pt.SeqNr)
		if reads == 1 {
			return cause
		}
		return nil
	})

	actual, err := store.getEventsByIDSinceSeqNr(id, 0)

	require.Empty(t, actual)
	requireKind(t, err, eventstore.KindStorage)
	var storage *eventstore.StorageError
	require.ErrorAs(t, err, &storage)
	assert.ErrorIs(t, err, cause)
	assert.ErrorIs(t, storage.Unwrap(), cause)
	require.Equal(t, 1, reads)
	require.True(t, store.mu.TryLock(), "a failed read must release its lock")
	store.mu.Unlock()
	actual, err = store.getEventsByIDSinceSeqNr(id, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected, actual)
	assert.Equal(t, 2, reads)
}

func TestGetEventsWaitsForPublicationAndHoldsStoreReadLock(t *testing.T) {
	store, err := NewStore()
	require.NoError(t, err)
	id := &observedReadID{typeName: "Account", value: "interleaved-read", captured: make(chan struct{})}
	fixedID, err := eventstore.NewAggregateID(id.typeName, id.value)
	require.NoError(t, err)
	expected := newReadTestEvents(t, fixedID)
	for _, event := range expected[:2] {
		require.NoError(t, store.persistEvent(event))
	}
	before, err := store.getEventsByIDSinceSeqNr(fixedID, 0)
	require.NoError(t, err)
	requireReadEvents(t, expected[:2], before)
	commitEntered, releaseCommit := make(chan struct{}), make(chan struct{})
	readEntered, releaseRead := make(chan struct{}), make(chan struct{})
	unblockCommit := sync.OnceFunc(func() { close(releaseCommit) })
	unblockRead := sync.OnceFunc(func() { close(releaseRead) })
	store.hooks = testhook.New()
	store.hooks.OnFail(testhook.PhaseCommit, func(pt testhook.Point) error {
		assert.Equal(t, "Account-interleaved-read", pt.AggregateID)
		assert.Equal(t, int64(3), pt.SeqNr)
		close(commitEntered)
		<-releaseCommit
		return nil
	})
	store.hooks.OnFail(testhook.PhaseReadEvents, func(pt testhook.Point) error {
		assert.Equal(t, "Account-interleaved-read", pt.AggregateID)
		assert.Equal(t, int64(0), pt.SeqNr)
		close(readEntered)
		<-releaseRead
		return nil
	})
	writeResult := make(chan error, 1)
	writeDone := make(chan struct{})
	go func() { writeResult <- store.persistEvent(expected[2]); close(writeDone) }()
	readResult := make(chan eventReadResult, 1)
	readDone := make(chan struct{})
	// Every exit releases both hooks and collects the goroutines it has started.
	t.Cleanup(func() { unblockCommit(); unblockRead(); waitForReadSignal(t, writeDone, "writer exit") })
	waitForReadSignal(t, commitEntered, "real append before publication")
	if store.mu.TryRLock() {
		store.mu.RUnlock()
		t.Error("read lock was available before publication")
	}
	if store.mu.TryLock() {
		store.mu.Unlock()
		t.Error("write lock was available before publication")
	}
	t.Cleanup(func() { unblockCommit(); unblockRead(); waitForReadSignal(t, readDone, "reader exit") })
	go func() {
		events, err := store.getEventsByIDSinceSeqNr(id, 0)
		readResult <- eventReadResult{events, err}
		close(readDone)
	}()
	waitForReadSignal(t, id.captured, "read entry during the stopped append")
	select {
	case <-readEntered:
		t.Error("read-events hook ran before publication")
	default:
	}
	select {
	case <-readDone:
		t.Error("read returned before publication")
	default:
	}

	unblockCommit()
	waitForReadSignal(t, writeDone, "append publication")
	require.NoError(t, <-writeResult)
	waitForReadSignal(t, readEntered, "read-events hook after publication")
	if store.mu.TryLock() {
		store.mu.Unlock()
		t.Error("read-events hook did not hold the store read lock")
	}
	if store.mu.TryRLock() {
		store.mu.RUnlock()
	} else {
		t.Error("a read operation must permit another store read lock")
	}
	unblockRead()
	waitForReadSignal(t, readDone, "read completion")
	actual := <-readResult

	require.NoError(t, actual.err)
	requireReadEvents(t, expected, actual.events)
	requireReadEvents(t, expected[:2], before)
	t.Log("append 3 stopped before publication; reader entered during the stop and obtained committed events 1, 2 and 3 under the store read lock")
}
