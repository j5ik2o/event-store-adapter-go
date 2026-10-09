package memory_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Domain values implement no library interfaces, including cloning interfaces.
type domainEvent struct {
	AggregateID string
	Items       []string
}

type domainState struct {
	Total  int
	Labels map[string]string
}

type domainSerializer[T any] struct {
	serialize   func(T) ([]byte, error)
	deserialize func([]byte) (T, error)
}

func (s *domainSerializer[T]) Serialize(value T) ([]byte, error) {
	if s.serialize != nil {
		return s.serialize(value)
	}
	return json.Marshal(value)
}

func (s *domainSerializer[T]) Deserialize(data []byte) (T, error) {
	if s.deserialize != nil {
		return s.deserialize(data)
	}
	var value T
	err := json.Unmarshal(data, &value)
	// A serializer is allowed to reuse or mutate its input bytes.
	for i := range data {
		data[i] = '!'
	}
	return value, err
}

func newDomainStore(t *testing.T, store *memory.Store) eventstore.EventStore[domainEvent, domainState] {
	t.Helper()
	entry, err := memory.New(store, &domainSerializer[domainEvent]{}, &domainSerializer[domainState]{})
	require.NoError(t, err)
	return entry
}

func domainEnvelope(t *testing.T, id eventstore.AggregateID, seq eventstore.SeqNr, value domainEvent) eventstore.EventEnvelope[domainEvent] {
	t.Helper()
	event, err := eventstore.NewEventEnvelope(id, seq, time.Unix(1720000000, int64(seq)+123456789), value,
		eventstore.WithManifest(" event/日本語\x00 "))
	require.NoError(t, err)
	return event
}

func domainSnapshot(t *testing.T, seq eventstore.SeqNr, value domainState) eventstore.SnapshotEnvelope[domainState] {
	t.Helper()
	snapshot, err := eventstore.NewSnapshotEnvelope(value, seq, eventstore.WithManifest(" snapshot/型\x00 "))
	require.NoError(t, err)
	return snapshot
}

func requireExternalKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok)
	require.Equal(t, expected, kind)
}

func TestNewDomainRoundTripAndMutationIsolation(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	entry := newDomainStore(t, store)
	id, err := eventstore.NewAggregateID("Order", "item-1")
	require.NoError(t, err)
	empty, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.Nil(t, empty)
	noEvents, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Empty(t, noEvents)

	payload := domainEvent{AggregateID: "domain-id", Items: []string{"second", "first"}}
	first := domainEnvelope(t, id, 1, payload)
	require.NoError(t, entry.PersistEvent(t.Context(), first))
	headOnly, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, headOnly)
	assert.Equal(t, eventstore.SeqNr(1), headOnly.HeadSeqNr)
	assert.Nil(t, headOnly.Snapshot)
	state := domainState{Total: 2, Labels: map[string]string{"e\u0301": "é"}}
	second := domainEnvelope(t, id, 2, domainEvent{Items: []string{"third"}})
	snapshot := domainSnapshot(t, 2, state)
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), second, snapshot))
	third := domainEnvelope(t, id, 3, domainEvent{Items: []string{"fourth"}})
	require.NoError(t, entry.PersistEvent(t.Context(), third))
	before, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, before.Snapshot)
	assert.Equal(t, eventstore.SeqNr(3), before.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(2), before.Snapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), before.Snapshot.Manifest())
	assert.Equal(t, state, before.Snapshot.Aggregate())

	payload.Items[0] = "changed input"
	state.Labels["e\u0301"] = "changed input"
	before.Snapshot.Aggregate().Labels["e\u0301"] = "changed result"
	readEvents, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Len(t, readEvents, 3)
	for i, original := range []eventstore.EventEnvelope[domainEvent]{first, second, third} {
		assert.Equal(t, original.AggregateID(), readEvents[i].AggregateID())
		assert.Equal(t, original.SeqNr(), readEvents[i].SeqNr())
		assert.Equal(t, original.OccurredAt(), readEvents[i].OccurredAt())
		assert.Equal(t, original.Manifest(), readEvents[i].Manifest())
	}
	assert.Equal(t, domainEvent{AggregateID: "domain-id", Items: []string{"second", "first"}}, readEvents[0].Payload())
	readEvents[0].Payload().Items[0] = "changed result"
	again, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"second", "first"}, again[0].Payload().Items)
	unchanged, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, domainState{Total: 2, Labels: map[string]string{"e\u0301": "é"}}, unchanged.Snapshot.Aggregate())
	fourth := domainEnvelope(t, id, 4, domainEvent{Items: []string{"fifth"}})
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), fourth, domainSnapshot(t, 4, domainState{Total: 4})))
	latest, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, eventstore.SeqNr(4), latest.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(4), latest.Snapshot.SeqNr())
	assert.Equal(t, domainState{Total: 4}, latest.Snapshot.Aggregate())
	tail, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 2)
	require.NoError(t, err)
	require.Len(t, tail, 3)
	for i, original := range []eventstore.EventEnvelope[domainEvent]{second, third, fourth} {
		assert.Equal(t, original.SeqNr(), tail[i].SeqNr())
		assert.Equal(t, original.Payload(), tail[i].Payload())
	}
	assert.Equal(t, eventstore.SeqNr(3), unchanged.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(2), unchanged.Snapshot.SeqNr())
	t.Log("real four-operation round trip; snapshot 2/head 3 then snapshot 4/head 4; input and restored-value mutations isolated")
}

func TestNewSharesStoreAndIsolatesSeparateStores(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	first, second := newDomainStore(t, store), newDomainStore(t, store)
	id, err := eventstore.NewAggregateID("Order", "shared")
	require.NoError(t, err)
	one := domainEnvelope(t, id, 1, domainEvent{Items: []string{"one"}})
	two := domainEnvelope(t, id, 2, domainEvent{Items: []string{"two"}})
	require.NoError(t, first.PersistEventAndSnapshot(t.Context(), one, domainSnapshot(t, 1, domainState{Total: 1})))
	require.NoError(t, second.PersistEvent(t.Context(), two))
	shared, err := first.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, eventstore.SeqNr(2), shared.HeadSeqNr)
	assert.Equal(t, eventstore.SeqNr(1), shared.Snapshot.SeqNr())
	separate, err := memory.NewStore()
	require.NoError(t, err)
	isolated := newDomainStore(t, separate)
	missing, err := isolated.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	require.Nil(t, missing)
	require.NoError(t, isolated.PersistEventAndSnapshot(t.Context(), one, domainSnapshot(t, 1, domainState{Total: 99})))
	other, err := isolated.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, domainState{Total: 99}, other.Snapshot.Aggregate())
	stillShared, err := second.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, domainState{Total: 1}, stillShared.Snapshot.Aggregate())
	events, err := second.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, one.Payload(), events[0].Payload())
	assert.Equal(t, two.Payload(), events[1].Payload())
}

type scratchSerializer struct{ scratch [128]byte }

func (s *scratchSerializer) Serialize(value string) ([]byte, error) {
	data := s.scratch[:len(value)]
	copy(data, value)
	return data, nil
}

func (*scratchSerializer) Deserialize(data []byte) (string, error) { return string(data), nil }

func TestNewFixesEachSerializerScratchResult(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	serializer := &scratchSerializer{}
	entry, err := memory.New(store, serializer, serializer)
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Order", "scratch")
	require.NoError(t, err)
	for i, value := range []string{"first-event", "paired-event", "later-event-overwrites"} {
		event, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(i+1), time.Unix(0, 123456789), value)
		require.NoError(t, err)
		if i == 1 {
			snapshot, err := eventstore.NewSnapshotEnvelope("paired-state", 2)
			require.NoError(t, err)
			require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), event, snapshot))
		} else {
			require.NoError(t, entry.PersistEvent(t.Context(), event))
		}
	}
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Len(t, events, 3)
	for i, expected := range []string{"first-event", "paired-event", "later-event-overwrites"} {
		assert.Equal(t, expected, events[i].Payload())
	}
	snapshot, err := entry.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, "paired-state", snapshot.Snapshot.Aggregate())
	assert.Equal(t, eventstore.SeqNr(3), snapshot.HeadSeqNr)
}

func TestNewRejectsNilDependenciesAtConstruction(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	var nilEvents *domainSerializer[domainEvent]
	var nilSnapshots *domainSerializer[domainState]
	for _, tc := range []struct {
		name      string
		store     *memory.Store
		events    eventstore.Serializer[domainEvent]
		snapshots eventstore.Serializer[domainState]
	}{
		{"nil store", nil, &domainSerializer[domainEvent]{}, &domainSerializer[domainState]{}},
		{"nil events", store, nil, &domainSerializer[domainState]{}},
		{"typed nil events", store, nilEvents, &domainSerializer[domainState]{}},
		{"nil snapshots", store, &domainSerializer[domainEvent]{}, nil},
		{"typed nil snapshots", store, &domainSerializer[domainEvent]{}, nilSnapshots},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, err := memory.New(tc.store, tc.events, tc.snapshots)
			require.Nil(t, entry)
			requireExternalKind(t, err, eventstore.KindConfiguration)
			var failure *eventstore.ConfigurationError
			require.ErrorAs(t, err, &failure)
			require.NotNil(t, failure.Unwrap())
		})
	}
}

func TestNewSerializationFailureDoesNotCommit(t *testing.T) {
	for _, operation := range []string{"event-only", "pair event", "pair snapshot"} {
		t.Run(operation, func(t *testing.T) {
			store, err := memory.NewStore()
			require.NoError(t, err)
			healthy := newDomainStore(t, store)
			id, err := eventstore.NewAggregateID("Order", "serialization")
			require.NoError(t, err)
			first := domainEnvelope(t, id, 1, domainEvent{Items: []string{"committed"}})
			require.NoError(t, healthy.PersistEventAndSnapshot(t.Context(), first, domainSnapshot(t, 1, domainState{Total: 1})))
			cause := errors.New("serializer failed")
			events, snapshots := &domainSerializer[domainEvent]{}, &domainSerializer[domainState]{}
			if operation == "pair snapshot" {
				snapshots.serialize = func(domainState) ([]byte, error) { return nil, cause }
			} else {
				events.serialize = func(domainEvent) ([]byte, error) { return nil, cause }
			}
			entry, err := memory.New(store, events, snapshots)
			require.NoError(t, err)
			candidate := domainEnvelope(t, id, 2, domainEvent{Items: []string{"candidate"}})
			if operation == "event-only" {
				err = entry.PersistEvent(t.Context(), candidate)
			} else {
				err = entry.PersistEventAndSnapshot(t.Context(), candidate, domainSnapshot(t, 2, domainState{Total: 2}))
			}
			requireExternalKind(t, err, eventstore.KindSerialization)
			require.ErrorIs(t, err, cause)
			read, err := healthy.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			assert.Equal(t, eventstore.SeqNr(1), read.HeadSeqNr)
			assert.Equal(t, domainState{Total: 1}, read.Snapshot.Aggregate())
			journal, err := healthy.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
			require.NoError(t, err)
			require.Len(t, journal, 1)
			assert.Equal(t, first.Payload(), journal[0].Payload())
			require.NoError(t, healthy.PersistEvent(t.Context(), candidate))
		})
	}
}

func TestNewRestorationFailureReturnsNoPartialResult(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	healthy := newDomainStore(t, store)
	id, err := eventstore.NewAggregateID("Order", "restore")
	require.NoError(t, err)
	require.NoError(t, healthy.PersistEvent(t.Context(), domainEnvelope(t, id, 1, domainEvent{Items: []string{"one"}})))
	require.NoError(t, healthy.PersistEventAndSnapshot(t.Context(), domainEnvelope(t, id, 2, domainEvent{Items: []string{"two"}}), domainSnapshot(t, 2, domainState{Total: 2})))
	cause := errors.New("restoration failed")
	decoded := 0
	events := &domainSerializer[domainEvent]{deserialize: func(data []byte) (domainEvent, error) {
		decoded++
		if decoded == 2 {
			return domainEvent{}, cause
		}
		var value domainEvent
		err := json.Unmarshal(data, &value)
		return value, err
	}}
	snapshots := &domainSerializer[domainState]{deserialize: func([]byte) (domainState, error) { return domainState{}, cause }}
	entry, err := memory.New(store, events, snapshots)
	require.NoError(t, err)
	partial, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	requireExternalKind(t, err, eventstore.KindSerialization)
	require.ErrorIs(t, err, cause)
	require.Nil(t, partial)
	assert.Equal(t, 2, decoded)
	read, err := entry.GetLatestSnapshotByID(t.Context(), id)
	requireExternalKind(t, err, eventstore.KindSerialization)
	require.ErrorIs(t, err, cause)
	require.Nil(t, read)
	intact, err := healthy.GetLatestSnapshotByID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, domainState{Total: 2}, intact.Snapshot.Aggregate())
}

func waitExternal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not complete at the controlled boundary")
	}
}

func TestNewRestoresOutsideStoreLock(t *testing.T) {
	for _, operation := range []string{"snapshot", "events"} {
		t.Run(operation, func(t *testing.T) {
			store, err := memory.NewStore()
			require.NoError(t, err)
			writer := newDomainStore(t, store)
			id, err := eventstore.NewAggregateID("Order", "concurrent-restore")
			require.NoError(t, err)
			require.NoError(t, writer.PersistEvent(t.Context(), domainEnvelope(t, id, 1, domainEvent{Items: []string{"one"}})))
			require.NoError(t, writer.PersistEventAndSnapshot(t.Context(), domainEnvelope(t, id, 2, domainEvent{Items: []string{"two"}}), domainSnapshot(t, 2, domainState{Total: 2})))
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			pause := sync.OnceFunc(func() { close(entered); <-release })
			events := &domainSerializer[domainEvent]{deserialize: func(data []byte) (domainEvent, error) {
				pause()
				var value domainEvent
				err := json.Unmarshal(data, &value)
				return value, err
			}}
			snapshots := &domainSerializer[domainState]{deserialize: func(data []byte) (domainState, error) {
				pause()
				var value domainState
				err := json.Unmarshal(data, &value)
				return value, err
			}}
			reader, err := memory.New(store, events, snapshots)
			require.NoError(t, err)
			readDone, writeDone := make(chan struct{}), make(chan struct{})
			var read *eventstore.SnapshotRead[domainState]
			var journal []eventstore.EventEnvelope[domainEvent]
			var readErr, writeErr error
			t.Cleanup(func() { unblock(); waitExternal(t, readDone) })
			go func() {
				defer close(readDone)
				if operation == "snapshot" {
					read, readErr = reader.GetLatestSnapshotByID(t.Context(), id)
				} else {
					journal, readErr = reader.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
				}
			}()
			waitExternal(t, entered)
			third := domainEnvelope(t, id, 3, domainEvent{Items: []string{"three"}})
			thirdSnapshot := domainSnapshot(t, 3, domainState{Total: 3})
			t.Cleanup(func() { unblock(); waitExternal(t, writeDone) })
			go func() { writeErr = writer.PersistEventAndSnapshot(t.Context(), third, thirdSnapshot); close(writeDone) }()
			waitExternal(t, writeDone)
			require.NoError(t, writeErr)
			unblock()
			waitExternal(t, readDone)
			require.NoError(t, readErr)
			if operation == "snapshot" {
				assert.Equal(t, eventstore.SeqNr(2), read.HeadSeqNr)
				assert.Equal(t, eventstore.SeqNr(2), read.Snapshot.SeqNr())
				assert.Equal(t, domainState{Total: 2}, read.Snapshot.Aggregate())
			} else {
				require.Len(t, journal, 2)
				assert.Equal(t, eventstore.SeqNr(1), journal[0].SeqNr())
				assert.Equal(t, eventstore.SeqNr(2), journal[1].SeqNr())
			}
			latest, err := writer.GetLatestSnapshotByID(t.Context(), id)
			require.NoError(t, err)
			assert.Equal(t, eventstore.SeqNr(3), latest.HeadSeqNr)
			t.Log("restoration stopped after byte capture; same-Store pair 3 committed before restoration resumed; captured result remained at head 2")
		})
	}
}

type changingDomainID struct {
	typeName, value       string
	typeCalls, valueCalls int
}

func (id *changingDomainID) TypeName() string {
	id.typeCalls++
	if id.typeCalls > 1 {
		return "invalid-type"
	}
	return id.typeName
}

func (id *changingDomainID) Value() string {
	id.valueCalls++
	if id.valueCalls > 1 {
		return "different"
	}
	return id.value
}

func TestNewReadIDFailuresPreserveRequestedSequence(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	entry := newDomainStore(t, store)
	for _, tc := range []struct {
		name       string
		id         func() eventstore.AggregateID
		rule       string
		valueCalls int
	}{
		{"nil", func() eventstore.AggregateID { return nil }, "T-2", 0},
		{"typed nil", func() eventstore.AggregateID { return (*changingDomainID)(nil) }, "T-2", 0},
		{"hyphen in type", func() eventstore.AggregateID {
			return &changingDomainID{typeName: "Order-Item", value: "1"}
		}, "T-11", 0},
		{"too long", func() eventstore.AggregateID {
			return &changingDomainID{typeName: "Order", value: strings.Repeat("x", 1024)}
		}, "T-12", 1},
	} {
		for _, seqNr := range []eventstore.SeqNr{0, 2, -1, eventstore.MaxSeqNr + 1} {
			t.Run(fmt.Sprintf("%s/since=%d", tc.name, seqNr), func(t *testing.T) {
				id := tc.id()

				result, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, seqNr)

				require.Nil(t, result)
				requireExternalKind(t, err, eventstore.KindContractViolation)
				var violation *eventstore.ContractViolationError
				require.ErrorAs(t, err, &violation)
				assert.Equal(t, tc.rule, violation.Rule)
				assert.Contains(t, err.Error(), tc.rule)
				assert.Nil(t, violation.Unwrap())
				if counted, ok := id.(*changingDomainID); ok && counted != nil {
					assert.Equal(t, 1, counted.typeCalls)
					assert.Equal(t, tc.valueCalls, counted.valueCalls)
				}
				t.Logf("rule=%s requested_seq_nr=%d diagnostic_seq_nr=%v message=%q",
					violation.Rule, seqNr, violation.SeqNr, err.Error())
				require.NotNil(t, violation.SeqNr)
				assert.Equal(t, seqNr, *violation.SeqNr)
				assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", seqNr))
			})
		}
	}
}

func TestNewUsesFixedExactAggregateID(t *testing.T) {
	store, err := memory.NewStore()
	require.NoError(t, err)
	entry := newDomainStore(t, store)
	id := &changingDomainID{typeName: "Order", value: "fixed"}
	first := domainEnvelope(t, id, 1, domainEvent{Items: []string{"fixed"}})
	id.typeName, id.value = "changed", "after envelope construction"
	snapshot := domainSnapshot(t, 1, domainState{Total: 1})
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), first, snapshot))
	assert.Equal(t, 1, id.typeCalls)
	assert.Equal(t, 1, id.valueCalls)
	assert.Equal(t, "Order-fixed", first.AggregateID())
	related, err := eventstore.NewAggregateID("Order", "fixed-extra")
	require.NoError(t, err)
	require.NoError(t, entry.PersistEvent(t.Context(), domainEnvelope(t, related, 1, domainEvent{Items: []string{"other"}})))

	readID := &changingDomainID{typeName: "Order", value: "fixed"}
	read, err := entry.GetLatestSnapshotByID(t.Context(), readID)
	require.NoError(t, err)
	require.NotNil(t, read.Snapshot)
	assert.Equal(t, domainState{Total: 1}, read.Snapshot.Aggregate())
	assert.Equal(t, 1, readID.typeCalls)
	assert.Equal(t, 1, readID.valueCalls)
	eventID := &changingDomainID{typeName: "Order", value: "fixed"}
	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), eventID, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "Order-fixed", events[0].AggregateID())
	assert.Equal(t, domainEvent{Items: []string{"fixed"}}, events[0].Payload())
	assert.Equal(t, 1, eventID.typeCalls)
	assert.Equal(t, 1, eventID.valueCalls)
	otherEvents, err := entry.GetEventsByIDSinceSeqNr(t.Context(), related, 0)
	require.NoError(t, err)
	require.Len(t, otherEvents, 1)
	assert.Equal(t, domainEvent{Items: []string{"other"}}, otherEvents[0].Payload())
}
