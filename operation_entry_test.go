package eventstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// entryBoundary only counts calls and returns controlled responses; it does not commit data.
type entryBoundary struct {
	persistCalls  int
	pairCalls     int
	snapshotCalls int
	eventsCalls   int
	err           error
	snapshotRead  *SnapshotRead[[]byte]
	eventsRead    []EventEnvelope[[]byte]
}

func (b *entryBoundary) PersistEvent(context.Context, EventEnvelope[[]byte]) error {
	b.persistCalls++
	return b.err
}

func (b *entryBoundary) PersistEventAndSnapshot(context.Context, EventEnvelope[[]byte], SnapshotEnvelope[[]byte]) error {
	b.pairCalls++
	return b.err
}

func (b *entryBoundary) GetLatestSnapshotByID(context.Context, AggregateID) (*SnapshotRead[[]byte], error) {
	b.snapshotCalls++
	return b.snapshotRead, b.err
}

func (b *entryBoundary) GetEventsByIDSinceSeqNr(context.Context, AggregateID, SeqNr) ([]EventEnvelope[[]byte], error) {
	b.eventsCalls++
	return b.eventsRead, b.err
}

func (b *entryBoundary) calls() int {
	return b.persistCalls + b.pairCalls + b.snapshotCalls + b.eventsCalls
}

type entrySerializer struct {
	base               Serializer[string]
	serializeCalls     int
	deserializeCalls   int
	serializeErr       error
	deserializeErr     error
	deserializeFailure int
}

func (s *entrySerializer) Serialize(value string) ([]byte, error) {
	s.serializeCalls++
	if s.serializeErr != nil {
		return nil, s.serializeErr
	}
	return s.base.Serialize(value)
}

func (s *entrySerializer) Deserialize(data []byte) (string, error) {
	s.deserializeCalls++
	if s.deserializeErr != nil && s.deserializeCalls == s.deserializeFailure {
		return "", s.deserializeErr
	}
	return s.base.Deserialize(data)
}

func newEntryForTest(t *testing.T, boundary *entryBoundary) (EventStore[string, string], *entrySerializer, *entrySerializer) {
	t.Helper()
	events := &entrySerializer{base: NewJSONSerializer[string]()}
	snapshots := &entrySerializer{base: NewJSONSerializer[string]()}
	store, err := NewOperationEntry(events, snapshots, func(...Option) (EventStore[[]byte, []byte], error) {
		return boundary, nil
	})
	require.NoError(t, err)
	return store, events, snapshots
}

func TestOperationEntryRevalidatesEventBeforeBothWrites(t *testing.T) {
	valid, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 123), "payload")
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope("state", 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*EventEnvelope[string])
		rule   string
	}{
		{"zero envelope", func(e *EventEnvelope[string]) { *e = EventEnvelope[string]{} }, "T-2"},
		{"zero event number", func(e *EventEnvelope[string]) { e.seqNr = 0 }, "W-6"},
		{"negative number", func(e *EventEnvelope[string]) { e.seqNr = -1 }, "T-9"},
		{"number above maximum", func(e *EventEnvelope[string]) { e.seqNr = MaxSeqNr + 1 }, "T-9"},
		{"time below minimum", func(e *EventEnvelope[string]) { e.occurredAt = minOccurredAt.Add(-time.Nanosecond) }, "T-13"},
		{"time above maximum", func(e *EventEnvelope[string]) { e.occurredAt = maxOccurredAt.Add(time.Nanosecond) }, "T-13"},
	} {
		for _, pair := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pair=%t", tc.name, pair), func(t *testing.T) {
				event := valid
				tc.change(&event)
				boundary := &entryBoundary{}
				store, events, snapshots := newEntryForTest(t, boundary)

				var err error
				if pair {
					err = store.PersistEventAndSnapshot(context.Background(), event, snapshot)
				} else {
					err = store.PersistEvent(context.Background(), event)
				}

				violation := requireContractViolation(t, err, tc.rule)
				if tc.rule == "T-2" {
					assert.Nil(t, violation.SeqNr)
				} else {
					require.NotNil(t, violation.SeqNr)
					assert.Equal(t, event.SeqNr(), *violation.SeqNr)
				}
				assert.Zero(t, boundary.calls())
				assert.Zero(t, events.serializeCalls)
				assert.Zero(t, snapshots.serializeCalls)
			})
		}
	}
}

func TestOperationEntryRevalidatesSnapshotBeforeSerialization(t *testing.T) {
	event, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), "payload")
	require.NoError(t, err)
	valid, err := NewSnapshotEnvelope("state", 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*SnapshotEnvelope[string])
		rule   string
	}{
		{"zero envelope", func(s *SnapshotEnvelope[string]) { *s = SnapshotEnvelope[string]{} }, "T-10"},
		{"negative number", func(s *SnapshotEnvelope[string]) { s.seqNr = -1 }, "T-9"},
		{"number above maximum", func(s *SnapshotEnvelope[string]) { s.seqNr = MaxSeqNr + 1 }, "T-9"},
		{"mismatched zero", func(s *SnapshotEnvelope[string]) { s.seqNr = 0 }, "W-9"},
		{"mismatched positive", func(s *SnapshotEnvelope[string]) { s.seqNr = 2 }, "W-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := valid
			tc.change(&snapshot)
			boundary := &entryBoundary{}
			store, events, snapshots := newEntryForTest(t, boundary)

			err := store.PersistEventAndSnapshot(context.Background(), event, snapshot)

			violation := requireContractViolation(t, err, tc.rule)
			switch tc.rule {
			case "T-10":
				assert.Nil(t, violation.SeqNr)
			case "T-9":
				require.NotNil(t, violation.SeqNr)
				assert.Equal(t, snapshot.SeqNr(), *violation.SeqNr)
			case "W-9":
				require.NotNil(t, violation.SeqNr)
				require.NotNil(t, violation.SnapshotSeqNr)
				assert.Equal(t, event.SeqNr(), *violation.SeqNr)
				assert.Equal(t, snapshot.SeqNr(), *violation.SnapshotSeqNr)
				assert.Contains(t, err.Error(), "seq_nr=1")
				assert.Contains(t, err.Error(), fmt.Sprintf("snapshot_seq_nr=%d", snapshot.SeqNr()))
			}
			assert.Zero(t, boundary.calls())
			assert.Zero(t, events.serializeCalls)
			assert.Zero(t, snapshots.serializeCalls)
		})
	}
}

func TestOperationEntryValidatesReadIDsBeforeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   AggregateID
		rule string
	}{
		{"nil", nil, "T-2"},
		{"typed nil pointer", (*nilPointerID)(nil), "T-2"},
		{"typed nil map", nilMapID(nil), "T-2"},
		{"typed nil slice", nilSliceID(nil), "T-2"},
		{"typed nil function", nilFuncID(nil), "T-2"},
		{"typed nil channel", nilChanID(nil), "T-2"},
		{"hyphen in type", userID{"Order-Item", "1"}, "T-11"},
		{"too long", userID{"Order", strings.Repeat("x", 1024)}, "T-12"},
	} {
		for _, operation := range []struct {
			name   string
			events bool
			seqNr  SeqNr
		}{
			{"snapshot", false, 0},
			{"events since zero", true, 0},
			{"events since positive", true, 2},
			{"events since negative", true, -1},
			{"events since above maximum", true, MaxSeqNr + 1},
		} {
			t.Run(tc.name+"/"+operation.name, func(t *testing.T) {
				boundary := &entryBoundary{}
				store, eventSerializer, snapshotSerializer := newEntryForTest(t, boundary)

				var err error
				if !operation.events {
					var result *SnapshotRead[string]
					result, err = store.GetLatestSnapshotByID(context.Background(), tc.id)
					assert.Nil(t, result)
				} else {
					var result []EventEnvelope[string]
					result, err = store.GetEventsByIDSinceSeqNr(context.Background(), tc.id, operation.seqNr)
					assert.Nil(t, result)
				}

				violation := requireContractViolation(t, err, tc.rule)
				assert.Nil(t, violation.Unwrap())
				assert.Zero(t, boundary.calls())
				assert.Zero(t, eventSerializer.serializeCalls)
				assert.Zero(t, snapshotSerializer.serializeCalls)
				assert.Zero(t, eventSerializer.deserializeCalls)
				assert.Zero(t, snapshotSerializer.deserializeCalls)
				if operation.events {
					t.Logf("rule=%s requested_seq_nr=%d diagnostic_seq_nr=%v message=%q boundary_calls=%d serializer_calls=0",
						violation.Rule, operation.seqNr, violation.SeqNr, err.Error(), boundary.calls())
					require.NotNil(t, violation.SeqNr)
					assert.Equal(t, operation.seqNr, *violation.SeqNr)
					assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", operation.seqNr))
				} else {
					assert.Nil(t, violation.SeqNr)
				}
			})
		}
	}
}

func TestOperationEntryValidatesGeneralReadSequence(t *testing.T) {
	for _, seqNr := range []SeqNr{-1, MaxSeqNr + 1, 0, MaxSeqNr} {
		t.Run(fmt.Sprint(seqNr), func(t *testing.T) {
			boundary := &entryBoundary{}
			store, _, _ := newEntryForTest(t, boundary)

			result, err := store.GetEventsByIDSinceSeqNr(context.Background(), userID{"Order", "1"}, seqNr)

			assert.Nil(t, result)
			if seqNr < 0 || seqNr > MaxSeqNr {
				violation := requireContractViolation(t, err, "T-9")
				require.NotNil(t, violation.SeqNr)
				assert.Equal(t, seqNr, *violation.SeqNr)
				assert.Zero(t, boundary.calls())
			} else {
				require.NoError(t, err)
				assert.Equal(t, 1, boundary.eventsCalls)
			}
		})
	}
}

func TestOperationEntrySerializationFailuresDoNotCallBoundary(t *testing.T) {
	event, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), "payload")
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope("state", 1)
	require.NoError(t, err)
	cause := errors.New("serializer failed")
	for _, serializerErr := range []error{cause, &SerializationError{Cause: cause}, &ConfigurationError{Cause: cause}} {
		for _, stage := range []string{"event", "pair event", "pair snapshot"} {
			t.Run(fmt.Sprintf("%T/%s", serializerErr, stage), func(t *testing.T) {
				boundary := &entryBoundary{}
				store, events, snapshots := newEntryForTest(t, boundary)
				if stage == "pair snapshot" {
					snapshots.serializeErr = serializerErr
				} else {
					events.serializeErr = serializerErr
				}

				var err error
				if stage == "event" {
					err = store.PersistEvent(context.Background(), event)
				} else {
					err = store.PersistEventAndSnapshot(context.Background(), event, snapshot)
				}

				failure := requireSerializationFailure(t, err)
				assert.ErrorIs(t, err, cause)
				assert.ErrorIs(t, failure.Unwrap(), cause)
				assert.ErrorIs(t, err, serializerErr)
				assert.Zero(t, boundary.calls())
				assert.Equal(t, 1, events.serializeCalls)
				if stage == "pair snapshot" {
					assert.Equal(t, 1, snapshots.serializeCalls)
				} else {
					assert.Zero(t, snapshots.serializeCalls)
				}
			})
		}
	}
}

func TestOperationEntryDeserializationFailuresReturnNoResult(t *testing.T) {
	event, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), []byte(`"payload"`))
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope([]byte(`"state"`), 1)
	require.NoError(t, err)
	cause := errors.New("deserializer failed")
	for _, serializerErr := range []error{cause, &SerializationError{Cause: cause}, &ConfigurationError{Cause: cause}} {
		for _, stage := range []string{"events", "snapshot"} {
			t.Run(fmt.Sprintf("%T/%s", serializerErr, stage), func(t *testing.T) {
				boundary := &entryBoundary{
					eventsRead:   []EventEnvelope[[]byte]{event, event},
					snapshotRead: &SnapshotRead[[]byte]{Snapshot: &snapshot, HeadSeqNr: 2},
				}
				store, events, snapshots := newEntryForTest(t, boundary)

				var err error
				if stage == "events" {
					events.deserializeErr, events.deserializeFailure = serializerErr, 2
					var result []EventEnvelope[string]
					result, err = store.GetEventsByIDSinceSeqNr(context.Background(), userID{"Order", "1"}, 0)
					assert.Nil(t, result, "a later failure must not return a partial list")
					assert.Equal(t, 2, events.deserializeCalls)
				} else {
					snapshots.deserializeErr, snapshots.deserializeFailure = serializerErr, 1
					var result *SnapshotRead[string]
					result, err = store.GetLatestSnapshotByID(context.Background(), userID{"Order", "1"})
					assert.Nil(t, result)
					assert.Equal(t, 1, snapshots.deserializeCalls)
				}

				failure := requireSerializationFailure(t, err)
				assert.ErrorIs(t, err, cause)
				assert.ErrorIs(t, failure.Unwrap(), cause)
				assert.ErrorIs(t, err, serializerErr)
				assert.Equal(t, 1, boundary.calls())
			})
		}
	}
}

func TestOperationEntryPreservesBoundaryErrorCategoriesAndCauses(t *testing.T) {
	id := userID{"Order", "1"}
	event, err := NewEventEnvelope(id, 1, time.Unix(0, 0), "payload")
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope("state", 1)
	require.NoError(t, err)
	cause := errors.New("boundary failed")
	seqNr, snapshotSeqNr, headSeqNr := SeqNr(1), SeqNr(2), SeqNr(3)
	for _, tc := range []struct {
		name       string
		kind       Kind
		classified Error
	}{
		{"optimistic lock", KindOptimisticLock, &OptimisticLockError{AggregateID: "Order-1", SeqNr: seqNr, HeadSeqNr: &headSeqNr, Cause: cause}},
		{"contract violation", KindContractViolation, &ContractViolationError{Rule: "W-9", SeqNr: &seqNr, SnapshotSeqNr: &snapshotSeqNr, Cause: cause}},
		{"serialization", KindSerialization, &SerializationError{Cause: cause}},
		{"configuration", KindConfiguration, &ConfigurationError{Cause: cause}},
		{"storage", KindStorage, &StorageError{Cause: cause}},
		{"unclassified", KindStorage, nil},
	} {
		for _, operation := range []string{"persist", "pair", "snapshot", "events", "open"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				original := error(cause)
				if tc.classified != nil {
					original = tc.classified
				}
				wrapped := fmt.Errorf("boundary: %w", original)
				boundary := &entryBoundary{err: wrapped}
				store, events, snapshots := newEntryForTest(t, boundary)

				var err error
				switch operation {
				case "persist":
					err = store.PersistEvent(context.Background(), event)
				case "pair":
					err = store.PersistEventAndSnapshot(context.Background(), event, snapshot)
				case "snapshot":
					var result *SnapshotRead[string]
					result, err = store.GetLatestSnapshotByID(context.Background(), id)
					assert.Nil(t, result)
				case "events":
					var result []EventEnvelope[string]
					result, err = store.GetEventsByIDSinceSeqNr(context.Background(), id, 0)
					assert.Nil(t, result)
				case "open":
					openCalls := 0
					store, err = NewOperationEntry(events, snapshots, func(...Option) (EventStore[[]byte, []byte], error) {
						openCalls++
						return nil, wrapped
					})
					assert.Nil(t, store)
					assert.Equal(t, 1, openCalls)
				}

				kind, ok := KindOf(err)
				require.True(t, ok)
				assert.Equal(t, tc.kind, kind)
				assert.ErrorIs(t, err, cause)
				assert.ErrorIs(t, err, wrapped)
				var classified Error
				require.ErrorAs(t, err, &classified)
				assert.ErrorIs(t, classified.Unwrap(), cause)
				if tc.classified != nil {
					assert.Equal(t, tc.classified, classified, "concrete type and diagnostics must survive")
				} else {
					var storage *StorageError
					require.ErrorAs(t, err, &storage)
					assert.Equal(t, wrapped, errors.Unwrap(storage))
				}
				if operation == "open" {
					assert.Zero(t, boundary.calls())
				} else {
					assert.Equal(t, 1, boundary.calls())
				}
				assert.Zero(t, events.deserializeCalls)
				assert.Zero(t, snapshots.deserializeCalls)
			})
		}
	}
}

func TestNewOperationEntryRejectsNilDependencies(t *testing.T) {
	serializer := NewJSONSerializer[string]()
	for _, tc := range []struct {
		name      string
		events    Serializer[string]
		snapshots Serializer[string]
		openNil   bool
		boundary  EventStore[[]byte, []byte]
		openCalls int
	}{
		{"nil events", nil, serializer, false, &entryBoundary{}, 0},
		{"typed nil events", (*entrySerializer)(nil), serializer, false, &entryBoundary{}, 0},
		{"nil snapshots", serializer, nil, false, &entryBoundary{}, 0},
		{"typed nil snapshots", serializer, (*entrySerializer)(nil), false, &entryBoundary{}, 0},
		{"nil constructor", serializer, serializer, true, nil, 0},
		{"nil boundary", serializer, serializer, false, nil, 1},
		{"typed nil boundary", serializer, serializer, false, (*entryBoundary)(nil), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var open func(...Option) (EventStore[[]byte, []byte], error)
			if !tc.openNil {
				open = func(...Option) (EventStore[[]byte, []byte], error) {
					calls++
					return tc.boundary, nil
				}
			}

			store, err := NewOperationEntry(tc.events, tc.snapshots, open)

			assert.Nil(t, store)
			var configuration *ConfigurationError
			require.ErrorAs(t, err, &configuration)
			kind, ok := KindOf(err)
			require.True(t, ok)
			assert.Equal(t, KindConfiguration, kind)
			require.NotNil(t, configuration.Unwrap())
			assert.Equal(t, tc.openCalls, calls)
		})
	}
}

func TestPrepareEventFailures(t *testing.T) {
	serializer := &entrySerializer{base: NewJSONSerializer[string]()}
	prepared, err := PrepareEvent(serializer, EventEnvelope[string]{})
	requireContractViolation(t, err, "T-2")
	assert.Empty(t, prepared.AggregateID())
	assert.Zero(t, serializer.serializeCalls)

	event, err := NewEventEnvelope(userID{"Order", "prepare"}, 1, time.Unix(0, 123), "payload")
	require.NoError(t, err)
	for _, missing := range []Serializer[string]{nil, (*entrySerializer)(nil)} {
		_, err := PrepareEvent(missing, event)
		var configuration *ConfigurationError
		require.ErrorAs(t, err, &configuration)
	}
	cause := errors.New("serializer failed")
	for _, original := range []error{cause, &SerializationError{Cause: cause}, &ConfigurationError{Cause: cause}} {
		serializer.serializeErr = original
		prepared, err = PrepareEvent(serializer, event)
		requireSerializationFailure(t, err)
		assert.ErrorIs(t, err, original)
		assert.ErrorIs(t, err, cause)
		assert.Empty(t, prepared.AggregateID())
	}
}

func TestPrepareEventAndSnapshot(t *testing.T) {
	event, err := NewEventEnvelope(userID{"Order", "pair"}, 1, time.Unix(0, -1), "event", WithManifest("event/型"))
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope("state", 1, WithManifest("state/型"))
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		event    EventEnvelope[string]
		snapshot SnapshotEnvelope[string]
		rule     string
	}{
		{"event", EventEnvelope[string]{}, snapshot, "T-2"},
		{"snapshot", event, SnapshotEnvelope[string]{}, "T-10"},
		{"number", event, func() SnapshotEnvelope[string] {
			s, e := NewSnapshotEnvelope("state", 2)
			require.NoError(t, e)
			return s
		}(), "W-9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serializer := &entrySerializer{base: NewJSONSerializer[string]()}
			_, _, err := PrepareEventAndSnapshot(serializer, serializer, tc.event, tc.snapshot)
			var violation *ContractViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, tc.rule, violation.Rule)
			require.Zero(t, serializer.serializeCalls)
		})
	}
	for _, missing := range []Serializer[string]{nil, (*entrySerializer)(nil)} {
		serializer := &entrySerializer{base: NewJSONSerializer[string]()}
		_, _, err := PrepareEventAndSnapshot(serializer, missing, event, snapshot)
		kind, ok := KindOf(err)
		require.True(t, ok)
		require.Equal(t, KindConfiguration, kind)
		require.Zero(t, serializer.serializeCalls)
		_, _, err = PrepareEventAndSnapshot(missing, serializer, event, snapshot)
		kind, ok = KindOf(err)
		require.True(t, ok)
		require.Equal(t, KindConfiguration, kind)
		require.Zero(t, serializer.serializeCalls)
	}
	serializer := NewJSONSerializer[string]()
	preparedEvent, preparedSnapshot, err := PrepareEventAndSnapshot(serializer, serializer, event, snapshot)
	require.NoError(t, err)
	require.Equal(t, event.AggregateID(), preparedEvent.AggregateID())
	require.Equal(t, event.OccurredAt(), preparedEvent.OccurredAt())
	require.Equal(t, event.Manifest(), preparedEvent.Manifest())
	require.Equal(t, snapshot.Manifest(), preparedSnapshot.Manifest())
	require.Equal(t, snapshot.SeqNr(), preparedSnapshot.SeqNr())
	require.Equal(t, []byte(`"event"`), preparedEvent.Payload())
	require.Equal(t, []byte(`"state"`), preparedSnapshot.Aggregate())
}
