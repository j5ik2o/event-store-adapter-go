package eventstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These domain types have no library-specific methods.
type domainEvent struct {
	AggregateID string   `json:"aggregate_id"`
	SeqNr       string   `json:"seq_nr"`
	OccurredAt  string   `json:"occurred_at"`
	Manifest    string   `json:"manifest"`
	Items       []string `json:"items"`
}

type domainState struct {
	Total    int               `json:"total"`
	Manifest string            `json:"manifest"`
	Labels   map[string]string `json:"labels"`
}

type recordingSerializer[T any] struct {
	base       eventstore.Serializer[T]
	serialized []T
	restored   [][]byte
}

func (s *recordingSerializer[T]) Serialize(value T) ([]byte, error) {
	s.serialized = append(s.serialized, value)
	return s.base.Serialize(value)
}

func (s *recordingSerializer[T]) Deserialize(data []byte) (T, error) {
	s.restored = append(s.restored, bytes.Clone(data))
	return s.base.Deserialize(data)
}

type boundaryCall struct {
	name  string
	ctx   context.Context
	id    eventstore.AggregateID
	aid   string
	since eventstore.SeqNr
}

// observedBoundary records write arguments and exposes explicitly controlled read responses.
// It has no head checks, persistence, locking or retention implementation.
type observedBoundary struct {
	openCalls        int
	settings         storeoptions.Options
	calls            []boundaryCall
	eventWrites      []eventstore.EventEnvelope[[]byte]
	snapshotWrites   []eventstore.SnapshotEnvelope[[]byte]
	eventsResponse   []eventstore.EventEnvelope[[]byte]
	snapshotResponse *eventstore.SnapshotRead[[]byte]
}

func (b *observedBoundary) open(opts ...eventstore.Option) (eventstore.EventStore[[]byte, []byte], error) {
	b.openCalls++
	settings, err := storeoptions.Apply(func(cause error) error {
		return &eventstore.ConfigurationError{Cause: cause}
	}, opts...)
	if err != nil {
		return nil, err
	}
	b.settings = settings
	return b, nil
}

func (b *observedBoundary) PersistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	b.calls = append(b.calls, boundaryCall{name: "PersistEvent", ctx: ctx})
	b.eventWrites = append(b.eventWrites, event)
	return nil
}

func (b *observedBoundary) PersistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	b.calls = append(b.calls, boundaryCall{name: "PersistEventAndSnapshot", ctx: ctx})
	b.eventWrites = append(b.eventWrites, event)
	b.snapshotWrites = append(b.snapshotWrites, snapshot)
	return nil
}

func (b *observedBoundary) GetLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	b.calls = append(b.calls, boundaryCall{name: "GetLatestSnapshotByID", ctx: ctx, id: id, aid: aid})
	if err != nil {
		return nil, err
	}
	return b.snapshotResponse, nil
}

func (b *observedBoundary) GetEventsByIDSinceSeqNr(ctx context.Context, id eventstore.AggregateID, since eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	aid, err := eventstore.AidString(id)
	b.calls = append(b.calls, boundaryCall{name: "GetEventsByIDSinceSeqNr", ctx: ctx, id: id, aid: aid, since: since})
	if err != nil {
		return nil, err
	}
	return b.eventsResponse, nil
}

func TestOperationEntryExternalDomainAndOptionsRoundTrip(t *testing.T) {
	events := &recordingSerializer[domainEvent]{base: eventstore.NewJSONSerializer[domainEvent]()}
	snapshots := &recordingSerializer[domainState]{base: eventstore.NewJSONSerializer[domainState]()}
	boundary := &observedBoundary{}
	count, err := eventstore.KeepLatest(3)
	require.NoError(t, err)
	notifications := 0
	store, err := eventstore.NewOperationEntry(events, snapshots, boundary.open,
		eventstore.WithRetentionCount(count),
		eventstore.WithRetentionMode(eventstore.RetentionTTL),
		eventstore.WithTTLGraceSeconds(42),
		eventstore.WithRetentionFailureHandler(func(context.Context, error) { notifications++ }),
	)
	require.NoError(t, err)
	assert.Equal(t, 1, boundary.openCalls)
	require.NotNil(t, boundary.settings.RetentionCount)
	assert.Equal(t, 3, *boundary.settings.RetentionCount)
	assert.Equal(t, storeoptions.RetentionTTL, boundary.settings.RetentionMode)
	assert.Equal(t, int64(42), boundary.settings.TTLGraceSeconds)
	require.NotNil(t, boundary.settings.RetentionFailureHandler)

	id, err := eventstore.NewAggregateID("Order", "item-1")
	require.NoError(t, err)
	at := time.Date(2026, time.October, 9, 12, 34, 56, 123456789, time.FixedZone("domain", 9*60*60))
	payload := domainEvent{
		AggregateID: "domain-id", SeqNr: "domain-number", OccurredAt: "domain-time", Manifest: "domain-manifest",
		Items: []string{"second", "first"},
	}
	event, err := eventstore.NewEventEnvelope(id, 7, at, payload, eventstore.WithManifest("  opaque/イベント\x00\n "))
	require.NoError(t, err)
	payload2 := domainEvent{AggregateID: "another-domain-id", Items: []string{"third"}}
	event2, err := eventstore.NewEventEnvelope(id, 8, at.Add(time.Nanosecond), payload2)
	require.NoError(t, err)
	state := domainState{Total: 2, Manifest: "state-manifest", Labels: map[string]string{"e\u0301": "é"}}
	snapshot, err := eventstore.NewSnapshotEnvelope(state, 8, eventstore.WithManifest(" snapshot/型-v99\x00 "))
	require.NoError(t, err)
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "operation context")

	require.NoError(t, store.PersistEvent(ctx, event))
	require.NoError(t, store.PersistEventAndSnapshot(ctx, event2, snapshot))

	require.Len(t, boundary.eventWrites, 2)
	require.Len(t, boundary.snapshotWrites, 1)
	assert.Equal(t, []domainEvent{payload, payload2}, events.serialized)
	assert.Equal(t, []domainState{state}, snapshots.serialized)
	for i, original := range []eventstore.EventEnvelope[domainEvent]{event, event2} {
		stored := boundary.eventWrites[i]
		require.NoError(t, stored.Validate())
		assert.Equal(t, original.AggregateID(), stored.AggregateID())
		assert.Equal(t, original.SeqNr(), stored.SeqNr())
		assert.Equal(t, original.OccurredAt(), stored.OccurredAt())
		assert.Equal(t, original.Manifest(), stored.Manifest())
		wantBytes, err := json.Marshal(original.Payload())
		require.NoError(t, err)
		assert.Equal(t, wantBytes, stored.Payload())
	}
	storedSnapshot := boundary.snapshotWrites[0]
	require.NoError(t, storedSnapshot.Validate())
	assert.Equal(t, snapshot.SeqNr(), storedSnapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), storedSnapshot.Manifest())
	wantSnapshotBytes, err := json.Marshal(state)
	require.NoError(t, err)
	assert.Equal(t, wantSnapshotBytes, storedSnapshot.Aggregate())

	// Feed exactly the observed write bytes back through the controlled read boundary.
	boundary.eventsResponse = boundary.eventWrites
	boundary.snapshotResponse = &eventstore.SnapshotRead[[]byte]{Snapshot: &storedSnapshot, HeadSeqNr: 11}
	readSnapshot, err := store.GetLatestSnapshotByID(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, readSnapshot)
	require.NotNil(t, readSnapshot.Snapshot)
	require.NoError(t, readSnapshot.Snapshot.Validate())
	assert.Equal(t, eventstore.SeqNr(11), readSnapshot.HeadSeqNr)
	assert.Equal(t, snapshot.SeqNr(), readSnapshot.Snapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), readSnapshot.Snapshot.Manifest())
	assert.Equal(t, state, readSnapshot.Snapshot.Aggregate())
	assert.Equal(t, [][]byte{storedSnapshot.Aggregate()}, snapshots.restored)

	readEvents, err := store.GetEventsByIDSinceSeqNr(ctx, id, 0)
	require.NoError(t, err)
	require.Len(t, readEvents, 2)
	for i, original := range []eventstore.EventEnvelope[domainEvent]{event, event2} {
		restored := readEvents[i]
		require.NoError(t, restored.Validate())
		assert.Equal(t, original.AggregateID(), restored.AggregateID())
		assert.Equal(t, original.SeqNr(), restored.SeqNr())
		assert.Equal(t, original.OccurredAt(), restored.OccurredAt())
		assert.Equal(t, original.Manifest(), restored.Manifest())
		assert.Equal(t, original.Payload(), restored.Payload())
	}
	assert.Equal(t, [][]byte{boundary.eventWrites[0].Payload(), boundary.eventWrites[1].Payload()}, events.restored)
	assert.Equal(t, []boundaryCall{
		{name: "PersistEvent", ctx: ctx},
		{name: "PersistEventAndSnapshot", ctx: ctx},
		{name: "GetLatestSnapshotByID", ctx: ctx, id: id, aid: "Order-item-1"},
		{name: "GetEventsByIDSinceSeqNr", ctx: ctx, id: id, aid: "Order-item-1", since: 0},
	}, boundary.calls)
	assert.Zero(t, notifications, "the common entry does not run retention")
}

type changingAggregateID struct {
	typeName      string
	value         string
	laterTypeName string
	laterValue    string
	typeNameCalls int
	valueCalls    int
}

func (id *changingAggregateID) TypeName() string {
	id.typeNameCalls++
	if id.typeNameCalls == 1 {
		return id.typeName
	}
	return id.laterTypeName
}

func (id *changingAggregateID) Value() string {
	id.valueCalls++
	if id.valueCalls == 1 {
		return id.value
	}
	return id.laterValue
}

func TestOperationEntryReadIDsRemainValidated(t *testing.T) {
	for _, tc := range []struct {
		name, typeName, value, laterTypeName, laterValue string
	}{
		{"changed key", "Order", "item-1", "Other", "item-2"},
		{"later invalid type", "Order", "item-1", "Other-Type", "item-2"},
		{"empty type", "", "item-1", "Other", "item-2"},
		{"empty value", "Order", "", "Other", "item-2"},
		{"empty parts", "", "", "Other", "item-2"},
	} {
		for _, operation := range []string{"GetLatestSnapshotByID", "GetEventsByIDSinceSeqNr"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				id := &changingAggregateID{
					typeName: tc.typeName, value: tc.value,
					laterTypeName: tc.laterTypeName, laterValue: tc.laterValue,
				}
				boundary := &observedBoundary{}
				serializer := eventstore.NewJSONSerializer[string]()
				store, err := eventstore.NewOperationEntry(serializer, serializer, boundary.open)
				require.NoError(t, err)
				type contextKey struct{}
				ctx := context.WithValue(context.Background(), contextKey{}, "read context")
				var since eventstore.SeqNr

				if operation == "GetLatestSnapshotByID" {
					_, err = store.GetLatestSnapshotByID(ctx, id)
				} else {
					since = 7
					_, err = store.GetEventsByIDSinceSeqNr(ctx, id, since)
				}

				require.Len(t, boundary.calls, 1)
				call := boundary.calls[0]
				t.Logf("boundary calls=%d aid=%q since=%d TypeName calls=%d Value calls=%d error=%v",
					len(boundary.calls), call.aid, call.since, id.typeNameCalls, id.valueCalls, err)
				assert.NoError(t, err)
				assert.Equal(t, operation, call.name)
				assert.Equal(t, ctx, call.ctx)
				assert.Equal(t, since, call.since)
				assert.Equal(t, tc.typeName+"-"+tc.value, call.aid)
				assert.Equal(t, 1, id.typeNameCalls)
				assert.Equal(t, 1, id.valueCalls)
				// Reusing the received ID must still yield the originally validated parts.
				assert.Equal(t, tc.typeName, call.id.TypeName())
				assert.Equal(t, tc.value, call.id.Value())
				assert.Equal(t, 1, id.typeNameCalls)
				assert.Equal(t, 1, id.valueCalls)
			})
		}
	}
}

// bufferSerializer reuses one output buffer and changes its deserialization input.
type bufferSerializer struct {
	scratch    [64]byte
	serialized []string
	restored   []string
}

func TestPrepareEventOwnsBytesAndFixedMetadata(t *testing.T) {
	id := &changingAggregateID{typeName: "Order", value: "item-1", laterTypeName: "Other-Type", laterValue: "changed"}
	at := time.Unix(123, 456789123)
	first, err := eventstore.NewEventEnvelope(id, 1, at, "first", eventstore.WithManifest(" 型\x00 "))
	require.NoError(t, err)
	serializer := &bufferSerializer{}
	prepared, err := eventstore.PrepareEvent(serializer, first)
	require.NoError(t, err)

	// The serializer is allowed to reuse its buffer after preparation returns.
	_, err = serializer.Serialize("overwritten")
	require.NoError(t, err)
	require.Equal(t, "Order-item-1", prepared.AggregateID())
	require.Equal(t, first.SeqNr(), prepared.SeqNr())
	require.Equal(t, at, prepared.OccurredAt())
	require.Equal(t, first.Manifest(), prepared.Manifest())
	require.Equal(t, []byte("first"), prepared.Payload())
	require.NoError(t, prepared.Validate())
	require.Equal(t, 1, id.typeNameCalls)
	require.Equal(t, 1, id.valueCalls)

	domain := domainEvent{AggregateID: "payload-id", SeqNr: "payload-number", Items: []string{"任意"}}
	jsonSerializer := &recordingSerializer[domainEvent]{base: eventstore.NewJSONSerializer[domainEvent]()}
	fixedID, err := eventstore.NewAggregateID("Order", "domain")
	require.NoError(t, err)
	event, err := eventstore.NewEventEnvelope(fixedID, 1, at, domain)
	require.NoError(t, err)
	stored, err := eventstore.PrepareEvent(jsonSerializer, event)
	require.NoError(t, err)
	require.Equal(t, []domainEvent{domain}, jsonSerializer.serialized)
	var restored domainEvent
	require.NoError(t, json.Unmarshal(stored.Payload(), &restored))
	require.Equal(t, domain, restored)
}

func (s *bufferSerializer) Serialize(value string) ([]byte, error) {
	s.serialized = append(s.serialized, value)
	data := s.scratch[:len(value)]
	copy(data, value)
	return data, nil
}

func (s *bufferSerializer) Deserialize(data []byte) (string, error) {
	value := string(data)
	s.restored = append(s.restored, value)
	for i := range data {
		data[i] = '!'
	}
	return value, nil
}

func TestOperationEntryCopiesSerializedBytes(t *testing.T) {
	serializer := &bufferSerializer{}
	boundary := &observedBoundary{}
	store, err := eventstore.NewOperationEntry(serializer, serializer, boundary.open)
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Order", "item-1")
	require.NoError(t, err)
	at := time.Date(2026, time.October, 9, 12, 34, 56, 123456789, time.FixedZone("domain", 9*60*60))
	event, err := eventstore.NewEventEnvelope(id, 7, at, "event-before", eventstore.WithManifest(" event/型\x00 "))
	require.NoError(t, err)
	pairEvent, err := eventstore.NewEventEnvelope(id, 8, at.Add(time.Nanosecond), "event-pair", eventstore.WithManifest(" pair/イベント\x00 "))
	require.NoError(t, err)
	snapshot, err := eventstore.NewSnapshotEnvelope("state-pair", 8, eventstore.WithManifest(" snapshot/型\x00 "))
	require.NoError(t, err)
	laterEvent, err := eventstore.NewEventEnvelope(id, 9, at.Add(2*time.Nanosecond), "event-later-overwrites", eventstore.WithManifest(" later/型\x00 "))
	require.NoError(t, err)
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "write context")

	require.NoError(t, store.PersistEvent(ctx, event))
	require.Len(t, boundary.eventWrites, 1)
	assert.Equal(t, []byte(event.Payload()), boundary.eventWrites[0].Payload())
	require.NoError(t, store.PersistEventAndSnapshot(ctx, pairEvent, snapshot))
	require.Len(t, boundary.eventWrites, 2)
	require.Len(t, boundary.snapshotWrites, 1)
	t.Logf("after pair: boundary calls=%d event bytes=%q, %q snapshot bytes=%q",
		len(boundary.calls), boundary.eventWrites[0].Payload(), boundary.eventWrites[1].Payload(), boundary.snapshotWrites[0].Aggregate())
	assert.Equal(t, []byte(event.Payload()), boundary.eventWrites[0].Payload())
	assert.Equal(t, []byte(pairEvent.Payload()), boundary.eventWrites[1].Payload())
	assert.Equal(t, []byte(snapshot.Aggregate()), boundary.snapshotWrites[0].Aggregate())

	// The same entry, serializers and boundary survive another scratch-buffer overwrite.
	require.NoError(t, store.PersistEvent(ctx, laterEvent))
	require.Len(t, boundary.eventWrites, 3)
	require.Len(t, boundary.snapshotWrites, 1)
	t.Logf("after later write: boundary calls=%d event bytes=%q, %q, %q snapshot bytes=%q",
		len(boundary.calls), boundary.eventWrites[0].Payload(), boundary.eventWrites[1].Payload(), boundary.eventWrites[2].Payload(), boundary.snapshotWrites[0].Aggregate())
	for i, original := range []eventstore.EventEnvelope[string]{event, pairEvent, laterEvent} {
		stored := boundary.eventWrites[i]
		require.NoError(t, stored.Validate())
		assert.Equal(t, original.AggregateID(), stored.AggregateID())
		assert.Equal(t, original.SeqNr(), stored.SeqNr())
		assert.Equal(t, original.OccurredAt(), stored.OccurredAt())
		assert.Equal(t, original.Manifest(), stored.Manifest())
		assert.Equal(t, []byte(original.Payload()), stored.Payload())
	}
	storedSnapshot := boundary.snapshotWrites[0]
	require.NoError(t, storedSnapshot.Validate())
	assert.Equal(t, snapshot.SeqNr(), storedSnapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), storedSnapshot.Manifest())
	assert.Equal(t, []byte(snapshot.Aggregate()), storedSnapshot.Aggregate())
	assert.Equal(t, []string{event.Payload(), pairEvent.Payload(), snapshot.Aggregate(), laterEvent.Payload()}, serializer.serialized)
	assert.Equal(t, []boundaryCall{
		{name: "PersistEvent", ctx: ctx},
		{name: "PersistEventAndSnapshot", ctx: ctx},
		{name: "PersistEvent", ctx: ctx},
	}, boundary.calls)
}

func TestOperationEntryCopiesDeserializationInputs(t *testing.T) {
	serializer := &bufferSerializer{}
	boundary := &observedBoundary{}
	store, err := eventstore.NewOperationEntry(serializer, serializer, boundary.open)
	require.NoError(t, err)
	id, err := eventstore.NewAggregateID("Order", "item-1")
	require.NoError(t, err)
	at := time.Unix(0, 123456789)
	for i, payload := range []string{"first-event", "second-event"} {
		event, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(7+i), at.Add(time.Duration(i)), []byte(payload), eventstore.WithManifest(" event/型\x00 "))
		require.NoError(t, err)
		boundary.eventsResponse = append(boundary.eventsResponse, event)
	}
	snapshot, err := eventstore.NewSnapshotEnvelope([]byte("state-pair"), 7, eventstore.WithManifest(" snapshot/型\x00 "))
	require.NoError(t, err)
	boundary.snapshotResponse = &eventstore.SnapshotRead[[]byte]{Snapshot: &snapshot, HeadSeqNr: 11}
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "read context")

	readSnapshot, err := store.GetLatestSnapshotByID(ctx, id)

	require.NoError(t, err)
	require.NotNil(t, readSnapshot)
	require.NotNil(t, readSnapshot.Snapshot)
	require.NoError(t, readSnapshot.Snapshot.Validate())
	assert.Equal(t, eventstore.SeqNr(11), readSnapshot.HeadSeqNr)
	assert.Equal(t, snapshot.SeqNr(), readSnapshot.Snapshot.SeqNr())
	assert.Equal(t, snapshot.Manifest(), readSnapshot.Snapshot.Manifest())
	assert.Equal(t, "state-pair", readSnapshot.Snapshot.Aggregate())
	assert.Equal(t, []byte("state-pair"), boundary.snapshotResponse.Snapshot.Aggregate())

	readEvents, err := store.GetEventsByIDSinceSeqNr(ctx, id, 7)

	require.NoError(t, err)
	require.Len(t, readEvents, 2)
	for i, want := range []string{"first-event", "second-event"} {
		original := boundary.eventsResponse[i]
		restored := readEvents[i]
		require.NoError(t, restored.Validate())
		assert.Equal(t, original.AggregateID(), restored.AggregateID())
		assert.Equal(t, original.SeqNr(), restored.SeqNr())
		assert.Equal(t, original.OccurredAt(), restored.OccurredAt())
		assert.Equal(t, original.Manifest(), restored.Manifest())
		assert.Equal(t, want, restored.Payload())
		assert.Equal(t, []byte(want), original.Payload())
	}
	t.Logf("after reads: boundary calls=%d event bytes=%q, %q snapshot bytes=%q",
		len(boundary.calls), boundary.eventsResponse[0].Payload(), boundary.eventsResponse[1].Payload(), boundary.snapshotResponse.Snapshot.Aggregate())
	assert.Equal(t, []byte("state-pair"), boundary.snapshotResponse.Snapshot.Aggregate())
	assert.Equal(t, []string{"state-pair", "first-event", "second-event"}, serializer.restored)
	assert.Equal(t, []boundaryCall{
		{name: "GetLatestSnapshotByID", ctx: ctx, id: id, aid: "Order-item-1"},
		{name: "GetEventsByIDSinceSeqNr", ctx: ctx, id: id, aid: "Order-item-1", since: 7},
	}, boundary.calls)
}

func TestOperationEntrySnapshotAbsenceRemainsDistinct(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *eventstore.SnapshotRead[[]byte]
	}{
		{"no head", nil},
		{"head without snapshot", &eventstore.SnapshotRead[[]byte]{HeadSeqNr: 12}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshots := &recordingSerializer[domainState]{base: eventstore.NewJSONSerializer[domainState]()}
			boundary := &observedBoundary{snapshotResponse: tc.response}
			store, err := eventstore.NewOperationEntry(eventstore.NewJSONSerializer[domainEvent](), snapshots, boundary.open)
			require.NoError(t, err)
			id, err := eventstore.NewAggregateID("Order", "1")
			require.NoError(t, err)

			result, err := store.GetLatestSnapshotByID(context.Background(), id)

			require.NoError(t, err)
			if tc.response == nil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Nil(t, result.Snapshot)
				assert.Equal(t, eventstore.SeqNr(12), result.HeadSeqNr)
			}
			assert.Empty(t, snapshots.restored)
			require.Len(t, boundary.calls, 1)
			assert.Equal(t, "GetLatestSnapshotByID", boundary.calls[0].name)
		})
	}
}

func TestOperationEntryEmptyEventsDoNotDeserialize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response []eventstore.EventEnvelope[[]byte]
	}{
		{"nil", nil},
		{"empty", []eventstore.EventEnvelope[[]byte]{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &recordingSerializer[domainEvent]{base: eventstore.NewJSONSerializer[domainEvent]()}
			boundary := &observedBoundary{eventsResponse: tc.response}
			store, err := eventstore.NewOperationEntry(events, eventstore.NewJSONSerializer[domainState](), boundary.open)
			require.NoError(t, err)
			id, err := eventstore.NewAggregateID("Order", "1")
			require.NoError(t, err)

			result, err := store.GetEventsByIDSinceSeqNr(context.Background(), id, 0)

			require.NoError(t, err)
			if tc.response == nil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Empty(t, result)
			}
			assert.Empty(t, events.restored)
			require.Len(t, boundary.calls, 1)
			assert.Equal(t, "GetEventsByIDSinceSeqNr", boundary.calls[0].name)
		})
	}
}

func TestOperationEntryExplicitNullPayloadsReachBoundaryAndRestore(t *testing.T) {
	boundary := &observedBoundary{}
	store, err := eventstore.NewOperationEntry(
		eventstore.NewJSONSerializer[*domainEvent](), eventstore.NewJSONSerializer[*domainState](), boundary.open,
		eventstore.WithRetentionCount(eventstore.NoRetention()),
	)
	require.NoError(t, err)
	assert.Nil(t, boundary.settings.RetentionCount)
	id, err := eventstore.NewAggregateID("Order", "1")
	require.NoError(t, err)
	event, err := eventstore.NewEventEnvelope[*domainEvent](id, 1, time.Unix(0, 0), nil)
	require.NoError(t, err)
	snapshot, err := eventstore.NewSnapshotEnvelope[*domainState](nil, 1)
	require.NoError(t, err)

	require.NoError(t, store.PersistEvent(context.Background(), event))
	require.NoError(t, store.PersistEventAndSnapshot(context.Background(), event, snapshot))

	require.Len(t, boundary.eventWrites, 2)
	require.Len(t, boundary.snapshotWrites, 1)
	for _, stored := range boundary.eventWrites {
		assert.Equal(t, []byte("null"), stored.Payload())
		require.NoError(t, stored.Validate())
	}
	storedSnapshot := boundary.snapshotWrites[0]
	assert.Equal(t, []byte("null"), storedSnapshot.Aggregate())
	require.NoError(t, storedSnapshot.Validate())
	boundary.eventsResponse = boundary.eventWrites[:1]
	boundary.snapshotResponse = &eventstore.SnapshotRead[[]byte]{Snapshot: &storedSnapshot, HeadSeqNr: 1}
	readEvents, err := store.GetEventsByIDSinceSeqNr(context.Background(), id, 0)
	require.NoError(t, err)
	require.Len(t, readEvents, 1)
	assert.Nil(t, readEvents[0].Payload())
	readSnapshot, err := store.GetLatestSnapshotByID(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, readSnapshot)
	require.NotNil(t, readSnapshot.Snapshot)
	require.NoError(t, readSnapshot.Snapshot.Validate())
	assert.Nil(t, readSnapshot.Snapshot.Aggregate())
}

func TestNewOperationEntryPassesRealOptionFailuresThroughConsumer(t *testing.T) {
	cause := errors.New("option failed")
	for _, tc := range []struct {
		name   string
		option eventstore.Option
	}{
		{"unconstructed retention count", eventstore.WithRetentionCount(eventstore.RetentionCount{})},
		{"negative grace", eventstore.WithTTLGraceSeconds(-1)},
		{"nil option", nil},
		{"option cause", func(*storeoptions.Options) error { return &eventstore.ConfigurationError{Cause: cause} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boundary := &observedBoundary{}
			laterCalls := 0

			store, err := eventstore.NewOperationEntry(
				eventstore.NewJSONSerializer[domainEvent](), eventstore.NewJSONSerializer[domainState](), boundary.open,
				tc.option, func(*storeoptions.Options) error { laterCalls++; return nil },
			)

			assert.Nil(t, store)
			kind, ok := eventstore.KindOf(err)
			require.True(t, ok)
			assert.Equal(t, eventstore.KindConfiguration, kind)
			var failure *eventstore.ConfigurationError
			require.ErrorAs(t, err, &failure)
			require.NotNil(t, failure.Unwrap())
			if tc.name == "option cause" {
				assert.ErrorIs(t, err, cause)
				assert.ErrorIs(t, failure.Unwrap(), cause)
			}
			assert.Equal(t, 1, boundary.openCalls)
			assert.Empty(t, boundary.calls)
			assert.Zero(t, laterCalls)
		})
	}
}
