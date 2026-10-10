package eventstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
)

// NewOperationEntry connects payload serializers to a byte-oriented storage boundary.
// open receives the public Options and owns their application and storage-specific checks.
// The entry validates write inputs before serialization and calls the boundary only after
// all payloads are serialized. Head checks, commit, locking and retention belong to the boundary.
func NewOperationEntry[E, A any](
	eventSerializer Serializer[E],
	snapshotSerializer Serializer[A],
	open func(...Option) (EventStore[[]byte, []byte], error),
	opts ...Option,
) (EventStore[E, A], error) {
	if nilDependency(eventSerializer) {
		return nil, newConfigurationError(errors.New("event serializer is nil"))
	}
	if nilDependency(snapshotSerializer) {
		return nil, newConfigurationError(errors.New("snapshot serializer is nil"))
	}
	if open == nil {
		return nil, newConfigurationError(errors.New("storage boundary constructor is nil"))
	}
	boundary, err := open(opts...)
	if err != nil {
		return nil, classifyStorageError(err)
	}
	if nilDependency(boundary) {
		return nil, newConfigurationError(errors.New("storage boundary is nil"))
	}
	return &operationEntry[E, A]{
		eventSerializer:    eventSerializer,
		snapshotSerializer: snapshotSerializer,
		boundary:           boundary,
	}, nil
}

type operationEntry[E, A any] struct {
	eventSerializer    Serializer[E]
	snapshotSerializer Serializer[A]
	boundary           EventStore[[]byte, []byte]
}

func (s *operationEntry[E, A]) PersistEvent(ctx context.Context, event EventEnvelope[E]) error {
	stored, err := PrepareEvent(s.eventSerializer, event)
	if err != nil {
		return err
	}
	return classifyStorageError(s.boundary.PersistEvent(ctx, stored))
}

func (s *operationEntry[E, A]) PersistEventAndSnapshot(ctx context.Context, event EventEnvelope[E], snapshot SnapshotEnvelope[A]) error {
	storedEvent, storedSnapshot, err := PrepareEventAndSnapshot(s.eventSerializer, s.snapshotSerializer, event, snapshot)
	if err != nil {
		return err
	}
	return classifyStorageError(s.boundary.PersistEventAndSnapshot(ctx, storedEvent, storedSnapshot))
}

// PrepareEventAndSnapshot validates both envelopes and W-9 before serialization.
// Each payload is copied before the next serializer can reuse its returned buffer.
// The prepared envelopes retain the input metadata and own their bytes.
func PrepareEventAndSnapshot[E, A any](eventSerializer Serializer[E], snapshotSerializer Serializer[A], event EventEnvelope[E], snapshot SnapshotEnvelope[A]) (EventEnvelope[[]byte], SnapshotEnvelope[[]byte], error) {
	if err := event.Validate(); err != nil {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, err
	}
	if event.seqNr != snapshot.seqNr {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, &ContractViolationError{Rule: "W-9", SeqNr: &event.seqNr, SnapshotSeqNr: &snapshot.seqNr}
	}
	if nilDependency(snapshotSerializer) {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, newConfigurationError(errors.New("snapshot serializer is nil"))
	}
	storedEvent, err := PrepareEvent(eventSerializer, event)
	if err != nil {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, err
	}
	data, err := snapshotSerializer.Serialize(snapshot.aggregate)
	if err != nil {
		return EventEnvelope[[]byte]{}, SnapshotEnvelope[[]byte]{}, classifySerializationError(err)
	}
	data = bytes.Clone(data)
	storedSnapshot := SnapshotEnvelope[[]byte]{
		aggregate:   data,
		seqNr:       snapshot.seqNr,
		manifest:    snapshot.manifest,
		constructed: snapshot.constructed,
	}
	return storedEvent, storedSnapshot, nil
}

func (s *operationEntry[E, A]) GetLatestSnapshotByID(ctx context.Context, id AggregateID) (*SnapshotRead[A], error) {
	aid, err := AidString(id)
	if err != nil {
		return nil, err
	}
	typeName, value, _ := strings.Cut(aid, "-")
	validatedID := aggregateID{typeName: typeName, value: value}
	stored, err := s.boundary.GetLatestSnapshotByID(ctx, validatedID)
	if err != nil {
		return nil, classifyStorageError(err)
	}
	if stored == nil {
		return nil, nil
	}
	result := &SnapshotRead[A]{HeadSeqNr: stored.HeadSeqNr}
	if stored.Snapshot != nil {
		aggregate, err := s.snapshotSerializer.Deserialize(bytes.Clone(stored.Snapshot.aggregate))
		if err != nil {
			return nil, classifySerializationError(err)
		}
		result.Snapshot = &SnapshotEnvelope[A]{
			aggregate:   aggregate,
			seqNr:       stored.Snapshot.seqNr,
			manifest:    stored.Snapshot.manifest,
			constructed: stored.Snapshot.constructed,
		}
	}
	return result, nil
}

func (s *operationEntry[E, A]) GetEventsByIDSinceSeqNr(ctx context.Context, id AggregateID, seqNr SeqNr) ([]EventEnvelope[E], error) {
	aid, err := AidString(id)
	if err != nil {
		if violation, ok := err.(*ContractViolationError); ok {
			violation.SeqNr = &seqNr
		}
		return nil, err
	}
	if err := seqNr.Validate(); err != nil {
		return nil, err
	}
	typeName, value, _ := strings.Cut(aid, "-")
	validatedID := aggregateID{typeName: typeName, value: value}
	stored, err := s.boundary.GetEventsByIDSinceSeqNr(ctx, validatedID, seqNr)
	if err != nil {
		return nil, classifyStorageError(err)
	}
	if stored == nil {
		return nil, nil
	}
	result := make([]EventEnvelope[E], len(stored))
	for i, event := range stored {
		payload, err := s.eventSerializer.Deserialize(bytes.Clone(event.payload))
		if err != nil {
			return nil, classifySerializationError(err)
		}
		result[i] = EventEnvelope[E]{
			aggregateID: event.aggregateID,
			seqNr:       event.seqNr,
			occurredAt:  event.occurredAt,
			manifest:    event.manifest,
			payload:     payload,
		}
	}
	return result, nil
}

// PrepareEvent validates an event and serializes only its payload for a storage
// boundary. It preserves the envelope's fixed metadata and owns the returned bytes.
// Paired writes must validate both envelopes and W-9 before calling this function.
func PrepareEvent[E any](serializer Serializer[E], event EventEnvelope[E]) (EventEnvelope[[]byte], error) {
	if err := event.Validate(); err != nil {
		return EventEnvelope[[]byte]{}, err
	}
	if nilDependency(serializer) {
		return EventEnvelope[[]byte]{}, newConfigurationError(errors.New("event serializer is nil"))
	}
	data, err := serializer.Serialize(event.payload)
	if err != nil {
		return EventEnvelope[[]byte]{}, classifySerializationError(err)
	}
	data = bytes.Clone(data)
	return EventEnvelope[[]byte]{
		aggregateID: event.aggregateID,
		seqNr:       event.seqNr,
		occurredAt:  event.occurredAt,
		manifest:    event.manifest,
		payload:     data,
	}, nil
}

func classifySerializationError(err error) error {
	if kind, ok := KindOf(err); ok && kind == KindSerialization {
		return err
	}
	return &SerializationError{Cause: err}
}

func classifyStorageError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := KindOf(err); ok {
		return err
	}
	return &StorageError{Cause: err}
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
