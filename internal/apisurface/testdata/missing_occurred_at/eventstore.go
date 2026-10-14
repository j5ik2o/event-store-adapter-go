// Package eventstore is a minimal stand-in for the root package used to test
// the API surface check. Each sibling directory deviates from it in one place.
package eventstore

import (
	"context"
	"time"
)

type AggregateID interface{ String() string }

type SeqNr int64

type EnvelopeOption struct{ manifest string }

func WithManifest(manifest string) EnvelopeOption { return EnvelopeOption{manifest: manifest} }

type EventEnvelope[E any] struct {
	aggregateID string
	seqNr       SeqNr
	occurredAt  time.Time
	manifest    string
	payload     E
}

func NewEventEnvelope[E any](id AggregateID, seqNr SeqNr, occurredAt time.Time, payload E, opts ...EnvelopeOption) (EventEnvelope[E], error) {
	return EventEnvelope[E]{}, nil
}

func (e EventEnvelope[E]) AggregateID() string { return e.aggregateID }
func (e EventEnvelope[E]) SeqNr() SeqNr        { return e.seqNr }
func (e EventEnvelope[E]) Manifest() string    { return e.manifest }
func (e EventEnvelope[E]) Payload() E          { return e.payload }

type SnapshotEnvelope[A any] struct {
	aggregate A
	seqNr     SeqNr
	manifest  string
}

func NewSnapshotEnvelope[A any](aggregate A, seqNr SeqNr, opts ...EnvelopeOption) (SnapshotEnvelope[A], error) {
	return SnapshotEnvelope[A]{}, nil
}

func (s SnapshotEnvelope[A]) Aggregate() A     { return s.aggregate }
func (s SnapshotEnvelope[A]) SeqNr() SeqNr     { return s.seqNr }
func (s SnapshotEnvelope[A]) Manifest() string { return s.manifest }

type SnapshotRead[A any] struct {
	Snapshot  *SnapshotEnvelope[A]
	HeadSeqNr SeqNr
}

type EventStore[E, A any] interface {
	PersistEvent(ctx context.Context, event EventEnvelope[E]) error
	PersistEventAndSnapshot(ctx context.Context, event EventEnvelope[E], snapshot SnapshotEnvelope[A]) error
	GetLatestSnapshotByID(ctx context.Context, id AggregateID) (*SnapshotRead[A], error)
	GetEventsByIDSinceSeqNr(ctx context.Context, id AggregateID, seqNr SeqNr) ([]EventEnvelope[E], error)
}

type Kind int

type Error interface {
	error
	Kind() Kind
	Unwrap() error
}

type baseError struct{ err error }

func (e *baseError) Error() string { return "error" }
func (e *baseError) Kind() Kind    { return 0 }
func (e *baseError) Unwrap() error { return e.err }

type OptimisticLockError struct{ baseError }
type ContractViolationError struct{ baseError }
type SerializationError struct{ baseError }
type ConfigurationError struct{ baseError }
type StorageError struct{ baseError }
