package eventstore

import "time"

// EventEnvelope carries one event with validated metadata and an arbitrary payload.
// Its zero value is invalid; use NewEventEnvelope to supply the required values.
type EventEnvelope[E any] struct {
	aggregateID string
	seqNr       SeqNr
	occurredAt  time.Time
	manifest    string
	payload     E
}

// NewEventEnvelope validates the ID, event sequence number and occurrence time.
// An explicitly supplied nil or zero payload is a value, not a missing argument.
func NewEventEnvelope[E any](id AggregateID, seqNr SeqNr, occurredAt time.Time, payload E, opts ...EnvelopeOption) (EventEnvelope[E], error) {
	aid, err := AidString(id)
	if err != nil {
		if violation, ok := err.(*ContractViolationError); ok {
			violation.SeqNr = &seqNr
		}
		return EventEnvelope[E]{}, err
	}
	e := EventEnvelope[E]{aggregateID: aid, seqNr: seqNr, occurredAt: occurredAt, payload: payload}
	for _, opt := range opts {
		e.manifest = opt.manifest
	}
	if err := e.Validate(); err != nil {
		return EventEnvelope[E]{}, err
	}
	return e, nil
}

// Validate checks required metadata and the event's number and time boundaries.
// It can be reused at operation entrances to reject an unconstructed envelope.
func (e EventEnvelope[E]) Validate() error {
	if e.aggregateID == "" {
		return &ContractViolationError{Rule: "T-2"}
	}
	if err := e.seqNr.ValidateAsEventSeqNr(); err != nil {
		return err
	}
	return validateOccurredAt(e.occurredAt, e.seqNr)
}

func (e EventEnvelope[E]) AggregateID() string   { return e.aggregateID }
func (e EventEnvelope[E]) SeqNr() SeqNr          { return e.seqNr }
func (e EventEnvelope[E]) OccurredAt() time.Time { return e.occurredAt }
func (e EventEnvelope[E]) Manifest() string      { return e.manifest }
func (e EventEnvelope[E]) Payload() E            { return e.payload }
