package eventstore

// SnapshotEnvelope carries aggregate state and the number reflected in that state.
// It does not contain a head number. Its zero value is invalid.
type SnapshotEnvelope[A any] struct {
	aggregate   A
	seqNr       SeqNr
	manifest    string
	constructed bool // Null aggregate and sequence zero are both valid explicit values.
}

// NewSnapshotEnvelope validates the general sequence-number range, including zero.
// An explicitly supplied nil or zero aggregate is a value, not a missing argument.
func NewSnapshotEnvelope[A any](aggregate A, seqNr SeqNr, opts ...EnvelopeOption) (SnapshotEnvelope[A], error) {
	s := SnapshotEnvelope[A]{aggregate: aggregate, seqNr: seqNr, constructed: true}
	for _, opt := range opts {
		s.manifest = opt.manifest
	}
	if err := s.Validate(); err != nil {
		return SnapshotEnvelope[A]{}, err
	}
	return s, nil
}

// Validate rejects an unconstructed snapshot and checks its general number range.
// Matching the event or head number is the responsibility of operation entrances.
func (s SnapshotEnvelope[A]) Validate() error {
	if !s.constructed {
		return &ContractViolationError{Rule: "T-10"}
	}
	return s.seqNr.Validate()
}

func (s SnapshotEnvelope[A]) Aggregate() A     { return s.aggregate }
func (s SnapshotEnvelope[A]) SeqNr() SeqNr     { return s.seqNr }
func (s SnapshotEnvelope[A]) Manifest() string { return s.manifest }
