package eventstore

// SeqNr is an aggregate sequence number in the T-9 range, 0 through 2^53-1.
type SeqNr int64

const MaxSeqNr SeqNr = 1<<53 - 1

// Validate checks the general T-9 range. Zero is valid outside the event context.
func (n SeqNr) Validate() error {
	if n < 0 || n > MaxSeqNr {
		return &ContractViolationError{Rule: "T-9", SeqNr: &n}
	}
	return nil
}

// ValidateAsEventSeqNr checks T-9 and rejects an event's zero number under W-6.
func (n SeqNr) ValidateAsEventSeqNr() error {
	if err := n.Validate(); err != nil {
		return err
	}
	if n == 0 {
		return &ContractViolationError{Rule: "W-6", SeqNr: &n}
	}
	return nil
}
