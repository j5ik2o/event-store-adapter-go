package eventstore

import "time"

var (
	minOccurredAt = time.Date(1677, time.September, 21, 0, 12, 43, 145224192, time.UTC)
	maxOccurredAt = time.Date(2262, time.April, 11, 23, 47, 16, 854775807, time.UTC)
)

// validateOccurredAt checks T-13 before callers can convert the time with UnixNano.
// time.Time can represent instants outside the signed 64-bit epoch-nanosecond range.
func validateOccurredAt(at time.Time, seqNr SeqNr) error {
	if at.Before(minOccurredAt) || at.After(maxOccurredAt) {
		return &ContractViolationError{Rule: "T-13", SeqNr: &seqNr}
	}
	return nil
}
