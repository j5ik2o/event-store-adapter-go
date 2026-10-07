package eventstore

import (
	"errors"
	"fmt"
)

// Kind is one of the five error categories defined by E-1.
type Kind int

const (
	KindOptimisticLock Kind = iota + 1
	KindContractViolation
	KindSerialization
	KindConfiguration
	KindStorage
)

// Error is a classified error that preserves its original cause.
type Error interface {
	error
	Kind() Kind
	Unwrap() error
}

// OptimisticLockError reports a conflicting append. HeadSeqNr is nil when unknown.
// Its message contains only the E-2 diagnostic values, never the cause's text.
type OptimisticLockError struct {
	AggregateID string
	SeqNr       SeqNr
	HeadSeqNr   *SeqNr
	Cause       error
}

func (e *OptimisticLockError) Error() string {
	msg := fmt.Sprintf("optimistic lock: aid=%q seq_nr=%d", e.AggregateID, e.SeqNr)
	if e.HeadSeqNr != nil {
		msg += fmt.Sprintf(" head_seq_nr=%d", *e.HeadSeqNr)
	}
	return msg
}

func (e *OptimisticLockError) Kind() Kind    { return KindOptimisticLock }
func (e *OptimisticLockError) Unwrap() error { return e.Cause }

// ContractViolationError reports a violated rule and any related sequence numbers.
// Nil sequence numbers are absent; a pointer to zero is a present diagnostic value.
// SnapshotSeqNr carries the differing snapshot number for W-9.
type ContractViolationError struct {
	Rule          string
	SeqNr         *SeqNr
	SnapshotSeqNr *SeqNr
	Cause         error
}

func (e *ContractViolationError) Error() string {
	msg := "contract violation: " + e.Rule
	if e.SeqNr != nil {
		msg += fmt.Sprintf(" seq_nr=%d", *e.SeqNr)
	}
	if e.SnapshotSeqNr != nil {
		msg += fmt.Sprintf(" snapshot_seq_nr=%d", *e.SnapshotSeqNr)
	}
	return msg
}

func (e *ContractViolationError) Kind() Kind    { return KindContractViolation }
func (e *ContractViolationError) Unwrap() error { return e.Cause }

// SerializationError reports payload serialization or deserialization failure.
// The cause is available through Unwrap, without exposing payload data in the message.
type SerializationError struct {
	Cause error
}

func (e *SerializationError) Error() string { return "serialization error" }
func (e *SerializationError) Kind() Kind    { return KindSerialization }
func (e *SerializationError) Unwrap() error { return e.Cause }

// ConfigurationError reports invalid or mismatched configuration.
// The cause is available through Unwrap, without exposing credentials in the message.
type ConfigurationError struct {
	Cause error
}

func (e *ConfigurationError) Error() string { return "configuration error" }
func (e *ConfigurationError) Kind() Kind    { return KindConfiguration }
func (e *ConfigurationError) Unwrap() error { return e.Cause }

// StorageError reports storage failure or missing stored data.
// The cause is available through Unwrap, without exposing raw storage errors in the message.
type StorageError struct {
	Cause error
}

func (e *StorageError) Error() string { return "storage error" }
func (e *StorageError) Kind() Kind    { return KindStorage }
func (e *StorageError) Unwrap() error { return e.Cause }

// KindOf finds the first classified Error, including through wrapping.
// Nil and errors without one of the five categories return (0, false).
func KindOf(err error) (Kind, bool) {
	var classified Error
	if !errors.As(err, &classified) {
		return 0, false
	}
	kind := classified.Kind()
	switch kind {
	case KindOptimisticLock, KindContractViolation, KindSerialization, KindConfiguration, KindStorage:
		return kind, true
	default:
		return 0, false
	}
}
