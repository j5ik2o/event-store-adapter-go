package eventstore

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sensitiveCause struct {
	calls int
}

func (e *sensitiveCause) Error() string {
	e.calls++
	return "raw SDK error payload=private-payload password=private-password endpoint=private-endpoint"
}

func TestErrorsClassificationCauseAndSafeMessages(t *testing.T) {
	seq, head := SeqNr(7), SeqNr(6)
	cause := &sensitiveCause{}
	cases := []struct {
		name   string
		err    Error
		kind   Kind
		target any
	}{
		{"optimistic lock", &OptimisticLockError{AggregateID: "Order-1", SeqNr: seq, HeadSeqNr: &head, Cause: cause}, KindOptimisticLock, new(*OptimisticLockError)},
		{"contract violation", &ContractViolationError{Rule: "W-9", SeqNr: &seq, Cause: cause}, KindContractViolation, new(*ContractViolationError)},
		{"serialization", &SerializationError{Cause: cause}, KindSerialization, new(*SerializationError)},
		{"configuration", &ConfigurationError{Cause: cause}, KindConfiguration, new(*ConfigurationError)},
		{"storage", &StorageError{Cause: cause}, KindStorage, new(*StorageError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{tc.err, fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", tc.err))} {
				kind, ok := KindOf(err)
				require.True(t, ok)
				assert.Equal(t, tc.kind, kind)
				require.True(t, errors.As(err, tc.target))
				assert.Same(t, tc.err, reflect.ValueOf(tc.target).Elem().Interface())
				var common Error
				require.True(t, errors.As(err, &common))
				assert.Equal(t, tc.kind, common.Kind())
				assert.Same(t, cause, common.Unwrap())
				assert.ErrorIs(t, err, cause)
				for _, secret := range []string{"raw SDK error", "private-payload", "private-password", "private-endpoint"} {
					assert.NotContains(t, err.Error(), secret)
				}
			}
			assert.Same(t, cause, errors.Unwrap(tc.err))
			assert.Zero(t, cause.calls, "generating messages must not call the cause's Error method")
		})
	}
}

func TestErrorsWithoutCause(t *testing.T) {
	for _, err := range []Error{
		&OptimisticLockError{}, &ContractViolationError{}, &SerializationError{}, &ConfigurationError{}, &StorageError{},
	} {
		assert.Nil(t, err.Unwrap())
		assert.NotEmpty(t, err.Error())
	}
}

type unknownKindError struct{}

func (unknownKindError) Error() string { return "storage" }
func (unknownKindError) Kind() Kind    { return 99 }
func (unknownKindError) Unwrap() error { return nil }

func TestKindOfUnclassified(t *testing.T) {
	for _, err := range []error{nil, errors.New("storage"), fmt.Errorf("wrap: %w", errors.New("contract-violation")), unknownKindError{}} {
		kind, ok := KindOf(err)
		assert.False(t, ok)
		assert.Zero(t, kind)
	}
}

func TestKindOfOutermostCategory(t *testing.T) {
	err := &StorageError{Cause: &SerializationError{}}
	kind, ok := KindOf(err)
	require.True(t, ok)
	assert.Equal(t, KindStorage, kind)
}

func TestOptimisticLockDiagnostics(t *testing.T) {
	zero := SeqNr(0)
	err := &OptimisticLockError{AggregateID: "Order-item-1", SeqNr: 7}
	assert.Contains(t, err.Error(), `aid="Order-item-1"`)
	assert.Contains(t, err.Error(), "seq_nr=7")
	assert.NotContains(t, err.Error(), "head_seq_nr")
	err.HeadSeqNr = &zero
	assert.Contains(t, err.Error(), "head_seq_nr=0")
	head := SeqNr(42)
	err.HeadSeqNr = &head
	assert.Contains(t, err.Error(), "head_seq_nr=42")
}

func TestContractViolationDiagnostics(t *testing.T) {
	err := &ContractViolationError{Rule: "T-11"}
	assert.Contains(t, err.Error(), "T-11")
	assert.NotContains(t, err.Error(), "seq_nr")
	for _, n := range []SeqNr{0, -1, MaxSeqNr + 1} {
		err := &ContractViolationError{Rule: "T-9", SeqNr: &n}
		assert.Contains(t, err.Error(), "T-9")
		assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", n))
	}
	event, snapshot := SeqNr(7), SeqNr(9)
	err = &ContractViolationError{Rule: "W-9", SeqNr: &event, SnapshotSeqNr: &snapshot}
	assert.Contains(t, err.Error(), "W-9")
	assert.Contains(t, err.Error(), "seq_nr=7")
	assert.Contains(t, err.Error(), "snapshot_seq_nr=9")
}

func requireContractViolation(t *testing.T, err error, rule string) *ContractViolationError {
	t.Helper()
	require.Error(t, err)
	var violation *ContractViolationError
	require.ErrorAs(t, err, &violation)
	assert.Equal(t, rule, violation.Rule)
	assert.Contains(t, err.Error(), rule)
	kind, ok := KindOf(err)
	require.True(t, ok)
	assert.Equal(t, KindContractViolation, kind)
	return violation
}
