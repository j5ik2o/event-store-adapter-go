package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

type eventSerializer[T any] struct {
	serialize func(T) ([]byte, error)
}

func requireEventKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok)
	require.Equal(t, expected, kind)
	var classified eventstore.Error
	require.ErrorAs(t, err, &classified)
}

func (s eventSerializer[T]) Serialize(value T) ([]byte, error) { return s.serialize(value) }
func (eventSerializer[T]) Deserialize([]byte) (value T, err error) {
	return value, errors.New("deserialization is not used in write tests")
}

func eventEnvelope[T any](t *testing.T, typeName, value string, seqNr eventstore.SeqNr, at time.Time, payload T, manifest string) eventstore.EventEnvelope[T] {
	t.Helper()
	id, err := eventstore.NewAggregateID(typeName, value)
	require.NoError(t, err)
	event, err := eventstore.NewEventEnvelope(id, seqNr, at, payload, eventstore.WithManifest(manifest))
	require.NoError(t, err)
	return event
}

func TestPersistEventPreparationDoesNotSend(t *testing.T) {
	r := dynamodbtest.NewRecorder()
	store := &opened{client: configurationUnitClient(r), settings: configurationSettings(t)}
	calls := 0
	cause := errors.New("serialization failed")
	serializer := eventSerializer[string]{serialize: func(string) ([]byte, error) { calls++; return nil, cause }}
	ctx := dynamodbtest.WithOperation(context.Background(), 1)
	err := persistEvent(ctx, store, serializer, eventstore.EventEnvelope[string]{})
	requireEventKind(t, err, eventstore.KindContractViolation)
	require.Zero(t, calls)
	err = persistEvent(ctx, store, serializer, eventEnvelope(t, "Order", "1", 1, time.Unix(0, 123), "payload", ""))
	requireEventKind(t, err, eventstore.KindSerialization)
	require.Equal(t, cause, errors.Unwrap(err))
	require.Equal(t, 1, calls)
	require.Empty(t, r.Requests(1))
}

func TestPersistEventItemSizeUpperBounds(t *testing.T) {
	// These sizes follow design 4.5 independently of the estimator.
	for _, seqNr := range []eventstore.SeqNr{1, 2, eventstore.MaxSeqNr} {
		event := eventEnvelope(t, "型", "値", seqNr, time.Unix(0, -1), []byte{0, 1, 255}, "文")
		journal, head := eventItems(event)
		journalSize, err := itemSizeUpperBound(journal)
		require.NoError(t, err)
		// Names: 3+6+11+8+7; values: aid(7)+N(21)+N(21)+S(3)+B(3).
		require.Equal(t, 90, journalSize)
		headSize, err := itemSizeUpperBound(head)
		require.NoError(t, err)
		// Top names 24, aid 7, type 3, N 21, L(3+1), M(3+4+80).
		require.Equal(t, 146, headSize)
	}
}

func TestPersistEventOversizedItemsDoNotSend(t *testing.T) {
	for _, tc := range []struct {
		name, typeName, manifest string
		payloadBytes             int
	}{
		{"journal payload", "Order", "", maxItemBytes},
		{"journal manifest", "Order", strings.Repeat("文", maxItemBytes/3+1), 0},
		{"head only", strings.Repeat("x", 1000), "", 408000},
	} {
		for _, seqNr := range []eventstore.SeqNr{1, 2} {
			t.Run(fmt.Sprintf("%s/seq=%d", tc.name, seqNr), func(t *testing.T) {
				r := dynamodbtest.NewRecorder()
				store := &opened{client: configurationUnitClient(r), settings: configurationSettings(t)}
				serializer := eventSerializer[string]{serialize: func(string) ([]byte, error) { return make([]byte, tc.payloadBytes), nil }}
				event := eventEnvelope(t, tc.typeName, "v", seqNr, time.Unix(0, 123), "payload", tc.manifest)
				err := persistEvent(dynamodbtest.WithOperation(t.Context(), 1), store, serializer, event)
				requireEventKind(t, err, eventstore.KindContractViolation)
				require.Empty(t, r.Requests(1))
				if tc.name == "head only" {
					prepared, err := eventstore.PrepareEvent(serializer, event)
					require.NoError(t, err)
					journal, head := eventItems(prepared)
					j, err := itemSizeUpperBound(journal)
					require.NoError(t, err)
					h, err := itemSizeUpperBound(head)
					require.NoError(t, err)
					require.LessOrEqual(t, j, maxItemBytes)
					require.Greater(t, h, maxItemBytes)
				}
			})
		}
	}
}

func TestPersistEventCancellationClassification(t *testing.T) {
	headItem := func(n string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"seq_nr": &types.AttributeValueMemberN{Value: n}}
	}
	reason := func(code string, item map[string]types.AttributeValue) types.CancellationReason {
		return types.CancellationReason{Code: aws.String(code), Item: item}
	}
	none := reason("None", nil)
	for _, tc := range []struct {
		name    string
		seqNr   eventstore.SeqNr
		reasons []types.CancellationReason
		kind    eventstore.Kind
	}{
		{"new duplicate", 1, []types.CancellationReason{none, reason("ConditionalCheckFailed", headItem("1"))}, eventstore.KindOptimisticLock},
		{"update duplicate", 2, []types.CancellationReason{none, reason("ConditionalCheckFailed", headItem("2"))}, eventstore.KindOptimisticLock},
		{"stale update", 2, []types.CancellationReason{none, reason("ConditionalCheckFailed", headItem("3"))}, eventstore.KindOptimisticLock},
		{"gap", 4, []types.CancellationReason{none, reason("ConditionalCheckFailed", headItem("2"))}, eventstore.KindContractViolation},
		{"missing head", 2, []types.CancellationReason{none, reason("ConditionalCheckFailed", nil)}, eventstore.KindContractViolation},
		{"head before journal", 4, []types.CancellationReason{reason("ConditionalCheckFailed", nil), reason("ConditionalCheckFailed", headItem("2"))}, eventstore.KindContractViolation},
		{"journal only", 2, []types.CancellationReason{reason("ConditionalCheckFailed", nil), none}, eventstore.KindOptimisticLock},
		{"conflict journal before gap", 4, []types.CancellationReason{reason("TransactionConflict", nil), reason("ConditionalCheckFailed", headItem("2"))}, eventstore.KindOptimisticLock},
		{"conflict head", 2, []types.CancellationReason{reason("ConditionalCheckFailed", nil), reason("TransactionConflict", nil)}, eventstore.KindOptimisticLock},
		{"throttling journal", 2, []types.CancellationReason{reason("ProvisionedThroughputExceeded", nil), none}, eventstore.KindStorage},
		{"throttling head", 2, []types.CancellationReason{none, reason("ThrottlingError", nil)}, eventstore.KindStorage},
		{"no reasons", 2, nil, eventstore.KindStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			canceled := &types.TransactionCanceledException{Message: aws.String("raw SDK message"), CancellationReasons: tc.reasons}
			cause := fmt.Errorf("SDK operation: %w", canceled)
			err := classifyWriteError(cause, "Order-1", tc.seqNr)
			requireEventKind(t, err, tc.kind)
			require.Equal(t, cause, errors.Unwrap(err))
			require.ErrorIs(t, err, canceled)
			if tc.kind == eventstore.KindOptimisticLock {
				require.NotContains(t, err.Error(), "raw SDK message")
			}
			if tc.kind == eventstore.KindContractViolation {
				var violation *eventstore.ContractViolationError
				require.ErrorAs(t, err, &violation)
				require.Equal(t, "W-8", violation.Rule)
				require.Equal(t, tc.seqNr, *violation.SeqNr)
			}
		})
	}
	for _, cause := range []error{errors.New("transport"), &types.ProvisionedThroughputExceededException{}, &types.TransactionConflictException{}} {
		err := classifyWriteError(cause, "Order-1", 2)
		requireEventKind(t, err, eventstore.KindStorage)
		require.Equal(t, cause, errors.Unwrap(err))
	}
}
