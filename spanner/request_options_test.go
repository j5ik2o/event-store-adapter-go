package spanner_test

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file prove the transaction options by the requests that really reach the emulator, which is
// the strongest observation the emulator allows for SPN-1, SPN-6, SPN-9, SPN-10 and SPN-11.

func requireSerializableReadWrite(t *testing.T, options []*sppb.TransactionOptions, count int) {
	t.Helper()
	require.Len(t, options, count)
	for _, option := range options {
		require.NotNil(t, option.GetReadWrite(), "an append runs in a read-write transaction: %v", option)
		assert.Equal(t, sppb.TransactionOptions_SERIALIZABLE, option.GetIsolationLevel(), "the isolation level is requested explicitly")
	}
}

func requireStrongReadOnly(t *testing.T, options []*sppb.TransactionOptions, count int) {
	t.Helper()
	require.Len(t, options, count)
	for _, option := range options {
		require.NotNil(t, option.GetReadOnly(), "a read runs in a read-only transaction: %v", option)
		assert.True(t, option.GetReadOnly().GetStrong(), "the read bound is strong, never a stale timestamp")
		assert.Nil(t, option.GetReadWrite())
	}
}

func TestTransactionOptionsAppendIsASerializableReadWriteTransaction(t *testing.T) {
	l := provision(t, layoutSpec{})
	client, recorder := newRecordingClient(t)
	entry := l.store(t, client)
	recorder.reset()

	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Order", "1"), 1, "one")))

	requireSerializableReadWrite(t, recorder.beginOptions(), 1)
	assert.Contains(t, recorder.methods(), "Commit")
}

func TestTransactionOptionsRetentionIsAnotherSerializableReadWriteTransaction(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	client, recorder := newRecordingClient(t)
	entry := l.store(t, client)
	id := aggregate(t, "Order", "2")
	persistSnapshots(t, entry, id, 1, 1)
	recorder.reset()

	persistSnapshots(t, entry, id, 2, 2)

	requireSerializableReadWrite(t, recorder.beginOptions(), 2) // the append, then the retention transaction
}

func TestTransactionOptionsReadsUseOneStrongReadOnlyTransactionEach(t *testing.T) {
	l := provision(t, layoutSpec{})
	client, recorder := newRecordingClient(t)
	entry := l.store(t, client)
	id := aggregate(t, "Order", "3")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))

	for name, read := range map[string]func() error{
		"latest snapshot": func() error { _, err := entry.GetLatestSnapshotByID(t.Context(), id); return err },
		"events":          func() error { _, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1); return err },
	} {
		t.Run(name, func(t *testing.T) {
			recorder.reset()

			require.NoError(t, read())

			requireStrongReadOnly(t, recorder.beginOptions(), 1)
			var transactionIDs [][]byte
			for _, selector := range recorder.transactionSelectors() {
				if id := selector.GetId(); id != nil {
					transactionIDs = append(transactionIDs, id)
				}
			}
			for _, transactionID := range transactionIDs {
				assert.Equal(t, transactionIDs[0], transactionID, "every request of one read shares the same transaction")
			}
		})
	}
}

func TestTransactionOptionsConstructionObservesTheLayoutInOneStrongReadOnlyTransaction(t *testing.T) {
	l := provision(t, layoutSpec{})
	client, recorder := newRecordingClient(t)

	_, err := l.tryOpen(client, l.config())

	require.NoError(t, err)
	requireStrongReadOnly(t, recorder.beginOptions(), 1)
	for _, request := range recorder.all() {
		assert.NotEqual(t, "Commit", request.method, "construction never writes")
		assert.NotEqual(t, "ExecuteBatchDml", request.method, "construction never writes")
	}
}

func TestTransactionOptionsConstructionRejectedBeforeAnyRequestWhenSettingsAreInvalid(t *testing.T) {
	client, recorder := newRecordingClient(t)

	_, err := spanner.New(t.Context(), client, spanner.Config{TablePrefix: "esa", ChangeFeed: true},
		eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

	requireKind(t, err, eventstore.KindConfiguration)
	assert.Empty(t, recorder.all())
}
