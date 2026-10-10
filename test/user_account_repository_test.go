package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/dynamodb"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/memory"
	"github.com/stretchr/testify/require"
)

type userAccountRepository struct {
	eventStore eventstore.EventStore[userAccountEvent, userAccount]
}

func newUserAccountRepository(store eventstore.EventStore[userAccountEvent, userAccount]) *userAccountRepository {
	return &userAccountRepository{eventStore: store}
}
func (r *userAccountRepository) storeEvent(ctx context.Context, id userAccountID, seqNr eventstore.SeqNr, payload userAccountEvent) error {
	event, err := eventstore.NewEventEnvelope(id, seqNr, time.Now(), payload, eventstore.WithManifest("user-account-event/v1"))
	if err != nil {
		return err
	}
	return r.eventStore.PersistEvent(ctx, event)
}
func (r *userAccountRepository) storeEventAndSnapshot(ctx context.Context, event userAccountEvent, account *userAccount) error {
	envelope, err := eventstore.NewEventEnvelope(account.ID, account.seqNr, time.Now(), event, eventstore.WithManifest("user-account-event/v1"))
	if err != nil {
		return err
	}
	snapshot, err := eventstore.NewSnapshotEnvelope(*account, account.seqNr, eventstore.WithManifest("user-account-state/v1"))
	if err != nil {
		return err
	}
	return r.eventStore.PersistEventAndSnapshot(ctx, envelope, snapshot)
}
func (r *userAccountRepository) findByID(ctx context.Context, id userAccountID) (*userAccount, error) {
	read, err := r.eventStore.GetLatestSnapshotByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, fmt.Errorf("user account %s not found", id)
	}
	since := eventstore.SeqNr(1)
	var state *userAccount
	if read.Snapshot != nil {
		restored := read.Snapshot.Aggregate()
		restored.seqNr = read.Snapshot.SeqNr()
		state = &restored
		since = read.Snapshot.SeqNr() + 1
	}
	events, err := r.eventStore.GetEventsByIDSinceSeqNr(ctx, id, since)
	if err != nil {
		return nil, err
	}
	return replayUserAccount(id, events, state)
}

func TestUserAccountRepository(t *testing.T) {
	for _, backend := range []string{"memory", "dynamodb"} {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			keep, err := eventstore.KeepLatest(1)
			require.NoError(t, err)
			options := []eventstore.Option{eventstore.WithRetentionCount(keep)}
			var store eventstore.EventStore[userAccountEvent, userAccount]
			if backend == "memory" {
				state, err := memory.NewStore(options...)
				require.NoError(t, err)
				store, err = memory.New(state, eventstore.NewJSONSerializer[userAccountEvent](), eventstore.NewJSONSerializer[userAccount]())
				require.NoError(t, err)
			} else {
				environment, err := dynamodbtest.Start(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					require.NoError(t, environment.Close(cleanup))
				})
				tables, err := environment.CreateTables(ctx, false)
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					require.NoError(t, tables.Close(cleanup))
				})
				journal, _ := tables.TableName("journal")
				snapshot, _ := tables.TableName("snapshot")
				head, _ := tables.TableName("head")
				store, err = dynamodb.New(ctx, environment.NewClient(), dynamodb.Config{JournalTableName: journal, SnapshotTableName: snapshot, HeadTableName: head, SnapshotHistoryIndexName: tables.HistoryIndexName()}, eventstore.NewJSONSerializer[userAccountEvent](), eventstore.NewJSONSerializer[userAccount](), options...)
				require.NoError(t, err)
			}
			repository := newUserAccountRepository(store)
			initial, created := newUserAccount(userAccountID("with-snapshot"), "first")
			require.NoError(t, repository.storeEventAndSnapshot(ctx, created, initial))
			renamed, changed := initial.Rename("second")
			require.NoError(t, repository.storeEvent(ctx, renamed.ID, renamed.seqNr, changed))
			latest, err := store.GetLatestSnapshotByID(ctx, initial.ID)
			require.NoError(t, err)
			require.Equal(t, eventstore.SeqNr(2), latest.HeadSeqNr)
			require.Equal(t, eventstore.SeqNr(1), latest.Snapshot.SeqNr())
			restored, err := repository.findByID(ctx, initial.ID)
			require.NoError(t, err)
			require.Equal(t, renamed, restored, "event 2 must be replayed after snapshot 1, despite head 2")
			third, changedAgain := restored.Rename("third")
			require.NoError(t, repository.storeEventAndSnapshot(ctx, changedAgain, third))
			restored, err = repository.findByID(ctx, third.ID)
			require.NoError(t, err)
			require.Equal(t, third, restored)

			initial, created = newUserAccount(userAccountID("without-snapshot"), "first")
			require.NoError(t, repository.storeEvent(ctx, initial.ID, initial.seqNr, created))
			renamed, changed = initial.Rename("second")
			require.NoError(t, repository.storeEvent(ctx, renamed.ID, renamed.seqNr, changed))
			latest, err = store.GetLatestSnapshotByID(ctx, initial.ID)
			require.NoError(t, err)
			require.Nil(t, latest.Snapshot)
			require.Equal(t, eventstore.SeqNr(2), latest.HeadSeqNr)
			restored, err = repository.findByID(ctx, initial.ID)
			require.NoError(t, err)
			require.Equal(t, renamed, restored, "a missing snapshot requires replay from event 1")
			_, err = repository.findByID(ctx, userAccountID("missing"))
			require.ErrorContains(t, err, "not found")
		})
	}
}
