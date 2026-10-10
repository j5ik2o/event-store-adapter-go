package test

import (
	"fmt"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/stretchr/testify/require"
)

type userAccountID string

func (id userAccountID) TypeName() string { return "UserAccount" }
func (id userAccountID) Value() string    { return string(id) }

type userAccount struct {
	ID    userAccountID `json:"id"`
	Name  string        `json:"name"`
	seqNr eventstore.SeqNr
}

func newUserAccount(id userAccountID, name string) (*userAccount, userAccountEvent) {
	return &userAccount{ID: id, Name: name, seqNr: 1}, userAccountEvent{Kind: "created", Name: name}
}
func (account *userAccount) Rename(name string) (*userAccount, userAccountEvent) {
	next := *account
	next.Name = name
	next.seqNr++
	return &next, userAccountEvent{Kind: "name-changed", Name: name}
}
func replayUserAccount(id userAccountID, events []eventstore.EventEnvelope[userAccountEvent], snapshot *userAccount) (*userAccount, error) {
	result := snapshot
	for _, envelope := range events {
		payload := envelope.Payload()
		switch payload.Kind {
		case "created":
			result = &userAccount{ID: id, Name: payload.Name}
		case "name-changed":
			if result == nil {
				return nil, fmt.Errorf("name change precedes account creation")
			}
			next := *result
			next.Name = payload.Name
			result = &next
		default:
			return nil, fmt.Errorf("unknown user account event %q", payload.Kind)
		}
		result.seqNr = envelope.SeqNr()
	}
	return result, nil
}

func TestReplayUserAccount(t *testing.T) {
	id := userAccountID("1")
	initial, created := newUserAccount(id, "first")
	renamed, changed := initial.Rename("second")
	require.Equal(t, eventstore.SeqNr(1), initial.seqNr)
	require.Equal(t, "first", initial.Name)
	at := time.Unix(1, 0)
	first, err := eventstore.NewEventEnvelope(id, initial.seqNr, at, created)
	require.NoError(t, err)
	second, err := eventstore.NewEventEnvelope(id, renamed.seqNr, at, changed)
	require.NoError(t, err)
	fromEvents, err := replayUserAccount(id, []eventstore.EventEnvelope[userAccountEvent]{first, second}, nil)
	require.NoError(t, err)
	require.Equal(t, renamed, fromEvents)
	fromSnapshot, err := replayUserAccount(id, []eventstore.EventEnvelope[userAccountEvent]{second}, initial)
	require.NoError(t, err)
	require.Equal(t, renamed, fromSnapshot)
	_, err = replayUserAccount(id, []eventstore.EventEnvelope[userAccountEvent]{second}, nil)
	require.Error(t, err)
	unknown, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(1), at, userAccountEvent{Kind: "unknown"})
	require.NoError(t, err)
	_, err = replayUserAccount(id, []eventstore.EventEnvelope[userAccountEvent]{unknown}, nil)
	require.Error(t, err)
}
