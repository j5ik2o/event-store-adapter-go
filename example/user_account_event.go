package example

import (
	"fmt"

	esag "github.com/j5ik2o/event-store-adapter-go"
)

type userAccountCreated struct {
	Id          string
	AggregateId esag.AggregateId
	TypeName    string
	SeqNr       uint64
	Name        string
	OccurredAt  uint64
}

func newUserAccountCreated(id string, aggregateId esag.AggregateId, seqNr uint64, name string, occurredAt uint64) *userAccountCreated {
	return &userAccountCreated{
		Id:          id,
		AggregateId: aggregateId,
		TypeName:    "UserAccountCreated",
		SeqNr:       seqNr,
		Name:        name,
		OccurredAt:  occurredAt,
	}
}

func (e *userAccountCreated) String() string {
	return fmt.Sprintf("UserAccountCreated{Id: %s, AggregateId: %s, SeqNr: %d, Name: %s, OccurredAt: %d}", e.Id, e.AggregateId, e.SeqNr, e.Name, e.OccurredAt)
}

func (e *userAccountCreated) GetId() string {
	return e.Id
}

func (e *userAccountCreated) GetTypeName() string {
	return e.TypeName
}

func (e *userAccountCreated) GetAggregateId() esag.AggregateId {
	return e.AggregateId
}

func (e *userAccountCreated) GetSeqNr() uint64 {
	return e.SeqNr
}

func (e *userAccountCreated) GetOccurredAt() uint64 {
	return e.OccurredAt
}

func (e *userAccountCreated) IsCreated() bool {
	return true
}

// ---

type userAccountNameChanged struct {
	Id          string
	AggregateId esag.AggregateId
	TypeName    string
	SeqNr       uint64
	Name        string
	OccurredAt  uint64
}

func newUserAccountNameChanged(id string, aggregateId esag.AggregateId, seqNr uint64, name string, occurredAt uint64) *userAccountNameChanged {
	return &userAccountNameChanged{
		Id:          id,
		AggregateId: aggregateId,
		TypeName:    "UserAccountNameChanged",
		SeqNr:       seqNr,
		Name:        name,
		OccurredAt:  occurredAt,
	}
}

func (e *userAccountNameChanged) String() string {
	return fmt.Sprintf("UserAccountNameChanged{Id: %s, AggregateId: %s, SeqNr: %d, Name: %s, OccurredAt: %d}", e.Id, e.AggregateId, e.SeqNr, e.Name, e.OccurredAt)
}

func (e *userAccountNameChanged) GetId() string {
	return e.Id
}

func (e *userAccountNameChanged) GetTypeName() string {
	return e.TypeName
}

func (e *userAccountNameChanged) GetAggregateId() esag.AggregateId {
	return e.AggregateId
}

func (e *userAccountNameChanged) GetSeqNr() uint64 {
	return e.SeqNr
}

func (e *userAccountNameChanged) GetOccurredAt() uint64 {
	return e.OccurredAt
}

func (e *userAccountNameChanged) IsCreated() bool {
	return false
}
