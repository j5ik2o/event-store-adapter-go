package event_store_adapter_go

import (
	"fmt"
)

type UserAccountCreated struct {
	Id          string
	AggregateId AggregateId
	TypeName    string
	SeqNr       uint64
	Name        string
	OccurredAt  uint64
}

func NewUserAccountCreated(id string, aggregateId AggregateId, seqNr uint64, name string, occurredAt uint64) *UserAccountCreated {
	return &UserAccountCreated{
		Id:          id,
		AggregateId: aggregateId,
		TypeName:    "UserAccountCreated",
		SeqNr:       seqNr,
		Name:        name,
		OccurredAt:  occurredAt,
	}
}

func (e *UserAccountCreated) String() string {
	return fmt.Sprintf("UserAccountCreated{Id: %s, AggregateId: %s, SeqNr: %d, Name: %s, OccurredAt: %d}", e.Id, e.AggregateId, e.SeqNr, e.Name, e.OccurredAt)
}

func (e *UserAccountCreated) GetId() string {
	return fmt.Sprintf("%s", e.Id)
}

func (e *UserAccountCreated) GetTypeName() string {
	return e.TypeName
}

func (e *UserAccountCreated) GetAggregateId() AggregateId {
	return e.AggregateId
}

func (e *UserAccountCreated) GetSeqNr() uint64 {
	return e.SeqNr
}

func (e *UserAccountCreated) GetOccurredAt() uint64 {
	return e.OccurredAt
}

func (e *UserAccountCreated) IsCreated() bool {
	return true
}

// ---

type UserAccountNameChanged struct {
	Id          string
	AggregateId AggregateId
	TypeName    string
	SeqNr       uint64
	Name        string
	OccurredAt  uint64
}

func NewUserAccountNameChanged(id string, aggregateId AggregateId, seqNr uint64, name string, occurredAt uint64) *UserAccountNameChanged {
	return &UserAccountNameChanged{
		Id:          id,
		AggregateId: aggregateId,
		TypeName:    "UserAccountNameChanged",
		SeqNr:       seqNr,
		Name:        name,
		OccurredAt:  occurredAt,
	}
}

func (e *UserAccountNameChanged) String() string {
	return fmt.Sprintf("UserAccountNameChanged{Id: %s, AggregateId: %s, SeqNr: %d, Name: %s, OccurredAt: %d}", e.Id, e.AggregateId, e.SeqNr, e.Name, e.OccurredAt)
}

func (e *UserAccountNameChanged) GetId() string {
	return e.Id
}

func (e *UserAccountNameChanged) GetTypeName() string {
	return e.TypeName
}

func (e *UserAccountNameChanged) GetAggregateId() AggregateId {
	return e.AggregateId
}

func (e *UserAccountNameChanged) GetSeqNr() uint64 {
	return e.SeqNr
}

func (e *UserAccountNameChanged) GetOccurredAt() uint64 {
	return e.OccurredAt
}

func (e *UserAccountNameChanged) IsCreated() bool {
	return false
}
