package event_store_adapter_go

import (
	"fmt"
	"github.com/oklog/ulid"
	"math/rand"
	"time"
)

type UserAccountId struct {
	Value string
}

func (id *UserAccountId) GetTypeName() string {
	return "UserAccountId"
}

func (id *UserAccountId) String() string {
	return id.Value
}

type UserAccount struct {
	Id      UserAccountId
	Name    string
	SeqNr   uint64
	Version uint64
}

func NewUserAccount(id UserAccountId, name string) (*UserAccount, *UserAccountCreated) {
	aggregate := UserAccount{
		Id:      id,
		Name:    name,
		SeqNr:   0,
		Version: 1,
	}
	aggregate.SeqNr += 1
	eventId := NewULID()
	return &aggregate, NewUserAccountCreated(eventId.String(), &id, aggregate.SeqNr, name, uint64(time.Now().UnixNano()))
}

func ReplayUserAccount(events []Event, snapshot *UserAccount, version uint64) *UserAccount {
	result := snapshot
	for _, event := range events {
		result = result.ApplyEvent(event)
	}
	result.Version = version
	return result
}

func (ua *UserAccount) ApplyEvent(event Event) *UserAccount {
	switch e := event.(type) {
	case *UserAccountNameChanged:
		update, err := ua.Rename(e.Name)
		if err != nil {
			panic(err)
		}
		return update.aggregate
	}
	return ua
}

func (ua *UserAccount) String() string {
	return fmt.Sprintf("UserAccount{Id: %s, Name: %s}", ua.Id.String(), ua.Name)
}

func (ua *UserAccount) GetId() AggregateId {
	return &ua.Id
}

type UserAccountResult struct {
	aggregate *UserAccount
	event     *UserAccountNameChanged
}

func (ua *UserAccount) Rename(name string) (*UserAccountResult, error) {
	if ua.Name == name {
		panic("name is the same")
	}
	updatedUserAccount := *ua
	updatedUserAccount.Name = name
	updatedUserAccount.SeqNr += 1
	event := NewUserAccountNameChanged(NewULID().String(), &ua.Id, updatedUserAccount.SeqNr, name, uint64(time.Now().UnixNano()))
	return &UserAccountResult{&updatedUserAccount, event}, nil
}

func (ua *UserAccount) Equals(other *UserAccount) bool {
	return ua.Id.Value == other.Id.Value && ua.Name == other.Name
}

func NewULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
