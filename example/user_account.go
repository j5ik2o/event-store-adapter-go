package example

import (
	"fmt"
	"math/rand"
	"time"

	esag "github.com/j5ik2o/event-store-adapter-go"
	"github.com/oklog/ulid/v2"
)

type userAccountId struct {
	Value string
}

func newUserAccountId(value string) userAccountId {
	return userAccountId{Value: value}
}

func (id *userAccountId) GetTypeName() string {
	return "UserAccountId"
}

func (id *userAccountId) GetValue() string {
	return id.Value
}

func (id *userAccountId) String() string {
	return fmt.Sprintf("%s-%s", id.GetTypeName(), id.Value)
}

type userAccount struct {
	Id      userAccountId
	Name    string
	SeqNr   uint64
	Version uint64
}

func newUserAccount(id userAccountId, name string) (*userAccount, *userAccountCreated) {
	aggregate := userAccount{
		Id:      id,
		Name:    name,
		SeqNr:   0,
		Version: 1,
	}
	aggregate.SeqNr += 1
	eventId := newULID()
	return &aggregate, newUserAccountCreated(eventId.String(), &id, aggregate.SeqNr, name, uint64(time.Now().UnixNano()))
}

func replayUserAccount(events []esag.Event, snapshot *userAccount, version uint64) *userAccount {
	result := snapshot
	for _, event := range events {
		result = result.applyEvent(event)
	}
	result.Version = version
	return result
}

func (ua *userAccount) applyEvent(event esag.Event) *userAccount {
	switch e := event.(type) {
	case *userAccountNameChanged:
		update, err := ua.Rename(e.Name)
		if err != nil {
			panic(err)
		}
		return update.Aggregate
	}
	return ua
}

func (ua *userAccount) String() string {
	return fmt.Sprintf("UserAccount{Id: %s, Name: %s}", ua.Id.String(), ua.Name)
}

func (ua *userAccount) GetId() esag.AggregateId {
	return &ua.Id
}

type userAccountResult struct {
	Aggregate *userAccount
	Event     *userAccountNameChanged
}

func (ua *userAccount) Rename(name string) (*userAccountResult, error) {
	if ua.Name == name {
		panic("name is the same")
	}
	updatedUserAccount := *ua
	updatedUserAccount.Name = name
	updatedUserAccount.SeqNr += 1
	event := newUserAccountNameChanged(newULID().String(), &ua.Id, updatedUserAccount.SeqNr, name, uint64(time.Now().UnixNano()))
	return &userAccountResult{&updatedUserAccount, event}, nil
}

func (ua *userAccount) Equals(other *userAccount) bool {
	return ua.Id.Value == other.Id.Value && ua.Name == other.Name
}

func newULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
