package event_store_adapter_go

import (
	"fmt"
	"hash/fnv"
)

type KeyResolver interface {
	ResolvePkey(event Event, shardCount uint64) string
	ResolveSkey(event Event, seqNr uint64) string
}

type DefaultKeyResolver struct {
}

func (kr *DefaultKeyResolver) ResolvePkey(event Event, shardCount uint64) string {
	idTypeName := event.GetAggregateId().GetTypeName()
	value := event.GetAggregateId().GetValue()
	h := fnv.New64a()
	_, err := h.Write([]byte(value))
	if err != nil {
		panic(err)
	}
	hashValue := h.Sum64()
	remainder := hashValue % shardCount
	return fmt.Sprintf("%s-%d", idTypeName, remainder)
}

func (kr *DefaultKeyResolver) ResolveSkey(event Event, seqNr uint64) string {
	idTypeName := event.GetAggregateId().GetTypeName()
	value := event.GetAggregateId().GetValue()
	return fmt.Sprintf("%s-%s-%d", idTypeName, value, seqNr)
}
