package pkg

import (
	"fmt"
	"hash/fnv"
)

type KeyResolver interface {
	ResolvePkey(aggregateId AggregateId, shardCount uint64) string
	ResolveSkey(aggregateId AggregateId, seqNr uint64) string
}

type DefaultKeyResolver struct {
}

func (kr *DefaultKeyResolver) ResolvePkey(aggregateId AggregateId, shardCount uint64) string {
	idTypeName := aggregateId.GetTypeName()
	value := aggregateId.GetValue()
	h := fnv.New64a()
	_, err := h.Write([]byte(value))
	if err != nil {
		panic(err)
	}
	hashValue := h.Sum64()
	remainder := hashValue % shardCount
	return fmt.Sprintf("%s-%d", idTypeName, remainder)
}

func (kr *DefaultKeyResolver) ResolveSkey(aggregateId AggregateId, seqNr uint64) string {
	idTypeName := aggregateId.GetTypeName()
	value := aggregateId.GetValue()
	return fmt.Sprintf("%s-%s-%d", idTypeName, value, seqNr)
}
