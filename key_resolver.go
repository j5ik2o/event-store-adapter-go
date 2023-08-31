package event_store_adapter_go

import (
	"fmt"
	"hash/fnv"
)

type KeyResolver interface {
	ResolvePkey(idTypeName string, value string, shardCount uint64) string
	ResolveSkey(idTypeName string, value string, seqNr uint64) string
}

type DefaultKeyResolver struct {
}

func (kr *DefaultKeyResolver) ResolvePkey(idTypeName string, value string, shardCount uint64) string {
	h := fnv.New64a()
	_, err := h.Write([]byte(value))
	if err != nil {
		panic(err)
	}
	hashValue := h.Sum64()
	remainder := hashValue % shardCount
	return fmt.Sprintf("%s-%d", idTypeName, remainder)
}

func (kr *DefaultKeyResolver) ResolveSkey(idTypeName string, value string, seqNr uint64) string {
	return fmt.Sprintf("%s-%s-%d", idTypeName, value, seqNr)
}
