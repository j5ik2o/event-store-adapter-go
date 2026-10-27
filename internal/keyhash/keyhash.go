// Package keyhash computes the key prefix that storages with a hashed key use (hash.md K-1, K-2).
package keyhash

import "encoding/binary"

const (
	offsetBasis uint64 = 0xcbf29ce484222325
	prime       uint64 = 0x100000001b3
)

// FNV1a64 returns the FNV-1a 64 hash of b as an unsigned 64-bit integer (K-1).
func FNV1a64(b []byte) uint64 {
	h := offsetBasis
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// Prefix returns the hash of the UTF-8 aid as 8 big-endian bytes. The hash is neither converted to a signed
// integer nor reduced by a shard count (K-2).
func Prefix(aid string) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, FNV1a64([]byte(aid)))
	return out
}
