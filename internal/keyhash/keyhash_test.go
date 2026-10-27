package keyhash_test

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The four values are the reference table of hash.md (K-1), also distributed as conformance/values/hash.json.
func TestFNV1a64MatchesTheReferenceTable(t *testing.T) {
	for input, want := range map[string]uint64{
		"a":                          0xaf63dc4c8601ec8c,
		"01H42K4ABWQ5V2XQEP3A48VE0Z": 0xadb103af5fe2a91a,
		"01H42KBHCW1BZG504J4ZXKA2F2": 0x0aa990285e0388ca,
		"ユーザー":                       0x309f20171820053e,
	} {
		assert.Equal(t, want, keyhash.FNV1a64([]byte(input)), input)
	}
}

func TestFNV1a64OfEmptyInputIsTheOffsetBasis(t *testing.T) {
	assert.Equal(t, uint64(0xcbf29ce484222325), keyhash.FNV1a64(nil))
}

// K-2 / the Spanner profile: the key prefix is the unsigned hash as 8 big-endian bytes, never reduced by a shard count.
func TestPrefixIsTheUnsignedHashInEightBigEndianBytes(t *testing.T) {
	prefix := keyhash.Prefix("Order-1")

	require.Len(t, prefix, 8)
	assert.Equal(t, "04694b5b07ad70cd", hex.EncodeToString(prefix), "fixed value shared with the JavaScript implementation")
	assert.Equal(t, keyhash.FNV1a64([]byte("Order-1")), binary.BigEndian.Uint64(prefix))
}

func TestPrefixKeepsTheTopBitOfTheHash(t *testing.T) {
	// 0xaf63dc4c8601ec8c has the top bit set: a conversion to a signed integer, or a remainder, would change the bytes.
	assert.Equal(t, "af63dc4c8601ec8c", hex.EncodeToString(keyhash.Prefix("a")))
}

func TestPrefixSeparatesAggregatesWhoseIDsShareAPrefix(t *testing.T) {
	assert.NotEqual(t, keyhash.Prefix("Order-1"), keyhash.Prefix("Order-10"))
}

func TestPrefixReturnsAnIndependentSliceEachTime(t *testing.T) {
	first := keyhash.Prefix("Order-1")
	first[0] ^= 0xff

	assert.Equal(t, "04694b5b07ad70cd", hex.EncodeToString(keyhash.Prefix("Order-1")))
}
