package bigtable_test

import (
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collide forces the listed aggregates onto the same hash (the hook is the only way to get a real collision).
func collide(aids ...string) *testhook.Hooks {
	hooks := testhook.New()
	hooks.OnKeyHash(func(aid string) (uint64, bool) {
		for _, candidate := range aids {
			if aid == candidate {
				return 0x01, true
			}
		}
		return 0, false
	})
	return hooks
}

func TestHashCollisionKeepsAggregatesApart(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t, hooksOption(collide("Order-1", "Order-10")))
	one, ten := aggregate(t, "Order", "1"), aggregate(t, "Order", "10")
	for seq := eventstore.SeqNr(1); seq <= 2; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, one, seq, "one")))
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, ten, seq, "ten")))
	}

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), one, 1)

	require.NoError(t, err)
	require.Len(t, events, 2)
	for _, e := range events {
		assert.Equal(t, "Order-1", e.AggregateID())
		assert.Equal(t, "one", e.Payload())
	}
}

func TestRowKeyLengthStopsAPrefixAidFromSharingARange(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t, hooksOption(collide("Order-1", "Order-1\x00")))
	short := aggregate(t, "Order", "1")
	long := aggregate(t, "Order", "1\x00")
	for seq := eventstore.SeqNr(1); seq <= 3; seq++ {
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, long, seq, "long")))
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, short, seq, "short")))
	}

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), short, 1)

	require.NoError(t, err)
	require.Len(t, events, 3, "the NUL-extended aid has a different length in its key and is not read")
	for _, e := range events {
		assert.Equal(t, "short", e.Payload())
	}
}

func TestRowKeyLayoutIsHashLengthAidAndBigEndianSeqNr(t *testing.T) {
	l := provision(t, layoutSpec{})
	entry := l.newEntry(t)
	id := aggregate(t, "Order", "ünï-9")
	aid := aidOf(t, id)
	for seq := eventstore.SeqNr(1); seq <= 258; seq++ { // 256 and 257 cross the one-byte boundary of the number
		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, seq, "p")))
	}

	keys := l.journalKeys(t)

	require.Len(t, keys, 259, "configuration row plus journal rows 1..258 (every committed event is written out after the commit)")
	assert.Equal(t, "\x00", keys[0])
	assert.Equal(t, journalKey(aid, 1), keys[1])
	assert.Equal(t, journalKey(aid, 256), keys[256])
	assert.Equal(t, journalKey(aid, 257), keys[257], "the byte order of the keys is the sequence order")
	assert.Equal(t, journalKey(aid, 258), keys[258])
	assert.Equal(t, []string{"\x00", string(rowPrefix(aid))}, rowKeys(t, l.data, l.head), "the head key is P(aid)")
}

func TestRowKeyConfigurationRowIsNeverReadOrRetained(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1))})
	entry := l.newEntry(t)
	id := aggregate(t, "Order", "cfg")
	persistSnapshots(t, entry, id, 1, 4)

	events, err := entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)

	require.NoError(t, err)
	require.Len(t, events, 4)
	assert.NotNil(t, cells(t, l.data, l.journal, "\x00"), "the configuration row survives retention")
	assert.Contains(t, cells(t, l.data, l.journal, "\x00"), "config:metadata")
}
