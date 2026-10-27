package bigtable

import (
	"encoding/binary"
	"fmt"
	"strconv"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

const (
	familyEvent    = "event"
	familyHistory  = "history"
	familyHead     = "head"
	familySnapshot = "snapshot"
	familyConfig   = "config"

	eventEnvelope   = "envelope"
	eventHasSnap    = "has_snapshot"
	historyState    = "state"
	historyEnvelope = "envelope"
	headSeqNr       = "seq_nr"
	headLastEvent   = "last_event"
	headHasSnapshot = "has_snapshot"
	snapshotCurrent = "current"
	configMetadata  = "metadata"

	stateActive  = "active"
	stateRetired = "retired"
	flagSet      = "1"
	flagUnset    = "0"

	// configRowKey is a single byte. A data row key always starts with the 8-byte hash and the 2-byte length, so it
	// is never equal to it.
	configRowKey = "\x00"

	// cellTimestamp is the fixed timestamp of every cell (BT-6); occurred_at is never derived from it.
	cellTimestamp gbigtable.Timestamp = 0
)

// journalFamilies and headFamilies are the column families that each table must have.
var (
	journalFamilies = []string{familyEvent, familyHistory, familyConfig}
	headFamilies    = []string{familyHead, familySnapshot, familyConfig}
)

// prefix builds P(aid) = H || L16(len(aid)) || aid (BT-3). The hash is FNV-1a 64 as 8 big-endian bytes unless a test
// hook forces a collision.
func prefix(hooks *testhook.Hooks, aid string) ([]byte, error) {
	if len(aid) > 0xFFFF {
		return nil, &eventstore.ContractViolationError{Rule: "T-12"}
	}
	var key []byte
	if forced, ok := hooks.KeyHash(aid); ok {
		key = binary.BigEndian.AppendUint64(nil, forced)
	} else {
		key = keyhash.Prefix(aid)
	}
	key = binary.BigEndian.AppendUint16(key, uint16(len(aid)))
	return append(key, aid...), nil
}

// journalRowKey is P(aid) || U64BE(seq_nr) (BT-4).
func journalRowKey(prefix []byte, seqNr eventstore.SeqNr) string {
	return string(binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), uint64(seqNr)))
}

// journalSeqNr extracts the seq_nr from a journal row key of the aggregate; ok is false for any other key.
func journalSeqNr(prefix []byte, rowKey string) (eventstore.SeqNr, bool) {
	if len(rowKey) != len(prefix)+8 || rowKey[:len(prefix)] != string(prefix) {
		return 0, false
	}
	return eventstore.SeqNr(binary.BigEndian.Uint64([]byte(rowKey[len(prefix):]))), true
}

// formatHeadSeqNr is the 20 zero-padded decimal digits of head:seq_nr.
func formatHeadSeqNr(seqNr eventstore.SeqNr) string { return fmt.Sprintf("%020d", seqNr) }

func parseHeadSeqNr(value []byte) (eventstore.SeqNr, bool) {
	if len(value) != 20 {
		return 0, false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || n < 1 || n > uint64(eventstore.MaxSeqNr) {
		return 0, false
	}
	return eventstore.SeqNr(n), true
}

func flagOf(has bool) string {
	if has {
		return flagSet
	}
	return flagUnset
}

// columnFilter matches exactly one column of a family: [qualifier, qualifier+0x00).
func columnFilter(family, qualifier string) gbigtable.Filter {
	return gbigtable.ColumnRangeFilter(family, qualifier, qualifier+"\x00")
}

// latestCell selects the newest cell of one column.
func latestCell(family, qualifier string) gbigtable.Filter {
	return gbigtable.ChainFilters(columnFilter(family, qualifier), gbigtable.LatestNFilter(1))
}

// latestCellEquals is true when the newest cell of the column holds exactly value: [value, value+0x00).
func latestCellEquals(family, qualifier string, value []byte) gbigtable.Filter {
	return gbigtable.ChainFilters(columnFilter(family, qualifier), gbigtable.LatestNFilter(1),
		gbigtable.ValueRangeFilter(value, append(append([]byte(nil), value...), 0)))
}

func setCell(m *gbigtable.Mutation, family, qualifier string, value []byte) {
	m.Set(family, qualifier, cellTimestamp, value)
}

// replaceCell deletes every version of the column and writes the new value in the same mutation (BT-6).
func replaceCell(m *gbigtable.Mutation, family, qualifier string, value []byte) {
	m.DeleteCellsInColumn(family, qualifier)
	m.Set(family, qualifier, cellTimestamp, value)
}

// cell returns the value of "family:qualifier" in a row read with a latest-cell filter.
func cell(row gbigtable.Row, family, qualifier string) ([]byte, bool) {
	column := family + ":" + qualifier
	for _, item := range row[family] {
		if item.Column == column {
			return item.Value, true
		}
	}
	return nil, false
}
