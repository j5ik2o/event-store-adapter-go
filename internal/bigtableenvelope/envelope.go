// Package bigtableenvelope encodes and decodes the fixed binary envelopes that the Bigtable storage keeps in its
// cells (BT-D6a, ADR-0008). All integers are big-endian; the leading byte is the format version 0x01.
//
//	event    = 0x01 || U16(len(aid)) || aid || U64(seq_nr) || I64(occurred_at_ns) || U32(len(manifest)) || manifest || U32(len(payload)) || payload
//	snapshot = 0x01 || U64(seq_nr) || U32(len(manifest)) || manifest || U32(len(payload)) || payload
package bigtableenvelope

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
)

const version = 1

// Event is the content of an event envelope.
type Event struct {
	AID          string
	SeqNr        uint64
	OccurredAtNs int64
	Manifest     string
	Payload      []byte
}

// Snapshot is the content of a snapshot envelope.
type Snapshot struct {
	SeqNr    uint64
	Manifest string
	Payload  []byte
}

// EventLen is the encoded size of an event envelope.
func EventLen(aid, manifest string, payloadLen int) int {
	return 1 + 2 + len(aid) + 8 + 8 + 4 + len(manifest) + 4 + payloadLen
}

// SnapshotLen is the encoded size of a snapshot envelope.
func SnapshotLen(manifest string, payloadLen int) int {
	return 1 + 8 + 4 + len(manifest) + 4 + payloadLen
}

// EncodeEvent encodes an event envelope. It fails when a length does not fit its field.
func EncodeEvent(e Event) ([]byte, error) {
	if len(e.AID) > math.MaxUint16 {
		return nil, fmt.Errorf("the aid is longer than %d bytes", math.MaxUint16)
	}
	if err := fitsU32(len(e.Manifest), len(e.Payload)); err != nil {
		return nil, err
	}
	out := make([]byte, 0, EventLen(e.AID, e.Manifest, len(e.Payload)))
	out = append(out, version)
	out = binary.BigEndian.AppendUint16(out, uint16(len(e.AID)))
	out = append(out, e.AID...)
	out = binary.BigEndian.AppendUint64(out, e.SeqNr)
	out = binary.BigEndian.AppendUint64(out, uint64(e.OccurredAtNs))
	out = binary.BigEndian.AppendUint32(out, uint32(len(e.Manifest)))
	out = append(out, e.Manifest...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(e.Payload)))
	return append(out, e.Payload...), nil
}

// EncodeSnapshot encodes a snapshot envelope.
func EncodeSnapshot(s Snapshot) ([]byte, error) {
	if err := fitsU32(len(s.Manifest), len(s.Payload)); err != nil {
		return nil, err
	}
	out := make([]byte, 0, SnapshotLen(s.Manifest, len(s.Payload)))
	out = append(out, version)
	out = binary.BigEndian.AppendUint64(out, s.SeqNr)
	out = binary.BigEndian.AppendUint32(out, uint32(len(s.Manifest)))
	out = append(out, s.Manifest...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(s.Payload)))
	return append(out, s.Payload...), nil
}

func fitsU32(lengths ...int) error {
	for _, n := range lengths {
		if uint64(n) > math.MaxUint32 {
			return errors.New("a field is longer than 4294967295 bytes")
		}
	}
	return nil
}

func storageError(format string, args ...any) error {
	return &eventstore.StorageError{Cause: fmt.Errorf("malformed envelope: "+format, args...)}
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.err = storageError("truncated")
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u16() int {
	if b := r.take(2); b != nil {
		return int(binary.BigEndian.Uint16(b))
	}
	return 0
}

func (r *reader) u32() int {
	if b := r.take(4); b != nil {
		return int(binary.BigEndian.Uint32(b))
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

func (r *reader) text(n int, what string) string {
	b := r.take(n)
	if r.err == nil && !utf8.Valid(b) {
		r.err = storageError("%s is not valid UTF-8", what)
	}
	return string(b)
}

func (r *reader) header() {
	if b := r.take(1); b != nil && b[0] != version {
		r.err = storageError("unknown format version %d", b[0])
	}
}

func (r *reader) finish() error {
	if r.err == nil && len(r.b) != 0 {
		r.err = storageError("%d unused trailing bytes", len(r.b))
	}
	return r.err
}

func checkSeqNr(seq uint64) error {
	if seq < 1 || seq > uint64(eventstore.MaxSeqNr) {
		return storageError("seq_nr %d is outside 1 through 2^53-1", seq)
	}
	return nil
}

// DecodeEvent decodes an event envelope. Payload aliases b.
func DecodeEvent(b []byte) (Event, error) {
	r := &reader{b: b}
	r.header()
	aid := r.text(r.u16(), "aid")
	seq := r.u64()
	ns := int64(r.u64())
	manifest := r.text(r.u32(), "manifest")
	payload := r.take(r.u32())
	if err := r.finish(); err != nil {
		return Event{}, err
	}
	if err := checkSeqNr(seq); err != nil {
		return Event{}, err
	}
	return Event{AID: aid, SeqNr: seq, OccurredAtNs: ns, Manifest: manifest, Payload: payload}, nil
}

// DecodeSnapshot decodes a snapshot envelope. Payload aliases b.
func DecodeSnapshot(b []byte) (Snapshot, error) {
	r := &reader{b: b}
	r.header()
	seq := r.u64()
	manifest := r.text(r.u32(), "manifest")
	payload := r.take(r.u32())
	if err := r.finish(); err != nil {
		return Snapshot{}, err
	}
	if err := checkSeqNr(seq); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{SeqNr: seq, Manifest: manifest, Payload: payload}, nil
}
