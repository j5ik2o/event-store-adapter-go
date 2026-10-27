package bigtableenvelope_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtableenvelope"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type valueCase struct {
	ID        string `json:"id"`
	Operation string `json:"operation"`
	Input     struct {
		AID          string `json:"aid"`
		SeqNr        uint64 `json:"seq_nr"`
		OccurredAtNs string `json:"occurred_at_ns"`
		Manifest     string `json:"manifest"`
		PayloadHex   string `json:"payload_hex"`
		Kind         string `json:"kind"`
		BytesHex     string `json:"bytes_hex"`
	} `json:"input"`
	Expect struct {
		Value string          `json:"value"`
		Error json.RawMessage `json:"error"`
	} `json:"expect"`
}

func loadCases(t *testing.T) []valueCase {
	t.Helper()
	raw, err := os.ReadFile("../../conformance/values/bigtable-envelope.json")
	require.NoError(t, err)
	var file struct {
		Cases []valueCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	require.Len(t, file.Cases, 10)
	return file.Cases
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func requireStorage(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok)
	assert.Equal(t, eventstore.KindStorage, kind, "a decoding failure is a storage error, not a serialization error: %v", err)
}

// TestDistributedValues runs the 10 distributed envelope cases: the encoding equals the expected bytes and
// the expected bytes decode back to the input; malformed bytes are storage errors.
func TestDistributedValues(t *testing.T) {
	for _, c := range loadCases(t) {
		t.Run(c.ID, func(t *testing.T) {
			switch c.Operation {
			case "encodeBigtableEventEnvelope":
				ns, err := strconv.ParseInt(c.Input.OccurredAtNs, 10, 64)
				require.NoError(t, err)
				event := bigtableenvelope.Event{AID: c.Input.AID, SeqNr: c.Input.SeqNr, OccurredAtNs: ns,
					Manifest: c.Input.Manifest, Payload: mustHex(t, c.Input.PayloadHex)}
				got, err := bigtableenvelope.EncodeEvent(event)
				require.NoError(t, err)
				want := mustHex(t, c.Expect.Value)
				assert.Equal(t, want, got)
				assert.Equal(t, len(want), bigtableenvelope.EventLen(event.AID, event.Manifest, len(event.Payload)))
				back, err := bigtableenvelope.DecodeEvent(want)
				require.NoError(t, err)
				assert.Equal(t, event.AID, back.AID)
				assert.Equal(t, event.SeqNr, back.SeqNr)
				assert.Equal(t, event.OccurredAtNs, back.OccurredAtNs)
				assert.Equal(t, event.Manifest, back.Manifest)
				assert.Equal(t, event.Payload, append([]byte{}, back.Payload...))
			case "encodeBigtableSnapshotEnvelope":
				snapshot := bigtableenvelope.Snapshot{SeqNr: c.Input.SeqNr, Manifest: c.Input.Manifest, Payload: mustHex(t, c.Input.PayloadHex)}
				got, err := bigtableenvelope.EncodeSnapshot(snapshot)
				require.NoError(t, err)
				want := mustHex(t, c.Expect.Value)
				assert.Equal(t, want, got)
				assert.Equal(t, len(want), bigtableenvelope.SnapshotLen(snapshot.Manifest, len(snapshot.Payload)))
				back, err := bigtableenvelope.DecodeSnapshot(want)
				require.NoError(t, err)
				assert.Equal(t, snapshot.SeqNr, back.SeqNr)
				assert.Equal(t, snapshot.Manifest, back.Manifest)
				assert.Equal(t, snapshot.Payload, append([]byte{}, back.Payload...))
			case "decodeBigtableEnvelope":
				require.Equal(t, "event", c.Input.Kind)
				_, err := bigtableenvelope.DecodeEvent(mustHex(t, c.Input.BytesHex))
				requireStorage(t, err)
			default:
				t.Fatalf("unknown operation %s", c.Operation)
			}
		})
	}
}

func TestDecodeRejectsMalformedEnvelopes(t *testing.T) {
	valid, err := bigtableenvelope.EncodeEvent(bigtableenvelope.Event{AID: "Order-9", SeqNr: 1, Manifest: "m", Payload: []byte("p")})
	require.NoError(t, err)
	validSnapshot, err := bigtableenvelope.EncodeSnapshot(bigtableenvelope.Snapshot{SeqNr: 1, Manifest: "m", Payload: []byte("p")})
	require.NoError(t, err)

	t.Run("a truncated event is a storage error", func(t *testing.T) {
		_, err := bigtableenvelope.DecodeEvent(valid[:len(valid)-1])
		requireStorage(t, err)
	})
	t.Run("an empty event is a storage error", func(t *testing.T) {
		_, err := bigtableenvelope.DecodeEvent(nil)
		requireStorage(t, err)
	})
	t.Run("an event with a trailing byte is a storage error", func(t *testing.T) {
		_, err := bigtableenvelope.DecodeEvent(append(append([]byte{}, valid...), 0))
		requireStorage(t, err)
	})
	t.Run("a snapshot with a trailing byte is a storage error", func(t *testing.T) {
		_, err := bigtableenvelope.DecodeSnapshot(append(append([]byte{}, validSnapshot...), 0))
		requireStorage(t, err)
	})
	t.Run("a truncated snapshot is a storage error", func(t *testing.T) {
		_, err := bigtableenvelope.DecodeSnapshot(validSnapshot[:len(validSnapshot)-1])
		requireStorage(t, err)
	})
	t.Run("an unknown snapshot version is a storage error", func(t *testing.T) {
		bad := append([]byte{}, validSnapshot...)
		bad[0] = 2
		_, err := bigtableenvelope.DecodeSnapshot(bad)
		requireStorage(t, err)
	})
	t.Run("invalid UTF-8 in the aid is a storage error", func(t *testing.T) {
		bad := append([]byte{}, valid...)
		bad[3] = 0xff // first byte of the aid
		_, err := bigtableenvelope.DecodeEvent(bad)
		requireStorage(t, err)
	})
	t.Run("invalid UTF-8 in the manifest is a storage error", func(t *testing.T) {
		bad := append([]byte{}, valid...)
		bad[3+7+8+8+4] = 0xff // the manifest byte
		_, err := bigtableenvelope.DecodeEvent(bad)
		requireStorage(t, err)
	})
	t.Run("a seq_nr outside 1..2^53-1 is rejected on decode", func(t *testing.T) {
		zero := append([]byte{}, valid...)
		copy(zero[3+7:], make([]byte, 8))
		_, err := bigtableenvelope.DecodeEvent(zero)
		requireStorage(t, err)
		over := append([]byte{}, valid...)
		copy(over[3+7:], []byte{0, 0x20, 0, 0, 0, 0, 0, 0})
		_, err = bigtableenvelope.DecodeEvent(over)
		requireStorage(t, err)
	})
}
