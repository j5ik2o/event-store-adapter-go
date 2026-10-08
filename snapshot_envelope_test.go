package eventstore

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotEnvelopeAccessors(t *testing.T) {
	aggregate := struct{ Count int }{Count: 3}
	s, err := NewSnapshotEnvelope(aggregate, 7)
	require.NoError(t, err)
	require.NoError(t, s.Validate())
	assert.Equal(t, aggregate, s.Aggregate())
	assert.Equal(t, SeqNr(7), s.SeqNr())
	assert.Empty(t, s.Manifest())
}

func TestSnapshotEnvelopeManifest(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []EnvelopeOption
		want string
	}{
		{"omitted", nil, ""},
		{"empty", []EnvelopeOption{WithManifest("")}, ""},
		{"uninterpreted", []EnvelopeOption{WithManifest("  arbitrary/型-v999\x00\n ")}, "  arbitrary/型-v999\x00\n "},
		{"last", []EnvelopeOption{WithManifest("first"), WithManifest("last")}, "last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSnapshotEnvelope("domain", 0, tc.opts...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, s.Manifest())
			assert.Equal(t, "domain", s.Aggregate())
		})
	}
}

func TestSnapshotEnvelopeMissingAndExplicitValues(t *testing.T) {
	var zero SnapshotEnvelope[any]
	v := requireContractViolation(t, zero.Validate(), "T-10")
	assert.Nil(t, v.SeqNr)
	assert.NotContains(t, v.Error(), "seq_nr")
	for _, aggregate := range []any{nil, (*string)(nil), json.RawMessage("null"), 0, false, ""} {
		t.Run(fmt.Sprintf("%T/%v", aggregate, aggregate), func(t *testing.T) {
			s, err := NewSnapshotEnvelope(aggregate, 0)
			require.NoError(t, err)
			require.NoError(t, s.Validate())
			assert.Equal(t, aggregate, s.Aggregate())
			assert.Zero(t, s.SeqNr())
		})
	}
}

func TestSnapshotEnvelopeSequenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n     SeqNr
		valid bool
	}{
		{0, true}, {1, true}, {MaxSeqNr, true}, {-1, false}, {MaxSeqNr + 1, false},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			s, err := NewSnapshotEnvelope("aggregate", tc.n)
			if tc.valid {
				require.NoError(t, err)
				require.NoError(t, s.Validate())
				assert.Equal(t, tc.n, s.SeqNr())
				return
			}
			v := requireContractViolation(t, err, "T-9")
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, tc.n, *v.SeqNr)
			assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", tc.n))
		})
	}
}
