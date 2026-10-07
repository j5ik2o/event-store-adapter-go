package eventstore

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOccurredAtBoundaries(t *testing.T) {
	seq := SeqNr(7)
	for _, tc := range []struct {
		iso    string
		valid  bool
		wantNs int64
	}{
		{"1677-09-21T00:12:43.145224192Z", true, math.MinInt64},
		{"2262-04-11T23:47:16.854775807Z", true, math.MaxInt64},
		{"1677-09-21T00:12:43.145224191Z", false, 0},
		{"2262-04-11T23:47:16.854775808Z", false, 0},
		{"1970-01-01T00:00:00Z", true, 0},
		{"1969-12-31T23:59:59.999999999Z", true, -1},
		{"1970-01-01T00:00:00.123456789Z", true, 123456789},
		{"0001-01-01T00:00:00Z", false, 0},
		{"9999-12-31T23:59:59Z", false, 0},
		{"2262-04-12T08:47:16.854775807+09:00", true, math.MaxInt64},
	} {
		t.Run(tc.iso, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339Nano, tc.iso)
			require.NoError(t, err)
			err = validateOccurredAt(at, seq)
			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, tc.wantNs, at.UnixNano(), "conversion happens only after validation")
				return
			}
			v := requireContractViolation(t, err, "T-13")
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, seq, *v.SeqNr)
			assert.Contains(t, err.Error(), "seq_nr=7")
		})
	}
}
