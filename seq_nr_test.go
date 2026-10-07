package eventstore

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeqNrValidationBoundaries(t *testing.T) {
	assert.Equal(t, SeqNr(9007199254740991), MaxSeqNr)
	for _, tc := range []struct {
		n    SeqNr
		rule string
	}{
		{0, ""}, {1, ""}, {MaxSeqNr, ""}, {-1, "T-9"}, {MaxSeqNr + 1, "T-9"}, {math.MinInt64, "T-9"}, {math.MaxInt64, "T-9"},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			for _, event := range []bool{false, true} {
				t.Run(fmt.Sprintf("event=%t", event), func(t *testing.T) {
					rule := tc.rule
					var err error
					if event {
						err = tc.n.ValidateAsEventSeqNr()
						if tc.n == 0 {
							rule = "W-6"
						}
					} else {
						err = tc.n.Validate()
					}
					if rule == "" {
						require.NoError(t, err)
						return
					}
					v := requireContractViolation(t, err, rule)
					require.NotNil(t, v.SeqNr)
					assert.Equal(t, tc.n, *v.SeqNr)
					assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", tc.n))
				})
			}
		})
	}
}
