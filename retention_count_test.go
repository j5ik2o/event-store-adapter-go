package eventstore

import (
	"fmt"
	"math"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoRetention(t *testing.T) {
	count := NoRetention()

	got, err := storeoptions.Apply(newConfigurationError, WithRetentionCount(count))

	require.NoError(t, err)
	assert.Nil(t, got.RetentionCount)
}

func TestKeepLatestPreservesPositiveCount(t *testing.T) {
	for _, n := range []int{1, 7, math.MaxInt} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			count, err := KeepLatest(n)
			require.NoError(t, err)

			got, err := storeoptions.Apply(newConfigurationError, WithRetentionCount(count))

			require.NoError(t, err)
			require.NotNil(t, got.RetentionCount)
			assert.Equal(t, n, *got.RetentionCount)
		})
	}
}

func TestKeepLatestRejectsNonPositiveCount(t *testing.T) {
	for _, n := range []int{0, -1, math.MinInt} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			_, err := KeepLatest(n)

			classified := requireConfiguration(t, err)
			assert.Contains(t, classified.Unwrap().Error(), fmt.Sprint(n))
		})
	}
}
