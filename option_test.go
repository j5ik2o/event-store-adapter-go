package eventstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonOptionsDefaults(t *testing.T) {
	got, err := storeoptions.Apply[Option](newConfigurationError)

	require.NoError(t, err)
	assert.Nil(t, got.RetentionCount)
	assert.Equal(t, storeoptions.RetentionDelete, got.RetentionMode)
	assert.Zero(t, got.TTLGraceSeconds)
	assert.Nil(t, got.RetentionFailureHandler)
}

func TestWithRetentionCountRejectsZeroValue(t *testing.T) {
	got, err := storeoptions.Apply(newConfigurationError, WithRetentionCount(RetentionCount{}))

	requireConfiguration(t, err)
	assert.Nil(t, got.RetentionCount)
}

func TestWithRetentionCountOverridesEarlierSetting(t *testing.T) {
	first, err := KeepLatest(3)
	require.NoError(t, err)
	last, err := KeepLatest(8)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		count RetentionCount
		want  int
	}{
		{"positive count", last, 8},
		{"no history", NoRetention(), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := storeoptions.Apply(newConfigurationError, WithRetentionCount(first), WithRetentionCount(tc.count))

			require.NoError(t, err)
			if tc.want == 0 {
				assert.Nil(t, got.RetentionCount)
			} else {
				require.NotNil(t, got.RetentionCount)
				assert.Equal(t, tc.want, *got.RetentionCount)
			}
		})
	}
}

func TestWithRetentionModeAcceptsDeleteAndTTL(t *testing.T) {
	positive, err := KeepLatest(2)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		count RetentionCount
	}{
		{"no history", NoRetention()},
		{"positive count", positive},
	} {
		for _, mode := range []RetentionMode{RetentionDelete, RetentionTTL} {
			t.Run(fmt.Sprintf("%s/mode=%d", tc.name, mode), func(t *testing.T) {
				got, err := storeoptions.Apply(newConfigurationError, WithRetentionMode(mode), WithRetentionCount(tc.count))

				require.NoError(t, err)
				assert.Equal(t, mode, RetentionMode(got.RetentionMode))
			})
		}
	}
}

func TestWithRetentionModeValidationDependsOnFinalCount(t *testing.T) {
	positive, err := KeepLatest(2)
	require.NoError(t, err)
	for _, mode := range []RetentionMode{0, -1, 99} {
		for _, modeFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("mode=%d/first=%t", mode, modeFirst), func(t *testing.T) {
				opts := []Option{WithRetentionMode(mode), WithRetentionCount(positive)}
				if !modeFirst {
					opts[0], opts[1] = opts[1], opts[0]
				}

				_, err := storeoptions.Apply(newConfigurationError, opts...)

				classified := requireConfiguration(t, err)
				assert.Contains(t, classified.Unwrap().Error(), fmt.Sprint(mode))

				opts = append(opts, WithRetentionCount(NoRetention()))
				got, err := storeoptions.Apply(newConfigurationError, opts...)

				require.NoError(t, err)
				assert.Nil(t, got.RetentionCount)
				assert.Equal(t, mode, RetentionMode(got.RetentionMode))
			})
		}
	}
}

func TestWithTTLGraceSecondsPreservesNonNegativeSeconds(t *testing.T) {
	for _, seconds := range []int64{0, 1, 86400, math.MaxInt64} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			got, err := storeoptions.Apply(newConfigurationError, WithTTLGraceSeconds(seconds))

			require.NoError(t, err)
			assert.Equal(t, seconds, got.TTLGraceSeconds)
		})
	}
}

func TestWithTTLGraceSecondsRejectsNegativeSeconds(t *testing.T) {
	for _, seconds := range []int64{-1, math.MinInt64} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			_, err := storeoptions.Apply(newConfigurationError, WithTTLGraceSeconds(seconds))

			classified := requireConfiguration(t, err)
			assert.Contains(t, classified.Unwrap().Error(), fmt.Sprint(seconds))
		})
	}
}

func TestWithRetentionFailureHandlerStoresWithoutCalling(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "caller context")
	cause := errors.New("retention failed")
	calls := 0
	var receivedValue any
	var receivedError error
	handler := func(ctx context.Context, err error) {
		calls++
		receivedValue = ctx.Value(contextKey{})
		receivedError = err
	}

	got, err := storeoptions.Apply(newConfigurationError, WithRetentionFailureHandler(handler))

	require.NoError(t, err)
	assert.Zero(t, calls)
	require.NotNil(t, got.RetentionFailureHandler)
	// Invoke the stored function only to verify its transfer, not store notification behavior.
	got.RetentionFailureHandler(ctx, cause)
	assert.Equal(t, 1, calls)
	assert.Equal(t, "caller context", receivedValue)
	assert.ErrorIs(t, receivedError, cause)
}

func TestWithRetentionFailureHandlerAcceptsNil(t *testing.T) {
	calls := 0
	got, err := storeoptions.Apply(newConfigurationError,
		WithRetentionFailureHandler(func(context.Context, error) { calls++ }),
		WithRetentionFailureHandler(nil),
	)

	require.NoError(t, err)
	assert.Nil(t, got.RetentionFailureHandler)
	assert.Zero(t, calls)
}

func TestWithRetentionFailureHandlerIsNotCalledOnConfigurationFailure(t *testing.T) {
	calls := 0
	_, err := storeoptions.Apply(newConfigurationError,
		WithRetentionFailureHandler(func(context.Context, error) { calls++ }),
		WithTTLGraceSeconds(-1),
	)

	requireConfiguration(t, err)
	assert.Zero(t, calls)
}

func TestNewConfigurationErrorPreservesCause(t *testing.T) {
	cause := errors.New("invalid setting")

	err := newConfigurationError(cause)

	requireConfiguration(t, err)
	assert.ErrorIs(t, err, cause)
}

func requireConfiguration(t *testing.T, err error) *ConfigurationError {
	t.Helper()
	require.Error(t, err)
	var classified *ConfigurationError
	require.ErrorAs(t, err, &classified)
	kind, ok := KindOf(err)
	require.True(t, ok)
	assert.Equal(t, KindConfiguration, kind)
	cause := errors.Unwrap(classified)
	require.NotNil(t, cause)
	assert.ErrorIs(t, err, cause)
	return classified
}
