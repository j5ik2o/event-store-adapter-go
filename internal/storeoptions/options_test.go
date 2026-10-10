package storeoptions_test

import (
	"context"
	"errors"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configurationError(cause error) error {
	return &eventstore.ConfigurationError{Cause: cause}
}

func TestApplyPublicOptionsFromAnotherPackage(t *testing.T) {
	count, err := eventstore.KeepLatest(5)
	require.NoError(t, err)
	calls := 0
	opts := []eventstore.Option{
		eventstore.WithRetentionCount(count),
		eventstore.WithRetentionMode(eventstore.RetentionTTL),
		eventstore.WithTTLGraceSeconds(42),
		eventstore.WithRetentionFailureHandler(func(context.Context, error) { calls++ }),
	}

	got, err := storeoptions.Apply(configurationError, opts...)

	require.NoError(t, err)
	require.NotNil(t, got.RetentionCount)
	assert.Equal(t, 5, *got.RetentionCount)
	assert.Equal(t, storeoptions.RetentionTTL, got.RetentionMode)
	assert.Equal(t, int64(42), got.TTLGraceSeconds)
	assert.NotNil(t, got.RetentionFailureHandler)
	assert.Zero(t, calls)
}

func TestApplyBuildsFreshSettings(t *testing.T) {
	count, err := eventstore.KeepLatest(3)
	require.NoError(t, err)
	opts := []eventstore.Option{
		eventstore.WithRetentionCount(count),
		eventstore.WithRetentionMode(eventstore.RetentionTTL),
		eventstore.WithTTLGraceSeconds(9),
	}
	first, err := storeoptions.Apply(configurationError, opts...)
	require.NoError(t, err)

	second, err := storeoptions.Apply[eventstore.Option](configurationError)

	require.NoError(t, err)
	assert.Nil(t, second.RetentionCount)
	assert.Equal(t, storeoptions.RetentionDelete, second.RetentionMode)
	assert.Zero(t, second.TTLGraceSeconds)
	require.NotNil(t, first.RetentionCount)
	assert.Equal(t, 3, *first.RetentionCount)
	assert.Equal(t, storeoptions.RetentionTTL, first.RetentionMode)
	assert.Equal(t, int64(9), first.TTLGraceSeconds)
}

func TestApplyKeepsHooksScopedToOneConstruction(t *testing.T) {
	hooks := testhook.New()
	first, err := storeoptions.Apply(configurationError, eventstore.Option(func(o *storeoptions.Options) error {
		o.Hooks = hooks
		return nil
	}))
	require.NoError(t, err)
	require.Same(t, hooks, first.Hooks)
	second, err := storeoptions.Apply[eventstore.Option](configurationError)
	require.NoError(t, err)
	require.Nil(t, second.Hooks)
}

func TestApplyRejectsInvalidCommonSettings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option eventstore.Option
	}{
		{"zero count", func(o *storeoptions.Options) error { n := 0; o.RetentionCount = &n; return nil }},
		{"negative count", func(o *storeoptions.Options) error { n := -1; o.RetentionCount = &n; return nil }},
		{"negative grace", func(o *storeoptions.Options) error { o.TTLGraceSeconds = -1; return nil }},
		{"unknown mode with history", func(o *storeoptions.Options) error {
			n := 1
			o.RetentionCount = &n
			o.RetentionMode = 99
			return nil
		}},
		{"nil option", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := storeoptions.Apply(configurationError, tc.option)

			var classified *eventstore.ConfigurationError
			require.ErrorAs(t, err, &classified)
			kind, ok := eventstore.KindOf(err)
			require.True(t, ok)
			assert.Equal(t, eventstore.KindConfiguration, kind)
			require.NotNil(t, classified.Unwrap())
			assert.ErrorIs(t, err, classified.Unwrap())
		})
	}
}

func TestApplyStopsAndPreservesOptionErrorCause(t *testing.T) {
	cause := errors.New("option rejected")
	classified := &eventstore.ConfigurationError{Cause: cause}
	calls := 0
	opts := []eventstore.Option{
		func(*storeoptions.Options) error { return classified },
		func(*storeoptions.Options) error { calls++; return nil },
	}

	_, err := storeoptions.Apply(configurationError, opts...)

	assert.ErrorIs(t, err, cause)
	var got *eventstore.ConfigurationError
	require.ErrorAs(t, err, &got)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok)
	assert.Equal(t, eventstore.KindConfiguration, kind)
	assert.ErrorIs(t, got.Unwrap(), cause)
	assert.Zero(t, calls)
}
