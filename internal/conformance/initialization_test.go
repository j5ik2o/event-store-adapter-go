package conformance

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitializationInjection(t *testing.T) {
	// Given operation 0 faults, When opening finishes, Then existing accounting
	// checks exact and minimum counts and prevents later firing.
	injection, finish := NewInitializationInjection([]FaultSpec{
		countSpec(0, "configuration-read", "sdk-response", 1),
		untilSpec(0, "configuration-create", "sdk-error"),
	})
	require.NotNil(t, injection.Hooks)
	require.True(t, injection.Faults[0].TryApply())
	require.False(t, injection.Faults[0].TryApply())
	require.True(t, injection.Faults[1].TryApply())
	require.NoError(t, finish())
	require.False(t, injection.Faults[1].TryApply())

	injection, finish = NewInitializationInjection([]FaultSpec{countSpec(0, "configuration-read", "sdk-response", 1)})
	require.Zero(t, injection.Faults[0].Fired())
	require.ErrorContains(t, finish(), "fired 0 times")

	injection, finish = NewInitializationInjection([]FaultSpec{countSpec(1, "commit", "sdk-error", 1)})
	require.False(t, injection.Faults[0].TryApply())
	require.ErrorContains(t, finish(), "operation 1")
}

func TestInitializationInjectionConcurrentCounts(t *testing.T) {
	injection, finish := NewInitializationInjection([]FaultSpec{countSpec(0, "configuration-read", "sdk-response", 7)})
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Go(func() { injection.Faults[0].TryApply() })
	}
	workers.Wait()
	require.Equal(t, 7, injection.Faults[0].Fired())
	require.NoError(t, finish())
}
