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

func TestOperationInjection(t *testing.T) {
	// Given faults for distinct operations, When operation 2 is active, Then
	// only its real applications count and finishing detects an unfired fault.
	injection, finish := NewOperationInjection(2, []FaultSpec{
		countSpec(2, "commit", "sdk-error", 1),
		countSpec(1, "commit", "sdk-error", 1),
	})
	require.True(t, injection.Faults[0].TryApply())
	require.False(t, injection.Faults[1].TryApply())
	require.ErrorContains(t, finish(), "operation 1")
	require.False(t, injection.Faults[0].CanApply())

	injection, finish = NewOperationInjection(3, []FaultSpec{untilSpec(3, "commit", "storage-error")})
	require.True(t, injection.Faults[0].TryApply())
	require.True(t, injection.Faults[0].TryApply())
	require.NoError(t, finish())
	require.False(t, injection.Faults[0].TryApply())
}
