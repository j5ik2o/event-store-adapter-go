package testhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBigtablePhases covers the two phases that exist only for Bigtable (the 14 phases of the schema).
func TestBigtablePhases(t *testing.T) {
	t.Run("the journal-write phases are known", func(t *testing.T) {
		previous, ok := ParsePhase("journal-write-previous")
		require.True(t, ok)
		assert.Equal(t, PhaseJournalWritePrevious, previous)
		tail, ok := ParsePhase("journal-write-tail")
		require.True(t, ok)
		assert.Equal(t, PhaseJournalWriteTail, tail)
	})
	t.Run("all 14 phases are listed", func(t *testing.T) {
		assert.Len(t, Phases(), 14)
	})
	t.Run("a hook on journal-write-tail is not called for journal-write-previous", func(t *testing.T) {
		h := New()
		called := false
		h.OnFail(PhaseJournalWriteTail, func(Point) error { called = true; return nil })
		require.NoError(t, h.Fail(Point{Phase: PhaseJournalWritePrevious}))
		assert.False(t, called)
	})
}

func TestKeyHash(t *testing.T) {
	t.Run("without a registered override the hook reports no override", func(t *testing.T) {
		_, ok := New().KeyHash("Order-1")
		assert.False(t, ok, "an unregistered hook must not change the real hash")
	})
	t.Run("a registered override applies to the aggregate it chooses", func(t *testing.T) {
		h := New()
		h.OnKeyHash(func(aid string) (uint64, bool) {
			if aid == "Order-1" || aid == "Order-10" {
				return 1, true
			}
			return 0, false
		})
		v, ok := h.KeyHash("Order-1")
		assert.True(t, ok)
		assert.Equal(t, uint64(1), v)
		_, ok = h.KeyHash("Order-2")
		assert.False(t, ok, "other aggregates keep the real hash")
	})
	t.Run("a nil Hooks reports no override", func(t *testing.T) {
		var h *Hooks
		_, ok := h.KeyHash("Order-1")
		assert.False(t, ok)
	})
}
