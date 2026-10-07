package conformance

import (
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countSpec(op int, phase, kind string, n int) FaultSpec {
	return FaultSpec{Operation: op, Phase: phase, Kind: kind, Repeat: "count", Count: n}
}

func untilSpec(op int, phase, kind string) FaultSpec {
	return FaultSpec{Operation: op, Phase: phase, Kind: kind, Repeat: "until-operation-finishes"}
}

func TestFault_Counting(t *testing.T) {
	t.Run("count applies exactly the given number of times and then stops", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "storage-error", 2)}, cur)
		require.Len(t, fs, 1)
		assert.True(t, fs[0].TryApply())
		assert.True(t, fs[0].TryApply())
		assert.False(t, fs[0].TryApply())
		assert.False(t, fs[0].TryApply())
		assert.Equal(t, 2, fs[0].Fired())
	})

	t.Run("until-operation-finishes keeps applying and counts every firing", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{untilSpec(1, "read-events", "storage-error")}, cur)
		for i := 0; i < 5; i++ {
			assert.True(t, fs[0].TryApply())
		}
		assert.Equal(t, 5, fs[0].Fired())
	})

	t.Run("a fault is applied only while its operation runs", func(t *testing.T) {
		cur := &operationCursor{}
		fs := newFaults([]FaultSpec{countSpec(2, "commit", "storage-error", 1)}, cur)
		cur.Set(0)
		assert.False(t, fs[0].TryApply())
		cur.Set(1)
		assert.False(t, fs[0].TryApply())
		cur.Set(2)
		assert.True(t, fs[0].TryApply())
		assert.Equal(t, 1, fs[0].Fired())
	})

	t.Run("operation 0 is applied while the store is created", func(t *testing.T) {
		cur := &operationCursor{}
		fs := newFaults([]FaultSpec{countSpec(0, "configuration-read", "sdk-error", 1)}, cur)
		assert.True(t, fs[0].TryApply())
	})

	t.Run("each fault has its own count", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{
			countSpec(1, "commit", "storage-error", 1),
			countSpec(1, "read-events", "storage-error", 1),
		}, cur)
		require.Len(t, fs, 2)
		assert.True(t, fs[0].TryApply())
		assert.Equal(t, 1, fs[0].Fired())
		assert.Equal(t, 0, fs[1].Fired())
	})
}

func TestVerifyFaults(t *testing.T) {
	cur := &operationCursor{}
	cur.Set(1)

	t.Run("count passes when fired exactly the given number of times", func(t *testing.T) {
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "storage-error", 2)}, cur)
		fs[0].TryApply()
		fs[0].TryApply()
		assert.Empty(t, verifyFaults(fs))
	})

	t.Run("count fails when fired fewer times, naming the fault", func(t *testing.T) {
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "storage-error", 2)}, cur)
		fs[0].TryApply()
		msg := verifyFaults(fs)
		assert.NotEmpty(t, msg)
		assert.Contains(t, msg, "commit")
	})

	t.Run("count fails when never fired", func(t *testing.T) {
		fs := newFaults([]FaultSpec{countSpec(1, "retention-delete", "storage-error", 1)}, cur)
		msg := verifyFaults(fs)
		assert.Contains(t, msg, "retention-delete")
	})

	t.Run("until-operation-finishes passes with one firing or more", func(t *testing.T) {
		fs := newFaults([]FaultSpec{untilSpec(1, "read-events", "storage-error")}, cur)
		fs[0].TryApply()
		assert.Empty(t, verifyFaults(fs))
		fs[0].TryApply()
		fs[0].TryApply()
		assert.Empty(t, verifyFaults(fs))
	})

	t.Run("until-operation-finishes fails with no firing", func(t *testing.T) {
		fs := newFaults([]FaultSpec{untilSpec(1, "read-events", "storage-error")}, cur)
		assert.Contains(t, verifyFaults(fs), "read-events")
	})

	t.Run("no faults is fine", func(t *testing.T) {
		assert.Empty(t, verifyFaults(nil))
	})

	t.Run("the failing fault is named even when others pass", func(t *testing.T) {
		fs := newFaults([]FaultSpec{
			countSpec(1, "commit", "storage-error", 1),
			untilSpec(1, "read-snapshot", "storage-error"),
		}, cur)
		fs[0].TryApply()
		msg := verifyFaults(fs)
		assert.Contains(t, msg, "read-snapshot")
	})
}

func TestRegisterHookFaults(t *testing.T) {
	phase := func(s string) testhook.Phase {
		p, ok := testhook.ParsePhase(s)
		require.True(t, ok, s)
		return p
	}

	t.Run("a hook fault makes the matching phase fail and counts the firing", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "storage-error", 1)}, cur)
		h := testhook.New()
		registerHookFaults(h, fs)
		assert.Error(t, h.Fail(testhook.Point{Phase: phase("commit"), AggregateID: "Order-1", SeqNr: 1}))
		assert.Equal(t, 1, fs[0].Fired())
		assert.NoError(t, h.Fail(testhook.Point{Phase: phase("commit"), AggregateID: "Order-1", SeqNr: 2}))
		assert.Equal(t, 1, fs[0].Fired())
	})

	t.Run("a serialization fault fails the serialize phase", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(1, "serialize-event", "serialization-error", 1)}, cur)
		h := testhook.New()
		registerHookFaults(h, fs)
		assert.Error(t, h.Fail(testhook.Point{Phase: phase("serialize-event")}))
		assert.Equal(t, 1, fs[0].Fired())
	})

	t.Run("a hook of another phase does not fire the fault", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "storage-error", 1)}, cur)
		h := testhook.New()
		registerHookFaults(h, fs)
		assert.NoError(t, h.Fail(testhook.Point{Phase: phase("read-events")}))
		assert.Equal(t, 0, fs[0].Fired())
	})

	t.Run("a hook fault outside its operation does not fire", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(2, "commit", "storage-error", 1)}, cur)
		h := testhook.New()
		registerHookFaults(h, fs)
		assert.NoError(t, h.Fail(testhook.Point{Phase: phase("commit")}))
		assert.Equal(t, 0, fs[0].Fired())
	})

	t.Run("an sdk fault is left to the backend and is not registered on the hooks", func(t *testing.T) {
		cur := &operationCursor{}
		cur.Set(1)
		fs := newFaults([]FaultSpec{countSpec(1, "commit", "sdk-error", 1)}, cur)
		h := testhook.New()
		registerHookFaults(h, fs)
		assert.NoError(t, h.Fail(testhook.Point{Phase: phase("commit")}))
		assert.Equal(t, 0, fs[0].Fired())
	})
}
