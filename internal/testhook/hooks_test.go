package testhook

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustPhase(t *testing.T, s string) Phase {
	t.Helper()
	p, ok := ParsePhase(s)
	require.True(t, ok, s)
	return p
}

func TestPhases(t *testing.T) {
	want := []string{
		"serialize-event", "serialize-snapshot", "deserialize-event", "deserialize-snapshot",
		"commit", "read-events", "read-snapshot", "retention-query", "retention-delete",
		"retention-mark", "configuration-read", "configuration-create",
	}
	t.Run("the 12 phases of the schema are known", func(t *testing.T) {
		assert.Len(t, Phases(), 12)
		for _, s := range want {
			_, ok := ParsePhase(s)
			assert.True(t, ok, s)
		}
	})
	t.Run("an unknown phase is rejected", func(t *testing.T) {
		_, ok := ParsePhase("read-interleave")
		assert.False(t, ok)
		_, ok = ParsePhase("")
		assert.False(t, ok)
	})
}

func TestHooks_Fail(t *testing.T) {
	commit := mustPhase(t, "commit")
	readEvents := mustPhase(t, "read-events")

	t.Run("hooks run in registration order and the first error is returned", func(t *testing.T) {
		h := New()
		var order []int
		e1, e2 := errors.New("first"), errors.New("second")
		h.OnFail(commit, func(Point) error { order = append(order, 1); return e1 })
		h.OnFail(commit, func(Point) error { order = append(order, 2); return e2 })
		err := h.Fail(Point{Phase: commit, AggregateID: "Order-1", SeqNr: 3})
		assert.ErrorIs(t, err, e1)
		assert.Equal(t, []int{1}, order)
	})

	t.Run("a later hook runs when the earlier one returns nil", func(t *testing.T) {
		h := New()
		e2 := errors.New("second")
		h.OnFail(commit, func(Point) error { return nil })
		h.OnFail(commit, func(Point) error { return e2 })
		assert.ErrorIs(t, h.Fail(Point{Phase: commit}), e2)
	})

	t.Run("the point is passed through unchanged", func(t *testing.T) {
		h := New()
		var got Point
		h.OnFail(commit, func(p Point) error { got = p; return nil })
		in := Point{Phase: commit, AggregateID: "Order-1", SeqNr: 7, Payload: []byte(`{"a":1}`)}
		require.NoError(t, h.Fail(in))
		assert.Equal(t, in, got)
	})

	t.Run("hooks of another phase are not called", func(t *testing.T) {
		h := New()
		called := false
		h.OnFail(commit, func(Point) error { called = true; return errors.New("x") })
		assert.NoError(t, h.Fail(Point{Phase: readEvents}))
		assert.False(t, called)
	})

	t.Run("a nil Hooks is safe and does nothing", func(t *testing.T) {
		var h *Hooks
		assert.NoError(t, h.Fail(Point{Phase: commit}))
		assert.False(t, h.Now().IsZero())
		assert.NotPanics(t, func() { h.Sleep(0) })
		pages, ok, err := h.HistoryPages("Order-1", 1)
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, pages)
	})
}

func TestHooks_ClockAndSleeper(t *testing.T) {
	t.Run("Now returns the injected clock", func(t *testing.T) {
		h := New()
		fixed := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
		h.SetClock(func() time.Time { return fixed })
		assert.Equal(t, fixed, h.Now())
	})
	t.Run("Now falls back to the real clock when none is injected", func(t *testing.T) {
		assert.WithinDuration(t, time.Now(), New().Now(), time.Minute)
	})
	t.Run("Sleep calls the injected sleeper instead of waiting", func(t *testing.T) {
		h := New()
		var got []time.Duration
		h.SetSleeper(func(d time.Duration) { got = append(got, d) })
		start := time.Now()
		h.Sleep(time.Hour)
		assert.Equal(t, []time.Duration{time.Hour}, got)
		assert.Less(t, time.Since(start), time.Minute)
	})
}

func TestHooks_HistoryPages(t *testing.T) {
	t.Run("without a hook, ok is false", func(t *testing.T) {
		pages, ok, err := New().HistoryPages("Order-1", 2)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, pages)
	})
	t.Run("the registered hook receives aid and just-written seq_nr", func(t *testing.T) {
		h := New()
		var gotAid string
		var gotSeq int64
		h.OnHistoryPages(func(aid string, justWritten int64) ([][]int64, bool, error) {
			gotAid, gotSeq = aid, justWritten
			return [][]int64{{2, 1}}, true, nil
		})
		pages, ok, err := h.HistoryPages("Order-1", 2)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]int64{{2, 1}}, pages)
		assert.Equal(t, "Order-1", gotAid)
		assert.Equal(t, int64(2), gotSeq)
	})
	t.Run("the hook error is returned", func(t *testing.T) {
		h := New()
		boom := errors.New("boom")
		h.OnHistoryPages(func(string, int64) ([][]int64, bool, error) { return nil, false, boom })
		_, _, err := h.HistoryPages("Order-1", 2)
		assert.ErrorIs(t, err, boom)
	})
}

func TestHooks_ProvideHistory(t *testing.T) {
	t.Run("History reads through the provided reader", func(t *testing.T) {
		h := New()
		boom := errors.New("no such aggregate")
		var gotAid string
		h.ProvideHistory(func(aid string) (History, error) { gotAid = aid; return History{}, boom })
		_, err := h.History("Order-9")
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, "Order-9", gotAid)
	})
	t.Run("History without a reader is an error", func(t *testing.T) {
		_, err := New().History("Order-9")
		assert.Error(t, err)
	})
}

// The hook types must not depend on the core types: non-test files import the standard library only.
func TestImportsAreStandardLibraryOnly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		af, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imp := range af.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			first := strings.SplitN(p, "/", 2)[0]
			assert.NotContains(t, first, ".", "%s imports %s", f, p)
		}
		checked++
	}
	assert.Positive(t, checked)
}
