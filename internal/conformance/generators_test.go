package conformance

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func genCase(payload map[string]any, gens ...map[string]any) map[string]any {
	gl := make([]any, 0, len(gens))
	for _, g := range gens {
		gl = append(gl, g)
	}
	return map[string]any{
		"id": "c",
		"fixtures": map[string]any{
			"events": map[string]any{"e1": map[string]any{"payload": payload}},
		},
		"store":      map[string]any{"retention_mode": ""},
		"generators": gl,
	}
}

func gen(target, ch string, n int) map[string]any {
	return map[string]any{"target": target, "character": ch, "byte_length": json.Number(strconv.Itoa(n))}
}

func payloadOf(t *testing.T, c map[string]any) map[string]any {
	t.Helper()
	return c["fixtures"].(map[string]any)["events"].(map[string]any)["e1"].(map[string]any)["payload"].(map[string]any)
}

func TestMaterialize(t *testing.T) {
	t.Run("escaped pointers are decoded and the original is untouched", func(t *testing.T) {
		c := genCase(map[string]any{"a/b": "", "m~n": ""},
			gen("/fixtures/events/e1/payload/a~1b", "x", 3),
			gen("/fixtures/events/e1/payload/m~0n", "あ", 6))
		out, err := materialize(c)
		require.NoError(t, err)
		p := payloadOf(t, out)
		assert.Equal(t, "xxx", p["a/b"])
		assert.Equal(t, "ああ", p["m~n"])
		orig := payloadOf(t, c)
		assert.Equal(t, "", orig["a/b"])
		assert.Equal(t, "", orig["m~n"])
	})

	t.Run("invalid targets are rejected", func(t *testing.T) {
		for name, target := range map[string]string{
			"non-empty value":  "/fixtures/events/e1/payload/v",
			"missing location": "/fixtures/events/e1/payload/missing",
			"outside fixtures": "/store/retention_mode",
		} {
			c := genCase(map[string]any{"v": "abc"}, gen(target, "x", 3))
			_, err := materialize(c)
			assert.Error(t, err, name)
		}
	})

	t.Run("bad escape, duplicate pointer, indivisible length are rejected", func(t *testing.T) {
		_, err := materialize(genCase(map[string]any{"a": ""}, gen("/fixtures/events/e1/payload/a~2", "x", 3)))
		assert.Error(t, err, "bad escape")

		_, err = materialize(genCase(map[string]any{"a": ""},
			gen("/fixtures/events/e1/payload/a", "x", 3),
			gen("/fixtures/events/e1/payload/a", "x", 3)))
		assert.Error(t, err, "duplicate")

		_, err = materialize(genCase(map[string]any{"a": ""}, gen("/fixtures/events/e1/payload/a", "あ", 10)))
		assert.Error(t, err, "indivisible")
	})
}

func TestLoadData_Generators(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	var found bool
	for _, s := range d.Scenarios {
		if s.Materialized == nil {
			continue
		}
		fx, ok := s.Materialized["fixtures"].(map[string]any)
		if !ok {
			continue
		}
		evs, _ := fx["events"].(map[string]any)
		e1, _ := evs["e1"].(map[string]any)
		pl, _ := e1["payload"].(map[string]any)
		text, ok := pl["text"].(string)
		if ok && len(text) == 320000 {
			found = true
			assert.Equal(t, 320000, len(text))
			assert.Equal(t, "x", text[:1])
			assert.Equal(t, "x", text[len(text)-1:])
		}
	}
	assert.True(t, found, "expanded 320000-byte payload text must exist")
}

func TestMaterialize_Character(t *testing.T) {
	t.Run("U+FFFD is a valid character", func(t *testing.T) {
		c := genCase(map[string]any{"a": ""}, gen("/fixtures/events/e1/payload/a", "\uFFFD", 6))
		out, err := materialize(c)
		require.NoError(t, err)
		assert.Equal(t, "\uFFFD\uFFFD", payloadOf(t, out)["a"])
	})

	t.Run("an invalid one-byte encoding is rejected", func(t *testing.T) {
		c := genCase(map[string]any{"a": ""}, gen("/fixtures/events/e1/payload/a", "\xff", 1))
		_, err := materialize(c)
		assert.Error(t, err)
	})
}

func TestMaterialize_ByteLengthSpelling(t *testing.T) {
	build := func(n string) map[string]any {
		return genCase(map[string]any{"a": ""}, map[string]any{
			"target": "/fixtures/events/e1/payload/a", "character": "x", "byte_length": json.Number(n),
		})
	}
	for n, want := range map[string]string{"6.0": "xxxxxx", "3e0": "xxx"} {
		out, err := materialize(build(n))
		require.NoError(t, err, n)
		assert.Equal(t, want, payloadOf(t, out)["a"], n)
	}
	for _, n := range []string{"1.5", "0", "-1", "1e30"} {
		_, err := materialize(build(n))
		assert.Error(t, err, n)
	}
}
