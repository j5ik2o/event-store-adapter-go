package conformance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeStrictJSON(t *testing.T) {
	t.Run("nested same-name keys are not duplicates", func(t *testing.T) {
		v, err := decodeStrictJSON([]byte(`{"a":1,"b":{"a":2}}`))
		require.NoError(t, err)
		m, ok := v.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, json.Number("1"), m["a"])
		inner, ok := m["b"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, json.Number("2"), inner["a"])
	})

	t.Run("duplicate key at the same level is rejected", func(t *testing.T) {
		v, err := decodeStrictJSON([]byte(`{"a":1,"a":2}`))
		require.Error(t, err)
		assert.Nil(t, v)
	})

	for _, in := range []string{`{"a":NaN}`, `{"a":Infinity}`, `{"a":1} {"b":2}`} {
		t.Run("rejects "+in, func(t *testing.T) {
			_, err := decodeStrictJSON([]byte(in))
			assert.Error(t, err)
		})
	}
}

func TestBigInt(t *testing.T) {
	t.Run("json number keeps arbitrary precision", func(t *testing.T) {
		n, err := bigIntFromJSONNumber(json.Number("9007199254740993"))
		require.NoError(t, err)
		assert.Equal(t, "9007199254740993", n.String())
		n, err = bigIntFromJSONNumber(json.Number("-1"))
		require.NoError(t, err)
		assert.Equal(t, "-1", n.String())
	})

	t.Run("non-integer numbers are rejected", func(t *testing.T) {
		for _, s := range []string{"1.0", "1e3", "0.5"} {
			_, err := bigIntFromJSONNumber(json.Number(s))
			assert.Error(t, err, s)
		}
	})

	t.Run("non json number values are rejected", func(t *testing.T) {
		_, err := bigIntFromJSONNumber("1")
		assert.Error(t, err)
		_, err = bigIntFromJSONNumber(float64(1))
		assert.Error(t, err)
	})

	t.Run("decimal string beyond int64 is kept", func(t *testing.T) {
		n, err := bigIntFromDecimalString("-9223372036854775809")
		require.NoError(t, err)
		assert.Equal(t, "-9223372036854775809", n.String())
	})

	t.Run("malformed decimal strings are rejected", func(t *testing.T) {
		for _, s := range []string{"", "1.0", "0x10", " 1", "1e3", "+1", "--1"} {
			_, err := bigIntFromDecimalString(s)
			assert.Error(t, err, s)
		}
		_, err := bigIntFromDecimalString(json.Number("1"))
		assert.Error(t, err)
	})
}
