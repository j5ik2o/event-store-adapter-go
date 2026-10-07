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

	t.Run("integer values spelled with a fraction or exponent are accepted", func(t *testing.T) {
		for s, want := range map[string]string{"1.0": "1", "1e3": "1000", "-1.0e0": "-1", "12E1": "120"} {
			n, err := bigIntFromJSONNumber(json.Number(s))
			require.NoError(t, err, s)
			assert.Equal(t, want, n.String(), s)
		}
	})

	t.Run("numbers with a fractional part are rejected", func(t *testing.T) {
		for _, s := range []string{"0.5", "1e-1", "1.5"} {
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

func TestDecodeStrictJSON_InvalidUTF8(t *testing.T) {
	for name, in := range map[string][]byte{
		"string value": {'"', 0xff, '"'},
		"object key":   append(append([]byte(`{"`), 0xc3), []byte(`":1}`)...),
	} {
		_, err := decodeStrictJSON(in)
		assert.Error(t, err, name)
	}
	_, err := decodeStrictJSON([]byte(`"あ"`))
	assert.NoError(t, err)
}

func TestDecodeStrictJSON_Surrogate(t *testing.T) {
	t.Run("a paired surrogate escape is accepted", func(t *testing.T) {
		v, err := decodeStrictJSON([]byte(`"😀"`))
		require.NoError(t, err)
		assert.Equal(t, "😀", v)
	})

	t.Run("an escaped backslash before ud800 is a plain string", func(t *testing.T) {
		v, err := decodeStrictJSON([]byte(`"\\ud800"`))
		require.NoError(t, err)
		assert.Equal(t, `\ud800`, v)
	})

	for _, in := range []string{`"\ud800"`, `"\ud800A"`, `"\ud800x"`} {
		t.Run("an unpaired high surrogate is rejected: "+in, func(t *testing.T) {
			v, err := decodeStrictJSON([]byte(in))
			require.Error(t, err)
			assert.Nil(t, v)
		})
	}

	for _, in := range []string{`"\udc00"`, `{"\ud800":1}`} {
		t.Run("a lone low surrogate or a high surrogate in a key is rejected: "+in, func(t *testing.T) {
			_, err := decodeStrictJSON([]byte(in))
			assert.Error(t, err)
		})
	}
}
