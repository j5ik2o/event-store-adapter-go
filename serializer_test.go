package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONSerializerDomainRoundTrip(t *testing.T) {
	type domain struct {
		Name    string   `json:"name"`
		Count   int64    `json:"count"`
		Enabled bool     `json:"enabled"`
		Tags    []string `json:"tags"`
	}
	want := domain{Name: "注文", Count: math.MaxInt64, Enabled: true, Tags: []string{"second", "first"}}
	serializer := NewJSONSerializer[domain]()
	data, err := serializer.Serialize(want)
	require.NoError(t, err)
	got, err := serializer.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestJSONSerializerPayloadOnly(t *testing.T) {
	payload := map[string]any{
		"aggregate_id": "domain-id", "seq_nr": "domain-number", "occurred_at": "domain-time",
		"manifest": "domain-manifest", "nested": []any{true, "1", json.Number("1"), nil},
	}
	event, err := NewEventEnvelope(userID{"Order", "library-id"}, 7, time.Unix(0, 0), payload, WithManifest("library-event"))
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope(payload, 3, WithManifest("library-snapshot"))
	require.NoError(t, err)
	serializer := NewJSONSerializer[map[string]any]()
	for name, value := range map[string]map[string]any{"event": event.Payload(), "snapshot": snapshot.Aggregate()} {
		t.Run(name, func(t *testing.T) {
			data, err := serializer.Serialize(value)
			require.NoError(t, err)
			assert.JSONEq(t, `{"aggregate_id":"domain-id","seq_nr":"domain-number","occurred_at":"domain-time","manifest":"domain-manifest","nested":[true,"1",1,null]}`, string(data))
			restored, err := serializer.Deserialize(data)
			require.NoError(t, err)
			assert.Equal(t, payload, restored)
		})
	}
}

func TestJSONSerializerJSONValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"null", nil}, {"false", false}, {"true", true}, {"string", "1"}, {"empty string", ""},
		{"zero", json.Number("0")}, {"integer", json.Number("9007199254740993")},
		{"decimal", json.Number("12345678901234567890.1234567890123456789")},
		{"exponent", json.Number("-1.234567890123456789e+1000")},
		{"empty array", []any{}}, {"empty object", map[string]any{}},
		{"nested", map[string]any{"e\u0301": "e\u0301", "é": "é", "values": []any{json.Number("2"), "1", false, nil, []any{json.Number("1"), json.Number("0")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serializer := NewJSONSerializer[any]()
			data, err := serializer.Serialize(tc.value)
			require.NoError(t, err)
			got, err := serializer.Deserialize(data)
			require.NoError(t, err)
			assert.Equal(t, tc.value, got, "JSON types, array order and exact numbers must survive")
		})
	}
}

func TestJSONSerializerWhitespaceAndKeyOrder(t *testing.T) {
	serializer := NewJSONSerializer[any]()
	want := map[string]any{"a": json.Number("1"), "b": []any{nil, false}}
	for _, input := range []string{`{"a":1,"b":[null,false]}`, " \n { \"b\" : [null, false], \"a\" : 1 } \t\n"} {
		got, err := serializer.Deserialize([]byte(input))
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestJSONSerializerTypedNull(t *testing.T) {
	type domain struct{ Name string }
	serializer := NewJSONSerializer[*domain]()
	data, err := serializer.Serialize(nil)
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
	got, err := serializer.Deserialize(data)
	require.NoError(t, err)
	assert.Nil(t, got)

	rawSerializer := NewJSONSerializer[json.RawMessage]()
	raw, err := rawSerializer.Deserialize([]byte("null"))
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage("null"), raw)
}

func TestJSONSerializerDeserializeFailures(t *testing.T) {
	for _, input := range []string{"", " \n\t", "{", "[1,]", "NaN", "Infinity", `{"password":"private-password"`, "null null", "1 2", "{} trailing", "{} {"} {
		t.Run(input, func(t *testing.T) {
			got, err := NewJSONSerializer[any]().Deserialize([]byte(input))
			assert.Nil(t, got)
			failure := requireSerializationFailure(t, err)
			require.NotNil(t, failure.Unwrap())
			assert.NotContains(t, err.Error(), "private-password")
		})
	}
	got, err := NewJSONSerializer[any]().Deserialize(nil)
	assert.Nil(t, got)
	requireSerializationFailure(t, err)
}

func TestJSONSerializerDoesNotReturnPartiallyRestoredValue(t *testing.T) {
	type domain struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	got, err := NewJSONSerializer[domain]().Deserialize([]byte(`{"name":"private-payload","count":"private-password"}`))
	assert.Equal(t, domain{}, got)
	failure := requireSerializationFailure(t, err)
	var mismatch *json.UnmarshalTypeError
	require.ErrorAs(t, failure, &mismatch)
	assert.NotContains(t, err.Error(), "private-payload")
	assert.NotContains(t, err.Error(), "private-password")
}

func TestJSONSerializerSerializeUnsupportedValues(t *testing.T) {
	for _, value := range []any{make(chan int), math.NaN(), math.Inf(1), json.RawMessage("{")} {
		t.Run(fmt.Sprintf("%T/%v", value, value), func(t *testing.T) {
			data, err := NewJSONSerializer[any]().Serialize(value)
			assert.Nil(t, data)
			failure := requireSerializationFailure(t, err)
			require.NotNil(t, failure.Unwrap())
		})
	}
}

type marshalFailure struct{ cause error }

func (v marshalFailure) MarshalJSON() ([]byte, error) { return nil, v.cause }

type unmarshalFailure struct{}

func (*unmarshalFailure) UnmarshalJSON([]byte) error { return &sensitiveCause{} }

func TestJSONSerializerMarshalCauseAndSafeMessage(t *testing.T) {
	cause := &sensitiveCause{}
	data, err := NewJSONSerializer[marshalFailure]().Serialize(marshalFailure{cause: cause})
	assert.Nil(t, data)
	failure := requireSerializationFailure(t, err)
	var marshalerError *json.MarshalerError
	require.ErrorAs(t, failure, &marshalerError)
	assert.Same(t, marshalerError, errors.Unwrap(failure))
	assert.ErrorIs(t, failure, cause)
	assert.NotContains(t, failure.Error(), "private-")
	assert.Zero(t, cause.calls, "the outer message must not evaluate the cause's text")
}

func TestJSONSerializerUnmarshalCauseAndSafeMessage(t *testing.T) {
	_, err := NewJSONSerializer[unmarshalFailure]().Deserialize([]byte(`{"password":"private-password"}`))
	failure := requireSerializationFailure(t, err)
	var cause *sensitiveCause
	require.ErrorAs(t, failure, &cause)
	assert.Same(t, cause, errors.Unwrap(failure))
	assert.ErrorIs(t, failure, cause)
	assert.NotContains(t, failure.Error(), "private-")
	assert.Zero(t, cause.calls)
}

func TestEnvelopesDoNotSerializeDomainValues(t *testing.T) {
	cause := &sensitiveCause{}
	payload := marshalFailure{cause: cause}
	event, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), payload)
	require.NoError(t, err)
	snapshot, err := NewSnapshotEnvelope(payload, 0)
	require.NoError(t, err)
	serializer := NewJSONSerializer[marshalFailure]()
	for _, value := range []marshalFailure{event.Payload(), snapshot.Aggregate()} {
		_, err := serializer.Serialize(value)
		requireSerializationFailure(t, err)
		assert.ErrorIs(t, err, cause)
	}
}

func requireSerializationFailure(t *testing.T, err error) *SerializationError {
	t.Helper()
	require.Error(t, err)
	var failure *SerializationError
	for _, wrapped := range []error{err, fmt.Errorf("outer: %w", err)} {
		require.ErrorAs(t, wrapped, &failure)
		kind, ok := KindOf(wrapped)
		require.True(t, ok)
		assert.Equal(t, KindSerialization, kind)
	}
	return failure
}
