package conformance_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

func TestHookSerializerPreservesPayloadAndFailureCause(t *testing.T) {
	hooks := testhook.New()
	serializer := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}
	payload := json.RawMessage(`{"number":9007199254740993,"label":"日本語"}`)
	encoded, err := serializer.Serialize(payload)
	require.NoError(t, err)
	decoded, err := serializer.Deserialize(encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	cause := errors.New("declared serializer fault")
	hooks.OnFail(testhook.PhaseSerializeEvent, func(testhook.Point) error { return cause })
	_, err = serializer.Serialize(payload)
	require.ErrorIs(t, err, cause)
	hooks.OnFail(testhook.PhaseDeserializeEvent, func(testhook.Point) error { return cause })
	_, err = serializer.Deserialize(encoded)
	require.ErrorIs(t, err, cause)
}
