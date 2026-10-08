package eventstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Serializer serializes only payload or aggregate state, without envelope metadata.
// Domain values need no library-specific interface.
type Serializer[T any] interface {
	Serialize(value T) ([]byte, error)
	Deserialize(data []byte) (T, error)
}

type jsonSerializer[T any] struct{}

// NewJSONSerializer returns the default encoding/json serializer.
// Generic JSON numbers are restored as json.Number to retain their precision.
func NewJSONSerializer[T any]() Serializer[T] {
	return jsonSerializer[T]{}
}

func (jsonSerializer[T]) Serialize(value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, &SerializationError{Cause: err}
	}
	return data, nil
}

func (jsonSerializer[T]) Deserialize(data []byte) (T, error) {
	var value, zero T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return zero, &SerializationError{Cause: err}
	}
	// Decode requires exactly one JSON value, with only whitespace after it.
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return zero, &SerializationError{Cause: err}
	}
	return value, nil
}
