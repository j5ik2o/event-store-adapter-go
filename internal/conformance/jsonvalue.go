package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
)

// decodeStrictJSON decodes a single JSON value. Numbers are kept as json.Number,
// duplicate keys in the same object, NaN, Infinity and trailing data are rejected.
func decodeStrictJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("unexpected data after the top-level JSON value")
		}
		return nil, err
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch delim {
	case '{':
		obj := map[string]any{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("object key is not a string: %v", keyTok)
			}
			if _, dup := obj[key]; dup {
				return nil, fmt.Errorf("duplicate key %q", key)
			}
			val, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			obj[key] = val
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return obj, nil
	case '[':
		arr := []any{}
		for dec.More() {
			val, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, val)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return arr, nil
	}
	return nil, fmt.Errorf("unexpected delimiter %v", delim)
}

// bigIntFromJSONNumber converts a json.Number that is an integer literal into a big.Int.
func bigIntFromJSONNumber(v any) (*big.Int, error) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, fmt.Errorf("expected a JSON number, got %T", v)
	}
	i, ok := new(big.Int).SetString(n.String(), 10)
	if !ok {
		return nil, fmt.Errorf("%q is not an integer literal", n.String())
	}
	return i, nil
}

var decimalString = regexp.MustCompile(`^-?[0-9]+$`)

// bigIntFromDecimalString converts a decimal integer string into a big.Int.
func bigIntFromDecimalString(v any) (*big.Int, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected a decimal string, got %T", v)
	}
	if !decimalString.MatchString(s) {
		return nil, fmt.Errorf("%q is not a decimal integer string", s)
	}
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("%q is not a decimal integer string", s)
	}
	return i, nil
}
