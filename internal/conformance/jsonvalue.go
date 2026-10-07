package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"unicode/utf8"
)

// decodeStrictJSON decodes a single JSON value. Numbers are kept as json.Number,
// duplicate keys in the same object, NaN, Infinity and trailing data are rejected.
func decodeStrictJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("JSON is not valid UTF-8")
	}
	if err := checkSurrogateEscapes(data); err != nil {
		return nil, err
	}
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

// bigIntFromJSONNumber converts a json.Number whose mathematical value is an integer
// (1, 1.0 and 1e3 included) into a big.Int. Values with a fractional part are rejected.
func bigIntFromJSONNumber(v any) (*big.Int, error) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, fmt.Errorf("expected a JSON number, got %T", v)
	}
	r, ok := new(big.Rat).SetString(n.String())
	if !ok || !r.IsInt() {
		return nil, fmt.Errorf("%q is not an integer", n.String())
	}
	return new(big.Int).Set(r.Num()), nil
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

// checkSurrogateEscapes rejects \uXXXX escapes of surrogates that are not a high surrogate
// immediately followed by a low surrogate escape. encoding/json would silently replace them
// with U+FFFD. It covers object keys as well as string values.
func checkSurrogateEscapes(data []byte) error {
	inString := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if !inString {
			inString = c == '"'
			continue
		}
		switch c {
		case '"':
			inString = false
		case '\\':
			if i+1 >= len(data) {
				return nil
			}
			if data[i+1] != 'u' {
				i++ // skips the escaped character, including an escaped backslash
				continue
			}
			r, ok := hex4(data, i+2)
			if !ok {
				return nil // malformed escape: the decoder reports it
			}
			i += 5
			switch {
			case r >= 0xDC00 && r <= 0xDFFF:
				return fmt.Errorf("unpaired low surrogate escape \\u%04x", r)
			case r >= 0xD800 && r <= 0xDBFF:
				if i+2 < len(data) && data[i+1] == '\\' && data[i+2] == 'u' {
					if low, ok := hex4(data, i+3); ok && low >= 0xDC00 && low <= 0xDFFF {
						i += 6
						continue
					}
				}
				return fmt.Errorf("unpaired high surrogate escape \\u%04x", r)
			}
		}
	}
	return nil
}

func hex4(data []byte, at int) (rune, bool) {
	if at+4 > len(data) {
		return 0, false
	}
	var r rune
	for _, b := range data[at : at+4] {
		switch {
		case b >= '0' && b <= '9':
			r = r<<4 | rune(b-'0')
		case b >= 'a' && b <= 'f':
			r = r<<4 | rune(b-'a'+10)
		case b >= 'A' && b <= 'F':
			r = r<<4 | rune(b-'A'+10)
		default:
			return 0, false
		}
	}
	return r, true
}
