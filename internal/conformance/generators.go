package conformance

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// decodePointer splits a JSON Pointer into reference tokens, decoding ~1 and ~0.
func decodePointer(p string) ([]string, error) {
	if !strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("pointer %q must start with /", p)
	}
	parts := strings.Split(p[1:], "/")
	for i, part := range parts {
		for j := 0; j < len(part); j++ {
			if part[j] == '~' && (j+1 >= len(part) || (part[j+1] != '0' && part[j+1] != '1')) {
				return nil, fmt.Errorf("pointer %q has an invalid escape", p)
			}
		}
		part = strings.ReplaceAll(part, "~1", "/")
		parts[i] = strings.ReplaceAll(part, "~0", "~")
	}
	return parts, nil
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, x := range t {
			m[k] = deepCopy(x)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, x := range t {
			s[i] = deepCopy(x)
		}
		return s
	default:
		return v
	}
}

// materialize returns a deep copy of the case with every generator applied.
// The given case is not modified.
func materialize(c map[string]any) (map[string]any, error) {
	out := deepCopy(c).(map[string]any)
	raw, has := out["generators"]
	if !has {
		return out, nil
	}
	gens, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("generators is not an array")
	}
	seen := map[string]bool{}
	for i, item := range gens {
		g, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("generators[%d] is not an object", i)
		}
		target, ok := g["target"].(string)
		if !ok {
			return nil, fmt.Errorf("generators[%d].target is not a string", i)
		}
		if seen[target] {
			return nil, fmt.Errorf("generators[%d]: target %q is specified more than once", i, target)
		}
		seen[target] = true
		tokens, err := decodePointer(target)
		if err != nil {
			return nil, fmt.Errorf("generators[%d]: %w", i, err)
		}
		if len(tokens) < 3 || tokens[0] != "fixtures" || (tokens[1] != "events" && tokens[1] != "snapshots") {
			return nil, fmt.Errorf("generators[%d]: target %q is outside fixtures events/snapshots", i, target)
		}
		chStr, ok := g["character"].(string)
		if !ok || utf8.RuneCountInString(chStr) != 1 {
			return nil, fmt.Errorf("generators[%d]: character must be one Unicode character", i)
		}
		r, size := utf8.DecodeRuneInString(chStr)
		if r == utf8.RuneError && size <= 1 {
			return nil, fmt.Errorf("generators[%d]: character is not valid UTF-8", i)
		}
		num, ok := g["byte_length"].(json.Number)
		if !ok {
			return nil, fmt.Errorf("generators[%d].byte_length is not a number", i)
		}
		n, err := bigIntFromJSONNumber(num)
		if err != nil || n.Sign() <= 0 || !n.IsInt64() || int64(int(n.Int64())) != n.Int64() {
			return nil, fmt.Errorf("generators[%d].byte_length %q is not a positive integer", i, num)
		}
		length := int(n.Int64())
		if length%utf8.RuneLen(r) != 0 {
			return nil, fmt.Errorf("generators[%d]: byte_length %d is not a multiple of the %d-byte character", i, length, utf8.RuneLen(r))
		}
		if err := setGenerated(out, tokens, strings.Repeat(chStr, length/utf8.RuneLen(r))); err != nil {
			return nil, fmt.Errorf("generators[%d]: target %q: %w", i, target, err)
		}
	}
	return out, nil
}

func setGenerated(root map[string]any, tokens []string, value string) error {
	var cur any = root
	for _, tok := range tokens[:len(tokens)-1] {
		next, err := child(cur, tok)
		if err != nil {
			return err
		}
		cur = next
	}
	last := tokens[len(tokens)-1]
	existing, err := child(cur, last)
	if err != nil {
		return err
	}
	if s, ok := existing.(string); !ok || s != "" {
		return fmt.Errorf("target is not an empty string")
	}
	switch t := cur.(type) {
	case map[string]any:
		t[last] = value
	case []any:
		idx, _ := strconv.Atoi(last)
		t[idx] = value
	}
	return nil
}

func child(container any, tok string) (any, error) {
	switch t := container.(type) {
	case map[string]any:
		v, ok := t[tok]
		if !ok {
			return nil, fmt.Errorf("no member %q", tok)
		}
		return v, nil
	case []any:
		idx, err := strconv.Atoi(tok)
		if err != nil || idx < 0 || idx >= len(t) || strconv.Itoa(idx) != tok {
			return nil, fmt.Errorf("invalid array index %q", tok)
		}
		return t[idx], nil
	}
	return nil, fmt.Errorf("cannot descend into %T at %q", container, tok)
}
