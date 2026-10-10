package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// compareObservation compares every declared observation. Unknown observations
// fail rather than being silently omitted. Actual data is retained in the report.
func compareObservation(ctx context.Context, operation int, step StepPlan, hooks *testhook.Hooks, store Store, backend Backend, result *CaseResult) string {
	reader, connected := backend.(ObservationReader)
	if !connected {
		for key := range step.Observe {
			if key != "history" && key != "notifications" {
				return "observation reader is not connected: " + key
			}
		}
		return checkObservation(step, hooks, store)
	}
	actual, err := reader.Observe(ctx, operation, step)
	if err != nil {
		return "observation: " + err.Error()
	}
	if len(result.Operations) > 0 {
		result.Operations[len(result.Operations)-1].Observation = actual
	}
	for key, expected := range step.Observe {
		var mismatch string
		switch key {
		case "items":
			want, _ := expected.([]any)
			got, _ := actual[key].([]any)
			if len(want) != len(got) {
				return "items count differs"
			}
			bindings := map[string]any{}
			for i, entry := range want {
				w := entry.(map[string]any)
				g, ok := got[i].(map[string]any)
				if !ok {
					return fmt.Sprintf("items[%d] is missing", i)
				}
				if !jsonEqual(w["attributes"], g["attributes"]) {
					return fmt.Sprintf("items[%d] attribute set/types differ", i)
				}
				for _, field := range []string{"table", "values"} {
					if value, exists := w[field]; exists && !jsonSubset(value, g[field]) {
						return fmt.Sprintf("items[%d].%s differs", i, field)
					}
				}
				for _, field := range []string{"binary_json", "nested_attributes"} {
					if value, exists := w[field]; exists && !jsonEqual(value, g[field]) {
						return fmt.Sprintf("items[%d].%s differs", i, field)
					}
				}
				if b, ok := w["bindings"].(map[string]any); ok {
					values, _ := g["values"].(map[string]any)
					for attribute, name := range b {
						value, exists := values[attribute]
						if !exists || value == "" {
							return "generated store ID is missing"
						}
						binding := fmt.Sprint(name)
						if previous, bound := bindings[binding]; bound && !jsonEqual(previous, value) {
							return "generated store IDs differ"
						}
						bindings[binding] = value
					}
				}
			}
		case "requests":
			want, _ := expected.([]any)
			got, _ := actual[key].([]any)
			next := 0
			for _, entry := range want {
				found := false
				for next < len(got) {
					g := got[next]
					next++
					if jsonSubset(entry, g) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("no distinct ordered request matches %v; got %v", entry, got)
				}
			}
		case "no_requests_in_phases":
			counts, _ := actual["request_count"].(map[string]any)
			for _, phase := range expected.([]any) {
				if n, exists := counts[fmt.Sprint(phase)]; exists && fmt.Sprint(n) != "0" {
					return "unexpected request in phase " + fmt.Sprint(phase)
				}
			}
		case "request_count", "minimum_request_count":
			counts, _ := actual["request_count"].(map[string]any)
			for phase, count := range expected.(map[string]any) {
				want, err1 := bigIntFromJSONNumber(count)
				value := counts[phase]
				if value == nil {
					value = json.Number("0")
				}
				got, err2 := bigIntFromJSONNumber(value)
				if err1 != nil || err2 != nil || (key == "request_count" && want.Cmp(got) != 0) || (key == "minimum_request_count" && want.Cmp(got) > 0) {
					return fmt.Sprintf("%s.%s: want %v, got %v", key, phase, count, value)
				}
			}
		case "history":
			want := expected.(map[string]any)
			got, _ := actual[key].(map[string]any)
			for _, field := range []string{"active", "marked"} {
				if !unorderedEqual(want[field], got[field]) {
					return fmt.Sprintf("history.%s differs: want %v, got %v", field, want[field], got[field])
				}
			}
			absent, ok := int64s(want["absent"])
			if !ok {
				return "history.absent is invalid"
			}
			for _, field := range []string{"active", "marked"} {
				present, ok := int64s(got[field])
				if !ok {
					return "observed history is invalid"
				}
				for _, n := range absent {
					for _, m := range present {
						if n == m {
							return fmt.Sprintf("history.absent %d is present", n)
						}
					}
				}
			}
		case "notifications":
			if !jsonEqual(expected, actual[key]) {
				mismatch = "notifications differ"
			}
		default:
			mismatch = "unknown observation: " + key
		}
		if mismatch != "" {
			return mismatch
		}
	}
	return ""
}

// jsonSubset preserves scalar types and exact array length while allowing
// unspecified object values. Attribute sets are checked separately and exactly.
func jsonSubset(expected, actual any) bool {
	switch want := expected.(type) {
	case map[string]any:
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range want {
			other, exists := got[key]
			if !exists {
				return false
			}
			if key == "keys" || key == "put_tables" || key == "all" || key == "remove" || key == "target_seq_nrs" {
				if !unorderedEqual(value, other) {
					return false
				}
			} else if !jsonSubset(value, other) {
				return false
			}
		}
		return true
	case []any:
		got, ok := actual.([]any)
		if !ok || len(want) != len(got) {
			return false
		}
		for i := range want {
			if !jsonSubset(want[i], got[i]) {
				return false
			}
		}
		return true
	default:
		return jsonEqual(expected, actual)
	}
}

func unorderedEqual(a, b any) bool {
	x, ok1 := a.([]any)
	y, ok2 := b.([]any)
	if !ok1 || !ok2 || len(x) != len(y) {
		return false
	}
	encode := func(values []any) []string {
		out := make([]string, len(values))
		for i, value := range values {
			raw, _ := json.Marshal(value)
			out[i] = string(raw)
		}
		sort.Strings(out)
		return out
	}
	return reflect.DeepEqual(encode(x), encode(y))
}
