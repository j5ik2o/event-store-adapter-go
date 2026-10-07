package conformance

import (
	"fmt"
	"os"
	"sort"
)

// RequiredList maps a backend name to the IDs of the cases that must not fail or stay unverified.
type RequiredList map[string][]string

// GateResult is the outcome of evaluating the required list.
type GateResult struct {
	Lists      RequiredList `json:"lists"`
	Violations []string     `json:"violations"`
}

var requiredBackends = []string{"memory", "dynamodb"}

// LoadRequired reads required.json. The keys must be exactly memory and dynamodb.
func LoadRequired(path string) (RequiredList, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	doc, err := decodeStrictJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: top level is not an object", path)
	}
	if len(obj) != len(requiredBackends) {
		return nil, fmt.Errorf("%s: keys must be exactly %v", path, requiredBackends)
	}
	req := RequiredList{}
	for _, backend := range requiredBackends {
		v, present := obj[backend]
		if !present {
			return nil, fmt.Errorf("%s: key %q is missing", path, backend)
		}
		ids, err := stringList(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", path, backend, err)
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if seen[id] {
				return nil, fmt.Errorf("%s: %s: duplicate case id %q", path, backend, id)
			}
			seen[id] = true
		}
		req[backend] = ids
	}
	return req, nil
}

// EvaluateRequired records a violation for each listed case whose status is failure or unverified
// on the listed backend. A value-table case has no backend and is judged by its own result under either list. An ID that is not in the results, or that is listed under a backend
// the case does not target, is an error of the runner.
func EvaluateRequired(req RequiredList, results []CaseResult) (GateResult, error) {
	type key struct{ id, backend string }
	byKey := make(map[key]CaseResult, len(results))
	knownID := make(map[string]bool, len(results))
	for _, r := range results {
		byKey[key{r.ID, r.Backend}] = r
		knownID[r.ID] = true
	}
	gate := GateResult{Lists: req, Violations: []string{}}
	backends := make([]string, 0, len(req))
	for b := range req {
		backends = append(backends, b)
	}
	sort.Strings(backends)
	for _, backend := range backends {
		known := false
		for _, b := range requiredBackends {
			known = known || b == backend
		}
		if !known {
			return GateResult{}, fmt.Errorf("required list has unknown backend %q", backend)
		}
		for _, id := range req[backend] {
			if !knownID[id] {
				return GateResult{}, fmt.Errorf("required case %q (%s) is not in the data", id, backend)
			}
			r, ok := byKey[key{id, backend}]
			if !ok {
				// A value-table case has no backend: it is judged by its own result, whichever list names it.
				r, ok = byKey[key{id, ""}]
			}
			if !ok {
				return GateResult{}, fmt.Errorf("required case %q is listed under %s but does not target it", id, backend)
			}
			if r.Status == StatusFailure || r.Status == StatusUnverified {
				gate.Violations = append(gate.Violations, fmt.Sprintf("%s: required case %q is %s", backend, id, r.Status))
			}
		}
	}
	return gate, nil
}
