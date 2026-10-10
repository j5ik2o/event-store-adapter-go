package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadRequired(t *testing.T) {
	t.Run("the shipped list requires every applicable distributed case", func(t *testing.T) {
		req, err := LoadRequired("required.json")
		require.NoError(t, err)
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		assert.Len(t, req, 2)
		assert.Len(t, req["memory"], 60)
		assert.Len(t, req["dynamodb"], 104)
		require.NoError(t, CheckRequiredCoverage(d, req))
		deleteOne := RequiredList{"memory": req["memory"][1:], "dynamodb": req["dynamodb"]}
		require.Error(t, CheckRequiredCoverage(d, deleteOne))
		addExcluded := RequiredList{"memory": append(append([]string{}, req["memory"]...), "fnv-empty"), "dynamodb": req["dynamodb"]}
		require.Error(t, CheckRequiredCoverage(d, addExcluded))
	})

	write := func(t *testing.T, body string) string {
		p := filepath.Join(t.TempDir(), "required.json")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}

	t.Run("key set must be exactly memory and dynamodb", func(t *testing.T) {
		_, err := LoadRequired(write(t, `{"memory": []}`))
		assert.Error(t, err)
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": [], "other": []}`))
		assert.Error(t, err)
	})

	t.Run("duplicate id is rejected", func(t *testing.T) {
		_, err := LoadRequired(write(t, `{"memory": ["a","a"], "dynamodb": []}`))
		assert.Error(t, err)
	})

	t.Run("missing file is an error", func(t *testing.T) {
		_, err := LoadRequired(filepath.Join(t.TempDir(), "none.json"))
		assert.Error(t, err)
	})
}

func TestRequiredCoreValues(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	req := RequiredList{"memory": {}, "dynamodb": {}}
	for _, c := range d.Values {
		if c.Operation == "buildAid" || c.Operation == "validateSeqNr" {
			for _, name := range requiredBackends {
				req[name] = append(req[name], c.ID)
			}
		}
	}
	var results []CaseResult
	for _, c := range d.Values {
		if contains(req["memory"], c.ID) {
			results = append(results, runValueCase(c))
		}
	}
	gate, err := EvaluateRequired(req, results)
	require.NoError(t, err)
	assert.Empty(t, gate.Violations)
	for _, status := range []Status{StatusFailure, StatusUnverified} {
		for i, r := range results {
			if r.Backend != "" || !contains(req["memory"], r.ID) {
				continue
			}
			t.Run(r.ID+"/"+string(status), func(t *testing.T) {
				changed := append([]CaseResult(nil), results...)
				changed[i].Status = status
				gate, err := EvaluateRequired(req, changed)
				require.NoError(t, err)
				require.Len(t, gate.Violations, 2)
				assert.Contains(t, gate.Violations[0], r.ID)
				assert.Contains(t, gate.Violations[1], r.ID)
			})
		}
	}
}

func TestEvaluateRequired(t *testing.T) {
	results := []CaseResult{
		{ID: "u", Status: Status("unverified"), Reason: "r", Backend: "memory"},
		{ID: "u", Status: Status("unverified"), Reason: "r", Backend: "dynamodb"},
		{ID: "n", Status: Status("not-applicable"), Reason: "r", Backend: "memory"},
		{ID: "n", Status: Status("not-applicable"), Reason: "r", Backend: "dynamodb"},
		{ID: "f", Status: Status("failure"), Reason: "r", Backend: "memory"},
		{ID: "s", Status: Status("success"), Reason: "r", Backend: "memory"},
		{ID: "d", Status: Status("success"), Reason: "r", Backend: "dynamodb"},
		{ID: "split", Status: Status("success"), Reason: "r", Backend: "memory"},
		{ID: "split", Status: Status("unverified"), Reason: "r", Backend: "dynamodb"},
	}
	t.Run("duplicate backend results are a runner error", func(t *testing.T) {
		_, err := EvaluateRequired(RequiredList{"memory": {"s"}, "dynamodb": {}}, append(results, results[5]))
		require.Error(t, err)
	})

	t.Run("empty list has no violations", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)
	})
	t.Run("unverified required case is a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"u"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Len(t, g.Violations, 1)
	})
	t.Run("failed required case is a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"f"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Len(t, g.Violations, 1)
	})
	t.Run("not-applicable required case is not a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"n"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)
	})
	t.Run("successful required case is not a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"s"}, "dynamodb": {"d"}}, results)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)
	})
	t.Run("id absent from the report is a runner error", func(t *testing.T) {
		_, err := EvaluateRequired(RequiredList{"memory": {"ghost"}, "dynamodb": {}}, results)
		assert.Error(t, err)
	})
	t.Run("id placed under a backend the case does not target is a runner error", func(t *testing.T) {
		_, err := EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {"f"}}, results)
		assert.Error(t, err)
	})
	t.Run("a case is judged by its own backend: memory success does not hide dynamodb unverified", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"split"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)

		g, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {"split"}}, results)
		require.NoError(t, err)
		require.Len(t, g.Violations, 1)
		assert.Contains(t, g.Violations[0], "dynamodb")
	})
}

func TestEvaluateRequired_ValueCases(t *testing.T) {
	results := []CaseResult{
		{ID: "val-u", Status: Status("unverified"), Reason: "r"},
		{ID: "val-s", Status: Status("success"), Reason: "r"},
		{ID: "val-n", Status: Status("not-applicable"), Reason: "r"},
		{ID: "val-f", Status: Status("failure"), Reason: "r"},
	}

	t.Run("an unverified value case is a violation under either backend list", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"val-u"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Len(t, g.Violations, 1)
		g, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {"val-u"}}, results)
		require.NoError(t, err)
		assert.Len(t, g.Violations, 1)
	})
	t.Run("a failed value case is a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"val-f"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Len(t, g.Violations, 1)
	})
	t.Run("a successful or not-applicable value case is not a violation", func(t *testing.T) {
		g, err := EvaluateRequired(RequiredList{"memory": {"val-s", "val-n"}, "dynamodb": {}}, results)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)
	})
	t.Run("an unknown id is still a runner error", func(t *testing.T) {
		_, err := EvaluateRequired(RequiredList{"memory": {"ghost"}, "dynamodb": {}}, results)
		assert.Error(t, err)
	})
}
