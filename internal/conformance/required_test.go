package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadRequired(t *testing.T) {
	t.Run("the shipped list starts empty per backend", func(t *testing.T) {
		req, err := LoadRequired("required.json")
		require.NoError(t, err)
		assert.Len(t, req, 2)
		for _, k := range []string{"memory", "dynamodb"} {
			v, ok := req[k]
			assert.True(t, ok, k)
			assert.Empty(t, v, k)
		}
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
