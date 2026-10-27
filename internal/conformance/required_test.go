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
		assert.Len(t, req, 5)
		assert.Len(t, req["memory"], 67)
		assert.Len(t, req["dynamodb"], 106)
		assert.Len(t, req["sqlite"], 68)
		assert.Len(t, req["spanner"], 79)
		assert.Len(t, req["bigtable"], 87)
		require.NoError(t, CheckRequiredCoverage(d, req))
		deleteOne := RequiredList{"memory": req["memory"][1:], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": req["spanner"], "bigtable": req["bigtable"]}
		require.Error(t, CheckRequiredCoverage(d, deleteOne))
		addExcluded := RequiredList{"memory": append(append([]string{}, req["memory"]...), "fnv-empty"), "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": req["spanner"], "bigtable": req["bigtable"]}
		require.Error(t, CheckRequiredCoverage(d, addExcluded))
		deleteSqlite := RequiredList{"memory": req["memory"], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"][1:], "spanner": req["spanner"], "bigtable": req["bigtable"]}
		require.Error(t, CheckRequiredCoverage(d, deleteSqlite), "a missing SQLite case must stop the gate")
		deleteSpanner := RequiredList{"memory": req["memory"], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": req["spanner"][1:], "bigtable": req["bigtable"]}
		require.Error(t, CheckRequiredCoverage(d, deleteSpanner), "a missing Spanner case must stop the gate")
		emptySpanner := RequiredList{"memory": req["memory"], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": {}, "bigtable": req["bigtable"]}
		require.Error(t, CheckRequiredCoverage(d, emptySpanner), "an empty Spanner list must not pass")
		deleteBigtable := RequiredList{"memory": req["memory"], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": req["spanner"], "bigtable": req["bigtable"][1:]}
		require.Error(t, CheckRequiredCoverage(d, deleteBigtable), "a missing Bigtable case must stop the gate")
		emptyBigtable := RequiredList{"memory": req["memory"], "dynamodb": req["dynamodb"], "sqlite": req["sqlite"], "spanner": req["spanner"], "bigtable": {}}
		require.Error(t, CheckRequiredCoverage(d, emptyBigtable), "an empty Bigtable list must not pass")
		for _, id := range []string{"bigtable-envelope-event-empty", "bigtable-envelope-event-length-mismatch", "hash-fnv1a64-1", "store-shared-persistent"} {
			assert.Contains(t, req["bigtable"], id)
		}
		for _, name := range []string{"memory", "dynamodb", "sqlite", "spanner"} {
			assert.NotContains(t, req[name], "bigtable-envelope-event-empty", "the envelope table runs only for Bigtable")
		}
		for _, id := range []string{"sqlite-ttl-unsupported", "sqlite-change-feed-unsupported", "sqlite-write-lock-busy", "store-shared-persistent"} {
			assert.Contains(t, req["sqlite"], id)
		}
		for _, id := range []string{"spanner-aborted-retry-within-limit", "spanner-aborted-retry-limit-exceeded", "spanner-commit-response-lost", "spanner-change-feed-unsupported",
			"spanner-seeded-ttl-grace-policy-mismatch", "store-shared-persistent", "hash-fnv1a64-1", "hash-fnv1a64-4"} {
			assert.Contains(t, req["spanner"], id)
		}
		for _, name := range []string{"memory", "dynamodb", "sqlite"} {
			assert.NotContains(t, req[name], "hash-fnv1a64-1", "the hash table runs only where a storage uses the hash")
		}
	})

	write := func(t *testing.T, body string) string {
		p := filepath.Join(t.TempDir(), "required.json")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}

	t.Run("key set must be exactly memory, dynamodb, sqlite, spanner and bigtable", func(t *testing.T) {
		_, err := LoadRequired(write(t, `{"memory": []}`))
		assert.Error(t, err)
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": []}`))
		assert.Error(t, err, "sqlite is required")
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": [], "sqlite": []}`))
		assert.Error(t, err, "spanner is required")
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": [], "sqlite": [], "spanner": []}`))
		assert.Error(t, err, "bigtable is required")
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": [], "sqlite": [], "spanner": [], "bigtable": [], "other": []}`))
		assert.Error(t, err)
		_, err = LoadRequired(write(t, `{"memory": [], "dynamodb": [], "sqlite": [], "spanner": [], "bigtable": []}`))
		assert.NoError(t, err)
	})

	t.Run("duplicate id is rejected", func(t *testing.T) {
		_, err := LoadRequired(write(t, `{"memory": ["a","a"], "dynamodb": [], "sqlite": [], "spanner": [], "bigtable": []}`))
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
	req := RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {}, "spanner": {}, "bigtable": {}}
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
				require.Len(t, gate.Violations, len(requiredBackends), "a value case is judged under every backend list")
				for _, violation := range gate.Violations {
					assert.Contains(t, violation, r.ID)
				}
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
	t.Run("a required spanner case that did not succeed is a violation naming spanner", func(t *testing.T) {
		spannerResults := []CaseResult{
			{ID: "sp-ok", Status: Status("success"), Reason: "r", Backend: "spanner"},
			{ID: "sp-skipped", Status: Status("unverified"), Reason: "r", Backend: "spanner"},
			{ID: "sp-failed", Status: Status("failure"), Reason: "r", Backend: "spanner"},
		}
		g, err := EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {}, "spanner": {"sp-ok"}, "bigtable": {}}, spannerResults)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)

		g, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {}, "spanner": {"sp-ok", "sp-skipped", "sp-failed"}, "bigtable": {}}, spannerResults)
		require.NoError(t, err)
		require.Len(t, g.Violations, 2)
		for _, violation := range g.Violations {
			assert.Contains(t, violation, "spanner")
		}

		_, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {}, "spanner": {"sp-not-run"}, "bigtable": {}}, spannerResults)
		assert.Error(t, err, "a required case that was never executed must stop the gate: the CI fails")
	})
	t.Run("a required sqlite case that did not succeed is a violation naming sqlite", func(t *testing.T) {
		sqliteResults := []CaseResult{
			{ID: "sq-ok", Status: Status("success"), Reason: "r", Backend: "sqlite"},
			{ID: "sq-skipped", Status: Status("unverified"), Reason: "r", Backend: "sqlite"},
			{ID: "sq-failed", Status: Status("failure"), Reason: "r", Backend: "sqlite"},
		}
		g, err := EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {"sq-ok"}}, sqliteResults)
		require.NoError(t, err)
		assert.Empty(t, g.Violations)

		g, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {"sq-ok", "sq-skipped", "sq-failed"}}, sqliteResults)
		require.NoError(t, err)
		require.Len(t, g.Violations, 2)
		for _, violation := range g.Violations {
			assert.Contains(t, violation, "sqlite")
		}

		_, err = EvaluateRequired(RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {"sq-not-run"}}, sqliteResults)
		assert.Error(t, err, "a required case that was never executed must stop the gate")
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

func TestEvaluateRequiredBigtable(t *testing.T) {
	results := []CaseResult{
		{ID: "bt-ok", Status: Status("success"), Reason: "r", Backend: "bigtable"},
		{ID: "bt-skipped", Status: Status("unverified"), Reason: "r", Backend: "bigtable"},
		{ID: "bt-failed", Status: Status("failure"), Reason: "r", Backend: "bigtable"},
	}
	base := func(ids ...string) RequiredList {
		return RequiredList{"memory": {}, "dynamodb": {}, "sqlite": {}, "spanner": {}, "bigtable": ids}
	}

	g, err := EvaluateRequired(base("bt-ok"), results)
	require.NoError(t, err)
	assert.Empty(t, g.Violations)

	g, err = EvaluateRequired(base("bt-ok", "bt-skipped", "bt-failed"), results)
	require.NoError(t, err)
	require.Len(t, g.Violations, 2)
	for _, violation := range g.Violations {
		assert.Contains(t, violation, "bigtable")
	}

	_, err = EvaluateRequired(base("bt-not-run"), results)
	assert.Error(t, err, "a required Bigtable case that was not run stops the gate")
}
