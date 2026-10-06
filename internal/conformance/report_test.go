package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var statusKeys = []string{"success", "failure", "not-applicable", "unverified", "unrepresentable"}

func loadClassified(t *testing.T) []CaseResult {
	t.Helper()
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	return classifyCases(d)
}

func TestClassify(t *testing.T) {
	results := loadClassified(t)
	counts := map[Status]int{}
	for _, r := range results {
		counts[r.Status]++
		assert.NotEmpty(t, r.Reason, r.ID)
	}
	assert.Equal(t, 0, counts[Status("success")])
	assert.Equal(t, 0, counts[Status("failure")])
	assert.Equal(t, 16, counts[Status("not-applicable")])
	assert.Equal(t, 142, counts[Status("unverified")])
	assert.Equal(t, 0, counts[Status("unrepresentable")])

	byID := map[string]CaseResult{}
	for _, r := range results {
		byID[r.ID] = r
	}
	for _, id := range []string{"hash-fnv1a64-1", "occurred-at-millisecond-min-inside", "core-time-millisecond-below-min"} {
		require.Contains(t, byID, id)
		assert.Equal(t, Status("not-applicable"), byID[id].Status, id)
	}
	assert.Equal(t, Status("unverified"), byID["seq-zero-value"].Status)
}

func TestReport(t *testing.T) {
	results := loadClassified(t)
	m := ManifestResult{Version: "1.0.0", Verified: true, FileCount: 22}
	gate := GateResult{Lists: RequiredList{"memory": {}, "dynamodb": {}}}
	rep := BuildReport(m, results, nil, gate, nil)

	path := filepath.Join(t.TempDir(), "conformance-report.json")
	require.NoError(t, WriteReport(path, rep))
	b, err := os.ReadFile(path)
	require.NoError(t, err)

	var top map[string]any
	require.NoError(t, json.Unmarshal(b, &top))
	for _, k := range []string{"manifest", "summary", "cases", "required"} {
		assert.Contains(t, top, k)
	}
	summary, ok := top["summary"].(map[string]any)
	require.True(t, ok)
	for _, k := range statusKeys {
		assert.Contains(t, summary, k)
	}
	assert.EqualValues(t, 0, summary["success"])
	assert.EqualValues(t, 16, summary["not-applicable"])
	assert.EqualValues(t, 142, summary["unverified"])
	cases, ok := top["cases"].([]any)
	require.True(t, ok)
	assert.Len(t, cases, 158, "one result per case id and backend")
}

func TestReport_RulesCountEveryStatus(t *testing.T) {
	results := loadClassified(t)
	rep := BuildReport(ManifestResult{Verified: true}, results, nil, GateResult{}, nil)
	b, err := json.Marshal(rep)
	require.NoError(t, err)
	var top map[string]any
	require.NoError(t, json.Unmarshal(b, &top))
	rules, ok := top["rules"].(map[string]any)
	require.True(t, ok, "rules summary must be an object keyed by rule id")
	require.NotEmpty(t, rules)
	for id, v := range rules {
		counts, ok := v.(map[string]any)
		require.True(t, ok, id)
		for _, k := range statusKeys {
			assert.Contains(t, counts, k, id)
		}
	}
}

func TestFailureReasons(t *testing.T) {
	okReport := func() Report {
		return BuildReport(ManifestResult{Verified: true}, loadClassified(t), nil, GateResult{}, nil)
	}

	t.Run("no reasons when nothing is wrong", func(t *testing.T) {
		assert.Empty(t, okReport().FailureReasons())
	})
	t.Run("manifest mismatch", func(t *testing.T) {
		r := okReport()
		r.Manifest.Verified = false
		assert.NotEmpty(t, r.FailureReasons())
	})
	t.Run("required violation", func(t *testing.T) {
		r := okReport()
		r.Required.Violations = []string{"memory: x is unverified"}
		assert.NotEmpty(t, r.FailureReasons())
	})
	t.Run("runner error", func(t *testing.T) {
		r := okReport()
		r.Errors = []string{"boom"}
		assert.NotEmpty(t, r.FailureReasons())
	})
	t.Run("unverified and not-applicable cases alone do not fail", func(t *testing.T) {
		assert.Empty(t, okReport().FailureReasons())
	})
}

func TestClassify_PerBackend(t *testing.T) {
	d := &Data{
		Scenarios: []ScenarioCase{{ID: "s", Rules: []string{"T-1"}, Backends: []string{"memory", "dynamodb"}}},
		Layouts:   []LayoutCase{{ID: "l", Rules: []string{"L-1"}}},
		Values:    []ValueCase{{ID: "v", Operation: "buildAid"}},
	}
	got := classifyCases(d)
	type key struct{ id, backend string }
	seen := map[key]Status{}
	for _, r := range got {
		seen[key{r.ID, r.Backend}] = r.Status
	}
	assert.Len(t, got, 4)
	assert.Contains(t, seen, key{"s", "memory"})
	assert.Contains(t, seen, key{"s", "dynamodb"})
	assert.Contains(t, seen, key{"l", "dynamodb"})
	assert.Contains(t, seen, key{"v", ""})
}

func TestReport_ExcludedRules(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	rep := BuildReport(ManifestResult{Verified: true}, classifyCases(d), d.Exclusions, GateResult{}, nil)
	b, err := json.Marshal(rep)
	require.NoError(t, err)
	var top struct {
		Excluded []RuleExclusion `json:"excluded_rules"`
	}
	require.NoError(t, json.Unmarshal(b, &top))
	byRule := map[string]RuleExclusion{}
	for _, e := range top.Excluded {
		byRule[e.Rule] = e
	}
	require.Contains(t, byRule, "W-5")
	require.Contains(t, byRule, "R-7")
	assert.Equal(t, "deleted", byRule["W-5"].Status)
	assert.NotEmpty(t, byRule["W-5"].Reason)
	assert.Equal(t, "caller-obligation", byRule["R-7"].Status)
	assert.NotEmpty(t, byRule["R-7"].Reason)

	empty := BuildReport(ManifestResult{}, nil, nil, GateResult{}, nil)
	assert.NotNil(t, empty.ExcludedRules, "serialized as [] not null")
}

func TestImplementationVersion(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	bi := func(version string, rev string) *debug.BuildInfo {
		b := &debug.BuildInfo{}
		b.Main.Version = version
		if rev != "" {
			b.Settings = []debug.BuildSetting{{Key: "vcs.revision", Value: rev}}
		}
		return b
	}
	sha := env(map[string]string{"GITHUB_SHA": "abc123"})
	none := env(nil)

	assert.Equal(t, "v2.0.0", implementationVersion(bi("v2.0.0", "rev1"), true, sha))
	assert.Equal(t, "rev1", implementationVersion(bi("(devel)", "rev1"), true, sha))
	assert.Equal(t, "abc123", implementationVersion(bi("(devel)", ""), true, sha))
	assert.Equal(t, "abc123", implementationVersion(nil, false, sha))
	assert.Equal(t, "(devel)", implementationVersion(bi("(devel)", ""), true, none))
}
