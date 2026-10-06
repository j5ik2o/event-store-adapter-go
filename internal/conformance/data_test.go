package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadData(t *testing.T) {
	t.Run("reads the real data: 116 cases, 30 value cases", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		assert.Equal(t, 116, distinctIDs(classifyCases(d)))
		assert.Len(t, d.Values, 30)
	})

	t.Run("operation is limited to four kinds", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		allowed := map[string]bool{"buildAid": true, "validateOccurredAt": true, "validateSeqNr": true, "fnv1a64": true}
		for _, v := range d.Values {
			assert.True(t, allowed[v.Operation], v.ID)
		}
	})

	t.Run("unknown operation is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"operation": "fnv1a64"`, `"operation": "unknownOp"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("wrong format is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"format": "values"`, `"format": "scenarios"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("wrong version is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"version": "1.0.0"`, `"version": "2.0.0"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("json file without a known format mapping is rejected", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "extra.json"), []byte(`{"format":"values","version":"1.0.0"}`), 0o644))
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("broken schema json is rejected", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "schema", "values.schema.json"), []byte(`{`), 0o644))
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("duplicate key inside data is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "coverage.json"), `"format": "coverage",`, `"format": "coverage", "format": "coverage",`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})
}

func TestLoadData_ValueInputsKeepPrecision(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byID := map[string]ValueCase{}
	for _, v := range d.Values {
		byID[v.ID] = v
	}
	require.Contains(t, byID, "seq-above-max-value")
	require.Contains(t, byID, "seq-negative-value")
	require.Contains(t, byID, "occurred-at-below-min")
	require.Contains(t, byID, "occurred-at-above-max")

	require.NotNil(t, byID["seq-above-max-value"].Input.SeqNr)
	assert.Equal(t, "9007199254740992", byID["seq-above-max-value"].Input.SeqNr.String())
	require.NotNil(t, byID["seq-negative-value"].Input.SeqNr)
	assert.Equal(t, "-1", byID["seq-negative-value"].Input.SeqNr.String())
	require.NotNil(t, byID["occurred-at-below-min"].Input.EpochNanoseconds)
	assert.Equal(t, "-9223372036854775809", byID["occurred-at-below-min"].Input.EpochNanoseconds.String())
	require.NotNil(t, byID["occurred-at-above-max"].Input.EpochNanoseconds)
	assert.Equal(t, "9223372036854775808", byID["occurred-at-above-max"].Input.EpochNanoseconds.String())
}

func TestLoadData_DuplicateID(t *testing.T) {
	t.Run("real data has unique ids", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		assert.Equal(t, 116, distinctIDs(classifyCases(d)))
	})

	t.Run("duplicate id across files is rejected and named", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"id": "hash-fnv1a64-1"`, `"id": "seq-zero-value"`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "seq-zero-value")
	})
}

func distinctIDs(rs []CaseResult) int {
	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.ID] = true
	}
	return len(seen)
}

func TestLoadData_SchemaValidation(t *testing.T) {
	t.Run("a scenario without description is rejected by the scenarios schema", func(t *testing.T) {
		root := copyTree(t)
		files, err := filepath.Glob(filepath.Join(root, "scenarios", "core", "*.json"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		b, err := os.ReadFile(files[0])
		require.NoError(t, err)
		var doc map[string]any
		require.NoError(t, json.Unmarshal(b, &doc))
		delete(doc["cases"].([]any)[0].(map[string]any), "description")
		out, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(files[0], out, 0o644))
		_, err = LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("an unknown property in a value case is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"id": "hash-fnv1a64-1"`, `"extra": true, "id": "hash-fnv1a64-1"`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("an invalid generator is caught by the schema before expansion", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "dynamodb", "read.json"), `"byte_length": 320000`, `"byte_length": 0`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("a missing schema file is an error", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.Remove(filepath.Join(root, "schema", "layout.schema.json")))
		_, err := LoadData(root)
		assert.Error(t, err)
	})
}

func TestLoadData_Exclusions(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byRule := map[string]RuleExclusion{}
	for _, e := range d.Exclusions {
		byRule[e.Rule] = e
	}
	require.Contains(t, byRule, "W-5")
	require.Contains(t, byRule, "R-7")
	assert.Equal(t, "deleted", byRule["W-5"].Status)
	assert.Equal(t, "caller-obligation", byRule["R-7"].Status)
	assert.NotEmpty(t, byRule["W-5"].Reason)
	assert.NotEmpty(t, byRule["R-7"].Reason)
}
