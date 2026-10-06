package conformance

import (
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
		assert.Len(t, classifyCases(d), 116)
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
		assert.Len(t, classifyCases(d), 116)
	})

	t.Run("duplicate id across files is rejected and named", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"id": "hash-fnv1a64-1"`, `"id": "seq-zero-value"`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "seq-zero-value")
	})
}
