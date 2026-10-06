package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyManifest(t *testing.T) {
	t.Run("untouched data verifies", func(t *testing.T) {
		r, err := VerifyManifest(dataRoot())
		require.NoError(t, err)
		assert.True(t, r.Verified)
		assert.Empty(t, r.Mismatches)
		assert.Equal(t, 22, r.FileCount)
	})

	t.Run("a one-byte change in a copy is detected with its path", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "seq-nr.json"), `"seq-zero-value"`, `"seq-zero-valuf"`)
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.True(t, containsPath(r.Mismatches, "values/seq-nr.json"), r.Mismatches)
	})

	t.Run("an added file is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "extra.txt"), []byte("x"), 0o644))
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.True(t, containsPath(r.Mismatches, "extra.txt"), r.Mismatches)
	})

	t.Run("a removed file is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.Remove(filepath.Join(root, "values", "hash.json")))
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.True(t, containsPath(r.Mismatches, "values/hash.json"), r.Mismatches)
	})

	t.Run("a changed manifest version is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "manifest.json"), `"version": "1.0.0"`, `"version": "9.9.9"`)
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.NotEmpty(t, r.Mismatches)
		assert.Equal(t, "9.9.9", r.Version, "the report names the manifest version that was read")
	})

	t.Run("the real manifest reports its own version", func(t *testing.T) {
		r, err := VerifyManifest(dataRoot())
		require.NoError(t, err)
		assert.Equal(t, DataVersion, r.Version)
	})

	t.Run("a symbolic link is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		link := filepath.Join(root, "link.json")
		if err := os.Symlink(filepath.Join(root, "coverage.json"), link); err != nil {
			t.Skip("symlink unsupported")
		}
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
	})

	t.Run("an unknown field in the manifest is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "manifest.json"), `"format": "manifest"`, `"format": "manifest", "extra": true`)
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.True(t, containsPath(r.Mismatches, `unknown field "extra"`), r.Mismatches)
	})

	t.Run("an unknown field in a file entry is a mismatch", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "manifest.json"), `"path": ".gitattributes"`, `"path": ".gitattributes", "extra": true`)
		r, err := VerifyManifest(root)
		require.NoError(t, err)
		assert.False(t, r.Verified)
		assert.True(t, containsPath(r.Mismatches, `files[0]: unknown field "extra"`), r.Mismatches)
	})

	t.Run("a broken manifest is a runner error", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.json"), []byte(`{`), 0o644))
		_, err := VerifyManifest(root)
		assert.Error(t, err)
	})
}

func containsPath(ms []string, p string) bool {
	for _, m := range ms {
		if strings.Contains(m, p) {
			return true
		}
	}
	return false
}
