package conformance

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func dataRoot() string {
	return filepath.Join("..", "..", "conformance")
}

// copyTree copies src into a fresh temporary directory so that tests never touch conformance/ itself.
func copyTree(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	src := dataRoot()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	require.NoError(t, err)
	return dst
}

func replaceInFile(t *testing.T, path, old, repl string) {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	s := string(b)
	require.Contains(t, s, old)
	out := []byte(strings.Replace(s, old, repl, 1))
	require.NoError(t, os.WriteFile(path, out, 0o644))
}
