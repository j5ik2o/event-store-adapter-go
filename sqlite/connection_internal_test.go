package sqlite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The SQLite library version must be at least 3.51.3, compared per numeric component.
func TestMeetsMinimumVersion(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{
		{"3.51.3", true},
		{"3.51.4", true},
		{"3.52.0", true},
		{"3.53.4", true},
		{"3.100.0", true},
		{"4.0.0", true},
		{"3.51.2", false},
		{"3.51.0", false},
		{"3.9.9", false},
		{"3.50.99", false},
		{"2.99.99", false},
		{"", false},
		{"not-a-version", false},
		{"3.51", false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			assert.Equal(t, tc.want, meetsMinimumVersion(tc.version))
		})
	}
}

// A Windows drive path must not become the authority of the URI; the result is the same on every OS.
func TestBuildFileURI(t *testing.T) {
	for name, tc := range map[string]struct{ path, mode, want string }{
		"posix":          {"/var/data/events.db", "rw", "file:///var/data/events.db?mode=rw&cache=private"},
		"windows drive":  {"C:/Users/u/events.db", "rwc", "file:///C:/Users/u/events.db?mode=rwc&cache=private"},
		"windows spaces": {"D:/my data/events.db", "rw", "file:///D:/my%20data/events.db?mode=rw&cache=private"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, buildFileURI(tc.path, tc.mode))
		})
	}
}
