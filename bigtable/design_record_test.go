package bigtable_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNameInRecord = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)

// definedTests collects every `func TestXxx(` of the test files that can back a rule of the Bigtable profile.
func definedTests(t *testing.T) map[string]bool {
	t.Helper()
	defined := map[string]bool{}
	declaration := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\(`)
	for _, dir := range []string{".", "../internal/conformance", "../internal/keyhash", "../internal/bigtableenvelope", "../internal/bigtabletest", "../internal/testhook"} {
		files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
		require.NoError(t, err)
		for _, file := range files {
			raw, err := os.ReadFile(file)
			require.NoError(t, err)
			for _, match := range declaration.FindAllStringSubmatch(string(raw), -1) {
				defined[match[1]] = true
			}
		}
	}
	return defined
}

// TestDesignRecordMapsEveryRuleToTestsThatExist checks the design record of the Bigtable storage: every rule of the
// profile (BT-1 .. BT-17, BT-D1 .. BT-D8) and of the hash (K-1, K-2) is mentioned, and every test the record names exists.
func TestDesignRecordMapsEveryRuleToTestsThatExist(t *testing.T) {
	defined := definedTests(t)
	for _, path := range []string{"../docs/design/bigtable-backend.md", "../docs/design/bigtable-backend.ja.md"} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err, "the design record must exist")
		record := string(raw)

		for n := 1; n <= 17; n++ {
			assert.Regexp(t, regexp.MustCompile(`BT-`+strconv.Itoa(n)+`\b`), record, "%s must account for BT-%d", path, n)
		}
		for _, decision := range []string{"BT-D1", "BT-D2", "BT-D3", "BT-D4", "BT-D5", "BT-D6a", "BT-D6b", "BT-D7", "BT-D8"} {
			assert.Regexp(t, regexp.MustCompile(regexp.QuoteMeta(decision)+`\b`), record, "%s must account for %s", path, decision)
		}
		for _, rule := range []string{"K-1", "K-2"} {
			assert.Contains(t, record, rule, "%s must account for %s", path, rule)
		}
		names := testNameInRecord.FindAllString(record, -1)
		assert.NotEmpty(t, names, "%s must name the tests that give the evidence", path)
		for _, name := range names {
			assert.True(t, defined[name], "%s names %s, which is not a test of this repository", path, name)
		}
	}
}

func TestDesignRecordEnglishAndJapaneseNameTheSameTests(t *testing.T) {
	names := func(path string) []string {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		seen := map[string]bool{}
		var out []string
		for _, name := range testNameInRecord.FindAllString(string(raw), -1) {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
		return out
	}

	assert.ElementsMatch(t, names("../docs/design/bigtable-backend.md"), names("../docs/design/bigtable-backend.ja.md"))
}
