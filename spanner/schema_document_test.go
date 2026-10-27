package spanner_test

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var schemaBlock = regexp.MustCompile("(?s)<!-- spanner-schema:([a-z-]+) -->\\s*```sql\\n(.*?)```")

// schemaBlocks returns the marked SQL blocks of a schema document, keyed by marker.
func schemaBlocks(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	blocks := map[string]string{}
	for _, match := range schemaBlock.FindAllStringSubmatch(string(raw), -1) {
		blocks[match[1]] = match[2]
	}
	return blocks
}

func statementsOf(block, prefix string) []string {
	var out []string
	for _, statement := range strings.Split(block, ";") {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			out = append(out, regexp.MustCompile(`\besa_`).ReplaceAllString(statement, prefix+"_"))
		}
	}
	return out
}

func TestSchemaDocumentEnglishAndJapaneseCarryTheSameStatements(t *testing.T) {
	english := schemaBlocks(t, "../docs/SPANNER_DATABASE_SCHEMA.md")
	japanese := schemaBlocks(t, "../docs/SPANNER_DATABASE_SCHEMA.ja.md")

	require.Len(t, english, 5, "tables, snapshot-history-delete, snapshot-history-ttl, config-delete and config-ttl")
	assert.Equal(t, english, japanese, "the Japanese supplement must not change any statement")
}

// TestSchemaDocumentStatementsProvisionAnOpenableDestination runs the statements of the document, exactly as an
// operator would, and then opens the destination with the library.
func TestSchemaDocumentStatementsProvisionAnOpenableDestination(t *testing.T) {
	blocks := schemaBlocks(t, "../docs/SPANNER_DATABASE_SCHEMA.md")
	require.Len(t, blocks, 5)
	for _, tc := range []struct {
		name          string
		history       string
		config        string
		cfg           spanner.Config
		retentionMode eventstore.RetentionMode
		grace         bool
	}{
		{"delete", "snapshot-history-delete", "config-delete", spanner.Config{}, eventstore.RetentionDelete, false},
		{"ttl", "snapshot-history-ttl", "config-ttl", spanner.Config{TTLGraceDays: 7}, eventstore.RetentionTTL, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := fmt.Sprintf("d%d", prefixCount.Add(1))
			require.NoError(t, database.ApplyDDL(t.Context(), statementsOf(blocks["tables"], prefix)...))
			require.NoError(t, database.ApplyDDL(t.Context(), statementsOf(blocks[tc.history], prefix)...))
			raw := newClient(t)
			for _, statement := range statementsOf(blocks[tc.config], prefix) {
				execute(t, raw, statement, nil)
			}
			cfg := tc.cfg
			cfg.TablePrefix = prefix

			entry, err := spanner.New(t.Context(), raw, cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](),
				eventstore.WithRetentionMode(tc.retentionMode), keepOption(t, 3))

			require.NoError(t, err, "the documented DDL and configuration row are accepted as they are")
			id := aggregate(t, "Account", "doc")
			persistSnapshots(t, entry, id, 1, 4)
			history := queryInts(t, raw, "SELECT seq_nr FROM "+prefix+"_snapshot_history ORDER BY seq_nr", nil)
			if tc.grace {
				assert.Equal(t, []int64{1, 2, 3, 4}, history, "TTL mode marks old rows and keeps them until Spanner removes them")
			} else {
				assert.Equal(t, []int64{2, 3, 4}, history, "delete mode keeps the newest three")
			}
		})
	}
}

var readmeSpannerSection = regexp.MustCompile(`(?s)### Spanner\n(.*?)\n(?:### |## )`)

// TestReadmeFlow follows the documented usage twice: the first start of a provisioned destination and a restart
// with a fresh client. The same construction call is used both times; nothing initializes the destination in the program.
func TestReadmeFlow(t *testing.T) {
	keepCounts := map[string]int{}
	keepPattern := regexp.MustCompile(`KeepLatest\((\d+)\)`)
	for _, path := range []string{"../README.md", "../README.ja.md"} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		section := readmeSpannerSection.FindStringSubmatch(string(raw))
		require.NotNil(t, section, "%s must have a Spanner section", path)
		assert.Contains(t, section[1], `github.com/j5ik2o/event-store-adapter-go/v5/spanner`, path)
		assert.Contains(t, section[1], "spanner.New(ctx, client, spanner.Config{", path)
		assert.Contains(t, section[1], "TablePrefix", path)
		for _, forbidden := range []string{"CREATE TABLE", "UpdateDatabaseDdl", "Initialize("} {
			assert.NotContains(t, section[1], forbidden, "%s: the program must not initialize the destination at every start", path)
		}
		// Only the Spanner section is searched: other sections of the README also show KeepLatest.
		matches := keepPattern.FindAllStringSubmatch(section[1], -1)
		require.Len(t, matches, 1, "%s: the Spanner section must show exactly one KeepLatest", path)
		count, err := strconv.Atoi(matches[0][1])
		require.NoError(t, err)
		keepCounts[path] = count
	}

	blocks := schemaBlocks(t, "../docs/SPANNER_DATABASE_SCHEMA.md")
	prefix := "esa"
	// The README uses the prefix "esa": provision it exactly as the document says, once.
	require.NoError(t, database.ApplyDDL(t.Context(), statementsOf(blocks["tables"], prefix)...))
	require.NoError(t, database.ApplyDDL(t.Context(), statementsOf(blocks["snapshot-history-delete"], prefix)...))
	provisioner := newClient(t)
	for _, statement := range statementsOf(blocks["config-delete"], prefix) {
		execute(t, provisioner, statement, nil)
	}
	for path, count := range keepCounts {
		t.Run(path, func(t *testing.T) {
			id := aggregate(t, "Account", "readme-"+strings.NewReplacer("../", "", ".", "-").Replace(path))
			first, err := spanner.New(t.Context(), newClient(t), spanner.Config{TablePrefix: "esa"},
				eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), keepOption(t, count))
			require.NoError(t, err, "first start with the retention count the README shows")
			require.NoError(t, first.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))

			restarted, err := spanner.New(t.Context(), newClient(t), spanner.Config{TablePrefix: "esa"},
				eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), keepOption(t, count))
			require.NoError(t, err, "restart: the same call succeeds again against the same destination")
			events, err := restarted.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
			require.NoError(t, err)
			require.Len(t, events, 1)
			assert.Equal(t, "one", events[0].Payload())
		})
	}
}
