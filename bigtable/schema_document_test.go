package bigtable_test

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var schemaBlock = regexp.MustCompile("(?s)<!-- bigtable-schema:([a-z-]+) -->\\s*```sh\\n(.*?)```")

// schemaBlocks returns the marked `cbt` blocks of a schema document, keyed by marker.
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

func TestSchemaDocumentEnglishAndJapaneseCarryTheSameProcedures(t *testing.T) {
	english := schemaBlocks(t, "../docs/BIGTABLE_DATABASE_SCHEMA.md")
	japanese := schemaBlocks(t, "../docs/BIGTABLE_DATABASE_SCHEMA.ja.md")

	require.NotEmpty(t, english, "the document marks its cbt procedures")
	assert.Contains(t, english, "tables")
	assert.Equal(t, english, japanese, "the Japanese supplement must not change any step")
}

func TestSchemaDocumentHasTheSevenSectionsAndTheRecoveryProcedure(t *testing.T) {
	heading := regexp.MustCompile(`(?m)^## `)
	for _, path := range []string{"../docs/BIGTABLE_DATABASE_SCHEMA.md", "../docs/BIGTABLE_DATABASE_SCHEMA.ja.md"} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Len(t, heading.FindAllString(string(raw), -1), 7, "%s must have seven H2 sections", path)
		assert.Contains(t, string(raw), "### ", path)
	}
	raw, err := os.ReadFile("../docs/BIGTABLE_DATABASE_SCHEMA.md")
	require.NoError(t, err)
	assert.Contains(t, string(raw), "Recovering from an interrupted initialization")
}

// applyCbt runs the `cbt createtable/createfamily/setgcpolicy` lines of a document block against the emulator, with
// table names substituted by the caller.
func applyCbt(t *testing.T, l *layout, block string, names map[string]string) {
	t.Helper()
	for _, line := range strings.Split(block, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "cbt" {
			continue
		}
		for i := range fields {
			if replacement, ok := names[fields[i]]; ok {
				fields[i] = replacement
			}
		}
		switch fields[1] {
		case "createtable":
			require.NoError(t, l.admin.CreateTable(t.Context(), fields[2]), line)
		case "createfamily":
			require.NoError(t, l.admin.CreateColumnFamily(t.Context(), fields[2], fields[3]), line)
		case "setgcpolicy":
			require.Equal(t, "maxversions=1", fields[4], line)
			require.NoError(t, l.admin.SetGCPolicy(t.Context(), fields[2], fields[3], gbigtable.MaxVersionsPolicy(1)), line)
		}
	}
}

// TestSchemaDocumentProceduresProvisionAnOpenableDestination runs the documented cbt steps for the tables, writes
// the configuration through Initialize, and opens the destination.
func TestSchemaDocumentProceduresProvisionAnOpenableDestination(t *testing.T) {
	blocks := schemaBlocks(t, "../docs/BIGTABLE_DATABASE_SCHEMA.md")
	require.Contains(t, blocks, "tables")
	n := tableSeq.Add(1)
	l := &layout{journal: fmt.Sprintf("esa_journal_%d", n), head: fmt.Sprintf("esa_head_%d", n)}
	var err error
	l.admin, err = emulator.NewAdminClient(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.admin.Close() })
	l.data = newDataClient(t)

	applyCbt(t, l, blocks["tables"], map[string]string{"esa_journal": l.journal, "esa_head": l.head})

	require.NoError(t, bigtable.Initialize(t.Context(), l.data, bigtable.Config{JournalTableID: l.journal, HeadTableID: l.head}))
	entry, err := l.tryOpen(bigtable.Config{JournalTableID: l.journal, HeadTableID: l.head})
	require.NoError(t, err, "the documented tables are accepted as they are")
	id := aggregate(t, "Account", "doc")
	require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
}

var readmeBigtableSection = regexp.MustCompile(`(?s)### Bigtable\n(.*?)\n(?:### |## )`)

// TestReadmeFlow follows the documented usage: the one-time preparation and the per-start opening. The same
// opening call is used at the first start and after a restart with fresh clients.
func TestReadmeFlow(t *testing.T) {
	for _, path := range []string{"../README.md", "../README.ja.md"} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		section := readmeBigtableSection.FindStringSubmatch(string(raw))
		require.NotNil(t, section, "%s must have a Bigtable section", path)
		assert.Contains(t, section[1], `github.com/j5ik2o/event-store-adapter-go/v5/bigtable`, path)
		assert.Contains(t, section[1], "bigtable.Initialize(", path, "the one-time preparation is shown")
		assert.Contains(t, section[1], "bigtable.New(", path)
		assert.Contains(t, section[1], "JournalTableID", path)
		assert.Contains(t, section[1], "KeepLatest(3)", path)
	}

	l := bareTables(t, layoutSpec{})
	cfg := bigtable.Config{JournalTableID: l.journal, HeadTableID: l.head}
	keep := keepOption(t, 3)
	require.NoError(t, bigtable.Initialize(t.Context(), l.data, cfg, keep), "first start: one-time preparation")
	id := aggregate(t, "Account", "readme")
	first, err := l.tryOpen(cfg, keep)
	require.NoError(t, err)
	require.NoError(t, first.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))

	restarted, err := bigtable.New(t.Context(), newDataClient(t), l.admin, cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), keep)
	require.NoError(t, err, "restart: the same call succeeds again against the same destination")
	events, err := restarted.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	requireKind(t, bigtable.Initialize(t.Context(), l.data, cfg, keep), eventstore.KindConfiguration)
}
