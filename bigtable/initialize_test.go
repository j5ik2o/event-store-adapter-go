package bigtable_test

import (
	"regexp"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lowercaseUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func bareTables(t *testing.T, spec layoutSpec) *layout {
	t.Helper()
	spec.skipJournalConfig, spec.skipHeadConfig = true, true
	return provision(t, spec)
}

func TestInitializeWritesTheSameConfigurationToBothTables(t *testing.T) {
	l := bareTables(t, layoutSpec{keep: ptr(int64(3))})

	require.NoError(t, bigtable.Initialize(t.Context(), l.data, l.config(), keepOption(t, 3)))

	journal, head := cells(t, l.data, l.journal, "\x00"), cells(t, l.data, l.head, "\x00")
	require.Contains(t, journal, "config:metadata")
	assert.JSONEq(t, journal["config:metadata"], head["config:metadata"], "both rows carry the same JSON")
	assert.Equal(t, journal["config:metadata"], head["config:metadata"])
	assert.Regexp(t, `"keep_snapshot_count":"3"`, journal["config:metadata"], "the count is a decimal string")
	assert.Regexp(t, `"layout_version":1`, journal["config:metadata"])
	assert.Regexp(t, `"retention_mode":"delete"`, journal["config:metadata"])
	assert.Regexp(t, `"envelope_limit_bytes":1048576`, journal["config:metadata"])
	matches := regexp.MustCompile(`"store_id":"([^"]+)"`).FindStringSubmatch(journal["config:metadata"])
	require.Len(t, matches, 2)
	assert.Regexp(t, lowercaseUUID, matches[1])
	assert.Len(t, l.journalKeys(t), 1, "no data row is created")

	_, err := l.tryOpen(l.config(), keepOption(t, 3))
	require.NoError(t, err, "an initialized pair of tables opens")
}

func TestInitializeWritesNullWhenNoHistoryIsKept(t *testing.T) {
	l := bareTables(t, layoutSpec{})

	require.NoError(t, bigtable.Initialize(t.Context(), l.data, l.config()))

	assert.Regexp(t, `"keep_snapshot_count":null`, cells(t, l.data, l.journal, "\x00")["config:metadata"])
}

func TestInitializeTwiceIsAConfigurationErrorThatWritesNothing(t *testing.T) {
	l := bareTables(t, layoutSpec{})
	require.NoError(t, bigtable.Initialize(t.Context(), l.data, l.config()))
	before := cells(t, l.data, l.journal, "\x00")["config:metadata"]

	err := bigtable.Initialize(t.Context(), l.data, l.config())

	requireKind(t, err, eventstore.KindConfiguration)
	assert.Equal(t, before, cells(t, l.data, l.journal, "\x00")["config:metadata"], "the existing row is untouched")
}

func TestInitializeRefusesAHalfInitializedPairAndTheDocumentedRecoveryRepairsIt(t *testing.T) {
	l := bareTables(t, layoutSpec{})
	// Only the journal table has its row: the state after an interrupted initialization.
	require.NoError(t, bigtable.Initialize(t.Context(), l.data, l.config()))
	headRow := cells(t, l.data, l.head, "\x00")
	require.NotNil(t, headRow)
	deleteRow(t, l, l.head)
	_, err := l.tryOpen(l.config())
	requireKind(t, err, eventstore.KindConfiguration)

	err = bigtable.Initialize(t.Context(), l.data, l.config())
	requireKind(t, err, eventstore.KindConfiguration)
	assert.Nil(t, cells(t, l.data, l.head, "\x00"), "Initialize never completes the missing row on its own")

	// Recovery: delete the leftover row from the table that has it, then initialize again.
	deleteRow(t, l, l.journal)
	require.NoError(t, bigtable.Initialize(t.Context(), l.data, l.config()))
	_, err = l.tryOpen(l.config())
	require.NoError(t, err)
}

func TestInitializeValidatesTheSettingsBeforeAnyRequest(t *testing.T) {
	data, _ := recordingClients(t)
	err := bigtable.Initialize(t.Context(), data.client, bigtable.Config{JournalTableID: "same", HeadTableID: "same"})
	requireKind(t, err, eventstore.KindConfiguration)
	err = bigtable.Initialize(t.Context(), data.client, bigtable.Config{JournalTableID: "j", HeadTableID: "h", EnvelopeLimitBytes: 16})
	requireKind(t, err, eventstore.KindConfiguration)
	err = bigtable.Initialize(t.Context(), nil, bigtable.Config{JournalTableID: "j", HeadTableID: "h"})
	requireKind(t, err, eventstore.KindConfiguration)
	assert.Zero(t, data.recorder.count())
}
