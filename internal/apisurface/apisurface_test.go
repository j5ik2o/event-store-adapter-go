package apisurface

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	surfacePath = filepath.Join("..", "..", "api-surface", "surface.json")
	mappingPath = filepath.Join("..", "..", "api-surface", "go.json")
	rootDir     = filepath.Join("..", "..")
)

func lines(problems []Problem) string {
	var b strings.Builder
	for _, p := range problems {
		b.WriteString(p.String())
		b.WriteString("\n")
	}
	return b.String()
}

// The source importer is not safe for concurrent use, so the sub-tests share
// one Loader and run sequentially.
func TestSurface(t *testing.T) {
	s, err := LoadSurface(surfacePath)
	require.NoError(t, err)
	m, err := LoadMapping(mappingPath)
	require.NoError(t, err)
	loader := NewLoader()

	t.Run("root package matches the surface", func(t *testing.T) {
		pkg, err := loader.Load(rootDir)
		require.NoError(t, err)
		require.Equal(t, m.Package, pkg.Name())
		require.Empty(t, Check(pkg, s, m), "problems:\n%s", lines(Check(pkg, s, m)))
	})

	negatives := []struct {
		dir string
		// path is the only surface item that may be reported.
		path string
		// message must appear in the report.
		message string
	}{
		{"missing_occurred_at", "event_envelope.occurred_at", "EventEnvelope has no method OccurredAt (want OccurredAt() time.Time)"},
		{"renamed_manifest", "event_envelope.manifest", "EventEnvelope has no method Manifest (want Manifest() string)"},
		{"seq_nr_returns_int", "event_envelope.seq_nr", "EventEnvelope.SeqNr returns int, want SeqNr"},
		{"constructor_missing_seq_nr", "event_envelope constructor", "required element not accepted: seq_nr (SeqNr)"},
		{"events_return_payloads", "store.get_events_by_id_since_seq_nr", "want func(context.Context, AggregateID, SeqNr) ([]EventEnvelope[E], error)"},
		{"missing_storage_error", "error_categories.storage", "type StorageError not found in package eventstore"},
	}
	for _, tc := range negatives {
		t.Run("detects "+tc.dir, func(t *testing.T) {
			pkg, err := loader.Load(filepath.Join("testdata", tc.dir))
			require.NoError(t, err)
			problems := Check(pkg, s, m)
			require.NotEmpty(t, problems, "the deviation was not reported")
			for _, p := range problems {
				require.Equal(t, tc.path, p.Path, "unrelated problem reported:\n%s", lines(problems))
			}
			require.Contains(t, lines(problems), tc.message)
		})
	}
}

func TestMappingGapsAreReported(t *testing.T) {
	s, err := LoadSurface(surfacePath)
	require.NoError(t, err)
	m, err := LoadMapping(mappingPath)
	require.NoError(t, err)
	pkg, err := NewLoader().Load(rootDir)
	require.NoError(t, err)

	delete(m.Types, "snapshot_read")
	delete(m.Store.Operations, "persist_event")
	delete(m.Errors.Names, "storage")
	got := lines(Check(pkg, s, m))
	require.Contains(t, got, "snapshot_read: go.json has no mapping for type snapshot_read")
	require.Contains(t, got, "store.persist_event: go.json has no mapping for operation persist_event")
	require.Contains(t, got, "error_categories.storage: go.json has no mapping for error category storage")
}
