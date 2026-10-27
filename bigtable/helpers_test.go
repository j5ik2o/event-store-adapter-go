package bigtable_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtabletest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const validStoreID = "6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b"

var (
	emulator *bigtabletest.Environment
	tableSeq atomic.Int64
)

// TestMain starts one real emulator for the package. A failure to start it fails the run: no test is skipped.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	environment, err := bigtabletest.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot start the Bigtable emulator:", err)
		os.Exit(1)
	}
	emulator = environment
	code := m.Run()
	if err := emulator.Close(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "cannot stop the Bigtable emulator:", err)
		code = 1
	}
	os.Exit(code)
}

func ptr[T any](v T) *T { return &v }

// layoutSpec describes the destination an operator provisions outside the library.
type layoutSpec struct {
	// keep is the keep_snapshot_count of the configuration rows; nil means null.
	keep *int64
	// limit is envelope_limit_bytes of the configuration rows; 0 means the default 1048576.
	limit int
	// journalConfig and headConfig override the JSON of one table's configuration row.
	journalConfig, headConfig string
	// skipJournalConfig and skipHeadConfig leave one configuration row out.
	skipJournalConfig, skipHeadConfig bool
}

// layout is a provisioned pair of tables with raw clients that are independent of every Store.
type layout struct {
	journal, head string
	data          *gbigtable.Client
	admin         *gbigtable.AdminClient
	spec          layoutSpec
}

func provision(t *testing.T, spec layoutSpec) *layout {
	t.Helper()
	n := tableSeq.Add(1)
	l := &layout{journal: fmt.Sprintf("journal%d", n), head: fmt.Sprintf("head%d", n), spec: spec}
	ctx := context.Background()
	var err error
	l.admin, err = emulator.NewAdminClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.admin.Close() })
	l.data = newDataClient(t)
	require.NoError(t, bigtabletest.CreateTables(ctx, l.admin, l.journal, l.head))
	limit := spec.limit
	if limit == 0 {
		limit = 1048576
	}
	config := bigtabletest.ConfigJSON(validStoreID, spec.keep, limit)
	journalConfig, headConfig := config, config
	if spec.journalConfig != "" {
		journalConfig = spec.journalConfig
	}
	if spec.headConfig != "" {
		headConfig = spec.headConfig
	}
	if !spec.skipJournalConfig {
		require.NoError(t, bigtabletest.WriteConfig(ctx, l.data, l.journal, journalConfig))
	}
	if !spec.skipHeadConfig {
		require.NoError(t, bigtabletest.WriteConfig(ctx, l.data, l.head, headConfig))
	}
	return l
}

func newDataClient(t *testing.T, opts ...option.ClientOption) *gbigtable.Client {
	t.Helper()
	client, err := emulator.NewClient(context.Background(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (l *layout) config() bigtable.Config {
	return bigtable.Config{JournalTableID: l.journal, HeadTableID: l.head, EnvelopeLimitBytes: l.spec.limit}
}

// options returns the store options that match the layout: the retention count.
func (l *layout) options(t *testing.T, extra ...eventstore.Option) []eventstore.Option {
	t.Helper()
	var opts []eventstore.Option
	if l.spec.keep != nil {
		opts = append(opts, keepOption(t, int(*l.spec.keep)))
	}
	return append(opts, extra...)
}

func (l *layout) tryOpen(cfg bigtable.Config, opts ...eventstore.Option) (eventstore.EventStore[string, string], error) {
	return bigtable.New(context.Background(), l.data, l.admin, cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), opts...)
}

func (l *layout) newEntry(t *testing.T, extra ...eventstore.Option) eventstore.EventStore[string, string] {
	t.Helper()
	entry, err := l.tryOpen(l.config(), l.options(t, extra...)...)
	require.NoError(t, err)
	return entry
}

func keepOption(t *testing.T, n int) eventstore.Option {
	t.Helper()
	count, err := eventstore.KeepLatest(n)
	require.NoError(t, err)
	return eventstore.WithRetentionCount(count)
}

func hooksOption(h *testhook.Hooks) eventstore.Option {
	return func(o *storeoptions.Options) error {
		o.Hooks = h
		return nil
	}
}

func aggregate(t *testing.T, typeName, value string) eventstore.AggregateID {
	t.Helper()
	id, err := eventstore.NewAggregateID(typeName, value)
	require.NoError(t, err)
	return id
}

func eventAt(t *testing.T, id eventstore.AggregateID, seq eventstore.SeqNr, payload string) eventstore.EventEnvelope[string] {
	t.Helper()
	event, err := eventstore.NewEventEnvelope(id, seq, time.Unix(1720000000+int64(seq), 123456789), payload,
		eventstore.WithManifest("event/v1"))
	require.NoError(t, err)
	return event
}

func snapshotAt(t *testing.T, seq eventstore.SeqNr, state string) eventstore.SnapshotEnvelope[string] {
	t.Helper()
	snapshot, err := eventstore.NewSnapshotEnvelope(state, seq, eventstore.WithManifest("snapshot/v1"))
	require.NoError(t, err)
	return snapshot
}

func requireKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok, "error must use the existing classification: %v", err)
	require.Equal(t, expected, kind, "%v", err)
}

func persistSnapshots(t *testing.T, entry eventstore.EventStore[string, string], id eventstore.AggregateID, from, to eventstore.SeqNr) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq)), snapshotAt(t, seq, fmt.Sprint("state-", seq))))
	}
}

// rowPrefix builds P(aid) = H || L16 || A independently of the library, with the standard library hash.
func rowPrefix(aid string) []byte {
	h := fnv.New64a()
	_, _ = h.Write([]byte(aid))
	key := binary.BigEndian.AppendUint64(nil, h.Sum64())
	key = binary.BigEndian.AppendUint16(key, uint16(len(aid)))
	return append(key, aid...)
}

func journalKey(aid string, seq uint64) string {
	return string(binary.BigEndian.AppendUint64(rowPrefix(aid), seq))
}

// cells reads the latest cell of every column of one row through an independent raw client; nil when the row is absent.
func cells(t *testing.T, client *gbigtable.Client, tableID, rowKey string) map[string]string {
	t.Helper()
	row, err := client.Open(tableID).ReadRow(context.Background(), rowKey, gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
	require.NoError(t, err)
	if len(row) == 0 {
		return nil
	}
	out := map[string]string{}
	for family, items := range row {
		for _, item := range items {
			out[item.Column] = string(item.Value)
			require.EqualValues(t, 0, item.Timestamp, "every cell is written with timestamp 0 (BT-6): %s", item.Column)
		}
		_ = family
	}
	return out
}

// rowKeys lists every row key of a table, including the configuration row.
func rowKeys(t *testing.T, client *gbigtable.Client, tableID string) []string {
	t.Helper()
	var keys []string
	require.NoError(t, client.Open(tableID).ReadRows(context.Background(), gbigtable.InfiniteRange(""), func(r gbigtable.Row) bool {
		keys = append(keys, r.Key())
		return true
	}, gbigtable.RowFilter(gbigtable.StripValueFilter())))
	return keys
}

func (l *layout) journalKeys(t *testing.T) []string { return rowKeys(t, l.data, l.journal) }

func aidOf(t *testing.T, id eventstore.AggregateID) string {
	t.Helper()
	aid, err := eventstore.AidString(id)
	require.NoError(t, err)
	return aid
}

type captureHandler struct {
	mu      sync.Mutex
	records int
	panics  bool
}

func (*captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	h.records++
	h.mu.Unlock()
	if h.panics {
		panic(errors.New("assertion failed inside the logging path"))
	}
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }
func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records
}

func captureLog(t *testing.T, panics bool) *captureHandler {
	t.Helper()
	previous := slog.Default()
	handler := &captureHandler{panics: panics}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func failAt(hooks *testhook.Hooks, phase testhook.Phase, message string) {
	hooks.OnFail(phase, func(testhook.Point) error { return &testhook.InjectedError{Phase: phase, Message: message} })
}

// deleteRow removes the configuration row (key 0x00) from one table, as the documented recovery procedure does.
func deleteRow(t *testing.T, l *layout, tableID string) {
	t.Helper()
	mutation := gbigtable.NewMutation()
	mutation.DeleteRow()
	require.NoError(t, l.data.Open(tableID).Apply(context.Background(), "\x00", mutation))
}
