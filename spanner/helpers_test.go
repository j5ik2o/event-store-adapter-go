package spanner_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/spannertest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const validStoreID = "6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b"

var (
	emulator    *spannertest.Environment
	database    *spannertest.Database
	prefixCount atomic.Int64
)

// TestMain starts one real emulator for the package. A failure to start it fails the run: no test is skipped.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	environment, err := spannertest.Start(ctx)
	if err == nil {
		emulator = environment
		database, err = environment.CreateDatabase(ctx)
	}
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot start the Spanner emulator:", err)
		if emulator != nil {
			_ = emulator.Close(context.Background())
		}
		os.Exit(1)
	}
	code := m.Run()
	if err := emulator.Close(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "cannot stop the Spanner emulator:", err)
		code = 1
	}
	os.Exit(code)
}

// layoutSpec describes the destination an operator provisions outside the library.
type layoutSpec struct {
	// keep is the keep_snapshot_count of the config row; nil means NULL.
	keep *int64
	// graceDays > 0 creates the TTL layout (row deletion policy and ttl_grace_days); 0 creates the delete layout.
	graceDays int64
	// config overrides the row that is written after the DDL. nil derives it from keep and graceDays.
	config *spannertest.ConfigRow
	// skipConfig leaves the config table empty.
	skipConfig bool
}

func ptr[T any](v T) *T { return &v }

// layout is a provisioned prefix inside the shared database with a raw client that is independent of every Store.
type layout struct {
	prefix string
	raw    *gspanner.Client
	spec   layoutSpec
}

func provision(t *testing.T, spec layoutSpec) *layout {
	t.Helper()
	prefix := fmt.Sprintf("t%d", prefixCount.Add(1))
	ctx := context.Background()
	require.NoError(t, database.ApplyDDL(ctx, spannertest.LayoutDDL(prefix, spec.graceDays)...))
	if !spec.skipConfig {
		row := spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: spec.keep, RetentionMode: "delete"}
		if spec.graceDays > 0 {
			row.RetentionMode = "ttl"
			row.TTLGraceDays = ptr(spec.graceDays)
		}
		if spec.config != nil {
			row = *spec.config
		}
		require.NoError(t, database.InsertConfig(ctx, prefix, row))
	}
	raw := newClient(t)
	return &layout{prefix: prefix, raw: raw, spec: spec}
}

func newClient(t *testing.T, opts ...option.ClientOption) *gspanner.Client {
	t.Helper()
	client, err := database.NewClient(context.Background(), opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func (l *layout) table(name string) string { return l.prefix + "_" + name }

func (l *layout) config() spanner.Config {
	cfg := spanner.Config{TablePrefix: l.prefix}
	if l.spec.graceDays > 0 {
		cfg.TTLGraceDays = l.spec.graceDays
	}
	return cfg
}

// options returns the store options that match the layout: the retention count and mode.
func (l *layout) options(t *testing.T, extra ...eventstore.Option) []eventstore.Option {
	t.Helper()
	var opts []eventstore.Option
	if l.spec.keep != nil {
		opts = append(opts, keepOption(t, int(*l.spec.keep)))
	}
	if l.spec.graceDays > 0 {
		opts = append(opts, eventstore.WithRetentionMode(eventstore.RetentionTTL))
	}
	return append(opts, extra...)
}

func (l *layout) tryOpen(client *gspanner.Client, cfg spanner.Config, opts ...eventstore.Option) (eventstore.EventStore[string, string], error) {
	return spanner.New(context.Background(), client, cfg, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), opts...)
}

// store creates a Store over client with the layout's matching settings plus extra options.
func (l *layout) store(t *testing.T, client *gspanner.Client, extra ...eventstore.Option) eventstore.EventStore[string, string] {
	t.Helper()
	entry, err := l.tryOpen(client, l.config(), l.options(t, extra...)...)
	require.NoError(t, err)
	return entry
}

func (l *layout) newEntry(t *testing.T, extra ...eventstore.Option) eventstore.EventStore[string, string] {
	t.Helper()
	return l.store(t, l.raw, extra...)
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

// queryInt runs a query that returns one INT64 and reads it through an independent strong read.
func queryInt(t *testing.T, client *gspanner.Client, sql string, params map[string]any) int64 {
	t.Helper()
	values := queryInts(t, client, sql, params)
	require.Len(t, values, 1, sql)
	return values[0]
}

func queryInts(t *testing.T, client *gspanner.Client, sql string, params map[string]any) []int64 {
	t.Helper()
	statement := gspanner.NewStatement(sql)
	statement.Params = params
	rows := client.Single().Query(context.Background(), statement)
	defer rows.Stop()
	out := []int64{}
	for {
		row, err := rows.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}
		require.NoError(t, err, sql)
		var n int64
		require.NoError(t, row.Columns(&n), sql)
		out = append(out, n)
	}
}

func (l *layout) count(t *testing.T, name string) int64 {
	t.Helper()
	return queryInt(t, l.raw, "SELECT COUNT(*) FROM "+l.table(name), nil)
}

func (l *layout) historySeqNrs(t *testing.T, aid string) []int64 {
	t.Helper()
	return queryInts(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_history")+" WHERE aid = @aid ORDER BY seq_nr", map[string]any{"aid": aid})
}

func (l *layout) unmarkedHistorySeqNrs(t *testing.T, aid string) []int64 {
	t.Helper()
	return queryInts(t, l.raw, "SELECT seq_nr FROM "+l.table("snapshot_history")+" WHERE aid = @aid AND expired_at IS NULL ORDER BY seq_nr", map[string]any{"aid": aid})
}

func persistSnapshots(t *testing.T, entry eventstore.EventStore[string, string], id eventstore.AggregateID, from, to eventstore.SeqNr) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, seq, fmt.Sprint(seq)), snapshotAt(t, seq, fmt.Sprint("state-", seq))))
	}
}

func queryString(t *testing.T, client *gspanner.Client, sql string, params map[string]any) string {
	t.Helper()
	statement := gspanner.NewStatement(sql)
	statement.Params = params
	rows := client.Single().Query(context.Background(), statement)
	defer rows.Stop()
	row, err := rows.Next()
	require.NoError(t, err, sql)
	var out gspanner.NullString
	require.NoError(t, row.Columns(&out), sql)
	return out.StringVal
}
