package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gbigtable "cloud.google.com/go/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtabletest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

var bigtableTables atomic.Int64

const bigtableStoreID = "6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b"

// bigtableBackend runs the conformance data against a real Bigtable emulator. Every scenario gets its own pair of
// tables inside one instance. It reuses publicStore for the four operations.
type bigtableBackend struct {
	shadow      *publicBackend
	environment *bigtabletest.Environment
	journal     string
	head        string
	plan        *conformance.ScenarioPlan
	data        *gbigtable.Client
	admin       *gbigtable.AdminClient
}

func newBigtableBackend(environment *bigtabletest.Environment) *bigtableBackend {
	return &bigtableBackend{
		environment: environment,
		shadow:      &publicBackend{name: "bigtable", notifications: map[int][]string{}, waits: map[int][]time.Duration{}},
	}
}

func (b *bigtableBackend) Name() string { return "bigtable" }

// Injectable accepts the faults at the store boundary (storage errors in any phase, including the two journal-write
// phases) and the serializer faults, as Spanner does.
func (b *bigtableBackend) Injectable(f conformance.FaultSpec) bool {
	if strings.HasPrefix(f.Phase, "serialize-") || strings.HasPrefix(f.Phase, "deserialize-") {
		return f.Kind == "serialization-error"
	}
	return f.Kind == "storage-error" || f.Phase == "retention-query" && f.Kind == "sdk-response"
}

// Prepare provisions the two tables the way an operator does and writes configuration rows that match the settings.
func (b *bigtableBackend) Prepare(ctx context.Context, plan *conformance.ScenarioPlan) (conformance.Backend, func() error, error) {
	n := bigtableTables.Add(1)
	fresh := newBigtableBackend(b.environment)
	fresh.plan, fresh.journal, fresh.head = plan, fmt.Sprintf("cj%d", n), fmt.Sprintf("ch%d", n)
	cleanup := func() error {
		var errs []error
		if fresh.data != nil {
			errs = append(errs, fresh.data.Close())
		}
		if fresh.admin != nil {
			errs = append(errs, fresh.admin.Close())
		}
		return errors.Join(errs...)
	}
	var err error
	if fresh.admin, err = b.environment.NewAdminClient(ctx); err == nil {
		if fresh.data, err = b.environment.NewClient(ctx); err == nil {
			err = bigtabletest.CreateTables(ctx, fresh.admin, fresh.journal, fresh.head)
		}
	}
	if err == nil {
		limit := 1048576
		if plan.Store.EnvelopeLimitBytes != nil {
			limit = int(*plan.Store.EnvelopeLimitBytes)
		}
		var keep *int64
		if plan.Store.RetentionCount != nil && *plan.Store.RetentionCount > 0 {
			keep = plan.Store.RetentionCount
		}
		config := bigtabletest.ConfigJSON(bigtableStoreID, keep, limit)
		for _, table := range []string{fresh.journal, fresh.head} {
			if err == nil {
				err = bigtabletest.WriteConfig(ctx, fresh.data, table, config)
			}
		}
	}
	if err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	return fresh, cleanup, nil
}

func (b *bigtableBackend) Seed(context.Context, []map[string]any) error {
	return fmt.Errorf("Bigtable scenarios do not support physical seed items")
}

// SeedConfiguration replaces the configuration rows that Prepare wrote with the values the scenario declares for
// each table (seed.configuration.journal and seed.configuration.head).
func (b *bigtableBackend) SeedConfiguration(ctx context.Context, values map[string]any) error {
	for table, id := range map[string]string{"journal": b.journal, "head": b.head} {
		row, ok := values[table].(map[string]any)
		if !ok {
			return fmt.Errorf("seed.configuration.%s is missing", table)
		}
		encoded, err := encodeConfigurationRow(row)
		if err != nil {
			return err
		}
		if err := bigtabletest.WriteConfig(ctx, b.data, id, encoded); err != nil {
			return err
		}
	}
	return nil
}

// encodeConfigurationRow turns the decoded seed object into the JSON that is stored. keep_snapshot_count is a decimal
// string in the stored row, while the scenario data carries it as a number or null.
func encodeConfigurationRow(row map[string]any) (string, error) {
	out := map[string]any{}
	for key, value := range row {
		out[key] = value
	}
	if number, ok := row["keep_snapshot_count"].(json.Number); ok {
		out["keep_snapshot_count"] = number.String()
	}
	raw, err := json.Marshal(out)
	return string(raw), err
}

func (b *bigtableBackend) open(ctx context.Context, cfg conformance.StoreConfig, hooks *testhook.Hooks) (*publicStore, error) {
	hooks.SetSleeper(func(d time.Duration) {
		b.shadow.mu.Lock()
		defer b.shadow.mu.Unlock()
		b.shadow.waits[b.shadow.operation] = append(b.shadow.waits[b.shadow.operation], d)
	})
	opts, err := b.shadow.options(cfg, hooks)
	if err != nil {
		return nil, err
	}
	config := bigtable.Config{JournalTableID: b.journal, HeadTableID: b.head, ChangeFeed: cfg.ChangeFeed,
		JournalWriteFailureHandler: func(ctx context.Context, _ error) {
			b.shadow.mu.Lock()
			defer b.shadow.mu.Unlock()
			n := conformance.OperationNumber(ctx)
			b.shadow.notifications[n] = append(b.shadow.notifications[n], "journal-write-failure")
		}}
	if cfg.EnvelopeLimitBytes != nil {
		config.EnvelopeLimitBytes = int(*cfg.EnvelopeLimitBytes)
	}
	events := &payloadTreeSerializer{raw: &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}
	snapshots := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeSnapshot, deserialize: testhook.PhaseDeserializeSnapshot}
	entry, err := bigtable.New[any, json.RawMessage](ctx, b.data, b.admin, config, events, snapshots, opts...)
	if err != nil {
		return nil, err
	}
	return &publicStore{actual: entry, backend: b.shadow, hooks: hooks, written: map[int]any{}, read: map[int][]any{}}, nil
}

func (b *bigtableBackend) Open(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection) (conformance.Store, error) {
	store, err := b.open(ctx, cfg, inj.Hooks)
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	return store, nil
}

// CreateStore opens the same destination again with the same settings.
func (b *bigtableBackend) CreateStore(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection, _ conformance.Store) (conformance.Store, error) {
	return b.Open(ctx, cfg, inj)
}

// Observe reads the actual history through the independent client. A history entry is active when the journal row
// says so, plus the head's snapshot that has not been written to a journal row yet. Nothing is marked: Bigtable
// deletes retired history instead of marking it.
func (b *bigtableBackend) Observe(ctx context.Context, operation int, step conformance.StepPlan) (map[string]any, error) {
	b.shadow.mu.Lock()
	notifications := append([]string{}, b.shadow.notifications[operation]...)
	b.shadow.mu.Unlock()
	out := map[string]any{"notifications": notifications}
	if _, requested := step.Observe["history"]; requested {
		active, err := b.activeHistory(ctx, step.AID.TypeName+"-"+step.AID.Value)
		if err != nil {
			return nil, err
		}
		out["history"] = map[string]any{"active": active, "marked": []any{}}
	}
	value, err := conformance.JSONValue(out)
	if err != nil {
		return nil, err
	}
	return value.(map[string]any), nil
}

func (b *bigtableBackend) activeHistory(ctx context.Context, aid string) ([]any, error) {
	prefix := append(keyhash.Prefix(aid), byte(len(aid)>>8), byte(len(aid)))
	prefix = append(prefix, aid...)
	seqs := map[int64]bool{}
	err := b.data.Open(b.journal).ReadRows(ctx, gbigtable.PrefixRange(string(prefix)), func(row gbigtable.Row) bool {
		key := row.Key()
		if len(key) != len(prefix)+8 {
			return true
		}
		seq := int64(0)
		for _, c := range []byte(key[len(prefix):]) {
			seq = seq<<8 | int64(c)
		}
		for _, item := range row["history"] {
			if item.Column == "history:state" && string(item.Value) == "active" {
				seqs[seq] = true
			}
		}
		return true
	}, gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
	if err != nil {
		return nil, err
	}
	if b.plan != nil && b.plan.Store.RetentionCount != nil && *b.plan.Store.RetentionCount > 0 {
		head, err := b.data.Open(b.head).ReadRow(ctx, string(prefix), gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
		if err != nil {
			return nil, err
		}
		var seqNr, hasSnapshot string
		for _, item := range head["head"] {
			switch item.Column {
			case "head:seq_nr":
				seqNr = string(item.Value)
			case "head:has_snapshot":
				hasSnapshot = string(item.Value)
			}
		}
		if hasSnapshot == "1" && seqNr != "" {
			h, err := strconv.ParseInt(seqNr, 10, 64)
			if err != nil {
				return nil, err
			}
			journalRow := string(prefix) + string([]byte{byte(h >> 56), byte(h >> 48), byte(h >> 40), byte(h >> 32), byte(h >> 24), byte(h >> 16), byte(h >> 8), byte(h)})
			existing, err := b.data.Open(b.journal).ReadRow(ctx, journalRow, gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
			if err != nil {
				return nil, err
			}
			if len(existing) == 0 {
				seqs[h] = true
			}
		}
	}
	ordered := make([]int64, 0, len(seqs))
	for seq := range seqs {
		ordered = append(ordered, seq)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	out := make([]any, len(ordered))
	for i, seq := range ordered {
		out[i] = seq
	}
	return out, nil
}
