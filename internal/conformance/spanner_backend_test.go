package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	gspanner "cloud.google.com/go/spanner"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/spannertest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"google.golang.org/api/iterator"
)

var spannerPrefixes atomic.Int64

// spannerBackend runs the conformance data against a real Spanner emulator. Every scenario gets its own table prefix
// inside one database, as the JavaScript implementation's runner does. It reuses publicStore for the four operations.
type spannerBackend struct {
	shadow   *publicBackend
	database *spannertest.Database
	prefix   string
	client   *gspanner.Client
	plan     *conformance.ScenarioPlan
}

func newSpannerBackend(database *spannertest.Database) *spannerBackend {
	return &spannerBackend{
		database: database,
		shadow:   &publicBackend{name: "spanner", notifications: map[int][]string{}, waits: map[int][]time.Duration{}},
	}
}

func (b *spannerBackend) Name() string { return "spanner" }

// Injectable follows SQLite for the faults at the store boundary and adds the commit abort of SPN-7.
func (b *spannerBackend) Injectable(f conformance.FaultSpec) bool {
	if strings.HasPrefix(f.Phase, "serialize-") || strings.HasPrefix(f.Phase, "deserialize-") {
		return f.Kind == "serialization-error"
	}
	return f.Kind == "storage-error" || f.Kind == "transaction-aborted" && f.Phase == "commit" || f.Phase == "retention-query" && f.Kind == "sdk-response"
}

// Prepare provisions the tables the way an operator does: the DDL for the mode of the scenario (the row deletion policy
// of the seed wins over the store setting) and a config row that matches the store settings.
func (b *spannerBackend) Prepare(ctx context.Context, plan *conformance.ScenarioPlan) (conformance.Backend, func() error, error) {
	prefix := fmt.Sprintf("c%d", spannerPrefixes.Add(1))
	fresh := newSpannerBackend(b.database)
	fresh.prefix, fresh.plan = prefix, plan
	var policyDays int64
	switch {
	case plan.SeedRowDeletionPolicyDays != nil:
		policyDays = *plan.SeedRowDeletionPolicyDays
	case plan.Store.RetentionMode == "ttl" && plan.Store.TTLGraceDays != nil:
		policyDays = *plan.Store.TTLGraceDays
	}
	cleanup := func() error {
		if fresh.client != nil {
			fresh.client.Close()
		}
		return nil
	}
	if err := b.database.ApplyDDL(ctx, spannertest.LayoutDDL(prefix, max(policyDays, 0))...); err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	row := spannertest.ConfigRow{StoreID: "6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b", LayoutVersion: 1, RetentionMode: "delete"}
	if plan.Store.RetentionCount != nil && *plan.Store.RetentionCount > 0 {
		row.KeepSnapshotCount = plan.Store.RetentionCount
	}
	if plan.Store.RetentionMode == "ttl" && plan.Store.TTLGraceDays != nil && *plan.Store.TTLGraceDays > 0 {
		row.RetentionMode, row.TTLGraceDays = "ttl", plan.Store.TTLGraceDays
	}
	client, err := b.database.NewClient(ctx)
	if err == nil {
		fresh.client = client
		err = b.database.InsertConfig(ctx, prefix, row)
	}
	if err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	return fresh, cleanup, nil
}

func (b *spannerBackend) Seed(context.Context, []map[string]any) error {
	return fmt.Errorf("Spanner scenarios do not support physical seed items")
}

// SeedConfiguration replaces the config row that Prepare wrote with the values the scenario declares.
func (b *spannerBackend) SeedConfiguration(ctx context.Context, values map[string]any) error {
	statement := gspanner.NewStatement("UPDATE " + b.prefix + "_config SET store_id = @store_id, layout_version = @layout_version, keep_snapshot_count = @keep, retention_mode = @mode, ttl_grace_days = @grace, change_feed_enabled = @feed WHERE singleton = 1")
	statement.Params = map[string]any{
		"store_id": values["store_id"], "layout_version": nullableInteger(values["layout_version"]),
		"keep": nullableInteger(values["keep_snapshot_count"]), "mode": values["retention_mode"],
		"grace": nullableInteger(values["ttl_grace_days"]), "feed": values["change_feed_enabled"],
	}
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *gspanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, statement)
		return err
	})
	return err
}

func (b *spannerBackend) open(ctx context.Context, cfg conformance.StoreConfig, hooks *testhook.Hooks) (*publicStore, error) {
	hooks.SetSleeper(func(d time.Duration) {
		b.shadow.mu.Lock()
		defer b.shadow.mu.Unlock()
		b.shadow.waits[b.shadow.operation] = append(b.shadow.waits[b.shadow.operation], d)
	})
	opts, err := b.shadow.options(cfg, hooks)
	if err != nil {
		return nil, err
	}
	config := spanner.Config{TablePrefix: b.prefix, ChangeFeed: cfg.ChangeFeed}
	if cfg.TTLGraceDays != nil {
		config.TTLGraceDays = *cfg.TTLGraceDays
	}
	if cfg.TransactionRetryLimit != nil {
		limit := int(*cfg.TransactionRetryLimit)
		config.TransactionRetryLimit = &limit
	}
	events := &payloadTreeSerializer{raw: &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}
	snapshots := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeSnapshot, deserialize: testhook.PhaseDeserializeSnapshot}
	entry, err := spanner.New[any, json.RawMessage](ctx, b.client, config, events, snapshots, opts...)
	if err != nil {
		return nil, err
	}
	return &publicStore{actual: entry, backend: b.shadow, hooks: hooks, written: map[int]any{}, read: map[int][]any{}}, nil
}

func (b *spannerBackend) Open(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection) (conformance.Store, error) {
	store, err := b.open(ctx, cfg, inj.Hooks)
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	return store, nil
}

// CreateStore opens the same destination again with the same settings: new and shared stores both use the tables.
func (b *spannerBackend) CreateStore(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection, _ conformance.Store) (conformance.Store, error) {
	return b.Open(ctx, cfg, inj)
}

// Observe reads the actual snapshot history through the independent client: unmarked rows are active, marked rows are
// those whose expired_at is set.
func (b *spannerBackend) Observe(ctx context.Context, operation int, step conformance.StepPlan) (map[string]any, error) {
	b.shadow.mu.Lock()
	notifications := append([]string{}, b.shadow.notifications[operation]...)
	b.shadow.mu.Unlock()
	out := map[string]any{"notifications": notifications}
	if _, requested := step.Observe["history"]; requested {
		statement := gspanner.NewStatement("SELECT seq_nr, expired_at IS NOT NULL FROM " + b.prefix + "_snapshot_history WHERE aid = @aid ORDER BY seq_nr")
		statement.Params = map[string]any{"aid": step.AID.TypeName + "-" + step.AID.Value}
		rows := b.client.Single().Query(ctx, statement)
		defer rows.Stop()
		active, marked := []any{}, []any{}
		for {
			row, err := rows.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			var seq int64
			var isMarked bool
			if err := row.Columns(&seq, &isMarked); err != nil {
				return nil, err
			}
			if isMarked {
				marked = append(marked, seq)
			} else {
				active = append(active, seq)
			}
		}
		out["history"] = map[string]any{"active": active, "marked": marked}
	}
	value, err := conformance.JSONValue(out)
	if err != nil {
		return nil, err
	}
	return value.(map[string]any), nil
}

// nullableInteger converts a decoded JSON number into the *int64 that a nullable INT64 parameter takes.
func nullableInteger(v any) any {
	switch number := v.(type) {
	case nil:
		return gspanner.NullInt64{}
	case json.Number:
		n, err := number.Int64()
		if err != nil {
			return v
		}
		return gspanner.NullInt64{Int64: n, Valid: true}
	}
	return v
}
