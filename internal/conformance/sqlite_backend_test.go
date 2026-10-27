package conformance_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/sqlite"
	_ "modernc.org/sqlite" // driver "sqlite": the adapter seeds configuration, observes history and holds the write lock through it.
)

// sqliteBackend runs the conformance data against real SQLite files in a private temporary directory.
// It reuses publicStore (the public-API adapter shared with the memory backend) for the four operations.
type sqliteBackend struct {
	mu     sync.Mutex
	shadow *publicBackend
	dir    string
	path   string
	stores []*sqlite.Store
	// holder is the connection that holdWriteLock opened. It is separate from every Store connection.
	holder   *sql.Conn
	holderDB *sql.DB
}

func newSQLiteBackend() *sqliteBackend {
	return &sqliteBackend{shadow: &publicBackend{name: "sqlite", notifications: map[int][]string{}, waits: map[int][]time.Duration{}}}
}

func (b *sqliteBackend) Name() string { return "sqlite" }

// Injectable follows the memory backend: serialization faults and storage-error hooks at the store boundary.
func (b *sqliteBackend) Injectable(f conformance.FaultSpec) bool {
	if strings.HasPrefix(f.Phase, "serialize-") || strings.HasPrefix(f.Phase, "deserialize-") {
		return f.Kind == "serialization-error"
	}
	return f.Kind == "storage-error" || f.Phase == "retention-query" && f.Kind == "sdk-response"
}

// Prepare creates an isolated file and initializes it with the retention count of the scenario.
func (b *sqliteBackend) Prepare(ctx context.Context, plan *conformance.ScenarioPlan) (conformance.Backend, func() error, error) {
	dir, err := os.MkdirTemp("", "esa-sqlite-conformance-")
	if err != nil {
		return nil, nil, err
	}
	fresh := newSQLiteBackend()
	fresh.dir = dir
	fresh.path = filepath.Join(dir, "events.db")
	cleanup := func() error {
		fresh.releaseLock()
		fresh.closeStores()
		return os.RemoveAll(dir)
	}
	opts, err := b.initOptions(plan.Store)
	if err == nil {
		err = sqlite.Initialize(ctx, fresh.path, sqlite.Config{}, opts...)
	}
	if err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	return fresh, cleanup, nil
}

// initOptions never asks for TTL or a change feed: those requests must be rejected by Open, not by Prepare.
func (b *sqliteBackend) initOptions(cfg conformance.StoreConfig) ([]eventstore.Option, error) {
	if cfg.RetentionCount == nil {
		return nil, nil
	}
	count, err := eventstore.KeepLatest(int(*cfg.RetentionCount))
	if err != nil {
		// Invalid counts are exercised by Open; the file is initialized without history.
		return nil, nil
	}
	return []eventstore.Option{eventstore.WithRetentionCount(count)}, nil
}

func (b *sqliteBackend) Seed(context.Context, []map[string]any) error {
	return fmt.Errorf("SQLite scenarios do not support physical seed items")
}

// SeedConfiguration replaces the config row that Prepare wrote with the values the scenario declares.
func (b *sqliteBackend) SeedConfiguration(ctx context.Context, values map[string]any) error {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(b.path))
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, "UPDATE config SET store_id = ?, layout_version = ?, keep_snapshot_count = ?, retention_mode = ?, change_feed_enabled = ?",
		values["store_id"], integerValue(values["layout_version"]), integerValue(values["keep_snapshot_count"]), values["retention_mode"], integerValue(values["change_feed_enabled"]))
	return err
}

func (b *sqliteBackend) open(ctx context.Context, cfg conformance.StoreConfig, hooks *testhook.Hooks) (*publicStore, *sqlite.Store, error) {
	opts, err := b.shadow.options(cfg, hooks)
	if err != nil {
		return nil, nil, err
	}
	config := sqlite.Config{ChangeFeed: cfg.ChangeFeed}
	if cfg.LockWaitMilliseconds != nil {
		wait := time.Duration(*cfg.LockWaitMilliseconds) * time.Millisecond
		config.LockWait = &wait
	}
	store, err := sqlite.Open(ctx, b.path, config, opts...)
	if err != nil {
		return nil, nil, err
	}
	events := &payloadTreeSerializer{raw: &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}
	snapshots := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeSnapshot, deserialize: testhook.PhaseDeserializeSnapshot}
	entry, err := sqlite.New[any, json.RawMessage](store, events, snapshots)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	b.mu.Lock()
	b.stores = append(b.stores, store)
	b.mu.Unlock()
	return &publicStore{actual: entry, backend: b.shadow, hooks: hooks, written: map[int]any{}, read: map[int][]any{}}, store, nil
}

func (b *sqliteBackend) Open(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection) (conformance.Store, error) {
	store, owner, err := b.open(ctx, cfg, inj.Hooks)
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	return &sqliteStore{publicStore: store, owner: owner}, nil
}

// CreateStore opens the same file again with the same settings. New and shared stores both use the file,
// because SQLite keeps the settings in the file and the client settings are compared with them.
func (b *sqliteBackend) CreateStore(ctx context.Context, cfg conformance.StoreConfig, inj conformance.Injection, _ conformance.Store) (conformance.Store, error) {
	return b.Open(ctx, cfg, inj)
}

func (b *sqliteBackend) HoldWriteLock(ctx context.Context) error {
	if b.holder != nil {
		return fmt.Errorf("the write lock is already held")
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(b.path))
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return err
	}
	b.holder, b.holderDB = conn, db
	return nil
}

func (b *sqliteBackend) ReleaseWriteLock(ctx context.Context) error {
	if b.holder == nil {
		return fmt.Errorf("the write lock is not held")
	}
	_, err := b.holder.ExecContext(ctx, "ROLLBACK")
	b.releaseLock()
	return err
}

func (b *sqliteBackend) releaseLock() {
	if b.holder != nil {
		_ = b.holder.Close()
		_ = b.holderDB.Close()
		b.holder, b.holderDB = nil, nil
	}
}

func (b *sqliteBackend) closeStores() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, store := range b.stores {
		_ = store.Close()
	}
	b.stores = nil
}

// Observe reads the actual snapshot history of the file through a separate connection.
func (b *sqliteBackend) Observe(ctx context.Context, operation int, step conformance.StepPlan) (map[string]any, error) {
	b.shadow.mu.Lock()
	notifications := append([]string{}, b.shadow.notifications[operation]...)
	b.shadow.mu.Unlock()
	out := map[string]any{"notifications": notifications}
	if _, requested := step.Observe["history"]; requested {
		db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(b.path))
		if err != nil {
			return nil, err
		}
		defer db.Close()
		rows, err := db.QueryContext(ctx, "SELECT seq_nr FROM snapshot_history WHERE aid = ? ORDER BY seq_nr", step.AID.TypeName+"-"+step.AID.Value)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		active := []any{}
		for rows.Next() {
			var n int64
			if err := rows.Scan(&n); err != nil {
				return nil, err
			}
			active = append(active, n)
		}
		if err := rows.Err(); err != nil {
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

// sqliteStore closes the Store it owns, so that a scenario never leaves a connection holding the file.
type sqliteStore struct {
	*publicStore
	owner *sqlite.Store
}

func (s *sqliteStore) Close() error { return s.owner.Close() }

// integerValue converts a number of the decoded JSON (json.Number) into an int64 that SQLite stores as INTEGER.
func integerValue(v any) any {
	number, ok := v.(json.Number)
	if !ok {
		return v
	}
	n, err := number.Int64()
	if err != nil {
		return v
	}
	return n
}
