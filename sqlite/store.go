package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// Store owns one connection to the SQLite file and serializes the operations that use it,
// so that a connection is never shared by two operations at once (SQ-17).
// Calls through the same *Store share the connection and the verified settings.
// Its zero value is invalid; use Open.
type Store struct {
	mu       sync.Mutex
	c        *connection
	closed   bool
	settings settings
	hooks    *testhook.Hooks
}

// Open opens an initialized file, verifies its layout and config against the client settings and returns the Store.
// It never creates a file and never initializes a database. A missing file is a StorageError.
func Open(ctx context.Context, path string, cfg Config, opts ...eventstore.Option) (*Store, error) {
	checked, err := validateConfig(cfg, opts...)
	if err != nil {
		return nil, err
	}
	c, err := openConnection(ctx, path, false, checked)
	if err != nil {
		return nil, err
	}
	if err := verifyInReadTransaction(ctx, c.conn, checked); err != nil {
		_ = c.close()
		return nil, err
	}
	return &Store{c: c, settings: checked, hooks: checked.common.Hooks}, nil
}

// verifyInReadTransaction reads the layout and config from one point in time.
func verifyInReadTransaction(ctx context.Context, conn *sql.Conn, cfg settings) error {
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if err := verifyConfiguration(ctx, conn, cfg); err != nil {
		rollback(conn)
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		rollback(conn)
		return &eventstore.StorageError{Cause: err}
	}
	return nil
}

// Close releases the connection. Closing again returns nil; later operations fail with a StorageError.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.c.close()
}

var errClosed = errors.New("sqlite store is closed")

// lock takes the Store lock for one operation. The caller unlocks it.
func (s *Store) lock() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return &eventstore.StorageError{Cause: errClosed}
	}
	return nil
}

// rollback ends an open transaction. It ignores the caller's cancellation so that a cancelled operation
// still leaves the connection outside a transaction. A failure here leaves nothing for the caller to decide.
func rollback(conn *sql.Conn) {
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
}
