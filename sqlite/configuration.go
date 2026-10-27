package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
)

// Initialize creates the layout and the config row in a new file or an empty database (SQ-2, SQ-D6).
// Any other database, including an initialized one, is rejected with a ConfigurationError and keeps its tables.
// Settings are validated before the file is created. TTL retention and a change feed are configuration errors.
func Initialize(ctx context.Context, path string, cfg Config, opts ...eventstore.Option) error {
	checked, err := validateConfig(cfg, opts...)
	if err != nil {
		return err
	}
	c, err := openConnection(ctx, path, true, checked)
	if err != nil {
		return err
	}
	defer c.close()
	return initializeLayout(ctx, c.conn, checked)
}

func initializeLayout(ctx context.Context, conn *sql.Conn, cfg settings) (err error) {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()
	empty, err := isUninitialized(ctx, conn)
	if err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if !empty {
		return &eventstore.ConfigurationError{Cause: errors.New("the SQLite database is not empty")}
	}
	for _, statement := range createTableStatements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return &eventstore.StorageError{Cause: err}
		}
	}
	storeID, err := newStoreID()
	if err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled)
	  VALUES (1, ?, 1, ?, 'delete', 0)`, storeID, cfg.keepSnapshotCount()); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	committed = true
	return nil
}

// verifyConfiguration verifies the layout and compares the config row with the client settings (SQ-2, SQ-19, SQ-20).
func verifyConfiguration(ctx context.Context, conn *sql.Conn, cfg settings) error {
	if err := verifyLayout(ctx, conn); err != nil {
		return err
	}
	mismatch := func(detail string) error {
		return &eventstore.ConfigurationError{Cause: fmt.Errorf("SQLite configuration mismatch: %s", detail)}
	}
	rows, err := conn.QueryContext(ctx, "SELECT store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled FROM config")
	if err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	defer rows.Close()
	type row struct {
		storeID    string
		layout     int64
		keep       sql.NullInt64
		mode       string
		changeFeed int64
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.storeID, &r.layout, &r.keep, &r.mode, &r.changeFeed); err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if len(found) != 1 {
		return mismatch("config must have exactly one row")
	}
	r := found[0]
	if r.layout != 1 {
		return &eventstore.ConfigurationError{Cause: fmt.Errorf("unsupported SQLite layout version %d", r.layout)}
	}
	if !isLowercaseUUID(r.storeID) {
		return mismatch("store_id is not a lowercase UUID")
	}
	switch r.changeFeed {
	case 0:
	case 1:
		return &eventstore.ConfigurationError{Cause: errors.New("the file enables a change feed, which this storage does not support")}
	default:
		return mismatch("change_feed_enabled is neither 0 nor 1")
	}
	want := cfg.keepSnapshotCount()
	if r.mode != "delete" || r.keep.Valid != (want != nil) || (want != nil && r.keep.Int64 != *want) {
		return mismatch("retention differs from the client settings")
	}
	return nil
}

// newStoreID returns a random version 4 UUID in lowercase text form.
func newStoreID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// isLowercaseUUID accepts only the 36-character form with lowercase hexadecimal digits and hyphens.
func isLowercaseUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return false
			}
		default:
			c := s[i]
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}
