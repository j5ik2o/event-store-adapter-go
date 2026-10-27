package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// minimumVersion is the lowest SQLite library version this storage accepts (SQ-15).
var minimumVersion = [3]int{3, 51, 3}

// meetsMinimumVersion compares a "major.minor.patch" string numerically, component by component.
// Anything that is not exactly three integers is rejected.
func meetsMinimumVersion(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}
	var numbers [3]int
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return false
		}
		numbers[i] = n
	}
	for i := range numbers {
		if numbers[i] != minimumVersion[i] {
			return numbers[i] > minimumVersion[i]
		}
	}
	return true
}

// fileURI builds a "file:" URI for an absolute path so that the name is never read as a URI by accident.
// Open mode "rw" never creates the file; "rwc" is used by Initialize only.
func fileURI(path string, create bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	mode := "rw"
	if create {
		mode = "rwc"
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(absolute), RawQuery: "mode=" + mode + "&cache=private"}
	return u.String(), nil
}

// connection is the single database connection that a Store (or Initialize) owns.
type connection struct {
	db   *sql.DB
	conn *sql.Conn
}

func (c *connection) close() error {
	return errors.Join(c.conn.Close(), c.db.Close())
}

// openConnection opens the file, checks the library version and applies and verifies the connection settings (SQ-1, SQ-14, SQ-15).
func openConnection(ctx context.Context, path string, create bool, cfg settings) (*connection, error) {
	uri, err := fileURI(path, create)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, &eventstore.StorageError{Cause: err}
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, &eventstore.StorageError{Cause: err}
	}
	c := &connection{db: db, conn: conn}
	if err := c.configure(ctx, cfg); err != nil {
		_ = c.close()
		return nil, err
	}
	return c, nil
}

func (c *connection) configure(ctx context.Context, cfg settings) error {
	var version string
	if err := c.conn.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if !meetsMinimumVersion(version) {
		return &eventstore.ConfigurationError{Cause: fmt.Errorf("unsupported SQLite version %q", version)}
	}
	mismatch := func(name string, actual any) error {
		return &eventstore.ConfigurationError{Cause: fmt.Errorf("connection setting %s is %v", name, actual)}
	}
	// The wait is set first so that the following settings also wait for locks.
	waitMillis := cfg.lockWait.Milliseconds()
	if _, err := c.conn.ExecContext(ctx, "PRAGMA busy_timeout = "+strconv.FormatInt(waitMillis, 10)); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	// locking_mode is NORMAL before WAL is enabled: EXCLUSIVE would lock other connections out.
	for _, pragma := range []struct{ set, name, want string }{
		{"PRAGMA locking_mode = NORMAL", "locking_mode", "normal"},
		{"PRAGMA journal_mode = WAL", "journal_mode", "wal"},
	} {
		var actual string
		if err := c.conn.QueryRowContext(ctx, pragma.set).Scan(&actual); err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		if !strings.EqualFold(actual, pragma.want) {
			return mismatch(pragma.name, actual)
		}
	}
	if _, err := c.conn.ExecContext(ctx, "PRAGMA synchronous = FULL"); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	if _, err := c.conn.ExecContext(ctx, "PRAGMA read_uncommitted = OFF"); err != nil {
		return &eventstore.StorageError{Cause: err}
	}
	for _, pragma := range []struct {
		query, name string
		want        int64
	}{
		{"PRAGMA synchronous", "synchronous", 2},
		{"PRAGMA read_uncommitted", "read_uncommitted", 0},
		{"PRAGMA busy_timeout", "busy_timeout", waitMillis},
	} {
		var actual int64
		if err := c.conn.QueryRowContext(ctx, pragma.query).Scan(&actual); err != nil {
			return &eventstore.StorageError{Cause: err}
		}
		if actual != pragma.want {
			return mismatch(pragma.name, actual)
		}
	}
	return nil
}
