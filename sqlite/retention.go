package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// retainHistoryLocked deletes the oldest history rows in a second BEGIN IMMEDIATE, after the append is committed (SQ-10).
// The caller holds the Store lock. The kept rows are the newest `RetentionCount` of the visible history plus the number
// just written. current, head and journal are never touched.
func (s *Store) retainHistoryLocked(ctx context.Context, aid string, justWritten eventstore.SeqNr) (err error) {
	conn := s.c.conn
	count := *s.settings.common.RetentionCount
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()
	if err := s.hooks.Fail(testhook.Point{Phase: testhook.PhaseRetentionQuery, AggregateID: aid, SeqNr: int64(justWritten)}); err != nil {
		return err
	}
	visible, err := visibleHistory(ctx, conn, aid)
	if err != nil {
		return err
	}
	pages, replaced, err := s.hooks.HistoryPages(aid, int64(justWritten))
	if err != nil {
		return err
	}
	if replaced {
		visible = visible[:0]
		for _, page := range pages {
			visible = append(visible, page...)
		}
	}
	visible = append(visible, int64(justWritten))
	slices.Sort(visible)
	visible = slices.Compact(visible)
	if len(visible) > count {
		obsolete := visible[:len(visible)-count]
		if err := s.hooks.Fail(testhook.Point{Phase: testhook.PhaseRetentionDelete, AggregateID: aid, SeqNrs: slices.Clone(obsolete)}); err != nil {
			return err
		}
		for _, seqNr := range obsolete {
			if _, err := conn.ExecContext(ctx, "DELETE FROM snapshot_history WHERE aid = ? AND seq_nr = ?", aid, seqNr); err != nil {
				return err
			}
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

func visibleHistory(ctx context.Context, conn *sql.Conn, aid string) ([]int64, error) {
	rows, err := conn.QueryContext(ctx, "SELECT seq_nr FROM snapshot_history WHERE aid = ? ORDER BY seq_nr DESC", aid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var visible []int64
	for rows.Next() {
		var seqNr int64
		if err := rows.Scan(&seqNr); err != nil {
			return nil, err
		}
		visible = append(visible, seqNr)
	}
	return visible, rows.Err()
}

// notifyRetentionFailure is called only after the Store lock is released. Both notification paths
// preserve the committed success even if application notification code panics.
func (s *Store) notifyRetentionFailure(ctx context.Context, aid string, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("sqlite retention failed: aid=%q: %w", aid, cause)}
	runRetentionNotification(func() {
		slog.ErrorContext(ctx, "sqlite snapshot retention failed", "aid", aid, "error", failure)
	})
	if handler := s.settings.common.RetentionFailureHandler; handler != nil {
		runRetentionNotification(func() { handler(ctx, failure) })
	}
}

func runRetentionNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}
