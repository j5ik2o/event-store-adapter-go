package sqlite

import (
	"context"
	"database/sql"
	"errors"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// getLatestSnapshotByID reads head and snapshot_current in one read transaction (SQ-8).
// Restoration of the payload belongs to the caller after the Store lock is released.
func (s *Store) getLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, err
	}
	if err := s.lock(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	conn := s.c.conn
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return nil, storageError(err)
	}
	done := false
	defer func() {
		if !done {
			rollback(conn)
		}
	}()

	head, err := readHead(ctx, conn, aid)
	if err != nil {
		return nil, err
	}
	if err := s.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadSnapshot, AggregateID: aid}); err != nil {
		return nil, storageError(err)
	}
	var result *eventstore.SnapshotRead[[]byte]
	if head != nil {
		result = &eventstore.SnapshotRead[[]byte]{HeadSeqNr: *head}
		var seqValue int64
		var manifest string
		var payload []byte
		err := conn.QueryRowContext(ctx, "SELECT seq_nr, manifest, payload FROM snapshot_current WHERE aid = ?", aid).Scan(&seqValue, &manifest, &payload)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return nil, storageError(err)
		default:
			seqNr, err := storedSeqNr(seqValue)
			if err != nil {
				return nil, err
			}
			snapshot, err := eventstore.NewSnapshotEnvelope(payload, seqNr, eventstore.WithManifest(manifest))
			if err != nil {
				return nil, err
			}
			result.Snapshot = &snapshot
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, storageError(err)
	}
	done = true
	return result, nil
}

// readHead returns the head seq_nr of the aggregate, or nil when it has none.
func readHead(ctx context.Context, conn *sql.Conn, aid string) (*eventstore.SeqNr, error) {
	var value int64
	err := conn.QueryRowContext(ctx, "SELECT seq_nr FROM head WHERE aid = ?", aid).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, storageError(err)
	}
	seqNr, err := storedSeqNr(value)
	if err != nil {
		return nil, err
	}
	return &seqNr, nil
}

// storedSeqNr converts a stored seq_nr and rejects a value outside 1 through 2^53-1.
func storedSeqNr(value int64) (eventstore.SeqNr, error) {
	seqNr := eventstore.SeqNr(value)
	if seqNr < 1 || seqNr > eventstore.MaxSeqNr {
		return 0, storageError(errors.New("a stored seq_nr is out of range"))
	}
	return seqNr, nil
}
