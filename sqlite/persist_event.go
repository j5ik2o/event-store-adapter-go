package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

func (s *Store) persistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return s.commit(ctx, event, nil)
}

func (s *Store) persistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	seqNr, snapshotSeqNr := event.SeqNr(), snapshot.SeqNr()
	if seqNr != snapshotSeqNr {
		return &eventstore.ContractViolationError{Rule: "W-9", SeqNr: &seqNr, SnapshotSeqNr: &snapshotSeqNr}
	}
	return s.commit(ctx, event, &snapshot)
}

// commit appends one event, with its snapshot if present, in one transaction, then applies retention in a second one.
// The retention failure is reported after the Store lock is released and never changes the append result.
func (s *Store) commit(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot *eventstore.SnapshotEnvelope[[]byte]) error {
	if err := s.lock(); err != nil {
		return err
	}
	var retentionErr error
	err := s.appendLocked(ctx, event, snapshot)
	if err == nil && snapshot != nil && s.settings.common.RetentionCount != nil {
		retentionErr = s.retainHistoryLocked(ctx, event.AggregateID(), snapshot.SeqNr())
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if retentionErr != nil {
		s.notifyRetentionFailure(ctx, event.AggregateID(), retentionErr)
	}
	return nil
}

// appendLocked takes the write lock first (BEGIN IMMEDIATE) and only then compares the head (SQ-5, SQ-17).
// Every failure rolls the transaction back, so nothing is committed (SQ-6).
func (s *Store) appendLocked(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot *eventstore.SnapshotEnvelope[[]byte]) (err error) {
	conn := s.c.conn
	aid, seqNr := event.AggregateID(), event.SeqNr()
	typeName, _, _ := strings.Cut(aid, "-")
	occurredAtNs := event.OccurredAt().UnixNano()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return storageError(err)
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()

	var head *eventstore.SeqNr
	var headValue int64
	switch err := conn.QueryRowContext(ctx, "SELECT seq_nr FROM head WHERE aid = ?", aid).Scan(&headValue); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return storageError(err)
	default:
		h := eventstore.SeqNr(headValue)
		if err := h.Validate(); err != nil {
			return storageError(errors.New("a stored seq_nr is out of range"))
		}
		head = &h
	}
	if head != nil && seqNr <= *head {
		return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: head}
	}
	var expected eventstore.SeqNr = 1
	if head != nil {
		expected = *head + 1
	}
	if seqNr != expected {
		return &eventstore.ContractViolationError{Rule: "W-8", SeqNr: &seqNr}
	}

	if _, err := conn.ExecContext(ctx, "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (?, ?, ?, ?, ?)",
		aid, int64(seqNr), occurredAtNs, event.Manifest(), event.Payload()); err != nil {
		return appendError(err, aid, seqNr, head)
	}
	if head != nil {
		_, err = conn.ExecContext(ctx, "UPDATE head SET seq_nr = ?, event_occurred_at_ns = ?, event_manifest = ?, event_payload = ? WHERE aid = ?",
			int64(seqNr), occurredAtNs, event.Manifest(), event.Payload(), aid)
	} else {
		_, err = conn.ExecContext(ctx, "INSERT INTO head (aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload) VALUES (?, ?, ?, ?, ?, ?)",
			aid, typeName, int64(seqNr), occurredAtNs, event.Manifest(), event.Payload())
	}
	if err != nil {
		return appendError(err, aid, seqNr, head)
	}
	if snapshot != nil {
		if _, err := conn.ExecContext(ctx, `INSERT INTO snapshot_current (aid, seq_nr, manifest, payload) VALUES (?, ?, ?, ?)
		  ON CONFLICT(aid) DO UPDATE SET seq_nr = excluded.seq_nr, manifest = excluded.manifest, payload = excluded.payload`,
			aid, int64(seqNr), snapshot.Manifest(), snapshot.Aggregate()); err != nil {
			return storageError(err)
		}
		if s.settings.common.RetentionCount != nil {
			if _, err := conn.ExecContext(ctx, "INSERT INTO snapshot_history (aid, seq_nr, manifest, payload) VALUES (?, ?, ?, ?)",
				aid, int64(seqNr), snapshot.Manifest(), snapshot.Aggregate()); err != nil {
				return storageError(err)
			}
		}
	}
	if err := s.hooks.Fail(testhook.Point{
		Phase: testhook.PhaseCommit, AggregateID: aid, SeqNr: int64(seqNr), Payload: bytes.Clone(event.Payload()),
	}); err != nil {
		return storageError(err)
	}
	// The success of COMMIT is the point of no return. If it fails the outcome is unknown, so it is a
	// StorageError and the caller must not renumber the event.
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return storageError(err)
	}
	committed = true
	return nil
}
