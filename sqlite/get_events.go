package sqlite

import (
	"context"
	"errors"
	"strings"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// getEventsByIDSinceSeqNr reads the head h and then the journal up to h in one read transaction (SQ-9).
// A gap or a missing tail is a StorageError: a shortened result is never returned as a success.
func (s *Store) getEventsByIDSinceSeqNr(ctx context.Context, id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		if violation, ok := err.(*eventstore.ContractViolationError); ok {
			violation.SeqNr = &seqNr
		}
		return nil, err
	}
	if err := seqNr.Validate(); err != nil {
		return nil, err
	}
	typeName, value, _ := strings.Cut(aid, "-")
	fixedID, err := eventstore.NewAggregateID(typeName, value)
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
	if err := s.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadEvents, AggregateID: aid, SeqNr: int64(seqNr)}); err != nil {
		return nil, storageError(err)
	}
	var events []eventstore.EventEnvelope[[]byte]
	start := max(seqNr, 1)
	if head != nil && start <= *head {
		rows, err := conn.QueryContext(ctx, `SELECT seq_nr, occurred_at_ns, manifest, payload FROM journal
		  WHERE aid = ? AND seq_nr >= ? AND seq_nr <= ? ORDER BY seq_nr`, aid, int64(start), int64(*head))
		if err != nil {
			return nil, storageError(err)
		}
		next := start
		for rows.Next() {
			var seqValue, occurredAtNs int64
			var manifest string
			var payload []byte
			if err := rows.Scan(&seqValue, &occurredAtNs, &manifest, &payload); err != nil {
				_ = rows.Close()
				return nil, storageError(err)
			}
			stored, err := storedSeqNr(seqValue)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			if stored != next {
				_ = rows.Close()
				return nil, storageError(errors.New("the journal has a gap"))
			}
			event, err := eventstore.NewEventEnvelope(fixedID, stored, time.Unix(0, occurredAtNs).UTC(), payload, eventstore.WithManifest(manifest))
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			events = append(events, event)
			next++
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, storageError(err)
		}
		if next != *head+1 {
			return nil, storageError(errors.New("the journal lacks events up to the head"))
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, storageError(err)
	}
	done = true
	return events, nil
}
