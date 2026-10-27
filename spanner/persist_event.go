package spanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"google.golang.org/grpc/codes"
)

func (b *storageBoundary) PersistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	return b.persist(ctx, event, nil)
}

func (b *storageBoundary) PersistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	return b.persist(ctx, event, &snapshot)
}

func (b *storageBoundary) persist(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot *eventstore.SnapshotEnvelope[[]byte]) error {
	aid := event.AggregateID()
	seqNr := event.SeqNr()
	typeName, _, _ := strings.Cut(aid, "-")
	hashPrefix := keyhash.Prefix(aid)
	occurredAtNs := event.OccurredAt().UnixNano()
	point := testhook.Point{Phase: testhook.PhaseCommit, AggregateID: aid, SeqNr: int64(seqNr), Payload: bytes.Clone(event.Payload())}
	t := b.settings.tables

	// Everything below is fixed before the first attempt, so that a re-run writes the same envelope.
	err := b.runReadWrite(ctx, &point, b.settings.retryLimit, func(ctx context.Context, tx *gspanner.ReadWriteStmtBasedTransaction) error {
		var head *eventstore.SeqNr
		row, err := tx.ReadRow(ctx, t.head, gspanner.Key{hashPrefix, aid}, []string{"seq_nr"})
		switch {
		case gspanner.ErrCode(err) == codes.NotFound:
		case err != nil:
			return err
		default:
			var stored int64
			if err := row.Columns(&stored); err != nil {
				return err
			}
			h, err := storedSeqNr(stored)
			if err != nil {
				return err
			}
			head = &h
		}
		if head != nil && seqNr <= *head {
			return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: head}
		}
		expected := eventstore.SeqNr(1)
		if head != nil {
			expected = *head + 1
		}
		if seqNr != expected {
			return &eventstore.ContractViolationError{Rule: "W-8", SeqNr: &seqNr}
		}
		update := func(sql string, params map[string]any) error {
			statement := gspanner.NewStatement(sql)
			statement.Params = params
			_, err := tx.Update(ctx, statement)
			return err
		}
		conflict := func(err error) error {
			if isAlreadyExists(err) {
				return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: head}
			}
			return err
		}
		key := map[string]any{"hp": hashPrefix, "aid": aid, "seq": int64(seqNr), "nanos": occurredAtNs, "manifest": event.Manifest(), "payload": event.Payload()}
		if err := update("INSERT INTO `"+t.journal+"` (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, @aid, @seq, @nanos, @manifest, @payload)", key); err != nil {
			return conflict(err)
		}
		if head == nil {
			key["type_name"] = typeName
			if err := update("INSERT INTO `"+t.head+"` (hash_prefix, aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload) VALUES (@hp, @aid, @type_name, @seq, @nanos, @manifest, @payload)", key); err != nil {
				return conflict(err)
			}
		} else if err := update("UPDATE `"+t.head+"` SET seq_nr = @seq, event_occurred_at_ns = @nanos, event_manifest = @manifest, event_payload = @payload WHERE hash_prefix = @hp AND aid = @aid", key); err != nil {
			return err
		}
		if snapshot != nil {
			snapshotRow := map[string]any{"hp": hashPrefix, "aid": aid, "seq": int64(snapshot.SeqNr()), "manifest": snapshot.Manifest(), "payload": snapshot.Aggregate()}
			if err := update("INSERT OR UPDATE `"+t.snapshotCurrent+"` (hash_prefix, aid, seq_nr, manifest, payload) VALUES (@hp, @aid, @seq, @manifest, @payload)", snapshotRow); err != nil {
				return err
			}
			if b.settings.common.RetentionCount != nil {
				if err := update("INSERT INTO `"+t.snapshotHistory+"` (hash_prefix, aid, seq_nr, manifest, payload, expired_at) VALUES (@hp, @aid, @seq, @manifest, @payload, NULL)", snapshotRow); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if snapshot != nil && b.settings.common.RetentionCount != nil {
		if err := b.retain(ctx, aid, hashPrefix, snapshot.SeqNr()); err != nil {
			b.notifyRetentionFailure(ctx, aid, err)
		}
	}
	return nil
}

// notifyRetentionFailure reports a retention failure through the log and the application handler. Neither path
// can change the committed append: a panic in either is recovered.
func (b *storageBoundary) notifyRetentionFailure(ctx context.Context, aid string, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("spanner retention failed: aid=%q: %w", aid, cause)}
	runNotification(func() {
		slog.ErrorContext(ctx, "spanner snapshot retention failed", "aid", aid, "error", failure)
	})
	if handler := b.settings.common.RetentionFailureHandler; handler != nil {
		runNotification(func() { handler(ctx, failure) })
	}
}

func runNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}

// storedSeqNr converts a stored seq_nr and rejects a value outside 1 through 2^53-1.
func storedSeqNr(value int64) (eventstore.SeqNr, error) {
	seqNr := eventstore.SeqNr(value)
	if seqNr < 1 || seqNr > eventstore.MaxSeqNr {
		return 0, storageError(errors.New("a stored seq_nr is out of range"))
	}
	return seqNr, nil
}
