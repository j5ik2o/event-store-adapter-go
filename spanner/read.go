package spanner

import (
	"context"
	"errors"
	"strings"
	"time"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// readHead returns the head seq_nr of the aggregate, or nil when it has none.
func (b *storageBoundary) readHead(ctx context.Context, tx *gspanner.ReadOnlyTransaction, hashPrefix []byte, aid string) (*eventstore.SeqNr, error) {
	row, err := tx.ReadRow(ctx, b.settings.tables.head, gspanner.Key{hashPrefix, aid}, []string{"seq_nr"})
	if gspanner.ErrCode(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, storageError(err)
	}
	var stored int64
	if err := row.Columns(&stored); err != nil {
		return nil, storageError(err)
	}
	seqNr, err := storedSeqNr(stored)
	if err != nil {
		return nil, err
	}
	return &seqNr, nil
}

// GetLatestSnapshotByID reads the head and the current snapshot in one strongly consistent read-only transaction
// (SPN-9, SPN-10), so both come from the same point in time.
func (b *storageBoundary) GetLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, err
	}
	hashPrefix := keyhash.Prefix(aid)
	tx := b.client.ReadOnlyTransaction()
	defer tx.Close()
	head, err := b.readHead(ctx, tx, hashPrefix, aid)
	if err != nil {
		return nil, err
	}
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadSnapshot, AggregateID: aid}); err != nil {
		return nil, storageError(err)
	}
	if head == nil {
		return nil, nil
	}
	result := &eventstore.SnapshotRead[[]byte]{HeadSeqNr: *head}
	row, err := tx.ReadRow(ctx, b.settings.tables.snapshotCurrent, gspanner.Key{hashPrefix, aid}, []string{"seq_nr", "manifest", "payload"})
	switch {
	case gspanner.ErrCode(err) == codes.NotFound:
	case err != nil:
		return nil, storageError(err)
	default:
		var stored int64
		var manifest string
		var payload []byte
		if err := row.Columns(&stored, &manifest, &payload); err != nil {
			return nil, storageError(err)
		}
		seqNr, err := storedSeqNr(stored)
		if err != nil {
			return nil, err
		}
		snapshot, err := eventstore.NewSnapshotEnvelope(payload, seqNr, eventstore.WithManifest(manifest))
		if err != nil {
			return nil, err
		}
		result.Snapshot = &snapshot
	}
	return result, nil
}

// GetEventsByIDSinceSeqNr reads the head h and then the journal range [seqNr, h] in one strongly consistent
// read-only transaction. A gap or a missing tail is a storage error: a shortened result is never a success.
func (b *storageBoundary) GetEventsByIDSinceSeqNr(ctx context.Context, id eventstore.AggregateID, seqNr eventstore.SeqNr) ([]eventstore.EventEnvelope[[]byte], error) {
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
	hashPrefix := keyhash.Prefix(aid)
	tx := b.client.ReadOnlyTransaction()
	defer tx.Close()
	head, err := b.readHead(ctx, tx, hashPrefix, aid)
	if err != nil {
		return nil, err
	}
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadEvents, AggregateID: aid, SeqNr: int64(seqNr)}); err != nil {
		return nil, storageError(err)
	}
	start := max(seqNr, 1)
	if head == nil || start > *head {
		return nil, nil
	}
	keys := gspanner.KeyRange{Start: gspanner.Key{hashPrefix, aid, int64(start)}, End: gspanner.Key{hashPrefix, aid, int64(*head)}, Kind: gspanner.ClosedClosed}
	rows := tx.Read(ctx, b.settings.tables.journal, keys, []string{"seq_nr", "occurred_at_ns", "manifest", "payload"})
	defer rows.Stop()
	var events []eventstore.EventEnvelope[[]byte]
	next := start
	for {
		row, err := rows.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, storageError(err)
		}
		var stored, occurredAtNs int64
		var manifest string
		var payload []byte
		if err := row.Columns(&stored, &occurredAtNs, &manifest, &payload); err != nil {
			return nil, storageError(err)
		}
		seq, err := storedSeqNr(stored)
		if err != nil {
			return nil, err
		}
		if seq != next {
			return nil, storageError(errors.New("the journal has a gap"))
		}
		event, err := eventstore.NewEventEnvelope(fixedID, seq, time.Unix(0, occurredAtNs).UTC(), payload, eventstore.WithManifest(manifest))
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		next++
	}
	if next != *head+1 {
		return nil, storageError(errors.New("the journal lacks events up to the head"))
	}
	return events, nil
}
