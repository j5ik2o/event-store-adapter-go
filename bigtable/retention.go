package bigtable

import (
	"context"
	"fmt"
	"slices"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// retain runs after an append that carried a snapshot has committed (BT-11). It fixes the head position h, reads the
// active history of journal rows 1..h, and retires every row older than the newest keep entries. The head's snapshot
// that has no journal row yet counts as an entry (h is added). Retired rows lose only history:envelope; the event
// cells stay, so a late writer finds event:envelope and cannot recreate the history (BT-12).
func (b *storageBoundary) retain(ctx context.Context, aid string, rowPrefix []byte, h eventstore.SeqNr) error {
	keep := *b.settings.common.RetentionCount
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseRetentionQuery, AggregateID: aid, SeqNr: int64(h)}); err != nil {
		return storageError(err)
	}
	visible, err := b.activeHistory(ctx, rowPrefix, h)
	if err != nil {
		return err
	}
	pages, replaced, err := b.hooks.HistoryPages(aid, int64(h))
	if err != nil {
		return storageError(err)
	}
	if replaced {
		visible = visible[:0]
		for _, page := range pages {
			for _, seq := range page {
				visible = append(visible, eventstore.SeqNr(seq))
			}
		}
	}
	visible = append(visible, h)
	slices.Sort(visible)
	visible = slices.Compact(visible)
	if len(visible) <= keep {
		return nil
	}
	obsolete := visible[:len(visible)-keep]
	numbers := make([]int64, len(obsolete))
	for i, seq := range obsolete {
		numbers[i] = int64(seq)
	}
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseRetentionDelete, AggregateID: aid, SeqNrs: numbers}); err != nil {
		return storageError(err)
	}
	for _, seq := range obsolete {
		if err := b.retire(ctx, rowPrefix, seq); err != nil {
			return err
		}
	}
	return nil
}

// activeHistory lists the journal rows 1..h whose history is active, in sequence order. Only the state column is
// read, so no history payload is transferred. The configuration row (key 0x00) is outside the range.
func (b *storageBoundary) activeHistory(ctx context.Context, rowPrefix []byte, h eventstore.SeqNr) ([]eventstore.SeqNr, error) {
	var active []eventstore.SeqNr
	var failure error
	rows := gbigtable.NewRange(journalRowKey(rowPrefix, 1), journalRowKey(rowPrefix, h+1))
	err := b.tableOf(b.settings.journalTable).ReadRows(ctx, rows, func(row gbigtable.Row) bool {
		seq, ok := journalSeqNr(rowPrefix, row.Key())
		if !ok {
			failure = invalidData("a journal row key does not hold a seq_nr")
			return false
		}
		state, _ := cell(row, familyHistory, historyState)
		switch string(state) {
		case stateActive:
			active = append(active, seq)
		case stateRetired:
		default:
			failure = invalidData(fmt.Sprintf("a journal row holds an unknown history state: seq_nr %d", seq))
			return false
		}
		return true
	}, gbigtable.RowFilter(latestCell(familyHistory, historyState)))
	if failure != nil {
		return nil, failure
	}
	if err != nil {
		return nil, storageError(err)
	}
	return active, nil
}

// retire deletes the history envelope and marks the row retired in one conditional write that succeeds only while the
// state is still active. A row that is not active (already retired, or absent) is left alone.
func (b *storageBoundary) retire(ctx context.Context, rowPrefix []byte, seq eventstore.SeqNr) error {
	retired := gbigtable.NewMutation()
	retired.DeleteCellsInColumn(familyHistory, historyEnvelope)
	replaceCell(retired, familyHistory, historyState, []byte(stateRetired))
	condition := gbigtable.NewCondMutation(latestCellEquals(familyHistory, historyState, []byte(stateActive)), retired, nil)
	var matched bool
	if err := b.tableOf(b.settings.journalTable).Apply(ctx, journalRowKey(rowPrefix, seq), condition, gbigtable.GetCondMutationResult(&matched)); err != nil {
		return storageError(err)
	}
	return nil
}
