package spanner

import (
	"context"
	"slices"

	gspanner "cloud.google.com/go/spanner"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"google.golang.org/api/iterator"
)

// retentionBatchSize bounds the rows one retention transaction deletes or marks, so that no request exceeds the
// service's mutation limits (SPN-10).
const retentionBatchSize = 1000

// retain runs after the append is committed, in separate SERIALIZABLE transactions (SPN-11). The newest
// RetentionCount unmarked history rows stay; older rows are deleted (delete mode) or marked once with expired_at
// (TTL mode, SPN-12). Marked rows are not counted. When more than one part is needed, candidates are selected again
// for every part.
func (b *storageBoundary) retain(ctx context.Context, aid string, hashPrefix []byte, justWritten eventstore.SeqNr) error {
	keep := *b.settings.common.RetentionCount
	phase := testhook.PhaseRetentionDelete
	if b.settings.common.RetentionMode == storeoptions.RetentionTTL {
		phase = testhook.PhaseRetentionMark
	}
	t := b.settings.tables
	for {
		var more bool
		point := testhook.Point{Phase: testhook.PhaseRetentionQuery, AggregateID: aid, SeqNr: int64(justWritten)}
		err := b.runReadWrite(ctx, nil, b.settings.retryLimit, func(ctx context.Context, tx *gspanner.ReadWriteStmtBasedTransaction) error {
			more = false
			if err := b.hooks.Fail(point); err != nil {
				return err
			}
			statement := gspanner.NewStatement("SELECT seq_nr FROM `" + t.snapshotHistory + "` WHERE hash_prefix = @hp AND aid = @aid AND expired_at IS NULL ORDER BY seq_nr DESC")
			statement.Params = map[string]any{"hp": hashPrefix, "aid": aid}
			rows := tx.Query(ctx, statement)
			defer rows.Stop()
			var visible []int64
			for {
				row, err := rows.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var stored int64
				if err := row.Columns(&stored); err != nil {
					return err
				}
				seqNr, err := storedSeqNr(stored)
				if err != nil {
					return err
				}
				visible = append(visible, int64(seqNr))
			}
			pages, replaced, err := b.hooks.HistoryPages(aid, int64(justWritten))
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
			if len(visible) <= keep {
				return nil
			}
			obsolete := visible[:len(visible)-keep]
			if len(obsolete) > retentionBatchSize {
				obsolete = obsolete[:retentionBatchSize]
				more = true
			}
			if err := b.hooks.Fail(testhook.Point{Phase: phase, AggregateID: aid, SeqNrs: slices.Clone(obsolete)}); err != nil {
				return err
			}
			sql := "DELETE FROM `" + t.snapshotHistory + "` WHERE hash_prefix = @hp AND aid = @aid AND seq_nr IN UNNEST(@seqs)"
			if phase == testhook.PhaseRetentionMark {
				sql = "UPDATE `" + t.snapshotHistory + "` SET expired_at = CURRENT_TIMESTAMP() WHERE hash_prefix = @hp AND aid = @aid AND seq_nr IN UNNEST(@seqs) AND expired_at IS NULL"
			}
			update := gspanner.NewStatement(sql)
			update.Params = map[string]any{"hp": hashPrefix, "aid": aid, "seqs": obsolete}
			changed, err := tx.Update(ctx, update)
			if err != nil {
				return err
			}
			// Select again only when this part was full and removed something, so the loop always ends.
			more = more && changed > 0
			return nil
		})
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}
