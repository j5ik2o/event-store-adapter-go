package memory

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// retainHistory runs after publication while the commit still owns the Store lock.
// A zero justWritten means an event-only append: it retries existing history only.
func (s *Store) retainHistory(aid string, current *record, justWritten eventstore.SeqNr) error {
	if s.settings.RetentionCount == nil {
		return nil
	}
	if err := s.hooks.Fail(testhook.Point{
		Phase: testhook.PhaseRetentionQuery, AggregateID: aid, SeqNr: int64(justWritten),
	}); err != nil {
		return err
	}
	pages, replaced, err := s.hooks.HistoryPages(aid, int64(justWritten))
	if err != nil {
		return err
	}
	selected := make(map[int64]struct{})
	if replaced {
		for _, page := range pages {
			for _, seqNr := range page {
				selected[seqNr] = struct{}{}
			}
		}
	} else {
		for _, snapshot := range current.history {
			selected[int64(snapshot.seqNr)] = struct{}{}
		}
	}
	if justWritten > 0 {
		selected[int64(justWritten)] = struct{}{}
	}
	seqNrs := make([]int64, 0, len(selected))
	for seqNr := range selected {
		seqNrs = append(seqNrs, seqNr)
	}
	slices.Sort(seqNrs)
	count := *s.settings.RetentionCount
	if len(seqNrs) <= count {
		return nil
	}
	obsolete := seqNrs[:len(seqNrs)-count]
	if err := s.hooks.Fail(testhook.Point{
		Phase: testhook.PhaseRetentionDelete, AggregateID: aid, SeqNrs: slices.Clone(obsolete),
	}); err != nil {
		return err
	}
	for _, seqNr := range obsolete {
		for i, snapshot := range current.history {
			if int64(snapshot.seqNr) == seqNr {
				current.history = slices.Delete(current.history, i, i+1)
				break
			}
		}
	}
	return nil
}

// notifyRetentionFailure is called only after unlocking. Both notification paths
// preserve the committed success even if application notification code panics.
func (s *Store) notifyRetentionFailure(ctx context.Context, aid string, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("memory retention failed: aid=%q: %w", aid, cause)}
	runRetentionNotification(func() {
		slog.ErrorContext(ctx, "memory snapshot retention failed", "aid", aid, "error", failure)
	})
	if handler := s.settings.RetentionFailureHandler; handler != nil {
		runRetentionNotification(func() { handler(ctx, failure) })
	}
}

func runRetentionNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}
