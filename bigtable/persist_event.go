package bigtable

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtableenvelope"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

func (b *storageBoundary) PersistEvent(ctx context.Context, event eventstore.EventEnvelope[[]byte]) error {
	return b.persist(ctx, event, nil)
}

func (b *storageBoundary) PersistEventAndSnapshot(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot eventstore.SnapshotEnvelope[[]byte]) error {
	return b.persist(ctx, event, &snapshot)
}

func (b *storageBoundary) tableOf(name string) *gbigtable.Table { return b.data.Open(name) }

// checkLimit rejects an envelope over the limit before anything is written (BT-16).
func (b *storageBoundary) checkLimit(length int, seqNr eventstore.SeqNr, snapshotSeqNr *eventstore.SeqNr) error {
	if length <= b.settings.envelopeLimit {
		return nil
	}
	return &eventstore.ContractViolationError{Rule: "BT-16", SeqNr: &seqNr, SnapshotSeqNr: snapshotSeqNr}
}

func (b *storageBoundary) persist(ctx context.Context, event eventstore.EventEnvelope[[]byte], snapshot *eventstore.SnapshotEnvelope[[]byte]) error {
	aid := event.AggregateID()
	seqNr := event.SeqNr()

	// Input checks and encoding finish before any request (BT-7).
	if err := b.checkLimit(bigtableenvelope.EventLen(aid, event.Manifest(), len(event.Payload())), seqNr, nil); err != nil {
		return err
	}
	eventBytes, err := bigtableenvelope.EncodeEvent(bigtableenvelope.Event{AID: aid, SeqNr: uint64(seqNr),
		OccurredAtNs: event.OccurredAt().UnixNano(), Manifest: event.Manifest(), Payload: event.Payload()})
	if err != nil {
		return storageError(err)
	}
	var snapshotBytes []byte
	if snapshot != nil {
		snapshotSeq := snapshot.SeqNr()
		if err := b.checkLimit(bigtableenvelope.SnapshotLen(snapshot.Manifest(), len(snapshot.Aggregate())), seqNr, &snapshotSeq); err != nil {
			return err
		}
		snapshotBytes, err = bigtableenvelope.EncodeSnapshot(bigtableenvelope.Snapshot{SeqNr: uint64(snapshotSeq),
			Manifest: snapshot.Manifest(), Payload: snapshot.Aggregate()})
		if err != nil {
			return storageError(err)
		}
	}
	rowPrefix, err := prefix(b.hooks, aid)
	if err != nil {
		return err
	}

	// Steps 1 and 2: read the head strongly and compare the input's seq_nr with h (W-3, W-7, W-8).
	head, err := b.readHead(ctx, rowPrefix, aid)
	if err != nil {
		return err
	}
	var current eventstore.SeqNr
	if head != nil {
		current = head.seqNr
	}
	if seqNr <= current {
		return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: &current}
	}
	if seqNr != current+1 {
		return &eventstore.ContractViolationError{Rule: "W-8", SeqNr: &seqNr}
	}

	keep := b.settings.common.RetentionCount
	// Step 3: write the head's last event h to its journal row. If this cannot be confirmed the head does not advance.
	if head != nil {
		point := testhook.Point{Phase: testhook.PhaseJournalWritePrevious, AggregateID: aid, SeqNr: int64(head.seqNr)}
		if err := b.hooks.Fail(point); err != nil {
			return storageError(err)
		}
		var history []byte
		if keep != nil && head.hasSnapshot && head.currentSnapshot != nil {
			history = head.currentSnapshot
		}
		if err := b.writeJournalRow(ctx, rowPrefix, head.seqNr, head.lastEvent, history, head.hasSnapshot); err != nil {
			return err
		}
	}

	// Step 4: the conditional write on the head row is the commit point (H-1).
	commit := testhook.Point{Phase: testhook.PhaseCommit, AggregateID: aid, SeqNr: int64(seqNr), Payload: bytes.Clone(event.Payload())}
	if err := b.hooks.Fail(commit); err != nil {
		return storageError(err)
	}
	if err := b.commitHead(ctx, aid, rowPrefix, seqNr, head, eventBytes, snapshotBytes); err != nil {
		return err
	}
	if err := b.hooks.CommitResponse(commit); err != nil {
		return storageError(fmt.Errorf("the response of the commit was lost, so its outcome is unknown: %w", err))
	}

	// Step 5: write the new last event out. A failure never changes the committed append; it is reported.
	var tailHistory []byte
	if keep != nil {
		tailHistory = snapshotBytes
	}
	tail := func() error {
		if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseJournalWriteTail, AggregateID: aid, SeqNr: int64(seqNr)}); err != nil {
			return storageError(err)
		}
		return b.writeJournalRow(ctx, rowPrefix, seqNr, eventBytes, tailHistory, snapshot != nil)
	}
	if err := tail(); err != nil {
		b.notifyJournalWriteFailure(ctx, aid, seqNr, err)
	}
	if snapshot != nil && keep != nil {
		if err := b.retain(ctx, aid, rowPrefix, seqNr); err != nil {
			b.notifyRetentionFailure(ctx, aid, err)
		}
	}
	return nil
}

// writeJournalRow creates the journal row only if event:envelope does not exist (BT-5), in one CheckAndMutateRow.
// An existing row is never overwritten: its event, flag and active history are verified instead. A retired history
// is not recreated, so a late writer cannot bring deleted history back.
func (b *storageBoundary) writeJournalRow(ctx context.Context, rowPrefix []byte, seqNr eventstore.SeqNr, event, history []byte, hasSnapshot bool) error {
	key := journalRowKey(rowPrefix, seqNr)
	creation := gbigtable.NewMutation()
	setCell(creation, familyEvent, eventEnvelope, event)
	setCell(creation, familyEvent, eventHasSnap, []byte(flagOf(hasSnapshot)))
	if history != nil {
		setCell(creation, familyHistory, historyState, []byte(stateActive))
		setCell(creation, familyHistory, historyEnvelope, history)
	}
	var existed bool
	condition := gbigtable.NewCondMutation(latestCell(familyEvent, eventEnvelope), nil, creation)
	table := b.tableOf(b.settings.journalTable)
	if err := table.Apply(ctx, key, condition, gbigtable.GetCondMutationResult(&existed)); err != nil {
		return storageError(err)
	}
	if !existed {
		return nil
	}
	row, err := table.ReadRow(ctx, key, gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
	if err != nil {
		return storageError(err)
	}
	invalid := func(what string) error { return storageError(fmt.Errorf("an existing journal row %s", what)) }
	if len(row) == 0 {
		return invalid("disappeared while it was verified")
	}
	if stored, _ := cell(row, familyEvent, eventEnvelope); !bytes.Equal(stored, event) {
		return invalid("holds a different event envelope")
	}
	if stored, _ := cell(row, familyEvent, eventHasSnap); string(stored) != flagOf(hasSnapshot) {
		return invalid("holds a different has_snapshot flag")
	}
	if history == nil {
		return nil
	}
	state, hasState := cell(row, familyHistory, historyState)
	switch {
	case !hasState, string(state) == stateRetired:
		return nil // written before a count was set, or retired: history is never recreated
	case string(state) == stateActive:
		if stored, _ := cell(row, familyHistory, historyEnvelope); !bytes.Equal(stored, history) {
			return invalid("holds a missing or different history envelope")
		}
		return nil
	default:
		return invalid("holds an unknown history state")
	}
}

// commitHead is the conditional write on the head row. A condition that does not hold means another writer advanced
// the head after step 1: the result is an optimistic lock error without a head_seq_nr (E-2). A lost response is a
// storage error that does not claim failure, and the write is never repeated.
func (b *storageBoundary) commitHead(ctx context.Context, aid string, rowPrefix []byte, seqNr eventstore.SeqNr, previous *headState, event, snapshot []byte) error {
	mutation := gbigtable.NewMutation()
	var condition *gbigtable.Mutation
	var matched bool
	if previous == nil {
		setCell(mutation, familyHead, headSeqNr, []byte(formatHeadSeqNr(seqNr)))
		setCell(mutation, familyHead, headLastEvent, event)
		setCell(mutation, familyHead, headHasSnapshot, []byte(flagOf(snapshot != nil)))
		if snapshot != nil {
			setCell(mutation, familySnapshot, snapshotCurrent, snapshot)
		}
		// A new aggregate is created on the side where head:seq_nr does not exist.
		condition = gbigtable.NewCondMutation(latestCell(familyHead, headSeqNr), nil, mutation)
	} else {
		replaceCell(mutation, familyHead, headSeqNr, []byte(formatHeadSeqNr(seqNr)))
		replaceCell(mutation, familyHead, headLastEvent, event)
		replaceCell(mutation, familyHead, headHasSnapshot, []byte(flagOf(snapshot != nil)))
		if snapshot != nil {
			replaceCell(mutation, familySnapshot, snapshotCurrent, snapshot)
		}
		// An update succeeds only on the side where head:seq_nr still holds the h that was read.
		condition = gbigtable.NewCondMutation(latestCellEquals(familyHead, headSeqNr, []byte(formatHeadSeqNr(previous.seqNr))), mutation, nil)
	}
	if err := b.tableOf(b.settings.headTable).Apply(ctx, string(rowPrefix), condition, gbigtable.GetCondMutationResult(&matched)); err != nil {
		return storageError(fmt.Errorf("the outcome of the commit is unknown: %w", err))
	}
	if (previous == nil && matched) || (previous != nil && !matched) {
		return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr}
	}
	return nil
}

// notifyJournalWriteFailure reports a failed write-out through the log and the application handler. Neither path can
// change the committed append: a panic in either is recovered.
func (b *storageBoundary) notifyJournalWriteFailure(ctx context.Context, aid string, seqNr eventstore.SeqNr, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("bigtable journal write-out failed: aid=%q seq_nr=%d: %w", aid, seqNr, cause)}
	runNotification(func() {
		slog.WarnContext(ctx, "bigtable journal write-out failed", "category", "journal-write-failure", "aid", aid, "seq_nr", uint64(seqNr), "error", failure)
	})
	if handler := b.settings.journalWriteFailure; handler != nil {
		runNotification(func() { handler(ctx, failure) })
	}
}

// notifyRetentionFailure reports a retention failure the same way.
func (b *storageBoundary) notifyRetentionFailure(ctx context.Context, aid string, cause error) {
	failure := &eventstore.StorageError{Cause: fmt.Errorf("bigtable retention failed: aid=%q: %w", aid, cause)}
	runNotification(func() {
		slog.ErrorContext(ctx, "bigtable snapshot retention failed", "aid", aid, "error", failure)
	})
	if handler := b.settings.common.RetentionFailureHandler; handler != nil {
		runNotification(func() { handler(ctx, failure) })
	}
}

func runNotification(notify func()) {
	defer func() { _ = recover() }()
	notify()
}
