package bigtable

import (
	"context"
	"errors"
	"strings"
	"time"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtableenvelope"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

// headState is the verified content of a head row.
type headState struct {
	seqNr           eventstore.SeqNr
	lastEvent       []byte
	last            bigtableenvelope.Event
	hasSnapshot     bool
	currentSnapshot []byte // nil when the aggregate has no snapshot
	current         bigtableenvelope.Snapshot
}

func invalidData(what string) error {
	return storageError(errors.New(what))
}

// readHead reads the head row once, strongly, and validates it. A row with a missing or inconsistent required cell is
// never treated as a new aggregate. A missing row returns nil.
func (b *storageBoundary) readHead(ctx context.Context, rowPrefix []byte, aid string) (*headState, error) {
	row, err := b.tableOf(b.settings.headTable).ReadRow(ctx, string(rowPrefix), gbigtable.RowFilter(gbigtable.LatestNFilter(1)))
	if err != nil {
		return nil, storageError(err)
	}
	if len(row) == 0 {
		return nil, nil
	}
	seqValue, _ := cell(row, familyHead, headSeqNr)
	seqNr, ok := parseHeadSeqNr(seqValue)
	if !ok {
		return nil, invalidData("the head row has a missing or invalid seq_nr")
	}
	lastEvent, ok := cell(row, familyHead, headLastEvent)
	if !ok {
		return nil, invalidData("the head row has no last event")
	}
	last, err := bigtableenvelope.DecodeEvent(lastEvent)
	if err != nil {
		return nil, err
	}
	if last.AID != aid || eventstore.SeqNr(last.SeqNr) != seqNr {
		return nil, invalidData("the last event of the head row does not match the head")
	}
	flag, _ := cell(row, familyHead, headHasSnapshot)
	var hasSnapshot bool
	switch string(flag) {
	case flagSet:
		hasSnapshot = true
	case flagUnset:
	default:
		return nil, invalidData("a stored has_snapshot flag is missing or invalid")
	}
	state := &headState{seqNr: seqNr, lastEvent: lastEvent, last: last, hasSnapshot: hasSnapshot}
	if current, ok := cell(row, familySnapshot, snapshotCurrent); ok {
		decoded, err := bigtableenvelope.DecodeSnapshot(current)
		if err != nil {
			return nil, err
		}
		// With the head's snapshot the current snapshot equals the head; without it, it is older (profile chapter 4).
		snapSeq := eventstore.SeqNr(decoded.SeqNr)
		if (hasSnapshot && snapSeq != seqNr) || (!hasSnapshot && snapSeq >= seqNr) {
			return nil, invalidData("the current snapshot does not match the head seq_nr")
		}
		state.currentSnapshot, state.current = current, decoded
	} else if hasSnapshot {
		return nil, invalidData("the head row says it has a snapshot but holds none")
	}
	return state, nil
}

// GetLatestSnapshotByID reads the head row once: head:seq_nr and snapshot:current come from the same row (BT-8).
func (b *storageBoundary) GetLatestSnapshotByID(ctx context.Context, id eventstore.AggregateID) (*eventstore.SnapshotRead[[]byte], error) {
	aid, err := eventstore.AidString(id)
	if err != nil {
		return nil, err
	}
	rowPrefix, err := prefix(b.hooks, aid)
	if err != nil {
		return nil, err
	}
	head, err := b.readHead(ctx, rowPrefix, aid)
	if err != nil {
		return nil, err
	}
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadSnapshot, AggregateID: aid}); err != nil {
		return nil, storageError(err)
	}
	if head == nil {
		return nil, nil
	}
	result := &eventstore.SnapshotRead[[]byte]{HeadSeqNr: head.seqNr}
	if head.currentSnapshot != nil {
		snapshot, err := eventstore.NewSnapshotEnvelope(head.current.Payload, eventstore.SeqNr(head.current.SeqNr), eventstore.WithManifest(head.current.Manifest))
		if err != nil {
			return nil, err
		}
		result.Snapshot = &snapshot
	}
	return result, nil
}

// GetEventsByIDSinceSeqNr reads the head first and keeps its h and last event for the whole call (BT-9). With
// start = max(1, seqNr): no head or start > h returns nothing; start == h returns only the head's event; otherwise the
// journal range [start, h) is read and the head's event is appended. A gap in the range is a storage error.
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
	rowPrefix, err := prefix(b.hooks, aid)
	if err != nil {
		return nil, err
	}
	head, err := b.readHead(ctx, rowPrefix, aid)
	if err != nil {
		return nil, err
	}
	if err := b.hooks.Fail(testhook.Point{Phase: testhook.PhaseReadEvents, AggregateID: aid, SeqNr: int64(seqNr)}); err != nil {
		return nil, storageError(err)
	}
	start := max(seqNr, 1)
	if head == nil || start > head.seqNr {
		return nil, nil
	}
	var stored []bigtableenvelope.Event
	if start < head.seqNr {
		if stored, err = b.readJournal(ctx, rowPrefix, aid, start, head.seqNr); err != nil {
			return nil, err
		}
	}
	stored = append(stored, head.last)
	events := make([]eventstore.EventEnvelope[[]byte], 0, len(stored))
	for _, fields := range stored {
		event, err := eventstore.NewEventEnvelope(fixedID, eventstore.SeqNr(fields.SeqNr), time.Unix(0, fields.OccurredAtNs).UTC(),
			fields.Payload, eventstore.WithManifest(fields.Manifest))
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// readJournal reads the journal rows [start, h) of one aggregate. The range is built from the binary keys, never from
// decimal text. Rows must be exactly start..h-1 in order; a short or shifted result is a storage error (BT-10).
func (b *storageBoundary) readJournal(ctx context.Context, rowPrefix []byte, aid string, start, h eventstore.SeqNr) ([]bigtableenvelope.Event, error) {
	gap := func() error { return invalidData("the journal has a gap in the requested range") }
	var events []bigtableenvelope.Event
	var failure error
	next := start
	rows := gbigtable.NewRange(journalRowKey(rowPrefix, start), journalRowKey(rowPrefix, h))
	filter := latestCell(familyEvent, eventEnvelope)
	err := b.tableOf(b.settings.journalTable).ReadRows(ctx, rows, func(row gbigtable.Row) bool {
		seq, ok := journalSeqNr(rowPrefix, row.Key())
		if !ok || seq != next {
			failure = gap()
			return false
		}
		envelope, ok := cell(row, familyEvent, eventEnvelope)
		if !ok {
			failure = invalidData("a journal row has no event envelope")
			return false
		}
		fields, err := bigtableenvelope.DecodeEvent(envelope)
		if err != nil {
			failure = err
			return false
		}
		if fields.AID != aid || eventstore.SeqNr(fields.SeqNr) != seq {
			failure = invalidData("a journal row holds an envelope of another aid or seq_nr")
			return false
		}
		events = append(events, fields)
		next++
		return true
	}, gbigtable.RowFilter(filter))
	if failure != nil {
		return nil, failure
	}
	if err != nil {
		return nil, storageError(err)
	}
	if next != h {
		return nil, gap()
	}
	return events, nil
}
