package sqlite

import (
	"errors"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	driver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// appendError classifies a failed write. Only a primary key conflict on journal or on a new head is an
// optimistic lock failure; busy, locked, I/O and every other failure is a StorageError (SQ-7).
func appendError(err error, aid string, seqNr eventstore.SeqNr, head *eventstore.SeqNr) error {
	var sqliteErr *driver.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
		return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: head}
	}
	return &eventstore.StorageError{Cause: err}
}

func storageError(err error) error {
	return &eventstore.StorageError{Cause: err}
}
