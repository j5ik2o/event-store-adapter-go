package spanner

import (
	"errors"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"google.golang.org/grpc/codes"

	gspanner "cloud.google.com/go/spanner"
)

func storageError(err error) error {
	return &eventstore.StorageError{Cause: err}
}

// isAborted tells whether err is a transaction abort that the storage re-runs (SPN-7): the server's ABORTED or an
// injected one.
func isAborted(err error) bool {
	var injected *testhook.InjectedError
	if errors.As(err, &injected) {
		return injected.Aborted
	}
	return gspanner.ErrCode(err) == codes.Aborted
}

func isAlreadyExists(err error) bool {
	return gspanner.ErrCode(err) == codes.AlreadyExists
}

// classified keeps an error that already has one of the five classes of E-1; everything else is a storage error.
func classified(err error) error {
	if _, ok := eventstore.KindOf(err); ok {
		return err
	}
	return storageError(err)
}
