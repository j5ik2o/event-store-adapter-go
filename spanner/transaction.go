package spanner

import (
	"context"
	"fmt"
	"time"

	gspanner "cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
)

const (
	firstRetryWait = 20 * time.Millisecond
	maxRetryWait   = time.Second
)

// runReadWrite runs body in a SERIALIZABLE read-write transaction (SPN-1). An aborted attempt is re-run with a fresh
// transaction up to retryLimit times; the first attempt is not counted (SPN-7). body must build everything it writes
// from values fixed before the call, so that a re-run uses the same envelope. When the commit is refused or its
// response is lost the outcome may be unknown, so it is a storage error and nothing compensates (SPN-8).
func (b *storageBoundary) runReadWrite(ctx context.Context, point *testhook.Point, retryLimit int, body func(context.Context, *gspanner.ReadWriteStmtBasedTransaction) error) error {
	wait := firstRetryWait
	var last error
	for attempt := 0; attempt <= retryLimit; attempt++ {
		if attempt > 0 {
			if err := b.waitForRetry(ctx, wait); err != nil {
				return storageError(err)
			}
			wait = min(wait*2, maxRetryWait)
		}
		err := b.attempt(ctx, point, body)
		if err == nil {
			return nil
		}
		if !isAborted(err) {
			return classified(err)
		}
		last = err
	}
	return storageError(fmt.Errorf("the transaction was aborted more than %d times: %w", retryLimit, last))
}

// attempt runs one transaction. It returns an aborted error as is so the caller can re-run it.
func (b *storageBoundary) attempt(ctx context.Context, point *testhook.Point, body func(context.Context, *gspanner.ReadWriteStmtBasedTransaction) error) (err error) {
	tx, err := gspanner.NewReadWriteStmtBasedTransactionWithOptions(ctx, b.client, gspanner.TransactionOptions{IsolationLevel: sppb.TransactionOptions_SERIALIZABLE})
	if err != nil {
		return err
	}
	finished := false
	defer func() {
		if !finished {
			// An aborted or failed attempt must release its locks, even when ctx is cancelled.
			rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			tx.Rollback(rollbackCtx)
		}
	}()
	if err := body(ctx, tx); err != nil {
		return err
	}
	if point != nil {
		if err := b.hooks.Fail(*point); err != nil {
			return err
		}
	}
	finished = true
	if _, err := tx.Commit(ctx); err != nil {
		if isAborted(err) {
			return err
		}
		return storageError(fmt.Errorf("the outcome of the commit is unknown: %w", err))
	}
	// The commit succeeded on the server. A lost response must not roll anything back.
	if point == nil {
		return nil
	}
	if err := b.hooks.CommitResponse(*point); err != nil {
		return storageError(fmt.Errorf("the response of the commit was lost, so its outcome is unknown: %w", err))
	}
	return nil
}

func (b *storageBoundary) waitForRetry(ctx context.Context, delay time.Duration) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if b.hooks == nil {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		return context.Cause(ctx)
	}
	completed := make(chan struct{})
	go func() {
		b.hooks.Sleep(delay)
		close(completed)
	}()
	select {
	case <-ctx.Done():
	case <-completed:
	}
	return context.Cause(ctx)
}
