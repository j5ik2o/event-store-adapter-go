package dynamodb

import (
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

func classifyWriteError(cause error, aid string, seqNr eventstore.SeqNr) error {
	storage := &eventstore.StorageError{Cause: cause}
	var canceled *types.TransactionCanceledException
	if !errors.As(cause, &canceled) {
		return storage
	}
	reasons := canceled.CancellationReasons
	for _, reason := range reasons {
		if aws.ToString(reason.Code) == "TransactionConflict" {
			return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, Cause: cause}
		}
	}
	// Keep None entries and their action positions; head is the second action.
	if len(reasons) > headAction && aws.ToString(reasons[headAction].Code) == "ConditionalCheckFailed" {
		if seqNr == 1 {
			return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, Cause: cause}
		}
		var headSeqNr eventstore.SeqNr
		if item := reasons[headAction].Item; len(item) != 0 {
			number, ok := item["seq_nr"].(*types.AttributeValueMemberN)
			if !ok {
				return storage
			}
			n, err := strconv.ParseInt(number.Value, 10, 64)
			headSeqNr = eventstore.SeqNr(n)
			if err != nil || headSeqNr.ValidateAsEventSeqNr() != nil {
				return storage
			}
		}
		if seqNr <= headSeqNr {
			return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, HeadSeqNr: &headSeqNr, Cause: cause}
		}
		if seqNr > headSeqNr+1 {
			return &eventstore.ContractViolationError{Rule: "W-8", SeqNr: &seqNr, Cause: cause}
		}
		return storage
	}
	if len(reasons) > journalAction && aws.ToString(reasons[journalAction].Code) == "ConditionalCheckFailed" {
		return &eventstore.OptimisticLockError{AggregateID: aid, SeqNr: seqNr, Cause: cause}
	}
	return storage
}
