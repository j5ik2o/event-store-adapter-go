package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

const reasonTimeNoBackend = "時刻の値表は書込み・読戻しを要求するが、保存先に未接続（設計 5.1）"

// valueAggregateID deliberately has a caller-defined representation in addition to
// the two core ID methods. buildAid must use the latter, not userString.
type valueAggregateID struct {
	parts      AggregateIDArg
	userString string
}

func (id valueAggregateID) TypeName() string { return id.parts.TypeName }
func (id valueAggregateID) Value() string    { return id.parts.Value }
func (id valueAggregateID) String() string   { return id.userString }
func (id valueAggregateID) AsString() string { return id.userString }

// runValueCase invokes core validation rather than duplicating its rules.
// Time cases remain unverified until actual storage and readback are connected.
func runValueCase(c ValueCase) CaseResult {
	res := CaseResult{ID: c.ID, Rules: nonNil(c.Rules), Expected: c.Expect}
	var value any
	var err error
	switch c.Operation {
	case "buildAid":
		var parts AggregateIDArg
		parts, err = parseAID(c.Input.Raw["aggregate_id"])
		if err == nil {
			userString, _ := c.Input.Raw["user_string"].(string)
			value, err = eventstore.AidString(valueAggregateID{parts: parts, userString: userString})
		}
	case "validateSeqNr":
		if c.Input.SeqNr == nil {
			err = fmt.Errorf("input.seq_nr is missing")
			break
		}
		if !c.Input.SeqNr.IsInt64() {
			res.Status = StatusUnrepresentable
			res.Reason = fmt.Sprintf("seq_nr %s は Go の int64 で表現できない", c.Input.SeqNr)
			res.Actual = map[string]any{"seq_nr": json.Number(c.Input.SeqNr.String())}
			return res
		}
		n := eventstore.SeqNr(c.Input.SeqNr.Int64())
		value = json.Number(strconv.FormatInt(int64(n), 10))
		switch c.Input.Raw["context"] {
		case "value":
			err = n.Validate()
		case "event":
			var event eventstore.EventEnvelope[map[string]any]
			event, err = eventstore.NewEventEnvelope(
				valueAggregateID{parts: AggregateIDArg{TypeName: "ConformanceSeqNr", Value: c.ID}},
				n, time.Unix(0, 123000000).UTC(), map[string]any{},
			)
			if err == nil {
				value = json.Number(strconv.FormatInt(int64(event.SeqNr()), 10))
			}
		default:
			err = fmt.Errorf("unsupported seq_nr context %v", c.Input.Raw["context"])
		}
	case "validateOccurredAt":
		res.Status, res.Reason = StatusUnverified, reasonTimeNoBackend
		return res
	default:
		err = fmt.Errorf("unsupported value operation %q", c.Operation)
	}
	return compareValueResult(res, c.Expect, value, err)
}

// coreOperationError maps the actual core Kind to the existing comparison boundary.
// Unclassified errors retain their type, even when their message resembles a category.
func coreOperationError(err error) error {
	kind, ok := eventstore.KindOf(err)
	if !ok {
		return err
	}
	var category string
	switch kind {
	case eventstore.KindOptimisticLock:
		category = "optimistic-lock"
	case eventstore.KindContractViolation:
		category = "contract-violation"
	case eventstore.KindSerialization:
		category = "serialization"
	case eventstore.KindConfiguration:
		category = "configuration"
	case eventstore.KindStorage:
		category = "storage"
	}
	return &OperationError{Category: category, Message: err.Error()}
}

func compareValueResult(res CaseResult, expect map[string]any, value any, err error) CaseResult {
	err = coreOperationError(err)
	res.Actual = map[string]any{"value": value}
	if err != nil {
		var classified *OperationError
		if errors.As(err, &classified) {
			res.Actual = map[string]any{"error": map[string]any{"category": classified.Category, "message": classified.Message}}
		} else {
			res.Actual = map[string]any{"error": errorText(err)}
		}
	}
	var mismatch string
	if expectedError, present := expect["error"]; present {
		exp, parseErr := parseErrorExpect(expectedError)
		if parseErr != nil {
			mismatch = "エラー期待値が不正: " + parseErr.Error()
		} else {
			mismatch = matchError(exp, err)
		}
	} else if err != nil {
		mismatch = "値を期待したがエラーになった: " + errorText(err)
	} else if expectedValue, present := expect["value"]; !present || !jsonEqual(expectedValue, value) {
		mismatch = fmt.Sprintf("値が合わない: want %v, got %v", expect["value"], value)
	}
	if mismatch != "" {
		res.Status, res.Reason = StatusFailure, mismatch
	} else {
		res.Status, res.Reason = StatusSuccess, ""
	}
	return res
}
