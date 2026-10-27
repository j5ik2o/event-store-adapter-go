package conformance

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtableenvelope"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/keyhash"
)

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
// Time cases are dispatched to runTimeCase by RunBackend.
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
	case "fnv1a64":
		input, _ := c.Input.Raw["utf8"].(string)
		value = fnvValue(input, c.Expect)
	case "encodeBigtableEventEnvelope", "encodeBigtableSnapshotEnvelope", "decodeBigtableEnvelope":
		value, err = runBigtableEnvelope(c)
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

var fnvExpectedFormat = regexp.MustCompile(`^0x[0-9a-fA-F]{16}$`)

// fnvValue computes FNV-1a 64 of the UTF-8 input as an unsigned 64-bit integer and returns it in the "0x" and sixteen
// digits form of the data. An expected value that does not have that form never equals the result; one that has it
// is compared as an unsigned integer, not as text.
func fnvValue(input string, expect map[string]any) string {
	actual := keyhash.FNV1a64([]byte(input))
	text := fmt.Sprintf("0x%016x", actual)
	if want, ok := expect["value"].(string); ok && fnvExpectedFormat.MatchString(want) {
		if parsed, err := strconv.ParseUint(want[2:], 16, 64); err == nil && parsed == actual {
			return want
		}
	}
	return text
}

// runBigtableEnvelope runs the envelope table of Bigtable (BT-D6a) against the library's own functions. An encoding is
// returned as lower-case hexadecimal; the expected bytes are then decoded and must give back the input. A decoding
// returns the error the library reports (a storage error for a malformed envelope).
func runBigtableEnvelope(c ValueCase) (any, error) {
	in := c.Input.Raw
	manifest, _ := in["manifest"].(string)
	switch c.Operation {
	case "encodeBigtableEventEnvelope":
		aid, _ := in["aid"].(string)
		nanos, err := bigIntFromDecimalString(in["occurred_at_ns"])
		if err != nil || !nanos.IsInt64() || c.Input.SeqNr == nil || !c.Input.SeqNr.IsUint64() {
			return nil, fmt.Errorf("the envelope input does not fit the envelope fields")
		}
		payload, _ := hex.DecodeString(in["payload_hex"].(string))
		event := bigtableenvelope.Event{AID: aid, SeqNr: c.Input.SeqNr.Uint64(), OccurredAtNs: nanos.Int64(), Manifest: manifest, Payload: payload}
		encoded, err := bigtableenvelope.EncodeEvent(event)
		if err != nil {
			return nil, err
		}
		if want, ok := c.Expect["value"].(string); ok {
			raw, hexErr := hex.DecodeString(want)
			if hexErr != nil {
				return nil, hexErr
			}
			back, err := bigtableenvelope.DecodeEvent(raw)
			if err != nil || back.AID != event.AID || back.SeqNr != event.SeqNr || back.OccurredAtNs != event.OccurredAtNs ||
				back.Manifest != event.Manifest || !bytes.Equal(back.Payload, event.Payload) {
				return nil, fmt.Errorf("the expected bytes do not decode to the input: %v", err)
			}
		}
		return hex.EncodeToString(encoded), nil
	case "encodeBigtableSnapshotEnvelope":
		if c.Input.SeqNr == nil || !c.Input.SeqNr.IsUint64() {
			return nil, fmt.Errorf("the envelope input does not fit the envelope fields")
		}
		payload, _ := hex.DecodeString(in["payload_hex"].(string))
		snapshot := bigtableenvelope.Snapshot{SeqNr: c.Input.SeqNr.Uint64(), Manifest: manifest, Payload: payload}
		encoded, err := bigtableenvelope.EncodeSnapshot(snapshot)
		if err != nil {
			return nil, err
		}
		if want, ok := c.Expect["value"].(string); ok {
			raw, hexErr := hex.DecodeString(want)
			if hexErr != nil {
				return nil, hexErr
			}
			back, err := bigtableenvelope.DecodeSnapshot(raw)
			if err != nil || back.SeqNr != snapshot.SeqNr || back.Manifest != snapshot.Manifest || !bytes.Equal(back.Payload, snapshot.Payload) {
				return nil, fmt.Errorf("the expected bytes do not decode to the input: %v", err)
			}
		}
		return hex.EncodeToString(encoded), nil
	default:
		raw, _ := hex.DecodeString(in["bytes_hex"].(string))
		var err error
		if in["kind"] == "event" {
			_, err = bigtableenvelope.DecodeEvent(raw)
		} else {
			_, err = bigtableenvelope.DecodeSnapshot(raw)
		}
		return nil, err
	}
}
