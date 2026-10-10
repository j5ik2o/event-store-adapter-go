package dynamodb

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
)

var decimalNumber = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func readString(item map[string]types.AttributeValue, name string) (string, error) {
	value, ok := item[name].(*types.AttributeValueMemberS)
	if !ok || value == nil {
		return "", fmt.Errorf("stored %s is not S", name)
	}
	return value.Value, nil
}

func readInteger(item map[string]types.AttributeValue, name string) (int64, error) {
	value, ok := item[name].(*types.AttributeValueMemberN)
	if !ok || value == nil || !decimalNumber.MatchString(value.Value) {
		return 0, fmt.Errorf("stored %s is not a decimal N", name)
	}
	number, ok := new(big.Rat).SetString(value.Value)
	if !ok || !number.IsInt() || !number.Num().IsInt64() {
		return 0, fmt.Errorf("stored %s is not an int64 integer", name)
	}
	return number.Num().Int64(), nil
}

func readPayload(item map[string]types.AttributeValue) ([]byte, error) {
	value, ok := item["payload"].(*types.AttributeValueMemberB)
	if !ok || value == nil {
		return nil, fmt.Errorf("stored payload is not B")
	}
	return value.Value, nil
}

func readAid(item map[string]types.AttributeValue, aid string) error {
	actual, err := readString(item, "aid")
	if err != nil {
		return err
	}
	if actual != aid {
		return fmt.Errorf("stored aid differs from the requested aggregate")
	}
	return nil
}

// readEventMetadata is shared by journal items and the head's one event map.
func readEventMetadata(id eventstore.AggregateID, item map[string]types.AttributeValue) (eventstore.EventEnvelope[[]byte], error) {
	seqNr, err := readInteger(item, "seq_nr")
	if err != nil {
		return eventstore.EventEnvelope[[]byte]{}, err
	}
	occurredAt, err := readInteger(item, "occurred_at")
	if err != nil {
		return eventstore.EventEnvelope[[]byte]{}, err
	}
	manifest, err := readString(item, "manifest")
	if err != nil {
		return eventstore.EventEnvelope[[]byte]{}, err
	}
	payload, err := readPayload(item)
	if err != nil {
		return eventstore.EventEnvelope[[]byte]{}, err
	}
	return eventstore.NewEventEnvelope(id, eventstore.SeqNr(seqNr), time.Unix(0, occurredAt).UTC(), payload, eventstore.WithManifest(manifest))
}

func readHead(id eventstore.AggregateID, aid string, item map[string]types.AttributeValue) (eventstore.SeqNr, error) {
	if err := readAid(item, aid); err != nil {
		return 0, err
	}
	typeName, err := readString(item, "type_name")
	if err != nil {
		return 0, err
	}
	expectedType, _, _ := strings.Cut(aid, "-")
	if typeName != expectedType {
		return 0, fmt.Errorf("stored head type_name differs from aid")
	}
	seqNr, err := readInteger(item, "seq_nr")
	if err != nil {
		return 0, err
	}
	events, ok := item["events"].(*types.AttributeValueMemberL)
	if !ok || events == nil || len(events.Value) != 1 {
		return 0, fmt.Errorf("stored head events must be L with one event")
	}
	metadata, ok := events.Value[0].(*types.AttributeValueMemberM)
	if !ok || metadata == nil {
		return 0, fmt.Errorf("stored head event is not M")
	}
	event, err := readEventMetadata(id, metadata.Value)
	if err != nil {
		return 0, err
	}
	if event.SeqNr() != eventstore.SeqNr(seqNr) {
		return 0, fmt.Errorf("stored head and its event have different sequence numbers")
	}
	return event.SeqNr(), nil
}

func readCurrentSnapshot(aid string, item map[string]types.AttributeValue) (eventstore.SnapshotEnvelope[[]byte], error) {
	if err := readAid(item, aid); err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	key, err := readInteger(item, "skey")
	if err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	if key != 0 {
		return eventstore.SnapshotEnvelope[[]byte]{}, fmt.Errorf("stored current snapshot skey is not zero")
	}
	seqNr, err := readInteger(item, "seq_nr")
	if err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	updatedAt, err := readInteger(item, "last_updated_at")
	if err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	if updatedAt < time.Unix(0, math.MinInt64).UnixMilli() || updatedAt > time.Unix(0, math.MaxInt64).UnixMilli() {
		return eventstore.SnapshotEnvelope[[]byte]{}, fmt.Errorf("stored last_updated_at is outside the occurrence time range")
	}
	manifest, err := readString(item, "manifest")
	if err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	payload, err := readPayload(item)
	if err != nil {
		return eventstore.SnapshotEnvelope[[]byte]{}, err
	}
	return eventstore.NewSnapshotEnvelope(payload, eventstore.SeqNr(seqNr), eventstore.WithManifest(manifest))
}
