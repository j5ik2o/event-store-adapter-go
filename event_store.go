package event_store_adapter_go

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// EventStore for DynamoDB.
type EventStore struct {
	client               *dynamodb.Client
	journalTableName     string
	snapshotTableName    string
	journalAidIndexName  string
	snapshotAidIndexName string
	shardCount           uint64
	keyResolver          KeyResolver
	eventSerializer      EventSerializer
	snapshotSerializer   SnapshotSerializer
}

// EventStoreOption is an option for EventStore.
type EventStoreOption func(*EventStore) error

// WithKeyResolver sets a key resolver.
func WithKeyResolver(keyResolver KeyResolver) EventStoreOption {
	return func(es *EventStore) error {
		es.keyResolver = keyResolver
		return nil
	}
}

// WithEventSerializer sets an event serializer.
func WithEventSerializer(eventSerializer EventSerializer) EventStoreOption {
	return func(es *EventStore) error {
		es.eventSerializer = eventSerializer
		return nil
	}
}

// WithSnapshotSerializer sets a snapshot serializer.
func WithSnapshotSerializer(snapshotSerializer SnapshotSerializer) EventStoreOption {
	return func(es *EventStore) error {
		es.snapshotSerializer = snapshotSerializer
		return nil
	}
}

// NewEventStore returns a new EventStore.
func NewEventStore(
	client *dynamodb.Client,
	journalTableName string,
	snapshotTableName string,
	journalAidIndexName string,
	snapshotAidIndexName string,
	shardCount uint64,
	options ...EventStoreOption,
) (*EventStore, error) {
	es := &EventStore{
		client,
		journalTableName,
		snapshotTableName,
		journalAidIndexName,
		snapshotAidIndexName,
		shardCount,
		&DefaultKeyResolver{},
		&JsonEventSerializer{},
		&JsonSnapshotSerializer{},
	}
	for _, option := range options {
		if err := option(es); err != nil {
			return nil, err
		}
	}
	return es, nil
}

// putSnapshot returns a PutInput for snapshot.
func (es *EventStore) putSnapshot(event Event, aggregate Aggregate) (*types.Put, error) {
	pkey := es.keyResolver.ResolvePkey(event, es.shardCount)
	skey := es.keyResolver.ResolveSkey(event, 0)
	payload, err := es.snapshotSerializer.Serialize(aggregate)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.snapshotTableName),
		Item: map[string]types.AttributeValue{
			"pkey":    &types.AttributeValueMemberS{Value: pkey},
			"skey":    &types.AttributeValueMemberS{Value: skey},
			"aid":     &types.AttributeValueMemberS{Value: event.GetAggregateId().String()},
			"seq_nr":  &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSeqNr(), 10)},
			"payload": &types.AttributeValueMemberB{Value: payload},
			"version": &types.AttributeValueMemberN{Value: "1"},
		},
	}
	return &input, nil
}

// updateSnapshot returns an UpdateInput for snapshot.
func (es *EventStore) updateSnapshot(event Event, version uint64, aggregate Aggregate) (*types.Update, error) {
	pkey := es.keyResolver.ResolvePkey(event, es.shardCount)
	skey := es.keyResolver.ResolveSkey(event, 0)
	update := types.Update{
		TableName:        aws.String(es.snapshotTableName),
		UpdateExpression: aws.String("SET #version=:after_version"),
		Key: map[string]types.AttributeValue{
			"pkey": &types.AttributeValueMemberS{Value: pkey},
			"skey": &types.AttributeValueMemberS{Value: skey},
		},
		ExpressionAttributeNames: map[string]string{
			"#version": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":before_version": &types.AttributeValueMemberN{Value: strconv.FormatUint(version, 10)},
			":after_version":  &types.AttributeValueMemberN{Value: strconv.FormatUint(version+1, 10)},
		},
		ConditionExpression: aws.String("#version=:before_version"),
	}
	if aggregate != nil {
		payload, err := es.snapshotSerializer.Serialize(aggregate)
		if err != nil {
			return nil, err
		}
		update.UpdateExpression = aws.String("SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version")
		update.ExpressionAttributeNames["#seq_nr"] = "seq_nr"
		update.ExpressionAttributeNames["#payload"] = "payload"
		update.ExpressionAttributeValues[":seq_nr"] = &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSeqNr(), 10)}
		update.ExpressionAttributeValues[":payload"] = &types.AttributeValueMemberB{Value: payload}
	}
	return &update, nil
}

// putJournal returns a PutInput for journal.
func (es *EventStore) putJournal(event Event) (*types.Put, error) {
	pkey := es.keyResolver.ResolvePkey(event, es.shardCount)
	skey := es.keyResolver.ResolveSkey(event, event.GetSeqNr())
	payload, err := es.eventSerializer.Serialize(event)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.journalTableName),
		Item: map[string]types.AttributeValue{
			"pkey":        &types.AttributeValueMemberS{Value: pkey},
			"skey":        &types.AttributeValueMemberS{Value: skey},
			"aid":         &types.AttributeValueMemberS{Value: event.GetAggregateId().String()},
			"seq_nr":      &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSeqNr(), 10)},
			"payload":     &types.AttributeValueMemberB{Value: payload},
			"occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetOccurredAt(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}
	return &input, nil
}

// GetSnapshotById returns a snapshot by aggregateId.
//
// - aggregateId is an aggregateId to get a snapshot.
// - converter is a converter to convert a map to an aggregate.
//
// Returns a snapshot and an error.
func (es *EventStore) GetSnapshotById(aggregateId AggregateId, converter AggregateConverter) (*AggregateWithSeqNrWithVersion, error) {
	result, err := es.client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr > :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.String()},
			":seq_nr": &types.AttributeValueMemberN{Value: "0"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(result.Items) == 1 {
		seqNr, err := strconv.ParseUint(result.Items[0]["seq_nr"].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			return nil, err
		}
		version, err := strconv.ParseUint(result.Items[0]["version"].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			return nil, err
		}
		var aggregateMap map[string]interface{}
		err = es.snapshotSerializer.Deserialize(result.Items[0]["payload"].(*types.AttributeValueMemberB).Value, &aggregateMap)
		if err != nil {
			return nil, err
		}
		aggregate, err := converter(aggregateMap)
		if err != nil {
			return nil, err
		}
		return &AggregateWithSeqNrWithVersion{aggregate, seqNr, version}, nil
	} else {
		return nil, nil
	}
}

// GetEventsByIdAndSeqNr returns events by aggregateId and seqNr.
//
// - aggregateId is an aggregateId to get events.
// - seqNr is a seqNr to get events.
// - converter is a converter to convert a map to an event.
//
// Returns events and an error.
func (es *EventStore) GetEventsByIdAndSeqNr(aggregateId AggregateId, seqNr uint64, converter EventConverter) ([]Event, error) {
	result, err := es.client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              aws.String(es.journalTableName),
		IndexName:              aws.String(es.journalAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr > :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.String()},
			":seq_nr": &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)},
		},
	})
	if err != nil {
		return nil, err
	}
	var events []Event
	if len(result.Items) > 0 {
		for _, item := range result.Items {
			var eventMap map[string]interface{}
			err = es.eventSerializer.Deserialize(item["payload"].(*types.AttributeValueMemberB).Value, &eventMap)
			if err != nil {
				return nil, err
			}
			event, err := converter(eventMap)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}
	return events, nil
}

// StoreEventWithSnapshot stores an event and a snapshot atomically.
//
// - event is an event to store.
// - version is a version of the aggregate.
// - aggregate is an aggregate to store.
//   - Required when event is created, otherwise you can choose whether or not to save a snapshot.
//   - If you do not want to save snapshots, specify nil.
//
// Occurs an error, if the event and the snapshot can not stored.
func (es *EventStore) StoreEventWithSnapshot(event Event, version uint64, aggregate Aggregate) error {
	if event.IsCreated() && aggregate != nil {
		putJournal, err := es.putJournal(event)
		if err != nil {
			return err
		}
		putSnapshot, err := es.putSnapshot(event, aggregate)
		if err != nil {
			return err
		}
		_, err = es.client.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Put: putSnapshot},
				{Put: putJournal},
			},
		})
		if err != nil {
			return err
		}
	} else if event.IsCreated() && aggregate != nil {
		panic("Aggregate is not found")
	} else {
		putJournal, err := es.putJournal(event)
		if err != nil {
			return err
		}
		updateSnapshot, err := es.updateSnapshot(event, version, aggregate)
		if err != nil {
			return err
		}
		_, err = es.client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Update: updateSnapshot},
				{Put: putJournal},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
