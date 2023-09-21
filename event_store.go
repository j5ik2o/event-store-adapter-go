package event_store_adapter_go

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

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
	keepSnapshot         bool
	keepSnapshotCount    uint32
	deleteTtl            time.Duration
	keyResolver          KeyResolver
	eventSerializer      EventSerializer
	snapshotSerializer   SnapshotSerializer
}

// EventStoreOption is an option for EventStore.
type EventStoreOption func(*EventStore) error

// WithKeepSnapshot sets whether or not to keep snapshots.
//
// - If you want to keep snapshots, specify true.
// - If you do not want to keep snapshots, specify false.
// - The default is false.
//
// Returns an EventStoreOption.
func WithKeepSnapshot(keepSnapshot bool) EventStoreOption {
	return func(es *EventStore) error {
		es.keepSnapshot = keepSnapshot
		return nil
	}
}

// WithDeleteTtl sets a delete ttl.
//
// - If you want to delete snapshots, specify a time.Duration.
// - If you do not want to delete snapshots, specify math.MaxInt64.
// - The default is math.MaxInt64.
//
// Returns an EventStoreOption.
func WithDeleteTtl(deleteTtl time.Duration) EventStoreOption {
	return func(es *EventStore) error {
		es.deleteTtl = deleteTtl
		return nil
	}
}

// WithKeepSnapshotCount sets a keep snapshot count.
//
// - If you want to keep snapshots, specify a keep snapshot count.
// - If you do not want to keep snapshots, specify math.MaxInt64.
// - The default is math.MaxInt64.
//
// Returns an EventStoreOption.
func WithKeepSnapshotCount(keepSnapshotCount uint32) EventStoreOption {
	return func(es *EventStore) error {
		es.keepSnapshotCount = keepSnapshotCount
		return nil
	}
}

// WithKeyResolver sets a key resolver.
//
// - If you want to change the key resolver, specify a KeyResolver.
// - The default is DefaultKeyResolver.
//
// Returns an EventStoreOption.
func WithKeyResolver(keyResolver KeyResolver) EventStoreOption {
	return func(es *EventStore) error {
		es.keyResolver = keyResolver
		return nil
	}
}

// WithEventSerializer sets an event serializer.
//
// - If you want to change the event serializer, specify an EventSerializer.
// - The default is JsonEventSerializer.
//
// Returns an EventStoreOption.
func WithEventSerializer(eventSerializer EventSerializer) EventStoreOption {
	return func(es *EventStore) error {
		es.eventSerializer = eventSerializer
		return nil
	}
}

// WithSnapshotSerializer sets a snapshot serializer.
//
// - If you want to change the snapshot serializer, specify a SnapshotSerializer.
// - The default is JsonSnapshotSerializer.
//
// Returns an EventStoreOption.
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
	if client == nil {
		panic("client is nil")
	}
	if journalTableName == "" {
		panic("journalTableName is empty")
	}
	if snapshotTableName == "" {
		panic("snapshotTableName is empty")
	}
	if journalAidIndexName == "" {
		panic("journalAidIndexName is empty")
	}
	if snapshotAidIndexName == "" {
		panic("snapshotAidIndexName is empty")
	}
	if shardCount == 0 {
		panic("shardCount is zero")
	}
	es := &EventStore{
		client,
		journalTableName,
		snapshotTableName,
		journalAidIndexName,
		snapshotAidIndexName,
		shardCount,
		false,
		1,
		math.MaxInt64,
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
//
// - event is an event to store.
// - seqNr is a seqNr of the event.
// - aggregate is an aggregate to store.
//
// Returns a PutInput and an error.
func (es *EventStore) putSnapshot(event Event, seqNr uint64, aggregate Aggregate) (*types.Put, error) {
	if event == nil {
		panic("event is nil")
	}
	if aggregate == nil {
		panic("aggregate is nil")
	}
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), seqNr)
	payload, err := es.snapshotSerializer.Serialize(aggregate)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.snapshotTableName),
		Item: map[string]types.AttributeValue{
			"pkey":    &types.AttributeValueMemberS{Value: pkey},
			"skey":    &types.AttributeValueMemberS{Value: skey},
			"aid":     &types.AttributeValueMemberS{Value: event.GetAggregateId().AsString()},
			"seq_nr":  &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)},
			"payload": &types.AttributeValueMemberB{Value: payload},
			"version": &types.AttributeValueMemberN{Value: "1"},
			"ttl":     &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}
	return &input, nil
}

// updateSnapshot returns an UpdateInput for snapshot.
//
// - event is an event to store.
// - seqNr is a seqNr of the event.
// - version is a version of the aggregate.
// - aggregate is an aggregate to store.
//   - Required when event is created, otherwise you can choose whether or not to save a snapshot.
//
// Returns an UpdateInput and an error.
func (es *EventStore) updateSnapshot(event Event, seqNr uint64, version uint64, aggregate Aggregate) (*types.Update, error) {
	if event == nil {
		panic("event is nil")
	}
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), seqNr)
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
		update.ExpressionAttributeValues[":seq_nr"] = &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)}
		update.ExpressionAttributeValues[":payload"] = &types.AttributeValueMemberB{Value: payload}
	}
	return &update, nil
}

// putJournal returns a PutInput for journal.
//
// - event is an event to store.
//
// Returns a PutInput and an error.
func (es *EventStore) putJournal(event Event) (*types.Put, error) {
	if event == nil {
		panic("event is nil")
	}
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), event.GetSeqNr())
	payload, err := es.eventSerializer.Serialize(event)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.journalTableName),
		Item: map[string]types.AttributeValue{
			"pkey":        &types.AttributeValueMemberS{Value: pkey},
			"skey":        &types.AttributeValueMemberS{Value: skey},
			"aid":         &types.AttributeValueMemberS{Value: event.GetAggregateId().AsString()},
			"seq_nr":      &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSeqNr(), 10)},
			"payload":     &types.AttributeValueMemberB{Value: payload},
			"occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetOccurredAt(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}
	return &input, nil
}

// GetLatestSnapshotById returns a snapshot by aggregateId.
//
// - aggregateId is an aggregateId to get a snapshot.
// - converter is a converter to convert a map to an aggregate.
//
// Returns a snapshot and an error.
func (es *EventStore) GetLatestSnapshotById(aggregateId AggregateId, converter AggregateConverter) (*AggregateResult, error) {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}
	if converter == nil {
		panic("converter is nil")
	}
	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr = :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: "0"},
		},
		Limit: aws.Int32(1),
	}
	result, err := es.client.Query(context.Background(), request)
	if err != nil {
		return nil, &IOError{EventStoreBaseError{"Failed to GetLatestSnapshotById query", err}}
	}
	if len(result.Items) == 0 {
		return &AggregateResult{}, nil
	} else if len(result.Items) == 1 {
		version, err := strconv.ParseUint(result.Items[0]["version"].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			return nil, &DeserializationError{EventStoreBaseError{"Failed to parse the version", err}}
		}
		var aggregateMap map[string]interface{}
		err = es.snapshotSerializer.Deserialize(result.Items[0]["payload"].(*types.AttributeValueMemberB).Value, &aggregateMap)
		if err != nil {
			return nil, err
		}
		aggregate, err := converter(aggregateMap)
		if err != nil {
			return nil, &DeserializationError{EventStoreBaseError{"Failed to convert the snapshot", err}}
		}
		return &AggregateResult{aggregate.WithVersion(version)}, nil
	} else {
		panic("len(result.Items) > 1")
	}
}

// GetEventsByIdSinceSeqNr returns events by aggregateId and seqNr.
//
// - aggregateId is an aggregateId to get events.
// - seqNr is a seqNr to get events.
// - converter is a converter to convert a map to an event.
//
// Returns events and an error.
func (es *EventStore) GetEventsByIdSinceSeqNr(aggregateId AggregateId, seqNr uint64, converter EventConverter) ([]Event, error) {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}
	if converter == nil {
		panic("converter is nil")
	}
	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.journalTableName),
		IndexName:              aws.String(es.journalAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr >= :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)},
		},
	}
	result, err := es.client.Query(context.Background(), request)
	if err != nil {
		return nil, &IOError{EventStoreBaseError{"Failed to GetEventsByIdSinceSeqNr query", err}}
	}
	var events []Event
	if len(result.Items) > 0 {
		for _, item := range result.Items {
			var eventMap map[string]interface{}
			if err := es.eventSerializer.Deserialize(item["payload"].(*types.AttributeValueMemberB).Value, &eventMap); err != nil {
				return nil, err
			}
			event, err := converter(eventMap)
			if err != nil {
				return nil, &DeserializationError{EventStoreBaseError{"Failed to convert the event", err}}
			}
			events = append(events, event)
		}
	}
	return events, nil
}

func (es *EventStore) PersistEvent(event Event, version uint64) error {
	if event.IsCreated() {
		panic("event is created")
	}
	if err := es.updateEventAndSnapshotOpt(event, version, nil); err != nil {
		return err
	}
	if err := es.tryPurgeExcessSnapshots(event); err != nil {
		return err
	}
	return nil
}

func (es *EventStore) PersistEventAndSnapshot(event Event, aggregate Aggregate) error {
	if event.IsCreated() {
		if err := es.createEventAndSnapshot(event, aggregate); err != nil {
			return err
		}
	} else {
		if err := es.updateEventAndSnapshotOpt(event, aggregate.GetVersion(), aggregate); err != nil {
			return err
		}
		if err := es.tryPurgeExcessSnapshots(event); err != nil {
			return err
		}
	}
	return nil
}

func (es *EventStore) tryPurgeExcessSnapshots(event Event) error {
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		if es.deleteTtl < math.MaxInt64 {
			if err := es.deleteExcessSnapshots(event.GetAggregateId()); err != nil {
				return err
			}
		} else {
			if err := es.updateTtlOfExcessSnapshots(event.GetAggregateId()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (es *EventStore) updateEventAndSnapshotOpt(event Event, version uint64, aggregate Aggregate) error {
	if event == nil {
		panic("event is nil")
	}
	putJournal, err := es.putJournal(event)
	if err != nil {
		return err
	}
	updateSnapshot, err := es.updateSnapshot(event, 0, version, aggregate)
	if err != nil {
		return err
	}
	transactItems := []types.TransactWriteItem{
		{Update: updateSnapshot},
		{Put: putJournal},
	}
	if es.keepSnapshot && aggregate != nil {
		putSnapshot2, err := es.putSnapshot(event, aggregate.GetSeqNr(), aggregate)
		if err != nil {
			return err
		}
		transactItems = append(transactItems, types.TransactWriteItem{Put: putSnapshot2})
	}
	request := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}
	_, err = es.client.TransactWriteItems(context.Background(), request)
	if err != nil {
		var t *types.TransactionCanceledException
		switch {
		case errors.As(err, &t):
			return &TransactionCanceledError{EventStoreBaseError{"Transaction write was canceled", err}}
		default:
			return &IOError{EventStoreBaseError{"Failed to transact write items", err}}
		}
	}
	return nil
}

func (es *EventStore) createEventAndSnapshot(event Event, aggregate Aggregate) error {
	if event == nil {
		panic("event is nil")
	}
	putJournal, err := es.putJournal(event)
	if err != nil {
		return err
	}
	putSnapshot, err := es.putSnapshot(event, 0, aggregate)
	if err != nil {
		return err
	}
	transactItems := []types.TransactWriteItem{
		{Put: putSnapshot},
		{Put: putJournal},
	}

	if es.keepSnapshot {
		putSnapshot2, err := es.putSnapshot(event, aggregate.GetSeqNr(), aggregate)
		if err != nil {
			return err
		}
		transactItems = append(transactItems, types.TransactWriteItem{Put: putSnapshot2})
	}
	request := &dynamodb.TransactWriteItemsInput{TransactItems: transactItems}
	if _, err = es.client.TransactWriteItems(context.TODO(), request); err != nil {
		var t *types.TransactionCanceledException
		switch {
		case errors.As(err, &t):
			return &TransactionCanceledError{EventStoreBaseError{"Transaction write was canceled", err}}
		default:
			return &IOError{EventStoreBaseError{"Failed to transact write items", err}}
		}
	}
	return nil
}

func (es *EventStore) deleteExcessSnapshots(aggregateId AggregateId) error {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		snapshotCount, err := es.getSnapshotCount(aggregateId)
		if err != nil {
			return err
		}
		snapshotCount -= 1
		excessCount := uint32(snapshotCount) - es.keepSnapshotCount
		if excessCount > 0 {
			keys, err := es.getLastSnapshotKeys(aggregateId, int32(excessCount))
			if err != nil {
				return err
			}
			var requests []types.WriteRequest
			for _, key := range keys {
				request := types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pkey": &types.AttributeValueMemberS{Value: key.pkey},
							"skey": &types.AttributeValueMemberS{Value: key.skey},
						},
					},
				}
				requests = append(requests, request)
			}
			request := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{es.snapshotTableName: requests}}
			if _, err = es.client.BatchWriteItem(context.Background(), request); err != nil {
				return &IOError{EventStoreBaseError{"Failed to deleteExcessSnapshots updateItem", err}}
			}
		}
	}
	return nil
}

func (es *EventStore) updateTtlOfExcessSnapshots(aggregateId AggregateId) error {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		snapshotCount, err := es.getSnapshotCount(aggregateId)
		if err != nil {
			return err
		}
		snapshotCount -= 1
		excessCount := uint32(snapshotCount) - es.keepSnapshotCount
		if excessCount > 0 {
			keys, err := es.getLastSnapshotKeys(aggregateId, int32(excessCount))
			if err != nil {
				return err
			}
			ttl := time.Now().Add(es.deleteTtl).Unix()
			for _, key := range keys {
				request := &dynamodb.UpdateItemInput{
					TableName: aws.String(es.snapshotTableName),
					Key: map[string]types.AttributeValue{
						"pkey": &types.AttributeValueMemberS{Value: key.pkey},
						"skey": &types.AttributeValueMemberS{Value: key.skey},
					},
					UpdateExpression: aws.String("SET #ttl=:ttl"),
					ExpressionAttributeNames: map[string]string{
						"#ttl": "ttl",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
					},
				}
				if _, err := es.client.UpdateItem(context.Background(), request); err != nil {
					return &IOError{EventStoreBaseError{"Failed to updateTtlOfExcessSnapshots updateItem", err}}
				}
			}
		}
	}
	return nil
}

func (es *EventStore) getSnapshotCount(id AggregateId) (int32, error) {
	if id == nil {
		panic("id is nil")
	}
	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid"),
		ExpressionAttributeNames: map[string]string{
			"#aid": "aid",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid": &types.AttributeValueMemberS{Value: id.AsString()},
		},
		Select: types.SelectCount,
	}
	response, err := es.client.Query(context.Background(), request)
	if err != nil {
		return 0, &IOError{EventStoreBaseError{"Failed to getSnapshotCount query", err}}
	}
	return response.Count, nil
}

type pkeyAndSkey struct {
	pkey string
	skey string
}

func (es *EventStore) getLastSnapshotKeys(aid AggregateId, limit int32) ([]pkeyAndSkey, error) {
	if aid == nil {
		panic("aid is nil")
	}
	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr > :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aid.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: "0"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(limit),
	}
	if es.deleteTtl < math.MaxInt64 {
		request.FilterExpression = aws.String("#ttl = :ttl")
		request.ExpressionAttributeNames["#ttl"] = "ttl"
		request.ExpressionAttributeValues[":ttl"] = &types.AttributeValueMemberN{Value: "0"}
	}
	response, err := es.client.Query(context.Background(), request)
	if err != nil {
		return nil, &IOError{EventStoreBaseError{"Failed to getLastSnapshotKeys query", err}}
	}
	var pkeySkeys []pkeyAndSkey
	for _, item := range response.Items {
		pkey := item["pkey"].(*types.AttributeValueMemberS).Value
		skey := item["skey"].(*types.AttributeValueMemberS).Value
		pkeySkey := pkeyAndSkey{
			pkey: pkey,
			skey: skey,
		}
		pkeySkeys = append(pkeySkeys, pkeySkey)
	}
	return pkeySkeys, nil
}
