package event_store_adapter_go

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type EventStore struct {
	client               *dynamodb.Client
	journalTableName     string
	journalAidIndexName  string
	snapshotTableName    string
	snapshotAidIndexName string
	keyResolver          KeyResolver
	shardCount           uint64
}

func NewEventStore(client *dynamodb.Client, journalTableName string, snapshotTableName string,
	journalAidIndexName string,
	snapshotAidIndexName string, keyResolver KeyResolver, shardCount uint64) *EventStore {
	return &EventStore{client, journalTableName,
		journalAidIndexName,
		snapshotTableName,
		snapshotAidIndexName, keyResolver, shardCount}

}

func (es *EventStore) putSnapshot(event Event, aggregate Aggregate) (*types.Put, error) {
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), 0)
	payload, err := json.Marshal(aggregate)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.snapshotTableName),
		Item: map[string]types.AttributeValue{
			"pkey":    &types.AttributeValueMemberS{Value: pkey},
			"skey":    &types.AttributeValueMemberS{Value: skey},
			"aid":     &types.AttributeValueMemberS{Value: event.GetAggregateId().String()},
			"seq_nr":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", event.GetSeqNr())},
			"payload": &types.AttributeValueMemberS{Value: string(payload)},
			"version": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", 1)},
		},
	}
	return &input, nil
}

func (es *EventStore) updateSnapshot(event Event, version uint64, aggregate Aggregate) (*types.Update, error) {
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), 0)
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
			":before_version": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", version)},
			":after_version":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", version+1)},
		},
		ConditionExpression: aws.String("#version=:before_version"),
	}
	if aggregate != nil {
		payload, err := json.Marshal(aggregate)
		if err != nil {
			return nil, err
		}
		update.UpdateExpression = aws.String("SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version")
		update.ExpressionAttributeNames["#seq_nr"] = "seq_nr"
		update.ExpressionAttributeNames["#payload"] = "payload"
		update.ExpressionAttributeValues[":seq_nr"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", event.GetSeqNr())}
		update.ExpressionAttributeValues[":payload"] = &types.AttributeValueMemberS{Value: string(payload)}
	}
	return &update, nil
}

func (es *EventStore) putJournal(event Event) (*types.Put, error) {
	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId().GetTypeName(), event.GetAggregateId().String(), event.GetSeqNr())
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	input := types.Put{
		TableName: aws.String(es.journalTableName),
		Item: map[string]types.AttributeValue{
			"pkey":        &types.AttributeValueMemberS{Value: pkey},
			"skey":        &types.AttributeValueMemberS{Value: skey},
			"aid":         &types.AttributeValueMemberS{Value: event.GetAggregateId().String()},
			"seq_nr":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", event.GetSeqNr())},
			"payload":     &types.AttributeValueMemberS{Value: string(payload)},
			"occurred_at": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", event.GetOccurredAt())},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}
	return &input, nil
}

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
			":seq_nr": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", 0)},
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
		err = json.Unmarshal([]byte(result.Items[0]["payload"].(*types.AttributeValueMemberS).Value), &aggregateMap)
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
			":seq_nr": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", seqNr)},
		},
	})
	if err != nil {
		return nil, err
	}
	var events []Event
	if len(result.Items) > 0 {
		for _, item := range result.Items {
			var aggregateMap map[string]interface{}
			err = json.Unmarshal([]byte(item["payload"].(*types.AttributeValueMemberS).Value), &aggregateMap)
			if err != nil {
				return nil, err
			}
			event, err := converter(aggregateMap)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}
	return events, nil
}

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
